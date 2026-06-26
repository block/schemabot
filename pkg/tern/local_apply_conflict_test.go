package tern

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// newNoActiveChangeClient builds a MySQL client whose engine always reports no
// running schema change — the exact response Spirit gives whenever it has no
// active runner, which is true both after a crash and while a task is
// intentionally paused.
func newNoActiveChangeClient(database string, tasks []*storage.Task) *LocalClient {
	return &LocalClient{
		config: LocalConfig{
			Database: database,
			Type:     storage.DatabaseTypeMySQL,
		},
		storage: &exactProgressStorage{
			tasks: &exactProgressTaskStore{tasks: tasks},
			logs:  &mockApplyLogStore{},
		},
		spiritEngine: &fakeControlEngine{
			progressResult: &engine.ProgressResult{
				State:   engine.StatePending,
				Message: "No active schema change",
			},
		},
		logger: slog.Default(),
	}
}

// A stopped task keeps its Spirit checkpoint and is resumable via Start. When an
// unrelated Apply runs on the same database, the engine reports no active work
// for that database — but the stopped task must stay stopped so its checkpoint
// and the operator's ability to resume are preserved, and the new apply must be
// refused rather than running over paused work.
func TestConflictCheckPreservesStoppedTask(t *testing.T) {
	stopped := &storage.Task{
		ID:             1,
		TaskIdentifier: "task-stopped",
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		TableName:      "users",
		State:          state.Task.Stopped,
	}
	client := newNoActiveChangeClient("testdb", []*storage.Task{stopped})

	resolved := client.tryResolveStaleTask(t.Context(), stopped, "testdb")
	assert.False(t, resolved, "stopped task must not be resolved as stale")
	assert.Equal(t, state.Task.Stopped, stopped.State, "stopped task must remain resumable")
	assert.Empty(t, stopped.ErrorMessage, "no abandoned-task error should be written to a stopped task")
	assert.Nil(t, stopped.CompletedAt)

	plan := &storage.Plan{Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}
	err := client.checkActiveTaskConflict(t.Context(), plan)
	require.Error(t, err, "a new apply must be refused while a stopped task holds the database")
	assert.Contains(t, err.Error(), "schema change already in progress")
	assert.Equal(t, state.Task.Stopped, stopped.State)
}

// A failed_retryable task is awaiting an operator retry and still owns its Spirit
// checkpoint. A new Apply on the same database sees no active engine work, but the
// task must not be converted to a terminal failure — doing so would silently void
// the operator's retry budget.
func TestConflictCheckPreservesRetryableTask(t *testing.T) {
	retryable := &storage.Task{
		ID:             2,
		TaskIdentifier: "task-retryable",
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		TableName:      "orders",
		State:          state.Task.FailedRetryable,
		ErrorMessage:   "engine connection reset",
	}
	client := newNoActiveChangeClient("testdb", []*storage.Task{retryable})

	resolved := client.tryResolveStaleTask(t.Context(), retryable, "testdb")
	assert.False(t, resolved, "retryable task must not be resolved as stale")
	assert.Equal(t, state.Task.FailedRetryable, retryable.State, "retryable task must remain retryable")
	assert.Equal(t, "engine connection reset", retryable.ErrorMessage, "original retry error must be preserved")
	assert.Nil(t, retryable.CompletedAt)

	plan := &storage.Plan{Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}
	err := client.checkActiveTaskConflict(t.Context(), plan)
	require.Error(t, err, "a new apply must be refused while a retryable task holds the database")
	assert.Contains(t, err.Error(), "schema change already in progress")
	assert.Equal(t, state.Task.FailedRetryable, retryable.State)
}

// When the engine's Progress call errors it may return a nil result. The conflict
// check must treat the task as unresolved (and keep it blocking) without
// dereferencing the result when err is non-nil — an earlier version logged
// result.State before the error check and panicked, crashing the Apply RPC
// whenever Progress failed (e.g. a DB connection torn down during shutdown).
func TestConflictCheckHandlesProgressError(t *testing.T) {
	running := &storage.Task{
		ID:             5,
		TaskIdentifier: "task-running",
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		TableName:      "events",
		State:          state.Task.Running,
	}
	client := &LocalClient{
		config: LocalConfig{
			Database: "testdb",
			Type:     storage.DatabaseTypeMySQL,
		},
		storage: &exactProgressStorage{
			tasks: &exactProgressTaskStore{tasks: []*storage.Task{running}},
			logs:  &mockApplyLogStore{},
		},
		spiritEngine: &fakeControlEngine{
			progressErr: errors.New("engine unreachable"),
		},
		logger: slog.Default(),
	}

	require.NotPanics(t, func() {
		resolved := client.tryResolveStaleTask(t.Context(), running, "testdb")
		assert.False(t, resolved, "task must not be resolved when the engine progress call errors")
	})
	assert.Equal(t, state.Task.Running, running.State, "task state must be left untouched on progress error")
	assert.Nil(t, running.CompletedAt)
}

// A running task whose engine has gone silent (e.g. the server crashed mid-apply)
// is genuinely abandoned: storage believes work is in flight but the engine has no
// runner. Such a task is failed so it stops blocking new applies for the database.
func TestConflictCheckFailsAbandonedInFlightTask(t *testing.T) {
	for _, inFlightState := range []string{
		state.Task.Running,
		state.Task.CuttingOver,
		state.Task.WaitingForCutover,
		state.Task.Recovering,
	} {
		t.Run(inFlightState, func(t *testing.T) {
			running := &storage.Task{
				ID:             3,
				TaskIdentifier: "task-running",
				Database:       "testdb",
				DatabaseType:   storage.DatabaseTypeMySQL,
				TableName:      "events",
				State:          inFlightState,
			}
			client := newNoActiveChangeClient("testdb", []*storage.Task{running})

			resolved := client.tryResolveStaleTask(t.Context(), running, "testdb")
			assert.True(t, resolved, "abandoned in-flight task must be resolved")
			assert.Equal(t, state.Task.Failed, running.State, "abandoned in-flight task must be failed")
			assert.Contains(t, running.ErrorMessage, "server may have crashed")
			assert.NotNil(t, running.CompletedAt)
		})
	}
}

// Once an abandoned in-flight task has been failed, it no longer blocks the
// database, so a new apply is admitted.
func TestConflictCheckAdmitsApplyAfterFailingAbandonedTask(t *testing.T) {
	running := &storage.Task{
		ID:             4,
		TaskIdentifier: "task-running",
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		TableName:      "events",
		State:          state.Task.Running,
	}
	client := newNoActiveChangeClient("testdb", []*storage.Task{running})

	plan := &storage.Plan{Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}
	err := client.checkActiveTaskConflict(t.Context(), plan)
	require.NoError(t, err, "new apply should proceed once the abandoned task is failed")
	assert.Equal(t, state.Task.Failed, running.State)
}
