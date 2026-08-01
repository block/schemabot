package tern

import (
	"context"
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
	err := client.checkActiveTaskConflict(t.Context(), plan, "")
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
	err := client.checkActiveTaskConflict(t.Context(), plan, "")
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

// A sharded apply is dispatched one shard at a time, and different shards are
// distinct physical primaries that run concurrently. The conflict check must
// therefore be per-shard: an active task on another shard of the same database
// must not refuse a new shard's apply (otherwise a sharded fan-out serializes on
// its first shard), while same-shard work still conflicts.
func TestConflictCheckIsPerShard(t *testing.T) {
	activeShard := &storage.Task{
		ID:             1,
		TaskIdentifier: "task-shard-neg40",
		Database:       "cdb_resolute",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Namespace:      "cdb_resolute_sharded",
		TableName:      "mutes",
		Shard:          "-40",
		State:          state.Task.Running,
	}
	// The engine reports active work (not "No active schema change"), so the
	// running task genuinely blocks rather than being cleaned up as abandoned.
	client := &LocalClient{
		config: LocalConfig{Database: "cdb_resolute", Type: storage.DatabaseTypeMySQL},
		storage: &exactProgressStorage{
			tasks: &exactProgressTaskStore{tasks: []*storage.Task{activeShard}},
			logs:  &mockApplyLogStore{},
		},
		spiritEngine: &fakeControlEngine{
			progressResult: &engine.ProgressResult{State: engine.StateRunning, Message: "Copying rows"},
		},
		logger: slog.Default(),
	}
	plan := &storage.Plan{Database: "cdb_resolute", DatabaseType: storage.DatabaseTypeMySQL}
	tasks := []*storage.Task{activeShard}

	// Assert the shard gate directly via findBlockingTask. checkActiveTaskConflict
	// wraps it in a stale-task retry loop that sleeps ~1s on the blocking case,
	// which this test does not need to exercise.

	// A different shard is not a conflict — it runs concurrently.
	assert.Empty(t, client.findBlockingTask(t.Context(), tasks, plan, "40-80"),
		"an active task on shard -40 must not block an apply on shard 40-80")
	assert.Equal(t, state.Task.Running, activeShard.State, "the other shard's task is left running")

	// The same shard still conflicts.
	assert.Equal(t, "task-shard-neg40", client.findBlockingTask(t.Context(), tasks, plan, "-40"),
		"an active task on shard -40 must block another apply on shard -40")
}

// A pending task whose apply already reached a terminal state is orphaned: the
// apply will never be claimed again, so the task can never start, and pending
// means no engine work or checkpoint exists. The conflict check cancels it so
// it stops blocking the database, and the new apply is admitted.
func TestConflictCheckCancelsOrphanedPendingTask(t *testing.T) {
	for _, applyState := range []string{
		state.Apply.Completed,
		state.Apply.Failed,
		state.Apply.Cancelled,
	} {
		t.Run(applyState, func(t *testing.T) {
			orphan := &storage.Task{
				ID:             6,
				ApplyID:        61,
				TaskIdentifier: "task-orphan",
				Database:       "testdb",
				DatabaseType:   storage.DatabaseTypeMySQL,
				TableName:      "users",
				Shard:          "-80",
				State:          state.Task.Pending,
			}
			client := newNoActiveChangeClient("testdb", []*storage.Task{orphan})
			client.storage.(*exactProgressStorage).applies = &mockApplyStore{apply: &storage.Apply{
				ID: 61, ApplyIdentifier: "apply-terminal", Database: "testdb",
				DatabaseType: storage.DatabaseTypeMySQL, State: applyState,
			}}

			plan := &storage.Plan{Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}
			err := client.checkActiveTaskConflict(t.Context(), plan, "")
			require.NoError(t, err, "an orphaned pending task must not refuse a new apply")
			assert.Equal(t, state.Task.Cancelled, orphan.State, "the orphaned task must be cancelled")
			assert.Contains(t, orphan.ErrorMessage, "orphaned")
			assert.NotNil(t, orphan.CompletedAt)
		})
	}
}

// A pending task whose apply is still active is normal queued work — the drive
// that owns it will start it. The conflict check must leave it pending and
// refuse the new apply.
func TestConflictCheckPreservesPendingTaskOfActiveApply(t *testing.T) {
	pending := &storage.Task{
		ID:             7,
		ApplyID:        71,
		TaskIdentifier: "task-pending-active",
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		TableName:      "users",
		State:          state.Task.Pending,
	}
	client := newNoActiveChangeClient("testdb", []*storage.Task{pending})
	client.storage.(*exactProgressStorage).applies = &mockApplyStore{apply: &storage.Apply{
		ID: 71, ApplyIdentifier: "apply-active", Database: "testdb",
		DatabaseType: storage.DatabaseTypeMySQL, State: state.Apply.Running,
	}}

	plan := &storage.Plan{Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}
	err := client.checkActiveTaskConflict(t.Context(), plan, "")
	require.Error(t, err, "a pending task of an active apply must refuse a new apply")
	assert.Contains(t, err.Error(), "schema change already in progress")
	assert.Equal(t, state.Task.Pending, pending.State, "the pending task must be left untouched")
	assert.Empty(t, pending.ErrorMessage)
	assert.Nil(t, pending.CompletedAt)
}

// When the pending task's apply cannot be loaded — a storage failure or a
// missing row — the ownership question is unresolved, so the task keeps
// blocking rather than being cancelled on uncertainty.
func TestConflictCheckKeepsPendingTaskOnApplyLookupUncertainty(t *testing.T) {
	t.Run("apply row missing", func(t *testing.T) {
		pending := &storage.Task{
			ID:             8,
			ApplyID:        81,
			TaskIdentifier: "task-pending-no-apply",
			Database:       "testdb",
			DatabaseType:   storage.DatabaseTypeMySQL,
			TableName:      "users",
			State:          state.Task.Pending,
		}
		client := newNoActiveChangeClient("testdb", []*storage.Task{pending})
		client.storage.(*exactProgressStorage).applies = &mockApplyStore{apply: nil}

		plan := &storage.Plan{Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}
		err := client.checkActiveTaskConflict(t.Context(), plan, "")
		require.Error(t, err, "a pending task with a missing apply row must keep blocking")
		assert.Equal(t, state.Task.Pending, pending.State)
		assert.Nil(t, pending.CompletedAt)
	})

	t.Run("apply load error", func(t *testing.T) {
		pending := &storage.Task{
			ID:             9,
			ApplyID:        91,
			TaskIdentifier: "task-pending-load-error",
			Database:       "testdb",
			DatabaseType:   storage.DatabaseTypeMySQL,
			TableName:      "users",
			State:          state.Task.Pending,
		}
		client := newNoActiveChangeClient("testdb", []*storage.Task{pending})
		client.storage.(*exactProgressStorage).applies = &erroringApplyStore{err: errors.New("storage down")}

		plan := &storage.Plan{Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}
		err := client.checkActiveTaskConflict(t.Context(), plan, "")
		require.Error(t, err, "a pending task must keep blocking when its apply cannot be loaded")
		assert.Equal(t, state.Task.Pending, pending.State)
		assert.Nil(t, pending.CompletedAt)
	})
}

// erroringApplyStore fails every load, standing in for storage that is
// unavailable while the conflict check runs.
type erroringApplyStore struct {
	storage.ApplyStore
	err error
}

func (s *erroringApplyStore) Get(context.Context, int64) (*storage.Apply, error) {
	return nil, s.err
}

// An orphan only stops blocking once its cancellation is durably written. If
// the write fails, the task must keep blocking — reporting it resolved would
// admit the new apply while storage still records the orphan as active work —
// and the task must be left pending so a later conflict check retries the
// cancellation cleanly.
func TestConflictCheckKeepsOrphanWhenCancellationWriteFails(t *testing.T) {
	orphan := &storage.Task{
		ID:             10,
		ApplyID:        101,
		TaskIdentifier: "task-orphan-write-fails",
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		TableName:      "users",
		State:          state.Task.Pending,
	}
	client := newNoActiveChangeClient("testdb", []*storage.Task{orphan})
	stor := client.storage.(*exactProgressStorage)
	stor.tasks = &updateFailingTaskStore{
		exactProgressTaskStore: stor.tasks.(*exactProgressTaskStore),
		updateErr:              errors.New("storage down"),
	}
	stor.applies = &mockApplyStore{apply: &storage.Apply{
		ID: 101, ApplyIdentifier: "apply-terminal", Database: "testdb",
		DatabaseType: storage.DatabaseTypeMySQL, State: state.Apply.Completed,
	}}

	plan := &storage.Plan{Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}
	err := client.checkActiveTaskConflict(t.Context(), plan, "")
	require.Error(t, err, "the orphan must keep blocking when its cancellation cannot be written")
	assert.Contains(t, err.Error(), "schema change already in progress")
	assert.Equal(t, state.Task.Pending, orphan.State, "the task must be restored to pending for a clean retry")
	assert.Empty(t, orphan.ErrorMessage)
	assert.Nil(t, orphan.CompletedAt)
}

// updateFailingTaskStore serves tasks normally but fails every state write,
// standing in for storage that becomes unavailable mid-conflict-check.
type updateFailingTaskStore struct {
	*exactProgressTaskStore
	updateErr error
}

func (s *updateFailingTaskStore) Update(context.Context, *storage.Task) error {
	return s.updateErr
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
	err := client.checkActiveTaskConflict(t.Context(), plan, "")
	require.NoError(t, err, "new apply should proceed once the abandoned task is failed")
	assert.Equal(t, state.Task.Failed, running.State)
}
