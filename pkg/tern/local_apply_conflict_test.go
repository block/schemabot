package tern

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// newNoActiveChangeClient builds a MySQL client whose engine always reports no
// running schema change — the exact response Spirit gives whenever it has no
// active runner, which is true both after a crash and while a task is
// intentionally paused. The tasks' parent apply defaults to a running apply
// with no lease, so no live driver claims authority over the tasks; tests that
// exercise apply state or lease ownership replace the applies store.
func newNoActiveChangeClient(database string, tasks []*storage.Task) *LocalClient {
	return &LocalClient{
		config: LocalConfig{
			Database: database,
			Type:     storage.DatabaseTypeMySQL,
		},
		storage: &exactProgressStorage{
			applies: &mockApplyStore{apply: unleasedRunningApply(database)},
			tasks:   &exactProgressTaskStore{tasks: tasks},
			logs:    &mockApplyLogStore{},
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

// unleasedRunningApply builds a running parent apply with no lease — the shape
// of an apply whose driver has released or lost its claim, so the conflict
// check's engine probe (not a live drive) is the best available signal.
func unleasedRunningApply(database string) *storage.Apply {
	return &storage.Apply{
		ID:              1,
		ApplyIdentifier: "apply-under-test",
		Database:        database,
		DatabaseType:    storage.DatabaseTypeMySQL,
		State:           state.Apply.Running,
	}
}

// A stopped task keeps its Spirit checkpoint and is resumable via Start. When an
// unrelated Apply runs on the same database, the engine reports no active work
// for that database — but the stopped task must stay stopped so its checkpoint
// and the operator's ability to resume are preserved. Its apply is still
// claimable here, so a driver will reach the task on its own and the new apply
// is refused rather than running over work someone is about to drive.
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

	resolved := client.tryResolveStaleTask(t.Context(), stopped, unleasedRunningApply("testdb"), "testdb")
	assert.False(t, resolved, "stopped task must not be resolved as stale")
	assert.Equal(t, state.Task.Stopped, stopped.State, "stopped task must remain resumable")
	assert.Empty(t, stopped.ErrorMessage, "no abandoned-task error should be written to a stopped task")
	assert.Nil(t, stopped.CompletedAt)

	plan := &storage.Plan{Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}
	_, err := client.checkActiveTaskConflict(t.Context(), plan, "", 0)
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

	resolved := client.tryResolveStaleTask(t.Context(), retryable, unleasedRunningApply("testdb"), "testdb")
	assert.False(t, resolved, "retryable task must not be resolved as stale")
	assert.Equal(t, state.Task.FailedRetryable, retryable.State, "retryable task must remain retryable")
	assert.Equal(t, "engine connection reset", retryable.ErrorMessage, "original retry error must be preserved")
	assert.Nil(t, retryable.CompletedAt)

	plan := &storage.Plan{Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}
	_, err := client.checkActiveTaskConflict(t.Context(), plan, "", 0)
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
		resolved := client.tryResolveStaleTask(t.Context(), running, unleasedRunningApply("testdb"), "testdb")
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

			resolved := client.tryResolveStaleTask(t.Context(), running, unleasedRunningApply("testdb"), "testdb")
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
			applies: &mockApplyStore{apply: unleasedRunningApply("cdb_resolute")},
			tasks:   &exactProgressTaskStore{tasks: []*storage.Task{activeShard}},
			logs:    &mockApplyLogStore{},
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
	assert.False(t, client.findBlockingTask(t.Context(), tasks, plan, "40-80", 0).blocks(),
		"an active task on shard -40 must not block an apply on shard 40-80")
	assert.Equal(t, state.Task.Running, activeShard.State, "the other shard's task is left running")

	// The same shard still conflicts.
	assert.Equal(t, "task-shard-neg40", client.findBlockingTask(t.Context(), tasks, plan, "-40", 0).taskIdentifier,
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
			_, err := client.checkActiveTaskConflict(t.Context(), plan, "", 0)
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
	_, err := client.checkActiveTaskConflict(t.Context(), plan, "", 0)
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
		_, err := client.checkActiveTaskConflict(t.Context(), plan, "", 0)
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
		_, err := client.checkActiveTaskConflict(t.Context(), plan, "", 0)
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
	_, err := client.checkActiveTaskConflict(t.Context(), plan, "", 0)
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

// While a live driver holds the apply's lease (fresh heartbeat), the local
// engine probe says nothing about that driver's work — Spirit progress is
// in-memory and per-process. The conflict check must leave the task alone and
// refuse the new apply, whether the local engine memory happens to report a
// terminal state from an older run on the same database or has no memory of
// the database at all (which would otherwise read as abandoned work).
func TestConflictCheckLeavesActivelyDrivenTask(t *testing.T) {
	engineMemories := map[string]*engine.ProgressResult{
		"terminal memory of an older run": {State: engine.StateCompleted, Message: "Complete"},
		"no memory of the database":       {State: engine.StatePending, Message: "No active schema change"},
	}
	for name, memory := range engineMemories {
		t.Run(name, func(t *testing.T) {
			running := &storage.Task{
				ID:             11,
				ApplyID:        111,
				TaskIdentifier: "task-driven-elsewhere",
				Database:       "testdb",
				DatabaseType:   storage.DatabaseTypeMySQL,
				TableName:      "users",
				State:          state.Task.Running,
			}
			client := newNoActiveChangeClient("testdb", []*storage.Task{running})
			stor := client.storage.(*exactProgressStorage)
			stor.applies = &mockApplyStore{apply: &storage.Apply{
				ID: 111, ApplyIdentifier: "apply-driven", Database: "testdb",
				DatabaseType: storage.DatabaseTypeMySQL, State: state.Apply.Running,
				LeaseOwner: "other-host/4242/driver-0", LeaseToken: "token-live",
				UpdatedAt: time.Now(),
			}}
			client.spiritEngine = &fakeControlEngine{progressResult: memory}

			plan := &storage.Plan{Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}
			_, err := client.checkActiveTaskConflict(t.Context(), plan, "", 0)
			require.Error(t, err, "a task with an actively driven apply must refuse a new apply")
			assert.Contains(t, err.Error(), "schema change already in progress")
			assert.Equal(t, state.Task.Running, running.State, "the driven task must be left untouched")
			assert.Empty(t, running.ErrorMessage)
			assert.Nil(t, running.CompletedAt)
		})
	}
}

// When the apply's lease has gone stale but was last held by another process,
// a terminal report from the local engine is this process's memory of an older
// run on the same database — not the completing driver's own report. The task
// must keep blocking so driver stale-claim recovery settles it with real state.
func TestConflictCheckRefusesForeignTerminalReport(t *testing.T) {
	running := &storage.Task{
		ID:             12,
		ApplyID:        121,
		TaskIdentifier: "task-foreign-lease",
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		TableName:      "users",
		State:          state.Task.Running,
	}
	client := newNoActiveChangeClient("testdb", []*storage.Task{running})
	stor := client.storage.(*exactProgressStorage)
	stor.applies = &mockApplyStore{apply: &storage.Apply{
		ID: 121, ApplyIdentifier: "apply-foreign", Database: "testdb",
		DatabaseType: storage.DatabaseTypeMySQL, State: state.Apply.Running,
		LeaseOwner: "other-host/4242/driver-0", LeaseToken: "token-stale",
		UpdatedAt: time.Now().Add(-2 * storage.ApplyLeaseStaleAfter),
	}}
	client.spiritEngine = &fakeControlEngine{
		progressResult: &engine.ProgressResult{State: engine.StateCompleted, Message: "Complete"},
	}

	plan := &storage.Plan{Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}
	_, err := client.checkActiveTaskConflict(t.Context(), plan, "", 0)
	require.Error(t, err, "another process's task must not be stamped from this process's engine memory")
	assert.Contains(t, err.Error(), "schema change already in progress")
	assert.Equal(t, state.Task.Running, running.State, "the task must be left for driver recovery")
	assert.Nil(t, running.CompletedAt)
}

// A terminal report is trusted when the stale lease was last held by this
// process: the engine memory is the completing driver's own record (e.g. the
// drive finished but the final storage write was lost), so the task is settled
// with the engine's state and the new apply is admitted.
func TestConflictCheckStampsOwnProcessTerminalReport(t *testing.T) {
	running := &storage.Task{
		ID:             13,
		ApplyID:        131,
		TaskIdentifier: "task-own-lease",
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		TableName:      "users",
		State:          state.Task.Running,
	}
	client := newNoActiveChangeClient("testdb", []*storage.Task{running})
	stor := client.storage.(*exactProgressStorage)
	stor.applies = &mockApplyStore{apply: &storage.Apply{
		ID: 131, ApplyIdentifier: "apply-own", Database: "testdb",
		DatabaseType: storage.DatabaseTypeMySQL, State: state.Apply.Running,
		LeaseOwner: storage.LeaseOwnerProcess() + "/driver-0", LeaseToken: "token-own",
		UpdatedAt: time.Now().Add(-2 * storage.ApplyLeaseStaleAfter),
	}}
	client.spiritEngine = &fakeControlEngine{
		progressResult: &engine.ProgressResult{State: engine.StateCompleted, Message: "Complete"},
	}

	plan := &storage.Plan{Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}
	_, err := client.checkActiveTaskConflict(t.Context(), plan, "", 0)
	require.NoError(t, err, "this process's own terminal report must settle the task and admit the new apply")
	assert.Equal(t, state.Task.Completed, running.State, "the task must carry the engine's terminal state")
	assert.NotNil(t, running.CompletedAt)
}

// Crash recovery survives ownership gating: when the stale lease belongs to a
// process that crashed (a restarted process has a new pid, so even the same pod
// counts as another process) and the engine has no active work, the abandoned
// in-flight task is still failed so it stops blocking the database.
func TestConflictCheckFailsAbandonedTaskWithStaleForeignLease(t *testing.T) {
	running := &storage.Task{
		ID:             14,
		ApplyID:        141,
		TaskIdentifier: "task-crashed-owner",
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		TableName:      "events",
		State:          state.Task.Running,
	}
	client := newNoActiveChangeClient("testdb", []*storage.Task{running})
	stor := client.storage.(*exactProgressStorage)
	stor.applies = &mockApplyStore{apply: &storage.Apply{
		ID: 141, ApplyIdentifier: "apply-crashed", Database: "testdb",
		DatabaseType: storage.DatabaseTypeMySQL, State: state.Apply.Running,
		LeaseOwner: "crashed-host/4242/driver-0", LeaseToken: "token-crashed",
		UpdatedAt: time.Now().Add(-2 * storage.ApplyLeaseStaleAfter),
	}}

	plan := &storage.Plan{Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}
	_, err := client.checkActiveTaskConflict(t.Context(), plan, "", 0)
	require.NoError(t, err, "an abandoned task under a crashed process's stale lease must be failed and unblock")
	assert.Equal(t, state.Task.Failed, running.State)
	assert.Contains(t, running.ErrorMessage, "server may have crashed")
	assert.NotNil(t, running.CompletedAt)
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
	_, err := client.checkActiveTaskConflict(t.Context(), plan, "", 0)
	require.NoError(t, err, "new apply should proceed once the abandoned task is failed")
	assert.Equal(t, state.Task.Failed, running.State)
}

// The refusal an apply gets when another holds the database is the whole of what
// an operator sees when their apply dies on arrival, so it has to name what is
// being changed, the apply to act on, and what frees the database. A stopped
// apply holds the database by design, so a refusal naming only the task leaves
// an operator with an identifier they cannot act on and no indication that a
// decision is owed.
func TestBlockingTaskDescribesTheApplyAndItsResolution(t *testing.T) {
	for _, tc := range []struct {
		name     string
		blocking blockingTask
		want     []string
	}{
		{
			name: "a stopped apply holds its database until an operator decides its fate",
			blocking: blockingTask{
				taskIdentifier: "task-holding",
				table:          "xfers",
				apply:          &storage.Apply{ApplyIdentifier: "apply-holding", State: state.Apply.Stopped},
			},
			want: []string{"table xfers", "task-holding", "apply-holding", "stopped", "started or cancelled"},
		},
		{
			name: "a running apply releases its database on its own, unless it parks for cutover",
			blocking: blockingTask{
				taskIdentifier: "task-holding",
				table:          "xfers",
				apply:          &storage.Apply{ApplyIdentifier: "apply-holding", State: state.Apply.Running},
			},
			want: []string{
				"table xfers", "task-holding", "apply-holding", "running",
				"releases the database when it finishes", "unless it parks for cutover",
			},
		},
		{
			name: "a post-copy phase is still the running family",
			blocking: blockingTask{
				taskIdentifier: "task-holding",
				table:          "xfers",
				apply:          &storage.Apply{ApplyIdentifier: "apply-holding", State: state.Apply.CatchingUp},
			},
			want: []string{"apply-holding", "releases the database when it finishes"},
		},
		{
			name: "a sharded change names the shard, since only that shard is held",
			blocking: blockingTask{
				taskIdentifier: "task-holding",
				table:          "xfers",
				shard:          "-40",
				apply:          &storage.Apply{ApplyIdentifier: "apply-holding", State: state.Apply.Running},
			},
			want: []string{"table xfers shard -40", "task-holding", "apply-holding"},
		},
		{
			name: "a multi-table atomic change records no table, so the task names it",
			blocking: blockingTask{
				taskIdentifier: "task-holding",
				apply:          &storage.Apply{ApplyIdentifier: "apply-holding", State: state.Apply.Running},
			},
			want: []string{"task task-holding is held by apply apply-holding"},
		},
		{
			name: "a sharded multi-table change still names the shard it holds",
			blocking: blockingTask{
				taskIdentifier: "task-holding",
				shard:          "-40",
				apply:          &storage.Apply{ApplyIdentifier: "apply-holding", State: state.Apply.Running},
			},
			want: []string{"shard -40 (task task-holding)", "apply-holding"},
		},
		{
			name: "a retryable failure rests holding its database, so a decision is owed",
			blocking: blockingTask{
				taskIdentifier: "task-holding",
				table:          "xfers",
				apply:          &storage.Apply{ApplyIdentifier: "apply-holding", State: state.Apply.FailedRetryable},
			},
			want: []string{"table xfers", "task-holding", "apply-holding", "failed_retryable", "retried or cancelled"},
		},
		{
			name: "an apply parked at the cutover barrier holds its database until cut over",
			blocking: blockingTask{
				taskIdentifier: "task-holding",
				table:          "xfers",
				apply:          &storage.Apply{ApplyIdentifier: "apply-holding", State: state.Apply.WaitingForCutover},
			},
			want: []string{"table xfers", "task-holding", "apply-holding", "waiting_for_cutover", "cut over or cancelled"},
		},
		{
			name: "an apply in its revert window rests holding its database, so a decision is owed",
			blocking: blockingTask{
				taskIdentifier: "task-holding",
				table:          "xfers",
				apply:          &storage.Apply{ApplyIdentifier: "apply-holding", State: state.Apply.RevertWindow},
			},
			want: []string{
				"table xfers", "task-holding", "apply-holding", "revert_window",
				"reverted or skip-reverted",
			},
		},
		{
			name: "a revert already under way finishes on its own",
			blocking: blockingTask{
				taskIdentifier: "task-holding",
				table:          "xfers",
				apply:          &storage.Apply{ApplyIdentifier: "apply-holding", State: state.Apply.Reverting},
			},
			want: []string{"apply-holding", "reverting", "releases the database when the revert finishes"},
		},
		{
			name: "finalizing skip-revert finishes on its own too",
			blocking: blockingTask{
				taskIdentifier: "task-holding",
				table:          "xfers",
				apply:          &storage.Apply{ApplyIdentifier: "apply-holding", State: state.Apply.SkippingRevert},
			},
			want: []string{"apply-holding", "skipping_revert", "releases the database when the revert finishes"},
		},
		{
			name: "an apply that could not be loaded still reports what it blocks on",
			blocking: blockingTask{
				taskIdentifier: "task-holding",
				table:          "xfers",
			},
			want: []string{"table xfers", "task-holding", "could not be loaded"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			described := tc.blocking.describe()
			for _, want := range tc.want {
				assert.Contains(t, described, want)
			}
		})
	}
}

// The refusal is a composed sentence an operator reads out of a CLI error, so
// its shape is part of what is being delivered: the table leads, the task
// identifier stays attached as the CLI handle, the apply and its state follow,
// and the resolution hangs off a semicolon. Pinning both branches exactly
// catches an edit that reorders the parts, drops the parenthesised handle, or
// loses the separator — all of which every substring assertion still passes.
func TestBlockingTaskComposesTheWholeRefusal(t *testing.T) {
	held := blockingTask{
		taskIdentifier: "task-holding",
		table:          "xfers",
		shard:          "-40",
		apply:          &storage.Apply{ApplyIdentifier: "apply-holding", State: state.Apply.Stopped},
	}
	assert.Equal(t,
		"table xfers shard -40 (task task-holding) is held by apply apply-holding (stopped); "+
			"it holds the database until it is started or cancelled",
		held.describe())

	unloadable := blockingTask{
		taskIdentifier: "task-holding",
		table:          "xfers",
	}
	assert.Equal(t,
		"table xfers (task task-holding) is held by an apply that could not be loaded",
		unloadable.describe())
}

// A state whose next move is not certain gets no resolution line rather than a
// guess, so an operator is never pointed at an action the apply will not honour.
func TestBlockingTaskOffersNoResolutionForAnUncertainState(t *testing.T) {
	blocking := blockingTask{
		taskIdentifier: "task-holding",
		apply:          &storage.Apply{ApplyIdentifier: "apply-holding", State: state.Apply.CuttingOver},
	}

	assert.Empty(t, blocking.resolution(), "cutting over has no operator action to offer")
	assert.Contains(t, blocking.describe(), state.Apply.CuttingOver, "the state is still named")
}

// Every other resting state clears by starting, retrying, or cutting over — or
// by cancelling. The revert window is the exception: the change has already cut
// over, so stop and cancel are permanently rejected there, and offering cancel
// would point an operator at a command the control path refuses.
func TestBlockingTaskOffersNoCancelInsideTheRevertWindow(t *testing.T) {
	for _, applyState := range []string{
		state.Apply.RevertWindow, state.Apply.Reverting, state.Apply.SkippingRevert,
	} {
		t.Run(applyState, func(t *testing.T) {
			blocking := blockingTask{
				taskIdentifier: "task-holding",
				table:          "xfers",
				apply:          &storage.Apply{ApplyIdentifier: "apply-holding", State: applyState},
			}

			assert.NotEmpty(t, blocking.resolution(), "the revert phase names what clears the database")
			assert.NotContains(t, blocking.describe(), "cancel",
				"cancel is rejected once a change has cut over, so the refusal must not offer it")
		})
	}
}

// restingStoppedTask builds a stopped task and the stopped apply that owns it —
// a copy left resting on the target, with its checkpoint intact and no driver
// on its way.
func restingStoppedTask() (*storage.Task, *storage.Apply) {
	task := &storage.Task{
		ID:             1,
		TaskIdentifier: "task-stopped",
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		TableName:      "users",
		State:          state.Task.Stopped,
	}
	apply := &storage.Apply{
		ID:              1,
		ApplyIdentifier: "apply-holding-testdb",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		State:           state.Apply.Stopped,
	}
	return task, apply
}

// restingTaskClient builds a client whose only task is the given resting one,
// with the control requests the store should report against its apply.
func restingTaskClient(task *storage.Task, apply *storage.Apply, controlRequests storage.ControlRequestStore) *LocalClient {
	return &LocalClient{
		config: LocalConfig{Database: "testdb", Type: storage.DatabaseTypeMySQL},
		storage: &exactProgressStorage{
			applies:         &mockApplyStore{apply: apply},
			tasks:           &exactProgressTaskStore{tasks: []*storage.Task{task}},
			logs:            &mockApplyLogStore{},
			controlRequests: controlRequests,
		},
		spiritEngine: &fakeControlEngine{
			progressResult: &engine.ProgressResult{State: engine.StateRunning, Message: "Copying rows"},
		},
		logger: slog.Default(),
	}
}

// storageWithoutControlRequests is a storage that has no control request store
// at all, the way a partially-wired storage implementation reports one.
type storageWithoutControlRequests struct {
	*exactProgressStorage
}

func (s *storageWithoutControlRequests) ControlRequests() storage.ControlRequestStore { return nil }

// unreadableControlRequestStore fails every pending-request read, standing in
// for a storage outage while the conflict check is deciding.
type unreadableControlRequestStore struct {
	storage.ControlRequestStore
}

func (s *unreadableControlRequestStore) GetPending(context.Context, int64, storage.ControlOperation) (*storage.ApplyControlRequest, error) {
	return nil, errors.New("control request read failed")
}

// A stopped apply's task rests on the target with its checkpoint intact, and no
// driver claims it until a person asks. It therefore stops holding the database
// at the moment of the stop, so an unrelated apply proceeds instead of being
// refused for as long as the stopped copy sits there. The resting task itself is
// left exactly as it was, so starting it later still resumes from its
// checkpoint.
func TestConflictCheckReleasesRestingStoppedTask(t *testing.T) {
	stopped, holdingApply := restingStoppedTask()
	client := restingTaskClient(stopped, holdingApply, nil)
	plan := &storage.Plan{Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}

	blocking := client.findBlockingTask(t.Context(), []*storage.Task{stopped}, plan, "", 0)
	assert.False(t, blocking.blocks(), "a resting stopped task no longer holds the database")

	_, err := client.checkActiveTaskConflict(t.Context(), plan, "", 0)
	require.NoError(t, err, "a new apply proceeds past a resting stopped task")

	assert.Equal(t, state.Task.Stopped, stopped.State, "the resting task stays resumable")
	assert.Empty(t, stopped.ErrorMessage, "releasing the hold must not write a failure onto the task")
	assert.Nil(t, stopped.CompletedAt)
}

// An operator command that a driver has not delivered yet means a driver is
// still on its way to the stopped apply: a start resumes its copy and a cancel
// reclaims it. Either way the database stays held until the command lands, and
// the refusal names the apply an operator acts on rather than the task alone.
func TestConflictCheckKeepsRestingTaskAwaitingAnOperatorCommand(t *testing.T) {
	for _, operation := range []storage.ControlOperation{storage.ControlOperationStart, storage.ControlOperationCancel} {
		t.Run(string(operation), func(t *testing.T) {
			stopped, holdingApply := restingStoppedTask()
			client := restingTaskClient(stopped, holdingApply, &testControlRequestStore{
				requests: []*storage.ApplyControlRequest{{
					ID:        1,
					ApplyID:   holdingApply.ID,
					Operation: operation,
					Status:    storage.ControlRequestPending,
				}},
			})
			plan := &storage.Plan{Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}

			blocking := client.findBlockingTask(t.Context(), []*storage.Task{stopped}, plan, "", 0)

			require.True(t, blocking.blocks(), "a %s a driver has not delivered keeps the database held", operation)
			assert.Equal(t, "task-stopped", blocking.taskIdentifier)
			assert.Equal(t, "users", blocking.table, "the refusal names the table being changed")
			assert.Equal(t, "apply-holding-testdb", blocking.applyIdentifier(),
				"the refusal names the apply an operator acts on, not only the task")
			assert.Equal(t, state.Apply.Stopped, blocking.applyStateName())
			assert.Contains(t, blocking.describe(), "table users",
				"the table an operator recognizes leads the refusal")
		})
	}
}

// A stopped apply whose lease is still fresh is being settled by a live driver
// right now, so its task keeps holding the database until that driver is done
// with it.
func TestConflictCheckKeepsRestingTaskUnderAFreshLease(t *testing.T) {
	stopped, holdingApply := restingStoppedTask()
	holdingApply.LeaseOwner = "driver-settling-the-stop"
	holdingApply.UpdatedAt = time.Now()
	client := restingTaskClient(stopped, holdingApply, nil)
	plan := &storage.Plan{Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}

	blocking := client.findBlockingTask(t.Context(), []*storage.Task{stopped}, plan, "", 0)

	require.True(t, blocking.blocks(), "a live driver's stopped apply keeps holding the database")
	assert.Equal(t, "apply-holding-testdb", blocking.applyIdentifier())
}

// Releasing the hold requires proving no driver is coming. When the apply's
// control requests cannot be read that proof is unavailable, so the task keeps
// blocking rather than admitting a new apply on an unverified assumption.
func TestConflictCheckKeepsRestingTaskWhenControlRequestsAreUnreadable(t *testing.T) {
	stopped, holdingApply := restingStoppedTask()
	client := restingTaskClient(stopped, holdingApply, &unreadableControlRequestStore{})
	plan := &storage.Plan{Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}

	blocking := client.findBlockingTask(t.Context(), []*storage.Task{stopped}, plan, "", 0)

	require.True(t, blocking.blocks(), "an unreadable control request keeps the database held")
	assert.Equal(t, "apply-holding-testdb", blocking.applyIdentifier())
}

// A storage with no control request store cannot say whether a command is
// waiting for a driver, and an unanswerable question is not a "no". The task
// keeps blocking rather than being released on a check that never ran.
func TestConflictCheckKeepsRestingTaskWhenControlRequestsAreUnconfigured(t *testing.T) {
	stopped, holdingApply := restingStoppedTask()
	client := restingTaskClient(stopped, holdingApply, nil)
	client.storage = &storageWithoutControlRequests{exactProgressStorage: client.storage.(*exactProgressStorage)}
	plan := &storage.Plan{Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}

	require.NotPanics(t, func() {
		blocking := client.findBlockingTask(t.Context(), []*storage.Task{stopped}, plan, "", 0)
		require.True(t, blocking.blocks(), "an unconfigured control request store keeps the database held")
		assert.Equal(t, "apply-holding-testdb", blocking.applyIdentifier())
	})
}
