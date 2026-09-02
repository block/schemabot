package tern

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	spiritstatus "github.com/block/spirit/pkg/status"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func groupedSyncClient(taskStore storage.TaskStore) *LocalClient {
	return &LocalClient{
		config: LocalConfig{Database: "appdb", Type: storage.DatabaseTypeMySQL},
		storage: &exactProgressStorage{
			tasks:           taskStore,
			controlRequests: &testControlRequestStore{},
			logs:            &mockApplyLogStore{},
		},
		logger: slog.Default(),
	}
}

// A grouped drive reports one apply-wide state for work spanning many tables,
// but each table reaches its endgame on its own schedule. The stored task is
// the single render surface for the CLI and the PR comment, so each task takes
// the phase its own table reported — a table catching up on accumulated changes
// while a sibling is still copying rows must render as catching up, not as a
// serene fully-copied bar — and each takes its own row counts and throttle
// state rather than a sibling's.
func TestSyncAtomicTaskProgress_RefinesPhaseAndDisplayPerTask(t *testing.T) {
	catchingUp := &storage.Task{
		ID: 1, ApplyID: 1, TaskIdentifier: "task-1",
		Database: "appdb", DatabaseType: storage.DatabaseTypeMySQL,
		TableName: "mutes", State: state.Task.Running,
	}
	copying := &storage.Task{
		ID: 2, ApplyID: 1, TaskIdentifier: "task-2",
		Database: "appdb", DatabaseType: storage.DatabaseTypeMySQL,
		TableName: "payments", State: state.Task.Running,
	}
	taskStore := &stateRecordingTaskStore{
		exactProgressTaskStore: &exactProgressTaskStore{tasks: []*storage.Task{catchingUp, copying}},
	}
	result := &engine.ProgressResult{
		State: engine.StateRunning,
		Tables: []engine.TableProgress{
			{Table: "mutes", State: spiritstatus.ApplyChangeset.String(), RowsCopied: 900, RowsTotal: 900, Progress: 100},
			{Table: "payments", State: spiritstatus.CopyRows.String(), RowsCopied: 20, RowsTotal: 100, Progress: 20, Throttled: true, ThrottleReason: "replica lag"},
		},
	}
	client := groupedSyncClient(taskStore)

	client.syncAtomicTaskProgress(t.Context(), slog.Default(), []*storage.Task{catchingUp, copying}, result, state.Task.Running, time.Now(), settledTaskSet{})

	assert.Equal(t, state.Task.CatchingUp, catchingUp.State, "a table applying its changeset renders as catching up")
	assert.EqualValues(t, 100, catchingUp.ProgressPercent)
	assert.False(t, catchingUp.Throttled, "a task takes its own throttle state, never a sibling's")

	assert.Equal(t, state.Task.Running, copying.State, "a table still copying rows keeps the apply-wide state")
	assert.EqualValues(t, 20, copying.RowsCopied)
	assert.EqualValues(t, 100, copying.RowsTotal)
	assert.True(t, copying.Throttled)
	assert.Equal(t, "replica lag", copying.ThrottleReason)

	assert.Len(t, taskStore.states, 2, "every task ends the tick with a persisted write; the operator reads that write as the drive's liveness signal")
}

// An engine serving a stale snapshot — reattaching after a restart, say —
// can report a table back in row copy after storage already advanced it past
// that phase. The stored state holds where it is, because a poll never moves a
// task backward, and the operator still sees the counters the poll carried:
// refusing the state claim must not freeze the display alongside it.
func TestSyncAtomicTaskProgress_RefusedStateClaimStillRefreshesDisplay(t *testing.T) {
	checksumming := &storage.Task{
		ID: 1, ApplyID: 1, TaskIdentifier: "task-1",
		Database: "appdb", DatabaseType: storage.DatabaseTypeMySQL,
		TableName: "mutes", State: state.Task.Checksumming,
		RowsCopied: 900, RowsTotal: 900, ProgressPercent: 100,
	}
	taskStore := &stateRecordingTaskStore{
		exactProgressTaskStore: &exactProgressTaskStore{tasks: []*storage.Task{checksumming}},
	}
	result := &engine.ProgressResult{
		State: engine.StateRunning,
		Tables: []engine.TableProgress{
			{Table: "mutes", State: spiritstatus.CopyRows.String(), RowsCopied: 120, RowsTotal: 900, Progress: 13},
		},
	}
	client := groupedSyncClient(taskStore)

	client.syncAtomicTaskProgress(t.Context(), slog.Default(), []*storage.Task{checksumming}, result, state.Task.Running, time.Now(), settledTaskSet{})

	assert.Equal(t, state.Task.Checksumming, checksumming.State, "a poll claiming an earlier phase never rewinds the stored state")
	assert.EqualValues(t, 120, checksumming.RowsCopied, "the display takes the poll's counters even when its state claim is refused")
	assert.EqualValues(t, 13, checksumming.ProgressPercent)
}

// Row counts are estimates, so an engine can report a completed schema change
// whose final per-table sample still reads a fraction short of its own total.
// The operator sees a finished bar on a finished change: the completed poll
// ends the percentage at 100 whatever the last sample carried.
func TestSyncAtomicTaskProgress_CompletedPollFinishesTheBar(t *testing.T) {
	copied := &storage.Task{
		ID: 1, ApplyID: 1, TaskIdentifier: "task-1",
		Database: "appdb", DatabaseType: storage.DatabaseTypeMySQL,
		TableName: "mutes", State: state.Task.Running,
	}
	taskStore := &stateRecordingTaskStore{
		exactProgressTaskStore: &exactProgressTaskStore{tasks: []*storage.Task{copied}},
	}
	result := &engine.ProgressResult{
		State: engine.StateCompleted,
		Tables: []engine.TableProgress{
			{Table: "mutes", State: spiritstatus.CopyRows.String(), RowsCopied: 8_912, RowsTotal: 9_000, Progress: 99},
		},
	}
	client := groupedSyncClient(taskStore)

	client.syncAtomicTaskProgress(t.Context(), slog.Default(), []*storage.Task{copied}, result, state.Task.Completed, time.Now(), settledTaskSet{})

	assert.EqualValues(t, 100, copied.ProgressPercent, "a completed poll finishes the bar even when the last table sample fell short")
	assert.EqualValues(t, 8_912, copied.RowsCopied, "the row counters keep what the engine reported")
	assert.Equal(t, state.Task.Completed, copied.State)
	assert.NotNil(t, copied.CompletedAt)
}

// A table the engine did not report on still belongs to the apply, so its task
// takes the apply-wide state and keeps whatever progress the last report left
// behind. An absent snapshot degrades the display and nothing else.
func TestSyncAtomicTaskProgress_UnreportedTableKeepsApplyStateAndLastProgress(t *testing.T) {
	unreported := &storage.Task{
		ID: 1, ApplyID: 1, TaskIdentifier: "task-1",
		Database: "appdb", DatabaseType: storage.DatabaseTypeMySQL,
		TableName: "mutes", State: state.Task.Running,
		RowsCopied: 500, RowsTotal: 1000, ProgressPercent: 50,
	}
	taskStore := &stateRecordingTaskStore{
		exactProgressTaskStore: &exactProgressTaskStore{tasks: []*storage.Task{unreported}},
	}
	result := &engine.ProgressResult{
		State:  engine.StateRunning,
		Tables: []engine.TableProgress{{Table: "payments", State: spiritstatus.Checksum.String(), RowsCopied: 7}},
	}
	client := groupedSyncClient(taskStore)

	client.syncAtomicTaskProgress(t.Context(), slog.Default(), []*storage.Task{unreported}, result, state.Task.Running, time.Now(), settledTaskSet{})

	assert.Equal(t, state.Task.Running, unreported.State, "a sibling table's phase never refines a task the engine did not report on")
	assert.EqualValues(t, 500, unreported.RowsCopied, "an unreported table keeps its last known progress")
	assert.EqualValues(t, 50, unreported.ProgressPercent)
	assert.Len(t, taskStore.states, 1)
}

// A task the drive already settled from the live target schema is out of the
// engine's hands: the poll that sent the drive to the target reports no active
// schema change, so it carries neither progress to display nor a state the
// task may take. The projection must leave such a task alone outright rather
// than write it again and lean on the no-backward guard to reject the claim.
func TestSyncAtomicTaskProgress_SettledTaskTakesNothingFromThePoll(t *testing.T) {
	settledTask := &storage.Task{
		ID: 1, ApplyID: 1, TaskIdentifier: "task-1",
		Database: "appdb", DatabaseType: storage.DatabaseTypeMySQL,
		TableName: "mutes", State: state.Task.Completed,
		RowsCopied: 900, RowsTotal: 900, ProgressPercent: 100,
	}
	taskStore := &stateRecordingTaskStore{
		exactProgressTaskStore: &exactProgressTaskStore{tasks: []*storage.Task{settledTask}},
	}
	result := &engine.ProgressResult{
		State:  engine.StateRunning,
		Tables: []engine.TableProgress{{Table: "mutes", State: spiritstatus.CopyRows.String(), RowsCopied: 3, RowsTotal: 900, Progress: 1, Throttled: true, ThrottleReason: "replica lag"}},
	}
	client := groupedSyncClient(taskStore)
	settled := settledTaskSet{}
	settled.add(settledTask)

	client.syncAtomicTaskProgress(t.Context(), slog.Default(), []*storage.Task{settledTask}, result, state.Task.Running, time.Now(), settled)

	assert.Equal(t, state.Task.Completed, settledTask.State, "the target's verdict stands; the poll claims nothing")
	assert.EqualValues(t, 900, settledTask.RowsCopied, "a settled task keeps the progress its settlement left")
	assert.EqualValues(t, 100, settledTask.ProgressPercent)
	assert.False(t, settledTask.Throttled, "an engine that lost the work reports nothing a settled task should show")
	assert.Empty(t, taskStore.states, "settlement already persisted the task; the projection writes it no further")
}
