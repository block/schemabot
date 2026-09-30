package tern

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// An operator stopping a schema change and a pod winding down both end the
// sequential drive early, but they mean opposite things: the operator wants the
// apply to rest stopped until they start it again, while a winding-down pod is
// handing an apply that is still live back for another driver to claim and
// resume. checkTaskReady is where the two arrive, so it must report them as
// distinct outcomes.
func TestCheckTaskReady_DistinguishesOperatorStopFromDriveCancellation(t *testing.T) {
	task := &storage.Task{
		ID: 1, ApplyID: 1, TaskIdentifier: "task-1",
		Database: "orders", TableName: "line_items", State: state.Task.Pending,
	}
	client := &LocalClient{
		storage: &exactProgressStorage{tasks: &exactProgressTaskStore{tasks: []*storage.Task{task}}},
		logger:  slog.Default(),
	}

	t.Run("operator stop", func(t *testing.T) {
		task.State = state.Task.Stopped
		assert.Equal(t, taskStopped, client.checkTaskReady(t.Context(), slog.Default(), task),
			"a task the operator stopped rests stopped")
	})

	t.Run("drive cancelled", func(t *testing.T) {
		task.State = state.Task.Pending
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		assert.Equal(t, taskHandover, client.checkTaskReady(ctx, slog.Default(), task),
			"a cancelled drive hands the apply over rather than stopping it")
	})
}

// A drive whose context is cancelled mid-copy is not an operator stop: the
// engine's work is still live and another driver must be able to reclaim and
// resume it. The poll must report the handover so the caller leaves the apply
// alone instead of finalizing it.
func TestPollTaskToCompletion_CancelledDriveHandsOver(t *testing.T) {
	task := &storage.Task{
		ID: 1, ApplyID: 1, TaskIdentifier: "task-1",
		Database: "orders", TableName: "line_items", State: state.Task.Running,
	}
	apply := &storage.Apply{
		ID: 1, ApplyIdentifier: "apply-1", Database: "orders",
		Environment: "staging", State: state.Apply.Running,
	}
	client := &LocalClient{
		customEngine: &fakeControlEngine{},
		storage: &exactProgressStorage{
			applies:         &exactProgressApplyStore{apply: apply},
			tasks:           &exactProgressTaskStore{tasks: []*storage.Task{task}},
			controlRequests: &testControlRequestStore{},
			logs:            &mockApplyLogStore{},
		},
		logger: slog.Default(),
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	assert.Equal(t, taskHandover, client.pollTaskToCompletion(ctx, apply, task, nil, nil))
	assert.Equal(t, state.Task.Running, task.State, "the task stays running for the next driver to resume")
}

// cancellingEngine cancels the drive from inside Apply, modelling a pod winding
// down while a schema change is in flight: the drive's context goes away, the
// engine's work does not.
type cancellingEngine struct {
	engine.Engine
	cancel context.CancelFunc
}

func (e *cancellingEngine) Name() string { return "cancelling" }

func (e *cancellingEngine) Apply(context.Context, *engine.ApplyRequest) (*engine.ApplyResult, error) {
	e.cancel()
	return &engine.ApplyResult{Accepted: true}, nil
}

func (e *cancellingEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	return &engine.ProgressResult{State: engine.StateRunning}, nil
}

// When a pod winds down mid-apply, the drive's context is cancelled while the
// schema change is still live on the target. The apply must be left active so a
// peer driver reclaims and resumes it. Recording the cancellation as an operator
// stop would instead park the apply until a human ran start, turning every
// restart into manual operator work.
func TestExecuteApplySequential_CancelledDriveLeavesApplyActive(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	task := &storage.Task{
		ID: 1, ApplyID: 1, TaskIdentifier: "task-1",
		Database: "orders", TableName: "line_items", State: state.Task.Pending,
		DDL: "ALTER TABLE line_items ADD COLUMN sku VARCHAR(64)",
	}
	apply := &storage.Apply{
		ID: 1, ApplyIdentifier: "apply-1", Database: "orders",
		Environment: "staging", State: state.Apply.Pending,
	}
	applies := &snapshotApplyStore{stored: *apply}
	client := &LocalClient{
		config:            LocalConfig{Database: "orders"},
		customEngine:      &cancellingEngine{cancel: cancel},
		heartbeatInterval: time.Hour,
		storage: &exactProgressStorage{
			applies:         applies,
			tasks:           &exactProgressTaskStore{tasks: []*storage.Task{task}},
			controlRequests: &testControlRequestStore{},
			logs:            &mockApplyLogStore{},
		},
		logger: slog.Default(),
	}

	require.NoError(t, client.executeApplySequential(ctx, apply, []*storage.Task{task}, &storage.Plan{}, nil))

	stored, err := applies.Get(t.Context(), apply.ID)
	require.NoError(t, err)
	assert.False(t, state.IsState(stored.State, state.Apply.Stopped),
		"a cancelled drive must not park the apply stopped; stored state was %q", stored.State)
	assert.True(t, state.IsState(stored.State, state.Apply.Running),
		"the apply stays running so a peer driver reclaims it; stored state was %q", stored.State)
	assert.Nil(t, stored.CompletedAt, "the apply is not finished, so it is not stamped completed")
}

// countingEngine records whether a drive reached the engine.
type countingEngine struct {
	engine.Engine
	applyCalls int
}

func (e *countingEngine) Name() string { return "counting" }

func (e *countingEngine) Apply(context.Context, *engine.ApplyRequest) (*engine.ApplyResult, error) {
	e.applyCalls++
	return &engine.ApplyResult{Accepted: true}, nil
}

func (e *countingEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	return &engine.ProgressResult{State: engine.StateCompleted}, nil
}

// terminalWriteRefusingTaskStore keeps task rows by value, the way a real store
// does, so a read returns the last persisted write rather than the drive's
// in-memory task. It refuses a write that would settle a task into a terminal
// state with err, for the first refusals such writes (every one when refusals
// is negative), standing in for storage that turns down a drive's final write.
type terminalWriteRefusingTaskStore struct {
	storage.TaskStore
	stored   map[string]storage.Task
	err      error
	refusals int
}

func newTerminalWriteRefusingTaskStore(err error, refusals int, tasks ...*storage.Task) *terminalWriteRefusingTaskStore {
	s := &terminalWriteRefusingTaskStore{stored: map[string]storage.Task{}, err: err, refusals: refusals}
	for _, task := range tasks {
		s.stored[task.TaskIdentifier] = *task
	}
	return s
}

func (s *terminalWriteRefusingTaskStore) Get(_ context.Context, taskIdentifier string) (*storage.Task, error) {
	stored, ok := s.stored[taskIdentifier]
	if !ok {
		return nil, nil
	}
	return &stored, nil
}

func (s *terminalWriteRefusingTaskStore) GetByApplyID(context.Context, int64) ([]*storage.Task, error) {
	tasks := make([]*storage.Task, 0, len(s.stored))
	for _, stored := range s.stored {
		tasks = append(tasks, &stored)
	}
	return tasks, nil
}

func (s *terminalWriteRefusingTaskStore) Update(_ context.Context, task *storage.Task) error {
	if state.IsTerminalTaskState(task.State) && s.refusals != 0 {
		s.refusals--
		return s.err
	}
	s.stored[task.TaskIdentifier] = *task
	return nil
}

// A sequential drive finishes one table's schema change before it starts the
// next, and moves on only once the finished table's outcome is durable. When
// storage refuses that write — because another driver took the lease, or
// because storage keeps failing — the drive must start no further table: the
// next table's DDL would run while storage still records the first as in
// flight, and the apply could finalize completed over a task row still
// running. The apply stays active for a later drive to settle. A single
// refused write is retried at the next poll, and once it lands the drive
// carries on to the next table as normal.
func TestExecuteApplySequential_StartsNoFurtherTaskUntilTheTerminalWriteLands(t *testing.T) {
	for name, tc := range map[string]struct {
		err       error
		refusals  int
		completes bool
	}{
		"lease lost":            {err: fmt.Errorf("task task-1 update: %w", storage.ErrApplyLeaseLost), refusals: -1},
		"storage keeps failing": {err: errors.New("storage down"), refusals: -1},
		"storage blip":          {err: errors.New("storage down"), refusals: 1, completes: true},
	} {
		t.Run(name, func(t *testing.T) {
			first := &storage.Task{
				ID: 1, ApplyID: 1, TaskIdentifier: "task-1",
				Database: "orders", TableName: "line_items", State: state.Task.Pending,
				DDL: "ALTER TABLE line_items ADD COLUMN sku VARCHAR(64)",
			}
			second := &storage.Task{
				ID: 2, ApplyID: 1, TaskIdentifier: "task-2",
				Database: "orders", TableName: "shipments", State: state.Task.Pending,
				DDL: "ALTER TABLE shipments ADD COLUMN carrier VARCHAR(64)",
			}
			apply := &storage.Apply{
				ID: 1, ApplyIdentifier: "apply-1", Database: "orders",
				Environment: "staging", State: state.Apply.Pending,
			}
			applies := &snapshotApplyStore{stored: *apply}
			tasks := newTerminalWriteRefusingTaskStore(tc.err, tc.refusals, first, second)
			eng := &countingEngine{}
			client := &LocalClient{
				config:                   LocalConfig{Database: "orders"},
				customEngine:             eng,
				heartbeatInterval:        time.Hour,
				taskPollIntervalOverride: time.Millisecond,
				storage: &exactProgressStorage{
					applies:         applies,
					tasks:           tasks,
					controlRequests: &testControlRequestStore{},
					logs:            &mockApplyLogStore{},
				},
				logger: slog.Default(),
			}

			client.executeApplySequential(t.Context(), apply, []*storage.Task{first, second}, &storage.Plan{}, nil)

			storedApply, err := applies.Get(t.Context(), apply.ID)
			require.NoError(t, err)
			storedFirst, err := tasks.Get(t.Context(), first.TaskIdentifier)
			require.NoError(t, err)
			storedSecond, err := tasks.Get(t.Context(), second.TaskIdentifier)
			require.NoError(t, err)

			if tc.completes {
				assert.Equal(t, 2, eng.applyCalls, "once the retried write lands the drive starts the next table")
				assert.Equal(t, state.Task.Completed, storedFirst.State)
				assert.Equal(t, state.Task.Completed, storedSecond.State)
				assert.Equal(t, state.Apply.Completed, storedApply.State)
				return
			}
			assert.Equal(t, 1, eng.applyCalls, "no further table's DDL starts while the finished table's outcome is not durable")
			assert.Equal(t, state.Task.Running, storedFirst.State, "storage still records the finished table as in flight")
			assert.Equal(t, state.Task.Pending, storedSecond.State, "the next table is left for a later drive")
			assert.Equal(t, state.Task.Running, first.State, "the drive's own task does not claim a terminal state storage refused")
			assert.Nil(t, first.CompletedAt)
			assert.Equal(t, state.Apply.Running, storedApply.State, "the apply is not finalized over an unsettled task")
			assert.Nil(t, storedApply.CompletedAt)
		})
	}
}

// A drive records running before it hands any work to the engine. When that
// write fails because the apply is no longer this driver's, whether another
// writer already finished it or another driver took the lease, the drive stands
// down without starting a table: work it started could never be recorded
// against the apply. The same holds when another apply went active on the
// target, since the work would run alongside that apply's schema change.
func TestExecuteApply_StandsDownWhenStartWriteEndsTheDrive(t *testing.T) {
	refusals := map[string]error{
		"reopen refused": fmt.Errorf("apply apply-1 is completed; update to running would reopen it: %w", storage.ErrApplyReopenRefused),
		"lease lost":     fmt.Errorf("apply apply-1 lease taken by another driver: %w", storage.ErrApplyLeaseLost),
		"target busy":    fmt.Errorf("apply apply-2 is active on orders/staging: %w", storage.ErrActiveApplyExists),
	}
	drives := map[string]func(c *LocalClient, ctx context.Context, apply *storage.Apply, tasks []*storage.Task){
		"sequential": func(c *LocalClient, ctx context.Context, apply *storage.Apply, tasks []*storage.Task) {
			require.NoError(t, c.executeApplySequential(ctx, apply, tasks, &storage.Plan{}, nil))
		},
		"grouped": func(c *LocalClient, ctx context.Context, apply *storage.Apply, tasks []*storage.Task) {
			plan := &storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{"orders": {}}}
			require.NoError(t, c.executeGroupedApply(ctx, apply, tasks, plan, nil, false))
		},
	}
	for driveName, drive := range drives {
		for refusalName, refusal := range refusals {
			t.Run(driveName+"/"+refusalName, func(t *testing.T) {
				task := &storage.Task{
					ID: 1, ApplyID: 1, TaskIdentifier: "task-1",
					Database: "orders", Namespace: "orders", TableName: "line_items", State: state.Task.Pending,
					DDL: "ALTER TABLE line_items ADD COLUMN sku VARCHAR(64)",
				}
				apply := &storage.Apply{
					ID: 1, ApplyIdentifier: "apply-1", Database: "orders",
					Environment: "staging", State: state.Apply.Pending,
				}
				eng := &countingEngine{}
				client := &LocalClient{
					config:            LocalConfig{Database: "orders"},
					customEngine:      eng,
					heartbeatInterval: time.Hour,
					storage: &exactProgressStorage{
						applies:         &snapshotApplyStore{stored: *apply, err: refusal},
						tasks:           &exactProgressTaskStore{tasks: []*storage.Task{task}},
						controlRequests: &testControlRequestStore{},
						logs:            &mockApplyLogStore{},
					},
					logger: slog.Default(),
				}

				drive(client, t.Context(), apply, []*storage.Task{task})

				assert.Zero(t, eng.applyCalls, "no engine work starts once the apply is no longer this driver's")
				assert.Equal(t, state.Task.Pending, task.State, "the task is left for whoever owns the apply")
			})
		}
	}
}
