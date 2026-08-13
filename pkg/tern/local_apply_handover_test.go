package tern

import (
	"context"
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

	client.executeApplySequential(ctx, apply, []*storage.Task{task}, &storage.Plan{}, nil)

	stored, err := applies.Get(t.Context(), apply.ID)
	require.NoError(t, err)
	assert.False(t, state.IsState(stored.State, state.Apply.Stopped),
		"a cancelled drive must not park the apply stopped; stored state was %q", stored.State)
	assert.True(t, state.IsState(stored.State, state.Apply.Running),
		"the apply stays running so a peer driver reclaims it; stored state was %q", stored.State)
	assert.Nil(t, stored.CompletedAt, "the apply is not finished, so it is not stamped completed")
}
