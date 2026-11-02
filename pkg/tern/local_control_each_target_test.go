package tern

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// eachTargetControlEngine is an engine that runs each target as its own schema
// change: it implements engine.TargetDrainer, and every Stop or Cancel call
// reaches one target only. It answers its Nth control call with the Nth entry of
// stopErrs / cancelErrs (nil once the list runs out), so a test can script what
// each target's engine work reports.
type eachTargetControlEngine struct {
	*controlCaptureEngine
	stopCalls   int
	cancelCalls int
	stopErrs    []error
	cancelErrs  []error
}

func (e *eachTargetControlEngine) Stop(_ context.Context, _ *engine.ControlRequest) (*engine.ControlResult, error) {
	call := e.stopCalls
	e.stopCalls++
	if call < len(e.stopErrs) && e.stopErrs[call] != nil {
		return nil, e.stopErrs[call]
	}
	return &engine.ControlResult{Accepted: true}, nil
}

func (e *eachTargetControlEngine) Cancel(_ context.Context, _ *engine.ControlRequest) (*engine.ControlResult, error) {
	call := e.cancelCalls
	e.cancelCalls++
	if call < len(e.cancelErrs) && e.cancelErrs[call] != nil {
		return nil, e.cancelErrs[call]
	}
	return &engine.ControlResult{Accepted: true}, nil
}

func (e *eachTargetControlEngine) DrainContext(context.Context) error { return nil }

func (e *eachTargetControlEngine) DrainTargetContext(context.Context, engine.DrainTarget) error {
	return nil
}

// newEachTargetControlTestClient builds a MySQL-family control test client
// whose engine runs each target separately.
func newEachTargetControlTestClient(apply *storage.Apply, tasks []*storage.Task, eng *eachTargetControlEngine) *LocalClient {
	client := newMySQLControlTestClient(apply, tasks, eng.controlCaptureEngine)
	client.spiritEngine = eng
	return client
}

// eachTargetTestApply returns a running apply with one task per shard of the
// same table, every task in the given state.
func eachTargetTestApply(identifier string, taskStates ...string) (*storage.Apply, []*storage.Task) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: identifier,
		State:           state.Apply.Running,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
	}
	shards := []string{"-40", "40-80", "80-c0", "c0-"}
	tasks := make([]*storage.Task, 0, len(taskStates))
	for i, taskState := range taskStates {
		tasks = append(tasks, &storage.Task{
			ID:             int64(i + 1),
			ApplyID:        apply.ID,
			TaskIdentifier: identifier + "-task-" + shards[i],
			Database:       "testdb",
			Namespace:      "testdb",
			Shard:          shards[i],
			State:          taskState,
		})
	}
	return apply, tasks
}

// An operator stops an apply whose table is copying on two shards at once while
// a third shard waits its turn. Every copying shard is stopped on the engine
// before the apply is recorded stopped, so no shard keeps copying (and later
// cuts over) under a stopped record.
func TestLocalClient_StopReachesEveryTargetWithLiveWork(t *testing.T) {
	apply, tasks := eachTargetTestApply("apply-stop-each-target",
		state.Task.Running, state.Task.Running, state.Task.Pending)
	eng := &eachTargetControlEngine{controlCaptureEngine: &controlCaptureEngine{}}
	client := newEachTargetControlTestClient(apply, tasks, eng)

	resp, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.NoError(t, err)
	assert.Equal(t, 2, eng.stopCalls, "each shard with live work gets its own engine stop")
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(3), resp.StoppedCount)
	for _, task := range tasks {
		assert.Equal(t, state.Task.Stopped, task.State, "task %s", task.TaskIdentifier)
	}
	assert.Equal(t, state.Apply.Stopped, apply.State)
}

// One shard cuts over in the moment between the operator's stop and the engine
// reaching it, while the other shard is still copying. The finished shard is
// recorded completed and the copying shard stopped: the stop records what each
// target actually reached instead of overwriting a finished schema change as
// stopped.
func TestLocalClient_StopSettlesTargetThatAlreadyCompleted(t *testing.T) {
	apply, tasks := eachTargetTestApply("apply-stop-target-completed",
		state.Task.CuttingOver, state.Task.Running)
	eng := &eachTargetControlEngine{
		controlCaptureEngine: &controlCaptureEngine{},
		stopErrs:             []error{engine.NewAlreadyCompletedError("schema change already completed")},
	}
	client := newEachTargetControlTestClient(apply, tasks, eng)

	resp, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.NoError(t, err)
	assert.Equal(t, 2, eng.stopCalls, "the stop continues past the completed shard to the copying one")
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(1), resp.StoppedCount)
	assert.Equal(t, int64(1), resp.SkippedCount)
	assert.Equal(t, state.Task.Completed, tasks[0].State)
	assert.Equal(t, 100, int(tasks[0].ProgressPercent))
	assert.NotNil(t, tasks[0].CompletedAt)
	assert.Equal(t, state.Task.Stopped, tasks[1].State)
	assert.Equal(t, state.Apply.Stopped, apply.State)
}

// Every copying shard finishes before the engine sees the stop. Each is settled
// completed and the apply is finalized from its tasks, so it reads completed
// rather than stopped.
func TestLocalClient_StopCompletesApplyWhenEveryTargetAlreadyCompleted(t *testing.T) {
	apply, tasks := eachTargetTestApply("apply-stop-all-targets-completed",
		state.Task.CuttingOver, state.Task.CuttingOver)
	alreadyCompleted := engine.NewAlreadyCompletedError("schema change already completed")
	eng := &eachTargetControlEngine{
		controlCaptureEngine: &controlCaptureEngine{},
		stopErrs:             []error{alreadyCompleted, alreadyCompleted},
	}
	client := newEachTargetControlTestClient(apply, tasks, eng)

	resp, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.NoError(t, err)
	assert.Equal(t, 2, eng.stopCalls)
	assert.Equal(t, int64(0), resp.StoppedCount)
	assert.Equal(t, int64(2), resp.SkippedCount)
	for _, task := range tasks {
		assert.Equal(t, state.Task.Completed, task.State, "task %s", task.TaskIdentifier)
	}
	assert.Equal(t, state.Apply.Completed, apply.State)
}

// A shard's engine work refuses the stop while the engine still reports live
// work. The stop fails closed: nothing is recorded stopped, so the request stays
// pending for a later drive to retry.
func TestLocalClient_StopFailsClosedWhenOneTargetRefuses(t *testing.T) {
	apply, tasks := eachTargetTestApply("apply-stop-target-refused",
		state.Task.Running, state.Task.Running)
	eng := &eachTargetControlEngine{
		controlCaptureEngine: &controlCaptureEngine{
			progressResult: &engine.ProgressResult{State: engine.StateRunning},
		},
		stopErrs: []error{nil, engine.NewPermanentError("stop refused")},
	}
	client := newEachTargetControlTestClient(apply, tasks, eng)

	_, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "stop refused")
	assert.Equal(t, 2, eng.stopCalls)
	for _, task := range tasks {
		assert.Equal(t, state.Task.Running, task.State, "task %s", task.TaskIdentifier)
	}
	assert.Equal(t, state.Apply.Running, apply.State)
}

// An operator cancels an apply whose table is copying on two shards. Each
// copying shard is cancelled on the engine before the apply is recorded
// cancelled.
func TestLocalClient_CancelReachesEveryTargetWithLiveWork(t *testing.T) {
	apply, tasks := eachTargetTestApply("apply-cancel-each-target",
		state.Task.Running, state.Task.Running, state.Task.Pending)
	eng := &eachTargetControlEngine{controlCaptureEngine: &controlCaptureEngine{}}
	client := newEachTargetControlTestClient(apply, tasks, eng)

	resp, err := client.cancelOwnedApply(t.Context(), &ternv1.CancelRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.NoError(t, err)
	assert.Equal(t, 2, eng.cancelCalls, "each shard with live work gets its own engine cancel")
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(3), resp.CancelledCount)
	for _, task := range tasks {
		assert.Equal(t, state.Task.Cancelled, task.State, "task %s", task.TaskIdentifier)
	}
	assert.Equal(t, state.Apply.Cancelled, apply.State)
}

// One shard cuts over before the cancel reaches it while the other is still
// copying. The finished shard is recorded completed and the copying shard
// cancelled.
func TestLocalClient_CancelSettlesTargetThatAlreadyCompleted(t *testing.T) {
	apply, tasks := eachTargetTestApply("apply-cancel-target-completed",
		state.Task.CuttingOver, state.Task.Running)
	eng := &eachTargetControlEngine{
		controlCaptureEngine: &controlCaptureEngine{},
		cancelErrs:           []error{engine.NewAlreadyCompletedError("schema change already completed")},
	}
	client := newEachTargetControlTestClient(apply, tasks, eng)

	resp, err := client.cancelOwnedApply(t.Context(), &ternv1.CancelRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.NoError(t, err)
	assert.Equal(t, 2, eng.cancelCalls, "the cancel continues past the completed shard to the copying one")
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(1), resp.CancelledCount)
	assert.Equal(t, state.Task.Completed, tasks[0].State)
	assert.Equal(t, state.Task.Cancelled, tasks[1].State)
}

// An engine that drives the apply as a single operation still gets exactly one
// stop, which takes the whole operation down.
func TestLocalClient_StopCallsSingleOperationEngineOnce(t *testing.T) {
	apply, tasks := eachTargetTestApply("apply-stop-single-operation",
		state.Task.Running, state.Task.Running)
	eng := &controlCaptureEngine{}
	stopCalls := 0
	eng.onStop = func() { stopCalls++ }
	client := newMySQLControlTestClient(apply, tasks, eng)

	resp, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.NoError(t, err)
	assert.Equal(t, 1, stopCalls)
	assert.Equal(t, int64(2), resp.StoppedCount)
	assert.Equal(t, state.Apply.Stopped, apply.State)
}
