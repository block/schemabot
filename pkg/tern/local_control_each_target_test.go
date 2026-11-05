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
// reaches only the target its request's resume state names, which it records.
// It answers its Nth control call with the Nth entry of stopErrs / cancelErrs
// (nil once the list runs out), so a test can script what each target's engine
// work reports. A progress request whose resume state names a target in
// progressByTarget gets that target's own progress.
type eachTargetControlEngine struct {
	*controlCaptureEngine
	stopCalls        int
	cancelCalls      int
	stopErrs         []error
	cancelErrs       []error
	stopTargets      []string
	cancelTargets    []string
	progressByTarget map[string]*engine.ProgressResult
}

// requestTarget returns the target a control request's resume state names.
func requestTarget(req *engine.ControlRequest) string {
	if req == nil || req.ResumeState == nil {
		return ""
	}
	return req.ResumeState.Metadata
}

func (e *eachTargetControlEngine) Stop(_ context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	call := e.stopCalls
	e.stopCalls++
	e.stopTargets = append(e.stopTargets, requestTarget(req))
	if call < len(e.stopErrs) && e.stopErrs[call] != nil {
		return nil, e.stopErrs[call]
	}
	return &engine.ControlResult{Accepted: true}, nil
}

func (e *eachTargetControlEngine) Cancel(_ context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	call := e.cancelCalls
	e.cancelCalls++
	e.cancelTargets = append(e.cancelTargets, requestTarget(req))
	if call < len(e.cancelErrs) && e.cancelErrs[call] != nil {
		return nil, e.cancelErrs[call]
	}
	return &engine.ControlResult{Accepted: true}, nil
}

func (e *eachTargetControlEngine) Progress(ctx context.Context, req *engine.ProgressRequest) (*engine.ProgressResult, error) {
	if req != nil && req.ResumeState != nil {
		if progress, ok := e.progressByTarget[req.ResumeState.Metadata]; ok {
			return progress, nil
		}
	}
	return e.controlCaptureEngine.Progress(ctx, req)
}

func (e *eachTargetControlEngine) DrainContext(context.Context) error { return nil }

func (e *eachTargetControlEngine) DrainTargetContext(context.Context, engine.DrainTarget) error {
	return nil
}

// perOperationResumeStateStore serves each apply operation its own persisted
// engine resume state, the routing a per-target engine reads to find the work
// a control request is for.
type perOperationResumeStateStore struct {
	storage.ApplyOperationStore
	byOperation map[int64]*storage.EngineResumeState
}

func (s *perOperationResumeStateStore) GetEngineResumeState(_ context.Context, operationID int64) (*storage.EngineResumeState, error) {
	if stored, ok := s.byOperation[operationID]; ok {
		return stored, nil
	}
	return nil, storage.ErrEngineResumeStateNotFound
}

func (s *perOperationResumeStateStore) ListByApply(context.Context, int64) ([]*storage.ApplyOperation, error) {
	return nil, nil
}

// newEachTargetControlTestClient builds a MySQL-family control test client
// whose engine runs each target separately.
func newEachTargetControlTestClient(apply *storage.Apply, tasks []*storage.Task, eng *eachTargetControlEngine) *LocalClient {
	client := newPerOperationControlTestClient(apply, tasks, eng.controlCaptureEngine)
	client.spiritEngine = eng
	return client
}

// newPerOperationControlTestClient builds a MySQL-family control test client
// where each task's apply operation persists resume state naming the task's
// shard.
func newPerOperationControlTestClient(apply *storage.Apply, tasks []*storage.Task, eng *controlCaptureEngine) *LocalClient {
	client := newMySQLControlTestClient(apply, tasks, eng)
	resumeStates := &perOperationResumeStateStore{byOperation: map[int64]*storage.EngineResumeState{}}
	for _, task := range tasks {
		resumeStates.byOperation[*task.ApplyOperationID] = &storage.EngineResumeState{Metadata: task.Shard}
	}
	client.storage.(*controlTestStorage).applyOperations = resumeStates
	return client
}

// eachTargetTestApply returns a running apply with one task per shard of the
// same table, each in its own apply operation and in the given state.
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
		operationID := int64(100 + i)
		tasks = append(tasks, &storage.Task{
			ID:               int64(i + 1),
			ApplyID:          apply.ID,
			ApplyOperationID: &operationID,
			TaskIdentifier:   identifier + "-task-" + shards[i],
			Database:         "testdb",
			Namespace:        "testdb",
			Shard:            shards[i],
			State:            taskState,
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
	assert.Equal(t, []string{"-40", "40-80"}, eng.stopTargets, "each shard with live work gets its own engine stop")
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
	assert.Equal(t, []string{"-40", "40-80"}, eng.stopTargets, "the stop continues past the completed shard to the copying one")
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

// Two shards are copying when the operator stops the apply. The first shard
// stops on the engine; the second refuses while the engine still reports live
// work there. The stop fails closed for the second shard, which keeps running
// and leaves the request pending for a retry, while the first shard, whose
// work is already down, is recorded stopped rather than left reading running.
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
	assert.Equal(t, state.Task.Stopped, tasks[0].State, "the shard the engine stopped")
	assert.Equal(t, state.Task.Running, tasks[1].State, "the shard that refused keeps running")
	assert.Equal(t, state.Apply.Running, apply.State)
}

// The cancel counterpart: the first shard cancels on the engine and the second
// refuses while still copying. The second shard keeps running for a retry and
// the first is recorded cancelled.
func TestLocalClient_CancelFailsClosedWhenOneTargetRefuses(t *testing.T) {
	apply, tasks := eachTargetTestApply("apply-cancel-target-refused",
		state.Task.Running, state.Task.Running)
	eng := &eachTargetControlEngine{
		controlCaptureEngine: &controlCaptureEngine{
			progressResult: &engine.ProgressResult{State: engine.StateRunning},
		},
		cancelErrs: []error{nil, engine.NewPermanentError("cancel refused")},
	}
	client := newEachTargetControlTestClient(apply, tasks, eng)

	_, err := client.cancelOwnedApply(t.Context(), &ternv1.CancelRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "cancel refused")
	assert.Equal(t, []string{"-40", "40-80"}, eng.cancelTargets)
	assert.Equal(t, state.Task.Cancelled, tasks[0].State, "the shard the engine cancelled")
	assert.Equal(t, state.Task.Running, tasks[1].State, "the shard that refused keeps running")
	assert.Equal(t, state.Apply.Running, apply.State)
}

// A durable stop or cancel reaches two copying shards. The first takes effect
// on the engine; the second has already failed on its own, and the engine
// refuses with a settled-outcome refusal. The request resolves on that refusal
// and the drive keeps going to record the second shard's failure, so the first
// shard, which no retry will revisit, is recorded stopped or cancelled before
// the request resolves.
func TestLocalClient_PendingControlRecordsTargetsReachedBeforeASettledRefusal(t *testing.T) {
	refusal := engine.NewSettledOutcomeError("the schema change on shard 40-80 already failed")
	tests := []struct {
		name        string
		operation   storage.ControlOperation
		configure   func(eng *eachTargetControlEngine)
		process     func(t *testing.T, client *LocalClient, apply *storage.Apply) (bool, error)
		reachedTask string
	}{
		{
			name:      "stop",
			operation: storage.ControlOperationStop,
			configure: func(eng *eachTargetControlEngine) { eng.stopErrs = []error{nil, refusal} },
			process: func(t *testing.T, client *LocalClient, apply *storage.Apply) (bool, error) {
				return client.processPendingStopControlRequest(t.Context(), apply)
			},
			reachedTask: state.Task.Stopped,
		},
		{
			name:      "cancel",
			operation: storage.ControlOperationCancel,
			configure: func(eng *eachTargetControlEngine) { eng.cancelErrs = []error{nil, refusal} },
			process: func(t *testing.T, client *LocalClient, apply *storage.Apply) (bool, error) {
				return client.processPendingCancelControlRequest(t.Context(), apply)
			},
			reachedTask: state.Task.Cancelled,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			apply, tasks := eachTargetTestApply("apply-pending-"+tc.name+"-partial-refusal",
				state.Task.Running, state.Task.Running)
			eng := &eachTargetControlEngine{controlCaptureEngine: &controlCaptureEngine{}}
			tc.configure(eng)
			client := newEachTargetControlTestClient(apply, tasks, eng)
			controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
				ApplyID:     apply.ID,
				Operation:   tc.operation,
				Status:      storage.ControlRequestPending,
				RequestedBy: "cli:alice",
			}}}
			client.storage.(*controlTestStorage).controlRequests = controlRequests

			standDown, err := tc.process(t, client, apply)

			require.NoError(t, err)
			assert.False(t, standDown, "the drive keeps going to record the refusing shard's own outcome")
			assert.Equal(t, tc.reachedTask, tasks[0].State, "the shard the engine reached")
			assert.Equal(t, state.Task.Running, tasks[1].State, "the refusing shard is left for the drive's poll")
			assert.Equal(t, state.Apply.Running, apply.State)
			resolved, err := controlRequests.GetByOperation(t.Context(), apply.ID, tc.operation)
			require.NoError(t, err)
			require.NotNil(t, resolved)
			assert.Equal(t, storage.ControlRequestFailed, resolved.Status)
			assert.Equal(t, refusal.Error(), resolved.ErrorMessage)
		})
	}
}

// Two shards of the same table are copying at different points when the
// operator stops the apply. Each stopped shard keeps the rows it copied, read
// from its own engine work, rather than one shard's count stamped on both.
func TestLocalClient_StopRecordsEachTargetsOwnProgress(t *testing.T) {
	apply, tasks := eachTargetTestApply("apply-stop-each-target-progress",
		state.Task.Running, state.Task.Running)
	for _, task := range tasks {
		task.TableName = "orders"
	}
	shardProgress := func(rowsCopied int64) *engine.ProgressResult {
		return &engine.ProgressResult{State: engine.StateStopped, Tables: []engine.TableProgress{{
			Namespace: "testdb", Table: "orders", RowsCopied: rowsCopied, RowsTotal: 1000, Progress: int(rowsCopied / 10),
		}}}
	}
	eng := &eachTargetControlEngine{
		controlCaptureEngine: &controlCaptureEngine{},
		progressByTarget: map[string]*engine.ProgressResult{
			"-40":   shardProgress(100),
			"40-80": shardProgress(700),
		},
	}
	client := newEachTargetControlTestClient(apply, tasks, eng)

	resp, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.NoError(t, err)
	assert.Equal(t, int64(2), resp.StoppedCount)
	assert.Equal(t, int64(100), tasks[0].RowsCopied, "shard -40")
	assert.Equal(t, 10, int(tasks[0].ProgressPercent), "shard -40")
	assert.Equal(t, int64(700), tasks[1].RowsCopied, "shard 40-80")
	assert.Equal(t, 70, int(tasks[1].ProgressPercent), "shard 40-80")
	for _, task := range tasks {
		assert.Equal(t, state.Task.Stopped, task.State, "task %s", task.TaskIdentifier)
		assert.Equal(t, int64(1000), task.RowsTotal, "task %s", task.TaskIdentifier)
	}
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
	assert.Equal(t, []string{"-40", "40-80"}, eng.cancelTargets, "each shard with live work gets its own engine cancel")
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
	assert.Equal(t, []string{"-40", "40-80"}, eng.cancelTargets, "the cancel continues past the completed shard to the copying one")
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
	client := newPerOperationControlTestClient(apply, tasks, eng)

	resp, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.NoError(t, err)
	assert.Equal(t, 1, stopCalls)
	assert.Equal(t, int64(2), resp.StoppedCount)
	assert.Equal(t, state.Apply.Stopped, apply.State)
}

// The first shard stops on the engine and the second has already failed on its
// own, but the first shard's stopped row cannot be written because its
// operation lease moved to a peer. The request must not resolve on the second
// shard's refusal over a first shard that still reads running: it stays pending
// so a retry repeats the sweep.
func TestLocalClient_PendingStopStaysPendingWhenAReachedTargetCannotBeRecorded(t *testing.T) {
	apply, tasks := eachTargetTestApply("apply-pending-stop-unrecorded-target",
		state.Task.Running, state.Task.Running)
	eng := &eachTargetControlEngine{
		controlCaptureEngine: &controlCaptureEngine{},
		stopErrs:             []error{nil, engine.NewSettledOutcomeError("the schema change on shard 40-80 already failed")},
	}
	client := newEachTargetControlTestClient(apply, tasks, eng)
	stor := client.storage.(*controlTestStorage)
	stor.tasks = &selectivelyFailingTaskStore{
		controlTestTaskStore: &controlTestTaskStore{tasks: tasks},
		failIdentifier:       tasks[0].TaskIdentifier,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	stor.controlRequests = controlRequests

	_, err := client.processPendingStopControlRequest(t.Context(), apply)

	require.Error(t, err)
	require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
	assert.False(t, engine.IsSettledOutcome(err), "the later shard's refusal must not resolve the request")
	assert.Contains(t, err.Error(), "already failed", "the later shard's refusal stays readable for triage")
	assert.Equal(t, state.Task.Running, tasks[0].State)
	assert.Equal(t, state.Task.Running, tasks[1].State)
	pending, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	assert.NotNil(t, pending, "the request stays pending for a retry")
}

// Two shards copy under their own operations, each with its own drive, and the
// operator's stop is consumed by one of them. The drive the stop wakes can be
// the other shard's, and that drive settles its operation from its task rows as
// it returns. So it is woken only once every task already reads stopped; woken
// earlier, it would leave its operation running and the stop would wait for
// that operation's lease to go stale.
func TestLocalClient_StopWakesTheDriveOnlyAfterEveryTaskReadsStopped(t *testing.T) {
	apply, tasks := eachTargetTestApply("apply-stop-wakes-after-record",
		state.Task.Running, state.Task.Running)
	eng := &eachTargetControlEngine{controlCaptureEngine: &controlCaptureEngine{}}
	client := newEachTargetControlTestClient(apply, tasks, eng)

	var statesAtWake []string
	client.setApplyCancel(func(error) {
		stored, err := client.storage.Tasks().GetByApplyID(t.Context(), apply.ID)
		require.NoError(t, err)
		for _, task := range stored {
			statesAtWake = append(statesAtWake, task.State)
		}
	})

	resp, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	assert.Equal(t, []string{"-40", "40-80"}, eng.stopTargets, "each copying shard gets its own engine stop")
	assert.Equal(t, []string{state.Task.Stopped, state.Task.Stopped}, statesAtWake,
		"the woken drive must read every task stopped")
}
