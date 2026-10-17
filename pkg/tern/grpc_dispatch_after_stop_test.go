package tern

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// stoppedBeforeDispatch is a rollout whose deployment carries two operations:
// the first dispatched before a stop and shares the deployment's data-plane
// apply, and the second was still waiting for its first dispatch when the stop
// landed, so the start claim moved it to resuming. The operator's start is
// pending.
type stoppedBeforeDispatch struct {
	client       *GRPCClient
	apply        *storage.Apply
	scope        applyTaskScope
	undispatched *storage.ApplyOperation
	controls     *testControlRequestStore
	logs         *mockApplyLogStore
}

func stoppedBeforeDispatchFixture(t *testing.T, server *capturingTernServer, siblingRemoteID string) *stoppedBeforeDispatch {
	t.Helper()
	client, cleanup := testCapturingGRPCClient(t, server)
	t.Cleanup(cleanup)

	apply := &storage.Apply{
		ID:              1,
		ApplyIdentifier: "apply-stopped-before-dispatch",
		Database:        "shardapp",
		Environment:     "staging",
		State:           state.Apply.Resuming,
	}
	sibling := &storage.ApplyOperation{ID: 1, ApplyID: apply.ID, Deployment: "data-plane", OperationKey: "shard-001/step-1", State: state.Apply.Running, ExternalID: siblingRemoteID}
	undispatched := &storage.ApplyOperation{ID: 2, ApplyID: apply.ID, Deployment: "data-plane", OperationKey: "shard-002/step-1", State: state.Apply.Resuming}
	controls := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ID: 1, ApplyID: apply.ID, Operation: storage.ControlOperationStart, Status: storage.ControlRequestPending, RequestedBy: "cli:operator",
	}}}
	logs := &mockApplyLogStore{}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: apply},
		tasks:           &mockTaskStore{},
		logs:            logs,
		controlRequests: controls,
		operations:      &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{1: sibling, 2: undispatched}},
	}
	return &stoppedBeforeDispatch{
		client:       client,
		apply:        apply,
		scope:        applyTaskScope{applyOperationID: undispatched.ID, operation: undispatched, multiOperation: true},
		undispatched: undispatched,
		controls:     controls,
		logs:         logs,
	}
}

func (f *stoppedBeforeDispatch) wait(t *testing.T) (bool, error) {
	t.Helper()
	return f.client.startStoppedDeploymentApplyBeforeAttach(t.Context(), f.apply, f.scope)
}

func (f *stoppedBeforeDispatch) controlRequest(t *testing.T, operation storage.ControlOperation) *storage.ApplyControlRequest {
	t.Helper()
	for _, req := range f.controls.requests {
		if req.Operation == operation {
			return req
		}
	}
	require.Failf(t, "no control request", "no %s control request recorded", operation)
	return nil
}

// stoppedProgress keeps the shared data-plane apply reading stopped for longer
// than any test's grace period.
func stoppedProgress() []ternv1.State {
	states := make([]ternv1.State, 20)
	for i := range states {
		states[i] = ternv1.State_STATE_STOPPED
	}
	return states
}

// withShortStoppedAfterStartGrace shortens the grace period so a test can run
// it out after a few polls.
func withShortStoppedAfterStartGrace(t *testing.T) {
	t.Helper()
	original := grpcStoppedAfterStartGracePeriod
	grpcStoppedAfterStartGracePeriod = 3 * grpcProgressPollInterval
	t.Cleanup(func() { grpcStoppedAfterStartGracePeriod = original })
}

// A stop caught one shard's operation before its first dispatch, and the
// start then dispatches it while the deployment's shared data-plane apply
// still reads stopped, because the sibling drives asking for its start have
// not landed yet. The dispatch asks for the start itself and waits for the
// shared apply to leave stopped, so the attach joins the resumed apply
// instead of being refused and failing the rollout.
func TestGRPCClient_DispatchAfterStopStartsTheStoppedDeploymentApplyBeforeAttaching(t *testing.T) {
	server := &capturingTernServer{progressStates: []ternv1.State{
		ternv1.State_STATE_STOPPED,
		ternv1.State_STATE_STOPPED,
		ternv1.State_STATE_RUNNING,
	}}
	f := stoppedBeforeDispatchFixture(t, server, "remote-shared")

	standDown, err := f.wait(t)
	require.NoError(t, err)
	assert.False(t, standDown, "no stop or cancel landed, so the dispatch goes ahead")
	assert.Equal(t, "remote-shared", server.getStartApplyID(), "the deployment's shared data-plane apply is started")
	assert.Equal(t, "cli:operator", server.startCaller, "the start goes out as the operator who asked for it")
	assert.Equal(t, 1, server.startCalls, "an accepted start is not sent again")
	server.mu.Lock()
	remaining := len(server.progressStates)
	server.mu.Unlock()
	assert.Zero(t, remaining, "the dispatch waits until the shared apply reads running")
}

// A shared data-plane apply that already resumed is attached to as is.
func TestGRPCClient_DispatchAfterStopLeavesAResumedDeploymentApplyAlone(t *testing.T) {
	server := &capturingTernServer{progressState: ternv1.State_STATE_RUNNING, progressStateSet: true}
	f := stoppedBeforeDispatchFixture(t, server, "remote-shared")

	standDown, err := f.wait(t)
	require.NoError(t, err)
	assert.False(t, standDown)
	assert.Equal(t, "remote-shared", server.getProgressApplyID(), "the shared apply is read before attaching")
	assert.False(t, server.startWasCalled(), "a running shared apply is not started again")
}

// The first of a deployment's operations to dispatch creates its data-plane
// apply, so there is nothing to start and nothing is read.
func TestGRPCClient_DispatchAfterStopWithNoDispatchedSiblingCreatesTheDeploymentApply(t *testing.T) {
	server := &capturingTernServer{progressState: ternv1.State_STATE_STOPPED, progressStateSet: true}
	f := stoppedBeforeDispatchFixture(t, server, "")

	standDown, err := f.wait(t)
	require.NoError(t, err)
	assert.False(t, standDown)
	assert.Empty(t, server.getProgressApplyID(), "no data-plane apply exists yet to read")
	assert.False(t, server.startWasCalled())
}

// Only a dispatch the start claim moved to resuming reads the shared apply. A
// first dispatch that no stop interrupted goes straight to the attach.
func TestGRPCClient_FirstDispatchWithoutAStopSkipsTheDeploymentApplyCheck(t *testing.T) {
	server := &capturingTernServer{progressState: ternv1.State_STATE_STOPPED, progressStateSet: true}
	f := stoppedBeforeDispatchFixture(t, server, "remote-shared")
	f.undispatched.State = state.Apply.Running

	standDown, err := f.wait(t)
	require.NoError(t, err)
	assert.False(t, standDown)
	assert.Empty(t, server.getProgressApplyID())
	assert.False(t, server.startWasCalled())
}

// A stop lands while the dispatch waits for the shared apply to leave stopped.
// The stop settles the undispatched operation, so the dispatch stands down and
// no work is sent to the data plane for it.
func TestGRPCClient_DispatchAfterStopStandsDownForAStopLandingMidWait(t *testing.T) {
	server := &capturingTernServer{progressStates: stoppedProgress()}
	f := stoppedBeforeDispatchFixture(t, server, "remote-shared")
	server.startHook = func(int) error {
		f.controls.requests = append(f.controls.requests, &storage.ApplyControlRequest{
			ID: 2, ApplyID: f.apply.ID, Operation: storage.ControlOperationStop, Status: storage.ControlRequestPending, RequestedBy: "cli:operator",
		})
		return errors.New("data plane unavailable")
	}

	standDown, err := f.wait(t)
	require.NoError(t, err)
	assert.True(t, standDown, "the stop settles the undispatched operation")
	assert.Equal(t, 1, server.startCalls, "no start is sent once the stop is pending")
	assert.Nil(t, server.applyReq, "no work is dispatched after the stop")
	assert.Equal(t, state.Apply.Stopped, f.undispatched.State, "the stop records the undispatched operation stopped")
}

// The data plane refuses the first start. The dispatch sends it again on the
// next poll, and once the shared apply resumes the attach goes ahead.
func TestGRPCClient_DispatchAfterStopRetriesAFailedStart(t *testing.T) {
	server := &capturingTernServer{progressStates: []ternv1.State{
		ternv1.State_STATE_STOPPED,
		ternv1.State_STATE_STOPPED,
		ternv1.State_STATE_RUNNING,
	}}
	server.startHook = func(call int) error {
		if call == 1 {
			return errors.New("data plane unavailable")
		}
		return nil
	}
	f := stoppedBeforeDispatchFixture(t, server, "remote-shared")

	standDown, err := f.wait(t)
	require.NoError(t, err)
	assert.False(t, standDown)
	assert.Equal(t, 2, server.startCalls, "the failed start is sent again")
	assert.Equal(t, storage.ControlRequestPending, f.controlRequest(t, storage.ControlOperationStart).Status, "the start stays pending for the drives that apply it")
}

// The shared apply accepts the start but still reads stopped when the grace
// period runs out. The undispatched operation goes back to stopped and the
// operator's start fails with a message saying so, so no later claim waits on
// the same start again and the operator can re-issue it.
func TestGRPCClient_DispatchAfterStopReturnsTheOperationToStoppedWhenTheApplyStaysStopped(t *testing.T) {
	withShortStoppedAfterStartGrace(t)
	server := &capturingTernServer{progressStates: stoppedProgress()}
	f := stoppedBeforeDispatchFixture(t, server, "remote-shared")

	standDown, err := f.wait(t)
	require.Error(t, err)
	assert.False(t, standDown)
	assert.Contains(t, err.Error(), "still reads stopped")
	assert.Contains(t, err.Error(), "after this drive asked for its start")
	assert.NotContains(t, err.Error(), "%!", "the error renders no missing format operand")
	assert.Equal(t, 1, server.startCalls)
	assert.Nil(t, server.applyReq, "nothing is attached to a stopped apply")
	assert.Equal(t, state.Apply.Stopped, f.undispatched.State, "the operation is stopped again")
	start := f.controlRequest(t, storage.ControlOperationStart)
	assert.Equal(t, storage.ControlRequestFailed, start.Status, "the operator's start fails")
	assert.Equal(t, "Deployment data-plane did not resume, so operation shard-002/step-1 stays stopped and the start can be re-issued", start.ErrorMessage)
	assert.True(t, hasLogMessageContaining(f.logs.logs, "Deployment data-plane did not resume"), "the apply log records the warning")
}

// With no start pending, a sibling drive already settled the operator's start,
// so this dispatch sends none of its own. When the shared apply never resumes,
// the operation goes back to stopped.
func TestGRPCClient_DispatchAfterStopSendsNoStartWithoutAPendingStart(t *testing.T) {
	withShortStoppedAfterStartGrace(t)
	server := &capturingTernServer{progressStates: stoppedProgress()}
	f := stoppedBeforeDispatchFixture(t, server, "remote-shared")
	f.controls.requests[0].Status = storage.ControlRequestFailed

	standDown, err := f.wait(t)
	require.Error(t, err)
	assert.False(t, standDown)
	assert.Contains(t, err.Error(), "no start was pending")
	assert.False(t, server.startWasCalled(), "no start goes out without the operator's pending start")
	assert.Equal(t, state.Apply.Stopped, f.undispatched.State, "the operation is stopped again")
}
