package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// stoppedBeforeDispatchFixture is a rollout whose deployment carries two
// operations: the first dispatched before a stop and shares the deployment's
// data-plane apply, and the second was still waiting for its first dispatch
// when the stop landed, so the start claim moved it to resuming.
func stoppedBeforeDispatchFixture(t *testing.T, server *capturingTernServer, siblingRemoteID string) (*GRPCClient, *storage.Apply, applyTaskScope) {
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
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{},
		logs:    &mockApplyLogStore{},
		controlRequests: &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
			ApplyID: apply.ID, Operation: storage.ControlOperationStart, Status: storage.ControlRequestPending, RequestedBy: "cli:operator",
		}}},
		operations: &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{1: sibling, 2: undispatched}},
	}
	scope := applyTaskScope{applyOperationID: undispatched.ID, operation: undispatched, multiOperation: true}
	return client, apply, scope
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
	client, apply, scope := stoppedBeforeDispatchFixture(t, server, "remote-shared")

	standDown, err := client.startStoppedDeploymentApplyBeforeAttach(t.Context(), apply, scope)
	require.NoError(t, err)
	assert.False(t, standDown, "no stop or cancel landed, so the dispatch goes ahead")
	assert.Equal(t, "remote-shared", server.getStartApplyID(), "the deployment's shared data-plane apply is started")
	server.mu.Lock()
	remaining := len(server.progressStates)
	server.mu.Unlock()
	assert.Zero(t, remaining, "the dispatch waits until the shared apply reads running")
}

// A shared data-plane apply that already resumed is attached to as is.
func TestGRPCClient_DispatchAfterStopLeavesAResumedDeploymentApplyAlone(t *testing.T) {
	server := &capturingTernServer{progressState: ternv1.State_STATE_RUNNING, progressStateSet: true}
	client, apply, scope := stoppedBeforeDispatchFixture(t, server, "remote-shared")

	standDown, err := client.startStoppedDeploymentApplyBeforeAttach(t.Context(), apply, scope)
	require.NoError(t, err)
	assert.False(t, standDown)
	assert.Equal(t, "remote-shared", server.getProgressApplyID(), "the shared apply is read before attaching")
	assert.False(t, server.startWasCalled(), "a running shared apply is not started again")
}

// The first of a deployment's operations to dispatch creates its data-plane
// apply, so there is nothing to start and nothing is read.
func TestGRPCClient_DispatchAfterStopWithNoDispatchedSiblingCreatesTheDeploymentApply(t *testing.T) {
	server := &capturingTernServer{progressState: ternv1.State_STATE_STOPPED, progressStateSet: true}
	client, apply, scope := stoppedBeforeDispatchFixture(t, server, "")

	standDown, err := client.startStoppedDeploymentApplyBeforeAttach(t.Context(), apply, scope)
	require.NoError(t, err)
	assert.False(t, standDown)
	assert.Empty(t, server.getProgressApplyID(), "no data-plane apply exists yet to read")
	assert.False(t, server.startWasCalled())
}

// Only a dispatch the start claim moved to resuming reads the shared apply. A
// first dispatch that no stop interrupted goes straight to the attach.
func TestGRPCClient_FirstDispatchWithoutAStopSkipsTheDeploymentApplyCheck(t *testing.T) {
	server := &capturingTernServer{progressState: ternv1.State_STATE_STOPPED, progressStateSet: true}
	client, apply, scope := stoppedBeforeDispatchFixture(t, server, "remote-shared")
	scope.operation.State = state.Apply.Running

	standDown, err := client.startStoppedDeploymentApplyBeforeAttach(t.Context(), apply, scope)
	require.NoError(t, err)
	assert.False(t, standDown)
	assert.Empty(t, server.getProgressApplyID())
	assert.False(t, server.startWasCalled())
}
