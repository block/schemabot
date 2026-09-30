//go:build integration

package tern

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// cutoverTurnMember is one operation of a seeded multi-operation apply: its
// deployment, the shard work key that tells same-deployment operations apart,
// the remote apply it was dispatched to, and the state of it and its task.
type cutoverTurnMember struct {
	deployment string
	// target defaults to one target per deployment.
	target       string
	operationKey string
	remoteID     string
	opState      string
	taskState    string
}

// seedCutoverTurnApply stores a running multi-operation apply started with
// manual --defer-cutover, one task per operation, and a pending operator
// cutover request. It returns the apply and its operations in member order.
func seedCutoverTurnApply(t *testing.T, stor storage.Storage, identifier, cutoverPolicy string, members []cutoverTurnMember) (*storage.Apply, []*storage.ApplyOperation) {
	t.Helper()
	ctx := t.Context()
	now := time.Now()
	options := storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true})
	apply := &storage.Apply{
		ApplyIdentifier: identifier,
		Database:        "payments",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		Environment:     "staging",
		Deployment:      members[0].deployment,
		Caller:          "cutover-turn-test",
		Engine:          storage.EngineForType(storage.DatabaseTypeMySQL),
		State:           state.Apply.Running,
		Options:         options,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	groups := make([]*storage.ApplyOperationWithTasks, 0, len(members))
	for _, m := range members {
		target := m.target
		if target == "" {
			target = "payments-" + m.deployment
		}
		groups = append(groups, &storage.ApplyOperationWithTasks{
			Operation: &storage.ApplyOperation{
				Deployment:    m.deployment,
				OperationKey:  m.operationKey,
				Target:        target,
				ExternalID:    m.remoteID,
				State:         m.opState,
				CutoverPolicy: cutoverPolicy,
				OnFailure:     storage.OnFailureHalt,
				CreatedAt:     now,
				UpdatedAt:     now,
			},
			Tasks: []*storage.Task{{
				TaskIdentifier: identifier + "-" + m.deployment + "-" + m.operationKey,
				Database:       "payments",
				DatabaseType:   storage.DatabaseTypeMySQL,
				Engine:         storage.EngineForType(storage.DatabaseTypeMySQL),
				Repository:     "octocat/hello-world",
				PullRequest:    1,
				Environment:    "staging",
				State:          m.taskState,
				Options:        options,
				Namespace:      "payments",
				TableName:      "widgets",
				DDL:            "ALTER TABLE widgets ADD COLUMN c int",
				DDLAction:      "alter",
				CreatedAt:      now,
				UpdatedAt:      now,
			}},
		})
	}
	applyID, err := stor.Applies().CreateWithGroupedOperations(ctx, apply, groups)
	require.NoError(t, err, "seed multi-operation apply")
	apply.ID = applyID

	ops := make([]*storage.ApplyOperation, 0, len(groups))
	for _, group := range groups {
		op, err := stor.ApplyOperations().Get(ctx, group.Operation.ID)
		require.NoError(t, err)
		require.NotNil(t, op)
		ops = append(ops, op)
	}
	requestCutover(t, stor, applyID)
	return apply, ops
}

// requestCutover records a pending operator cutover request for the apply.
func requestCutover(t *testing.T, stor storage.Storage, applyID int64) {
	t.Helper()
	requestCutoverBoundTo(t, stor, applyID, 0)
}

// requestCutoverBoundTo records a pending operator cutover request bound to
// one operation, as intake binds it to the member whose turn it is. A zero
// operation ID records an unbound request.
func requestCutoverBoundTo(t *testing.T, stor storage.Storage, applyID, operationID int64) {
	t.Helper()
	req := &storage.ApplyControlRequest{
		ApplyID:     applyID,
		Operation:   storage.ControlOperationCutover,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}
	if operationID != 0 {
		req.Metadata = storage.CutoverRequestMetadata(operationID)
	}
	_, alreadyPending, err := stor.ControlRequests().RequestPending(t.Context(), req)
	require.NoError(t, err)
	require.False(t, alreadyPending)
}

// driveCutoverRequestAs runs one pass of the pending-cutover handling as the
// drive of op would: an operation-scoped drive holding its own copy of the
// parent apply, loaded the way the operator loads it.
func driveCutoverRequestAs(t *testing.T, client *GRPCClient, apply *storage.Apply, op *storage.ApplyOperation) {
	t.Helper()
	driveApply := *apply
	driveApply.Deployment = op.Deployment
	scope, err := client.loadOperationApplyTaskScope(t.Context(), &driveApply, op.ID)
	require.NoError(t, err)
	require.NoError(t, client.processPendingCutoverControlRequest(t.Context(), &driveApply, scope))
}

// setMemberState moves one operation and its tasks to a new state, as the
// data plane's progress would.
func setMemberState(t *testing.T, stor storage.Storage, op *storage.ApplyOperation, opState, taskState string) {
	t.Helper()
	ctx := t.Context()
	tasks, err := stor.Tasks().GetByApplyOperationID(ctx, op.ID)
	require.NoError(t, err)
	for _, task := range tasks {
		task.State = taskState
		require.NoError(t, stor.Tasks().Update(ctx, task))
	}
	if opState == state.ApplyOperation.Completed {
		require.NoError(t, stor.ApplyOperations().MarkCompleted(ctx, op.ID))
		return
	}
	require.NoError(t, stor.ApplyOperations().UpdateState(ctx, op.ID, opState))
}

// requireCutoverRequestPending asserts whether the apply has a pending cutover
// request.
func requireCutoverRequestPending(t *testing.T, stor storage.Storage, applyID int64, pending bool, msg string) {
	t.Helper()
	req, err := stor.ControlRequests().GetPending(t.Context(), applyID, storage.ControlOperationCutover)
	require.NoError(t, err)
	if pending {
		require.NotNil(t, req, msg)
		return
	}
	require.Nil(t, req, msg)
}

// A barrier rollout started with --defer-cutover has two later deployments
// parked while the first is still copying. The operator's single cutover
// request must not be taken by a parked later deployment, whose drive would
// cut over its own target out of deployment order, nor by the copying
// deployment, which has nothing to cut over. It waits until the first
// deployment parks and is then cut over there; the next request goes to the
// next deployment in order, never past it.
func TestGRPCClient_CutoverRequestWaitsForTheDeploymentWhoseTurnItIs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	_, dsn := setupMySQLContainer(t)
	stor := createControlPlaneStorage(t, dsn)
	server := &capturingTernServer{cutoverAccepted: true}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	client.storage = stor

	apply, ops := seedCutoverTurnApply(t, stor, "apply-cutover-turn-order", storage.CutoverPolicyBarrier, []cutoverTurnMember{
		{deployment: "eu", remoteID: "remote-eu", opState: state.ApplyOperation.Running, taskState: state.Task.Running},
		{deployment: "us", remoteID: "remote-us", opState: state.ApplyOperation.WaitingForCutover, taskState: state.Task.WaitingForCutover},
		{deployment: "au", remoteID: "remote-au", opState: state.ApplyOperation.WaitingForCutover, taskState: state.Task.WaitingForCutover},
	})
	eu, us, au := ops[0], ops[1], ops[2]

	driveCutoverRequestAs(t, client, apply, us)
	driveCutoverRequestAs(t, client, apply, au)
	driveCutoverRequestAs(t, client, apply, eu)
	assert.Empty(t, server.getCutoverApplyID(), "no deployment may cut over while the first in order is still copying")
	requireCutoverRequestPending(t, stor, apply.ID, true, "the request waits for the deployment whose turn it is")

	setMemberState(t, stor, eu, state.ApplyOperation.WaitingForCutover, state.Task.WaitingForCutover)
	driveCutoverRequestAs(t, client, apply, au)
	assert.Empty(t, server.getCutoverApplyID(), "a later parked deployment still waits for the first")
	driveCutoverRequestAs(t, client, apply, eu)
	assert.Equal(t, "remote-eu", server.getCutoverApplyID(), "the first deployment takes the request once it parks")
	requireCutoverRequestPending(t, stor, apply.ID, false, "the request is completed by the deployment that took it")

	setMemberState(t, stor, eu, state.ApplyOperation.Completed, state.Task.Completed)
	requestCutover(t, stor, apply.ID)
	driveCutoverRequestAs(t, client, apply, au)
	assert.Equal(t, "remote-eu", server.getCutoverApplyID(), "the third deployment may not skip past the second")
	requireCutoverRequestPending(t, stor, apply.ID, true, "the next request waits for the second deployment")
	driveCutoverRequestAs(t, client, apply, us)
	assert.Equal(t, "remote-us", server.getCutoverApplyID(), "the second deployment takes the next request")
	requireCutoverRequestPending(t, stor, apply.ID, false, "the second deployment completes the request")
}

// Under the parallel policy every deployment copies at once, so the first
// deployment can park while a later one is still copying. The copying
// deployment's drive sees the apply as ready for cutover — a sibling is
// parked — but has nothing of its own to cut over, so it must leave the
// request for the parked deployment rather than send the cutover to its own
// still-copying remote apply.
func TestGRPCClient_CopyingDeploymentLeavesCutoverRequestForParkedDeployment(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	_, dsn := setupMySQLContainer(t)
	stor := createControlPlaneStorage(t, dsn)
	server := &capturingTernServer{cutoverAccepted: true}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	client.storage = stor

	apply, ops := seedCutoverTurnApply(t, stor, "apply-cutover-turn-copying", storage.CutoverPolicyParallel, []cutoverTurnMember{
		{deployment: "eu", remoteID: "remote-eu", opState: state.ApplyOperation.WaitingForCutover, taskState: state.Task.WaitingForCutover},
		{deployment: "us", remoteID: "remote-us", opState: state.ApplyOperation.Running, taskState: state.Task.Running},
	})

	driveCutoverRequestAs(t, client, apply, ops[1])
	assert.Empty(t, server.getCutoverApplyID(), "a copying deployment must not take the cutover request")
	requireCutoverRequestPending(t, stor, apply.ID, true, "the request stays pending for the parked deployment")

	driveCutoverRequestAs(t, client, apply, ops[0])
	assert.Equal(t, "remote-eu", server.getCutoverApplyID())
	requireCutoverRequestPending(t, stor, apply.ID, false, "the parked deployment completes the request")
}

// The shard work operations of one deployment share its remote apply and cut
// over together as whichever batch is parked: a multi-shard apply started with
// --defer-cutover takes one cutover per batch. A shard still copying ahead of
// a parked one in the same deployment therefore does not hold the parked
// shard's turn, and the parked shard's drive cuts over the deployment's remote
// apply.
func TestGRPCClient_ShardSiblingDoesNotHoldCutoverRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	_, dsn := setupMySQLContainer(t)
	stor := createControlPlaneStorage(t, dsn)
	server := &capturingTernServer{cutoverAccepted: true}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	client.storage = stor

	apply, ops := seedCutoverTurnApply(t, stor, "apply-cutover-turn-shards", storage.CutoverPolicyBarrier, []cutoverTurnMember{
		{deployment: "eu", operationKey: "commerce/-80/widgets", remoteID: "remote-eu", opState: state.ApplyOperation.Running, taskState: state.Task.Running},
		{deployment: "eu", operationKey: "commerce/80-/widgets", remoteID: "remote-eu", opState: state.ApplyOperation.WaitingForCutover, taskState: state.Task.WaitingForCutover},
	})

	driveCutoverRequestAs(t, client, apply, ops[1])
	assert.Equal(t, "remote-eu", server.getCutoverApplyID(), "a parked shard cuts over its deployment's batch")
	requireCutoverRequestPending(t, stor, apply.ID, false, "the parked shard completes the request")
}

// A rolling rollout serializes whole deployments, so the one member that has
// parked is the only one active. Its drive takes the cutover request exactly
// as before: the turn gate applies only where the automatic cutover claim
// orders the swaps.
func TestGRPCClient_RollingDeploymentTakesCutoverRequestUnchanged(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	_, dsn := setupMySQLContainer(t)
	stor := createControlPlaneStorage(t, dsn)
	server := &capturingTernServer{cutoverAccepted: true}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	client.storage = stor

	apply, ops := seedCutoverTurnApply(t, stor, "apply-cutover-turn-rolling", storage.CutoverPolicyRolling, []cutoverTurnMember{
		{deployment: "eu", remoteID: "remote-eu", opState: state.ApplyOperation.Completed, taskState: state.Task.Completed},
		{deployment: "us", remoteID: "remote-us", opState: state.ApplyOperation.WaitingForCutover, taskState: state.Task.WaitingForCutover},
		{deployment: "au", opState: state.ApplyOperation.Pending, taskState: state.Task.Pending},
	})

	driveCutoverRequestAs(t, client, apply, ops[1])
	assert.Equal(t, "remote-us", server.getCutoverApplyID())
	requireCutoverRequestPending(t, stor, apply.ID, false, "the parked rolling deployment completes the request")
}

// Two targets listed under one deployment are separate rollout members, each
// its own database with its own cutover. A barrier rollout started with
// --defer-cutover whose first target is still copying therefore holds the
// second target's cutover exactly as an earlier deployment would, even though
// both targets belong to the same deployment.
func TestGRPCClient_EarlierTargetOfTheSameDeploymentHoldsCutoverRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	_, dsn := setupMySQLContainer(t)
	stor := createControlPlaneStorage(t, dsn)
	server := &capturingTernServer{cutoverAccepted: true}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	client.storage = stor

	apply, ops := seedCutoverTurnApply(t, stor, "apply-cutover-turn-targets", storage.CutoverPolicyBarrier, []cutoverTurnMember{
		{deployment: "primary", target: "orders-001", operationKey: "orders-001", remoteID: "remote-001", opState: state.ApplyOperation.Running, taskState: state.Task.Running},
		{deployment: "primary", target: "orders-002", operationKey: "orders-002", remoteID: "remote-002", opState: state.ApplyOperation.WaitingForCutover, taskState: state.Task.WaitingForCutover},
	})

	driveCutoverRequestAs(t, client, apply, ops[1])
	assert.Empty(t, server.getCutoverApplyID(), "a later target may not cut over while an earlier target is still copying")
	requireCutoverRequestPending(t, stor, apply.ID, true, "the request waits for the earlier target")

	setMemberState(t, stor, ops[0], state.ApplyOperation.Completed, state.Task.Completed)
	driveCutoverRequestAs(t, client, apply, ops[1])
	assert.Equal(t, "remote-002", server.getCutoverApplyID(), "the later target takes the request once the earlier one has completed")
	requireCutoverRequestPending(t, stor, apply.ID, false, "the later target completes the request")
}

// One cutover command cuts over one member. A request accepted for eu is bound
// to eu, so us's drive leaves it alone while eu can still take it. If eu then
// completes while the request is still pending, as happens when the data plane
// took eu's cutover but its answer never came back, the request is settled as
// landed rather than taken by us: us cuts over only on a command of its own.
func TestGRPCClient_BoundCutoverRequestNeverPassesToTheNextMember(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	_, dsn := setupMySQLContainer(t)
	stor := createControlPlaneStorage(t, dsn)
	server := &capturingTernServer{cutoverAccepted: true}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	client.storage = stor

	apply, ops := seedCutoverTurnApply(t, stor, "apply-cutover-turn-bound", storage.CutoverPolicyBarrier, []cutoverTurnMember{
		{deployment: "eu", remoteID: "remote-eu", opState: state.ApplyOperation.WaitingForCutover, taskState: state.Task.WaitingForCutover},
		{deployment: "us", remoteID: "remote-us", opState: state.ApplyOperation.WaitingForCutover, taskState: state.Task.WaitingForCutover},
	})
	eu, us := ops[0], ops[1]
	require.NoError(t, stor.ControlRequests().CompletePending(t.Context(), apply.ID, storage.ControlOperationCutover))
	requestCutoverBoundTo(t, stor, apply.ID, eu.ID)

	driveCutoverRequestAs(t, client, apply, us)
	assert.Empty(t, server.getCutoverApplyID(), "a request bound to eu is not us's to take")
	requireCutoverRequestPending(t, stor, apply.ID, true, "the request waits for eu")

	setMemberState(t, stor, eu, state.ApplyOperation.Completed, state.Task.Completed)
	driveCutoverRequestAs(t, client, apply, us)
	assert.Empty(t, server.getCutoverApplyID(), "eu's command must not cut over us")
	requireCutoverRequestPending(t, stor, apply.ID, false, "the request settles once eu has cut over")
	settled, err := stor.ControlRequests().GetByOperation(t.Context(), apply.ID, storage.ControlOperationCutover)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, storage.ControlRequestCompleted, settled.Status, "eu's cutover landed, so its command completed")

	requestCutoverBoundTo(t, stor, apply.ID, us.ID)
	driveCutoverRequestAs(t, client, apply, us)
	assert.Equal(t, "remote-us", server.getCutoverApplyID(), "us cuts over on its own command")
}

// A request bound to a member that ended without cutting over had no effect,
// so it is failed rather than left pending for a member that will never take
// it, or passed to the next one.
func TestGRPCClient_CutoverRequestBoundToAFailedMemberFails(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	_, dsn := setupMySQLContainer(t)
	stor := createControlPlaneStorage(t, dsn)
	server := &capturingTernServer{cutoverAccepted: true}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	client.storage = stor

	apply, ops := seedCutoverTurnApply(t, stor, "apply-cutover-turn-bound-failed", storage.CutoverPolicyBarrier, []cutoverTurnMember{
		{deployment: "eu", remoteID: "remote-eu", opState: state.ApplyOperation.Failed, taskState: state.Task.Failed},
		{deployment: "us", remoteID: "remote-us", opState: state.ApplyOperation.WaitingForCutover, taskState: state.Task.WaitingForCutover},
	})
	require.NoError(t, stor.ControlRequests().CompletePending(t.Context(), apply.ID, storage.ControlOperationCutover))
	requestCutoverBoundTo(t, stor, apply.ID, ops[0].ID)

	driveCutoverRequestAs(t, client, apply, ops[1])
	assert.Empty(t, server.getCutoverApplyID(), "a request bound to a failed member cuts nothing over")
	settled, err := stor.ControlRequests().GetByOperation(t.Context(), apply.ID, storage.ControlOperationCutover)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, storage.ControlRequestFailed, settled.Status)
	assert.Equal(t, "cutover request was not applied because the member it was accepted for is failed", settled.ErrorMessage)
}
