package tern

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// grpcShardHaltFixture is a control-plane drive of shard -80 of target
// orders-001, dispatched to the data plane as its own remote apply, beside
// shard 80- of the same target and shard -80 of target orders-002.
type grpcShardHaltFixture struct {
	server *capturingTernServer
	client *GRPCClient
	apply  *storage.Apply
	task   *storage.Task
	ops    *mockApplyOperationStore
	logs   *mockApplyLogStore
	scope  applyTaskScope
}

func newGRPCShardHaltFixture(t *testing.T, taskState string) *grpcShardHaltFixture {
	t.Helper()
	server := &capturingTernServer{}
	client, cleanup := testCapturingGRPCClient(t, server)
	t.Cleanup(cleanup)

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-shard-halt",
		PlanID:          99,
		Database:        "orders",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.RunningDegraded,
	}
	shard := func(id int64, target, key, remote string) *storage.ApplyOperation {
		return &storage.ApplyOperation{
			ID: id, ApplyID: apply.ID, Deployment: "orders", Target: target,
			OperationKey:  storage.TargetOperationKey(target, storage.ShardOperationKey("orders", key, "orders")),
			OperationKind: storage.ApplyOperationKindWork, OnFailure: storage.OnFailureHalt,
			State: state.ApplyOperation.Running, ExternalID: remote,
		}
	}
	driven := shard(42, "orders-001", "-80", "remote-orders-001-a")
	ops := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		42: driven,
		43: shard(43, "orders-001", "80-", "remote-orders-001-b"),
		44: shard(44, "orders-002", "-80", "remote-orders-002-a"),
	}}
	task := &storage.Task{
		ID: 11, TaskIdentifier: "task-orders-001-a", ApplyID: apply.ID, ApplyOperationID: &driven.ID,
		Namespace: "orders", TableName: "orders", State: taskState,
	}
	logs := &mockApplyLogStore{}
	storedApply := *apply
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            logs,
		controlRequests: &testControlRequestStore{},
		operations:      ops,
	}
	return &grpcShardHaltFixture{
		server: server, client: client, apply: apply, task: task, ops: ops, logs: logs,
		scope: applyTaskScope{applyOperationID: driven.ID, operation: driven, multiOperation: true},
	}
}

// Shard 80- of target orders-001 fails under on_failure halt while the
// control plane polls shard -80, which runs on the data plane as its own
// remote apply. The drive asks the data plane to cancel that remote apply
// once, names the failed shard in the apply log, and keeps polling so the
// remote's cancelled outcome reaches the operation the way any remote outcome
// does. A later poll inside the resend interval sends nothing more.
func TestGRPCClient_FailedShardCancelsItsSiblingsRemoteApply(t *testing.T) {
	f := newGRPCShardHaltFixture(t, state.Task.Running)
	f.ops.ops[43].State = state.ApplyOperation.Failed

	var sentAt time.Time
	require.NoError(t, f.client.cancelRemoteForFailedSibling(t.Context(), f.apply, f.scope, &sentAt))
	assert.Equal(t, 1, f.server.getCancelCalls())
	assert.Equal(t, "remote-orders-001-a", f.server.getCancelApplyID(), "the drive cancels its own shard's remote apply")
	assert.Equal(t, sameMemberHaltCaller, f.server.getCancelCaller())
	assert.False(t, sentAt.IsZero())
	require.Len(t, f.logs.logs, 1)
	assert.Equal(t, "Cancelled orders-001/orders/-80/orders: orders-001/orders/80-/orders failed, and on_failure halt stops the rest of the work on orders-001", f.logs.logs[0].Message)

	require.NoError(t, f.client.cancelRemoteForFailedSibling(t.Context(), f.apply, f.scope, &sentAt))
	assert.Equal(t, 1, f.server.getCancelCalls(), "the data plane holds the cancel durably, so a poll inside the resend interval sends nothing")
	assert.Len(t, f.logs.logs, 1)
}

// A shard of target orders-002 fails under on_failure halt. Target orders-001
// is its own rollout member, so its shard keeps running on the data plane.
// Under continue, a failed shard of the same target halts nothing either.
func TestGRPCClient_FailedShardLeavesOtherMembersRunning(t *testing.T) {
	t.Run("another target", func(t *testing.T) {
		f := newGRPCShardHaltFixture(t, state.Task.Running)
		f.ops.ops[44].State = state.ApplyOperation.Failed
		var sentAt time.Time
		require.NoError(t, f.client.cancelRemoteForFailedSibling(t.Context(), f.apply, f.scope, &sentAt))
		assert.Zero(t, f.server.getCancelCalls())
		assert.True(t, sentAt.IsZero())
	})
	t.Run("continue", func(t *testing.T) {
		f := newGRPCShardHaltFixture(t, state.Task.Running)
		f.ops.ops[42].OnFailure = storage.OnFailureContinue
		f.ops.ops[43].State = state.ApplyOperation.Failed
		var sentAt time.Time
		require.NoError(t, f.client.cancelRemoteForFailedSibling(t.Context(), f.apply, f.scope, &sentAt))
		assert.Zero(t, f.server.getCancelCalls())
	})
}

// Shard 80- of target orders-001 fails under on_failure halt after shard -80
// has started to cut over on the data plane. The drive sends no cancel, so the
// swap finishes, and the rollout still settles failed on shard 80-.
func TestGRPCClient_FailedShardLeavesASiblingThatIsCuttingOver(t *testing.T) {
	f := newGRPCShardHaltFixture(t, state.Task.CuttingOver)
	f.ops.ops[43].State = state.ApplyOperation.Failed
	var sentAt time.Time
	require.NoError(t, f.client.cancelRemoteForFailedSibling(t.Context(), f.apply, f.scope, &sentAt))
	assert.Zero(t, f.server.getCancelCalls(), "nothing cancels a shard mid-swap")
	assert.Empty(t, f.logs.logs)
}

// Shard 80- of target orders-001 fails under on_failure halt, and the data
// plane answers the halt cancel for shard -80 with an error, as it does once
// the remote has already settled or while it restarts. The drive does not
// abort: it keeps polling, so a settled remote's own outcome reaches the
// operation, and it sends the cancel again only on the resend cadence.
func TestGRPCClient_FailedShardHaltCancelErrorKeepsPolling(t *testing.T) {
	f := newGRPCShardHaltFixture(t, state.Task.Running)
	f.ops.ops[43].State = state.ApplyOperation.Failed
	f.server.cancelErr = status.Error(codes.Internal, "apply remote-orders-001-a is already terminal (state: failed)")

	var sentAt time.Time
	require.NoError(t, f.client.cancelRemoteForFailedSibling(t.Context(), f.apply, f.scope, &sentAt))
	assert.Equal(t, 1, f.server.getCancelCalls())
	assert.False(t, sentAt.IsZero(), "the failed send starts the resend cadence")
	assert.Empty(t, f.logs.logs, "no cancel was accepted, so the apply log records none")

	require.NoError(t, f.client.cancelRemoteForFailedSibling(t.Context(), f.apply, f.scope, &sentAt))
	assert.Equal(t, 1, f.server.getCancelCalls(), "a poll inside the resend interval sends nothing")
}

// Shard 80- of target orders-001 fails under on_failure halt, and shard -80
// was dispatched into the remote apply its deployment shares with target
// orders-002. The data plane cancels a remote apply as a whole, so a cancel
// would also stop orders-002, another rollout member. The drive sends none,
// logs why once, and keeps polling.
func TestGRPCClient_FailedShardSendsNoCancelForASharedRemoteApply(t *testing.T) {
	f := newGRPCShardHaltFixture(t, state.Task.Running)
	f.ops.ops[43].State = state.ApplyOperation.Failed
	f.scope.deploymentOperationKeys = []string{f.ops.ops[42].OperationKey, f.ops.ops[43].OperationKey, f.ops.ops[44].OperationKey}

	var sentAt time.Time
	require.NoError(t, f.client.cancelRemoteForFailedSibling(t.Context(), f.apply, f.scope, &sentAt))
	assert.Zero(t, f.server.getCancelCalls(), "a shared remote apply is never cancelled for one member's failure")
	assert.False(t, sentAt.IsZero())
	assert.Empty(t, f.logs.logs)
}
