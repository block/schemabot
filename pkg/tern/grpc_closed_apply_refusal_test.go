package tern

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

const closedApplyRefusal = "apply remote-payments for this idempotency key is terminal (cancelled); operation payments-002 cannot attach"

// refusedMemberDrive is payments-002's drive against a data plane that
// refused its dispatch, and the stores the drive wrote.
type refusedMemberDrive struct {
	err    error
	fx     memberTargetFixture
	tasks  *mockTaskStore
	logs   *mockApplyLogStore
	server *capturingTernServer
}

// driveRefusedMember drives payments-002 against a data plane that refuses
// its dispatch because the shared remote apply ended in closedState.
func driveRefusedMember(t *testing.T, closedState string) refusedMemberDrive {
	t.Helper()
	server := &capturingTernServer{applyResponse: &ternv1.ApplyResponse{
		Accepted:         false,
		ErrorMessage:     closedApplyRefusal,
		ClosedApplyState: closedState,
	}}
	client, cleanup := testCapturingGRPCClient(t, server)
	t.Cleanup(cleanup)
	fx := newMemberTargetFixture(t, client)
	store := client.storage.(*mockStorage)
	logs := &mockApplyLogStore{}
	store.logs = logs

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApplyOperation(ctx, fx.apply, fx.second)
	return refusedMemberDrive{err: err, fx: fx, tasks: store.tasks, logs: logs, server: server}
}

// The operator cancels a sharded apply while one shard is still being
// dispatched. The data plane refuses that shard because the apply it would
// join has been cancelled. The shard never started, so it settles cancelled
// with its siblings: the drive ends without an error, the shard's task carries
// no error text for the pull request to render, and the parent apply is left
// for the rollout projection to derive.
func TestGRPCClient_ShardRefusedByACancelledApplySettlesCancelled(t *testing.T) {
	d := driveRefusedMember(t, state.Apply.Cancelled)
	require.NoError(t, d.err)
	fx, tasks, logs, server := d.fx, d.tasks, d.logs, d.server

	second := fx.operations.ops[fx.second]
	assert.Equal(t, state.ApplyOperation.Cancelled, second.State)
	require.NotNil(t, second.CompletedAt)
	require.Len(t, tasks.updated, 1, "only the refused shard's task is settled")
	assert.Equal(t, "task-orders-002", tasks.updated[0].TaskIdentifier)
	assert.Equal(t, state.Task.Cancelled, tasks.updated[0].State)
	assert.Empty(t, tasks.updated[0].ErrorMessage, "the data plane's refusal text never reaches the pull request")
	assert.Equal(t, state.ApplyOperation.Pending, fx.operations.ops[fx.first].State, "the sibling operation is untouched")
	assert.Equal(t, state.Apply.Pending, fx.apply.State, "an operation-scoped drive never writes the parent apply")
	assert.Nil(t, server.getProgressRequest(), "a refused shard is never polled")

	require.Len(t, logs.logs, 1)
	assert.Equal(t, "Remote apply operation 42 (deployment default) cancelled before it started: the apply had already ended (cancelled)", logs.logs[0].Message)
	assert.NotContains(t, logs.logs[0].Message, "idempotency key")
}

// A sibling shard fails and closes the apply while this shard is still being
// dispatched. This shard did not run, so it settles cancelled rather than as a
// second failure, and the sibling's failure stays the apply's verdict.
func TestGRPCClient_ShardRefusedByAFailedApplySettlesCancelled(t *testing.T) {
	d := driveRefusedMember(t, state.Apply.Failed)
	require.NoError(t, d.err)
	fx, tasks := d.fx, d.tasks

	assert.Equal(t, state.ApplyOperation.Cancelled, fx.operations.ops[fx.second].State)
	require.Len(t, tasks.updated, 1)
	assert.Equal(t, state.Task.Cancelled, tasks.updated[0].State)
	assert.Empty(t, tasks.updated[0].ErrorMessage)
	assert.Equal(t, state.Apply.Failed, state.DeriveApplyState([]string{state.Task.Failed, tasks.updated[0].State}),
		"a failed sibling outranks the cancelled shard when the apply's state is derived")
}

// The operator stops a sharded MySQL apply while one shard is still being
// dispatched. The shard is stopped with its siblings, so a later start
// dispatches it again.
func TestGRPCClient_ShardRefusedByAStoppedApplySettlesStopped(t *testing.T) {
	d := driveRefusedMember(t, state.Apply.Stopped)
	require.NoError(t, d.err)
	fx, tasks := d.fx, d.tasks

	second := fx.operations.ops[fx.second]
	assert.Equal(t, state.ApplyOperation.Stopped, second.State)
	assert.Nil(t, second.CompletedAt, "a stopped operation is not complete")
	require.Len(t, tasks.updated, 1)
	assert.Equal(t, state.Task.Stopped, tasks.updated[0].State)
	assert.Empty(t, tasks.updated[0].ErrorMessage)
}

// An apply that completed without this shard did not put the change on it, so
// the refusal is not one the shard can join: it keeps failing closed, and an
// operator looks at the target.
func TestGRPCClient_ShardRefusedByACompletedApplyFailsClosed(t *testing.T) {
	d := driveRefusedMember(t, state.Apply.Completed)
	require.Error(t, d.err)
	assert.Contains(t, d.err.Error(), closedApplyRefusal)
	fx, tasks := d.fx, d.tasks

	assert.NotEqual(t, state.ApplyOperation.Cancelled, fx.operations.ops[fx.second].State)
	require.NotEmpty(t, tasks.updated)
	failed := tasks.updated[len(tasks.updated)-1]
	assert.Equal(t, "task-orders-002", failed.TaskIdentifier)
	assert.Equal(t, state.Task.Failed, failed.State, "the projection derives the operation failed from its task")
}

// A refusal that names no closed state is a refusal like any other: the shard
// fails closed with the data plane's reason.
func TestGRPCClient_ShardRefusedWithoutAClosedStateFailsClosed(t *testing.T) {
	d := driveRefusedMember(t, "")
	require.Error(t, d.err)
	assert.Contains(t, d.err.Error(), closedApplyRefusal)
	require.NotEmpty(t, d.tasks.updated)
	assert.Equal(t, state.Task.Failed, d.tasks.updated[len(d.tasks.updated)-1].State)
}

func TestClosedApplyTerminalization(t *testing.T) {
	for _, tc := range []struct {
		closedState    string
		databaseType   string
		settles        bool
		taskState      string
		operationState string
	}{
		{state.Apply.Cancelled, storage.DatabaseTypeMySQL, true, state.Task.Cancelled, state.ApplyOperation.Cancelled},
		{state.Apply.Failed, storage.DatabaseTypeMySQL, true, state.Task.Cancelled, state.ApplyOperation.Cancelled},
		{state.Apply.Stopped, storage.DatabaseTypeMySQL, true, state.Task.Stopped, state.ApplyOperation.Stopped},
		{"STATE_CANCELLED", storage.DatabaseTypeMySQL, true, state.Task.Cancelled, state.ApplyOperation.Cancelled},
		{state.Apply.Completed, storage.DatabaseTypeMySQL, false, "", ""},
		{state.Apply.Reverted, storage.DatabaseTypeMySQL, false, "", ""},
	} {
		t.Run(tc.closedState, func(t *testing.T) {
			terminalization, ok := closedApplyTerminalization(tc.closedState, tc.databaseType)
			assert.Equal(t, tc.settles, ok)
			assert.Equal(t, tc.taskState, terminalization.taskState)
			assert.Equal(t, tc.operationState, terminalization.operationState)
		})
	}
	stopped, ok := closedApplyTerminalization(state.Apply.Stopped, storage.DatabaseTypeVitess)
	require.True(t, ok)
	assert.Equal(t, stopUndispatchedTerminalization(storage.DatabaseTypeVitess), stopped,
		"a stopped apply settles a shard the way an undispatched stop does on that engine")
}
