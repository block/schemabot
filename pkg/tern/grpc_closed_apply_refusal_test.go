package tern

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
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
// its dispatch because the shared remote apply ended in closedState. configure
// adjusts the stores before the drive.
func driveRefusedMember(t *testing.T, closedState string, configure ...func(*mockStorage)) refusedMemberDrive {
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
	for _, fn := range configure {
		fn(store)
	}

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
	assert.Empty(t, logs.logs[0].OldState, "the apply-scoped entry names no transition the apply did not make")
	assert.Empty(t, logs.logs[0].NewState)
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

// Storage refuses the write that cancels the shard's task. The task set is
// then only partly settled, so the drive returns ErrTaskVerdictRefused: the
// operator leaves the operation row as found and claimable instead of
// deriving it from that set, and the next claim settles the shard again.
func TestGRPCClient_ShardRefusedByAClosedApplyStaysClaimableWhenItsTaskWriteFails(t *testing.T) {
	d := driveRefusedMember(t, state.Apply.Cancelled, func(store *mockStorage) {
		store.tasks.updateErr = errors.New("storage unavailable")
	})
	require.Error(t, d.err)
	assert.ErrorIs(t, d.err, ErrTaskVerdictRefused)
	assert.Contains(t, d.err.Error(), "task-orders-002")
	assert.Equal(t, state.ApplyOperation.Pending, d.fx.operations.ops[d.fx.second].State, "the operation row is not written over a partly settled task set")
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

// refusedTasklessDrive is a VSchema-only operation's drive against a data
// plane that refused its dispatch because the apply had been cancelled.
type refusedTasklessDrive struct {
	client      *GRPCClient
	server      *capturingTernServer
	apply       *storage.Apply
	operationID int64
	tasks       *mockTaskStore
	logs        *mockApplyLogStore
	operations  *mockApplyOperationStore
}

// newRefusedTasklessDrive builds a VSchema-only change with one work
// operation and no task rows, against a data plane that refuses its dispatch
// because the apply it would join has been cancelled.
func newRefusedTasklessDrive(t *testing.T) refusedTasklessDrive {
	t.Helper()
	server := &capturingTernServer{applyResponse: &ternv1.ApplyResponse{
		Accepted:         false,
		ErrorMessage:     closedApplyRefusal,
		ClosedApplyState: state.Apply.Cancelled,
	}}
	client, cleanup := testCapturingGRPCClient(t, server)
	t.Cleanup(cleanup)

	apply := &storage.Apply{
		ID:              9,
		ApplyIdentifier: "apply-vschema-only",
		PlanID:          77,
		Database:        "commerce",
		DatabaseType:    storage.DatabaseTypeVitess,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "commerce-target"})
	operationID := int64(61)
	vschema := `{"sharded":true,"vindexes":{"xxhash":{"type":"xxhash"}}}`
	d := refusedTasklessDrive{
		client:      client,
		server:      server,
		apply:       apply,
		operationID: operationID,
		tasks:       &mockTaskStore{},
		logs:        &mockApplyLogStore{},
		operations: &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
			operationID: {
				ID:            operationID,
				ApplyID:       apply.ID,
				Deployment:    "commerce-deployment",
				OperationKind: storage.ApplyOperationKindWork,
				State:         state.ApplyOperation.Running,
			},
		}},
	}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   d.tasks,
		logs:    d.logs,
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-vschema-only",
			SchemaFiles:    schema.SchemaFiles{"commerce": {Files: map[string]string{storage.VSchemaArtifactName: vschema}}},
			Namespaces:     map[string]*storage.NamespacePlanData{"commerce": {Artifacts: map[string]string{storage.VSchemaArtifactName: vschema}}},
		}},
		operations: d.operations,
	}
	return d
}

// drive runs one claim of the operation under its own lease.
func (d refusedTasklessDrive) drive(t *testing.T) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	ctx = storage.WithOperationLease(ctx, storage.OperationLease{
		ApplyID:     d.apply.ID,
		OperationID: d.operationID,
		Owner:       "host/1/driver-0",
		Token:       "operation-token",
	})
	return d.client.ResumeApplyOperation(ctx, d.apply, d.operationID)
}

// assertSettledCancelled checks the operation's row settled cancelled with
// one apply log entry, and that nothing else was written.
func (d refusedTasklessDrive) assertSettledCancelled(t *testing.T) {
	t.Helper()
	op := d.operations.ops[d.operationID]
	assert.Equal(t, state.ApplyOperation.Cancelled, op.State)
	require.NotNil(t, op.CompletedAt)
	assert.Empty(t, d.tasks.updated, "a taskless operation has no task to settle")
	assert.Equal(t, state.Apply.Running, d.apply.State, "an operation-scoped drive never writes the parent apply")
	assert.Nil(t, d.server.getProgressRequest(), "a refused operation is never polled")
	require.Len(t, d.logs.logs, 1)
	assert.Equal(t, "Remote apply operation 61 (deployment commerce-deployment) cancelled before it started: the apply had already ended (cancelled)", d.logs.logs[0].Message)
	assert.Empty(t, d.logs.logs[0].OldState, "the apply-scoped entry names no transition the apply did not make")
	assert.Empty(t, d.logs.logs[0].NewState)
}

// A VSchema-only change has one work operation and no task rows. The operator
// cancels the apply while that operation is still being dispatched, and the
// data plane refuses it. The operator drives the operation under its own
// lease. The operation never started, so its own row settles cancelled: with
// no task to derive it from, a row left running would hold the apply open
// forever.
func TestGRPCClient_TasklessOperationRefusedByACancelledApplySettlesItsRow(t *testing.T) {
	d := newRefusedTasklessDrive(t)
	require.NoError(t, d.drive(t))
	require.NotNil(t, d.server.getApplyRequest(), "the operation reached the data plane before it was refused")
	d.assertSettledCancelled(t)
}

// The same VSchema-only operation is refused, and storage refuses the write
// that settles its row. The drive returns an error that names the operation,
// the row stays running and claimable, and no apply log entry claims a
// settlement that did not land. The next claim is refused again, and this
// time its row settles cancelled.
func TestGRPCClient_TasklessOperationRefusedByACancelledApplyRetriesItsRowWrite(t *testing.T) {
	d := newRefusedTasklessDrive(t)
	d.operations.markTerminalErr = errors.New("storage unavailable")

	err := d.drive(t)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "apply_operation 61")
	assert.Contains(t, err.Error(), "storage unavailable")
	op := d.operations.ops[d.operationID]
	assert.Equal(t, state.ApplyOperation.Running, op.State, "the row stays claimable for the next drive")
	assert.Nil(t, op.CompletedAt)
	assert.Empty(t, d.logs.logs, "no log entry claims a settlement storage refused")

	d.operations.markTerminalErr = nil
	require.NoError(t, d.drive(t))
	d.assertSettledCancelled(t)
}

// A whole-apply drive has no siblings whose outcome a refused dispatch could
// take, so a refusal that names a closed state still fails the apply closed
// with the data plane's reason, and an operator looks at the target.
func TestGRPCClient_WholeApplyRefusedByAClosedApplyFailsClosed(t *testing.T) {
	server := &capturingTernServer{applyResponse: &ternv1.ApplyResponse{
		Accepted:         false,
		ErrorMessage:     closedApplyRefusal,
		ClosedApplyState: state.Apply.Cancelled,
	}}
	client, cleanup := testCapturingGRPCClient(t, server)
	t.Cleanup(cleanup)

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-whole",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "testdb-target"})
	tasks := &mockTaskStore{tasks: []*storage.Task{{
		ID:             11,
		TaskIdentifier: "task-users",
		ApplyID:        apply.ID,
		TableName:      "users",
		DDL:            "ALTER TABLE users ADD COLUMN email varchar(255)",
		DDLAction:      "alter",
		Namespace:      "default",
		State:          state.Task.Pending,
	}}}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   tasks,
		plans:   &mockPlanStore{plan: &storage.Plan{ID: apply.PlanID, PlanIdentifier: "plan-whole"}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApply(ctx, apply)
	require.Error(t, err)
	assert.Contains(t, err.Error(), closedApplyRefusal)
	assert.Equal(t, state.Apply.Failed, apply.State, "the refusal fails the apply closed rather than settling it cancelled")
	require.NotEmpty(t, tasks.updated)
	failed := tasks.updated[len(tasks.updated)-1]
	assert.Equal(t, "task-users", failed.TaskIdentifier)
	assert.Equal(t, state.Task.Failed, failed.State)
}
