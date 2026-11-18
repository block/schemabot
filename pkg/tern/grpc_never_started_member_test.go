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

// A rollout halts on failure on the data plane. payments-002 failed there
// while the control plane was dispatching payments-003, so the data plane
// settled the apply as failed before payments-003 began and reports that it
// will never start. On the same poll it settles a revert an operator asked
// for as rejected. Polled scoped to its operation, payments-003's drive
// records the rejection, fails the member's task with the reason it never
// ran, and ends with ErrRemoteOperationNeverStarts instead of polling it.
func TestGRPCClient_PollEndsForAMemberItsRemoteApplySettledBefore(t *testing.T) {
	server := &capturingTernServer{
		progressState:       ternv1.State_STATE_PENDING,
		progressStateSet:    true,
		progressApplyState:  ternv1.State_STATE_FAILED,
		progressNeverStarts: true,
		progressTables: []*ternv1.TableProgress{{
			Namespace: "payments",
			TableName: "users",
			Status:    state.Task.Pending,
		}},
		progressSettled: []*ternv1.SettledControlRequest{{
			Operation:    string(storage.ControlOperationRevert),
			Status:       string(storage.ControlRequestFailed),
			ErrorMessage: "deploy request is outside its revert window",
			RequestedBy:  "cli:alice",
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-halted-rollout",
		Database:        "payments",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	operationID := int64(42)
	task := &storage.Task{
		ID:               11,
		TaskIdentifier:   "task-users",
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		Namespace:        "payments",
		TableName:        "users",
		State:            state.Task.Pending,
	}
	operation := &storage.ApplyOperation{
		ID: operationID, ApplyID: apply.ID, Deployment: "default", Target: "payments-003",
		State: state.ApplyOperation.Running, ExternalID: "remote-apply-shared", ExternalOperationID: "32",
	}
	controlRequests := &testControlRequestStore{}
	storedApply := *apply
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
		operations:      &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{operationID: operation}},
	}
	scope := applyTaskScope{applyOperationID: operationID, operation: operation, multiOperation: true, memberTarget: "payments-003", operationLeaseOnly: true}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	err := client.pollForCompletion(ctx, apply, false, scope, false)

	require.ErrorIs(t, err, ErrRemoteOperationNeverStarts)
	assert.Equal(t, "32", server.progressReq.GetApplyOperationId(), "the drive polled its own operation")
	assert.Equal(t, state.Task.Failed, task.State, "the member's task carries the failure the operator derives the operation from")
	assert.Equal(t, "remote apply remote-apply-shared settled as failed before this operation started, so the data plane will not run it", task.ErrorMessage)
	assert.Equal(t, state.Apply.Running, storedApply.State, "an operation-scoped drive leaves the parent to the operator's projection")

	rejected, err := controlRequests.GetByOperation(t.Context(), apply.ID, storage.ControlOperationRevert)
	require.NoError(t, err)
	require.NotNil(t, rejected, "the rejection the data plane settled survives the drive that ends here")
	assert.Equal(t, storage.ControlRequestFailed, rejected.Status)
}

// A rollout on the data plane ended before reaching payments-003, and the
// state it ended in carries over to the member. An operator cancelling or
// reverting the rollout leaves payments-003's task cancelled or reverted, so
// the control plane's rollout derives the verdict the data plane did, rather
// than a failure nobody had. A failure fails it.
func TestGRPCClient_MemberItsRemoteApplySettledBeforeSettlesTheSameWay(t *testing.T) {
	for _, tc := range []struct {
		remote    ternv1.State
		taskState string
	}{
		{remote: ternv1.State_STATE_FAILED, taskState: state.Task.Failed},
		{remote: ternv1.State_STATE_CANCELLED, taskState: state.Task.Cancelled},
		{remote: ternv1.State_STATE_REVERTED, taskState: state.Task.Reverted},
	} {
		t.Run(tc.remote.String(), func(t *testing.T) {
			server := &capturingTernServer{
				progressState:       ternv1.State_STATE_PENDING,
				progressStateSet:    true,
				progressApplyState:  tc.remote,
				progressNeverStarts: true,
				progressTables:      []*ternv1.TableProgress{{Namespace: "payments", TableName: "users", Status: state.Task.Pending}},
			}
			client, cleanup := testCapturingGRPCClient(t, server)
			defer cleanup()

			apply := &storage.Apply{
				ID: 7, ApplyIdentifier: "apply-ended-rollout", Database: "payments",
				DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging", State: state.Apply.Running,
			}
			operationID := int64(42)
			task := &storage.Task{
				ID: 11, TaskIdentifier: "task-users", ApplyID: apply.ID, ApplyOperationID: &operationID,
				Namespace: "payments", TableName: "users", State: state.Task.Pending,
			}
			operation := &storage.ApplyOperation{
				ID: operationID, ApplyID: apply.ID, Deployment: "default", Target: "payments-003",
				State: state.ApplyOperation.Running, ExternalID: "remote-apply-shared", ExternalOperationID: "32",
			}
			storedApply := *apply
			client.storage = &mockStorage{
				applies:         &mockApplyStore{apply: &storedApply},
				tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
				logs:            &mockApplyLogStore{},
				controlRequests: &testControlRequestStore{},
				operations:      &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{operationID: operation}},
			}
			scope := applyTaskScope{applyOperationID: operationID, operation: operation, multiOperation: true, memberTarget: "payments-003", operationLeaseOnly: true}

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			err := client.pollForCompletion(ctx, apply, false, scope, false)

			require.ErrorIs(t, err, ErrRemoteOperationNeverStarts)
			assert.Equal(t, tc.taskState, task.State)
			assert.NotNil(t, task.CompletedAt, "the member's task is settled")
			assert.Equal(t, "remote apply remote-apply-shared settled as "+ProtoStateToStorage(tc.remote)+" before this operation started, so the data plane will not run it", task.ErrorMessage)
			assert.Equal(t, state.Apply.Running, storedApply.State, "an operation-scoped drive leaves the parent to the operator's projection")
		})
	}
}

// The data plane's verdict decides whether a member will never start. An
// answer scoped to the operation that carries it ends the drive. One without
// it keeps the drive polling, whatever the apply's state, since only the data
// plane can tell a pending member no driver has claimed from one whose drive
// has not yet advanced a task. So does an answer for the whole apply.
func TestRemoteOperationNeverStarts(t *testing.T) {
	for _, tc := range []struct {
		name       string
		resp       *ternv1.ProgressResponse
		neverStart bool
	}{
		{name: "scoped answer reporting it", resp: &ternv1.ProgressResponse{ApplyOperationId: "32", State: ternv1.State_STATE_PENDING, ApplyState: ternv1.State_STATE_FAILED, OperationNeverStarts: true}, neverStart: true},
		{name: "pending in a failed apply without the verdict", resp: &ternv1.ProgressResponse{ApplyOperationId: "32", State: ternv1.State_STATE_PENDING, ApplyState: ternv1.State_STATE_FAILED}},
		{name: "data plane that does not report it", resp: &ternv1.ProgressResponse{ApplyOperationId: "32", State: ternv1.State_STATE_PENDING}},
		{name: "answer for the whole apply", resp: &ternv1.ProgressResponse{State: ternv1.State_STATE_PENDING, ApplyState: ternv1.State_STATE_FAILED, OperationNeverStarts: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.neverStart, remoteOperationNeverStarts(tc.resp))
		})
	}
}
