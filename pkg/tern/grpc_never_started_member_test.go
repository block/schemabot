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

// A rollout halts on failure. payments-002 failed on the data plane while the
// control plane was dispatching payments-003, so the data plane settled the
// apply as failed before payments-003 began. Polled scoped to its operation,
// payments-003 answers pending beside the failed apply, and the drive ends
// with ErrRemoteOperationNeverStarts instead of polling a member that will
// never start.
func TestGRPCClient_PollEndsForAMemberItsRemoteApplySettledBefore(t *testing.T) {
	server := &capturingTernServer{
		progressState:      ternv1.State_STATE_PENDING,
		progressStateSet:   true,
		progressApplyState: ternv1.State_STATE_FAILED,
		progressTables: []*ternv1.TableProgress{{
			Namespace: "payments",
			TableName: "users",
			Status:    state.Task.Pending,
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
	assert.Equal(t, "32", server.progressReq.GetApplyOperationId(), "the drive polled its own operation")
	assert.Equal(t, state.Task.Pending, task.State, "the member's task stays pending: it never ran")
}

// Only a pending member of a settled apply is one the data plane will never
// start. A member that is running, an apply still running or paused for a
// retry, a stopped apply that can be started again, a data plane that does not
// report the apply's state, and an answer for the whole apply all keep the
// drive polling.
func TestRemoteOperationNeverStarts(t *testing.T) {
	for _, tc := range []struct {
		name       string
		resp       *ternv1.ProgressResponse
		neverStart bool
	}{
		{name: "pending in a failed apply", resp: &ternv1.ProgressResponse{ApplyOperationId: "32", State: ternv1.State_STATE_PENDING, ApplyState: ternv1.State_STATE_FAILED}, neverStart: true},
		{name: "pending in a cancelled apply", resp: &ternv1.ProgressResponse{ApplyOperationId: "32", State: ternv1.State_STATE_PENDING, ApplyState: ternv1.State_STATE_CANCELLED}, neverStart: true},
		{name: "pending in a running apply", resp: &ternv1.ProgressResponse{ApplyOperationId: "32", State: ternv1.State_STATE_PENDING, ApplyState: ternv1.State_STATE_RUNNING}},
		{name: "pending in an apply paused for a retry", resp: &ternv1.ProgressResponse{ApplyOperationId: "32", State: ternv1.State_STATE_PENDING, ApplyState: ternv1.State_STATE_FAILED_RETRYABLE}},
		{name: "pending in a stopped apply", resp: &ternv1.ProgressResponse{ApplyOperationId: "32", State: ternv1.State_STATE_PENDING, ApplyState: ternv1.State_STATE_STOPPED}},
		{name: "running in a failed apply", resp: &ternv1.ProgressResponse{ApplyOperationId: "32", State: ternv1.State_STATE_RUNNING, ApplyState: ternv1.State_STATE_FAILED}},
		{name: "apply state not reported", resp: &ternv1.ProgressResponse{ApplyOperationId: "32", State: ternv1.State_STATE_PENDING}},
		{name: "answer for the whole apply", resp: &ternv1.ProgressResponse{State: ternv1.State_STATE_PENDING, ApplyState: ternv1.State_STATE_FAILED}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.neverStart, remoteOperationNeverStarts(tc.resp))
		})
	}
}
