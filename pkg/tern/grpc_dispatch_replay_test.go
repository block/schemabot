package tern

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// failingPlanStore answers every plan read with an error, standing in for a
// storage blip on the control plane.
type failingPlanStore struct {
	storage.PlanStore
}

func (failingPlanStore) GetByID(context.Context, int64) (*storage.Plan, error) {
	return nil, errors.New("plan storage unavailable")
}

// failingPlanReadsStorage is mockStorage with every plan read failing.
type failingPlanReadsStorage struct {
	*mockStorage
}

func (failingPlanReadsStorage) Plans() storage.PlanStore {
	return failingPlanStore{}
}

// lostDispatchFixture is a whole apply recorded running with no remote apply
// id: a driver sent the dispatch and stopped before the returned id reached
// storage.
type lostDispatchFixture struct {
	apply           *storage.Apply
	task            *storage.Task
	controlRequests *testControlRequestStore
	storage         *mockStorage
}

func newLostDispatchFixture(applyState string, requests ...storage.ControlOperation) *lostDispatchFixture {
	apply := &storage.Apply{
		ID:              21,
		ApplyIdentifier: "apply-lost-dispatch",
		PlanID:          210,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           applyState,
		Attempt:         2,
	}
	task := &storage.Task{
		ID:             22,
		TaskIdentifier: "task-lost-dispatch",
		ApplyID:        apply.ID,
		TableName:      "users",
		State:          state.Task.Running,
	}
	controlRequests := &testControlRequestStore{}
	for _, op := range requests {
		controlRequests.requests = append(controlRequests.requests, &storage.ApplyControlRequest{
			ApplyID:     apply.ID,
			Operation:   op,
			Status:      storage.ControlRequestPending,
			RequestedBy: "cli:alice",
		})
	}
	return &lostDispatchFixture{
		apply:           apply,
		task:            task,
		controlRequests: controlRequests,
		storage: &mockStorage{
			applies:         &mockApplyStore{apply: apply},
			tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
			logs:            &mockApplyLogStore{},
			controlRequests: controlRequests,
			plans: &mockPlanStore{plan: &storage.Plan{
				ID:             apply.PlanID,
				PlanIdentifier: "plan-lost-dispatch",
			}},
		},
	}
}

func resumeLostDispatch(t *testing.T, client *GRPCClient, apply *storage.Apply) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	return client.ResumeApply(ctx, apply)
}

func TestGRPCClient_LostDispatchReplayAdoptsTheRemoteApplyTheDataPlaneReturns(t *testing.T) {
	// A driver dispatched an apply and died before storing the remote apply id.
	// The next drive re-sends the dispatch under the key the lost one carried,
	// so the data plane returns the remote apply it already created. The drive
	// stores that id and drives the apply to completion instead of failing it
	// while the change keeps running on the target.
	server := &capturingTernServer{remoteApplyID: "remote-original"}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	fixture := newLostDispatchFixture(state.Apply.Running)
	client.storage = fixture.storage
	wantKey := remoteApplyIdempotencyKey(fixture.apply, wholeApplyTaskScope())

	require.NoError(t, resumeLostDispatch(t, client, fixture.apply))

	req := server.getApplyRequest()
	require.NotNil(t, req, "the drive must re-send the dispatch")
	assert.Equal(t, wantKey, req.IdempotencyKey, "the re-sent dispatch must carry the key the lost dispatch used")
	assert.Equal(t, "plan-lost-dispatch", req.PlanId)
	assert.Equal(t, 2, fixture.apply.Attempt, "a replay must not charge an attempt, which would rotate the key")
	assert.Equal(t, "remote-original", fixture.apply.ExternalID)
	assert.Equal(t, state.Apply.Completed, fixture.apply.State)
	assert.Empty(t, fixture.apply.ErrorMessage)
}

func TestGRPCClient_LostDispatchReplayResumesWorkStoppedBeforeItsFirstDispatch(t *testing.T) {
	// A stop caught a whole apply before its first dispatch and recorded it
	// stopped in storage alone; a start then claimed it into resuming. Resuming
	// with no remote apply id is dispatched under the apply's unchanged key, so
	// the start takes effect instead of failing the apply.
	server := &capturingTernServer{remoteApplyID: "remote-after-start"}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	fixture := newLostDispatchFixture(state.Apply.Resuming)
	client.storage = fixture.storage

	require.NoError(t, resumeLostDispatch(t, client, fixture.apply))

	req := server.getApplyRequest()
	require.NotNil(t, req, "a resumed apply with no remote apply id must be dispatched")
	assert.Equal(t, remoteApplyIdempotencyKey(fixture.apply, wholeApplyTaskScope()), req.IdempotencyKey)
	assert.Equal(t, "remote-after-start", fixture.apply.ExternalID)
	assert.Equal(t, state.Apply.Completed, fixture.apply.State)
}

func TestGRPCClient_LostDispatchReplayWithoutAVerdictLeavesTheWorkUntouched(t *testing.T) {
	// A re-sent dispatch that gets no verdict proves nothing about the remote
	// apply an earlier dispatch may have created. The drive leaves the apply,
	// its tasks, and the operator's pending cancel exactly as they were, so the
	// next claim re-sends the same key. Recording failed_retryable instead would
	// charge an attempt on the next claim, rotate the key, and start a second
	// remote apply for the same change.
	testCases := []struct {
		name          string
		applyErr      error
		applyResponse *ternv1.ApplyResponse
		failPlanReads bool
		wantSent      bool
		wantError     string
	}{
		{
			name:          "rejection without a named conflict",
			applyResponse: &ternv1.ApplyResponse{Accepted: false, ErrorMessage: "read active tasks: storage unavailable"},
			wantSent:      true,
			wantError:     "read active tasks: storage unavailable",
		},
		{
			name:          "accepted without a remote apply id",
			applyResponse: &ternv1.ApplyResponse{Accepted: true},
			wantSent:      true,
			wantError:     "remote apply accepted without apply_id",
		},
		{
			name:      "retryable rejection",
			applyErr:  status.Error(codes.Unavailable, "data plane restarting"),
			wantSent:  true,
			wantError: "data plane restarting",
		},
		{
			name:      "ambiguous outcome",
			applyErr:  status.Error(codes.DeadlineExceeded, "deadline waiting for response"),
			wantSent:  true,
			wantError: "ambiguous remote dispatch outcome",
		},
		{
			name:          "control-plane plan read failure",
			failPlanReads: true,
			wantError:     "plan storage unavailable",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := &capturingTernServer{applyErr: tc.applyErr, applyResponse: tc.applyResponse}
			client, cleanup := testCapturingGRPCClient(t, server)
			defer cleanup()
			fixture := newLostDispatchFixture(state.Apply.Running, storage.ControlOperationCancel)
			client.storage = fixture.storage
			if tc.failPlanReads {
				client.storage = failingPlanReadsStorage{fixture.storage}
			}

			err := resumeLostDispatch(t, client, fixture.apply)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantError)
			assert.ErrorIs(t, err, ErrRemoteDispatchUnresolved, "the drive loop counts a replay without a verdict by this error")

			if tc.wantSent {
				assert.NotNil(t, server.getApplyRequest(), "the dispatch must have been re-sent")
			} else {
				assert.Nil(t, server.getApplyRequest(), "nothing is sent when the plan cannot be read")
			}
			assert.Equal(t, state.Apply.Running, fixture.apply.State)
			assert.Equal(t, 2, fixture.apply.Attempt)
			assert.Empty(t, fixture.apply.ExternalID)
			assert.Empty(t, fixture.apply.ErrorMessage)
			assert.Equal(t, state.Task.Running, fixture.task.State)
			assert.Empty(t, fixture.task.ErrorMessage)

			cancelReq, err := fixture.controlRequests.GetPending(t.Context(), fixture.apply.ID, storage.ControlOperationCancel)
			require.NoError(t, err)
			assert.NotNil(t, cancelReq, "the cancel must stay pending for the drive that recovers the remote apply")
		})
	}
}

func TestGRPCClient_LostDispatchReplayRefusedFailsClosedWithPendingControlRequests(t *testing.T) {
	// A re-sent dispatch the data plane refuses outright can never succeed as
	// sent, so the apply fails closed. A failed apply is never claimed again, so
	// the operator's pending stop and cancel fail with it rather than waiting
	// forever on a drive that will not come. A named conflict is such a
	// refusal: the data plane resolves the idempotency key before it checks
	// for conflicts, so the conflict proves no remote apply carries the key.
	testCases := []struct {
		name          string
		applyErr      error
		applyResponse *ternv1.ApplyResponse
		wantError     string
	}{
		{
			name:      "permanent rejection",
			applyErr:  status.Error(codes.InvalidArgument, "plan no longer matches the target"),
			wantError: "plan no longer matches the target",
		},
		{
			name: "named conflict",
			applyResponse: &ternv1.ApplyResponse{
				Accepted:     false,
				ErrorMessage: "another schema change is running on users",
				Conflict:     &ternv1.ApplyConflict{Table: "users", BlockingState: "running"},
			},
			wantError: "Table users is held by a schema change",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := &capturingTernServer{applyErr: tc.applyErr, applyResponse: tc.applyResponse}
			client, cleanup := testCapturingGRPCClient(t, server)
			defer cleanup()
			fixture := newLostDispatchFixture(state.Apply.Running, storage.ControlOperationCancel, storage.ControlOperationStop)
			client.storage = fixture.storage

			err := resumeLostDispatch(t, client, fixture.apply)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantError)
			assert.NotErrorIs(t, err, ErrRemoteDispatchUnresolved, "a refusal is a verdict")

			assert.Equal(t, state.Apply.Failed, fixture.apply.State)
			assert.Contains(t, fixture.apply.ErrorMessage, tc.wantError)
			assert.Empty(t, fixture.apply.ExternalID)
			assert.Equal(t, state.Task.Failed, fixture.task.State)
			for _, op := range []storage.ControlOperation{storage.ControlOperationCancel, storage.ControlOperationStop} {
				req, err := fixture.controlRequests.GetByOperation(t.Context(), fixture.apply.ID, op)
				require.NoError(t, err)
				require.NotNilf(t, req, "the %s request must exist", op)
				assert.Equalf(t, storage.ControlRequestFailed, req.Status, "the refused replay must fail the pending %s", op)
				assert.Contains(t, req.ErrorMessage, tc.wantError)
			}
		})
	}
}

func TestGRPCClient_LostDispatchReplayDeliversAPendingStopBeforePolling(t *testing.T) {
	// The operator stopped an apply whose remote apply id never reached
	// storage. The replay recovers the id of the remote apply that is still
	// copying, and the drive hands it the stop at once rather than waiting for
	// the first progress poll, so the change halts as soon as the drive can
	// address it.
	// The remote apply reports running until the stop lands, then stopped.
	server := &capturingTernServer{
		remoteApplyID:    "remote-still-copying",
		progressStates:   []ternv1.State{ternv1.State_STATE_RUNNING},
		progressState:    ternv1.State_STATE_STOPPED,
		progressStateSet: true,
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	fixture := newLostDispatchFixture(state.Apply.Running, storage.ControlOperationStop)
	client.storage = fixture.storage

	require.NoError(t, resumeLostDispatch(t, client, fixture.apply))

	assert.Equal(t, "remote-still-copying", fixture.apply.ExternalID)
	assert.Equal(t, "remote-still-copying", server.getStopApplyID(), "the stop must reach the recovered remote apply")
	assert.Zero(t, server.getProgressCallsAtFirstStop(), "the stop must be delivered before any progress poll")
	assert.Equal(t, state.Apply.Stopped, fixture.apply.State)
	stopReq, err := fixture.controlRequests.GetByOperation(t.Context(), fixture.apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	require.NotNil(t, stopReq)
	assert.Equal(t, storage.ControlRequestCompleted, stopReq.Status)
}

func TestGRPCClient_LostDispatchReplayCompletesAPendingStart(t *testing.T) {
	// A stop caught a whole apply before its first dispatch, and a start then
	// claimed it into resuming with its tasks still stopped. The replay
	// dispatches the work, and the drive then reconciles the start the way it
	// does for any resumed apply: the data plane already reports the work
	// running, so no remote start is sent, the stopped tasks are requeued, and
	// the start request completes before the drive settles into polling.
	server := &capturingTernServer{
		remoteApplyID:    "remote-after-start",
		progressState:    ternv1.State_STATE_RUNNING,
		progressStateSet: true,
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	fixture := newLostDispatchFixture(state.Apply.Resuming, storage.ControlOperationStart)
	fixture.task.State = state.Task.Stopped
	client.storage = fixture.storage

	// The remote apply keeps running, so the drive polls until the deadline.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.Error(t, client.ResumeApply(ctx, fixture.apply))

	assert.Equal(t, "remote-after-start", fixture.apply.ExternalID)
	assert.False(t, server.startWasCalled(), "the data plane already runs the work, so no remote start is sent")
	assert.Equal(t, state.Apply.Running, fixture.apply.State)
	assert.NotEqual(t, state.Task.Stopped, fixture.task.State, "the stopped task must be requeued for the start")
	startReq, err := fixture.controlRequests.GetByOperation(t.Context(), fixture.apply.ID, storage.ControlOperationStart)
	require.NoError(t, err)
	require.NotNil(t, startReq)
	assert.Equal(t, storage.ControlRequestCompleted, startReq.Status)
}
