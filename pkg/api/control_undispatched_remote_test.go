package api

import (
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// sendControlRequest sends one control request through the routed handler.
func sendControlRequest(t *testing.T, svc *Service, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	req := httptest.NewRequestWithContext(t.Context(), "POST", path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

// newRemoteStopTestService builds a service for one running apply with one
// running task, the given operation rows, and the stores a stop test reads.
func newRemoteStopTestService(client tern.Client, apply *storage.Apply, operations storage.ApplyOperationStore) (*Service, *controlTestStores) {
	stores := &controlTestStores{
		controls:  &memoryControlRequestStore{},
		applyLogs: &capturingApplyLogStore{},
	}
	task := &storage.Task{ID: 31, TaskIdentifier: "task-" + apply.ApplyIdentifier, ApplyID: apply.ID, State: state.Task.Running}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{
		applies:    &staticApplyStore{apply: apply},
		tasks:      &capturingTaskStore{tasks: []*storage.Task{task}},
		controls:   stores.controls,
		applyLogs:  stores.applyLogs,
		operations: operations,
	}, testServerConfig(), map[string]tern.Client{"default/staging": client}, logger)
	return svc, stores
}

// listenForOperatorWake makes the service behave as if its operator were
// running, and returns the channel a wake lands on.
func listenForOperatorWake(svc *Service) <-chan struct{} {
	wake := make(chan struct{}, 1)
	svc.operatorMu.Lock()
	svc.stopRecovery = make(chan struct{})
	svc.operatorWake = wake
	svc.operatorMu.Unlock()
	return wake
}

// An apply that has not been sent to remote Tern yet has no remote apply ID:
// remote Tern names an apply by the ID it returns at dispatch, and SchemaBot's
// own apply identifier is unknown there. The immediate attempt has nothing to
// address, so it is skipped and the durable request carries the control.
func TestImmediateControlSkippedForUndispatchedRemoteApply(t *testing.T) {
	t.Run("cancel records the durable request without calling remote Tern", func(t *testing.T) {
		mock := &mockTernClient{isRemote: true, cancelResp: &ternv1.CancelResponse{Accepted: true}}
		apply := activeTestApply("apply-remote-cancel-undispatched")
		apply.State = state.Apply.Pending
		svc := newControlTestService(mock, apply)

		w := sendControlRequest(t, svc, "/api/cancel",
			`{"environment": "staging", "apply_id": "apply-remote-cancel-undispatched", "caller": "cli:remote"}`)

		assert.Less(t, w.Code, http.StatusBadRequest, w.Body.String())
		assert.Nil(t, mock.cancelReq, "no remote apply exists to cancel, so no cancel RPC is sent")
		pending, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
		require.NoError(t, err)
		assert.NotNil(t, pending, "the durable cancel request stays queued for the apply owner")
	})

	t.Run("stop records the durable request and says why no immediate stop ran", func(t *testing.T) {
		mock := &mockTernClient{isRemote: true, stopResp: &ternv1.StopResponse{Accepted: true}}
		apply := activeTestApply("apply-remote-stop-undispatched")
		task := &storage.Task{
			ID:             31,
			TaskIdentifier: "task-remote-stop-undispatched",
			ApplyID:        apply.ID,
			State:          state.Task.Running,
		}
		svc, stores := newControlTestServiceWithStores(mock, apply, []*storage.Task{task})

		w := sendControlRequest(t, svc, "/api/stop",
			`{"environment": "staging", "apply_id": "apply-remote-stop-undispatched", "caller": "cli:remote"}`)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		assert.Nil(t, mock.stopReq, "no remote apply exists to stop, so no stop RPC is sent")
		pending, err := stores.controls.GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
		require.NoError(t, err)
		assert.NotNil(t, pending, "the durable stop request stays queued for the apply owner")
		assert.True(t, hasApplyLogMessageContaining(stores.applyLogs.logs, "has not been sent to the engine yet"),
			"the apply log tells the operator why no immediate stop ran")
		assert.False(t, hasApplyLogMessageContaining(stores.applyLogs.logs, "Immediate stop attempt failed"),
			"a skipped attempt is not reported as a failed one")
	})

	// A drive under the operation lease records the remote apply ID on its
	// operation row and leaves the apply row empty. Such an apply has been sent,
	// so the apply log must not say otherwise, and the drive that owns the
	// operation delivers the stop with the ID it resolves.
	t.Run("stop on an apply whose operation holds the remote ID is left to that operation's drive", func(t *testing.T) {
		mock := &mockTernClient{isRemote: true, stopResp: &ternv1.StopResponse{Accepted: true}}
		apply := activeTestApply("apply-remote-stop-operation-id")
		operation := &storage.ApplyOperation{
			ID:           7,
			ApplyID:      apply.ID,
			Deployment:   DefaultDeployment,
			OperationKey: "testdb",
			ExternalID:   "remote-apply-from-operation",
			State:        state.ApplyOperation.Running,
		}
		svc, stores := newRemoteStopTestService(mock, apply, &staticApplyOperationStore{operations: []*storage.ApplyOperation{operation}})

		w := sendControlRequest(t, svc, "/api/stop",
			`{"environment": "staging", "apply_id": "apply-remote-stop-operation-id", "caller": "cli:remote"}`)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		assert.Nil(t, mock.stopReq, "the handler holds no remote apply ID of its own, so it sends no stop RPC")
		pending, err := stores.controls.GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
		require.NoError(t, err)
		assert.NotNil(t, pending, "the durable stop request stays queued for the operation's drive")
		assert.True(t, hasApplyLogMessageContaining(stores.applyLogs.logs, "Stop queued; the apply owner delivers the durable stop request to the engine"),
			"the apply log says the stop is on its way to the engine")
		assert.False(t, hasApplyLogMessageContaining(stores.applyLogs.logs, "has not been sent to the engine yet"),
			"an apply that is running remotely is not reported as unsent")
	})

	t.Run("stop says nothing about dispatch when the operation rows cannot be read", func(t *testing.T) {
		mock := &mockTernClient{isRemote: true, stopResp: &ternv1.StopResponse{Accepted: true}}
		apply := activeTestApply("apply-remote-stop-operations-unreadable")
		svc, stores := newRemoteStopTestService(mock, apply, &staticApplyOperationStore{err: errors.New("storage unavailable")})

		w := sendControlRequest(t, svc, "/api/stop",
			`{"environment": "staging", "apply_id": "apply-remote-stop-operations-unreadable", "caller": "cli:remote"}`)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		assert.Nil(t, mock.stopReq, "SchemaBot's own apply identifier is never sent to remote Tern in place of a remote apply ID")
		pending, err := stores.controls.GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
		require.NoError(t, err)
		assert.NotNil(t, pending, "the durable stop request stays queued for the apply owner")
		assert.True(t, hasApplyLogMessageContaining(stores.applyLogs.logs, "Stop queued; the apply owner completes the durable stop request"),
			"the apply log says the stop is queued")
		assert.False(t, hasApplyLogMessageContaining(stores.applyLogs.logs, "has not been sent to the engine yet"),
			"dispatch state that could not be read is not asserted")
		assert.False(t, hasApplyLogMessageContaining(stores.applyLogs.logs, "storage unavailable"),
			"the storage error stays in the server log")
	})

	t.Run("cancel on an apply whose operation holds the remote ID is left to that operation's drive", func(t *testing.T) {
		mock := &mockTernClient{isRemote: true, cancelResp: &ternv1.CancelResponse{Accepted: true}}
		apply := activeTestApply("apply-remote-cancel-operation-id")
		operation := &storage.ApplyOperation{
			ID:           7,
			ApplyID:      apply.ID,
			Deployment:   DefaultDeployment,
			OperationKey: "testdb",
			ExternalID:   "remote-apply-from-operation",
			State:        state.ApplyOperation.Running,
		}
		svc := newControlTestServiceWithOperations(mock, apply, nil, []*storage.ApplyOperation{operation})

		w := sendControlRequest(t, svc, "/api/cancel",
			`{"environment": "staging", "apply_id": "apply-remote-cancel-operation-id", "caller": "cli:remote"}`)

		assert.Less(t, w.Code, http.StatusBadRequest, w.Body.String())
		assert.Nil(t, mock.cancelReq, "the handler holds no remote apply ID of its own, so it sends no cancel RPC")
		pending, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
		require.NoError(t, err)
		assert.NotNil(t, pending, "the durable cancel request stays queued for the operation's drive")
	})

	t.Run("a dispatched remote apply is still cancelled by its remote ID", func(t *testing.T) {
		mock := &mockTernClient{isRemote: true, cancelResp: &ternv1.CancelResponse{Accepted: true}}
		apply := activeTestApply("apply-remote-cancel-dispatched")
		apply.ExternalID = "remote-apply-cancel"
		svc := newControlTestService(mock, apply)

		w := sendControlRequest(t, svc, "/api/cancel",
			`{"environment": "staging", "apply_id": "apply-remote-cancel-dispatched", "caller": "cli:remote"}`)

		assert.Less(t, w.Code, http.StatusBadRequest, w.Body.String())
		require.NotNil(t, mock.cancelReq)
		assert.Equal(t, "remote-apply-cancel", mock.cancelReq.ApplyId)
	})
}

// A data-plane client that answers a revert or skip-revert with neither a
// response nor an error leaves the outcome unknown. The handler treats it as a
// failed immediate attempt: the durable request stays pending for the apply
// owner, rather than the handler dereferencing the missing response.
func TestImmediateRevertControlsTolerateMissingResponse(t *testing.T) {
	revertWindowApply := func(applyID string) *storage.Apply {
		apply := activeTestApply(applyID)
		apply.State = state.Apply.RevertWindow
		apply.Engine = storage.EnginePlanetScale
		apply.DatabaseType = storage.DatabaseTypeVitess
		return apply
	}

	t.Run("revert", func(t *testing.T) {
		mock := &mockTernClient{}
		apply := revertWindowApply("apply-revert-nil-response")
		svc := newControlTestService(mock, apply)
		wake := listenForOperatorWake(svc)

		w := sendControlRequest(t, svc, "/api/revert", `{"environment": "staging", "apply_id": "apply-revert-nil-response"}`)

		assert.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
		require.NotNil(t, mock.revertReq, "the immediate revert was attempted")
		assert.Len(t, wake, 1, "the apply owner is woken to retry the revert now, not on its next poll")
		pending, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationRevert)
		require.NoError(t, err)
		assert.NotNil(t, pending, "the durable revert request stays queued for the apply owner")
	})

	t.Run("skip-revert", func(t *testing.T) {
		mock := &mockTernClient{}
		apply := revertWindowApply("apply-skip-revert-nil-response")
		svc := newControlTestService(mock, apply)
		wake := listenForOperatorWake(svc)

		w := sendControlRequest(t, svc, "/api/skip-revert", `{"environment": "staging", "apply_id": "apply-skip-revert-nil-response"}`)

		assert.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
		require.NotNil(t, mock.skipRevertReq, "the immediate skip-revert was attempted")
		assert.Len(t, wake, 1, "the apply owner is woken to retry the skip-revert now, not on its next poll")
		pending, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationSkipRevert)
		require.NoError(t, err)
		assert.NotNil(t, pending, "the durable skip-revert request stays queued for the apply owner")
	})
}
