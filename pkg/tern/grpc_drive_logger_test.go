package tern

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The gRPC drive binds the apply's identity to a drive-scoped logger at the
// drive boundary, so every line of the remote drive inherits apply_id,
// database, environment, repo, and pr without hand-listing them per call. A
// client constructed without a base logger falls back to slog.Default so the
// drive never panics on a nil logger.
func TestGRPCApplyLogger_BindsIdentity(t *testing.T) {
	t.Run("bound logger carries the apply identity", func(t *testing.T) {
		var records []capturedLog
		client := &GRPCClient{logger: slog.New(captureHandler{records: &records})}

		apply := driveLoggerTestApply(state.Apply.Running)
		client.applyLogger(apply).Warn("remote drive line")

		line := requireCapturedLog(t, records, "remote drive line")
		assertLogCarriesApplyIdentity(t, line)
	})

	t.Run("nil base logger falls back to the default logger", func(t *testing.T) {
		client := &GRPCClient{}
		apply := driveLoggerTestApply(state.Apply.Running)
		require.NotNil(t, client.applyLogger(apply))
	})
}

// A failed drive-lease heartbeat is logged through the identity-bound drive
// logger, so the warning an operator sees when a peer can reclaim the work
// carries the apply identity plus the current mutable state without
// duplicated identity keys.
func TestDriveEndingHeartbeatFailure_LogsCarryApplyIdentity(t *testing.T) {
	apply := driveLoggerTestApply(state.Apply.Running)
	apply.ExternalID = "remote-apply-9"
	var records []capturedLog
	logger := slog.New(captureHandler{records: &records}).With(apply.IdentityLogAttrs()...)

	hbErr := errors.New("connection refused")
	lastSuccess := time.Now().Add(-storage.ApplyLeaseStaleAfter)
	require.Error(t, driveEndingHeartbeatFailure(logger, apply, hbErr, lastSuccess))

	line := requireCapturedLog(t, records,
		"gRPC drive heartbeat has failed for the full lease staleness window; a peer driver can reclaim the work, so this owner will stop driving and writing apply state")
	assertLogCarriesApplyIdentity(t, line)
	assert.Equal(t, state.Apply.Running, line.attrs["state"])
	assert.Equal(t, "remote-apply-9", line.attrs["external_id"])
	assert.Equal(t, hbErr, line.attrs["error"])
}

// A retransmitted control request logs through the identity-bound drive
// logger: a fresh request re-sends at info, and one pending past the stale
// threshold escalates to warn — both carrying the apply identity and the
// request's operation, requester, and pending duration.
func TestLogRemoteControlResend_LogsCarryApplyIdentity(t *testing.T) {
	t.Run("fresh request re-sends at info", func(t *testing.T) {
		apply := driveLoggerTestApply(state.Apply.Running)
		var records []capturedLog
		logger := slog.New(captureHandler{records: &records}).With(apply.IdentityLogAttrs()...)

		now := time.Now()
		controlReq := &storage.ApplyControlRequest{
			Operation:   storage.ControlOperationStop,
			RequestedBy: "alice",
			CreatedAt:   now.Add(-remoteControlResendInterval),
		}
		logRemoteControlResend(t.Context(), logger, apply, controlReq, now)

		line := requireCapturedLog(t, records, "re-sent pending remote control request to the data plane")
		assert.Equal(t, slog.LevelInfo, line.level)
		assertLogCarriesApplyIdentity(t, line)
		assert.Equal(t, state.Apply.Running, line.attrs["state"])
		assert.Equal(t, string(storage.ControlOperationStop), line.attrs["operation"])
		assert.Equal(t, "alice", line.attrs["requested_by"])
	})

	t.Run("stale request escalates to warn", func(t *testing.T) {
		apply := driveLoggerTestApply(state.Apply.Running)
		var records []capturedLog
		logger := slog.New(captureHandler{records: &records}).With(apply.IdentityLogAttrs()...)

		now := time.Now()
		controlReq := &storage.ApplyControlRequest{
			Operation:   storage.ControlOperationCancel,
			RequestedBy: "alice",
			CreatedAt:   now.Add(-remoteControlStaleThreshold),
		}
		logRemoteControlResend(t.Context(), logger, apply, controlReq, now)

		line := requireCapturedLog(t, records,
			"remote control request accepted but still unconsumed by the data plane; driver keeps re-sending and polling — check data-plane logs for the failing consume")
		assert.Equal(t, slog.LevelWarn, line.level)
		assertLogCarriesApplyIdentity(t, line)
		assert.Equal(t, string(storage.ControlOperationCancel), line.attrs["operation"])
	})
}

// A skipped remote apply state transition logs through the identity-bound
// drive logger at a severity matching the skip reason — error when the stored
// row could not be reloaded, warn when it is missing, debug when it is already
// terminal — always carrying the apply identity and the transition context.
func TestLogSkippedRemoteApplyTransition_LogsCarryApplyIdentity(t *testing.T) {
	remoteApply := driveLoggerTestApply(state.Apply.Running)
	remoteApply.ExternalID = "remote-apply-9"
	storedApply := driveLoggerTestApply("completed")

	tests := []struct {
		name        string
		status      storedApplyTransitionStatus
		storedApply *storage.Apply
		err         error
		wantLevel   slog.Level
	}{
		{
			name:      "reload failure logs at error with the cause",
			status:    storedApplyTransitionReloadFailed,
			err:       errors.New("connection refused"),
			wantLevel: slog.LevelError,
		},
		{
			name:      "missing stored apply logs at warn",
			status:    storedApplyTransitionMissing,
			wantLevel: slog.LevelWarn,
		},
		{
			name:        "already-terminal stored apply logs at debug with its state",
			status:      storedApplyTransitionAlreadyTerminal,
			storedApply: storedApply,
			wantLevel:   slog.LevelDebug,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var records []capturedLog
			logger := slog.New(captureHandler{records: &records}).With(remoteApply.IdentityLogAttrs()...)

			logSkippedRemoteApplyTransition(t.Context(), logger, "persist remote terminal apply", remoteApply, tt.storedApply, tt.status, tt.err)

			line := requireCapturedLog(t, records, "skipping remote gRPC apply state transition")
			assert.Equal(t, tt.wantLevel, line.level)
			assertLogCarriesApplyIdentity(t, line)
			assert.Equal(t, "persist remote terminal apply", line.attrs["operation"])
			assert.Equal(t, "remote-apply-9", line.attrs["external_id"])
			if tt.err != nil {
				assert.Equal(t, tt.err, line.attrs["error"])
			}
			if tt.storedApply != nil {
				assert.Equal(t, tt.storedApply.State, line.attrs["stored_state"])
			}
		})
	}
}

// An operation-only drive that leaves a shared apply-level stop or cancel
// request for the operator projection logs through the identity-bound drive
// logger, carrying the apply identity plus the operation and remote apply id
// an operator needs to see which sibling deferred.
func TestLogOperationDriveLeavesParent_LogsCarryApplyIdentity(t *testing.T) {
	apply := driveLoggerTestApply(state.Apply.Running)
	scope := applyTaskScope{
		applyOperationID: 7,
		operation:        &storage.ApplyOperation{ID: 7, ExternalID: "remote-op-1"},
		multiOperation:   true,
	}

	t.Run("stop", func(t *testing.T) {
		var records []capturedLog
		logger := slog.New(captureHandler{records: &records}).With(apply.IdentityLogAttrs()...)

		logOperationDriveLeavesParentStop(logger, apply, scope)

		line := requireCapturedLog(t, records, "operation-only drive leaving apply-level stop request for operator projection")
		assertLogCarriesApplyIdentity(t, line)
		assert.Equal(t, int64(7), line.attrs["apply_operation_id"])
		assert.Equal(t, "remote-op-1", line.attrs["remote_apply_id"])
	})

	t.Run("cancel", func(t *testing.T) {
		var records []capturedLog
		logger := slog.New(captureHandler{records: &records}).With(apply.IdentityLogAttrs()...)

		logOperationDriveLeavesParentCancel(logger, apply, scope)

		line := requireCapturedLog(t, records, "operation-only drive leaving apply-level cancel request for operator projection")
		assertLogCarriesApplyIdentity(t, line)
		assert.Equal(t, int64(7), line.attrs["apply_operation_id"])
		assert.Equal(t, "remote-op-1", line.attrs["remote_apply_id"])
	})
}

// Settling an operator's cancel against one undispatched deployment of a
// fan-out is the line an operator pulls up when asking what happened to the
// command on that deployment, so it has to stay reachable by the `deployment`
// facet every other apply log carries. The apply's routing deployment and its
// state come from MutableLogAttrs; the operation's own deployment — which can
// differ from the apply row's — is named unambiguously as operation_deployment
// on both the per-task skip line and the settlement line.
func TestTerminalizeUndispatchedApplyOperation_LogsCarryBothDeployments(t *testing.T) {
	apply := driveLoggerTestApply(state.Apply.Running)
	apply.Deployment = "deploy-primary"
	opID := int64(7)
	op := &storage.ApplyOperation{ID: opID, ApplyID: apply.ID, Deployment: "deploy-eu", State: state.ApplyOperation.Pending}
	scope := applyTaskScope{applyOperationID: opID, operation: op, multiOperation: true}
	tasks := []*storage.Task{
		{ID: 1, TaskIdentifier: "task-done", ApplyID: apply.ID, ApplyOperationID: &opID, TableName: "orders", State: state.Task.Completed},
		{ID: 2, TaskIdentifier: "task-pending", ApplyID: apply.ID, ApplyOperationID: &opID, TableName: "users", State: state.Task.Pending},
	}

	var records []capturedLog
	client := &GRPCClient{
		logger: slog.New(captureHandler{records: &records}),
		storage: &mockStorage{
			applies:    &mockApplyStore{apply: apply},
			tasks:      &mockTaskStore{tasks: tasks},
			operations: &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{opID: op}},
			logs:       &mockApplyLogStore{},
		},
	}

	require.NoError(t, client.terminalizeUndispatchedApplyOperation(t.Context(), apply, "alice", scope, cancelUndispatchedTerminalization()))

	for _, msg := range []string{
		"leaving terminal gRPC task unchanged while settling an undispatched operation's control request",
		"settled undispatched multi-operation gRPC apply operation; apply-level control request remains pending for siblings",
	} {
		line := requireCapturedLog(t, records, msg)
		assertLogCarriesApplyIdentity(t, line)
		assert.Equal(t, "deploy-primary", line.attrs["deployment"], "%s: the apply's routing deployment stays filterable", msg)
		assert.Equal(t, "deploy-eu", line.attrs["operation_deployment"], "%s: the operation's own deployment is named unambiguously", msg)
		assert.Equal(t, state.Apply.Running, line.attrs["state"], msg)
		assert.Equal(t, opID, line.attrs["apply_operation_id"], msg)
		assert.Equal(t, storage.ControlOperationCancel, line.attrs["control_operation"], msg)
	}
	skip := requireCapturedLog(t, records, "leaving terminal gRPC task unchanged while settling an undispatched operation's control request")
	assert.Equal(t, "task-done", skip.attrs["task_id"])
	settled := requireCapturedLog(t, records, "settled undispatched multi-operation gRPC apply operation; apply-level control request remains pending for siblings")
	assert.Equal(t, "alice", settled.attrs["requested_by"])
	assert.Equal(t, state.ApplyOperation.Pending, settled.attrs["old_operation_state"])
	assert.Equal(t, state.ApplyOperation.Cancelled, settled.attrs["new_operation_state"])
}
