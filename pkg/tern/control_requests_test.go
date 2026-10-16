package tern

import (
	"bytes"
	"fmt"
	"log/slog"
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A terminal apply moots a pending stop whatever the outcome, so the sweep
// completes it and no request lingers pending forever.
func TestSettlePendingRequestsForTerminalApply(t *testing.T) {
	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-terminal-sweep",
		Database:        "testdb",
		Environment:     "staging",
		State:           state.Apply.Completed,
	}
	sweptOps := []storage.ControlOperation{
		storage.ControlOperationStop,
		// A pending row for a retired operation, written by a previous release;
		// no driver services it, so the sweep is its only settlement path.
		storage.ControlOperation("volume"),
	}
	requests := make([]*storage.ApplyControlRequest, 0, len(sweptOps))
	for _, op := range sweptOps {
		requests = append(requests, &storage.ApplyControlRequest{
			ApplyID: apply.ID, Operation: op, Status: storage.ControlRequestPending,
		})
	}
	controlRequests := &testControlRequestStore{requests: requests}
	store := &mockStorage{controlRequests: controlRequests}

	require.NoError(t, SettlePendingRequestsForTerminalApply(t.Context(), store, discardLogger(), apply))

	for _, op := range sweptOps {
		pending, err := controlRequests.GetPending(t.Context(), apply.ID, op)
		require.NoError(t, err)
		assert.Nil(t, pending, "pending %s request must be completed once the apply is terminal", op)
		swept, err := controlRequests.GetByOperation(t.Context(), apply.ID, op)
		require.NoError(t, err)
		require.NotNil(t, swept)
		assert.Equal(t, storage.ControlRequestCompleted, swept.Status)
	}
}

// An on_failure=pause rollout released by an operator and then settled leaves
// its release pending: no drive consumes it, because the pending row is the
// latch. The sweep completes it so it does not stay pending forever, and the
// completed row still latches, so the rollout stays released.
func TestSettlePendingRequestsForTerminalApplyCompletesAReleaseAndKeepsItsLatch(t *testing.T) {
	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-released-then-failed",
		Database:        "testdb",
		Environment:     "staging",
		State:           state.Apply.Failed,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{
		{ApplyID: apply.ID, Operation: storage.ControlOperationRelease, Status: storage.ControlRequestPending, RequestedBy: "cli:alice"},
	}}
	store := &mockStorage{controlRequests: controlRequests}

	require.NoError(t, SettlePendingRequestsForTerminalApply(t.Context(), store, discardLogger(), apply))

	pending, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationRelease)
	require.NoError(t, err)
	assert.Nil(t, pending, "a release must not stay pending on a terminal apply")
	release, err := controlRequests.GetByOperation(t.Context(), apply.ID, storage.ControlOperationRelease)
	require.NoError(t, err)
	require.NotNil(t, release)
	assert.Equal(t, storage.ControlRequestCompleted, release.Status)
	assert.True(t, release.ReleasesPausedRollout(), "completing the release must keep the rollout released")
}

// When the sweep completes a stop, the server log records it with the
// operator who asked, so a stop answered by the reaper or the post-drive
// settlement is traceable to its requester.
func TestSettlePendingRequestsForTerminalApplyLogsTheCompletedStop(t *testing.T) {
	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-stop-logged",
		Database:        "testdb",
		Environment:     "staging",
		State:           state.Apply.Stopped,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{
		{ApplyID: apply.ID, Operation: storage.ControlOperationStop, Status: storage.ControlRequestPending, RequestedBy: "cli:alice"},
	}}
	store := &mockStorage{controlRequests: controlRequests}
	var logged bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logged, nil))

	require.NoError(t, SettlePendingRequestsForTerminalApply(t.Context(), store, logger, apply))

	assert.Contains(t, logged.String(), `msg="completed pending stop request"`)
	assert.Contains(t, logged.String(), "apply_id=apply-stop-logged")
	assert.Contains(t, logged.String(), "requested_by=cli:alice")
}

// A cutover still pending when the apply settles is answered by the outcome the
// apply reached: an apply that cut over completes it, and every other terminal
// state fails it with a reason, so it never lingers pending for the reaper to
// re-list on every pass.
func TestSettlePendingRequestsForTerminalApplySettlesCutoverByOutcome(t *testing.T) {
	cases := []struct {
		applyState string
		wantStatus storage.ControlRequestStatus
		wantError  string
	}{
		{applyState: state.Apply.Completed, wantStatus: storage.ControlRequestCompleted},
		{applyState: state.Apply.Reverted, wantStatus: storage.ControlRequestCompleted},
		{applyState: state.Apply.Cancelled, wantStatus: storage.ControlRequestFailed,
			wantError: "cutover request was not applied because apply is cancelled"},
		{applyState: state.Apply.Failed, wantStatus: storage.ControlRequestFailed,
			wantError: "cutover request was not applied because apply is failed"},
		{applyState: state.Apply.Stopped, wantStatus: storage.ControlRequestFailed,
			wantError: cutoverSupersededByStopMessage},
	}
	for _, tc := range cases {
		t.Run(tc.applyState, func(t *testing.T) {
			apply := &storage.Apply{
				ID:              7,
				ApplyIdentifier: "apply-terminal-cutover",
				Database:        "testdb",
				Environment:     "staging",
				State:           tc.applyState,
			}
			controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{
				{ApplyID: apply.ID, Operation: storage.ControlOperationCutover, Status: storage.ControlRequestPending, RequestedBy: "cli:alice"},
			}}
			store := &mockStorage{controlRequests: controlRequests, logs: &mockApplyLogStore{}}

			require.NoError(t, SettlePendingRequestsForTerminalApply(t.Context(), store, discardLogger(), apply))

			settled, err := controlRequests.GetByOperation(t.Context(), apply.ID, storage.ControlOperationCutover)
			require.NoError(t, err)
			require.NotNil(t, settled)
			assert.Equal(t, tc.wantStatus, settled.Status)
			assert.Equal(t, tc.wantError, settled.ErrorMessage)
		})
	}
}

// A cutover bound to one operation is answered from that operation's outcome,
// not the parent apply's (CO-6): an operation that cut over before a sibling
// failed the apply completes its request, and one that did not fails it with
// its own reason.
func TestSettlePendingRequestsForTerminalApplySettlesBoundCutoverByOperation(t *testing.T) {
	cases := []struct {
		name           string
		operationState string
		wantStatus     storage.ControlRequestStatus
		wantError      string
	}{
		{name: "operation cut over", operationState: state.ApplyOperation.Completed, wantStatus: storage.ControlRequestCompleted},
		{name: "operation in revert window", operationState: state.ApplyOperation.RevertWindow, wantStatus: storage.ControlRequestCompleted},
		{name: "operation stopped", operationState: state.ApplyOperation.Stopped, wantStatus: storage.ControlRequestFailed,
			wantError: cutoverSupersededByStopMessage},
		{name: "operation cancelled", operationState: state.ApplyOperation.Cancelled, wantStatus: storage.ControlRequestFailed,
			wantError: "cutover request was not applied because the operation it was sent for is cancelled"},
		{name: "operation never ended", operationState: state.ApplyOperation.WaitingForCutover, wantStatus: storage.ControlRequestFailed,
			wantError: "cutover request was not applied because apply is failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-bound", Database: "testdb", Environment: "staging", State: state.Apply.Failed}
			controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{
				{ID: 1, ApplyID: apply.ID, Operation: storage.ControlOperationCutover, Status: storage.ControlRequestPending,
					RequestedBy: "cli:alice", Metadata: storage.CutoverRequestMetadata(42)},
			}}
			ops := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
				42: {ID: 42, ApplyID: apply.ID, Deployment: "region-a", State: tc.operationState},
			}}
			store := &mockStorage{controlRequests: controlRequests, logs: &mockApplyLogStore{}, operations: ops}

			require.NoError(t, SettlePendingRequestsForTerminalApply(t.Context(), store, discardLogger(), apply))

			settled, err := controlRequests.GetByOperation(t.Context(), apply.ID, storage.ControlOperationCutover)
			require.NoError(t, err)
			require.NotNil(t, settled)
			assert.Equal(t, tc.wantStatus, settled.Status)
			assert.Equal(t, tc.wantError, settled.ErrorMessage)
		})
	}
}

// A stop that takes effect on an apply still held open answers a cutover bound
// to one operation from that operation: one that already cut over completes
// it, and one the stop halted fails it as superseded.
func TestSettlePendingCutoverAfterStopSettlesBoundCutoverByOperation(t *testing.T) {
	cases := []struct {
		name           string
		operationState string
		wantStatus     storage.ControlRequestStatus
		wantError      string
	}{
		{name: "operation cut over", operationState: state.ApplyOperation.Completed, wantStatus: storage.ControlRequestCompleted},
		{name: "operation stopped", operationState: state.ApplyOperation.Stopped, wantStatus: storage.ControlRequestFailed,
			wantError: cutoverSupersededByStopMessage},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-bound-held-open", Database: "testdb", Environment: "staging", State: state.Apply.RunningDegraded}
			controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{
				{ID: 1, ApplyID: apply.ID, Operation: storage.ControlOperationCutover, Status: storage.ControlRequestPending,
					RequestedBy: "cli:alice", Metadata: storage.CutoverRequestMetadata(42)},
			}}
			ops := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
				42: {ID: 42, ApplyID: apply.ID, Deployment: "region-a", State: tc.operationState},
			}}
			store := &mockStorage{controlRequests: controlRequests, logs: &mockApplyLogStore{}, operations: ops}

			require.NoError(t, SettlePendingCutoverAfterStop(t.Context(), store, discardLogger(), apply))

			settled, err := controlRequests.GetByOperation(t.Context(), apply.ID, storage.ControlOperationCutover)
			require.NoError(t, err)
			require.NotNil(t, settled)
			assert.Equal(t, tc.wantStatus, settled.Status)
			assert.Equal(t, tc.wantError, settled.ErrorMessage)
		})
	}
}

// A bound cutover whose operation cannot be read stays pending: the lookup
// error is returned rather than recorded as an outcome the operation may not
// have reached.
func TestSettlePendingCutoverForResolvedApplyPropagatesBoundOperationLookupError(t *testing.T) {
	apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-bound-lookup", Database: "testdb", Environment: "staging", State: state.Apply.Failed}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{
		{ID: 1, ApplyID: apply.ID, Operation: storage.ControlOperationCutover, Status: storage.ControlRequestPending,
			RequestedBy: "cli:alice", Metadata: storage.CutoverRequestMetadata(42)},
	}}
	store := &mockStorage{controlRequests: controlRequests, logs: &mockApplyLogStore{}, operations: &mockApplyOperationStore{}}

	err := SettlePendingCutoverForResolvedApply(t.Context(), store, discardLogger(), apply)
	require.ErrorIs(t, err, storage.ErrApplyOperationNotFound)

	pending, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCutover)
	require.NoError(t, err)
	assert.NotNil(t, pending, "a cutover whose operation could not be read must stay pending")
}

// A bound cutover whose operation does not exist fails, as it does on the
// bound operation's drive, rather than staying pending on a resolved apply.
func TestSettlePendingCutoverForResolvedApplyFailsBoundCutoverOfMissingOperation(t *testing.T) {
	apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-bound-missing", Database: "testdb", Environment: "staging", State: state.Apply.Failed}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{
		{ID: 1, ApplyID: apply.ID, Operation: storage.ControlOperationCutover, Status: storage.ControlRequestPending,
			RequestedBy: "cli:alice", Metadata: storage.CutoverRequestMetadata(42)},
	}}
	ops := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{42: nil}}
	store := &mockStorage{controlRequests: controlRequests, logs: &mockApplyLogStore{}, operations: ops}

	require.NoError(t, SettlePendingCutoverForResolvedApply(t.Context(), store, discardLogger(), apply))

	settled, err := controlRequests.GetByOperation(t.Context(), apply.ID, storage.ControlOperationCutover)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, storage.ControlRequestFailed, settled.Status)
	assert.Equal(t, "cutover request was not applied because the operation it was sent for (42) does not exist", settled.ErrorMessage)
}

// A stop that lands on an apply which already cut over does not fail the
// cutover as superseded: the cutover took effect, so it completes.
func TestSettlePendingCutoverAfterStopCompletesCutoverOfCutOverApply(t *testing.T) {
	for _, applyState := range []string{state.Apply.Completed, state.Apply.RevertWindow} {
		t.Run(applyState, func(t *testing.T) {
			apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-cut-over", Database: "testdb", Environment: "staging", State: applyState}
			controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{
				{ID: 1, ApplyID: apply.ID, Operation: storage.ControlOperationCutover, Status: storage.ControlRequestPending, RequestedBy: "cli:alice"},
			}}
			store := &mockStorage{controlRequests: controlRequests, logs: &mockApplyLogStore{}}

			require.NoError(t, SettlePendingCutoverAfterStop(t.Context(), store, discardLogger(), apply))

			settled, err := controlRequests.GetByOperation(t.Context(), apply.ID, storage.ControlOperationCutover)
			require.NoError(t, err)
			require.NotNil(t, settled)
			assert.Equal(t, storage.ControlRequestCompleted, settled.Status)
			assert.Empty(t, settled.ErrorMessage)
		})
	}
}

// A stop request completes only through the path that settles the cutover it
// superseded; the generic completion refuses it and leaves it pending.
func TestCompletePendingControlRequestsRefusesStop(t *testing.T) {
	apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-stop-refused", Database: "testdb", Environment: "staging", State: state.Apply.Stopped}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{
		{ID: 1, ApplyID: apply.ID, Operation: storage.ControlOperationStop, Status: storage.ControlRequestPending, RequestedBy: "cli:bob"},
	}}
	store := &mockStorage{controlRequests: controlRequests, logs: &mockApplyLogStore{}}

	err := completePendingControlRequests(t.Context(), store, apply, storage.ControlOperationStop)
	require.ErrorContains(t, err, "completePendingStopControlRequest")

	pending, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	assert.NotNil(t, pending)
}

// A revert or skip-revert still pending when the apply settles can no longer
// act, and the outcome the apply reached says whether it took effect. A revert
// sent in the revert window whose immediate engine call failed stays pending;
// if the window then expires and the apply completes, the change is permanent,
// so the revert is answered as not applied rather than reported carried out.
// Likewise a skip-revert outrun by a revert. Only the outcome each command asked
// for completes it. A request that did not take effect also leaves a warning in
// the apply history, so an operator triaging from the history alone sees that
// the command was accepted and never landed.
func TestSettlePendingRequestsForTerminalApplySettlesRevertPhaseByOutcome(t *testing.T) {
	cases := []struct {
		name       string
		operation  storage.ControlOperation
		applyState string
		wantStatus storage.ControlRequestStatus
		wantError  string
		wantEvent  string
	}{
		{name: "revert that reverted", operation: storage.ControlOperationRevert, applyState: state.Apply.Reverted,
			wantStatus: storage.ControlRequestCompleted},
		{name: "revert outrun by completion", operation: storage.ControlOperationRevert, applyState: state.Apply.Completed,
			wantStatus: storage.ControlRequestFailed,
			wantError:  "the schema change completed before the revert could take effect; the change is live on the target",
			wantEvent:  "Revert did not take effect: the schema change completed before the revert could take effect; the change is live on the target (caller: cli:alice)"},
		{name: "revert outrun by a proto-form completed state", operation: storage.ControlOperationRevert, applyState: "STATE_COMPLETED",
			wantStatus: storage.ControlRequestFailed,
			wantError:  "the schema change completed before the revert could take effect; the change is live on the target",
			wantEvent:  "Revert did not take effect: the schema change completed before the revert could take effect; the change is live on the target (caller: cli:alice)"},
		{name: "revert outrun by failure", operation: storage.ControlOperationRevert, applyState: state.Apply.Failed,
			wantStatus: storage.ControlRequestFailed,
			wantError:  "the schema change reached Failed before the revert could take effect",
			wantEvent:  "Revert did not take effect: the schema change reached Failed before the revert could take effect (caller: cli:alice)"},
		{name: "revert on a stopped apply", operation: storage.ControlOperationRevert, applyState: state.Apply.Stopped,
			wantStatus: storage.ControlRequestFailed,
			wantError:  "the schema change reached Stopped before the revert could take effect",
			wantEvent:  "Revert did not take effect: the schema change reached Stopped before the revert could take effect (caller: cli:alice)"},
		{name: "revert outrun by a cancel", operation: storage.ControlOperationRevert, applyState: state.Apply.Cancelled,
			wantStatus: storage.ControlRequestFailed,
			wantError:  "the schema change reached Cancelled before the revert could take effect",
			wantEvent:  "Revert did not take effect: the schema change reached Cancelled before the revert could take effect (caller: cli:alice)"},
		{name: "skip-revert that completed", operation: storage.ControlOperationSkipRevert, applyState: state.Apply.Completed,
			wantStatus: storage.ControlRequestCompleted},
		{name: "skip-revert that completed in proto form", operation: storage.ControlOperationSkipRevert, applyState: "STATE_COMPLETED",
			wantStatus: storage.ControlRequestCompleted},
		{name: "skip-revert outrun by a revert", operation: storage.ControlOperationSkipRevert, applyState: state.Apply.Reverted,
			wantStatus: storage.ControlRequestFailed,
			wantError:  "the schema change reached Reverted before the skip-revert could take effect",
			wantEvent:  "Skip-revert did not take effect: the schema change reached Reverted before the skip-revert could take effect (caller: cli:alice)"},
		{name: "skip-revert outrun by failure", operation: storage.ControlOperationSkipRevert, applyState: state.Apply.Failed,
			wantStatus: storage.ControlRequestFailed,
			wantError:  "the schema change reached Failed before the skip-revert could take effect",
			wantEvent:  "Skip-revert did not take effect: the schema change reached Failed before the skip-revert could take effect (caller: cli:alice)"},
		{name: "skip-revert on a stopped apply", operation: storage.ControlOperationSkipRevert, applyState: state.Apply.Stopped,
			wantStatus: storage.ControlRequestFailed,
			wantError:  "the schema change reached Stopped before the skip-revert could take effect",
			wantEvent:  "Skip-revert did not take effect: the schema change reached Stopped before the skip-revert could take effect (caller: cli:alice)"},
		{name: "skip-revert outrun by a cancel", operation: storage.ControlOperationSkipRevert, applyState: state.Apply.Cancelled,
			wantStatus: storage.ControlRequestFailed,
			wantError:  "the schema change reached Cancelled before the skip-revert could take effect",
			wantEvent:  "Skip-revert did not take effect: the schema change reached Cancelled before the skip-revert could take effect (caller: cli:alice)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			apply := &storage.Apply{
				ID:              7,
				ApplyIdentifier: "apply-terminal-revert-phase",
				Database:        "testdb",
				Environment:     "staging",
				State:           tc.applyState,
			}
			controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{
				{ApplyID: apply.ID, Operation: tc.operation, Status: storage.ControlRequestPending, RequestedBy: "cli:alice"},
			}}
			logs := &mockApplyLogStore{}
			store := &mockStorage{controlRequests: controlRequests, logs: logs}

			require.NoError(t, SettlePendingRequestsForTerminalApply(t.Context(), store, discardLogger(), apply))

			settled, err := controlRequests.GetByOperation(t.Context(), apply.ID, tc.operation)
			require.NoError(t, err)
			require.NotNil(t, settled)
			assert.Equal(t, tc.wantStatus, settled.Status)
			assert.Equal(t, tc.wantError, settled.ErrorMessage)
			if tc.wantEvent == "" {
				assert.Empty(t, logs.logs, "a command that took effect adds no did-not-take-effect event")
				return
			}
			require.Len(t, logs.logs, 1)
			assert.Equal(t, storage.LogLevelWarn, logs.logs[0].Level)
			wantEventType := map[storage.ControlOperation]string{
				storage.ControlOperationRevert:     storage.LogEventRevertTriggered,
				storage.ControlOperationSkipRevert: storage.LogEventSkipRevertTriggered,
			}[tc.operation]
			assert.Equal(t, wantEventType, logs.logs[0].EventType)
			assert.Equal(t, tc.wantEvent, logs.logs[0].Message)
		})
	}
}

// A state the registry does not carry has no operator-facing name, so the
// reason names no state rather than echoing the stored token.
func TestRevertPhaseOutrunReasonUnknownState(t *testing.T) {
	assert.Equal(t, "the schema change settled before the revert could take effect",
		revertPhaseOutrunReason(storage.ControlOperationRevert, "not_a_state"))
	assert.Equal(t, "the schema change settled before the skip-revert could take effect",
		revertPhaseOutrunReason(storage.ControlOperationSkipRevert, "not_a_state"))
}

// The remote drive acts on a revert or skip-revert only in the revert window.
// Once the apply has settled it answers the request by the outcome, without an
// engine call, so the remote and local drives give the operator the same
// answer. While the apply is still on its way out of the window the request
// stays pending: it may be the command carrying the apply there, and the
// terminal settlement answers it once the outcome is stored.
func TestSettlePendingRevertPhaseRequestOutsideWindow(t *testing.T) {
	cases := []struct {
		name       string
		operation  storage.ControlOperation
		applyState string
		wantStatus storage.ControlRequestStatus
	}{
		{name: "revert while reverting", operation: storage.ControlOperationRevert, applyState: state.Apply.Reverting,
			wantStatus: storage.ControlRequestPending},
		{name: "revert while skipping the revert", operation: storage.ControlOperationRevert, applyState: state.Apply.SkippingRevert,
			wantStatus: storage.ControlRequestPending},
		{name: "skip-revert while skipping the revert", operation: storage.ControlOperationSkipRevert, applyState: state.Apply.SkippingRevert,
			wantStatus: storage.ControlRequestPending},
		{name: "revert on a reverted apply", operation: storage.ControlOperationRevert, applyState: state.Apply.Reverted,
			wantStatus: storage.ControlRequestCompleted},
		{name: "revert on a completed apply", operation: storage.ControlOperationRevert, applyState: state.Apply.Completed,
			wantStatus: storage.ControlRequestFailed},
		{name: "skip-revert on a completed apply", operation: storage.ControlOperationSkipRevert, applyState: state.Apply.Completed,
			wantStatus: storage.ControlRequestCompleted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-left-window", Database: "testdb", Environment: "staging", State: tc.applyState}
			controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{
				{ApplyID: apply.ID, Operation: tc.operation, Status: storage.ControlRequestPending, RequestedBy: "cli:alice"},
			}}
			store := &mockStorage{controlRequests: controlRequests}

			require.NoError(t, settlePendingRevertPhaseRequestOutsideWindow(t.Context(), store, discardLogger(), apply, tc.operation))

			got, err := controlRequests.GetByOperation(t.Context(), apply.ID, tc.operation)
			require.NoError(t, err)
			require.NotNil(t, got)
			assert.Equal(t, tc.wantStatus, got.Status)
		})
	}
}

// A cancel that an apply outran never took effect, so the sweep resolves it as a
// rejection rather than reporting it applied: the failed request is what lights
// up the PR comment's "Command not applied" notice and what a re-issued cancel
// is answered with, and the apply history records the same outcome.
func TestSettlePendingRequestsForTerminalApplyFailsAnOutrunCancel(t *testing.T) {
	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-mooted-cancel",
		Database:        "testdb",
		Environment:     "staging",
		State:           state.Apply.Completed,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{
		{ApplyID: apply.ID, Operation: storage.ControlOperationCancel, Status: storage.ControlRequestPending, RequestedBy: "armand"},
	}}
	logs := &mockApplyLogStore{}
	store := &mockStorage{controlRequests: controlRequests, logs: logs}

	require.NoError(t, SettlePendingRequestsForTerminalApply(t.Context(), store, discardLogger(), apply))

	pending, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.Nil(t, pending, "the mooted cancel must not stay pending")
	settled, err := controlRequests.GetByOperation(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, storage.ControlRequestFailed, settled.Status)
	assert.Equal(t, "the schema change completed before the cancel could take effect; the change is live on the target",
		settled.ErrorMessage)

	require.Len(t, logs.logs, 1, "settling a mooted cancel must record exactly one apply event")
	assert.Equal(t, storage.LogLevelWarn, logs.logs[0].Level)
	assert.Equal(t, "Cancel did not take effect: the schema change completed before the cancel could take effect; the change is live on the target (caller: armand)",
		logs.logs[0].Message)
}

// The settle is a conditional update on a still-pending row, and a write that
// matches no row reports success — so a second settle must not append a second
// disclosure. Two settles of the same outrun cancel leave one recorded outcome
// and one apply event, rather than an operator reading the same warning twice.
func TestSettlingAnAlreadySettledCancelRecordsNothingFurther(t *testing.T) {
	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-resettled-cancel",
		Database:        "testdb",
		Environment:     "staging",
		State:           state.Apply.Completed,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{
		{ApplyID: apply.ID, Operation: storage.ControlOperationCancel, Status: storage.ControlRequestPending, RequestedBy: "armand"},
	}}
	logs := &mockApplyLogStore{}
	store := &mockStorage{controlRequests: controlRequests, logs: logs}

	require.NoError(t, SettlePendingCancelForResolvedApply(t.Context(), store, discardLogger(), apply))
	require.NoError(t, SettlePendingCancelForResolvedApply(t.Context(), store, discardLogger(), apply))

	settled, err := controlRequests.GetByOperation(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, storage.ControlRequestFailed, settled.Status)
	assert.Len(t, logs.logs, 1, "the second settle found nothing pending, so it must add no further history")
}

// A drive settles the cancel it consumed only once the stored apply has
// resolved. While the apply is still live the request stays pending, so a
// sibling drive still has the command to deliver — settling it here would
// answer a command that has not been carried out.
func TestSettlePendingCancelIfStoredApplyResolvedLeavesALiveApplyAlone(t *testing.T) {
	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-still-running",
		Database:        "testdb",
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{
		{ApplyID: apply.ID, Operation: storage.ControlOperationCancel, Status: storage.ControlRequestPending, RequestedBy: "armand"},
	}}
	logs := &mockApplyLogStore{}
	store := &mockStorage{applies: &mockApplyStore{apply: apply}, controlRequests: controlRequests, logs: logs}

	settled, err := settlePendingCancelIfStoredApplyResolved(t.Context(), store, discardLogger(), apply)

	require.NoError(t, err)
	assert.False(t, settled, "a live apply has not answered the cancel yet")
	pending, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.NotNil(t, pending, "the cancel stays deliverable while the apply is still running")
	assert.Empty(t, logs.logs, "nothing was settled, so the history must not claim an outcome")
}

// An apply that settles cancelled resolved its pending cancel rather than
// outrunning it, so the request completes and the history records that the
// command took effect.
func TestSettlePendingRequestsForCancelledApplyCompletesTheCancel(t *testing.T) {
	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-cancelled-sweep",
		Database:        "testdb",
		Environment:     "staging",
		State:           state.Apply.Cancelled,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{
		{ApplyID: apply.ID, Operation: storage.ControlOperationCancel, Status: storage.ControlRequestPending, RequestedBy: "armand"},
	}}
	logs := &mockApplyLogStore{}
	store := &mockStorage{controlRequests: controlRequests, logs: logs}

	require.NoError(t, SettlePendingRequestsForTerminalApply(t.Context(), store, discardLogger(), apply))

	settled, err := controlRequests.GetByOperation(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, storage.ControlRequestCompleted, settled.Status)
	require.Len(t, logs.logs, 1)
	assert.Equal(t, storage.LogLevelInfo, logs.logs[0].Level)
	assert.Equal(t, "Pending cancel request completed for cancelled apply (caller: armand)", logs.logs[0].Message)
}

// A cancel outrun by a terminal state other than completed names that state
// without claiming the whole change is live: only a completed apply is known to
// have landed every table, so the reason sends the operator to look at the
// target only there. A cancelled apply is the cancel having taken effect, and a
// stopped apply stays cancellable, so neither outran the command.
func TestCancelOutrunReason(t *testing.T) {
	reason := cancelOutrunReason(state.Apply.Failed)
	assert.Equal(t, "the schema change reached Failed before the cancel could take effect", reason)
	assert.NotContains(t, reason, "live on the target")
	assert.Empty(t, cancelOutrunReason(state.Apply.Cancelled))
	assert.Empty(t, cancelOutrunReason(state.Apply.Stopped))

	assert.Equal(t, reason, cancelOutrunReason("STATE_FAILED"),
		"a proto-form state must reach the operator in the same vocabulary as every other surface")

	assert.Equal(t, "the schema change reached Retrying before the cancel could take effect",
		cancelOutrunReason(state.Apply.FailedRetryable),
		"the reason is prose an operator reads, so it names the state the way every other surface does rather than by its stored token")

	unregistered := cancelOutrunReason("some_future_state")
	assert.Equal(t, "the schema change settled before the cancel could take effect", unregistered,
		"a state the registry cannot name has no operator vocabulary, so the reason names none")
	assert.NotContains(t, unregistered, "some_future_state",
		"the stored token must never reach an operator as if it were the state's name")
}

// A stopped apply is terminal but remains cancellable: the sweep must complete
// the mooted stop while keeping a pending cancel deliverable, so a cancel
// issued against the stopped apply is still delivered by the next drive
// instead of being silently consumed.
func TestSettlePendingRequestsForStoppedApplyKeepsCancelPending(t *testing.T) {
	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-stopped-sweep",
		Database:        "testdb",
		Environment:     "staging",
		State:           state.Apply.Stopped,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{
		{ApplyID: apply.ID, Operation: storage.ControlOperationStop, Status: storage.ControlRequestPending},
		{ApplyID: apply.ID, Operation: storage.ControlOperationCancel, Status: storage.ControlRequestPending},
	}}
	store := &mockStorage{controlRequests: controlRequests}

	require.NoError(t, SettlePendingRequestsForTerminalApply(t.Context(), store, discardLogger(), apply))

	pendingStop, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	assert.Nil(t, pendingStop, "the stop is mooted once the apply settles stopped")
	pendingCancel, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.NotNil(t, pendingCancel, "a stopped apply remains cancellable; the pending cancel must stay deliverable")
}

// A control request can be rejected on either of two paths: the API's immediate
// attempt, or the driver's retry after that attempt never landed. Both write the
// same durable record, so both must spell the schema change the same way — an
// operator reading a failed request should never see the remote data plane's
// identifier just because the retry, rather than the immediate call, was the one
// that reached the data plane.
func TestFailPendingControlRequestsRecordsTheOperatorApplyID(t *testing.T) {
	const (
		operatorID    = "apply-driver-retry"
		remoteID      = "apply-remote999"
		remoteOpID    = "apply-remote-op42"
		rejectionCopy = "revert was not accepted: %s is no longer in its revert window"
	)

	tests := []struct {
		name       string
		externalID string
		remoteIDs  []string
	}{
		{
			name:       "a single-operation apply names the remote apply id",
			externalID: remoteID,
			remoteIDs:  []string{remoteID},
		},
		{
			name:      "a multi-operation apply names the claimed operation's remote id",
			remoteIDs: []string{remoteOpID},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apply := &storage.Apply{
				ID:              9,
				ApplyIdentifier: operatorID,
				ExternalID:      tt.externalID,
				Database:        "testdb",
				Environment:     "staging",
				State:           state.Apply.RevertWindow,
			}
			controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
				ApplyID:   apply.ID,
				Operation: storage.ControlOperationRevert,
				Status:    storage.ControlRequestPending,
			}}}
			store := &mockStorage{controlRequests: controlRequests}

			rejection := fmt.Sprintf(rejectionCopy, tt.remoteIDs[0])
			require.NoError(t, failPendingControlRequests(t.Context(), store, apply,
				storage.ControlOperationRevert, rejection, tt.remoteIDs...))

			failed, err := controlRequests.GetByOperation(t.Context(), apply.ID, storage.ControlOperationRevert)
			require.NoError(t, err)
			require.NotNil(t, failed)
			assert.Equal(t, storage.ControlRequestFailed, failed.Status)
			assert.Equal(t, fmt.Sprintf(rejectionCopy, operatorID), failed.ErrorMessage,
				"the stored rejection must name the apply the operator asked about")
		})
	}
}
