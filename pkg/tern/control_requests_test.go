package tern

import (
	"fmt"
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A terminal apply moots every pending window/stop control request: a stop is
// settled, and a revert or skip-revert can no longer act once the revert window
// is gone — including a request that lost to a contradictory command (e.g. a
// revert still pending after skip-revert finalized the apply). The sweep
// completes all of them so no request lingers pending forever.
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
		storage.ControlOperationRevert,
		storage.ControlOperationSkipRevert,
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

	require.NoError(t, settlePendingRequestsForTerminalApply(t.Context(), store, discardLogger(), apply))

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

	require.NoError(t, settlePendingRequestsForTerminalApply(t.Context(), store, discardLogger(), apply))

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

	require.NoError(t, settlePendingRequestsForTerminalApply(t.Context(), store, discardLogger(), apply))

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

	require.NoError(t, settlePendingRequestsForTerminalApply(t.Context(), store, discardLogger(), apply))

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
