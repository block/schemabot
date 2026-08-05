package tern

import (
	"log/slog"
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A terminal apply moots every pending window/stop control request: a stop is
// settled, a revert or skip-revert can no longer act once the revert window
// is gone — including a request that lost to a contradictory command (e.g. a
// revert still pending after skip-revert finalized the apply) — and a cancel
// has nothing left to terminate. The sweep completes all of them so no request
// lingers pending forever.
func TestCompletePendingRequestsForTerminalApply(t *testing.T) {
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
		storage.ControlOperationCancel,
	}
	requests := make([]*storage.ApplyControlRequest, 0, len(sweptOps))
	for _, op := range sweptOps {
		requests = append(requests, &storage.ApplyControlRequest{
			ApplyID: apply.ID, Operation: op, Status: storage.ControlRequestPending, RequestedBy: "armand",
		})
	}
	controlRequests := &testControlRequestStore{requests: requests}
	logs := &mockApplyLogStore{}
	store := &mockStorage{controlRequests: controlRequests, logs: logs}

	require.NoError(t, completePendingRequestsForTerminalApply(t.Context(), store, slog.New(slog.DiscardHandler), apply))

	for _, op := range sweptOps {
		pending, err := controlRequests.GetPending(t.Context(), apply.ID, op)
		require.NoError(t, err)
		assert.Nil(t, pending, "pending %s request must be completed once the apply is terminal", op)
		swept, err := controlRequests.GetByOperation(t.Context(), apply.ID, op)
		require.NoError(t, err)
		require.NotNil(t, swept)
		assert.Equal(t, storage.ControlRequestCompleted, swept.Status)
	}

	// The completed apply mooted the pending cancel, so the apply history
	// discloses that the accepted cancel never took effect.
	require.Len(t, logs.logs, 1, "sweeping a mooted cancel must record exactly one apply event")
	event := logs.logs[0]
	assert.Equal(t, apply.ID, event.ApplyID)
	assert.Equal(t, storage.LogLevelWarn, event.Level)
	assert.Contains(t, event.Message, "Cancel did not take effect")
	assert.Contains(t, event.Message, "completed on the engine before the cancel could take effect")
	assert.Contains(t, event.Message, "the change is live on the target")
	assert.Contains(t, event.Message, "(caller: armand)")
}

// An apply that settles cancelled resolved its pending cancel rather than
// mooting it: the sweep completes the request without writing a
// did-not-take-effect event, since the cancel did take effect.
func TestCompletePendingRequestsForCancelledApplyWritesNoMootEvent(t *testing.T) {
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

	require.NoError(t, completePendingRequestsForTerminalApply(t.Context(), store, slog.New(slog.DiscardHandler), apply))

	pending, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.Nil(t, pending, "the cancel request must be completed once the apply settles cancelled")
	assert.Empty(t, logs.logs, "a cancel that took effect is not mooted; no disclosure event belongs in the history")
}

// A cancel mooted by a non-completed terminal state names that state in the
// disclosure without claiming the change is live: a failed apply's schema
// change is not live on the target, so the event must not say it is.
func TestMootedCancelEventMessageNamesNonCompletedTerminalState(t *testing.T) {
	msg := mootedCancelEventMessage(state.Apply.Failed, &storage.ApplyControlRequest{RequestedBy: "armand"})
	assert.Contains(t, msg, "Cancel did not take effect")
	assert.Contains(t, msg, "reached failed on the engine before the cancel could take effect")
	assert.NotContains(t, msg, "live on the target")
	assert.True(t, strings.HasSuffix(msg, "(caller: armand)"), "message must end with the caller suffix: %q", msg)
}

// A stopped apply is terminal but remains cancellable: the sweep must complete
// the mooted stop while keeping a pending cancel deliverable, so a cancel
// issued against the stopped apply is still delivered by the next drive
// instead of being silently consumed.
func TestCompletePendingRequestsForStoppedApplyKeepsCancelPending(t *testing.T) {
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

	require.NoError(t, completePendingRequestsForTerminalApply(t.Context(), store, slog.New(slog.DiscardHandler), apply))

	pendingStop, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	assert.Nil(t, pendingStop, "the stop is mooted once the apply settles stopped")
	pendingCancel, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.NotNil(t, pendingCancel, "a stopped apply remains cancellable; the pending cancel must stay deliverable")
}
