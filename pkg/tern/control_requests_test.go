package tern

import (
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
			ApplyID: apply.ID, Operation: op, Status: storage.ControlRequestPending,
		})
	}
	controlRequests := &testControlRequestStore{requests: requests}
	store := &mockStorage{controlRequests: controlRequests}

	require.NoError(t, completePendingRequestsForTerminalApply(t.Context(), store, apply))

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

	require.NoError(t, completePendingRequestsForTerminalApply(t.Context(), store, apply))

	pendingStop, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	assert.Nil(t, pendingStop, "the stop is mooted once the apply settles stopped")
	pendingCancel, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.NotNil(t, pendingCancel, "a stopped apply remains cancellable; the pending cancel must stay deliverable")
}
