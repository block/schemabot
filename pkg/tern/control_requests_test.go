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
