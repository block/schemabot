package api

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

// The operation claim fails against storage on every attempt. A failed claim
// settles nothing, so the driver must make one attempt per wake or tick and
// leave the next to the poll cadence rather than retrying in a tight loop
// against a store that is already struggling.
func TestDrivePassesDoesNotRetryAFailedClaim(t *testing.T) {
	ops := &claimLadderOperationStore{
		recoverOperationStore: &recoverOperationStore{},
		claimErr:              errors.New("storage unavailable"),
	}
	applies := &operationClaimApplyStore{}
	store := &mockStorageWithApplyStores{applies: applies, operations: ops}
	svc := New(store, testServerConfig(), nil, slog.New(slog.DiscardHandler))

	svc.drivePasses(t.Context(), 1, openClaimGate())

	assert.Equal(t, 1, ops.claims, "a failed claim must not send the driver into another pass")
	assert.Equal(t, 1, applies.stopProbes, "the ladder runs exactly once")
}
