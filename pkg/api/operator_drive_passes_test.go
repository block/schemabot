package api

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// drivePassesDeadline bounds a run of claim passes in these tests, and how
// long a test waits for a running operator to settle a rollout whose units
// become claimable one after another. It is far below the poll interval the
// tests configure, so only a driver that claims the next unit as soon as the
// previous one settles can meet it.
const drivePassesDeadline = 20 * time.Second

// drivePassesWithin runs one driver's claim passes under drivePassesDeadline
// and fails the test if the run is still going when the deadline ends it. A
// cancelled context ends the run, so a pass that keeps reporting settled work
// fails here instead of hanging the suite.
func drivePassesWithin(t *testing.T, svc *Service, stop <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), drivePassesDeadline)
	defer cancel()
	svc.drivePasses(ctx, 1, stop)
	require.NoError(t, ctx.Err(), "the driver's claim passes must end before the deadline")
}

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

	drivePassesWithin(t, svc, openClaimGate())

	assert.Equal(t, 1, ops.claims, "a failed claim must not send the driver into another pass")
	assert.Equal(t, 1, applies.stopProbes, "the ladder runs exactly once")
}

// Only a claim that starts an operation wakes a peer driver. A retry
// redispatch, or a recovery that re-leases a crashed drive, must not, or a
// fast failure would hand the same retry from peer to peer at claim speed.
func TestClaimStartsOperationOnlyForStarts(t *testing.T) {
	for claimedFrom, starts := range map[string]bool{
		state.ApplyOperation.Pending:         true,
		state.ApplyOperation.Stopped:         true,
		state.ApplyOperation.Running:         false,
		state.ApplyOperation.FailedRetryable: false,
		state.ApplyOperation.Resuming:        false,
	} {
		assert.Equal(t, starts, claimStartsOperation(&storage.ApplyOperation{State: claimedFrom}),
			"a claim from %s", claimedFrom)
	}
}

// A woken driver's claim finds the operation it was woken for row-locked by
// another transaction for an instant, such as a peer's claim scanning past it.
// The driver claims again after a short backoff in the same run instead of
// going back to sleep with the operation unstarted until its next tick.
func TestDrivePassesRetriesAContendedClaim(t *testing.T) {
	ops := &claimLadderOperationStore{
		recoverOperationStore: &recoverOperationStore{},
		contendedClaims:       2,
	}
	applies := &operationClaimApplyStore{}
	store := &mockStorageWithApplyStores{applies: applies, operations: ops}
	svc := New(store, testServerConfig(), nil, slog.New(slog.DiscardHandler))

	drivePassesWithin(t, svc, openClaimGate())

	assert.Equal(t, 3, ops.claims, "the driver claims again after each contended claim, and stops once a claim finds nothing")
	for i, filter := range ops.claimFilters {
		assert.True(t, filter.ReportContention, "claim %d asks storage to report contention", i)
	}
}

// A claimable operation stays locked through every retry, as under a long
// transaction. The driver gives up after a bounded number of retries and
// leaves the operation to its next wake or tick, rather than polling storage.
func TestDrivePassesBoundsContendedClaimRetries(t *testing.T) {
	ops := &claimLadderOperationStore{
		recoverOperationStore: &recoverOperationStore{},
		contendedClaims:       contendedClaimRetries + 10,
	}
	applies := &operationClaimApplyStore{}
	store := &mockStorageWithApplyStores{applies: applies, operations: ops}
	svc := New(store, testServerConfig(), nil, slog.New(slog.DiscardHandler))

	drivePassesWithin(t, svc, openClaimGate())

	assert.Equal(t, 1+contendedClaimRetries, ops.claims, "one claim plus a bounded number of retries")
}

// Claiming stops while a contended claim waits to retry. The driver returns
// without claiming again.
func TestDrivePassesDoesNotRetryAContendedClaimAfterClaimingStops(t *testing.T) {
	ops := &claimLadderOperationStore{
		recoverOperationStore: &recoverOperationStore{},
		contendedClaims:       contendedClaimRetries + 10,
	}
	applies := &operationClaimApplyStore{}
	store := &mockStorageWithApplyStores{applies: applies, operations: ops}
	svc := New(store, testServerConfig(), nil, slog.New(slog.DiscardHandler))

	stop := make(chan struct{})
	ctx, cancel := context.WithTimeout(t.Context(), drivePassesDeadline)
	defer cancel()
	assert.Equal(t, claimContended, svc.driveTick(ctx, 1, stop), "the first claim is contended")
	close(stop)
	assert.False(t, svc.waitForContendedClaimRetry(ctx, 1, stop, time.Hour), "a closed claim gate ends the wait at once")
	assert.Equal(t, 1, ops.claims)
}
