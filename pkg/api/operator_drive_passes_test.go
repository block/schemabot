package api

import (
	"bytes"
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
func drivePassesWithin(t *testing.T, svc *Service, stop <-chan struct{}, pass claimPass) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), drivePassesDeadline)
	defer cancel()
	svc.drivePasses(ctx, 1, stop, pass)
	require.NoError(t, ctx.Err(), "the driver's claim passes must end before the deadline")
}

// contendedClaimService returns a service whose operation claim reports
// contention for the given number of claims in a row, and the store that
// counts those claims.
func contendedClaimService(t *testing.T, contended int) (*Service, *claimLadderOperationStore) {
	t.Helper()
	ops := &claimLadderOperationStore{
		recoverOperationStore: &recoverOperationStore{},
		contendedClaims:       contended,
	}
	store := &mockStorageWithApplyStores{applies: &operationClaimApplyStore{}, operations: ops}
	return New(store, testServerConfig(), nil, slog.New(slog.DiscardHandler)), ops
}

// captureLogs points the service's logger at a buffer, at Debug, and returns
// the buffer. drivePasses runs on the test goroutine, so the buffer needs no
// lock.
func captureLogs(svc *Service) *bytes.Buffer {
	var logs bytes.Buffer
	svc.logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return &logs
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

	drivePassesWithin(t, svc, openClaimGate(), passExpectsWork)

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
	svc, ops := contendedClaimService(t, 2)

	drivePassesWithin(t, svc, openClaimGate(), passExpectsWork)

	assert.Equal(t, 3, ops.claims, "the driver claims again after each contended claim, and stops once a claim finds nothing")
	for i, filter := range ops.claimFilters {
		assert.True(t, filter.ReportContention, "claim %d asks storage to report contention", i)
	}
}

// A poll tick finds nothing to claim, which is what every tick on an idle
// plane does. Asking storage to report contention would run the claim query a
// second time on each of those ticks, so a polled pass does not ask.
func TestDrivePassesDoesNotAskAPolledClaimToReportContention(t *testing.T) {
	svc, ops := contendedClaimService(t, 0)

	drivePassesWithin(t, svc, openClaimGate(), passPolled)

	require.Len(t, ops.claimFilters, 1, "a polled pass that finds nothing claims once")
	assert.False(t, ops.claimFilters[0].ReportContention, "a polled claim does not ask storage to report contention")
}

// Every claim retry takes nothing while its lock-free reread finds an
// operation claimable. The driver gives up after a bounded number of retries and
// leaves the operation to its next wake or tick, rather than polling storage,
// and says so at Warn with the lease owner it claimed as.
func TestDrivePassesBoundsContendedClaimRetries(t *testing.T) {
	svc, ops := contendedClaimService(t, contendedClaimRetries+10)
	logs := captureLogs(svc)

	drivePassesWithin(t, svc, openClaimGate(), passExpectsWork)

	assert.Equal(t, 1+contendedClaimRetries, ops.claims, "one claim plus a bounded number of retries")
	assert.Contains(t, logs.String(), `level=WARN msg="operator: every claim retry took no apply_operation while its lock-free reread found one claimable`)
	assert.Contains(t, logs.String(), "lease_owner="+driverLeaseOwner(1))
}

// Claiming stops, or the operator shuts down, while a contended claim is in
// hand. The driver's wait before the retry sees it and the run returns from
// there, without another pass reaching the claim ladder (AV-14).
func TestDrivePassesEndsAContendedRetryWhenTheDriverStops(t *testing.T) {
	for name, tc := range map[string]struct {
		stopDriver func(stop chan struct{}, cancel context.CancelFunc)
		waitLog    string
	}{
		"claiming stops": {
			stopDriver: func(stop chan struct{}, _ context.CancelFunc) { close(stop) },
			waitLog:    "not retrying a contended claim; claiming has stopped",
		},
		"the operator shuts down": {
			stopDriver: func(_ chan struct{}, cancel context.CancelFunc) { cancel() },
			waitLog:    "not retrying a contended claim; the operator is shutting down",
		},
	} {
		t.Run(name, func(t *testing.T) {
			svc, ops := contendedClaimService(t, contendedClaimRetries+10)
			logs := captureLogs(svc)
			stop := make(chan struct{})
			ctx, cancel := context.WithTimeout(t.Context(), drivePassesDeadline)
			defer cancel()
			ops.onContendedClaim = func(claims int) {
				if claims == 2 {
					tc.stopDriver(stop, cancel)
				}
			}

			svc.drivePasses(ctx, 1, stop, passExpectsWork)

			assert.Equal(t, 2, ops.claims, "no claim follows the one in hand when the driver stopped")
			assert.Contains(t, logs.String(), tc.waitLog, "the wait before the retry ends the run")
			assert.NotContains(t, logs.String(), "skipping the claim ladder", "no further pass starts after the wait ends the run")
		})
	}
}
