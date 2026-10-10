package api

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/drain"
	"github.com/block/schemabot/pkg/storage"
)

// A process told to shut down closes its claim gate first and brings its drives
// down later. Between the two, idle drivers must have returned so no new apply
// is claimed, the gate must tolerate being closed again by StopOperator, and a
// later StartOperator must open a fresh gate rather than inherit the closed one.
func TestStopClaimingReturnsIdleDriversAndLeavesStopOperatorIdempotent(t *testing.T) {
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{}, &capturingApplyStore{})
	svc.config.Drivers = 2
	require.NoError(t, svc.SetOperatorPollInterval(time.Hour))

	svc.StartOperator(t.Context())
	t.Cleanup(svc.StopOperator)

	svc.StopClaiming()
	assert.True(t, drain.Wait(&svc.recoveryWg, driverDrainTimeout), "idle drivers return once the claim gate closes")

	// Closing a closed gate is a no-op, and StopOperator closes it again itself.
	svc.StopClaiming()
	svc.StopOperator()

	// A restarted operator claims again: the gate is per StartOperator.
	svc.StartOperator(t.Context())
	svc.operatorMu.Lock()
	gateOpen := svc.stopRecovery != nil && !svc.claimingStopped
	svc.operatorMu.Unlock()
	assert.True(t, gateOpen, "a restarted operator opens a fresh claim gate")
	svc.StopOperator()
}

// gateClosingOperationStore closes the driver pool's claim gate and queues a
// wake from inside the first operation claim, so the driver comes back to its
// select with both the gate and a wake ready at once.
type gateClosingOperationStore struct {
	*claimLadderOperationStore
	closeGate func()
}

func (s *gateClosingOperationStore) FindNextApplyOperation(ctx context.Context, owner string) (*storage.ApplyOperation, error) {
	if s.closeGate != nil {
		s.closeGate()
		s.closeGate = nil
	}
	return s.claimLadderOperationStore.FindNextApplyOperation(ctx, owner)
}

// Claiming stops while a driver is mid-claim and a wake for newly queued work
// lands at the same moment. The driver finishes the claim it had started, but
// it must not start another: once the gate is closed this process claims no new
// apply, whichever of the ready signals the driver's select happens to pick.
func TestDriverClaimsNothingOnceTheClaimGateClosesEvenWithAWakeQueued(t *testing.T) {
	// A select choosing among ready cases picks one at random, so the race is
	// only proven closed by running it until the unwanted choice is near certain.
	const attempts = 64
	for range attempts {
		ops := &claimLadderOperationStore{recoverOperationStore: &recoverOperationStore{}}
		stop := make(chan struct{})
		wake := make(chan struct{}, 1)
		gate := &gateClosingOperationStore{claimLadderOperationStore: ops, closeGate: func() {
			close(stop)
			wake <- struct{}{}
		}}
		store := &mockStorageWithApplyStores{applies: &operationClaimApplyStore{}, operations: gate}
		svc := New(store, testServerConfig(), nil, slog.New(slog.DiscardHandler))
		require.NoError(t, svc.SetOperatorPollInterval(time.Hour))

		svc.operatorDriver(t.Context(), 1, stop, wake)

		require.Equal(t, 1, ops.claims, "a driver must not claim again after the claim gate closes")
	}
}

// openClaimGate returns a claim gate that never closes, for tests that drive a
// tick or the claim ladder directly and are not about shutdown.
func openClaimGate() <-chan struct{} {
	return make(chan struct{})
}

// closedClaimGate returns a claim gate StopClaiming has already closed.
func closedClaimGate() <-chan struct{} {
	stop := make(chan struct{})
	close(stop)
	return stop
}

// gateClosingStopProbeStore closes the claim gate from inside the first rung of
// the ladder, the stop-reconciliation probe, so the rungs behind it run after
// claiming has stopped.
type gateClosingStopProbeStore struct {
	*operationClaimApplyStore
	closeGate func()
}

func (s *gateClosingStopProbeStore) FindNextApplyForStopReconciliation(ctx context.Context, owner string) (*storage.Apply, error) {
	if s.closeGate != nil {
		s.closeGate()
		s.closeGate = nil
	}
	return s.operationClaimApplyStore.FindNextApplyForStopReconciliation(ctx, owner)
}

// A driver is admitted to a claim pass while the gate is open, and claiming
// stops while its first rung is still querying. The rungs behind it must not
// claim: each one re-reads the gate before its own query, so a pass that
// overlaps StopClaiming claims nothing after the closure it can observe.
func TestDriverAdmittedBeforeTheClaimGateClosesClaimsNothingAfterIt(t *testing.T) {
	ops := &claimLadderOperationStore{recoverOperationStore: &recoverOperationStore{}}
	stop := make(chan struct{})
	applies := &gateClosingStopProbeStore{operationClaimApplyStore: &operationClaimApplyStore{}, closeGate: func() { close(stop) }}
	store := &mockStorageWithApplyStores{applies: applies, operations: ops}
	svc := New(store, testServerConfig(), nil, slog.New(slog.DiscardHandler))
	require.NoError(t, svc.SetOperatorPollInterval(time.Hour))

	svc.operatorDriver(t.Context(), 1, stop, make(chan struct{}, 1))

	assert.Equal(t, 1, applies.stopProbes, "the rung that was already running finishes")
	assert.Zero(t, ops.claims, "no rung behind the closure may claim")
}

// Claiming has already stopped when a driver and a reaper start, as happens
// when StopClaiming lands during StartOperator. Neither runs a pass: the driver
// exits without a claim and the reaper without a sweep.
func TestDriverAndReaperStartedAfterTheClaimGateClosesRunNoPass(t *testing.T) {
	ops := &claimLadderOperationStore{recoverOperationStore: &recoverOperationStore{}}
	svc, _ := claimLadderService(ops)
	stop := closedClaimGate()

	svc.operatorDriver(t.Context(), 1, stop, make(chan struct{}, 1))
	assert.Zero(t, ops.claims, "a driver started after claiming stopped must not claim")

	passes := 0
	svc.reaperLoop(t.Context(), stop, time.Hour, "probe", func(context.Context) { passes++ })
	assert.Zero(t, passes, "a reaper started after claiming stopped must not sweep")
}

// A tick admitted just before StopClaiming re-reads the gate on entry, so a
// pass that starts after the closure does not begin the ladder at all.
func TestDriveTickSkipsTheClaimLadderOnceClaimingStopped(t *testing.T) {
	ops := &claimLadderOperationStore{recoverOperationStore: &recoverOperationStore{}}
	applies := &operationClaimApplyStore{}
	store := &mockStorageWithApplyStores{applies: applies, operations: ops}
	svc := New(store, testServerConfig(), nil, slog.New(slog.DiscardHandler))

	svc.driveTick(t.Context(), 1, closedClaimGate())

	assert.Zero(t, applies.stopProbes, "a tick that starts after claiming stopped must not begin the ladder")
	assert.Zero(t, ops.claims, "a tick that starts after claiming stopped must not claim")
}
