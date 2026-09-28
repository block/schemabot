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
