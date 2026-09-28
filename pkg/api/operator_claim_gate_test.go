package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/drain"
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
