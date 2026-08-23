package api

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// The driver-occupancy gauges answer how much claim capacity remains: each
// successful claim marks a driver busy until its drive returns, so pool size
// minus busy — summed across processes — is the number of drivers still free
// to pick up queued work. This verifies overlapping claims stack in the busy
// gauge and each release frees exactly one slot.
func TestMarkDriverBusyTracksHeldClaims(t *testing.T) {
	reader := newStuckPendingMetricReader(t)
	svc := newOperatorTestService(nil)

	firstIdle := svc.markDriverBusy(t.Context())
	assert.Equal(t, int64(1), gaugeValue(t, reader, "schemabot.operator.drivers_busy"))

	secondIdle := svc.markDriverBusy(t.Context())
	assert.Equal(t, int64(2), gaugeValue(t, reader, "schemabot.operator.drivers_busy"))

	firstIdle()
	assert.Equal(t, int64(1), gaugeValue(t, reader, "schemabot.operator.drivers_busy"))

	secondIdle()
	assert.Equal(t, int64(0), gaugeValue(t, reader, "schemabot.operator.drivers_busy"))
}

// claimedStopReconciliationApplyStore serves a fixed stop-reconciliation claim.
// It embeds the interface so only that method needs an implementation; any
// other call panics, which keeps the test honest about the code path it covers.
type claimedStopReconciliationApplyStore struct {
	storage.ApplyStore
	apply *storage.Apply
}

func (s *claimedStopReconciliationApplyStore) FindNextApplyForStopReconciliation(context.Context, string) (*storage.Apply, error) {
	return s.apply, nil
}

// A claim that cannot be driven — here a stop-reconciliation claim that came
// back without a valid lease token — must still release the driver's busy slot
// on exit. A slot that leaks on an early exit would ratchet the busy gauge
// upward until it reports a saturated pool that is actually idle.
func TestRecoverApplyPendingStopReleasesBusySlotOnInvalidLease(t *testing.T) {
	reader := newStuckPendingMetricReader(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{
		applies: &claimedStopReconciliationApplyStore{apply: &storage.Apply{
			ID:              7,
			ApplyIdentifier: "apply-7",
			Database:        "appdb",
			Deployment:      "east",
			Environment:     "staging",
		}},
	}, testServerConfig(), nil, logger)

	consumed := svc.recoverApplyPendingStop(t.Context(), 1, driverLeaseOwner(1))

	assert.True(t, consumed, "a claimed apply consumes the tick even when its lease is invalid")
	assert.Equal(t, int64(0), gaugeValue(t, reader, "schemabot.operator.drivers_busy"),
		"the busy slot must be released when the claim exits without driving")
}

// Starting the operator seeds both occupancy gauges so the pool's capacity is
// visible before any claim: driver_pool_size reports the configured pool size
// and drivers_busy starts at zero.
func TestStartOperatorSeedsDriverPoolGauges(t *testing.T) {
	reader := newStuckPendingMetricReader(t)
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{}, &capturingApplyStore{})
	svc.config.Drivers = 3
	require.NoError(t, svc.SetOperatorPollInterval(time.Hour))
	svc.StartOperator(t.Context())
	t.Cleanup(svc.StopOperator)

	assert.Equal(t, int64(3), gaugeValue(t, reader, "schemabot.operator.driver_pool_size"))
	assert.Equal(t, int64(0), gaugeValue(t, reader, "schemabot.operator.drivers_busy"))
}
