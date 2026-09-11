package api

import (
	"bytes"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// A drive that never returns must not take the process down with it. Shutdown
// cancels the pool and waits, and a drive still running when that wait is spent
// is abandoned: its claim is left to go stale so a peer's stranded reaper can
// reclaim the apply, which is the recovery that a process refusing to exit is
// standing in the way of. Shutdown names what it walked away from, so the apply
// can be followed to whichever process picks it up.
func TestStopOperatorAbandonsADriveThatIgnoresItsContext(t *testing.T) {
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{}, &capturingApplyStore{})
	require.NoError(t, svc.SetOperatorPollInterval(time.Hour))

	var logs bytes.Buffer
	svc.logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	svc.StartOperator(t.Context())

	// A drive that observes neither its cancelled context nor the stop channel.
	// Releasing it at the end of the test keeps the goroutine from outliving the
	// run, which is a luxury the production case does not have.
	stuck := make(chan struct{})
	t.Cleanup(func() { close(stuck) })
	svc.recoveryWg.Go(func() { <-stuck })

	// The claim the stuck drive is running under, which shutdown finds still
	// registered and reports rather than hands back.
	apply := &storage.Apply{ApplyIdentifier: "apply_stuck", Database: "orders", Deployment: "west"}
	svc.heldClaims[7] = heldClaim{
		lease:      storage.ApplyLease{ApplyID: 7},
		deployment: apply.Deployment,
		logAttrs:   apply.IdentityLogAttrs(),
	}

	start := time.Now()
	svc.StopOperator()
	elapsed := time.Since(start)

	assert.GreaterOrEqual(t, elapsed, driverDrainTimeout, "shutdown gives the drive its full drain before abandoning it")
	assert.Less(t, elapsed, driverDrainTimeout+5*time.Second, "shutdown returns on the drain rather than on the drive")

	output := logs.String()
	assert.Contains(t, output, "drivers did not return within the shutdown drain")
	assert.Contains(t, output, "abandoned an apply whose driver did not return")
	assert.Contains(t, output, "apply_stuck")

	// The claim is still registered: handing it back while its drive may still
	// be writing would invite a peer onto a target this process has not let go
	// of. It goes stale instead, and is reclaimed on staleness.
	assert.Len(t, svc.heldClaimsSnapshot(), 1)
}
