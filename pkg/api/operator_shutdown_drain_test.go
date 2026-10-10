package api

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
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

// blockingExpiryApplyStore parks the retryable-expiry reaper inside storage and
// ignores the context while it is there, which is the production shape of a
// maintenance pass that will not return: a query against a database that has
// stopped answering. reaperLoop runs its first pass before its first tick, so
// the reaper is parked from the moment the operator starts.
type blockingExpiryApplyStore struct {
	*capturingApplyStore

	release   <-chan struct{}
	entered   chan struct{}
	enterOnce sync.Once
}

func (s *blockingExpiryApplyStore) ExpireRetryable(context.Context, int) ([]*storage.RetryableApplyExpiration, error) {
	s.enterOnce.Do(func() { close(s.entered) })
	<-s.release
	return nil, nil
}

// A reaper pass that never returns costs only its own bound. The reapers stop on
// the same signal as the drivers but hold no claim and drive nothing, so a stuck
// one has nothing the later stages would run against. Deciding the drives' fate
// on it would leave this process's engines copying and its healthy claims to
// expire for nothing, which is the recovery those stages exist to avoid.
func TestStopOperatorBringsDrivesDownWhenOnlyAReaperIsStuck(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	store := &blockingExpiryApplyStore{
		capturingApplyStore: &capturingApplyStore{},
		release:             release,
		entered:             make(chan struct{}),
	}

	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{}, store)
	require.NoError(t, svc.SetOperatorPollInterval(time.Hour))

	var logs bytes.Buffer
	svc.logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Every driver returns normally; the real retryable-expiry reaper is the one
	// goroutine that does not, parked inside storage.
	svc.StartOperator(t.Context())
	select {
	case <-store.entered:
	case <-time.After(monitorDrainTimeout):
		t.Fatal("the retryable-expiry reaper never reached storage")
	}

	start := time.Now()
	svc.StopOperator()
	elapsed := time.Since(start)

	assert.GreaterOrEqual(t, elapsed, monitorDrainTimeout, "the reaper gets its own drain before being abandoned")
	assert.Less(t, elapsed, monitorDrainTimeout+5*time.Second, "stopping returns on the reaper's bound, not on the reaper")

	output := logs.String()
	assert.Contains(t, output, "background monitor did not return within the shutdown drain")
	assert.Contains(t, output, "operator_reapers")
	assert.NotContains(t, output, "drivers did not return within the shutdown drain",
		"the drivers returned; only the reaper did not")
	assert.NotContains(t, output, "abandoned an apply whose driver did not return")
}

// Past its bound a monitor loop is still running, so a caller must not announce
// that it stopped. An operator reading that a cleaner stopped, while it is in
// fact still executing against a database that has stopped answering, is reading
// the opposite of what is true at the one moment the distinction matters.
func TestStopPendingDropsCleanerDoesNotReportStoppedWhenItOutlastsItsDrain(t *testing.T) {
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{}, &capturingApplyStore{})

	var logs bytes.Buffer
	svc.logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// The running cleaner is set up directly rather than through
	// StartPendingDropsCleaner, whose own preconditions — the quarantine being
	// on, and a local target to reap — decide whether the loop starts and say
	// nothing about how its stop reports. This is the state that start leaves
	// behind: a cancel to call, and a loop in the group to wait for.
	_, cancel := context.WithCancel(t.Context())
	svc.pendingDropsCancel = cancel

	stuck := make(chan struct{})
	t.Cleanup(func() { close(stuck) })
	svc.pendingDropsWg.Go(func() { <-stuck })

	start := time.Now()
	svc.StopPendingDropsCleaner()
	elapsed := time.Since(start)

	assert.GreaterOrEqual(t, elapsed, monitorDrainTimeout, "the cleaner gets its full drain before being abandoned")
	assert.Less(t, elapsed, monitorDrainTimeout+5*time.Second, "stopping returns on the drain rather than on the cleaner")

	output := logs.String()
	assert.Contains(t, output, "background monitor did not return within the shutdown drain")
	assert.Contains(t, output, "pending_drops_cleaner")
	assert.NotContains(t, output, "pending drops cleaner stopped")
}
