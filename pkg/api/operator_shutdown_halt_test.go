package api

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// haltingTernClient records whether shutdown halted it, and how many operator
// drivers were still running when it did.
type haltingTernClient struct {
	*mockTernClient
	haltErr     error
	halts       atomic.Int64
	driversSeen atomic.Int64
	svc         *Service
}

func (c *haltingTernClient) HaltForShutdown(context.Context) error {
	c.halts.Add(1)
	if c.svc != nil {
		c.driversSeen.Store(c.svc.driversBusy.Load())
	}
	return c.haltErr
}

// An engine that runs its schema change inside this process keeps the target's
// lock for as long as its work lives, and that work outlives the drive that
// started it. Shutting down without bringing it down leaves the target locked
// while the process no longer renews the apply's lease, so every driver that
// reclaims the apply is refused and burns a recovery attempt. Stopping the
// operator must halt those engines, and only after the drives it cancelled have
// returned — a halt racing a live drive would land while the drive is still
// reading engine state.
func TestStopOperatorHaltsInProcessEnginesAfterDrivesReturn(t *testing.T) {
	client := &haltingTernClient{mockTernClient: &mockTernClient{}}
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), client, &capturingApplyStore{})
	client.svc = svc
	require.NoError(t, svc.SetOperatorPollInterval(time.Hour))

	svc.StartOperator(t.Context())
	svc.StopOperator()

	assert.Equal(t, int64(1), client.halts.Load(), "the client's engine is halted exactly once on shutdown")
	assert.Equal(t, int64(0), client.driversSeen.Load(),
		"the halt runs after the cancelled drives have returned, not alongside them")
}

// Shutdown must not stop at the first engine that will not come down: every
// other target would stay locked behind it. Each failure is reported and the
// remaining clients are still halted.
func TestStopOperatorHaltsEveryClientDespiteFailures(t *testing.T) {
	failing := &haltingTernClient{mockTernClient: &mockTernClient{}, haltErr: errors.New("runner still copying")}
	healthy := &haltingTernClient{mockTernClient: &mockTernClient{}}
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), failing, &capturingApplyStore{})
	svc.RegisterTernClient("west", "staging", healthy)
	require.NoError(t, svc.SetOperatorPollInterval(time.Hour))

	svc.StartOperator(t.Context())
	svc.StopOperator()

	assert.Equal(t, int64(1), failing.halts.Load())
	assert.Equal(t, int64(1), healthy.halts.Load(), "a client that will not come down does not leave its peers held")
}

// A deployment can mix targets driven in this process with targets served by a
// separate Tern service. A client that delegates to that service owns no engines
// here — it halts its own on its own shutdown — so shutdown skips it and still
// halts the local ones rather than failing on the one it cannot halt.
func TestStopOperatorSkipsClientsWithoutInProcessEngines(t *testing.T) {
	remote := &mockTernClient{}
	local := &haltingTernClient{mockTernClient: &mockTernClient{}}
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), remote, &capturingApplyStore{})
	svc.RegisterTernClient("west", "staging", local)
	require.NoError(t, svc.SetOperatorPollInterval(time.Hour))

	svc.StartOperator(t.Context())
	svc.StopOperator()

	assert.Equal(t, int64(1), local.halts.Load(), "the in-process target is still halted")
}

// A process going away holds claims on the applies its drivers were driving.
// Left alone, those claims stay fresh until the staleness window elapses, so
// the work sits idle for a full minute before any peer driver can pick it up.
// Shutdown hands them back instead, making the apply claimable on the next
// poll.
func TestReleaseHeldClaimsHandsBackAnActiveApply(t *testing.T) {
	apply := &storage.Apply{
		ID: 123, ApplyIdentifier: "apply-123", Database: "orders",
		Environment: "staging", State: state.Apply.Running,
		LeaseOwner: "departing/driver-0", LeaseToken: "held-token",
	}
	applies := &capturingApplyStore{apply: apply}
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{}, applies)

	svc.releaseHeldClaims([]heldClaim{{lease: apply.Lease(), logAttrs: apply.IdentityLogAttrs()}})

	released := applies.releasedClaimLeases()
	require.Len(t, released, 1, "the claim on a still-active apply is handed back")
	assert.Equal(t, "held-token", released[0].Token)
}

// An apply that finished while shutdown was in progress has nothing to hand
// over, and backdating its heartbeat would misreport when it settled. Shutdown
// leaves it alone.
func TestReleaseHeldClaimsLeavesSettledAppliesAlone(t *testing.T) {
	apply := &storage.Apply{
		ID: 123, ApplyIdentifier: "apply-123", Database: "orders",
		Environment: "staging", State: state.Apply.Completed,
		LeaseOwner: "departing/driver-0", LeaseToken: "held-token",
	}
	applies := &capturingApplyStore{apply: apply}
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{}, applies)

	svc.releaseHeldClaims([]heldClaim{{lease: apply.Lease(), logAttrs: apply.IdentityLogAttrs()}})

	assert.Empty(t, applies.releasedClaimLeases(), "a settled apply is not handed back")
}

// A drive registers the claim it is driving under and deregisters it on the way
// out, so shutdown hands back exactly the claims still in flight — never one a
// peer driver has since rotated onto itself.
func TestTrackHeldClaimRegistersOnlyTheClaimsInFlight(t *testing.T) {
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{}, &capturingApplyStore{})
	apply := &storage.Apply{
		ID: 123, ApplyIdentifier: "apply-123", Database: "orders",
		Environment: "staging", LeaseOwner: "driver-0", LeaseToken: "held-token",
	}

	done := svc.trackHeldClaim(apply)
	claimed := svc.heldClaimsSnapshot()
	require.Len(t, claimed, 1)
	assert.Equal(t, apply.Lease(), claimed[0].lease)
	assert.Contains(t, claimed[0].logAttrs, "apply-123",
		"the apply's identity is captured while the apply is in hand, so shutdown can name it even if the reload fails")

	// A peer rotates the lease onto itself mid-drive.
	rotated := &storage.Apply{
		ID: 123, ApplyIdentifier: "apply-123", Database: "orders",
		Environment: "staging", LeaseOwner: "driver-1", LeaseToken: "rotated-token",
	}
	svc.trackHeldClaim(rotated)

	done()
	held := svc.heldClaimsSnapshot()
	require.Len(t, held, 1, "a drive must not deregister a claim another driver now holds")
	assert.Equal(t, rotated.Lease(), held[0].lease)
}

// A drive that never held a usable lease has no claim to hand back, so nothing
// is registered for shutdown to release.
func TestTrackHeldClaimIgnoresAnInvalidLease(t *testing.T) {
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{}, &capturingApplyStore{})

	done := svc.trackHeldClaim(&storage.Apply{ID: 123, ApplyIdentifier: "apply-123"})

	assert.Empty(t, svc.heldClaimsSnapshot())
	done()
}
