package api

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// haltingTernClient records whether shutdown halted it, and how many operator
// drivers were still running when it did.
type haltingTernClient struct {
	*mockTernClient
	haltErr     error
	halts       atomic.Int64
	driversSeen atomic.Int64
	svc         *Service
	order       *shutdownOrderRecorder
}

func (c *haltingTernClient) HaltForShutdown(context.Context) error {
	c.halts.Add(1)
	if c.svc != nil {
		c.driversSeen.Store(c.svc.driversBusy.Load())
	}
	if c.order != nil {
		c.order.record("halt")
	}
	return c.haltErr
}

// shutdownOrderRecorder records shutdown's externally visible steps in the order
// they happened, so a test can pin the sequence rather than only that each step
// ran at all.
type shutdownOrderRecorder struct {
	mu    sync.Mutex
	steps []string
}

func (r *shutdownOrderRecorder) record(step string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.steps = append(r.steps, step)
}

func (r *shutdownOrderRecorder) recorded() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.steps)
}

// orderedApplyStore notes when the apply claim was handed back, relative to the
// other shutdown steps.
type orderedApplyStore struct {
	*capturingApplyStore
	order *shutdownOrderRecorder
}

func (s *orderedApplyStore) ReleaseClaim(ctx context.Context, lease storage.ApplyLease) (bool, error) {
	s.order.record("release-apply")
	return s.capturingApplyStore.ReleaseClaim(ctx, lease)
}

// orderedOperationStore notes when the operation claim was handed back, and
// records the leases it was asked to release.
type orderedOperationStore struct {
	*queuedOperationClaimStore
	order    *shutdownOrderRecorder
	mu       sync.Mutex
	released []storage.OperationLease
}

func (s *orderedOperationStore) ReleaseClaim(_ context.Context, lease storage.OperationLease) (bool, error) {
	s.order.record("release-operation")
	s.mu.Lock()
	defer s.mu.Unlock()
	s.released = append(s.released, lease)
	return true, nil
}

func (s *orderedOperationStore) releasedLeases() []storage.OperationLease {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.released)
}

// newShutdownOrderTestService builds an operator-capable service whose engine
// halt and claim handbacks all report into one recorder.
func newShutdownOrderTestService(t *testing.T, apply *storage.Apply) (*Service, *haltingTernClient, *orderedApplyStore, *orderedOperationStore) {
	t.Helper()
	order := &shutdownOrderRecorder{}
	client := &haltingTernClient{mockTernClient: &mockTernClient{}, order: order}
	tasks := &capturingTaskStore{}
	base := &capturingApplyStore{apply: apply, taskStore: tasks}
	applies := &orderedApplyStore{capturingApplyStore: base, order: order}
	operations := &orderedOperationStore{queuedOperationClaimStore: &queuedOperationClaimStore{applies: base}, order: order}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{
		plans:      &staticPlanStore{plan: trustedQueueApplyTestPlan()},
		applies:    applies,
		tasks:      tasks,
		locks:      &emptyLockStore{},
		applyLogs:  &noopApplyLogStore{},
		controls:   &memoryControlRequestStore{},
		operations: operations,
	}, testServerConfig(), map[string]tern.Client{"default/staging": client}, logger)
	client.svc = svc
	require.NoError(t, svc.SetOperatorPollInterval(time.Hour))
	return svc, client, applies, operations
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

// Shutdown's steps only work in one order. Handing a claim back before the
// engines are down invites a peer driver onto a target this process still holds
// its lock on, and the peer is refused. Handing the operation back before its
// parent apply invites the peer to claim work whose parent it cannot claim, so
// it has to reconcile its way out instead of resuming. Engines come down first,
// then the parent apply, then the operation a peer claims through.
func TestStopOperatorHandsClaimsBackOnlyAfterEnginesAreDown(t *testing.T) {
	apply := &storage.Apply{
		ID: 123, ApplyIdentifier: "apply-123", Database: "orders",
		Environment: "staging", State: state.Apply.Running,
		LeaseOwner: "departing/driver-0", LeaseToken: "held-token",
	}
	operation := &storage.ApplyOperation{
		ID: 7, ApplyID: 123, Deployment: DefaultDeployment,
		LeaseOwner: "departing/driver-0", LeaseToken: "held-operation-token",
	}
	svc, client, applies, operations := newShutdownOrderTestService(t, apply)
	svc.trackHeldClaim(apply)
	svc.trackHeldOperationClaim(operation, operation.Lease())

	svc.StartOperator(t.Context())
	svc.StopOperator()

	require.Len(t, applies.releasedClaimLeases(), 1, "the apply claim this process was driving under is handed back")
	require.Len(t, operations.releasedLeases(), 1, "the operation claim the drive was actually claimed under is handed back too")
	assert.Equal(t, "held-operation-token", operations.releasedLeases()[0].Token)
	assert.Equal(t, []string{"halt", "release-apply", "release-operation"}, client.order.recorded())
}

// A driver reaches work through its apply_operation, so an operation left
// claimed idles the schema change for the whole staleness window even when the
// parent apply is already claimable. Shutdown hands the operation back too.
func TestReleaseHeldOperationClaimsHandsBackAClaimedOperation(t *testing.T) {
	operation := &storage.ApplyOperation{
		ID: 7, ApplyID: 123, Deployment: DefaultDeployment,
		LeaseOwner: "departing/driver-0", LeaseToken: "held-operation-token",
	}
	svc, _, _, operations := newShutdownOrderTestService(t, &storage.Apply{ID: 123, ApplyIdentifier: "apply-123"})

	svc.releaseHeldOperationClaims([]heldOperationClaim{{lease: operation.Lease(), logAttrs: operation.LogAttrs()}})

	released := operations.releasedLeases()
	require.Len(t, released, 1)
	assert.Equal(t, int64(7), released[0].OperationID)
	assert.Equal(t, "held-operation-token", released[0].Token)
}

// A drive registers the operation claim it is running under and deregisters it
// on the way out, so shutdown hands back exactly the operation claims still in
// flight — never one a peer driver has since rotated onto itself.
func TestTrackHeldOperationClaimRegistersOnlyTheClaimsInFlight(t *testing.T) {
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{}, &capturingApplyStore{})
	operation := &storage.ApplyOperation{
		ID: 7, ApplyID: 123, Deployment: DefaultDeployment,
		LeaseOwner: "driver-0", LeaseToken: "held-operation-token",
	}

	done := svc.trackHeldOperationClaim(operation, operation.Lease())
	claimed := svc.heldOperationClaimsSnapshot()
	require.Len(t, claimed, 1)
	assert.Equal(t, operation.Lease(), claimed[0].lease)

	// A peer rotates the operation lease onto itself mid-drive.
	rotated := &storage.ApplyOperation{
		ID: 7, ApplyID: 123, Deployment: DefaultDeployment,
		LeaseOwner: "driver-1", LeaseToken: "rotated-operation-token",
	}
	svc.trackHeldOperationClaim(rotated, rotated.Lease())

	done()
	held := svc.heldOperationClaimsSnapshot()
	require.Len(t, held, 1, "a drive must not deregister an operation claim another driver now holds")
	assert.Equal(t, rotated.Lease(), held[0].lease)
}

// A drive that never held a usable operation lease has no claim to hand back,
// so nothing is registered for shutdown to release.
func TestTrackHeldOperationClaimIgnoresAnInvalidLease(t *testing.T) {
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), &mockTernClient{}, &capturingApplyStore{})
	operation := &storage.ApplyOperation{ID: 7, ApplyID: 123, Deployment: DefaultDeployment}

	done := svc.trackHeldOperationClaim(operation, operation.Lease())

	assert.Empty(t, svc.heldOperationClaimsSnapshot())
	done()
}
