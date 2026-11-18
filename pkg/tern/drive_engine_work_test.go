package tern

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// ownedWorkEngine records the owner each start ran under and each owner a
// halt was scoped to. A halt blocks until its context ends when block is set,
// standing in for work that is slow to come down.
type ownedWorkEngine struct {
	engine.Engine
	block bool

	mu          sync.Mutex
	startOwners []string
	haltOwners  []string
	shutdowns   int
}

func (e *ownedWorkEngine) Name() string { return "owned-work" }

func (e *ownedWorkEngine) Apply(ctx context.Context, _ *engine.ApplyRequest) (*engine.ApplyResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.startOwners = append(e.startOwners, engine.WorkOwnerFromContext(ctx))
	return &engine.ApplyResult{Accepted: true}, nil
}

func (e *ownedWorkEngine) HaltForShutdown(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.shutdowns++
	return nil
}

func (e *ownedWorkEngine) HaltWorkOwnedBy(ctx context.Context, owner string) error {
	e.mu.Lock()
	e.haltOwners = append(e.haltOwners, owner)
	block := e.block
	e.mu.Unlock()
	if block {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (e *ownedWorkEngine) halts() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.haltOwners...)
}

func newOwnedWorkClient(eng engine.Engine) *LocalClient {
	return &LocalClient{
		config:       LocalConfig{Database: "appdb", Type: "owned-work"},
		customEngine: eng,
		logger:       slog.Default(),
	}
}

// driveContext is the context a drive of apply 1 runs under for the claim
// with token.
func driveContext(ctx context.Context, token string) context.Context {
	return storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: 1, Owner: "driver", Token: token})
}

// Two drives of the same apply hold different claims, so they are different
// owners; a drive's nested calls share its claim and so its owner.
func TestDriveWorkOwnerNamesTheClaim(t *testing.T) {
	driveA := driveContext(t.Context(), "token-a")
	driveB := driveContext(t.Context(), "token-b")

	assert.NotEqual(t, driveWorkOwner(driveA), driveWorkOwner(driveB))
	assert.Equal(t, driveWorkOwner(driveA), driveWorkOwner(context.WithoutCancel(driveA)))
	dual := storage.WithOperationLease(driveA, storage.OperationLease{ApplyID: 1, OperationID: 2, Owner: "driver", Token: "op-token"})
	assert.NotEqual(t, driveWorkOwner(driveA), driveWorkOwner(dual), "the operation claim is part of the drive's identity")
	assert.Empty(t, driveWorkOwner(t.Context()), "a caller with no claim is the empty owner, which halts nothing")
}

// The engine is shared by every drive of the target in this process. Drive A
// hands its apply back, and drive B claims it here and starts its own run
// before A's exit halt runs. A's halt reaches only the run A started, so B's
// run keeps going under B's claim.
func TestDriveExitHaltReachesOnlyTheDrivesOwnWork(t *testing.T) {
	eng := &ownedWorkEngine{}
	client := newOwnedWorkClient(eng)
	driveA := driveContext(t.Context(), "token-a")
	driveB := driveContext(t.Context(), "token-b")

	_, err := client.applyWithEngine(driveA, eng, &engine.ApplyRequest{Database: "appdb"})
	require.NoError(t, err)
	_, err = client.applyWithEngine(driveB, eng, &engine.ApplyRequest{Database: "appdb"})
	require.NoError(t, err)
	client.haltEngineWorkLeftByDrive(driveA, slog.Default())

	assert.Equal(t, []string{driveWorkOwner(driveA), driveWorkOwner(driveB)}, eng.startOwners, "each run is started under its drive's owner")
	assert.Equal(t, []string{driveWorkOwner(driveA)}, eng.halts(), "drive A's exit halt is scoped to drive A's work")
	assert.Zero(t, eng.shutdowns, "a drive's exit never halts every run on the engine")
}

// Every run started without a claim belongs to the same empty owner, so a
// drive that returns with no claim on its context has no work of its own to
// name. Its exit halt reaches nothing rather than every unowned run.
func TestDriveExitHaltWithoutAClaimHaltsNothing(t *testing.T) {
	eng := &ownedWorkEngine{}
	client := newOwnedWorkClient(eng)

	_, err := client.applyWithEngine(t.Context(), eng, &engine.ApplyRequest{Database: "appdb"})
	require.NoError(t, err)
	client.haltEngineWorkLeftByDrive(t.Context(), slog.Default())

	assert.Equal(t, []string{""}, eng.startOwners, "the run is started under the empty owner")
	assert.Empty(t, eng.halts(), "the exit halt does not reach the unowned run")
	assert.Zero(t, eng.shutdowns)
}

// A drive that was cancelled for its own reasons and is still halting its
// work when the operator begins shutting down gives the halt up to the
// shutdown. The shutdown waits for its drives for less than the halt's bound,
// so a drive that kept halting would cost every drive its engine halt and
// claim hand-back.
func TestShutdownTakesOverADriveHaltAlreadyUnderway(t *testing.T) {
	eng := &ownedWorkEngine{block: true}
	client := newOwnedWorkClient(eng)
	operatorCtx, shutDown := NewOperatorContext(t.Context())
	defer shutDown()
	drive, stall := context.WithCancel(driveContext(operatorCtx, "token-a"))
	stall()

	halted := make(chan struct{})
	go func() {
		defer close(halted)
		client.haltEngineWorkLeftByDrive(drive, slog.Default())
	}()
	require.Eventually(t, func() bool { return len(eng.halts()) == 1 }, driveHaltTestDeadline, time.Millisecond,
		"the stalled drive halts its own work")

	shutDown()
	select {
	case <-halted:
	case <-time.After(driveHaltTestDeadline):
		require.FailNow(t, "the drive kept halting after the operator began shutting down")
	}
}

// The operator's own parent context can be cancelled before the operator is
// stopped, which ends every drive with a cause that does not say shutdown.
// Each drive then halts its own work, and a shutdown that begins while it does
// still takes the halt over.
func TestShutdownTakesOverAHaltAfterTheOperatorsParentIsCancelled(t *testing.T) {
	eng := &ownedWorkEngine{block: true}
	client := newOwnedWorkClient(eng)
	parent, cancelParent := context.WithCancel(t.Context())
	operatorCtx, shutDown := NewOperatorContext(parent)
	defer shutDown()
	drive := driveContext(operatorCtx, "token-a")
	cancelParent()

	halted := make(chan struct{})
	go func() {
		defer close(halted)
		client.haltEngineWorkLeftByDrive(drive, slog.Default())
	}()
	require.Eventually(t, func() bool { return len(eng.halts()) == 1 }, driveHaltTestDeadline, time.Millisecond,
		"the drive ended by its parent halts its own work")

	shutDown()
	select {
	case <-halted:
	case <-time.After(driveHaltTestDeadline):
		require.FailNow(t, "the drive kept halting after the operator began shutting down")
	}
}

// A drive whose own cancellation came first, but which returns after the
// operator has begun shutting down, leaves the halt to the shutdown.
func TestDriveReturningDuringShutdownLeavesTheHaltToIt(t *testing.T) {
	eng := &ownedWorkEngine{}
	client := newOwnedWorkClient(eng)
	operatorCtx, shutDown := NewOperatorContext(t.Context())
	drive, stall := context.WithCancel(driveContext(operatorCtx, "token-a"))
	stall()
	shutDown()

	client.haltEngineWorkLeftByDrive(drive, slog.Default())

	assert.Empty(t, eng.halts())
}

// driveHaltTestDeadline bounds a wait on a drive's exit halt.
const driveHaltTestDeadline = 5 * time.Second

// A drive's exit halt runs on the claim the drive held. For a drive cancelled
// while its heartbeat still renews the claim, the last renewal is at most one
// heartbeat interval old, so however long the halt takes it gives up before
// the claim can go stale and a peer is never invited onto a target the halt is
// still bringing down. A drive ended because its heartbeat failed for the
// whole window starts the halt with a stale claim; owner scoping and
// lease-guarded writes keep that halt safe, not this bound.
func TestDriveExitHaltEndsBeforeTheClaimCanGoStale(t *testing.T) {
	assert.Less(t, defaultHeartbeatInterval+driveHandoverHaltTimeout, storage.ApplyLeaseStaleAfter)
}

// haltRecordingControlEngine is a control engine whose in-process work can be
// halted per owner, recording each owner a halt was scoped to.
type haltRecordingControlEngine struct {
	*fakeControlEngine
	haltOwners []string
}

func (e *haltRecordingControlEngine) HaltWorkOwnedBy(_ context.Context, owner string) error {
	e.haltOwners = append(e.haltOwners, owner)
	return nil
}

// updateRefusingApplyStore refuses every apply write.
type updateRefusingApplyStore struct {
	*exactProgressApplyStore
	err error
}

func (s *updateRefusingApplyStore) Update(context.Context, *storage.Apply) error { return s.err }

// A grouped resume whose engine has accepted the reattach can still return
// before its poll begins: here the apply's running state cannot be written.
// The drive halts the engine work it started as it returns, scoped to its own
// claim, rather than leave the work holding the target behind a drive that no
// longer polls it.
func TestLaunchAtomicResume_ExitBeforeThePollHaltsTheAcceptedWork(t *testing.T) {
	operations := &exactProgressApplyOperationStore{data: &storage.EngineResumeState{
		ApplyOperationID: 7,
		MigrationContext: "ctx-reattach",
		Metadata:         `{"branch_name":"branch-1","deploy_request_id":5}`,
	}}
	accepted := &engine.ApplyResult{Accepted: true, ResumeState: &engine.ResumeState{
		MigrationContext: "ctx-reattach",
		Metadata:         `{"branch_name":"branch-1","deploy_request_id":5}`,
	}}
	client, apply, tasks, plan, applyStore := reattachResumeFixture(operations, accepted)
	eng := &haltRecordingControlEngine{fakeControlEngine: client.planetscaleEngine.(*fakeControlEngine)}
	client.planetscaleEngine = eng
	client.storage.(*exactProgressStorage).applies = &updateRefusingApplyStore{exactProgressApplyStore: applyStore, err: errors.New("storage unavailable")}
	drive := driveContext(t.Context(), "token-a")

	err := client.launchAtomicResume(drive, apply, tasks, plan, apply.GetOptions().Map(), "Recovering from checkpoint", true, false, false)

	require.ErrorContains(t, err, "storage unavailable")
	assert.Equal(t, []string{driveWorkOwner(drive)}, eng.haltOwners, "the accepted work is halted under the drive's own owner")
	assert.Equal(t, state.Apply.Recovering, applyStore.apply.State, "the apply stays recoverable for a later drive")
}

// A hold is escalated once it has lasted past the bound, and once per hold.
// The grouped drive's refusals come a lease-staleness window apart, and they
// still read as one hold.
func TestTargetHeldWaitsEscalateOncePerHold(t *testing.T) {
	var waits targetHeldWaits
	start := time.Now()

	_, escalate := waits.observe(1, "", start)
	assert.False(t, escalate)
	_, escalate = waits.observe(1, "", start.Add(storage.ApplyLeaseStaleAfter))
	assert.False(t, escalate, "a hand-back cycle later, the hold has not yet lasted past the bound")
	heldFor, escalate := waits.observe(1, "", start.Add(targetHeldEscalationAfter))
	assert.True(t, escalate)
	assert.Equal(t, targetHeldEscalationAfter, heldFor)
	_, escalate = waits.observe(1, "", start.Add(targetHeldEscalationAfter+time.Second))
	assert.False(t, escalate, "the hold is escalated once")

	_, escalate = waits.observe(2, "", start)
	assert.False(t, escalate, "another apply's hold is measured on its own")

	later := start.Add(targetHeldEscalationAfter + targetHeldRefusalGap + time.Minute)
	heldFor, escalate = waits.observe(1, "", later)
	assert.False(t, escalate)
	assert.Zero(t, heldFor, "a refusal long after the last one starts a new hold")

	waits.clear(1)
	heldFor, _ = waits.observe(1, "", later.Add(time.Second))
	assert.Zero(t, heldFor, "a start that got past the refusal ends the hold")
}

// A sequential apply refused on one table and then another is two holds. The
// first table's escalation, and the time it was held, do not carry over to the
// second, which escalates under its own name once it has been held past the
// bound itself, even when the first settled without starting again.
func TestTargetHeldWaitsMeasureEachRefusedTableOnItsOwn(t *testing.T) {
	var waits targetHeldWaits
	start := time.Now()

	_, escalate := waits.observe(1, "orders", start)
	assert.False(t, escalate)
	_, escalate = waits.observe(1, "orders", start.Add(targetHeldEscalationAfter))
	require.True(t, escalate, "orders is escalated once it has been held past the bound")

	paymentsStart := start.Add(targetHeldEscalationAfter + 31*time.Second)
	heldFor, escalate := waits.observe(1, "payments", paymentsStart)
	assert.False(t, escalate)
	assert.Zero(t, heldFor, "payments is measured from its own first refusal, not from orders'")
	heldFor, escalate = waits.observe(1, "payments", paymentsStart.Add(targetHeldEscalationAfter))
	assert.True(t, escalate, "payments escalates on its own, although orders already did")
	assert.Equal(t, targetHeldEscalationAfter, heldFor)
}

// A hold past the bound reaches the apply's timeline, naming the table and
// what the operator can do, so a wait no drive will end on its own does not
// sit behind its first warning.
func TestObserveTargetHeldEscalatesToTheTimeline(t *testing.T) {
	client, apply, _, _ := lostWorkPollFixture(&phaseSequenceEngine{}, lostWorkTrustBudgetAmple)
	now := time.Now()
	client.targetHeld.waits = map[targetHeldKey]*targetHeldWait{{applyID: apply.ID, table: "orders"}: {since: now.Add(-targetHeldEscalationAfter), lastSeen: now}}

	client.observeTargetHeld(t.Context(), slog.Default(), apply, "orders")

	assertApplyLogContains(t, client, "Table orders has been held by another run of a schema change for 2m0s")
	assertApplyLogContains(t, client, "Find the run holding it, or stop the apply.")
}
