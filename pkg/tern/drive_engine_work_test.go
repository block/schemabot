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
	assert.Empty(t, driveWorkOwner(t.Context()), "a caller with no claim is the empty owner")
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
