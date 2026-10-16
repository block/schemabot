//go:build integration

package api

import (
	"context"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// A parallel rollout of three members has copied everything and parked at the
// cutover barrier. Its swaps run one at a time, so only the first is claimable;
// each completed swap is what makes the next one claimable. With a single
// driver and an hour-long poll interval, the operator must still cut all three
// over back to back, in rollout order, because the driver re-runs its claim
// ladder as soon as a swap settles instead of waiting for its next tick.
func TestOperatorCutsOverParallelMembersBackToBackWithoutWaitingForATick(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)
	members := []string{"region-a", "region-b", "region-c"}
	seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
		applyIdentifier: "drive-passes-parallel-cutover",
		parentState:     state.Apply.WaitingForCutover,
		cutoverPolicy:   storage.CutoverPolicyParallel,
		onFailure:       storage.OnFailureHalt,
		deployments:     members,
		opState:         state.ApplyOperation.WaitingForCutover,
		taskState:       state.Task.WaitingForCutover,
	})

	rec := &driveRecorder{}
	clients := make(map[string]tern.Client, len(members))
	for dep, client := range matrixClients(stor, rec, map[string]matrixOutcome{
		"region-a": {taskState: state.Task.Completed},
		"region-b": {taskState: state.Task.Completed},
		"region-c": {taskState: state.Task.Completed},
	}) {
		clients[dep] = &cutoverMatrixClient{matrixTernClient: client.(*matrixTernClient)}
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(stor, &ServerConfig{Drivers: 1}, clients, logger)
	require.NoError(t, svc.SetOperatorPollInterval(time.Hour))

	svc.StartOperator(ctx)
	t.Cleanup(svc.StopOperator)

	deadline := time.Now().Add(drivePassesDeadline)
	for !state.IsState(getApply(t, ctx, stor, seed.applyID).State, state.Apply.Completed) {
		require.True(t, time.Now().Before(deadline),
			"the rollout must finish cutting over well inside one poll interval; swaps driven so far: %v", rec.resumeOrder())
		time.Sleep(20 * time.Millisecond)
	}

	assert.Equal(t, members, rec.resumeOrder(), "swaps must run one at a time in rollout order")
	for _, dep := range members {
		assert.Equal(t, state.ApplyOperation.Completed, opState(t, ctx, stor, seed.opID(dep)),
			"operation %s must be completed", dep)
	}
}

// A parallel rollout of three targets is queued while every driver is idle,
// and the wake that follows reaches one driver. The rollout must still start
// all three copies at once, within the apply's driver cap, rather than one
// per poll tick: each driver that starts a copy wakes an idle peer, which
// claims the next. Every drive blocks until all three are running together,
// so the rollout only finishes if the starts fan out at claim speed.
func TestOperatorStartsParallelMembersTogetherFromOneWake(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := t.Context()
	db := openMatrixStorage(t)
	members := []string{"region-a", "region-b", "region-c"}
	// The apply's driver cap and this process's share of it are both raised to
	// the member count, so neither bound is what holds a start back.
	stor := mysqlstore.New(db, storage.WithMaxDriversPerApply(len(members)), storage.WithDriversPerProcess(2*len(members)))
	claims := &claimMissCountingStorage{Storage: stor}

	rec := &driveRecorder{}
	gate := newDriveGate(t, len(members))
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(claims, &ServerConfig{Drivers: len(members)}, matrixClients(stor, rec, map[string]matrixOutcome{
		"region-a": {taskState: state.Task.Completed, gate: gate},
		"region-b": {taskState: state.Task.Completed, gate: gate},
		"region-c": {taskState: state.Task.Completed, gate: gate},
	}), logger)
	require.NoError(t, svc.SetOperatorPollInterval(time.Hour))

	svc.StartOperator(ctx)
	t.Cleanup(svc.StopOperator)

	// Every driver runs one pass at startup. Seed only once each has found no
	// copy to claim, so the rollout is started by the wake alone.
	deadline := time.Now().Add(drivePassesDeadline)
	for claims.misses() < len(members) {
		require.True(t, time.Now().Before(deadline), "every driver must finish its startup pass")
		time.Sleep(10 * time.Millisecond)
	}
	seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
		applyIdentifier: "drive-passes-parallel-start",
		parentState:     state.Apply.Pending,
		cutoverPolicy:   storage.CutoverPolicyParallel,
		onFailure:       storage.OnFailureHalt,
		deployments:     members,
		opState:         state.ApplyOperation.Pending,
		taskState:       state.Task.Pending,
	})
	svc.WakeOperator("drive-passes-parallel-start", "payments", "staging")

	for !state.IsState(getApply(t, ctx, stor, seed.applyID).State, state.Apply.Completed) {
		require.True(t, time.Now().Before(deadline),
			"every target must start from one wake; copies driven so far: %v", rec.resumeOrder())
		time.Sleep(20 * time.Millisecond)
	}

	assert.ElementsMatch(t, members, rec.resumeOrder(), "every target must be driven exactly once")
	for _, dep := range members {
		assert.Equal(t, state.ApplyOperation.Completed, opState(t, ctx, stor, seed.opID(dep)),
			"operation %s must be completed", dep)
	}
}

// A rolling rollout starts each member only once the one before it completes.
// One call to drivePasses, the work a driver does for a single wake or tick,
// must carry the rollout from its first member to its last and then return
// once a pass finds nothing left to claim.
func TestDrivePassesCarriesARollingRolloutThroughEveryMember(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)
	members := []string{"region-a", "region-b", "region-c"}
	seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
		applyIdentifier: "drive-passes-rolling",
		parentState:     state.Apply.Pending,
		cutoverPolicy:   storage.CutoverPolicyRolling,
		onFailure:       storage.OnFailureHalt,
		deployments:     members,
		opState:         state.ApplyOperation.Pending,
		taskState:       state.Task.Pending,
	})
	rec := &driveRecorder{}
	svc := newMatrixService(t, stor, matrixClients(stor, rec, map[string]matrixOutcome{
		"region-a": {taskState: state.Task.Completed},
		"region-b": {taskState: state.Task.Completed},
		"region-c": {taskState: state.Task.Completed},
	}))

	drivePassesWithin(t, svc.Service, openClaimGate())

	assert.Equal(t, members, rec.resumeOrder(), "every member must be driven, in order, from a single wake")
	assert.Equal(t, state.Apply.Completed, getApply(t, ctx, stor, seed.applyID).State)
	assert.Equal(t, 1, svc.matrixSummary.count(), "the aggregate terminal summary must publish exactly once")
}

// Claiming stops while the driver is in the middle of a rolling rollout's
// first member. That drive finishes and settles, which would normally send the
// driver straight into another pass, but the pass re-reads the claim gate and
// claims nothing: the next member stays pending for whichever process claims
// next.
func TestDrivePassesStopsWhenTheClaimGateClosesMidRun(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)
	seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
		applyIdentifier: "drive-passes-gate-closes",
		parentState:     state.Apply.Pending,
		cutoverPolicy:   storage.CutoverPolicyRolling,
		onFailure:       storage.OnFailureHalt,
		deployments:     []string{"region-a", "region-b"},
		opState:         state.ApplyOperation.Pending,
		taskState:       state.Task.Pending,
	})
	rec := &driveRecorder{}
	stop := make(chan struct{})
	clients := matrixClients(stor, rec, map[string]matrixOutcome{
		"region-a": {taskState: state.Task.Completed},
		"region-b": {taskState: state.Task.Completed},
	})
	clients["region-a/staging"] = &gateClosingMatrixClient{
		matrixTernClient: clients["region-a/staging"].(*matrixTernClient),
		closeGate:        func() { close(stop) },
	}
	svc := newMatrixService(t, stor, clients)

	drivePassesWithin(t, svc.Service, stop)

	assert.Equal(t, []string{"region-a"}, rec.resumeOrder(), "no pass may claim after the claim gate closes")
	assert.Equal(t, state.ApplyOperation.Completed, opState(t, ctx, stor, seed.opID("region-a")),
		"the drive that was running when claiming stopped still settles")
	assert.Equal(t, state.ApplyOperation.Pending, opState(t, ctx, stor, seed.opID("region-b")),
		"the next member is left for whichever process claims next")
}

// A member's drive ends in a retryable failure. The operation settles to
// failed_retryable, which the copy claim offers again, so a driver that
// re-ran its ladder on it would spend the apply's retry budget back to back.
// drivePasses must stop after the one drive and leave the retry to the next
// wake or tick.
func TestDrivePassesLeavesARetryableFailureToTheNextTick(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)
	seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
		applyIdentifier: "drive-passes-retryable",
		parentState:     state.Apply.Pending,
		cutoverPolicy:   storage.CutoverPolicyRolling,
		onFailure:       storage.OnFailureHalt,
		deployments:     []string{"region-a", "region-b"},
		opState:         state.ApplyOperation.Pending,
		taskState:       state.Task.Pending,
	})
	rec := &driveRecorder{}
	svc := newMatrixService(t, stor, matrixClients(stor, rec, map[string]matrixOutcome{
		"region-a": {taskState: state.Task.FailedRetryable},
		"region-b": {taskState: state.Task.Completed},
	}))

	drivePassesWithin(t, svc.Service, openClaimGate())

	assert.Equal(t, 1, rec.resumeCount(), "a retryable failure must not send the driver into another pass")
	assert.Equal(t, state.ApplyOperation.FailedRetryable, opState(t, ctx, stor, seed.opID("region-a")))
	assert.Equal(t, state.ApplyOperation.Pending, opState(t, ctx, stor, seed.opID("region-b")))
}

// cutoverMatrixClient drives a parked operation through its swap the same way
// matrixTernClient drives a copy: by moving that operation's own task rows to
// the configured outcome under the operation lease on the context.
type cutoverMatrixClient struct {
	*matrixTernClient
}

func (c *cutoverMatrixClient) ResumeApplyOperationCutover(ctx context.Context, apply *storage.Apply, applyOperationID int64) error {
	return c.ResumeApplyOperation(ctx, apply, applyOperationID)
}

// gateClosingMatrixClient closes the driver's claim gate from inside its drive,
// the moment StopClaiming would land while a driver is busy.
type gateClosingMatrixClient struct {
	*matrixTernClient
	closeGate func()
}

func (c *gateClosingMatrixClient) ResumeApplyOperation(ctx context.Context, apply *storage.Apply, applyOperationID int64) error {
	c.closeGate()
	return c.matrixTernClient.ResumeApplyOperation(ctx, apply, applyOperationID)
}

// claimMissCountingStorage counts the copy claims that found nothing to claim,
// so a test can tell when every driver has finished a pass.
type claimMissCountingStorage struct {
	storage.Storage
	count atomic.Int64
}

func (c *claimMissCountingStorage) ApplyOperations() storage.ApplyOperationStore {
	return claimMissCountingOperations{ApplyOperationStore: c.Storage.ApplyOperations(), parent: c}
}

func (c *claimMissCountingStorage) misses() int {
	return int(c.count.Load())
}

type claimMissCountingOperations struct {
	storage.ApplyOperationStore
	parent *claimMissCountingStorage
}

func (o claimMissCountingOperations) FindNextApplyOperation(ctx context.Context, owner string) (*storage.ApplyOperation, error) {
	op, err := o.ApplyOperationStore.FindNextApplyOperation(ctx, owner)
	if err == nil && op == nil {
		o.parent.count.Add(1)
	}
	return op, err
}
