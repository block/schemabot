//go:build integration

package api

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
	"github.com/block/schemabot/pkg/testutil"
)

// TestOperatorMultiOperationMatrix proves the DORMANT multi-deployment fan-out
// path before the config flip enables it. Config.Validate() still rejects
// >1 deployment, so a real apply never owns more than one operation today; this
// matrix reaches the multi-op path the only way production cannot — by seeding
// an apply with N apply_operations via CreateWithGroupedOperations (the exact
// dormant bypass helper) and driving the operator's unexported claim/drive
// entrypoints directly against real MySQL storage.
//
// The drive is wired to deterministic per-deployment tern.Client fakes that
// mutate only their own operation's task rows under the operation lease already
// on the context. This exercises the real SQL claim gate (FOR UPDATE SKIP
// LOCKED, sibling ordering, pending-stop block, stale-active reclaim), the real
// operation-lease authorization, and the real aggregate-CAS projection — but
// avoids Spirit DDL, progress polling, and the background operator goroutines,
// all of which would add flakiness irrelevant to the fan-out invariants.
//
// Each subtest maps to the safety invariants in the fan-out implementation plan
// risk table (ordering, halt/continue, barrier overlap, pending-stop,
// op-lease-only parent writes, single-publisher terminal summary).
func TestOperatorMultiOperationMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := startMatrixContainer(t, ctx)
	stor := mysqlstore.New(db)

	t.Run("RollingHaltCompletesInOrder", func(t *testing.T) {
		resetMatrixTables(t, ctx, db)
		seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
			applyIdentifier: "matrix-rolling-halt-ok",
			parentState:     state.Apply.Pending,
			cutoverPolicy:   storage.CutoverPolicyRolling,
			onFailure:       storage.OnFailureHalt,
			deployments:     []string{"region-a", "region-b", "region-c"},
			opState:         state.ApplyOperation.Pending,
			taskState:       state.Task.Pending,
		})

		rec := &driveRecorder{}
		svc := newMatrixService(t, stor, matrixClients(stor, rec, map[string]matrixOutcome{
			"region-a": {taskState: state.Task.Completed},
			"region-b": {taskState: state.Task.Completed},
			"region-c": {taskState: state.Task.Completed},
		}))

		driveNextOperation(t, ctx, svc, 1)
		driveNextOperation(t, ctx, svc, 2)
		driveNextOperation(t, ctx, svc, 3)

		assert.Equal(t, []string{"region-a", "region-b", "region-c"}, rec.resumeOrder(),
			"rolling policy must drive deployments serially in deployment_order")
		for _, dep := range seed.deployments {
			assert.Equal(t, state.ApplyOperation.Completed, opState(t, ctx, stor, seed.opID(dep)),
				"operation %s must be completed", dep)
		}
		apply := getApply(t, ctx, stor, seed.applyID)
		assert.Equal(t, state.Apply.Completed, apply.State, "parent apply must aggregate to completed")
		assert.NotNil(t, apply.StartedAt, "parent started_at must be stamped by the projection")
		assert.NotNil(t, apply.CompletedAt, "parent completed_at must be stamped by the projection")
		assert.Equal(t, 1, svc.matrixSummary.count(), "the aggregate terminal summary must publish exactly once")
		assert.Len(t, svc.matrixSummary.lastTasks(), len(seed.deployments),
			"the summary must include every operation's task")
		assert.Zero(t, svc.matrixSummary.recoveredCount(), "a multi-op drive must not fire the per-driver recovered observer")
	})

	t.Run("ShardedWorkCompletesBeforeGroupFinalizer", func(t *testing.T) {
		// A sharded apply with namespace-level finalization drives shard work
		// operations first, keeps the aggregate non-terminal while the finalizer is
		// pending, then completes only after the finalizer operation succeeds.
		resetMatrixTables(t, ctx, db)
		seed := seedShardedFinalizerApply(t, ctx, stor)

		rec := &driveRecorder{}
		svc := newMatrixService(t, stor, matrixClients(stor, rec, map[string]matrixOutcome{
			"region-a": {taskState: state.Task.Completed},
		}))

		driveNextOperation(t, ctx, svc, 1)
		driveNextOperation(t, ctx, svc, 2)

		assert.Equal(t, state.ApplyOperation.Completed, opState(t, ctx, stor, seed.workAID))
		assert.Equal(t, state.ApplyOperation.Completed, opState(t, ctx, stor, seed.workBID))
		assert.Equal(t, state.ApplyOperation.Pending, opState(t, ctx, stor, seed.finalizerID))
		assert.Equal(t, state.Apply.Pending, getApply(t, ctx, stor, seed.applyID).State,
			"completed shard work plus a pending finalizer must remain non-terminal")

		driveNextOperation(t, ctx, svc, 3)

		assert.Equal(t, []string{
			"commerce/-80/users",
			"commerce/80-/users",
			"commerce/group_finalizer",
		}, rec.resumeOperationKeys())
		assert.Equal(t, []string{
			storage.ApplyOperationKindWork,
			storage.ApplyOperationKindWork,
			storage.ApplyOperationKindGroupFinalizer,
		}, rec.resumeOperationKinds())
		assert.Equal(t, state.ApplyOperation.Completed, opState(t, ctx, stor, seed.finalizerID))
		assert.Equal(t, state.Apply.Completed, getApply(t, ctx, stor, seed.applyID).State)
		assert.Equal(t, 1, svc.matrixSummary.count(), "terminal summary publishes after the finalizer completes")
	})

	t.Run("RollingHaltFailureStopsAtFirstDeployment", func(t *testing.T) {
		resetMatrixTables(t, ctx, db)
		seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
			applyIdentifier: "matrix-rolling-halt-fail",
			parentState:     state.Apply.Pending,
			cutoverPolicy:   storage.CutoverPolicyRolling,
			onFailure:       storage.OnFailureHalt,
			deployments:     []string{"region-a", "region-b", "region-c"},
			opState:         state.ApplyOperation.Pending,
			taskState:       state.Task.Pending,
		})

		rec := &driveRecorder{}
		svc := newMatrixService(t, stor, matrixClients(stor, rec, map[string]matrixOutcome{
			"region-a": {taskState: state.Task.Failed, errMsg: "region-a boom"},
			"region-b": {taskState: state.Task.Completed},
			"region-c": {taskState: state.Task.Completed},
		}))

		driveNextOperation(t, ctx, svc, 1) // region-a fails
		driveNextOperation(t, ctx, svc, 2) // halt: nothing claimable behind the failed sibling

		assert.Equal(t, []string{"region-a"}, rec.resumeOrder(),
			"halt must not start a later deployment behind a failed earlier sibling")
		assert.Equal(t, state.ApplyOperation.Failed, opState(t, ctx, stor, seed.opID("region-a")))
		assert.Equal(t, state.ApplyOperation.Pending, opState(t, ctx, stor, seed.opID("region-b")))
		assert.Equal(t, state.ApplyOperation.Pending, opState(t, ctx, stor, seed.opID("region-c")))
		apply := getApply(t, ctx, stor, seed.applyID)
		assert.Equal(t, state.Apply.Failed, apply.State, "first failure halts the rollout to failed")
		assert.Contains(t, apply.ErrorMessage, "region-a", "aggregate message must name the failed deployment")
		assert.Equal(t, 1, svc.matrixSummary.count(), "terminal summary publishes once on the failed verdict")
	})

	t.Run("RollingContinueFailureRunsRemainingAndSettlesFailed", func(t *testing.T) {
		resetMatrixTables(t, ctx, db)
		seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
			applyIdentifier: "matrix-rolling-continue",
			parentState:     state.Apply.Pending,
			cutoverPolicy:   storage.CutoverPolicyRolling,
			onFailure:       storage.OnFailureContinue,
			deployments:     []string{"region-a", "region-b"},
			opState:         state.ApplyOperation.Pending,
			taskState:       state.Task.Pending,
		})

		rec := &driveRecorder{}
		svc := newMatrixService(t, stor, matrixClients(stor, rec, map[string]matrixOutcome{
			"region-a": {taskState: state.Task.Failed, errMsg: "region-a degraded"},
			"region-b": {taskState: state.Task.Completed},
		}))

		// region-a fails; continue holds the rollout running_degraded, no summary.
		driveNextOperation(t, ctx, svc, 1)
		assert.Equal(t, state.ApplyOperation.Failed, opState(t, ctx, stor, seed.opID("region-a")))
		assert.Equal(t, state.Apply.RunningDegraded, getApply(t, ctx, stor, seed.applyID).State,
			"continue must hold the parent running_degraded while siblings are in flight")
		assert.Equal(t, 0, svc.matrixSummary.count(), "no terminal summary until the rollout settles")

		// region-b runs (continue exempts it from the failed earlier sibling) and
		// the rollout settles to failed — continue governs continuation, not the verdict.
		driveNextOperation(t, ctx, svc, 2)
		assert.Equal(t, []string{"region-a", "region-b"}, rec.resumeOrder())
		assert.Equal(t, state.ApplyOperation.Completed, opState(t, ctx, stor, seed.opID("region-b")))
		apply := getApply(t, ctx, stor, seed.applyID)
		assert.Equal(t, state.Apply.Failed, apply.State, "a continued rollout still settles to the failed verdict")
		assert.Contains(t, apply.ErrorMessage, "region-a", "the failed verdict names the first failure")
		assert.Equal(t, 1, svc.matrixSummary.count(), "exactly one terminal summary once settled")
	})

	t.Run("BarrierParksCopyAndClaimsNextDeployment", func(t *testing.T) {
		resetMatrixTables(t, ctx, db)
		seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
			applyIdentifier: "matrix-barrier-park",
			parentState:     state.Apply.Pending,
			cutoverPolicy:   storage.CutoverPolicyBarrier,
			onFailure:       storage.OnFailureHalt,
			deployments:     []string{"region-a", "region-b"},
			opState:         state.ApplyOperation.Pending,
			taskState:       state.Task.Pending,
		})

		rec := &driveRecorder{}
		// Each copy drive parks its operation at the cutover barrier
		// (waiting_for_cutover) rather than completing.
		svc := newMatrixService(t, stor, matrixClients(stor, rec, map[string]matrixOutcome{
			"region-a": {taskState: state.Task.WaitingForCutover},
			"region-b": {taskState: state.Task.WaitingForCutover},
		}))

		driveNextOperation(t, ctx, svc, 1) // region-a parks at the barrier
		// Under barrier a later deployment's COPY phase may start once the earlier
		// sibling reaches the barrier — it does not wait for completion.
		driveNextOperation(t, ctx, svc, 2) // region-b's copy is allowed to start

		assert.Equal(t, []string{"region-a", "region-b"}, rec.resumeOrder(),
			"barrier must let the next deployment's copy start once the earlier sibling parks")
		assert.Equal(t, state.ApplyOperation.WaitingForCutover, opState(t, ctx, stor, seed.opID("region-a")))
		assert.Equal(t, state.ApplyOperation.WaitingForCutover, opState(t, ctx, stor, seed.opID("region-b")))
		apply := getApply(t, ctx, stor, seed.applyID)
		assert.False(t, state.IsTerminalApplyState(apply.State), "a parked rollout must not be terminal")
		assert.Equal(t, state.Apply.WaitingForCutover, apply.State)
		assert.Equal(t, 0, svc.matrixSummary.count(), "no terminal summary while parked at the barrier")

		// With both copy phases parked and reserved for the ordered cutover claim,
		// the copy claim finds nothing more to drive.
		rec2 := rec.resumeOrder()
		driveNextOperation(t, ctx, svc, 3)
		assert.Equal(t, rec2, rec.resumeOrder(), "parked barrier operations must not be re-leased by the copy claim")
	})

	t.Run("PendingStopStopsPendingSiblingsAndCompletesStop", func(t *testing.T) {
		resetMatrixTables(t, ctx, db)
		// A continue rollout that already failed one deployment and still has a
		// pending sibling, with a queued stop: the stop must halt the pending
		// sibling and settle the apply rather than starting more deployments.
		seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
			applyIdentifier: "matrix-pending-stop",
			parentState:     state.Apply.RunningDegraded,
			cutoverPolicy:   storage.CutoverPolicyRolling,
			onFailure:       storage.OnFailureContinue,
			deployments:     []string{"region-a", "region-b"},
			perOpState: map[string]string{
				"region-a": state.ApplyOperation.Failed,
				"region-b": state.ApplyOperation.Pending,
			},
			perTaskState: map[string]string{
				"region-a": state.Task.Failed,
				"region-b": state.Task.Pending,
			},
		})
		requestPendingStop(t, ctx, stor, seed.applyID)

		rec := &driveRecorder{}
		// Stop reconciliation drives the data-plane stop through the apply's
		// deployment client before terminalizing the pending siblings, so every
		// deployment must resolve to a client — as it always does in production,
		// where Config.Validate ties each deployment to a configured tern
		// deployment. No operation is driven under a pending stop (the claim gate
		// blocks the pending sibling and the stop path uses the whole-apply drive),
		// so these outcomes are never reached via ResumeApplyOperation.
		svc := newMatrixService(t, stor, matrixClients(stor, rec, map[string]matrixOutcome{
			"region-a": {taskState: state.Task.Failed},
			"region-b": {taskState: state.Task.Pending},
		}))

		svc.recoverApplies(ctx, 1)

		assert.Empty(t, rec.resumeOrder(), "stop reconciliation must not drive any operation")
		assert.Equal(t, state.ApplyOperation.Stopped, opState(t, ctx, stor, seed.opID("region-b")),
			"the pending sibling must be stopped, not started, under a pending stop")
		apply := getApply(t, ctx, stor, seed.applyID)
		assert.Equal(t, state.Apply.Failed, apply.State, "failed + stopped settles the rollout to failed")
		assert.True(t, stopRequestCompleted(t, ctx, stor, seed.applyID), "the pending stop request must be completed")
		assert.Equal(t, 1, svc.matrixSummary.count(), "the settled rollout still owes exactly one terminal summary")
	})

	t.Run("ConcurrentSiblingCompletionsPublishOneSummary", func(t *testing.T) {
		resetMatrixTables(t, ctx, db)
		seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
			applyIdentifier: "matrix-concurrent",
			parentState:     state.Apply.Running,
			cutoverPolicy:   storage.CutoverPolicyRolling,
			onFailure:       storage.OnFailureHalt,
			deployments:     []string{"region-a", "region-b"},
			opState:         state.ApplyOperation.Running,
			taskState:       state.Task.Running,
		})
		// Backdate both operation heartbeats so the stale-active reclaim makes both
		// rows claimable at once by two racing drivers.
		stalenessBackdate(t, ctx, db, seed.applyID)

		rec := &driveRecorder{}
		// Both drives block until both have entered ResumeApplyOperation, proving
		// the operation leases do not serialize on a shared parent lease. Each
		// drive also probes that a direct parent write fails closed under the
		// operation-only lease.
		gate := newDriveGate(t, 2)
		svc := newMatrixService(t, stor, matrixClients(stor, rec, map[string]matrixOutcome{
			"region-a": {taskState: state.Task.Completed, gate: gate, probeParentWrite: true},
			"region-b": {taskState: state.Task.Completed, gate: gate, probeParentWrite: true},
		}))

		// Each driver polls for an operation the way the operator loop does: a
		// single FindNextApplyOperation can return nil when a peer claims first,
		// so retry until this driver drives one. The gate then holds both inside
		// their drive at once, proving the operation leases do not serialize.
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); driveOneOperation(t, ctx, svc, rec, 1) }()
		go func() { defer wg.Done(); driveOneOperation(t, ctx, svc, rec, 2) }()
		wg.Wait()

		assert.ElementsMatch(t, []string{"region-a", "region-b"}, rec.resumeOrder(),
			"both operations must be claimed concurrently by distinct drivers")
		assert.Equal(t, state.ApplyOperation.Completed, opState(t, ctx, stor, seed.opID("region-a")))
		assert.Equal(t, state.ApplyOperation.Completed, opState(t, ctx, stor, seed.opID("region-b")))
		assert.Equal(t, state.Apply.Completed, getApply(t, ctx, stor, seed.applyID).State)
		assert.Equal(t, 1, svc.matrixSummary.count(),
			"only the aggregate-CAS winner publishes the terminal summary")
		parentWriteErrs := rec.parentWriteErrors()
		require.Len(t, parentWriteErrs, 2, "both drives must have probed the parent write")
		for _, err := range parentWriteErrs {
			assert.ErrorIs(t, err, storage.ErrApplyLeaseLost,
				"an operation-only lease must not authorize a direct parent applies write")
		}
	})

	t.Run("ConcurrentSameDeploymentShardWorkDrives", func(t *testing.T) {
		// The per-shard work operations of one deployment fan out: two drivers
		// claim and drive both pending shards of region-a at the same time. The
		// group_finalizer then drives only after both shards complete, and the
		// parent terminalizes after the finalizer. Under serial claiming the
		// second shard would be unclaimable while the first runs, so the drive
		// gate would time out — this asserts the shards run concurrently.
		resetMatrixTables(t, ctx, db)
		seed := seedShardedFinalizerApply(t, ctx, stor)

		rec := &driveRecorder{}
		// Both shard work drives block until both have entered ResumeApplyOperation;
		// the finalizer drive does not participate in the gate.
		gate := newDriveGate(t, 2)
		svc := newMatrixService(t, stor, matrixClients(stor, rec, map[string]matrixOutcome{
			"region-a": {taskState: state.Task.Completed, gate: gate},
		}))

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); driveOneOperation(t, ctx, svc, rec, 1) }()
		go func() { defer wg.Done(); driveOneOperation(t, ctx, svc, rec, 2) }()
		wg.Wait()

		assert.ElementsMatch(t, []string{"commerce/-80/users", "commerce/80-/users"}, rec.resumeOperationKeys(),
			"both shard work operations of the deployment must be claimed concurrently by distinct drivers")
		assert.Equal(t, state.ApplyOperation.Completed, opState(t, ctx, stor, seed.workAID))
		assert.Equal(t, state.ApplyOperation.Completed, opState(t, ctx, stor, seed.workBID))
		assert.Equal(t, state.ApplyOperation.Pending, opState(t, ctx, stor, seed.finalizerID),
			"the finalizer must remain pending until both shard work siblings complete")
		assert.Equal(t, state.Apply.Pending, getApply(t, ctx, stor, seed.applyID).State,
			"the parent must stay non-terminal while the finalizer is pending")

		// The finalizer is now claimable; drive it and the parent terminalizes.
		driveNextOperation(t, ctx, svc, 1)
		assert.Equal(t, state.ApplyOperation.Completed, opState(t, ctx, stor, seed.finalizerID))
		assert.Equal(t, state.Apply.Completed, getApply(t, ctx, stor, seed.applyID).State)
		assert.Equal(t, 1, svc.matrixSummary.count(),
			"the aggregate terminal summary publishes once after the finalizer completes")
	})

	t.Run("PendingStopHaltsRemainingShardWork", func(t *testing.T) {
		// A pending stop must halt every not-yet-started shard of a deployment,
		// even though the shards are independently claimable. With both shard
		// work operations still pending, the queued stop stops them rather than
		// starting more shard work.
		resetMatrixTables(t, ctx, db)
		seed := seedShardedFinalizerApply(t, ctx, stor)
		requestPendingStop(t, ctx, stor, seed.applyID)

		rec := &driveRecorder{}
		svc := newMatrixService(t, stor, matrixClients(stor, rec, map[string]matrixOutcome{
			"region-a": {taskState: state.Task.Pending},
		}))

		svc.recoverApplies(ctx, 1)

		assert.Empty(t, rec.resumeOrder(), "stop reconciliation must not drive any shard work")
		assert.Equal(t, state.ApplyOperation.Stopped, opState(t, ctx, stor, seed.workAID),
			"a pending shard must be stopped, not started, under a pending stop")
		assert.Equal(t, state.ApplyOperation.Stopped, opState(t, ctx, stor, seed.workBID),
			"every remaining pending shard must be stopped under a pending stop")
		assert.Equal(t, state.ApplyOperation.Stopped, opState(t, ctx, stor, seed.finalizerID),
			"the finalizer is halted, not started, once the rollout is stopped")
		assert.True(t, stopRequestCompleted(t, ctx, stor, seed.applyID), "the pending stop request must be completed")
	})
}

// --- harness ------------------------------------------------------------

func startMatrixContainer(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	container, err := mysql.Run(ctx,
		"mysql:8.0",
		mysql.WithDatabase("schemabot_test"),
		mysql.WithUsername("root"),
		mysql.WithPassword("test"),
	)
	require.NoError(t, err, "failed to start mysql")
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	})

	dsn, err := testutil.ContainerConnectionString(ctx, container, "parseTime=true")
	require.NoError(t, err, "failed to get connection string")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	require.NoError(t, EnsureSchema(dsn, logger), "failed to ensure schema")

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "failed to open database")
	require.NoError(t, db.PingContext(ctx), "failed to ping database")
	t.Cleanup(func() { utils.CloseAndLog(db) })
	return db
}

// resetMatrixTables clears the rows the matrix touches so each subtest shares one
// container without leaking claimable operations across cases.
func resetMatrixTables(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	for _, table := range []string{"tasks", "apply_operations", "apply_control_requests", "apply_logs", "applies"} {
		_, err := db.ExecContext(ctx, "DELETE FROM "+table)
		require.NoErrorf(t, err, "reset table %s", table)
	}
}

// --- seeding ------------------------------------------------------------

type multiOpSeed struct {
	applyIdentifier string
	parentState     string
	cutoverPolicy   string
	onFailure       string
	deployments     []string
	// opState / taskState set a uniform initial state for every operation/task.
	opState   string
	taskState string
	// perOpState / perTaskState override the uniform state per deployment.
	perOpState   map[string]string
	perTaskState map[string]string
}

type seededMultiOpApply struct {
	applyID     int64
	deployments []string
	ops         map[string]int64
}

func (s seededMultiOpApply) opID(deployment string) int64 { return s.ops[deployment] }

type seededShardedFinalizerApply struct {
	applyID     int64
	workAID     int64
	workBID     int64
	finalizerID int64
}

func seedGroupedApply(t *testing.T, ctx context.Context, stor storage.Storage, spec multiOpSeed) seededMultiOpApply {
	t.Helper()
	require.NotEmpty(t, spec.deployments, "seedGroupedApply requires at least one deployment")
	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: spec.applyIdentifier,
		Database:        "payments",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		Environment:     "staging",
		Deployment:      spec.deployments[0],
		Caller:          "matrix-test",
		Engine:          storage.EngineForType(storage.DatabaseTypeMySQL),
		State:           spec.parentState,
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{}),
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	groups := make([]*storage.ApplyOperationWithTasks, 0, len(spec.deployments))
	for _, dep := range spec.deployments {
		opState := spec.opState
		if s, ok := spec.perOpState[dep]; ok {
			opState = s
		}
		taskState := spec.taskState
		if s, ok := spec.perTaskState[dep]; ok {
			taskState = s
		}
		groups = append(groups, &storage.ApplyOperationWithTasks{
			Operation: &storage.ApplyOperation{
				Deployment:    dep,
				Target:        "payments-" + dep,
				State:         opState,
				CutoverPolicy: spec.cutoverPolicy,
				OnFailure:     spec.onFailure,
				CreatedAt:     now,
				UpdatedAt:     now,
			},
			Tasks: []*storage.Task{{
				TaskIdentifier: spec.applyIdentifier + "-" + dep,
				Database:       "payments",
				DatabaseType:   storage.DatabaseTypeMySQL,
				Engine:         storage.EngineForType(storage.DatabaseTypeMySQL),
				Repository:     "octocat/hello-world",
				PullRequest:    1,
				Environment:    "staging",
				State:          taskState,
				Options:        storage.MarshalApplyOptions(storage.ApplyOptions{}),
				Namespace:      "payments",
				TableName:      "widgets",
				DDL:            "ALTER TABLE widgets ADD COLUMN c int",
				DDLAction:      "alter",
				CreatedAt:      now,
				UpdatedAt:      now,
			}},
		})
	}

	applyID, err := stor.Applies().CreateWithGroupedOperations(ctx, apply, groups)
	require.NoError(t, err, "seed grouped apply")

	ops := make(map[string]int64, len(groups))
	for _, group := range groups {
		ops[group.Operation.Deployment] = group.Operation.ID
	}
	return seededMultiOpApply{applyID: applyID, deployments: spec.deployments, ops: ops}
}

func seedShardedFinalizerApply(t *testing.T, ctx context.Context, stor storage.Storage) seededShardedFinalizerApply {
	t.Helper()
	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: "matrix-sharded-finalizer",
		Database:        "payments",
		DatabaseType:    storage.DatabaseTypeStrata,
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		Environment:     "staging",
		Deployment:      "region-a",
		Caller:          "matrix-test",
		Engine:          storage.EngineForType(storage.DatabaseTypeStrata),
		State:           state.Apply.Pending,
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{}),
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	groups := []*storage.ApplyOperationWithTasks{
		newShardedWorkGroup(now, "commerce/-80/users", "users", "alter", "-80", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"),
		newShardedWorkGroup(now, "commerce/80-/users", "users", "alter", "80-", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"),
		newTaskLessFinalizerGroup(now, "commerce/group_finalizer"),
	}

	applyID, err := stor.Applies().CreateWithGroupedOperations(ctx, apply, groups)
	require.NoError(t, err, "seed sharded finalizer apply")
	return seededShardedFinalizerApply{
		applyID:     applyID,
		workAID:     groups[0].Operation.ID,
		workBID:     groups[1].Operation.ID,
		finalizerID: groups[2].Operation.ID,
	}
}

func newShardedWorkGroup(now time.Time, operationKey, table, action, shard, ddl string) *storage.ApplyOperationWithTasks {
	return &storage.ApplyOperationWithTasks{
		Operation: &storage.ApplyOperation{
			Deployment:    "region-a",
			OperationKey:  operationKey,
			OperationKind: storage.ApplyOperationKindWork,
			Target:        "payments-region-a",
			State:         state.ApplyOperation.Pending,
			CutoverPolicy: storage.CutoverPolicyRolling,
			OnFailure:     storage.OnFailureHalt,
			CreatedAt:     now,
			UpdatedAt:     now,
		},
		Tasks: []*storage.Task{{
			TaskIdentifier: "task-" + operationKey,
			Database:       "payments",
			DatabaseType:   storage.DatabaseTypeStrata,
			Engine:         storage.EngineForType(storage.DatabaseTypeStrata),
			Repository:     "octocat/hello-world",
			PullRequest:    1,
			Environment:    "staging",
			State:          state.Task.Pending,
			Options:        storage.MarshalApplyOptions(storage.ApplyOptions{}),
			Namespace:      "commerce",
			TableName:      table,
			Shard:          shard,
			DDL:            ddl,
			DDLAction:      action,
			CreatedAt:      now,
			UpdatedAt:      now,
		}},
	}
}

// newTaskLessFinalizerGroup seeds a group_finalizer operation with no task,
// matching how apply-create builds finalizers: the VSchema change is
// reconstructed from the plan at drive time, not carried as a task.
func newTaskLessFinalizerGroup(now time.Time, operationKey string) *storage.ApplyOperationWithTasks {
	return &storage.ApplyOperationWithTasks{
		Operation: &storage.ApplyOperation{
			Deployment:    "region-a",
			OperationKey:  operationKey,
			OperationKind: storage.ApplyOperationKindGroupFinalizer,
			Target:        "payments-region-a",
			State:         state.ApplyOperation.Pending,
			CutoverPolicy: storage.CutoverPolicyRolling,
			OnFailure:     storage.OnFailureHalt,
			CreatedAt:     now,
			UpdatedAt:     now,
		},
	}
}

func requestPendingStop(t *testing.T, ctx context.Context, stor storage.Storage, applyID int64) {
	t.Helper()
	_, _, err := stor.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     applyID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "matrix-test",
	})
	require.NoError(t, err, "request pending stop")
}

func stalenessBackdate(t *testing.T, ctx context.Context, db *sql.DB, applyID int64) {
	t.Helper()
	_, err := db.ExecContext(ctx,
		"UPDATE apply_operations SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE apply_id = ?", applyID)
	require.NoError(t, err, "backdate operation heartbeats")
}

// --- service + fakes ----------------------------------------------------

func newMatrixService(t *testing.T, stor storage.Storage, clients map[string]tern.Client) *matrixService {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := &ServerConfig{OperatorClaimOperations: new(true)}
	svc := New(stor, cfg, clients, logger)

	summary := &matrixSummaryRecorder{}
	svc.OnApplyTerminalSummary = func(_ context.Context, apply *storage.Apply, tasks []*storage.Task) error {
		summary.record(apply, tasks)
		return nil
	}
	svc.OnApplyRecovered = func(*storage.Apply) { summary.recordRecovered() }
	return &matrixService{Service: svc, matrixSummary: summary}
}

type matrixService struct {
	*Service
	matrixSummary *matrixSummaryRecorder
}

func driveNextOperation(t *testing.T, ctx context.Context, svc *matrixService, driverID int) {
	t.Helper()
	svc.recoverApplyOperation(ctx, driverID, fmt.Sprintf("matrix-driver-%d", driverID))
}

// driveOneOperation polls like a real operator driver until this driver actually
// drives one operation. A single claim can find nothing when a peer claims first,
// so it retries with a bounded deadline rather than giving up after one attempt.
func driveOneOperation(t *testing.T, ctx context.Context, svc *matrixService, rec *driveRecorder, driverID int) {
	t.Helper()
	owner := fmt.Sprintf("matrix-driver-%d", driverID)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		before := rec.resumeCount()
		svc.recoverApplyOperation(ctx, driverID, owner)
		if rec.resumeCount() > before {
			return // this driver (or a peer it unblocked) drove an operation
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Errorf("driver %d never drove an operation within the deadline", driverID)
}

// matrixClients builds one deterministic tern.Client per deployment, keyed by
// the "deployment/environment" format the service uses to route.
func matrixClients(stor storage.Storage, rec *driveRecorder, outcomes map[string]matrixOutcome) map[string]tern.Client {
	clients := make(map[string]tern.Client, len(outcomes))
	for dep, outcome := range outcomes {
		clients[dep+"/staging"] = &matrixTernClient{
			mockTernClient: &mockTernClient{},
			stor:           stor,
			deployment:     dep,
			rec:            rec,
			outcome:        outcome,
		}
	}
	return clients
}

type matrixOutcome struct {
	taskState        string
	errMsg           string
	gate             *driveGate
	probeParentWrite bool
}

// matrixTernClient is a deterministic tern.Client that simulates one
// deployment's drive by transitioning that operation's own task rows under the
// operation lease already on the context, then letting the real operator derive
// the operation and aggregate parent state. It embeds mockTernClient only to
// satisfy the rest of the tern.Client interface.
type matrixTernClient struct {
	*mockTernClient
	stor       storage.Storage
	deployment string
	rec        *driveRecorder
	outcome    matrixOutcome
}

func (m *matrixTernClient) ResumeApplyOperation(ctx context.Context, apply *storage.Apply, applyOperationID int64) error {
	op, err := m.stor.ApplyOperations().Get(ctx, applyOperationID)
	if err != nil {
		return fmt.Errorf("matrix fake: load operation %d: %w", applyOperationID, err)
	}
	if op == nil {
		return fmt.Errorf("matrix fake: operation %d not found", applyOperationID)
	}
	m.rec.recordResume(m.deployment, op)

	// The gate proves work drives run concurrently; the group_finalizer drives
	// only after its work siblings settle, so it never participates in the gate.
	if m.outcome.gate != nil && op.OperationKind != storage.ApplyOperationKindGroupFinalizer {
		m.outcome.gate.arriveAndWait()
	}

	if m.outcome.probeParentWrite {
		// A direct parent write under the operation-only lease must fail closed:
		// the parent applies row is owned solely by the projection CAS.
		probe := *apply
		probe.State = state.Apply.Completed
		m.rec.recordParentWrite(m.stor.Applies().Update(ctx, &probe))
	}

	// A group_finalizer carries no tasks: the real drive (driveGroupFinalizer)
	// applies the namespace VSchema and marks the operation row directly. Simulate
	// that outcome here rather than transitioning task rows it does not have.
	if op.OperationKind == storage.ApplyOperationKindGroupFinalizer {
		if state.IsState(m.outcome.taskState, state.Task.Failed) {
			return m.stor.ApplyOperations().MarkFailed(ctx, op.ID, m.outcome.errMsg)
		}
		if state.IsState(m.outcome.taskState, state.Task.Completed) {
			return m.stor.ApplyOperations().MarkCompleted(ctx, op.ID)
		}
		return nil
	}

	tasks, err := m.stor.Tasks().GetByApplyOperationID(ctx, applyOperationID)
	if err != nil {
		return fmt.Errorf("matrix fake: load tasks for operation %d: %w", applyOperationID, err)
	}
	for _, task := range tasks {
		task.State = m.outcome.taskState
		if state.IsState(m.outcome.taskState, state.Task.Failed) {
			task.ErrorMessage = m.outcome.errMsg
		}
		if state.IsTerminalTaskState(m.outcome.taskState) {
			now := time.Now()
			task.CompletedAt = &now
		}
		if err := m.stor.Tasks().Update(ctx, task); err != nil {
			return fmt.Errorf("matrix fake: update task %d for deployment %s: %w", task.ID, m.deployment, err)
		}
	}
	return nil
}

// --- recorders ----------------------------------------------------------

type driveRecorder struct {
	mu          sync.Mutex
	order       []string
	opKeys      []string
	opKinds     []string
	parentWrite []error
}

func (r *driveRecorder) recordResume(deployment string, op *storage.ApplyOperation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.order = append(r.order, deployment)
	r.opKeys = append(r.opKeys, op.OperationKey)
	r.opKinds = append(r.opKinds, op.OperationKind)
}

func (r *driveRecorder) recordParentWrite(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.parentWrite = append(r.parentWrite, err)
}

func (r *driveRecorder) resumeOrder() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.order...)
}

func (r *driveRecorder) resumeCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.order)
}

func (r *driveRecorder) resumeOperationKeys() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.opKeys...)
}

func (r *driveRecorder) resumeOperationKinds() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.opKinds...)
}

func (r *driveRecorder) parentWriteErrors() []error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]error(nil), r.parentWrite...)
}

type matrixSummaryRecorder struct {
	mu        sync.Mutex
	calls     int
	tasks     []*storage.Task
	recovered int
}

func (r *matrixSummaryRecorder) record(_ *storage.Apply, tasks []*storage.Task) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	r.tasks = tasks
}

func (r *matrixSummaryRecorder) recordRecovered() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recovered++
}

func (r *matrixSummaryRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func (r *matrixSummaryRecorder) recoveredCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.recovered
}

func (r *matrixSummaryRecorder) lastTasks() []*storage.Task {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*storage.Task(nil), r.tasks...)
}

// driveGate releases all participants only once the expected number have arrived,
// proving the operation drives run concurrently rather than serializing.
type driveGate struct {
	t        *testing.T
	wg       sync.WaitGroup
	released chan struct{}
	once     sync.Once
}

func newDriveGate(t *testing.T, participants int) *driveGate {
	g := &driveGate{t: t, released: make(chan struct{})}
	g.wg.Add(participants)
	return g
}

func (g *driveGate) arriveAndWait() {
	g.wg.Done()
	g.once.Do(func() {
		go func() {
			g.wg.Wait()
			close(g.released)
		}()
	})
	select {
	case <-g.released:
	case <-time.After(20 * time.Second):
		// A participant never arrived within the deadline: the drives serialized
		// instead of running concurrently, which defeats the invariant this gate
		// enforces. Fail the test, then unblock so it reports the failure rather
		// than hanging.
		g.t.Errorf("drive gate timed out: drives serialized instead of running concurrently")
	}
}

// --- read helpers -------------------------------------------------------

func getApply(t *testing.T, ctx context.Context, stor storage.Storage, applyID int64) *storage.Apply {
	t.Helper()
	apply, err := stor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	return apply
}

func opState(t *testing.T, ctx context.Context, stor storage.Storage, opID int64) string {
	t.Helper()
	op, err := stor.ApplyOperations().Get(ctx, opID)
	require.NoError(t, err)
	require.NotNil(t, op)
	return op.State
}

func stopRequestCompleted(t *testing.T, ctx context.Context, stor storage.Storage, applyID int64) bool {
	t.Helper()
	pending, err := stor.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationStop)
	require.NoError(t, err)
	return pending == nil
}
