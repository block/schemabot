//go:build integration

package api

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// A deployment-keyed apply receives its operations one dispatch at a time, so
// the first-attached operation can be claimed — and finish — while the
// generation manifest still expects siblings. This test proves the drive-mode
// decision honors the manifest: the lone attached operation drives under the
// operation lease, the projection holds the parent apply open after that
// operation completes, a late sibling dispatch still attaches, and the apply
// reaches its whole-generation verdict only once every declared key has
// attached and finished.
func TestOperatorManifestKeyedApplyWaitsForLateSiblings(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const (
		deployment = "region-a"
		keyUsers   = "commerce/-80/users"
		keyOrders  = "commerce/-80/orders"
	)

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)
	resetMatrixTables(t, ctx, db)

	seed := seedKeyedManifestApply(t, ctx, stor, deployment, keyUsers, []string{keyUsers, keyOrders})

	rec := &driveRecorder{}
	// Each drive also probes that a direct parent applies write fails closed:
	// on a manifest-carrying apply the parent row is owned solely by the
	// projection CAS, so a drive holding only an operation lease must be
	// refused — a successful write here would terminalize the parent and make
	// the late sibling dispatch unattachable.
	svc := newMatrixService(t, stor, matrixClients(stor, rec, map[string]matrixOutcome{
		deployment: {taskState: state.Task.Completed, probeParentWrite: true},
	}))

	// Drive the lone attached operation to completion. The manifest still
	// expects the orders sibling, so the parent must stay open.
	driveNextOperation(t, ctx, svc, 1)

	firstOp, err := stor.ApplyOperations().Get(ctx, seed.firstOpID)
	require.NoError(t, err)
	require.NotNil(t, firstOp)
	assert.Equal(t, state.ApplyOperation.Completed, firstOp.State,
		"the attached operation's own drive must settle it")

	held := getApply(t, ctx, stor, seed.applyID)
	assert.Equal(t, state.Apply.Running, held.State,
		"the projection must hold the apply running while manifest keys are unattached")
	assert.Nil(t, held.CompletedAt, "a held apply must not carry a completion time")
	assert.Zero(t, svc.matrixSummary.count(),
		"no terminal summary may publish while the generation is incomplete")
	assert.Zero(t, svc.matrixSummary.recoveredCount(),
		"a manifest-carrying apply must not take the per-driver parent-lease path")

	// The sibling dispatch arrives late, exactly as a control plane fanning out
	// a large generation delivers it. It must attach to a still-active apply.
	attachSiblingOperation(t, ctx, stor, held, deployment, keyOrders)

	driveNextOperation(t, ctx, svc, 2)

	done := getApply(t, ctx, stor, seed.applyID)
	assert.Equal(t, state.Apply.Completed, done.State,
		"a fully attached and finished manifest completes the apply")
	assert.NotNil(t, done.CompletedAt, "the whole-generation verdict stamps completed_at")
	assert.Equal(t, 1, svc.matrixSummary.count(),
		"the aggregate terminal summary must publish exactly once")

	ops, err := stor.ApplyOperations().ListByApply(ctx, seed.applyID)
	require.NoError(t, err)
	require.Len(t, ops, 2)
	for _, op := range ops {
		assert.Equal(t, state.ApplyOperation.Completed, op.State,
			"operation %s must be completed", op.OperationKey)
	}

	parentWriteErrs := rec.parentWriteErrors()
	require.Len(t, parentWriteErrs, 2, "both drives must have probed the parent write")
	for _, err := range parentWriteErrs {
		assert.ErrorIs(t, err, storage.ErrApplyLeaseLost,
			"a manifest-carrying apply's drives hold only operation leases, so a direct parent applies write must be refused")
	}
}

// One deployment addresses two targets, but only commerce-001 needs work.
// The converged member stays completed on the control plane without a remote
// dispatch. Real gRPC admission and both planes' operator projections settle
// the deployment's one remote parent, releasing it for the next apply. The
// matrix driver supplies only deterministic task execution, not admission or
// parent state, so the immutable manifest and target reservation are exercised.
func TestOperatorRemoteManifestConvergedMemberSettlesAndAcceptsNextApply(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const (
		deployment = "region-a"
		working    = "commerce-001"
		converged  = "commerce-002"
	)
	cpStor := mysqlstore.New(openMatrixStorage(t))
	dpDatabase := newStorageDatabaseWithSchema(t)
	dpStor := mysqlstore.New(openStorageDB(t, dpDatabase.DSN))
	logger := slog.Default()
	dpClient, err := tern.NewLocalClient(tern.LocalConfig{
		Database: deployment, Type: storage.DatabaseTypeMySQL, TargetDSN: dpDatabase.DSN,
	}, dpStor, logger)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(dpClient) })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "localhost:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	tern.NewServer(dpClient, logger).Register(grpcServer)
	serveErr := make(chan error, 1)
	go func() { serveErr <- grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		// Stop returns nil from Serve, so any other result is a server failure
		// the fixture would otherwise report only as a later timeout.
		grpcServer.Stop()
		assert.NoError(t, <-serveErr)
	})
	cpClient, err := tern.NewGRPCClient(tern.Config{Address: listener.Addr().String(), Storage: cpStor, Logger: logger})
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(cpClient) })

	now := time.Now()
	plan := &storage.Plan{
		PlanIdentifier: "remote-manifest-working", Database: "commerce", DatabaseType: storage.DatabaseTypeMySQL,
		Deployment: deployment, Target: working, Environment: "staging", CreatedAt: now,
		Namespaces: map[string]*storage.NamespacePlanData{
			"commerce": {Tables: []storage.TableChange{{Namespace: "commerce", Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `c` int", Operation: "alter"}}},
		},
	}
	plan.ID, err = cpStor.Plans().Create(ctx, plan)
	require.NoError(t, err)
	// The reviewed working target's plan already exists on the data plane that
	// produced it. No target re-plan or DDL execution is needed in this fixture.
	remotePlan := *plan
	remotePlan.ID = 0
	_, err = dpStor.Plans().Create(ctx, &remotePlan)
	require.NoError(t, err)
	convergedPlan := &storage.Plan{
		PlanIdentifier: "remote-manifest-converged", Database: "commerce", DatabaseType: storage.DatabaseTypeMySQL,
		Deployment: deployment, Target: converged, Environment: "staging", CreatedAt: now,
	}
	convergedPlan.ID, err = cpStor.Plans().Create(ctx, convergedPlan)
	require.NoError(t, err)
	options := storage.ApplyOptions{Target: working}
	groups, _, err := buildApplyOperationGroups(plan, applyTaskChanges(plan), []applyMember{
		{Target: routing.ExecutionTarget{Deployment: deployment, Target: working}, Plan: plan},
		{Target: routing.ExecutionTarget{Deployment: deployment, Target: converged}, Plan: convergedPlan},
	}, "staging", options, storage.CutoverPolicyRolling, storage.OnFailureHalt, now)
	require.NoError(t, err)
	require.Len(t, groups, 2)
	require.Empty(t, groups[1].Tasks)
	require.Equal(t, state.ApplyOperation.Completed, groups[1].Operation.State)
	require.Nil(t, groups[1].Operation.StartedAt)
	apply := &storage.Apply{
		ApplyIdentifier: "remote-manifest-rollout", PlanID: plan.ID, Database: "commerce", DatabaseType: storage.DatabaseTypeMySQL,
		Deployment: deployment, Environment: "staging", Engine: storage.EngineSpirit, State: state.Apply.Pending,
		Options: storage.MarshalApplyOptions(options), CreatedAt: now, UpdatedAt: now,
	}
	apply.ID, err = cpStor.Applies().CreateWithGroupedOperations(ctx, apply, groups)
	require.NoError(t, err)
	cpService := newMatrixService(t, cpStor, map[string]tern.Client{deployment + "/staging": cpClient})
	dpRecorder := &driveRecorder{}
	dpService := newMatrixService(t, dpStor, matrixClients(dpStor, dpRecorder, map[string]matrixOutcome{
		deployment: {taskState: state.Task.Completed},
	}))

	driveDone := make(chan struct{})
	go func() {
		cpService.recoverApplyOperation(ctx, 1, "remote-manifest-driver")
		close(driveDone)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-driveDone:
		case <-time.After(30 * time.Second):
			t.Error("control-plane drive did not exit before teardown")
		}
	})
	var remoteID string
	require.Eventually(t, func() bool {
		op, getErr := cpStor.ApplyOperations().Get(ctx, groups[0].Operation.ID)
		if getErr != nil || op == nil {
			return false
		}
		remoteID = op.RemoteApplyID()
		return remoteID != ""
	}, 10*time.Second, 20*time.Millisecond, "the working member never dispatched")

	remote, err := dpStor.Applies().GetByApplyIdentifier(ctx, remoteID)
	require.NoError(t, err)
	require.NotNil(t, remote)
	workingStep := storage.TargetOperationKey(working, storage.RolloutStepOperationKey(1))
	assert.Equal(t, []string{workingStep}, remote.ExpectedOperationKeys)
	remoteOps, err := dpStor.ApplyOperations().ListByApply(ctx, remote.ID)
	require.NoError(t, err)
	require.Len(t, remoteOps, 1)
	assert.Equal(t, workingStep, remoteOps[0].OperationKey, "the converged member still requires target-qualified dispatch keys")
	assert.Equal(t, 1, remoteOps[0].RolloutStep, "the data plane runs the step the control plane dispatched")
	driveNextOperation(t, ctx, dpService, 2)
	remote = getApply(t, ctx, dpStor, remote.ID)
	require.Equal(t, state.Apply.Completed, remote.State, "the remote parent must settle without waiting for a converged member dispatch")
	assert.Equal(t, []string{workingStep}, remote.ExpectedOperationKeys, "settling the generation never changes its manifest")
	assert.Equal(t, []string{workingStep}, dpRecorder.resumeOperationKeys())
	select {
	case <-driveDone:
	case <-ctx.Done():
		t.Fatal("the control-plane drive did not finish after remote completion")
	}
	require.Equal(t, state.Apply.Completed, getApply(t, ctx, cpStor, apply.ID).State)
	settled, err := cpStor.ApplyOperations().Get(ctx, groups[1].Operation.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Nil(t, settled.StartedAt)
	assert.Empty(t, settled.RemoteApplyID(), "the converged member remains a local placeholder")

	next, err := cpClient.Apply(ctx, &ternv1.ApplyRequest{
		PlanId: plan.PlanIdentifier, Database: "commerce", Type: storage.DatabaseTypeMySQL,
		Target: working, Environment: "staging", IdempotencyKey: "remote-manifest-next-apply",
	})
	require.NoError(t, err)
	require.True(t, next.Accepted, "the completed remote parent must release the deployment for the next apply: %s", next.ErrorMessage)
	assert.NotEqual(t, remoteID, next.ApplyId)
}

type seededKeyedManifestApply struct {
	applyID   int64
	firstOpID int64
}

// seedKeyedManifestApply stores an apply exactly as a keyed dispatch creates
// one: the generation manifest recorded at creation, and only the dispatching
// operation attached with its task.
func seedKeyedManifestApply(t *testing.T, ctx context.Context, stor storage.Storage, deployment, firstKey string, manifest []string) seededKeyedManifestApply {
	t.Helper()
	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier:       "keyed-manifest-hold",
		Database:              "commerce",
		DatabaseType:          storage.DatabaseTypeMySQL,
		Repository:            "octocat/hello-world",
		PullRequest:           1,
		Environment:           "staging",
		Deployment:            deployment,
		Caller:                "manifest-test",
		Engine:                storage.EngineForType(storage.DatabaseTypeMySQL),
		State:                 state.Apply.Pending,
		Options:               storage.MarshalApplyOptions(storage.ApplyOptions{}),
		IdempotencyKey:        "schemabot:v1:keyed-manifest-hold",
		ExpectedOperationKeys: manifest,
		CreatedAt:             now,
		UpdatedAt:             now,
	}
	operations := []*storage.ApplyOperation{{
		Deployment:   deployment,
		OperationKey: firstKey,
		Target:       "commerce-" + deployment,
		State:        state.ApplyOperation.Pending,
		CreatedAt:    now,
		UpdatedAt:    now,
	}}
	tasks := []*storage.Task{keyedManifestTask("keyed-manifest-hold-users", "users", now)}

	applyID, err := stor.Applies().CreateWithTasksAndOperations(ctx, apply, tasks, operations)
	require.NoError(t, err, "seed keyed manifest apply")
	return seededKeyedManifestApply{applyID: applyID, firstOpID: operations[0].ID}
}

// attachSiblingOperation attaches one more operation and task to the apply the
// way a later sibling dispatch does, requiring the apply to still be active.
func attachSiblingOperation(t *testing.T, ctx context.Context, stor storage.Storage, apply *storage.Apply, deployment, key string) {
	t.Helper()
	now := time.Now()
	operation := &storage.ApplyOperation{
		Deployment:   deployment,
		OperationKey: key,
		Target:       "commerce-" + deployment,
		State:        state.ApplyOperation.Pending,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	tasks := []*storage.Task{keyedManifestTask("keyed-manifest-hold-orders", "orders", now)}
	require.NoError(t, stor.Applies().AttachOperationWithTasks(ctx, apply, operation, tasks),
		"a manifest-declared sibling must attach to the still-active apply")
}

func keyedManifestTask(identifier, table string, now time.Time) *storage.Task {
	return &storage.Task{
		TaskIdentifier: identifier,
		Database:       "commerce",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Engine:         storage.EngineForType(storage.DatabaseTypeMySQL),
		Repository:     "octocat/hello-world",
		PullRequest:    1,
		Environment:    "staging",
		State:          state.Task.Pending,
		Options:        storage.MarshalApplyOptions(storage.ApplyOptions{}),
		Namespace:      "commerce",
		TableName:      table,
		DDL:            "ALTER TABLE `" + table + "` ADD COLUMN `c` int",
		DDLAction:      "alter",
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}
