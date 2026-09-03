//go:build integration

package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
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
