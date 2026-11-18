//go:build integration

package tern

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine/planetscale"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// A queued PlanetScale apply has an operation row but no stored engine resume
// state yet, because the engine records a branch only once it is driven. The
// branch reconcile reads it as using the branch its plan names, and nothing
// else, so the pass neither stops on the missing record nor leaves the branch
// the apply is about to create unprotected.
func TestBranchesInUseNamesAQueuedApplyBranchFromItsPlan(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	client := newSpiritControlClient(t, dsn, stor)
	ctx := t.Context()

	apply := dispatchQueuedApply(t, stor, client, []storage.TableChange{{
		Namespace: "testdb", Table: "users",
		DDL: "ALTER TABLE `users` ADD COLUMN reconcile_note VARCHAR(255)", Operation: "alter",
	}})
	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, ops, 1, "a dispatched apply carries its operation row before it is driven")
	_, err = stor.ApplyOperations().GetEngineResumeState(ctx, ops[0].ID)
	require.ErrorIs(t, err, storage.ErrEngineResumeStateNotFound, "nothing has recorded a branch for the queued apply")

	// The helper dispatches a MySQL apply; the reconcile reads only
	// PlanetScale ones.
	db, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	_, err = db.ExecContext(ctx, "UPDATE applies SET database_type = ? WHERE id = ?", storage.DatabaseTypeVitess, apply.ID)
	require.NoError(t, err)

	plan, err := stor.Plans().GetByID(ctx, apply.PlanID)
	require.NoError(t, err)
	require.NotNil(t, plan)

	inUse, err := client.branchesInUse(ctx, "testdb")
	require.NoError(t, err)
	require.Equal(t, map[string]bool{planetscale.GenerateBranchName("testdb", plan.PlanIdentifier): true}, inUse)
}

// A data plane that resolves targets per request finds a settled PlanetScale
// apply's target in its own storage, reaches it through the router, and
// deletes the orphaned branch only under the apply target that apply's drive
// takes: while that target is held the branch is kept, and once it is
// released the branch is deleted.
func TestTargetRouterReconcilesAStoredTargetUnderTheHoldItsDriveTakes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	ctx := t.Context()

	// The helper dispatches a MySQL apply; this one stands in for a
	// PlanetScale apply on the target testdb-target that has since settled.
	queued := dispatchQueuedApply(t, stor, newSpiritControlClient(t, dsn, stor), []storage.TableChange{{
		Namespace: "testdb", Table: "users",
		DDL: "ALTER TABLE `users` ADD COLUMN reconcile_note VARCHAR(255)", Operation: "alter",
	}})
	db, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	_, err = db.ExecContext(ctx, "UPDATE applies SET database_type = ?, state = ? WHERE id = ?", storage.DatabaseTypeVitess, state.Apply.Completed, queued.ID)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "UPDATE apply_operations SET target = ? WHERE apply_id = ?", "testdb-target", queued.ID)
	require.NoError(t, err)
	settled, err := stor.Applies().Get(ctx, queued.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)

	orphan := "schemabot-testdb-target-ps-aaaa0001"
	planetScale := &branchReconcileClient{branches: []*ps.DatabaseBranch{
		{Name: "main", Production: true, CreatedAt: branchReconcileNow.Add(-48 * time.Hour)},
		{Name: orphan, CreatedAt: branchReconcileNow.Add(-48 * time.Hour)},
	}}
	router, err := NewTargetRouter(TargetRouterConfig{
		Resolver: planetScaleTargetResolver(),
		Storage:  stor,
		Logger:   slog.New(slog.DiscardHandler),
		LocalClientFactory: func(cfg LocalConfig, st storage.Storage, logger *slog.Logger) (Client, error) {
			return &LocalClient{config: cfg, storage: st, logger: logger, psClientFunc: func(_, _ string) (psclient.PSClient, error) { return planetScale, nil }}, nil
		},
	})
	require.NoError(t, err)
	opts := BranchReconcileOptions{Grace: branchReconcileGrace, Delete: true, Now: branchReconcileNow}

	var held RecentTargetsReconcileResult
	var heldErr error
	require.NoError(t, stor.Applies().WithTargetHeld(ctx, settled.Database, settled.DatabaseType, settled.Environment, func(ctx context.Context) error {
		held, heldErr = router.ReconcileRecentTargetBranches(ctx, opts)
		return nil
	}))
	require.ErrorContains(t, heldErr, "hold apply target testdb/vitess/"+localClientTestEnvironment+" to delete branch "+orphan,
		"the reconcile waits on the same apply target the drive holds")
	assert.Equal(t, RecentTargetsReconcileResult{Targets: 1}, held)
	assert.Empty(t, planetScale.deleted, "a branch is kept while its apply target is held")

	released, err := router.ReconcileRecentTargetBranches(ctx, opts)
	require.NoError(t, err)
	assert.Equal(t, RecentTargetsReconcileResult{Targets: 1, Deleted: 1}, released)
	assert.Equal(t, []string{orphan}, planetScale.deleted)
}
