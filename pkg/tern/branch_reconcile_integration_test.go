//go:build integration

package tern

import (
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine/planetscale"
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
