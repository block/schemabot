//go:build integration

package spirit

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/pendingdrops"
)

// releaseTestCleanup empties the target and the quarantine after the test. A
// release deliberately leaves the live table behind and may leave preserved
// copies in the quarantine, and the tests that plan against this database
// expect to find only what they created themselves.
func releaseTestCleanup(t *testing.T, db *sql.DB, tables ...string) {
	t.Helper()
	dropTablesOnCleanup(t, db, tables...)
	cleanupCtx := context.WithoutCancel(t.Context())
	t.Cleanup(func() {
		_, err := db.ExecContext(cleanupCtx,
			fmt.Sprintf("DROP DATABASE IF EXISTS %s", quoteIdentifier(pendingdrops.Database)))
		assert.NoError(t, err, "drop pending drops database")
	})
}

// seedArtifact creates an artifact table with the given number of rows so a
// test can tell a preserved copy from an empty stand-in.
func seedArtifact(t *testing.T, db *sql.DB, name string, rows int) {
	t.Helper()
	_, err := db.ExecContext(t.Context(),
		fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY AUTO_INCREMENT)", quoteIdentifier(name)))
	require.NoError(t, err, "create artifact %s", name)
	for range rows {
		_, err := db.ExecContext(t.Context(), fmt.Sprintf("INSERT INTO %s VALUES ()", quoteIdentifier(name)))
		require.NoError(t, err, "seed artifact %s", name)
	}
}

// quarantinedRowCount returns the number of rows in a quarantined table.
func quarantinedRowCount(t *testing.T, db *sql.DB, name string) int {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", quoteIdentifier(pendingdrops.Database), quoteIdentifier(name)),
	).Scan(&count), "count rows in quarantined %s", name)
	return count
}

// quarantineDatabaseExists reports whether the quarantine database was created.
func quarantineDatabaseExists(t *testing.T, db *sql.DB) bool {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?",
		pendingdrops.Database).Scan(&count))
	return count > 0
}

// Cancelling a schema change that stopped before cutover leaves only the shadow
// table it was copying into. Those rows are preserved in the quarantine so the
// work stays recoverable, the checkpoints describing a copy that no longer
// exists are dropped, and the user's live table is untouched.
func TestEngine_ReleaseCancelledArtifacts_PreCutoverPreservesCopy(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	const baseTable = "orders"
	releaseTestCleanup(t, db, baseTable)
	seedArtifact(t, db, baseTable, 3)
	seedArtifact(t, db, utils.NewTableName(baseTable), 7)
	seedArtifact(t, db, utils.CheckpointTableName(baseTable), 1)
	seedArtifact(t, db, sharedCheckpointTable, 1)
	seedArtifact(t, db, deferredCutoverSentinelTable, 1)

	eng := New(Config{})
	result, err := eng.ReleaseCancelledArtifacts(t.Context(), &engine.ReleaseArtifactsRequest{
		Database:    "testdb",
		Tables:      []string{baseTable},
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err, "ReleaseCancelledArtifacts()")
	require.NotNil(t, result)

	assert.True(t, tableExists(t, db, baseTable), "the live table must be left alone")
	assert.False(t, tableExists(t, db, utils.NewTableName(baseTable)), "the shadow table must leave the target")

	require.Len(t, result.Preserved, 1, "only the shadow table holds rows before cutover")
	assert.Equal(t, "testdb."+utils.NewTableName(baseTable), result.Preserved[0].Source)

	quarantined := listQuarantinedTables(t, db)
	require.Len(t, quarantined, 1)
	assert.Contains(t, quarantined[0], utils.NewTableName(baseTable))
	assert.Equal(t, pendingdrops.Database+"."+quarantined[0], result.Preserved[0].Destination)
	quarantinedAt, ok := pendingdrops.ParseTimestamp(quarantined[0])
	require.True(t, ok, "quarantined name %q must carry a parseable timestamp", quarantined[0])
	assert.WithinDuration(t, time.Now(), quarantinedAt, time.Minute)
	assert.Equal(t, 7, quarantinedRowCount(t, db, quarantined[0]), "the copied rows must survive")

	assert.ElementsMatch(t, []string{
		utils.CheckpointTableName(baseTable),
		sharedCheckpointTable,
		deferredCutoverSentinelTable,
	}, result.Discarded)
	for _, name := range result.Discarded {
		assert.False(t, tableExists(t, db, name), "metadata artifact should be dropped: %s", name)
	}
}

// A schema change cancelled after its tables were swapped leaves both the
// shadow table and the original it replaced. Both hold rows, so both are
// preserved in the same move.
func TestEngine_ReleaseCancelledArtifacts_PostCutoverPreservesBothCopies(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	const baseTable = "payments"
	releaseTestCleanup(t, db, baseTable)
	seedArtifact(t, db, baseTable, 2)
	seedArtifact(t, db, utils.NewTableName(baseTable), 5)
	seedArtifact(t, db, utils.OldTableName(baseTable), 9)

	eng := New(Config{})
	result, err := eng.ReleaseCancelledArtifacts(t.Context(), &engine.ReleaseArtifactsRequest{
		Database:    "testdb",
		Tables:      []string{baseTable},
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err, "ReleaseCancelledArtifacts()")

	assert.True(t, tableExists(t, db, baseTable), "the live table must be left alone")
	require.Len(t, result.Preserved, 2)
	assert.ElementsMatch(t, []string{
		"testdb." + utils.NewTableName(baseTable),
		"testdb." + utils.OldTableName(baseTable),
	}, []string{result.Preserved[0].Source, result.Preserved[1].Source})

	quarantined := listQuarantinedTables(t, db)
	require.Len(t, quarantined, 2)
	rowCounts := map[string]int{}
	for _, name := range quarantined {
		rowCounts[name] = quarantinedRowCount(t, db, name)
	}
	assert.ElementsMatch(t, []int{5, 9}, []int{rowCounts[quarantined[0]], rowCounts[quarantined[1]]},
		"both copies keep their rows")
}

// A deployment that runs no quarantine also runs no cleaner to empty one, so
// preserving a copy there would strand it in a database nothing ever sweeps.
// The copy is dropped outright instead, the same disposal the deployment chose
// for tables an operator asked to delete.
func TestEngine_ReleaseCancelledArtifacts_QuarantineDisabledDropsCopy(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	const baseTable = "shipments"
	releaseTestCleanup(t, db, baseTable)
	seedArtifact(t, db, baseTable, 1)
	seedArtifact(t, db, utils.NewTableName(baseTable), 4)
	seedArtifact(t, db, utils.CheckpointTableName(baseTable), 1)

	eng := New(Config{DisablePendingDrops: true})
	result, err := eng.ReleaseCancelledArtifacts(t.Context(), &engine.ReleaseArtifactsRequest{
		Database:    "testdb",
		Tables:      []string{baseTable},
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err, "ReleaseCancelledArtifacts()")

	assert.True(t, tableExists(t, db, baseTable), "the live table must be left alone")
	assert.False(t, tableExists(t, db, utils.NewTableName(baseTable)), "the shadow table must be dropped")
	assert.Empty(t, result.Preserved, "nothing is preserved where the quarantine is off")
	assert.ElementsMatch(t, []string{
		utils.NewTableName(baseTable),
		utils.CheckpointTableName(baseTable),
	}, result.Discarded)
	assert.False(t, quarantineDatabaseExists(t, db),
		"no quarantine database should be created when the quarantine is disabled")
}

// Releasing a target that carries no artifacts succeeds and touches nothing, so
// a cancel that arrives after cleanup already ran, or for a schema change that
// never reached the copy phase, is not turned into an error.
func TestEngine_ReleaseCancelledArtifacts_NothingToRelease(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	const baseTable = "invoices"
	releaseTestCleanup(t, db, baseTable)
	seedArtifact(t, db, baseTable, 1)

	eng := New(Config{})
	result, err := eng.ReleaseCancelledArtifacts(t.Context(), &engine.ReleaseArtifactsRequest{
		Database:    "testdb",
		Tables:      []string{baseTable},
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err, "ReleaseCancelledArtifacts()")

	assert.True(t, tableExists(t, db, baseTable), "the live table must be left alone")
	assert.Empty(t, result.Preserved)
	assert.Empty(t, result.Discarded)
	assert.False(t, quarantineDatabaseExists(t, db),
		"a release with nothing to preserve must not create the quarantine database")
}

// A caller that names the same table twice reclaims its artifacts once. The
// quarantine move is a single atomic RENAME, so a source repeated within it
// would fail the whole release and leave every artifact on the target.
func TestEngine_ReleaseCancelledArtifacts_RepeatedTableReleasedOnce(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	const baseTable = "receipts"
	releaseTestCleanup(t, db, baseTable)
	seedArtifact(t, db, baseTable, 1)
	seedArtifact(t, db, utils.NewTableName(baseTable), 5)
	seedArtifact(t, db, utils.CheckpointTableName(baseTable), 1)

	eng := New(Config{})
	result, err := eng.ReleaseCancelledArtifacts(t.Context(), &engine.ReleaseArtifactsRequest{
		Database:    "testdb",
		Tables:      []string{baseTable, baseTable},
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err, "ReleaseCancelledArtifacts()")
	require.NotNil(t, result)

	require.Len(t, result.Preserved, 1, "the copy must be preserved exactly once")
	assert.Equal(t, "testdb."+utils.NewTableName(baseTable), result.Preserved[0].Source)
	assert.Equal(t, []string{utils.CheckpointTableName(baseTable)}, result.Discarded)

	assert.True(t, tableExists(t, db, baseTable), "the live table must be left alone")
	assert.False(t, tableExists(t, db, utils.NewTableName(baseTable)), "the shadow table must leave the target")

	quarantined := listQuarantinedTables(t, db)
	require.Len(t, quarantined, 1)
	assert.Equal(t, 5, quarantinedRowCount(t, db, quarantined[0]), "the copied rows must survive")
}
