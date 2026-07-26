//go:build integration

package pendingdrops

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createSourceTable creates a table in testdb and registers cleanup so tests
// sharing the container never leak sources into each other.
func createSourceTable(t *testing.T, db *sql.DB, name string) {
	t.Helper()
	_, err := db.ExecContext(t.Context(), fmt.Sprintf("CREATE TABLE `testdb`.`%s` (id INT PRIMARY KEY)", name))
	require.NoError(t, err, "create source table")
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(t.Context())
		_, err := db.ExecContext(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS `testdb`.`%s`", name))
		require.NoError(t, err, "drop source table")
	})
}

// A quarantine records its destination in the intents ledger before the
// rename executes, so a later run that finds the source table missing can
// prove the rename landed and report the exact quarantine location.
func TestMoveTables_RecordsIntentAndAlreadyQuarantinedFindsIt(t *testing.T) {
	db := setupCleanerTest(t)
	createSourceTable(t, db, "move_ledger_source")

	moved, err := MoveTables(t.Context(), db,
		[]TableMove{{SchemaName: "testdb", TableName: "move_ledger_source"}}, time.Now())
	require.NoError(t, err)
	require.Len(t, moved, 1)

	quarantineTable, quarantined, err := AlreadyQuarantined(t.Context(), db, "testdb", "move_ledger_source")
	require.NoError(t, err)
	assert.True(t, quarantined, "a moved table must be provable as quarantined")
	assert.Equal(t, moved[0].QuarantineTable, quarantineTable,
		"the ledger must point at the destination the rename produced")
}

// A missing source table with no quarantine ledger at all is not provably
// quarantined: nothing was ever moved on this host.
func TestAlreadyQuarantined_NoLedgerReportsNotQuarantined(t *testing.T) {
	db := setupCleanerTest(t)

	quarantineTable, quarantined, err := AlreadyQuarantined(t.Context(), db, "testdb", "never_moved")
	require.NoError(t, err)
	assert.False(t, quarantined)
	assert.Empty(t, quarantineTable)
}

// An existing ledger without an intent row for the source table is not proof:
// only the table's own recorded rename counts.
func TestAlreadyQuarantined_NoIntentRowReportsNotQuarantined(t *testing.T) {
	db := setupCleanerTest(t)
	createSourceTable(t, db, "move_other_source")
	_, err := MoveTables(t.Context(), db,
		[]TableMove{{SchemaName: "testdb", TableName: "move_other_source"}}, time.Now())
	require.NoError(t, err)

	quarantineTable, quarantined, err := AlreadyQuarantined(t.Context(), db, "testdb", "never_moved")
	require.NoError(t, err)
	assert.False(t, quarantined, "another table's quarantine must not count as this table's")
	assert.Empty(t, quarantineTable)
}

// An intent whose recorded destination no longer exists proves nothing: either
// the rename never landed or the quarantined copy was reclaimed, and in both
// cases the missing source cannot be treated as quarantined.
func TestAlreadyQuarantined_MissingDestinationReportsNotQuarantined(t *testing.T) {
	db := setupCleanerTest(t)
	createSourceTable(t, db, "move_reclaimed_source")
	moved, err := MoveTables(t.Context(), db,
		[]TableMove{{SchemaName: "testdb", TableName: "move_reclaimed_source"}}, time.Now())
	require.NoError(t, err)
	require.Len(t, moved, 1)

	_, err = db.ExecContext(t.Context(),
		fmt.Sprintf("DROP TABLE `%s`.`%s`", Database, moved[0].QuarantineTable))
	require.NoError(t, err, "reclaim the quarantined copy")

	quarantineTable, quarantined, err := AlreadyQuarantined(t.Context(), db, "testdb", "move_reclaimed_source")
	require.NoError(t, err)
	assert.False(t, quarantined, "a reclaimed destination must not count as quarantined")
	assert.Empty(t, quarantineTable)
}

// When the same source table is quarantined more than once, the latest intent
// is authoritative — it reflects the most recent rename of that source.
func TestAlreadyQuarantined_LatestIntentWins(t *testing.T) {
	db := setupCleanerTest(t)

	createSourceTable(t, db, "move_repeat_source")
	first, err := MoveTables(t.Context(), db,
		[]TableMove{{SchemaName: "testdb", TableName: "move_repeat_source"}}, time.Now().Add(-time.Hour))
	require.NoError(t, err)
	require.Len(t, first, 1)

	_, err = db.ExecContext(t.Context(), "CREATE TABLE `testdb`.`move_repeat_source` (id INT PRIMARY KEY)")
	require.NoError(t, err, "recreate source table")
	second, err := MoveTables(t.Context(), db,
		[]TableMove{{SchemaName: "testdb", TableName: "move_repeat_source"}}, time.Now())
	require.NoError(t, err)
	require.Len(t, second, 1)

	quarantineTable, quarantined, err := AlreadyQuarantined(t.Context(), db, "testdb", "move_repeat_source")
	require.NoError(t, err)
	assert.True(t, quarantined)
	assert.Equal(t, second[0].QuarantineTable, quarantineTable,
		"the latest quarantine of the source must win")
}

// The cleaner prunes intent rows on the same retention schedule as the
// quarantined tables they describe, and never treats the ledger itself as a
// quarantined table.
func TestCleaner_PrunesExpiredIntentsKeepsFresh(t *testing.T) {
	db := setupCleanerTest(t)

	createSourceTable(t, db, "prune_expired_source")
	expired, err := MoveTables(t.Context(), db,
		[]TableMove{{SchemaName: "testdb", TableName: "prune_expired_source"}}, time.Now().Add(-8*24*time.Hour))
	require.NoError(t, err)
	require.Len(t, expired, 1)

	createSourceTable(t, db, "prune_fresh_source")
	fresh, err := MoveTables(t.Context(), db,
		[]TableMove{{SchemaName: "testdb", TableName: "prune_fresh_source"}}, time.Now())
	require.NoError(t, err)
	require.Len(t, fresh, 1)

	cleaner := testCleaner(t, DefaultRetention, false)
	require.NoError(t, cleaner.Run(t.Context()))

	tables := quarantinedTables(t, db)
	assert.NotContains(t, tables, expired[0].QuarantineTable, "the expired quarantined table is reclaimed")
	assert.Contains(t, tables, fresh[0].QuarantineTable, "the fresh quarantined table survives")
	assert.Contains(t, tables, IntentsTable, "the ledger itself is never dropped")

	var intentCount int
	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf(
		"SELECT COUNT(*) FROM `%s`.`%s` WHERE source_table = ?", Database, IntentsTable),
		"prune_expired_source").Scan(&intentCount))
	assert.Equal(t, 0, intentCount, "the expired intent row is pruned with its table")

	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf(
		"SELECT COUNT(*) FROM `%s`.`%s` WHERE source_table = ?", Database, IntentsTable),
		"prune_fresh_source").Scan(&intentCount))
	assert.Equal(t, 1, intentCount, "the fresh intent row survives")
}

// In dry-run mode the cleaner reports without deleting: intent rows survive
// alongside the tables that would be dropped.
func TestCleaner_DryRunKeepsIntents(t *testing.T) {
	db := setupCleanerTest(t)

	createSourceTable(t, db, "dryrun_intent_source")
	expired, err := MoveTables(t.Context(), db,
		[]TableMove{{SchemaName: "testdb", TableName: "dryrun_intent_source"}}, time.Now().Add(-8*24*time.Hour))
	require.NoError(t, err)
	require.Len(t, expired, 1)

	cleaner := testCleaner(t, DefaultRetention, true)
	require.NoError(t, cleaner.Run(t.Context()))

	tables := quarantinedTables(t, db)
	assert.Contains(t, tables, expired[0].QuarantineTable, "dry run drops nothing")

	var intentCount int
	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf(
		"SELECT COUNT(*) FROM `%s`.`%s` WHERE source_table = ?", Database, IntentsTable),
		"dryrun_intent_source").Scan(&intentCount))
	assert.Equal(t, 1, intentCount, "dry run prunes no intent rows")
}
