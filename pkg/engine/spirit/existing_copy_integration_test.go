//go:build integration

package spirit

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/block/spirit/pkg/checkpoint"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// findCopy looks for a copy on the target the way the engine does, on a pool of
// its own that lives no longer than the lookup.
func findCopy(t *testing.T, dsn string, tables []string) (*existingCopy, error) {
	t.Helper()

	target := &lazyTargetDB{dsn: dsn}
	defer target.close()
	return findExistingCopy(t.Context(), target, tables)
}

// seedCopy puts a Spirit copy on the target: a shadow table for each of tables,
// and a checkpoint under checkpointTable recording the batch it was made for.
// An empty checkpointTable seeds the shadow tables alone, which is what a copy
// whose checkpoint this batch cannot reach looks like.
func seedCopy(t *testing.T, db *sql.DB, tables []string, checkpointTable, statement string) {
	t.Helper()

	for _, table := range tables {
		_, err := db.ExecContext(t.Context(),
			fmt.Sprintf("CREATE TABLE `%s` (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)", utils.NewTableName(table)))
		require.NoError(t, err, "create shadow table for %s", table)
	}
	if checkpointTable == "" {
		return
	}

	cp := checkpoint.NewTable(db, checkpointTable, checkpoint.Transient)
	require.NoError(t, cp.Create(t.Context()), "create checkpoint table %s", checkpointTable)
	require.NoError(t, cp.Write(t.Context(), checkpoint.Record{
		Statement:       statement,
		CopierWatermark: `{"Key":["id"],"LowerBound":3952903346}`,
		Position:        "mysql-bin.024891:19443021",
	}), "write checkpoint row")
}

// A batch that meets its own copy on the target continues it: Spirit resumes
// from the checkpoint rather than re-copying, so the operator is told the copy
// is being adopted and nothing is destroyed.
func TestExistingCopyAdoptsMatchingBatch(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	const batch = "ALTER TABLE `xfers` ADD INDEX r_token (r_token)"
	seedCopy(t, db, []string{"xfers"}, utils.CheckpointTableName("xfers"), batch)

	found, err := findCopy(t, dsn, []string{"xfers"})
	require.NoError(t, err, "findExistingCopy")
	require.NotNil(t, found, "a copy of xfers is on the target")
	assert.Equal(t, []string{"xfers"}, found.CopiedTables)
	assert.Equal(t, "_xfers_chkpnt", found.CheckpointTable)
	assert.Equal(t, batch, found.Statement)

	disposition, reason := found.Disposition(batch, DefaultCheckpointMaxAge)
	assert.Equal(t, engine.CopyAdopt, disposition)
	assert.Empty(t, reason)
}

// A target with no copy has nothing to disclose, so the apply proceeds
// silently, exactly as it does today.
func TestExistingCopyAbsentOnCleanTarget(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	found, err := findCopy(t, dsn, []string{"xfers"})
	require.NoError(t, err, "findExistingCopy")
	require.Nil(t, found, "no copy of xfers is on the target")

	disposition, reason := found.Disposition("ALTER TABLE `xfers` ADD INDEX r_token (r_token)", DefaultCheckpointMaxAge)
	assert.Equal(t, engine.CopyNone, disposition)
	assert.Empty(t, reason)
}

// Spirit's checkpoint identity is the whole joined batch, not the statement for
// the table being copied. A batch that gains or loses an unrelated table
// therefore discards the copy even when the ALTER for the copied table is
// unchanged, which is the case the operator most needs warned about because the
// remedy is to restore the batch rather than to narrow it.
func TestExistingCopyDiscardsOnBatchDrift(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	const xfersAlter = "ALTER TABLE `xfers` ADD INDEX r_token (r_token)"
	const ledgerAlter = "ALTER TABLE `ledger_entries` ADD COLUMN note VARCHAR(64)"
	seedCopy(t, db, []string{"xfers", "ledger_entries"}, sharedCheckpointTable, xfersAlter+"; "+ledgerAlter)

	t.Run("the copy's own batch adopts it", func(t *testing.T) {
		found, err := findCopy(t, dsn, []string{"xfers", "ledger_entries"})
		require.NoError(t, err, "findExistingCopy")
		require.NotNil(t, found)
		assert.Equal(t, sharedCheckpointTable, found.CheckpointTable,
			"a batch of two or more reads the schema-level checkpoint")

		disposition, _ := found.Disposition(xfersAlter+"; "+ledgerAlter, DefaultCheckpointMaxAge)
		assert.Equal(t, engine.CopyAdopt, disposition)
	})

	t.Run("narrowing to the copied table discards it", func(t *testing.T) {
		found, err := findCopy(t, dsn, []string{"xfers"})
		require.NoError(t, err, "findExistingCopy")
		require.NotNil(t, found, "the shadow table is still a copy this batch will destroy")
		assert.Equal(t, []string{"xfers"}, found.CopiedTables)
		assert.Equal(t, "_xfers_chkpnt", found.CheckpointTable,
			"a batch of one reads the per-table checkpoint, which this copy did not write")
		assert.Empty(t, found.Statement, "the copy's checkpoint is unreachable from a batch of one")

		disposition, reason := found.Disposition(xfersAlter, DefaultCheckpointMaxAge)
		assert.Equal(t, engine.CopyDiscard, disposition)
		assert.Equal(t, engine.DiscardStatementDiffers, reason)
	})
}

// Dropping one table from a batch of three leaves a batch of two, so both the
// copy and the batch that meets it read the schema-level checkpoint: the
// checkpoint is reachable and its statement is intact. The copy is still
// discarded, on the joined statement alone, because Spirit's identity is the
// whole batch. This is the drift an operator sees when a table's change moves
// to its own PR while the tables around it keep copying.
func TestExistingCopyDiscardsWhenAnUnrelatedTableLeavesTheBatch(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	const xfersAlter = "ALTER TABLE `xfers` ADD INDEX r_token (r_token)"
	const ledgerAlter = "ALTER TABLE `ledger_entries` ADD COLUMN note VARCHAR(64)"
	const accountsAlter = "ALTER TABLE `accounts` ADD COLUMN region VARCHAR(16)"
	seedCopy(t, db, []string{"xfers", "ledger_entries", "accounts"}, sharedCheckpointTable,
		xfersAlter+"; "+ledgerAlter+"; "+accountsAlter)

	found, err := findCopy(t, dsn, []string{"xfers", "ledger_entries"})
	require.NoError(t, err, "findExistingCopy")
	require.NotNil(t, found)
	assert.Equal(t, []string{"xfers", "ledger_entries"}, found.CopiedTables,
		"the batch reports the copied tables it names, not the ones it dropped")
	assert.Equal(t, sharedCheckpointTable, found.CheckpointTable,
		"both batches have two or more statements, so both read the schema-level checkpoint")
	require.NotEmpty(t, found.Statement,
		"the checkpoint is reachable, so the discard rests on the statement comparison alone")

	disposition, reason := found.Disposition(xfersAlter+"; "+ledgerAlter, DefaultCheckpointMaxAge)
	assert.Equal(t, engine.CopyDiscard, disposition)
	assert.Equal(t, engine.DiscardStatementDiffers, reason)

	// The ALTER for each surviving table is unchanged; only the batch around
	// them shrank.
	assert.Contains(t, found.Statement, xfersAlter)
	assert.Contains(t, found.Statement, ledgerAlter)
}

// A checkpoint older than the configured maximum cannot be replayed, so Spirit
// starts fresh and the copy is lost. The operator is warned that applying
// re-copies from zero.
func TestExistingCopyDiscardsExpiredCheckpoint(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	const batch = "ALTER TABLE `xfers` ADD INDEX r_token (r_token)"
	seedCopy(t, db, []string{"xfers"}, utils.CheckpointTableName("xfers"), batch)

	found, err := findCopy(t, dsn, []string{"xfers"})
	require.NoError(t, err, "findExistingCopy")
	require.NotNil(t, found)
	assert.Equal(t, batch, found.Statement, "the statement still matches; only the age disqualifies it")

	// The checkpoint was written moments ago, so bound the age below its own
	// age rather than waiting for a real expiry.
	disposition, reason := found.Disposition(batch, -time.Second)
	assert.Equal(t, engine.CopyDiscard, disposition)
	assert.Equal(t, engine.DiscardCheckpointExpired, reason)
}

// A shadow table with no checkpoint at all is an orphan from an apply that
// failed before its first dump. It is still a copy this batch destroys, so it
// is disclosed rather than reported as an empty target.
func TestExistingCopyWithoutCheckpointDiscards(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	seedCopy(t, db, []string{"xfers"}, "", "")

	found, err := findCopy(t, dsn, []string{"xfers"})
	require.NoError(t, err, "findExistingCopy")
	require.NotNil(t, found, "a shadow table with no checkpoint is still a copy")
	assert.Empty(t, found.Statement)
	assert.Zero(t, found.Age)

	disposition, reason := found.Disposition("ALTER TABLE `xfers` ADD INDEX r_token (r_token)", DefaultCheckpointMaxAge)
	assert.Equal(t, engine.CopyDiscard, disposition)
	assert.Equal(t, engine.DiscardStatementDiffers, reason)
}
