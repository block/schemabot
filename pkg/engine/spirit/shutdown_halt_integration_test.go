//go:build integration

package spirit

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// haltDeadline bounds the halt itself, which checkpoints the copy and then
// waits for its goroutine to exit.
const haltDeadline = 30 * time.Second

// A driver that shuts down mid-copy must leave the schema change resumable by
// whichever driver reclaims the apply. Spirit writes checkpoints on its own
// periodic cadence, so a halt that only cancelled the copy would strand every
// row copied since the last one and the reclaiming driver would recopy them.
// Halting takes its own checkpoint first, so the work already done survives the
// shutdown.
func TestHaltForShutdownCheckpointsAnInFlightCopy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long-running integration test in short mode")
	}

	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	const tableName = "halt_checkpoint_test"
	_, err := db.ExecContext(t.Context(), fmt.Sprintf("CREATE TABLE `%s` ("+
		"id INT PRIMARY KEY AUTO_INCREMENT, "+
		"name VARCHAR(50) NOT NULL)", tableName))
	require.NoError(t, err, "create table")
	seedTableRows(t, db, tableName)

	eng := New(Config{
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
		// Small chunks and a single thread keep the copy observable for long
		// enough to halt it partway through.
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         1,
	})

	ctx := t.Context()
	applyResult, err := eng.Apply(ctx, &engine.ApplyRequest{
		Database: "testdb",
		Changes: []engine.SchemaChange{{
			Namespace: "testdb",
			TableChanges: []engine.TableChange{{
				Table: tableName,
				// Widening the column forces a full table copy rather than an
				// in-place change, so there is copy progress to checkpoint.
				DDL: fmt.Sprintf("ALTER TABLE `%s` MODIFY COLUMN `name` varchar(100) NOT NULL", tableName),
			}},
		}},
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err, "Apply()")
	defer eng.Drain()
	// Drain waits on the copy goroutine with nothing cancelling it, so a test
	// that fails before it halts would wait out the whole copy. Halting first
	// bounds that unwind.
	defer haltForCleanup(t, eng)
	require.True(t, applyResult.Accepted, "apply not accepted: %s", applyResult.Message)

	// Enough rows that the copy is unambiguously underway, but far short of the
	// seeded total so the halt still lands mid-copy.
	rowsCopied := waitForCopyProgress(t, eng, 2000)
	t.Logf("halting after %d rows copied", rowsCopied)

	checkpointTable := utils.CheckpointTableName(tableName)
	require.Zero(t, checkpointRowCount(t, db, checkpointTable),
		"the copy has not run long enough for Spirit's periodic checkpoint, so any checkpoint after the halt is the halt's own")

	// Bound the halt: it waits for the copy goroutine to exit, so one that will
	// not come down fails here rather than hanging until the package timeout.
	haltCtx, cancelHalt := context.WithTimeout(ctx, haltDeadline)
	defer cancelHalt()
	require.NoError(t, eng.HaltForShutdown(haltCtx), "HaltForShutdown()")

	// Spirit keeps one checkpoint row per run, so the halt's own dump is the
	// only write that can account for it.
	require.Equal(t, 1, checkpointRowCount(t, db, checkpointTable),
		"halting checkpoints the copy before cancelling it, so the reclaiming driver resumes from where the copy stopped rather than recopying from the last periodic checkpoint")

	// The checkpoint has to carry resume state, not just exist: a copier
	// watermark says which chunks are already copied, and a binlog position
	// says where the reclaiming driver picks up the changes made since.
	copierWatermark, binlogPosition := latestCheckpoint(t, db, checkpointTable)
	assert.NotEmpty(t, copierWatermark, "the checkpoint records how far the copy got")
	assert.NotEmpty(t, binlogPosition, "the checkpoint records where to resume reading changes")
}

// haltForCleanup brings a still-running schema change down so a test that fails
// before halting unwinds promptly instead of waiting out the whole copy.
func haltForCleanup(t *testing.T, eng *Engine) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), haltDeadline)
	defer cancel()
	if err := eng.HaltForShutdown(ctx); err != nil {
		t.Logf("halting the schema change during cleanup: %v", err)
	}
}

// checkpointRowCount returns how many checkpoints Spirit has written for the
// table, treating a table that does not exist yet as none written.
func checkpointRowCount(t *testing.T, db *sql.DB, checkpointTable string) int {
	t.Helper()

	if !tableExists(t, db, checkpointTable) {
		return 0
	}

	var count int
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM `%s`", checkpointTable)).Scan(&count),
		"count checkpoints in %s", checkpointTable)
	return count
}

// latestCheckpoint returns the resume state Spirit recorded in its most recent
// checkpoint for the table: how far the copy got, and where a reclaiming driver
// picks up the changes made since.
func latestCheckpoint(t *testing.T, db *sql.DB, checkpointTable string) (copierWatermark, binlogPosition string) {
	t.Helper()

	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf(
		"SELECT copier_watermark, binlog_position FROM `%s` ORDER BY id DESC LIMIT 1", checkpointTable),
	).Scan(&copierWatermark, &binlogPosition), "read latest checkpoint from %s", checkpointTable)
	return copierWatermark, binlogPosition
}
