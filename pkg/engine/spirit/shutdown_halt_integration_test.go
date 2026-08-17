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

	"github.com/block/spirit/pkg/status"
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

	const tableName = "halt_checkpoint_test"
	eng, db := startCopyForcingAlter(t, tableName)
	defer eng.Drain()
	// Drain waits on the copy goroutine with nothing cancelling it, so a test
	// that fails before it halts would wait out the whole copy. Halting first
	// bounds that unwind.
	defer haltForCleanup(t, eng)

	// Enough rows that the copy is unambiguously underway, but far short of the
	// seeded total so the halt still lands mid-copy.
	rowsCopied := waitForCopyProgress(t, eng, 2000)
	t.Logf("halting after %d rows copied", rowsCopied)

	// Rows copied proves chunks are landing, not that a checkpoint can be
	// written: the copier feeds chunks back as their writes finish, out of
	// order, and a checkpoint needs the contiguous low watermark that only
	// exists once every chunk behind it has fed back. Wait for that watermark
	// so the halt's dump asserts checkpointing behavior, not watermark timing.
	waitForCheckpointReadiness(t, eng)

	// The readiness wait proved the watermark by dumping a checkpoint of its
	// own. Clear it: every dump appends a row, so the probe's row would
	// satisfy the count below even if the halt never dumped. The watermark
	// only advances while the copy runs, so clearing the row does not undo
	// readiness.
	checkpointTable := utils.CheckpointTableName(tableName)
	_, err := db.ExecContext(t.Context(), fmt.Sprintf("DELETE FROM `%s`", checkpointTable))
	require.NoError(t, err, "clear the readiness probe's checkpoint from %s", checkpointTable)

	// Bound the halt: it waits for the copy goroutine to exit, so one that will
	// not come down fails here rather than hanging until the package timeout.
	haltCtx, cancelHalt := context.WithTimeout(t.Context(), haltDeadline)
	defer cancelHalt()
	require.NoError(t, eng.HaltForShutdown(haltCtx), "HaltForShutdown()")

	// The probe's row was cleared, so the halt's own dump is the only write
	// that can account for the row that exists now.
	require.Equal(t, 1, checkpointRowCount(t, db, checkpointTable),
		"halting checkpoints the copy before cancelling it, so the reclaiming driver resumes from where the copy stopped rather than recopying from the last periodic checkpoint")

	// The checkpoint has to carry resume state, not just exist: a copier
	// watermark says which chunks are already copied, and a binlog position
	// says where the reclaiming driver picks up the changes made since.
	copierWatermark, binlogPosition := latestCheckpoint(t, db, checkpointTable)
	assert.NotEmpty(t, copierWatermark, "the checkpoint records how far the copy got")
	assert.NotEmpty(t, binlogPosition, "the checkpoint records where to resume reading changes")
}

// The checkpoint a halt takes is best effort: losing it costs the reclaiming
// driver the rows copied since Spirit's last periodic checkpoint, which is
// recoverable. Leaving the copy running is not — Spirit holds an advisory lock
// on the table for as long as the copy goroutine lives, so a halt that gave up
// on the checkpoint failure would keep the target locked while nothing renews
// the apply's lease, and every driver that reclaims the apply is refused the
// lock. A checkpoint that cannot be written must not stop the copy coming down.
func TestHaltForShutdownStopsACopyItCannotCheckpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long-running integration test in short mode")
	}

	const tableName = "halt_checkpoint_failure_test"
	eng, db := startCopyForcingAlter(t, tableName)
	defer eng.Drain()
	defer haltForCleanup(t, eng)

	waitForCopyProgress(t, eng, 2000)

	// Drop the table the checkpoint is written to, so the halt's dump fails
	// against a copy that is still running.
	checkpointTable := utils.CheckpointTableName(tableName)
	_, err := db.ExecContext(t.Context(), fmt.Sprintf("DROP TABLE `%s`", checkpointTable))
	require.NoError(t, err, "drop checkpoint table %s", checkpointTable)

	haltCtx, cancelHalt := context.WithTimeout(t.Context(), haltDeadline)
	defer cancelHalt()
	require.NoError(t, eng.HaltForShutdown(haltCtx),
		"a checkpoint that cannot be written is not a reason to report the halt as failed")

	// The copy is cancelled, not finished: it never reaches the cutover that
	// would put the widened column on the table.
	requireCopyGoroutineExited(t, eng)
	assert.Equal(t, "varchar(50)", columnType(t, db, tableName, "name"),
		"the halt cancelled the copy, so it never cut over to the widened column")
}

// startCopyForcingAlter starts an ALTER that copies the table rather than
// changing it in place, so there is copy progress for the caller to act on
// mid-flight. It returns once the engine has accepted the change; the caller
// waits for the copy to become observable.
func startCopyForcingAlter(t *testing.T, tableName string) (*Engine, *sql.DB) {
	t.Helper()

	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	_, err := db.ExecContext(t.Context(), fmt.Sprintf("CREATE TABLE `%s` ("+
		"id INT PRIMARY KEY AUTO_INCREMENT, "+
		"name VARCHAR(50) NOT NULL)", tableName))
	require.NoError(t, err, "create table %s", tableName)
	seedTableRows(t, db, tableName)

	eng := New(Config{
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
		// Small chunks and a single thread keep the copy observable for long
		// enough to act on it partway through.
		TargetChunkTime: 100 * time.Millisecond,
		Threads:         1,
	})

	applyResult, err := eng.Apply(t.Context(), &engine.ApplyRequest{
		Database: "testdb",
		Changes: []engine.SchemaChange{{
			Namespace: "testdb",
			TableChanges: []engine.TableChange{{
				Table: tableName,
				// Widening the column forces a full table copy rather than an
				// in-place change.
				DDL: fmt.Sprintf("ALTER TABLE `%s` MODIFY COLUMN `name` varchar(100) NOT NULL", tableName),
			}},
		}},
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err, "Apply()")
	require.True(t, applyResult.Accepted, "apply not accepted: %s", applyResult.Message)

	return eng, db
}

// waitForCheckpointReadiness blocks until the schema change can write a
// checkpoint, proven by dumping one. Copy progress alone does not imply this:
// the checkpoint records the copy's contiguous low watermark, which lags the
// rows-copied count until every chunk behind it has fed back.
func waitForCheckpointReadiness(t *testing.T, eng *Engine) {
	t.Helper()

	eng.mu.Lock()
	rm := eng.runningSchemaChange
	eng.mu.Unlock()
	require.NotNil(t, rm, "no schema change is running")
	require.NotEmpty(t, rm.runners, "the schema change has no runners")

	deadline := time.Now().Add(copyProgressPollDeadline)
	for time.Now().Before(deadline) {
		err := rm.runners[0].DumpCheckpoint(t.Context())
		if err == nil {
			return
		}
		require.ErrorIs(t, err, status.ErrWatermarkNotReady,
			"the checkpoint dump failed for a reason other than the watermark not being ready yet")
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("the copy's low watermark was not checkpoint-ready within %s", copyProgressPollDeadline)
}

// requireCopyGoroutineExited waits for the copy goroutine to exit, bounded so a
// goroutine the halt left running fails the test instead of hanging the package
// until its timeout.
func requireCopyGoroutineExited(t *testing.T, eng *Engine) {
	t.Helper()

	drained := make(chan struct{})
	go func() {
		eng.Drain()
		close(drained)
	}()

	select {
	case <-drained:
	case <-time.After(haltDeadline):
		t.Fatal("the copy goroutine is still running after the halt, so the target stays locked while nothing renews the apply's lease")
	}
}

// columnType returns a column's declared type, so a test can tell a table that
// was cut over to a new definition from one that was left alone.
func columnType(t *testing.T, db *sql.DB, tableName, columnName string) string {
	t.Helper()

	var columnType string
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS "+
			"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?",
		tableName, columnName,
	).Scan(&columnType), "read type of %s.%s", tableName, columnName)
	return columnType
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
