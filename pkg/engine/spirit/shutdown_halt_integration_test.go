//go:build integration

package spirit

import (
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

// haltCheckpointPollDeadline bounds the wait for the copy to get underway. The
// halt has to land while rows are still being copied, so a copy that never
// starts is a failed test rather than one that waits indefinitely.
const haltCheckpointPollDeadline = 30 * time.Second

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
	require.True(t, applyResult.Accepted, "apply not accepted: %s", applyResult.Message)

	rowsCopied := waitForCopyProgress(t, eng)
	t.Logf("halting after %d rows copied", rowsCopied)

	checkpointTable := utils.CheckpointTableName(tableName)
	require.Zero(t, checkpointRowCount(t, db, checkpointTable),
		"the copy has not run long enough for Spirit's periodic checkpoint, so any checkpoint after the halt is the halt's own")

	require.NoError(t, eng.HaltForShutdown(ctx), "HaltForShutdown()")

	assert.Positive(t, checkpointRowCount(t, db, checkpointTable),
		"halting checkpoints the copy before cancelling it, so the reclaiming driver resumes from where the copy stopped rather than recopying from the last periodic checkpoint")
}

// waitForCopyProgress blocks until the schema change reports copied rows,
// returning how many had been copied. It fails rather than returning zero: a
// halt before the copy starts would checkpoint nothing and pass the assertions
// it is meant to exercise.
func waitForCopyProgress(t *testing.T, eng *Engine) int64 {
	t.Helper()

	// Enough rows that the copy is unambiguously underway, but far short of the
	// seeded total so the halt still lands mid-copy.
	const wantRowsCopied = 2000

	deadline := time.Now().Add(haltCheckpointPollDeadline)
	for time.Now().Before(deadline) {
		progress, err := eng.Progress(t.Context(), &engine.ProgressRequest{})
		require.NoError(t, err, "Progress()")
		require.NotEqual(t, engine.StateCompleted, progress.State,
			"the copy finished before it could be halted; seed more rows so it stays in flight")

		if len(progress.Tables) > 0 && progress.Tables[0].RowsCopied >= wantRowsCopied {
			return progress.Tables[0].RowsCopied
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("copy did not reach %d rows within %s", wantRowsCopied, haltCheckpointPollDeadline)
	return 0
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
