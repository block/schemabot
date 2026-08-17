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

// resumeDeadline bounds the second schema change, which copies the table either
// from the halted copy's watermark or from zero. Either way it must finish
// within the bound rather than hanging the package until its timeout.
const resumeDeadline = 30 * time.Second

// haltedCopy is a real Spirit row copy left on a target by a driver that came
// down mid-copy: the shadow table it was filling, the checkpoint recording the
// batch it was made for, and the identity of that shadow table so a later run
// can be told to have continued it rather than rebuilt it.
type haltedCopy struct {
	dsn           string
	db            *sql.DB
	table         string
	alter         string
	shadowTableID int64
}

// haltCopyMidFlight runs a schema change that forces a full table copy, waits
// until the copy is observably underway, and halts the driver. What it leaves
// on the target is what a pod restart leaves: a partly filled shadow table and
// a checkpoint naming the batch that filled it.
func haltCopyMidFlight(t *testing.T, tableName string) haltedCopy {
	t.Helper()

	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	_, err := db.ExecContext(t.Context(), fmt.Sprintf("CREATE TABLE `%s` ("+
		"id INT PRIMARY KEY AUTO_INCREMENT, "+
		"name VARCHAR(50) NOT NULL)", tableName))
	require.NoError(t, err, "create table %s", tableName)
	seedTableRows(t, db, tableName)

	// Widening the column past the length-prefix boundary forces a full copy
	// rather than an in-place change, so there is a copy to halt.
	alter := fmt.Sprintf("ALTER TABLE `%s` MODIFY COLUMN `name` varchar(100) NOT NULL", tableName)

	eng := New(Config{
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
		// A single thread keeps the copy in flight long enough to halt it
		// partway through.
		Threads: 1,
	})
	defer eng.Drain()

	applyResult, err := eng.Apply(t.Context(), &engine.ApplyRequest{
		Database: "testdb",
		Changes: []engine.SchemaChange{{
			Namespace:    "testdb",
			TableChanges: []engine.TableChange{{Table: tableName, DDL: alter}},
		}},
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err, "Apply()")
	require.True(t, applyResult.Accepted, "apply not accepted: %s", applyResult.Message)

	rowsCopied := waitForCopyProgress(t, eng, 2000)
	t.Logf("halting after %d rows copied", rowsCopied)

	haltCtx, cancelHalt := context.WithTimeout(t.Context(), haltDeadline)
	defer cancelHalt()
	require.NoError(t, eng.HaltForShutdown(haltCtx), "HaltForShutdown()")

	shadowTable := utils.NewTableName(tableName)
	require.True(t, tableExists(t, db, shadowTable),
		"the halted copy leaves its shadow table on the target")
	require.Equal(t, 1, checkpointRowCount(t, db, utils.CheckpointTableName(tableName)),
		"the halted copy leaves a checkpoint naming the batch it was made for")

	return haltedCopy{
		dsn:           dsn,
		db:            db,
		table:         tableName,
		alter:         alter,
		shadowTableID: innodbTableID(t, db, shadowTable),
	}
}

// runAlterToCompletion runs alter through the full engine path — routing, the
// existing-copy report, then Spirit — and waits for it to finish.
func runAlterToCompletion(t *testing.T, c haltedCopy, alter string) {
	t.Helper()

	host, username, password, database, err := parseDSN(c.dsn)
	require.NoError(t, err, "parseDSN")

	eng := New(Config{Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))})
	defer eng.Drain()

	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{
		database: database,
		tables:   []string{c.table},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	ctx, cancel := context.WithTimeout(t.Context(), resumeDeadline)
	defer cancel()
	eng.executeSchemaChange(ctx, host, username, password, database, []string{alter}, false, directPolicy{})

	eng.mu.Lock()
	finalState := eng.runningSchemaChange.state
	eng.mu.Unlock()
	require.Equal(t, engine.StateCompleted, finalState, "the schema change did not finish within %s", resumeDeadline)
}

// innodbTableID returns the InnoDB identity of a table in the test database.
// InnoDB assigns a fresh id to every table it creates, so the id distinguishes
// the physical table a copy built from a replacement built in its place, which
// a name or a row count cannot.
func innodbTableID(t *testing.T, db *sql.DB, tableName string) int64 {
	t.Helper()

	var tableID int64
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT TABLE_ID FROM information_schema.INNODB_TABLES WHERE NAME = ?",
		"testdb/"+tableName,
	).Scan(&tableID), "read InnoDB table id of %s", tableName)
	return tableID
}

// observedDisposition reports what Spirit did with the shadow table a halted
// copy left behind, read from the target rather than from Spirit's output. A
// resumed copy keeps filling the same physical table and cuts that table over,
// so the table now serving reads carries the shadow table's identity. A
// discarded copy drops the shadow table and builds a new one in its place,
// which is a different physical table.
func observedDisposition(t *testing.T, c haltedCopy) engine.CopyDisposition {
	t.Helper()

	if innodbTableID(t, c.db, c.table) == c.shadowTableID {
		return engine.CopyAdopt
	}
	return engine.CopyDiscard
}

// A schema change that meets its own halted copy continues it. Spirit compares
// the statement it was given against the checkpoint and resumes from the
// copier's watermark, so the rows already copied survive the restart and the
// operator is told the copy is being continued before it runs.
func TestHaltedCopyResumesForTheSameStatement(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long-running integration test in short mode")
	}

	halted := haltCopyMidFlight(t, "resume_same_statement_test")

	found, err := findCopy(t, halted.dsn, []string{halted.table})
	require.NoError(t, err, "findExistingCopy")
	require.NotNil(t, found, "the halted copy is on the target")
	assert.Equal(t, []string{halted.table}, found.CopiedTables)
	assert.Equal(t, halted.alter, found.Statement,
		"the checkpoint records the batch verbatim, which is what Spirit compares against")

	predicted, reason := found.Disposition(halted.alter, DefaultCheckpointMaxAge)
	require.Equal(t, engine.CopyAdopt, predicted)
	require.Empty(t, reason)

	runAlterToCompletion(t, halted, halted.alter)

	assert.Equal(t, predicted, observedDisposition(t, halted),
		"the copy the operator was told would be continued is the one that was cut over")
	assert.Equal(t, "varchar(100)", columnType(t, halted.db, halted.table, "name"),
		"the resumed copy cut the widened column over")
}

// A schema change whose statement differs from the halted copy's throws that
// copy away and re-copies the table from zero. Spirit will not resume a
// checkpoint it did not write for this exact batch, so the operator is warned
// before the copy already paid for is destroyed.
func TestHaltedCopyIsDiscardedForADifferentStatement(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long-running integration test in short mode")
	}

	halted := haltCopyMidFlight(t, "resume_other_statement_test")

	// A different width for the same column: still a full copy, and still a
	// statement the halted copy's checkpoint does not match.
	widerAlter := fmt.Sprintf("ALTER TABLE `%s` MODIFY COLUMN `name` varchar(200) NOT NULL", halted.table)

	found, err := findCopy(t, halted.dsn, []string{halted.table})
	require.NoError(t, err, "findExistingCopy")
	require.NotNil(t, found, "the halted copy is on the target")

	predicted, reason := found.Disposition(widerAlter, DefaultCheckpointMaxAge)
	require.Equal(t, engine.CopyDiscard, predicted)
	require.Equal(t, engine.DiscardStatementDiffers, reason)

	runAlterToCompletion(t, halted, widerAlter)

	assert.Equal(t, predicted, observedDisposition(t, halted),
		"the copy the operator was warned about was destroyed and the table re-copied from zero")
	assert.Equal(t, "varchar(200)", columnType(t, halted.db, halted.table, "name"),
		"the fresh copy cut the new definition over")
}
