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

	started := startCopyForcingAlter(t, tableName)
	defer started.eng.Drain()

	rowsCopied := waitForCopyProgress(t, started.eng, 2000)
	t.Logf("halting after %d rows copied", rowsCopied)

	haltCtx, cancelHalt := context.WithTimeout(t.Context(), haltDeadline)
	defer cancelHalt()
	require.NoError(t, started.eng.HaltForShutdown(haltCtx), "HaltForShutdown()")

	shadowTable := utils.NewTableName(tableName)
	require.True(t, tableExists(t, started.db, shadowTable),
		"the halted copy leaves its shadow table on the target")
	require.Equal(t, 1, checkpointRowCount(t, started.db, utils.CheckpointTableName(tableName)),
		"the halted copy leaves a checkpoint naming the batch it was made for")

	return haltedCopy{
		dsn:           started.dsn,
		db:            started.db,
		table:         tableName,
		alter:         started.alter,
		shadowTableID: innodbTableID(t, started.db, shadowTable),
	}
}

// trackingEngine builds an engine already tracking a running schema change for
// c's table, which is what the execution paths expect to find when a test drives
// them directly rather than through Apply. The caller drains it.
func trackingEngine(t *testing.T, c haltedCopy, logger *slog.Logger) *Engine {
	t.Helper()

	_, _, _, database, err := parseDSN(c.dsn)
	require.NoError(t, err, "parseDSN")

	eng := New(Config{Logger: logger})
	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{
		database: database,
		tables:   []string{c.table},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()
	return eng
}

// runAlterToCompletion runs alter through the initial-apply path — routing, the
// existing-copy report, then Spirit — on eng, and waits for it to finish.
func runAlterToCompletion(t *testing.T, eng *Engine, c haltedCopy, alter string) {
	t.Helper()

	runToCompletion(t, eng, c, func(ctx context.Context, host, username, password, database string) {
		eng.executeSchemaChange(ctx, host, username, password, database, []string{alter}, false, directPolicy{})
	})
}

// resumeAlterToCompletion runs alter through the operator start path, which
// hands Spirit the stored statement directly instead of re-routing the plan,
// and waits for it to finish. This is the path a stopped copy comes back on.
func resumeAlterToCompletion(t *testing.T, eng *Engine, c haltedCopy, alter string) {
	t.Helper()

	runToCompletion(t, eng, c, func(ctx context.Context, host, username, password, database string) {
		eng.resumeSchemaChange(ctx, host, username, password, database, []string{alter}, alter, false, directPolicy{})
	})
}

func runToCompletion(t *testing.T, eng *Engine, c haltedCopy, run func(ctx context.Context, host, username, password, database string)) {
	t.Helper()

	host, username, password, database, err := parseDSN(c.dsn)
	require.NoError(t, err, "parseDSN")

	ctx, cancel := context.WithTimeout(t.Context(), resumeDeadline)
	defer cancel()
	run(ctx, host, username, password, database)

	eng.mu.Lock()
	finalState := eng.runningSchemaChange.state
	eng.mu.Unlock()
	require.Equal(t, engine.StateCompleted, finalState, "the schema change did not finish within %s", resumeDeadline)
}

// stdoutLogger is the logger the resume tests use when the assertion is about
// what happened on the target rather than what was logged.
func stdoutLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
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

	eng := trackingEngine(t, halted, stdoutLogger())
	defer eng.Drain()
	runAlterToCompletion(t, eng, halted, halted.alter)

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

	eng := trackingEngine(t, halted, stdoutLogger())
	defer eng.Drain()
	runAlterToCompletion(t, eng, halted, widerAlter)

	assert.Equal(t, predicted, observedDisposition(t, halted),
		"the copy the operator was warned about was destroyed and the table re-copied from zero")
	assert.Equal(t, "varchar(200)", columnType(t, halted.db, halted.table, "name"),
		"the fresh copy cut the new definition over")
}

// An operator who stops a copy and starts it again later meets the discard that
// matters most: the start is what destroys the copy, and the operator asked for
// it. The resumed apply hands Spirit its stored statement without going back
// through routing, so it must disclose the discard on that path too — otherwise
// the one apply an operator most needs warned about is the one that says
// nothing.
func TestResumedApplyDisclosesADiscardedCopy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long-running integration test in short mode")
	}

	halted := haltCopyMidFlight(t, "resume_disclosure_test")

	// A different width for the same column: still a full copy, and still a
	// statement the halted copy's checkpoint does not match, so the resume
	// rebuilds rather than continues.
	widerAlter := fmt.Sprintf("ALTER TABLE `%s` MODIFY COLUMN `name` varchar(200) NOT NULL", halted.table)

	logger, sink := capturingLogger()
	eng := trackingEngine(t, halted, logger)
	defer eng.Drain()
	resumeAlterToCompletion(t, eng, halted, widerAlter)

	line := onlyCopyReportLine(t, sink)
	assert.Equal(t, slog.LevelWarn, line.level,
		"the start an operator issued destroyed a copy, so the resume warns about it")
	assert.Equal(t, engine.DiscardStatementDiffers, line.attrs["reason"])
	assert.Equal(t, "["+halted.table+"]", line.attrs["copied_tables"],
		"the table whose copy was destroyed is named")

	assert.Equal(t, engine.CopyDiscard, observedDisposition(t, halted),
		"the copy the resume warned about was destroyed and the table re-copied from zero")
}
