//go:build integration

package spirit

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/mysqlerr"
)

// applyTableChange starts a schema change for one table's DDL through the
// engine's Apply entrypoint, so tests exercise the same accept path a drive
// uses. It returns once the engine has accepted the change; the caller polls
// Progress for the outcome.
func applyTableChange(t *testing.T, eng *Engine, dsn, tableName, ddl string) {
	t.Helper()

	result, err := eng.Apply(t.Context(), &engine.ApplyRequest{
		Database: testDatabase,
		Changes: []engine.SchemaChange{{
			Namespace:    testDatabase,
			TableChanges: []engine.TableChange{{Table: tableName, DDL: ddl}},
		}},
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err, "Apply()")
	require.True(t, result.Accepted, "apply not accepted: %s", result.Message)
}

// waitForTerminalOutcome polls Progress until the schema change reports the
// wanted terminal state, and fails on any other terminal outcome so a change
// that fails while a completion is expected (or vice versa) surfaces its
// actual result instead of timing out.
func waitForTerminalOutcome(t *testing.T, eng *Engine, want engine.State) *engine.ProgressResult {
	t.Helper()

	deadline := time.Now().Add(copyProgressPollDeadline)
	for time.Now().Before(deadline) {
		progress, err := eng.Progress(t.Context(), &engine.ProgressRequest{})
		require.NoError(t, err, "Progress()")
		if progress.State == want {
			return progress
		}
		require.False(t, progress.State.IsTerminal(),
			"schema change reached %s while waiting for %s: %s", progress.State, want, progress.ErrorMessage)
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("schema change did not reach %s within %s", want, copyProgressPollDeadline)
	return nil
}

func newDrainOutcomeTestEngine() *Engine {
	return New(Config{
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
}

// A schema change can land on the target and be drained before the owner's
// next progress poll arrives. The poll must still observe the completed
// outcome — with the table and DDL that ran — because reporting pending would
// tell the caller the work never started even though the target now carries
// the change.
func TestEngine_Progress_DrainedCompletedChangeReportsCompleted(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	_, err := db.ExecContext(t.Context(), "CREATE TABLE `drain_completed` ("+
		"id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(50) NOT NULL)")
	require.NoError(t, err, "create table")

	eng := newDrainOutcomeTestEngine()
	applyTableChange(t, eng, dsn, "drain_completed",
		"ALTER TABLE `drain_completed` ADD COLUMN `email` varchar(255) NULL")
	defer eng.Drain()

	waitForTerminalOutcome(t, eng, engine.StateCompleted)

	eng.Drain()

	result, err := eng.Progress(t.Context(), &engine.ProgressRequest{})
	require.NoError(t, err, "Progress()")
	assert.Equal(t, engine.StateCompleted, result.State)
	assert.Empty(t, result.ErrorMessage)
	assert.False(t, result.Retryable)
	require.Len(t, result.Tables, 1)
	assert.Equal(t, "drain_completed", result.Tables[0].Table)
	assert.Equal(t, testDatabase, result.Tables[0].Namespace)
	assert.Contains(t, result.Tables[0].DDL, "ADD COLUMN")
	assert.Contains(t, result.Tables[0].DDL, "email")
	assert.Equal(t, 100, result.Tables[0].Progress)
}

// A failed schema change's error message is the operator's only handle on why
// the target was left unchanged. A drain that lands between the failure and
// the next progress poll must carry the failed state and its error message
// through to that poll.
func TestEngine_Progress_DrainedFailedChangeReportsFailure(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	_, err := db.ExecContext(t.Context(), "CREATE TABLE `drain_failed` (id INT PRIMARY KEY)")
	require.NoError(t, err, "create table")

	eng := newDrainOutcomeTestEngine()
	applyTableChange(t, eng, dsn, "drain_failed",
		"ALTER TABLE `drain_failed` DROP COLUMN `nonexistent_column`")
	defer eng.Drain()

	waitForTerminalOutcome(t, eng, engine.StateFailed)

	eng.Drain()

	result, err := eng.Progress(t.Context(), &engine.ProgressRequest{})
	require.NoError(t, err, "Progress()")
	assert.Equal(t, engine.StateFailed, result.State)
	assert.Equal(t, mysqlerr.ReasonFromText("(errno 1091)"), result.ErrorMessage)
	assert.NotContains(t, result.ErrorMessage, "nonexistent_column")
	assert.True(t, result.Retryable)
}

// A drained outcome answers progress polls only until the engine accepts new
// work. Once a fresh Apply is accepted, every poll reflects the new schema
// change: the previous change's failure must never resurface as the new
// change's state, error message, or table list.
func TestEngine_Apply_AfterDrainedFailureStartsFreshProgress(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	_, err := db.ExecContext(t.Context(), "CREATE TABLE `drain_first` (id INT PRIMARY KEY)")
	require.NoError(t, err, "create first table")
	_, err = db.ExecContext(t.Context(), "CREATE TABLE `drain_second` ("+
		"id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(50) NOT NULL)")
	require.NoError(t, err, "create second table")

	eng := newDrainOutcomeTestEngine()
	applyTableChange(t, eng, dsn, "drain_first",
		"ALTER TABLE `drain_first` DROP COLUMN `nonexistent_column`")
	defer eng.Drain()

	waitForTerminalOutcome(t, eng, engine.StateFailed)
	eng.Drain()

	failed, err := eng.Progress(t.Context(), &engine.ProgressRequest{})
	require.NoError(t, err, "Progress()")
	require.Equal(t, engine.StateFailed, failed.State)

	applyTableChange(t, eng, dsn, "drain_second",
		"ALTER TABLE `drain_second` ADD COLUMN `email` varchar(255) NULL")

	// waitForTerminalOutcome fails on any terminal state other than the wanted
	// one, so the second change must report its own completion end to end.
	final := waitForTerminalOutcome(t, eng, engine.StateCompleted)
	assert.Empty(t, final.ErrorMessage)
	require.Len(t, final.Tables, 1)
	assert.Equal(t, "drain_second", final.Tables[0].Table)
}
