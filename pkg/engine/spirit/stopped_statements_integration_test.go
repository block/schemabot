//go:build integration

package spirit

import (
	"database/sql"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// A stopped run left a statement whose end on the server could not be
// confirmed. While that statement's session is still in the processlist, the
// engine refuses to start another schema change on the database and reports
// the target as held, so the drive waits instead of failing the schema change.
// Once an operator kills the session, the next start runs the schema change and
// the engine stops reporting a statement that may still run.
func TestEngine_StartWaitsForAStoppedRunsStatementToLeaveTheServer(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	host, username, _, database, err := parseDSN(dsn)
	require.NoError(t, err, "parse DSN")

	// A dedicated session stands in for the stopped run's statement.
	session, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err, "open session pool")
	t.Cleanup(func() { utils.CloseAndLog(session) })
	session.SetMaxOpenConns(1)
	var connID int
	require.NoError(t, session.QueryRowContext(t.Context(), "SELECT CONNECTION_ID()").Scan(&connID), "read session id")

	eng := New(Config{Logger: slog.Default()})
	eng.unconfirmedStatements = []unconfirmedStatement{{host: host, username: username, database: database, connID: connID}}
	require.True(t, eng.CancelledStatementMayStillRun(), "the recorded statement may still run")

	createHeld := "CREATE TABLE held_orders (id BIGINT UNSIGNED NOT NULL PRIMARY KEY)"
	applyTableChange(t, eng, dsn, "held_orders", createHeld)
	refused := waitForTerminalOutcome(t, eng, engine.StateFailed)
	assert.True(t, refused.TargetHeld, "a statement still running holds the target")
	assert.Contains(t, refused.ErrorMessage, "is still running on the server", "the refusal names why the schema change did not start")
	assert.False(t, tableExists(t, db, "held_orders"), "the schema change did not start")
	assert.True(t, eng.CancelledStatementMayStillRun(), "the statement is still recorded while its session runs")

	_, err = db.ExecContext(t.Context(), fmt.Sprintf("KILL %d", connID))
	require.NoError(t, err, "kill the stopped run's session")
	waitForSessionGone(t, db, connID)

	applyTableChange(t, eng, dsn, "held_orders", createHeld)
	waitForTerminalOutcome(t, eng, engine.StateCompleted)
	assert.True(t, tableExists(t, db, "held_orders"), "the schema change ran once the session was gone")
	assert.False(t, eng.CancelledStatementMayStillRun(), "a confirmed end clears the record")
}

// A statement recorded under a different user than the one a later start
// connects as cannot be checked: a user does not see another user's sessions
// without the PROCESS privilege, so a missing session proves nothing. The
// start is refused as a held target even though no such session is running.
func TestEngine_StartWaitsWhenAStoppedRunsStatementWasIssuedByAnotherUser(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	host, _, _, database, err := parseDSN(dsn)
	require.NoError(t, err, "parse DSN")
	var connID int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT MAX(id) + 1000 FROM information_schema.processlist").Scan(&connID), "pick an unused session id")

	eng := New(Config{Logger: slog.Default()})
	eng.unconfirmedStatements = []unconfirmedStatement{{host: host, username: "some_other_user", database: database, connID: connID}}

	applyTableChange(t, eng, dsn, "other_user_orders", "CREATE TABLE other_user_orders (id BIGINT UNSIGNED NOT NULL PRIMARY KEY)")
	refused := waitForTerminalOutcome(t, eng, engine.StateFailed)
	assert.True(t, refused.TargetHeld, "a statement that cannot be checked holds the target")
	assert.False(t, tableExists(t, db, "other_user_orders"), "the schema change did not start")
	assert.True(t, eng.CancelledStatementMayStillRun(), "the statement stays recorded")
}

// waitForSessionGone polls until connID leaves the processlist.
func waitForSessionGone(t *testing.T, db *sql.DB, connID int) {
	t.Helper()
	deadline := time.Now().Add(copyProgressPollDeadline)
	for time.Now().Before(deadline) {
		var sessions int
		require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM information_schema.processlist WHERE id = ?", connID).Scan(&sessions))
		if sessions == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("session %d was still in the processlist after %s", connID, copyProgressPollDeadline)
}
