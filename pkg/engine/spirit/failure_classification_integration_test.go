//go:build integration

package spirit

import (
	"database/sql"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// runToTerminalProgress runs ddl through the engine's own execution path and
// returns the progress an operator's poller would read once it settles.
func runToTerminalProgress(t *testing.T, dsn, table, ddl string) *engine.ProgressResult {
	t.Helper()

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	eng := New(Config{Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))})
	defer eng.Drain()

	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{
		database: database,
		tables:   []string{table},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	eng.executeSchemaChange(t.Context(), host, username, password, database, []string{ddl}, false, directPolicy{})

	result, err := eng.Progress(t.Context(), &engine.ProgressRequest{})
	require.NoError(t, err, "Progress()")
	return result
}

// A unique index over data that already contains duplicates is rejected by the
// target on every attempt. The apply fails permanently rather than entering
// operator recovery: the automatic retries can only reproduce the rejection,
// and while they run the apply holds the database's active-apply slot and the
// operator reads an apply that still looks like it might recover.
func TestDuplicateDataFailsTheApplyWithoutRetrying(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	_, err := db.ExecContext(t.Context(), "CREATE TABLE `duplicate_unique_index` ("+
		"id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, "+
		"email VARCHAR(100) NOT NULL"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err, "create table")

	_, err = db.ExecContext(t.Context(),
		"INSERT INTO `duplicate_unique_index` (email) VALUES ('a@example.com'), ('a@example.com')")
	require.NoError(t, err, "seed duplicate rows")

	result := runToTerminalProgress(t, dsn, "duplicate_unique_index",
		"ALTER TABLE `duplicate_unique_index` ADD UNIQUE INDEX `idx_email` (`email`)")

	assert.Equal(t, engine.StateFailed, result.State)
	assert.False(t, result.Retryable,
		"the duplicate rows are still there on the next attempt, so retrying can only reproduce the rejection")
	assert.NotEmpty(t, result.ErrorMessage,
		"a permanent failure must still say what the target objected to")
	assert.Zero(t, indexCount(t, db, "duplicate_unique_index", "idx_email"),
		"the index the operator asked for was not added")
}

// indexCount reports how many parts of the named index exist on the table, and
// zero when the index was not created.
func indexCount(t *testing.T, db *sql.DB, tableName, indexName string) int {
	t.Helper()

	var count int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.STATISTICS "+
			"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?",
		tableName, indexName,
	).Scan(&count), "read index %s on %s", indexName, tableName)
	return count
}
