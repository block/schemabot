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
	"github.com/block/schemabot/pkg/pendingdrops"
)

// cleanupPendingDropsDB removes the quarantine database so each test observes
// only its own quarantined tables.
func cleanupPendingDropsDB(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(t.Context(), fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", pendingdrops.Database))
	require.NoError(t, err, "drop pending drops database")
}

// listQuarantinedTables returns the table names currently in the quarantine database.
func listQuarantinedTables(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(),
		"SELECT table_name FROM information_schema.tables WHERE table_schema = ? ORDER BY table_name",
		pendingdrops.Database)
	require.NoError(t, err, "query quarantined tables")
	defer utils.CloseAndLog(rows)

	var tables []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name), "scan table name")
		tables = append(tables, name)
	}
	require.NoError(t, rows.Err(), "iterate quarantined tables")
	return tables
}

// runDDLApply runs DDL statements through the engine and returns the final
// engine state.
func runDDLApply(t *testing.T, eng *Engine, dsn string, ddlStatements []string) engine.State {
	t.Helper()

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	eng.mu.Lock()
	eng.runningMigration = &runningMigration{
		database: database,
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	eng.executeMigration(t.Context(), host, username, password, database, ddlStatements, false)

	eng.mu.Lock()
	defer eng.mu.Unlock()
	return eng.runningMigration.state
}

// A dropped table is quarantined in the pending drops database with its data
// intact instead of being dropped, so an accidental drop is recoverable until
// the retention period expires.
func TestEngine_ExecuteSchemaChange_DropTableQuarantines(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	_, err := db.ExecContext(t.Context(), `CREATE TABLE drop_me (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(100) NOT NULL
	)`)
	require.NoError(t, err, "create table")
	for i := range 10 {
		_, err := db.ExecContext(t.Context(), `INSERT INTO drop_me (name) VALUES (?)`, fmt.Sprintf("row-%d", i))
		require.NoError(t, err, "insert data")
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	state := runDDLApply(t, eng, dsn, []string{"DROP TABLE `drop_me`"})
	assert.Equal(t, engine.StateCompleted, state)

	// The table is gone from the target database.
	var count int
	err = db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'testdb' AND table_name = 'drop_me'").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "drop_me should be gone from testdb")

	// The table is quarantined with a parseable timestamp prefix and its data intact.
	quarantined := listQuarantinedTables(t, db)
	require.Len(t, quarantined, 1)
	assert.Contains(t, quarantined[0], "_drop_me")
	quarantinedAt, ok := pendingdrops.ParseTimestamp(quarantined[0])
	require.True(t, ok, "quarantine table name %q must carry a parseable timestamp", quarantined[0])
	assert.WithinDuration(t, time.Now(), quarantinedAt, time.Minute)

	var rowCount int
	err = db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", pendingdrops.Database, quarantined[0])).Scan(&rowCount)
	require.NoError(t, err)
	assert.Equal(t, 10, rowCount, "quarantined table keeps its data")
}

// When pending drops is disabled, DROP TABLE executes directly and no
// quarantine database is created.
func TestEngine_ExecuteSchemaChange_DropTableDisabled(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	_, err := db.ExecContext(t.Context(), `CREATE TABLE drop_me_direct (
		id INT PRIMARY KEY AUTO_INCREMENT
	)`)
	require.NoError(t, err, "create table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger, DisablePendingDrops: true})

	state := runDDLApply(t, eng, dsn, []string{"DROP TABLE `drop_me_direct`"})
	assert.Equal(t, engine.StateCompleted, state)

	var count int
	err = db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'testdb' AND table_name = 'drop_me_direct'").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "drop_me_direct should be dropped")

	var dbCount int
	err = db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?",
		pendingdrops.Database).Scan(&dbCount)
	require.NoError(t, err)
	assert.Equal(t, 0, dbCount, "no quarantine database should be created when pending drops is disabled")
}

// DROP TABLE IF EXISTS on a missing table completes without creating a
// quarantine entry, matching MySQL's IF EXISTS semantics.
func TestEngine_ExecuteSchemaChange_DropTableIfExistsMissing(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	state := runDDLApply(t, eng, dsn, []string{"DROP TABLE IF EXISTS `never_existed`"})
	assert.Equal(t, engine.StateCompleted, state)

	quarantined := listQuarantinedTables(t, db)
	assert.Empty(t, quarantined, "missing table must not create a quarantine entry")
}

// Multi-table DROP TABLE is quarantined atomically: if any required table is
// missing, existing tables remain in the target database and no partial
// quarantine is recorded.
func TestEngine_ExecuteSchemaChange_DropTableMultiTableFailureIsAtomic(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(t.Context())
		_, err := db.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS `keep_on_failure`")
		require.NoError(t, err, "drop keep_on_failure")
		_, err = db.ExecContext(cleanupCtx, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", pendingdrops.Database))
		require.NoError(t, err, "drop pending drops database")
	})

	_, err := db.ExecContext(t.Context(), `CREATE TABLE keep_on_failure (
		id INT PRIMARY KEY AUTO_INCREMENT
	)`)
	require.NoError(t, err, "create table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	state := runDDLApply(t, eng, dsn, []string{"DROP TABLE `keep_on_failure`, `missing_table`"})
	assert.Equal(t, engine.StateFailed, state)

	var count int
	err = db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'testdb' AND table_name = 'keep_on_failure'").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "existing table should remain when the multi-table quarantine fails")

	quarantined := listQuarantinedTables(t, db)
	assert.Empty(t, quarantined, "failed multi-table quarantine must not move any table")
}

// DROP VIEW parses as a TiDB DropTableStmt, but views are not recoverable table
// data and cannot be renamed into the pending drops database. The view is
// dropped directly and no quarantine entry is created.
func TestEngine_ExecuteSchemaChange_DropViewExecutesDirectly(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(t.Context())
		_, err := db.ExecContext(cleanupCtx, "DROP VIEW IF EXISTS `drop_view_target`")
		require.NoError(t, err, "drop view")
		_, err = db.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS `drop_view_base`")
		require.NoError(t, err, "drop base table")
	})

	_, err := db.ExecContext(t.Context(), `CREATE TABLE drop_view_base (
		id INT PRIMARY KEY AUTO_INCREMENT
	)`)
	require.NoError(t, err, "create base table")
	_, err = db.ExecContext(t.Context(), "CREATE VIEW `drop_view_target` AS SELECT id FROM `drop_view_base`")
	require.NoError(t, err, "create view")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	state := runDDLApply(t, eng, dsn, []string{"DROP VIEW `drop_view_target`"})
	assert.Equal(t, engine.StateCompleted, state)

	var count int
	err = db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.views WHERE table_schema = 'testdb' AND table_name = 'drop_view_target'").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "view should be dropped")

	quarantined := listQuarantinedTables(t, db)
	assert.Empty(t, quarantined, "views must not create quarantine entries")
}

// DROP TEMPORARY TABLE also parses as a TiDB DropTableStmt, but temporary
// tables are connection-local and cannot be moved with RENAME TABLE. The
// statement executes directly and no quarantine entry is created.
func TestEngine_ExecuteSchemaChange_DropTemporaryTableExecutesDirectly(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	state := runDDLApply(t, eng, dsn, []string{"DROP TEMPORARY TABLE IF EXISTS `drop_temp_target`"})
	assert.Equal(t, engine.StateCompleted, state)

	quarantined := listQuarantinedTables(t, db)
	assert.Empty(t, quarantined, "temporary tables must not create quarantine entries")
}
