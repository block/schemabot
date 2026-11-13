//go:build integration

package spirit

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
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
	eng.runningSchemaChange = &runningSchemaChange{
		database: database,
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	eng.executeSchemaChange(t.Context(), host, username, password, database, ddlStatements, false, directPolicy{})

	eng.mu.Lock()
	defer eng.mu.Unlock()
	return eng.runningSchemaChange.state
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

// The DROP phase re-runs from its first statement whenever an apply resumes,
// so on a deployment that drops tables outright the phase must tolerate the
// tables an earlier attempt already dropped and still drop the ones that
// remain, rather than failing the apply on the first absent table.
func TestEngine_ExecuteSchemaChange_DirectDropSkipsAlreadyAbsentTables(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	for _, name := range []string{"resumed_first", "resumed_second"} {
		_, err := db.ExecContext(t.Context(),
			fmt.Sprintf("CREATE TABLE `%s` (id INT PRIMARY KEY AUTO_INCREMENT)", name))
		require.NoError(t, err, "create table %s", name)
	}

	// Stand in for the earlier attempt: the first table is already dropped
	// when the phase restarts from the top.
	_, err := db.ExecContext(t.Context(), "DROP TABLE `resumed_first`")
	require.NoError(t, err, "pre-drop resumed_first")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger, DisablePendingDrops: true})

	state := runDDLApply(t, eng, dsn, []string{
		"DROP TABLE `resumed_first`",
		"DROP TABLE `resumed_second`",
	})
	assert.Equal(t, engine.StateCompleted, state)

	var count int
	err = db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'testdb' AND table_name = 'resumed_second'").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "the statement after the absent table must still run")

	quarantined := listQuarantinedTables(t, db)
	assert.Empty(t, quarantined, "the direct path must not quarantine anything")
}

// A multi-table DROP on the direct path drops the tables that are still there
// and leaves the already-absent ones alone, so a stop partway through a
// multi-table statement still converges on the planned end state.
func TestEngine_ExecuteSchemaChange_DirectDropMultiTablePartiallyAbsent(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	_, err := db.ExecContext(t.Context(),
		"CREATE TABLE `partial_present` (id INT PRIMARY KEY AUTO_INCREMENT)")
	require.NoError(t, err, "create table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger, DisablePendingDrops: true})

	state := runDDLApply(t, eng, dsn, []string{"DROP TABLE `partial_absent`, `partial_present`"})
	assert.Equal(t, engine.StateCompleted, state)

	var count int
	err = db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'testdb' AND table_name = 'partial_present'").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "the table that was still present must be dropped")
}

// The direct path forwards drops that carry no base-table data or that already
// tolerate a missing target: views, temporary tables, and an explicit IF
// EXISTS. Each must execute as written rather than going through the existence
// check, which only understands base tables.
func TestEngine_ExecuteSchemaChange_DirectDropForwardsBypassedStatements(t *testing.T) {
	tests := []struct {
		name   string
		setup  string
		ddl    string
		absent string
	}{
		{
			name:   "view",
			setup:  "CREATE VIEW `direct_view` AS SELECT 1 AS one",
			ddl:    "DROP VIEW `direct_view`",
			absent: "direct_view",
		},
		{
			name:   "if exists on a present table",
			setup:  "CREATE TABLE `direct_if_exists` (id INT PRIMARY KEY AUTO_INCREMENT)",
			ddl:    "DROP TABLE IF EXISTS `direct_if_exists`",
			absent: "direct_if_exists",
		},
		{
			name:   "if exists on a missing table",
			ddl:    "DROP TABLE IF EXISTS `direct_never_existed`",
			absent: "direct_never_existed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn, db := setupTestMySQL(t)
			cleanupTables(t, db)
			cleanupPendingDropsDB(t, db)

			if tt.setup != "" {
				_, err := db.ExecContext(t.Context(), tt.setup)
				require.NoError(t, err, "setup")
			}

			logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
			eng := New(Config{Logger: logger, DisablePendingDrops: true})

			state := runDDLApply(t, eng, dsn, []string{tt.ddl})
			assert.Equal(t, engine.StateCompleted, state)

			var count int
			err := db.QueryRowContext(t.Context(),
				"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'testdb' AND table_name = ?",
				tt.absent).Scan(&count)
			require.NoError(t, err)
			assert.Equal(t, 0, count, "%s should not exist in testdb", tt.absent)

			quarantined := listQuarantinedTables(t, db)
			assert.Empty(t, quarantined, "the direct path must not quarantine anything")
		})
	}
}

// resumeStoppedDropPhase stands in for a schema change whose first attempt ran
// the attempted DROP statements and was then stopped, and resumes it through
// the engine's own Start, which replays the plan from its first statement. It
// returns the resumed run's final state and failure reason.
func resumeStoppedDropPhase(t *testing.T, eng *Engine, dsn string, attempted, plan []string) (engine.State, string) {
	t.Helper()

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	rm := &runningSchemaChange{
		database:     database,
		originalDDLs: plan,
		state:        engine.StateRunning,
		started:      time.Now(),
		host:         host,
		username:     username,
		password:     password,
	}
	eng.installRunningSchemaChange(rm)
	require.True(t, eng.executeDropStatements(t.Context(), host, username, password, database, attempted),
		"the first attempt must run its DROP statements")

	eng.mu.Lock()
	rm.state = engine.StateStopped
	eng.mu.Unlock()

	result, err := eng.Start(t.Context(), &engine.ControlRequest{Database: database})
	require.NoError(t, err, "resume the stopped schema change")
	require.True(t, result.Accepted, "resume must be accepted: %s", result.Message)

	done := make(chan struct{})
	go func() {
		rm.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(resumeDeadline):
		require.FailNow(t, "resumed schema change did not finish", "deadline %s", resumeDeadline)
	}

	eng.mu.Lock()
	defer eng.mu.Unlock()
	return rm.state, rm.errorMessage
}

// A schema change dropping two tables is stopped after the first drop moved
// its table into pending drops. Resuming replays the DROP phase from its first
// statement, so the first table is already gone when the replay reaches it.
// The replay recognizes that table as its own quarantine and moves on, the
// second table is quarantined, and the schema change completes with each
// table in pending drops exactly once.
func TestEngine_ResumeDropPhase_SkipsTableAlreadyQuarantined(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	for _, name := range []string{"resume_quarantined_first", "resume_quarantined_second"} {
		_, err := db.ExecContext(t.Context(),
			fmt.Sprintf("CREATE TABLE `%s` (id INT PRIMARY KEY AUTO_INCREMENT)", name))
		require.NoError(t, err, "create table %s", name)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	// The apply log is how an operator learns where a skipped table's data
	// went, so the skip must reach it with the quarantine table name.
	var applyLog struct {
		mu    sync.Mutex
		lines []string
	}
	eng.SetLogCallback(func(_ slog.Level, table, msg string) {
		applyLog.mu.Lock()
		defer applyLog.mu.Unlock()
		applyLog.lines = append(applyLog.lines, table+": "+msg)
	})

	plan := []string{
		"DROP TABLE `resume_quarantined_first`",
		"DROP TABLE `resume_quarantined_second`",
	}
	state, reason := resumeStoppedDropPhase(t, eng, dsn, plan[:1], plan)
	require.Equal(t, engine.StateCompleted, state, "resumed schema change failed: %s", reason)

	var count int
	err := db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'testdb' AND table_name LIKE 'resume_quarantined_%'").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "both tables must be gone from testdb")

	quarantined := listQuarantinedTables(t, db)
	require.Len(t, quarantined, 2, "each table is quarantined exactly once: %v", quarantined)
	assert.Contains(t, quarantined[0]+" "+quarantined[1], "_resume_quarantined_first")
	assert.Contains(t, quarantined[0]+" "+quarantined[1], "_resume_quarantined_second")

	var firstCopy string
	for _, name := range quarantined {
		if strings.HasSuffix(name, "_resume_quarantined_first") {
			firstCopy = name
		}
	}
	applyLog.mu.Lock()
	defer applyLog.mu.Unlock()
	assert.Contains(t, applyLog.lines,
		fmt.Sprintf("resume_quarantined_first: table already quarantined as `%s`.`%s` earlier in this schema change; nothing left to quarantine",
			pendingdrops.Database, firstCopy),
		"the skip must tell the apply log where the table was quarantined: %v", applyLog.lines)
}

// A stop lands while the quarantine RENAME is waiting on a metadata lock. The
// client abandons the statement with a context error, but the server still
// completes the RENAME once the lock is released, so the table is in pending
// drops although the first attempt never saw the RENAME succeed. The replayed
// DROP must recognize that copy as this attempt's own work rather than fail on
// a table it reports it never quarantined while the retention clock runs on
// the copy.
func TestEngine_ResumeDropPhase_StopDuringBlockedRename(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	_, err := db.ExecContext(t.Context(), "CREATE TABLE `stop_mid_rename` (id INT PRIMARY KEY AUTO_INCREMENT)")
	require.NoError(t, err, "create stop_mid_rename")

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parse DSN")

	eng := New(Config{Logger: slog.Default()})
	eng.installRunningSchemaChange(&runningSchemaChange{
		database: database,
		state:    engine.StateRunning,
		host:     host,
		username: username,
		password: password,
	})

	// A READ lock held on another connection makes the RENAME wait for its
	// exclusive metadata lock.
	locker, err := db.Conn(t.Context())
	require.NoError(t, err, "open locking connection")
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(t.Context())
		_, _ = locker.ExecContext(cleanupCtx, "UNLOCK TABLES")
		_ = locker.Close()
	})
	_, err = locker.ExecContext(t.Context(), "LOCK TABLES `stop_mid_rename` READ")
	require.NoError(t, err, "lock stop_mid_rename")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	firstAttempt := make(chan error, 1)
	go func() {
		firstAttempt <- eng.quarantineDroppedTables(ctx, host, username, password, database, "DROP TABLE `stop_mid_rename`")
	}()

	waitFor(t, "the RENAME to block on the metadata lock", func() bool {
		var waiting int
		require.NoError(t, db.QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM information_schema.processlist WHERE info LIKE 'RENAME TABLE%stop_mid_rename%'").Scan(&waiting))
		return waiting > 0
	})
	cancel()
	require.Error(t, <-firstAttempt, "the stopped attempt must report that its RENAME did not complete")

	_, err = locker.ExecContext(t.Context(), "UNLOCK TABLES")
	require.NoError(t, err, "release the metadata lock")
	waitFor(t, "the server-side RENAME to complete", func() bool {
		exists, err := tableExistsInSchema(t.Context(), db, database, "stop_mid_rename")
		require.NoError(t, err)
		return !exists
	})
	require.Len(t, listQuarantinedTables(t, db), 1, "the stopped RENAME still quarantined the table")

	err = eng.quarantineDroppedTables(t.Context(), host, username, password, database, "DROP TABLE `stop_mid_rename`")
	require.NoError(t, err, "the replayed DROP must skip the table its own stopped attempt quarantined")
	assert.Len(t, listQuarantinedTables(t, db), 1, "the replay must not quarantine the table a second time")
}

// A stop abandons a quarantine RENAME that is waiting on a metadata lock, and
// the engine replays the DROP while that RENAME is still queued on the server.
// The replay still sees the table, so it records a fresh destination and
// queues behind the abandoned RENAME. Once the lock is released the abandoned
// RENAME lands at the first destination and the replay's own RENAME finds no
// table. The replay must find the data at the first destination and report the
// table quarantined, rather than fail on a table that is safely in pending
// drops, and a further replay skips it the same way.
func TestEngine_ResumeDropPhase_ReplayRacingAnAbandonedRename(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	_, err := db.ExecContext(t.Context(), "CREATE TABLE `replay_races_rename` (id INT PRIMARY KEY AUTO_INCREMENT)")
	require.NoError(t, err, "create replay_races_rename")

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parse DSN")

	eng := New(Config{Logger: slog.Default()})
	eng.installRunningSchemaChange(&runningSchemaChange{
		database: database,
		state:    engine.StateRunning,
		host:     host,
		username: username,
		password: password,
	})

	locker, err := db.Conn(t.Context())
	require.NoError(t, err, "open locking connection")
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(t.Context())
		_, _ = locker.ExecContext(cleanupCtx, "UNLOCK TABLES")
		_ = locker.Close()
	})
	_, err = locker.ExecContext(t.Context(), "LOCK TABLES `replay_races_rename` READ")
	require.NoError(t, err, "lock replay_races_rename")

	const drop = "DROP TABLE `replay_races_rename`"
	// Only statements parked on a metadata lock count. A CREATE DATABASE that is
	// still executing would otherwise read as the first attempt's blocked RENAME,
	// letting the test stop that attempt before it ever sent the RENAME.
	blockedStatements := func(infoPatterns ...string) int {
		var blocked int
		for _, pattern := range infoPatterns {
			var count int
			require.NoError(t, db.QueryRowContext(t.Context(), `
				SELECT COUNT(*) FROM information_schema.processlist
				WHERE state LIKE 'Waiting for %metadata lock' AND info LIKE ?`, pattern).Scan(&count))
			blocked += count
		}
		return blocked
	}
	const blockedRename = "RENAME TABLE%replay_races_rename%"
	const blockedCreateDatabase = "CREATE DATABASE IF NOT EXISTS%"

	target := dropTarget{schema: database, table: "replay_races_rename"}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	firstAttempt := make(chan error, 1)
	go func() { firstAttempt <- eng.quarantineDroppedTables(ctx, host, username, password, database, drop) }()
	waitFor(t, "the first RENAME to block on the metadata lock", func() bool { return blockedStatements(blockedRename) >= 1 })
	cancel()
	require.Error(t, receiveWithin(t, firstAttempt, "the stopped attempt to return"),
		"the stopped attempt must report that its RENAME did not complete")
	firstRecorded := eng.quarantinedDrops(target)
	require.Len(t, firstRecorded, 1, "the stopped attempt recorded its destination before its RENAME")

	// Destination names carry a millisecond timestamp, so a replay that follows
	// quickly enough would record the same name as the stopped attempt and the
	// test could not tell a record that keeps both apart from one that keeps
	// only the latest. Hold the replay until it is sure to pick a new name.
	waitFor(t, "the clock to move past the first destination's millisecond", func() bool {
		next, err := pendingdrops.Destinations([]pendingdrops.TableMove{{SchemaName: database, TableName: target.table}}, time.Now())
		require.NoError(t, err, "compute the replay's destination")
		return next[0].QuarantineTable != firstRecorded[0].QuarantineTable
	})

	replay := make(chan error, 1)
	go func() { replay <- eng.quarantineDroppedTables(t.Context(), host, username, password, database, drop) }()
	waitFor(t, "the replay to queue behind the abandoned RENAME", func() bool {
		return blockedStatements(blockedRename, blockedCreateDatabase) >= 2
	})
	recorded := eng.quarantinedDrops(target)
	require.Len(t, recorded, 2, "the replay must add its destination to the record, not replace the first")
	assert.Equal(t, firstRecorded[0], recorded[0], "the stopped attempt's destination stays first")
	assert.NotEqual(t, recorded[0].QuarantineTable, recorded[1].QuarantineTable, "the replay recorded a destination of its own")

	_, err = locker.ExecContext(t.Context(), "UNLOCK TABLES")
	require.NoError(t, err, "release the metadata lock")
	require.NoError(t, receiveWithin(t, replay, "the replay to return"),
		"the replay's own RENAME finds no table, and the copy at the first destination accounts for it")
	require.Equal(t, []string{firstRecorded[0].QuarantineTable}, listQuarantinedTables(t, db),
		"the abandoned RENAME quarantined the table at the first destination")

	err = eng.quarantineDroppedTables(t.Context(), host, username, password, database, drop)
	require.NoError(t, err, "the next replay must find the copy the abandoned RENAME wrote")
	assert.Equal(t, []string{firstRecorded[0].QuarantineTable}, listQuarantinedTables(t, db),
		"the table is quarantined exactly once")
}

// receiveWithin returns the next value on results, failing the test if none
// arrives within resumeDeadline.
func receiveWithin(t *testing.T, results <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-results:
		return err
	case <-time.After(resumeDeadline):
		require.FailNow(t, "timed out waiting for "+what, "deadline %s", resumeDeadline)
		return nil
	}
}

// A schema change quarantines `durable_orders` and its server restarts, so the
// next attempt runs on an engine with no memory of the first. The target's
// quarantine log still names the copy the apply moved the table to, so the
// replayed DROP recognizes the copy as its own and skips the table. An attempt
// for a different apply finds no such record and fails rather than report the
// drop recoverable.
func TestEngine_ResumeDropPhase_RecognizesItsCopyAfterARestart(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	_, err := db.ExecContext(t.Context(), "CREATE TABLE `durable_orders` (id INT PRIMARY KEY AUTO_INCREMENT)")
	require.NoError(t, err, "create durable_orders")
	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parse DSN")

	attempt := func(applyIdentifier string) error {
		eng := engineForApply(t, dsn, applyIdentifier)
		return eng.quarantineDroppedTables(t.Context(), host, username, password, database, "DROP TABLE `durable_orders`")
	}

	require.NoError(t, attempt("apply-orders"), "the first attempt quarantines the table")
	copies := listQuarantinedTables(t, db)
	require.Len(t, copies, 2, "the copy and the quarantine log: %v", copies)
	recorded, err := pendingdrops.RecordedQuarantines(t.Context(), db, "apply-orders", database, "durable_orders")
	require.NoError(t, err)
	require.Len(t, recorded, 1, "the log names the copy before anything else reads it")
	assert.Contains(t, copies, recorded[0].QuarantineTable)

	require.NoError(t, attempt("apply-orders"), "a fresh engine for the same apply skips the table it already quarantined")
	assert.Len(t, listQuarantinedTables(t, db), 2, "the table is quarantined exactly once")

	err = attempt("apply-other")
	require.Error(t, err)
	message, ok := engine.OperatorMessageOf(err)
	require.True(t, ok, "error must carry an operator message")
	assert.Contains(t, message, "`durable_orders` does not exist and was not quarantined by this attempt of the schema change")
}

// breakQuarantineLog leaves a table under the quarantine log's name whose
// columns the log's reads and writes do not match, so every statement against
// the log fails the way a damaged or foreign table at that name would.
func breakQuarantineLog(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(t.Context(), fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", pendingdrops.Database))
	require.NoError(t, err, "create pending drops database")
	_, err = db.ExecContext(t.Context(), fmt.Sprintf("CREATE TABLE `%s`.`%s` (unrelated INT)", pendingdrops.Database, pendingdrops.LogTable))
	require.NoError(t, err, "create a quarantine log with the wrong shape")
}

// makeQuarantineLogReadOnly leaves a view under the quarantine log's name that
// reads like an empty log but refuses every insert, so the quarantine can
// check its record and cannot write one.
func makeQuarantineLogReadOnly(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, stmt := range []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", pendingdrops.Database),
		fmt.Sprintf("CREATE TABLE `%s`.`log_base` (quarantine_table varchar(64), source_schema varchar(64), "+
			"source_table varchar(64), apply_identifier varchar(255), quarantined_at datetime(3))", pendingdrops.Database),
		fmt.Sprintf("CREATE VIEW `%s`.`%s` AS SELECT DISTINCT quarantine_table, source_schema, source_table, apply_identifier, quarantined_at FROM `%s`.`log_base`",
			pendingdrops.Database, pendingdrops.LogTable, pendingdrops.Database),
	} {
		_, err := db.ExecContext(t.Context(), stmt)
		require.NoError(t, err, stmt)
	}
}

// engineForApply returns an engine tracking a running schema change that
// belongs to the named apply, the way Apply leaves it before the DROP phase.
func engineForApply(t *testing.T, dsn, applyIdentifier string) *Engine {
	t.Helper()
	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parse DSN")
	eng := New(Config{Logger: slog.Default()})
	eng.installRunningSchemaChange(&runningSchemaChange{
		database: database, state: engine.StateRunning,
		host: host, username: username, password: password,
		applyIdentifier: applyIdentifier,
	})
	return eng
}

// A schema change that names its apply does not quarantine a table it cannot
// record in the target's quarantine log. With the record refused, the table
// stays in place and the statement fails: moving it anyway would leave a copy
// in pending drops that no later attempt of the apply could recognize as its
// own, so the replay would report the drop final.
func TestEngine_DropPhase_RefusesToQuarantineWhenTheLogCannotBeWritten(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)
	makeQuarantineLogReadOnly(t, db)

	_, err := db.ExecContext(t.Context(), "CREATE TABLE `probe_keep` (id INT PRIMARY KEY AUTO_INCREMENT)")
	require.NoError(t, err, "create probe_keep")
	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parse DSN")

	eng := engineForApply(t, dsn, "apply-a")
	err = eng.quarantineDroppedTables(t.Context(), host, username, password, database, "DROP TABLE `probe_keep`")
	require.Error(t, err, "the drop must fail when the quarantine log cannot be written")
	assert.Contains(t, err.Error(), "record which apply quarantines the DROP TABLE targets before moving them")

	exists, err := tableExistsInSchema(t.Context(), db, database, "probe_keep")
	require.NoError(t, err)
	assert.True(t, exists, "the table stays in place when its quarantine cannot be recorded")
	assert.Equal(t, []string{"log_base", pendingdrops.LogTable}, listQuarantinedTables(t, db), "no copy lands in pending drops")
}

// A DROP TABLE target that is missing is resolved against the target's
// quarantine log. When the log cannot be read, the statement fails rather than
// read as "never quarantined": IF EXISTS would otherwise report the drop done
// over a copy this apply may well hold, and a plain DROP would report a
// recoverable table unrecoverable.
func TestEngine_DropPhase_FailsAMissingTargetWhenTheLogCannotBeRead(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)
	breakQuarantineLog(t, db)

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parse DSN")

	eng := engineForApply(t, dsn, "apply-a")
	err = eng.quarantineDroppedTables(t.Context(), host, username, password, database, "DROP TABLE IF EXISTS `probe_missing`")
	require.Error(t, err, "a missing target must not be skipped when the quarantine log cannot be read")
	assert.Contains(t, err.Error(), "read which copies apply apply-a recorded for DROP TABLE target")
}

// An apply that drops a table through Apply records, on the target, that its
// apply identifier moved the table into pending drops, so any later attempt of
// the same apply can find the copy.
func TestEngine_Apply_RecordsWhichApplyQuarantinedTheTable(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	_, err := db.ExecContext(t.Context(), "CREATE TABLE `apply_drops_me` (id INT PRIMARY KEY AUTO_INCREMENT)")
	require.NoError(t, err, "create apply_drops_me")

	eng := New(Config{Logger: slog.Default()})
	defer eng.Drain()
	result, err := eng.Apply(t.Context(), &engine.ApplyRequest{
		ApplyIdentifier: "apply-drops-me",
		Database:        "testdb",
		Changes: []engine.SchemaChange{{
			Namespace:    "testdb",
			TableChanges: []engine.TableChange{{Table: "apply_drops_me", DDL: "DROP TABLE `apply_drops_me`"}},
		}},
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err, "Apply()")
	require.True(t, result.Accepted, "apply not accepted: %s", result.Message)
	waitForTerminalOutcome(t, eng, engine.StateCompleted)

	recorded, err := pendingdrops.RecordedQuarantines(t.Context(), db, "apply-drops-me", "testdb", "apply_drops_me")
	require.NoError(t, err)
	require.Len(t, recorded, 1)
	assert.Contains(t, listQuarantinedTables(t, db), recorded[0].QuarantineTable, "the record names the copy that holds the table")
}

// waitFor polls condition until it holds or the deadline passes.
func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !condition() {
		require.True(t, time.Now().Before(deadline), "timed out waiting for %s", what)
		time.Sleep(20 * time.Millisecond)
	}
}

// A resumed DROP phase reaches a table that is missing although this schema
// change never quarantined it, while pending drops holds a same-named table
// quarantined by some other change. Nothing this schema change did explains
// the missing table and SchemaBot holds no copy of it, so the replay fails
// the statement rather than report a recoverable drop, and the table after
// it is left in place.
func TestEngine_ResumeDropPhase_FailsOnMissingTableItNeverQuarantined(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(t.Context())
		_, err := db.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS `resume_untouched_second`")
		require.NoError(t, err, "drop resume_untouched_second")
		_, err = db.ExecContext(cleanupCtx, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", pendingdrops.Database))
		require.NoError(t, err, "drop pending drops database")
	})

	_, err := db.ExecContext(t.Context(), "CREATE TABLE `resume_untouched_second` (id INT PRIMARY KEY AUTO_INCREMENT)")
	require.NoError(t, err, "create resume_untouched_second")

	// Another change quarantined a table of the same name, so pending drops
	// holds a copy named for resume_vanished_first that this schema change
	// did not make.
	_, err = db.ExecContext(t.Context(), "CREATE TABLE `resume_vanished_first` (id INT PRIMARY KEY AUTO_INCREMENT)")
	require.NoError(t, err, "create resume_vanished_first")
	foreign, err := pendingdrops.MoveTable(t.Context(), db, "testdb", "resume_vanished_first", time.Now())
	require.NoError(t, err, "quarantine resume_vanished_first outside the schema change")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	plan := []string{
		"DROP TABLE `resume_vanished_first`",
		"DROP TABLE `resume_untouched_second`",
	}
	state, reason := resumeStoppedDropPhase(t, eng, dsn, nil, plan)
	assert.Equal(t, engine.StateFailed, state)
	assert.Contains(t, reason, "`resume_vanished_first` does not exist and was not quarantined by this attempt of the schema change")

	var count int
	err = db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'testdb' AND table_name = 'resume_untouched_second'").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "the statement after the failed one must not run")

	assert.Equal(t, []string{foreign}, listQuarantinedTables(t, db),
		"only the other change's copy may be in pending drops")
}

// A replay record does not make a missing DROP target safe after retention has
// removed the recorded pending drops copy. The engine fails closed rather than
// reporting the table as recoverable, and the message names the copy it was
// looking for, retention as the likely cause, and what unblocks the change.
func TestEngine_ResumeDropPhase_FailsWhenRecordedCopyWasRemoved(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parse DSN")

	eng := New(Config{Logger: slog.Default()})
	eng.installRunningSchemaChange(&runningSchemaChange{
		database: database,
		quarantinedDrops: map[dropTarget][]pendingdrops.QuarantinedTable{
			{schema: database, table: "removed_copy"}: {{
				SchemaName:       database,
				TableName:        "removed_copy",
				QuarantineSchema: pendingdrops.Database,
				QuarantineTable:  "20260101000000_removed_copy",
			}},
		},
	})

	err = eng.quarantineDroppedTables(t.Context(), host, username, password, database,
		"DROP TABLE `removed_copy`")
	require.Error(t, err)
	message, ok := engine.OperatorMessageOf(err)
	require.True(t, ok, "error must carry an operator message")
	assert.Contains(t, message,
		"DROP TABLE target `removed_copy` does not exist and the pending drops copy this attempt recorded for it, `"+
			pendingdrops.Database+"`.`20260101000000_removed_copy`, is not present")
	assert.Contains(t, message, "retention period has removed it")
	assert.Contains(t, message, "plan the schema change again against the live schema before resuming it")
	assert.Empty(t, listQuarantinedTables(t, db), "a failed replay must not quarantine anything")
}

// A table that a stopped attempt and its replay both set out to quarantine
// carries two recorded destinations. When neither copy is present the engine
// fails closed as for one, and the message names every destination it looked
// for, so the operator can check each of them against retention.
func TestEngine_ResumeDropPhase_FailsWhenEveryRecordedCopyWasRemoved(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parse DSN")

	eng := New(Config{Logger: slog.Default()})
	eng.installRunningSchemaChange(&runningSchemaChange{
		database: database,
		quarantinedDrops: map[dropTarget][]pendingdrops.QuarantinedTable{
			{schema: database, table: "removed_copies"}: {
				{
					SchemaName:       database,
					TableName:        "removed_copies",
					QuarantineSchema: pendingdrops.Database,
					QuarantineTable:  "20260101000000000_removed_copies",
				},
				{
					SchemaName:       database,
					TableName:        "removed_copies",
					QuarantineSchema: pendingdrops.Database,
					QuarantineTable:  "20260101000000001_removed_copies",
				},
			},
		},
	})

	err = eng.quarantineDroppedTables(t.Context(), host, username, password, database,
		"DROP TABLE `removed_copies`")
	require.Error(t, err)
	message, ok := engine.OperatorMessageOf(err)
	require.True(t, ok, "error must carry an operator message")
	assert.Contains(t, message,
		"DROP TABLE target `removed_copies` does not exist and none of the pending drops copies this attempt recorded for it (`"+
			pendingdrops.Database+"`.`20260101000000000_removed_copies`, `"+
			pendingdrops.Database+"`.`20260101000000001_removed_copies`) is present")
	assert.Contains(t, message, "retention period has removed it")
	assert.Empty(t, listQuarantinedTables(t, db), "a failed replay must not quarantine anything")
}

// A table that a stopped attempt and its replay both set out to quarantine
// carries two recorded destinations. When the stopped attempt's RENAME never
// landed and the replay's did, the table sits at the replay's destination; a
// further replay finds it there and skips the table instead of reporting the
// drop as final.
func TestEngine_ResumeDropPhase_SkipsTableQuarantinedAtTheReplaysDestination(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parse DSN")

	first := pendingdrops.QuarantinedTable{
		SchemaName:       database,
		TableName:        "landed_at_replay",
		QuarantineSchema: pendingdrops.Database,
		QuarantineTable:  "20260101000000000_landed_at_replay",
	}
	second := first
	second.QuarantineTable = "20260101000000001_landed_at_replay"

	_, err = db.ExecContext(t.Context(), fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", pendingdrops.Database))
	require.NoError(t, err, "create pending drops database")
	_, err = db.ExecContext(t.Context(),
		fmt.Sprintf("CREATE TABLE `%s`.`%s` (id INT PRIMARY KEY)", pendingdrops.Database, second.QuarantineTable))
	require.NoError(t, err, "create the copy at the replay's destination")

	eng := New(Config{Logger: slog.Default()})
	eng.installRunningSchemaChange(&runningSchemaChange{
		database: database,
		quarantinedDrops: map[dropTarget][]pendingdrops.QuarantinedTable{
			{schema: database, table: "landed_at_replay"}: {first, second},
		},
	})

	err = eng.quarantineDroppedTables(t.Context(), host, username, password, database,
		"DROP TABLE `landed_at_replay`")
	require.NoError(t, err, "the copy at the replay's destination holds the table, so the drop is not final")
	assert.Equal(t, []string{second.QuarantineTable}, listQuarantinedTables(t, db))
}

// quarantineOvertakenBy runs the quarantine of drop for apply-overtaken while
// overtaking is already queued on the server ahead of its RENAME, and returns
// the quarantine's result. A READ lock on lockedTables holds both statements
// until each has queued, and releasing it lets overtaking land first, the way
// a RENAME an earlier attempt abandoned lands ahead of the replay's.
func quarantineOvertakenBy(t *testing.T, db *sql.DB, dsn, lockedTables, overtaking, drop string) error {
	t.Helper()
	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parse DSN")

	locker, err := db.Conn(t.Context())
	require.NoError(t, err, "open locking connection")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 10*time.Second)
		defer cancel()
		_, err := locker.ExecContext(cleanupCtx, "UNLOCK TABLES")
		assert.NoError(t, err, "release the lock on %s", lockedTables)
		assert.NoError(t, locker.Close(), "close the locking connection")
	})
	_, err = locker.ExecContext(t.Context(), "LOCK TABLES "+lockedTables)
	require.NoError(t, err, "lock %s", lockedTables)

	waiting := func() int {
		var count int
		require.NoError(t, db.QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM information_schema.processlist WHERE state LIKE 'Waiting for %metadata lock'").Scan(&count))
		return count
	}
	overtaken := make(chan error, 1)
	go func() {
		_, err := db.ExecContext(t.Context(), overtaking)
		overtaken <- err
	}()
	waitFor(t, "the overtaking statement to queue on the lock", func() bool { return waiting() >= 1 })

	eng := engineForApply(t, dsn, "apply-overtaken")
	quarantine := make(chan error, 1)
	go func() {
		quarantine <- eng.quarantineDroppedTables(t.Context(), host, username, password, database, drop)
	}()
	waitFor(t, "the quarantine to queue behind the overtaking statement", func() bool { return waiting() >= 2 })

	_, err = locker.ExecContext(t.Context(), "UNLOCK TABLES")
	require.NoError(t, err, "release the lock")
	require.NoError(t, receiveWithin(t, overtaken, "the overtaking statement to finish"), "the overtaking statement")
	return receiveWithin(t, quarantine, "the quarantine to return")
}

// recordEarlierCopy records in the target's quarantine log that an earlier
// attempt of apply-overtaken moved `testdb`.`table` to quarantineTable.
func recordEarlierCopy(t *testing.T, db *sql.DB, table, quarantineTable string) {
	t.Helper()
	require.NoError(t, pendingdrops.RecordQuarantines(t.Context(), db, "apply-overtaken", []pendingdrops.QuarantinedTable{{
		SchemaName: "testdb", TableName: table,
		QuarantineSchema: pendingdrops.Database, QuarantineTable: quarantineTable,
	}}, time.Now()), "record the earlier attempt's copy of %s", table)
}

// A resume on an engine with no memory of the earlier attempt quarantines two
// tables while the RENAME that attempt abandoned is still queued ahead of it.
// The abandoned RENAME lands first, at the copies the quarantine log names, so
// the resume's own RENAME finds no table. Both tables are in pending drops at
// copies this apply recorded, so the statement succeeds and leaves the copies
// alone.
func TestEngine_DropPhase_SucceedsWhenOvertakenByTheEarlierAttemptsRename(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	for _, name := range []string{"overtaken_first", "overtaken_second"} {
		_, err := db.ExecContext(t.Context(), fmt.Sprintf("CREATE TABLE `%s` (id INT PRIMARY KEY)", name))
		require.NoError(t, err, "create %s", name)
	}
	const firstCopy = "20260101000000000_overtaken_first"
	const secondCopy = "20260101000000000_overtaken_second"
	recordEarlierCopy(t, db, "overtaken_first", firstCopy)
	recordEarlierCopy(t, db, "overtaken_second", secondCopy)

	err := quarantineOvertakenBy(t, db, dsn, "`overtaken_first` READ, `overtaken_second` READ",
		fmt.Sprintf("RENAME TABLE `testdb`.`overtaken_first` TO `%[1]s`.`%[2]s`, `testdb`.`overtaken_second` TO `%[1]s`.`%[3]s`",
			pendingdrops.Database, firstCopy, secondCopy),
		"DROP TABLE `overtaken_first`, `overtaken_second`")
	require.NoError(t, err, "both tables are at copies this apply recorded")
	assert.Equal(t, []string{firstCopy, secondCopy, pendingdrops.LogTable}, listQuarantinedTables(t, db),
		"each table is in pending drops exactly once, at the earlier attempt's copy")
	assert.False(t, tableExists(t, db, "overtaken_first"), "the first table is gone from the target")
	assert.False(t, tableExists(t, db, "overtaken_second"), "the second table is gone from the target")
}

// While the quarantine RENAME waits, something other than SchemaBot drops the
// table outright. The RENAME finds no table and no copy this schema change
// recorded holds it, so the statement fails with the server's error rather
// than report a drop SchemaBot holds no copy of.
func TestEngine_DropPhase_FailsWhenOvertakenByADropThatLeavesNoCopy(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	_, err := db.ExecContext(t.Context(), "CREATE TABLE `overtaken_dropped` (id INT PRIMARY KEY)")
	require.NoError(t, err, "create overtaken_dropped")

	err = quarantineOvertakenBy(t, db, dsn, "`overtaken_dropped` READ",
		"DROP TABLE `testdb`.`overtaken_dropped`", "DROP TABLE `overtaken_dropped`")
	require.Error(t, err, "a table found nowhere must fail the statement")
	assert.Contains(t, err.Error(), "Error 1146", "the server's error is returned unchanged")
	assert.Contains(t, err.Error(), "RENAME TABLE `testdb`.`overtaken_dropped`")
	assert.Equal(t, []string{pendingdrops.LogTable}, listQuarantinedTables(t, db), "no copy exists in pending drops")
}

// A DROP of two tables is overtaken by a RENAME that puts the first at the copy
// an earlier attempt recorded and moves the second out of the target without a
// copy. One table is accounted for and the other is not, so the statement
// fails rather than report both quarantined, and the first table's copy is
// left in place.
func TestEngine_DropPhase_FailsWhenOvertakenRenameAccountsForOnlySomeTables(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	for _, name := range []string{"overtaken_kept", "overtaken_lost"} {
		_, err := db.ExecContext(t.Context(), fmt.Sprintf("CREATE TABLE `%s` (id INT PRIMARY KEY)", name))
		require.NoError(t, err, "create %s", name)
	}
	const earlierCopy = "20260101000000000_overtaken_kept"
	recordEarlierCopy(t, db, "overtaken_kept", earlierCopy)

	err := quarantineOvertakenBy(t, db, dsn, "`overtaken_kept` READ, `overtaken_lost` READ",
		fmt.Sprintf("RENAME TABLE `testdb`.`overtaken_kept` TO `%s`.`%s`, `testdb`.`overtaken_lost` TO `testdb`.`overtaken_lost_elsewhere`",
			pendingdrops.Database, earlierCopy),
		"DROP TABLE `overtaken_kept`, `overtaken_lost`")
	require.Error(t, err, "a table no recorded copy holds must fail the statement")
	assert.Contains(t, err.Error(), "Error 1146", "the server's error is returned unchanged")
	assert.Equal(t, []string{earlierCopy, pendingdrops.LogTable}, listQuarantinedTables(t, db),
		"only the earlier attempt's copy of the first table is in pending drops")
}

// A DROP of two tables is overtaken by a RENAME that puts the first at the copy
// an earlier attempt recorded, while the second, which an earlier attempt also
// quarantined, has since been created again on the target. A copy of the
// second exists, but the table is still in place and was not quarantined by
// this statement, so the statement fails.
func TestEngine_DropPhase_FailsWhenOvertakenRenameLeavesATableInPlace(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	for _, name := range []string{"overtaken_moved", "overtaken_recreated"} {
		_, err := db.ExecContext(t.Context(), fmt.Sprintf("CREATE TABLE `%s` (id INT PRIMARY KEY)", name))
		require.NoError(t, err, "create %s", name)
	}
	const movedCopy = "20260101000000000_overtaken_moved"
	const recreatedCopy = "20260101000000000_overtaken_recreated"
	recordEarlierCopy(t, db, "overtaken_moved", movedCopy)
	recordEarlierCopy(t, db, "overtaken_recreated", recreatedCopy)
	_, err := db.ExecContext(t.Context(),
		fmt.Sprintf("CREATE TABLE `%s`.`%s` (id INT PRIMARY KEY)", pendingdrops.Database, recreatedCopy))
	require.NoError(t, err, "create the earlier copy of overtaken_recreated")

	err = quarantineOvertakenBy(t, db, dsn, "`overtaken_moved` READ, `overtaken_recreated` READ",
		fmt.Sprintf("RENAME TABLE `testdb`.`overtaken_moved` TO `%s`.`%s`", pendingdrops.Database, movedCopy),
		"DROP TABLE `overtaken_moved`, `overtaken_recreated`")
	require.Error(t, err, "a table still on the target must fail the statement")
	assert.Contains(t, err.Error(), "Error 1146", "the server's error is returned unchanged")
	assert.True(t, tableExists(t, db, "overtaken_recreated"), "the second table is still on the target")
}

// An earlier attempt of this apply quarantined `overtaken_sessions`, and the
// table has since been created again with new rows. While the quarantine
// RENAME of the new table waits, something other than SchemaBot drops it
// outright. The earlier copy is still in pending drops, but it was there before
// the RENAME was issued, so it holds the old rows and not the ones just
// dropped. The statement fails with the server's error rather than report the
// new table as recoverable.
func TestEngine_DropPhase_FailsWhenOvertakenByADropAndOnlyAnOlderCopyExists(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	const olderCopy = "20260101000000000_overtaken_sessions"
	recordEarlierCopy(t, db, "overtaken_sessions", olderCopy)
	_, err := db.ExecContext(t.Context(),
		fmt.Sprintf("CREATE TABLE `%s`.`%s` (id INT PRIMARY KEY)", pendingdrops.Database, olderCopy))
	require.NoError(t, err, "create the earlier copy of overtaken_sessions")
	_, err = db.ExecContext(t.Context(),
		fmt.Sprintf("INSERT INTO `%s`.`%s` (id) VALUES (1)", pendingdrops.Database, olderCopy))
	require.NoError(t, err, "seed the earlier copy with the old rows")
	_, err = db.ExecContext(t.Context(), "CREATE TABLE `overtaken_sessions` (id INT PRIMARY KEY)")
	require.NoError(t, err, "create overtaken_sessions again")
	_, err = db.ExecContext(t.Context(), "INSERT INTO `overtaken_sessions` (id) VALUES (2)")
	require.NoError(t, err, "seed the new overtaken_sessions with new rows")

	err = quarantineOvertakenBy(t, db, dsn, "`overtaken_sessions` READ",
		"DROP TABLE `testdb`.`overtaken_sessions`", "DROP TABLE `overtaken_sessions`")
	require.Error(t, err, "a copy older than the RENAME does not hold the dropped table")
	assert.Contains(t, err.Error(), "Error 1146", "the server's error is returned unchanged")
	assert.Contains(t, err.Error(), "RENAME TABLE `testdb`.`overtaken_sessions`")
	assert.False(t, tableExists(t, db, "overtaken_sessions"), "the new table is gone from the target")
	assert.Equal(t, []string{olderCopy, pendingdrops.LogTable}, listQuarantinedTables(t, db),
		"only the earlier copy is in pending drops")
	var ids string
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT GROUP_CONCAT(id ORDER BY id) FROM `%s`.`%s`", pendingdrops.Database, olderCopy)).Scan(&ids),
		"read the earlier copy")
	assert.Equal(t, "1", ids, "the earlier copy holds only the old rows, not the dropped table's")
}

// The quarantine sees `stale_window_sessions` on the target. Before it notes
// which copies are already in pending drops, the RENAME an earlier attempt
// abandoned lands and moves that table to its recorded copy, the table is
// created again with new rows, and something other than SchemaBot drops the
// new table outright. The quarantine's own RENAME then finds no table. The
// only copy holds the old rows, and it was in pending drops by the time the
// copies were noted, so it does not count: the statement fails with the
// server's error and the apply log never says the dropped table is
// recoverable.
func TestEngine_DropPhase_FailsWhenAnOlderCopyArrivesJustAfterTheTableIsSeen(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)
	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parse DSN")

	const olderCopy = "20260101000000000_stale_window_sessions"
	recordEarlierCopy(t, db, "stale_window_sessions", olderCopy)
	_, err = db.ExecContext(t.Context(), "CREATE TABLE `stale_window_sessions` (id INT PRIMARY KEY)")
	require.NoError(t, err, "create stale_window_sessions")
	_, err = db.ExecContext(t.Context(), "INSERT INTO `stale_window_sessions` (id) VALUES (1)")
	require.NoError(t, err, "seed stale_window_sessions with the old rows")

	eng := engineForApply(t, dsn, "apply-overtaken")
	var applyLog struct {
		mu    sync.Mutex
		lines []string
	}
	eng.SetLogCallback(func(_ slog.Level, table, msg string) {
		applyLog.mu.Lock()
		defer applyLog.mu.Unlock()
		applyLog.lines = append(applyLog.lines, table+": "+msg)
	})
	tableSeen := make(chan struct{})
	copiesMayBeNoted := make(chan struct{})
	eng.dropTargetSeenWindow = func() {
		close(tableSeen)
		select {
		case <-copiesMayBeNoted:
		case <-t.Context().Done():
		}
	}

	quarantine := make(chan error, 1)
	go func() {
		quarantine <- eng.quarantineDroppedTables(t.Context(), host, username, password, database,
			"DROP TABLE `stale_window_sessions`")
	}()
	select {
	case <-tableSeen:
	case <-time.After(resumeDeadline):
		require.FailNow(t, "timed out waiting for the quarantine to see the table on the target")
	}

	_, err = db.ExecContext(t.Context(),
		fmt.Sprintf("RENAME TABLE `testdb`.`stale_window_sessions` TO `%s`.`%s`", pendingdrops.Database, olderCopy))
	require.NoError(t, err, "land the earlier attempt's RENAME")
	_, err = db.ExecContext(t.Context(), "CREATE TABLE `stale_window_sessions` (id INT PRIMARY KEY)")
	require.NoError(t, err, "create stale_window_sessions again")
	_, err = db.ExecContext(t.Context(), "INSERT INTO `stale_window_sessions` (id) VALUES (2)")
	require.NoError(t, err, "seed the new stale_window_sessions with new rows")
	_, err = db.ExecContext(t.Context(), "DROP TABLE `testdb`.`stale_window_sessions`")
	require.NoError(t, err, "drop the new stale_window_sessions outright")
	close(copiesMayBeNoted)

	err = receiveWithin(t, quarantine, "the quarantine to return")
	require.Error(t, err, "a copy that was already in pending drops when the copies were noted does not hold the dropped table")
	assert.Contains(t, err.Error(), "Error 1146", "the server's error is returned unchanged")
	assert.Contains(t, err.Error(), "RENAME TABLE `testdb`.`stale_window_sessions`")
	assert.False(t, tableExists(t, db, "stale_window_sessions"), "the new table is gone from the target")
	assert.Equal(t, []string{olderCopy, pendingdrops.LogTable}, listQuarantinedTables(t, db),
		"only the earlier copy is in pending drops")
	var ids string
	require.NoError(t, db.QueryRowContext(t.Context(),
		fmt.Sprintf("SELECT GROUP_CONCAT(id ORDER BY id) FROM `%s`.`%s`", pendingdrops.Database, olderCopy)).Scan(&ids),
		"read the earlier copy")
	assert.Equal(t, "1", ids, "the earlier copy holds only the old rows, not the dropped table's")
	applyLog.mu.Lock()
	defer applyLog.mu.Unlock()
	assert.Empty(t, applyLog.lines, "the apply log must not report the dropped table as quarantined or recoverable")
}

// A quarantine RENAME finds a table missing, and when the engine looks again
// `overtaken_back_in_place` is on the target: the earlier attempt's RENAME
// moved it to its recorded copy and it has been created again since. The copy
// arrived after the table was seen, so it would count, but the table in place
// now was not quarantined by anyone. The statement fails with the RENAME's
// own error and the table is left where it is.
func TestEngine_DropPhase_FailsWhenAnOvertakenTableIsBackOnTheTarget(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	const earlierCopy = "20260101000000000_overtaken_back_in_place"
	recordEarlierCopy(t, db, "overtaken_back_in_place", earlierCopy)
	_, err := db.ExecContext(t.Context(),
		fmt.Sprintf("CREATE TABLE `%s`.`%s` (id INT PRIMARY KEY)", pendingdrops.Database, earlierCopy))
	require.NoError(t, err, "create the earlier copy of overtaken_back_in_place")
	_, err = db.ExecContext(t.Context(), "CREATE TABLE `overtaken_back_in_place` (id INT PRIMARY KEY)")
	require.NoError(t, err, "create overtaken_back_in_place again")

	eng := engineForApply(t, dsn, "apply-overtaken")
	renameErr := errors.New("rename tables to pending drops: Error 1146 (42S02): Table 'testdb.overtaken_back_in_place' doesn't exist")
	moved := []pendingdrops.QuarantinedTable{{
		SchemaName: "testdb", TableName: "overtaken_back_in_place",
		QuarantineSchema: pendingdrops.Database, QuarantineTable: "20260202000000000_overtaken_back_in_place",
	}}
	err = eng.resolveOvertakenQuarantine(t.Context(), db, moved, nil, renameErr)
	require.ErrorIs(t, err, renameErr, "a table still on the target must fail the statement with the RENAME's error")
	assert.True(t, tableExists(t, db, "overtaken_back_in_place"), "the table is still on the target")
	assert.Equal(t, []string{earlierCopy, pendingdrops.LogTable}, listQuarantinedTables(t, db),
		"the earlier copy is left alone")
}

// The schema change is stopped while its quarantine RENAME waits on a
// metadata lock, so the RENAME fails because the attempt was cancelled, not
// because a table went missing. Only a missing table is a reason to look for
// an earlier attempt's copy, so the cancellation is returned as it is, with
// nothing about that search added to it.
func TestEngine_DropPhase_ReturnsARenameFailureThatIsNotAMissingTableUnchanged(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)
	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parse DSN")

	_, err = db.ExecContext(t.Context(), "CREATE TABLE `cancelled_mid_rename` (id INT PRIMARY KEY)")
	require.NoError(t, err, "create cancelled_mid_rename")

	locker, err := db.Conn(t.Context())
	require.NoError(t, err, "open locking connection")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 10*time.Second)
		defer cancel()
		_, err := locker.ExecContext(cleanupCtx, "UNLOCK TABLES")
		assert.NoError(t, err, "release the lock on cancelled_mid_rename")
		assert.NoError(t, locker.Close(), "close the locking connection")
	})
	_, err = locker.ExecContext(t.Context(), "LOCK TABLES `cancelled_mid_rename` READ")
	require.NoError(t, err, "lock cancelled_mid_rename")

	eng := engineForApply(t, dsn, "apply-overtaken")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	quarantine := make(chan error, 1)
	go func() {
		quarantine <- eng.quarantineDroppedTables(ctx, host, username, password, database,
			"DROP TABLE `cancelled_mid_rename`")
	}()
	waitFor(t, "the RENAME to block on the metadata lock", func() bool {
		var waiting int
		require.NoError(t, db.QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM information_schema.processlist WHERE info LIKE 'RENAME TABLE%cancelled_mid_rename%'").Scan(&waiting))
		return waiting > 0
	})
	cancel()

	err = receiveWithin(t, quarantine, "the cancelled quarantine to return")
	require.ErrorIs(t, err, context.Canceled, "the cancellation is the error the statement fails with")
	assert.Contains(t, err.Error(), "RENAME TABLE `testdb`.`cancelled_mid_rename`")
	assert.NotContains(t, err.Error(), "checking whether an earlier attempt already moved them",
		"a RENAME that did not report a missing table is not checked against earlier copies")

	// The server still runs the RENAME the client gave up on once the lock is
	// released, so the test waits for it rather than leave it queued.
	_, err = locker.ExecContext(t.Context(), "UNLOCK TABLES")
	require.NoError(t, err, "release the metadata lock")
	waitFor(t, "the server-side RENAME to complete", func() bool { return !tableExists(t, db, "cancelled_mid_rename") })
}

// A replay looks for the pending drops copy it recorded and cannot tell
// whether it is there, because the existence check fails. The failure is
// returned rather than read as "the copy is missing": a missing copy reports
// the drop final and its data unrecoverable, while here the copy is in fact
// in pending drops.
func TestFindRecordedCopy_ReturnsTheExistenceCheckFailure(t *testing.T) {
	_, db := setupTestMySQL(t)
	cleanupPendingDropsDB(t, db)

	recorded := pendingdrops.QuarantinedTable{
		SchemaName:       "testdb",
		TableName:        "unchecked_copy",
		QuarantineSchema: pendingdrops.Database,
		QuarantineTable:  "20260101000000000_unchecked_copy",
	}
	_, err := db.ExecContext(t.Context(), fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", pendingdrops.Database))
	require.NoError(t, err, "create pending drops database")
	_, err = db.ExecContext(t.Context(),
		fmt.Sprintf("CREATE TABLE `%s`.`%s` (id INT PRIMARY KEY)", pendingdrops.Database, recorded.QuarantineTable))
	require.NoError(t, err, "create the recorded copy")

	// A cancelled context fails the existence check before it reaches MySQL.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, found, err := findRecordedCopy(ctx, db, []pendingdrops.QuarantinedTable{recorded})
	require.ErrorIs(t, err, context.Canceled, "the existence check's failure is returned, not read as a missing copy")
	assert.Contains(t, err.Error(), "check recorded pending drops table `20260101000000000_unchecked_copy` exists")
	assert.False(t, found)
}
