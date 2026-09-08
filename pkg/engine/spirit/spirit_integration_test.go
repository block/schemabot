//go:build integration

package spirit

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/checksum"
	spiritmigration "github.com/block/spirit/pkg/migration"
	"github.com/block/spirit/pkg/migration/check"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/mysqlerr"
	"github.com/block/schemabot/pkg/pendingdrops"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"

	drivermysql "github.com/block/mysql"
)

// Shared test infrastructure
var (
	sharedDSN       string
	sharedContainer testcontainers.Container
)

// copyProgressPollDeadline bounds the wait for a table copy to get underway.
// Tests that act on a schema change mid-copy need the copy observably in
// flight, so one that never starts is a failed test rather than one that waits
// indefinitely.
const copyProgressPollDeadline = 30 * time.Second

// waitForCopyProgress blocks until the schema change reports at least
// wantRowsCopied rows copied, returning how many had been copied. It fails
// rather than returning early: an operation that lands before the copy is
// underway exercises none of the mid-copy behavior its caller asserts on.
func waitForCopyProgress(t *testing.T, eng *Engine, wantRowsCopied int64) int64 {
	t.Helper()

	deadline := time.Now().Add(copyProgressPollDeadline)
	for time.Now().Before(deadline) {
		progress, err := eng.Progress(t.Context(), &engine.ProgressRequest{})
		require.NoError(t, err, "Progress()")
		require.NotEqual(t, engine.StateFailed, progress.State,
			"the schema change failed before its copy could be observed: %s", progress.ErrorMessage)
		require.NotEqual(t, engine.StateCompleted, progress.State,
			"the copy finished before it could be acted on; seed more rows so it stays in flight")

		if len(progress.Tables) > 0 && progress.Tables[0].RowsCopied >= wantRowsCopied {
			return progress.Tables[0].RowsCopied
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("copy did not reach %d rows within %s", wantRowsCopied, copyProgressPollDeadline)
	return 0
}

func TestMain(m *testing.M) {
	ctx := context.Background()

	var err error
	sharedContainer, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testutil.MySQLContainerRequest("mysql:8.0", testDatabase),
		Started:          true,
		Reuse:            os.Getenv("DEBUG") != "",
	})
	if err != nil {
		log.Fatalf("start mysql container: %v", err)
	}

	sharedDSN, err = testutil.MySQLDSN(ctx, sharedContainer, testDatabase, "parseTime=true", "multiStatements=true")
	if err != nil {
		log.Fatalf("build mysql dsn: %v", err)
	}

	code := m.Run()

	// Cleanup
	if os.Getenv("DEBUG") == "" {
		_ = sharedContainer.Terminate(ctx)
	}

	os.Exit(code)
}

// testSchemaFiles wraps a flat map of filenames to content into a schema.SchemaFiles
// with a single namespace matching the test database name.
func testSchemaFiles(files map[string]string) schema.SchemaFiles {
	return schema.SchemaFiles{"testdb": &schema.Namespace{Files: files}}
}

func tableSchemaNames(schemas []table.TableSchema) []string {
	names := make([]string, 0, len(schemas))
	for _, schema := range schemas {
		names = append(names, schema.Name)
	}
	return names
}

// setupTestMySQL returns a connection to the shared MySQL container.
// Each test should clean up its own tables.
func setupTestMySQL(t *testing.T) (string, *sql.DB) {
	t.Helper()

	db, err := sql.Open("block-mysql", sharedDSN)
	require.NoError(t, err, "connect to mysql")
	t.Cleanup(func() { utils.CloseAndLog(db) })

	return sharedDSN, db
}

// cleanupTables drops all tables in the test database to ensure test isolation
func cleanupTables(t *testing.T, db *sql.DB) {
	t.Helper()

	rows, err := db.QueryContext(t.Context(), "SHOW TABLES")
	require.NoError(t, err, "show tables")
	defer utils.CloseAndLog(rows)

	var tables []string
	for rows.Next() {
		var table string
		require.NoError(t, rows.Scan(&table), "scan table")
		tables = append(tables, table)
	}

	for _, table := range tables {
		if _, err := db.ExecContext(t.Context(), fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table)); err != nil {
			t.Logf("warning: drop table %s: %v", table, err)
		}
	}
}

func tableExists(t *testing.T, db *sql.DB, tableName string) bool {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
		tableName,
	).Scan(&count))
	return count > 0
}

func TestEngine_Plan_AddColumn(t *testing.T) {
	dsn, db := setupTestMySQL(t)

	// Create initial table
	_, err := db.ExecContext(t.Context(), `CREATE TABLE users (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(100) NOT NULL
	)`)
	require.NoError(t, err, "create table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	// Plan with desired schema that has an additional column
	result, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"users.sql": `CREATE TABLE users (
				id INT PRIMARY KEY AUTO_INCREMENT,
				name VARCHAR(100) NOT NULL,
				email VARCHAR(255) NULL
			)`,
		}),
		Credentials: &engine.Credentials{
			DSN: dsn,
		},
	})
	require.NoError(t, err, "Plan()")

	require.False(t, result.NoChanges, "expected changes, got NoChanges")
	require.NotEmpty(t, result.FlatDDL(), "expected DDL statements")

	// Verify the DDL contains an ADD COLUMN for email
	found := false
	for _, ddl := range result.FlatDDL() {
		t.Logf("DDL: %s", ddl)
		if containsAddColumn(ddl, "email") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected DDL to add email column, got: %v", result.FlatDDL())
}

func TestEngine_Plan_DropColumn(t *testing.T) {
	dsn, db := setupTestMySQL(t)

	// Create initial table with extra column
	_, err := db.ExecContext(t.Context(), `CREATE TABLE products (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(100) NOT NULL,
		description TEXT,
		deprecated_field VARCHAR(50)
	)`)
	require.NoError(t, err, "create table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	// Plan with desired schema that removes deprecated_field
	result, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"products.sql": `CREATE TABLE products (
				id INT PRIMARY KEY AUTO_INCREMENT,
				name VARCHAR(100) NOT NULL,
				description TEXT
			)`,
		}),
		Credentials: &engine.Credentials{
			DSN: dsn,
		},
	})
	require.NoError(t, err, "Plan()")

	require.False(t, result.NoChanges, "expected changes, got NoChanges")
	require.NotEmpty(t, result.FlatDDL(), "expected DDL statements")

	t.Logf("DDL statements: %v", result.FlatDDL())
	require.NotEmpty(t, result.Changes)
	require.NotEmpty(t, result.Changes[0].TableChanges)
	change := result.Changes[0].TableChanges[0]
	assert.Equal(t, "products", change.Table)
	assert.True(t, change.IsUnsafe)
	assert.Contains(t, change.UnsafeReason, "Unsafe operation detected: \"DROP COLUMN `deprecated_field`\"")
	assert.True(t, result.HasErrors(), "Spirit unsafe drop lint should remain the blocking gate")
}

func TestEngine_Plan_DropTable(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	_, err := db.ExecContext(t.Context(), `CREATE TABLE legacy_users (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(100) NOT NULL
	)`)
	require.NoError(t, err, "create table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	result, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database:    "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{}),
		Credentials: &engine.Credentials{
			DSN: dsn,
		},
	})
	require.NoError(t, err, "Plan()")

	require.False(t, result.NoChanges, "expected changes, got NoChanges")
	require.NotEmpty(t, result.Changes)
	require.NotEmpty(t, result.Changes[0].TableChanges)
	change := result.Changes[0].TableChanges[0]
	assert.Equal(t, "legacy_users", change.Table)
	assert.True(t, change.IsUnsafe)
	assert.Contains(t, change.UnsafeReason, "DROP TABLE `legacy_users`")
	assert.True(t, result.HasErrors(), "Spirit unsafe drop lint should remain the blocking gate")
	_, err = db.ExecContext(t.Context(), "DROP TABLE IF EXISTS `legacy_users`")
	require.NoError(t, err, "drop table")
}

// A desired schema that swaps a table's primary key diffs to a DROP PRIMARY KEY,
// one that introduces a referential constraint diffs to an ADD FOREIGN KEY, and
// one that reorders an ENUM's values diffs to a MODIFY COLUMN — all statements
// Spirit deterministically refuses at apply time. The plan carries the
// execution-mode verdict on those changes — mode "blocked" with the engine's
// reason — so the operator learns at plan time that the apply will fail, while
// an ordinary change in the same plan stays on the engine's default path.
//
// The ENUM case is the one the engine can only judge against the table's current
// column types, so it also proves the plan supplies the engine with the current
// definition, not just the statement.
func TestEngine_Plan_ExecutionVerdictBlocked(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupCtx := context.WithoutCancel(t.Context())
	t.Cleanup(func() {
		for _, table := range []string{"orders", "accounts", "notes", "shipments"} {
			_, err := db.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS `"+table+"`")
			assert.NoError(t, err, "drop table %s", table)
		}
	})

	_, err := db.ExecContext(t.Context(), `CREATE TABLE accounts (
		id INT NOT NULL AUTO_INCREMENT,
		tenant_id INT NOT NULL,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create accounts table")
	_, err = db.ExecContext(t.Context(), `CREATE TABLE notes (
		id INT NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create notes table")
	_, err = db.ExecContext(t.Context(), `CREATE TABLE orders (
		id INT NOT NULL AUTO_INCREMENT,
		note_id INT NOT NULL,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create orders table")
	_, err = db.ExecContext(t.Context(), `CREATE TABLE shipments (
		id INT NOT NULL AUTO_INCREMENT,
		status ENUM('new','shipped','done') NOT NULL DEFAULT 'new',
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create shipments table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	result, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"accounts.sql": `CREATE TABLE accounts (
				id INT NOT NULL AUTO_INCREMENT,
				tenant_id INT NOT NULL,
				PRIMARY KEY (id, tenant_id)
			)`,
			"notes.sql": `CREATE TABLE notes (
				id INT NOT NULL AUTO_INCREMENT,
				body TEXT,
				PRIMARY KEY (id)
			)`,
			"orders.sql": `CREATE TABLE orders (
				id INT NOT NULL AUTO_INCREMENT,
				note_id INT NOT NULL,
				PRIMARY KEY (id),
				CONSTRAINT fk_orders_note FOREIGN KEY (note_id) REFERENCES notes (id)
			)`,
			"shipments.sql": `CREATE TABLE shipments (
				id INT NOT NULL AUTO_INCREMENT,
				status ENUM('shipped','new','done') NOT NULL DEFAULT 'new',
				PRIMARY KEY (id)
			)`,
		}),
		Credentials: &engine.Credentials{
			DSN: dsn,
		},
	})
	require.NoError(t, err, "Plan()")
	require.False(t, result.NoChanges, "expected changes, got NoChanges")

	verdicts := make(map[string]engine.TableChange)
	for _, tc := range result.FlatTableChanges() {
		t.Logf("table %s: ddl=%s mode=%q reason=%q", tc.Table, tc.DDL, tc.ExecutionMode, tc.ModeReason)
		verdicts[tc.Table] = tc
	}

	accounts, ok := verdicts["accounts"]
	require.True(t, ok, "plan carries the accounts change")
	assert.Contains(t, accounts.DDL, "DROP PRIMARY KEY")
	assert.Equal(t, "blocked", accounts.ExecutionMode, "the refused statement carries the blocked verdict")
	assert.Equal(t, "dropping primary key is not supported", accounts.ModeReason)

	notes, ok := verdicts["notes"]
	require.True(t, ok, "plan carries the notes change")
	assert.Empty(t, notes.ExecutionMode, "an ordinary ADD COLUMN stays on the engine's default path")
	assert.Empty(t, notes.ModeReason)

	orders, ok := verdicts["orders"]
	require.True(t, ok, "plan carries the orders change")
	assert.Contains(t, orders.DDL, "FOREIGN KEY")
	assert.Contains(t, orders.DDL, "fk_orders_note")
	assert.Equal(t, "blocked", orders.ExecutionMode, "adding a foreign key carries the blocked verdict")
	assert.Equal(t, "adding foreign key constraints is not supported", orders.ModeReason)

	shipments, ok := verdicts["shipments"]
	require.True(t, ok, "plan carries the shipments change")
	assert.Contains(t, shipments.DDL, "MODIFY COLUMN")
	assert.Contains(t, shipments.DDL, "status")
	assert.Equal(t, "blocked", shipments.ExecutionMode, "reordering ENUM values carries the blocked verdict")
	assert.Contains(t, shipments.ModeReason, `unsafe ENUM value reorder on column "status"`)
}

// The execution-mode verdict asks the engine which statements it refuses, so
// this test proves the answer against the engine itself: every statement shape
// the verdict classifies as blocked is handed to a real Spirit schema change,
// which must refuse it with the very reason the verdict recorded. The engine
// attempts MySQL's native DDL before its refusal checks run, so a shape the
// native DDL can complete is not a refusal at all — an engine upgrade that
// relaxes one of these checks, or moves it behind the native DDL, fails this
// test rather than letting the plan claim an apply will fail when it succeeds.
func TestEngine_SpiritRefusesVerdictBlockedStatements(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupCtx := context.WithoutCancel(t.Context())
	t.Cleanup(func() {
		for _, table := range []string{"verdict_orders", "verdict_refs"} {
			_, err := db.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS `"+table+"`")
			assert.NoError(t, err, "drop table %s", table)
		}
	})

	_, err := db.ExecContext(t.Context(), `CREATE TABLE verdict_refs (
		id INT NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create verdict_refs table")
	_, err = db.ExecContext(t.Context(), `CREATE TABLE verdict_orders (
		id INT NOT NULL AUTO_INCREMENT,
		tenant_id INT NOT NULL,
		ref_id INT NOT NULL,
		status ENUM('new','shipped','done') NOT NULL DEFAULT 'new',
		perms SET('read','write','execute') DEFAULT NULL,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create verdict_orders table")

	// The engine's ENUM/SET refusals compare the statement against the table's
	// current column types, which the plan and the apply-time routing both read
	// from the target.
	var tableName, currentCreateTable string
	require.NoError(t, db.QueryRowContext(t.Context(), "SHOW CREATE TABLE `verdict_orders`").Scan(&tableName, &currentCreateTable),
		"show create table verdict_orders")

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	tests := []struct {
		name string
		stmt string
	}{
		{
			name: "drop primary key",
			stmt: "ALTER TABLE `verdict_orders` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `tenant_id`)",
		},
		{
			name: "add foreign key",
			stmt: "ALTER TABLE `verdict_orders` ADD CONSTRAINT `fk_verdict_ref` FOREIGN KEY (`ref_id`) REFERENCES `verdict_refs` (`id`)",
		},
		{
			name: "explicit algorithm clause",
			stmt: "ALTER TABLE `verdict_orders` ADD COLUMN `note` VARCHAR(64), ALGORITHM=INPLACE",
		},
		{
			name: "explicit lock clause",
			stmt: "ALTER TABLE `verdict_orders` ADD COLUMN `note` VARCHAR(64), LOCK=NONE",
		},
		{
			name: "enum value reorder",
			stmt: "ALTER TABLE `verdict_orders` MODIFY COLUMN `status` ENUM('shipped','new','done') NOT NULL DEFAULT 'new'",
		},
		{
			name: "enum value inserted in the middle",
			stmt: "ALTER TABLE `verdict_orders` MODIFY COLUMN `status` ENUM('new','pending','shipped','done') NOT NULL DEFAULT 'new'",
		},
		{
			name: "set member reorder",
			stmt: "ALTER TABLE `verdict_orders` MODIFY COLUMN `perms` SET('write','read','execute')",
		},
		{
			name: "enum to numeric conversion",
			stmt: "ALTER TABLE `verdict_orders` MODIFY COLUMN `status` INT NOT NULL DEFAULT 0",
		},
		{
			name: "set to enum conversion",
			stmt: "ALTER TABLE `verdict_orders` MODIFY COLUMN `perms` ENUM('read','write','execute')",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason, refused, err := check.StatementRefusal(t.Context(), tt.stmt, currentCreateTable, discardLogger())
			require.NoError(t, err, "StatementRefusal")
			require.True(t, refused, "the verdict must classify this statement as blocked")
			require.NotEmpty(t, reason)

			runner, err := spiritmigration.NewRunner(&spiritmigration.Migration{
				Host:      host,
				Username:  username,
				Password:  &password,
				Database:  database,
				Statement: tt.stmt,
			})
			require.NoError(t, err, "NewRunner")
			defer utils.CloseAndLog(runner)

			runCtx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			runErr := runner.Run(runCtx)
			require.Error(t, runErr, "the engine must refuse a statement the verdict marked blocked")
			assert.Contains(t, runErr.Error(), reason,
				"the engine's refusal must carry the reason the verdict recorded")
		})
	}
}

// Statements the engine's own preflight checks refuse, but that MySQL's native
// DDL — which the engine attempts first — can complete. The verdict must stay
// silent on these: routing them to direct execution, or reporting that the apply
// will fail, would both be wrong for a schema change the engine applies cleanly.
func TestEngine_VerdictSilentOnNativeDDLStatements(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupCtx := context.WithoutCancel(t.Context())
	t.Cleanup(func() {
		_, err := db.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS `verdict_fastpath`")
		assert.NoError(t, err, "drop table verdict_fastpath")
	})

	_, err := db.ExecContext(t.Context(), `CREATE TABLE verdict_fastpath (
		id INT NOT NULL AUTO_INCREMENT,
		email VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create verdict_fastpath table")

	var tableName, currentCreateTable string
	require.NoError(t, db.QueryRowContext(t.Context(), "SHOW CREATE TABLE `verdict_fastpath`").Scan(&tableName, &currentCreateTable),
		"show create table verdict_fastpath")

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	tests := []struct {
		name string
		stmt string
	}{
		{
			name: "drop and re-add the same column",
			stmt: "ALTER TABLE `verdict_fastpath` DROP COLUMN `email`, ADD COLUMN `email` VARCHAR(255) NOT NULL",
		},
		{
			name: "rename a column onto a freed name",
			stmt: "ALTER TABLE `verdict_fastpath` RENAME COLUMN `email` TO `contact_email`, ADD COLUMN `email` VARCHAR(255) NOT NULL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason, refused, err := check.StatementRefusal(t.Context(), tt.stmt, currentCreateTable, discardLogger())
			require.NoError(t, err, "StatementRefusal")
			assert.False(t, refused, "the verdict must not claim a refusal the native DDL path completes")
			assert.Empty(t, reason)

			runner, err := spiritmigration.NewRunner(&spiritmigration.Migration{
				Host:      host,
				Username:  username,
				Password:  &password,
				Database:  database,
				Statement: tt.stmt,
			})
			require.NoError(t, err, "NewRunner")
			defer utils.CloseAndLog(runner)

			runCtx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			require.NoError(t, runner.Run(runCtx), "the engine must apply a statement the verdict left silent")

			// Restore the starting shape so the next case sees the same table.
			_, err = db.ExecContext(t.Context(), "DROP TABLE `verdict_fastpath`")
			require.NoError(t, err, "drop verdict_fastpath")
			_, err = db.ExecContext(t.Context(), `CREATE TABLE verdict_fastpath (
				id INT NOT NULL AUTO_INCREMENT,
				email VARCHAR(255) NOT NULL,
				PRIMARY KEY (id)
			)`)
			require.NoError(t, err, "recreate verdict_fastpath")
		})
	}
}

func TestEngine_Plan_NoChanges(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db) // Start with clean database

	// Create table - use simple column types without AUTO_INCREMENT
	// to avoid MySQL's SHOW CREATE TABLE formatting differences
	_, err := db.ExecContext(t.Context(), `CREATE TABLE orders (
		id INT NOT NULL,
		status VARCHAR(50) NOT NULL,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	// Plan with same schema (using same format as MySQL's SHOW CREATE TABLE output)
	result, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"orders.sql": `CREATE TABLE orders (
				id INT NOT NULL,
				status VARCHAR(50) NOT NULL,
				PRIMARY KEY (id)
			)`,
		}),
		Credentials: &engine.Credentials{
			DSN: dsn,
		},
	})
	require.NoError(t, err, "Plan()")

	assert.True(t, result.NoChanges, "expected NoChanges, got DDL: %v", result.FlatDDL())
}

func TestEngine_Plan_NewTable(t *testing.T) {
	dsn, _ := setupTestMySQL(t)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	// Plan with new table (database is empty)
	result, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"accounts.sql": `CREATE TABLE accounts (
				id INT PRIMARY KEY AUTO_INCREMENT,
				name VARCHAR(100) NOT NULL,
				balance DECIMAL(10,2) DEFAULT 0
			)`,
		}),
		Credentials: &engine.Credentials{
			DSN: dsn,
		},
	})
	require.NoError(t, err, "Plan()")

	require.False(t, result.NoChanges, "expected changes for new table, got NoChanges")
	require.NotEmpty(t, result.FlatDDL(), "expected DDL statements for new table")

	// Verify it's a CREATE TABLE
	found := false
	for _, ddl := range result.FlatDDL() {
		t.Logf("DDL: %s", ddl)
		if containsCreate(ddl) {
			found = true
			break
		}
	}
	assert.True(t, found, "expected CREATE TABLE statement, got: %v", result.FlatDDL())
}

func TestEngine_Plan_LintViolationMapping(t *testing.T) {
	dsn, _ := setupTestMySQL(t)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	// Plan with a TIMESTAMP column which triggers the Y2038 overflow linter
	result, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"events.sql": `CREATE TABLE events (
				id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`,
		}),
		Credentials: &engine.Credentials{
			DSN: dsn,
		},
	})
	require.NoError(t, err, "Plan()")
	require.False(t, result.NoChanges)

	// Verify lint violations are populated with correct fields
	require.NotEmpty(t, result.LintViolations, "expected lint violations for TIMESTAMP column")

	var found bool
	for _, w := range result.LintViolations {
		if w.Table == "events" && strings.Contains(w.Message, "TIMESTAMP") {
			found = true
			assert.NotEmpty(t, w.Linter, "Linter name should be populated")
			assert.Contains(t, []string{"error", "warning", "info"}, w.Severity,
				"Severity should be a normalized lowercase string")
			break
		}
	}
	assert.True(t, found, "expected a TIMESTAMP-related lint warning for 'events' table, got: %v", result.LintViolations)
}

func TestEngine_Plan_MissingCredentials(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	_, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"users.sql": "CREATE TABLE users (id INT PRIMARY KEY)",
		}),
		Credentials: nil,
	})
	require.Error(t, err, "expected error for missing credentials")
}

func TestEngine_Name(t *testing.T) {
	eng := New(Config{})
	assert.Equal(t, "spirit", eng.Name())
}

func TestEngine_Plan_EmptyDSN(t *testing.T) {
	eng := New(Config{})

	_, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database:    "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{"users.sql": "CREATE TABLE users (id INT)"}),
		Credentials: &engine.Credentials{DSN: ""},
	})
	require.Error(t, err, "expected error for empty DSN")
}

func TestEngine_Apply_MissingCredentials(t *testing.T) {
	eng := New(Config{})

	_, err := eng.Apply(t.Context(), &engine.ApplyRequest{
		Database:    "testdb",
		Credentials: nil,
	})
	require.Error(t, err, "expected error for missing credentials")
}

func TestEngine_Apply_EmptyDSN(t *testing.T) {
	eng := New(Config{})

	_, err := eng.Apply(t.Context(), &engine.ApplyRequest{
		Database:    "testdb",
		Credentials: &engine.Credentials{DSN: ""},
	})
	require.Error(t, err, "expected error for empty DSN")
}

func TestEngine_Progress_NoMigration(t *testing.T) {
	eng := New(Config{})

	result, err := eng.Progress(t.Context(), &engine.ProgressRequest{})
	require.NoError(t, err, "Progress()")

	assert.Equal(t, engine.StatePending, result.State)
	assert.Equal(t, "No active schema change", result.Message)
}

func TestEngine_Progress_WithMigration(t *testing.T) {
	eng := New(Config{})
	eng.runningSchemaChange = &runningSchemaChange{
		database: "testdb",
		tables:   []string{"users"},
		state:    engine.StateRunning,
		// Note: Progress message comes from runners[0].Progress() when available.
		// Without runners, falls back to "Schema change <state>" message.
	}

	result, err := eng.Progress(t.Context(), &engine.ProgressRequest{
		ResumeState: &engine.ResumeState{MigrationContext: "test"},
	})
	require.NoError(t, err, "Progress()")

	assert.Equal(t, engine.StateRunning, result.State)
	// Without a real runner, message falls back to "Schema change <state>"
	assert.Equal(t, "Schema change running", result.Message)
}

func TestEngine_Stop_NoMigration(t *testing.T) {
	eng := New(Config{})

	_, err := eng.Stop(t.Context(), &engine.ControlRequest{})
	require.Error(t, err, "expected error for stop with no active schema change")
}

func TestEngine_Stop_WithMigration(t *testing.T) {
	eng := New(Config{})
	eng.runningSchemaChange = &runningSchemaChange{
		database: "testdb",
		state:    engine.StateRunning,
	}

	result, err := eng.Stop(t.Context(), &engine.ControlRequest{
		ResumeState: &engine.ResumeState{MigrationContext: "test"},
	})
	require.NoError(t, err, "Stop()")

	assert.True(t, result.Accepted, "expected Accepted to be true")
	assert.Equal(t, engine.StateStopped, eng.runningSchemaChange.state)
}

func TestEngine_Start_NotSupported(t *testing.T) {
	eng := New(Config{})

	_, err := eng.Start(t.Context(), &engine.ControlRequest{})
	require.Error(t, err, "expected error for start")
}

func TestEngine_Cutover_NoActiveMigration(t *testing.T) {
	eng := New(Config{})

	_, err := eng.Cutover(t.Context(), &engine.ControlRequest{
		ResumeState: &engine.ResumeState{MigrationContext: "test"},
	})
	require.Error(t, err, "expected error for cutover without active schema change")
	assert.Contains(t, err.Error(), "DSN credentials required")
}

func TestEngine_Cutover_NoActiveChangeWithCredentialsAttemptsStatelessSignal(t *testing.T) {
	eng := New(Config{})

	_, err := eng.Cutover(t.Context(), &engine.ControlRequest{
		Database: "testdb",
		Credentials: &engine.Credentials{
			DSN: "root@tcp(127.0.0.1:1)/testdb",
		},
		ResumeState: &engine.ResumeState{MigrationContext: "test"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ping database for cutover")
}

func TestEngine_Revert_NotSupported(t *testing.T) {
	eng := New(Config{})

	_, err := eng.Revert(t.Context(), &engine.ControlRequest{})
	require.Error(t, err, "expected error for revert")
}

func TestEngine_SkipRevert_NotSupported(t *testing.T) {
	eng := New(Config{})

	_, err := eng.SkipRevert(t.Context(), &engine.ControlRequest{})
	require.Error(t, err, "expected error for skip revert")
}

func TestNew_Defaults(t *testing.T) {
	eng := New(Config{})

	assert.NotNil(t, eng.logger, "expected logger to be set")
	assert.NotNil(t, eng.linter, "expected linter to be set")
	assert.Equal(t, DefaultThreads, eng.threads)
	assert.Equal(t, DefaultLockWaitTimeout, eng.lockWaitTimeout)
	// Pin the literal value so a change to the constant itself is caught here,
	// not just propagation from Config into the engine.
	assert.Equal(t, 10*time.Second, eng.lockWaitTimeout)
}

func TestNew_CustomConfig(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	eng := New(Config{
		Logger:          logger,
		Threads:         8,
		LockWaitTimeout: DefaultLockWaitTimeout * 2,
	})

	assert.Equal(t, logger, eng.logger, "expected custom logger")
	assert.Equal(t, 8, eng.threads)
	assert.Equal(t, DefaultLockWaitTimeout*2, eng.lockWaitTimeout, "expected custom lock wait timeout to override the default")
}

func TestSetSchemaChangeCompleted(t *testing.T) {
	eng := New(Config{})

	// No running schema change - should not panic
	eng.setSchemaChangeCompleted()

	// With running schema change
	eng.runningSchemaChange = &runningSchemaChange{
		state: engine.StateRunning,
	}
	eng.setSchemaChangeCompleted()

	assert.Equal(t, engine.StateCompleted, eng.runningSchemaChange.state)
}

func TestParseDSN_Valid(t *testing.T) {
	tests := []struct {
		name     string
		dsn      string
		wantHost string
		wantUser string
		wantPass string
		wantDB   string
	}{
		{
			name:     "full DSN",
			dsn:      "root:password@tcp(localhost:3306)/testdb",
			wantHost: "localhost:3306",
			wantUser: "root",
			wantPass: "password",
			wantDB:   "testdb",
		},
		{
			name:     "DSN with query params",
			dsn:      "user:pass@tcp(host:3306)/db?parseTime=true",
			wantHost: "host:3306",
			wantUser: "user",
			wantPass: "pass",
			wantDB:   "db",
		},
		{
			name:     "no password",
			dsn:      "user@tcp(host:3306)/db",
			wantHost: "host:3306",
			wantUser: "user",
			wantPass: "",
			wantDB:   "db",
		},
		{
			name:     "no database",
			dsn:      "user:pass@tcp(host:3306)/",
			wantHost: "host:3306",
			wantUser: "user",
			wantPass: "pass",
			wantDB:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, user, pass, db, err := parseDSN(tt.dsn)
			require.NoError(t, err, "parseDSN()")
			assert.Equal(t, tt.wantHost, host, "host")
			assert.Equal(t, tt.wantUser, user, "user")
			assert.Equal(t, tt.wantPass, pass, "pass")
			assert.Equal(t, tt.wantDB, db, "db")
		})
	}
}

func TestParseDSN_Invalid(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
	}{
		{"no @", "user:pass"},
		{"no tcp()", "user:pass@localhost:3306/db"},
		{"no closing paren", "user:pass@tcp(localhost:3306/db"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, _, err := parseDSN(tt.dsn)
			assert.Error(t, err, "expected error")
		})
	}
}

func TestEngine_FetchCurrentSchema(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db) // Start with clean database

	// Create some tables
	_, err := db.ExecContext(t.Context(), `CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))`)
	require.NoError(t, err, "create t1")
	_, err = db.ExecContext(t.Context(), `CREATE TABLE t2 (id INT PRIMARY KEY, value INT)`)
	require.NoError(t, err, "create t2")
	_, err = db.ExecContext(t.Context(), `CREATE TABLE t1_archive_2026_06 (id INT PRIMARY KEY, name VARCHAR(100))`)
	require.NoError(t, err, "create archive table")
	_, err = db.ExecContext(t.Context(), `CREATE TABLE _spirit_t1_ghost (id INT PRIMARY KEY, name VARCHAR(100))`)
	require.NoError(t, err, "create internal table")

	eng := New(Config{})
	schemas, err := eng.fetchCurrentSchema(t.Context(), dsn, "testdb")
	require.NoError(t, err, "fetchCurrentSchema()")

	assert.Len(t, schemas, 2)
	assert.ElementsMatch(t, []string{"t1", "t2"}, tableSchemaNames(schemas))
}

// A cancel that reaches a still-running schema change must clear the
// resumability artifacts so a later apply starts cleanly, while preserving the
// user's live base table. It disposes of them under the same policy as a cancel
// that finds no runner alive: the copies are preserved in the quarantine, the
// metadata describing them is dropped.
func TestEngine_CancelledArtifactCleanup(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	baseTable := "customers"
	releaseTestCleanup(t, db, baseTable)
	copies := []string{
		utils.NewTableName(baseTable),
		utils.OldTableName(baseTable),
	}
	metadata := []string{
		utils.CheckpointTableName(baseTable),
		"_spirit_sentinel",
		"_spirit_checkpoint",
	}

	_, err := db.ExecContext(t.Context(), fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", quoteIdentifier(baseTable)))
	require.NoError(t, err)
	for _, artifact := range append(copies, metadata...) {
		_, err := db.ExecContext(t.Context(), fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", quoteIdentifier(artifact)))
		require.NoError(t, err, "create artifact %s", artifact)
	}

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err)
	eng := New(Config{})
	err = eng.dropCancelledArtifacts(t.Context(), &runningSchemaChange{
		database: database,
		tables:   []string{baseTable},
		host:     host,
		username: username,
		password: password,
	})
	require.NoError(t, err)

	assert.True(t, tableExists(t, db, baseTable))
	for _, artifact := range append(copies, metadata...) {
		assert.False(t, tableExists(t, db, artifact), "artifact should leave the target: %s", artifact)
	}
	assert.Len(t, listQuarantinedTables(t, db), len(copies),
		"the copies must be preserved rather than dropped")
}

// Archive tables are maintained outside declarative schema files, so a plan
// must not propose dropping live archive tables that are absent from Git.
func TestEngine_Plan_IgnoresArchiveTables(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	_, err := db.ExecContext(t.Context(), `CREATE TABLE executions (id INT PRIMARY KEY, name VARCHAR(100))`)
	require.NoError(t, err, "create executions")
	_, err = db.ExecContext(t.Context(), `CREATE TABLE executions_archive_2026_06 (id INT PRIMARY KEY, name VARCHAR(100))`)
	require.NoError(t, err, "create archive table")

	eng := New(Config{})
	result, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"executions.sql": `CREATE TABLE executions (id INT PRIMARY KEY, name VARCHAR(100))`,
		}),
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err, "Plan()")
	require.NotNil(t, result)

	assert.True(t, result.NoChanges, "archive tables must not create DROP TABLE plan entries: %v", result.FlatDDL())
}

func TestEngine_Apply_NoChanges(t *testing.T) {
	dsn, _ := setupTestMySQL(t)

	// Empty database - Apply will re-plan with no SchemaFiles,
	// see no tables in DB, and return NoChanges
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	// Apply with empty database and no schema files - should return "No changes to apply"
	result, err := eng.Apply(t.Context(), &engine.ApplyRequest{
		Database: "testdb",
		Credentials: &engine.Credentials{
			DSN: dsn,
		},
	})
	require.NoError(t, err, "Apply()")

	assert.True(t, result.Accepted, "expected Accepted to be true")
	assert.Equal(t, "No changes to apply", result.Message)
}

func TestEngine_Apply_WithChanges(t *testing.T) {
	dsn, db := setupTestMySQL(t)

	// Create initial table
	_, err := db.ExecContext(t.Context(), `CREATE TABLE items (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(100) NOT NULL
	)`)
	require.NoError(t, err, "create table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	// Store desired schema in engine's plan cache by calling Plan first
	// (Apply needs to re-plan since it doesn't have schema files)
	// Note: This tests the "changes detected" path of Apply

	// First, let's see what Plan would produce
	planResult, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"items.sql": `CREATE TABLE items (
				id INT PRIMARY KEY AUTO_INCREMENT,
				name VARCHAR(100) NOT NULL,
				price DECIMAL(10,2) NULL
			)`,
		}),
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err, "Plan()")

	require.False(t, planResult.NoChanges, "expected changes from plan")

	t.Logf("Plan DDL: %v", planResult.FlatDDL())

	// Apply would need schema files passed through, but our current implementation
	// re-runs Plan without them. This is a known limitation.
	// For now, test that Apply detects the issue correctly when called without Plan context.
}

func TestEngine_Progress_WithRunners(t *testing.T) {
	eng := New(Config{})
	eng.runningSchemaChange = &runningSchemaChange{
		database: "testdb",
		tables:   []string{"users", "orders"},
		state:    engine.StateRunning,
		runners:  nil, // No actual runners, just testing the path
	}

	result, err := eng.Progress(t.Context(), &engine.ProgressRequest{})
	require.NoError(t, err, "Progress()")

	assert.Equal(t, engine.StateRunning, result.State)
}

func TestEngine_Progress_NamespaceFromApplyChanges(t *testing.T) {
	// Verify that TableProgress.Namespace is set from ApplyRequest.Changes,
	// not left empty. Without this, the progress key lookup in
	// syncAtomicTaskProgress fails silently (task has namespace="orders",
	// engine returns namespace=""), and row progress is never persisted.
	eng := New(Config{})
	eng.runningSchemaChange = &runningSchemaChange{
		database:       "orders",
		tableNamespace: map[string]string{"orders": "orders", "users": "myapp"},
		tables:         []string{"orders", "users"},
		ddls:           []string{"ALTER TABLE orders ADD INDEX idx_status (status)", "ALTER TABLE users ADD COLUMN x INT"},
		state:          engine.StateRunning,
		runners:        nil, // No actual runners — testing the fallback path
	}

	result, err := eng.Progress(t.Context(), &engine.ProgressRequest{})
	require.NoError(t, err)
	require.Len(t, result.Tables, 2)

	// Each table should have the correct namespace from tableNamespace map
	for _, tp := range result.Tables {
		switch tp.Table {
		case "orders":
			assert.Equal(t, "orders", tp.Namespace, "orders table should have namespace 'orders'")
		case "users":
			assert.Equal(t, "myapp", tp.Namespace, "users table should have namespace 'myapp'")
		default:
			t.Fatalf("unexpected table: %s", tp.Table)
		}
	}
}

// TestEngine_Progress_ClosedRunnerIsNotCompletion verifies that a progress
// poll observing a runner in teardown (Spirit status "close") reports the
// tracked state instead of inferring terminal success. Terminal outcomes are
// recorded before the runner is closed, so a closing runner alongside a
// non-terminal tracked state means the apply is still in flight.
func TestEngine_Progress_ClosedRunnerIsNotCompletion(t *testing.T) {
	host, username, password, database, err := parseDSN(sharedDSN)
	require.NoError(t, err, "parseDSN")

	runner, err := spiritmigration.NewRunner(&spiritmigration.Migration{
		Host:      host,
		Username:  username,
		Password:  &password,
		Database:  database,
		Statement: "ALTER TABLE `progress_close` ADD COLUMN `email` varchar(255) NULL",
	})
	require.NoError(t, err, "NewRunner")
	require.NoError(t, runner.Close(), "close runner")

	t.Run("running state keeps reporting running", func(t *testing.T) {
		eng := New(Config{})
		eng.runningSchemaChange = &runningSchemaChange{
			database: database,
			tables:   []string{"progress_close"},
			state:    engine.StateRunning,
			runners:  []*spiritmigration.Runner{runner},
		}

		result, err := eng.Progress(t.Context(), &engine.ProgressRequest{})
		require.NoError(t, err, "Progress()")
		assert.Equal(t, engine.StateRunning, result.State)
	})

	t.Run("failed state keeps reporting failed", func(t *testing.T) {
		eng := New(Config{})
		eng.runningSchemaChange = &runningSchemaChange{
			database:     database,
			tables:       []string{"progress_close"},
			state:        engine.StateFailed,
			errorMessage: "schema change failed: ddl error",
			runners:      []*spiritmigration.Runner{runner},
		}

		result, err := eng.Progress(t.Context(), &engine.ProgressRequest{})
		require.NoError(t, err, "Progress()")
		assert.Equal(t, engine.StateFailed, result.State)
		assert.Equal(t, "schema change failed: ddl error", result.ErrorMessage)
		assert.True(t, result.Retryable)
	})

	t.Run("completed state keeps reporting completed", func(t *testing.T) {
		eng := New(Config{})
		eng.runningSchemaChange = &runningSchemaChange{
			database:     database,
			tables:       []string{"progress_close"},
			state:        engine.StateCompleted,
			deferCutover: true,
			runners:      []*spiritmigration.Runner{runner},
		}

		result, err := eng.Progress(t.Context(), &engine.ProgressRequest{})
		require.NoError(t, err, "Progress()")
		assert.Equal(t, engine.StateCompleted, result.State)
	})
}

func TestEngine_FetchCurrentSchema_EmptyDatabase(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db) // Start with clean database

	eng := New(Config{})
	schemas, err := eng.fetchCurrentSchema(t.Context(), dsn, "testdb")
	require.NoError(t, err, "fetchCurrentSchema()")

	assert.Empty(t, schemas, "expected 0 tables for empty database")
}

// TestEngine_ExecuteMigration_AddColumn tests running an actual Spirit schema change
// that adds a column to an existing table.
func TestEngine_ExecuteMigration_AddColumn(t *testing.T) {
	dsn, db := setupTestMySQL(t)

	// Create initial table with some data
	_, err := db.ExecContext(t.Context(), `CREATE TABLE test_migrate (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(100) NOT NULL
	)`)
	require.NoError(t, err, "create table")

	// Insert some test data
	for i := range 10 {
		_, err := db.ExecContext(t.Context(), `INSERT INTO test_migrate (name) VALUES (?)`, fmt.Sprintf("test-%d", i))
		require.NoError(t, err, "insert data")
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	// Parse DSN to get connection info for Spirit
	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	// Run the schema change directly using executeSchemaChange
	ddlStatements := []string{
		"ALTER TABLE `test_migrate` ADD COLUMN `email` varchar(255) NULL",
	}

	// Set up running schema change state
	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{
		database: database,
		tables:   []string{"test_migrate"},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	// Execute the schema change synchronously for testing
	eng.executeSchemaChange(t.Context(), host, username, password, database, ddlStatements, false, directPolicy{})

	// Check that schema change completed
	eng.mu.Lock()
	finalState := eng.runningSchemaChange.state
	eng.mu.Unlock()

	assert.Equal(t, engine.StateCompleted, finalState)

	// Verify the column was added
	var columnCount int
	err = db.QueryRowContext(t.Context(), `
		SELECT COUNT(*) FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = 'testdb'
		AND TABLE_NAME = 'test_migrate'
		AND COLUMN_NAME = 'email'
	`).Scan(&columnCount)
	require.NoError(t, err, "check column")
	assert.Equal(t, 1, columnCount, "expected email column to exist")

	// Verify data is still intact
	var rowCount int
	err = db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM test_migrate`).Scan(&rowCount)
	require.NoError(t, err, "count rows")
	assert.Equal(t, 10, rowCount)
}

// TestEngine_ExecuteMigration_ModifyColumn tests running a Spirit schema change
// that modifies a column type.
func TestEngine_ExecuteMigration_ModifyColumn(t *testing.T) {
	dsn, db := setupTestMySQL(t)

	// Create initial table
	_, err := db.ExecContext(t.Context(), `CREATE TABLE test_modify (
		id INT PRIMARY KEY AUTO_INCREMENT,
		status VARCHAR(50) NOT NULL
	)`)
	require.NoError(t, err, "create table")

	// Insert test data
	for range 5 {
		_, err := db.ExecContext(t.Context(), `INSERT INTO test_modify (status) VALUES (?)`, "active")
		require.NoError(t, err, "insert data")
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	// Modify column to be larger
	ddlStatements := []string{
		"ALTER TABLE `test_modify` MODIFY COLUMN `status` varchar(100) NOT NULL",
	}

	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{
		database: database,
		tables:   []string{"test_modify"},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	eng.executeSchemaChange(t.Context(), host, username, password, database, ddlStatements, false, directPolicy{})

	eng.mu.Lock()
	finalState := eng.runningSchemaChange.state
	eng.mu.Unlock()

	assert.Equal(t, engine.StateCompleted, finalState)

	// Verify the column was modified
	var charMaxLen int
	err = db.QueryRowContext(t.Context(), `
		SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = 'testdb'
		AND TABLE_NAME = 'test_modify'
		AND COLUMN_NAME = 'status'
	`).Scan(&charMaxLen)
	require.NoError(t, err, "check column")
	assert.Equal(t, 100, charMaxLen, "expected status column to be varchar(100)")
}

// TestEngine_ExecuteMigration_DropColumn tests running a Spirit schema change
// that drops a column.
func TestEngine_ExecuteMigration_DropColumn(t *testing.T) {
	dsn, db := setupTestMySQL(t)

	// Create initial table with extra column
	_, err := db.ExecContext(t.Context(), `CREATE TABLE test_drop (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(100) NOT NULL,
		deprecated_col VARCHAR(50)
	)`)
	require.NoError(t, err, "create table")

	// Insert test data
	_, err = db.ExecContext(t.Context(), `INSERT INTO test_drop (name, deprecated_col) VALUES ('test', 'old')`)
	require.NoError(t, err, "insert data")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	ddlStatements := []string{
		"ALTER TABLE `test_drop` DROP COLUMN `deprecated_col`",
	}

	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{
		database: database,
		tables:   []string{"test_drop"},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	eng.executeSchemaChange(t.Context(), host, username, password, database, ddlStatements, false, directPolicy{})

	eng.mu.Lock()
	finalState := eng.runningSchemaChange.state
	eng.mu.Unlock()

	assert.Equal(t, engine.StateCompleted, finalState)

	// Verify the column was dropped
	var columnCount int
	err = db.QueryRowContext(t.Context(), `
		SELECT COUNT(*) FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = 'testdb'
		AND TABLE_NAME = 'test_drop'
		AND COLUMN_NAME = 'deprecated_col'
	`).Scan(&columnCount)
	require.NoError(t, err, "check column")
	assert.Equal(t, 0, columnCount, "expected deprecated_col to be dropped")
}

// TestEngine_ExecuteMigration_AddIndex tests running a Spirit schema change
// that adds an index.
func TestEngine_ExecuteMigration_AddIndex(t *testing.T) {
	dsn, db := setupTestMySQL(t)

	// Create initial table
	_, err := db.ExecContext(t.Context(), `CREATE TABLE test_index (
		id INT PRIMARY KEY AUTO_INCREMENT,
		email VARCHAR(255) NOT NULL
	)`)
	require.NoError(t, err, "create table")

	// Insert test data
	for i := range 5 {
		_, err := db.ExecContext(t.Context(), `INSERT INTO test_index (email) VALUES (?)`, fmt.Sprintf("user%d@example.com", i))
		require.NoError(t, err, "insert data")
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	ddlStatements := []string{
		"ALTER TABLE `test_index` ADD INDEX `idx_email` (`email`)",
	}

	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{
		database: database,
		tables:   []string{"test_index"},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	eng.executeSchemaChange(t.Context(), host, username, password, database, ddlStatements, false, directPolicy{})

	eng.mu.Lock()
	finalState := eng.runningSchemaChange.state
	eng.mu.Unlock()

	assert.Equal(t, engine.StateCompleted, finalState)

	// Verify the index was added
	var indexCount int
	err = db.QueryRowContext(t.Context(), `
		SELECT COUNT(*) FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = 'testdb'
		AND TABLE_NAME = 'test_index'
		AND INDEX_NAME = 'idx_email'
	`).Scan(&indexCount)
	require.NoError(t, err, "check index")
	assert.NotZero(t, indexCount, "expected idx_email index to exist")
}

// TestEngine_ExecuteMigration_InvalidSQL tests that executeSchemaChange handles
// invalid SQL gracefully by setting state to Failed.
func TestEngine_ExecuteMigration_InvalidSQL(t *testing.T) {
	dsn, db := setupTestMySQL(t)

	// Create a table
	_, err := db.ExecContext(t.Context(), `CREATE TABLE test_invalid (id INT PRIMARY KEY)`)
	require.NoError(t, err, "create table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	eng := New(Config{Logger: logger})

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	// Invalid SQL - column doesn't exist
	ddlStatements := []string{
		"ALTER TABLE `test_invalid` DROP COLUMN `nonexistent_column`",
	}

	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{
		database: database,
		tables:   []string{"test_invalid"},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	eng.executeSchemaChange(t.Context(), host, username, password, database, ddlStatements, false, directPolicy{})

	eng.mu.Lock()
	finalState := eng.runningSchemaChange.state
	eng.mu.Unlock()

	assert.Equal(t, engine.StateFailed, finalState, "expected StateFailed for invalid SQL")
}

// A UNIQUE index over duplicate values fails checksum consistently, so the
// engine reports the runner failure as permanent instead of retrying it.
func TestEngine_ChecksumDifferencesArePermanent(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	_, err := db.ExecContext(t.Context(), `CREATE TABLE checksum_duplicates (
		id INT NOT NULL AUTO_INCREMENT,
		duplicate_value INT NOT NULL,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err, "create checksum_duplicates table")
	_, err = db.ExecContext(t.Context(), "INSERT INTO `checksum_duplicates` (`duplicate_value`) VALUES (1), (1)")
	require.NoError(t, err, "insert duplicate values")

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")
	eng := New(Config{Logger: discardLogger()})

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	err = eng.executeSpiritMigration(ctx, host, username, password, database,
		"ALTER TABLE `checksum_duplicates` ADD UNIQUE KEY `uq_duplicate_value` (`duplicate_value`)", false)

	require.Error(t, err)
	assert.False(t, engine.IsRetryable(err))
	assert.ErrorIs(t, err, checksum.ErrDifferencesExhausted)
}

// TestEngine_Progress_FailingApplyNeverReportsCompleted verifies that a
// schema change which fails against the database is never observable as
// completed: progress polls taken at any point during the apply — including
// while the failed runner is torn down — report running and then failed.
func TestEngine_Progress_FailingApplyNeverReportsCompleted(t *testing.T) {
	dsn, db := setupTestMySQL(t)

	_, err := db.ExecContext(t.Context(), `CREATE TABLE fail_progress (id INT PRIMARY KEY)`)
	require.NoError(t, err, "create table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	eng := New(Config{Logger: logger})

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	ddlStatements := []string{
		"ALTER TABLE `fail_progress` DROP COLUMN `nonexistent_column`",
	}

	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{
		database: database,
		tables:   []string{"fail_progress"},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	ctx := t.Context()
	done := make(chan struct{})
	go func() {
		defer close(done)
		eng.executeSchemaChange(ctx, host, username, password, database, ddlStatements, false, directPolicy{})
	}()

	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for applyFinished := false; !applyFinished; {
		select {
		case <-done:
			applyFinished = true
		case <-deadline.C:
			t.Fatal("timed out waiting for the schema change to fail")
		case <-ticker.C:
		}
		result, err := eng.Progress(t.Context(), &engine.ProgressRequest{})
		require.NoError(t, err, "Progress()")
		assert.NotEqual(t, engine.StateCompleted, result.State,
			"failing schema change reported terminal success")
	}

	result, err := eng.Progress(t.Context(), &engine.ProgressRequest{})
	require.NoError(t, err, "Progress()")
	assert.Equal(t, engine.StateFailed, result.State)
	// The target says which column is missing by quoting the statement, and
	// what reaches the pull request is SchemaBot's account of the error code.
	assert.Equal(t, mysqlerr.ReasonFromText("(errno 1091)"), result.ErrorMessage)
	assert.NotContains(t, result.ErrorMessage, "nonexistent_column")
	assert.True(t, result.Retryable)
}

// TestEngine_ExecuteMigration_MultipleStatements tests running multiple
// DDL statements in sequence on different tables.
// Note: Spirit doesn't support multiple DDL statements on the same table
// in a single schema change due to binlog subscription conflicts.
func TestEngine_ExecuteMigration_MultipleStatements(t *testing.T) {
	dsn, db := setupTestMySQL(t)

	// Create two initial tables for multi-table schema change
	_, err := db.ExecContext(t.Context(), `CREATE TABLE test_multi_a (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(50) NOT NULL
	)`)
	require.NoError(t, err, "create table a")

	_, err = db.ExecContext(t.Context(), `CREATE TABLE test_multi_b (
		id INT PRIMARY KEY AUTO_INCREMENT,
		title VARCHAR(100) NOT NULL
	)`)
	require.NoError(t, err, "create table b")

	// Insert test data
	_, err = db.ExecContext(t.Context(), `INSERT INTO test_multi_a (name) VALUES ('test')`)
	require.NoError(t, err, "insert data a")
	_, err = db.ExecContext(t.Context(), `INSERT INTO test_multi_b (title) VALUES ('test')`)
	require.NoError(t, err, "insert data b")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	// Multiple DDL statements on different tables
	ddlStatements := []string{
		"ALTER TABLE `test_multi_a` ADD COLUMN `email` varchar(255) NULL",
		"ALTER TABLE `test_multi_b` ADD COLUMN `description` varchar(500) NULL",
	}

	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{
		database: database,
		tables:   []string{"test_multi_a", "test_multi_b"},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	eng.executeSchemaChange(t.Context(), host, username, password, database, ddlStatements, false, directPolicy{})

	eng.mu.Lock()
	finalState := eng.runningSchemaChange.state
	eng.mu.Unlock()

	assert.Equal(t, engine.StateCompleted, finalState)

	// Verify columns were added to both tables
	var columnCountA int
	err = db.QueryRowContext(t.Context(), `
		SELECT COUNT(*) FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = 'testdb'
		AND TABLE_NAME = 'test_multi_a'
		AND COLUMN_NAME = 'email'
	`).Scan(&columnCountA)
	require.NoError(t, err, "check column a")
	assert.Equal(t, 1, columnCountA, "expected 1 new column in test_multi_a")

	var columnCountB int
	err = db.QueryRowContext(t.Context(), `
		SELECT COUNT(*) FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = 'testdb'
		AND TABLE_NAME = 'test_multi_b'
		AND COLUMN_NAME = 'description'
	`).Scan(&columnCountB)
	require.NoError(t, err, "check column b")
	assert.Equal(t, 1, columnCountB, "expected 1 new column in test_multi_b")
}

// threadsConnected reports MySQL's current server-side connection count, used to
// observe whether the Spirit runner driving a single DDL statement releases its
// connection pool once the statement finishes.
func threadsConnected(t *testing.T, db *sql.DB) int {
	t.Helper()
	var name string
	var value int
	require.NoError(t, db.QueryRowContext(t.Context(), "SHOW STATUS LIKE 'Threads_connected'").Scan(&name, &value), "read Threads_connected")
	return value
}

// TestEngine_ExecuteMigration_SingleStatementReleasesConnections applies many
// CREATE TABLE and direct DROP TABLE statements in sequence on one engine. Each
// statement runs through Spirit's single-statement path, which opens a connection
// pool and background routines per runner that only the runner's Close releases.
// Every statement completes and the server connection count stays bounded across
// the whole sequence, so a long run of single-statement applies neither leaks
// connections nor exhausts the server connection limit.
func TestEngine_ExecuteMigration_SingleStatementReleasesConnections(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger, DisablePendingDrops: true})

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	// Record a stable baseline once connection churn from setup has settled, so
	// the post-sequence comparison reflects only what the applies leave behind.
	var baseline int
	require.Eventually(t, func() bool {
		first := threadsConnected(t, db)
		time.Sleep(200 * time.Millisecond)
		second := threadsConnected(t, db)
		if first == second {
			baseline = second
			return true
		}
		return false
	}, 10*time.Second, 200*time.Millisecond, "wait for connections to settle")

	const iterations = 12
	for i := range iterations {
		table := fmt.Sprintf("seq_table_%d", i)
		createDDL := fmt.Sprintf("CREATE TABLE `%s` (`id` bigint unsigned NOT NULL AUTO_INCREMENT, `name` varchar(100) NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", table)

		eng.mu.Lock()
		eng.runningSchemaChange = &runningSchemaChange{
			database: database,
			tables:   []string{table},
			state:    engine.StateRunning,
			started:  time.Now(),
		}
		eng.mu.Unlock()

		eng.executeSchemaChange(t.Context(), host, username, password, database, []string{createDDL}, false, directPolicy{})

		eng.mu.Lock()
		createState := eng.runningSchemaChange.state
		eng.mu.Unlock()
		require.Equal(t, engine.StateCompleted, createState, "CREATE TABLE %s did not complete", table)

		var afterCreate int
		require.NoError(t, db.QueryRowContext(t.Context(), `
			SELECT COUNT(*) FROM information_schema.TABLES
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		`, database, table).Scan(&afterCreate), "check table exists after CREATE")
		require.Equal(t, 1, afterCreate, "expected table %s to exist after CREATE", table)

		dropDDL := fmt.Sprintf("DROP TABLE `%s`", table)

		eng.mu.Lock()
		eng.runningSchemaChange = &runningSchemaChange{
			database: database,
			tables:   []string{table},
			state:    engine.StateRunning,
			started:  time.Now(),
		}
		eng.mu.Unlock()

		eng.executeSchemaChange(t.Context(), host, username, password, database, []string{dropDDL}, false, directPolicy{})

		eng.mu.Lock()
		dropState := eng.runningSchemaChange.state
		eng.mu.Unlock()
		require.Equal(t, engine.StateCompleted, dropState, "DROP TABLE %s did not complete", table)

		var afterDrop int
		require.NoError(t, db.QueryRowContext(t.Context(), `
			SELECT COUNT(*) FROM information_schema.TABLES
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		`, database, table).Scan(&afterDrop), "check table dropped")
		require.Equal(t, 0, afterDrop, "expected table %s to be dropped", table)
	}

	// A leaked pool would hold open roughly one connection per runner, so 24
	// applies would push the count far above the baseline. Once the runners are
	// closed the count returns near the baseline; allow a small margin for the
	// server's own background churn.
	var settled int
	require.Eventually(t, func() bool {
		settled = threadsConnected(t, db)
		return settled <= baseline+5
	}, 15*time.Second, 250*time.Millisecond, "connections did not return near baseline (baseline=%d)", baseline)
	assert.LessOrEqual(t, settled, baseline+5,
		"server connections grew far beyond baseline after %d single-statement applies (baseline=%d, settled=%d)",
		iterations*2, baseline, settled)
}

// TestEngine_ExecuteSchemaChange_SingleStatementRoutesSpiritLogs runs a CREATE
// TABLE through Spirit's single-statement path with the engine's log callback
// registered. Spirit's INFO log lines for the run are routed through the
// callback — the same path that feeds operator-visible apply logs — so
// single-statement applies (CREATE/DROP/RENAME) are traceable in the apply
// log stream just like ALTERs.
func TestEngine_ExecuteSchemaChange_SingleStatementRoutesSpiritLogs(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger, DisablePendingDrops: true})

	var mu sync.Mutex
	var captured []string
	capturedTables := make(map[string]bool)
	eng.SetLogCallback(func(level slog.Level, table, msg string) {
		mu.Lock()
		defer mu.Unlock()
		captured = append(captured, msg)
		capturedTables[table] = true
	})

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	createDDL := "CREATE TABLE `log_routed` (`id` bigint unsigned NOT NULL AUTO_INCREMENT, `name` varchar(100) NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{
		database: database,
		tables:   []string{"log_routed"},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	eng.executeSchemaChange(t.Context(), host, username, password, database, []string{createDDL}, false, directPolicy{})

	eng.mu.Lock()
	finalState := eng.runningSchemaChange.state
	eng.mu.Unlock()
	require.Equal(t, engine.StateCompleted, finalState, "CREATE TABLE did not complete")
	require.True(t, tableExists(t, db, "log_routed"), "expected log_routed to exist after CREATE")

	mu.Lock()
	defer mu.Unlock()
	assert.Contains(t, captured, "Starting spirit migration",
		"Spirit's run-start log line was not routed through the engine log callback")
	assert.Contains(t, captured, "apply complete",
		"Spirit's completion log line was not routed through the engine log callback")
	// Every routed line must carry the table so operators can attribute
	// interleaved log lines during multi-table applies.
	assert.Equal(t, map[string]bool{"log_routed": true}, capturedTables,
		"every routed Spirit log line should carry the table being changed")
}

// TestEngine_Apply_StartsGoroutine tests that Apply starts a schema change goroutine
// when there are changes to apply. We test this by checking that state transitions happen.
func TestEngine_Apply_StartsGoroutine(t *testing.T) {
	dsn, db := setupTestMySQL(t)

	// Create initial table
	_, err := db.ExecContext(t.Context(), `CREATE TABLE apply_test (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(100) NOT NULL
	)`)
	require.NoError(t, err, "create table")

	// Insert some data
	for i := range 5 {
		_, err := db.ExecContext(t.Context(), `INSERT INTO apply_test (name) VALUES (?)`, fmt.Sprintf("test-%d", i))
		require.NoError(t, err, "insert data")
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	// First call Plan to see what would change (for logging only)
	planResult, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"apply_test.sql": `CREATE TABLE apply_test (
				id INT PRIMARY KEY AUTO_INCREMENT,
				name VARCHAR(100) NOT NULL,
				created_at DATETIME DEFAULT CURRENT_TIMESTAMP
			)`,
		}),
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err, "Plan()")
	t.Logf("Plan result: NoChanges=%v, DDL=%v", planResult.NoChanges, planResult.FlatDDL())

	// Now test Apply - it will re-plan with empty SchemaFiles and see a table to drop
	// This tests the full Apply path including goroutine start
	result, err := eng.Apply(t.Context(), &engine.ApplyRequest{
		Database: "testdb",
		Credentials: &engine.Credentials{
			DSN: dsn,
		},
	})
	require.NoError(t, err, "Apply()")
	defer eng.Drain()

	assert.True(t, result.Accepted, "expected Accepted to be true")

	// Give the goroutine time to start
	time.Sleep(100 * time.Millisecond)

	// Check that a schema change was started
	eng.mu.Lock()
	hasRunningSchemaChange := eng.runningSchemaChange != nil
	eng.mu.Unlock()

	if !hasRunningSchemaChange {
		t.Log("Note: schema change may have completed very quickly")
	}
}

// TestEngine_Progress_WithProgressCallback tests Progress with a callback set
func TestEngine_Progress_WithNilCallback(t *testing.T) {
	eng := New(Config{})
	eng.runningSchemaChange = &runningSchemaChange{
		database:         "testdb",
		tables:           []string{"users"},
		state:            engine.StateRunning,
		progressCallback: nil, // No callback
	}

	result, err := eng.Progress(t.Context(), &engine.ProgressRequest{})
	require.NoError(t, err, "Progress()")

	// Should fall back to default message
	assert.Equal(t, engine.StateRunning, result.State)
}

// TestEngine_Progress_WithEmptyCallback tests Progress when callback returns empty
func TestEngine_Progress_WithEmptyCallback(t *testing.T) {
	eng := New(Config{})
	eng.runningSchemaChange = &runningSchemaChange{
		database: "testdb",
		tables:   []string{"users"},
		state:    engine.StateRunning,
		progressCallback: func() string {
			return "" // Empty summary
		},
	}

	result, err := eng.Progress(t.Context(), &engine.ProgressRequest{})
	require.NoError(t, err, "Progress()")

	// Should use default message when callback returns empty
	t.Logf("Message: %s", result.Message)
}

// TestEngine_FetchCurrentSchema_ConnectionError tests fetchCurrentSchema with bad DSN
func TestEngine_FetchCurrentSchema_ConnectionError(t *testing.T) {
	eng := New(Config{})

	// Use invalid DSN
	_, err := eng.fetchCurrentSchema(t.Context(), "invalid:invalid@tcp(localhost:9999)/nonexistent", "testdb")
	assert.Error(t, err, "expected error for invalid DSN")
}

// TestEngine_Plan_ConnectionError tests Plan with bad DSN
func TestEngine_Plan_ConnectionError(t *testing.T) {
	eng := New(Config{})

	_, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database:    "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{"users.sql": "CREATE TABLE users (id INT)"}),
		Credentials: &engine.Credentials{
			DSN: "invalid:invalid@tcp(localhost:9999)/nonexistent",
		},
	})
	assert.Error(t, err, "expected error for invalid DSN")
}

func TestEngine_ExecuteMigration_CancelledContextKeepsStoppedState(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	_, err := db.ExecContext(t.Context(), `CREATE TABLE stop_pending_drop (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(100) NOT NULL
	)`)
	require.NoError(t, err, "create table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{Logger: logger})

	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{
		database: database,
		tables:   []string{"stop_pending_drop"},
		state:    engine.StateStopped,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	ddlStatements := []string{
		"CREATE TABLE `stop_pending_create` (`id` INT PRIMARY KEY AUTO_INCREMENT) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		"DROP TABLE `stop_pending_drop`",
	}

	eng.executeSchemaChange(ctx, host, username, password, database, ddlStatements, false, directPolicy{})

	eng.mu.Lock()
	finalState := eng.runningSchemaChange.state
	eng.mu.Unlock()
	assert.Equal(t, engine.StateStopped, finalState, "cancelled context must leave state Stopped, not Failed/Completed")

	assert.False(t, testutil.TableExists(t, db, "testdb", "stop_pending_create"),
		"CREATE TABLE must not run after the context is cancelled")
	assert.True(t, testutil.TableExists(t, db, "testdb", "stop_pending_drop"),
		"DROP TABLE must not run after the context is cancelled")
}

// Stopping a running ALTER that has a DROP TABLE queued after it must leave the
// change in Stopped, never Failed/Completed: the post-ALTER DROP phase sees the
// cancelled context and must not overwrite the operator-set Stopped state. The
// change must then be resumable from its checkpoint.
func TestEngine_Stop_DuringAlterWithPendingDrop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long-running integration test in short mode")
	}

	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	_, err := db.ExecContext(t.Context(), `CREATE TABLE stop_alter_target (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(50) NOT NULL
	)`)
	require.NoError(t, err, "create alter target table")

	_, err = db.ExecContext(t.Context(), `CREATE TABLE stop_drop_target (
		id INT PRIMARY KEY AUTO_INCREMENT
	)`)
	require.NoError(t, err, "create drop target table")

	// Seed enough rows that the ALTER table copy is observably in-flight when we
	// stop it, so the stop lands during the ALTER phase before the DROP phase.
	seedTableRows(t, db, "stop_alter_target")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{
		Logger:  logger,
		Threads: 1,
	})

	ctx := t.Context()

	applyResult, err := eng.Apply(ctx, &engine.ApplyRequest{
		Database: "testdb",
		Changes: []engine.SchemaChange{
			{
				Namespace: "testdb",
				TableChanges: []engine.TableChange{
					{Table: "stop_alter_target", DDL: "ALTER TABLE `stop_alter_target` MODIFY COLUMN `name` varchar(100) NOT NULL"},
					{Table: "stop_drop_target", DDL: "DROP TABLE `stop_drop_target`"},
				},
			},
		},
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err, "Apply()")
	defer eng.Drain()
	require.True(t, applyResult.Accepted, "Apply not accepted: %s", applyResult.Message)

	// Wait until the ALTER copy is observably in-flight before stopping, so the
	// stop lands mid-ALTER (the DROP phase has not yet started).
	waitForCopyProgress(t, eng, 1)

	stopResult, err := eng.Stop(ctx, &engine.ControlRequest{Database: "testdb"})
	require.NoError(t, err, "Stop()")
	require.True(t, stopResult.Accepted, "Stop not accepted: %s", stopResult.Message)

	// Stop() waits for the goroutine to exit, so the final state is settled here.
	progress, err := eng.Progress(ctx, &engine.ProgressRequest{})
	require.NoError(t, err, "Progress() after stop")
	assert.Equal(t, engine.StateStopped, progress.State,
		"stopping mid-ALTER with a pending DROP must report Stopped, not %s", progress.State)

	// The pending DROP must not have run — its table must still exist.
	assert.True(t, testutil.TableExists(t, db, "testdb", "stop_drop_target"),
		"pending DROP must not run when the change is stopped mid-ALTER")

	// Resuming a stopped change must finish the entire plan from its checkpoint:
	// complete the ALTER and then run the queued DROP phase. The default DROP
	// behavior quarantines the table into the pending drops database rather than
	// dropping it, so a resume that only finishes the ALTER and skips the DROP
	// phase would leave the table unquarantined. The test waits for completion and
	// verifies both effects.
	startResult, err := eng.Start(ctx, &engine.ControlRequest{
		Database:    "testdb",
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err, "Start() must permit resume of a stopped change")
	require.True(t, startResult.Accepted, "resume not accepted: %s", startResult.Message)

	resumeDeadline := time.Now().Add(5 * time.Minute)
	var finalState engine.State
	for time.Now().Before(resumeDeadline) {
		progress, perr := eng.Progress(ctx, &engine.ProgressRequest{})
		require.NoError(t, perr, "Progress() during resume")
		finalState = progress.State
		require.NotEqual(t, engine.StateFailed, finalState, "resume failed: %s", progress.ErrorMessage)
		if finalState == engine.StateCompleted {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Equal(t, engine.StateCompleted, finalState,
		"resumed change must run to completion, not stall in %s", finalState)

	// The ALTER's effect must be present: the resumed copy must finish widening
	// name from varchar(50) to varchar(100).
	var nameLen int
	require.NoError(t, db.QueryRowContext(t.Context(), `
		SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = 'testdb'
		AND TABLE_NAME = 'stop_alter_target'
		AND COLUMN_NAME = 'name'
	`).Scan(&nameLen), "read name column length")
	assert.Equal(t, 100, nameLen, "resumed ALTER must widen name to varchar(100)")

	// The queued DROP phase must run on resume: its target table is quarantined
	// into the pending drops database, so it is gone from the source database and
	// present in the pending drops database with a parseable timestamp prefix.
	assert.False(t, testutil.TableExists(t, db, "testdb", "stop_drop_target"),
		"resumed DROP phase must remove stop_drop_target from the source database")

	quarantined := listQuarantinedTables(t, db)
	require.Len(t, quarantined, 1, "resumed DROP phase must quarantine exactly one table")
	assert.Contains(t, quarantined[0], "_stop_drop_target",
		"quarantined table name must carry the dropped table's name")
	_, ok := pendingdrops.ParseTimestamp(quarantined[0])
	assert.True(t, ok, "quarantine table name %q must carry a parseable timestamp", quarantined[0])
}

// seedTableRows inserts enough narrow rows that a Spirit table copy on the table
// stays observably in-flight across several progress polls, so a stop reliably
// lands mid-copy. Rows are intentionally narrow (no large payload) so both the
// seed and a subsequent resume-to-completion copy stay well within the suite
// timeout. The seeded column matches the tables used for stop-mid-flight
// scenarios.
func seedTableRows(t *testing.T, db *sql.DB, tableName string) {
	t.Helper()

	seqGen := `(SELECT @row := @row + 1 AS seq FROM
		(SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
		(SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
		(SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c,
		(SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) d,
		(SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) e,
		(SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) f,
		(SELECT @row := 0) r) nums`

	const rowCount = 500000
	query := fmt.Sprintf(
		"INSERT INTO `%s` (name) SELECT CONCAT('name-', seq) FROM %s LIMIT %d",
		tableName, seqGen, rowCount,
	)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, query)
	require.NoError(t, err, "seed %d rows into %s", rowCount, tableName)

	var rows int
	require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName)).Scan(&rows), "count seeded rows")
	require.GreaterOrEqual(t, rows, rowCount, "expected at least %d seeded rows", rowCount)
}

func containsAddColumn(ddl, column string) bool {
	// Simple check - in real code would use proper parsing
	return contains(ddl, "ADD") && contains(ddl, column)
}

func containsCreate(ddl string) bool {
	return contains(ddl, "CREATE")
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// Stateless control operations (cutover and deferred-cutover sentinel lookup
// without an in-memory schema change) must address the schema the DSN connects
// to, not the request's logical database name: under per-deployment schema
// overrides the DSN carries the physical schema while the request carries the
// canonical name. Addressing the request database instead would silently no-op
// the sentinel drop (the cutover reports success while the sentinel survives)
// and report the deferred-cutover signal as absent.
func TestEngine_StatelessControlAddressesDSNSchema(t *testing.T) {
	ctx := t.Context()
	db, err := sql.Open("block-mysql", sharedDSN)
	require.NoError(t, err, "open database")
	defer utils.CloseAndLog(db)

	const (
		canonical      = "ctl_ovr_bikeshare"
		physicalSchema = "ctl_ovr_bikeshare_region2_env1"
	)

	_, err = db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+physicalSchema+"`")
	require.NoError(t, err, "create physical schema")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		cleanupDB, cleanupErr := sql.Open("block-mysql", sharedDSN)
		require.NoError(t, cleanupErr, "open database for stateless control cleanup")
		defer utils.CloseAndLog(cleanupDB)
		_, cleanupErr = cleanupDB.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS `"+physicalSchema+"`")
		assert.NoError(t, cleanupErr, "drop physical schema")
	})
	_, err = db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS `"+physicalSchema+"`.`_spirit_sentinel` (`id` int NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB")
	require.NoError(t, err, "create sentinel table in physical schema")

	// The DSN names the physical schema; the request carries the canonical
	// database name, which exists as no schema on this cluster.
	cfg, err := drivermysql.ParseDSN(sharedDSN)
	require.NoError(t, err, "parse shared DSN")
	cfg.DBName = physicalSchema
	creds := &engine.Credentials{DSN: cfg.FormatDSN()}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	eng := New(Config{Logger: logger})

	exists, err := eng.DeferredCutoverSignalExists(ctx, &engine.DeferredCutoverSignalRequest{
		Database:    canonical,
		Credentials: creds,
	})
	require.NoError(t, err, "deferred cutover signal lookup")
	assert.True(t, exists, "sentinel in the DSN's physical schema is found despite the canonical request database")

	result, err := eng.Cutover(ctx, &engine.ControlRequest{
		Database:    canonical,
		Credentials: creds,
	})
	require.NoError(t, err, "stateless cutover")
	require.NotNil(t, result)
	assert.True(t, result.Accepted, "stateless cutover is accepted")

	var count int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = '_spirit_sentinel'",
		physicalSchema,
	).Scan(&count), "count sentinel tables")
	assert.Zero(t, count, "cutover drops the sentinel in the DSN's physical schema, not the canonical name")

	exists, err = eng.DeferredCutoverSignalExists(ctx, &engine.DeferredCutoverSignalRequest{
		Database:    canonical,
		Credentials: creds,
	})
	require.NoError(t, err, "deferred cutover signal lookup after cutover")
	assert.False(t, exists, "signal is reported absent after the sentinel drop")
}

// TestNewSpiritMigrationRunSettings verifies the Spirit run settings applied to
// every schema change this engine starts: the fleet defaults resolve when the
// engine is built without overrides, and metadata overrides carry through.
func TestNewSpiritMigrationRunSettings(t *testing.T) {
	dsn, _ := setupTestMySQL(t)
	host, username, password, database, err := parseDSN(dsn)
	require.NoError(t, err, "parseDSN")

	eng := New(Config{})
	m := eng.newSpiritMigration(host, username, password, database, "ALTER TABLE t1 ADD COLUMN c1 INT")
	assert.Equal(t, DefaultCheckpointMaxAge, m.CheckpointMaxAge)
	assert.Equal(t, DefaultChecksumYieldTimeout, m.ChecksumYieldTimeout)
	assert.True(t, m.EnableExperimentalAutoscaling, "autoscaling defaults to enabled")
	assert.True(t, m.InterpolateParams)
	assert.Zero(t, m.WriteThreads, "write threads auto-size for the target")
	assert.Equal(t, maxCommitLatency, m.MaxCommitLatency,
		"commit-latency throttle must be set explicitly; Spirit disables the throttler on zero")

	settings, err := SettingsFromMetadata(map[string]string{
		MetadataEnableExperimentalAutoscaling: "false",
		MetadataCheckpointMaxAge:              "24h",
		MetadataChecksumYieldTimeout:          "6h",
	})
	require.NoError(t, err, "SettingsFromMetadata")
	eng = New(Config{Settings: settings})
	m = eng.newSpiritMigration(host, username, password, database, "ALTER TABLE t1 ADD COLUMN c1 INT")
	assert.Equal(t, 24*time.Hour, m.CheckpointMaxAge)
	assert.Equal(t, 6*time.Hour, m.ChecksumYieldTimeout)
	assert.False(t, m.EnableExperimentalAutoscaling, "autoscaling override disables it")
}
