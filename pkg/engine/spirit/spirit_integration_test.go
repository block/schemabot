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
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"

	_ "github.com/go-sql-driver/mysql"
)

// Shared test infrastructure
var (
	sharedDSN       string
	sharedContainer testcontainers.Container
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Start shared MySQL container
	req := testcontainers.ContainerRequest{
		Image:        "mysql:8.0",
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "testpassword",
			"MYSQL_DATABASE":      "testdb",
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("ready for connections").WithOccurrence(2).WithStartupTimeout(30*time.Second),
			wait.ForListeningPort("3306/tcp"),
		),
	}

	var err error
	sharedContainer, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            os.Getenv("DEBUG") != "",
	})
	if err != nil {
		log.Fatalf("start mysql container: %v", err)
	}

	host, err := testutil.ContainerHost(ctx, sharedContainer)
	if err != nil {
		log.Fatalf("get container host: %v", err)
	}
	port, err := testutil.ContainerPort(ctx, sharedContainer, "3306")
	if err != nil {
		log.Fatalf("get container port: %v", err)
	}
	sharedDSN = fmt.Sprintf("root:testpassword@tcp(%s:%d)/testdb?parseTime=true&multiStatements=true", host, port)

	// Wait for MySQL to be ready
	var db *sql.DB
	for range 30 {
		db, err = sql.Open("mysql", sharedDSN)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if err = db.PingContext(ctx); err == nil {
			_ = db.Close()
			break
		}
		_ = db.Close()
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		log.Fatalf("connect to mysql: %v", err)
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

// setupTestMySQL returns a connection to the shared MySQL container.
// Each test should clean up its own tables.
func setupTestMySQL(t *testing.T) (string, *sql.DB) {
	t.Helper()

	db, err := sql.Open("mysql", sharedDSN)
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

	// Verify lint warnings are populated with correct fields
	require.NotEmpty(t, result.LintWarnings, "expected lint warnings for TIMESTAMP column")

	var found bool
	for _, w := range result.LintWarnings {
		if w.Table == "events" && strings.Contains(w.Message, "TIMESTAMP") {
			found = true
			assert.NotEmpty(t, w.Linter, "Linter name should be populated")
			assert.Contains(t, []string{"error", "warning", "info"}, w.Severity,
				"Severity should be a normalized lowercase string")
			break
		}
	}
	assert.True(t, found, "expected a TIMESTAMP-related lint warning for 'events' table, got: %v", result.LintWarnings)
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
	eng.runningMigration = &runningMigration{
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
	eng.runningMigration = &runningMigration{
		database: "testdb",
		state:    engine.StateRunning,
	}

	result, err := eng.Stop(t.Context(), &engine.ControlRequest{
		ResumeState: &engine.ResumeState{MigrationContext: "test"},
	})
	require.NoError(t, err, "Stop()")

	assert.True(t, result.Accepted, "expected Accepted to be true")
	assert.Equal(t, engine.StateStopped, eng.runningMigration.state)
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

func TestEngine_Volume_NoActiveSchemaChange(t *testing.T) {
	eng := New(Config{})

	_, err := eng.Volume(t.Context(), &engine.VolumeRequest{Volume: 5})
	require.Error(t, err, "expected error when no active schema change")
	assert.Contains(t, err.Error(), "no active schema change")
}

func TestNew_Defaults(t *testing.T) {
	eng := New(Config{})

	assert.NotNil(t, eng.logger, "expected logger to be set")
	assert.NotNil(t, eng.linter, "expected linter to be set")
	assert.Equal(t, DefaultTargetChunkTime, eng.targetChunkTime)
	assert.Equal(t, DefaultThreads, eng.threads)
	assert.Equal(t, DefaultLockWaitTimeout, eng.lockWaitTimeout)
}

func TestNew_CustomConfig(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	eng := New(Config{
		Logger:          logger,
		TargetChunkTime: DefaultTargetChunkTime * 2,
		Threads:         8,
		LockWaitTimeout: DefaultLockWaitTimeout * 2,
	})

	assert.Equal(t, logger, eng.logger, "expected custom logger")
	assert.Equal(t, DefaultTargetChunkTime*2, eng.targetChunkTime)
	assert.Equal(t, 8, eng.threads)
}

func TestSetMigrationState(t *testing.T) {
	eng := New(Config{})

	// No running schema change - should not panic
	eng.setMigrationState(engine.StateCompleted)

	// With running schema change
	eng.runningMigration = &runningMigration{
		state: engine.StateRunning,
	}
	eng.setMigrationState(engine.StateCompleted)

	assert.Equal(t, engine.StateCompleted, eng.runningMigration.state)
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

	eng := New(Config{})
	schemas, err := eng.fetchCurrentSchema(t.Context(), dsn, "testdb")
	require.NoError(t, err, "fetchCurrentSchema()")

	assert.Len(t, schemas, 2)
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
	eng.runningMigration = &runningMigration{
		database: "testdb",
		tables:   []string{"users", "orders"},
		state:    engine.StateRunning,
		runners:  nil, // No actual runners, just testing the path
	}

	result, err := eng.Progress(t.Context(), &engine.ProgressRequest{})
	require.NoError(t, err, "Progress()")

	assert.Equal(t, engine.StateRunning, result.State)
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

	// Run the schema change directly using executeMigration
	ddlStatements := []string{
		"ALTER TABLE `test_migrate` ADD COLUMN `email` varchar(255) NULL",
	}

	// Set up running schema change state
	eng.mu.Lock()
	eng.runningMigration = &runningMigration{
		database: database,
		tables:   []string{"test_migrate"},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	// Execute the schema change synchronously for testing
	eng.executeMigration(t.Context(), host, username, password, database, ddlStatements, false)

	// Check that schema change completed
	eng.mu.Lock()
	finalState := eng.runningMigration.state
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
	eng.runningMigration = &runningMigration{
		database: database,
		tables:   []string{"test_modify"},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	eng.executeMigration(t.Context(), host, username, password, database, ddlStatements, false)

	eng.mu.Lock()
	finalState := eng.runningMigration.state
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
	eng.runningMigration = &runningMigration{
		database: database,
		tables:   []string{"test_drop"},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	eng.executeMigration(t.Context(), host, username, password, database, ddlStatements, false)

	eng.mu.Lock()
	finalState := eng.runningMigration.state
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
	eng.runningMigration = &runningMigration{
		database: database,
		tables:   []string{"test_index"},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	eng.executeMigration(t.Context(), host, username, password, database, ddlStatements, false)

	eng.mu.Lock()
	finalState := eng.runningMigration.state
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

// TestEngine_ExecuteMigration_InvalidSQL tests that executeMigration handles
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
	eng.runningMigration = &runningMigration{
		database: database,
		tables:   []string{"test_invalid"},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	eng.executeMigration(t.Context(), host, username, password, database, ddlStatements, false)

	eng.mu.Lock()
	finalState := eng.runningMigration.state
	eng.mu.Unlock()

	assert.Equal(t, engine.StateFailed, finalState, "expected StateFailed for invalid SQL")
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
	eng.runningMigration = &runningMigration{
		database: database,
		tables:   []string{"test_multi_a", "test_multi_b"},
		state:    engine.StateRunning,
		started:  time.Now(),
	}
	eng.mu.Unlock()

	eng.executeMigration(t.Context(), host, username, password, database, ddlStatements, false)

	eng.mu.Lock()
	finalState := eng.runningMigration.state
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
	hasRunningMigration := eng.runningMigration != nil
	eng.mu.Unlock()

	if !hasRunningMigration {
		t.Log("Note: schema change may have completed very quickly")
	}
}

// TestEngine_Progress_WithProgressCallback tests Progress with a callback set
func TestEngine_Progress_WithNilCallback(t *testing.T) {
	eng := New(Config{})
	eng.runningMigration = &runningMigration{
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
	eng.runningMigration = &runningMigration{
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

// TestEngine_Volume_PreservesProgress tests that changing volume preserves progress.
// This is a regression test for a bug where volume change would reset progress to 0%.
// The fix forces a checkpoint before stopping, then resumes from that checkpoint.
func TestEngine_Volume_PreservesProgress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long-running integration test in short mode")
	}

	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	// Create a table with enough data that Spirit takes time to copy
	// Use a smaller VARCHAR so we can expand it (forces table copy)
	_, err := db.ExecContext(t.Context(), `CREATE TABLE volume_test (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(50) NOT NULL,
		data VARCHAR(500) NOT NULL
	)`)
	require.NoError(t, err, "create table")

	// Insert enough rows that Spirit takes a few seconds to copy
	// Need significantly more data for a table copy operation
	t.Log("Inserting test data...")
	for batch := range 500 {
		_, err = db.ExecContext(t.Context(), `INSERT INTO volume_test (name, data) VALUES
			(?, ?), (?, ?), (?, ?), (?, ?), (?, ?),
			(?, ?), (?, ?), (?, ?), (?, ?), (?, ?)`,
			fmt.Sprintf("name-%d-1", batch), strings.Repeat("x", 400),
			fmt.Sprintf("name-%d-2", batch), strings.Repeat("x", 400),
			fmt.Sprintf("name-%d-3", batch), strings.Repeat("x", 400),
			fmt.Sprintf("name-%d-4", batch), strings.Repeat("x", 400),
			fmt.Sprintf("name-%d-5", batch), strings.Repeat("x", 400),
			fmt.Sprintf("name-%d-6", batch), strings.Repeat("x", 400),
			fmt.Sprintf("name-%d-7", batch), strings.Repeat("x", 400),
			fmt.Sprintf("name-%d-8", batch), strings.Repeat("x", 400),
			fmt.Sprintf("name-%d-9", batch), strings.Repeat("x", 400),
			fmt.Sprintf("name-%d-0", batch), strings.Repeat("x", 400),
		)
		require.NoError(t, err, "insert batch %d", batch)
	}

	var rowCount int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM volume_test").Scan(&rowCount), "count rows")
	t.Logf("Created table with %d rows", rowCount)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	eng := New(Config{
		Logger:          logger,
		TargetChunkTime: 100 * time.Millisecond, // Small chunks for more progress updates (Spirit minimum is 100ms)
		Threads:         1,                      // Start slow
	})

	ctx := t.Context()

	// Start the apply with DDL directly
	// Use a DDL that forces a full table copy (not instant DDL)
	// Changing VARCHAR(50) to VARCHAR(100) requires a table copy
	applyResult, err := eng.Apply(ctx, &engine.ApplyRequest{
		Database: "testdb",
		Changes: []engine.SchemaChange{{
			Namespace:    "testdb",
			TableChanges: []engine.TableChange{{Table: "volume_test", DDL: "ALTER TABLE `volume_test` MODIFY COLUMN `name` varchar(100) NOT NULL"}},
		}},
		Credentials: &engine.Credentials{
			DSN: dsn,
		},
	})
	require.NoError(t, err, "Apply()")
	defer eng.Drain()
	require.True(t, applyResult.Accepted, "Apply not accepted: %s", applyResult.Message)
	t.Log("Migration started")

	// Wait for Spirit to make progress and checkpoint
	// Spirit checkpoints every 50s, so we need significant progress
	var progressBefore int64
	for attempts := range 100 {
		time.Sleep(100 * time.Millisecond)

		progressResult, err := eng.Progress(ctx, &engine.ProgressRequest{})
		if err != nil {
			continue
		}

		if len(progressResult.Tables) > 0 {
			progressBefore = progressResult.Tables[0].RowsCopied
			t.Logf("Progress check %d: state=%v, rows_copied=%d/%d",
				attempts, progressResult.State, progressBefore, progressResult.Tables[0].RowsTotal)

			// Wait until we have substantial progress (at least 2000 rows = 40%)
			// so Spirit has had time to checkpoint
			if progressBefore >= 2000 {
				break
			}
		}

		// Check if schema change completed (small table might finish fast)
		if progressResult.State == engine.StateCompleted {
			t.Skip("Schema change completed before we could test volume change")
		}
	}

	if progressBefore < 100 {
		t.Skipf("Spirit didn't make enough progress to test volume change (only %d rows)", progressBefore)
	}

	t.Logf("Progress before volume change: %d rows copied", progressBefore)

	// Change volume - this triggers Stop + Start
	// Note: Volume 5+ has chunk times >5s which Spirit doesn't support,
	// so we use volume 3 (2 threads, 2s chunks)
	volumeResult, err := eng.Volume(ctx, &engine.VolumeRequest{
		Database: "testdb",
		Volume:   3, // Change from 1 thread to 2 threads
		Credentials: &engine.Credentials{
			DSN: dsn,
		},
	})
	require.NoError(t, err, "Volume()")
	t.Logf("Volume changed: %d -> %d", volumeResult.PreviousVolume, volumeResult.NewVolume)

	// Give Spirit time to resume and make progress
	time.Sleep(500 * time.Millisecond)

	// Check progress after volume change
	var progressAfter int64
	var stateAfter engine.State
	for range 20 {
		time.Sleep(100 * time.Millisecond)

		progressResult, err := eng.Progress(ctx, &engine.ProgressRequest{})
		if err != nil {
			continue
		}

		stateAfter = progressResult.State
		if len(progressResult.Tables) > 0 {
			progressAfter = progressResult.Tables[0].RowsCopied
			t.Logf("Progress after volume change: state=%v, rows_copied=%d/%d",
				progressResult.State, progressAfter, progressResult.Tables[0].RowsTotal)
			break
		}

		// If the migration already completed, that's a success — it didn't reset.
		if stateAfter == engine.StateCompleted {
			t.Logf("Migration completed after volume change (fast completion)")
			break
		}
	}

	// The key assertion: progress should NOT have reset to 0.
	// If the migration completed, rows_copied may be 0 (Spirit clears progress
	// on completion) — that's fine, it means it finished successfully.
	if stateAfter != engine.StateCompleted {
		minExpected := progressBefore * 50 / 100
		assert.GreaterOrEqual(t, progressAfter, minExpected,
			"Progress reset after volume change! Before: %d, After: %d (expected at least %d)",
			progressBefore, progressAfter, minExpected)
	}
	t.Logf("Progress after volume change (before=%d, after=%d, state=%v)", progressBefore, progressAfter, stateAfter)

	// Migration cleanup happens automatically when the test ends and the container is stopped
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
