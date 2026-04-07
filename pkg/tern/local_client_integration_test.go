//go:build integration

package tern

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/spirit"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/testutil"
)

// Shared test infrastructure
var (
	sharedContainer *mysql.MySQLContainer
	sharedDSN       string
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Start shared MySQL container
	var err error
	sharedContainer, err = mysql.Run(ctx,
		"mysql:8.0",
		mysql.WithDatabase("testdb"),
		mysql.WithUsername("root"),
		mysql.WithPassword("test"),
	)
	if err != nil {
		log.Fatalf("failed to start MySQL container: %v", err)
	}

	sharedDSN, err = testutil.ContainerConnectionString(ctx, sharedContainer, "parseTime=true", "interpolateParams=true", "multiStatements=true")
	if err != nil {
		_ = sharedContainer.Terminate(ctx)
		log.Fatalf("failed to get connection string: %v", err)
	}

	// Wait for MySQL to be ready
	db, err := sql.Open("mysql", sharedDSN)
	if err != nil {
		_ = sharedContainer.Terminate(ctx)
		log.Fatalf("failed to open database: %v", err)
	}

	for range 30 {
		if err := db.PingContext(ctx); err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	_ = db.Close()

	// Note: Storage schema (tasks, plans, etc.) is NOT applied here.
	// This avoids test interference when Plan/Apply runs - Spirit's differ
	// would see storage tables as "extra" and propose to DROP them.
	// Tests that need storage tables should use setupStorageSchema().

	code := m.Run()

	// Cleanup
	if os.Getenv("DEBUG") == "" {
		_ = sharedContainer.Terminate(ctx)
	}

	os.Exit(code)
}

// setupMySQLContainer returns the shared MySQL container and DSN.
// The container is managed by TestMain, so tests don't need to terminate it.
func setupMySQLContainer(t *testing.T) (*mysql.MySQLContainer, string) {
	t.Helper()
	return sharedContainer, sharedDSN
}

// cleanupTestTables removes test tables to avoid conflicts between tests
func cleanupTestTables(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "failed to open database for cleanup")
	defer utils.CloseAndLog(db)

	// Drop test tables (not storage schema tables)
	testTables := []string{"users", "products", "orders", "accounts", "items", "test_table"}
	for _, table := range testTables {
		_, _ = db.ExecContext(t.Context(), "DROP TABLE IF EXISTS `"+table+"`")
	}
}

// cleanupTasks removes all tasks from the tasks table to ensure clean state.
// This is needed because tasks from previous tests can affect tests that expect no active schema change.
func cleanupTasks(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "failed to open database for task cleanup")
	defer utils.CloseAndLog(db)

	// Delete all tasks and applies to reset state
	_, _ = db.ExecContext(t.Context(), "DELETE FROM tasks")
	_, _ = db.ExecContext(t.Context(), "DELETE FROM applies")
}

// setupStorageSchema creates the storage schema tables (tasks, plans, etc.)
// Tests that use LocalClient with storage functionality should call this.
// Note: Run BEFORE cleanupTestTables to avoid conflicts.
//
// This inlines the EnsureSchema logic from pkg/api because the tern test
// package cannot import api (api imports tern, creating a cycle).
func setupStorageSchema(t *testing.T, dsn string) {
	t.Helper()
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	entries, err := schema.MySQLFS.ReadDir("mysql")
	require.NoError(t, err, "read schema directory")
	files := make(map[string]string)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		content, err := schema.MySQLFS.ReadFile("mysql/" + entry.Name())
		require.NoError(t, err, "read schema file %s", entry.Name())
		files[entry.Name()] = string(content)
	}
	schemaFiles := schema.SchemaFiles{
		"testdb": &schema.Namespace{Files: files},
	}

	eng := spirit.New(spirit.Config{Logger: logger})
	planResult, err := eng.Plan(ctx, &engine.PlanRequest{
		Database:    "testdb",
		SchemaFiles: schemaFiles,
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err, "plan schema")
	if planResult.NoChanges {
		return
	}
	_, err = eng.Apply(ctx, &engine.ApplyRequest{
		Database:    "testdb",
		Changes:     planResult.Changes,
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err, "apply schema")
	for {
		progress, err := eng.Progress(ctx, &engine.ProgressRequest{
			Database:    "testdb",
			Credentials: &engine.Credentials{DSN: dsn},
		})
		require.NoError(t, err, "check progress")
		if progress.State == engine.StateFailed {
			require.Fail(t, "schema change failed", progress.ErrorMessage)
		}
		if progress.State.IsTerminal() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// createStorage creates a storage instance from DSN for testing.
// Requires setupStorageSchema to have been called first.
func createStorage(t *testing.T, dsn string) storage.Storage {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "failed to open database for storage")
	return mysqlstore.New(db)
}

// buildSchemaWithAllTables builds schema files including ALL existing tables in the database.
// This is necessary because the differ will see tables not in schema files as "extra" and
// propose to DROP them. By including storage tables, we ensure only the intended changes
// are made to test tables.
//
// testTableSchemas maps table names to their desired CREATE TABLE statements.
// Tables not in testTableSchemas will have their current schema preserved.
func buildSchemaWithAllTables(t *testing.T, dsn string, testTableSchemas map[string]string) map[string]string {
	t.Helper()

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "failed to open database for schema building")
	defer utils.CloseAndLog(db)

	tables, err := table.LoadSchemaFromDB(t.Context(), db)
	require.NoError(t, err, "failed to load schema from database")

	schemaFiles := make(map[string]string)
	for _, ts := range tables {
		if desiredSchema, ok := testTableSchemas[ts.Name]; ok {
			schemaFiles[ts.Name+".sql"] = desiredSchema
		} else {
			schemaFiles[ts.Name+".sql"] = ts.Schema
		}
	}

	return schemaFiles
}

// waitForApplyComplete polls Progress until the apply reaches a terminal state or times out.
// Fails the test immediately if the apply enters FAILED state.
func waitForApplyComplete(t *testing.T, client *LocalClient, ctx context.Context, applyID string) {
	t.Helper()
	sawRunning := false
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		progress, err := client.Progress(ctx, &ternv1.ProgressRequest{
			Type:     "mysql",
			Database: "testdb",
			ApplyId:  applyID,
		})
		if err != nil {
			t.Logf("Progress() error: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		switch progress.State {
		case ternv1.State_STATE_COMPLETED:
			return
		case ternv1.State_STATE_FAILED:
			t.Fatalf("apply %s failed: %s", applyID, progress.ErrorMessage)
		case ternv1.State_STATE_NO_ACTIVE_CHANGE:
			// NO_ACTIVE_CHANGE means "no tasks found for this database" — either
			// the background goroutine hasn't created tasks yet, or they've
			// been cleaned up after completion. Only treat as done if we
			// previously saw the apply in progress.
			if sawRunning {
				return
			}
		default:
			sawRunning = true
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("apply did not complete within 30s")
}

func TestLocalClient_NewLocalClient(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	container, dsn := setupMySQLContainer(t)
	_ = container              // container is managed by TestMain
	setupStorageSchema(t, dsn) // need storage tables

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)

	config := LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}

	client, err := NewLocalClient(config, stor, logger)
	assert.NoError(t, err, "unexpected error")
	assert.NotNil(t, client, "expected client but got nil")
	if client != nil {
		_ = client.Close()
	}
}

func TestLocalClient_Close(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	container, dsn := setupMySQLContainer(t)
	_ = container              // container is managed by TestMain
	setupStorageSchema(t, dsn) // need storage tables

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err, "failed to create client")

	assert.NoError(t, client.Close(), "Close() returned error")
}

func TestLocalClient_Health(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	container, dsn := setupMySQLContainer(t)
	_ = container              // container is managed by TestMain
	setupStorageSchema(t, dsn) // need storage tables

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err, "failed to create client")
	defer utils.CloseAndLog(client)

	ctx := t.Context()
	assert.NoError(t, client.Health(ctx), "Health() returned error")
}

func TestLocalClient_Plan(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	container, dsn := setupMySQLContainer(t)
	_ = container              // container is managed by TestMain
	setupStorageSchema(t, dsn) // need storage tables

	ctx := t.Context()

	// Create initial table
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "failed to open database")
	defer utils.CloseAndLog(db)

	_, err = db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY)")
	require.NoError(t, err, "failed to create table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err, "failed to create client")
	defer utils.CloseAndLog(client)
	resp, err := client.Plan(ctx, &ternv1.PlanRequest{
		Type:     "mysql",
		Database: "testdb",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"default": {
				Files: map[string]string{
					"users.sql": "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
				},
			},
		},
	})
	require.NoError(t, err, "Plan() returned error")

	assert.NotEmpty(t, resp.PlanId, "expected plan_id but got empty string")
	assert.NotEmpty(t, resp.Changes, "expected at least one schema change")
}

func TestLocalClient_Plan_UsesConfigDatabase(t *testing.T) {
	// In local mode, LocalClient always uses the database from config,
	// not from the request. This test verifies that behavior.
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	container, dsn := setupMySQLContainer(t)
	_ = container              // container is managed by TestMain
	setupStorageSchema(t, dsn) // need storage tables
	cleanupTestTables(t, dsn)  // ensure no leftover tables from prior tests
	cleanupTasks(t, dsn)       // ensure no stale tasks

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err, "failed to create client")
	defer utils.CloseAndLog(client)

	ctx := t.Context()
	// Even with empty database in request, LocalClient uses config.Database
	resp, err := client.Plan(ctx, &ternv1.PlanRequest{
		Type:     "mysql",
		Database: "", // ignored in local mode
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"default": {
				Files: map[string]string{
					"users.sql": "CREATE TABLE users (id INT PRIMARY KEY)",
				},
			},
		},
	})
	require.NoError(t, err, "Plan() should succeed with config database")
	assert.NotEmpty(t, resp.PlanId, "expected plan_id to be set")
}

func TestLocalClient_Apply_PlanNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	container, dsn := setupMySQLContainer(t)
	_ = container              // container is managed by TestMain
	setupStorageSchema(t, dsn) // need storage tables

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err, "failed to create client")
	defer utils.CloseAndLog(client)

	ctx := t.Context()
	resp, err := client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId: "nonexistent-plan-id",
	})
	require.NoError(t, err, "Apply() returned error")
	assert.False(t, resp.Accepted, "expected apply to be rejected for nonexistent plan")
}

func TestLocalClient_Apply(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	container, dsn := setupMySQLContainer(t)
	_ = container              // container is managed by TestMain
	setupStorageSchema(t, dsn) // need storage tables
	cleanupTestTables(t, dsn)  // ensure clean state

	ctx := t.Context()

	// Create initial table
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "failed to open database")
	defer utils.CloseAndLog(db)

	_, err = db.ExecContext(ctx, "CREATE TABLE users (id INT PRIMARY KEY)")
	require.NoError(t, err, "failed to create table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err, "failed to create client")
	defer utils.CloseAndLog(client)

	// Build schema files including all storage tables to avoid DROP TABLE for them
	schemaFiles := buildSchemaWithAllTables(t, dsn, map[string]string{
		"users": "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
	})

	// Create a plan with desired schema (CREATE TABLE with additional column)
	// Spirit.Diff will compute the ALTER statement from current → desired
	planResp, err := client.Plan(ctx, &ternv1.PlanRequest{
		Type:     "mysql",
		Database: "testdb",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"default": {
				Files: schemaFiles,
			},
		},
	})
	require.NoError(t, err, "Plan() returned error")
	require.NotEmpty(t, planResp.PlanId, "expected plan_id but got empty string")

	// Now apply the plan
	applyResp, err := client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId: planResp.PlanId,
	})
	require.NoError(t, err, "Apply() returned error")

	assert.True(t, applyResp.Accepted, "expected apply to be accepted, got error: %s", applyResp.ErrorMessage)

	// Wait for schema change to complete by polling Progress
	waitForApplyComplete(t, client, ctx, applyResp.ApplyId)

	// Verify the column was added
	var columnCount int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'testdb' AND TABLE_NAME = 'users' AND COLUMN_NAME = 'email'").Scan(&columnCount)
	require.NoError(t, err, "query columns")
	assert.Equal(t, 1, columnCount, "expected email column to exist, got count %d", columnCount)
}

func TestLocalClient_Progress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	container, dsn := setupMySQLContainer(t)
	_ = container              // container is managed by TestMain
	setupStorageSchema(t, dsn) // need tasks table
	cleanupTasks(t, dsn)       // ensure no leftover tasks from other tests

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err, "failed to create client")
	defer utils.CloseAndLog(client)

	ctx := t.Context()
	resp, err := client.Progress(ctx, &ternv1.ProgressRequest{
		Type:     "mysql",
		Database: "testdb",
	})
	require.NoError(t, err, "Progress() returned error")
	// With no active schema change, state should be STATE_NO_ACTIVE_CHANGE
	assert.Equal(t, ternv1.State_STATE_NO_ACTIVE_CHANGE, resp.State, "expected STATE_NO_ACTIVE_CHANGE when no active schema change, got: %v", resp.State)
}

func TestLocalClient_Progress_UsesConfigDatabase(t *testing.T) {
	// In local mode, LocalClient always uses the database from config,
	// not from the request. This test verifies that behavior.
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	container, dsn := setupMySQLContainer(t)
	_ = container              // container is managed by TestMain
	cleanupTestTables(t, dsn)  // remove leftover tables from prior tests
	setupStorageSchema(t, dsn) // need storage tables
	cleanupTasks(t, dsn)       // ensure no leftover tasks from other tests

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err, "failed to create client")
	defer utils.CloseAndLog(client)

	ctx := t.Context()
	// Even with empty database in request, LocalClient uses config.Database
	resp, err := client.Progress(ctx, &ternv1.ProgressRequest{
		Type:     "mysql",
		Database: "", // ignored in local mode
	})
	require.NoError(t, err, "Progress() should succeed with config database")
	// With no active schema change, state should be STATE_NO_ACTIVE_CHANGE
	assert.Equal(t, ternv1.State_STATE_NO_ACTIVE_CHANGE, resp.State, "expected STATE_NO_ACTIVE_CHANGE when no active schema change, got: %v", resp.State)
}

func TestLocalClient_Cutover_NoActiveMigration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	container, dsn := setupMySQLContainer(t)
	_ = container              // container is managed by TestMain
	setupStorageSchema(t, dsn) // need storage tables

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err, "failed to create client")
	defer utils.CloseAndLog(client)

	ctx := t.Context()
	// Cutover without an active schema change should return an error
	_, err = client.Cutover(ctx, &ternv1.CutoverRequest{
		Type:     "mysql",
		Database: "testdb",
	})
	assert.Error(t, err, "expected error for cutover without active schema change")
}

func TestLocalClient_Stop_NoMigration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	container, dsn := setupMySQLContainer(t)
	_ = container              // container is managed by TestMain
	setupStorageSchema(t, dsn) // need storage tables
	cleanupTasks(t, dsn)       // ensure no leftover tasks from other tests

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err, "failed to create client")
	defer utils.CloseAndLog(client)

	ctx := t.Context()
	// Stop with no active schema change returns error from Spirit engine
	_, err = client.Stop(ctx, &ternv1.StopRequest{
		Type:     "mysql",
		Database: "testdb",
	})
	require.Error(t, err, "expected Stop() to return error when no active schema change")
	// Error should mention no active schema change
	assert.Contains(t, err.Error(), "no active schema change")
}

func TestLocalClient_Start_NoStoppedMigration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	container, dsn := setupMySQLContainer(t)
	_ = container              // container is managed by TestMain
	setupStorageSchema(t, dsn) // need storage tables

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err, "failed to create client")
	defer utils.CloseAndLog(client)

	ctx := t.Context()
	// Start requires a stopped schema change to resume - returns error when none exists
	_, err = client.Start(ctx, &ternv1.StartRequest{
		Type:     "mysql",
		Database: "testdb",
	})
	require.Error(t, err, "expected Start() to return error when no stopped schema change")
	// Error should mention no stopped schema change
	assert.Contains(t, err.Error(), "no stopped schema change")
}

func TestLocalClient_Volume_NoActiveMigration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	container, dsn := setupMySQLContainer(t)
	_ = container              // container is managed by TestMain
	setupStorageSchema(t, dsn) // need storage tables

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err, "failed to create client")
	defer utils.CloseAndLog(client)

	ctx := t.Context()
	// Volume requires an active schema change - returns error when none exists
	_, err = client.Volume(ctx, &ternv1.VolumeRequest{
		Type:     "mysql",
		Database: "testdb",
		Volume:   5,
	})
	require.Error(t, err, "expected Volume() to return error when no active schema change")
	// Error should mention no active schema change
	assert.Contains(t, err.Error(), "no active schema change")
}

func TestLocalClient_Revert_NoActiveMigration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	container, dsn := setupMySQLContainer(t)
	_ = container              // container is managed by TestMain
	setupStorageSchema(t, dsn) // need storage tables

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err, "failed to create client")
	defer utils.CloseAndLog(client)

	ctx := t.Context()
	// Revert requires an active schema change - returns error when none exists
	_, err = client.Revert(ctx, &ternv1.RevertRequest{
		Type:     "mysql",
		Database: "testdb",
	})
	require.Error(t, err, "expected Revert() to return error when no active schema change")
	// Error should mention no active schema change
	assert.Contains(t, err.Error(), "no active schema change")
}

func TestLocalClient_SkipRevert_NoActiveMigration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	container, dsn := setupMySQLContainer(t)
	_ = container              // container is managed by TestMain
	setupStorageSchema(t, dsn) // need storage tables

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err, "failed to create client")
	defer utils.CloseAndLog(client)

	ctx := t.Context()
	// SkipRevert requires an active schema change - returns error when none exists
	_, err = client.SkipRevert(ctx, &ternv1.SkipRevertRequest{
		Type:     "mysql",
		Database: "testdb",
	})
	require.Error(t, err, "expected SkipRevert() to return error when no active schema change")
	// Error should mention no active schema change
	assert.Contains(t, err.Error(), "no active schema change")
}

// TestLocalClient_Apply_MultiTableSequential tests applying changes to multiple
// tables in sequential mode (no --defer-cutover). This verifies that each DDL
// is processed as a separate task and all tasks complete.
func TestLocalClient_Apply_MultiTableSequential(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	container, dsn := setupMySQLContainer(t)
	_ = container              // container is managed by TestMain
	setupStorageSchema(t, dsn) // need storage tables
	cleanupTestTables(t, dsn)  // ensure clean state

	ctx := t.Context()

	// Create two initial tables
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "failed to open database")
	defer utils.CloseAndLog(db)

	_, err = db.ExecContext(ctx, "CREATE TABLE test_users (id INT PRIMARY KEY)")
	require.NoError(t, err, "failed to create test_users table")

	_, err = db.ExecContext(ctx, "CREATE TABLE test_orders (id INT PRIMARY KEY)")
	require.NoError(t, err, "failed to create test_orders table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	stor := createStorage(t, dsn)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err, "failed to create client")
	defer utils.CloseAndLog(client)

	// Load current schema for all tables (including storage) so the differ
	// only sees changes for test_users and test_orders.
	tables, err := table.LoadSchemaFromDB(t.Context(), db)
	require.NoError(t, err, "failed to load schema from database")

	schemaFiles := make(map[string]string)
	for _, ts := range tables {
		switch ts.Name {
		case "test_users":
			schemaFiles[ts.Name+".sql"] = "CREATE TABLE test_users (id INT PRIMARY KEY, email VARCHAR(255))"
		case "test_orders":
			schemaFiles[ts.Name+".sql"] = "CREATE TABLE test_orders (id INT PRIMARY KEY, total_cents INT)"
		default:
			schemaFiles[ts.Name+".sql"] = ts.Schema
		}
	}

	// Create a plan that modifies BOTH test tables
	planResp, err := client.Plan(ctx, &ternv1.PlanRequest{
		Type:     "mysql",
		Database: "testdb",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"default": {
				Files: schemaFiles,
			},
		},
	})
	require.NoError(t, err, "Plan() returned error")
	require.NotEmpty(t, planResp.PlanId, "expected plan_id but got empty string")

	// Flatten all table changes from all namespaces
	var allTables []*ternv1.TableChange
	for _, sc := range planResp.Changes {
		allTables = append(allTables, sc.TableChanges...)
	}

	// Verify the plan has exactly 2 table changes (test_users and test_orders)
	if len(allTables) != 2 {
		t.Logf("Plan has %d table changes (expected 2):", len(allTables))
		for _, tc := range allTables {
			t.Logf("  - %s: %s", tc.TableName, tc.Ddl)
		}
		require.Len(t, allTables, 2, "expected 2 table changes in plan, got %d", len(allTables))
	}
	t.Logf("Plan has %d table changes:", len(allTables))
	for _, tc := range allTables {
		t.Logf("  - %s: %s", tc.TableName, tc.Ddl)
	}

	// Apply the plan in sequential mode (no defer_cutover)
	applyResp, err := client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId: planResp.PlanId,
		// No options means sequential mode
	})
	require.NoError(t, err, "Apply() returned error")
	require.True(t, applyResp.Accepted, "expected apply to be accepted, got error: %s", applyResp.ErrorMessage)

	// Wait for schema changes to complete (both tables should be modified)
	// Poll for completion rather than fixed sleep
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		progress, err := client.Progress(ctx, &ternv1.ProgressRequest{
			Type:     "mysql",
			Database: "testdb",
		})
		if err != nil {
			t.Logf("Progress() error: %v", err)
		} else {
			t.Logf("Progress state: %v", progress.State)
			if progress.State == ternv1.State_STATE_COMPLETED ||
				progress.State == ternv1.State_STATE_NO_ACTIVE_CHANGE {
				break
			}
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Verify BOTH tables were modified

	// Check test_users table has email column
	var usersColumnCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = 'testdb'
		AND TABLE_NAME = 'test_users'
		AND COLUMN_NAME = 'email'
	`).Scan(&usersColumnCount)
	require.NoError(t, err, "query test_users columns")
	assert.Equal(t, 1, usersColumnCount, "expected email column in test_users table, got count %d", usersColumnCount)

	// Check test_orders table has total_cents column
	var ordersColumnCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = 'testdb'
		AND TABLE_NAME = 'test_orders'
		AND COLUMN_NAME = 'total_cents'
	`).Scan(&ordersColumnCount)
	require.NoError(t, err, "query test_orders columns")
	assert.Equal(t, 1, ordersColumnCount, "expected total_cents column in test_orders table, got count %d", ordersColumnCount)

	t.Logf("Verification: test_users.email=%d, test_orders.total_cents=%d", usersColumnCount, ordersColumnCount)
}

// TestLocalClient_StartApplyHeartbeat directly tests the heartbeat mechanism
// by creating an apply record and verifying startApplyHeartbeat advances
// updated_at independently of Spirit execution. This is the shared heartbeat
// used by both sequential and atomic code paths.
func TestLocalClient_StartApplyHeartbeat(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	container, dsn := setupMySQLContainer(t)
	_ = container
	setupStorageSchema(t, dsn)

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	stor := createStorage(t, dsn)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	// Use a short heartbeat interval so the ticker fires during the test
	client.heartbeatInterval = 1 * time.Second

	ctx := t.Context()

	// Insert a minimal apply record directly — avoids populating all required
	// foreign keys and JSON columns that the storage layer demands.
	result, err := db.ExecContext(ctx, `
		INSERT INTO applies (apply_identifier, lock_id, plan_id, database_name,
			database_type, repository, pull_request, environment, engine, state, options)
		VALUES ('heartbeat-test-apply', 0, 0, 'testdb', 'mysql', '', 0, '', 'spirit', ?, '{}')
	`, state.Apply.Running)
	require.NoError(t, err)
	applyID, err := result.LastInsertId()
	require.NoError(t, err)
	apply := &storage.Apply{ID: applyID}

	// Snapshot updated_at right after creation
	var initialUpdatedAt time.Time
	err = db.QueryRowContext(ctx, "SELECT updated_at FROM applies WHERE id = ?", apply.ID).Scan(&initialUpdatedAt)
	require.NoError(t, err, "query initial updated_at")

	// Start the heartbeat and let it run for >1s
	cancel := client.startApplyHeartbeat(ctx, apply)
	time.Sleep(2 * time.Second)
	cancel()

	// Verify the heartbeat advanced updated_at
	var updatedAt time.Time
	err = db.QueryRowContext(ctx, "SELECT updated_at FROM applies WHERE id = ?", apply.ID).Scan(&updatedAt)
	require.NoError(t, err, "query apply updated_at")
	assert.True(t, updatedAt.After(initialUpdatedAt),
		"apply updated_at (%v) should have advanced beyond initial (%v) — heartbeat not firing",
		updatedAt, initialUpdatedAt)
}

// TestLocalClient_Apply_AtomicHeartbeat verifies that the atomic (defer-cutover)
// code path maintains heartbeats on the parent apply, matching the sequential test.
func TestLocalClient_Apply_AtomicHeartbeat(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	container, dsn := setupMySQLContainer(t)
	_ = container
	setupStorageSchema(t, dsn)
	cleanupTestTables(t, dsn)
	cleanupTasks(t, dsn)

	ctx := t.Context()

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	_, err = db.ExecContext(ctx, "CREATE TABLE test_atomic_hb (id INT PRIMARY KEY, val VARCHAR(50))")
	require.NoError(t, err)

	// Seed rows so Spirit has data to copy. The MODIFY COLUMN below forces a
	// full table copy (not instant DDL), ensuring Spirit reaches the sentinel
	// wait state when defer_cutover is set.
	for i := range 100 {
		_, err = db.ExecContext(ctx, "INSERT INTO test_atomic_hb (id, val) VALUES (?, ?)", i, fmt.Sprintf("row-%d", i))
		require.NoError(t, err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	stor := createStorage(t, dsn)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	// Use a short heartbeat interval so the ticker fires during the test
	client.heartbeatInterval = 1 * time.Second

	schemaFiles := buildSchemaWithAllTables(t, dsn, map[string]string{
		"test_atomic_hb": "CREATE TABLE test_atomic_hb (id INT PRIMARY KEY, val VARCHAR(100) NOT NULL)",
	})

	planResp, err := client.Plan(ctx, &ternv1.PlanRequest{
		Type:     "mysql",
		Database: "testdb",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"testdb": {Files: schemaFiles},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, planResp.PlanId)

	applyResp, err := client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:  planResp.PlanId,
		Options: map[string]string{"defer_cutover": "true"},
	})
	require.NoError(t, err)
	require.True(t, applyResp.Accepted, "apply rejected: %s", applyResp.ErrorMessage)

	// Wait for waiting_for_cutover — the apply sits here while heartbeat keeps running
	deadline := time.Now().Add(30 * time.Second)
	var st string
	for time.Now().Before(deadline) {
		err = db.QueryRowContext(ctx, "SELECT state FROM applies WHERE apply_identifier = ?", applyResp.ApplyId).Scan(&st)
		if err == nil && (st == state.Apply.WaitingForCutover || st == state.Apply.Completed || st == state.Apply.Failed) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.Equal(t, state.Apply.WaitingForCutover, st, "apply should reach waiting_for_cutover")

	// Snapshot updated_at while in waiting_for_cutover
	var initialUpdatedAt time.Time
	err = db.QueryRowContext(ctx, "SELECT updated_at FROM applies WHERE apply_identifier = ?", applyResp.ApplyId).Scan(&initialUpdatedAt)
	require.NoError(t, err, "query initial updated_at")

	// Wait long enough for the heartbeat ticker (1s) to fire at least once
	time.Sleep(2 * time.Second)

	// Verify heartbeat advanced updated_at while sitting in waiting_for_cutover
	var updatedAt time.Time
	err = db.QueryRowContext(ctx, "SELECT updated_at FROM applies WHERE apply_identifier = ?", applyResp.ApplyId).Scan(&updatedAt)
	require.NoError(t, err, "query apply updated_at")
	assert.True(t, updatedAt.After(initialUpdatedAt),
		"apply updated_at (%v) should have advanced beyond initial (%v) — heartbeat not firing during waiting_for_cutover",
		updatedAt, initialUpdatedAt)

	// Trigger cutover to complete the apply
	_, err = client.Cutover(ctx, &ternv1.CutoverRequest{
		Database:    "testdb",
		Environment: "",
	})
	require.NoError(t, err, "cutover")

	// Wait for completion with a fresh deadline
	cutoverDeadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(cutoverDeadline) {
		err = db.QueryRowContext(ctx, "SELECT state FROM applies WHERE apply_identifier = ?", applyResp.ApplyId).Scan(&st)
		if err == nil && (st == state.Apply.Completed || st == state.Apply.Failed) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	assert.Equal(t, state.Apply.Completed, st, "apply should be completed")
}
