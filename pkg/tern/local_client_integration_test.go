//go:build integration

package tern

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

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/mysql"

	waitutil "github.com/block/schemabot/e2e/testutil"
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

const localClientTestEnvironment = "development"

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
	waitutil.Poll(t, 30*time.Second, 500*time.Millisecond,
		func() bool {
			progress, err := client.Progress(ctx, &ternv1.ProgressRequest{
				ApplyId:     applyID,
				Environment: localClientTestEnvironment,
			})
			if err != nil {
				t.Logf("Progress() error: %v", err)
				return false
			}
			switch progress.State {
			case ternv1.State_STATE_COMPLETED:
				return true
			case ternv1.State_STATE_FAILED:
				t.Fatalf("apply %s failed: %s", applyID, progress.ErrorMessage)
			case ternv1.State_STATE_NO_ACTIVE_CHANGE:
				// NO_ACTIVE_CHANGE means "no tasks found for this database" — either
				// the background goroutine hasn't created tasks yet, or they've
				// been cleaned up after completion. Only treat as done if we
				// previously saw the apply in progress.
				return sawRunning
			default:
				sawRunning = true
			}
			return false
		},
		func() string { return fmt.Sprintf("apply %s did not complete within 30s", applyID) },
	)
}

type retryableFailureEngine struct {
	engine.Engine
}

func (e *retryableFailureEngine) Name() string { return "retryable-failure" }

func (e *retryableFailureEngine) Plan(context.Context, *engine.PlanRequest) (*engine.PlanResult, error) {
	return &engine.PlanResult{}, nil
}

func (e *retryableFailureEngine) Apply(context.Context, *engine.ApplyRequest) (*engine.ApplyResult, error) {
	return &engine.ApplyResult{Accepted: true}, nil
}

func (e *retryableFailureEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	return &engine.ProgressResult{
		State:        engine.StateFailed,
		Retryable:    true,
		ErrorMessage: "temporary engine failure",
		Tables: []engine.TableProgress{{
			Namespace: "testdb",
			Table:     "users",
			State:     state.Task.FailedRetryable,
			Progress:  45,
		}},
	}, nil
}

type leaseInspectingEngine struct {
	engine.Engine

	store         storage.Storage
	applyID       int64
	observedState string
}

func (e *leaseInspectingEngine) Name() string { return "lease-inspecting" }

func (e *leaseInspectingEngine) Apply(ctx context.Context, _ *engine.ApplyRequest) (*engine.ApplyResult, error) {
	apply, err := e.store.Applies().Get(ctx, e.applyID)
	if err != nil {
		return nil, fmt.Errorf("get apply during engine apply: %w", err)
	}
	if apply == nil {
		return nil, storage.ErrApplyNotFound
	}
	e.observedState = apply.State
	return &engine.ApplyResult{Accepted: true}, nil
}

func (e *leaseInspectingEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	return &engine.ProgressResult{
		State: engine.StateCompleted,
		Tables: []engine.TableProgress{{
			Namespace: "testdb",
			Table:     "users",
			State:     state.Task.Completed,
			Progress:  100,
		}},
	}, nil
}

type stagedGroupedResumeEngine struct {
	engine.Engine

	planResults []*engine.PlanResult
	planCalls   int
	applyCount  int
	drainCount  int
	applyErr    error
	applyResult *engine.ApplyResult
	progress    *engine.ProgressResult

	// applyRequests records a snapshot of each ApplyRequest so tests can
	// assert on the resume state and changes the resume path sends.
	applyRequests []*engine.ApplyRequest
	// progressResumeMetadata records the resume state metadata each Progress
	// poll carries, in call order.
	progressResumeMetadata []string
}

func (e *stagedGroupedResumeEngine) Name() string { return "staged-grouped-resume" }

func (e *stagedGroupedResumeEngine) Plan(context.Context, *engine.PlanRequest) (*engine.PlanResult, error) {
	if len(e.planResults) == 0 {
		return &engine.PlanResult{}, nil
	}
	idx := e.planCalls
	if idx >= len(e.planResults) {
		idx = len(e.planResults) - 1
	}
	e.planCalls++
	return e.planResults[idx], nil
}

func (e *stagedGroupedResumeEngine) Apply(_ context.Context, req *engine.ApplyRequest) (*engine.ApplyResult, error) {
	e.applyCount++
	snapshot := *req
	if req.ResumeState != nil {
		resumeState := *req.ResumeState
		snapshot.ResumeState = &resumeState
	}
	e.applyRequests = append(e.applyRequests, &snapshot)
	if e.applyErr != nil {
		return nil, e.applyErr
	}
	if e.applyResult != nil {
		return e.applyResult, nil
	}
	return &engine.ApplyResult{Accepted: true}, nil
}

func (e *stagedGroupedResumeEngine) Progress(_ context.Context, req *engine.ProgressRequest) (*engine.ProgressResult, error) {
	if req.ResumeState != nil {
		e.progressResumeMetadata = append(e.progressResumeMetadata, req.ResumeState.Metadata)
	}
	if e.progress != nil {
		return e.progress, nil
	}
	return &engine.ProgressResult{State: engine.StateCompleted}, nil
}

func (e *stagedGroupedResumeEngine) Drain() {
	e.drainCount++
}

func (e *stagedGroupedResumeEngine) DeferredCutoverSignalExists(ctx context.Context, req *engine.DeferredCutoverSignalRequest) (bool, error) {
	checker, ok := e.Engine.(engine.DeferredCutoverSignalChecker)
	if !ok {
		return false, fmt.Errorf("wrapped engine %T does not support deferred cutover signal lookup", e.Engine)
	}
	return checker.DeferredCutoverSignalExists(ctx, req)
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

// Pulling a live MySQL schema returns deterministic declarative files from the
// data-plane database without preserving volatile AUTO_INCREMENT counters.
func TestLocalClient_PullSchemaLoadsLiveMySQLSchema(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	container, dsn := setupMySQLContainer(t)
	_ = container // container is managed by TestMain
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "open database")
	defer utils.CloseAndLog(db)

	_, err = db.ExecContext(t.Context(), "DROP TABLE IF EXISTS `pull_schema_users`, `pull_schema_users_archive_2026_06_12`")
	require.NoError(t, err, "drop old pull schema tables")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		cleanupDB, cleanupErr := sql.Open("mysql", dsn)
		require.NoError(t, cleanupErr, "open database for pull schema cleanup")
		defer utils.CloseAndLog(cleanupDB)
		_, cleanupErr = cleanupDB.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS `pull_schema_users`, `pull_schema_users_archive_2026_06_12`")
		assert.NoError(t, cleanupErr, "drop pull schema tables")
	})
	_, err = db.ExecContext(t.Context(), "CREATE TABLE `pull_schema_users` (`id` bigint unsigned NOT NULL AUTO_INCREMENT, `email` varchar(255) NOT NULL, PRIMARY KEY (`id`), UNIQUE KEY `idx_email` (`email`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err, "create pull schema table")
	_, err = db.ExecContext(t.Context(), "CREATE TABLE `pull_schema_users_archive_2026_06_12` (`id` bigint unsigned NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err, "create archive table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, nil, logger)
	require.NoError(t, err, "create client")
	defer utils.CloseAndLog(client)

	resp, err := client.PullSchema(t.Context(), &ternv1.PullSchemaRequest{
		Type:        storage.DatabaseTypeMySQL,
		Environment: localClientTestEnvironment,
	})

	require.NoError(t, err, "pull schema")
	require.NotNil(t, resp)
	assert.Equal(t, "testdb", resp.Database)
	assert.Equal(t, storage.DatabaseTypeMySQL, resp.Type)
	assert.Equal(t, localClientTestEnvironment, resp.Environment)
	require.Contains(t, resp.Namespaces, "testdb")
	ddl := resp.Namespaces["testdb"].Tables["pull_schema_users"]
	assert.Contains(t, ddl, "CREATE TABLE `pull_schema_users`")
	assert.Contains(t, ddl, "`email` varchar(255) NOT NULL")
	assert.NotContains(t, ddl, "AUTO_INCREMENT=")
	assert.True(t, strings.HasSuffix(ddl, "\n"), "pulled schema file should end with a newline")
	assert.NotContains(t, resp.Namespaces["testdb"].Tables, "pull_schema_users_archive_2026_06_12")
}

// Pulling all live MySQL namespaces discovers application schemas while
// excluding system, SchemaBot storage, pending-drop, and underscore-prefixed
// namespaces from the exported declarative files.
func TestLocalClient_PullSchemaDiscoversNonReservedNamespaces(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	container, dsn := setupMySQLContainer(t)
	_ = container // container is managed by TestMain
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "open database")
	defer utils.CloseAndLog(db)

	for _, stmt := range []string{
		"CREATE DATABASE IF NOT EXISTS `pull_primary`",
		"CREATE DATABASE IF NOT EXISTS `pull_audit`",
		"CREATE DATABASE IF NOT EXISTS `_pull_reserved`",
		"CREATE TABLE IF NOT EXISTS `pull_primary`.`users` (`id` bigint unsigned NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		"CREATE TABLE IF NOT EXISTS `pull_audit`.`events` (`id` bigint unsigned NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		"CREATE TABLE IF NOT EXISTS `_pull_reserved`.`old_users` (`id` bigint unsigned NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	} {
		_, err = db.ExecContext(t.Context(), stmt)
		require.NoError(t, err, "prepare namespace discovery schema")
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		cleanupDB, cleanupErr := sql.Open("mysql", dsn)
		require.NoError(t, cleanupErr, "open database for namespace discovery cleanup")
		defer utils.CloseAndLog(cleanupDB)
		for _, stmt := range []string{
			"DROP DATABASE IF EXISTS `pull_primary`",
			"DROP DATABASE IF EXISTS `pull_audit`",
			"DROP DATABASE IF EXISTS `_pull_reserved`",
		} {
			_, cleanupErr = cleanupDB.ExecContext(cleanupCtx, stmt)
			assert.NoError(t, cleanupErr, "cleanup namespace discovery schema")
		}
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	client, err := NewLocalClient(LocalConfig{
		Database:  "logicaldb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsnWithoutDatabase(t, dsn),
	}, nil, logger)
	require.NoError(t, err, "create client")
	defer utils.CloseAndLog(client)

	resp, err := client.PullSchema(t.Context(), &ternv1.PullSchemaRequest{
		Database:    "logicaldb",
		Type:        storage.DatabaseTypeMySQL,
		Environment: localClientTestEnvironment,
	})

	require.NoError(t, err, "pull all namespaces")
	assert.Contains(t, resp.Namespaces, "pull_primary")
	assert.Contains(t, resp.Namespaces, "pull_audit")
	assert.NotContains(t, resp.Namespaces, "_pull_reserved")
	assert.NotContains(t, resp.Namespaces, "schemabot")
}

func dsnWithoutDatabase(t *testing.T, dsn string) string {
	t.Helper()
	cfg, err := drivermysql.ParseDSN(dsn)
	require.NoError(t, err)
	cfg.DBName = ""
	return cfg.FormatDSN()
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
		PlanId:      "nonexistent-plan-id",
		Environment: localClientTestEnvironment,
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
		PlanId:      planResp.PlanId,
		Environment: localClientTestEnvironment,
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

// An apply created via the Tern client must carry exactly one apply_operations
// row, and every task must link to it via ApplyOperationID. The operator claim
// loop selects work exclusively from apply_operations, and the engine
// resume-state path requires a non-nil ApplyOperationID, so an apply without
// this child row would never start, recover, or persist resume state.
func TestLocalClient_Apply_WritesApplyOperationRow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "failed to open database")
	defer utils.CloseAndLog(db)

	_, err = db.ExecContext(ctx, "CREATE TABLE users (id INT PRIMARY KEY)")
	require.NoError(t, err, "failed to create table")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err, "failed to create client")
	defer utils.CloseAndLog(client)

	schemaFiles := buildSchemaWithAllTables(t, dsn, map[string]string{
		"users": "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
	})

	planResp, err := client.Plan(ctx, &ternv1.PlanRequest{
		Type:     "mysql",
		Database: "testdb",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"default": {Files: schemaFiles},
		},
	})
	require.NoError(t, err, "Plan() returned error")
	require.NotEmpty(t, planResp.PlanId, "expected plan_id but got empty string")

	applyResp, err := client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:      planResp.PlanId,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err, "Apply() returned error")
	require.True(t, applyResp.Accepted, "expected apply to be accepted, got error: %s", applyResp.ErrorMessage)

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, applyResp.ApplyId)
	require.NoError(t, err, "lookup apply by identifier")
	require.NotNil(t, apply, "apply should exist in storage")

	operations, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err, "list apply operations")
	require.Len(t, operations, 1, "exactly one apply_operations row must be written per apply")

	op := operations[0]
	assert.Equal(t, apply.ID, op.ApplyID, "operation must reference the apply")
	assert.Equal(t, "testdb", op.Deployment, "operation deployment must mirror the apply deployment")
	assert.Equal(t, "testdb", op.Target, "operation target must mirror the resolved plan target")
	assert.Equal(t, state.ApplyOperation.Pending, op.State, "operation must start pending")

	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err, "list tasks for apply")
	require.NotEmpty(t, tasks, "apply must have at least one task")
	for _, task := range tasks {
		require.NotNil(t, task.ApplyOperationID, "task %s must link to the apply_operations row", task.TaskIdentifier)
		assert.Equal(t, op.ID, *task.ApplyOperationID, "task %s must reference the apply's operation row", task.TaskIdentifier)
	}

	// The apply still runs to completion against the real Spirit engine with the
	// operation row present (the sequential path is unaffected by the linkage).
	waitForApplyComplete(t, client, ctx, applyResp.ApplyId)

	var columnCount int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'testdb' AND TABLE_NAME = 'users' AND COLUMN_NAME = 'email'").Scan(&columnCount)
	require.NoError(t, err, "query columns")
	assert.Equal(t, 1, columnCount, "expected email column to exist, got count %d", columnCount)
}

// TestLocalClient_Progress verifies that progress is scoped to a concrete apply
// and returns the stored task details for that apply.
func TestLocalClient_Progress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-progress-%d", now.UnixNano()),
		PlanID:          1,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "testdb",
		Environment:     localClientTestEnvironment,
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Stopped,
		Caller:          "test",
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := stor.Applies().Create(ctx, apply)
	require.NoError(t, err)

	task := &storage.Task{
		TaskIdentifier: fmt.Sprintf("task-progress-%d", now.UnixNano()),
		ApplyID:        applyID,
		PlanID:         apply.PlanID,
		Database:       apply.Database,
		DatabaseType:   apply.DatabaseType,
		Engine:         apply.Engine,
		Environment:    apply.Environment,
		State:          state.Task.Stopped,
		Namespace:      "testdb",
		TableName:      "users",
		DDL:            "ALTER TABLE `users` ADD COLUMN `email` VARCHAR(255)",
		DDLAction:      "alter",
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	_, err = stor.Tasks().Create(ctx, task)
	require.NoError(t, err)

	progress, err := client.Progress(ctx, &ternv1.ProgressRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)

	assert.Equal(t, apply.ApplyIdentifier, progress.ApplyId)
	assert.Equal(t, ternv1.State_STATE_STOPPED, progress.State)
	assert.Equal(t, ternv1.Engine_ENGINE_SPIRIT, progress.Engine)
	require.Len(t, progress.Tables, 1)
	assert.Equal(t, "testdb", progress.Tables[0].Namespace)
	assert.Equal(t, "users", progress.Tables[0].TableName)
	assert.Equal(t, state.Task.Stopped, progress.Tables[0].Status)
	assert.Equal(t, ternv1.ChangeType_CHANGE_TYPE_ALTER, progress.Tables[0].ChangeType)
	assert.Equal(t, task.DDL, progress.Tables[0].Ddl)
}

func TestLocalClient_GroupedApplyKeepsClaimLeaseRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-lease-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {
				Tables: []storage.TableChange{
					{Namespace: "testdb", Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN lease_note VARCHAR(255)", Operation: "alter"},
				},
			},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)
	plan.ID = planID

	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-lease-%d", time.Now().UnixNano()),
		PlanID:          planID,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "testdb",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Pending,
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true}),
		Environment:     localClientTestEnvironment,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := stor.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	task := &storage.Task{
		TaskIdentifier: fmt.Sprintf("task-lease-users-%d", time.Now().UnixNano()),
		ApplyID:        applyID,
		PlanID:         planID,
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Engine:         storage.EngineSpirit,
		State:          state.Task.Pending,
		TableName:      "users",
		Namespace:      "testdb",
		DDL:            "ALTER TABLE `users` ADD COLUMN lease_note VARCHAR(255)",
		DDLAction:      "alter",
		Options:        storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true}),
		Environment:    localClientTestEnvironment,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	taskID, err := stor.Tasks().Create(ctx, task)
	require.NoError(t, err)
	task.ID = taskID

	claimed, err := stor.Applies().FindNextApply(ctx, "test-owner")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, state.Apply.Pending, claimed.State)

	persisted, err := stor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	require.Equal(t, state.Apply.Running, persisted.State)

	inspectingEngine := &leaseInspectingEngine{store: stor, applyID: applyID}
	client.spiritEngine = inspectingEngine

	require.NoError(t, client.ResumeApply(ctx, claimed))
	assert.Equal(t, state.Apply.Running, inspectingEngine.observedState)

	completed, err := stor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, completed)
	assert.Equal(t, state.Apply.Completed, completed.State)
}

// An operator worker resumes a single apply_operation — one deployment of an
// apply — rather than the whole apply. ResumeApplyOperation loads only that
// operation's tasks (via the operation-scoped read primitive) and drives them
// to completion through the same engine path as ResumeApply.
func TestLocalClient_ResumeApplyOperationDrivesOperationTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-op-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {
				Tables: []storage.TableChange{
					{Namespace: "testdb", Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN op_note VARCHAR(255)", Operation: "alter"},
				},
			},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)
	plan.ID = planID

	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-op-%d", time.Now().UnixNano()),
		PlanID:          planID,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "testdb",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Pending,
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true}),
		Environment:     localClientTestEnvironment,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := stor.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	operationID, err := stor.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID:    applyID,
		Deployment: "testdb",
		Target:     "testdb",
		State:      state.ApplyOperation.Pending,
	})
	require.NoError(t, err)

	task := &storage.Task{
		TaskIdentifier:   fmt.Sprintf("task-op-users-%d", time.Now().UnixNano()),
		ApplyID:          applyID,
		ApplyOperationID: &operationID,
		PlanID:           planID,
		Database:         "testdb",
		DatabaseType:     storage.DatabaseTypeMySQL,
		Engine:           storage.EngineSpirit,
		State:            state.Task.Pending,
		TableName:        "users",
		Namespace:        "testdb",
		DDL:              "ALTER TABLE `users` ADD COLUMN op_note VARCHAR(255)",
		DDLAction:        "alter",
		Options:          storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true}),
		Environment:      localClientTestEnvironment,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	taskID, err := stor.Tasks().Create(ctx, task)
	require.NoError(t, err)
	task.ID = taskID

	claimed, err := stor.Applies().FindNextApply(ctx, "test-owner")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, state.Apply.Pending, claimed.State)

	client.spiritEngine = &leaseInspectingEngine{store: stor, applyID: applyID}

	require.NoError(t, client.ResumeApplyOperation(ctx, claimed, operationID))

	completed, err := stor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, completed)
	assert.Equal(t, state.Apply.Completed, completed.State)

	tasks, err := stor.Tasks().GetByApplyOperationID(ctx, operationID)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, state.Task.Completed, tasks[0].State)
	require.NotNil(t, tasks[0].ApplyOperationID)
	assert.Equal(t, operationID, *tasks[0].ApplyOperationID)
}

// An operation that resolves to no tasks is an invalid or stale claim. The local
// drive must fail closed with ErrNoTasksForApplyOperation — matchable with
// errors.Is — without mutating the parent apply, so the operator can terminalize
// just that operation rather than marking the whole apply failed.
func TestLocalClient_ResumeApplyOperationFailsClosedOnNoTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-op-notasks-%d", time.Now().UnixNano()),
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "testdb",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Running,
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{}),
		Environment:     localClientTestEnvironment,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := stor.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	// Insert an operation but deliberately no tasks scoped to it.
	operationID, err := stor.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID:    applyID,
		Deployment: "testdb",
		Target:     "testdb",
		State:      state.ApplyOperation.Running,
	})
	require.NoError(t, err)

	err = client.ResumeApplyOperation(ctx, apply, operationID)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNoTasksForApplyOperation, "the empty-operation fail-closed signal must be matchable with errors.Is")

	// The parent apply must be untouched: the empty lookup is scoped to the one
	// operation, not the whole apply.
	after, err := stor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, state.Apply.Running, after.State, "a task-less operation must not mutate the parent apply state")
}

// This scenario covers an operator-owned grouped start where the target schema
// advances between the recovery re-plan and the final pre-dispatch schema check.
// The operator should complete durable state without reissuing engine apply work.
func TestLocalClient_ResumeApplyGroupedFinalSchemaCheckCompletesWithoutReapply(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	ddl := "ALTER TABLE `users` ADD COLUMN email varchar(255)"
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-grouped-final-check-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {
				Tables: []storage.TableChange{
					{Namespace: "testdb", Table: "users", DDL: ddl, Operation: "alter"},
				},
			},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)
	plan.ID = planID

	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-grouped-final-check-%d", time.Now().UnixNano()),
		PlanID:          planID,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "testdb",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Stopped,
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true}),
		Environment:     localClientTestEnvironment,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := stor.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	task := &storage.Task{
		TaskIdentifier: fmt.Sprintf("task-grouped-final-check-users-%d", time.Now().UnixNano()),
		ApplyID:        applyID,
		PlanID:         planID,
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Engine:         storage.EngineSpirit,
		State:          state.Task.Stopped,
		TableName:      "users",
		Namespace:      "testdb",
		DDL:            ddl,
		DDLAction:      "alter",
		Options:        storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true}),
		Environment:    localClientTestEnvironment,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	taskID, err := stor.Tasks().Create(ctx, task)
	require.NoError(t, err)
	task.ID = taskID

	_, alreadyPending, err := stor.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     applyID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "integration-test",
	})
	require.NoError(t, err)
	assert.False(t, alreadyPending)

	resumeEngine := &stagedGroupedResumeEngine{planResults: []*engine.PlanResult{
		{
			Changes: []engine.SchemaChange{{
				Namespace: "testdb",
				TableChanges: []engine.TableChange{{
					Table: "users",
					DDL:   ddl,
				}},
			}},
		},
		{NoChanges: true},
	}}
	client.spiritEngine = resumeEngine

	claimed, err := stor.Applies().FindNextApply(ctx, "test-owner")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, state.Apply.Stopped, claimed.State)

	require.NoError(t, client.ResumeApply(ctx, claimed))
	assert.Equal(t, 2, resumeEngine.planCalls)
	assert.Equal(t, 1, resumeEngine.drainCount)
	assert.Equal(t, 0, resumeEngine.applyCount)

	storedApply, err := stor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, storedApply)
	assert.Equal(t, state.Apply.Completed, storedApply.State)
	assert.NotNil(t, storedApply.CompletedAt)

	storedTask, err := stor.Tasks().Get(ctx, task.TaskIdentifier)
	require.NoError(t, err)
	require.NotNil(t, storedTask)
	assert.Equal(t, state.Task.Completed, storedTask.State)
	assert.Equal(t, 100, storedTask.ProgressPercent)
	assert.NotNil(t, storedTask.CompletedAt)

	pendingStart, err := stor.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationStart)
	require.NoError(t, err)
	assert.Nil(t, pendingStart)

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(ctx))
	var controlStatus string
	err = db.QueryRowContext(ctx, `
		SELECT status
		FROM apply_control_requests
		WHERE apply_id = ? AND operation = ?
	`, applyID, storage.ControlOperationStart).Scan(&controlStatus)
	require.NoError(t, err)
	assert.Equal(t, string(storage.ControlRequestCompleted), controlStatus)

	logs, err := stor.ApplyLogs().GetByApply(ctx, applyID)
	require.NoError(t, err)
	assert.True(t, hasLogMessageContaining(logs, "All tasks already completed on resume (final schema check shows no remaining changes)"))
}

// This scenario covers restart recovery where storage was waiting for cutover
// and Spirit's durable sentinel still exists. Row-copy progress remains visible
// during recovery, but durable storage stays cutover-blocking until Spirit proves
// cutover readiness again.
func TestLocalClient_ResumeApplyDeferredCutoverRecoveryPreservesCutoverReadyStorage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(ctx))
	_, err = db.ExecContext(ctx, "CREATE TABLE `users` (`id` INT PRIMARY KEY)")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE `_spirit_sentinel` (id int NOT NULL PRIMARY KEY)")
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(t.Context())
		cleanupDB, cleanupErr := sql.Open("mysql", dsn)
		require.NoError(t, cleanupErr)
		defer utils.CloseAndLog(cleanupDB)
		_, cleanupErr = cleanupDB.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS `_spirit_sentinel`")
		assert.NoError(t, cleanupErr)
	})

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	ddl := "ALTER TABLE `users` ADD COLUMN `recovery_note` varchar(255)"
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-cutover-recovery-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {
				Tables: []storage.TableChange{{Namespace: "testdb", Table: "users", DDL: ddl, Operation: "alter"}},
			},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)

	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-cutover-recovery-%d", time.Now().UnixNano()),
		PlanID:          planID,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "testdb",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.WaitingForCutover,
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true}),
		Environment:     localClientTestEnvironment,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := stor.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	task := &storage.Task{
		TaskIdentifier:  fmt.Sprintf("task-cutover-recovery-users-%d", time.Now().UnixNano()),
		ApplyID:         applyID,
		PlanID:          planID,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Engine:          storage.EngineSpirit,
		State:           state.Task.WaitingForCutover,
		TableName:       "users",
		Namespace:       "testdb",
		DDL:             ddl,
		DDLAction:       "alter",
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true}),
		Environment:     localClientTestEnvironment,
		ProgressPercent: 100,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	taskID, err := stor.Tasks().Create(ctx, task)
	require.NoError(t, err)
	task.ID = taskID

	recoveryEngine := &stagedGroupedResumeEngine{
		Engine: client.spiritEngine,
		planResults: []*engine.PlanResult{{
			Changes: []engine.SchemaChange{{
				Namespace:    "testdb",
				TableChanges: []engine.TableChange{{Table: "users", DDL: ddl}},
			}},
		}},
		progress: &engine.ProgressResult{
			State: engine.StatePending,
			Tables: []engine.TableProgress{{
				Namespace: "testdb",
				Table:     "users",
				State:     state.Task.Pending,
			}},
		},
	}
	client.spiritEngine = recoveryEngine

	recoverCtx, cancel := context.WithTimeout(ctx, 1200*time.Millisecond)
	defer cancel()
	err = client.ResumeApply(recoverCtx, apply)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	storedApply, err := stor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, storedApply)
	assert.Equal(t, state.Apply.Recovering, storedApply.State)

	storedTask, err := stor.Tasks().Get(ctx, task.TaskIdentifier)
	require.NoError(t, err)
	require.NotNil(t, storedTask)
	assert.Equal(t, state.Task.Recovering, storedTask.State)

	progressResp, err := client.Progress(ctx, &ternv1.ProgressRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	assert.Equal(t, ternv1.State_STATE_RECOVERING, progressResp.State)

	cutoverResp, err := client.Cutover(ctx, &ternv1.CutoverRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	require.NotNil(t, cutoverResp)
	assert.False(t, cutoverResp.Accepted)
	assert.Contains(t, cutoverResp.ErrorMessage, "recovering")

	recoveryEngine.progress = &engine.ProgressResult{
		State: engine.StateRunning,
		Tables: []engine.TableProgress{{
			Namespace: "testdb",
			Table:     "users",
			State:     state.Task.Running,
			Progress:  42,
		}},
	}
	progressResp, err = client.Progress(ctx, &ternv1.ProgressRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	assert.Equal(t, ternv1.State_STATE_RECOVERING, progressResp.State)

	storedApply, err = stor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, storedApply)
	assert.Equal(t, state.Apply.Recovering, storedApply.State)

	storedTask, err = stor.Tasks().Get(ctx, task.TaskIdentifier)
	require.NoError(t, err)
	require.NotNil(t, storedTask)
	assert.Equal(t, state.Task.Recovering, storedTask.State)

	recoveryEngine.progress = &engine.ProgressResult{
		State: engine.StateWaitingForCutover,
		Tables: []engine.TableProgress{{
			Namespace: "testdb",
			Table:     "users",
			State:     state.Task.WaitingForCutover,
			Progress:  100,
		}},
	}
	progressResp, err = client.Progress(ctx, &ternv1.ProgressRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	assert.Equal(t, ternv1.State_STATE_WAITING_FOR_CUTOVER, progressResp.State)

	storedApply, err = stor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, storedApply)
	assert.Equal(t, state.Apply.WaitingForCutover, storedApply.State)

	storedTask, err = stor.Tasks().Get(ctx, task.TaskIdentifier)
	require.NoError(t, err)
	require.NotNil(t, storedTask)
	assert.Equal(t, state.Task.WaitingForCutover, storedTask.State)
}

// This scenario covers a deferred-cutover recovery where Spirit's durable
// sentinel is still present but engine reattach fails before progress polling
// can prove cutover readiness. Storage should leave recovery through a visible
// retry-waiting state instead of retrying forever without an operator-visible outcome.
func TestLocalClient_ResumeApplyDeferredCutoverFailureMarksApplyRetryable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(ctx))
	_, err = db.ExecContext(ctx, "CREATE TABLE `users` (`id` INT PRIMARY KEY)")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE `_spirit_sentinel` (id int NOT NULL PRIMARY KEY)")
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(t.Context())
		cleanupDB, cleanupErr := sql.Open("mysql", dsn)
		require.NoError(t, cleanupErr)
		defer utils.CloseAndLog(cleanupDB)
		_, cleanupErr = cleanupDB.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS `_spirit_sentinel`")
		assert.NoError(t, cleanupErr)
	})

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	ddl := "ALTER TABLE `users` ADD COLUMN `recovery_failure_note` varchar(255)"
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-cutover-recovery-failure-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {
				Tables: []storage.TableChange{{Namespace: "testdb", Table: "users", DDL: ddl, Operation: "alter"}},
			},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)

	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-cutover-recovery-failure-%d", time.Now().UnixNano()),
		PlanID:          planID,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "testdb",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.WaitingForCutover,
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true}),
		Environment:     localClientTestEnvironment,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := stor.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	task := &storage.Task{
		TaskIdentifier:  fmt.Sprintf("task-cutover-recovery-failure-users-%d", time.Now().UnixNano()),
		ApplyID:         applyID,
		PlanID:          planID,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Engine:          storage.EngineSpirit,
		State:           state.Task.WaitingForCutover,
		TableName:       "users",
		Namespace:       "testdb",
		DDL:             ddl,
		DDLAction:       "alter",
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true}),
		Environment:     localClientTestEnvironment,
		ProgressPercent: 100,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	taskID, err := stor.Tasks().Create(ctx, task)
	require.NoError(t, err)
	task.ID = taskID

	recoveryEngine := &stagedGroupedResumeEngine{
		Engine: client.spiritEngine,
		planResults: []*engine.PlanResult{{
			Changes: []engine.SchemaChange{{
				Namespace:    "testdb",
				TableChanges: []engine.TableChange{{Table: "users", DDL: ddl}},
			}},
		}},
		applyErr: fmt.Errorf("synthetic deferred cutover recovery failure"),
	}
	client.spiritEngine = recoveryEngine

	err = client.ResumeApply(ctx, apply)
	require.NoError(t, err)

	storedApply, err := stor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, storedApply)
	assert.Equal(t, state.Apply.FailedRetryable, storedApply.State)
	assert.Contains(t, storedApply.ErrorMessage, "synthetic deferred cutover recovery failure")

	storedTask, err := stor.Tasks().Get(ctx, task.TaskIdentifier)
	require.NoError(t, err)
	require.NotNil(t, storedTask)
	assert.Equal(t, state.Task.FailedRetryable, storedTask.State)
	assert.Contains(t, storedTask.ErrorMessage, "synthetic deferred cutover recovery failure")
}

// This scenario covers a deferred-cutover recovery where the Spirit sentinel is
// already absent when SchemaBot restarts. The operator should reconcile against
// the live schema instead of blocking forever in cutover recovery.
func TestLocalClient_ResumeApplyDeferredCutoverAbsentSentinelReconcilesCompletedSchema(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(ctx))
	_, err = db.ExecContext(ctx, "CREATE TABLE `users` (`id` INT PRIMARY KEY, `recovery_note` varchar(255))")
	require.NoError(t, err)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	ddl := "ALTER TABLE `users` ADD COLUMN `recovery_note` varchar(255)"
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-cutover-no-sentinel-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {
				Tables: []storage.TableChange{{Namespace: "testdb", Table: "users", DDL: ddl, Operation: "alter"}},
			},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)

	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-cutover-no-sentinel-%d", time.Now().UnixNano()),
		PlanID:          planID,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "testdb",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.WaitingForCutover,
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true}),
		Environment:     localClientTestEnvironment,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := stor.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	task := &storage.Task{
		TaskIdentifier:  fmt.Sprintf("task-cutover-no-sentinel-users-%d", time.Now().UnixNano()),
		ApplyID:         applyID,
		PlanID:          planID,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Engine:          storage.EngineSpirit,
		State:           state.Task.WaitingForCutover,
		TableName:       "users",
		Namespace:       "testdb",
		DDL:             ddl,
		DDLAction:       "alter",
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true}),
		Environment:     localClientTestEnvironment,
		ProgressPercent: 100,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	taskID, err := stor.Tasks().Create(ctx, task)
	require.NoError(t, err)
	task.ID = taskID

	recoveryEngine := &stagedGroupedResumeEngine{
		Engine:      client.spiritEngine,
		planResults: []*engine.PlanResult{{}},
	}
	client.spiritEngine = recoveryEngine

	require.NoError(t, client.ResumeApply(ctx, apply))
	assert.Equal(t, 0, recoveryEngine.applyCount)

	storedApply, err := stor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, storedApply)
	assert.Equal(t, state.Apply.Completed, storedApply.State)

	storedTask, err := stor.Tasks().Get(ctx, task.TaskIdentifier)
	require.NoError(t, err)
	require.NotNil(t, storedTask)
	assert.Equal(t, state.Task.Completed, storedTask.State)
}

// This scenario covers a deferred-cutover recovery where the Spirit sentinel is
// absent but the live schema does not contain the desired schema. Storage was
// already cutover-ready, so SchemaBot fails closed instead of moving backward to
// running after losing the cutover signal.
func TestLocalClient_ResumeApplyDeferredCutoverAbsentSentinelFailsWhenWorkRemains(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(ctx))
	_, err = db.ExecContext(ctx, "CREATE TABLE `users` (`id` INT PRIMARY KEY)")
	require.NoError(t, err)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	ddl := "ALTER TABLE `users` ADD COLUMN `recovery_note` varchar(255)"
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-cutover-no-sentinel-work-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {
				Tables: []storage.TableChange{{Namespace: "testdb", Table: "users", DDL: ddl, Operation: "alter"}},
			},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)

	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-cutover-no-sentinel-work-%d", time.Now().UnixNano()),
		PlanID:          planID,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "testdb",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.WaitingForCutover,
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true}),
		Environment:     localClientTestEnvironment,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := stor.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	task := &storage.Task{
		TaskIdentifier:  fmt.Sprintf("task-cutover-no-sentinel-work-users-%d", time.Now().UnixNano()),
		ApplyID:         applyID,
		PlanID:          planID,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Engine:          storage.EngineSpirit,
		State:           state.Task.WaitingForCutover,
		TableName:       "users",
		Namespace:       "testdb",
		DDL:             ddl,
		DDLAction:       "alter",
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true}),
		Environment:     localClientTestEnvironment,
		ProgressPercent: 100,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	taskID, err := stor.Tasks().Create(ctx, task)
	require.NoError(t, err)
	task.ID = taskID

	recoveryEngine := &stagedGroupedResumeEngine{
		Engine: client.spiritEngine,
		planResults: []*engine.PlanResult{{
			Changes: []engine.SchemaChange{{
				Namespace: "testdb",
				TableChanges: []engine.TableChange{{
					Table: "users",
					DDL:   ddl,
				}},
			}},
		}},
	}
	client.spiritEngine = recoveryEngine

	require.NoError(t, client.ResumeApply(ctx, apply))
	assert.Equal(t, 0, recoveryEngine.applyCount)

	storedApply, err := stor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, storedApply)
	assert.Equal(t, state.Apply.Failed, storedApply.State)
	assert.Contains(t, storedApply.ErrorMessage, "manual reconciliation required")

	storedTask, err := stor.Tasks().Get(ctx, task.TaskIdentifier)
	require.NoError(t, err)
	require.NotNil(t, storedTask)
	assert.Equal(t, state.Task.Failed, storedTask.State)
	assert.Contains(t, storedTask.ErrorMessage, "manual reconciliation required")

	logs, err := stor.ApplyLogs().GetByApply(ctx, applyID)
	require.NoError(t, err)
	assert.True(t, hasLogMessageContaining(logs, "manual reconciliation required"))
}

// This scenario covers an operator-owned grouped start where remote execution is
// rejected after the durable start request was claimed. The start request should
// fail visibly instead of being marked completed before engine acceptance.
func TestLocalClient_ResumeApplyGroupedStartRequestFailsWhenEngineRejects(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	ddl := "ALTER TABLE `users` ADD COLUMN phone varchar(32)"
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-grouped-start-fails-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {
				Tables: []storage.TableChange{
					{Namespace: "testdb", Table: "users", DDL: ddl, Operation: "alter"},
				},
			},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)
	plan.ID = planID

	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-grouped-start-fails-%d", time.Now().UnixNano()),
		PlanID:          planID,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "testdb",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Stopped,
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true}),
		Environment:     localClientTestEnvironment,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := stor.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	task := &storage.Task{
		TaskIdentifier: fmt.Sprintf("task-grouped-start-fails-users-%d", time.Now().UnixNano()),
		ApplyID:        applyID,
		PlanID:         planID,
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Engine:         storage.EngineSpirit,
		State:          state.Task.Stopped,
		TableName:      "users",
		Namespace:      "testdb",
		DDL:            ddl,
		DDLAction:      "alter",
		Options:        storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true}),
		Environment:    localClientTestEnvironment,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	taskID, err := stor.Tasks().Create(ctx, task)
	require.NoError(t, err)
	task.ID = taskID

	_, alreadyPending, err := stor.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     applyID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "integration-test",
	})
	require.NoError(t, err)
	assert.False(t, alreadyPending)

	resumeEngine := &stagedGroupedResumeEngine{
		planResults: []*engine.PlanResult{{
			Changes: []engine.SchemaChange{{
				Namespace: "testdb",
				TableChanges: []engine.TableChange{{
					Table: "users",
					DDL:   ddl,
				}},
			}},
		}},
		applyErr: engine.NewPermanentError("engine refused grouped resume"),
	}
	client.spiritEngine = resumeEngine

	claimed, err := stor.Applies().FindNextApply(ctx, "test-owner")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, state.Apply.Stopped, claimed.State)

	err = client.ResumeApply(ctx, claimed)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "engine refused grouped resume")
	assert.Equal(t, 2, resumeEngine.planCalls)
	assert.Equal(t, 1, resumeEngine.drainCount)
	assert.Equal(t, 1, resumeEngine.applyCount)

	storedApply, err := stor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, storedApply)
	assert.Equal(t, state.Apply.Failed, storedApply.State)
	assert.Contains(t, storedApply.ErrorMessage, "engine refused grouped resume")
	assert.NotNil(t, storedApply.CompletedAt)

	storedTask, err := stor.Tasks().Get(ctx, task.TaskIdentifier)
	require.NoError(t, err)
	require.NotNil(t, storedTask)
	assert.Equal(t, state.Task.Failed, storedTask.State)
	assert.Contains(t, storedTask.ErrorMessage, "engine refused grouped resume")

	pendingStart, err := stor.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationStart)
	require.NoError(t, err)
	assert.Nil(t, pendingStart)

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(ctx))
	var controlStatus, controlError string
	err = db.QueryRowContext(ctx, `
		SELECT status, error_message
		FROM apply_control_requests
		WHERE apply_id = ? AND operation = ?
	`, applyID, storage.ControlOperationStart).Scan(&controlStatus, &controlError)
	require.NoError(t, err)
	assert.Equal(t, string(storage.ControlRequestFailed), controlStatus)
	assert.Contains(t, controlError, "engine refused grouped resume")

	logs, err := stor.ApplyLogs().GetByApply(ctx, applyID)
	require.NoError(t, err)
	assert.True(t, hasLogMessageContaining(logs, "Recovery failed: engine apply failed: engine refused grouped resume"))
}

// This scenario covers restart recovery of a grouped Vitess apply whose opaque
// engine resume state was persisted before the worker died. Recovery must hand
// that state back to the engine in exactly one grouped apply — even without
// defer-cutover — so the engine reattaches to the in-flight deploy request
// instead of opening a duplicate one. The rebuilt changes must keep the tasks'
// namespace/table identity for per-table progress matching, and progress
// polling must carry the engine's returned resume state so the deploy request
// stays observable and updated state is persisted.
func TestLocalClient_ResumeApplyVitessGroupedReattachesWithPersistedState(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeVitess,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	usersDDL := "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"
	ordersDDL := "ALTER TABLE `orders` ADD COLUMN `note` varchar(255)"
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-vitess-resume-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeVitess,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"commerce": {Tables: []storage.TableChange{{Namespace: "commerce", Table: "users", DDL: usersDDL, Operation: "alter"}}},
			"billing":  {Tables: []storage.TableChange{{Namespace: "billing", Table: "orders", DDL: ordersDDL, Operation: "alter"}}},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)

	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-vitess-resume-%d", now.UnixNano()),
		PlanID:          planID,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeVitess,
		Deployment:      "testdb",
		Engine:          storage.EnginePlanetScale,
		State:           state.Apply.Running,
		Environment:     localClientTestEnvironment,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	tasks := []*storage.Task{
		{
			TaskIdentifier: fmt.Sprintf("task-vitess-resume-users-%d", now.UnixNano()),
			PlanID:         planID,
			Database:       "testdb",
			DatabaseType:   storage.DatabaseTypeVitess,
			Engine:         storage.EnginePlanetScale,
			State:          state.Task.Running,
			Namespace:      "commerce",
			TableName:      "users",
			DDL:            usersDDL,
			DDLAction:      "alter",
			Environment:    localClientTestEnvironment,
			CreatedAt:      now,
			UpdatedAt:      now,
		},
		{
			TaskIdentifier: fmt.Sprintf("task-vitess-resume-orders-%d", now.UnixNano()),
			PlanID:         planID,
			Database:       "testdb",
			DatabaseType:   storage.DatabaseTypeVitess,
			Engine:         storage.EnginePlanetScale,
			State:          state.Task.Running,
			Namespace:      "billing",
			TableName:      "orders",
			DDL:            ordersDDL,
			DDLAction:      "alter",
			Environment:    localClientTestEnvironment,
			CreatedAt:      now,
			UpdatedAt:      now,
		},
	}
	operation := &storage.ApplyOperation{
		Deployment: "testdb",
		State:      state.ApplyOperation.Running,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	applyID, err := stor.Applies().CreateWithTasksAndOperations(ctx, apply, tasks, []*storage.ApplyOperation{operation})
	require.NoError(t, err)
	apply.ID = applyID

	persistedMetadata := `{"branch_name":"recovery-branch","deploy_request_id":42,"deploy_request_url":"https://example.test/deploys/42"}`
	require.NoError(t, stor.ApplyOperations().SaveEngineResumeState(ctx, operation.ID, &storage.EngineResumeState{
		ApplyOperationID: operation.ID,
		MigrationContext: apply.ApplyIdentifier,
		Metadata:         persistedMetadata,
	}))

	reattachedMetadata := `{"branch_name":"recovery-branch","deploy_request_id":42,"deploy_request_url":"https://example.test/deploys/42-reattached"}`
	finalMetadata := `{"branch_name":"recovery-branch","deploy_request_id":42,"deploy_request_url":"https://example.test/deploys/42-final"}`
	resumeEngine := &stagedGroupedResumeEngine{
		planResults: []*engine.PlanResult{{
			Changes: []engine.SchemaChange{
				{Namespace: "commerce", TableChanges: []engine.TableChange{{Table: "users", DDL: usersDDL}}},
				{Namespace: "billing", TableChanges: []engine.TableChange{{Table: "orders", DDL: ordersDDL}}},
			},
		}},
		applyResult: &engine.ApplyResult{
			Accepted:    true,
			ResumeState: &engine.ResumeState{MigrationContext: apply.ApplyIdentifier, Metadata: reattachedMetadata},
		},
		progress: &engine.ProgressResult{
			State:       engine.StateCompleted,
			ResumeState: &engine.ResumeState{MigrationContext: apply.ApplyIdentifier, Metadata: finalMetadata},
			Tables: []engine.TableProgress{
				{Namespace: "commerce", Table: "users", State: state.Task.Completed, Progress: 100},
				{Namespace: "billing", Table: "orders", State: state.Task.Completed, Progress: 100},
			},
		},
	}
	client.planetscaleEngine = resumeEngine

	resumeCtx, cancelResume := context.WithTimeout(ctx, 30*time.Second)
	defer cancelResume()
	require.NoError(t, client.ResumeApply(resumeCtx, apply))

	require.Len(t, resumeEngine.applyRequests, 1, "grouped recovery must issue exactly one engine apply")
	applyReq := resumeEngine.applyRequests[0]
	require.NotNil(t, applyReq.ResumeState)
	assert.Equal(t, apply.ApplyIdentifier, applyReq.ResumeState.MigrationContext)
	assert.JSONEq(t, persistedMetadata, applyReq.ResumeState.Metadata)

	require.Len(t, applyReq.Changes, 2)
	changesByNamespace := make(map[string][]engine.TableChange, len(applyReq.Changes))
	for _, sc := range applyReq.Changes {
		changesByNamespace[sc.Namespace] = sc.TableChanges
	}
	require.Len(t, changesByNamespace["commerce"], 1)
	assert.Equal(t, "users", changesByNamespace["commerce"][0].Table)
	assert.Equal(t, usersDDL, changesByNamespace["commerce"][0].DDL)
	require.Len(t, changesByNamespace["billing"], 1)
	assert.Equal(t, "orders", changesByNamespace["billing"][0].Table)
	assert.Equal(t, ordersDDL, changesByNamespace["billing"][0].DDL)

	require.NotEmpty(t, resumeEngine.progressResumeMetadata)
	assert.JSONEq(t, reattachedMetadata, resumeEngine.progressResumeMetadata[0])

	storedState, err := stor.ApplyOperations().GetEngineResumeState(ctx, operation.ID)
	require.NoError(t, err)
	assert.JSONEq(t, finalMetadata, storedState.Metadata)

	storedApply, err := stor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, storedApply)
	assert.Equal(t, state.Apply.Completed, storedApply.State)

	for _, task := range tasks {
		storedTask, err := stor.Tasks().Get(ctx, task.TaskIdentifier)
		require.NoError(t, err)
		require.NotNil(t, storedTask)
		assert.Equal(t, state.Task.Completed, storedTask.State)
	}
}

// This scenario covers restart recovery of a grouped deferred-cutover apply
// with no persisted engine resume state. The engine reattaches through its own
// durable checkpoints keyed by the schema change context, so recovery issues
// one grouped engine apply whose resume state carries the apply identifier and
// whose rebuilt changes keep the tasks' namespace and table so per-table
// progress matching still works.
func TestLocalClient_ResumeApplyGroupedRebuildsChangesFromTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	ddl := "ALTER TABLE `users` ADD COLUMN `resume_note` varchar(255)"
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-grouped-resume-changes-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {Tables: []storage.TableChange{{Namespace: "testdb", Table: "users", DDL: ddl, Operation: "alter"}}},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)

	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-grouped-resume-changes-%d", now.UnixNano()),
		PlanID:          planID,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "testdb",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Running,
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true}),
		Environment:     localClientTestEnvironment,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	task := &storage.Task{
		TaskIdentifier: fmt.Sprintf("task-grouped-resume-changes-users-%d", now.UnixNano()),
		PlanID:         planID,
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Engine:         storage.EngineSpirit,
		State:          state.Task.Running,
		TableName:      "users",
		Namespace:      "testdb",
		DDL:            ddl,
		DDLAction:      "alter",
		Options:        storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true}),
		Environment:    localClientTestEnvironment,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	operation := &storage.ApplyOperation{
		Deployment: "testdb",
		State:      state.ApplyOperation.Running,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	applyID, err := stor.Applies().CreateWithTasksAndOperations(ctx, apply, []*storage.Task{task}, []*storage.ApplyOperation{operation})
	require.NoError(t, err)
	apply.ID = applyID

	resumeEngine := &stagedGroupedResumeEngine{
		planResults: []*engine.PlanResult{{
			Changes: []engine.SchemaChange{{
				Namespace:    "testdb",
				TableChanges: []engine.TableChange{{Table: "users", DDL: ddl}},
			}},
		}},
		progress: &engine.ProgressResult{
			State: engine.StateCompleted,
			Tables: []engine.TableProgress{{
				Namespace: "testdb",
				Table:     "users",
				State:     state.Task.Completed,
				Progress:  100,
			}},
		},
	}
	client.spiritEngine = resumeEngine

	resumeCtx, cancelResume := context.WithTimeout(ctx, 30*time.Second)
	defer cancelResume()
	require.NoError(t, client.ResumeApply(resumeCtx, apply))

	require.Len(t, resumeEngine.applyRequests, 1, "grouped recovery must issue exactly one engine apply")
	applyReq := resumeEngine.applyRequests[0]
	require.NotNil(t, applyReq.ResumeState)
	assert.Equal(t, apply.ApplyIdentifier, applyReq.ResumeState.MigrationContext)
	assert.Empty(t, applyReq.ResumeState.Metadata)

	require.Len(t, applyReq.Changes, 1)
	assert.Equal(t, "testdb", applyReq.Changes[0].Namespace)
	require.Len(t, applyReq.Changes[0].TableChanges, 1)
	assert.Equal(t, "users", applyReq.Changes[0].TableChanges[0].Table)
	assert.Equal(t, ddl, applyReq.Changes[0].TableChanges[0].DDL)
	assert.Equal(t, statement.StatementAlterTable, applyReq.Changes[0].TableChanges[0].Operation)

	storedApply, err := stor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, storedApply)
	assert.Equal(t, state.Apply.Completed, storedApply.State)

	storedTask, err := stor.Tasks().Get(ctx, task.TaskIdentifier)
	require.NoError(t, err)
	require.NotNil(t, storedTask)
	assert.Equal(t, state.Task.Completed, storedTask.State)
	assert.Equal(t, 100, storedTask.ProgressPercent)
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
		Environment: localClientTestEnvironment,
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
		Environment: localClientTestEnvironment,
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
		Environment: localClientTestEnvironment,
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
		Environment: localClientTestEnvironment,
		Volume:      5,
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
		Environment: localClientTestEnvironment,
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
		Environment: localClientTestEnvironment,
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
		PlanId:      planResp.PlanId,
		Environment: localClientTestEnvironment,
		// No options means sequential mode
	})
	require.NoError(t, err, "Apply() returned error")
	require.True(t, applyResp.Accepted, "expected apply to be accepted, got error: %s", applyResp.ErrorMessage)

	// Wait for schema changes to complete (both tables should be modified)
	// Poll for completion rather than fixed sleep
	waitutil.Poll(t, 30*time.Second, 500*time.Millisecond,
		func() bool {
			progress, err := client.Progress(ctx, &ternv1.ProgressRequest{
				ApplyId:     applyResp.ApplyId,
				Environment: localClientTestEnvironment,
			})
			if err != nil {
				t.Logf("Progress() error: %v", err)
				return false
			}
			t.Logf("Progress state: %v", progress.State)
			return progress.State == ternv1.State_STATE_COMPLETED ||
				progress.State == ternv1.State_STATE_NO_ACTIVE_CHANGE
		},
		func() string { return "schema changes did not complete within 30s" },
	)

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
		PlanId:      planResp.PlanId,
		Environment: localClientTestEnvironment,
		Options:     map[string]string{"defer_cutover": "true"},
	})
	require.NoError(t, err)
	require.True(t, applyResp.Accepted, "apply rejected: %s", applyResp.ErrorMessage)

	// Wait for waiting_for_cutover — the apply sits here while heartbeat keeps running
	var st string
	waitutil.Poll(t, 30*time.Second, 500*time.Millisecond,
		func() bool {
			err = db.QueryRowContext(ctx, "SELECT state FROM applies WHERE apply_identifier = ?", applyResp.ApplyId).Scan(&st)
			return err == nil && (st == state.Apply.WaitingForCutover || st == state.Apply.Completed || st == state.Apply.Failed)
		},
		func() string {
			return fmt.Sprintf("apply %s did not reach a stable state, last: %q", applyResp.ApplyId, st)
		},
	)
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
		ApplyId:     applyResp.ApplyId,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err, "cutover")

	// Wait for completion with a fresh deadline
	waitutil.Poll(t, 30*time.Second, 500*time.Millisecond,
		func() bool {
			err = db.QueryRowContext(ctx, "SELECT state FROM applies WHERE apply_identifier = ?", applyResp.ApplyId).Scan(&st)
			return err == nil && (st == state.Apply.Completed || st == state.Apply.Failed)
		},
		func() string {
			return fmt.Sprintf("apply %s did not reach terminal state, last: %q", applyResp.ApplyId, st)
		},
	)

	assert.Equal(t, state.Apply.Completed, st, "apply should be completed")
}

// TestLocalClient_Apply_AtomicRejectsMultiNamespace verifies that atomic mode
// (--defer-cutover) fails early when the plan has multiple namespaces, since
// Spirit can only connect to one MySQL database per execution.
func TestLocalClient_Apply_AtomicRejectsMultiNamespace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	// Create a plan with two namespaces directly in storage
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   "mysql",
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"ns_one": {
				Tables: []storage.TableChange{
					{Namespace: "ns_one", Table: "users", DDL: "ALTER TABLE users ADD COLUMN x INT", Operation: "alter"},
				},
			},
			"ns_two": {
				Tables: []storage.TableChange{
					{Namespace: "ns_two", Table: "orders", DDL: "ALTER TABLE orders ADD COLUMN y INT", Operation: "alter"},
				},
			},
		},
	}
	_, err = stor.Plans().Create(ctx, plan)
	require.NoError(t, err)

	// Apply with defer_cutover (atomic mode) — should fail because of 2 namespaces
	applyResp, err := client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:      plan.PlanIdentifier,
		Environment: "staging",
		Options:     map[string]string{"defer_cutover": "true"},
	})
	require.NoError(t, err)
	require.True(t, applyResp.Accepted)

	// The apply should fail with multi-namespace error
	require.Eventually(t, func() bool {
		applies, err := stor.Applies().GetByDatabase(ctx, "testdb", "mysql", "")
		if err != nil || len(applies) == 0 {
			return false
		}
		latest := applies[0]
		return latest.State == state.Apply.Failed &&
			strings.Contains(latest.ErrorMessage, "one namespace per apply")
	}, 10*time.Second, 200*time.Millisecond, "apply should fail with multi-namespace error")
}

// TestLocalClient_Apply_SequentialNamespaceMatchesTask verifies that in sequential
// mode, the namespace passed to the engine matches the task's namespace (not the
// deployment database name). This ensures progress key matching works.
func TestLocalClient_Apply_SequentialNamespaceMatchesTask(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()

	// Create a table to alter
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE users (id INT PRIMARY KEY)")
	require.NoError(t, err)
	_ = db.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	stor := createStorage(t, dsn)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	// Load current schema
	dbConn, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(dbConn)

	tables, err := table.LoadSchemaFromDB(ctx, dbConn)
	require.NoError(t, err)

	schemaFiles := make(map[string]string)
	for _, ts := range tables {
		if ts.Name == "users" {
			schemaFiles[ts.Name+".sql"] = "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))"
		} else {
			schemaFiles[ts.Name+".sql"] = ts.Schema
		}
	}

	// Plan with namespace "testdb" (matches the DSN database name)
	planResp, err := client.Plan(ctx, &ternv1.PlanRequest{
		Type:     "mysql",
		Database: "testdb",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"testdb": {Files: schemaFiles},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, planResp.Changes)

	// Apply in sequential mode (no defer_cutover)
	applyResp, err := client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:      planResp.PlanId,
		Environment: "staging",
	})
	require.NoError(t, err)
	require.True(t, applyResp.Accepted)

	// Wait for completion
	require.Eventually(t, func() bool {
		applies, _ := stor.Applies().GetByDatabase(ctx, "testdb", "mysql", "")
		if len(applies) == 0 {
			return false
		}
		return applies[0].State == state.Apply.Completed
	}, 30*time.Second, 500*time.Millisecond, "apply should complete")

	// Verify task has correct namespace and progress was persisted
	applies, _ := stor.Applies().GetByDatabase(ctx, "testdb", "mysql", "")
	require.NotEmpty(t, applies)
	tasks, err := stor.Tasks().GetByApplyID(ctx, applies[0].ID)
	require.NoError(t, err)
	require.NotEmpty(t, tasks)

	task := tasks[0]
	assert.Equal(t, "testdb", task.Namespace, "task namespace should match schema directory")
	assert.Equal(t, "users", task.TableName)
	assert.Equal(t, state.Task.Completed, task.State)
}

// TestLocalClient_Apply_FailedAtomicHasErrorMessage verifies that when Spirit
// reports an atomic apply failure, the apply pauses for operator retry and the
// failure reason is persisted on both the apply and task records.
func TestLocalClient_Apply_FailedAtomicHasErrorMessage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)

	// Create a plan with an ALTER on a table that doesn't exist
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   "mysql",
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {
				Tables: []storage.TableChange{
					{Namespace: "testdb", Table: "nonexistent_table", DDL: "ALTER TABLE `nonexistent_table` ADD COLUMN x INT", Operation: "alter"},
				},
			},
		},
	}
	_, err = stor.Plans().Create(ctx, plan)
	require.NoError(t, err)

	applyResp, err := client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:      plan.PlanIdentifier,
		Environment: "staging",
		Options:     map[string]string{"defer_cutover": "true"},
	})
	require.NoError(t, err)
	require.True(t, applyResp.Accepted, "apply should be accepted: %s", applyResp.ErrorMessage)

	// Spirit failures are retryable by default. The first failure should pause
	// in failed_retryable instead of becoming permanently failed.
	require.Eventually(t, func() bool {
		applies, _ := stor.Applies().GetByDatabase(ctx, "testdb", "mysql", "")
		if len(applies) == 0 {
			return false
		}
		return applies[0].State == state.Apply.FailedRetryable
	}, 30*time.Second, 500*time.Millisecond, "apply should pause for operator retry")

	applies, _ := stor.Applies().GetByDatabase(ctx, "testdb", "mysql", "")
	require.NotEmpty(t, applies)
	assert.NotEmpty(t, applies[0].ErrorMessage, "apply.ErrorMessage should contain the failure reason")
	t.Logf("apply error: %s", applies[0].ErrorMessage)

	// Verify task also has error
	tasks, err := stor.Tasks().GetByApplyID(ctx, applies[0].ID)
	require.NoError(t, err)
	require.NotEmpty(t, tasks)
	assert.Equal(t, state.Task.FailedRetryable, tasks[0].State)
	assert.Nil(t, tasks[0].CompletedAt)
	assert.NotEmpty(t, tasks[0].ErrorMessage, "task.ErrorMessage should contain the failure reason")
}

func TestLocalClient_AtomicRetryableFailureQueuesOperatorRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	defer utils.CloseAndLog(client)
	client.spiritEngine = &retryableFailureEngine{}

	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-retryable-%d", time.Now().UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    "development",
		CreatedAt:      time.Now(),
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {
				Tables: []storage.TableChange{
					{Namespace: "testdb", Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN retry_note VARCHAR(255)", Operation: "alter"},
				},
			},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)
	plan.ID = planID

	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-retryable-%d", time.Now().UnixNano()),
		PlanID:          planID,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "testdb",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Running,
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true}),
		Environment:     "development",
		StartedAt:       &now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := stor.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	tasks := []*storage.Task{
		{
			TaskIdentifier: fmt.Sprintf("task-retryable-users-%d", time.Now().UnixNano()),
			ApplyID:        applyID,
			PlanID:         planID,
			Database:       "testdb",
			DatabaseType:   storage.DatabaseTypeMySQL,
			Engine:         storage.EngineSpirit,
			State:          state.Task.Running,
			TableName:      "users",
			Namespace:      "testdb",
			DDL:            "ALTER TABLE `users` ADD COLUMN retry_note VARCHAR(255)",
			DDLAction:      "alter",
			Options:        []byte("{}"),
			Environment:    "development",
			CreatedAt:      now,
			UpdatedAt:      now,
		},
		{
			TaskIdentifier:  fmt.Sprintf("task-retryable-orders-%d", time.Now().UnixNano()),
			ApplyID:         applyID,
			PlanID:          planID,
			Database:        "testdb",
			DatabaseType:    storage.DatabaseTypeMySQL,
			Engine:          storage.EngineSpirit,
			State:           state.Task.Completed,
			TableName:       "orders",
			Namespace:       "testdb",
			DDL:             "ALTER TABLE `orders` ADD COLUMN retry_note VARCHAR(255)",
			DDLAction:       "alter",
			ProgressPercent: 100,
			Options:         []byte("{}"),
			Environment:     "development",
			CompletedAt:     &now,
			CreatedAt:       now,
			UpdatedAt:       now,
		},
	}
	for _, task := range tasks {
		taskID, err := stor.Tasks().Create(ctx, task)
		require.NoError(t, err)
		task.ID = taskID
	}

	// The engine reports a failed result with Retryable=true. The local Tern
	// worker should stop this attempt, keep the apply non-terminal, and leave
	// already-completed task work untouched for the operator retry.
	client.pollForCompletionAtomic(ctx, apply, tasks, &engine.Credentials{DSN: dsn}, nil)

	failedApply, err := stor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, failedApply)
	assert.Equal(t, state.Apply.FailedRetryable, failedApply.State)
	assert.Nil(t, failedApply.CompletedAt)
	assert.Equal(t, "temporary engine failure", failedApply.ErrorMessage)

	failedTasks, err := stor.Tasks().GetByApplyID(ctx, applyID)
	require.NoError(t, err)
	require.Len(t, failedTasks, 2)
	var retryTask, completedTask *storage.Task
	for _, task := range failedTasks {
		switch task.TableName {
		case "users":
			retryTask = task
		case "orders":
			completedTask = task
		}
	}
	require.NotNil(t, retryTask)
	require.NotNil(t, completedTask)
	assert.Equal(t, state.Task.FailedRetryable, retryTask.State)
	assert.Nil(t, retryTask.CompletedAt)
	assert.Equal(t, "temporary engine failure", retryTask.ErrorMessage)
	assert.Equal(t, state.Task.Completed, completedTask.State)
	assert.NotNil(t, completedTask.CompletedAt)

	// When the operator claims this apply, retryable tasks are queued for the
	// next dispatch attempt. Completed tasks stay completed so successful table
	// work is not repeated.
	client.prepareRetryableTasksForResume(ctx, failedApply, failedTasks)

	preparedTasks, err := stor.Tasks().GetByApplyID(ctx, applyID)
	require.NoError(t, err)
	for _, task := range preparedTasks {
		switch task.TableName {
		case "users":
			assert.Equal(t, state.Task.Pending, task.State)
			assert.Empty(t, task.ErrorMessage)
			assert.Nil(t, task.CompletedAt)
			assert.Equal(t, 1, task.Attempt)
		case "orders":
			assert.Equal(t, state.Task.Completed, task.State)
			assert.Equal(t, 0, task.Attempt)
		}
	}
}
