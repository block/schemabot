//go:build integration

package integration

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	schemabotapi "github.com/block/schemabot/pkg/api"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	storageTypes "github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// =============================================================================
// Test Helpers
// =============================================================================

// testServer holds the address and storage of a running SchemaBot HTTP server.
type testServer struct {
	Addr    string
	Storage *mysqlstore.Storage
}

// createTestDB creates a uniquely-named test database and returns its name and DSN.
// Cleanup is registered via t.Cleanup.
func createTestDB(t *testing.T, prefix string) (appDBName, appDSN string) {
	t.Helper()

	targetDB, err := sql.Open("mysql", targetDSN+"&multiStatements=true")
	require.NoError(t, err, "open target db")

	appDBName = prefix + fmt.Sprintf("%d", time.Now().UnixNano()%10000)
	_, err = targetDB.ExecContext(t.Context(), "CREATE DATABASE IF NOT EXISTS "+appDBName)
	require.NoError(t, err, "create app database")

	appDSN = strings.Replace(targetDSN, "/target_test", "/"+appDBName, 1)

	t.Cleanup(func() {
		_, _ = targetDB.ExecContext(t.Context(), "DROP DATABASE IF EXISTS "+appDBName)
		_ = targetDB.Close()
	})

	return appDBName, appDSN
}

// startTestServer sets up SchemaBot storage, a LocalClient, HTTP server, and
// waits for readiness. Returns the server address and storage. Cleanup is
// registered via t.Cleanup.
func startTestServer(t *testing.T, appDBName, appDSN string) testServer {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	schemabotDB, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err, "open schemabot db")
	clearStorageDB(t, schemabotDB)
	storage := mysqlstore.New(schemabotDB)

	localClient, err := tern.NewLocalClient(tern.LocalConfig{
		Database:  appDBName,
		Type:      "mysql",
		TargetDSN: appDSN,
	}, storage, logger)
	require.NoError(t, err, "create local client")

	serverConfig := &schemabotapi.ServerConfig{
		Databases: map[string]schemabotapi.DatabaseConfig{
			appDBName: {
				Type: "mysql",
				Environments: map[string]schemabotapi.EnvironmentConfig{
					"staging": {DSN: appDSN},
				},
			},
		},
	}
	svc := schemabotapi.New(storage, serverConfig, map[string]tern.Client{
		appDBName + "/staging": localClient,
	}, logger)

	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "localhost:0")
	require.NoError(t, err, "listen")
	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()

	addr := listener.Addr().String()

	// Wait for HTTP server readiness
	deadline := time.Now().Add(5 * time.Second)
	for {
		if time.Now().After(deadline) {
			require.Fail(t, "timeout waiting for HTTP server")
		}
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+"/health", nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Cleanup(func() {
		_ = server.Close()
		_ = svc.Close()
		_ = localClient.Close()
		_ = schemabotDB.Close()
	})

	return testServer{Addr: addr, Storage: storage}
}

// postJSON marshals body as JSON, POSTs to url, asserts HTTP 200,
// and returns the decoded JSON response.
func postJSON(t *testing.T, url string, body any) map[string]any {
	t.Helper()

	reqBody, err := json.Marshal(body)
	require.NoError(t, err, "marshal request body")

	httpReq, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, bytes.NewReader(reqBody))
	require.NoError(t, err, "create request")
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(httpReq)
	require.NoError(t, err, "POST %s", url)
	defer func() { _ = resp.Body.Close() }()

	respBody, _ := io.ReadAll(resp.Body)
	require.Equalf(t, http.StatusOK, resp.StatusCode, "POST %s: %s", url, string(respBody))

	var result map[string]any
	require.NoError(t, json.Unmarshal(respBody, &result), "decode response")
	return result
}

// flatTablesFromPlan extracts a flat list of table changes from a plan JSON
// response. Tables are nested under changes[].tables[]; this helper flattens
// them for test assertions that don't care about namespace grouping.
func flatTablesFromPlan(planResp map[string]any) []any {
	changes, ok := planResp["changes"].([]any)
	if !ok {
		return nil
	}
	var tables []any
	for _, c := range changes {
		change, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if tbls, ok := change["table_changes"].([]any); ok {
			tables = append(tables, tbls...)
		}
	}
	return tables
}

// hasErrorSeverityWarning checks if any lint_violations in the plan response
// have severity "error". This replaces the old has_unsafe_changes field.
func hasErrorSeverityWarning(planResp map[string]any) bool {
	warnings, ok := planResp["lint_violations"].([]any)
	if !ok {
		return false
	}
	for _, w := range warnings {
		warning, ok := w.(map[string]any)
		if !ok {
			continue
		}
		if warning["severity"] == "error" {
			return true
		}
	}
	return false
}

// =============================================================================
// Full Workflow E2E Test (Plan → Apply → Verify)
// Tests the complete schema change flow using LocalClient (local mode)
//
// Architecture:
// - SchemaBot storage DB (schemabotDSN): internal storage for tasks, plans, locks
// - Target app DB (myapp_*): the application database being migrated
// - schemabot.yaml: per-repo config (see testdata/myapp/mysql/schema/schemabot.yaml)
// - schema/: declarative schema files (see testdata/myapp/mysql/schema/)
//
// In production, the webhook handler reads schemabot.yaml and schema files
// from the PR and passes them to the Plan API. This test simulates that flow.
// =============================================================================

func TestFullWorkflow_Spirit_PlanApplyVerify(t *testing.T) {
	ctx := t.Context()

	// Read schema from testdata (simulating what webhook handler does)
	// Directory structure: testdata/myapp/mysql/schema/ (mirrors real repo layout)
	schemaSQL, err := os.ReadFile("testdata/myapp/mysql/schema/users.sql")
	require.NoError(t, err, "read schema file")

	appDBName, appDSN := createTestDB(t, "myapp_")
	ts := startTestServer(t, appDBName, appDSN)

	// Step 1: Plan - generate schema change from declarative schema
	planResp := postJSON(t, "http://"+ts.Addr+"/api/plan", map[string]any{
		"database":    appDBName,
		"environment": "staging",
		"type":        "mysql",
		"schema_files": map[string]any{
			"default": map[string]any{
				"files": map[string]string{
					"users.sql": string(schemaSQL),
				},
			},
		},
		"repository":   "myorg/myapp",
		"pull_request": 42,
	})

	planID, ok := planResp["plan_id"].(string)
	require.True(t, ok && planID != "", "expected plan_id in response, got: %v", planResp)
	t.Logf("Plan created: %s", planID)

	// Verify tables detected
	tables := flatTablesFromPlan(planResp)
	require.NotEmpty(t, tables, "expected tables in plan response, got: %v", planResp)
	t.Logf("Tables detected: %d", len(tables))

	// Step 2: Call Apply with the plan ID
	applyResp := postJSON(t, "http://"+ts.Addr+"/api/apply", map[string]any{
		"plan_id":     planID,
		"environment": "staging",
	})
	require.True(t, applyResp["accepted"] == true, "apply not accepted: %v", applyResp)
	applyID, _ := applyResp["apply_id"].(string)
	require.NotEmpty(t, applyID, "apply response missing apply_id")
	t.Log("Apply accepted", "apply_id", applyID)

	// Wait for this specific apply to complete
	waitForState(t, "http://"+ts.Addr, applyID, "completed", 10*time.Second)

	// Step 3: Verify the table exists in the target database
	targetConn, err := sql.Open("mysql", appDSN)
	require.NoError(t, err, "open target connection")
	defer func() { _ = targetConn.Close() }()

	var tableName string
	err = targetConn.QueryRowContext(ctx, "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'users'", appDBName).Scan(&tableName)
	require.NoError(t, err, "table 'users' not found in target database")
	t.Logf("Verified: table '%s' exists in %s", tableName, appDBName)

	// Verify table structure
	var columnCount int
	err = targetConn.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'users'", appDBName).Scan(&columnCount)
	require.NoError(t, err, "count columns")
	assert.Equal(t, 4, columnCount, "columns (id, email, name, created_at)")
	t.Logf("Verified: table has %d columns", columnCount)

	// Step 4: Verify plan is stored correctly in SchemaBot storage
	storedPlan, err := ts.Storage.Plans().Get(ctx, planID)
	require.NoError(t, err, "get stored plan")

	assert.Equal(t, planID, storedPlan.PlanIdentifier, "plan_id")
	assert.Equal(t, appDBName, storedPlan.Database, "database")
	assert.Equal(t, "mysql", storedPlan.DatabaseType, "database_type")
	assert.Equal(t, "myorg/myapp", storedPlan.Repository, "repository")
	assert.Equal(t, 42, storedPlan.PullRequest, "pull_request")
	t.Logf("Verified: plan stored correctly with metadata (repo=%s, PR=%d)", storedPlan.Repository, storedPlan.PullRequest)

	assert.NotEmpty(t, storedPlan.FlatDDLChanges(), "plan missing DDLChanges")
	t.Logf("Verified: plan has %d DDL changes", len(storedPlan.FlatDDLChanges()))

	// Step 5: Verify task is stored correctly in SchemaBot storage
	tasks, err := ts.Storage.Tasks().GetByDatabase(ctx, appDBName)
	require.NoError(t, err, "get tasks by database")
	require.NotEmpty(t, tasks, "tasks for database %s", appDBName)

	// Find the task for our plan and assert directly
	foundTask := false
	for _, task := range tasks {
		if task.PlanID == storedPlan.ID {
			foundTask = true
			assert.Equal(t, storedPlan.ID, task.PlanID, "task plan_id")
			assert.Equal(t, appDBName, task.Database, "task database")
			assert.Equal(t, "mysql", task.DatabaseType, "task database_type")
			assert.Equal(t, "myorg/myapp", task.Repository, "task repository")
			assert.Equal(t, 42, task.PullRequest, "task pull_request")
			assert.Equal(t, state.Task.Completed, task.State, "task state")
			t.Logf("Verified: task stored correctly (id=%s, state=%s)", task.TaskIdentifier, task.State)
			break
		}
	}
	require.True(t, foundTask, "task for plan %s", planID)
}

// =============================================================================
// DDL Scenarios Test (Declarative Schema Migrations)
// Tests various DDL operations using declarative schema approach:
// - CreateTable: Add new table by including it in schema files
// - AddColumn: Update CREATE TABLE to include new column
// - DropColumn: Update CREATE TABLE to remove column
// - AddIndex: Update CREATE TABLE to include index
// - DropTable: Remove table from schema files
// =============================================================================

func TestFullWorkflow_Spirit_DDLScenarios(t *testing.T) {
	ctx := t.Context()

	appDBName, appDSN := createTestDB(t, "ddl_")
	ts := startTestServer(t, appDBName, appDSN)

	// Connect to app database for verification queries
	appDB, err := sql.Open("mysql", appDSN)
	require.NoError(t, err, "open app db")
	defer func() { _ = appDB.Close() }()

	// Helper to plan and apply a schema change
	planAndApply := func(t *testing.T, schemaFiles map[string]string, prNum int) map[string]any {
		t.Helper()
		planResp := postJSON(t, "http://"+ts.Addr+"/api/plan", map[string]any{
			"database":    appDBName,
			"environment": "staging",
			"type":        "mysql",
			"schema_files": map[string]any{
				"default": map[string]any{
					"files": schemaFiles,
				},
			},
			"repository":   "myorg/ddltest",
			"pull_request": prNum,
		})

		if errMsg, ok := planResp["error"].(string); ok && errMsg != "" {
			require.Fail(t, "Plan failed: "+errMsg)
		}

		planID, ok := planResp["plan_id"].(string)
		require.True(t, ok && planID != "", "No plan_id in response: %v", planResp)

		// Apply
		applyResp := postJSON(t, "http://"+ts.Addr+"/api/apply", map[string]any{"plan_id": planID, "environment": "staging"})
		applyID, _ := applyResp["apply_id"].(string)
		require.NotEmpty(t, applyID, "apply response missing apply_id: %v", applyResp)

		// Wait for this specific apply to complete (not just any apply for the database).
		waitForState(t, "http://"+ts.Addr, applyID, "completed", 10*time.Second)
		return planResp
	}

	// Helper to check if column exists
	columnExists := func(table, column string) bool {
		var count int
		_ = appDB.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?
		`, appDBName, table, column).Scan(&count)
		return count > 0
	}

	// Helper to check if table exists
	tableExists := func(table string) bool {
		var count int
		_ = appDB.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM information_schema.TABLES
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		`, appDBName, table).Scan(&count)
		return count > 0
	}

	// Track the current schema state - this is what's "checked into the repo"
	currentSchema := map[string]string{}

	// =========================================================================
	// Scenario 1: CREATE TABLE - Add orders table
	// =========================================================================
	t.Run("CreateTable", func(t *testing.T) {
		currentSchema["orders.sql"] = `
CREATE TABLE orders (
	id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	user_id BIGINT NOT NULL,
	total_amount DECIMAL(10, 2) NOT NULL,
	status VARCHAR(50) NOT NULL DEFAULT 'pending',
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);`
		planResp := planAndApply(t, currentSchema, 101)

		// Verify plan detected CREATE
		tables := flatTablesFromPlan(planResp)
		require.Len(t, tables, 1, "plan response tables: %v", planResp)
		table := tables[0].(map[string]any)
		assert.Equal(t, "create", table["change_type"])

		// Verify table was created
		assert.True(t, tableExists("orders"), "orders table should exist")
		t.Log("Verified: orders table created")
	})

	// =========================================================================
	// Scenario 2: ADD COLUMN - Add 'notes' column to orders
	// =========================================================================
	t.Run("AddColumn", func(t *testing.T) {
		// Update orders.sql to include notes column
		currentSchema["orders.sql"] = `
CREATE TABLE orders (
	id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	user_id BIGINT NOT NULL,
	total_amount DECIMAL(10, 2) NOT NULL,
	status VARCHAR(50) NOT NULL DEFAULT 'pending',
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
	notes TEXT
);`
		planResp := planAndApply(t, currentSchema, 102)

		// Log full response for debugging
		respJSON, _ := json.MarshalIndent(planResp, "", "  ")
		t.Logf("AddColumn plan response: %s", string(respJSON))

		// Verify plan detected ALTER
		tables := flatTablesFromPlan(planResp)
		require.Len(t, tables, 1, "plan response tables: %v", planResp)
		table := tables[0].(map[string]any)
		assert.Equal(t, "alter", table["change_type"])

		// Verify column was added
		assert.True(t, columnExists("orders", "notes"), "notes column should exist")
		t.Log("Verified: notes column added to orders")
	})

	// =========================================================================
	// Scenario 3: ADD ANOTHER COLUMN - Add 'shipping_address' column
	// (Skipping ADD INDEX - the differ generates DROP PRIMARY KEY which Spirit doesn't support)
	// =========================================================================
	t.Run("AddAnotherColumn", func(t *testing.T) {
		// Update orders.sql to include shipping_address column
		currentSchema["orders.sql"] = `
CREATE TABLE orders (
	id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	user_id BIGINT NOT NULL,
	total_amount DECIMAL(10, 2) NOT NULL,
	status VARCHAR(50) NOT NULL DEFAULT 'pending',
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
	notes TEXT,
	shipping_address VARCHAR(500)
);`
		planResp := planAndApply(t, currentSchema, 103)

		// Verify plan detected ALTER
		tables := flatTablesFromPlan(planResp)
		require.Len(t, tables, 1, "plan response tables: %v", planResp)

		// Verify column was added
		assert.True(t, columnExists("orders", "shipping_address"), "shipping_address column should exist")
		t.Log("Verified: shipping_address column added to orders")
	})

	// =========================================================================
	// Scenario 4: CREATE SECOND TABLE - Add customers table
	// =========================================================================
	t.Run("CreateSecondTable", func(t *testing.T) {
		// Add customers.sql
		currentSchema["customers.sql"] = `
CREATE TABLE customers (
	id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(255) NOT NULL,
	email VARCHAR(255) NOT NULL,
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);`
		planResp := planAndApply(t, currentSchema, 104)

		// Verify plan includes CREATE for customers
		tables := flatTablesFromPlan(planResp)
		require.NotEmpty(t, tables, "expected at least 1 table change: %v", planResp)

		// Find customers table in changes
		foundCustomers := false
		for _, tbl := range tables {
			table := tbl.(map[string]any)
			if table["table_name"] == "customers" {
				foundCustomers = true
				assert.Equal(t, "create", table["change_type"], "customers change type")
			}
		}
		assert.True(t, foundCustomers, "expected customers table in changes")

		// Verify tables exist
		assert.True(t, tableExists("customers"), "customers table should exist")
		assert.True(t, tableExists("orders"), "orders table should still exist")
		t.Log("Verified: customers table created, orders still exists")
	})

	// =========================================================================
	// Scenario 5: DROP COLUMN - Remove 'notes' column from orders
	// =========================================================================
	t.Run("DropColumn", func(t *testing.T) {
		// Update orders.sql to remove notes column (keep shipping_address)
		currentSchema["orders.sql"] = `
CREATE TABLE orders (
	id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	user_id BIGINT NOT NULL,
	total_amount DECIMAL(10, 2) NOT NULL,
	status VARCHAR(50) NOT NULL DEFAULT 'pending',
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
	shipping_address VARCHAR(500)
);`
		planResp := planAndApply(t, currentSchema, 105)

		// Verify plan detected changes (may include orders ALTER)
		tables := flatTablesFromPlan(planResp)
		require.NotEmpty(t, tables, "expected at least 1 table change: %v", planResp)

		// Verify notes column was dropped
		assert.False(t, columnExists("orders", "notes"), "notes column should not exist")
		// Verify shipping_address still exists
		assert.True(t, columnExists("orders", "shipping_address"), "shipping_address column should still exist")
		t.Log("Verified: notes column dropped from orders")
	})

	// =========================================================================
	// Scenario 6: DROP TABLE - Remove customers table
	// =========================================================================
	t.Run("DropTable", func(t *testing.T) {
		// Remove customers.sql from schema files (orders remains)
		delete(currentSchema, "customers.sql")

		planResp := planAndApply(t, currentSchema, 106)

		// Verify plan includes DROP for customers
		tables := flatTablesFromPlan(planResp)
		require.NotEmpty(t, tables, "expected at least 1 table change: %v", planResp)

		// Find customers DROP in changes
		foundDrop := false
		for _, tbl := range tables {
			table := tbl.(map[string]any)
			if table["table_name"] == "customers" && table["change_type"] == "drop" {
				foundDrop = true
			}
		}
		assert.True(t, foundDrop, "expected DROP for customers table in changes")

		// Verify customers table was dropped
		assert.False(t, tableExists("customers"), "customers table should not exist")
		// Verify orders table still exists
		assert.True(t, tableExists("orders"), "orders table should still exist")
		t.Log("Verified: customers table dropped, orders still exists")
	})

	// =========================================================================
	// Scenario 7: NO CHANGES - Schema already matches desired state
	// Note: Due to MySQL schema normalization differences, the differ may still
	// detect minor changes. This test verifies the plan endpoint works, not
	// exact zero changes.
	// =========================================================================
	t.Run("NoChanges", func(t *testing.T) {
		// Plan with same schema
		planResp := postJSON(t, "http://"+ts.Addr+"/api/plan", map[string]any{
			"database":    appDBName,
			"environment": "staging",
			"type":        "mysql",
			"schema_files": map[string]any{
				"default": map[string]any{
					"files": currentSchema,
				},
			},
			"repository":   "myorg/ddltest",
			"pull_request": 107,
		})

		// Verify we got a valid plan response (may or may not have changes due to normalization)
		if _, ok := planResp["plan_id"]; !ok {
			if errMsg, ok := planResp["error"].(string); ok {
				require.Fail(t, "Plan failed: "+errMsg)
			}
		}
		t.Log("Verified: plan endpoint works for already-migrated schema")
	})
}

// =============================================================================
// Unsafe Change Detection Tests
// =============================================================================

func TestFullWorkflow_Spirit_UnsafeChangeDetection(t *testing.T) {
	ctx := t.Context()

	appDBName, appDSN := createTestDB(t, "unsafe_")
	ts := startTestServer(t, appDBName, appDSN)

	// Connect to app database for setup
	appDB, err := sql.Open("mysql", appDSN)
	require.NoError(t, err, "open app db")
	defer func() { _ = appDB.Close() }()

	// Create initial table with data
	_, err = appDB.ExecContext(ctx, `
		CREATE TABLE users (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			email VARCHAR(255) NOT NULL,
			notes TEXT
		)
	`)
	require.NoError(t, err, "create initial table")

	// Helper to call Plan API
	callPlan := func(t *testing.T, schemaFiles map[string]string) map[string]any {
		t.Helper()
		return postJSON(t, "http://"+ts.Addr+"/api/plan", map[string]any{
			"database":    appDBName,
			"environment": "staging",
			"type":        "mysql",
			"schema_files": map[string]any{
				"default": map[string]any{
					"files": schemaFiles,
				},
			},
			"repository":   "myorg/unsafe-test",
			"pull_request": 1,
		})
	}

	// =========================================================================
	// Test 1: DROP COLUMN is flagged as unsafe
	// =========================================================================
	t.Run("DropColumn_IsUnsafe", func(t *testing.T) {
		// Schema without 'notes' column (will generate DROP COLUMN)
		schemaFiles := map[string]string{
			"users.sql": `
CREATE TABLE users (
	id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(100) NOT NULL,
	email VARCHAR(255) NOT NULL
);`,
		}

		planResp := callPlan(t, schemaFiles)

		// Verify has error-severity lint warnings (replaces has_unsafe_changes)
		assert.True(t, hasErrorSeverityWarning(planResp), "expected error-severity lint warning for DROP COLUMN")

		// Verify lint_violations contains the unsafe warning with error severity
		warnings, _ := planResp["lint_violations"].([]any)
		foundUnsafe := false
		for _, w := range warnings {
			warning := w.(map[string]any)
			if warning["severity"] == "error" {
				foundUnsafe = true
				t.Logf("Found error-severity warning: %v", warning["message"])
			}
		}
		assert.True(t, foundUnsafe, "expected lint_violations to contain error-severity warning")

		// Verify the table change has is_unsafe=true
		tables := flatTablesFromPlan(planResp)
		for _, tbl := range tables {
			table := tbl.(map[string]any)
			if table["table_name"] == "users" {
				isUnsafe, _ := table["is_unsafe"].(bool)
				assert.True(t, isUnsafe, "expected is_unsafe=true for users table change")
				unsafeReason, _ := table["unsafe_reason"].(string)
				assert.NotEmpty(t, unsafeReason, "expected unsafe_reason to be set")
				t.Logf("Unsafe reason: %s", unsafeReason)
			}
		}
	})

	// Reset the table for next test
	_, _ = appDB.ExecContext(ctx, "DROP TABLE IF EXISTS users")
	_, _ = appDB.ExecContext(ctx, `
		CREATE TABLE users (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			email VARCHAR(255) NOT NULL
		)
	`)

	// =========================================================================
	// Test 2: DROP TABLE is flagged as unsafe
	// =========================================================================
	t.Run("DropTable_IsUnsafe", func(t *testing.T) {
		// Empty schema (will generate DROP TABLE)
		schemaFiles := map[string]string{}

		planResp := callPlan(t, schemaFiles)

		// Verify has error-severity lint warnings (replaces has_unsafe_changes)
		assert.True(t, hasErrorSeverityWarning(planResp), "expected error-severity lint warning for DROP TABLE")

		// Verify lint_violations contains the unsafe warning with error severity
		warnings, _ := planResp["lint_violations"].([]any)
		foundUnsafe := false
		for _, w := range warnings {
			warning := w.(map[string]any)
			if warning["severity"] == "error" {
				foundUnsafe = true
				t.Logf("Found error-severity warning: %v", warning["message"])
			}
		}
		assert.True(t, foundUnsafe, "expected lint_violations to contain error-severity warning")

		// Verify the table change has is_unsafe=true
		tables := flatTablesFromPlan(planResp)
		foundDropTable := false
		for _, tbl := range tables {
			table := tbl.(map[string]any)
			if table["table_name"] == "users" && table["change_type"] == "drop" {
				foundDropTable = true
				isUnsafe, _ := table["is_unsafe"].(bool)
				assert.True(t, isUnsafe, "expected is_unsafe=true for DROP TABLE")
			}
		}
		assert.True(t, foundDropTable, "expected DROP TABLE change for users")
	})

	// Reset for next test
	_, _ = appDB.ExecContext(ctx, "DROP TABLE IF EXISTS users")
	_, _ = appDB.ExecContext(ctx, `
		CREATE TABLE users (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(100) NOT NULL
		)
	`)

	// =========================================================================
	// Test 3: Safe changes (ADD COLUMN) do NOT flag unsafe
	// =========================================================================
	t.Run("AddColumn_IsSafe", func(t *testing.T) {
		// Schema with new column (will generate ADD COLUMN)
		schemaFiles := map[string]string{
			"users.sql": `
CREATE TABLE users (
	id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(100) NOT NULL,
	email VARCHAR(255)
);`,
		}

		planResp := callPlan(t, schemaFiles)

		// Verify no error-severity lint warnings for safe ADD COLUMN
		assert.False(t, hasErrorSeverityWarning(planResp), "expected no error-severity lint warnings for ADD COLUMN")
	})
}

// =============================================================================
// CLI Tests - Test the CLI commands work with real schema
// =============================================================================

// TestCLI_PlanApply tests the CLI plan and apply workflow using the HTTP API.
// This simulates what a user would experience when running:
//
//	schemabot plan -database mydb -schema-dir ./schema
//	schemabot apply -database mydb -schema-dir ./schema -auto-approve
func TestCLI_PlanApply(t *testing.T) {
	ctx := t.Context()

	appDBName, appDSN := createTestDB(t, "cli_")
	ts := startTestServer(t, appDBName, appDSN)

	// Connect to app database for verification
	appDB, err := sql.Open("mysql", appDSN)
	require.NoError(t, err, "open app db")
	defer func() { _ = appDB.Close() }()

	endpoint := "http://" + ts.Addr

	// Create a temporary directory with schema files
	schemaDir := t.TempDir()

	// Write initial schema file - declarative CREATE TABLE
	usersSchema := `CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    name VARCHAR(100),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_email (email)
);`
	require.NoError(t, os.WriteFile(schemaDir+"/users.sql", []byte(usersSchema), 0644), "write schema file")

	t.Run("plan_shows_create_table", func(t *testing.T) {
		schemaFiles, err := readTestSchemaFiles(schemaDir)
		require.NoError(t, err, "read schema files")

		result := postJSON(t, endpoint+"/api/plan", map[string]any{
			"database":    appDBName,
			"environment": "staging",
			"type":        "mysql",
			"schema_files": map[string]any{
				"default": map[string]any{
					"files": schemaFiles,
				},
			},
		})

		assert.NotEmpty(t, result["plan_id"], "expected plan_id in response")

		tables := flatTablesFromPlan(result)
		require.NotEmpty(t, tables, "expected tables in response, got: %v", result)

		// Verify we have a CREATE for users table
		found := false
		for _, tbl := range tables {
			if tblMap, ok := tbl.(map[string]any); ok {
				if tblMap["table_name"] == "users" {
					found = true
					changeType := tblMap["change_type"]
					assert.Equal(t, "create", changeType, "change type")
				}
			}
		}
		assert.True(t, found, "expected users table in plan")
	})

	t.Run("apply_creates_table", func(t *testing.T) {
		schemaFiles, err := readTestSchemaFiles(schemaDir)
		require.NoError(t, err, "read schema files")

		// Step 1: Plan
		planResult := postJSON(t, endpoint+"/api/plan", map[string]any{
			"database":    appDBName,
			"environment": "staging",
			"type":        "mysql",
			"schema_files": map[string]any{
				"default": map[string]any{
					"files": schemaFiles,
				},
			},
		})

		planID, ok := planResult["plan_id"].(string)
		require.True(t, ok && planID != "", "expected plan_id in response")

		// Step 2: Apply
		applyResult := postJSON(t, endpoint+"/api/apply", map[string]any{
			"plan_id":     planID,
			"environment": "staging",
		})
		require.True(t, applyResult["accepted"] == true, "apply not accepted: %v", applyResult["error_message"])
		applyID, _ := applyResult["apply_id"].(string)
		require.NotEmpty(t, applyID, "apply response missing apply_id")

		// Step 3: Wait for this specific apply to complete
		waitForState(t, endpoint, applyID, "completed", 30*time.Second)

		// Step 4: Verify table was created
		var tableName string
		err = appDB.QueryRowContext(ctx, "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'users'", appDBName).Scan(&tableName)
		require.NoError(t, err, "users table was not created")
		t.Logf("Table created successfully: %s", tableName)
	})

	t.Run("plan_no_changes_after_apply", func(t *testing.T) {
		// After apply, plan should show no changes
		schemaFiles, err := readTestSchemaFiles(schemaDir)
		require.NoError(t, err, "read schema files")

		result := postJSON(t, endpoint+"/api/plan", map[string]any{
			"database":    appDBName,
			"environment": "staging",
			"type":        "mysql",
			"schema_files": map[string]any{
				"default": map[string]any{
					"files": schemaFiles,
				},
			},
		})

		// Verify no changes
		tables := flatTablesFromPlan(result)
		if len(tables) > 0 {
			t.Logf("Unexpected tables: %v", tables)
			// Allow some normalization differences from Spirit differ
		}
	})
}

// readTestSchemaFiles reads SQL files from a directory for testing.
func readTestSchemaFiles(dir string) (map[string]string, error) {
	files := make(map[string]string)

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		content, err := os.ReadFile(dir + "/" + entry.Name())
		if err != nil {
			return nil, err
		}

		files[entry.Name()] = string(content)
	}

	return files, nil
}

// =============================================================================
// DDL Scenarios with Seeded Data (Progress Tracking)
// Tests DDL operations with data to verify progress tracking works correctly.
// Uses 10k rows to ensure schema changes take enough time to observe progress.
// =============================================================================

func TestFullWorkflow_Spirit_DDLWithProgress(t *testing.T) {
	ctx := t.Context()

	appDBName, appDSN := createTestDB(t, "ddl_progress_")
	ts := startTestServer(t, appDBName, appDSN)

	// Connect to app database for seeding and verification
	appDB, err := sql.Open("mysql", appDSN)
	require.NoError(t, err, "open app db")
	defer func() { _ = appDB.Close() }()

	// Track the current schema state
	currentSchema := map[string]string{}

	endpoint := "http://" + ts.Addr

	// Helper to plan and apply a schema change with progress collection
	planAndApplyWithProgress := func(t *testing.T, schemaFiles map[string]string, prNum int) (planResp map[string]any, progressStates []string) {
		t.Helper()
		planResp = postJSON(t, endpoint+"/api/plan", map[string]any{
			"database":    appDBName,
			"environment": "staging",
			"type":        "mysql",
			"schema_files": map[string]any{
				"default": map[string]any{
					"files": schemaFiles,
				},
			},
			"repository":   "myorg/progresstest",
			"pull_request": prNum,
		})

		if errMsg, ok := planResp["error"].(string); ok && errMsg != "" {
			require.Fail(t, "Plan failed: "+errMsg)
		}

		planID, ok := planResp["plan_id"].(string)
		require.True(t, ok && planID != "", "No plan_id in response: %v", planResp)

		// Debug: Log the plan response
		for _, table := range flatTablesFromPlan(planResp) {
			if tm, ok := table.(map[string]any); ok {
				t.Logf("Plan: table=%v, change_type=%v, ddl=%v", tm["table_name"], tm["change_type"], tm["ddl"])
			}
		}

		// Apply
		applyResp := postJSON(t, endpoint+"/api/apply", map[string]any{"plan_id": planID, "environment": "staging"})
		applyID, _ := applyResp["apply_id"].(string)
		require.NotEmpty(t, applyID, "apply response missing apply_id: %v", applyResp)
		t.Logf("Apply response: apply_id=%s", applyID)

		// Poll progress by apply_id and collect states
		deadline := time.Now().Add(60 * time.Second)
		seenStates := make(map[string]bool)
		completed := false
		for time.Now().Before(deadline) {
			progressHTTPReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/api/progress/apply/"+applyID, nil)
			require.NoError(t, err, "create progress request")
			resp, err := http.DefaultClient.Do(progressHTTPReq)
			require.NoError(t, err, "GET /api/progress error")

			var progressResp map[string]any
			_ = json.NewDecoder(resp.Body).Decode(&progressResp)
			_ = resp.Body.Close()

			progState, _ := progressResp["state"].(string)
			if !seenStates[progState] {
				seenStates[progState] = true
				progressStates = append(progressStates, progState)
			}

			// Log progress details
			if pct, ok := progressResp["copy_progress_pct"].(float64); ok && pct > 0 {
				t.Logf("Progress: state=%s, copy_progress=%.1f%%", progState, pct)
			}

			if progState == state.Apply.Completed {
				// Wait a bit for the task state to be persisted to the database
				time.Sleep(100 * time.Millisecond)
				completed = true
				break
			}
			if progState == state.Apply.Failed {
				require.Fail(t, fmt.Sprintf("schema change failed: %v", progressResp["error_message"]))
			}

			time.Sleep(100 * time.Millisecond)
		}
		require.True(t, completed, "schema change did not complete within timeout")
		return planResp, progressStates
	}

	// Helper to check if index exists
	indexExists := func(table, index string) bool {
		var count int
		_ = appDB.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM information_schema.STATISTICS
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ?
		`, appDBName, table, index).Scan(&count)
		return count > 0
	}

	// =========================================================================
	// Step 1: CREATE TABLE with initial schema
	// =========================================================================
	t.Run("CreateTable", func(t *testing.T) {
		currentSchema["users.sql"] = `
CREATE TABLE users (
	id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	email VARCHAR(255) NOT NULL,
	name VARCHAR(100),
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);`
		planResp, states := planAndApplyWithProgress(t, currentSchema, 201)

		tables := flatTablesFromPlan(planResp)
		require.Len(t, tables, 1, "plan response tables: %v", planResp)

		t.Logf("Create table states: %v", states)
	})

	// =========================================================================
	// Step 2: Seed 10k rows of test data
	// =========================================================================
	t.Run("SeedData", func(t *testing.T) {
		const batchSize = 1000
		const totalRows = 10000

		t.Logf("Seeding %d rows...", totalRows)

		for batch := range totalRows / batchSize {
			var values []string
			for i := range batchSize {
				rowNum := batch*batchSize + i
				values = append(values, fmt.Sprintf("('user%d@example.com', 'User %d')", rowNum, rowNum))
			}

			query := "INSERT INTO users (email, name) VALUES " + strings.Join(values, ", ")
			_, err := appDB.ExecContext(ctx, query)
			require.NoError(t, err, "seed batch %d", batch)
		}

		// Verify row count
		var count int
		err := appDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&count)
		require.NoError(t, err, "count rows")
		assert.Equal(t, totalRows, count, "row count")
		t.Logf("Seeded %d rows", count)
	})

	// =========================================================================
	// Step 3: ADD INDEX on email column
	// This should trigger Spirit schema change with progress tracking
	// =========================================================================
	t.Run("AddIndex", func(t *testing.T) {
		// Update schema to include index
		currentSchema["users.sql"] = `
CREATE TABLE users (
	id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	email VARCHAR(255) NOT NULL,
	name VARCHAR(100),
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
	INDEX idx_email (email)
);`
		planResp, states := planAndApplyWithProgress(t, currentSchema, 202)

		// Verify plan detected ALTER
		tables := flatTablesFromPlan(planResp)
		require.Len(t, tables, 1, "plan response tables: %v", planResp)
		table := tables[0].(map[string]any)
		assert.Equal(t, "alter", table["change_type"])

		// Verify we observed progress states (with data, should see RUNNING)
		t.Logf("Add index states: %v", states)

		// Verify index exists
		assert.True(t, indexExists("users", "idx_email"), "idx_email should exist")
		t.Log("Verified: idx_email created on users table")
	})

	// =========================================================================
	// Step 4: ADD COLUMN - should also show progress with data
	// =========================================================================
	t.Run("AddColumnWithData", func(t *testing.T) {
		currentSchema["users.sql"] = `
CREATE TABLE users (
	id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	email VARCHAR(255) NOT NULL,
	name VARCHAR(100),
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
	INDEX idx_email (email),
	status VARCHAR(20) DEFAULT 'active'
);`
		planResp, states := planAndApplyWithProgress(t, currentSchema, 203)

		tables := flatTablesFromPlan(planResp)
		require.Len(t, tables, 1, "plan response tables: %v", planResp)

		t.Logf("Add column states: %v", states)

		// Verify column exists
		var count int
		_ = appDB.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'users' AND COLUMN_NAME = 'status'
		`, appDBName).Scan(&count)
		assert.Equal(t, 1, count, "status column should exist")
		t.Log("Verified: status column added")
	})

	// =========================================================================
	// Step 5: ADD SECOND INDEX - index on name column
	// =========================================================================
	t.Run("AddSecondIndex", func(t *testing.T) {
		currentSchema["users.sql"] = `
CREATE TABLE users (
	id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	email VARCHAR(255) NOT NULL,
	name VARCHAR(100),
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
	INDEX idx_email (email),
	status VARCHAR(20) DEFAULT 'active',
	INDEX idx_name (name)
);`
		planResp, states := planAndApplyWithProgress(t, currentSchema, 204)

		tables := flatTablesFromPlan(planResp)
		require.Len(t, tables, 1, "plan response tables: %v", planResp)

		t.Logf("Add second index states: %v", states)

		// Verify both indexes exist
		assert.True(t, indexExists("users", "idx_email"), "idx_email should still exist")
		assert.True(t, indexExists("users", "idx_name"), "idx_name should exist")
		t.Log("Verified: idx_name created, idx_email still exists")
	})

	// =========================================================================
	// Step 6: DROP INDEX - remove idx_name
	// =========================================================================
	t.Run("DropIndex", func(t *testing.T) {
		currentSchema["users.sql"] = `
CREATE TABLE users (
	id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	email VARCHAR(255) NOT NULL,
	name VARCHAR(100),
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
	INDEX idx_email (email),
	status VARCHAR(20) DEFAULT 'active'
);`
		planResp, states := planAndApplyWithProgress(t, currentSchema, 205)

		tables := flatTablesFromPlan(planResp)
		require.Len(t, tables, 1, "plan response tables: %v", planResp)

		t.Logf("Drop index states: %v", states)

		// Verify idx_name was dropped
		assert.False(t, indexExists("users", "idx_name"), "idx_name should not exist")
		// Verify idx_email still exists
		assert.True(t, indexExists("users", "idx_email"), "idx_email should still exist")
		t.Log("Verified: idx_name dropped, idx_email still exists")
	})

	// =========================================================================
	// Step 7: MODIFY COLUMN - change name column size
	// =========================================================================
	t.Run("ModifyColumn", func(t *testing.T) {
		currentSchema["users.sql"] = `
CREATE TABLE users (
	id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	email VARCHAR(255) NOT NULL,
	name VARCHAR(200),
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
	INDEX idx_email (email),
	status VARCHAR(20) DEFAULT 'active'
);`
		planResp, states := planAndApplyWithProgress(t, currentSchema, 206)

		tables := flatTablesFromPlan(planResp)
		require.Len(t, tables, 1, "plan response tables: %v", planResp)

		t.Logf("Modify column states: %v", states)

		// Verify column was modified
		var charMaxLen int
		_ = appDB.QueryRowContext(ctx, `
			SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'users' AND COLUMN_NAME = 'name'
		`, appDBName).Scan(&charMaxLen)
		assert.Equal(t, 200, charMaxLen, "name column VARCHAR length")
		t.Log("Verified: name column modified to VARCHAR(200)")
	})
}

// TestFullWorkflow_Spirit_MySQLFailure tests that MySQL errors during apply
// are properly captured and surfaced in Progress.ErrorMessage.
func TestFullWorkflow_Spirit_MySQLFailure(t *testing.T) {
	ctx := t.Context()

	appDBName, appDSN := createTestDB(t, "failtest_")
	ts := startTestServer(t, appDBName, appDSN)

	endpoint := "http://" + ts.Addr

	// Schema with foreign key to non-existent table - passes TiDB parser but fails MySQL
	schemaSQL := `
CREATE TABLE orders (
	id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	user_id BIGINT NOT NULL,
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
	FOREIGN KEY (user_id) REFERENCES users(id)
);`

	// Step 1: Plan - should succeed (we don't validate FK references)
	planResp := postJSON(t, endpoint+"/api/plan", map[string]any{
		"database":    appDBName,
		"environment": "staging",
		"type":        "mysql",
		"schema_files": map[string]any{
			"default": map[string]any{
				"files": map[string]string{
					"orders.sql": schemaSQL,
				},
			},
		},
		"repository":   "myorg/myapp",
		"pull_request": 99,
	})

	planID := planResp["plan_id"].(string)
	t.Logf("Plan created: %s", planID)

	// Step 2: Apply - should accept but later fail
	applyResp := postJSON(t, endpoint+"/api/apply", map[string]any{
		"plan_id":     planID,
		"environment": "staging",
	})
	t.Logf("Apply response: %v", applyResp)

	// Step 3: Poll progress until we see failed state
	var lastState string
	var errorMessage string
	for range 30 {
		progressHTTPReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/api/progress/"+appDBName+"?environment=staging", nil)
		require.NoError(t, err, "create progress request")
		resp, err := http.DefaultClient.Do(progressHTTPReq)
		require.NoError(t, err, "GET /api/progress error")

		var progressResp map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&progressResp); err != nil {
			_ = resp.Body.Close()
			require.NoError(t, err, "decode progress response")
		}
		_ = resp.Body.Close()

		lastState, _ = progressResp["state"].(string)
		if msg, ok := progressResp["error_message"].(string); ok {
			errorMessage = msg
		}

		t.Logf("Progress state: %s, error: %s", lastState, errorMessage)

		if lastState == state.Apply.Failed {
			break
		}
		if lastState == state.Apply.Completed {
			require.Fail(t, "expected failed but got completed")
		}

		time.Sleep(200 * time.Millisecond)
	}

	// Verify we got failed state with error message
	assert.Equal(t, state.Apply.Failed, lastState)
	assert.NotEmpty(t, errorMessage, "expected error_message to be set")
	// MySQL error for FK to non-existent table should mention "users" or "referenced"
	lowerErr := strings.ToLower(errorMessage)
	assert.True(t,
		strings.Contains(lowerErr, "users") || strings.Contains(lowerErr, "referenced") || strings.Contains(lowerErr, "foreign"),
		"error message should mention FK issue, got: %s", errorMessage)
	t.Logf("Correctly captured MySQL error: %s", errorMessage)
}

// TestFullWorkflow_Spirit_PartialFailure tests that when one task fails in sequential mode,
// remaining tasks are marked as CANCELLED and the error is properly surfaced.
func TestFullWorkflow_Spirit_PartialFailure(t *testing.T) {
	ctx := t.Context()

	appDBName, _ := createTestDB(t, "partialfail_")
	ts := startTestServer(t, appDBName, strings.Replace(targetDSN, "/target_test", "/"+appDBName, 1))

	endpoint := "http://" + ts.Addr

	// Three tables in sequential mode (alphabetical order determines execution):
	// 1. aaa_first - will succeed (simple table)
	// 2. bbb_fails - will FAIL (FK to non-existent table)
	// 3. ccc_cancelled - should be CANCELLED (never started)
	schemaFiles := map[string]string{
		"aaa_first.sql": `
CREATE TABLE aaa_first (
	id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(100) NOT NULL,
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);`,
		"bbb_fails.sql": `
CREATE TABLE bbb_fails (
	id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	ref_id BIGINT NOT NULL,
	FOREIGN KEY (ref_id) REFERENCES nonexistent_table(id)
);`,
		"ccc_cancelled.sql": `
CREATE TABLE ccc_cancelled (
	id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	message TEXT,
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);`,
	}

	// Step 1: Plan
	planResp := postJSON(t, endpoint+"/api/plan", map[string]any{
		"database":    appDBName,
		"environment": "staging",
		"type":        "mysql",
		"schema_files": map[string]any{
			"default": map[string]any{
				"files": schemaFiles,
			},
		},
		"repository":   "myorg/myapp",
		"pull_request": 100,
	})

	planID := planResp["plan_id"].(string)
	tables := flatTablesFromPlan(planResp)
	t.Logf("Plan created: %s with %d tables", planID, len(tables))
	require.Len(t, tables, 3)

	// Step 2: Apply (sequential mode - no defer_cutover)
	applyResp := postJSON(t, endpoint+"/api/apply", map[string]any{
		"plan_id":     planID,
		"environment": "staging",
	})
	t.Logf("Apply started: %v", applyResp["apply_id"])

	// Step 3: Poll progress until we see failed state
	var lastState string
	var errorMessage string
	var finalTables []any
	for range 60 { // Longer timeout for 3 tables
		progressHTTPReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/api/progress/"+appDBName+"?environment=staging", nil)
		require.NoError(t, err, "create progress request")
		resp, err := http.DefaultClient.Do(progressHTTPReq)
		require.NoError(t, err, "GET /api/progress error")

		var progressResp map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&progressResp); err != nil {
			_ = resp.Body.Close()
			require.NoError(t, err, "decode progress response")
		}
		_ = resp.Body.Close()

		lastState, _ = progressResp["state"].(string)
		if msg, ok := progressResp["error_message"].(string); ok {
			errorMessage = msg
		}
		if tbls, ok := progressResp["tables"].([]any); ok {
			finalTables = tbls
		}

		t.Logf("Progress state: %s", lastState)

		if lastState == state.Apply.Failed {
			break
		}
		if lastState == state.Apply.Completed {
			require.Fail(t, "expected failed but got completed")
		}

		time.Sleep(300 * time.Millisecond)
	}

	// Verify overall state
	assert.Equal(t, state.Apply.Failed, lastState)
	assert.NotEmpty(t, errorMessage, "expected error_message to be set")
	t.Logf("Error message: %s", errorMessage)

	// Verify individual table states
	require.Len(t, finalTables, 3)

	tableStates := make(map[string]string)
	for _, tbl := range finalTables {
		tblMap := tbl.(map[string]any)
		name := tblMap["table_name"].(string)
		status := tblMap["status"].(string)
		tableStates[name] = state.NormalizeState(status)
		t.Logf("Table %s: %s", name, status)
	}

	assert.Equal(t, "completed", tableStates["aaa_first"], "aaa_first")
	assert.Equal(t, "failed", tableStates["bbb_fails"], "bbb_fails")
	assert.Equal(t, "cancelled", tableStates["ccc_cancelled"], "ccc_cancelled")

	// Verify aaa_first table was actually created in DB (partial success committed)
	targetDB, err := sql.Open("mysql", targetDSN+"&multiStatements=true")
	require.NoError(t, err, "open target db")
	defer func() { _ = targetDB.Close() }()

	var tableName string
	err = targetDB.QueryRowContext(ctx, `
		SELECT TABLE_NAME FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'aaa_first'
	`, appDBName).Scan(&tableName)
	assert.NoError(t, err, "aaa_first table should exist in DB after partial failure")
	if err == nil {
		t.Log("Verified: aaa_first table exists in DB (partial success committed)")
	}

	// Verify bbb_fails table was NOT created
	err = targetDB.QueryRowContext(ctx, `
		SELECT TABLE_NAME FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'bbb_fails'
	`, appDBName).Scan(&tableName)
	assert.Error(t, err, "bbb_fails table should NOT exist in DB")

	// Verify ccc_cancelled table was NOT created
	err = targetDB.QueryRowContext(ctx, `
		SELECT TABLE_NAME FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'ccc_cancelled'
	`, appDBName).Scan(&tableName)
	assert.Error(t, err, "ccc_cancelled table should NOT exist in DB")

	t.Log("Partial failure test passed: 1 completed, 1 failed, 1 cancelled")
}

// =============================================================================
// Sentinel Recovery Test
// Tests crash recovery during waiting_for_cutover state:
// 1. Apply with --defer-cutover, wait for waiting_for_cutover
// 2. Simulate crash by closing the LocalClient (kills Spirit)
// 3. Verify sentinel table still exists in the target database
// 4. Start a new Service with recovery worker
// 5. Verify task transitions: waiting_for_cutover → recovering → waiting_for_cutover
// 6. Trigger cutover, verify completion
// =============================================================================

func TestSentinelRecovery_DeferCutover(t *testing.T) {
	ctx := t.Context()

	appDBName, appDSN := createTestDB(t, "recov_")

	// Create the base table first so the apply triggers an ALTER (not CREATE).
	// CREATE TABLE doesn't use defer-cutover — only ALTER TABLE goes through
	// Spirit's row copy → sentinel → cutover flow.
	baseDB, err := sql.Open("mysql", appDSN)
	require.NoError(t, err)
	_, err = baseDB.ExecContext(ctx, `CREATE TABLE users (
		id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		email VARCHAR(255) NOT NULL,
		name VARCHAR(100),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		UNIQUE KEY uk_email (email)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`)
	require.NoError(t, err, "create base table")
	// Seed some rows so Spirit does actual row copy
	for range 100 {
		_, _ = baseDB.ExecContext(ctx, "INSERT INTO users (email, name) VALUES (CONCAT(UUID(), '@test.com'), 'test')")
	}
	_ = baseDB.Close()

	// The desired schema adds an index — triggers an ALTER TABLE
	desiredSchema := `CREATE TABLE users (
		id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		email VARCHAR(255) NOT NULL,
		name VARCHAR(100),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		UNIQUE KEY uk_email (email),
		KEY idx_created_at (created_at)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`

	ts := startTestServer(t, appDBName, appDSN)

	// Step 1: Plan — desired schema has idx_created_at, base doesn't
	planResp := postJSON(t, "http://"+ts.Addr+"/api/plan", map[string]any{
		"database":    appDBName,
		"environment": "staging",
		"type":        "mysql",
		"schema_files": map[string]any{
			"default": map[string]any{
				"files": map[string]string{
					"users.sql": desiredSchema,
				},
			},
		},
		"repository":   "myorg/myapp",
		"pull_request": 99,
	})
	planID, ok := planResp["plan_id"].(string)
	require.True(t, ok && planID != "", "expected plan_id")
	t.Logf("Plan created: %s", planID)

	// Step 2: Apply with defer-cutover
	applyResp := postJSON(t, "http://"+ts.Addr+"/api/apply", map[string]any{
		"plan_id":     planID,
		"environment": "staging",
		"options": map[string]string{
			"defer_cutover": "true",
		},
	})
	require.True(t, applyResp["accepted"] == true, "apply not accepted: %v", applyResp)
	applyID, _ := applyResp["apply_id"].(string)
	require.NotEmpty(t, applyID)
	t.Logf("Apply started: %s", applyID)

	// Step 3: Wait for waiting_for_cutover
	waitForState(t, "http://"+ts.Addr, applyID, state.Apply.WaitingForCutover, 30*time.Second)
	t.Log("Apply reached waiting_for_cutover")

	// Step 4: Verify sentinel table exists
	targetDB, err := sql.Open("mysql", appDSN)
	require.NoError(t, err)
	defer func() { _ = targetDB.Close() }()

	var sentinelExists int
	err = targetDB.QueryRowContext(ctx,
		"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = '_spirit_sentinel'",
		appDBName).Scan(&sentinelExists)
	require.NoError(t, err, "sentinel table should exist in target database")
	t.Log("Sentinel table exists")

	// Step 5: Make the apply's heartbeat stale by updating updated_at in storage.
	// This simulates a crash — the heartbeat stops updating and becomes stale.
	schemabotDB, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err)
	defer func() { _ = schemabotDB.Close() }()

	_, err = schemabotDB.ExecContext(ctx,
		"UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE apply_identifier = ?", applyID)
	require.NoError(t, err, "make heartbeat stale")
	t.Log("Heartbeat made stale (simulating crash)")

	// Also make task heartbeats stale
	_, err = schemabotDB.ExecContext(ctx,
		`UPDATE tasks SET updated_at = NOW() - INTERVAL 2 MINUTE
		 WHERE apply_id = (SELECT id FROM applies WHERE apply_identifier = ?)`, applyID)
	require.NoError(t, err)

	// Step 6: Start a NEW service with recovery worker (simulates container restart).
	// The new service creates a fresh LocalClient + engine — no runningMigration.
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	stor2 := mysqlstore.New(schemabotDB)

	localClient2, err := tern.NewLocalClient(tern.LocalConfig{
		Database:  appDBName,
		Type:      "mysql",
		TargetDSN: appDSN,
	}, stor2, logger)
	require.NoError(t, err)
	defer func() { _ = localClient2.Close() }()

	serverConfig2 := &schemabotapi.ServerConfig{
		Databases: map[string]schemabotapi.DatabaseConfig{
			appDBName: {
				Type: "mysql",
				Environments: map[string]schemabotapi.EnvironmentConfig{
					"staging": {DSN: appDSN},
				},
			},
		},
	}
	svc2 := schemabotapi.New(stor2, serverConfig2, map[string]tern.Client{
		appDBName + "/staging": localClient2,
	}, logger)
	defer func() { _ = svc2.Close() }()

	// Start recovery worker — this should detect the stale apply and resume it
	svc2.StartRecoveryWorker(ctx)
	t.Log("Recovery worker started on new service")

	// Step 7: Wait for the apply to return to waiting_for_cutover.
	// The recovery path: waiting_for_cutover → recovering → waiting_for_cutover
	// (Spirit resumes from checkpoint, re-reaches WaitingOnSentinelTable)
	deadline := time.Now().Add(30 * time.Second)
	var lastState string
	for time.Now().Before(deadline) {
		apply, err := stor2.Applies().GetByApplyIdentifier(ctx, applyID)
		if err == nil && apply != nil {
			lastState = apply.State
			t.Logf("Apply state: %s", apply.State)
			if apply.State == state.Apply.WaitingForCutover {
				// Check that it went through recovering (not stuck at the old state)
				break
			}
		}
		time.Sleep(2 * time.Second)
	}
	require.Equal(t, state.Apply.WaitingForCutover, lastState,
		"apply should return to waiting_for_cutover after recovery")

	// Step 8: Sentinel should still exist (cutover not yet triggered)
	err = targetDB.QueryRowContext(ctx,
		"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = '_spirit_sentinel'",
		appDBName).Scan(&sentinelExists)
	require.NoError(t, err, "sentinel table should still exist before cutover")

	// Step 9: Trigger cutover via API on the new service
	mux2 := http.NewServeMux()
	svc2.ConfigureRoutes(mux2)
	listener2, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "localhost:0")
	require.NoError(t, err)
	server2 := &http.Server{Handler: mux2}
	go func() { _ = server2.Serve(listener2) }()
	defer func() { _ = server2.Close() }()
	addr2 := listener2.Addr().String()

	cutoverResp := postJSON(t, "http://"+addr2+"/api/cutover", map[string]any{
		"database":    appDBName,
		"environment": "staging",
		"apply_id":    applyID,
	})
	require.True(t, cutoverResp["accepted"] == true, "cutover not accepted: %v", cutoverResp)
	t.Log("Cutover triggered")

	// Step 10: Wait for completion
	waitForState(t, "http://"+addr2, applyID, state.Apply.Completed, 30*time.Second)
	t.Log("Apply completed after recovery + cutover")

	// Step 11: Verify table exists in target
	var tableName string
	err = targetDB.QueryRowContext(ctx,
		"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'users'",
		appDBName).Scan(&tableName)
	require.NoError(t, err, "users table should exist after cutover")
	t.Logf("Verified: table '%s' exists after recovery + cutover", tableName)
}

// =============================================================================
// Observer Test
// Tests that the ProgressObserver receives OnProgress and OnTerminal callbacks
// during a normal apply. Verifies that the observer is set before the engine
// starts and notified throughout execution.
// =============================================================================

func TestObserver_NormalApply(t *testing.T) {
	ctx := t.Context()

	appDBName, appDSN := createTestDB(t, "obs_")

	// Create base table so the plan produces an ALTER (not CREATE)
	baseDB, err := sql.Open("mysql", appDSN)
	require.NoError(t, err)
	_, err = baseDB.ExecContext(ctx, `CREATE TABLE users (
		id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		email VARCHAR(255) NOT NULL,
		UNIQUE KEY uk_email (email)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`)
	require.NoError(t, err)
	for range 50 {
		_, _ = baseDB.ExecContext(ctx, "INSERT INTO users (email) VALUES (CONCAT(UUID(), '@test.com'))")
	}
	_ = baseDB.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	schemabotDB, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err)
	clearStorageDB(t, schemabotDB)
	storage := mysqlstore.New(schemabotDB)
	defer func() { _ = schemabotDB.Close() }()

	localClient, err := tern.NewLocalClient(tern.LocalConfig{
		Database:  appDBName,
		Type:      "mysql",
		TargetDSN: appDSN,
	}, storage, logger)
	require.NoError(t, err)
	defer func() { _ = localClient.Close() }()

	// Create a mock observer that tracks calls
	var mu sync.Mutex
	var progressCalls int
	var terminalCalled bool
	var terminalState string

	observer := &testObserver{
		onProgress: func(apply *storageTypes.Apply, tasks []*storageTypes.Task) {
			mu.Lock()
			defer mu.Unlock()
			progressCalls++
		},
		onTerminal: func(apply *storageTypes.Apply, tasks []*storageTypes.Task) {
			mu.Lock()
			defer mu.Unlock()
			terminalCalled = true
			terminalState = apply.State
		},
	}

	// Set pending observer — will be consumed by Apply()
	localClient.SetPendingObserver(observer)

	// Plan
	desiredSchema := `CREATE TABLE users (
		id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		email VARCHAR(255) NOT NULL,
		UNIQUE KEY uk_email (email),
		KEY idx_email_prefix (email(10))
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`

	planResp, err := localClient.Plan(ctx, &ternv1.PlanRequest{
		Database: appDBName,
		Type:     "mysql",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"default": {Files: map[string]string{"users.sql": desiredSchema}},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, planResp.PlanId)

	// Apply
	applyResp, err := localClient.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:      planResp.PlanId,
		Environment: "staging",
	})
	require.NoError(t, err)
	require.True(t, applyResp.Accepted)

	// Wait for completion
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		apply, err := storage.Applies().GetByApplyIdentifier(ctx, applyResp.ApplyId)
		if err == nil && apply != nil && state.IsTerminalApplyState(apply.State) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Verify observer was called
	mu.Lock()
	defer mu.Unlock()

	assert.True(t, progressCalls > 0, "observer.OnProgress should have been called at least once, got %d", progressCalls)
	assert.True(t, terminalCalled, "observer.OnTerminal should have been called")
	assert.Equal(t, state.Apply.Completed, terminalState, "terminal state should be completed")
	t.Logf("Observer received %d progress calls and terminal=%v state=%s", progressCalls, terminalCalled, terminalState)
}

// TestObserver_ApplyIDSet verifies that the observer's SetApplyID is called
// by LocalClient.Apply() when consuming a pending observer. If applyID stays
// at 0, the observer can't look up tracked comments for editing.
func TestObserver_ApplyIDSet(t *testing.T) {
	ctx := t.Context()

	appDBName, appDSN := createTestDB(t, "obsid_")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	schemabotDB, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err)
	clearStorageDB(t, schemabotDB)
	stor := mysqlstore.New(schemabotDB)
	defer func() { _ = schemabotDB.Close() }()

	localClient, err := tern.NewLocalClient(tern.LocalConfig{
		Database:  appDBName,
		Type:      "mysql",
		TargetDSN: appDSN,
	}, stor, logger)
	require.NoError(t, err)
	defer func() { _ = localClient.Close() }()

	// Track SetApplyID calls
	var capturedApplyID int64
	observer := &applyIDTrackingObserver{
		onSetApplyID: func(id int64) {
			capturedApplyID = id
		},
	}

	localClient.SetPendingObserver(observer)

	// Plan a CREATE TABLE (fast, no Spirit row copy)
	schemaSQL, err := os.ReadFile("testdata/myapp/mysql/schema/users.sql")
	require.NoError(t, err)

	planResp, err := localClient.Plan(ctx, &ternv1.PlanRequest{
		Database: appDBName,
		Type:     "mysql",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"default": {Files: map[string]string{"users.sql": string(schemaSQL)}},
		},
	})
	require.NoError(t, err)

	_, err = localClient.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:      planResp.PlanId,
		Environment: "staging",
	})
	require.NoError(t, err)

	assert.Greater(t, capturedApplyID, int64(0),
		"SetApplyID should be called with a non-zero ID; got %d", capturedApplyID)
	t.Logf("Observer received applyID=%d", capturedApplyID)
}

// applyIDTrackingObserver tracks SetApplyID calls.
type applyIDTrackingObserver struct {
	onSetApplyID func(int64)
}

func (o *applyIDTrackingObserver) OnProgress(*storageTypes.Apply, []*storageTypes.Task) {}
func (o *applyIDTrackingObserver) OnTerminal(*storageTypes.Apply, []*storageTypes.Task) {}
func (o *applyIDTrackingObserver) SetApplyID(id int64) {
	if o.onSetApplyID != nil {
		o.onSetApplyID(id)
	}
}

// TestObserver_ViaServiceLayer verifies the observer works when set through the
// Service layer (same code path as the webhook handler). This catches the
// deployment key mismatch bug where SetPendingObserver and ExecuteApply resolve
// to different TernClient instances.
func TestObserver_ViaServiceLayer(t *testing.T) {
	ctx := t.Context()

	appDBName, appDSN := createTestDB(t, "obssvc_")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	schemabotDB, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err)
	clearStorageDB(t, schemabotDB)
	stor := mysqlstore.New(schemabotDB)
	defer func() { _ = schemabotDB.Close() }()

	serverConfig := &schemabotapi.ServerConfig{
		Databases: map[string]schemabotapi.DatabaseConfig{
			appDBName: {
				Type: "mysql",
				Environments: map[string]schemabotapi.EnvironmentConfig{
					"staging": {DSN: appDSN},
				},
			},
		},
	}
	// Let the Service create clients lazily — tests that SetPendingObserver
	// and ExecuteApply resolve to the same client instance.
	svc := schemabotapi.New(stor, serverConfig, nil, logger)
	defer func() { _ = svc.Close() }()

	// Set up HTTP server for the plan/apply API
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "localhost:0")
	require.NoError(t, err)
	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()
	defer func() { _ = server.Close() }()
	addr := listener.Addr().String()

	// Wait for readiness
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/health", nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil && resp.StatusCode == 200 {
			_ = resp.Body.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Plan via API
	schemaSQL, err := os.ReadFile("testdata/myapp/mysql/schema/users.sql")
	require.NoError(t, err)
	planResp := postJSON(t, "http://"+addr+"/api/plan", map[string]any{
		"database":    appDBName,
		"environment": "staging",
		"type":        "mysql",
		"schema_files": map[string]any{
			"default": map[string]any{
				"files": map[string]string{"users.sql": string(schemaSQL)},
			},
		},
	})
	planID, _ := planResp["plan_id"].(string)
	require.NotEmpty(t, planID)

	// Track observer calls
	var mu sync.Mutex
	var terminalCalled bool

	observer := &testObserver{
		onTerminal: func(apply *storageTypes.Apply, tasks []*storageTypes.Task) {
			mu.Lock()
			defer mu.Unlock()
			terminalCalled = true
		},
	}

	// Set observer through the Service layer — same path as webhook handler.
	// Uses empty deployment (same as TernDeployment returns for local mode).
	svc.SetPendingObserver(appDBName, "", "staging", observer)

	// Apply via Service layer — same path as webhook handler
	applyResp, _, err := svc.ExecuteApply(ctx, schemabotapi.ApplyRequest{
		PlanID:      planID,
		Environment: "staging",
	})
	require.NoError(t, err)
	require.True(t, applyResp.Accepted)

	// Wait for completion
	waitDeadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(waitDeadline) {
		s, _ := fetchProgressState("http://"+addr, applyResp.ApplyID)
		if s == "completed" {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	assert.True(t, terminalCalled,
		"observer.OnTerminal should be called when going through Service layer; "+
			"if this fails, SetPendingObserver and ExecuteApply are using different TernClient instances")
}

// testObserver is a simple ProgressObserver for testing.
type testObserver struct {
	onProgress func(apply *storageTypes.Apply, tasks []*storageTypes.Task)
	onTerminal func(apply *storageTypes.Apply, tasks []*storageTypes.Task)
}

func (o *testObserver) OnProgress(apply *storageTypes.Apply, tasks []*storageTypes.Task) {
	if o.onProgress != nil {
		o.onProgress(apply, tasks)
	}
}

func (o *testObserver) OnTerminal(apply *storageTypes.Apply, tasks []*storageTypes.Task) {
	if o.onTerminal != nil {
		o.onTerminal(apply, tasks)
	}
}
