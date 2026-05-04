//go:build e2e

// Package local contains end-to-end tests that run against a full Docker Compose
// stack — all components (SchemaBot server, MySQL instances) are real containers
// orchestrated by docker-compose. Nothing is in-process or mocked.
//
// Run with: make test-e2e
package local

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/e2e/testutil"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/e2eutil"
	"github.com/block/schemabot/pkg/state"
)

// TestMain runs before all tests and cleans up leftover state from previous runs.
func TestMain(m *testing.M) {
	// Clean up SchemaBot's state tables to ensure fresh state
	clearSchemaBotStateImpl()
	// Clear LocalScale deploy requests to prevent stale deploys from blocking tests
	resetLocalScaleStateOrFatal()

	// Clean up any leftover tables from previous runs in testapp database
	// Keep only the base fixture tables (users, orders, products) that are part of the testapp schema
	baseFixtureTables := map[string]bool{
		"users":    true,
		"orders":   true,
		"products": true,
	}

	dsn := os.Getenv("E2E_TESTAPP_STAGING_DSN")
	if dsn != "" {
		db, err := sql.Open("mysql", dsn)
		if err == nil {
			// Find all tables that are NOT base fixtures
			rows, err := db.QueryContext(context.Background(), `
				SELECT TABLE_NAME FROM information_schema.TABLES
				WHERE TABLE_SCHEMA = 'testapp' AND TABLE_TYPE = 'BASE TABLE'
			`)
			if err == nil {
				var tablesToDrop []string
				for rows.Next() {
					var name string
					_ = rows.Scan(&name)
					// Drop if not a base fixture table
					if !baseFixtureTables[name] {
						tablesToDrop = append(tablesToDrop, name)
					}
				}
				_ = rows.Close()
				// Drop Spirit internal tables first (they may have FK constraints)
				for _, tbl := range tablesToDrop {
					if strings.HasPrefix(tbl, "_") {
						_, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+tbl+"`")
					}
				}
				// Then drop test tables
				for _, tbl := range tablesToDrop {
					if !strings.HasPrefix(tbl, "_") {
						_, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+tbl+"`")
					}
				}
				if len(tablesToDrop) > 0 {
					log.Printf("Cleaned up %d leftover test tables", len(tablesToDrop))
				}
			}
			_ = db.Close()
		}
	}
	os.Exit(m.Run())
}

// =============================================================================
// Basic Smoke Tests
// =============================================================================

func TestLocal_SchemaBot_Health(t *testing.T) {
	baseURL := schemabotURL(t)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/health", nil)
	require.NoError(t, err, "create request")
	resp, err := http.DefaultClient.Do(req) //nolint:bodyclose // closed below
	require.NoError(t, err, "GET /health")
	defer utils.CloseAndLog(resp.Body)

	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "health check status: %s", string(body))

	var result map[string]string
	require.NoError(t, json.Unmarshal(body, &result), "decode response")

	assert.Equal(t, "ok", result["status"], "health status")
}

func TestLocal_SchemaBot_SchemaApplied(t *testing.T) {
	dsn := mysqlDSN(t)

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "connect to MySQL")
	defer utils.CloseAndLog(db)

	expectedTables := []string{
		"locks",
		"checks",
		"settings",
		"plans",
		"tasks",
		"vitess_apply_data",
		"vitess_tasks",
	}

	for _, table := range expectedTables {
		var count int
		err := db.QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'schemabot' AND table_name = ?",
			table,
		).Scan(&count)
		require.NoErrorf(t, err, "query for table %s", table)
		assert.Equalf(t, 1, count, "table %s not found in schemabot database", table)
	}
}

func TestLocal_Demo_TestAppTablesCreated(t *testing.T) {
	// Skip this test if demo tables don't exist (requires 'make demo' first)
	stagingDSN := testappStagingDSN(t)
	db, err := sql.Open("mysql", stagingDSN)
	if err != nil {
		t.Skip("Cannot connect to staging database")
	}
	defer utils.CloseAndLog(db)

	var count int
	_ = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'testapp' AND table_name = 'users'").Scan(&count)
	if count == 0 {
		t.Skip("Demo tables not present (run 'make demo' first)")
	}

	productionDSN := testappProductionDSN(t)
	expectedTables := []string{"users", "orders"}

	t.Run("staging", func(t *testing.T) {
		for _, table := range expectedTables {
			var cnt int
			err := db.QueryRowContext(t.Context(),
				"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'testapp' AND table_name = ?",
				table,
			).Scan(&cnt)
			require.NoErrorf(t, err, "query for table %s", table)
			assert.Equalf(t, 1, cnt, "table %s not found in staging testapp database", table)
		}
	})

	t.Run("production", func(t *testing.T) {
		prodDB, err := sql.Open("mysql", productionDSN)
		require.NoError(t, err, "connect to production MySQL")
		defer utils.CloseAndLog(prodDB)

		for _, table := range expectedTables {
			var cnt int
			err := prodDB.QueryRowContext(t.Context(),
				"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'testapp' AND table_name = ?",
				table,
			).Scan(&cnt)
			require.NoErrorf(t, err, "query for table %s", table)
			assert.Equalf(t, 1, cnt, "table %s not found in production testapp database", table)
		}
	})
}

// =============================================================================
// Settings Tests
// =============================================================================

func TestLocal_Settings_SetGetDelete(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	// Set require_review
	out := runCLI(t, binPath, "settings", "require_review", "true", "--endpoint", endpoint)
	e2eutil.AssertContains(t, out, "require_review")
	e2eutil.AssertContains(t, out, "true")

	// Get require_review
	out = runCLI(t, binPath, "settings", "require_review", "--endpoint", endpoint)
	e2eutil.AssertContains(t, out, "require_review")
	e2eutil.AssertContains(t, out, "true")

	// Set to false
	out = runCLI(t, binPath, "settings", "require_review", "false", "--endpoint", endpoint)
	e2eutil.AssertContains(t, out, "require_review")
	e2eutil.AssertContains(t, out, "false")

	// Verify it reads back as false
	out = runCLI(t, binPath, "settings", "require_review", "--endpoint", endpoint)
	e2eutil.AssertContains(t, out, "false")
}

func TestLocal_Settings_ScopedKey(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	// Set repo-scoped key
	out := runCLI(t, binPath, "settings", "require_review:octocat/hello-world", "true", "--endpoint", endpoint)
	e2eutil.AssertContains(t, out, "require_review:octocat/hello-world")

	// Clean up
	runCLI(t, binPath, "settings", "require_review:octocat/hello-world", "false", "--endpoint", endpoint)
}

func TestLocal_Settings_UnknownKey(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	_, err := runCLIWithError(t, binPath, "settings", "bogus_setting", "true", "--endpoint", endpoint)
	require.Error(t, err, "expected error for unknown setting key")
}

func TestLocal_Settings_List(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	out := runCLI(t, binPath, "settings", "--endpoint", endpoint)
	e2eutil.AssertContains(t, out, "require_review")
	e2eutil.AssertContains(t, out, "spirit_debug_logs")
}

// =============================================================================
// Plan Tests
// =============================================================================

func TestLocal_Plan_Help(t *testing.T) {
	binPath := buildCLI(t)

	out := runCLI(t, binPath, "plan", "--help")
	e2eutil.AssertContains(t, out, "--schema_dir")
	e2eutil.AssertContains(t, out, "--endpoint")
	e2eutil.AssertContains(t, out, "--environment")
}

func TestLocal_Plan_MissingConfig(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	// Run plan in a directory without schemabot.yaml
	emptyDir := t.TempDir()
	_, err := e2eutil.RunCLIWithErrorInDir(t, binPath, emptyDir, "plan", "--endpoint", endpoint)
	require.Error(t, err, "expected error for missing schemabot.yaml")
}

func TestLocal_Plan_NewTable(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	tableName := uniqueTableName("plan_products")
	defer dropTestTable(t, tableName)

	schemaDir := newSchemaDir(t)
	e2eutil.WriteFile(t, filepath.Join(schemaDir, tableName+".sql"), fmt.Sprintf(`
CREATE TABLE %s (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price_cents BIGINT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
`, tableName))

	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "plan", "-e", "staging", "--endpoint", endpoint)
	e2eutil.AssertContains(t, out, "Schema Change Plan")
	e2eutil.AssertContains(t, out, "CREATE TABLE")
	e2eutil.AssertContains(t, out, tableName)
}

func TestLocal_Plan_AddIndex(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	tableName := uniqueTableName("plan_items")

	createTestTable(t, tableName, fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			category VARCHAR(100),
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)
	`, tableName))

	schemaDir := newSchemaDir(t)
	e2eutil.WriteFile(t, filepath.Join(schemaDir, tableName+".sql"), fmt.Sprintf(`
CREATE TABLE %s (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_category (category)
);
`, tableName))

	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "plan", "-e", "staging", "--endpoint", endpoint)
	e2eutil.AssertContains(t, out, "ADD INDEX")
	e2eutil.AssertContains(t, out, "idx_category")
}

func TestLocal_Plan_UnsafeChanges(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	tableName := uniqueTableName("plan_accounts")

	createTestTable(t, tableName, fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			email VARCHAR(255) NOT NULL,
			legacy_field VARCHAR(100),
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)
	`, tableName))

	schemaDir := newSchemaDir(t)
	e2eutil.WriteFile(t, filepath.Join(schemaDir, tableName+".sql"), fmt.Sprintf(`
CREATE TABLE %s (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
`, tableName))

	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "plan", "-e", "staging", "--endpoint", endpoint)
	e2eutil.AssertContains(t, out, "DROP COLUMN")
	e2eutil.AssertContains(t, out, "legacy_field")
	// Unsafe changes should be shown with ⛔ (not ⚠️ lint warning)
	e2eutil.AssertContains(t, out, "Unsafe Changes Detected")
}

func TestLocal_Plan_NamespaceSubdirectory(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	tableName := uniqueTableName("plan_ns")
	defer dropTestTable(t, tableName)

	// Create schema dir with a namespace subdirectory instead of flat files.
	// The subdirectory name "testapp" becomes the namespace key.
	schemaDir := newSchemaDir(t)
	nsDir := filepath.Join(schemaDir, "testapp")
	require.NoError(t, os.MkdirAll(nsDir, 0755))
	e2eutil.WriteFile(t, filepath.Join(nsDir, tableName+".sql"), fmt.Sprintf(`
CREATE TABLE %s (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
`, tableName))

	// Write existing table schemas into the namespace subdir so the plan
	// only shows changes for our new table (not drops for missing tables).
	writeExistingTablesSchema(t, nsDir)

	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "plan", "-e", "staging", "--endpoint", endpoint)
	e2eutil.AssertContains(t, out, "Schema Change Plan")
	e2eutil.AssertContains(t, out, "CREATE TABLE")
	e2eutil.AssertContains(t, out, tableName)
}

// =============================================================================
// Apply Tests
// =============================================================================

func TestLocal_Apply_Help(t *testing.T) {
	binPath := buildCLI(t)

	out := runCLI(t, binPath, "apply", "--help")
	e2eutil.AssertContains(t, out, "--schema_dir")
	e2eutil.AssertContains(t, out, "--endpoint")
	e2eutil.AssertContains(t, out, "--environment")
	e2eutil.AssertContains(t, out, "--defer-cutover")
	e2eutil.AssertContains(t, out, "--allow-unsafe")
}

func TestLocal_Apply_CreateTable(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	tableName := uniqueTableName("apply_widgets")
	defer dropTestTable(t, tableName)

	db := openTestappStaging(t)

	schemaDir := newSchemaDir(t)

	// Write existing tables to avoid DROP TABLE
	writeExistingTablesSchema(t, schemaDir)

	e2eutil.WriteFile(t, filepath.Join(schemaDir, tableName+".sql"), fmt.Sprintf(`
CREATE TABLE %s (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    weight_grams INT DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
`, tableName))

	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "apply",
		"-s", ".",
		"-e", "staging",
		"--endpoint", endpoint,
		"-y",
		"--watch=false",
	)
	e2eutil.AssertContains(t, out, "Apply started")

	testutil.WaitForState(t, endpoint, e2eutil.ParseApplyID(t, out), state.Apply.Completed, 10*time.Second)

	var count int
	err := db.QueryRowContext(t.Context(), `
		SELECT COUNT(*) FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = 'testapp' AND TABLE_NAME = ?
	`, tableName).Scan(&count)
	require.NoError(t, err, "query for table")
	assert.Equalf(t, 1, count, "table %s should exist after apply", tableName)
}

func TestLocal_Apply_CreateMultipleTables(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	table1 := uniqueTableName("multi_alpha")
	table2 := uniqueTableName("multi_beta")
	table3 := uniqueTableName("multi_gamma")
	defer dropTestTable(t, table1)
	defer dropTestTable(t, table2)
	defer dropTestTable(t, table3)

	db := openTestappStaging(t)
	schemaDir := newSchemaDir(t)
	writeExistingTablesSchema(t, schemaDir)

	for _, tbl := range []string{table1, table2, table3} {
		e2eutil.WriteFile(t, filepath.Join(schemaDir, tbl+".sql"), fmt.Sprintf(`
CREATE TABLE %s (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
`, tbl))
	}

	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "apply",
		"-s", ".", "-e", "staging", "--endpoint", endpoint, "-y", "--watch=false",
	)
	e2eutil.AssertContains(t, out, "Apply started")

	testutil.WaitForState(t, endpoint, e2eutil.ParseApplyID(t, out), state.Apply.Completed, 15*time.Second)

	for _, tbl := range []string{table1, table2, table3} {
		var count int
		err := db.QueryRowContext(t.Context(), `
			SELECT COUNT(*) FROM information_schema.TABLES
			WHERE TABLE_SCHEMA = 'testapp' AND TABLE_NAME = ?
		`, tbl).Scan(&count)
		require.NoError(t, err, "query for table %s", tbl)
		assert.Equalf(t, 1, count, "table %s should exist after apply", tbl)
	}
}

func TestLocal_Apply_BlocksUnsafeWithoutFlag(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	tableName := uniqueTableName("apply_settings")

	createTestTable(t, tableName, fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			key_name VARCHAR(255) NOT NULL,
			value TEXT,
			deprecated_field VARCHAR(100)
		)
	`, tableName))

	schemaDir := newSchemaDir(t)
	writeExistingTablesSchema(t, schemaDir)
	e2eutil.WriteFile(t, filepath.Join(schemaDir, tableName+".sql"), fmt.Sprintf(`
CREATE TABLE %s (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    key_name VARCHAR(255) NOT NULL,
    value TEXT
);
`, tableName))

	_, err := e2eutil.RunCLIWithErrorInDir(t, binPath, schemaDir, "apply",
		"-s", ".",
		"-e", "staging",
		"--endpoint", endpoint,
		"-y",
		"--watch=false",
	)
	require.Error(t, err, "expected apply to fail without --allow-unsafe for DROP COLUMN")
}

func TestLocal_Apply_AllowsUnsafeWithFlag(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	tableName := uniqueTableName("apply_configs")

	createTestTable(t, tableName, fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			old_value TEXT
		)
	`, tableName))

	schemaDir := newSchemaDir(t)
	writeExistingTablesSchema(t, schemaDir)
	e2eutil.WriteFile(t, filepath.Join(schemaDir, tableName+".sql"), fmt.Sprintf(`
CREATE TABLE %s (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);
`, tableName))

	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "apply",
		"-s", ".",
		"-e", "staging",
		"--endpoint", endpoint,
		"-y",
		"--allow-unsafe",
		"--watch=false",
	)
	e2eutil.AssertContains(t, out, "Apply started")

	testutil.WaitForState(t, endpoint, e2eutil.ParseApplyID(t, out), state.Apply.Completed, 10*time.Second)
}

func TestLocal_Apply_DropIndexBlockedWithoutFlag(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	tableName := uniqueTableName("apply_indexed")

	// Create table WITH an index
	createTestTable(t, tableName, fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			email VARCHAR(255) NOT NULL,
			name VARCHAR(255),
			INDEX idx_email (email)
		)
	`, tableName))

	schemaDir := newSchemaDir(t)
	writeExistingTablesSchema(t, schemaDir)
	// Schema WITHOUT the index (will cause DROP INDEX)
	e2eutil.WriteFile(t, filepath.Join(schemaDir, tableName+".sql"), fmt.Sprintf(`
CREATE TABLE %s (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    name VARCHAR(255)
);
`, tableName))

	out, err := e2eutil.RunCLIWithErrorInDir(t, binPath, schemaDir, "apply",
		"-s", ".",
		"-e", "staging",
		"--endpoint", endpoint,
		"-y",
		"--watch=false",
	)

	// Should fail without --allow-unsafe
	require.Error(t, err, "expected apply to fail without --allow-unsafe for DROP INDEX")

	// Verify the output contains expected messages
	e2eutil.AssertContains(t, out, "Unsafe Changes Detected")
	e2eutil.AssertContains(t, out, "DROP INDEX")
	e2eutil.AssertContains(t, out, "--allow-unsafe")
	// Should also show the plan
	e2eutil.AssertContains(t, out, "ALTER TABLE")
}

func TestLocal_Apply_DropIndexAllowedWithFlag(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)
	ensureNoActiveChange(t, endpoint)

	tableName := uniqueTableName("apply_drop_idx")

	// Create table WITH an index
	createTestTable(t, tableName, fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			email VARCHAR(255) NOT NULL,
			INDEX idx_email (email)
		)
	`, tableName))

	schemaDir := newSchemaDir(t)
	writeExistingTablesSchema(t, schemaDir)
	// Schema WITHOUT the index (will cause DROP INDEX)
	e2eutil.WriteFile(t, filepath.Join(schemaDir, tableName+".sql"), fmt.Sprintf(`
CREATE TABLE %s (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL
);
`, tableName))

	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "apply",
		"-s", ".",
		"-e", "staging",
		"--endpoint", endpoint,
		"-y",
		"--allow-unsafe",
		"--watch=false",
	)

	// Should succeed with --allow-unsafe
	e2eutil.AssertContains(t, out, "Apply started")

	testutil.WaitForState(t, endpoint, e2eutil.ParseApplyID(t, out), state.Apply.Completed, 10*time.Second)

	// Verify index was actually dropped
	db := openTestappStaging(t)

	var indexCount int
	err := db.QueryRowContext(t.Context(), `
		SELECT COUNT(*) FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = 'testapp' AND TABLE_NAME = ? AND INDEX_NAME = 'idx_email'
	`, tableName).Scan(&indexCount)
	require.NoError(t, err, "check index")
	assert.Equal(t, 0, indexCount, "expected index idx_email to be dropped")
}

func TestLocal_Apply_DropTableBlockedWithoutFlag(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)
	ensureNoActiveChange(t, endpoint)

	tableToDrop := uniqueTableName("apply_to_drop")
	keeperTable := uniqueTableName("keeper_nodrop")

	// Create a table that we'll try to drop
	createTestTable(t, tableToDrop, fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			data VARCHAR(255)
		)
	`, tableToDrop))

	// Create a keeper table that stays in the schema (schema dir needs at least one .sql file)
	createTestTable(t, keeperTable, fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY
		)
	`, keeperTable))

	schemaDir := newSchemaDir(t)
	// Write base fixture schemas to prevent dropping them
	writeBaseFixtureSchemas(t, schemaDir)
	// Write keeper table schema manually
	e2eutil.WriteFile(t, filepath.Join(schemaDir, keeperTable+".sql"), fmt.Sprintf(`
CREATE TABLE %s (
  id bigint NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (id)
);
`, keeperTable))

	out, err := e2eutil.RunCLIWithErrorInDir(t, binPath, schemaDir, "apply",
		"-s", ".",
		"-e", "staging",
		"--endpoint", endpoint,
		"-y",
		"--watch=false",
	)

	// Should fail without --allow-unsafe (because tableToDrop will be dropped)
	require.Error(t, err, "expected apply to fail without --allow-unsafe for DROP TABLE")

	// Verify the output contains expected messages
	e2eutil.AssertContains(t, out, "Unsafe Changes Detected")
	e2eutil.AssertContains(t, out, "DROP TABLE")
	e2eutil.AssertContains(t, out, "--allow-unsafe")
}

func TestLocal_Apply_DropTableAllowedWithFlag(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)
	ensureNoActiveChange(t, endpoint)

	tableToDrop := uniqueTableName("apply_dropped")
	keeperTable := uniqueTableName("keeper_stays")

	// Create a table that we'll drop
	createTestTable(t, tableToDrop, fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			data VARCHAR(255)
		)
	`, tableToDrop))

	// Create a keeper table that stays in the schema (schema dir needs at least one .sql file)
	createTestTable(t, keeperTable, fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY
		)
	`, keeperTable))

	schemaDir := newSchemaDir(t)
	// Write base fixture schemas to prevent dropping them
	writeBaseFixtureSchemas(t, schemaDir)
	// Write keeper table schema manually
	e2eutil.WriteFile(t, filepath.Join(schemaDir, keeperTable+".sql"), fmt.Sprintf(`
CREATE TABLE %s (
  id bigint NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (id)
);
`, keeperTable))

	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "apply",
		"-s", ".",
		"-e", "staging",
		"--endpoint", endpoint,
		"-y",
		"--allow-unsafe",
		"--watch=false",
	)

	// Should succeed with --allow-unsafe
	e2eutil.AssertContains(t, out, "Apply started")

	testutil.WaitForState(t, endpoint, e2eutil.ParseApplyID(t, out), state.Apply.Completed, 10*time.Second)

	// Verify test table was actually dropped
	db := openTestappStaging(t)

	var tableCount int
	err := db.QueryRowContext(t.Context(), `
		SELECT COUNT(*) FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = 'testapp' AND TABLE_NAME = ?
	`, tableToDrop).Scan(&tableCount)
	require.NoError(t, err, "check table")
	assert.Equal(t, 0, tableCount, "expected table to be dropped")
}

// =============================================================================
// Defer Cutover Tests
// =============================================================================

func TestLocal_DeferCutover_FullWorkflow(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)
	ensureNoActiveChange(t, endpoint)

	tableName := uniqueTableName("defer_invoices")
	defer dropTestTable(t, tableName)

	db := openTestappStaging(t)

	_, err := db.ExecContext(t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			customer_id BIGINT NOT NULL,
			amount_cents BIGINT NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)
	`, tableName))
	require.NoError(t, err, "create table")

	// Seed test data using efficient SQL cross-joins
	seedTestRows(t, db, tableName,
		"customer_id, amount_cents",
		"FLOOR(1 + RAND() * 1000), FLOOR(100 + RAND() * 100000)",
		10000)

	schemaDir := newSchemaDir(t)
	writeExistingTablesSchema(t, schemaDir)

	e2eutil.WriteFile(t, filepath.Join(schemaDir, tableName+".sql"), fmt.Sprintf(`
CREATE TABLE %s (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    amount_cents BIGINT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_customer (customer_id)
);
`, tableName))

	t.Run("plan_shows_index", func(t *testing.T) {
		out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "plan", "-s", ".", "-e", "staging", "--endpoint", endpoint)
		e2eutil.AssertContains(t, out, "ADD INDEX")
		e2eutil.AssertContains(t, out, "idx_customer")
	})

	var applyID string
	t.Run("apply_with_defer_cutover", func(t *testing.T) {
		out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "apply", "-s", ".", "-e", "staging", "--endpoint", endpoint, "-y", "--defer-cutover", "--watch=false", "-o", "json")
		applyID = extractApplyID(t, out)
	})

	waitForTableInProgress(t, binPath, schemaDir, endpoint, applyID, tableName, 10*time.Second)

	t.Run("wait_for_cutover_ready", func(t *testing.T) {
		testutil.WaitForAnyState(t, endpoint, applyID, []string{state.Apply.WaitingForCutover, state.Apply.Completed}, 10*time.Second)
	})

	t.Run("trigger_cutover", func(t *testing.T) {
		out, err := e2eutil.RunCLIWithErrorInDir(t, binPath, schemaDir, "cutover", applyID, "--endpoint", endpoint, "--watch=false")
		if err != nil {
			stripped := e2eutil.StripANSI(out)
			if !strings.Contains(strings.ToLower(stripped), "complete") && !strings.Contains(strings.ToLower(stripped), "nothing to cutover") {
				require.NoErrorf(t, err, "cutover failed\nOutput: %s", out)
			}
		}
	})

	t.Run("verify_index_created", func(t *testing.T) {
		waitForIndex(t, db, tableName, "idx_customer", 10*time.Second)
	})

	t.Run("plan_shows_no_changes", func(t *testing.T) {
		out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "plan", "-s", ".", "-e", "staging", "--endpoint", endpoint)
		assertNotContains(t, out, "idx_customer")
	})
}

// =============================================================================
// Stop/Start Tests
// =============================================================================

func TestLocal_Stop_Help(t *testing.T) {
	binPath := buildCLI(t)
	out := runCLI(t, binPath, "stop", "--help")
	e2eutil.AssertContains(t, out, "apply-id")
	e2eutil.AssertContains(t, out, "--endpoint")
}

func TestLocal_Start_Help(t *testing.T) {
	binPath := buildCLI(t)
	out := runCLI(t, binPath, "start", "--help")
	e2eutil.AssertContains(t, out, "apply-id")
	e2eutil.AssertContains(t, out, "--endpoint")
}

func TestLocal_Stop_NoActiveChange(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	out, _ := runCLIWithError(t, binPath, "stop", "apply-nonexistent", "--endpoint", endpoint)
	if !strings.Contains(out, "No") && !strings.Contains(out, "not found") && !strings.Contains(out, "error") {
		t.Logf("Stop output: %s", out)
	}
}

func TestLocal_Start_NoStoppedChange(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	out, _ := runCLIWithError(t, binPath, "start", "apply-nonexistent", "--endpoint", endpoint, "--watch=false")
	if !strings.Contains(out, "No") && !strings.Contains(out, "not found") && !strings.Contains(out, "error") {
		t.Logf("Start output: %s", out)
	}
}

func TestLocal_StopStart_FullWorkflow(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)
	ensureNoActiveChange(t, endpoint)

	tableName := uniqueTableName("stopstart_events")
	defer dropTestTable(t, tableName)

	db := openTestappStaging(t)

	_, err := db.ExecContext(t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			event_type VARCHAR(100) NOT NULL,
			payload TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)
	`, tableName))
	require.NoError(t, err, "create table")

	seedTestRows(t, db, tableName,
		"event_type, payload",
		"ELT(FLOOR(1 + RAND() * 5), 'type_a', 'type_b', 'type_c', 'type_d', 'type_e'), CONCAT('payload data ', seq)",
		50000)

	schemaDir := newSchemaDir(t)
	writeExistingTablesSchema(t, schemaDir)

	e2eutil.WriteFile(t, filepath.Join(schemaDir, tableName+".sql"), fmt.Sprintf(`
CREATE TABLE %s (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    payload TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_event_type (event_type),
    INDEX idx_created (created_at)
);
`, tableName))

	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "apply", "-s", ".", "-e", "staging", "--endpoint", endpoint, "-y", "--defer-cutover", "--watch=false", "-o", "json")
	applyID := extractApplyID(t, out)

	waitForTableInProgress(t, binPath, schemaDir, endpoint, applyID, tableName, 10*time.Second)
	waitForApplyAnyState(t, endpoint, applyID, []string{state.Apply.Running, state.Apply.WaitingForCutover, state.Apply.Completed}, 10*time.Second)

	applyState, _ := fetchApplyState(endpoint, applyID)

	if applyState != state.Apply.Completed {
		stopOut, stopErr := e2eutil.RunCLIWithErrorInDir(t, binPath, schemaDir, "stop", applyID, "--endpoint", endpoint)
		if stopErr == nil && strings.Contains(e2eutil.StripANSI(stopOut), "stopped") {
			waitForApplyState(t, endpoint, applyID, state.Apply.Stopped, 30*time.Second)
			startOut := e2eutil.RunCLIInDir(t, binPath, schemaDir, "start", applyID, "--endpoint", endpoint, "--watch=false")
			e2eutil.AssertContains(t, startOut, "resumed")
		}
	}

	waitForApplyAnyState(t, endpoint, applyID, []string{state.Apply.WaitingForCutover, state.Apply.Completed}, 30*time.Second)

	cutoverOut, cutoverErr := e2eutil.RunCLIWithErrorInDir(t, binPath, schemaDir, "cutover", applyID, "--endpoint", endpoint, "--watch=false")
	if cutoverErr != nil {
		stripped := e2eutil.StripANSI(cutoverOut)
		if !strings.Contains(strings.ToLower(stripped), "complete") && !strings.Contains(strings.ToLower(stripped), "nothing to cutover") {
			t.Logf("cutover output: %s", cutoverOut)
		}
	}

	for _, indexName := range []string{"idx_event_type", "idx_created"} {
		waitForIndex(t, db, tableName, indexName, 30*time.Second)
	}
}

// TestLocal_StopStart_MultiTable_ResumeAll verifies that stopping a multi-table
// sequential apply mid-flight and then resuming completes ALL tables — including
// tables that hadn't started copying yet (0% progress). On resume, each table
// runs its own apply+poll cycle sequentially.
func TestLocal_StopStart_MultiTable_ResumeAll(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)
	ensureNoActiveChange(t, endpoint)

	// Create 3 tables with enough rows for Spirit to take measurable time
	table1 := uniqueTableName("resume_alpha")
	table2 := uniqueTableName("resume_beta")
	table3 := uniqueTableName("resume_gamma")
	defer dropTestTable(t, table1)
	defer dropTestTable(t, table2)
	defer dropTestTable(t, table3)

	db := openTestappStaging(t)

	for _, tbl := range []string{table1, table2, table3} {
		_, err := db.ExecContext(t.Context(), fmt.Sprintf(`
			CREATE TABLE %s (
				id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
				name VARCHAR(255) NOT NULL,
				amount BIGINT NOT NULL,
				created_at DATETIME DEFAULT CURRENT_TIMESTAMP
			)
		`, tbl))
		require.NoErrorf(t, err, "create table %s", tbl)
		seedTestRows(t, db, tbl,
			"name, amount",
			"CONCAT('item_', seq), FLOOR(1 + RAND() * 10000)",
			50000)
	}

	// Build schema dir with indexes on all 3 tables
	schemaDir := newSchemaDir(t)
	writeExistingTablesSchema(t, schemaDir)

	for _, tbl := range []string{table1, table2, table3} {
		e2eutil.WriteFile(t, filepath.Join(schemaDir, tbl+".sql"), fmt.Sprintf(`
CREATE TABLE %s (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    amount BIGINT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_name (name)
);
`, tbl))
	}

	// Plan should show 3 ADD INDEX changes
	t.Run("plan_shows_3_indexes", func(t *testing.T) {
		out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "plan", "-s", ".", "-e", "staging", "--endpoint", endpoint)
		e2eutil.AssertContains(t, out, "ADD INDEX")
		e2eutil.AssertContains(t, out, "idx_name")
	})

	// Apply without --defer-cutover (sequential mode)
	var applyID string
	t.Run("apply_sequential", func(t *testing.T) {
		out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "apply", "-s", ".", "-e", "staging", "--endpoint", endpoint, "-y", "--watch=false", "-o", "json")
		applyID = extractApplyID(t, out)
	})

	// Wait for the first table to appear in progress (sequential: one at a time)
	waitForApplyAnyState(t, endpoint, applyID, []string{state.Apply.Running, state.Apply.Completed}, 10*time.Second)

	// Stop while first task is in-flight (or shortly after)
	t.Run("stop_mid_flight", func(t *testing.T) {
		waitForApplyState(t, endpoint, applyID, state.Apply.Running, 3*time.Second)

		stopOut, stopErr := e2eutil.RunCLIWithErrorInDir(t, binPath, schemaDir, "stop", applyID, "--endpoint", endpoint)
		stripped := e2eutil.StripANSI(stopOut)
		if stopErr != nil && !strings.Contains(strings.ToLower(stripped), "stopped") {
			// If the apply already completed (tables too small), skip the stop/start test
			if strings.Contains(strings.ToLower(stripped), "no active") || strings.Contains(strings.ToLower(stripped), "not found") {
				t.Skip("Apply completed before we could stop — tables processed too fast")
			}
			require.NoErrorf(t, stopErr, "stop failed\nOutput: %s", stopOut)
		}
		e2eutil.AssertContains(t, stopOut, "stopped")
	})

	// Verify stopped state
	waitForApplyState(t, endpoint, applyID, state.Apply.Stopped, 10*time.Second)

	// Resume — this should re-plan, find remaining tables, and process them sequentially
	t.Run("start_resumes", func(t *testing.T) {
		startOut := e2eutil.RunCLIInDir(t, binPath, schemaDir, "start", applyID, "--endpoint", endpoint, "--watch=false")
		e2eutil.AssertContains(t, startOut, "resumed")
	})

	// Wait for everything to complete (3 tables × 50K rows each, sequential)
	waitForApplyState(t, endpoint, applyID, state.Apply.Completed, 60*time.Second)

	// Verify ALL 3 indexes exist
	t.Run("verify_all_indexes_created", func(t *testing.T) {
		for _, tbl := range []string{table1, table2, table3} {
			waitForIndex(t, db, tbl, "idx_name", 30*time.Second)
		}
	})

	// Verify plan shows no remaining changes
	t.Run("plan_shows_no_changes", func(t *testing.T) {
		out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "plan", "-s", ".", "-e", "staging", "--endpoint", endpoint)
		assertNotContains(t, out, "ADD INDEX")
	})
}

// =============================================================================
// Volume Tests
// =============================================================================

func TestLocal_Volume_Help(t *testing.T) {
	binPath := buildCLI(t)
	out := runCLI(t, binPath, "volume", "--help")
	e2eutil.AssertContains(t, out, "apply-id")
	e2eutil.AssertContains(t, out, "--volume")
}

func TestLocal_Volume_InvalidLevel(t *testing.T) {
	binPath := buildCLI(t)

	// Volume validation happens after apply_id resolution, so these will fail
	// at the resolution step (no such apply), which is still an error.
	_, err := runCLIWithError(t, binPath, "volume", "apply-fake", "-v", "0", "--endpoint", "http://localhost:9999")
	require.Error(t, err, "expected error for volume=0")

	_, err = runCLIWithError(t, binPath, "volume", "apply-fake", "-v", "12", "--endpoint", "http://localhost:9999")
	require.Error(t, err, "expected error for volume=12")
}

func TestLocal_Volume_NoActiveChange(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)

	_, err := runCLIWithError(t, binPath, "volume", "apply-nonexistent", "-v", "5", "--endpoint", endpoint)
	if err == nil {
		t.Log("volume with no active change didn't error - may be acceptable if there's a leftover state")
	}
}

func TestLocal_Volume_DuringApply(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)
	ensureNoActiveChange(t, endpoint)

	tableName := uniqueTableName("vol_metrics")
	defer dropTestTable(t, tableName)

	db := openTestappStaging(t)

	_, err := db.ExecContext(t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			value DECIMAL(20, 4),
			recorded_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)
	`, tableName))
	require.NoError(t, err, "create table")

	seedTestRows(t, db, tableName,
		"name, value",
		"CONCAT('metric_', seq), FLOOR(100 + RAND() * 10000)",
		10000)

	schemaDir := newSchemaDir(t)
	writeExistingTablesSchema(t, schemaDir)

	e2eutil.WriteFile(t, filepath.Join(schemaDir, tableName+".sql"), fmt.Sprintf(`
CREATE TABLE %s (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    value DECIMAL(20, 4),
    recorded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_name (name),
    INDEX idx_recorded (recorded_at)
);
`, tableName))

	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "apply", "-s", ".", "-e", "staging", "--endpoint", endpoint, "-y", "--defer-cutover", "--watch=false", "-o", "json")
	applyID := extractApplyID(t, out)

	waitForTableInProgress(t, binPath, schemaDir, endpoint, applyID, tableName, 10*time.Second)
	testutil.WaitForAnyState(t, endpoint, applyID, []string{state.Apply.Running, state.Apply.WaitingForCutover, state.Apply.Completed}, 10*time.Second)

	// Try volume adjustment while copy is in progress (not when waiting for cutover)
	// Volume adjustment during "Waiting for cutover" stops but can't restart properly
	prog, _ := testutil.FetchProgress(endpoint, applyID)
	if prog != nil && prog.State == state.Apply.Running {
		volumeOut, volumeErr := e2eutil.RunCLIWithErrorInDir(t, binPath, schemaDir, "volume", applyID, "-v", "8", "--endpoint", endpoint, "--watch=false")
		if volumeErr == nil {
			t.Logf("Volume adjustment succeeded: %s", volumeOut)
		} else {
			t.Logf("Volume adjustment skipped or failed: %v (output: %s)", volumeErr, volumeOut)
		}
	}

	// Complete the apply using the cleanup helper which handles all states
	ensureNoActiveChange(t, endpoint)
}

// =============================================================================
// Progress Tests
// =============================================================================

func TestLocal_Progress_Help(t *testing.T) {
	binPath := buildCLI(t)
	out := runCLI(t, binPath, "progress", "--help")
	e2eutil.AssertContains(t, out, "apply-id")
	e2eutil.AssertContains(t, out, "--endpoint")
	e2eutil.AssertContains(t, out, "watch")
}

func TestLocal_Progress_NoActiveChange(t *testing.T) {
	endpoint := schemabotURL(t)

	ensureNoActiveChange(t, endpoint)

	result, err := client.GetStatus(endpoint)
	require.NoError(t, err, "get status")
	// After clearing state, there should be no active applies for this database.
	for _, a := range result.Applies {
		if a.Database == "testapp" && a.Environment == "staging" {
			assert.Truef(t, state.IsTerminalApplyState(a.State),
				"expected terminal state, got: %s", a.State)
		}
	}
}

func TestLocal_Progress_DuringApply(t *testing.T) {
	binPath := buildCLI(t)
	endpoint := schemabotURL(t)
	ensureNoActiveChange(t, endpoint)

	tableName := uniqueTableName("prog_logs")
	defer dropTestTable(t, tableName)

	db := openTestappStaging(t)

	_, err := db.ExecContext(t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			level VARCHAR(20) NOT NULL,
			message TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)
	`, tableName))
	require.NoError(t, err, "create table")

	seedTestRows(t, db, tableName,
		"level, message",
		"ELT(FLOOR(1 + RAND() * 5), 'INFO', 'WARN', 'ERROR', 'DEBUG', 'TRACE'), CONCAT('Log message ', seq)",
		5000)

	schemaDir := newSchemaDir(t)
	writeExistingTablesSchema(t, schemaDir)

	e2eutil.WriteFile(t, filepath.Join(schemaDir, tableName+".sql"), fmt.Sprintf(`
CREATE TABLE %s (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    level VARCHAR(20) NOT NULL,
    message TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_level (level)
);
`, tableName))

	applyOut := e2eutil.RunCLIInDir(t, binPath, schemaDir, "apply", "-s", ".", "-e", "staging", "--endpoint", endpoint, "-y", "--defer-cutover", "--watch=false", "-o", "json")
	applyID := extractApplyID(t, applyOut)

	waitForTableInProgress(t, binPath, schemaDir, endpoint, applyID, tableName, 10*time.Second)
	testutil.WaitForAnyState(t, endpoint, applyID, []string{state.Apply.Running, state.Apply.WaitingForCutover, state.Apply.Completed}, 10*time.Second)

	out := e2eutil.RunCLIInDir(t, binPath, schemaDir, "progress", applyID, "--endpoint", endpoint, "--watch=false")
	e2eutil.AssertContains(t, out, tableName)

	prog, _ := testutil.FetchProgress(endpoint, applyID)
	if prog == nil || prog.State != state.Apply.Completed {
		testutil.WaitForAnyState(t, endpoint, applyID, []string{state.Apply.WaitingForCutover, state.Apply.Completed}, 10*time.Second)

		prog, _ = testutil.FetchProgress(endpoint, applyID)
		if prog != nil && prog.State == state.Apply.WaitingForCutover {
			e2eutil.RunCLIInDir(t, binPath, schemaDir, "cutover", applyID, "--endpoint", endpoint, "--watch=false")
			testutil.WaitForState(t, endpoint, applyID, state.Apply.Completed, 10*time.Second)
		}
	}
}

// =============================================================================
// Full Demo Validation
// =============================================================================

func TestLocal_Demo_FullValidation(t *testing.T) {
	baseURL := schemabotURL(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/health", nil)
	require.NoError(t, err, "create request")
	resp, err := http.DefaultClient.Do(req)
	require.NoErrorf(t, err, "SchemaBot not accessible at %s", baseURL)
	_ = resp.Body.Close()
	require.Equalf(t, http.StatusOK, resp.StatusCode, "SchemaBot health check failed")

	schemabotDSN := mysqlDSN(t)
	schemabotDB, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err, "connect to schemabot MySQL")
	defer utils.CloseAndLog(schemabotDB)

	schemabotTables := []string{"locks", "checks", "settings", "plans", "tasks", "vitess_apply_data", "vitess_tasks"}
	for _, table := range schemabotTables {
		var count int
		err := schemabotDB.QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'schemabot' AND table_name = ?",
			table,
		).Scan(&count)
		assert.NoErrorf(t, err, "query storage table %s", table)
		assert.Equalf(t, 1, count, "SchemaBot storage table %s missing", table)
	}

	stagingDSN := testappStagingDSN(t)
	stagingDB, err := sql.Open("mysql", stagingDSN)
	require.NoError(t, err, "connect to staging MySQL")
	defer utils.CloseAndLog(stagingDB)

	var demoTablesExist bool
	var count int
	_ = stagingDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'testapp' AND table_name = 'users'").Scan(&count)
	demoTablesExist = count > 0

	if demoTablesExist {
		testappTables := []string{"users", "orders"}
		for _, table := range testappTables {
			var cnt int
			err := stagingDB.QueryRowContext(t.Context(),
				"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'testapp' AND table_name = ?",
				table,
			).Scan(&cnt)
			assert.NoErrorf(t, err, "query staging table %s", table)
			assert.Equalf(t, 1, cnt, "staging testapp table %s missing", table)
		}

		productionDSN := testappProductionDSN(t)
		productionDB, err := sql.Open("mysql", productionDSN)
		require.NoError(t, err, "connect to production MySQL")
		defer utils.CloseAndLog(productionDB)

		for _, table := range testappTables {
			var cnt int
			err := productionDB.QueryRowContext(t.Context(),
				"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'testapp' AND table_name = ?",
				table,
			).Scan(&cnt)
			assert.NoErrorf(t, err, "query production table %s", table)
			assert.Equalf(t, 1, cnt, "production testapp table %s missing", table)
		}
		t.Log("Demo validation passed: SchemaBot healthy, all tables present")
	} else {
		t.Log("Demo validation passed: SchemaBot healthy (demo tables not present - run 'make demo' for full validation)")
	}
}
