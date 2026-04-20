//go:build integration

package integration

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	schemabotapi "github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// TestCLI_Schemabot_GRPC tests the schemabot CLI commands with gRPC backend.
func TestCLI_Schemabot_GRPC(t *testing.T) {
	// Build the schemabot binary
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	// Start SchemaBot server with gRPC backend (connects to Tern gRPC server)
	schemabotAddr := startSchemaBotWithGRPC(t)

	// Create test schema directory with schemabot.yaml config
	// The CLI expects .sql files in the same directory as schemabot.yaml
	schemaDir := newSchemaDirForDB(t, "testdb")
	writeFile(t, filepath.Join(schemaDir, "users.sql"), `
CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
`)

	endpoint := "http://" + schemabotAddr

	t.Run("help", func(t *testing.T) {
		out := runCLI(t, binPath, "--help")
		assertContains(t, out, "SchemaBot")
		assertContains(t, out, "plan")
		assertContains(t, out, "apply")
		assertContains(t, out, "progress")
	})

	t.Run("plan_help", func(t *testing.T) {
		out := runCLI(t, binPath, "plan", "--help")
		assertContains(t, out, "--schema_dir")
		assertContains(t, out, "--endpoint")
		assertContains(t, out, "--environment")
	})

	t.Run("plan_missing_config", func(t *testing.T) {
		// Run plan in a directory without schemabot.yaml
		emptyDir := t.TempDir()
		_, err := runCLIWithErrorInDir(t, binPath, emptyDir, "plan", "--endpoint", endpoint)
		require.Error(t, err, "expected error for missing schemabot.yaml")
	})

	t.Run("plan_spirit", func(t *testing.T) {
		// Run plan from the schema directory (where schemabot.yaml is)
		// Use -e staging explicitly since test setup doesn't have environments endpoint
		out := runCLIInDir(t, binPath, schemaDir, "plan", "-e", "staging", "--endpoint", endpoint)
		// Human-readable output format (engine type depends on backend config)
		assertContains(t, out, "Schema Change Plan")
		assertContains(t, out, "Database: testdb")
		assertContains(t, out, "CREATE TABLE")
	})

	t.Run("status_no_active_change", func(t *testing.T) {
		// Status for a database — shows apply history (empty when no applies have run).
		out := runCLIInDir(t, binPath, schemaDir, "status", "--database", "testdb", "--endpoint", endpoint)
		// Should show the database name in the output (even if empty history).
		assertContains(t, out, "testdb")
	})
}

// TestCLI_Schemabot_Local tests the schemabot CLI with embedded LocalClient (no gRPC).
func TestCLI_Schemabot_Local(t *testing.T) {
	// Build the schemabot binary
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	// Start SchemaBot server with embedded LocalClient (no gRPC dependency)
	schemabotAddr := startSchemaBotLocal(t)

	// Create test schema directory with schemabot.yaml config
	// The CLI expects .sql files in the same directory as schemabot.yaml
	schemaDir := newSchemaDirForDB(t, "testdb")
	writeFile(t, filepath.Join(schemaDir, "products.sql"), `
CREATE TABLE products (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2) NOT NULL
);
`)

	endpoint := "http://" + schemabotAddr

	t.Run("plan", func(t *testing.T) {
		// Run plan from the schema directory (where schemabot.yaml is)
		// Use -e staging explicitly since test setup doesn't have environments endpoint
		out := runCLIInDir(t, binPath, schemaDir, "plan", "-e", "staging", "--endpoint", endpoint)
		// Human-readable output format
		assertContains(t, out, "MySQL Schema Change Plan")
		assertContains(t, out, "Database: testdb")
		assertContains(t, out, "CREATE TABLE")
	})

	t.Run("status_no_active_change", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "status", "--database", "testdb", "--endpoint", endpoint)
		assertContains(t, out, "testdb")
	})

	t.Run("apply_accepted", func(t *testing.T) {
		// Run apply with auto-approve and no wait (just test that apply is accepted)
		// -s . refers to current directory (schemaDir) which has schemabot.yaml
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
		)
		// Human-readable output format
		assertContains(t, out, "MySQL Schema Change Apply")
		assertContains(t, out, "Apply started")
		waitForApplyFromOutput(t, endpoint, out, "completed", 30*time.Second)
	})
}

// TestCLI_DeferCutover tests the full defer-cutover workflow.
func TestCLI_DeferCutover(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	// Use a unique database name to avoid test pollution from other tests
	dbName := fmt.Sprintf("defer_%d", time.Now().UnixNano())

	schemabotAddr := startSchemaBotLocalDB(t, dbName)

	// Create schema with a table that will require row copying
	schemaDir := newSchemaDirForDB(t, dbName)
	// Start with base table
	writeFile(t, filepath.Join(schemaDir, "orders.sql"), `
CREATE TABLE orders (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    total_cents BIGINT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
`)

	endpoint := "http://" + schemabotAddr

	// First apply base schema
	t.Run("setup_base_schema", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
		)
		assertContains(t, out, "Apply started")

		// Wait for completion
		waitForApplyFromOutput(t, endpoint, out, "completed", 30*time.Second)
	})

	// Add an index to trigger a schema change
	writeFile(t, filepath.Join(schemaDir, "orders.sql"), `
CREATE TABLE orders (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    total_cents BIGINT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id)
);
`)

	t.Run("plan_shows_index_add", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "plan",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
		)
		assertContains(t, out, "ADD INDEX")
		assertContains(t, out, "idx_user_id")
	})

	var applyID string

	t.Run("apply_with_defer_cutover", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--defer-cutover",
			"--watch=false",
		)
		assertContains(t, out, "Apply started")
		assertContains(t, out, "Defer Cutover")
		applyID = parseApplyID(t, out)
	})

	t.Run("progress_shows_waiting_for_cutover", func(t *testing.T) {
		// Wait for WAITING_FOR_CUTOVER state
		waitForState(t, endpoint, applyID, "waiting_for_cutover", 30*time.Second)

		out := runCLIInDir(t, binPath, schemaDir, "progress",
			applyID,
			"--endpoint", endpoint,
			"--watch=false",
		)
		assertContains(t, out, "Waiting for cutover")
	})

	t.Run("cutover_completes_change", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "cutover",
			applyID,
			"--endpoint", endpoint,
			"--watch=false",
		)
		assertContains(t, out, "Cutover requested")

		// Wait for completion
		waitForState(t, endpoint, applyID, "completed", 30*time.Second)
	})

	t.Run("progress_shows_completed_after_cutover", func(t *testing.T) {
		// After cutover completes, progress shows the completed state
		out := runCLIInDir(t, binPath, schemaDir, "progress",
			applyID,
			"--endpoint", endpoint,
			"--watch=false",
		)
		assertContains(t, out, "Completed")
	})

	t.Run("plan_shows_no_changes", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "plan",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
		)
		assertContains(t, out, "No schema changes detected")
	})
}

// TestCLI_Progress_ByApplyID tests that progress can be fetched by apply ID using the local client.
func TestCLI_Progress_ByApplyID(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	dbName := fmt.Sprintf("byapplyid_%d", time.Now().UnixNano())

	schemabotAddr := startSchemaBotLocalDB(t, dbName)

	schemaDir := newSchemaDirForDB(t, dbName)
	writeFile(t, filepath.Join(schemaDir, "items.sql"), `
CREATE TABLE items (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
`)

	endpoint := "http://" + schemabotAddr

	// First apply base schema
	out := runCLIInDir(t, binPath, schemaDir, "apply",
		"-s", ".",
		"-e", "staging",
		"--endpoint", endpoint,
		"-y",
		"--watch=false",
	)
	assertContains(t, out, "Apply started")

	// Extract the apply ID from apply output
	applyID := parseApplyID(t, out)
	t.Logf("captured apply ID: %s", applyID)

	// Wait for completion
	waitForState(t, endpoint, applyID, "completed", 30*time.Second)

	// Fetch progress by apply ID
	out = runCLIInDir(t, binPath, schemaDir, "progress",
		applyID,
		"--endpoint", endpoint,
		"--watch=false",
	)
	assertContains(t, out, "items")
}

// TestCLI_AllowUnsafe tests that apply blocks on unsafe changes without --allow-unsafe.
func TestCLI_AllowUnsafe(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	// Use a unique database name to avoid test pollution
	dbName := fmt.Sprintf("unsafe_%d", time.Now().UnixNano())

	schemabotAddr := startSchemaBotLocalDB(t, dbName)

	schemaDir := newSchemaDirForDB(t, dbName)

	// Start with a table that has a column we'll later drop
	writeFile(t, filepath.Join(schemaDir, "accounts.sql"), `
CREATE TABLE accounts (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL
);
`)

	endpoint := "http://" + schemabotAddr

	// Apply base schema
	t.Run("setup_base_schema", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
		)
		assertContains(t, out, "Apply started")
		waitForApplyFromOutput(t, endpoint, out, "completed", 30*time.Second)
	})

	// Modify schema to drop a column (unsafe change)
	writeFile(t, filepath.Join(schemaDir, "accounts.sql"), `
CREATE TABLE accounts (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL
);
`)

	t.Run("plan_shows_unsafe_warning", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "plan",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
		)
		// Plan should show the change but not block
		assertContains(t, out, "ALTER TABLE")
		assertContains(t, out, "DROP COLUMN")
	})

	t.Run("apply_blocked_without_allow_unsafe", func(t *testing.T) {
		// Apply without --allow-unsafe should be blocked
		out, err := runCLIWithErrorInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
		)
		// Should exit with error
		require.Error(t, err, "expected apply to fail without --allow-unsafe")
		// Should show unsafe changes message
		assertContains(t, out, "Unsafe Changes Detected")
		assertContains(t, out, "--allow-unsafe")
	})

	t.Run("apply_succeeds_with_allow_unsafe", func(t *testing.T) {
		// Apply with --allow-unsafe should proceed
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
			"--allow-unsafe",
		)
		assertContains(t, out, "Apply started")
		// Should show warning about unsafe changes being allowed
		assertContains(t, out, "Unsafe Changes")
		assertContains(t, out, "--allow-unsafe enabled")

		// Wait for completion
		waitForApplyFromOutput(t, endpoint, out, "completed", 30*time.Second)
	})

	t.Run("plan_shows_no_changes_after_apply", func(t *testing.T) {
		// Spirit marks the task as completed in storage before the MySQL DDL
		// fully commits and becomes visible to a new connection. Retry the
		// plan until the schema converges.
		require.Eventually(t, func() bool {
			out := runCLIInDir(t, binPath, schemaDir, "plan",
				"-s", ".",
				"-e", "staging",
				"--endpoint", endpoint,
			)
			return strings.Contains(stripANSI(out), "No schema changes detected")
		}, 10*time.Second, 500*time.Millisecond, "plan never showed 'No schema changes detected'")
	})
}

// TestCLI_StopStart tests the stop and start command validation.
// Full stop/start functionality is tested at the engine level in spirit_integration_test.go.
func TestCLI_StopStart(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	t.Run("stop_help", func(t *testing.T) {
		// Verify help output shows correct usage with positional apply_id
		out := runCLI(t, binPath, "stop", "--help")
		assertContains(t, out, "stop")
		assertContains(t, out, "apply-id")
		assertContains(t, out, "--endpoint")
	})

	t.Run("start_help", func(t *testing.T) {
		// Verify help output shows correct usage with positional apply_id
		out := runCLI(t, binPath, "start", "--help")
		assertContains(t, out, "start")
		assertContains(t, out, "apply-id")
		assertContains(t, out, "--endpoint")
		assertContains(t, out, "watch")
	})

	t.Run("stop_requires_apply_id", func(t *testing.T) {
		_, err := runCLIWithError(t, binPath, "stop",
			"--endpoint", "http://localhost:9999",
		)
		require.Error(t, err, "expected error when apply_id is missing")
	})

	t.Run("start_requires_apply_id", func(t *testing.T) {
		_, err := runCLIWithError(t, binPath, "start",
			"--endpoint", "http://localhost:9999",
		)
		require.Error(t, err, "expected error when apply_id is missing")
	})
}

// TestCLI_Stop_NoActiveSchemaChange tests stop when there's nothing to stop.
func TestCLI_Stop_NoActiveSchemaChange(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	schemabotAddr := startSchemaBotLocalDB(t, fmt.Sprintf("stopnone_%d", time.Now().UnixNano()))

	endpoint := "http://" + schemabotAddr

	t.Run("stop_with_no_schema_change", func(t *testing.T) {
		// Stop with a non-existent apply ID should fail
		_, err := runCLIWithError(t, binPath, "stop",
			"apply-nonexistent",
			"--endpoint", endpoint,
		)
		require.Error(t, err, "expected error for stop with non-existent apply ID")
	})
}

// TestCLI_Start_NoStoppedSchemaChange tests start when there's nothing to start.
func TestCLI_Start_NoStoppedSchemaChange(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	schemabotAddr := startSchemaBotLocalDB(t, fmt.Sprintf("startnone_%d", time.Now().UnixNano()))

	endpoint := "http://" + schemabotAddr

	t.Run("start_with_no_stopped_schema_change", func(t *testing.T) {
		// Start with a non-existent apply ID should fail
		_, err := runCLIWithError(t, binPath, "start",
			"apply-nonexistent",
			"--endpoint", endpoint,
			"--watch=false",
		)
		require.Error(t, err, "expected error for start with non-existent apply ID")
	})
}

// TestCLI_StopStartFlow tests the full stop → start → complete flow.
// This is an integration test that verifies:
// 1. A schema change can be stopped mid-execution
// 2. A stopped schema change can be resumed with start
// 3. The resumed schema change completes successfully
func TestCLI_StopStartFlow(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	dbName := fmt.Sprintf("stopstart_%d", time.Now().UnixNano())

	schemabotAddr := startSchemaBotLocalDB(t, dbName)

	schemaDir := newSchemaDirForDB(t, dbName)

	endpoint := "http://" + schemabotAddr

	// Create a table with enough rows to allow stopping mid-copy
	// We need the copy phase to take long enough to stop
	writeFile(t, filepath.Join(schemaDir, "items.sql"), `
CREATE TABLE items (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
`)

	// Apply base schema
	t.Run("setup_base_schema", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
		)
		assertContains(t, out, "Apply started")
		waitForApplyFromOutput(t, endpoint, out, "completed", 30*time.Second)
	})

	// Insert rows into the table to make the copy phase take longer
	t.Run("insert_test_data", func(t *testing.T) {
		targetDSN := strings.Replace(targetDSN, "/target_test", "/"+dbName, 1)
		db, err := sql.Open("mysql", targetDSN)
		require.NoError(t, err, "open target db")
		defer utils.CloseAndLog(db)

		// Insert 50,000 rows - enough for the copy phase to be stoppable
		for i := range 500 {
			_, err := db.ExecContext(t.Context(), `
				INSERT INTO items (name, description, price)
				SELECT CONCAT('Item ', seq), REPEAT('Description text ', 10), RAND() * 1000
				FROM (
					SELECT @row := @row + 1 as seq
					FROM (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a,
					     (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b,
					     (SELECT @row := 0) r
				) numbers
			`)
			require.NoErrorf(t, err, "insert batch %d", i)
		}

		var count int
		_ = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM items").Scan(&count)
		t.Logf("Inserted %d rows into items table", count)
	})

	// Add an index - this triggers a copy operation
	writeFile(t, filepath.Join(schemaDir, "items.sql"), `
CREATE TABLE items (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_name (name),
    INDEX idx_price (price)
);
`)

	var applyID string

	// Apply with defer-cutover so we have time to stop
	t.Run("apply_schema_change", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--defer-cutover",
			"--watch=false",
		)
		assertContains(t, out, "Apply started")
		applyID = parseApplyID(t, out)
	})

	// Wait for running state (copy phase started)
	t.Run("wait_for_running", func(t *testing.T) {
		waitForAnyStateByApplyID(t, endpoint, applyID,
			[]string{"running", "waiting_for_cutover"}, 30*time.Second)
	})

	// Stop the schema change
	t.Run("stop_schema_change", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "stop",
			applyID,
			"--endpoint", endpoint,
		)
		assertContains(t, out, "Schema change stopped")

		// Wait for stopped state
		waitForState(t, endpoint, applyID, "stopped", 30*time.Second)
	})

	// Verify progress shows stopped state
	t.Run("progress_shows_stopped", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "progress",
			applyID,
			"--endpoint", endpoint,
			"--watch=false",
		)
		assertContains(t, out, "Stopped")
	})

	// Start (resume) the schema change
	t.Run("start_schema_change", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "start",
			applyID,
			"--endpoint", endpoint,
			"--watch=false",
		)
		assertContains(t, out, "Schema change resumed")
	})

	// Wait for waiting_for_cutover (copy complete) or running
	t.Run("wait_for_copy_complete", func(t *testing.T) {
		waitForState(t, endpoint, applyID, "waiting_for_cutover", 60*time.Second)
	})

	// Cutover to complete the change
	t.Run("cutover", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "cutover",
			applyID,
			"--endpoint", endpoint,
			"--watch=false",
		)
		assertContains(t, out, "Cutover requested")
	})

	// Wait for completion
	t.Run("wait_for_complete", func(t *testing.T) {
		waitForState(t, endpoint, applyID, "completed", 30*time.Second)
	})

	// Verify the schema change was applied
	t.Run("verify_indexes_exist", func(t *testing.T) {
		targetDSN := strings.Replace(targetDSN, "/target_test", "/"+dbName, 1)
		db, err := sql.Open("mysql", targetDSN)
		require.NoError(t, err, "open target db")
		defer utils.CloseAndLog(db)

		rows, err := db.QueryContext(t.Context(), "SHOW INDEX FROM items WHERE Key_name = 'idx_name'")
		require.NoError(t, err, "show index")
		hasIndex := rows.Next()
		_ = rows.Close()
		assert.True(t, hasIndex, "expected idx_name index to exist after stop/start/cutover")
	})

	t.Log("Stop/start flow completed successfully")
}

// TestCLI_StopStartByApplyID tests stop and start using --apply-id instead of --database/--environment.
func TestCLI_StopStartByApplyID(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	dbName := fmt.Sprintf("ssbyid_%d", time.Now().UnixNano())

	schemabotAddr := startSchemaBotLocalDB(t, dbName)

	schemaDir := newSchemaDirForDB(t, dbName)

	endpoint := "http://" + schemabotAddr

	// Create base table
	writeFile(t, filepath.Join(schemaDir, "items.sql"), `
CREATE TABLE items (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
`)

	t.Run("setup_base_schema", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
		)
		assertContains(t, out, "Apply started")
		waitForApplyFromOutput(t, endpoint, out, "completed", 30*time.Second)
	})

	// Insert enough data for the copy phase to be stoppable.
	// Spirit copies rows during ALTER TABLE; with too few rows it completes
	// before the stop command can intervene.
	t.Run("insert_test_data", func(t *testing.T) {
		targetDSN := strings.Replace(targetDSN, "/target_test", "/"+dbName, 1)
		db, err := sql.Open("mysql", targetDSN)
		require.NoError(t, err, "open target db")
		defer utils.CloseAndLog(db)

		for i := range 500 {
			_, err := db.ExecContext(t.Context(), `
				INSERT INTO items (name)
				SELECT CONCAT('Item ', seq)
				FROM (
					SELECT @row := @row + 1 as seq
					FROM (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a,
					     (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b,
					     (SELECT @row := 0) r
				) numbers
			`)
			require.NoErrorf(t, err, "insert batch %d", i)
		}
	})

	// Add index to trigger schema change
	writeFile(t, filepath.Join(schemaDir, "items.sql"), `
CREATE TABLE items (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_name (name)
);
`)

	var applyID string

	t.Run("apply_schema_change", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--defer-cutover",
			"--watch=false",
		)
		assertContains(t, out, "Apply started")
		applyID = parseApplyID(t, out)
		t.Logf("captured apply ID: %s", applyID)
	})

	t.Run("wait_for_running", func(t *testing.T) {
		waitForAnyStateByApplyID(t, endpoint, applyID,
			[]string{"running", "waiting_for_cutover"}, 30*time.Second)
	})

	// Stop using positional apply_id
	t.Run("stop_by_apply_id", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "stop",
			applyID,
			"--endpoint", endpoint,
		)
		assertContains(t, out, "Schema change stopped")
		waitForState(t, endpoint, applyID, "stopped", 30*time.Second)
	})

	// Start using positional apply_id
	t.Run("start_by_apply_id", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "start",
			applyID,
			"--endpoint", endpoint,
			"--watch=false",
		)
		assertContains(t, out, "Schema change resumed")
	})

	t.Run("wait_for_cutover", func(t *testing.T) {
		waitForState(t, endpoint, applyID, "waiting_for_cutover", 60*time.Second)
	})

	t.Run("cutover", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "cutover",
			applyID,
			"--endpoint", endpoint,
			"--watch=false",
		)
		assertContains(t, out, "Cutover requested")
	})

	t.Run("wait_for_complete", func(t *testing.T) {
		waitForState(t, endpoint, applyID, "completed", 30*time.Second)
	})

	t.Log("Stop/start by apply-id completed successfully")
}

// waitForApplyFromOutput parses an apply_id from CLI output and waits for the
// specified state by polling the progress-by-apply-id endpoint. This is the
// preferred wait method when multiple applies exist for the same database.
func waitForApplyFromOutput(t *testing.T, endpoint, cliOutput, expectedState string, timeout time.Duration) {
	t.Helper()
	applyID := parseApplyID(t, cliOutput)
	waitForState(t, endpoint, applyID, expectedState, timeout)
}

// waitForAnyStateByApplyID polls the progress API until any of the expected states is reached or timeout.
func waitForAnyStateByApplyID(t *testing.T, endpoint, applyID string, expectedStates []string, timeout time.Duration) {
	t.Helper()
	normalized := make([]string, len(expectedStates))
	for i, s := range expectedStates {
		normalized[i] = state.NormalizeState(s)
	}
	deadline := time.Now().Add(timeout)
	var lastState string
	for time.Now().Before(deadline) {
		s, err := fetchProgressState(endpoint, applyID)
		if err == nil {
			lastState = s
			if slices.Contains(normalized, s) {
				return
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	require.Failf(t, "timeout", "timeout waiting for apply %s state %v, last state: %q", applyID, expectedStates, lastState)
}

// startSchemaBotLocal starts a SchemaBot server with embedded LocalClient (no gRPC).
// Cleanup is registered via t.Cleanup.
func startSchemaBotLocal(t *testing.T) string {
	t.Helper()

	// Connect to SchemaBot storage and clear stale state from prior tests.
	db, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err, "open schemabot db")
	t.Cleanup(func() { utils.CloseAndLog(db) })
	clearStorageDB(t, db)

	storage := mysqlstore.New(db)

	// Build DSN for testdb (the target database for schema changes)
	// targetDSN points to tern_test (storage), testdbDSN points to testdb (target)
	testdbDSN := strings.Replace(targetDSN, "/target_test", "/testdb", 1)

	// Create actual LocalClient client (embedded, no gRPC)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	localClient, err := tern.NewLocalClient(tern.LocalConfig{
		Database:  "testdb",
		Type:      "mysql",
		TargetDSN: testdbDSN,
	}, storage, logger)
	require.NoError(t, err, "create local tern client")
	t.Cleanup(func() { utils.CloseAndLog(localClient) })

	// Create SchemaBot config with database registration (LocalClient mode)
	// Use "default" as the key since CLI defaults deployment to "default"
	config := &schemabotapi.ServerConfig{
		Databases: map[string]schemabotapi.DatabaseConfig{
			"default": {
				Type: "mysql",
				Environments: map[string]schemabotapi.EnvironmentConfig{
					"staging": {DSN: testdbDSN},
				},
			},
		},
	}

	// Pre-register the LocalClient with "default/staging" key
	ternClients := map[string]tern.Client{
		"default/staging": localClient,
	}

	svc := schemabotapi.New(storage, config, ternClients, logger)
	t.Cleanup(func() { utils.CloseAndLog(svc) })

	// Start HTTP server
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")

	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) //nolint:usetesting // runs after test context cancelled
		defer cancel()
		_ = server.Shutdown(ctx)
	})

	addr := listener.Addr().String()

	// Wait for server to be ready
	waitForHTTP(t, "http://"+addr+"/health", 5*time.Second)

	return addr
}

// startSchemaBotLocalDB starts a SchemaBot server with embedded LocalClient for a specific database.
// Creates the database if it doesn't exist. Cleanup is registered via t.Cleanup.
func startSchemaBotLocalDB(t *testing.T, dbName string) string {
	t.Helper()

	// Connect to SchemaBot storage and clear stale state from prior tests.
	db, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err, "open schemabot db")
	t.Cleanup(func() { utils.CloseAndLog(db) })
	clearStorageDB(t, db)

	storage := mysqlstore.New(db)

	// Create the target database
	targetDB, err := sql.Open("mysql", targetDSN+"&multiStatements=true")
	require.NoError(t, err, "open target db connection")
	t.Cleanup(func() {
		_, _ = targetDB.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+dbName) //nolint:usetesting // runs after test context cancelled
		_ = targetDB.Close()
	})

	_, err = targetDB.ExecContext(t.Context(), "CREATE DATABASE IF NOT EXISTS "+dbName)
	require.NoErrorf(t, err, "create database %s", dbName)

	// Build DSN for the target database
	dbDSN := strings.Replace(targetDSN, "/target_test", "/"+dbName, 1)

	// Create actual LocalClient client (embedded, no gRPC)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	localClient, err := tern.NewLocalClient(tern.LocalConfig{
		Database:  dbName,
		Type:      "mysql",
		TargetDSN: dbDSN,
	}, storage, logger)
	require.NoError(t, err, "create local tern client")
	t.Cleanup(func() { utils.CloseAndLog(localClient) })

	// Create SchemaBot config with database registration (LocalClient mode)
	// Use dbName as the key since CLI uses the database name from schemabot.yaml
	config := &schemabotapi.ServerConfig{
		Databases: map[string]schemabotapi.DatabaseConfig{
			dbName: {
				Type: "mysql",
				Environments: map[string]schemabotapi.EnvironmentConfig{
					"staging": {DSN: dbDSN},
				},
			},
		},
	}

	// Pre-register the LocalClient keyed by database/environment
	ternClients := map[string]tern.Client{
		dbName + "/staging": localClient,
	}

	svc := schemabotapi.New(storage, config, ternClients, logger)
	t.Cleanup(func() { utils.CloseAndLog(svc) })

	// Start HTTP server
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")

	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) //nolint:usetesting // runs after test context cancelled
		defer cancel()
		_ = server.Shutdown(ctx)
	})

	addr := listener.Addr().String()

	// Wait for server to be ready
	waitForHTTP(t, "http://"+addr+"/health", 5*time.Second)

	return addr
}

// startSchemaBotWithGRPC starts a SchemaBot server that connects to a Tern gRPC server.
// Cleanup is registered via t.Cleanup.
func startSchemaBotWithGRPC(t *testing.T) string {
	t.Helper()

	// Connect to SchemaBot storage and clear stale state from prior tests.
	db, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err, "open schemabot db")
	t.Cleanup(func() { utils.CloseAndLog(db) })
	clearStorageDB(t, db)

	storage := mysqlstore.New(db)

	// Create gRPC client to connect to the Tern gRPC server
	ternClient, err := tern.NewGRPCClient(tern.Config{Address: grpcAddr})
	require.NoError(t, err, "create tern client")
	t.Cleanup(func() { utils.CloseAndLog(ternClient) })

	// Create SchemaBot config with TernDeployments to route requests to gRPC backend
	config := &schemabotapi.ServerConfig{
		TernDeployments: schemabotapi.TernConfig{
			"default": schemabotapi.TernEndpoints{
				"staging": grpcAddr,
			},
		},
	}

	// Pre-register the client for default/staging
	ternClients := map[string]tern.Client{
		"default/staging": ternClient,
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := schemabotapi.New(storage, config, ternClients, logger)
	t.Cleanup(func() { utils.CloseAndLog(svc) })

	// Start HTTP server
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")

	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) //nolint:usetesting // runs after test context cancelled
		defer cancel()
		_ = server.Shutdown(ctx)
	})

	addr := listener.Addr().String()

	// Wait for server to be ready
	waitForHTTP(t, "http://"+addr+"/health", 5*time.Second)

	return addr
}

// buildBinary builds a Go binary and returns the path to it.
func buildBinary(t *testing.T, name, pkg string) string {
	t.Helper()

	// Build to a temp directory
	binDir := t.TempDir()
	binPath := filepath.Join(binDir, name)

	// Get the module root
	wd, err := os.Getwd()
	require.NoError(t, err, "getwd")
	// We're in e2e/, go up one level
	moduleRoot := filepath.Dir(wd)

	cmd := exec.CommandContext(t.Context(), "go", "build", "-o", binPath, pkg)
	cmd.Dir = moduleRoot
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	require.NoErrorf(t, cmd.Run(), "build %s\n%s", name, stderr.String())

	return binPath
}

func runCLI(t *testing.T, binPath string, args ...string) string {
	t.Helper()
	out, err := runCLIWithError(t, binPath, args...)
	require.NoErrorf(t, err, "CLI command failed\nOutput: %s", out)
	return out
}

func runCLIWithError(t *testing.T, binPath string, args ...string) (string, error) {
	t.Helper()
	return runCLIWithErrorInDir(t, binPath, "", args...)
}

// parseApplyID extracts an apply ID (e.g., "apply-abc12345") from CLI output.
func parseApplyID(t *testing.T, output string) string {
	t.Helper()
	re := regexp.MustCompile(`apply-[a-f0-9]+`)
	match := re.FindString(output)
	require.NotEmptyf(t, match, "no apply ID found in output:\n%s", output)
	return match
}

// waitForHTTP waits for an HTTP endpoint to be available.
// clearStorageDB deletes all rows from every table in the SchemaBot storage
// database. Called before each test to prevent stale plans/tasks/applies/locks
// from prior tests from interfering with state polling.
func clearStorageDB(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "SHOW TABLES")
	require.NoError(t, err, "clearStorageDB: SHOW TABLES")
	defer utils.CloseAndLog(rows)

	var tables []string
	for rows.Next() {
		var table string
		require.NoError(t, rows.Scan(&table), "clearStorageDB: scan table name")
		tables = append(tables, table)
	}
	for _, table := range tables {
		_, err := db.ExecContext(t.Context(), "DELETE FROM `"+table+"`")
		require.NoErrorf(t, err, "clearStorageDB: DELETE FROM %s", table)
	}
}

func waitForHTTP(t *testing.T, url string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
		require.NoError(t, err, "create request")
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.Failf(t, "timeout", "timeout waiting for %s", url)
}

// TestCLI_Volume tests the volume command validates inputs correctly.
// Full volume functionality is tested at the engine level in spirit_integration_test.go.
func TestCLI_Volume(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	t.Run("volume_help", func(t *testing.T) {
		// Verify help output shows correct usage with positional apply_id
		out := runCLI(t, binPath, "volume", "--help")
		assertContains(t, out, "volume")
		assertContains(t, out, "apply-id")
		assertContains(t, out, "--volume")
	})

	t.Run("volume_requires_apply_id", func(t *testing.T) {
		// Volume without apply_id should fail
		_, err := runCLIWithError(t, binPath, "volume",
			"-v", "5",
			"--endpoint", "http://localhost:9999",
		)
		require.Error(t, err, "expected error when apply_id is missing")
	})
}

// TestCLI_Volume_NoActiveSchemaChange tests volume when there's nothing to adjust.
func TestCLI_Volume_NoActiveSchemaChange(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	schemabotAddr := startSchemaBotLocalDB(t, fmt.Sprintf("volumenone_%d", time.Now().UnixNano()))

	endpoint := "http://" + schemabotAddr

	t.Run("volume_with_no_schema_change", func(t *testing.T) {
		// Volume with a non-existent apply ID should fail
		_, err := runCLIWithError(t, binPath, "volume",
			"apply-nonexistent",
			"-v", "5",
			"--endpoint", endpoint,
		)
		require.Error(t, err, "expected error for volume with non-existent apply ID")
	})
}

// TestCLI_Rollback tests the rollback command validation.
func TestCLI_Rollback(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	t.Run("rollback_help", func(t *testing.T) {
		out := runCLI(t, binPath, "rollback", "--help")
		assertContains(t, out, "rollback")
		assertContains(t, out, "<apply-id>")
		assertContains(t, out, "--endpoint")
	})

	t.Run("rollback_requires_apply_id", func(t *testing.T) {
		_, err := runCLIWithError(t, binPath, "rollback",
			"--endpoint", "http://localhost:9999",
		)
		require.Error(t, err, "expected error when apply ID is missing")
	})
}

// TestCLI_Rollback_ApplyNotFound tests rollback with a nonexistent apply ID.
func TestCLI_Rollback_ApplyNotFound(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	dbName := fmt.Sprintf("rollbacknone_%d", time.Now().UnixNano())

	schemabotAddr := startSchemaBotLocalDB(t, dbName)

	endpoint := "http://" + schemabotAddr

	t.Run("rollback_apply_not_found", func(t *testing.T) {
		_, err := runCLIWithError(t, binPath, "rollback",
			"apply_nonexistent0000",
			"--endpoint", endpoint,
			"-y",
		)
		require.Error(t, err, "expected error for nonexistent apply ID")
	})
}

// TestCLI_Rollback_FullWorkflow tests the complete rollback workflow.
func TestCLI_Rollback_FullWorkflow(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	dbName := fmt.Sprintf("rollback_%d", time.Now().UnixNano())

	schemabotAddr := startSchemaBotLocalDB(t, dbName)

	schemaDir := newSchemaDirForDB(t, dbName)

	// Start with a base table
	writeFile(t, filepath.Join(schemaDir, "items.sql"), `
CREATE TABLE items (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
`)

	endpoint := "http://" + schemabotAddr

	// Step 1: Apply base schema
	t.Run("setup_base_schema", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
		)
		assertContains(t, out, "Apply started")
		waitForApplyFromOutput(t, endpoint, out, "completed", 30*time.Second)
	})

	// Step 2: Modify schema - add a column
	writeFile(t, filepath.Join(schemaDir, "items.sql"), `
CREATE TABLE items (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
`)

	var applyID string
	t.Run("apply_schema_change", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
		)
		assertContains(t, out, "Apply started")
		applyID = parseApplyID(t, out)
		waitForState(t, endpoint, applyID, "completed", 30*time.Second)
	})

	t.Run("rollback_to_original", func(t *testing.T) {
		require.NotEmpty(t, applyID, "apply ID must be set by previous subtest")
		out := runCLIInDir(t, binPath, schemaDir, "rollback",
			applyID,
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
		)
		// Should show rollback plan with DROP COLUMN
		assertContains(t, out, "Rollback started")
		waitForApplyFromOutput(t, endpoint, out, "completed", 30*time.Second)
	})

	// Step 4: Verify schema matches original (plan should show we need to add the column back)
	t.Run("plan_shows_add_column_after_rollback", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "plan",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
		)
		// Schema should show we need to add description column back
		assertContains(t, out, "ADD COLUMN")
		assertContains(t, out, "description")
	})
}

// TestCLI_FixLint tests the fix-lint command for auto-fixing schema issues.
// This test doesn't require a server - it's a local file operation.
func TestCLI_FixLint(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	t.Run("help", func(t *testing.T) {
		out := runCLI(t, binPath, "fix-lint", "--help")
		assertContains(t, out, "--schema_dir")
		assertContains(t, out, "--dry-run")
	})

	t.Run("dry_run_detects_issues", func(t *testing.T) {
		schemaDir := t.TempDir()

		// Write a schema file with multiple lint issues
		writeFile(t, filepath.Join(schemaDir, "orders.sql"), `
CREATE TABLE orders (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    amount FLOAT NOT NULL,
    created_at TIMESTAMP
) CHARSET=latin1;
`)

		out := runCLI(t, binPath, "fix-lint", "-s", schemaDir, "--dry-run")

		// Should detect all issues
		assertContains(t, out, "Would fix")
		assertContains(t, out, "BIGINT")
		assertContains(t, out, "DECIMAL")
		assertContains(t, out, "utf8mb4")
		assertContains(t, out, "DEFAULT")

		// File should NOT be modified (dry-run)
		content, err := os.ReadFile(filepath.Join(schemaDir, "orders.sql"))
		require.NoError(t, err, "read schema file")
		assert.Contains(t, string(content), "INT NOT NULL", "dry-run should not modify files")
	})

	t.Run("applies_fixes", func(t *testing.T) {
		schemaDir := t.TempDir()

		// Write a schema file with lint issues
		writeFile(t, filepath.Join(schemaDir, "users.sql"), `
CREATE TABLE users (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255)
) CHARSET=latin1;
`)

		out := runCLI(t, binPath, "fix-lint", "-s", schemaDir)

		// Should report fixes applied
		assertContains(t, out, "Fixed")
		assertContains(t, out, "BIGINT")
		assertContains(t, out, "utf8mb4")

		// File should be modified
		content, err := os.ReadFile(filepath.Join(schemaDir, "users.sql"))
		require.NoError(t, err, "read schema file")
		contentStr := string(content)

		assert.Contains(t, contentStr, "BIGINT", "expected BIGINT in fixed file")
		assert.Contains(t, strings.ToUpper(contentStr), "UTF8MB4", "expected UTF8MB4 in fixed file")
	})

	t.Run("no_issues_found", func(t *testing.T) {
		schemaDir := t.TempDir()

		// Write a schema file that's already correct (canonical format)
		writeFile(t, filepath.Join(schemaDir, "perfect.sql"),
			"CREATE TABLE `perfect` (`id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,`name` VARCHAR(255) NOT NULL) DEFAULT CHARACTER SET = UTF8MB4")

		out := runCLI(t, binPath, "fix-lint", "-s", schemaDir)

		// Should report no issues
		assertContains(t, out, "No lint issues found")
	})

	t.Run("canonicalization", func(t *testing.T) {
		schemaDir := t.TempDir()

		// Write a schema with style issues only (lowercase, no backticks)
		writeFile(t, filepath.Join(schemaDir, "style.sql"), `
create table users (id bigint not null auto_increment primary key) charset=utf8mb4;
`)

		out := runCLI(t, binPath, "fix-lint", "-s", schemaDir)

		// Should report canonicalization
		assertContains(t, out, "canonical")

		// File should be canonicalized
		content, err := os.ReadFile(filepath.Join(schemaDir, "style.sql"))
		require.NoError(t, err, "read schema file")
		contentStr := string(content)

		// Should have uppercase keywords
		assert.Contains(t, contentStr, "CREATE TABLE", "expected uppercase CREATE TABLE")
		// Should have backtick identifiers
		assert.Contains(t, contentStr, "`users`", "expected backtick identifiers")
	})
}
