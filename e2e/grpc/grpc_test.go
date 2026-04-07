//go:build e2e

// Package grpc contains gRPC mode e2e tests. These verify that SchemaBot's GRPCClient
// works correctly when communicating with remote Tern services over gRPC.
//
// Architecture under test (docker-compose.grpc.yml):
//
//	                        +----------------------------+
//	Test --HTTP-->          |      SchemaBot Server      |
//	                        |  +----------------------+  |
//	                        |  |    GRPCClient         |  |
//	                        |  +----------+-----------+  |
//	                        |             |              |
//	                        |  +----------+-----------+  |
//	                        |  |  SchemaBot Storage    |--+-->  schemabot-mysql
//	                        |  +----------------------+  |
//	                        +-------------+--------------+
//	                                      | gRPC
//	                    +-----------------+------------------+
//	                    |                                    |
//	                    v                                    v
//	+-------------------------------+   +-------------------------------+
//	|  Tern Staging (SchemaBot      |   |  Tern Production (SchemaBot   |
//	|  binary in gRPC mode)         |   |  binary in gRPC mode)         |
//	|  +-------------------------+  |   |  +-------------------------+  |
//	|  | tern.Server             |  |   |  | tern.Server             |  |
//	|  |  +- LocalClient         |  |   |  |  +- LocalClient         |  |
//	|  |      +- Spirit Engine   |--+-+ |  |      +- Spirit Engine   |--+-+
//	|  +-------------------------+  | | |  +-------------------------+  | |
//	+-------------------------------+ | +-------------------------------+ |
//	                                  v                                   v
//	                         tern-staging-mysql              tern-production-mysql
//	                         (tern db + testapp)              (tern db + testapp)
//
// Each Tern service is a SchemaBot binary with GRPC_PORT set, which starts a gRPC
// server wrapping a LocalClient. The LocalClient uses Spirit to apply schema changes
// to the testapp database on its own MySQL instance. SchemaBot connects to these Tern
// services via GRPCClient based on the tern_deployments config.
package grpc

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMain cleans up leftover state from previous runs.
func TestMain(m *testing.M) {
	// Clean up SchemaBot's state tables to ensure fresh state
	dsn := os.Getenv("E2E_SCHEMABOT_MYSQL_DSN")
	if dsn != "" {
		db, err := sql.Open("mysql", dsn)
		if err == nil {
			rows, err := db.QueryContext(context.Background(), "SHOW TABLES")
			if err == nil {
				var tables []string
				for rows.Next() {
					var table string
					_ = rows.Scan(&table)
					tables = append(tables, table)
				}
				_ = rows.Close()
				for _, table := range tables {
					_, _ = db.ExecContext(context.Background(), "DELETE FROM `"+table+"`")
				}
			}
			_ = db.Close()
		}
	}

	// Clean up leftover test tables on Tern MySQL instances
	for _, envVar := range []string{"E2E_TERN_STAGING_MYSQL_DSN", "E2E_TERN_PRODUCTION_MYSQL_DSN"} {
		ternDSN := os.Getenv(envVar)
		if ternDSN == "" {
			continue
		}
		db, err := sql.Open("mysql", ternDSN)
		if err != nil {
			continue
		}
		rows, err := db.QueryContext(context.Background(), `
			SELECT TABLE_NAME FROM information_schema.TABLES
			WHERE TABLE_SCHEMA = 'testapp' AND TABLE_TYPE = 'BASE TABLE'
		`)
		if err == nil {
			for rows.Next() {
				var name string
				_ = rows.Scan(&name)
				_, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+name+"`")
			}
			_ = rows.Close()
		}
		_ = db.Close()
	}

	os.Exit(m.Run())
}

// =============================================================================
// Health Tests
// =============================================================================

func TestGRPC_SchemaBot_Health(t *testing.T) {
	resp := grpcGet(t, "/health") //nolint:bodyclose // closed via utils.CloseAndLog
	defer utils.CloseAndLog(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result map[string]string
	grpcDecodeJSON(t, resp, &result)
	assert.Equal(t, "ok", result["status"])
}

func TestGRPC_SchemaBot_TernHealth_Staging(t *testing.T) {
	resp := grpcGet(t, "/tern-health/default/staging") //nolint:bodyclose // closed via utils.CloseAndLog
	defer utils.CloseAndLog(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result map[string]string
	grpcDecodeJSON(t, resp, &result)
	assert.Equal(t, "ok", result["status"])
	assert.Equal(t, "default", result["deployment"])
	assert.Equal(t, "staging", result["environment"])
}

func TestGRPC_SchemaBot_TernHealth_Production(t *testing.T) {
	resp := grpcGet(t, "/tern-health/default/production") //nolint:bodyclose // closed via utils.CloseAndLog
	defer utils.CloseAndLog(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result map[string]string
	grpcDecodeJSON(t, resp, &result)
	assert.Equal(t, "ok", result["status"])
	assert.Equal(t, "default", result["deployment"])
	assert.Equal(t, "production", result["environment"])
}

func TestGRPC_SchemaBot_TernHealth_UnknownDeployment(t *testing.T) {
	resp := grpcGet(t, "/tern-health/unknown/staging") //nolint:bodyclose // closed via utils.CloseAndLog
	defer utils.CloseAndLog(resp.Body)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestGRPC_SchemaBot_TernHealth_UnknownEnvironment(t *testing.T) {
	resp := grpcGet(t, "/tern-health/default/unknown") //nolint:bodyclose // closed via utils.CloseAndLog
	defer utils.CloseAndLog(resp.Body)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestGRPC_CrossService_HealthFlow(t *testing.T) {
	for _, env := range []string{"staging", "production"} {
		t.Run(env, func(t *testing.T) {
			resp := grpcGet(t, fmt.Sprintf("/tern-health/default/%s", env)) //nolint:bodyclose // closed via utils.CloseAndLog
			defer utils.CloseAndLog(resp.Body)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			var result map[string]string
			grpcDecodeJSON(t, resp, &result)
			assert.Equal(t, env, result["environment"])
		})
	}
}

// =============================================================================
// Plan + Apply Workflow Tests
// =============================================================================

// TestGRPC_PlanApply_AddColumn tests a full plan -> apply -> verify workflow.
// Creates a table, plans an ADD COLUMN change, applies it, and verifies the column exists.
func TestGRPC_PlanApply_AddColumn(t *testing.T) {
	tableName := uniqueGRPCTableName("grpc_addcol")
	grpcCreateTestTable(t, "staging", tableName, fmt.Sprintf(
		`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL)`, tableName))

	grpcSeedRows(t, "staging", tableName, "name", "CONCAT('user_', seq)", 1000)
	grpcEnsureNoActiveChange(t, "testapp", "staging")

	// Plan: add an email column
	schemaFiles := map[string]string{
		tableName + ".sql": fmt.Sprintf(
			`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, email VARCHAR(255) DEFAULT NULL);`, tableName),
	}
	plan := grpcPlan(t, "testapp", "staging", schemaFiles)
	require.NotEmpty(t, plan.PlanID, "expected non-empty plan_id")
	planTables := plan.FlatTables()
	require.NotEmpty(t, planTables, "expected at least one table change in plan")

	// Verify the plan contains ADD COLUMN
	found := false
	for _, tc := range planTables {
		if tc.TableName == tableName && strings.Contains(strings.ToUpper(tc.DDL), "ADD COLUMN") {
			found = true
			break
		}
	}
	require.True(t, found, "expected ADD COLUMN DDL for %s, got tables: %+v", tableName, planTables)

	// Apply
	apply := grpcApply(t, plan.PlanID, "testapp", "staging", nil)
	require.True(t, apply.Accepted, "apply not accepted: %s", apply.ErrorMessage)

	// Wait for completion
	grpcWaitForState(t, "testapp", "staging", "COMPLETED", 3*time.Minute)

	// Verify column exists
	assert.True(t, grpcColumnExists(t, "staging", tableName, "email"),
		"expected column 'email' to exist on %s after apply", tableName)

	// Clean up revertable state
	grpcEnsureNoActiveChange(t, "testapp", "staging")
}

// TestGRPC_StopStart tests stopping and resuming an in-progress schema change.
// Uses MODIFY COLUMN (INT -> BIGINT) to force Spirit's copy-swap process (not instant DDL).
func TestGRPC_StopStart(t *testing.T) {
	tableName := uniqueGRPCTableName("grpc_stopstart")
	grpcCreateTestTable(t, "staging", tableName, fmt.Sprintf(
		`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, data TEXT)`, tableName))

	// Seed enough rows that Spirit's copy phase takes a noticeable amount of time
	grpcSeedRows(t, "staging", tableName, "name, data",
		"CONCAT('user_', seq), REPEAT('x', 500)", 100000)

	grpcEnsureNoActiveChange(t, "testapp", "staging")

	// Plan: widen PK from INT to BIGINT (forces full table copy, not instant DDL)
	schemaFiles := map[string]string{
		tableName + ".sql": fmt.Sprintf(
			`CREATE TABLE %s (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, data TEXT);`, tableName),
	}
	plan := grpcPlan(t, "testapp", "staging", schemaFiles)
	apply := grpcApply(t, plan.PlanID, "testapp", "staging", nil)
	require.True(t, apply.Accepted, "apply not accepted: %s", apply.ErrorMessage)

	// Wait for it to be running (not just planned)
	grpcWaitForAnyState(t, "testapp", "staging", []string{"APPLYING", "COPY", "COMPLETED"}, 60*time.Second)

	// If already completed, Spirit was too fast -- skip stop/start verification
	prog := grpcProgress(t, "testapp", "staging")
	if !strings.Contains(strings.ToUpper(prog.State), "COMPLETED") {
		// Stop
		stopResp := grpcPost(t, "/api/stop", map[string]string{ //nolint:bodyclose // closed via utils.CloseAndLog
			"database":    "testapp",
			"environment": "staging",
		})
		defer utils.CloseAndLog(stopResp.Body)
		var stopResult grpcSimpleResponse
		grpcDecodeJSON(t, stopResp, &stopResult)
		require.True(t, stopResult.Accepted, "stop not accepted: %s", stopResult.ErrorMessage)

		// Verify stopped
		grpcWaitForState(t, "testapp", "staging", "STOPPED", 30*time.Second)

		// Start
		startResp := grpcPost(t, "/api/start", map[string]string{ //nolint:bodyclose // closed via utils.CloseAndLog
			"database":    "testapp",
			"environment": "staging",
		})
		defer utils.CloseAndLog(startResp.Body)
		var startResult grpcSimpleResponse
		grpcDecodeJSON(t, startResp, &startResult)
		require.True(t, startResult.Accepted, "start not accepted: %s", startResult.ErrorMessage)
	}

	// Wait for completion
	grpcWaitForState(t, "testapp", "staging", "COMPLETED", 5*time.Minute)

	grpcEnsureNoActiveChange(t, "testapp", "staging")
}

// TestGRPC_StopStart_LocalStateSync verifies that after POST /api/start,
// SchemaBot's local applies.state transitions from "stopped" to "running"
// and both /api/status and /api/progress/apply/{apply_id} reflect the
// resumed state.
func TestGRPC_StopStart_LocalStateSync(t *testing.T) {
	tableName := uniqueGRPCTableName("grpc_startsync")
	grpcCreateTestTable(t, "staging", tableName, fmt.Sprintf(
		`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, data TEXT)`, tableName))

	// Seed enough rows that Spirit's copy phase takes a noticeable amount of time
	grpcSeedRows(t, "staging", tableName, "name, data",
		"CONCAT('user_', seq), REPEAT('x', 500)", 100000)

	grpcEnsureNoActiveChange(t, "testapp", "staging")

	// Plan: widen PK from INT to BIGINT (forces full table copy, not instant DDL)
	schemaFiles := map[string]string{
		tableName + ".sql": fmt.Sprintf(
			`CREATE TABLE %s (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, data TEXT);`, tableName),
	}
	plan := grpcPlan(t, "testapp", "staging", schemaFiles)
	apply := grpcApply(t, plan.PlanID, "testapp", "staging", nil)
	require.True(t, apply.Accepted, "apply not accepted: %s", apply.ErrorMessage)
	require.NotEmpty(t, apply.ApplyID, "expected non-empty apply_id")

	// Wait for it to be running (not just planned)
	grpcWaitForAnyState(t, "testapp", "staging", []string{"APPLYING", "COPY", "RUNNING", "COMPLETED"}, 60*time.Second)

	// If already completed, Spirit was too fast -- skip stop/start verification
	prog := grpcProgress(t, "testapp", "staging")
	if state.IsState(prog.State, state.Apply.Completed) {
		t.Skip("Apply completed before we could stop — tables processed too fast")
	}

	// Stop
	stopResp := grpcPost(t, "/api/stop", map[string]string{
		"database":    "testapp",
		"environment": "staging",
		"apply_id":    apply.ApplyID,
	})
	defer stopResp.Body.Close()
	var stopResult grpcSimpleResponse
	grpcDecodeJSON(t, stopResp, &stopResult)
	require.True(t, stopResult.Accepted, "stop not accepted: %s", stopResult.ErrorMessage)

	// Verify stopped via database-based progress (calls Tern directly)
	grpcWaitForState(t, "testapp", "staging", "STOPPED", 30*time.Second)

	// Wait for the background poller to sync the stopped state to local storage.
	grpcWaitForStatusState(t, apply.ApplyID, state.Apply.Stopped, false, 30*time.Second)

	startResp := grpcPost(t, "/api/start", map[string]string{
		"database":    "testapp",
		"environment": "staging",
		"apply_id":    apply.ApplyID,
	})
	defer startResp.Body.Close()
	var startResult grpcSimpleResponse
	grpcDecodeJSON(t, startResp, &startResult)
	require.True(t, startResult.Accepted, "start not accepted: %s", startResult.ErrorMessage)

	// After start, local storage must no longer show "stopped".
	grpcWaitForStatusState(t, apply.ApplyID, state.Apply.Stopped, true, 30*time.Second)

	// Also verify the apply_id-based progress endpoint reflects the resumed state.
	// handleProgressByApplyID serves from local storage for terminal states,
	// so if applies.state is still "stopped" it will return stale data.
	require.Eventually(t, func() bool {
		p := grpcProgressByApplyID(t, apply.ApplyID)
		return !state.IsState(p.State, state.Apply.Stopped)
	}, 30*time.Second, 500*time.Millisecond,
		"progress by apply_id must not show stopped after start")

	// Wait for completion
	grpcWaitForState(t, "testapp", "staging", "COMPLETED", 5*time.Minute)

	grpcEnsureNoActiveChange(t, "testapp", "staging")
}

// TestGRPC_Volume tests adjusting the schema change speed during an apply.
// Uses MODIFY COLUMN (INT -> BIGINT) to force Spirit's copy-swap process (not instant DDL).
func TestGRPC_Volume(t *testing.T) {
	tableName := uniqueGRPCTableName("grpc_volume")
	grpcCreateTestTable(t, "staging", tableName, fmt.Sprintf(
		`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, data TEXT)`, tableName))

	grpcSeedRows(t, "staging", tableName, "name, data",
		"CONCAT('user_', seq), REPEAT('x', 500)", 100000)

	grpcEnsureNoActiveChange(t, "testapp", "staging")

	// Plan: widen PK from INT to BIGINT (forces full table copy, not instant DDL)
	schemaFiles := map[string]string{
		tableName + ".sql": fmt.Sprintf(
			`CREATE TABLE %s (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, data TEXT);`, tableName),
	}
	plan := grpcPlan(t, "testapp", "staging", schemaFiles)
	apply := grpcApply(t, plan.PlanID, "testapp", "staging", nil)
	require.True(t, apply.Accepted, "apply not accepted: %s", apply.ErrorMessage)

	grpcWaitForAnyState(t, "testapp", "staging", []string{"APPLYING", "COPY", "COMPLETED"}, 60*time.Second)

	// Try to adjust volume (may fail if Spirit completed too fast -- that's OK)
	prog := grpcProgress(t, "testapp", "staging")
	if !strings.Contains(strings.ToUpper(prog.State), "COMPLETED") {
		resp := grpcPost(t, "/api/volume", map[string]any{ //nolint:bodyclose // closed via utils.CloseAndLog
			"database":    "testapp",
			"environment": "staging",
			"volume":      5,
		})
		defer utils.CloseAndLog(resp.Body)
		if resp.StatusCode == http.StatusOK {
			var volResp grpcVolumeResponse
			grpcDecodeJSON(t, resp, &volResp)
			t.Logf("volume adjustment accepted=%v", volResp.Accepted)
		} else {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			t.Logf("volume returned status %d (may have completed): %s", resp.StatusCode, string(body))
		}
	}

	// Wait for completion
	grpcWaitForState(t, "testapp", "staging", "COMPLETED", 5*time.Minute)

	grpcEnsureNoActiveChange(t, "testapp", "staging")
}

// TestGRPC_DeferCutover tests the deferred cutover workflow.
// Uses MODIFY COLUMN (INT -> BIGINT) to force Spirit's copy-swap process (not instant DDL).
// With defer_cutover, Spirit pauses at WAITING_FOR_CUTOVER until explicitly triggered.
func TestGRPC_DeferCutover(t *testing.T) {
	tableName := uniqueGRPCTableName("grpc_cutover")
	grpcCreateTestTable(t, "staging", tableName, fmt.Sprintf(
		`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, data TEXT)`, tableName))

	// Seed enough rows so Spirit goes through full copy-swap (not instant DDL)
	grpcSeedRows(t, "staging", tableName, "name, data",
		"CONCAT('user_', seq), REPEAT('x', 200)", 10000)

	grpcEnsureNoActiveChange(t, "testapp", "staging")

	// Plan: widen PK from INT to BIGINT (forces full table copy, not instant DDL)
	schemaFiles := map[string]string{
		tableName + ".sql": fmt.Sprintf(
			`CREATE TABLE %s (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, data TEXT);`, tableName),
	}
	plan := grpcPlan(t, "testapp", "staging", schemaFiles)

	// Apply with defer_cutover
	apply := grpcApply(t, plan.PlanID, "testapp", "staging", map[string]string{
		"defer_cutover": "true",
	})
	require.True(t, apply.Accepted, "apply not accepted: %s", apply.ErrorMessage)

	// Wait for waiting_for_cutover or completed (Spirit may be too fast even with copy-swap)
	finalState := grpcWaitForAnyState(t, "testapp", "staging",
		[]string{"WAITING_FOR_CUTOVER", "COMPLETED"}, 3*time.Minute)

	if strings.Contains(strings.ToUpper(finalState), "WAITING_FOR_CUTOVER") {
		// Trigger cutover
		resp := grpcPost(t, "/api/cutover", map[string]string{ //nolint:bodyclose // closed via utils.CloseAndLog
			"database":    "testapp",
			"environment": "staging",
		})
		defer utils.CloseAndLog(resp.Body)
		var cutoverResp grpcSimpleResponse
		grpcDecodeJSON(t, resp, &cutoverResp)
		require.True(t, cutoverResp.Accepted, "cutover not accepted: %s", cutoverResp.ErrorMessage)

		// Wait for completion
		grpcWaitForState(t, "testapp", "staging", "COMPLETED", 2*time.Minute)
	}

	grpcEnsureNoActiveChange(t, "testapp", "staging")
}

// TestGRPC_Status_StaleState verifies that the status endpoint reflects the real state
// from Tern, not just the stale local storage state.
//
// Bug: In gRPC mode, SchemaBot stores applies with state "pending" and never updates
// the local state from Tern. The progress endpoint works correctly (it calls Tern's
// Progress RPC), but status reads only from local storage.
//
// This test applies a schema change, waits for Tern to complete it (via progress),
// then checks that status reports the correct state — not "pending".
func TestGRPC_Status_StaleState(t *testing.T) {
	tableName := uniqueGRPCTableName("grpc_status")
	grpcCreateTestTable(t, "staging", tableName, fmt.Sprintf(
		`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL)`, tableName))

	grpcSeedRows(t, "staging", tableName, "name", "CONCAT('user_', seq)", 1000)
	grpcEnsureNoActiveChange(t, "testapp", "staging")

	// Plan: add an email column
	schemaFiles := map[string]string{
		tableName + ".sql": fmt.Sprintf(
			`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, email VARCHAR(255) DEFAULT NULL);`, tableName),
	}
	plan := grpcPlan(t, "testapp", "staging", schemaFiles)
	require.NotEmpty(t, plan.PlanID)

	// Apply
	apply := grpcApply(t, plan.PlanID, "testapp", "staging", nil)
	require.True(t, apply.Accepted, "apply not accepted: %s", apply.ErrorMessage)

	// Wait for completion via progress (this calls Tern's Progress RPC)
	grpcWaitForState(t, "testapp", "staging", "COMPLETED", 3*time.Minute)

	// Status reads from local storage, which is updated asynchronously by the
	// background poller. Poll until the poller has synced the completed state.
	var found *grpcStatusApplyEntry
	require.Eventually(t, func() bool {
		status := grpcStatus(t)
		for i := range status.Applies {
			if status.Applies[i].ApplyID == apply.ApplyID {
				found = &status.Applies[i]
				break
			}
		}
		return found != nil &&
			strings.Contains(strings.ToUpper(found.State), "COMPLETED") &&
			status.ActiveCount == 0
	}, 30*time.Second, 500*time.Millisecond,
		"status did not reflect completed state before timeout")

	// Regression guard: status must not remain pending after Tern completes.
	assert.NotEqual(t, "pending", found.State,
		"status must reflect Tern's completed state, not stale pending")
	assert.Contains(t, strings.ToUpper(found.State), "COMPLETED",
		"status should show completed state after Tern finishes the apply")

	grpcEnsureNoActiveChange(t, "testapp", "staging")
}

// TestGRPC_CrossService_PlanApply_Production tests that the production Tern service
// works independently from staging.
func TestGRPC_CrossService_PlanApply_Production(t *testing.T) {
	tableName := uniqueGRPCTableName("grpc_prod")
	grpcCreateTestTable(t, "production", tableName, fmt.Sprintf(
		`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL)`, tableName))

	grpcSeedRows(t, "production", tableName, "name", "CONCAT('user_', seq)", 1000)
	grpcEnsureNoActiveChange(t, "testapp", "production")

	// Plan: add a column
	colName := "prod_col"
	schemaFiles := map[string]string{
		tableName + ".sql": fmt.Sprintf(
			`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, %s VARCHAR(255) DEFAULT NULL);`, tableName, colName),
	}
	plan := grpcPlan(t, "testapp", "production", schemaFiles)
	require.NotEmpty(t, plan.PlanID, "expected non-empty plan_id")

	// Apply
	apply := grpcApply(t, plan.PlanID, "testapp", "production", nil)
	require.True(t, apply.Accepted, "apply not accepted: %s", apply.ErrorMessage)

	// Wait for completion
	grpcWaitForState(t, "testapp", "production", "COMPLETED", 3*time.Minute)

	// Verify column exists
	assert.True(t, grpcColumnExists(t, "production", tableName, colName),
		"expected column %q to exist on %s in production", colName, tableName)

	grpcEnsureNoActiveChange(t, "testapp", "production")
}
