//go:build e2e

// Package k8s contains Kubernetes e2e tests. These verify that the SchemaBot Helm
// chart deploys correctly on minikube and that the two-tier gRPC architecture
// (control plane with GRPCClient → data plane with LocalClient) works end-to-end.
//
// # Why two tiers?
//
// The two-tier architecture separates the control plane (API, GitHub integration,
// state management) from the data plane (database access, DDL execution). This is
// useful when:
//
//   - Strict tenant isolation: the data plane runs in the database owner's network,
//     and the control plane never has direct database credentials.
//   - Access control: the control plane only speaks gRPC to the data plane — it
//     cannot reach the target database directly, limiting blast radius.
//   - Network boundaries: control plane and data plane can live in different VPCs,
//     accounts, or clusters, connected only by a gRPC endpoint.
//
// Architecture under test:
//
//	                      +-------------------------------+
//	Test --HTTP-->        |   Control Plane (Helm)        |
//	                      |   +-------------------------+ |
//	                      |   |       GRPCClient        | |
//	                      |   +-----------+-------------+ |
//	                      |               |               |
//	                      |   SchemaBot Storage ----------+-->  mysql-control-plane
//	                      +---------------+---------------+
//	                                      | gRPC (:13370)
//	                                      v
//	                      +-------------------------------+
//	                      |   Data Plane (Helm)           |
//	                      |   +-------------------------+ |
//	                      |   |  LocalClient + Spirit   | |
//	                      |   +-----------+-------------+ |
//	                      |               |               |
//	                      |   Tern Storage + Target ------+-->  mysql-data-plane
//	                      +-------------------------------+     (tern db + testapp db)
//
// Both tiers are deployed via the same Helm chart with different values.
// The control plane uses tern_deployments (GRPCClient), the data plane uses
// databases (LocalClient) with grpc.enabled=true.
package k8s

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/block/schemabot/e2e/testutil"
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/spirit/pkg/lint"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var deployOpts = client.PlanOptions{Deployment: "data-plane"}

func storageDSNs(t *testing.T) []string {
	t.Helper()
	cfg, err := mysql.ParseDSN(testutil.TernStagingDSN(t))
	require.NoError(t, err, "parse tern DSN")
	cfg.DBName = "tern"
	return []string{
		cfg.FormatDSN(),
		testutil.SchemabotDSN(t),
	}
}

// cleanupState registers storage cleanup to run when the test finishes.
func cleanupState(t *testing.T) {
	t.Helper()
	dsns := storageDSNs(t)
	t.Cleanup(func() {
		for _, d := range dsns {
			testutil.ClearAllTables(t, d)
		}
	})
}

// httpGet performs a GET request and returns the response. The caller must close the body.
func httpGet(t *testing.T, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

// =============================================================================
// Health Tests
// =============================================================================

func TestK8s_ControlPlane_Health(t *testing.T) {
	resp := httpGet(t, testutil.Endpoint(t)+"/health")
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	var result map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.Equal(t, "ok", result["status"])
}

func TestK8s_DataPlaneHealth(t *testing.T) {
	resp := httpGet(t, testutil.Endpoint(t)+"/tern-health/data-plane/staging")
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	var result map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.Equal(t, "ok", result["status"])
	assert.Equal(t, "data-plane", result["deployment"])
	assert.Equal(t, "staging", result["environment"])
}

// =============================================================================
// Plan + Apply Workflow Tests
// =============================================================================

// TestK8s_PlanApply_AddColumn tests plan → apply → verify through the two-tier
// gRPC path in Kubernetes. The control plane receives the HTTP request and
// delegates to the data plane over gRPC, which runs Spirit against the target DB.
func TestK8s_PlanApply_AddColumn(t *testing.T) {
	ep, dsn := testutil.Endpoint(t), testutil.TernStagingDSN(t)
	tableName := testutil.UniqueTableName("k8s_addcol")

	testutil.CreateTestTableWithCleanup(t, dsn, tableName, fmt.Sprintf(
		`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL)`, tableName),
		storageDSNs(t)...)

	schemaFiles := map[string]string{
		tableName + ".sql": fmt.Sprintf(
			`CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, email VARCHAR(255) DEFAULT NULL);`, tableName),
	}

	planResp, err := client.CallPlanAPIWithFiles(ep, "testapp", "mysql", "staging",
		map[string]*apitypes.SchemaFiles{"testapp": {Files: schemaFiles}}, "", 0, deployOpts)
	require.NoError(t, err)
	require.NotEmpty(t, planResp.PlanID)

	found := false
	for _, tc := range planResp.FlatTables() {
		if tc.TableName == tableName {
			assert.Contains(t, tc.DDL, "email", "DDL should reference the email column")
			found = true
			break
		}
	}
	require.True(t, found, "expected table change for %s in plan", tableName)

	applyResp, err := client.CallApplyAPI(ep, planResp.PlanID, "testapp", "staging", "", nil, deployOpts)
	require.NoError(t, err)
	require.True(t, applyResp.Accepted, "apply not accepted: %s", applyResp.ErrorMessage)

	testutil.WaitForState(t, ep, applyResp.ApplyID, state.Apply.Completed, testutil.PollDeadline)

	// Verify column exists on the target database
	assert.True(t, testutil.ColumnExists(t, dsn, tableName, "email"),
		"expected column 'email' to exist on %s after apply", tableName)

	// Verify the completed apply via the progress API
	prog, err := testutil.FetchProgress(ep, applyResp.ApplyID)
	require.NoError(t, err, "fetch progress for completed apply")
	assert.True(t, state.IsState(prog.State, state.Apply.Completed), "state should be completed, got: %s", prog.State)
	require.NotEmpty(t, prog.Tables, "tables should be present in progress")
	assert.Equal(t, tableName, prog.Tables[0].TableName, "progress should report the correct table")

	// Verify external_id on the control plane's storage — this is set from the
	// data plane's apply ID returned over gRPC. Its presence proves the gRPC
	// Apply() call succeeded and the response was persisted.
	sbDB, err := sql.Open("mysql", testutil.SchemabotDSN(t))
	require.NoError(t, err)
	defer utils.CloseAndLog(sbDB)

	var externalID string
	err = sbDB.QueryRowContext(t.Context(),
		"SELECT external_id FROM applies WHERE apply_identifier = ?",
		applyResp.ApplyID,
	).Scan(&externalID)
	require.NoError(t, err, "query apply record on control plane")
	assert.NotEmpty(t, externalID, "external_id should be set — proves data plane returned its apply ID over gRPC")
}

// TestK8s_PlanApply_CreateTable tests creating a new table through the two-tier path.
func TestK8s_PlanApply_CreateTable(t *testing.T) {
	ep, dsn := testutil.Endpoint(t), testutil.TernStagingDSN(t)
	tableName := testutil.UniqueTableName("k8s_create")
	cleanupState(t)

	schemaFiles := map[string]string{
		tableName + ".sql": fmt.Sprintf(
			`CREATE TABLE %s (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, value VARCHAR(100) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, tableName),
	}

	applyID := testutil.ApplySchemaAndWait(t, ep, "testapp", "mysql", "staging", schemaFiles, testutil.PollDeadline, deployOpts)
	require.NotEmpty(t, applyID)

	// Verify the table was created on the target database with the correct columns
	tables, err := lint.LoadSchemaFromDSN(t.Context(), dsn)
	require.NoError(t, err)
	tbl := testutil.FindTable(tables, tableName)
	require.NotNil(t, tbl, "expected table %s to exist on target database", tableName)
	assert.NotNil(t, tbl.Columns.ByName("id"), "expected column 'id'")
	assert.NotNil(t, tbl.Columns.ByName("value"), "expected column 'value'")

	// Register cleanup for the table the apply created
	t.Cleanup(func() {
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return
		}
		defer utils.CloseAndLog(db)
		_, _ = db.ExecContext(t.Context(), "DROP TABLE IF EXISTS `"+tableName+"`")
	})
}

// TestK8s_Progress tests that progress reporting works over the gRPC path.
// Adds an index on a table with 500k rows to force Spirit's copy phase.
func TestK8s_Progress(t *testing.T) {
	ep, dsn := testutil.Endpoint(t), testutil.TernStagingDSN(t)
	tableName := testutil.UniqueTableName("k8s_progress")

	testutil.CreateTestTableWithCleanup(t, dsn, tableName, fmt.Sprintf(
		`CREATE TABLE %s (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, email VARCHAR(255) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`, tableName),
		storageDSNs(t)...)

	testutil.SeedRows(t, dsn, tableName, "name, email",
		"CONCAT('user_', seq), CONCAT('user_', seq, '@example.com')", 500000)

	// Plan: add an index on email (forces Spirit full table copy)
	schemaFiles := map[string]string{
		tableName + ".sql": fmt.Sprintf(
			`CREATE TABLE %s (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, email VARCHAR(255) DEFAULT NULL, KEY idx_email (email)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, tableName),
	}

	_, applyID := testutil.PlanAndApply(t, ep, "testapp", "mysql", "staging", schemaFiles, nil, deployOpts)

	// Wait for Running — 500k rows ensures Spirit's copy phase is always observable
	testutil.WaitForState(t, ep, applyID, state.Apply.Running, testutil.PollDeadline)

	prog, err := testutil.FetchProgress(ep, applyID)
	require.NoError(t, err)
	require.NotEmpty(t, prog.Tables, "expected tables in progress response")
	assert.Equal(t, tableName, prog.Tables[0].TableName, "progress should report the correct table")
	assert.Contains(t, prog.Tables[0].DDL, "idx_email", "progress DDL should reference the index being added")

	testutil.WaitForState(t, ep, applyID, state.Apply.Completed, testutil.PollDeadline)
}

// TestK8s_Scheduler_DataPlanePodRestartRecoversIndexAdd verifies scheduler
// recovery across the two-tier Kubernetes deployment. The control plane keeps
// the user-facing apply alive while the data plane pod, which owns the Spirit
// worker and target database access, is replaced mid-apply.
func TestK8s_Scheduler_DataPlanePodRestartRecoversIndexAdd(t *testing.T) {
	fixture := startRunningIndexAddApply(t, "k8s_sched_dp")

	crashedPod := crashPod(t, "data-plane")

	// The restarted data-plane scheduler claims stale local apply rows. Aging
	// the heartbeat avoids waiting for the production staleness threshold.
	markDataPlaneHeartbeatStale(t, fixture.DataPlaneApplyID)
	waitForReplacementPodReady(t, "data-plane", crashedPod, 2*time.Minute)
	waitForTernHealth(t, fixture.Endpoint, "data-plane", "staging", testutil.PollDeadline)

	testutil.WaitForState(t, fixture.Endpoint, fixture.ApplyID, state.Apply.Completed, 3*time.Minute)
	waitForIndex(t, fixture.TargetDSN, fixture.TableName, "idx_account_created", testutil.PollDeadline)
}

// TestK8s_Scheduler_ControlPlanePodRestartReconnectsToRunningDataPlane verifies
// that control-plane recovery restarts only the gRPC progress poller. The data
// plane keeps running the schema change while the control-plane pod is replaced.
func TestK8s_Scheduler_ControlPlanePodRestartReconnectsToRunningDataPlane(t *testing.T) {
	fixture := startRunningIndexAddApply(t, "k8s_sched_cp")

	crashedPod := crashPod(t, "control-plane")

	// The restarted control-plane scheduler claims the stale SchemaBot apply
	// row, then GRPCClient resumes progress polling using the data-plane apply ID.
	markControlPlaneHeartbeatStale(t, fixture.ApplyID)
	waitForReplacementPodReady(t, "control-plane", crashedPod, 2*time.Minute)

	recoveredEndpoint := startControlPlanePortForward(t)
	waitForTernHealth(t, recoveredEndpoint, "data-plane", "staging", testutil.PollDeadline)

	testutil.WaitForState(t, recoveredEndpoint, fixture.ApplyID, state.Apply.Completed, 3*time.Minute)
	waitForIndex(t, fixture.TargetDSN, fixture.TableName, "idx_account_created", testutil.PollDeadline)
}
