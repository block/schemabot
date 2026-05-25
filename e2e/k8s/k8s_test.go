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
// The control plane uses databases for server-side target/deployment routing
// plus tern_deployments for gRPC endpoints. The data plane uses databases
// with DSNs (LocalClient) and grpc.enabled=true.
package k8s

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/block/schemabot/e2e/testutil"
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/lint"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

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
		map[string]*apitypes.SchemaFiles{"testapp": {Files: schemaFiles}}, "", 0)
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

	applyResp, err := client.CallApplyAPI(ep, planResp.PlanID, "staging", "", nil)
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

	// Verify lifecycle fields on the control plane's storage. external_id is
	// the data-plane apply ID returned over gRPC, while started_at/completed_at
	// prove the control-plane poller persisted the remote lifecycle to storage.
	sbDB, err := sql.Open("mysql", testutil.SchemabotDSN(t))
	require.NoError(t, err)
	defer utils.CloseAndLog(sbDB)

	externalID, startedAt, completedAt := waitForControlPlaneApplyLifecycle(t, sbDB, applyResp.ApplyID)
	assert.NotEmpty(t, externalID, "external_id should be set — proves data plane returned its apply ID over gRPC")
	assert.False(t, completedAt.Before(startedAt), "completed_at should not be before started_at")

	// The control plane owns the operator-facing apply history even though the
	// data plane executes the schema change. Logs should show dispatch to the
	// data plane and the terminal state observed by the control-plane poller.
	waitForControlPlaneApplyLogs(t, ep, applyResp.ApplyID, tableName)
}

func waitForControlPlaneApplyLifecycle(t *testing.T, db *sql.DB, applyID string) (string, time.Time, time.Time) {
	t.Helper()

	var externalID string
	var startedAt, completedAt sql.NullTime
	var lastErr error
	testutil.Poll(t, testutil.PollDeadline, testutil.PollInterval,
		func() bool {
			lastErr = db.QueryRowContext(t.Context(),
				"SELECT external_id, started_at, completed_at FROM applies WHERE apply_identifier = ?",
				applyID,
			).Scan(&externalID, &startedAt, &completedAt)
			if lastErr != nil {
				return false
			}
			return externalID != "" && startedAt.Valid && completedAt.Valid && !completedAt.Time.Before(startedAt.Time)
		},
		func() string {
			return fmt.Sprintf("control-plane apply lifecycle was not persisted for %s: external_id=%q started_at=%v completed_at=%v last_err=%v",
				applyID, externalID, startedAt, completedAt, lastErr)
		})

	return externalID, startedAt.Time, completedAt.Time
}

func waitForControlPlaneApplyLogs(t *testing.T, endpoint string, applyID, tableName string) {
	t.Helper()

	var logs []*client.LogEntry
	var lastErr error
	testutil.Poll(t, testutil.PollDeadline, testutil.PollInterval,
		func() bool {
			logs, lastErr = client.GetLogs(endpoint, "", "", applyID, 50)
			return lastErr == nil &&
				hasLogNewState(logs, "Apply queued", state.Apply.Pending) &&
				hasLogTransition(logs, "Apply dispatched to remote Tern", state.Apply.Pending, state.Apply.Running) &&
				hasLogTransition(logs, "Remote apply reached terminal state: completed", state.Apply.Running, state.Apply.Completed) &&
				hasLogTransitionTo(logs, "Remote task "+tableName+" changed state", state.Task.Completed)
		},
		func() string {
			return fmt.Sprintf("control-plane apply logs did not contain the full remote lifecycle for %s: api_logs=%s last_err=%v",
				applyID, formatLogEntries(logs), lastErr)
		})
}

func hasLogNewState(logs []*client.LogEntry, messageContains, newState string) bool {
	for _, log := range logs {
		if strings.Contains(log.Message, messageContains) && log.NewState == newState {
			return true
		}
	}
	return false
}

func hasLogTransition(logs []*client.LogEntry, messageContains, oldState, newState string) bool {
	for _, log := range logs {
		if strings.Contains(log.Message, messageContains) &&
			log.EventType == storage.LogEventStateTransition &&
			log.OldState == oldState &&
			log.NewState == newState {
			return true
		}
	}
	return false
}

func hasLogTransitionTo(logs []*client.LogEntry, messageContains, newState string) bool {
	for _, log := range logs {
		if strings.Contains(log.Message, messageContains) &&
			log.EventType == storage.LogEventStateTransition &&
			log.NewState == newState {
			return true
		}
	}
	return false
}

func formatLogEntries(logs []*client.LogEntry) string {
	parts := make([]string, 0, len(logs))
	for _, log := range logs {
		parts = append(parts, fmt.Sprintf("%s:%s:%s->%s", log.EventType, log.Message, log.OldState, log.NewState))
	}
	return strings.Join(parts, " | ")
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

	applyID := testutil.ApplySchemaAndWait(t, ep, "testapp", "mysql", "staging", schemaFiles, testutil.PollDeadline)
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

	_, applyID := testutil.PlanAndApply(t, ep, "testapp", "mysql", "staging", schemaFiles, nil)

	// Wait for Running — 500k rows ensures Spirit's copy phase is always observable
	testutil.WaitForState(t, ep, applyID, state.Apply.Running, testutil.PollDeadline)

	prog, err := testutil.FetchProgress(ep, applyID)
	require.NoError(t, err)
	require.NotEmpty(t, prog.Tables, "expected tables in progress response")
	assert.Equal(t, tableName, prog.Tables[0].TableName, "progress should report the correct table")
	assert.Contains(t, prog.Tables[0].DDL, "idx_email", "progress DDL should reference the index being added")

	testutil.WaitForState(t, ep, applyID, state.Apply.Completed, testutil.PollDeadline)
}

// TestK8s_ProgressStableAcrossDataPlaneReplicas verifies that gRPC progress is
// stable when a data-plane service has multiple pods. Only one pod owns the
// in-memory Spirit runner, but every pod must be able to answer Progress from
// shared storage without downgrading row-copy progress.
func TestK8s_ProgressStableAcrossDataPlaneReplicas(t *testing.T) {
	pods := podNamesForInstance(t, "data-plane")
	require.GreaterOrEqual(t, len(pods), 2, "expected k8s e2e data plane to run at least two replicas")

	endpoints := make([]dataPlanePodEndpoint, 0, len(pods))
	for _, pod := range pods {
		endpoints = append(endpoints, dataPlanePodEndpoint{
			pod:     pod,
			address: startDataPlanePodGRPCPortForward(t, pod),
		})
	}

	fixture := startIndexAddApply(t, "k8s_dp_replicas", false)
	tablesByPod := waitForDataPlanePodsRowCopyProgress(t, endpoints, fixture.DataPlaneApplyID, fixture.TableName)

	for _, pod := range pods {
		table := tablesByPod[pod]
		require.NotNil(t, table, "data-plane pod %s should report row-copy progress", pod)
		assert.Equal(t, fixture.TableName, table.TableName, "data-plane pod %s should report the target table", pod)
		assert.Positive(t, table.RowsTotal, "data-plane pod %s should preserve row-copy totals", pod)
		assert.True(t, hasRowCopyProgress(table.RowsTotal, table.RowsCopied, table.PercentComplete), "data-plane pod %s should preserve row-copy progress", pod)
	}

	testutil.WaitForState(t, fixture.Endpoint, fixture.ApplyID, state.Apply.Completed, 3*time.Minute)
	waitForIndex(t, fixture.TargetDSN, fixture.TableName, "idx_account_created", testutil.PollDeadline)
}

type dataPlanePodEndpoint struct {
	pod     string
	address string
}

func waitForDataPlanePodsRowCopyProgress(t *testing.T, endpoints []dataPlanePodEndpoint, applyID, tableName string) map[string]*ternv1.TableProgress {
	t.Helper()

	clients := make(map[string]ternv1.TernClient, len(endpoints))
	for _, endpoint := range endpoints {
		conn, err := grpc.NewClient(endpoint.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		defer utils.CloseAndLog(conn)
		clients[endpoint.pod] = ternv1.NewTernClient(conn)
	}

	matchedTables := make(map[string]*ternv1.TableProgress, len(endpoints))
	lastResponses := make(map[string]*ternv1.ProgressResponse, len(endpoints))
	lastErrors := make(map[string]error, len(endpoints))
	testutil.Poll(t, testutil.PollDeadline, testutil.PollInterval,
		func() bool {
			for _, endpoint := range endpoints {
				if matchedTables[endpoint.pod] != nil {
					continue
				}
				ctx, cancel := context.WithTimeout(t.Context(), testutil.ProgressTimeout)
				resp, err := clients[endpoint.pod].Progress(ctx, &ternv1.ProgressRequest{
					ApplyId:     applyID,
					Database:    "testapp",
					Environment: "staging",
				})
				cancel()
				lastErrors[endpoint.pod] = err
				if err != nil {
					continue
				}
				lastResponses[endpoint.pod] = resp
				for _, table := range resp.Tables {
					if table.TableName == tableName && hasRowCopyProgress(table.RowsTotal, table.RowsCopied, table.PercentComplete) {
						matchedTables[endpoint.pod] = table
						break
					}
				}
			}
			return len(matchedTables) == len(endpoints)
		},
		func() string {
			return fmt.Sprintf("timeout waiting for all data-plane pods to preserve row-copy progress for %s: matched=%s last_responses=%s last_errors=%v",
				tableName, formatMatchedPods(matchedTables), formatPodProgressResponses(lastResponses), lastErrors)
		})
	return matchedTables
}

func formatMatchedPods(matchedTables map[string]*ternv1.TableProgress) string {
	pods := make([]string, 0, len(matchedTables))
	for pod := range matchedTables {
		pods = append(pods, pod)
	}
	return strings.Join(pods, ",")
}

func formatPodProgressResponses(responses map[string]*ternv1.ProgressResponse) string {
	parts := make([]string, 0, len(responses))
	for pod, resp := range responses {
		var tableParts []string
		for _, table := range resp.Tables {
			tableParts = append(tableParts, fmt.Sprintf("%s status=%s rows=%d/%d percent=%d",
				table.TableName, table.Status, table.RowsCopied, table.RowsTotal, table.PercentComplete))
		}
		parts = append(parts, fmt.Sprintf("%s state=%s tables=[%s]", pod, resp.State, strings.Join(tableParts, "; ")))
	}
	return strings.Join(parts, " | ")
}

func hasRowCopyProgress(rowsTotal, rowsCopied int64, percentComplete int32) bool {
	return rowsTotal > 0 && (rowsCopied > 0 || percentComplete > 0)
}

// TestK8s_Scheduler_DataPlanePodRestartRecoversIndexAdd verifies scheduler
// recovery across the two-tier Kubernetes deployment. The control plane keeps
// the user-facing apply alive while the data-plane pods are replaced mid-apply.
func TestK8s_Scheduler_DataPlanePodRestartRecoversIndexAdd(t *testing.T) {
	fixture := startRunningIndexAddApply(t, "k8s_sched_dp")

	crashedPods := crashPods(t, "data-plane")

	// The restarted data-plane scheduler claims stale local apply rows. Aging
	// the heartbeat avoids waiting for the production staleness threshold.
	markDataPlaneHeartbeatStale(t, fixture.DataPlaneApplyID)
	waitForPodsReadyAfterDeletion(t, "data-plane", crashedPods, 2*time.Minute)
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
