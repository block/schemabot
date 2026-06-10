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
	"github.com/block/schemabot/pkg/storage/mysqlstore"
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

// TestK8s_ApplyLinksTasksToApplyOperation verifies that an apply run through the
// two-tier gRPC stack records its per-deployment apply_operations row and links
// every task to it on the control plane. Under the single-deployment constraint
// each apply owns exactly one operation, so the operation-scoped task lookup the
// operator claim loop relies on returns that apply's full task set. This locks
// in the data model that operation-scoped resume builds on.
func TestK8s_ApplyLinksTasksToApplyOperation(t *testing.T) {
	cleanupState(t)

	ep, dsn := testutil.Endpoint(t), testutil.TernStagingDSN(t)
	tableName := testutil.UniqueTableName("k8s_applyop")

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

	applyResp, err := client.CallApplyAPI(ep, planResp.PlanID, "staging", "", nil)
	require.NoError(t, err)
	require.True(t, applyResp.Accepted, "apply not accepted: %s", applyResp.ErrorMessage)

	testutil.WaitForState(t, ep, applyResp.ApplyID, state.Apply.Completed, testutil.PollDeadline)

	// The control plane owns the apply, its apply_operations rows, and the
	// tasks. Inspect that storage directly to prove the per-operation linkage.
	sbDB, err := sql.Open("mysql", testutil.SchemabotDSN(t))
	require.NoError(t, err)
	defer utils.CloseAndLog(sbDB)
	require.NoError(t, sbDB.PingContext(t.Context()))
	store := mysqlstore.New(sbDB)

	apply, err := store.Applies().GetByApplyIdentifier(t.Context(), applyResp.ApplyID)
	require.NoError(t, err)
	require.NotNil(t, apply, "apply %s should exist in control-plane storage", applyResp.ApplyID)

	ops, err := store.ApplyOperations().ListByApply(t.Context(), apply.ID)
	require.NoError(t, err)
	require.Len(t, ops, 1, "single-deployment apply should own exactly one apply_operations row")
	assert.Equal(t, apply.Deployment, ops[0].Deployment, "operation deployment should match the apply")

	tasksByApply, err := store.Tasks().GetByApplyID(t.Context(), apply.ID)
	require.NoError(t, err)
	require.NotEmpty(t, tasksByApply, "apply should have at least one task")
	for _, task := range tasksByApply {
		require.NotNil(t, task.ApplyOperationID, "task %s must be linked to an apply_operation", task.TaskIdentifier)
		assert.Equal(t, ops[0].ID, *task.ApplyOperationID, "task %s should link to the apply's operation", task.TaskIdentifier)
	}

	// The operation-scoped lookup must return exactly the apply's tasks — the
	// invariant operation-scoped resume depends on.
	tasksByOp, err := store.Tasks().GetByApplyOperationID(t.Context(), ops[0].ID)
	require.NoError(t, err)
	assert.ElementsMatch(t, taskIdentifiers(tasksByApply), taskIdentifiers(tasksByOp),
		"operation-scoped lookup should return the same tasks as the whole apply")
}

func taskIdentifiers(tasks []*storage.Task) []string {
	ids := make([]string, 0, len(tasks))
	for _, task := range tasks {
		ids = append(ids, task.TaskIdentifier)
	}
	return ids
}

// TestK8s_RemoteFailureErrorVisibleInControlPlaneStatus verifies that a remote
// terminal failure observed over gRPC is copied into the control plane's status
// and logs. Operators should not need data-plane access to see why the apply
// failed.
func TestK8s_RemoteFailureErrorVisibleInControlPlaneStatus(t *testing.T) {
	cleanupState(t)

	endpoint := testutil.Endpoint(t)
	failureMessage := "remote schema change failed while checking target connectivity"
	tableName := testutil.UniqueTableName("k8s_remote_failure")

	dataPlaneApplyID := createStoredK8sApplyWithTask(t, storageDSNs(t)[0], &storage.Apply{
		ApplyIdentifier: testutil.UniqueTableName("apply-remote-failed"),
		Database:        "testapp",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "testapp",
		Environment:     "staging",
		Caller:          "e2e",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Failed,
		ErrorMessage:    failureMessage,
	}, &storage.Task{
		TaskIdentifier: testutil.UniqueTableName("task-remote-failed"),
		Database:       "testapp",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Namespace:      "testapp",
		TableName:      tableName,
		DDL:            fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `status_flag` int NULL", tableName),
		DDLAction:      "alter",
		Engine:         storage.EngineSpirit,
		Environment:    "staging",
		State:          state.Task.Failed,
		ErrorMessage:   failureMessage,
	})

	controlPlaneApplyID := createStoredK8sApplyWithTask(t, testutil.SchemabotDSN(t), &storage.Apply{
		ApplyIdentifier: testutil.UniqueTableName("apply-control-failed"),
		Database:        "testapp",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "data-plane",
		Environment:     "staging",
		Caller:          "e2e",
		ExternalID:      dataPlaneApplyID,
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Running,
	}, &storage.Task{
		TaskIdentifier: testutil.UniqueTableName("task-control-failed"),
		Database:       "testapp",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Namespace:      "testapp",
		TableName:      tableName,
		DDL:            fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `status_flag` int NULL", tableName),
		DDLAction:      "alter",
		Engine:         storage.EngineSpirit,
		Environment:    "staging",
		State:          state.Task.Running,
	})
	markControlPlaneHeartbeatStale(t, controlPlaneApplyID)

	var (
		progress *apitypes.ProgressResponse
		lastErr  error
	)
	testutil.Poll(t, testutil.PollDeadline, testutil.PollInterval,
		func() bool {
			progress, lastErr = testutil.FetchProgress(endpoint, controlPlaneApplyID)
			return lastErr == nil &&
				state.IsState(progress.State, state.Apply.Failed) &&
				progress.ErrorMessage == failureMessage
		},
		func() string {
			if progress == nil {
				return fmt.Sprintf("timeout waiting for remote failure to appear in control-plane status for %s: last_err=%v", controlPlaneApplyID, lastErr)
			}
			return fmt.Sprintf("timeout waiting for remote failure to appear in control-plane status for %s: state=%s error=%q last_err=%v",
				controlPlaneApplyID, progress.State, progress.ErrorMessage, lastErr)
		})

	require.Len(t, progress.Tables, 1)
	assert.Equal(t, tableName, progress.Tables[0].TableName)
	assert.Equal(t, state.Task.Failed, progress.Tables[0].Status)
	waitForControlPlaneFailureLog(t, endpoint, controlPlaneApplyID, failureMessage)
}

func createStoredK8sApplyWithTask(t *testing.T, dsn string, apply *storage.Apply, task *storage.Task) string {
	t.Helper()

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(t.Context()))

	now := time.Now()
	task.CreatedAt = now
	task.UpdatedAt = now
	if task.StartedAt == nil && !state.IsState(task.State, state.Task.Pending) {
		task.StartedAt = &now
	}
	if task.CompletedAt == nil && state.IsTerminalTaskState(task.State) {
		task.CompletedAt = &now
	}

	store := mysqlstore.New(db)
	_, err = store.Applies().CreateWithTasks(t.Context(), apply, []*storage.Task{task})
	require.NoError(t, err)
	return apply.ApplyIdentifier
}

func waitForControlPlaneFailureLog(t *testing.T, endpoint, applyID, failureMessage string) {
	t.Helper()

	var logs []*client.LogEntry
	var lastErr error
	testutil.Poll(t, testutil.PollDeadline, testutil.PollInterval,
		func() bool {
			logs, lastErr = client.GetLogs(endpoint, "", "", applyID, 50)
			return lastErr == nil && hasLogTransitionTo(logs, "Remote apply reached terminal state: failed: "+failureMessage, state.Apply.Failed)
		},
		func() string {
			return fmt.Sprintf("control-plane apply logs did not contain remote failure for %s: api_logs=%s last_err=%v",
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

// TestK8s_StopStartThroughOperator verifies that a stopped gRPC apply is
// resumed through durable control-plane operator state. The API accepts the
// start request before the data plane performs the resume, then the operator
// completes the handoff and the schema change finishes on the target database.
func TestK8s_StopStartThroughOperator(t *testing.T) {
	fixture := startRunningIndexAddApply(t, "k8s_stopstart")

	stopResp, err := client.CallStopAPI(fixture.Endpoint, "staging", fixture.ApplyID)
	require.NoError(t, err, "stop API call")
	require.True(t, stopResp.Accepted, "stop not accepted: %s", stopResp.ErrorMessage)

	testutil.WaitForState(t, fixture.Endpoint, fixture.ApplyID, state.Apply.Stopped, testutil.PollDeadline)

	startResp, err := client.CallStartAPI(fixture.Endpoint, "staging", fixture.ApplyID)
	require.NoError(t, err, "start API call")
	require.True(t, startResp.Accepted, "start not accepted: %s", startResp.ErrorMessage)
	assert.Equal(t, "queued", startResp.Status)

	waitForControlPlaneStartControlRequestCompleted(t, fixture.ApplyID)
	testutil.WaitForState(t, fixture.Endpoint, fixture.ApplyID, state.Apply.Completed, 3*time.Minute)
	waitForIndex(t, fixture.TargetDSN, fixture.TableName, "idx_account_created", testutil.PollDeadline)
}

// TestK8s_DeferCutoverThroughOperator verifies that a deferred gRPC apply is
// cut over through durable control-plane operator state. The API accepts the
// cutover request before the data plane performs cutover, then the operator
// completes the request and the target database reaches the planned schema.
func TestK8s_DeferCutoverThroughOperator(t *testing.T) {
	fixture := startIndexAddApplyWithOptions(t, "k8s_cutover", false, map[string]string{"defer_cutover": "true"}, 10000)

	testutil.WaitForState(t, fixture.Endpoint, fixture.ApplyID, state.Apply.WaitingForCutover, 3*time.Minute)

	cutoverResp, err := client.CallCutoverAPI(fixture.Endpoint, "staging", fixture.ApplyID)
	require.NoError(t, err, "cutover API call")
	require.True(t, cutoverResp.Accepted, "cutover not accepted: %s", cutoverResp.ErrorMessage)

	waitForControlPlaneControlRequestCompleted(t, fixture.ApplyID, storage.ControlOperationCutover)
	testutil.WaitForState(t, fixture.Endpoint, fixture.ApplyID, state.Apply.Completed, 3*time.Minute)
	waitForIndex(t, fixture.TargetDSN, fixture.TableName, "idx_account_created", testutil.PollDeadline)
}

// TestK8s_ManualDataPlaneCutoverReconcilesControlPlane verifies that an
// operator-side cutover performed directly against the data plane is reconciled
// by the control plane through remote progress polling.
func TestK8s_ManualDataPlaneCutoverReconcilesControlPlane(t *testing.T) {
	fixture := startIndexAddApplyWithOptions(t, "k8s_manual_cutover", false, map[string]string{"defer_cutover": "true"}, 10000)

	testutil.WaitForState(t, fixture.Endpoint, fixture.ApplyID, state.Apply.WaitingForCutover, 3*time.Minute)

	conn, err := grpc.NewClient(startDataPlaneServiceGRPCPortForward(t), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer utils.CloseAndLog(conn)

	dataPlaneClient := ternv1.NewTernClient(conn)
	waitForDataPlaneProgress(t, dataPlaneClient, fixture.DataPlaneApplyID)

	ctx, cancel := context.WithTimeout(t.Context(), testutil.ProgressTimeout)
	defer cancel()
	cutoverResp, err := dataPlaneClient.Cutover(ctx, &ternv1.CutoverRequest{
		ApplyId:     fixture.DataPlaneApplyID,
		Environment: "staging",
	})
	require.NoError(t, err, "manual data-plane cutover")
	require.True(t, cutoverResp.Accepted, "manual data-plane cutover not accepted: %s", cutoverResp.ErrorMessage)

	testutil.WaitForState(t, fixture.Endpoint, fixture.ApplyID, state.Apply.Completed, 3*time.Minute)
	waitForIndex(t, fixture.TargetDSN, fixture.TableName, "idx_account_created", testutil.PollDeadline)
}

func waitForDataPlaneProgress(t *testing.T, client ternv1.TernClient, applyID string) {
	t.Helper()

	var lastErr error
	testutil.Poll(t, testutil.PollDeadline, testutil.PollInterval,
		func() bool {
			ctx, cancel := context.WithTimeout(t.Context(), testutil.ProgressTimeout)
			_, lastErr = client.Progress(ctx, &ternv1.ProgressRequest{
				ApplyId:     applyID,
				Environment: "staging",
			})
			cancel()
			return lastErr == nil
		},
		func() string {
			return fmt.Sprintf("timeout waiting for data-plane progress for %s: last_err=%v", applyID, lastErr)
		})
}

func waitForControlPlaneStartControlRequestCompleted(t *testing.T, applyID string) {
	t.Helper()
	waitForControlPlaneControlRequestCompleted(t, applyID, storage.ControlOperationStart)
}

func waitForControlPlaneControlRequestCompleted(t *testing.T, applyID string, operation storage.ControlOperation) {
	t.Helper()

	db, err := sql.Open("mysql", testutil.SchemabotDSN(t))
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(t.Context()))

	var (
		status       string
		errorMessage sql.NullString
	)
	var lastErr error
	testutil.Poll(t, testutil.PollDeadline, testutil.PollInterval,
		func() bool {
			lastErr = db.QueryRowContext(t.Context(), `
				SELECT cr.status, cr.error_message
				FROM apply_control_requests cr
				JOIN applies a ON a.id = cr.apply_id
				WHERE a.apply_identifier = ? AND cr.operation = ?
			`, applyID, operation).Scan(&status, &errorMessage)
			return lastErr == nil && status == string(storage.ControlRequestCompleted)
		},
		func() string {
			return fmt.Sprintf("%s control request was not completed for %s: status=%q error_message=%q last_err=%v",
				operation, applyID, status, errorMessage.String, lastErr)
		})
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

// TestK8s_Operator_DataPlanePodRestartRecoversIndexAdd verifies operator
// recovery across the two-tier Kubernetes deployment. The control plane keeps
// the user-facing apply alive while the data-plane pods are replaced mid-apply.
func TestK8s_Operator_DataPlanePodRestartRecoversIndexAdd(t *testing.T) {
	fixture := startRunningIndexAddApply(t, "k8s_sched_dp")

	crashedPods := crashPods(t, "data-plane")

	// The restarted data-plane operator claims stale local apply rows. Aging
	// the heartbeat avoids waiting for the production staleness threshold.
	markDataPlaneHeartbeatStale(t, fixture.DataPlaneApplyID)
	waitForPodsReadyAfterDeletion(t, "data-plane", crashedPods, 2*time.Minute)
	waitForTernHealth(t, fixture.Endpoint, "data-plane", "staging", testutil.PollDeadline)

	testutil.WaitForState(t, fixture.Endpoint, fixture.ApplyID, state.Apply.Completed, 3*time.Minute)
	waitForIndex(t, fixture.TargetDSN, fixture.TableName, "idx_account_created", testutil.PollDeadline)
}

// TestK8s_Operator_ControlPlanePodRestartReconnectsToRunningDataPlane verifies
// that control-plane recovery restarts only the gRPC progress poller. The data
// plane keeps running the schema change while the control-plane pod is replaced.
func TestK8s_Operator_ControlPlanePodRestartReconnectsToRunningDataPlane(t *testing.T) {
	fixture := startRunningIndexAddApply(t, "k8s_sched_cp")

	crashedPod := crashPod(t, "control-plane")

	// The restarted control-plane operator claims the stale SchemaBot apply
	// row, then GRPCClient resumes progress polling using the data-plane apply ID.
	markControlPlaneHeartbeatStale(t, fixture.ApplyID)
	waitForReplacementPodReady(t, "control-plane", crashedPod, 2*time.Minute)

	recoveredEndpoint := startControlPlanePortForward(t)
	waitForTernHealth(t, recoveredEndpoint, "data-plane", "staging", testutil.PollDeadline)

	testutil.WaitForState(t, recoveredEndpoint, fixture.ApplyID, state.Apply.Completed, 3*time.Minute)
	waitForIndex(t, fixture.TargetDSN, fixture.TableName, "idx_account_created", testutil.PollDeadline)
}
