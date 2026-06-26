//go:build e2e

package k8s

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/block/schemabot/e2e/testutil"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestK8s_ProgressFromNonOwningPodDoesNotCorruptApply verifies that when one
// logical data-plane route is served by multiple instances sharing storage, a
// Progress request answered by an instance that is NOT executing the queried
// schema change cannot report or persist a terminal state while the owning
// instance is still copying rows.
//
// Each instance holds its own in-memory Spirit engine. An instance that has
// finished an unrelated schema change still holds that completed state in
// memory. If a Progress request for a different, in-flight apply is balanced
// onto it, it must fall back to shared stored state rather than trusting its own
// engine — otherwise it would report another schema change's terminal state and
// drive the in-flight apply's shared record to completed before its DDL has been
// applied, causing the owning instance to abandon the still-running copy.
//
// The control-plane gRPC client pins to a single data-plane instance, so this
// test drives plan/apply/progress against specific pods directly to place the
// owning and non-owning roles deterministically, the same way production
// service-mesh load balancing can spread Progress across instances.
func TestK8s_ProgressFromNonOwningPodDoesNotCorruptApply(t *testing.T) {
	cleanupState(t)

	pods := podNamesForInstance(t, "data-plane")
	require.GreaterOrEqual(t, len(pods), 2, "expected k8s e2e data plane to run at least two replicas")

	ownerClient := dialDataPlanePod(t, pods[0])
	nonOwnerClient := dialDataPlanePod(t, pods[1])

	targetDSN := testutil.TernStagingDSN(t)
	ternDSN := storageDSNs(t)[0]

	// Prime the non-owning instance with a completed schema change so its
	// in-memory engine holds terminal state for an unrelated schema change.
	primeTable := testutil.UniqueTableName("dp_prime")
	createIndexAddTable(t, primeTable, targetDSN, 2000)
	_, primeApplyID := planAndApplyIndexOnPod(t, nonOwnerClient, primeTable)
	waitForPodApplyState(t, nonOwnerClient, primeApplyID, ternv1.State_STATE_COMPLETED, 3*time.Minute)

	// Start a long-running schema change on the owning instance and let it reach
	// the row-copy phase so it is unambiguously in flight.
	longTable := testutil.UniqueTableName("dp_long")
	createIndexAddTable(t, longTable, targetDSN, 500000)
	_, longApplyID := planAndApplyIndexOnPod(t, ownerClient, longTable)
	waitForPodRowCopyInFlight(t, ownerClient, longApplyID, longTable)

	// While the owning instance is still copying, repeatedly ask the non-owning
	// instance — whose engine holds the primed schema change's completed state —
	// for the long apply's progress. It must never report the apply terminal,
	// never drive the shared record terminal, and never let the index appear
	// before the owner finishes the copy.
	observedInFlight := false
	for range 20 {
		ownerResp := podProgress(t, ownerClient, longApplyID)
		if ownerResp.State == ternv1.State_STATE_COMPLETED {
			break
		}
		require.Equal(t, ternv1.State_STATE_RUNNING, ownerResp.State,
			"owning instance should still be running the long apply")
		observedInFlight = true

		nonOwnerResp := podProgress(t, nonOwnerClient, longApplyID)
		assert.NotEqual(t, ternv1.State_STATE_COMPLETED, nonOwnerResp.State,
			"non-owning instance reported the in-flight apply as completed")
		assert.NotEqual(t, ternv1.State_STATE_FAILED, nonOwnerResp.State,
			"non-owning instance reported the in-flight apply as failed")

		applyState, _ := storedK8sApplyAndTaskStates(t, ternDSN, longApplyID)
		assert.False(t, state.IsTerminalApplyState(applyState),
			"non-owning instance drove the shared apply record terminal (%s) while the copy was in flight", applyState)

		// The index must not have been applied while the copy is still in flight.
		assert.False(t, indexExists(t, targetDSN, longTable, "idx_account_created"),
			"index was applied before the row copy finished")
	}
	require.True(t, observedInFlight, "expected to observe the long schema change in flight on the owning instance")

	// The owning instance finishes the apply correctly.
	waitForPodApplyState(t, ownerClient, longApplyID, ternv1.State_STATE_COMPLETED, 3*time.Minute)
	waitForIndex(t, targetDSN, longTable, "idx_account_created", testutil.PollDeadline)
}

// dialDataPlanePod port-forwards a single data-plane pod's gRPC port and returns
// a Tern client bound to that specific instance.
func dialDataPlanePod(t *testing.T, pod string) ternv1.TernClient {
	t.Helper()
	address := startDataPlanePodGRPCPortForward(t, pod)
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(conn) })
	client := ternv1.NewTernClient(conn)
	waitForPodGRPCReady(t, client)
	return client
}

// waitForPodGRPCReady waits for the pod's port-forward to accept connections.
// kubectl port-forward returns before the tunnel is established and grpc dials
// lazily, so the first RPC can hit a refused connection. A sentinel Progress
// call resolves once the transport is up — any application-level response (the
// apply is not found) means the connection works; only a transport-level
// Unavailable means the forward is not ready yet.
func waitForPodGRPCReady(t *testing.T, client ternv1.TernClient) {
	t.Helper()
	var lastErr error
	testutil.Poll(t, testutil.PollDeadline, testutil.PollInterval,
		func() bool {
			ctx, cancel := context.WithTimeout(t.Context(), testutil.ProgressTimeout)
			defer cancel()
			_, lastErr = client.Progress(ctx, &ternv1.ProgressRequest{ApplyId: "readiness-probe", Environment: "staging"})
			return status.Code(lastErr) != codes.Unavailable
		},
		func() string {
			return fmt.Sprintf("data-plane pod gRPC port-forward never became ready: %v", lastErr)
		})
}

// createIndexAddTable creates the index-add fixture table on the target and
// seeds it so the eventual index add has a measurable row-copy phase.
func createIndexAddTable(t *testing.T, tableName, targetDSN string, rowCount int) {
	t.Helper()
	testutil.CreateTestTableWithCleanup(t, targetDSN, tableName, fmt.Sprintf(
		`CREATE TABLE %s (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, account_id BIGINT NOT NULL, event_type VARCHAR(100) NOT NULL, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`, tableName),
		storageDSNs(t)...)
	testutil.SeedRows(t, targetDSN, tableName, "account_id, event_type",
		"FLOOR(1 + RAND() * 100000), ELT(FLOOR(1 + RAND() * 5), 'type_a', 'type_b', 'type_c', 'type_d', 'type_e')", rowCount)
}

// planAndApplyIndexOnPod plans and applies an index add for the given table
// directly against one data-plane instance, making that instance the owner of
// the resulting apply. Returns the data-plane plan and apply identifiers.
func planAndApplyIndexOnPod(t *testing.T, client ternv1.TernClient, tableName string) (planID, applyID string) {
	t.Helper()
	schemaFiles := map[string]*ternv1.SchemaFiles{
		"testapp": {Files: map[string]string{
			tableName + ".sql": fmt.Sprintf(
				`CREATE TABLE %s (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, account_id BIGINT NOT NULL, event_type VARCHAR(100) NOT NULL, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, KEY idx_account_created (account_id, created_at)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, tableName),
		}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	planResp, err := client.Plan(ctx, &ternv1.PlanRequest{
		Database:    "testapp",
		Type:        "mysql",
		Environment: "staging",
		SchemaFiles: schemaFiles,
	})
	require.NoError(t, err, "plan on data-plane pod")
	require.NotEmpty(t, planResp.PlanId, "plan on data-plane pod returned no changes")

	applyCtx, applyCancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer applyCancel()
	applyResp, err := client.Apply(applyCtx, &ternv1.ApplyRequest{
		PlanId:      planResp.PlanId,
		Environment: "staging",
		Caller:      "e2e",
		Options:     map[string]string{"allow_unsafe": "true"},
	})
	require.NoError(t, err, "apply on data-plane pod")
	require.True(t, applyResp.Accepted, "apply not accepted: %s", applyResp.ErrorMessage)
	require.NotEmpty(t, applyResp.ApplyId, "apply on data-plane pod returned empty apply id")
	return planResp.PlanId, applyResp.ApplyId
}

// podProgress fetches progress for an apply from a specific data-plane instance.
func podProgress(t *testing.T, client ternv1.TernClient, applyID string) *ternv1.ProgressResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testutil.ProgressTimeout)
	defer cancel()
	resp, err := client.Progress(ctx, &ternv1.ProgressRequest{ApplyId: applyID, Environment: "staging"})
	require.NoError(t, err, "progress from data-plane pod")
	return resp
}

// waitForPodApplyState polls a specific instance until the apply reaches the
// expected state.
func waitForPodApplyState(t *testing.T, client ternv1.TernClient, applyID string, expected ternv1.State, timeout time.Duration) {
	t.Helper()
	var last ternv1.State
	testutil.Poll(t, timeout, testutil.PollInterval,
		func() bool {
			last = podProgress(t, client, applyID).State
			return last == expected
		},
		func() string {
			return fmt.Sprintf("timeout waiting for apply %s to reach %s on data-plane pod, last state: %s", applyID, expected, last)
		})
}

// waitForPodRowCopyInFlight polls a specific instance until the apply is running
// with the table's row copy underway, so callers can act while it is in flight.
func waitForPodRowCopyInFlight(t *testing.T, client ternv1.TernClient, applyID, tableName string) {
	t.Helper()
	var last *ternv1.ProgressResponse
	testutil.Poll(t, 2*time.Minute, testutil.PollInterval,
		func() bool {
			last = podProgress(t, client, applyID)
			if last.State != ternv1.State_STATE_RUNNING {
				return false
			}
			for _, table := range last.Tables {
				if table.TableName == tableName && hasRowCopyProgress(table.RowsTotal, table.RowsCopied, table.PercentComplete) {
					return true
				}
			}
			return false
		},
		func() string {
			return fmt.Sprintf("timeout waiting for apply %s row copy to be in flight on %s: last=%v", applyID, tableName, last)
		})
}

// indexExists reports whether the named index is present on the table.
func indexExists(t *testing.T, dsn, tableName, indexName string) bool {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(t.Context()))

	rows, err := db.QueryContext(t.Context(), fmt.Sprintf("SHOW INDEX FROM `%s` WHERE Key_name = ?", tableName), indexName)
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	return rows.Next()
}
