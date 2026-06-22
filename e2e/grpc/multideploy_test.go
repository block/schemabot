//go:build e2e

package grpc

import (
	"net/http"
	"os"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Multi-deployment fan-out fixture.
//
// Scenario: a single (database, environment) — testapp/production — fans out to
// two deployments (eu, us), each backed by its own remote Tern over gRPC, with
// deployment_order [eu, us] and cutover_policy: barrier (see
// deploy/local/docker-compose.grpc-multideploy.yml and
// deploy/local/config/grpc-schemabot-multideploy.yaml).
//
// This file only verifies that the multi-deployment stack boots and that
// SchemaBot resolves both remote deployments — the foundation the ordered
// cutover acceptance test builds on.
//
// The fixture cannot boot until the server supports deployments maps with more
// than one entry, so these tests are gated behind E2E_MULTIDEPLOY=1 and are
// skipped in the standard gRPC e2e run. The make target
// test-e2e-grpc-multideploy sets the flag and stands up the multi-deployment
// stack.

func requireMultiDeploy(t *testing.T) {
	t.Helper()
	if os.Getenv("E2E_MULTIDEPLOY") != "1" {
		t.Skip("multi-deployment fixture gated behind E2E_MULTIDEPLOY=1 (requires server support for deployments maps with >1 entry)")
	}
}

func TestGRPCMultiDeploy_SchemaBot_Health(t *testing.T) {
	requireMultiDeploy(t)
	resp := grpcGet(t, "/health") //nolint:bodyclose // closed via utils.CloseAndLog
	defer utils.CloseAndLog(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result map[string]string
	grpcDecodeJSON(t, resp, &result)
	assert.Equal(t, "ok", result["status"])
}

func TestGRPCMultiDeploy_TernHealth_BothDeployments(t *testing.T) {
	requireMultiDeploy(t)
	for _, deployment := range []string{"eu", "us"} {
		resp := grpcGet(t, "/tern-health/"+deployment+"/production") //nolint:bodyclose // closed via utils.CloseAndLog
		func() {
			defer utils.CloseAndLog(resp.Body)
			require.Equalf(t, http.StatusOK, resp.StatusCode, "tern-health for deployment %q", deployment)
			var result map[string]string
			grpcDecodeJSON(t, resp, &result)
			assert.Equalf(t, "ok", result["status"], "tern-health status for deployment %q", deployment)
		}()
	}
}

// TestGRPCMultiDeploy_Plan_FansOut verifies a plan against the fanned-out
// (testapp, production) environment succeeds, proving SchemaBot loaded the
// >1-deployment config and resolved both remote deployments.
func TestGRPCMultiDeploy_Plan_FansOut(t *testing.T) {
	requireMultiDeploy(t)
	tableName := uniqueGRPCTableName("md_fanout")
	plan := grpcPlan(t, "testapp", "production", map[string]string{
		tableName + ".sql": "CREATE TABLE " + tableName + " (\n" +
			"  id BIGINT UNSIGNED AUTO_INCREMENT,\n" +
			"  PRIMARY KEY (id)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	})
	require.Empty(t, plan.Errors, "plan errors")
	require.NotEmpty(t, plan.PlanID, "plan_id")
}
