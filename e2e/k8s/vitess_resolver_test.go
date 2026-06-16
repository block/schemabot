//go:build e2e

package k8s

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/e2e/testutil"
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/state"
)

// TestK8sVitess_PlanApply_CreateTable exercises the dynamic data-plane
// resolution path for Vitess end to end. The control plane routes the
// testapp-vitess database to the vitess data plane carrying an opaque target (a
// DSID). The data plane resolves that DSID through a real Etre server to a
// PlanetScale organization, then drives the schema change through the
// PlanetScale API — served by LocalScale running real Vitess (vtcombo)
// in-process — and the apply runs to completion. It is the Vitess counterpart
// to the etre MySQL resolve-then-apply coverage, against real services.
func TestK8sVitess_PlanApply_CreateTable(t *testing.T) {
	ep := testutil.Endpoint(t)
	tableName := testutil.UniqueTableName("k8s_vitess_create")

	// Vitess schema files are keyed by keyspace (the namespace), so the new table
	// lands in the testapp keyspace LocalScale is configured with. The keyspace
	// starts empty, so a single CREATE TABLE plans cleanly (no base schema, no
	// DROPs).
	schemaFiles := map[string]string{
		tableName + ".sql": fmt.Sprintf(
			`CREATE TABLE %s (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, tableName),
	}

	planResp, err := client.CallPlanAPIWithFiles(ep, "testapp-vitess", "vitess", "staging",
		map[string]*apitypes.SchemaFiles{"testapp": {Files: schemaFiles}}, "", 0)
	require.NoError(t, err)
	require.NotEmpty(t, planResp.PlanID)

	// Assert the plan actually contains the CREATE, so a no-op plan from broken
	// resolution or keyspace wiring fails here rather than silently "succeeding".
	var ddl strings.Builder
	for _, change := range planResp.Changes {
		for _, tc := range change.TableChanges {
			ddl.WriteString(tc.DDL + "\n")
		}
	}
	require.Contains(t, ddl.String(), "CREATE TABLE", "plan produced no CREATE")
	require.Contains(t, ddl.String(), tableName, "plan does not target %s", tableName)

	applyResp, err := client.CallApplyAPI(ep, planResp.PlanID, "staging", "", nil)
	require.NoError(t, err)
	require.True(t, applyResp.Accepted, "apply not accepted: %s", applyResp.ErrorMessage)

	testutil.WaitForState(t, ep, applyResp.ApplyID, state.Apply.Completed, testutil.PollDeadline)
}
