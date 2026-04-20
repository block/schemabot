//go:build e2e || integration

package testutil

import (
	"testing"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/require"
)

// groupFiles groups flat schema files by namespace using the shared helper.
// Flat files (e.g., "users.sql") use the database name as the namespace;
// files with subdirectory paths (e.g., "payments/users.sql") use the
// subdirectory as the namespace.
func groupFiles(t *testing.T, files map[string]string, database string) map[string]*apitypes.SchemaFiles {
	t.Helper()
	grouped, err := schema.GroupFilesByNamespace(files, database)
	require.NoError(t, err, "group schema files by namespace")
	result := make(map[string]*apitypes.SchemaFiles, len(grouped))
	for ns, nsFiles := range grouped {
		result[ns] = &apitypes.SchemaFiles{Files: nsFiles.Files}
	}
	return result
}

// ApplySchemaAndWait plans and applies schema files, polling until completion or
// failure. Returns the apply ID. If the plan detects no changes, returns empty
// string with no error. Files are grouped by namespace using the shared helper.
func ApplySchemaAndWait(t *testing.T, endpoint, database, dbType, env string, schemaFiles map[string]string, timeout time.Duration) string {
	t.Helper()

	resp, err := client.CallPlanAPIWithFiles(endpoint, database, dbType, env, groupFiles(t, schemaFiles, database), "", 0)
	require.NoError(t, err, "plan API call")

	if resp.PlanID == "" {
		return "" // no changes needed
	}

	applyResp, err := client.CallApplyAPI(endpoint, resp.PlanID, database, env, "", nil)
	require.NoError(t, err, "apply API call")
	require.True(t, applyResp.Accepted, "apply not accepted: %s", applyResp.ErrorMessage)

	WaitForState(t, endpoint, applyResp.ApplyID, state.Apply.Completed, timeout)
	return applyResp.ApplyID
}

// PlanAndApply plans and applies schema files without waiting for completion.
// Returns the plan ID and apply ID. Useful when you need to interact with the
// apply mid-flight (stop, cutover, etc.). Files are grouped by namespace using
// the shared helper.
func PlanAndApply(t *testing.T, endpoint, database, dbType, env string, schemaFiles map[string]string, opts map[string]string) (planID, applyID string) {
	t.Helper()

	resp, err := client.CallPlanAPIWithFiles(endpoint, database, dbType, env, groupFiles(t, schemaFiles, database), "", 0)
	require.NoError(t, err, "plan API call")
	if resp.PlanID == "" {
		t.Fatal("plan returned no changes")
	}

	applyResp, err := client.CallApplyAPI(endpoint, resp.PlanID, database, env, "", opts)
	require.NoError(t, err, "apply API call")
	require.True(t, applyResp.Accepted, "apply not accepted: %s", applyResp.ErrorMessage)

	if applyResp.ApplyID == "" {
		t.Fatalf("apply returned empty apply_id (plan_id: %s)", resp.PlanID)
	}

	return resp.PlanID, applyResp.ApplyID
}
