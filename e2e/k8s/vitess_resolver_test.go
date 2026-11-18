//go:build e2e

package k8s

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/e2e/testutil"
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/state"
)

// The routed Vitess database and the environment every test in this file plans
// and applies against.
const (
	vitessDatabase    = "testapp-vitess"
	vitessEnvironment = "staging"
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
		tableName + ".sql": unshardedTableDDL(tableName),
	}

	planResp := vitessPlan(t, ep, "testapp", schemaFiles)

	// Assert the plan actually contains the CREATE, so a no-op plan from broken
	// resolution or keyspace wiring fails here rather than silently "succeeding".
	ddl := planDDL(planResp)
	require.Contains(t, ddl, "CREATE TABLE", "plan produced no CREATE")
	require.Contains(t, ddl, tableName, "plan does not target %s", tableName)

	vitessApplyAndWait(t, ep, planResp.PlanID)
}

// TestK8sVitess_PlanApply_VSchemaOnly covers a change that rewrites a keyspace's
// VSchema and touches no table. Such an apply carries real work for the data
// plane but produces no per-table task, so it is the shape that proves the
// control plane dispatches an operation over gRPC on the strength of the work
// itself rather than on the tasks hanging off it — a distinction the in-process
// path never has to make. It runs against a keyspace that holds no tables, so
// the desired state is the VSchema and nothing else.
func TestK8sVitess_PlanApply_VSchemaOnly(t *testing.T) {
	ep := testutil.Endpoint(t)
	sequenceName := testutil.UniqueTableName("k8s_vitess_seq")

	// An unsharded keyspace's VSchema is where a Vitess tenant declares the
	// sequences its sharded tables draw ids from. Declaring one is a VSchema
	// change on its own — no DDL accompanies it.
	schemaFiles := map[string]string{
		"vschema.json": fmt.Sprintf(`{"tables":{%q:{"type":"sequence"}}}`, sequenceName),
	}

	planResp := vitessPlan(t, ep, "testapp_seq", schemaFiles)

	change := namespaceChange(t, planResp, "testapp_seq")
	require.True(t, change.HasVSchemaChange(), "plan carries no VSchema change")
	require.Empty(t, change.TableChanges, "VSchema-only plan produced table changes")
	require.Empty(t, planResp.Shards, "VSchema-only plan produced shard work")
	assert.Contains(t, change.Metadata[apitypes.VSchemaDiffMetadataKey], sequenceName,
		"VSchema diff does not mention the declared sequence")

	applyID := vitessApplyAndWait(t, ep, planResp.PlanID)

	progress, err := testutil.FetchProgress(ep, applyID)
	require.NoError(t, err)
	assert.Empty(t, progress.Tables, "VSchema-only apply reported table work")

	assertConverged(t, ep, "testapp_seq", schemaFiles)
}

// TestK8sVitess_PlanApply_VSchemaWithDDL covers the shape a sharded Vitess
// tenant actually submits: a new table and, in the same change, the vindex entry
// that tells Vitess how to route it. Table DDL and VSchema travel on one
// operation, so the data plane has to apply the VSchema and run the table work
// for the same apply — and the VSchema has to land first, since a keyspace whose
// VSchema does not declare it sharded rejects the DDL.
func TestK8sVitess_PlanApply_VSchemaWithDDL(t *testing.T) {
	ep := testutil.Endpoint(t)
	tableName := testutil.UniqueTableName("k8s_vitess_vs")

	// The table carries no AUTO_INCREMENT: on a sharded keyspace ids come from a
	// sequence, and the point here is the vindex, not id allocation.
	schemaFiles := map[string]string{
		tableName + ".sql": fmt.Sprintf(
			`CREATE TABLE %s (
    id BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, tableName),
		"vschema.json": fmt.Sprintf(
			`{"sharded":true,"vindexes":{"hash":{"type":"hash"}},"tables":{%q:{"column_vindexes":[{"column":"id","name":"hash"}]}}}`,
			tableName),
	}

	planResp := vitessPlan(t, ep, "testapp_sharded", schemaFiles)

	change := namespaceChange(t, planResp, "testapp_sharded")
	require.True(t, change.HasVSchemaChange(), "plan carries no VSchema change")
	ddl := planDDL(planResp)
	require.Contains(t, ddl, "CREATE TABLE", "plan produced no CREATE alongside the VSchema")
	require.Contains(t, ddl, tableName, "plan does not target %s", tableName)

	applyID := vitessApplyAndWait(t, ep, planResp.PlanID)

	// The table work is asserted separately from the apply state because it is
	// the half a broken ordering drops: a VSchema applied without its DDL still
	// reports a completed apply.
	progress, err := testutil.FetchProgress(ep, applyID)
	require.NoError(t, err)
	table := tableProgress(t, progress, tableName)
	assert.Equal(t, "testapp_sharded", table.Keyspace, "table work ran against the wrong keyspace")
	assert.True(t, state.IsState(table.Status, state.Task.Completed),
		"table work ended in %q", table.Status)

	assertConverged(t, ep, "testapp_sharded", schemaFiles)
}

// unshardedTableDDL is the CREATE for a table that needs no vindex, so a plan
// against an unsharded keyspace stands alone without a VSchema.
func unshardedTableDDL(tableName string) string {
	return fmt.Sprintf(
		`CREATE TABLE %s (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, tableName)
}

// vitessPlan submits one keyspace's desired state to the control plane and
// returns the resulting plan.
func vitessPlan(t *testing.T, endpoint, namespace string, files map[string]string) *apitypes.PlanResponse {
	t.Helper()
	resp, err := client.CallPlanAPIWithFiles(endpoint, vitessDatabase, "vitess", vitessEnvironment,
		map[string]*apitypes.SchemaFiles{namespace: {Files: files}}, "", 0)
	require.NoError(t, err, "plan %s", namespace)
	require.Empty(t, resp.Errors, "plan %s reported errors", namespace)
	require.NotEmpty(t, resp.PlanID, "plan %s produced no plan id", namespace)
	return resp
}

// vitessApplyAndWait applies a plan, waits for it to reach completion, and
// returns the apply id so a caller can read back what the apply did.
func vitessApplyAndWait(t *testing.T, endpoint, planID string) string {
	t.Helper()
	resp, err := client.CallApplyAPI(endpoint, planID, vitessEnvironment, "", nil)
	require.NoError(t, err)
	require.True(t, resp.Accepted, "apply not accepted: %s", resp.ErrorMessage)
	testutil.WaitForState(t, endpoint, resp.ApplyID, state.Apply.Completed, testutil.PollDeadline)
	return resp.ApplyID
}

// assertConverged re-plans the desired state a completed apply just landed and
// requires it to be empty. This is how a tenant learns their change took: the
// same files that produced work now produce none. It holds for a VSchema as
// firmly as for DDL, because the differ compares normalized VSchemas rather than
// the raw JSON Vitess happens to store.
func assertConverged(t *testing.T, endpoint, namespace string, files map[string]string) {
	t.Helper()
	replan := vitessPlan(t, endpoint, namespace, files)
	assert.False(t, replan.HasChanges(),
		"%s still plans changes after the apply completed: %s", namespace, planSummary(replan))
}

// planSummary renders a plan's namespaces, VSchema state and DDL for a failure
// message, so a plan that should have been empty says what it still wants.
func planSummary(resp *apitypes.PlanResponse) string {
	var summary strings.Builder
	for _, change := range resp.Changes {
		if change == nil {
			continue
		}
		fmt.Fprintf(&summary, "\n%s: vschema_change=%t table_changes=%d",
			change.Namespace, change.HasVSchemaChange(), len(change.TableChanges))
		if diff := change.Metadata[apitypes.VSchemaDiffMetadataKey]; diff != "" {
			fmt.Fprintf(&summary, "\n  vschema diff: %s", diff)
		}
	}
	if ddl := planDDL(resp); ddl != "" {
		fmt.Fprintf(&summary, "\n  ddl: %s", ddl)
	}
	return summary.String()
}

// tableProgress returns the completed apply's entry for one table, failing when
// the apply ran no work for it.
func tableProgress(t *testing.T, progress *apitypes.ProgressResponse, tableName string) *apitypes.TableProgressResponse {
	t.Helper()
	for _, table := range progress.Tables {
		if table != nil && table.TableName == tableName {
			return table
		}
	}
	require.FailNowf(t, "table missing from apply", "apply ran no work for %s", tableName)
	return nil
}

// namespaceChange returns the plan's entry for one keyspace, failing when the
// keyspace planned no change at all — which is how broken resolution or keyspace
// wiring shows up, and it would otherwise read as an empty change.
func namespaceChange(t *testing.T, resp *apitypes.PlanResponse, namespace string) *apitypes.SchemaChangeResponse {
	t.Helper()
	for _, change := range resp.Changes {
		if change != nil && change.Namespace == namespace {
			return change
		}
	}
	require.FailNowf(t, "namespace missing from plan", "plan has no change for %s", namespace)
	return nil
}

// planDDL concatenates every table-change DDL a plan carries. A sharded keyspace
// records its work per shard as well as under the namespace, so both are read
// here and a caller can assert on the DDL without knowing which shape it took.
func planDDL(resp *apitypes.PlanResponse) string {
	var ddl strings.Builder
	for _, change := range resp.Changes {
		if change == nil {
			continue
		}
		for _, tc := range change.TableChanges {
			ddl.WriteString(tc.DDL + "\n")
		}
	}
	for _, shard := range resp.Shards {
		if shard == nil {
			continue
		}
		for _, tc := range shard.Changes {
			ddl.WriteString(tc.DDL + "\n")
		}
	}
	return ddl.String()
}
