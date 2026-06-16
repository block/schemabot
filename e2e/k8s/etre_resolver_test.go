//go:build e2e

package k8s

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/e2e/testutil"
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/state"
)

// TestK8sEtre_PlanApply_AddColumn exercises the dynamic data-plane resolution
// path end to end. The control plane routes the testapp database to the etre
// data plane carrying an opaque target (a DSID). The data plane resolves that
// DSID through Etre to a MySQL endpoint, assumes an IAM role and reads the DDL
// credential from Secrets Manager — both emulated by ministack — then applies
// the schema change against the resolved database. It is the highest-fidelity
// coverage of the resolve-then-apply path, exercising the real Etre server and
// the AWS credential flow rather than mocks.
func TestK8sEtre_PlanApply_AddColumn(t *testing.T) {
	ep, dsn := testutil.Endpoint(t), testutil.TernStagingDSN(t)
	tableName := testutil.UniqueTableName("k8s_etre_addcol")

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

	assert.True(t, testutil.ColumnExists(t, dsn, tableName, "email"),
		"expected column 'email' on %s after an etre-resolved apply", tableName)
}
