//go:build integration

package localscale_test

import (
	"testing"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/localscale"
	"github.com/block/schemabot/pkg/psclient"
)

// TestApply_RequireApproval_RejectsDeployment verifies that LocalScale rejects
// deploy requests when RequireApproval is enabled on the database.
// Tests the LocalScale feature directly via the PlanetScale API.
func TestApply_RequireApproval_RejectsDeployment(t *testing.T) {
	ctx := t.Context()

	lsc, err := localscale.RunContainer(ctx, localscale.ContainerConfig{
		Orgs: map[string]localscale.ContainerOrgConfig{
			testOrg: {Databases: map[string]localscale.ContainerDatabaseConfig{
				"approvaldb": {
					Keyspaces:       []localscale.ContainerKeyspaceConfig{{Name: "testkeyspace", Shards: 1}},
					RequireApproval: true,
				},
			}},
		},
	})
	require.NoError(t, err, "start LocalScale container")
	t.Cleanup(func() { _ = lsc.Terminate(ctx) })

	require.NoError(t, lsc.SeedVSchema(ctx, testOrg, "approvaldb", "testkeyspace", []byte(`{"sharded": false}`)))
	require.NoError(t, lsc.SeedDDL(ctx, testOrg, "approvaldb", "testkeyspace",
		"CREATE TABLE users (id bigint NOT NULL PRIMARY KEY, name varchar(255)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"))

	client, err := psclient.NewPSClientWithBaseURL("test", "test", lsc.URL())
	require.NoError(t, err)

	// Create branch and apply DDL
	_, err = client.CreateBranch(ctx, &ps.CreateDatabaseBranchRequest{
		Organization: testOrg,
		Database:     "approvaldb",
		Name:         "test-branch",
		ParentBranch: "main",
	})
	require.NoError(t, err)

	// Seed DDL on branch
	require.NoError(t, lsc.SeedDDL(ctx, testOrg, "approvaldb", "testkeyspace",
		"ALTER TABLE users ADD COLUMN email varchar(255)"))

	// Create deploy request
	dr, err := client.CreateDeployRequest(ctx, &ps.CreateDeployRequestRequest{
		Organization: testOrg,
		Database:     "approvaldb",
		Branch:       "test-branch",
		IntoBranch:   "main",
	})
	require.NoError(t, err)
	require.NotNil(t, dr)

	// Try to deploy — should fail with approval error
	_, err = client.DeployDeployRequest(ctx, &ps.PerformDeployRequest{
		Organization: testOrg,
		Database:     "approvaldb",
		Number:       dr.Number,
	})
	require.Error(t, err, "deploy should fail when approvals are required")
	assert.Contains(t, err.Error(), "approved",
		"error should mention approval requirement")
}
