//go:build integration

package localscale_test

import (
	"testing"
	"time"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestApply_RequireApproval_RejectsDeployment verifies that LocalScale rejects
// deploy requests when RequireApproval is enabled on the database.
// Uses the shared test container's "approvaldb" which has RequireApproval: true.
func TestApply_RequireApproval_RejectsDeployment(t *testing.T) {
	ctx := t.Context()

	require.NoError(t, testContainer.SeedVSchema(ctx, testOrg, "approvaldb", "testkeyspace", []byte(`{"sharded": false}`)))
	require.NoError(t, testContainer.SeedDDL(ctx, testOrg, "approvaldb", "testkeyspace",
		"CREATE TABLE IF NOT EXISTS users (id bigint NOT NULL PRIMARY KEY, name varchar(255)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"))

	// Create branch
	branchName := "approval-test-" + time.Now().Format("150405")
	_, err := testClient.CreateBranch(ctx, &ps.CreateDatabaseBranchRequest{
		Organization: testOrg,
		Database:     "approvaldb",
		Name:         branchName,
		ParentBranch: "main",
	})
	require.NoError(t, err)

	// Wait for branch to be ready.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		br, brErr := testClient.GetBranch(ctx, &ps.GetDatabaseBranchRequest{
			Organization: testOrg, Database: "approvaldb", Branch: branchName,
		})
		if brErr == nil && br.Ready {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Apply DDL on branch
	require.NoError(t, testContainer.SeedDDL(ctx, testOrg, "approvaldb", "testkeyspace",
		"ALTER TABLE users ADD COLUMN approval_col varchar(255)"))

	// Create deploy request
	dr, err := testClient.CreateDeployRequest(ctx, &ps.CreateDeployRequestRequest{
		Organization: testOrg,
		Database:     "approvaldb",
		Branch:       branchName,
		IntoBranch:   "main",
	})
	require.NoError(t, err)
	require.NotNil(t, dr)

	// Wait for deploy request to be ready (diff computed).
	deadline = time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		got, getErr := testClient.GetDeployRequest(ctx, &ps.GetDeployRequestRequest{
			Organization: testOrg, Database: "approvaldb", Number: dr.Number,
		})
		if getErr == nil && (got.DeploymentState == "ready" || got.DeploymentState == "no_changes") {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Try to deploy — should fail with approval error
	_, err = testClient.DeployDeployRequest(ctx, &ps.PerformDeployRequest{
		Organization: testOrg,
		Database:     "approvaldb",
		Number:       dr.Number,
	})
	require.Error(t, err, "deploy should fail when approvals are required")
	assert.Contains(t, err.Error(), "approved",
		"error should mention approval requirement")
}
