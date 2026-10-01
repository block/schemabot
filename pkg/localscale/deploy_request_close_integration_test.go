//go:build integration

package localscale_test

import (
	"log/slog"
	"os"
	"testing"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/planetscale"
	"github.com/block/schemabot/pkg/psclient"
)

// A deploy request that has not been deployed is retired by closing it: cancel
// only reaches a deploy that is queued or running, so it refuses a ready deploy
// request. Closing leaves the deployment state as it was, reports the deploy
// request closed, and makes it undeployable, and a second close is refused.
func TestCloseUndeployedDeployRequest(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	deferCleanupActiveDeployRequests(t)
	ctx := t.Context()

	branchName := createBranchWithDDL(t, ctx, "close-ready",
		map[string][]string{"testapp_sharded": {"ALTER TABLE users ADD COLUMN close_ready_col varchar(50)"}},
		nil,
	)
	dr := createDeploy(t, ctx, branchName, false)
	require.Equal(t, drState.Ready, dr.DeploymentState)
	assert.Equal(t, "open", dr.State, "a new deploy request is open")

	_, err := testClient.CancelDeployRequest(ctx, &ps.CancelDeployRequestRequest{
		Organization: testOrg, Database: testDB, Number: dr.Number,
	})
	require.Error(t, err, "cancel must refuse a deploy request that has not been deployed")

	closed, err := testClient.CloseDeployRequest(ctx, &ps.CloseDeployRequestRequest{
		Organization: testOrg, Database: testDB, Number: dr.Number,
	})
	require.NoError(t, err, "CloseDeployRequest")
	assert.Equal(t, "closed", closed.State)
	assert.Equal(t, drState.Ready, closed.DeploymentState, "closing leaves the deployment state as it was")
	assert.NotNil(t, closed.ClosedAt)

	got, err := testClient.GetDeployRequest(ctx, &ps.GetDeployRequestRequest{
		Organization: testOrg, Database: testDB, Number: dr.Number,
	})
	require.NoError(t, err, "GetDeployRequest")
	assert.Equal(t, "closed", got.State)
	assert.Equal(t, drState.Ready, got.DeploymentState)
	assert.NotNil(t, got.ClosedAt)
	assert.Nil(t, got.DeployedAt)

	_, err = testClient.DeployDeployRequest(ctx, &ps.PerformDeployRequest{
		Organization: testOrg, Database: testDB, Number: dr.Number,
	})
	require.Error(t, err, "a closed deploy request must not deploy")
	assert.Contains(t, err.Error(), "is closed")

	_, err = testClient.CloseDeployRequest(ctx, &ps.CloseDeployRequestRequest{
		Organization: testOrg, Database: testDB, Number: dr.Number,
	})
	require.Error(t, err, "a closed deploy request cannot be closed again")
	assert.Contains(t, err.Error(), "already closed")
}

// A no-change deploy request remains closed when a deploy arrives afterward.
// The no-change fast path must enforce the same close/deploy exclusion as a
// deploy request carrying DDL.
func TestClosedNoChangeDeployRequestCannotDeploy(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	deferCleanupActiveDeployRequests(t)
	ctx := t.Context()

	branchName := createBranchWithDDL(t, ctx, "close-no-change", nil, nil)
	dr := createDeploy(t, ctx, branchName, false)
	require.Equal(t, drState.NoChanges, dr.DeploymentState)

	closed, err := testClient.CloseDeployRequest(ctx, &ps.CloseDeployRequestRequest{
		Organization: testOrg, Database: testDB, Number: dr.Number,
	})
	require.NoError(t, err, "CloseDeployRequest")
	assert.Equal(t, "closed", closed.State)

	_, err = testClient.DeployDeployRequest(ctx, &ps.PerformDeployRequest{
		Organization: testOrg, Database: testDB, Number: dr.Number,
	})
	require.Error(t, err, "a closed no-change deploy request must not deploy")
	assert.Contains(t, err.Error(), "is closed")

	got, err := testClient.GetDeployRequest(ctx, &ps.GetDeployRequestRequest{
		Organization: testOrg, Database: testDB, Number: dr.Number,
	})
	require.NoError(t, err, "GetDeployRequest")
	assert.Equal(t, "closed", got.State)
	assert.Equal(t, drState.NoChanges, got.DeploymentState)
	assert.Nil(t, got.DeployedAt)
}

// A deferred deploy waiting for its start holds a ready deploy request. Cancel
// through the PlanetScale engine must close that deploy request rather than
// fail on the refused cancel, and a retried cancel must settle on the closed
// deploy request instead of failing again.
func TestPlanetScaleCancelClosesUndeployedDeployRequest(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	deferCleanupActiveDeployRequests(t)
	ctx := t.Context()

	branchName := createBranchWithDDL(t, ctx, "cancel-deferred",
		map[string][]string{"testapp_sharded": {"ALTER TABLE users ADD COLUMN cancel_deferred_col varchar(50)"}},
		nil,
	)
	dr := createDeploy(t, ctx, branchName, false)
	require.Equal(t, drState.Ready, dr.DeploymentState)

	eng := planetscale.NewWithClient(
		slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
		func(_, _ string) (psclient.PSClient, error) { return testClient, nil },
	)
	rs, err := planetscale.BuildResumeState(planetscale.ResumeData{
		BranchName:       branchName,
		DeployRequestID:  dr.Number,
		DeployRequestURL: dr.HtmlURL,
		DeferredDeploy:   true,
	})
	require.NoError(t, err, "build resume state")
	controlReq := func() *engine.ControlRequest {
		return &engine.ControlRequest{
			Database:    testDB,
			ResumeState: rs,
			Credentials: &engine.Credentials{Metadata: map[string]string{
				"organization": testOrg,
				"token_name":   "test",
				"token_value":  "test",
			}},
		}
	}

	result, err := eng.Cancel(ctx, controlReq())
	require.NoError(t, err, "cancel of an undeployed deploy request must close it")
	assert.True(t, result.Accepted)
	assert.Contains(t, result.Message, "closed before it was deployed")

	got, err := testClient.GetDeployRequest(ctx, &ps.GetDeployRequestRequest{
		Organization: testOrg, Database: testDB, Number: dr.Number,
	})
	require.NoError(t, err, "GetDeployRequest")
	assert.Equal(t, "closed", got.State)
	assert.Nil(t, got.DeployedAt)

	result, err = eng.Cancel(ctx, controlReq())
	require.NoError(t, err, "a retried cancel must settle on the closed deploy request")
	assert.True(t, result.Accepted)
	assert.Contains(t, result.Message, "already closed before it was deployed")
}
