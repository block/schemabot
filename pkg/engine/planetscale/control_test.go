package planetscale

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/psclient"
)

func TestStop_UsesDeployRequestCancel(t *testing.T) {
	e := &Engine{}

	_, err := e.Stop(t.Context(), &engine.ControlRequest{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active schema change")
}

func TestStart_RejectsNonDeferredDeploy(t *testing.T) {
	e := &Engine{}

	// Non-deferred metadata — Start should return "not supported"
	meta, err := encodePSMetadata(&psMetadata{
		BranchName:       "schemabot-mydb-abc",
		DeployRequestID:  1,
		DeployRequestURL: "https://example.test/deploys/1",
		DeferredDeploy:   false,
	})
	require.NoError(t, err)

	_, err = e.Start(t.Context(), &engine.ControlRequest{
		ResumeState: &engine.ResumeState{Metadata: meta},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")
}

func TestStart_AcceptsDeferredDeploy(t *testing.T) {
	// Start with DeferredDeploy=true should attempt to deploy
	// (will fail because no PS client, but validates dispatch logic)
	e := &Engine{}

	meta, err := encodePSMetadata(&psMetadata{
		BranchName:       "schemabot-mydb-abc",
		DeployRequestID:  1,
		DeployRequestURL: "https://example.test/deploys/1",
		IsInstant:        true,
		DeferredDeploy:   true,
	})
	require.NoError(t, err)

	_, err = e.Start(t.Context(), &engine.ControlRequest{
		ResumeState: &engine.ResumeState{Metadata: meta},
	})
	// Fails because no PS client configured — but proves it didn't reject as "not supported"
	assert.Error(t, err)
	assert.NotContains(t, err.Error(), "not supported")
}

// applyDeployRequestErrorClient fails every cutover attempt with a fixed error.
type applyDeployRequestErrorClient struct {
	psclient.PSClient
	err error
}

func (c *applyDeployRequestErrorClient) ApplyDeployRequest(context.Context, *ps.ApplyDeployRequestRequest) (*ps.DeployRequest, error) {
	return nil, c.err
}

// A deploy request can report pending_cutover before its staged changes are
// visible to the apply endpoint, so a cutover attempted in that window is
// rejected with a not-staged error even though the deploy request is healthy
// and will accept the cutover moments later. The engine classifies that
// rejection as not-ready so the drive retries on the next progress tick
// instead of reporting a cutover failure; any other rejection remains a plain
// error carrying the deleted-deploy-request hint.
func TestCutover_NotStagedRejectionIsNotReady(t *testing.T) {
	meta, err := encodePSMetadata(&psMetadata{
		BranchName:       "schemabot-mydb-abc",
		DeployRequestID:  7,
		DeployRequestURL: "https://example.test/deploys/7",
	})
	require.NoError(t, err)

	controlReq := func() *engine.ControlRequest {
		return &engine.ControlRequest{
			Database:    "mydb",
			ResumeState: &engine.ResumeState{Metadata: meta},
			Credentials: &engine.Credentials{Metadata: map[string]string{
				"organization": "org",
				"token_name":   "tn",
				"token_value":  "tv",
			}},
		}
	}

	newEngine := func(applyErr error) *Engine {
		return NewWithClient(slog.New(slog.NewTextHandler(os.Stdout, nil)),
			func(_, _ string) (psclient.PSClient, error) {
				return &applyDeployRequestErrorClient{err: applyErr}, nil
			})
	}

	t.Run("not-staged rejection reads as not-ready", func(t *testing.T) {
		e := newEngine(errors.New("Unable to complete the deploy, deploy request changes have not been staged."))

		_, err := e.Cutover(t.Context(), controlReq())

		require.Error(t, err)
		assert.True(t, engine.IsNotReady(err), "a not-staged rejection clears on its own once staging completes")
		assert.Contains(t, err.Error(), "cutover deploy request #7")
		assert.Contains(t, err.Error(), "changes have not been staged")
		assert.NotContains(t, err.Error(), "may have been deleted")
	})

	t.Run("other rejection stays a plain error", func(t *testing.T) {
		e := newEngine(errors.New("deploy request not found"))

		_, err := e.Cutover(t.Context(), controlReq())

		require.Error(t, err)
		assert.False(t, engine.IsNotReady(err))
		assert.Contains(t, err.Error(), "cutover deploy request #7 (may have been deleted)")
		assert.Contains(t, err.Error(), "deploy request not found")
	})
}
