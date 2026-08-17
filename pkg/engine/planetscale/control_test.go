package planetscale

import (
	"context"
	"errors"
	"fmt"
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

// cancelDeployRequestClient fails every cancel attempt with a fixed error and
// serves a fixed deploy request (or read error) for the follow-up state read.
type cancelDeployRequestClient struct {
	psclient.PSClient
	cancelErr error
	dr        *ps.DeployRequest
	getErr    error
}

func (c *cancelDeployRequestClient) CancelDeployRequest(context.Context, *ps.CancelDeployRequestRequest) (*ps.DeployRequest, error) {
	return nil, c.cancelErr
}

// capturingCancelClient records the cancel request so a test can assert which
// PlanetScale database name the API call addressed.
type capturingCancelClient struct {
	psclient.PSClient
	gotCancel *ps.CancelDeployRequestRequest
}

func (c *capturingCancelClient) CancelDeployRequest(_ context.Context, req *ps.CancelDeployRequestRequest) (*ps.DeployRequest, error) {
	c.gotCancel = req
	return &ps.DeployRequest{Number: req.Number, DeploymentState: deployState.Cancelled}, nil
}

// The database a target is registered under is an arbitrary routing key; when
// the credential metadata carries the PlanetScale database name, every
// PlanetScale API call addresses that name. Without the metadata the
// registered identifier doubles as the PlanetScale name.
func TestControl_AddressesPlanetScaleDatabaseFromCredentialMetadata(t *testing.T) {
	meta, err := encodePSMetadata(&psMetadata{
		BranchName:       "schemabot-mydb-abc",
		DeployRequestID:  7,
		DeployRequestURL: "https://example.test/deploys/7",
	})
	require.NoError(t, err)

	controlReq := func(credMetadata map[string]string) *engine.ControlRequest {
		return &engine.ControlRequest{
			Database:    "mydb",
			ResumeState: &engine.ResumeState{Metadata: meta},
			Credentials: &engine.Credentials{Metadata: credMetadata},
		}
	}

	t.Run("credential metadata database addresses the API", func(t *testing.T) {
		client := &capturingCancelClient{}
		e := NewWithClient(slog.New(slog.NewTextHandler(os.Stdout, nil)),
			func(_, _ string) (psclient.PSClient, error) { return client, nil })

		result, err := e.Cancel(t.Context(), controlReq(map[string]string{
			"organization": "org",
			"token_name":   "tn",
			"token_value":  "tv",
			"database":     "mydb_main",
		}))

		require.NoError(t, err)
		assert.True(t, result.Accepted)
		require.NotNil(t, client.gotCancel)
		assert.Equal(t, "mydb_main", client.gotCancel.Database)
	})

	t.Run("without the metadata the registered identifier is the name", func(t *testing.T) {
		client := &capturingCancelClient{}
		e := NewWithClient(slog.New(slog.NewTextHandler(os.Stdout, nil)),
			func(_, _ string) (psclient.PSClient, error) { return client, nil })

		result, err := e.Cancel(t.Context(), controlReq(map[string]string{
			"organization": "org",
			"token_name":   "tn",
			"token_value":  "tv",
		}))

		require.NoError(t, err)
		assert.True(t, result.Accepted)
		require.NotNil(t, client.gotCancel)
		assert.Equal(t, "mydb", client.gotCancel.Database)
	})
}

func (c *cancelDeployRequestClient) GetDeployRequest(context.Context, *ps.GetDeployRequestRequest) (*ps.DeployRequest, error) {
	if c.getErr != nil {
		return nil, c.getErr
	}
	return c.dr, nil
}

// PlanetScale rejects a cancel of a deploy request that is already closed, and
// a prior cancel attempt may be what closed it — cancel is retried across
// drives and pods, so a rejection of an already-cancelled deploy request must
// read as success. A deploy request that closed by completing is typed as
// already-completed so the caller reconciles to the completed outcome instead
// of retrying a rejection that can never succeed. A deploy request closed any
// other way (errored, or still in its revert window) must stay a plain error
// naming its live state: reporting a successful cancel or a completed change
// there would misrepresent what happened on the target.
func TestCancel_AlreadyClosedDeployRequest(t *testing.T) {
	meta, err := encodePSMetadata(&psMetadata{
		BranchName:       "schemabot-mydb-abc",
		DeployRequestID:  121,
		DeployRequestURL: "https://example.test/deploys/121",
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

	newEngine := func(client *cancelDeployRequestClient) *Engine {
		return NewWithClient(slog.New(slog.NewTextHandler(os.Stdout, nil)),
			func(_, _ string) (psclient.PSClient, error) {
				return client, nil
			})
	}
	closedErr := errors.New("The deploy request is closed.")

	for _, cancelledState := range []string{deployState.InProgressCancel, deployState.CompleteCancel, deployState.Cancelled} {
		t.Run("cancelled deploy request in state "+cancelledState+" reads as success", func(t *testing.T) {
			e := newEngine(&cancelDeployRequestClient{
				cancelErr: closedErr,
				dr:        &ps.DeployRequest{Number: 121, DeploymentState: cancelledState},
			})

			result, err := e.Cancel(t.Context(), controlReq())

			require.NoError(t, err)
			assert.True(t, result.Accepted)
			assert.Contains(t, result.Message, "Deploy request #121 already cancelled")
		})
	}

	for _, completedState := range []string{deployState.Complete, deployState.NoChanges} {
		t.Run("deploy request closed by completing in state "+completedState+" reads as already completed", func(t *testing.T) {
			e := newEngine(&cancelDeployRequestClient{
				cancelErr: closedErr,
				dr:        &ps.DeployRequest{Number: 121, DeploymentState: completedState},
			})

			_, err := e.Cancel(t.Context(), controlReq())

			require.Error(t, err)
			assert.True(t, engine.IsAlreadyCompleted(err), "a completed deploy request must type the rejection so the caller reconciles instead of retrying")
			assert.Contains(t, err.Error(), "cancel deploy request #121 rejected: the deploy request completed before the cancel arrived")
			assert.Contains(t, err.Error(), completedState)
			assert.Contains(t, err.Error(), "The deploy request is closed.")
		})
	}

	for _, otherState := range []string{deployState.CompletePendingRevert, deployState.CompleteError} {
		t.Run("deploy request in state "+otherState+" stays an error naming its state", func(t *testing.T) {
			e := newEngine(&cancelDeployRequestClient{
				cancelErr: closedErr,
				dr:        &ps.DeployRequest{Number: 121, DeploymentState: otherState},
			})

			_, err := e.Cancel(t.Context(), controlReq())

			require.Error(t, err)
			assert.False(t, engine.IsAlreadyCompleted(err), "only a fully completed deploy request may read as already completed")
			assert.Contains(t, err.Error(), fmt.Sprintf("cancel deploy request #121 rejected in deployment state %q", otherState))
			assert.Contains(t, err.Error(), "The deploy request is closed.")
		})
	}

	t.Run("failed state read keeps both errors", func(t *testing.T) {
		e := newEngine(&cancelDeployRequestClient{
			cancelErr: closedErr,
			getErr:    errors.New("deploy request not found"),
		})

		_, err := e.Cancel(t.Context(), controlReq())

		require.Error(t, err)
		assert.Contains(t, err.Error(), "cancel deploy request #121 (may have been deleted; state read also failed: deploy request not found)")
		assert.Contains(t, err.Error(), "The deploy request is closed.")
	})

	t.Run("deploy request PlanetScale has no record of reads as permanent", func(t *testing.T) {
		notFoundErr := &ps.Error{Code: ps.ErrNotFound, Meta: map[string]string{"message": "Not Found"}}
		e := newEngine(&cancelDeployRequestClient{
			cancelErr: closedErr,
			getErr:    notFoundErr,
		})

		_, err := e.Cancel(t.Context(), controlReq())

		require.Error(t, err)
		assert.False(t, engine.IsRetryable(err), "a deploy request with no record can never accept the cancel")
		assert.Contains(t, err.Error(), "cancel deploy request #121 rejected")
		assert.Contains(t, err.Error(), "The deploy request is closed.")
		assert.Contains(t, err.Error(), "deploy request not found")
		var psErr *ps.Error
		assert.ErrorAs(t, err, &psErr, "the not-found error must stay in the unwrap chain")
	})
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
