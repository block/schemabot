package planetscale

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

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
	closeErr  error
	closed    []*ps.CloseDeployRequestRequest
}

func (c *cancelDeployRequestClient) CancelDeployRequest(context.Context, *ps.CancelDeployRequestRequest) (*ps.DeployRequest, error) {
	return nil, c.cancelErr
}

func (c *cancelDeployRequestClient) CloseDeployRequest(_ context.Context, req *ps.CloseDeployRequestRequest) (*ps.DeployRequest, error) {
	c.closed = append(c.closed, req)
	if c.closeErr != nil {
		return nil, c.closeErr
	}
	return &ps.DeployRequest{Number: req.Number, State: deployRequestClosed, DeploymentState: c.dr.DeploymentState}, nil
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

// A deferred deploy waiting for its start holds a deploy request that is ready
// but not deployed, and PlanetScale rejects a cancel there because cancel only
// reaches a queued or running deploy. Both cancel and stop must close that
// deploy request instead and report success, so the apply settles rather than
// retrying a rejection that can never clear. A deploy request an earlier
// attempt already closed settles without closing again, and a failed close
// stays a plain error carrying both refusals.
func TestCancelAndStop_CloseUndeployedDeployRequest(t *testing.T) {
	meta, err := encodePSMetadata(&psMetadata{
		BranchName:       "schemabot-mydb-abc",
		DeployRequestID:  42,
		DeployRequestURL: "https://example.test/deploys/42",
		DeferredDeploy:   true,
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
	notDeployedErr := errors.New("cannot cancel: deploy request is in state \"ready\"")

	operations := []struct {
		name string
		run  func(e *Engine, req *engine.ControlRequest) (*engine.ControlResult, error)
	}{
		{name: "cancel", run: func(e *Engine, req *engine.ControlRequest) (*engine.ControlResult, error) {
			return e.Cancel(t.Context(), req)
		}},
		{name: "stop", run: func(e *Engine, req *engine.ControlRequest) (*engine.ControlResult, error) {
			return e.Stop(t.Context(), req)
		}},
	}

	for _, op := range operations {
		for _, undeployedState := range []string{deployState.Pending, deployState.Ready} {
			t.Run(op.name+" closes an open deploy request in state "+undeployedState, func(t *testing.T) {
				client := &cancelDeployRequestClient{
					cancelErr: notDeployedErr,
					dr:        &ps.DeployRequest{Number: 42, State: "open", DeploymentState: undeployedState},
				}

				result, err := op.run(newEngine(client), controlReq())

				require.NoError(t, err)
				assert.True(t, result.Accepted)
				assert.Equal(t, "Deploy request #42 closed before it was deployed", result.Message)
				require.Len(t, client.closed, 1, "the undeployed deploy request must be closed exactly once")
				assert.Equal(t, "org", client.closed[0].Organization)
				assert.Equal(t, "mydb", client.closed[0].Database)
				assert.Equal(t, uint64(42), client.closed[0].Number)
			})
		}

		t.Run(op.name+" settles a deploy request an earlier attempt already closed", func(t *testing.T) {
			client := &cancelDeployRequestClient{
				cancelErr: notDeployedErr,
				dr:        &ps.DeployRequest{Number: 42, State: deployRequestClosed, DeploymentState: deployState.Ready},
			}

			result, err := op.run(newEngine(client), controlReq())

			require.NoError(t, err)
			assert.True(t, result.Accepted)
			assert.Equal(t, "Deploy request #42 already closed before it was deployed", result.Message)
			assert.Empty(t, client.closed, "an already-closed deploy request must not be closed again")
		})

		t.Run(op.name+" surfaces a failed close with both refusals", func(t *testing.T) {
			client := &cancelDeployRequestClient{
				cancelErr: notDeployedErr,
				dr:        &ps.DeployRequest{Number: 42, State: "open", DeploymentState: deployState.Ready},
				closeErr:  errors.New("deploy request is deploying"),
			}

			_, err := op.run(newEngine(client), controlReq())

			require.Error(t, err)
			assert.False(t, engine.IsAlreadyCompleted(err))
			assert.Contains(t, err.Error(), `close undeployed deploy request #42 in deployment state "ready"`)
			assert.Contains(t, err.Error(), "deploy request is deploying")
			assert.Contains(t, err.Error(), `cannot cancel: deploy request is in state "ready"`)
		})

		t.Run(op.name+" does not close a deploy request that reports a deploy", func(t *testing.T) {
			deployedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
			client := &cancelDeployRequestClient{
				cancelErr: notDeployedErr,
				dr:        &ps.DeployRequest{Number: 42, State: "open", DeploymentState: deployState.Ready, DeployedAt: &deployedAt},
			}

			_, err := op.run(newEngine(client), controlReq())

			require.Error(t, err)
			assert.Contains(t, err.Error(), `cancel deploy request #42 rejected in deployment state "ready" after it reported a deploy`)
			assert.Empty(t, client.closed, "a deploy request that reports a deploy must never be closed")
		})
	}
}

// closeRacesDeployClient reads the deploy request as ready, but a deploy lands
// before the close does, so the close response reports the deploy.
type closeRacesDeployClient struct {
	cancelDeployRequestClient
	closeResponse *ps.DeployRequest
}

func (c *closeRacesDeployClient) CloseDeployRequest(_ context.Context, req *ps.CloseDeployRequestRequest) (*ps.DeployRequest, error) {
	c.closed = append(c.closed, req)
	return c.closeResponse, nil
}

// The deploy request state the engine classifies is one round trip old by the
// time the close is sent, and a deferred deploy request stays startable from
// the PlanetScale UI in between. A deploy that starts in that window must not
// be reported as a cancel that took effect, whichever field of the close
// response carries the news — the deploy timestamp or a deployment state past
// ready — and a close that returns nothing is not taken as proof either.
func TestCancel_CloseResponseReportingADeployIsNotAccepted(t *testing.T) {
	deployedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	tests := []struct {
		name          string
		closeResponse *ps.DeployRequest
		wantInError   []string
	}{
		{
			name:          "deployment state has moved to queued",
			closeResponse: &ps.DeployRequest{Number: 42, State: deployRequestClosed, DeploymentState: deployState.Queued, DeployedAt: &deployedAt},
			wantInError:   []string{`"queued"`, "2026-01-02T03:04:05Z", "the cancel has not taken effect"},
		},
		{
			name:          "deploy timestamp set while the deployment state still reads ready",
			closeResponse: &ps.DeployRequest{Number: 42, State: deployRequestClosed, DeploymentState: deployState.Ready, DeployedAt: &deployedAt},
			wantInError:   []string{`"ready"`, "2026-01-02T03:04:05Z"},
		},
		{
			name:          "deployment state past ready with no deploy timestamp",
			closeResponse: &ps.DeployRequest{Number: 42, State: deployRequestClosed, DeploymentState: deployState.Submitting},
			wantInError:   []string{`"submitting"`, "deployed at never"},
		},
		{
			name:          "close returned no deploy request",
			closeResponse: nil,
			wantInError:   []string{"close returned no deploy request"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta, err := encodePSMetadata(&psMetadata{BranchName: "schemabot-mydb-abc", DeployRequestID: 42, DeployRequestURL: "https://example.test/deploys/42", DeferredDeploy: true})
			require.NoError(t, err)
			client := &closeRacesDeployClient{
				cancelDeployRequestClient: cancelDeployRequestClient{
					cancelErr: errors.New(`cannot cancel: deploy request is in state "ready"`),
					dr:        &ps.DeployRequest{Number: 42, State: "open", DeploymentState: deployState.Ready},
				},
				closeResponse: tt.closeResponse,
			}
			e := NewWithClient(slog.New(slog.NewTextHandler(os.Stdout, nil)),
				func(_, _ string) (psclient.PSClient, error) { return client, nil })

			result, err := e.Cancel(t.Context(), &engine.ControlRequest{
				Database:    "mydb",
				ResumeState: &engine.ResumeState{Metadata: meta},
				Credentials: &engine.Credentials{Metadata: map[string]string{"organization": "org", "token_name": "tn", "token_value": "tv"}},
			})

			require.Error(t, err, "a close whose response reports a deploy must not read as an accepted cancel")
			assert.Nil(t, result)
			assert.False(t, engine.IsAlreadyCompleted(err), "the deploy is live, not completed; the next attempt's cancel must reach it")
			assert.True(t, engine.IsRetryable(err), "a plain error lets the next attempt's cancel reach the queued deploy")
			for _, want := range tt.wantInError {
				assert.Contains(t, err.Error(), want)
			}
			assert.Contains(t, err.Error(), `cannot cancel: deploy request is in state "ready"`, "the original refusal stays in the error")
			require.Len(t, client.closed, 1)
		})
	}
}

// The predicate Progress and resume share to recognise a deploy request that
// was closed before its deploy began. Only a closed request in an undeployed
// deployment state with no deploy timestamp qualifies: an open ready request
// is a deploy waiting to start, a closed request in a later state closed by
// deploying, and a closed no-change request completed rather than cancelled.
func TestDeployRequestClosedUndeployed(t *testing.T) {
	deployedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	tests := []struct {
		name string
		dr   ps.DeployRequest
		want bool
	}{
		{name: "closed while ready", dr: ps.DeployRequest{State: deployRequestClosed, DeploymentState: deployState.Ready}, want: true},
		{name: "closed while pending", dr: ps.DeployRequest{State: deployRequestClosed, DeploymentState: deployState.Pending}, want: true},
		{name: "open and ready", dr: ps.DeployRequest{State: "open", DeploymentState: deployState.Ready}, want: false},
		{name: "closed ready but reports a deploy", dr: ps.DeployRequest{State: deployRequestClosed, DeploymentState: deployState.Ready, DeployedAt: &deployedAt}, want: false},
		{name: "closed by completing", dr: ps.DeployRequest{State: deployRequestClosed, DeploymentState: deployState.Complete, DeployedAt: &deployedAt}, want: false},
		{name: "closed with no changes", dr: ps.DeployRequest{State: deployRequestClosed, DeploymentState: deployState.NoChanges}, want: false},
		{name: "closed by cancelling a running deploy", dr: ps.DeployRequest{State: deployRequestClosed, DeploymentState: deployState.Cancelled, DeployedAt: &deployedAt}, want: false},
		{name: "closed in a state this engine has never seen", dr: ps.DeployRequest{State: deployRequestClosed, DeploymentState: "some_future_state"}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, deployRequestClosedUndeployed(&tt.dr))
		})
	}
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
