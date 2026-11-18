package planetscale

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/enginetest"
	"github.com/block/schemabot/pkg/psclient"
)

// conformanceEngine builds a PlanetScale engine served by the given fake API
// client.
func conformanceEngine(client psclient.PSClient) *Engine {
	return NewWithClient(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
		func(_, _ string) (psclient.PSClient, error) {
			return client, nil
		})
}

// conformanceControlRequest addresses deploy request #121 with the credential
// metadata the engine needs to build its API client. No vtgate DSN is set, so
// progress reads report deploy-request state alone.
func conformanceControlRequest(t *testing.T) *engine.ControlRequest {
	t.Helper()
	meta, err := encodePSMetadata(&psMetadata{
		BranchName:       "schemabot-mydb-abc",
		DeployRequestID:  121,
		DeployRequestURL: "https://example.test/deploys/121",
	})
	require.NoError(t, err)
	return &engine.ControlRequest{
		Database:    "mydb",
		ResumeState: &engine.ResumeState{Metadata: meta},
		Credentials: conformanceCredentials(),
	}
}

func conformanceCredentials() *engine.Credentials {
	return &engine.Credentials{Metadata: map[string]string{
		"organization": "org",
		"token_name":   "tn",
		"token_value":  "tv",
	}}
}

// closedDeployRequestFixture primes an engine whose cancel attempts are
// rejected because the deploy request is closed, observed afterwards in the
// given deployment state.
func closedDeployRequestFixture(t *testing.T, deploymentState string) enginetest.ControlFixture {
	t.Helper()
	return enginetest.ControlFixture{
		Engine: conformanceEngine(&cancelDeployRequestClient{
			cancelErr: errors.New("The deploy request is closed."),
			dr:        &ps.DeployRequest{Number: 121, DeploymentState: deploymentState},
		}),
		Req: conformanceControlRequest(t),
	}
}

// missingDeployRequestFixture primes an engine whose cancel attempts are
// rejected and whose follow-up state read reports the deploy request does not
// exist.
func missingDeployRequestFixture(t *testing.T) enginetest.ControlFixture {
	t.Helper()
	return enginetest.ControlFixture{
		Engine: conformanceEngine(&cancelDeployRequestClient{
			cancelErr: errors.New("The deploy request is closed."),
			getErr:    &ps.Error{Code: ps.ErrNotFound},
		}),
		Req: conformanceControlRequest(t),
	}
}

// terminalProgressFixture primes an engine whose deploy request reports the
// given deployment state, and the engine-agnostic terminal state a progress
// read must map it to.
func terminalProgressFixture(t *testing.T, deploymentState string, want engine.State) enginetest.ProgressFixture {
	t.Helper()
	controlReq := conformanceControlRequest(t)
	return enginetest.ProgressFixture{
		Name: deploymentState,
		Engine: conformanceEngine(&cancelDeployRequestClient{
			dr: &ps.DeployRequest{Number: 121, DeploymentState: deploymentState},
		}),
		Req: &engine.ProgressRequest{
			Database:    "mydb",
			ResumeState: controlReq.ResumeState,
			Credentials: controlReq.Credentials,
		},
		Want: want,
	}
}

// The PlanetScale contract-suite run. PlanetScale's backend (the deploy
// request) is externally authoritative: control rejections are classified from
// the deploy request's live deployment state, and a deploy request the API has
// no record of can never accept a command.
func TestEngineConformance(t *testing.T) {
	enginetest.Run(t, enginetest.Harness{
		CancelAlreadyCompleted: func(t *testing.T) enginetest.ControlFixture {
			return closedDeployRequestFixture(t, deployState.Complete)
		},
		// Stop delegates to the deploy-request cancel, so a stop after the
		// deploy request completed is rejected the same way.
		StopAlreadyCompleted: func(t *testing.T) enginetest.ControlFixture {
			return closedDeployRequestFixture(t, deployState.Complete)
		},
		CancelNonexistent: missingDeployRequestFixture,
		StopNonexistent:   missingDeployRequestFixture,
		TerminalProgress: func(t *testing.T) []enginetest.ProgressFixture {
			return []enginetest.ProgressFixture{
				terminalProgressFixture(t, deployState.Complete, engine.StateCompleted),
				terminalProgressFixture(t, deployState.NoChanges, engine.StateCompleted),
				terminalProgressFixture(t, deployState.Cancelled, engine.StateCancelled),
				terminalProgressFixture(t, deployState.CompleteRevert, engine.StateReverted),
				terminalProgressFixture(t, deployState.Error, engine.StateFailed),
				terminalProgressFixture(t, deployState.Failed, engine.StateFailed),
			}
		},
		NotReady: func(t *testing.T) enginetest.NotReadyFixture {
			eng := conformanceEngine(&applyDeployRequestErrorClient{
				err: errors.New("deploy request changes have not been staged"),
			})
			req := conformanceControlRequest(t)
			return enginetest.NotReadyFixture{
				Invoke: func(ctx context.Context) error {
					_, err := eng.Cutover(ctx, req)
					return err
				},
			}
		},
	})
}
