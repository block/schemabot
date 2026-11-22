package planetscale

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/psclient"
)

// deployErrorFromAPI returns the error the PlanetScale SDK builds from a
// deploy call the API answered with status and body. Its message cannot be set
// on a hand-built *ps.Error, so the test goes through the real SDK parser.
func deployErrorFromAPI(t *testing.T, status int, body string) error {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)

	client, err := psclient.NewPSClientWithBaseURL("token-name", "token-value", srv.URL)
	require.NoError(t, err)
	_, err = client.DeployDeployRequest(t.Context(), &ps.PerformDeployRequest{Organization: "org", Database: "testdb", Number: 9})
	require.Error(t, err)
	return err
}

// approvalRequiredFromAPI is the SDK's error for a deploy PlanetScale refused
// because the deploy request needs approval first.
func approvalRequiredFromAPI(t *testing.T) error {
	t.Helper()
	return deployErrorFromAPI(t, http.StatusForbidden, `{"code":"unauthorized","message":"Deploy request must be approved before deploying."}`)
}

// A database that requires deploy request approval refuses the deploy of an
// unapproved request, and repeating the call cannot change the answer. The
// deploy call fails once with a permanent error that tells the operator to
// turn the approval requirement off and then re-plan, and the
// deploy request is never re-read for an outcome because the deploy did not
// start.
func TestDeployDeployRequest_ApprovalRequiredIsPermanent(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	refusal := approvalRequiredFromAPI(t)
	client := &deployOutcomeUnknownClient{deployErr: refusal, reread: &ps.DeployRequest{Number: 9, DeploymentState: deployState.Ready}}

	dr, err := e.deployDeployRequest(t.Context(), client, "org", "testdb", 9, false, nil)

	require.Error(t, err)
	assert.Nil(t, dr)
	var permanent *engine.PermanentError
	require.ErrorAs(t, err, &permanent)
	assert.ErrorIs(t, err, refusal)
	assert.False(t, engine.IsRetryable(err))
	assert.False(t, engine.IsOutcomeUnknown(err))
	assert.Contains(t, err.Error(), "deploy request #9 could not be deployed")
	assert.Contains(t, err.Error(), "PlanetScale deploy request approvals are not supported")
	assert.Contains(t, err.Error(), "Turn off 'Require administrator approval for deploy requests'")
	assert.Contains(t, err.Error(), "then re-plan and apply")
	assert.Equal(t, 1, client.deployCalls)
	assert.Equal(t, 0, client.getCalls)
}

// Starting a deferred deploy shares the deploy call with a fresh apply, so an
// approval refusal reaches the caller of Start as the same permanent error.
func TestStart_ApprovalRequiredIsPermanent(t *testing.T) {
	client := &deployOutcomeUnknownClient{deployErr: approvalRequiredFromAPI(t)}
	e := conformanceEngine(client)
	meta, err := encodePSMetadata(&psMetadata{
		BranchName:       "schemabot-mydb-abc",
		DeployRequestID:  121,
		DeployRequestURL: "https://example.test/deploys/121",
		DeferredDeploy:   true,
	})
	require.NoError(t, err)

	result, err := e.Start(t.Context(), &engine.ControlRequest{
		Database:    "mydb",
		ResumeState: &engine.ResumeState{Metadata: meta},
		Credentials: conformanceCredentials(),
	})

	require.Error(t, err)
	assert.Nil(t, result)
	var permanent *engine.PermanentError
	require.ErrorAs(t, err, &permanent)
	assert.False(t, engine.IsOutcomeUnknown(err))
	assert.Contains(t, err.Error(), "deploy request #121 could not be deployed: PlanetScale deploy request approvals are not supported")
	assert.Equal(t, 1, client.deployCalls)
}

// A fresh apply's deploy failure passes through unknownDeployOutcomeNeedsRecord
// unchanged when it is an approval refusal: the refusal proves the deploy did
// not start, so there is no unknown outcome for the resume state to need.
func TestUnknownDeployOutcomeNeedsRecord_ApprovalRefusalPassesThrough(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &deployOutcomeUnknownClient{deployErr: approvalRequiredFromAPI(t)}

	_, err := e.deployDeployRequest(t.Context(), client, "org", "testdb", 9, false, nil)
	require.Error(t, err)

	got := e.unknownDeployOutcomeNeedsRecord(err, false, "testdb", 9)

	assert.Same(t, err, got)
	var permanent *engine.PermanentError
	require.ErrorAs(t, got, &permanent)
	assert.NotContains(t, got.Error(), "resume state does not name it")
}

// A deploy call that fails ambiguously still goes through
// adoptDeployAfterError and ends as an unknown outcome, never as a permanent
// failure: the deploy may be running.
func TestDeployDeployRequest_AmbiguousFailureIsStillUnknownOutcome(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &deployOutcomeUnknownClient{
		deployErr: io.ErrUnexpectedEOF,
		reread:    &ps.DeployRequest{Number: 9, DeploymentState: deployState.Ready},
	}

	dr, err := e.deployDeployRequest(t.Context(), client, "org", "testdb", 9, false, nil)

	require.Error(t, err)
	assert.Nil(t, dr)
	assert.True(t, engine.IsOutcomeUnknown(err))
	assert.True(t, engine.IsRetryable(err))
	var permanent *engine.PermanentError
	assert.False(t, errors.As(err, &permanent))
	assert.Equal(t, 1, client.getCalls)
}

// A deploy refused for any other reason keeps its type: it is neither
// permanent nor an unknown outcome, so the drive handles it as it always did.
// An error from the API that only happens to mention approval in a transport
// failure is not an approval refusal either.
func TestDeployDeployRequest_UnrelatedRefusalKeepsItsType(t *testing.T) {
	tests := []struct {
		name      string
		deployErr error
	}{
		{name: "an invalid request from the API", deployErr: deployErrorFromAPI(t, http.StatusBadRequest, `{"code":"bad_request","message":"Deploy request is not in a deployable state."}`)},
		{name: "a permission refusal that is not about approval", deployErr: deployErrorFromAPI(t, http.StatusForbidden, `{"code":"unauthorized","message":"You are not authorized to perform this action"}`)},
		{name: "a transport failure whose text mentions approval", deployErr: errors.New("read tcp: connection reset while the deploy request was approved")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
			client := &deployOutcomeUnknownClient{
				deployErr: tt.deployErr,
				reread:    &ps.DeployRequest{Number: 9, DeploymentState: deployState.Failed},
			}

			dr, err := e.deployDeployRequest(t.Context(), client, "org", "testdb", 9, false, nil)

			require.Error(t, err)
			assert.Nil(t, dr)
			assert.ErrorIs(t, err, tt.deployErr)
			assert.True(t, engine.IsRetryable(err))
			assert.False(t, engine.IsOutcomeUnknown(err))
			var permanent *engine.PermanentError
			assert.False(t, errors.As(err, &permanent))
		})
	}
}
