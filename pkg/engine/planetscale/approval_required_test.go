package planetscale

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

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
	assert.NotContains(t, err.Error(), "Deploy request must be approved before deploying.", "the API's answer stays on the chain but out of the message")
	assert.Equal(t, 1, client.deployCalls)
	assert.Equal(t, 0, client.getCalls)
}

// An approval refusal that answers a retry proves nothing about the earlier
// attempt, which may have started the deploy before its response was lost.
// The refusal is not permanent: the deploy request is re-read, and a started
// deploy is adopted while one that still reads ready is reported as an
// unknown outcome for recovery to settle.
func TestDeployDeployRequest_ApprovalRefusalAnsweringARetryIsNotPermanent(t *testing.T) {
	shortenKeyspaceRetries(t, time.Millisecond, time.Second)

	t.Run("re-read shows the deploy queued", func(t *testing.T) {
		e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
		queued := &ps.DeployRequest{Number: 9, DeploymentState: deployState.Queued}
		client := &deployOutcomeUnknownClient{
			earlierDeployErrs: []error{&ps.Error{Code: ps.ErrInternal}},
			deployErr:         approvalRequiredFromAPI(t),
			reread:            queued,
		}

		dr, err := e.deployDeployRequest(t.Context(), client, "org", "testdb", 9, false, nil)

		require.NoError(t, err)
		assert.Same(t, queued, dr)
		assert.Equal(t, 2, client.deployCalls)
		assert.Equal(t, 1, client.getCalls)
	})

	t.Run("re-read still reads ready", func(t *testing.T) {
		e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
		refusal := approvalRequiredFromAPI(t)
		client := &deployOutcomeUnknownClient{
			earlierDeployErrs: []error{&ps.Error{Code: ps.ErrInternal}},
			deployErr:         refusal,
			reread:            &ps.DeployRequest{Number: 9, DeploymentState: deployState.Ready},
		}

		dr, err := e.deployDeployRequest(t.Context(), client, "org", "testdb", 9, false, nil)

		require.Error(t, err)
		assert.Nil(t, dr)
		assert.ErrorIs(t, err, refusal)
		var permanent *engine.PermanentError
		assert.False(t, errors.As(err, &permanent))
		assert.True(t, engine.IsOutcomeUnknown(err))
		assert.True(t, engine.IsRetryable(err))
		assert.Equal(t, 2, client.deployCalls)
		assert.Equal(t, 1, client.getCalls)
	})
}

// Only a definitive refusal proves the deploy did not start. An answer the
// SDK types as one to retry, or does not type at all, is not an approval
// refusal even when its text says the deploy request must be approved, so it
// keeps the handling of any unrefused deploy failure.
func TestDeployDeployRequest_ApprovalTextOnAnUndefinitiveAnswerIsNotPermanent(t *testing.T) {
	shortenKeyspaceRetries(t, time.Millisecond, time.Second)
	tests := []struct {
		name      string
		body      string
		wantCalls int
	}{
		{name: "an answer to retry", body: `{"code":"unprocessable","message":"Deploy request must be approved before deploying."}`, wantCalls: maxRetries},
		{name: "an answer the SDK does not type", body: `{"code":"internal","message":"Deploy request must be approved before deploying."}`, wantCalls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
			deployErr := deployErrorFromAPI(t, http.StatusUnprocessableEntity, tt.body)
			client := &deployOutcomeUnknownClient{
				deployErr: deployErr,
				reread:    &ps.DeployRequest{Number: 9, DeploymentState: deployState.Ready},
			}

			dr, err := e.deployDeployRequest(t.Context(), client, "org", "testdb", 9, false, nil)

			require.Error(t, err)
			assert.Nil(t, dr)
			assert.ErrorIs(t, err, deployErr)
			var permanent *engine.PermanentError
			assert.False(t, errors.As(err, &permanent))
			assert.True(t, engine.IsOutcomeUnknown(err))
			assert.Equal(t, tt.wantCalls, client.deployCalls)
			assert.Equal(t, 1, client.getCalls)
		})
	}
}

// Resuming an apply whose deploy request is ready but never deployed shares
// the deploy call with a fresh apply, so an approval refusal there fails the
// apply permanently too, rather than as a failed reattach that would be
// retried.
func TestResumeExistingDeployRequest_ApprovalRequiredIsPermanent(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &resumeDeployClient{
		recovered: &ps.DeployRequest{DeploymentState: deployState.Ready, HtmlURL: "https://app/dr/42"},
		deployErr: approvalRequiredFromAPI(t),
	}
	meta := &psMetadata{BranchName: "schemabot-testdb-abc", DeployRequestID: 42}

	result, err := e.resumeExistingDeployRequest(t.Context(), client, "org", resumeRequest(t, meta, ""), meta)

	require.Error(t, err)
	assert.Nil(t, result)
	var permanent *engine.PermanentError
	require.ErrorAs(t, err, &permanent)
	assert.False(t, errors.Is(err, engine.ErrReattachFailed))
	assert.False(t, engine.IsRetryable(err))
	assert.False(t, engine.IsOutcomeUnknown(err))
	assert.Contains(t, err.Error(), "deploy request #42 could not be deployed: PlanetScale deploy request approvals are not supported")
	assert.Equal(t, 1, client.deployCalls)
}

// refusedStartClient refuses every deploy with deployErr and serves the deploy
// request as current, which a close marks closed. closeErr fails the close;
// getReturnsNil and closeReturnsNil make the read or the close answer with no
// deploy request. Deploys, reads and closes are recorded.
type refusedStartClient struct {
	psclient.PSClient
	deployErr       error
	current         ps.DeployRequest
	closeErr        error
	getReturnsNil   bool
	closeReturnsNil bool
	deployCalls     int
	getCalls        int
	closes          []uint64
}

func (c *refusedStartClient) DeployDeployRequest(context.Context, *ps.PerformDeployRequest) (*ps.DeployRequest, error) {
	c.deployCalls++
	return nil, c.deployErr
}

func (c *refusedStartClient) GetDeployRequest(context.Context, *ps.GetDeployRequestRequest) (*ps.DeployRequest, error) {
	c.getCalls++
	if c.getReturnsNil {
		return nil, nil
	}
	current := c.current
	return &current, nil
}

func (c *refusedStartClient) CloseDeployRequest(_ context.Context, req *ps.CloseDeployRequestRequest) (*ps.DeployRequest, error) {
	c.closes = append(c.closes, req.Number)
	if c.closeErr != nil {
		return nil, c.closeErr
	}
	if c.closeReturnsNil {
		return nil, nil
	}
	c.current.State = deployRequestClosed
	closed := c.current
	return &closed, nil
}

// refusedStartRequest addresses deferred deploy request #121 on mydb, which
// the engine created unless adopted says it took it over from the branch.
func refusedStartRequest(t *testing.T, adopted bool) *engine.ControlRequest {
	t.Helper()
	meta, err := encodePSMetadata(&psMetadata{
		BranchName:           "schemabot-mydb-abc",
		DeployRequestID:      121,
		DeployRequestURL:     "https://example.test/deploys/121",
		DeployRequestAdopted: adopted,
		DeferredDeploy:       true,
	})
	require.NoError(t, err)
	return &engine.ControlRequest{
		Database:    "mydb",
		ResumeState: &engine.ResumeState{Metadata: meta},
		Credentials: conformanceCredentials(),
	}
}

// readyDeployRequest121 is deploy request #121 open and ready, never deployed.
func readyDeployRequest121() ps.DeployRequest {
	return ps.DeployRequest{Number: 121, State: deployRequestOpen, DeploymentState: deployState.Ready}
}

const approvalRefusal121 = "deploy request #121 could not be deployed: PlanetScale deploy request approvals are not supported"

// Starting a deferred deploy shares the deploy call with a fresh apply, so an
// approval refusal reaches the caller of Start as the same permanent error.
// The refusal fails the apply, so the engine closes the deploy request it
// created: left open, it could still be deployed from PlanetScale after
// SchemaBot reported the apply failed. Progress then reads it as closed
// before it deployed.
func TestStart_ApprovalRefusalClosesTheDeployRequest(t *testing.T) {
	refusal := approvalRequiredFromAPI(t)
	client := &refusedStartClient{deployErr: refusal, current: readyDeployRequest121()}
	e := conformanceEngine(client)
	req := refusedStartRequest(t, false)

	result, err := e.Start(t.Context(), req)

	require.Error(t, err)
	assert.Nil(t, result)
	var permanent *engine.PermanentError
	require.ErrorAs(t, err, &permanent)
	assert.ErrorIs(t, err, refusal)
	assert.False(t, engine.IsOutcomeUnknown(err))
	assert.Contains(t, err.Error(), approvalRefusal121)
	assert.NotContains(t, err.Error(), "still open")
	assert.Equal(t, 1, client.deployCalls)
	assert.Equal(t, []uint64{121}, client.closes, "the refused deploy request is closed")

	progress, err := e.Progress(t.Context(), &engine.ProgressRequest{Database: req.Database, ResumeState: req.ResumeState, Credentials: req.Credentials})
	require.NoError(t, err)
	assert.Equal(t, engine.StateCancelled, progress.State)
}

// When the close of a refused deploy request fails, the deploy request is
// still open and deployable from PlanetScale. Start still answers with a
// permanent error carrying the original refusal, and its message adds which
// deploy request is still open so the operator can close it. Why the close
// failed stays out of the message.
func TestStart_ApprovalRefusalWhoseCloseFailsNamesTheOpenDeployRequest(t *testing.T) {
	refusal := approvalRequiredFromAPI(t)
	closeErr := deployErrorFromAPI(t, http.StatusInternalServerError, `{"code":"internal","message":"upstream close exploded"}`)
	client := &refusedStartClient{deployErr: refusal, current: readyDeployRequest121(), closeErr: closeErr}
	e := conformanceEngine(client)

	result, err := e.Start(t.Context(), refusedStartRequest(t, false))

	require.Error(t, err)
	assert.Nil(t, result)
	var permanent *engine.PermanentError
	require.ErrorAs(t, err, &permanent)
	var approval *approvalRequiredError
	require.ErrorAs(t, err, &approval, "the original refusal stays on the chain")
	assert.ErrorIs(t, err, refusal)
	assert.ErrorIs(t, err, closeErr)
	assert.False(t, engine.IsRetryable(err))
	assert.Contains(t, err.Error(), approvalRefusal121)
	assert.Contains(t, err.Error(), "SchemaBot could not close deploy request #121 on database mydb (https://example.test/deploys/121), so it is still open; close it in PlanetScale so it cannot be deployed")
	assert.NotContains(t, err.Error(), "upstream close exploded", "the close failure stays on the chain but out of the message")
	assert.Equal(t, []uint64{121}, client.closes)
}

// A deploy request the engine adopted from the branch was opened by someone
// else, so a refused deploy leaves it open for them. The failure still names
// it, since it can be deployed from PlanetScale after the apply fails.
func TestStart_ApprovalRefusalLeavesAnAdoptedDeployRequestOpen(t *testing.T) {
	refusal := approvalRequiredFromAPI(t)
	client := &refusedStartClient{deployErr: refusal, current: readyDeployRequest121()}
	e := conformanceEngine(client)

	result, err := e.Start(t.Context(), refusedStartRequest(t, true))

	require.Error(t, err)
	assert.Nil(t, result)
	var permanent *engine.PermanentError
	require.ErrorAs(t, err, &permanent)
	assert.ErrorIs(t, err, refusal)
	assert.Contains(t, err.Error(), approvalRefusal121)
	assert.Contains(t, err.Error(), "SchemaBot did not open deploy request #121 on database mydb (https://example.test/deploys/121), so it left it open; close it in PlanetScale if it should not deploy")
	assert.Empty(t, client.closes, "an adopted deploy request is never closed")
	assert.Zero(t, client.getCalls)
}

// A deploy call that fails without a refusal, such as a dropped connection,
// may have started the deploy, so the deploy request is never closed and the
// caller gets an unknown outcome to recover from.
func TestStart_AmbiguousDeployFailureNeverClosesTheDeployRequest(t *testing.T) {
	client := &refusedStartClient{deployErr: errors.New("read tcp 10.0.0.1:443: connection reset by peer"), current: readyDeployRequest121()}
	e := conformanceEngine(client)

	result, err := e.Start(t.Context(), refusedStartRequest(t, false))

	require.Error(t, err)
	assert.Nil(t, result)
	assert.True(t, engine.IsOutcomeUnknown(err))
	assert.True(t, engine.IsRetryable(err))
	assert.Empty(t, client.closes, "a deploy that may be running is never closed")
}

// When the re-read before the close shows PlanetScale may have begun the
// deploy, the deploy request is not closed, and the failure tells the operator
// to check whether the change landed rather than to close it.
func TestStart_ApprovalRefusalOverARunningDeployRequestAdvisesCheckingIt(t *testing.T) {
	running := readyDeployRequest121()
	running.DeploymentState = deployState.InProgress
	client := &refusedStartClient{deployErr: approvalRequiredFromAPI(t), current: running}
	e := conformanceEngine(client)

	_, err := e.Start(t.Context(), refusedStartRequest(t, false))

	require.Error(t, err)
	var permanent *engine.PermanentError
	require.ErrorAs(t, err, &permanent)
	assert.Empty(t, client.closes, "a deploy request that reads as deployed is never closed")
	assert.Contains(t, err.Error(), "PlanetScale reports deploy request #121 on database mydb (https://example.test/deploys/121) may have started deploying; check in PlanetScale whether the change landed before applying again")
	assert.NotContains(t, err.Error(), "close it in PlanetScale")
}

// A read or close that answers with no deploy request leaves the deploy
// request unaccounted for, so the failure names it as still open.
func TestStart_ApprovalRefusalWithAnEmptyResponseNamesTheOpenDeployRequest(t *testing.T) {
	for _, tc := range []struct {
		name       string
		client     *refusedStartClient
		wantCloses []uint64
	}{
		{name: "read returns nothing", client: &refusedStartClient{getReturnsNil: true}},
		{name: "close returns nothing", client: &refusedStartClient{closeReturnsNil: true}, wantCloses: []uint64{121}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.client.deployErr = approvalRequiredFromAPI(t)
			tc.client.current = readyDeployRequest121()
			e := conformanceEngine(tc.client)

			_, err := e.Start(t.Context(), refusedStartRequest(t, false))

			require.Error(t, err)
			var permanent *engine.PermanentError
			require.ErrorAs(t, err, &permanent)
			assert.Contains(t, err.Error(), approvalRefusal121)
			assert.Contains(t, err.Error(), "SchemaBot could not close deploy request #121 on database mydb (https://example.test/deploys/121), so it is still open")
			assert.Equal(t, tc.wantCloses, tc.client.closes)
		})
	}
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
		{name: "a permission refusal that mentions approval without requiring it", deployErr: deployErrorFromAPI(t, http.StatusForbidden, `{"code":"unauthorized","message":"Only approved members of this organization can deploy."}`)},
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
