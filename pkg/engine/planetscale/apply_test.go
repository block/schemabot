package planetscale

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/schema"
)

type permanentVSchemaErrorClient struct {
	psclient.PSClient
	updateCalls int
}

var _ psclient.PSClient = (*permanentVSchemaErrorClient)(nil)

func (c *permanentVSchemaErrorClient) UpdateKeyspaceVSchema(context.Context, *ps.UpdateKeyspaceVSchemaRequest) (*ps.VSchema, error) {
	c.updateCalls++
	return nil, &ps.Error{Code: ps.ErrInvalid}
}

// A schema that is already converged produces a deploy request with no
// differences. The apply must be accepted so the orchestrator completes the
// change's tasks normally rather than reporting a spurious failure.
func TestNoChangesApplyResultIsAccepted(t *testing.T) {
	fresh := noChangesApplyResult("no changes detected")
	assert.True(t, fresh.Accepted)
	assert.Equal(t, "no changes detected", fresh.Message)

	resume := noChangesApplyResult("no changes detected on resume")
	assert.True(t, resume.Accepted)
	assert.Equal(t, "no changes detected on resume", resume.Message)
}

func TestApply_MainBranchReuseIsPermanent(t *testing.T) {
	e := NewWithClient(slog.New(slog.NewTextHandler(os.Stdout, nil)), func(_, _ string) (psclient.PSClient, error) {
		return nil, nil
	})

	_, err := e.Apply(t.Context(), &engine.ApplyRequest{
		Database: "testdb",
		Options: map[string]string{
			"branch": "main",
		},
		Credentials: &engine.Credentials{
			Metadata: map[string]string{
				"organization": "org",
				"token_name":   "token",
				"token_value":  "secret",
				"main_branch":  "main",
			},
		},
	})

	require.Error(t, err)
	assert.False(t, engine.IsRetryable(err))
	assert.Contains(t, err.Error(), "cannot reuse the main branch")
}

func TestApplyKeyspaceChanges_PermanentVSchemaErrorIsPermanent(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &permanentVSchemaErrorClient{}

	err := e.applyKeyspaceChanges(t.Context(),
		engine.SchemaChange{
			Namespace: "testapp",
			Metadata:  map[string]string{"vschema_changed": "true"},
		},
		schema.SchemaFiles{
			"testapp": &schema.Namespace{Files: map[string]string{"vschema.json": "{}"}},
		},
		&ps.DatabaseBranchPassword{},
		client,
		"org",
		"database",
		"branch",
	)

	require.Error(t, err)
	assert.False(t, engine.IsRetryable(err))
	assert.Equal(t, 1, client.updateCalls)
}

// pendingPollClient returns the deploy request as pending on every poll until
// a configured number of polls have occurred, then returns an error. With
// failPolls set, the error clears after that many consecutive failures and the
// request is served ready; with failPolls zero the API stays down. This drives
// the pending-poll loop through a transient API failure.
type pendingPollClient struct {
	psclient.PSClient
	number       uint64
	pollErr      error
	pollsBefore  int
	failPolls    int
	getCallCount int
	// script, when set, replaces the counters: poll n is answered by script[n-1]
	// (an error, or a deploy request in the given state), with the last entry
	// repeating once the script runs out.
	script []scriptedPoll
}

type scriptedPoll struct {
	err   error
	state string
}

func (c *pendingPollClient) GetDeployRequest(_ context.Context, req *ps.GetDeployRequestRequest) (*ps.DeployRequest, error) {
	c.getCallCount++
	if len(c.script) > 0 {
		step := c.script[min(c.getCallCount, len(c.script))-1]
		if step.err != nil {
			return nil, step.err
		}
		return &ps.DeployRequest{Number: req.Number, DeploymentState: step.state}, nil
	}
	if c.getCallCount > c.pollsBefore {
		if c.failPolls == 0 || c.getCallCount <= c.pollsBefore+c.failPolls {
			return nil, c.pollErr
		}
		return &ps.DeployRequest{Number: req.Number, DeploymentState: deployState.Ready}, nil
	}
	return &ps.DeployRequest{Number: req.Number, DeploymentState: deployState.Pending}, nil
}

// When PlanetScale keeps failing while a deploy request is still computing its
// schema diff, the apply driver gives up after maxRetries consecutive retryable
// failures and surfaces a wrapped error identifying the deploy request rather
// than panicking on a nil response or polling forever.
func TestWaitForDeployRequestPending_PollErrorIsWrapped(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	apiErr := &ps.Error{Code: ps.ErrInternal}
	client := &pendingPollClient{number: 7, pollErr: apiErr, pollsBefore: 1}

	_, err := e.waitForDeployRequestPending(t.Context(), client, "org", "testdb",
		&ps.DeployRequest{Number: 7, DeploymentState: deployState.Pending})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "poll deploy request 7")
	assert.ErrorIs(t, err, apiErr)
	assert.Equal(t, 1+maxRetries, client.getCallCount, "retryable failures are tolerated up to the bound, then surfaced")
}

// A single transient API error during the pending poll must not fail the apply:
// the deploy request already exists on PlanetScale, and failing here would
// abandon it (or fork a duplicate beside it on a Vitess resume). The poll
// carries on and returns the settled request once the API recovers.
func TestWaitForDeployRequestPending_TransientPollErrorIsRetried(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &pendingPollClient{number: 8, pollErr: &ps.Error{Code: ps.ErrInternal}, pollsBefore: 1, failPolls: 1}

	dr, err := e.waitForDeployRequestPending(t.Context(), client, "org", "testdb",
		&ps.DeployRequest{Number: 8, DeploymentState: deployState.Pending})

	require.NoError(t, err)
	require.NotNil(t, dr)
	assert.Equal(t, deployState.Ready, dr.DeploymentState)
	assert.Equal(t, 3, client.getCallCount, "pending, transient failure, then ready")
}

// The retry bound counts consecutive failures, not failures over the whole
// wait: a flaky API that answers between failures never accumulates to the
// bound, so a long diff computation is not failed by scattered errors.
func TestWaitForDeployRequestPending_SuccessfulPollResetsTheRetryBound(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	flake := &ps.Error{Code: ps.ErrInternal}
	client := &pendingPollClient{number: 12, script: []scriptedPoll{
		{err: flake}, {state: deployState.Pending},
		{err: flake}, {state: deployState.Pending},
		{err: flake}, {state: deployState.Pending},
		{err: flake}, {state: deployState.Ready},
	}}

	dr, err := e.waitForDeployRequestPending(t.Context(), client, "org", "testdb",
		&ps.DeployRequest{Number: 12, DeploymentState: deployState.Pending})

	require.NoError(t, err)
	require.NotNil(t, dr)
	assert.Equal(t, deployState.Ready, dr.DeploymentState)
	assert.Equal(t, 8, client.getCallCount)
}

// Only retryable PlanetScale errors are tolerated. A not-found, or any other
// error that will not clear on its own, fails the poll at once rather than
// burning the retry bound against an answer that is already final.
func TestWaitForDeployRequestPending_NonRetryablePollErrorFailsAtOnce(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	apiErr := &ps.Error{Code: ps.ErrNotFound}
	client := &pendingPollClient{number: 10, pollErr: apiErr, pollsBefore: 1, failPolls: 1}

	_, err := e.waitForDeployRequestPending(t.Context(), client, "org", "testdb",
		&ps.DeployRequest{Number: 10, DeploymentState: deployState.Pending})

	require.Error(t, err)
	assert.ErrorIs(t, err, apiErr)
	assert.Equal(t, 2, client.getCallCount, "a non-retryable error is not polled past")
}

// A deploy request that never leaves the pending state must not block the apply
// driver forever — cancelling the context stops the poll and returns a wrapped
// error naming the deploy request.
func TestWaitForDeployRequestPending_HonorsContextCancellation(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &pendingPollClient{number: 9, pollErr: errors.New("unreachable"), pollsBefore: 1_000_000}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	done := make(chan error, 1)
	go func() {
		_, err := e.waitForDeployRequestPending(ctx, client, "org", "testdb",
			&ps.DeployRequest{Number: 9, DeploymentState: deployState.Pending})
		done <- err
	}()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Contains(t, err.Error(), "deploy request 9")
		assert.Equal(t, 0, client.getCallCount)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("waitForDeployRequestPending did not return after context cancellation")
	}
}

// A nil deploy request indicates an upstream caller never created or fetched it;
// the poll loop surfaces a wrapped error naming the database rather than
// dereferencing nil.
func TestWaitForDeployRequestPending_NilDeployRequestIsRejected(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &pendingPollClient{number: 11, pollErr: errors.New("unreachable"), pollsBefore: 1_000_000}

	dr, err := e.waitForDeployRequestPending(t.Context(), client, "org", "testdb", nil)

	require.Error(t, err)
	assert.Nil(t, dr)
	assert.Contains(t, err.Error(), "deploy request is nil")
	assert.Contains(t, err.Error(), "testdb")
	assert.Equal(t, 0, client.getCallCount)
}

// deployRejectionClient rejects DeployDeployRequest with a configured error a
// set number of times before accepting, so tests can drive the deploy retry
// paths (still-validating polls, transient backoff, permanent rejections).
type deployRejectionClient struct {
	psclient.PSClient
	rejectErr   error
	rejections  int
	deployCalls int
}

func (c *deployRejectionClient) DeployDeployRequest(_ context.Context, req *ps.PerformDeployRequest) (*ps.DeployRequest, error) {
	c.deployCalls++
	if c.deployCalls <= c.rejections {
		return nil, c.rejectErr
	}
	return &ps.DeployRequest{Number: req.Number, DeploymentState: deployState.InProgress}, nil
}

// compressDeployValidationWait shrinks the validation poll budget so tests
// exercise the wait loop without real-time delays.
func compressDeployValidationWait(t *testing.T, wait, interval time.Duration) {
	t.Helper()
	origWait, origInterval := deployValidationWait, deployValidationPollInterval
	deployValidationWait, deployValidationPollInterval = wait, interval
	t.Cleanup(func() {
		deployValidationWait, deployValidationPollInterval = origWait, origInterval
	})
}

const stillValidatingMessage = "We're currently validating that these changes are safe to deploy. Please try again in a few moments."

// PlanetScale rejects the deploy call while the deploy request's safety
// validation is still running. The rejection clears on its own, so the deploy
// must be retried until validation completes and the apply proceeds — not
// converted into a terminal failure.
func TestDeployDeployRequest_RetriesWhileStillValidating(t *testing.T) {
	compressDeployValidationWait(t, time.Second, time.Millisecond)
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &deployRejectionClient{rejectErr: errors.New(stillValidatingMessage), rejections: 2}

	dr, err := e.deployDeployRequest(t.Context(), client, "org", "testdb", 124, false)

	require.NoError(t, err)
	require.NotNil(t, dr)
	assert.Equal(t, uint64(124), dr.Number)
	assert.Equal(t, 3, client.deployCalls)
}

// A deploy request whose validation never completes must not hold the apply
// forever: past the bounded wait the deploy fails with an error naming how
// long SchemaBot waited.
func TestDeployDeployRequest_ValidationPastDeadlineFails(t *testing.T) {
	compressDeployValidationWait(t, 100*time.Millisecond, 5*time.Millisecond)
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &deployRejectionClient{rejectErr: errors.New(stillValidatingMessage), rejections: 1_000_000}

	_, err := e.deployDeployRequest(t.Context(), client, "org", "testdb", 126, false)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "still validating")
	assert.Contains(t, err.Error(), deployValidationWait.String())
	assert.ErrorContains(t, err, stillValidatingMessage)
}

// Transient PlanetScale API errors on the deploy call retry with backoff up
// to the retry bound, then succeed once the API recovers.
func TestDeployDeployRequest_TransientErrorsRetry(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &deployRejectionClient{rejectErr: &ps.Error{Code: ps.ErrRetry}, rejections: 1}

	dr, err := e.deployDeployRequest(t.Context(), client, "org", "testdb", 7, true)

	require.NoError(t, err)
	require.NotNil(t, dr)
	assert.Equal(t, 2, client.deployCalls)
}

// A deploy rejected because the PlanetScale database requires administrator
// approval can never succeed on retry; it fails immediately with guidance to
// disable the approval requirement.
func TestDeployDeployRequest_ApprovalRequirementFailsImmediately(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &deployRejectionClient{rejectErr: errors.New("deploy request must be approved"), rejections: 1_000_000}

	_, err := e.deployDeployRequest(t.Context(), client, "org", "testdb", 9, false)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "Require administrator approval")
	assert.Equal(t, 1, client.deployCalls)
}

// resumeDeployClient returns a fixed deploy request on Get and records whether a
// deploy was started. The recovered deploy request lets resume tests drive the
// reattach path without a live PlanetScale API or vtgate.
type resumeDeployClient struct {
	psclient.PSClient
	recovered    *ps.DeployRequest
	getErr       error
	getCalls     int
	deployCalls  int
	lastDeploy   *ps.PerformDeployRequest
	deployResult *ps.DeployRequest
	autoCutover  bool
	// pendingGets is how many Get calls report the deploy request as still
	// computing its schema diff before the recovered state is served.
	pendingGets int
}

func (c *resumeDeployClient) DeployRequestAutoCutover(_ context.Context, _, _ string, _ uint64) (bool, error) {
	return c.autoCutover, nil
}

func (c *resumeDeployClient) GetDeployRequest(_ context.Context, req *ps.GetDeployRequestRequest) (*ps.DeployRequest, error) {
	c.getCalls++
	if c.getErr != nil {
		return nil, c.getErr
	}
	dr := *c.recovered
	dr.Number = req.Number
	if c.getCalls <= c.pendingGets {
		dr.DeploymentState = deployState.Pending
	}
	return &dr, nil
}

func (c *resumeDeployClient) DeployDeployRequest(_ context.Context, req *ps.PerformDeployRequest) (*ps.DeployRequest, error) {
	c.deployCalls++
	c.lastDeploy = req
	if c.deployResult != nil {
		return c.deployResult, nil
	}
	dr := *c.recovered
	dr.Number = req.Number
	dr.DeploymentState = deployState.InProgress
	return &dr, nil
}

func resumeRequest(t *testing.T, meta *psMetadata, migrationContext string) *engine.ApplyRequest {
	t.Helper()
	encoded, err := encodePSMetadata(meta)
	require.NoError(t, err)
	return &engine.ApplyRequest{
		Database: "testdb",
		Credentials: &engine.Credentials{
			Metadata: map[string]string{
				"organization": "org",
				"main_branch":  "main",
			},
		},
		ResumeState: &engine.ResumeState{
			MigrationContext: migrationContext,
			Metadata:         encoded,
		},
	}
}

// captureStateChanges installs an OnStateChange callback on req that records every
// persisted ResumeState, so resume tests can assert the engine durably persists a
// rediscovered Vitess context rather than only returning it in the ApplyResult.
func captureStateChanges(req *engine.ApplyRequest) *[]*engine.ResumeState {
	var persisted []*engine.ResumeState
	req.OnStateChange = func(state *engine.ResumeState) {
		persisted = append(persisted, state)
	}
	return &persisted
}

// deployRequestNeedsResumeDeploy gates the resume path that starts a deploy the
// crashed driver never began. It must fire only for a non-deferred deploy
// request that finished validation ("ready") and was never deployed.
func TestDeployRequestNeedsResumeDeploy(t *testing.T) {
	deployedAt := time.Now()
	cases := []struct {
		name        string
		dr          *ps.DeployRequest
		meta        *psMetadata
		deferDeploy bool
		want        bool
	}{
		{
			name: "ready non-deferred not deployed needs deploy",
			dr:   &ps.DeployRequest{DeploymentState: deployState.Ready},
			meta: &psMetadata{},
			want: true,
		},
		{
			name: "ready but deferred is left for operator-triggered deploy",
			dr:   &ps.DeployRequest{DeploymentState: deployState.Ready},
			meta: &psMetadata{DeferredDeploy: true},
			want: false,
		},
		{
			// The metadata flag is persisted after the deploy request is created,
			// so a crash inside that window recovers a deferred apply whose stored
			// metadata does not say so yet. The operator's request is what decides.
			name:        "ready and deferred by request before the flag was stored",
			dr:          &ps.DeployRequest{DeploymentState: deployState.Ready},
			meta:        &psMetadata{},
			deferDeploy: true,
			want:        false,
		},
		{
			name: "ready but already deployed is in flight",
			dr:   &ps.DeployRequest{DeploymentState: deployState.Ready, DeployedAt: &deployedAt},
			meta: &psMetadata{},
			want: false,
		},
		{
			name: "in progress is already running",
			dr:   &ps.DeployRequest{DeploymentState: deployState.InProgress},
			meta: &psMetadata{},
			want: false,
		},
		{
			name: "pending validation is not ready yet",
			dr:   &ps.DeployRequest{DeploymentState: deployState.Pending},
			meta: &psMetadata{},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, deployRequestNeedsResumeDeploy(tc.dr, tc.meta, tc.deferDeploy))
		})
	}
}

// deployRequestAwaitsDeferredDeployRecord gates the resume path that records a
// deferral the stopped driver never wrote. It must fire only for a deferred
// request that is ready, undeployed, and not yet recorded as deferred: a request
// already deployed out of band but still reporting ready must not be recorded
// as waiting for a deploy that has already happened.
func TestDeployRequestAwaitsDeferredDeployRecord(t *testing.T) {
	deployedAt := time.Now()
	cases := []struct {
		name        string
		dr          *ps.DeployRequest
		meta        *psMetadata
		deferDeploy bool
		want        bool
	}{
		{
			name:        "ready, deferred by request, not yet recorded",
			dr:          &ps.DeployRequest{DeploymentState: deployState.Ready},
			meta:        &psMetadata{},
			deferDeploy: true,
			want:        true,
		},
		{
			name:        "deferral already recorded",
			dr:          &ps.DeployRequest{DeploymentState: deployState.Ready},
			meta:        &psMetadata{DeferredDeploy: true},
			deferDeploy: true,
			want:        false,
		},
		{
			name: "not deferred by request",
			dr:   &ps.DeployRequest{DeploymentState: deployState.Ready},
			meta: &psMetadata{},
			want: false,
		},
		{
			name:        "already deployed but still reporting ready",
			dr:          &ps.DeployRequest{DeploymentState: deployState.Ready, DeployedAt: &deployedAt},
			meta:        &psMetadata{},
			deferDeploy: true,
			want:        false,
		},
		{
			name:        "in progress is already running",
			dr:          &ps.DeployRequest{DeploymentState: deployState.InProgress},
			meta:        &psMetadata{},
			deferDeploy: true,
			want:        false,
		},
		{
			name:        "no changes has nothing to hold for the operator",
			dr:          &ps.DeployRequest{DeploymentState: deployState.NoChanges},
			meta:        &psMetadata{},
			deferDeploy: true,
			want:        false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, deployRequestAwaitsDeferredDeployRecord(tc.dr, tc.meta, tc.deferDeploy))
		})
	}
}

// A driver that crashed between creating a non-deferred deploy request and
// starting it leaves the request stuck in "ready", which Progress reports as
// pending forever. Resuming must start the deploy so the schema change actually
// runs. The instant decision is taken from the recovered request's own
// eligibility, as the fresh path would have taken it: the record the stopped
// driver wrote carries no decision, and a stored flag is not what decides.
func TestResumeExistingDeployRequest_DeploysReadyNeverStarted(t *testing.T) {
	tests := []struct {
		name        string
		recovered   *ps.DeployRequest
		meta        *psMetadata
		wantInstant bool
	}{
		{
			name: "an eligible safe change deploys instant without a stored decision",
			recovered: &ps.DeployRequest{
				DeploymentState: deployState.Ready, HtmlURL: "https://app/dr/42",
				Deployment: &ps.Deployment{InstantDDLEligible: true},
			},
			meta:        &psMetadata{BranchName: "schemabot-testdb-abc", DeployRequestID: 42},
			wantInstant: true,
		},
		{
			name:        "a stored instant flag does not make an ineligible request instant",
			recovered:   &ps.DeployRequest{DeploymentState: deployState.Ready, HtmlURL: "https://app/dr/42"},
			meta:        &psMetadata{BranchName: "schemabot-testdb-abc", DeployRequestID: 42, IsInstant: true},
			wantInstant: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
			client := &resumeDeployClient{recovered: tt.recovered}
			req := resumeRequest(t, tt.meta, "apply-1a2b3c4d5e6f7890")

			result, err := e.resumeExistingDeployRequest(t.Context(), client, "org", req, tt.meta)

			require.NoError(t, err)
			require.NotNil(t, result)
			assert.True(t, result.Accepted)
			assert.Equal(t, 1, client.deployCalls)
			require.NotNil(t, client.lastDeploy)
			assert.Equal(t, uint64(42), client.lastDeploy.Number)
			assert.Equal(t, "testdb", client.lastDeploy.Database)
			assert.Equal(t, tt.wantInstant, client.lastDeploy.InstantDDL)
			assert.Contains(t, result.Message, "Resumed and deployed request #42")

			require.NotNil(t, result.ResumeState)
			stored, err := decodePSMetadata(result.ResumeState.Metadata)
			require.NoError(t, err)
			assert.Equal(t, tt.wantInstant, stored.IsInstant, "stored state carries the decision that was deployed")
		})
	}
}

// A deploy request that is already in flight when the driver resumes must not be
// re-deployed; resume simply reattaches and preserves the stored progress state.
func TestResumeExistingDeployRequest_InFlightIsNotRedeployed(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &resumeDeployClient{
		recovered: &ps.DeployRequest{DeploymentState: deployState.InProgress, HtmlURL: "https://app/dr/7"},
	}

	meta := &psMetadata{BranchName: "schemabot-testdb-xyz", DeployRequestID: 7}
	req := resumeRequest(t, meta, "singularity:real-context")
	persisted := captureStateChanges(req)

	result, err := e.resumeExistingDeployRequest(t.Context(), client, "org", req, meta)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Accepted)
	assert.Equal(t, 0, client.deployCalls)
	require.NotNil(t, result.ResumeState)
	assert.Equal(t, "singularity:real-context", result.ResumeState.MigrationContext)
	assert.Contains(t, result.Message, "Resumed deploy request #7")
	// A stored real Vitess context is authoritative — resume neither rediscovers
	// nor re-persists it.
	assert.Empty(t, *persisted)
}

// A driver can crash between creating a deploy request and deploying it, and the
// drive that recovers it cannot know how it was created — the setting is fixed at
// creation and the deploy request looks ordinary either way. So a recovered
// request whose cutover the operator deferred is verified before it is deployed,
// exactly as a fresh one is: held, and it deploys; not held, and it is refused
// with the deploy never started.
func TestResumeExistingDeployRequest_VerifiesTheCutoverHoldBeforeDeploying(t *testing.T) {
	tests := []struct {
		name        string
		autoCutover bool
		wantDeploys int
	}{
		{name: "cutover held by the deploy request", autoCutover: false, wantDeploys: 1},
		{name: "cutover owned by the backend", autoCutover: true, wantDeploys: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
			client := &resumeDeployClient{
				recovered:   &ps.DeployRequest{DeploymentState: deployState.Ready, HtmlURL: "https://app/dr/31"},
				autoCutover: tt.autoCutover,
			}

			meta := &psMetadata{BranchName: "schemabot-testdb-hold", DeployRequestID: 31}
			req := resumeRequest(t, meta, "apply-1a2b3c4d5e6f7890")
			req.Options = map[string]string{"defer_cutover": "true"}

			_, err := e.resumeExistingDeployRequest(t.Context(), client, "org", req, meta)

			assert.Equal(t, tt.wantDeploys, client.deployCalls)
			if tt.autoCutover {
				require.Error(t, err, "a recovered deploy request that would cut itself over must be refused")
				assert.Contains(t, err.Error(), "auto-cutover")
				return
			}
			require.NoError(t, err)
		})
	}
}

// Instant DDL swaps the schema as the deploy runs, so an instant-eligible deploy
// request recovered on resume must not run instant while the operator holds the
// cutover — that would leave them a gate with the swap already behind it. An
// ordinary resume of the same request deploys instantly.
func TestResumeExistingDeployRequest_DeferredCutoverDeclinesRecoveredInstantDDL(t *testing.T) {
	tests := []struct {
		name        string
		options     map[string]string
		wantInstant bool
	}{
		{name: "cutover deferred declines instant DDL", options: map[string]string{"defer_cutover": "true"}, wantInstant: false},
		{name: "cutover not deferred deploys instant", options: nil, wantInstant: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
			client := &resumeDeployClient{
				recovered: &ps.DeployRequest{
					DeploymentState: deployState.Ready, HtmlURL: "https://app/dr/58",
					Deployment: &ps.Deployment{InstantDDLEligible: true},
				},
			}

			meta := &psMetadata{BranchName: "schemabot-testdb-inst", DeployRequestID: 58}
			req := resumeRequest(t, meta, "apply-1a2b3c4d5e6f7890")
			req.Options = tt.options

			_, err := e.resumeExistingDeployRequest(t.Context(), client, "org", req, meta)

			require.NoError(t, err)
			require.Equal(t, 1, client.deployCalls)
			require.NotNil(t, client.lastDeploy)
			assert.Equal(t, tt.wantInstant, client.lastDeploy.InstantDDL)
		})
	}
}

// Instant DDL has no revert window, so an instant-eligible deploy request
// recovered on resume must not run instant when the change is unsafe — the
// change would land with no way to revert it. The row-copy path keeps the
// revert window, so instant DDL is declined and the deploy still starts.
func TestResumeExistingDeployRequest_UnsafeChangesDeclineRecoveredInstantDDL(t *testing.T) {
	tests := []struct {
		name        string
		changes     []engine.SchemaChange
		wantInstant bool
	}{
		{
			name: "a DROP COLUMN declines instant DDL",
			changes: []engine.SchemaChange{{
				Namespace:    "orders",
				TableChanges: []engine.TableChange{{DDL: "ALTER TABLE `users` DROP COLUMN `email`"}},
			}},
			wantInstant: false,
		},
		{
			name: "a DROP TABLE declines instant DDL",
			changes: []engine.SchemaChange{{
				Namespace:    "orders",
				TableChanges: []engine.TableChange{{DDL: "DROP TABLE `users`"}},
			}},
			wantInstant: false,
		},
		{
			name: "a safe additive change deploys instant",
			changes: []engine.SchemaChange{{
				Namespace:    "orders",
				TableChanges: []engine.TableChange{{DDL: "ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20)"}},
			}},
			wantInstant: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
			client := &resumeDeployClient{
				recovered: &ps.DeployRequest{
					DeploymentState: deployState.Ready, HtmlURL: "https://app/dr/61",
					Deployment: &ps.Deployment{InstantDDLEligible: true},
				},
			}

			meta := &psMetadata{BranchName: "schemabot-testdb-drop", DeployRequestID: 61}
			req := resumeRequest(t, meta, "apply-1a2b3c4d5e6f7890")
			req.Changes = tt.changes

			_, err := e.resumeExistingDeployRequest(t.Context(), client, "org", req, meta)

			require.NoError(t, err)
			require.Equal(t, 1, client.deployCalls)
			require.NotNil(t, client.lastDeploy)
			assert.Equal(t, tt.wantInstant, client.lastDeploy.InstantDDL)
		})
	}
}

// A deferred deploy request recovered on resume must wait for the
// operator-triggered deploy rather than being started automatically.
func TestResumeExistingDeployRequest_DeferredIsNotDeployed(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &resumeDeployClient{
		recovered: &ps.DeployRequest{DeploymentState: deployState.Ready, HtmlURL: "https://app/dr/9"},
	}

	meta := &psMetadata{BranchName: "schemabot-testdb-def", DeployRequestID: 9, DeferredDeploy: true}
	req := resumeRequest(t, meta, "apply-1a2b3c4d5e6f7890")

	result, err := e.resumeExistingDeployRequest(t.Context(), client, "org", req, meta)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 0, client.deployCalls)
	assert.Contains(t, result.Message, "Resumed deploy request #9")
}

// A driver that stops after creating a non-deferred deploy request, while
// PlanetScale is still computing its schema diff, recovers the request in
// "pending". Resume waits for the diff to finish and then starts the deploy,
// so the schema change runs instead of sitting in pending forever.
func TestResumeExistingDeployRequest_WaitsOutPendingThenDeploys(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &resumeDeployClient{
		recovered:   &ps.DeployRequest{DeploymentState: deployState.Ready, HtmlURL: "https://app/dr/44"},
		pendingGets: 1,
	}

	meta := &psMetadata{BranchName: "schemabot-testdb-pend", DeployRequestID: 44}
	req := resumeRequest(t, meta, "apply-1a2b3c4d5e6f7890")

	result, err := e.resumeExistingDeployRequest(t.Context(), client, "org", req, meta)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 2, client.getCalls, "resume polls past pending before deciding")
	assert.Equal(t, 1, client.deployCalls)
	require.NotNil(t, client.lastDeploy)
	assert.Equal(t, uint64(44), client.lastDeploy.Number)
	assert.Contains(t, result.Message, "Resumed and deployed request #44")
}

// A driver running a deferred deploy can stop after creating the deploy request
// but before recording the deferral, which is written only once the request is
// ready. Resume records the deferral itself, with the deploy left to the
// operator: progress then reports waiting_for_deploy instead of pending, and the
// operator-triggered deploy is accepted instead of refused. This holds whether
// the recovered request is already ready or still computing its schema diff.
func TestResumeExistingDeployRequest_RecordsDeferralTheStoppedDriverDidNot(t *testing.T) {
	tests := []struct {
		name        string
		pendingGets int
	}{
		{name: "recovered request already ready", pendingGets: 0},
		{name: "recovered request still computing its schema diff", pendingGets: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &resumeDeployClient{
				recovered:   &ps.DeployRequest{DeploymentState: deployState.Ready, HtmlURL: "https://app/dr/52"},
				pendingGets: tt.pendingGets,
			}
			e := NewWithClient(slog.New(slog.NewTextHandler(os.Stdout, nil)),
				func(_, _ string) (psclient.PSClient, error) { return client, nil })

			meta := &psMetadata{BranchName: "schemabot-testdb-defer", DeployRequestID: 52}
			req := resumeRequest(t, meta, "apply-1a2b3c4d5e6f7890")
			req.Options = map[string]string{"defer_deploy": "true"}
			persisted := captureStateChanges(req)

			result, err := e.resumeExistingDeployRequest(t.Context(), client, "org", req, meta)

			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, 0, client.deployCalls, "a deferred deploy is left for the operator")
			assert.Contains(t, result.Message, "Deploy request #52 ready — waiting for deploy")

			require.Len(t, *persisted, 1, "the deferral is durably recorded, not only returned")
			stored, err := decodePSMetadata((*persisted)[0].Metadata)
			require.NoError(t, err)
			assert.True(t, stored.DeferredDeploy)
			assert.Equal(t, uint64(52), stored.DeployRequestID)
			assert.Equal(t, "apply-1a2b3c4d5e6f7890", (*persisted)[0].MigrationContext)
			require.NotNil(t, result.ResumeState)
			assert.Equal(t, (*persisted)[0].Metadata, result.ResumeState.Metadata)

			creds := &engine.Credentials{Metadata: map[string]string{
				"organization": "org",
				"token_name":   "token",
				"token_value":  "secret",
			}}
			progress, err := e.Progress(t.Context(), &engine.ProgressRequest{
				Database:    "testdb",
				ResumeState: (*persisted)[0],
				Credentials: creds,
			})
			require.NoError(t, err)
			assert.Equal(t, engine.StateWaitingForDeploy, progress.State)

			started, err := e.Start(t.Context(), &engine.ControlRequest{
				Database:    "testdb",
				ResumeState: (*persisted)[0],
				Credentials: creds,
			})
			require.NoError(t, err)
			assert.True(t, started.Accepted)
			assert.Equal(t, 1, client.deployCalls)
			require.NotNil(t, client.lastDeploy)
			assert.Equal(t, uint64(52), client.lastDeploy.Number)
		})
	}
}

func TestResumeExistingDeployRequest_RecordsInstantDecisionForDeferredStart(t *testing.T) {
	tests := []struct {
		name        string
		options     map[string]string
		changes     []engine.SchemaChange
		wantInstant bool
	}{
		{
			name:        "instant-eligible safe change",
			options:     map[string]string{"defer_deploy": "true"},
			wantInstant: true,
		},
		{
			name:    "unsafe change",
			options: map[string]string{"defer_deploy": "true"},
			changes: []engine.SchemaChange{{
				Namespace:    "orders",
				TableChanges: []engine.TableChange{{DDL: "ALTER TABLE `users` DROP COLUMN `email`"}},
			}},
			wantInstant: false,
		},
		{
			name:        "deferred cutover",
			options:     map[string]string{"defer_deploy": "true", "defer_cutover": "true"},
			wantInstant: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &resumeDeployClient{
				recovered: &ps.DeployRequest{
					DeploymentState: deployState.Ready,
					HtmlURL:         "https://app/dr/53",
					Deployment:      &ps.Deployment{InstantDDLEligible: true},
				},
			}
			e := NewWithClient(slog.New(slog.NewTextHandler(os.Stdout, nil)),
				func(_, _ string) (psclient.PSClient, error) { return client, nil })
			meta := &psMetadata{BranchName: "schemabot-testdb-instant", DeployRequestID: 53}
			req := resumeRequest(t, meta, "apply-1a2b3c4d5e6f7890")
			req.Options = tt.options
			req.Changes = tt.changes
			persisted := captureStateChanges(req)

			_, err := e.resumeExistingDeployRequest(t.Context(), client, "org", req, meta)
			require.NoError(t, err)
			require.Len(t, *persisted, 1)
			stored, err := decodePSMetadata((*persisted)[0].Metadata)
			require.NoError(t, err)
			assert.Equal(t, tt.wantInstant, stored.IsInstant)

			started, err := e.Start(t.Context(), &engine.ControlRequest{
				Database:    "testdb",
				ResumeState: (*persisted)[0],
				Credentials: &engine.Credentials{Metadata: map[string]string{
					"organization": "org", "token_name": "token", "token_value": "secret",
				}},
			})
			require.NoError(t, err)
			assert.True(t, started.Accepted)
			require.NotNil(t, client.lastDeploy)
			assert.Equal(t, tt.wantInstant, client.lastDeploy.InstantDDL)
		})
	}
}

// A recovered deferred deploy whose cutover the operator also deferred is only
// recorded as waiting_for_deploy when the deploy request holds the cutover:
// Start trusts the stored record and does not re-verify it, so this resume-time
// check is the only thing standing between the operator's start and a request
// that would swap the schema on its own. A request holding auto-cutover on is
// refused, with nothing recorded and nothing deployed.
func TestResumeExistingDeployRequest_RecoveredDeferralVerifiesTheCutoverHold(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &resumeDeployClient{
		recovered:   &ps.DeployRequest{DeploymentState: deployState.Ready, HtmlURL: "https://app/dr/54"},
		autoCutover: true,
	}
	meta := &psMetadata{BranchName: "schemabot-testdb-hold", DeployRequestID: 54}
	req := resumeRequest(t, meta, "apply-1a2b3c4d5e6f7890")
	req.Options = map[string]string{"defer_deploy": "true", "defer_cutover": "true"}
	persisted := captureStateChanges(req)

	_, err := e.resumeExistingDeployRequest(t.Context(), client, "org", req, meta)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "auto-cutover")
	assert.Empty(t, *persisted, "a deferral is never recorded for a request that would cut itself over")
	assert.Equal(t, 0, client.deployCalls)
}

// A recovered deploy request whose diff settles to no changes has nothing to
// deploy, defer, or reattach to. Resume returns the same converged, accepted
// result as the fresh and branch-resume paths rather than reattaching and
// hunting for a Vitess context the change never created.
func TestResumeExistingDeployRequest_NoChangesIsConverged(t *testing.T) {
	tests := []struct {
		name    string
		options map[string]string
	}{
		{name: "non-deferred", options: nil},
		{name: "deferred", options: map[string]string{"defer_deploy": "true"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
			client := &resumeDeployClient{
				recovered:   &ps.DeployRequest{DeploymentState: deployState.NoChanges, HtmlURL: "https://app/dr/55"},
				pendingGets: 1,
			}
			meta := &psMetadata{BranchName: "schemabot-testdb-same", DeployRequestID: 55}
			req := resumeRequest(t, meta, "apply-1a2b3c4d5e6f7890")
			req.Options = tt.options
			persisted := captureStateChanges(req)

			result, err := e.resumeExistingDeployRequest(t.Context(), client, "org", req, meta)

			require.NoError(t, err)
			require.NotNil(t, result)
			assert.True(t, result.Accepted)
			assert.Equal(t, "no changes detected on resume", result.Message)
			assert.Equal(t, 0, client.deployCalls)
			assert.Empty(t, *persisted, "nothing to record for a change that never ran")
		})
	}
}

// A failed deploy request must not be resumed; the apply restarts fresh on a new
// branch. With nil credentials the fresh Apply fails fast, proving the recovered
// request was abandoned rather than reattached.
func TestResumeExistingDeployRequest_ErrorStateStartsFresh(t *testing.T) {
	e := NewWithClient(slog.New(slog.NewTextHandler(os.Stdout, nil)),
		func(_, _ string) (psclient.PSClient, error) {
			return nil, errors.New("client unavailable")
		})
	client := &resumeDeployClient{
		recovered: &ps.DeployRequest{DeploymentState: deployState.Error},
	}

	meta := &psMetadata{BranchName: "schemabot-testdb-err", DeployRequestID: 5}
	req := resumeRequest(t, meta, "apply-1a2b3c4d5e6f7890")

	_, err := e.resumeExistingDeployRequest(t.Context(), client, "org", req, meta)

	require.Error(t, err)
	assert.Equal(t, 0, client.deployCalls)
	assert.Nil(t, req.ResumeState)
}

// A transient error fetching the recovered deploy request must NOT be treated as
// "the deploy request was cleaned up." Starting fresh here would create a new
// branch and a second deploy request while the original is still actively
// deploying. Resume must propagate the transient error so it retries against the
// same deploy request instead of forking a duplicate schema change.
func TestResumeExistingDeployRequest_TransientGetErrorDoesNotFork(t *testing.T) {
	freshApplyCalled := false
	e := NewWithClient(slog.New(slog.NewTextHandler(os.Stdout, nil)),
		func(_, _ string) (psclient.PSClient, error) {
			freshApplyCalled = true
			return nil, errors.New("fresh apply must not run")
		})
	transientErr := &ps.Error{Code: ps.ErrInternal}
	client := &resumeDeployClient{getErr: transientErr}

	meta := &psMetadata{BranchName: "schemabot-testdb-xyz", DeployRequestID: 7}
	req := resumeRequest(t, meta, "singularity:in-flight")

	_, err := e.resumeExistingDeployRequest(t.Context(), client, "org", req, meta)

	require.Error(t, err)
	assert.ErrorIs(t, err, transientErr)
	assert.Contains(t, err.Error(), "get deploy request #7 on resume")
	// No fresh apply, no second deploy: the original deploy request is untouched.
	assert.False(t, freshApplyCalled)
	assert.Equal(t, 0, client.deployCalls)
	assert.Equal(t, 1, client.getCalls)
	// ResumeState is preserved so the next resume retries the same deploy request.
	require.NotNil(t, req.ResumeState)
	assert.Equal(t, "singularity:in-flight", req.ResumeState.MigrationContext)
}

// A genuine not-found means the deploy request really was cleaned up, so resume
// abandons the stale record and starts a fresh apply. The fresh Apply fails fast
// on the recovered request's incomplete credentials, proving the not-found path
// takes the start-fresh branch (which clears ResumeState) rather than
// propagating the not-found as a retryable error.
func TestResumeExistingDeployRequest_NotFoundStartsFresh(t *testing.T) {
	e := NewWithClient(slog.New(slog.NewTextHandler(os.Stdout, nil)),
		func(_, _ string) (psclient.PSClient, error) {
			return nil, errors.New("client unavailable")
		})
	client := &resumeDeployClient{getErr: &ps.Error{Code: ps.ErrNotFound}}

	meta := &psMetadata{BranchName: "schemabot-testdb-gone", DeployRequestID: 13}
	req := resumeRequest(t, meta, "apply-1a2b3c4d5e6f7890")

	_, err := e.resumeExistingDeployRequest(t.Context(), client, "org", req, meta)

	require.Error(t, err)
	assert.Equal(t, 0, client.deployCalls)
	assert.Equal(t, 1, client.getCalls)
	// The start-fresh branch clears ResumeState before re-running Apply, unlike
	// the transient-error path which preserves it for retry.
	assert.Nil(t, req.ResumeState)
}

// When no vtgate DSN is configured, resume cannot rediscover a Vitess context;
// it must preserve the stored identifier as-is rather than blanking it, so the
// apply record keeps whatever progress handle it already had.
func TestResolveResumeSchemaChangeContext_PreservesStoredWithoutDSN(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &resumeDeployClient{recovered: &ps.DeployRequest{}}

	req := &engine.ApplyRequest{
		Database:    "testdb",
		Credentials: &engine.Credentials{Metadata: map[string]string{"organization": "org", "main_branch": "main"}},
		ResumeState: &engine.ResumeState{MigrationContext: "apply-1a2b3c4d5e6f7890"},
	}

	got := e.resolveResumeSchemaChangeContext(t.Context(), client, req, map[string]MigrationContextTimestamps{}, time.Time{})

	assert.Equal(t, "apply-1a2b3c4d5e6f7890", got)
}

// A real Vitess context carries the "<system>:<uuid>" form that appears in SHOW
// VITESS_MIGRATIONS, while the tern-assigned apply identifier ("apply-<hex>")
// does not. Only the former drives per-shard progress, so the two must be
// distinguishable by the colon separator.
func TestIsRealVitessContext(t *testing.T) {
	cases := []struct {
		name             string
		migrationContext string
		want             bool
	}{
		{name: "singularity context is real", migrationContext: "singularity:17694ee9-aaaa-bbbb", want: true},
		{name: "revert context is real", migrationContext: "revert:singularity:17694ee9", want: true},
		{name: "tern apply identifier is not real", migrationContext: "apply-1a2b3c4d5e6f7890", want: false},
		{name: "tern task identifier is not real", migrationContext: "task-1a2b3c4d5e6f7890", want: false},
		{name: "empty is not real", migrationContext: "", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isRealVitessContext(tc.migrationContext))
		})
	}
}

// persistResumeSchemaChangeContext must durably record only a real Vitess context.
// Persisting the tern apply identifier or an empty value would carry no per-shard
// progress and could clobber a previously persisted real context.
func TestPersistResumeSchemaChangeContext(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))

	t.Run("persists real context", func(t *testing.T) {
		req := resumeRequest(t, &psMetadata{}, "")
		persisted := captureStateChanges(req)

		e.persistResumeSchemaChangeContext(req, "singularity:real-context", "encoded-meta")

		require.Len(t, *persisted, 1)
		assert.Equal(t, "singularity:real-context", (*persisted)[0].MigrationContext)
		assert.Equal(t, "encoded-meta", (*persisted)[0].Metadata)
	})

	t.Run("skips apply identifier", func(t *testing.T) {
		req := resumeRequest(t, &psMetadata{}, "")
		persisted := captureStateChanges(req)

		e.persistResumeSchemaChangeContext(req, "apply-1a2b3c4d5e6f7890", "encoded-meta")

		assert.Empty(t, *persisted)
	})

	t.Run("skips empty context", func(t *testing.T) {
		req := resumeRequest(t, &psMetadata{}, "")
		persisted := captureStateChanges(req)

		e.persistResumeSchemaChangeContext(req, "", "encoded-meta")

		assert.Empty(t, *persisted)
	})

	t.Run("no-op when callback is nil", func(t *testing.T) {
		req := resumeRequest(t, &psMetadata{}, "")
		req.OnStateChange = nil

		assert.NotPanics(t, func() {
			e.persistResumeSchemaChangeContext(req, "singularity:real-context", "encoded-meta")
		})
	})
}

// When the resume deploy path resolves a real Vitess context, it must persist
// that context via OnStateChange before returning. Otherwise a crash after the
// deploy starts but before the apply returns would leave storage holding the
// tern apply identifier, with no per-shard Vitess progress for the rest of the
// apply. The stored value here is already a real context, so resolution returns
// it deterministically without a live vtgate, and the persist must fire.
func TestResumeExistingDeployRequest_PersistsRealContextOnDeploy(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &resumeDeployClient{
		recovered: &ps.DeployRequest{DeploymentState: deployState.Ready, HtmlURL: "https://app/dr/42"},
	}

	meta := &psMetadata{BranchName: "schemabot-testdb-abc", DeployRequestID: 42}
	req := resumeRequest(t, meta, "singularity:real-context")
	persisted := captureStateChanges(req)

	result, err := e.resumeExistingDeployRequest(t.Context(), client, "org", req, meta)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 1, client.deployCalls)
	assert.Equal(t, "singularity:real-context", result.ResumeState.MigrationContext)

	require.Len(t, *persisted, 1)
	assert.Equal(t, "singularity:real-context", (*persisted)[0].MigrationContext)
	assert.NotEmpty(t, (*persisted)[0].Metadata)
}

// On the reattach-only path (deploy already in flight from a prior process), a
// stored value that is still the tern apply identifier means an earlier crash
// lost the discovered Vitess context. Resume must attempt rediscovery so
// per-shard progress can recover. Without a vtgate DSN discovery turns up
// nothing, so the stored identifier is preserved and nothing is persisted —
// proving the rediscovery branch is gated on the value being a non-real context
// without clobbering it.
func TestResumeExistingDeployRequest_ReattachRediscoversWhenApplyIdentifier(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &resumeDeployClient{
		recovered: &ps.DeployRequest{DeploymentState: deployState.InProgress, HtmlURL: "https://app/dr/7"},
	}

	meta := &psMetadata{BranchName: "schemabot-testdb-xyz", DeployRequestID: 7}
	req := resumeRequest(t, meta, "apply-1a2b3c4d5e6f7890")
	persisted := captureStateChanges(req)

	result, err := e.resumeExistingDeployRequest(t.Context(), client, "org", req, meta)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 0, client.deployCalls)
	require.NotNil(t, result.ResumeState)
	assert.Equal(t, "apply-1a2b3c4d5e6f7890", result.ResumeState.MigrationContext)
	// Discovery found no real context (no DSN), so the apply identifier is kept
	// rather than persisted.
	assert.Empty(t, *persisted)
}

// declineEventMessages filters the operator-facing events down to the row-copy
// decline announcements, so tests can assert the decline reached the apply's
// timeline and not only the engine log.
func declineEventMessages(events []engine.ApplyEvent) []string {
	var msgs []string
	for _, event := range events {
		if strings.Contains(event.Message, "Deploying with a row copy rather than instant DDL") {
			msgs = append(msgs, event.Message)
		}
	}
	return msgs
}

// branchResumeClient serves the branch-resume path — a driver that crashed
// after creating the branch but before the deploy request existed — without a
// live PlanetScale API. The branch is ready, its schema already matches the
// desired files (no remaining DDL to run), and the deploy request this resume
// creates is instant-DDL eligible so the instant decision is actually reached.
type branchResumeClient struct {
	psclient.PSClient
	deployCalls int
	lastDeploy  *ps.PerformDeployRequest
	lastCreate  *ps.CreateDeployRequestRequest
}

func (c *branchResumeClient) GetBranch(_ context.Context, req *ps.GetDatabaseBranchRequest) (*ps.DatabaseBranch, error) {
	return &ps.DatabaseBranch{Name: req.Branch, Ready: true}, nil
}

func (c *branchResumeClient) CreateDeployRequest(_ context.Context, req *ps.CreateDeployRequestRequest) (*ps.DeployRequest, error) {
	c.lastCreate = req
	return &ps.DeployRequest{
		Number:          77,
		HtmlURL:         "https://app/dr/77",
		DeploymentState: deployState.Ready,
		Deployment:      &ps.Deployment{InstantDDLEligible: true},
	}, nil
}

func (c *branchResumeClient) DeployDeployRequest(_ context.Context, req *ps.PerformDeployRequest) (*ps.DeployRequest, error) {
	c.deployCalls++
	c.lastDeploy = req
	return &ps.DeployRequest{Number: req.Number, DeploymentState: deployState.InProgress}, nil
}

// A resume that recovers a branch whose deploy request was never created takes
// the instant DDL decision fresh, exactly as a fresh apply does: an unsafe
// change deploys with a row copy so a revert window stays open, and the decline
// is announced on the apply's timeline, not only in the engine log. A safe
// change keeps instant DDL and announces nothing.
func TestResumeApply_BranchResumeDecidesInstantDDLAndAnnouncesDecline(t *testing.T) {
	tests := []struct {
		name        string
		ddl         string
		wantInstant bool
	}{
		{name: "an unsafe change declines instant DDL and announces the row copy", ddl: "ALTER TABLE `users` DROP COLUMN `email`", wantInstant: false},
		{name: "a safe change keeps instant DDL", ddl: "ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20)", wantInstant: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
			client := &branchResumeClient{}

			meta := &psMetadata{BranchName: "schemabot-testdb-crash"}
			req := resumeRequest(t, meta, "apply-1a2b3c4d5e6f7890")
			req.Changes = []engine.SchemaChange{{
				Namespace:    "orders",
				TableChanges: []engine.TableChange{{DDL: tt.ddl}},
			}}
			var events []engine.ApplyEvent
			req.OnEvent = func(event engine.ApplyEvent) { events = append(events, event) }

			result, err := e.resumeApply(t.Context(), client, "org", req)

			require.NoError(t, err)
			require.NotNil(t, result)
			assert.True(t, result.Accepted)
			require.Equal(t, 1, client.deployCalls)
			require.NotNil(t, client.lastDeploy)
			assert.Equal(t, tt.wantInstant, client.lastDeploy.InstantDDL)

			// The decision that deployed is the decision that was persisted.
			require.NotNil(t, result.ResumeState)
			stored, decodeErr := decodePSMetadata(result.ResumeState.Metadata)
			require.NoError(t, decodeErr)
			assert.Equal(t, tt.wantInstant, stored.IsInstant)

			declines := declineEventMessages(events)
			if tt.wantInstant {
				assert.Empty(t, declines)
			} else {
				require.Len(t, declines, 1)
				assert.Contains(t, declines[0], "DROP COLUMN")
				assert.Contains(t, declines[0], "revert window")
			}
		})
	}
}

// A resume that creates the deploy request a crashed drive never created hands
// the branch to that deploy request for deletion only when SchemaBot created
// the branch. An apply run against an operator-supplied branch keeps that
// branch after the deploy, exactly as a fresh drive would; a branch SchemaBot
// generated is still deleted by the deploy request.
func TestResumeApply_BranchResumeDeletesOnlySchemaBotBranch(t *testing.T) {
	tests := []struct {
		name           string
		branch         string
		options        map[string]string
		wantAutoDelete bool
	}{
		{
			name:           "an operator-supplied branch is kept after the deploy",
			branch:         "my-dev-branch",
			options:        map[string]string{"branch": "my-dev-branch"},
			wantAutoDelete: false,
		},
		{
			name:           "a branch SchemaBot generated is deleted by the deploy request",
			branch:         "schemabot-testdb-crash",
			options:        nil,
			wantAutoDelete: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
			client := &branchResumeClient{}

			req := resumeRequest(t, &psMetadata{BranchName: tt.branch}, "apply-1a2b3c4d5e6f7890")
			req.Options = tt.options

			result, err := e.resumeApply(t.Context(), client, "org", req)

			require.NoError(t, err)
			require.NotNil(t, result)
			assert.True(t, result.Accepted)
			require.NotNil(t, client.lastCreate)
			assert.Equal(t, tt.branch, client.lastCreate.Branch)
			assert.Equal(t, "main", client.lastCreate.IntoBranch)
			assert.Equal(t, tt.wantAutoDelete, client.lastCreate.AutoDeleteBranch)
		})
	}
}

// When resume declines instant DDL on an eligible recovered request, the
// decision must be visible everywhere it can be read later: the deploy runs
// with a row copy, the apply's timeline carries the announcement, and the
// returned metadata stores IsInstant=false. Metadata that reported instant
// would mislead Progress and any later consumer of the stored value about how
// the deploy actually ran.
func TestResumeExistingDeployRequest_NarrowedDecisionIsPersistedAndAnnounced(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &resumeDeployClient{
		recovered: &ps.DeployRequest{
			DeploymentState: deployState.Ready, HtmlURL: "https://app/dr/63",
			Deployment: &ps.Deployment{InstantDDLEligible: true},
		},
	}

	meta := &psMetadata{BranchName: "schemabot-testdb-narrow", DeployRequestID: 63}
	req := resumeRequest(t, meta, "apply-1a2b3c4d5e6f7890")
	req.Changes = []engine.SchemaChange{{
		Namespace:    "orders",
		TableChanges: []engine.TableChange{{DDL: "ALTER TABLE `users` DROP COLUMN `email`"}},
	}}
	var events []engine.ApplyEvent
	req.OnEvent = func(event engine.ApplyEvent) { events = append(events, event) }

	result, err := e.resumeExistingDeployRequest(t.Context(), client, "org", req, meta)

	require.NoError(t, err)
	require.NotNil(t, client.lastDeploy)
	assert.False(t, client.lastDeploy.InstantDDL)

	declines := declineEventMessages(events)
	require.Len(t, declines, 1)
	assert.Contains(t, declines[0], "DROP COLUMN")
	assert.Contains(t, declines[0], "revert window")

	require.NotNil(t, result.ResumeState)
	stored, err := decodePSMetadata(result.ResumeState.Metadata)
	require.NoError(t, err)
	assert.False(t, stored.IsInstant)
}
