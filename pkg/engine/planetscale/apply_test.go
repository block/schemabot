package planetscale

import (
	"context"
	"errors"
	"log/slog"
	"os"
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
// a configured number of polls have occurred, then returns an error. This
// drives the pending-poll loop through a transient API failure.
type pendingPollClient struct {
	psclient.PSClient
	number       uint64
	pollErr      error
	pollsBefore  int
	getCallCount int
}

func (c *pendingPollClient) GetDeployRequest(_ context.Context, req *ps.GetDeployRequestRequest) (*ps.DeployRequest, error) {
	c.getCallCount++
	if c.getCallCount > c.pollsBefore {
		return nil, c.pollErr
	}
	return &ps.DeployRequest{Number: req.Number, DeploymentState: deployState.Pending}, nil
}

// When PlanetScale returns a transient error while a deploy request is still
// computing its schema diff, the apply worker surfaces a wrapped error
// identifying the deploy request rather than panicking on a nil response.
func TestWaitForDeployRequestPending_PollErrorIsWrapped(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	apiErr := &ps.Error{Code: ps.ErrInternal}
	client := &pendingPollClient{number: 7, pollErr: apiErr, pollsBefore: 1}

	_, err := e.waitForDeployRequestPending(t.Context(), client, "org", "testdb",
		&ps.DeployRequest{Number: 7, DeploymentState: deployState.Pending})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "poll deploy request 7")
	assert.ErrorIs(t, err, apiErr)
	assert.Equal(t, 2, client.getCallCount)
}

// A deploy request that never leaves the pending state must not block the apply
// worker forever — cancelling the context stops the poll and returns a wrapped
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
}

func (c *resumeDeployClient) GetDeployRequest(_ context.Context, req *ps.GetDeployRequestRequest) (*ps.DeployRequest, error) {
	c.getCalls++
	if c.getErr != nil {
		return nil, c.getErr
	}
	dr := *c.recovered
	dr.Number = req.Number
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
// crashed worker never began. It must fire only for a non-deferred deploy
// request that finished validation ("ready") and was never deployed.
func TestDeployRequestNeedsResumeDeploy(t *testing.T) {
	deployedAt := time.Now()
	cases := []struct {
		name string
		dr   *ps.DeployRequest
		meta *psMetadata
		want bool
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
			assert.Equal(t, tc.want, deployRequestNeedsResumeDeploy(tc.dr, tc.meta))
		})
	}
}

// A worker that crashed between creating a non-deferred deploy request and
// starting it leaves the request stuck in "ready", which Progress reports as
// pending forever. Resuming must start the deploy so the schema change actually
// runs, carrying the instant DDL flag from the recovered metadata.
func TestResumeExistingDeployRequest_DeploysReadyNeverStarted(t *testing.T) {
	e := New(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	client := &resumeDeployClient{
		recovered: &ps.DeployRequest{DeploymentState: deployState.Ready, HtmlURL: "https://app/dr/42"},
	}

	meta := &psMetadata{BranchName: "schemabot-testdb-abc", DeployRequestID: 42, IsInstant: true}
	req := resumeRequest(t, meta, "apply-1a2b3c4d5e6f7890")

	result, err := e.resumeExistingDeployRequest(t.Context(), client, "org", req, meta)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Accepted)
	assert.Equal(t, 1, client.deployCalls)
	require.NotNil(t, client.lastDeploy)
	assert.Equal(t, uint64(42), client.lastDeploy.Number)
	assert.Equal(t, "testdb", client.lastDeploy.Database)
	assert.True(t, client.lastDeploy.InstantDDL)
	assert.Contains(t, result.Message, "Resumed and deployed request #42")
}

// A deploy request that is already in flight when the worker resumes must not be
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

	got := e.resolveResumeSchemaChangeContext(t.Context(), client, req, map[string]bool{})

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
