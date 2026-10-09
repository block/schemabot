package planetscale

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/psclient"
)

// branchLifecycleClient serves a branch that exists and is ready, and fails the
// credential request that follows it — the shape of an apply that dies while
// preparing its branch, before any deploy request exists. Deletions are
// recorded so the test can assert on the cleanup. A delete whose context has
// already ended fails without being recorded, as a real API request would.
type branchLifecycleClient struct {
	psclient.PSClient

	mu      sync.Mutex
	created []string
	deleted []string
}

func (c *branchLifecycleClient) GetBranch(context.Context, *ps.GetDatabaseBranchRequest) (*ps.DatabaseBranch, error) {
	return &ps.DatabaseBranch{Ready: true, SafeMigrations: true}, nil
}

func (c *branchLifecycleClient) CreateBranch(_ context.Context, req *ps.CreateDatabaseBranchRequest) (*ps.DatabaseBranch, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.created = append(c.created, req.Name)
	return &ps.DatabaseBranch{Name: req.Name, Ready: true}, nil
}

func (c *branchLifecycleClient) DeleteBranch(ctx context.Context, req *ps.DeleteDatabaseBranchRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleted = append(c.deleted, req.Branch)
	return nil
}

func (c *branchLifecycleClient) RefreshSchema(context.Context, string, string, string) error {
	return nil
}

func (c *branchLifecycleClient) CreateBranchPassword(context.Context, *ps.DatabaseBranchPasswordRequest) (*ps.DatabaseBranchPassword, error) {
	return nil, errors.New("branch credentials unavailable")
}

func (c *branchLifecycleClient) snapshot() (created, deleted []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.created...), append([]string(nil), c.deleted...)
}

// A branch SchemaBot creates is stranded if the apply fails before a deploy
// request exists to own its teardown, and PlanetScale branches are quota'd —
// the strand eventually blocks unrelated schema changes. The apply must clean up
// after itself, while leaving an operator-supplied branch alone: SchemaBot did
// not create it and must not remove it.
func TestApplyDeletesItsOwnBranchWhenItFailsBeforeTheDeployRequest(t *testing.T) {
	applyRequest := func(options map[string]string) *engine.ApplyRequest {
		return &engine.ApplyRequest{
			PlanID:      "plan-0123456789abcdef",
			Database:    "commerce",
			Credentials: conformanceCredentials(),
			Options:     options,
		}
	}

	t.Run("a branch this apply created is deleted", func(t *testing.T) {
		client := &branchLifecycleClient{}
		_, err := conformanceEngine(client).Apply(t.Context(), applyRequest(nil))
		require.Error(t, err)

		created, deleted := client.snapshot()
		require.Len(t, created, 1)
		assert.Equal(t, created, deleted, "the apply deletes exactly the branch it created")
	})

	t.Run("an operator-supplied branch is left alone", func(t *testing.T) {
		client := &branchLifecycleClient{}
		_, err := conformanceEngine(client).Apply(t.Context(), applyRequest(map[string]string{"branch": "operator-branch"}))
		require.Error(t, err)

		created, deleted := client.snapshot()
		assert.Empty(t, created)
		assert.Empty(t, deleted)
	})
}

// handbackClient ends the drive while the branch is being prepared, the way
// the operator cancels a drive whose lease was lost, that looked stalled, or
// whose instance is shutting down.
type handbackClient struct {
	branchLifecycleClient
	cancel context.CancelFunc
}

func (c *handbackClient) CreateBranchPassword(ctx context.Context, _ *ps.DatabaseBranchPasswordRequest) (*ps.DatabaseBranchPassword, error) {
	c.cancel()
	return nil, ctx.Err()
}

// A drive that ends while its branch is being prepared hands the apply to
// another driver, which resumes from the branch the stored resume state names.
// Deleting that branch would discard the resume's starting point, and after a
// lost lease would pull it from under a peer already preparing it. A branch
// the stored state never named, because no state was recorded or because
// storage refused the save, cannot be resumed, so it is still deleted.
func TestApplyKeepsItsBranchForTheDriverThatResumesIt(t *testing.T) {
	shortenEngineWaits(t)
	tests := []struct {
		name        string
		recorded    bool
		saveErr     error
		wantDeleted bool
	}{
		{name: "a branch the stored resume state names is kept", recorded: true, wantDeleted: false},
		{name: "a branch the stored resume state never named is deleted", recorded: false, wantDeleted: true},
		{name: "a branch whose resume state storage refused is deleted", recorded: true, saveErr: errors.New("apply apply-0123456789abcdef: storage unavailable"), wantDeleted: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			client := &handbackClient{cancel: cancel}
			var stored []*engine.ResumeState
			req := &engine.ApplyRequest{
				PlanID:      "plan-0123456789abcdef",
				Database:    "commerce",
				Credentials: conformanceCredentials(),
			}
			if tt.recorded {
				req.ResumeState = &engine.ResumeState{MigrationContext: "apply-0123456789abcdef"}
				req.OnStateChange = func(rs *engine.ResumeState) error {
					if tt.saveErr != nil {
						return tt.saveErr
					}
					stored = append(stored, rs)
					return nil
				}
			}

			_, err := conformanceEngine(client).Apply(ctx, req)
			require.ErrorIs(t, err, context.Canceled)

			created, deleted := client.snapshot()
			require.Len(t, created, 1)
			if tt.wantDeleted {
				assert.Equal(t, created, deleted)
				return
			}
			assert.Empty(t, deleted, "the branch the resume will look for must survive the handback")
			require.NotEmpty(t, stored)
			meta, decodeErr := decodePSMetadata(stored[len(stored)-1].Metadata)
			require.NoError(t, decodeErr)
			assert.Equal(t, created[0], meta.BranchName, "the kept branch is the one the stored state names")
		})
	}
}

// branchHandoverClient carries a fresh apply through branch preparation to the
// deploy request: credentials are issued, the request has no changes so no
// MySQL connection is opened, and the deploy request it creates reports no
// changes, so the apply returns right after creating it. The create request is
// recorded so the test can read the teardown decision the apply handed over.
type branchHandoverClient struct {
	branchLifecycleClient

	lastCreate  *ps.CreateDeployRequestRequest
	passwordTTL int
}

func (c *branchHandoverClient) CreateBranchPassword(_ context.Context, req *ps.DatabaseBranchPasswordRequest) (*ps.DatabaseBranchPassword, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.passwordTTL = req.TTL
	return &ps.DatabaseBranchPassword{}, nil
}

func (c *branchHandoverClient) CreateDeployRequest(_ context.Context, req *ps.CreateDeployRequestRequest) (*ps.DeployRequest, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastCreate = req
	return &ps.DeployRequest{Number: 91, DeploymentState: deployState.NoChanges}, nil
}

// Once the deploy request exists it owns the branch's teardown, and the apply
// tells it which way to go: a branch SchemaBot created is deleted when the
// deploy request deploys, while an operator-supplied branch is the operator's
// and outlives the deploy. A resumed drive that creates the deploy request
// later makes the same decision from the same stored options, so the fresh
// drive's answer is the one the resume has to match.
func TestApplyHandsOnlyItsOwnBranchToTheDeployRequest(t *testing.T) {
	applyRequest := func(options map[string]string) *engine.ApplyRequest {
		return &engine.ApplyRequest{
			PlanID:      "plan-0123456789abcdef",
			Database:    "commerce",
			Credentials: conformanceCredentials(),
			Options:     options,
		}
	}

	t.Run("a branch this apply created is deleted by the deploy request", func(t *testing.T) {
		client := &branchHandoverClient{}
		result, err := conformanceEngine(client).Apply(t.Context(), applyRequest(nil))
		require.NoError(t, err)
		assert.True(t, result.Accepted)

		created, deleted := client.snapshot()
		require.Len(t, created, 1)
		assert.Empty(t, deleted, "the deploy request owns the teardown; the apply does not delete the branch itself")
		require.NotNil(t, client.lastCreate)
		assert.Equal(t, created[0], client.lastCreate.Branch)
		assert.True(t, client.lastCreate.AutoDeleteBranch)
		assert.Equal(t, branchPasswordTTL(0), client.passwordTTL, "the branch password is sized by the keyspaces the apply can touch")
	})

	t.Run("an operator-supplied branch outlives the deploy request", func(t *testing.T) {
		client := &branchHandoverClient{}
		result, err := conformanceEngine(client).Apply(t.Context(), applyRequest(map[string]string{"branch": "operator-branch"}))
		require.NoError(t, err)
		assert.True(t, result.Accepted)

		created, deleted := client.snapshot()
		assert.Empty(t, created)
		assert.Empty(t, deleted)
		require.NotNil(t, client.lastCreate)
		assert.Equal(t, "operator-branch", client.lastCreate.Branch)
		assert.False(t, client.lastCreate.AutoDeleteBranch)
	})
}

// resumeReclaimClient serves a resumed branch that is ready and issues its
// credentials, so a resume runs on to its diff. Deletions are recorded, and
// deleteErr, when set, fails each one after recording it.
type resumeReclaimClient struct {
	branchLifecycleClient
	deleteErr error
}

func (c *resumeReclaimClient) CreateBranchPassword(context.Context, *ps.DatabaseBranchPasswordRequest) (*ps.DatabaseBranchPassword, error) {
	return &ps.DatabaseBranchPassword{}, nil
}

func (c *resumeReclaimClient) DeleteBranch(ctx context.Context, req *ps.DeleteDatabaseBranchRequest) error {
	if err := c.branchLifecycleClient.DeleteBranch(ctx, req); err != nil {
		return err
	}
	return c.deleteErr
}

// A resumed drive that fails for good before it creates the deploy request
// leaves nothing else to tear the branch down, so it deletes a branch SchemaBot
// created rather than strand it against quota. It keeps the branch whenever
// the branch is not SchemaBot's to delete or another drive may still need it:
// an operator-supplied branch belongs to the operator, a retryable failure
// leaves the branch for the retry, and a drive whose context ended is handing
// the apply to the driver that resumes it from that branch.
func TestReclaimBranchAfterFailedResume(t *testing.T) {
	const ownedBranch = "schemabot-commerce-1a2b"
	permanent := engine.NewPermanentError("branch differs from the declared schema on tables the plan does not change (commerce.legacy_audit)")
	retryable := errors.New("fetch branch schemabot-commerce-1a2b schema via MySQL on resume: connection refused")
	tests := []struct {
		name        string
		branch      string
		options     map[string]string
		cause       error
		driveEnded  bool
		wantDeleted []string
	}{
		{name: "a terminal failure deletes the branch SchemaBot created", branch: ownedBranch, cause: permanent, wantDeleted: []string{ownedBranch}},
		{name: "an operator-supplied branch is never deleted", branch: "my-dev-branch", options: map[string]string{"branch": "my-dev-branch"}, cause: permanent},
		{name: "a retryable failure keeps the branch for the retry", branch: ownedBranch, cause: retryable},
		{name: "a drive that ended keeps the branch for the driver that resumes it", branch: ownedBranch, cause: permanent, driveEnded: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tt.driveEnded {
				cancel()
			}
			client := &resumeReclaimClient{}
			req := resumeRequest(t, &psMetadata{BranchName: tt.branch}, "apply-1a2b3c4d5e6f7890")
			req.Options = tt.options

			conformanceEngine(client).reclaimBranchAfterFailedResume(ctx, client, "org", req, tt.branch, tt.cause)

			_, deleted := client.snapshot()
			assert.Equal(t, tt.wantDeleted, deleted)
		})
	}
}

// inFlightDeleteClient holds each branch delete open until its context ends,
// signalling started once the request is in flight, and records whether the
// delete was abandoned because its context ended. A delete detached from the
// drive's context is released only by the test's fallback, so it is recorded
// as not abandoned.
type inFlightDeleteClient struct {
	resumeReclaimClient
	started   chan struct{}
	abandoned chan bool
}

func (c *inFlightDeleteClient) DeleteBranch(ctx context.Context, _ *ps.DeleteDatabaseBranchRequest) error {
	close(c.started)
	select {
	case <-ctx.Done():
		c.abandoned <- true
		return ctx.Err()
	case <-time.After(5 * time.Second):
		c.abandoned <- false
		return nil
	}
}

// A resumed drive starts deleting its branch after a permanent failure, and
// its lease is lost while the delete request is still in flight. A peer may
// already be resuming the apply from that branch, so the old drive abandons
// the delete the moment its context ends rather than finishing it, and logs
// that it left the branch for the resuming driver instead of asking an
// operator to delete it by hand.
func TestReclaimBranchAfterFailedResume_LeaseLostDuringTheDeleteAbandonsIt(t *testing.T) {
	var logs bytes.Buffer
	client := &inFlightDeleteClient{started: make(chan struct{}), abandoned: make(chan bool, 1)}
	req := resumeRequest(t, &psMetadata{BranchName: "schemabot-commerce-1a2b"}, "apply-1a2b3c4d5e6f7890")
	req.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	ctx, loseLease := context.WithCancel(t.Context())
	defer loseLease()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conformanceEngine(client).reclaimBranchAfterFailedResume(ctx, client, "org", req, "schemabot-commerce-1a2b",
			engine.NewPermanentError("apply keyspace commerce: vschema rejected"))
	}()
	<-client.started
	loseLease()
	<-done

	assert.True(t, <-client.abandoned, "the delete must end with the drive's context, not run on past the lost lease")
	var record map[string]any
	require.NoError(t, json.Unmarshal(logs.Bytes(), &record), "exactly one log line: %s", logs.String())
	assert.Equal(t, "WARN", record["level"])
	assert.Equal(t, "drive ended while deleting the branch; abandoned the delete and left the branch for the driver that resumes the apply", record["msg"])
	assert.Equal(t, "schemabot-commerce-1a2b", record["branch"])
}

// A branch the reclaim could not delete is still against quota, so the failure
// is logged at error level with everything an operator needs to find the apply
// and delete the branch by hand: the apply, the organization, the PlanetScale
// database, the branch, and the failure that ended the drive.
func TestReclaimBranchAfterFailedResume_LogsAFailedDeleteForTriage(t *testing.T) {
	var logs bytes.Buffer
	client := &resumeReclaimClient{deleteErr: errors.New("branch delete refused")}
	req := resumeRequest(t, &psMetadata{BranchName: "schemabot-commerce-1a2b"}, "apply-1a2b3c4d5e6f7890")
	req.Logger = slog.New(slog.NewJSONHandler(&logs, nil)).With("apply_id", "apply-1a2b3c4d5e6f7890")
	cause := engine.NewPermanentError("apply keyspace commerce: vschema rejected")

	conformanceEngine(client).reclaimBranchAfterFailedResume(t.Context(), client, "org", req, "schemabot-commerce-1a2b", cause)

	var record map[string]any
	require.NoError(t, json.Unmarshal(logs.Bytes(), &record), "exactly one log line: %s", logs.String())
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "apply-1a2b3c4d5e6f7890", record["apply_id"])
	assert.Equal(t, "org", record["organization"])
	assert.Equal(t, "testdb", record["planetscale_database"])
	assert.Equal(t, "schemabot-commerce-1a2b", record["branch"])
	assert.Equal(t, "apply keyspace commerce: vschema rejected", record["apply_error"])
	assert.Equal(t, "branch delete refused", record["error"])
}

// A resume whose diff fails with a retryable error has not ended the apply for
// good, so it keeps the branch it resumed on and returns the diff's own error.
func TestResumeApply_RetryableFailureBeforeTheDeployRequestKeepsTheBranch(t *testing.T) {
	client := &resumeReclaimClient{}
	req := resumeRequest(t, &psMetadata{BranchName: "schemabot-testdb-1a2b"}, "apply-1a2b3c4d5e6f7890")
	req.Changes = []engine.SchemaChange{{
		Namespace:    "commerce",
		TableChanges: []engine.TableChange{{DDL: "this is not a statement"}},
	}}

	_, err := conformanceEngine(client).resumeApply(t.Context(), client, "org", req)

	require.Error(t, err)
	assert.True(t, engine.IsRetryable(err), "the diff failure must stay retryable: %v", err)
	assert.Contains(t, err.Error(), "resume branch schemabot-testdb-1a2b")
	_, deleted := client.snapshot()
	assert.Empty(t, deleted, "a retryable failure must leave the branch for the retry")
}
