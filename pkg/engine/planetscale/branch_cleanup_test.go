package planetscale

import (
	"context"
	"errors"
	"sync"
	"testing"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/psclient"
)

// branchLifecycleClient serves a branch that exists and is ready, and fails the
// credential request that follows it — the shape of an apply that dies while
// preparing its branch, before any deploy request exists. Deletions are
// recorded so the test can assert on the cleanup.
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

func (c *branchLifecycleClient) DeleteBranch(_ context.Context, req *ps.DeleteDatabaseBranchRequest) error {
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

	lastCreate *ps.CreateDeployRequestRequest
}

func (c *branchHandoverClient) CreateBranchPassword(context.Context, *ps.DatabaseBranchPasswordRequest) (*ps.DatabaseBranchPassword, error) {
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
