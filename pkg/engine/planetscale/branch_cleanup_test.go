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
