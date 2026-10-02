package webhook

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

// driftBlockedCheckStore returns a stored check carrying a review-time
// deployment drift block and records whether Upsert was called so a test can
// prove the apply-start path did not overwrite (clear) the block.
type driftBlockedCheckStore struct {
	storage.CheckStore
	stored      *storage.Check
	upsertCalls int
}

func (s *driftBlockedCheckStore) Get(ctx context.Context, repo string, pr int, environment, dbType, database string) (*storage.Check, error) {
	return s.stored, nil
}

func (s *driftBlockedCheckStore) Upsert(ctx context.Context, check *storage.Check) error {
	s.upsertCalls++
	return nil
}

type driftBlockedStorage struct {
	emptyStorage
	checks *driftBlockedCheckStore
}

func (s *driftBlockedStorage) Checks() storage.CheckStore {
	return s.checks
}

func TestUpdateCheckRecordForApplyStartRefusesDriftBlock(t *testing.T) {
	checks := &driftBlockedCheckStore{
		stored: &storage.Check{
			Repository:     "octocat/hello-world",
			PullRequest:    1,
			HeadSHA:        "abc123",
			Environment:    "production",
			DatabaseType:   "mysql",
			DatabaseName:   "orders",
			Status:         checkStatusCompleted,
			Conclusion:     "failure",
			BlockingReason: storage.ReviewTimeDeploymentDriftBlockingReason,
		},
	}
	service := api.New(&driftBlockedStorage{checks: checks}, &api.ServerConfig{
		Repos: map[string]api.RepoConfig{},
	}, nil, testLogger())
	h := &Handler{service: service, logger: testLogger()}

	schema := &ghclient.SchemaRequestResult{
		Database: "orders",
		Type:     "mysql",
	}

	apply := &storage.Apply{ID: 42, ApplyIdentifier: "apply-drift42"}
	err := h.updateCheckRecordForApplyStart(t.Context(), nil, "octocat/hello-world", 1, schema, "production", apply)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "review-time deployment drift block present")
	assert.Zero(t, checks.upsertCalls, "apply start must not overwrite a stored drift block")
}

// A stored namespace placement refusal blocks an apply start the same way a
// drift block does, and the refusal names the fix its reason calls for: the
// namespace placement, not a deployment to reconcile.
func TestUpdateCheckRecordForApplyStartRefusesNamespacePlacementBlock(t *testing.T) {
	checks := &driftBlockedCheckStore{
		stored: &storage.Check{
			Repository:     "octocat/hello-world",
			PullRequest:    1,
			HeadSHA:        "abc123",
			Environment:    "production",
			DatabaseType:   "mysql",
			DatabaseName:   "orders",
			Status:         checkStatusCompleted,
			Conclusion:     "failure",
			BlockingReason: storage.NamespacePlacementRefusedBlockingReason,
		},
	}
	service := api.New(&driftBlockedStorage{checks: checks}, &api.ServerConfig{
		Repos: map[string]api.RepoConfig{},
	}, nil, testLogger())
	h := &Handler{service: service, logger: testLogger()}

	schema := &ghclient.SchemaRequestResult{Database: "orders", Type: "mysql"}
	apply := &storage.Apply{ID: 43, ApplyIdentifier: "apply-placement43"}
	err := h.updateCheckRecordForApplyStart(t.Context(), nil, "octocat/hello-world", 1, schema, "production", apply)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "namespace placement refused this environment's plan; fix the namespace placement")
	assert.NotContains(t, err.Error(), "drift")
	assert.Zero(t, checks.upsertCalls, "apply start must not overwrite a stored placement block")
}
