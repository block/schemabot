package webhook

import (
	"context"
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// namespaceSelectingTernClient answers the primary's plan and every member's
// diff with the same engine and no changes, recording which namespaces each
// request carried.
type namespaceSelectingTernClient struct {
	tern.Client
	mu             sync.Mutex
	planNamespaces []string
	diffNamespaces []string
}

func (c *namespaceSelectingTernClient) Plan(_ context.Context, req *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.planNamespaces = slices.Sorted(maps.Keys(req.GetSchemaFiles()))
	return &ternv1.PlanResponse{PlanId: "plan-primary", Engine: ternv1.Engine_ENGINE_SPIRIT}, nil
}

func (c *namespaceSelectingTernClient) PlanDiff(_ context.Context, req *ternv1.PlanRequest) (*ternv1.PlanDiffResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.diffNamespaces = slices.Sorted(maps.Keys(req.GetSchemaFiles()))
	return &ternv1.PlanDiffResponse{Engine: ternv1.Engine_ENGINE_SPIRIT}, nil
}

func (c *namespaceSelectingTernClient) IsRemote() bool   { return true }
func (c *namespaceSelectingTernClient) Endpoint() string { return "tern-eu:9090" }

// A pull request plan on an environment whose primary selects namespaces runs
// the primary's plan and the review-time rollup the way the webhook does: the
// rollup is handed the primary member as the plan response recorded it. The
// response carries the primary's selection, so the rollup finds the same
// placement it resolves from config and the review is clean, with each member
// planned against only its own namespaces.
func TestReviewTimeDrift_SelectingPrimaryMatchesItsOwnPlan(t *testing.T) {
	client := &namespaceSelectingTernClient{}
	cfg := &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			"orders": {Type: storage.DatabaseTypeMySQL, Environments: map[string]api.EnvironmentConfig{
				"production": {Deployment: "eu", Targets: []api.TargetEntry{
					{Target: "orders-001", Namespaces: []string{"ns_0"}},
					{Target: "orders-002", Namespaces: []string{"ns_1"}},
				}},
			}},
		},
		TernDeployments: api.TernConfig{"eu": {"production": "tern-eu:9090"}},
	}
	h := &Handler{
		service: api.New(&planRetryStorage{}, cfg, map[string]tern.Client{"eu/production": client}, testLogger()),
		logger:  testLogger(),
	}
	files := &ternv1.SchemaFiles{Files: map[string]string{"orders.sql": "CREATE TABLE `orders` (id bigint primary key)"}}
	planReq := api.PlanRequest{
		Database:    "orders",
		Environment: "production",
		Type:        storage.DatabaseTypeMySQL,
		SchemaFiles: map[string]*ternv1.SchemaFiles{"ns_0": files, "ns_1": files},
	}

	planProto, planResp, err := h.executePlanProtoWithTransientRetry(t.Context(), planReq, "octocat/orders", 7)
	require.NoError(t, err)
	outcome, _ := h.reviewTimeDrift(t.Context(), planReq, planProto, plannedPrimaryMember(planResp), "octocat/orders", 7)

	assert.Equal(t, driftClean, outcome.state, "the primary's recorded selection matches the placement the rollup resolves")
	assert.Equal(t, []string{"ns_0"}, client.planNamespaces)
	assert.Equal(t, []string{"ns_1"}, client.diffNamespaces)
}
