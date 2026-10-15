//go:build integration

package webhook

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

// planFailingTernClient fails its first failPlans plans with a non-transient
// error naming an internal address, then plans like namespaceSelectingTernClient
// under a plan ID of its own.
type planFailingTernClient struct {
	namespaceSelectingTernClient
	failMu    sync.Mutex
	failPlans int
}

func (c *planFailingTernClient) Plan(ctx context.Context, req *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	c.failMu.Lock()
	failing := c.failPlans > 0
	if failing {
		c.failPlans--
	}
	c.failMu.Unlock()
	if failing {
		return nil, errors.New("connect to orders-001 at 10.0.0.5:3306: connection refused")
	}
	resp, err := c.namespaceSelectingTernClient.Plan(ctx, req)
	if err != nil {
		return nil, err
	}
	// A plan ID of its own, so production's plan does not collide with the
	// one staging stored.
	resp.PlanId = "plan-production"
	return resp, nil
}

// A pull request plans staging and production together. Staging plans with no
// changes, but production's plan fails outright. Production still stores a
// failing check row, so the aggregate folded from stored check state blocks the
// merge instead of passing on staging's row alone, a later plan of staging
// alone still fails it, and the stored row and aggregate never show the raw
// error. Once production plans cleanly, its row passes and so does the
// aggregate.
func TestE2EMultiEnvPlanEnvironmentErrorFailsAggregate(t *testing.T) {
	production := &planFailingTernClient{failPlans: 1}
	p := newMultiEnvPlanHarness(t, []api.TargetEntry{
		{Target: "orders-001", Namespaces: []string{"ns_0"}},
		{Target: "orders-002", Namespaces: []string{"ns_1"}},
	}, production, nil)

	assert.NotEmpty(t, p.comment(t, "schemabot plan"))

	aggregate := p.lastAggregate()
	require.NotNil(t, aggregate, "the aggregate check run is published")
	assert.Equal(t, checkConclusionFailure, aggregate.Conclusion, "staging passing alone must not pass the aggregate")
	require.NotNil(t, aggregate.Output)
	assert.NotContains(t, aggregate.Output.Summary, "10.0.0.5")

	byEnv := p.checksByEnv(t)
	require.Contains(t, byEnv, "staging")
	assert.Equal(t, checkConclusionSuccess, byEnv["staging"].Conclusion)
	require.Contains(t, byEnv, "production", "the environment that failed to plan still stores check state")
	assert.Equal(t, checkConclusionFailure, byEnv["production"].Conclusion)
	assert.Equal(t, "abc123", byEnv["production"].HeadSHA)
	assert.Empty(t, byEnv["production"].BlockingReason, "a plan failure is a plan result, not a block")
	assert.NotContains(t, byEnv["production"].ChangeSummary, "10.0.0.5")

	// A plan of staging alone folds production's failing row too.
	assert.NotEmpty(t, p.comment(t, "schemabot plan -e staging"))
	aggregate = p.lastAggregate()
	require.NotNil(t, aggregate, "the aggregate check run is published")
	assert.Equal(t, checkConclusionFailure, aggregate.Conclusion, "staging's plan must not pass the aggregate over production's failed plan")

	// Production now plans cleanly, which replaces its failing row.
	assert.NotEmpty(t, p.comment(t, "schemabot plan -e production"))
	byEnv = p.checksByEnv(t)
	require.Contains(t, byEnv, "production")
	assert.Equal(t, checkConclusionSuccess, byEnv["production"].Conclusion)
	aggregate = p.lastAggregate()
	require.NotNil(t, aggregate, "the aggregate check run is published")
	assert.Equal(t, checkConclusionSuccess, aggregate.Conclusion)
}
