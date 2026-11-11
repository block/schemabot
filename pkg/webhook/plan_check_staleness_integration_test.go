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
	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

const (
	planStartHeadSHA = "abc123"
	pushedHeadSHA    = "newsha456"
	// pushedHeadSummary marks the row the newer head's plan stored, so a test
	// can tell it apart from anything the older plan would write.
	pushedHeadSummary = "1 created"
)

// pushDuringPlanTernClient runs onPlan while its plan is in flight, then plans
// like namespaceSelectingTernClient, or fails when fail is set.
type pushDuringPlanTernClient struct {
	namespaceSelectingTernClient
	once   sync.Once
	onPlan func()
	fail   bool
}

func (c *pushDuringPlanTernClient) Plan(ctx context.Context, req *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	if c.onPlan != nil {
		c.once.Do(c.onPlan)
	}
	if c.fail {
		return nil, errors.New("connect to orders-001: connection refused")
	}
	resp, err := c.namespaceSelectingTernClient.Plan(ctx, req)
	if err != nil {
		return nil, err
	}
	resp.PlanId = "plan-production"
	return resp, nil
}

// pushNewerHead models a push landing while a plan of the older head runs:
// GitHub now reports the newer head, and the newer head's auto-plan has
// stored production's row on it.
func (p *namespacePlacementHarness) pushNewerHead(t *testing.T) {
	p.github.MovedHeadSHA.Store(new(pushedHeadSHA))
	// Upsert rather than require: this runs inside the tern client, on the
	// handler's goroutine, so the failure is asserted by the test body.
	if err := p.store.Checks().Upsert(t.Context(), &storage.Check{
		Repository: "octocat/hello-world", PullRequest: 1, HeadSHA: pushedHeadSHA,
		Environment: "production", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: p.dbName,
		CheckRunID: 1, Status: checkStatusCompleted, Conclusion: checkConclusionActionRequired,
		HasChanges: true, ChangeSummary: pushedHeadSummary,
	}); err != nil {
		t.Errorf("seed the newer head's production row: %v", err)
	}
}

// A plan of production starts on the PR's head, and a push lands while the
// plan runs, with the newer head's own plan storing production's row. The
// delivery's cached PR snapshot still names the older head, but the plan's
// result is checked against the head GitHub reports now, so neither a passing
// nor a failing result of the older head replaces the newer head's row. With
// no push, the same plan stores its row on the head it planned.
func TestE2EPlanResultOfAMovedHeadIsNotStored(t *testing.T) {
	targets := []api.TargetEntry{
		{Target: "orders-001", Namespaces: []string{"ns_0"}},
		{Target: "orders-002", Namespaces: []string{"ns_1"}},
	}
	for _, tc := range []struct {
		name string
		fail bool
	}{
		{name: "passing plan", fail: false},
		{name: "failing plan", fail: true},
	} {
		t.Run(tc.name+" after a push", func(t *testing.T) {
			production := &pushDuringPlanTernClient{fail: tc.fail}
			p := newMultiEnvPlanHarness(t, targets, production, nil)
			production.onPlan = func() { p.pushNewerHead(t) }

			assert.NotEmpty(t, p.comment(t, "schemabot plan -e production"))

			row := p.checksByEnv(t)["production"]
			require.NotNil(t, row, "the newer head's production row is still stored")
			assert.Equal(t, pushedHeadSHA, row.HeadSHA, "the older head's plan must not restamp the row")
			assert.Equal(t, checkConclusionActionRequired, row.Conclusion)
			assert.Equal(t, pushedHeadSummary, row.ChangeSummary)
		})
		t.Run(tc.name+" with no push", func(t *testing.T) {
			p := newMultiEnvPlanHarness(t, targets, &pushDuringPlanTernClient{fail: tc.fail}, nil)

			assert.NotEmpty(t, p.comment(t, "schemabot plan -e production"))

			row := p.checksByEnv(t)["production"]
			require.NotNil(t, row, "the plan stores production's row")
			assert.Equal(t, planStartHeadSHA, row.HeadSHA)
			if tc.fail {
				assert.Equal(t, checkConclusionFailure, row.Conclusion)
				assert.Equal(t, planFailedCheckSummary, row.ChangeSummary)
			} else {
				assert.Equal(t, checkConclusionSuccess, row.Conclusion)
			}
		})
	}
}

// Every plan writer shares one staleness decision. Within one delivery the
// PR snapshot is cached, so after a push the cached read still names the
// head the plan ran on. The decision reads the PR uncached and refuses the
// write, naming both heads, while the same write with no push is stored.
func TestPlanCheckRecordStalenessReadsTheCurrentHead(t *testing.T) {
	p := newMultiEnvPlanHarness(t, []api.TargetEntry{
		{Target: "orders-001", Namespaces: []string{"ns_0"}},
	}, &namespaceSelectingTernClient{}, nil)
	schema := &ghclient.SchemaRequestResult{HeadSHA: planStartHeadSHA, Database: p.dbName, Type: storage.DatabaseTypeMySQL}

	ctx := ghclient.WithPRInfoCache(t.Context())
	cached, err := p.client.FetchPullRequest(ctx, "octocat/hello-world", 1)
	require.NoError(t, err)
	require.Equal(t, planStartHeadSHA, cached.HeadSHA)

	headSHA, err := p.handler.storePlanCheckRecord(ctx, p.client, "octocat/hello-world", 1, schema, &apitypes.PlanResponse{}, "staging", reviewDriftOutcome{})
	require.NoError(t, err, "with no push the row is stored")
	assert.Equal(t, planStartHeadSHA, headSHA)
	require.NotNil(t, p.checksByEnv(t)["staging"])
	assert.Equal(t, planStartHeadSHA, p.checksByEnv(t)["staging"].HeadSHA)

	p.pushNewerHead(t)
	cached, err = p.client.FetchPullRequest(ctx, "octocat/hello-world", 1)
	require.NoError(t, err)
	require.Equal(t, planStartHeadSHA, cached.HeadSHA, "the delivery's cached snapshot still names the older head")

	_, err = p.handler.storePlanCheckRecord(ctx, p.client, "octocat/hello-world", 1, schema, &apitypes.PlanResponse{}, "production", reviewDriftOutcome{})
	require.ErrorContains(t, err, "plan head SHA "+planStartHeadSHA+" no longer matches current head SHA for PR "+pushedHeadSHA)
	row := p.checksByEnv(t)["production"]
	require.NotNil(t, row)
	assert.Equal(t, pushedHeadSHA, row.HeadSHA)
	assert.Equal(t, pushedHeadSummary, row.ChangeSummary)
}
