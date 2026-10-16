//go:build integration

package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/checkstate"
	ghclient "github.com/block/schemabot/pkg/github"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
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
	assert.Equal(t, planFailedCheckSummary, byEnv["production"].ChangeSummary, "the aggregate's row points at the plan comment, never at the raw error")
	assert.Contains(t, aggregate.Output.Summary, planFailedCheckSummary)

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

// A pull request plans staging and production together. Staging plans with no
// changes and stores its passing row, but production's plan fails outright and
// the failing row recording that cannot be stored, so stored check state holds
// nothing of production. The failing aggregate posted in its place is posted
// before the fold and carries a block, so the plan never publishes a passing
// aggregate, not even for a moment before the failure lands. A later plan of
// staging alone, whose fold would pass on staging's row, leaves the blocked
// aggregate failing too, since production has still not planned.
func TestE2EUnstoredMultiEnvPlanFailureHoldsAggregate(t *testing.T) {
	p := newMultiEnvPlanHarness(t, []api.TargetEntry{
		{Target: "orders-001", Namespaces: []string{"ns_0"}},
		{Target: "orders-002", Namespaces: []string{"ns_1"}},
	}, &planFailingTernClient{failPlans: 1}, func(st storage.Storage) storage.Storage {
		return &envPlanResultFailingStorage{Storage: st, env: "production"}
	})

	assert.NotEmpty(t, p.comment(t, "schemabot plan"))

	aggregates := p.publishedAggregates()
	require.NotEmpty(t, aggregates, "the aggregate check run is published")
	assertNoPassingAggregate(t, aggregates, "production's unrecorded plan failure")
	last := aggregates[len(aggregates)-1]
	assert.Equal(t, checkConclusionFailure, last.Conclusion)
	require.NotNil(t, last.Output)
	assert.Equal(t, planFailedUnstoredSummary, last.Output.Summary)
	byEnv := p.checksByEnv(t)
	assert.NotContains(t, byEnv, "production", "production's failing row could not be stored")
	p.assertAggregateBlocked(t, checkstate.BlockPlanFailedUnrecorded)

	// Staging plans and stores its row again, and its fold would pass on it;
	// the stored block holds the aggregate instead.
	assert.NotEmpty(t, p.comment(t, "schemabot plan -e staging"))
	assertNoPassingAggregate(t, p.publishedAggregates(), "staging's plan over production's unrecorded plan failure")
	p.assertAggregateBlocked(t, checkstate.BlockPlanFailedUnrecorded)
}

// A pull request plans staging and production together. Staging plans with no
// changes, production's plan fails, and storage refuses both production's
// failing row and the failing aggregate that would carry the block in its
// place. Nothing stored then records production's failure, so the plan does not
// fold the aggregate from stored check state at all: the failing aggregate it
// did publish is the last word, and no passing aggregate follows it.
func TestE2EUnrecordedMultiEnvPlanFailureSkipsFold(t *testing.T) {
	p := newMultiEnvPlanHarness(t, []api.TargetEntry{
		{Target: "orders-001", Namespaces: []string{"ns_0"}},
		{Target: "orders-002", Namespaces: []string{"ns_1"}},
	}, &planFailingTernClient{failPlans: 1}, func(st storage.Storage) storage.Storage {
		return &aggregateUpsertFailingStorage{Storage: &envPlanResultFailingStorage{Storage: st, env: "production"}}
	})

	assert.NotEmpty(t, p.comment(t, "schemabot plan"))

	aggregates := p.publishedAggregates()
	require.NotEmpty(t, aggregates, "the failing aggregate check run is published")
	assertNoPassingAggregate(t, aggregates, "production's unrecorded plan failure")
	assert.Equal(t, checkConclusionFailure, aggregates[len(aggregates)-1].Conclusion)
}

// A pull request plans staging and production together, but production's
// schema request fails: the repository's config ignores the only namespace
// production would plan, so it has no schema files to plan. Staging plans
// with no changes. Production still stores a failing check row, so the
// aggregate blocks the merge instead of passing on staging's row alone.
func TestE2EMultiEnvSchemaRequestErrorFailsAggregate(t *testing.T) {
	table := "CREATE TABLE `orders` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
	p := newMultiEnvPlanHarnessWithRepo(t, []api.TargetEntry{
		{Target: "orders-001", Namespaces: []string{"ns_production"}},
	}, &namespaceSelectingTernClient{}, nil, map[string]string{
		"ns_{env}/orders.sql": table,
	}, "ignore_namespaces:\n  - ns_production\n")

	assert.Contains(t, p.comment(t, "schemabot plan"), "no schema files found")

	byEnv := p.checksByEnv(t)
	require.Contains(t, byEnv, "staging")
	assert.Equal(t, checkConclusionSuccess, byEnv["staging"].Conclusion)
	require.Contains(t, byEnv, "production", "the environment whose schema request failed still stores check state")
	assert.Equal(t, checkConclusionFailure, byEnv["production"].Conclusion)
	assert.Equal(t, planFailedCheckSummary, byEnv["production"].ChangeSummary)
	aggregate := p.lastAggregate()
	require.NotNil(t, aggregate, "the aggregate check run is published")
	assert.Equal(t, checkConclusionFailure, aggregate.Conclusion, "staging passing alone must not pass the aggregate")
}

// A deployment publishes one aggregate per allowed environment. A pull
// request plans staging and production together; staging plans with no
// changes, production's plan fails, and the row recording that cannot be
// stored. Production's aggregate carries the plan_failed_unrecorded block
// while staging's aggregate passes on staging's own row, and a later plan of
// staging alone leaves production's aggregate blocked.
func TestE2EUnstoredPlanFailureBlocksOnlyItsEnvironmentAggregate(t *testing.T) {
	p := newMultiEnvPlanHarness(t, []api.TargetEntry{
		{Target: "orders-001", Namespaces: []string{"ns_0"}},
		{Target: "orders-002", Namespaces: []string{"ns_1"}},
	}, &planFailingTernClient{failPlans: 1}, func(st storage.Storage) storage.Storage {
		return &envPlanResultFailingStorage{Storage: st, env: "production"}
	})
	p.handler.service.Config().AllowedEnvironments = []string{"staging", "production"}

	assert.NotEmpty(t, p.comment(t, "schemabot plan"))
	p.assertEnvAggregate(t, "production", checkConclusionFailure, checkstate.BlockPlanFailedUnrecorded)
	p.assertEnvAggregate(t, "staging", checkConclusionSuccess, "")

	assert.NotEmpty(t, p.comment(t, "schemabot plan -e staging"))
	p.assertEnvAggregate(t, "production", checkConclusionFailure, checkstate.BlockPlanFailedUnrecorded)
	p.assertEnvAggregate(t, "staging", checkConclusionSuccess, "")
}

// postFailingAggregatesWithBlock reports false whenever the blocked aggregate
// it set out to post is not both published and stored: a caller about to fold
// the aggregate from stored check state reads false as "do not fold". It
// reports true only once the block is in place.
func TestPostFailingAggregatesWithBlockReportsWhetherTheBlockLanded(t *testing.T) {
	disabled := false
	tests := []struct {
		name         string
		headSHA      string
		createStatus int
		repos        map[string]api.RepoConfig
		want         bool
	}{
		{name: "posted and stored", headSHA: "abc123", createStatus: http.StatusCreated, want: true},
		{name: "check publishing disabled", headSHA: "abc123", createStatus: http.StatusCreated,
			repos: map[string]api.RepoConfig{"octocat/hello-world": {EnableChecks: &disabled}}},
		{name: "head moved", headSHA: "newsha456", createStatus: http.StatusCreated},
		{name: "check run not created", headSHA: "abc123", createStatus: http.StatusUnprocessableEntity},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := setupE2EServiceWithConfig(t, &api.ServerConfig{Repos: tt.repos})
			mux := http.NewServeMux()
			server := httptest.NewServer(mux)
			t.Cleanup(server.Close)
			mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(gh.PullRequest{
					Head: &gh.PullRequestBranch{Ref: new("feature-branch"), SHA: new(tt.headSHA)},
					Base: &gh.PullRequestBranch{Ref: new("main"), SHA: new("def456")},
					User: &gh.User{Login: new("testuser")},
				})
			})
			mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.createStatus)
				_ = json.NewEncoder(w).Encode(map[string]any{"id": 300})
			})
			client := gh.NewClient(nil)
			client.BaseURL, _ = url.Parse(server.URL + "/")
			h := newE2EHandler(t, svc, client)
			installClient := ghclient.NewInstallationClient(client, h.logger)

			got := h.postFailingAggregatesWithBlock(t.Context(), installClient, "octocat/hello-world", 1, "abc123",
				map[string]string{"production": planFailedUnstoredSummary}, planFailedUnrecordedBlock)
			assert.Equal(t, tt.want, got)

			stored, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, aggregateSentinel, aggregateSentinel, aggregateSentinel)
			require.NoError(t, err)
			if tt.want {
				require.NotNil(t, stored, "the blocked aggregate is stored")
				assert.Equal(t, checkstate.BlockPlanFailedUnrecorded, stored.BlockingReason)
			} else {
				assert.Nil(t, stored, "no blocked aggregate is stored")
			}
		})
	}
}

// assertEnvAggregate asserts the stored per-environment aggregate row's
// conclusion and blocking reason.
func (p *namespacePlacementHarness) assertEnvAggregate(t *testing.T, env, conclusion, reason string) {
	t.Helper()
	stored, err := p.store.Checks().Get(t.Context(), "octocat/hello-world", 1, env, aggregateSentinel, aggregateSentinel)
	require.NoError(t, err)
	require.NotNil(t, stored, "the %s aggregate is stored", env)
	assert.Equal(t, conclusion, stored.Conclusion, "%s aggregate conclusion", env)
	assert.Equal(t, reason, stored.BlockingReason, "%s aggregate blocking reason", env)
}

// aggregateUpsertFailingStorage fails every write of a stored aggregate row and
// lets every other write through.
type aggregateUpsertFailingStorage struct {
	storage.Storage
}

func (s *aggregateUpsertFailingStorage) Checks() storage.CheckStore {
	return &aggregateUpsertFailingCheckStore{CheckStore: s.Storage.Checks()}
}

type aggregateUpsertFailingCheckStore struct {
	storage.CheckStore
}

func (s *aggregateUpsertFailingCheckStore) Upsert(ctx context.Context, check *storage.Check) error {
	if checkstate.IsAggregate(check) {
		return errors.New("store aggregate check: injected failure")
	}
	return s.CheckStore.Upsert(ctx, check)
}

// publishedAggregates drains the published check runs and returns every
// aggregate among them, in publication order.
func (p *namespacePlacementHarness) publishedAggregates() []checkRunCapture {
	var aggregates []checkRunCapture
	for {
		select {
		case cr := <-p.github.checkRuns:
			if cr.Name == aggregateCheckName {
				aggregates = append(aggregates, cr)
			}
			continue
		default:
		}
		return aggregates
	}
}

// assertAggregateBlocked asserts the stored aggregate row fails with the given
// blocking reason.
func (p *namespacePlacementHarness) assertAggregateBlocked(t *testing.T, reason string) {
	t.Helper()
	stored, err := p.store.Checks().Get(t.Context(), "octocat/hello-world", 1, aggregateSentinel, aggregateSentinel, aggregateSentinel)
	require.NoError(t, err)
	require.NotNil(t, stored, "the failing aggregate is stored")
	assert.Equal(t, reason, stored.BlockingReason)
	assert.Equal(t, checkConclusionFailure, stored.Conclusion)
}

// assertNoPassingAggregate asserts no aggregate in a publication sequence
// passed.
func assertNoPassingAggregate(t *testing.T, aggregates []checkRunCapture, over string) {
	t.Helper()
	for i, aggregate := range aggregates {
		assert.NotEqual(t, checkConclusionSuccess, aggregate.Conclusion,
			"aggregate %d of %d passed over %s", i+1, len(aggregates), over)
	}
}
