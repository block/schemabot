//go:build integration

package webhook

import (
	"database/sql"
	"html"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// A pull request plans staging and production together. Staging's single
// target plans cleanly, but production's targets entries and the schema files
// disagree on where a namespace lives, so production refuses to plan. The
// refusal must still leave a failing stored check for production, so the
// aggregate folded from stored check state blocks the merge instead of passing
// on staging's row alone.
func TestE2EMultiEnvPlanNamespacePlacementFailsAggregate(t *testing.T) {
	t.Run("declared namespace no entry selects", func(t *testing.T) {
		// Nothing would plan or apply ns_1.
		runNamespacePlacementPlan(t, []api.TargetEntry{
			{Target: "orders-001", Namespaces: []string{"ns_0"}},
		}, `declares namespaces [ns_1] that no targets entry selects`)
	})
	t.Run("primary selects a namespace the files no longer declare", func(t *testing.T) {
		// The pull request removed ns_2's directory while the primary's entry
		// still selects it.
		runNamespacePlacementPlan(t, []api.TargetEntry{
			{Target: "orders-001", Namespaces: []string{"ns_0", "ns_2"}},
			{Target: "orders-002", Namespaces: []string{"ns_1"}},
		}, `target "orders-001" selects namespace "ns_2", which the schema files do not declare`)
	})
}

// namespacePlacementHarness is a webhook handler over real stored check state
// whose production environment is routed by the given targets entries, with a
// pull request carrying ns_0 and ns_1.
type namespacePlacementHarness struct {
	dbName     string
	store      storage.Storage
	handler    *Handler
	github     *planFlowResult
	production *namespaceSelectingTernClient
}

func newNamespacePlacementHarness(t *testing.T, productionTargets []api.TargetEntry) *namespacePlacementHarness {
	t.Helper()
	dbName := "webhook_ns_placement"
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	schemabotDB, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	t.Cleanup(func() { _ = schemabotDB.Close() })
	_, err = schemabotDB.ExecContext(ctx, "DELETE FROM checks WHERE repository = 'octocat/hello-world' AND pull_request = 1")
	require.NoError(t, err)
	_, err = schemabotDB.ExecContext(ctx, "DELETE FROM plans WHERE database_name = ?", dbName)
	require.NoError(t, err)
	st := mysqlstore.New(schemabotDB)

	serverConfig := &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			dbName: {Type: storage.DatabaseTypeMySQL, Environments: map[string]api.EnvironmentConfig{
				"staging":    {Deployment: "eu", Target: "orders-staging"},
				"production": {Deployment: "eu", Targets: productionTargets},
			}},
		},
		TernDeployments: api.TernConfig{"eu": {"staging": "tern-eu-staging:9090", "production": "tern-eu:9090"}},
		Repos:           map[string]api.RepoConfig{"octocat/hello-world": {}},
	}
	production := &namespaceSelectingTernClient{}
	svc := api.New(st, serverConfig, map[string]tern.Client{
		"eu/staging":    &namespaceSelectingTernClient{},
		"eu/production": production,
	}, logger)
	t.Cleanup(func() { _ = svc.Close() })

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	table := "CREATE TABLE `orders` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
	result := setupFakeGitHubForPlan(t, mux, map[string]string{
		"ns_0/orders.sql": table,
		"ns_1/orders.sql": table,
	}, "database: "+dbName+"\ntype: mysql\n", dbName)

	return &namespacePlacementHarness{
		dbName:     dbName,
		store:      st,
		handler:    NewHandler(svc, &fakeClientFactory{client: ghclient.NewInstallationClient(client, logger)}, nil, logger),
		github:     result,
		production: production,
	}
}

// comment runs one PR comment command and waits for SchemaBot's reply comment.
func (p *namespacePlacementHarness) comment(t *testing.T, command string) string {
	t.Helper()
	rr := httptest.NewRecorder()
	p.handler.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{comment: command, isPR: true}, nil))
	require.Equal(t, http.StatusOK, rr.Code)
	select {
	case body := <-p.github.comments:
		// The comment renders a refusal HTML-escaped.
		return html.UnescapeString(body)
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatalf("timed out waiting for the reply to %q", command)
		return ""
	}
}

// checksByEnv returns the harness database's stored check rows by environment.
func (p *namespacePlacementHarness) checksByEnv(t *testing.T) map[string]*storage.Check {
	t.Helper()
	checks, err := p.store.Checks().GetByPR(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	byEnv := map[string]*storage.Check{}
	for _, c := range checks {
		if c.DatabaseName == p.dbName {
			byEnv[c.Environment] = c
		}
	}
	return byEnv
}

// lastAggregate drains the published check runs and returns the last aggregate.
func (p *namespacePlacementHarness) lastAggregate() *checkRunCapture {
	var aggregate *checkRunCapture
	for {
		select {
		case cr := <-p.github.checkRuns:
			if cr.Name == aggregateCheckName {
				aggregate = &cr
			}
			continue
		default:
		}
		return aggregate
	}
}

func runNamespacePlacementPlan(t *testing.T, productionTargets []api.TargetEntry, wantRefusal string) {
	t.Helper()
	p := newNamespacePlacementHarness(t, productionTargets)

	assert.Contains(t, p.comment(t, "schemabot plan"), wantRefusal)
	assert.Nil(t, p.production.planNamespaces, "production plans nothing while its placement is refused")

	byEnv := p.checksByEnv(t)
	require.Contains(t, byEnv, "staging")
	assert.Equal(t, checkConclusionSuccess, byEnv["staging"].Conclusion)
	require.Contains(t, byEnv, "production", "the refused environment still stores check state")
	assert.Equal(t, checkConclusionFailure, byEnv["production"].Conclusion)
	assert.Equal(t, storage.NamespacePlacementRefusedBlockingReason, byEnv["production"].BlockingReason)
	assert.Equal(t, namespacePlacementCheckSummary, byEnv["production"].ChangeSummary)
	assert.Equal(t, "abc123", byEnv["production"].HeadSHA)

	aggregate := p.lastAggregate()
	require.NotNil(t, aggregate, "the aggregate check run is published")
	assert.Equal(t, checkStatusCompleted, aggregate.Status)
	assert.Equal(t, checkConclusionFailure, aggregate.Conclusion, "staging passing alone must not pass the aggregate")
}

// A pull request's production check passed on an earlier plan, and the server
// config then moves ns_1 off every production target. A plan of production
// alone is refused by the placement, and the refusal replaces production's
// stored row rather than only failing the aggregate it posts, so the next fold
// from stored check state, from a plan of any environment, cannot pass on the
// old success row.
func TestE2ESingleEnvPlanNamespacePlacementFailsStoredCheck(t *testing.T) {
	p := newNamespacePlacementHarness(t, []api.TargetEntry{
		{Target: "orders-001", Namespaces: []string{"ns_0"}},
	})
	for _, env := range []string{"staging", "production"} {
		require.NoError(t, p.store.Checks().Upsert(t.Context(), &storage.Check{
			Repository: "octocat/hello-world", PullRequest: 1, HeadSHA: "abc123",
			Environment: env, DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: p.dbName,
			CheckRunID: 1, Status: checkStatusCompleted, Conclusion: checkConclusionSuccess,
		}))
	}

	assert.Contains(t, p.comment(t, "schemabot plan -e production"), `declares namespaces [ns_1] that no targets entry selects`)
	assert.Nil(t, p.production.planNamespaces, "production plans nothing while its placement is refused")

	byEnv := p.checksByEnv(t)
	require.Contains(t, byEnv, "production")
	assert.Equal(t, checkConclusionFailure, byEnv["production"].Conclusion, "the old success row is replaced by the refusal")
	assert.Equal(t, storage.NamespacePlacementRefusedBlockingReason, byEnv["production"].BlockingReason)
	assert.Equal(t, namespacePlacementCheckSummary, byEnv["production"].ChangeSummary)
	aggregate := p.lastAggregate()
	require.NotNil(t, aggregate, "the aggregate check run is published")
	assert.Equal(t, checkConclusionFailure, aggregate.Conclusion)

	// A plan of the other environment folds the aggregate from stored rows,
	// production's included, so it still fails.
	assert.NotEmpty(t, p.comment(t, "schemabot plan -e staging"))
	aggregate = p.lastAggregate()
	require.NotNil(t, aggregate, "the aggregate check run is published")
	assert.Equal(t, checkConclusionFailure, aggregate.Conclusion, "staging's plan must not pass the aggregate over production's refusal")
}
