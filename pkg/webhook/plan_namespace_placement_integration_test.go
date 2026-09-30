//go:build integration

package webhook

import (
	"database/sql"
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

func runNamespacePlacementPlan(t *testing.T, productionTargets []api.TargetEntry, wantRefusal string) {
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

	h := NewHandler(svc, &fakeClientFactory{client: ghclient.NewInstallationClient(client, logger)}, nil, logger)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{comment: "schemabot plan", isPR: true}, nil))
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "multi-env plan started")

	select {
	case body := <-result.comments:
		assert.Contains(t, body, wantRefusal)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for multi-env plan comment")
	}
	assert.Nil(t, production.planNamespaces, "production plans nothing while its placement is refused")

	checks, err := st.Checks().GetByPR(ctx, "octocat/hello-world", 1)
	require.NoError(t, err)
	byEnv := map[string]*storage.Check{}
	for _, c := range checks {
		if c.DatabaseName == dbName {
			byEnv[c.Environment] = c
		}
	}
	require.Contains(t, byEnv, "staging")
	assert.Equal(t, checkConclusionSuccess, byEnv["staging"].Conclusion)
	require.Contains(t, byEnv, "production", "the refused environment still stores check state")
	assert.Equal(t, checkConclusionFailure, byEnv["production"].Conclusion)
	assert.Equal(t, namespacePlacementCheckSummary, byEnv["production"].ChangeSummary)
	assert.Equal(t, "abc123", byEnv["production"].HeadSHA)

	var aggregate *checkRunCapture
	for {
		select {
		case cr := <-result.checkRuns:
			if cr.Name == aggregateCheckName {
				aggregate = &cr
			}
			continue
		default:
		}
		break
	}
	require.NotNil(t, aggregate, "the aggregate check run is published")
	assert.Equal(t, checkStatusCompleted, aggregate.Status)
	assert.Equal(t, checkConclusionFailure, aggregate.Conclusion, "staging passing alone must not pass the aggregate")
}
