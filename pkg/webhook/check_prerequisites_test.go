package webhook

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func prerequisiteConfig() *api.ServerConfig {
	return &api.ServerConfig{AllowedEnvironments: []string{"sandbox", "production"}, EnvironmentOrder: []string{"staging", "sandbox", "production"}, Repos: map[string]api.RepoConfig{"octocat/hello-world": {}}, Databases: map[string]api.DatabaseConfig{
		"orders": {Type: "mysql", AllowedRepos: []string{"octocat/hello-world"}, EnvironmentOrder: []string{"staging", "sandbox", "production"}, Environments: map[string]api.EnvironmentConfig{"sandbox": {}, "production": {}}},
	}}
}

// A clean production plan still needs its own database's local predecessor to
// succeed on this head. Completing that predecessor refolds production without
// starting or rewriting a production apply.
func TestProductionFoldWaitsForLocalPrerequisites(t *testing.T) {
	for _, tc := range []struct {
		name, status, conclusion, sha string
		missing, removed              bool
		applyID                       int64
	}{
		{name: "missing", missing: true},
		{name: "pending", status: checkStatusCompleted, conclusion: checkConclusionActionRequired, sha: "abc123"},
		{name: "running", status: checkStatusInProgress, sha: "abc123"},
		{name: "failed", status: checkStatusCompleted, conclusion: checkConclusionFailure, sha: "abc123"},
		{name: "stale success", status: checkStatusCompleted, conclusion: checkConclusionSuccess, sha: "old"},
		{name: "stopped", status: checkStatusInProgress, sha: "abc123", applyID: 42},
		{name: "removed registration", status: checkStatusCompleted, conclusion: checkConclusionActionRequired, sha: "abc123", removed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := prerequisiteConfig()
			if tc.removed {
				delete(cfg.Databases["orders"].Environments, "sandbox")
			}
			production := &storage.Check{Environment: "production", DatabaseType: "mysql", DatabaseName: "orders", HeadSHA: "abc123", Status: checkStatusCompleted, Conclusion: checkConclusionSuccess}
			store := &foldCheckStore{byPR: []*storage.Check{production}}
			if !tc.missing {
				store.byPR = append(store.byPR, &storage.Check{Environment: "sandbox", DatabaseType: "mysql", DatabaseName: "orders", HeadSHA: tc.sha, Status: tc.status, Conclusion: tc.conclusion, ApplyID: tc.applyID})
			}
			h, mux, client := newFoldHandler(t, cfg, store)
			serveHeadSHA(t, mux, "abc123")
			serveAggregateScopeInputs(t, mux, []string{"schemabot.yaml"}, "database: orders\ntype: mysql\n")
			outputs := map[string]ghclient.CheckRunOptions{}
			mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
				var opts ghclient.CheckRunOptions
				require.NoError(t, json.NewDecoder(r.Body).Decode(&opts))
				outputs[opts.Name] = opts
				w.WriteHeader(http.StatusCreated)
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 555}))
			})
			_, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")
			require.NoError(t, err)
			prod := outputs["SchemaBot (production)"]
			assert.NotEqual(t, checkConclusionSuccess, prod.Conclusion)
			require.NotNil(t, prod.Output)
			assert.Contains(t, prod.Output.Summary, "Waiting for sandbox: orders")
			assert.Contains(t, prod.Output.Summary, "[SchemaBot (sandbox)]")
			assert.Equal(t, checkConclusionSuccess, production.Conclusion)
			assert.Empty(t, production.BlockingReason)
			// Only the predecessor's raw result changes between folds.
			store.byPR = []*storage.Check{production, {Environment: "sandbox", DatabaseType: "mysql", DatabaseName: "orders", HeadSHA: "abc123", Status: checkStatusCompleted, Conclusion: checkConclusionSuccess}}
			_, err = h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")
			require.NoError(t, err)
			assert.Equal(t, checkConclusionSuccess, outputs["SchemaBot (production)"].Conclusion)
			assert.NotContains(t, outputs["SchemaBot (production)"].Output.Summary, "Waiting for sandbox")
		})
	}
}

func TestPrerequisitesAreDatabaseSpecific(t *testing.T) {
	cfg := prerequisiteConfig()
	cfg.Databases["payments"] = api.DatabaseConfig{Type: "mysql", Environments: map[string]api.EnvironmentConfig{"production": {}}}
	h, _, _ := newFoldHandler(t, cfg, &foldCheckStore{})
	own := &storage.Check{Environment: "production", DatabaseType: "mysql", DatabaseName: "payments", HeadSHA: "head", Status: checkStatusCompleted, Conclusion: checkConclusionSuccess}
	checks := h.withLocalPrerequisites([]*storage.Check{own}, []*storage.Check{own}, "production", "octocat/hello-world", 1, "head")
	require.Len(t, checks, 1)
	assert.Same(t, own, checks[0])
}

// The repository publishes a sandbox check because another database is
// registered there, but the database this PR changes is registered only in
// production. Its pending production apply is not a sandbox prerequisite, so the
// sandbox check reports no work instead of waiting on production.
func TestLaterEnvironmentIgnoresDatabaseNotRegisteredThere(t *testing.T) {
	cfg := prerequisiteConfig()
	cfg.EnvironmentOrder = []string{"staging", "production"}
	cfg.Databases["payments"] = api.DatabaseConfig{Type: "mysql", AllowedRepos: []string{"octocat/hello-world"}, Environments: map[string]api.EnvironmentConfig{"production": {}}}
	production := &storage.Check{Environment: "production", DatabaseType: "mysql", DatabaseName: "payments", HeadSHA: "abc123", Status: checkStatusCompleted, Conclusion: checkConclusionActionRequired}
	store := &foldCheckStore{byPR: []*storage.Check{production}}
	h, mux, client := newFoldHandler(t, cfg, store)
	serveHeadSHA(t, mux, "abc123")
	serveAggregateScopeInputs(t, mux, []string{"schemabot.yaml"}, "database: payments\ntype: mysql\n")
	outputs := map[string]ghclient.CheckRunOptions{}
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var opts ghclient.CheckRunOptions
		require.NoError(t, json.NewDecoder(r.Body).Decode(&opts))
		outputs[opts.Name] = opts
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 555}))
	})
	_, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")
	require.NoError(t, err)
	sandbox := outputs["SchemaBot (sandbox)"]
	require.NotNil(t, sandbox.Output)
	assert.Equal(t, checkConclusionSuccess, sandbox.Conclusion)
	assert.NotContains(t, sandbox.Output.Summary, "Waiting for production")
	assert.Equal(t, checkConclusionActionRequired, outputs["SchemaBot (production)"].Conclusion)
}

// A database that stays configured but loses its registration in the later
// environment still carries a retained result there. The repository keeps
// publishing that environment for another database, and the retained result
// keeps waiting on its own unfinished predecessor instead of passing on its own.
func TestRetainedResultKeepsPrerequisitesAfterRemovedDestinationRegistration(t *testing.T) {
	cfg := prerequisiteConfig()
	delete(cfg.Databases["orders"].Environments, "production")
	cfg.Databases["payments"] = api.DatabaseConfig{Type: "mysql", AllowedRepos: []string{"octocat/hello-world"}, Environments: map[string]api.EnvironmentConfig{"production": {}}}
	production := &storage.Check{Environment: "production", DatabaseType: "mysql", DatabaseName: "orders", HeadSHA: "abc123", Status: checkStatusCompleted, Conclusion: checkConclusionSuccess}
	sandbox := &storage.Check{Environment: "sandbox", DatabaseType: "mysql", DatabaseName: "orders", HeadSHA: "abc123", Status: checkStatusCompleted, Conclusion: checkConclusionActionRequired}
	store := &foldCheckStore{byPR: []*storage.Check{production, sandbox}}
	h, mux, client := newFoldHandler(t, cfg, store)
	serveHeadSHA(t, mux, "abc123")
	serveAggregateScopeInputs(t, mux, []string{"schemabot.yaml"}, "database: orders\ntype: mysql\n")
	outputs := map[string]ghclient.CheckRunOptions{}
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var opts ghclient.CheckRunOptions
		require.NoError(t, json.NewDecoder(r.Body).Decode(&opts))
		outputs[opts.Name] = opts
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 555}))
	})
	_, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")
	require.NoError(t, err)
	prod := outputs["SchemaBot (production)"]
	require.NotNil(t, prod.Output)
	assert.NotEqual(t, checkConclusionSuccess, prod.Conclusion)
	assert.Contains(t, prod.Output.Summary, "Waiting for sandbox: orders")
	assert.Equal(t, checkConclusionActionRequired, outputs["SchemaBot (sandbox)"].Conclusion)
}

// Removing both the registration and its promotion order cannot make retained
// unfinished work disappear from a later environment's existing required gate.
func TestRetainedPrerequisiteSurvivesRemovedDatabase(t *testing.T) {
	cfg := prerequisiteConfig()
	delete(cfg.Databases, "orders")
	production := &storage.Check{Environment: "production", DatabaseType: "mysql", DatabaseName: "orders", HeadSHA: "abc123", Status: checkStatusCompleted, Conclusion: checkConclusionSuccess}
	sandbox := &storage.Check{Environment: "sandbox", DatabaseType: "mysql", DatabaseName: "orders", HeadSHA: "abc123", Status: checkStatusInProgress}
	store := &foldCheckStore{byPR: []*storage.Check{production, sandbox}}
	h, mux, client := newFoldHandler(t, cfg, store)
	serveHeadSHA(t, mux, "abc123")
	serveAggregateScopeInputs(t, mux, []string{"README.md"}, "")
	serveCheckRunCreate(t, mux)
	_, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")
	require.NoError(t, err)
	require.Len(t, store.upserted, 2)
	for _, aggregate := range store.upserted {
		assert.NotEqual(t, checkConclusionSuccess, aggregate.Conclusion)
	}
}

// A leader keeps the participant's production result separate while requiring
// its declared earlier environment to succeed through the trusted App contract.
func TestLeaderProductionIncludesParticipantPrerequisites(t *testing.T) {
	cfg := prerequisiteConfig()
	cfg.Databases = nil
	cfg.Repos["octocat/hello-world"] = api.RepoConfig{Aggregate: &api.AggregateConfig{Role: api.AggregateRoleLeader, ExpectedTenants: []api.ExpectedTenant{{Tenant: "tenant-a", Paths: []string{"services/a"}, CheckName: "SchemaBot A", Environments: []string{"sandbox", "production"}}}}}
	store := &foldCheckStore{}
	h, mux, client := newFoldHandler(t, cfg, store)
	serveHeadSHA(t, mux, "abc123")
	serveAggregateScopeInputs(t, mux, []string{"services/a/README.md"}, "")
	sandboxConclusion := checkConclusionActionRequired
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("check_name")
		conclusion := checkConclusionSuccess
		if name == "SchemaBot A (sandbox)" {
			conclusion = sandboxConclusion
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"total_count": 1, "check_runs": []map[string]any{{"id": 1, "name": name, "head_sha": "abc123", "status": checkStatusCompleted, "conclusion": conclusion, "app": map[string]any{"slug": "schemabot"}}}}))
	})
	outputs := map[string]ghclient.CheckRunOptions{}
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var opts ghclient.CheckRunOptions
		require.NoError(t, json.NewDecoder(r.Body).Decode(&opts))
		outputs[opts.Name] = opts
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 555}))
	})
	_, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")
	require.NoError(t, err)
	assert.Equal(t, checkConclusionActionRequired, outputs["SchemaBot (production)"].Conclusion)
	assert.Contains(t, outputs["SchemaBot (production)"].Output.Summary, "[SchemaBot A (sandbox)]")
	sandboxConclusion = checkConclusionSuccess
	_, err = h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")
	require.NoError(t, err)
	assert.Equal(t, checkConclusionSuccess, outputs["SchemaBot (production)"].Conclusion)
}

func TestRetainedPrerequisiteWithoutLaterResult(t *testing.T) {
	cfg := prerequisiteConfig()
	store := &foldCheckStore{byPR: []*storage.Check{{Environment: "sandbox", DatabaseType: "mysql", DatabaseName: "orders", HeadSHA: "abc123", Status: checkStatusCompleted, Conclusion: checkConclusionActionRequired}}}
	h, mux, client := newFoldHandler(t, cfg, store)
	serveHeadSHA(t, mux, "abc123")
	serveAggregateScopeInputs(t, mux, []string{"README.md"}, "")
	serveCheckRunCreate(t, mux)
	_, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")
	require.NoError(t, err)
	require.Len(t, store.upserted, 2)
	for _, aggregate := range store.upserted {
		assert.NotEqual(t, checkConclusionSuccess, aggregate.Conclusion)
	}
}

func TestMergeGroupPublishesOnlyApplicableEnvironmentNames(t *testing.T) {
	cfg := prerequisiteConfig()
	delete(cfg.Databases["orders"].Environments, "sandbox")
	h, mux, client := newFoldHandler(t, cfg, &foldCheckStore{})
	serveParticipantCheckRunsEmpty(t, mux, "abc123")
	var names []string
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var opts ghclient.CheckRunOptions
		require.NoError(t, json.NewDecoder(r.Body).Decode(&opts))
		names = append(names, opts.Name)
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 555}))
	})
	require.NoError(t, h.postPassingAggregateChecks(t.Context(), client, "octocat/hello-world", "abc123", mergeGroupCheckContent()))
	assert.Equal(t, []string{"SchemaBot (production)"}, names)
}

func TestPrerequisiteSummaryRespectsGitHubTextLimit(t *testing.T) {
	var checks []*storage.Check
	for range 2000 {
		checks = append(checks, &storage.Check{Environment: "sandbox", DatabaseName: strings.Repeat("orders", 20), BlockingReason: prerequisiteContribution})
	}
	notice := prerequisiteSummary(checks, "SchemaBot", "org/repo", "head")
	assert.LessOrEqual(t, len(notice), maxCheckRunTextLength/3)
	assert.Contains(t, notice, "More prerequisites remain")
	summary := appendPrerequisiteSummary(strings.Repeat("| orders | success |\n", 4000), notice)
	assert.LessOrEqual(t, len(summary), maxCheckRunTextLength)
	assert.Contains(t, summary, "More database results omitted.")
	assert.Contains(t, summary, "Waiting for sandbox")
}

// An incomplete order cannot turn a clean later plan into a passing check,
// whether the missing environment belongs to a local database or a participant.
func TestAggregateRejectsIncompletePromotionOrder(t *testing.T) {
	for _, scenario := range []string{"local target", "local predecessor", "participant predecessor", "participant target"} {
		t.Run(scenario, func(t *testing.T) {
			cfg := prerequisiteConfig()
			db := cfg.Databases["orders"]
			db.EnvironmentOrder = nil
			cfg.Databases["orders"] = db
			files := []string{"schemabot.yaml"}
			yaml := "database: orders\ntype: mysql\n"
			switch scenario {
			case "local target":
				cfg.EnvironmentOrder = []string{"sandbox"}
			case "local predecessor":
				cfg.EnvironmentOrder = []string{"production"}
			default:
				cfg.Databases = nil
				cfg.Repos["octocat/hello-world"] = api.RepoConfig{Aggregate: &api.AggregateConfig{Role: api.AggregateRoleLeader, ExpectedTenants: []api.ExpectedTenant{{Tenant: "a", Paths: []string{"schema"}, CheckName: "SchemaBot A", Environments: []string{"sandbox", "production"}}}}}
				files, yaml = []string{"schema/README.md"}, ""
				if scenario == "participant predecessor" {
					cfg.EnvironmentOrder = nil
				} else {
					cfg.EnvironmentOrder = []string{"sandbox"}
				}
			}
			store := &foldCheckStore{byPR: []*storage.Check{
				{Environment: "sandbox", DatabaseType: "mysql", DatabaseName: "orders", HeadSHA: "abc123", Status: checkStatusCompleted, Conclusion: checkConclusionActionRequired},
				{Environment: "production", DatabaseType: "mysql", DatabaseName: "orders", HeadSHA: "abc123", Status: checkStatusCompleted, Conclusion: checkConclusionSuccess},
			}}
			h, mux, client := newFoldHandler(t, cfg, store)
			serveHeadSHA(t, mux, "abc123")
			serveAggregateScopeInputs(t, mux, files, yaml)
			serveCheckRunCreate(t, mux)
			_, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")
			require.ErrorContains(t, err, "missing from the effective environment_order")
			require.Len(t, store.upserted, 2)
			for _, row := range store.upserted {
				assert.Equal(t, checkConclusionFailure, row.Conclusion)
			}
		})
	}
}
