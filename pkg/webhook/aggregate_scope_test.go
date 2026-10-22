package webhook

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/storage"
)

// serveAggregateScopeInputs exposes a changed root config and distinct head/base
// trees, so the real GitHub discovery client verifies the PR's own proposal.
func serveAggregateScopeInputs(t *testing.T, mux *http.ServeMux, paths []string, schemaConfig string) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		files := make([]map[string]string, 0, len(paths))
		for _, path := range paths {
			files = append(files, map[string]string{"filename": path, "status": "modified"})
		}
		require.NoError(t, json.NewEncoder(w).Encode(files))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]string{"default_branch": "main"}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/ref/heads/main", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"object": map[string]string{"sha": "base"}}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/", func(w http.ResponseWriter, req *http.Request) {
		entries := []map[string]string{}
		if strings.HasSuffix(req.URL.Path, "/abc123") {
			entries = append(entries, map[string]string{"path": "schemabot.yaml", "type": "blob", "sha": "config-head"})
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"tree": entries, "truncated": false}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schemabot.yaml", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]string{
			"type": "file", "encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte(schemaConfig)),
		}))
	})
}

// Publication follows repository registrations. Applicable work without a
// current result stays blocking, including before the planner stores a row.
func TestAggregateApplicableEnvironmentScope(t *testing.T) {
	for _, tc := range []struct {
		name         string
		served       []string
		databaseEnvs []string
		row          *storage.Check
		docs         bool
		blocked      string
	}{
		{name: "production only, planner pending", served: []string{"sandbox", "production"}, databaseEnvs: []string{"production"}, blocked: "production"},
		{name: "production only, plan succeeded", served: []string{"sandbox", "production"}, databaseEnvs: []string{"production"}, row: &storage.Check{HeadSHA: "abc123", Status: checkStatusCompleted, Conclusion: checkConclusionSuccess}},
		{name: "production only, stale plan", served: []string{"sandbox", "production"}, databaseEnvs: []string{"production"}, row: &storage.Check{HeadSHA: "old", Status: checkStatusCompleted, Conclusion: checkConclusionSuccess}, blocked: "production"},
		{name: "staging only, three required names", served: []string{"staging", "sandbox", "production"}, databaseEnvs: []string{"staging"}, blocked: "staging"},
		{name: "known database only on another publisher", served: []string{"sandbox", "production"}, databaseEnvs: []string{"staging"}},
		{name: "documentation", served: []string{"sandbox", "production"}, databaseEnvs: []string{"production"}, docs: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			environments := map[string]api.EnvironmentConfig{}
			for _, env := range tc.databaseEnvs {
				environments[env] = api.EnvironmentConfig{}
			}
			cfg := &api.ServerConfig{AllowedEnvironments: tc.served, Repos: map[string]api.RepoConfig{"octocat/hello-world": {}}, Databases: map[string]api.DatabaseConfig{"orders": {Type: "mysql", Environments: environments}}}
			store := &foldCheckStore{}
			if tc.row != nil {
				row := *tc.row
				row.DatabaseName = "orders"
				row.DatabaseType = "mysql"
				row.Environment = tc.databaseEnvs[0]
				store.byPR = []*storage.Check{&row}
			}
			h, mux, client := newFoldHandler(t, cfg, store)
			serveHeadSHA(t, mux, "abc123")
			paths := []string{"schemabot.yaml"}
			if tc.docs {
				paths = []string{"README.md"}
			}
			serveAggregateScopeInputs(t, mux, paths, "database: orders\ntype: mysql\n")
			serveCheckRunCreate(t, mux)
			_, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")
			require.NoError(t, err)
			expectedEnvironments := []string{}
			for _, env := range tc.served {
				for _, registered := range tc.databaseEnvs {
					if env == registered {
						expectedEnvironments = append(expectedEnvironments, env)
					}
				}
			}
			require.Len(t, store.upserted, len(expectedEnvironments))
			published := []string{}
			for _, check := range store.upserted {
				published = append(published, check.Environment)
			}
			assert.ElementsMatch(t, expectedEnvironments, published)
			for _, check := range store.upserted {
				assert.Equal(t, "abc123", check.HeadSHA)
				if check.Environment == tc.blocked {
					assert.Equal(t, checkStatusInProgress, check.Status)
					assert.Empty(t, check.Conclusion)
				} else {
					assert.Equal(t, checkStatusCompleted, check.Status)
					assert.Equal(t, checkConclusionSuccess, check.Conclusion)
					assert.False(t, check.HasChanges)
				}
			}
		})
	}
}

// Participant path selection and environment coverage are independent: a
// production-only participant never contributes a sandbox read, while a selected
// sandbox participant with no result holds the sandbox aggregate open.
func TestAggregateParticipantEnvironmentScope(t *testing.T) {
	for _, tc := range []struct {
		name, path         string
		wantSandboxBlocked bool
	}{
		{name: "production participant only", path: "services/a/README.md"},
		{name: "sandbox participant selected", path: "services/b/README.md", wantSandboxBlocked: true},
		{name: "neither selected", path: "README.md"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &api.ServerConfig{AllowedEnvironments: []string{"sandbox", "production"}, Repos: map[string]api.RepoConfig{"octocat/hello-world": {Aggregate: &api.AggregateConfig{Role: api.AggregateRoleLeader, ExpectedTenants: []api.ExpectedTenant{
				{Tenant: "a", Paths: []string{"services/a"}, CheckName: "SchemaBot A", Environments: []string{"production"}},
				{Tenant: "b", Paths: []string{"services/b"}, CheckName: "SchemaBot B", Environments: []string{"sandbox", "production"}},
			}}}}}
			store := &foldCheckStore{}
			h, mux, client := newFoldHandler(t, cfg, store)
			serveHeadSHA(t, mux, "abc123")
			serveAggregateScopeInputs(t, mux, []string{tc.path}, "")
			reads := []string{}
			mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, req *http.Request) {
				reads = append(reads, req.URL.Query().Get("check_name"))
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"total_count": 0, "check_runs": []any{}}))
			})
			serveCheckRunCreate(t, mux)
			_, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")
			require.NoError(t, err)
			require.Len(t, store.upserted, 2)
			assert.NotContains(t, reads, "SchemaBot A (sandbox)")
			if tc.wantSandboxBlocked {
				assert.Contains(t, reads, "SchemaBot B (sandbox)")
				assert.Equal(t, checkStatusInProgress, store.upserted[0].Status)
			} else {
				assert.Equal(t, checkConclusionSuccess, store.upserted[0].Conclusion)
			}
		})
	}
}

func TestAggregateScopeKeepsRetainedBlockers(t *testing.T) {
	for _, tc := range []struct {
		name             string
		store            *foldCheckStore
		discoveryFailure bool
		wantError        bool
	}{
		{name: "stored apply blocker", store: &foldCheckStore{byPR: []*storage.Check{{Environment: "sandbox", DatabaseType: "mysql", DatabaseName: "orders", ApplyID: 42, Status: checkStatusInProgress}}}},
		{name: "aggregate guard blocker", store: &foldCheckStore{get: &storage.Check{BlockingReason: "config_discovery_failed"}}},
		{name: "state read failed", store: &foldCheckStore{byPRErr: assert.AnError}, wantError: true},
		{name: "discovery failed", store: &foldCheckStore{}, discoveryFailure: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &api.ServerConfig{AllowedEnvironments: []string{"sandbox"}, Repos: map[string]api.RepoConfig{"octocat/hello-world": {}}}
			h, mux, client := newFoldHandler(t, cfg, tc.store)
			serveHeadSHA(t, mux, "abc123")
			if tc.discoveryFailure {
				mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "unavailable", http.StatusForbidden) })
			} else {
				serveAggregateScopeInputs(t, mux, []string{"README.md"}, "")
			}
			serveCheckRunCreate(t, mux)
			_, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			for _, check := range tc.store.upserted {
				assert.NotEqual(t, checkConclusionSuccess, check.Conclusion)
			}
		})
	}
}

func TestAggregateScopePendingDatabaseResults(t *testing.T) {
	scope := &aggregatePRScope{local: []*storage.Check{
		{Environment: "production", DatabaseType: "mysql", DatabaseName: "orders"},
		{Environment: "production", DatabaseType: "mysql", DatabaseName: "payments"},
	}}
	checks := scope.withPendingChecks([]*storage.Check{{Environment: "production", DatabaseType: "mysql", DatabaseName: "orders", HeadSHA: "abc123", Status: checkStatusCompleted, Conclusion: checkConclusionSuccess}}, "octocat/hello-world", 1, "abc123")
	require.Len(t, checks, 2)
	assert.Equal(t, "payments", checks[1].DatabaseName)
	conclusion, status := computeAggregate(checks, nil)
	assert.Empty(t, conclusion)
	assert.Equal(t, checkStatusInProgress, status)
}

func TestParticipantSelectedPathsWithoutSchemaWork(t *testing.T) {
	cfg := &api.ServerConfig{AllowedEnvironments: []string{"sandbox"}, Repos: map[string]api.RepoConfig{"octocat/hello-world": {Aggregate: &api.AggregateConfig{Role: api.AggregateRoleParticipant}}}, Databases: map[string]api.DatabaseConfig{"orders": {Type: "mysql", AllowedRepos: []string{"octocat/hello-world"}, AllowedDirs: []string{"services/a"}, Environments: map[string]api.EnvironmentConfig{"sandbox": {}}}}}
	for _, path := range []string{"services/a/README.md", "README.md"} {
		t.Run(path, func(t *testing.T) {
			store := &foldCheckStore{}
			h, mux, client := newFoldHandler(t, cfg, store)
			serveHeadSHA(t, mux, "abc123")
			serveAggregateScopeInputs(t, mux, []string{path}, "")
			serveCheckRunCreate(t, mux)
			_, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")
			require.NoError(t, err)
			if strings.HasPrefix(path, "services/a/") {
				require.Len(t, store.upserted, 1)
				assert.Equal(t, checkConclusionSuccess, store.upserted[0].Conclusion)
			} else {
				assert.Empty(t, store.upserted)
			}
		})
	}
}

// A push or retarget during discovery invalidates its no-work evidence. The
// final PR read bypasses the request cache before any aggregate can pass.
func TestAggregateScopeRejectsMovingPR(t *testing.T) {
	for _, change := range []string{"head", "base", "unreadable"} {
		t.Run(change, func(t *testing.T) {
			store := &foldCheckStore{}
			cfg := &api.ServerConfig{AllowedEnvironments: []string{"sandbox"}, Repos: map[string]api.RepoConfig{"octocat/hello-world": {}}}
			h, mux, client := newFoldHandler(t, cfg, store)
			reads := 0
			mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
				reads++
				head, base := "abc123", "main"
				if reads >= 3 {
					switch change {
					case "head":
						head = "newsha"
					case "base":
						base = "release"
					case "unreadable":
						http.Error(w, "denied", http.StatusForbidden)
						return
					}
				}
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"head": map[string]string{"sha": head}, "base": map[string]string{"ref": base}}))
			})
			serveAggregateScopeInputs(t, mux, []string{"README.md"}, "")
			serveCheckRunCreate(t, mux)
			_, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")
			require.Error(t, err)
			for _, check := range store.upserted {
				assert.NotEqual(t, checkConclusionSuccess, check.Conclusion)
			}
		})
	}
}

// Successful discovery cannot attribute a changed config to no owner merely
// because the leader's registry lacks the database.
func TestAggregateScopeRejectsUnknownOwnership(t *testing.T) {
	store := &foldCheckStore{}
	cfg := &api.ServerConfig{AllowedEnvironments: []string{"sandbox", "production"}, Repos: map[string]api.RepoConfig{"octocat/hello-world": {Aggregate: &api.AggregateConfig{
		Role:            api.AggregateRoleLeader,
		ExpectedTenants: []api.ExpectedTenant{{Tenant: "b", Paths: []string{"services/b"}, CheckName: "SchemaBot B", Environments: []string{"production"}}},
	}}}}
	h, mux, client := newFoldHandler(t, cfg, store)
	serveHeadSHA(t, mux, "abc123")
	serveAggregateScopeInputs(t, mux, []string{"schemabot.yaml"}, "database: unknown\ntype: mysql\n")
	serveCheckRunCreate(t, mux)
	_, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")
	require.ErrorContains(t, err, "cannot resolve ownership")
	for _, check := range store.upserted {
		assert.NotEqual(t, checkConclusionSuccess, check.Conclusion)
	}
}
