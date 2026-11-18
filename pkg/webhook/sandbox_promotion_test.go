package webhook

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

// A deployment serving sandbox and production verifies staging through its
// peer's Check Run and sandbox through local state. Only a sandbox success on
// this PR head permits production; unrelated databases keep staging-first order.
func TestSandboxPromotionWithSharedProductionDeployment(t *testing.T) {
	for _, tc := range []struct {
		name       string
		database   string
		sandbox    *storage.Check
		blocked    bool
		localReads int
	}{
		{name: "missing sandbox", database: "orders", blocked: true, localReads: 1},
		{name: "failed sandbox", database: "orders", sandbox: &storage.Check{HeadSHA: "head", Status: checkStatusCompleted, Conclusion: checkConclusionFailure}, blocked: true, localReads: 1},
		{name: "stale sandbox", database: "orders", sandbox: &storage.Check{HeadSHA: "old", Status: checkStatusCompleted, Conclusion: checkConclusionSuccess}, blocked: true, localReads: 1},
		{name: "current sandbox", database: "orders", sandbox: &storage.Check{HeadSHA: "head", Status: checkStatusCompleted, Conclusion: checkConclusionSuccess}, localReads: 1},
		{name: "database without sandbox", database: "payments"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, mux := setupGitHubServer(t)
			mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, r *http.Request) {
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"head": map[string]any{"sha": "head"}}))
			})
			mux.HandleFunc("GET /repos/octocat/hello-world/commits/head/check-runs", func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "SchemaBot (staging)", r.URL.Query().Get("check_name"))
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
					"total_count": 1,
					"check_runs":  []map[string]any{{"id": 1, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "success", "app": map[string]any{"slug": "schemabot"}}},
				}))
			})
			mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusCreated)
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 99}))
			})
			checks := &sequenceCheckStore{results: []*storage.Check{tc.sandbox}}
			cfg := &api.ServerConfig{
				AllowedEnvironments: []string{"sandbox", "production"},
				Databases: map[string]api.DatabaseConfig{
					"orders": {Type: "mysql", EnvironmentOrder: []string{"staging", "sandbox", "production"}, Environments: map[string]api.EnvironmentConfig{
						"sandbox":    {Deployment: "primary", Target: "orders-002"},
						"production": {Deployment: "primary", Target: "orders-001"},
					}},
					"payments": {Type: "mysql", Environments: map[string]api.EnvironmentConfig{
						"production": {Deployment: "primary", Target: "payments-001"},
					}},
				},
				TernDeployments: api.TernConfig{"primary": {"sandbox": "tern.example:9090", "production": "tern.example:9090"}},
			}
			require.NoError(t, cfg.Validate())
			service := api.New(&sequenceStorage{checks: checks}, cfg, nil, testLogger())
			t.Cleanup(func() { utils.CloseAndLog(service) })
			h := &Handler{
				service:   service,
				ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot")}),
				logger:    testLogger(), priorEnvCheckMaxAttempts: 1, priorEnvCheckRetryInterval: time.Nanosecond,
			}
			blocked, err := h.checkPriorEnvironments(t.Context(), "octocat/hello-world", 1, "head", tc.database, "mysql", "production", []string{"production"}, 12345, false)
			require.NoError(t, err)
			assert.Equal(t, tc.blocked, blocked)
			assert.Equal(t, tc.localReads, checks.calls)
			envs, err := h.allowedDatabaseEnvironments(tc.database)
			require.NoError(t, err)
			if tc.database == "payments" {
				assert.Equal(t, []string{"production"}, envs)
			} else {
				assert.Equal(t, []string{"sandbox", "production"}, envs)
			}
		})
	}
}
