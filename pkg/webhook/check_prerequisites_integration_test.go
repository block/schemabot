//go:build integration

package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/storage"

	"github.com/block/schemabot/pkg/checkstate"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A clean production plan is held by an unapplied sandbox plan in real storage.
// Updating only sandbox's result reopens production's existing required gate;
// both raw rows and both visible environment Check Runs remain independent.
func TestEnvironmentPrerequisiteFoldWithStorage(t *testing.T) {
	cfg := prerequisiteConfig()
	svc := setupE2EServiceWithConfig(t, cfg)
	seedCheck(t, svc, "orders", "production", checkConclusionSuccess)
	seedCheck(t, svc, "orders", "sandbox", checkConclusionActionRequired)
	client, mux := setupGitHubServer(t)
	install := ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot")
	h := NewHandler(svc, &fakeClientFactory{client: install}, nil, testLogger())
	serveHeadSHA(t, mux, "abc123")
	serveAggregateScopeInputs(t, mux, []string{"schemabot.yaml"}, "database: orders\ntype: mysql\n")
	// The fixture has no live Check Runs: the publisher must safely create them
	// when stored records name runs no longer found on GitHub.
	serveParticipantCheckRunsEmpty(t, mux, "abc123")
	posted := map[string]ghclient.CheckRunOptions{}
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var opts ghclient.CheckRunOptions
		require.NoError(t, json.NewDecoder(r.Body).Decode(&opts))
		posted[opts.Name] = opts
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 555}))
	})
	_, err := h.updateAggregateCheckOnce(t.Context(), install, "octocat/hello-world", 1, "abc123")
	require.NoError(t, err)
	assert.Equal(t, checkConclusionActionRequired, posted["SchemaBot (production)"].Conclusion)
	assert.Contains(t, posted["SchemaBot (production)"].Output.Summary, "Waiting for sandbox: orders")
	seedCheck(t, svc, "orders", "sandbox", checkConclusionSuccess)
	_, err = h.updateAggregateCheckOnce(t.Context(), install, "octocat/hello-world", 1, "abc123")
	require.NoError(t, err)
	assert.Equal(t, checkConclusionSuccess, posted["SchemaBot (production)"].Conclusion)
	raw, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "production", "mysql", "orders")
	require.NoError(t, err)
	assert.Equal(t, checkConclusionSuccess, raw.Conclusion)
	assert.Empty(t, raw.BlockingReason)
	aggregate, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "production", checkstate.AggregateSentinel, checkstate.AggregateSentinel)
	require.NoError(t, err)
	assert.Equal(t, checkConclusionSuccess, aggregate.Conclusion)
	applies, err := appliesForTestDatabase(t.Context(), svc, "orders")
	require.NoError(t, err)
	assert.Empty(t, applies)
}

// Two separately requested environments create distinct applies against their
// own targets. Production is refused before sandbox succeeds, and a sandbox
// completion alone never creates the production apply.
func TestSandboxAndProductionAppliesRemainSeparate(t *testing.T) {
	const database = "webhook_separate_environments"
	svc := setupE2EServiceMultiEnv(t, database)
	cfg := svc.Config()
	db := cfg.Databases[database]
	db.Environments["sandbox"] = db.Environments["staging"]
	delete(db.Environments, "staging")
	db.EnvironmentOrder = []string{"sandbox", "production"}
	cfg.Databases[database] = db
	cfg.AllowedEnvironments = []string{"sandbox", "production"}
	cfg.EnvironmentOrder = []string{"sandbox", "production"}
	sandboxClient, err := svc.TernClient(database, "staging")
	require.NoError(t, err)
	svc.RegisterTernClient(database, "sandbox", sandboxClient)
	require.NoError(t, svc.SetOperatorPollInterval(100*time.Millisecond))
	svc.StartOperator(t.Context())
	client, mux := setupGitHubServer(t)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"}, "database: "+database+"\ntype: mysql\n", database)
	// The apply observer can publish several intermediate comments/checks; drain
	// their capture channels while storage remains the assertion surface.
	drainDone := make(chan struct{})
	go func() {
		for {
			select {
			case <-result.comments:
			case <-result.checkRuns:
			case <-result.reactions:
			case <-drainDone:
				return
			}
		}
	}()
	defer close(drainDone)
	h := newE2EHandler(t, svc, client)
	_, err = h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "production", database, 12345, "testuser", CommandResult{})
	require.NoError(t, err)
	applies, err := appliesForTestDatabase(t.Context(), svc, database)
	require.NoError(t, err)
	require.Empty(t, applies)
	_, err = h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "sandbox", database, 12345, "testuser", CommandResult{})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		applies, err := appliesForTestDatabase(t.Context(), svc, database)
		if err != nil || len(applies) != 1 {
			return false
		}
		return applies[0].Environment == "sandbox" && state.IsState(applies[0].State, state.Apply.Completed)
	}, 30*time.Second, 50*time.Millisecond)
	applies, err = appliesForTestDatabase(t.Context(), svc, database)
	require.NoError(t, err)
	require.Len(t, applies, 1)
	sandboxID := applies[0].ApplyIdentifier
	_, err = h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "production", database, 12345, "testuser", CommandResult{})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		applies, err := appliesForTestDatabase(t.Context(), svc, database)
		if err != nil || len(applies) != 2 {
			return false
		}
		for _, apply := range applies {
			if !state.IsState(apply.State, state.Apply.Completed) {
				return false
			}
		}
		return true
	}, 30*time.Second, 50*time.Millisecond)
	applies, err = appliesForTestDatabase(t.Context(), svc, database)
	require.NoError(t, err)
	require.Len(t, applies, 2)
	for _, apply := range applies {
		if apply.Environment == "production" {
			assert.NotEqual(t, sandboxID, apply.ApplyIdentifier)
		}
	}
}

func appliesForTestDatabase(ctx context.Context, svc *api.Service, database string) ([]*storage.Apply, error) {
	applies, err := svc.Storage().Applies().GetByPR(ctx, "octocat/hello-world", 1)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(applies, func(apply *storage.Apply) bool { return apply.Database != database }), nil
}
