package webhook

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
)

// applyCommandCore returns a durability disposition (retry, err) that a future
// durable issue_comment driver consumes. The synchronous goSafe wrapper discards
// it, so these tests pin the contract directly on the core.

// A command bootstrap failure (here, the per-installation GitHub client cannot be
// created) is a transient infrastructure failure the same delivery could clear on
// a later attempt, so the core must report it as retryable with a non-nil error.
func TestApplyCommandCoreBootstrapFailureIsRetryable(t *testing.T) {
	h := &Handler{
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{
			forInstallationErr: errors.New("installation token unavailable"),
		}),
		logger: testLogger(),
	}

	retry, err := h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.Apply})

	require.Error(t, err)
	assert.True(t, retry, "a command bootstrap failure is a transient infra failure a durable driver should re-drive")
}

// A GitHub App resolution failure inside the bootstrap is deterministic per
// deployment config — the same repo resolves to the same missing App on every
// attempt — so the core must report it as terminal rather than re-driving a
// delivery that can only fail until an operator fixes the config. The command
// never ran and no PR comment could be posted, so the core also returns the
// error: the delivery is recorded as failed (its only triage trail) rather
// than completed.
func TestApplyCommandCoreAppResolutionFailureIsTerminal(t *testing.T) {
	h := &Handler{
		ghClients: ghclient.NewClientSet(nil),
		logger:    testLogger(),
	}

	retry, err := h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.Apply})

	require.Error(t, err)
	assert.ErrorIs(t, err, errGitHubAppResolution)
	assert.False(t, retry, "a deterministic GitHub App resolution failure must not be re-driven; recovery is fixing the deployment config")
}

// Terminal outcomes are the command's answer — a static skip or a config-shape
// rejection the same input will always produce — so the core reports them as
// (retry=false, err=nil): a durable driver must not re-drive them.
func TestApplyCommandCoreTerminalDispositions(t *testing.T) {
	// An unscoped fan-out apply for a database this deployment does not own is a
	// deliberate silent no-op, not a failure.
	t.Run("unowned unscoped fan-out is terminal and silent", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())
		serveSchemaConfigForDatabase(t, mux, "orders")

		retry, err := h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.Apply})

		require.NoError(t, err)
		assert.False(t, retry, "a non-owning fan-out skip is the command's terminal answer, not a retryable failure")
		assert.Empty(t, comments, "the terminal skip must stay silent")
	})

	// A schema-request rejection (database not configured on this server) posts
	// the command's answer as a PR comment; re-driving would only re-post it, so
	// it is terminal despite the visible error comment.
	t.Run("schema request rejection is terminal", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, nonAggregateConfig())
		serveSchemaConfigForDatabase(t, mux, "orders")

		retry, err := h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.Apply})

		require.NoError(t, err)
		assert.False(t, retry, "a config-shape rejection is the command's answer, not a transient failure")
		body := requireComment(t, comments, "database-not-configured apply error")
		assert.Contains(t, body, `database &#34;orders&#34; is not configured on this server`)
	})

	// Requesting an environment the database does not configure is a targeting
	// rejection the same command will always reproduce, so it is terminal even
	// though it surfaces through the generic schema-request error comment.
	t.Run("environment rejection is terminal", func(t *testing.T) {
		cfg := nonAggregateConfig()
		cfg.Databases = map[string]api.DatabaseConfig{
			"orders": {Environments: map[string]api.EnvironmentConfig{"production": {}}},
		}
		h, mux, comments := newFanOutSkipHandler(t, cfg)
		serveSchemaConfigForDatabase(t, mux, "orders")

		retry, err := h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.Apply})

		require.NoError(t, err)
		assert.False(t, retry, "an unconfigured-environment rejection is the command's answer, not a transient failure")
		body := requireComment(t, comments, "environment-not-configured apply error")
		assert.Contains(t, body, `database &#34;orders&#34; environment &#34;staging&#34; is not configured on this server`)
	})
}

// An aggregate participant cannot make a fleet-wide ownership decision when
// its local schema directory hints cannot recover config discovery from a
// truncated repository. It defers silently to the leader instead of retrying
// a permanent condition and eventually posting Apply Failed.
func TestApplyCommandCoreParticipantTruncatedDiscoveryDefersToLeader(t *testing.T) {
	h, mux, comments := newFanOutSkipHandler(t, aggregateParticipantConfig())
	serveTruncatedRepoWithChangedSchemaFile(t, mux)

	retry, err := h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "production", "", 12345, "hubot", CommandResult{Action: action.Apply})

	require.NoError(t, err)
	assert.False(t, retry, "participant discovery uncertainty is a terminal silent defer, not retryable work")
	assert.Empty(t, comments, "the participant must not post Apply Failed for incomplete fleet discovery")
}

// A GitHub read failure during config discovery (here, fetching the changed
// schemabot.yaml returns a server error) is a transient infrastructure failure:
// the same delivery could succeed once GitHub recovers, so the core must report
// it as retryable rather than treating the posted error comment as terminal.
func TestApplyCommandCoreTransientConfigReadFailureIsRetryable(t *testing.T) {
	h, mux, _ := newFanOutSkipHandler(t, nonAggregateConfig())
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"head": map[string]any{"sha": "abc123", "ref": "feature-branch"},
			"base": map[string]any{"sha": "def456", "ref": "main"},
			"user": map[string]any{"login": "testuser"},
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode([]map[string]string{{
			"filename": "schemabot.yaml",
			"status":   "modified",
		}}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schemabot.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	retry, err := h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.Apply})

	require.Error(t, err)
	assert.True(t, retry, "a transient GitHub config read failure must stay retryable for a durable driver")
}

// A checks read failure leaves the gate unevaluated, so the apply remains
// retryable and can be safely re-driven once GitHub recovers.
func TestApplyCommandCoreChecksGateEvaluationFailureIsRetryable(t *testing.T) {
	h, mux, _ := newApplyGateContractHandler(t, &emptyStorage{})
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/status", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	retry, err := h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.Apply})

	require.Error(t, err)
	assert.True(t, retry, "an unevaluated checks gate must be retried")
}

// A failing required check is a verified gate decision, so the apply is
// terminal for this command delivery rather than retrying the same block.
func TestApplyCommandCoreChecksGateMeritBlockIsTerminal(t *testing.T) {
	h, mux, _ := newApplyGateContractHandler(t, &emptyStorage{})
	registerCheckStatusRESTHandlers(mux, []checkStatusNode{{
		Typename: "CheckRun", Name: "CI / tests", Status: "completed", Conclusion: "failure", AppSlug: "github-actions",
	}})

	retry, err := h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.Apply})

	require.NoError(t, err)
	assert.False(t, retry, "a verified checks gate block is terminal")
}

// Base-freshness uncertainty is retryable and preserves the pending lock so a
// later confirmation can still execute the plan the user reviewed.
func TestApplyConfirmCommandCoreBaseFreshnessFailureIsRetryableAndKeepsPendingLock(t *testing.T) {
	locks := newApplyConfirmContractLockStore()
	h, mux, _ := newApplyGateContractHandler(t, &actorAuthStorage{locks: locks})
	registerCheckStatusRESTHandlers(mux, nil)
	registerBaseFreshnessRef(t, mux)
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/base-tip-sha...abc123", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	retry, err := h.applyConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.ApplyConfirm})

	require.Error(t, err)
	assert.True(t, retry, "base-freshness uncertainty must be retried")
	assert.Empty(t, locks.released, "verification failure must not release the pending lock")
	assert.Empty(t, locks.releasedIfPending, "verification failure must not conditionally release the pending lock")
}

// A verified base-schema change makes the confirmation stale and terminal;
// releasing the observed pending intent lets the PR create a fresh plan.
func TestApplyConfirmCommandCoreStaleBaseIsTerminalAndReleasesPendingLock(t *testing.T) {
	locks := newApplyConfirmContractLockStore()
	h, mux, _ := newApplyGateContractHandler(t, &actorAuthStorage{locks: locks})
	registerCheckStatusRESTHandlers(mux, nil)
	registerBaseFreshnessRef(t, mux)
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/base-tip-sha...abc123", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"merge_base_commit": map[string]string{"sha": "merge-base-sha"},
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/merge-base-sha", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"tree": []map[string]string{{"path": "schema", "type": "tree", "sha": "schema-old"}}}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/base-tip-sha", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"tree": []map[string]string{{"path": "schema", "type": "tree", "sha": "schema-new"}}}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/schema-old", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"tree": []map[string]string{{"path": "orders", "type": "tree", "sha": "orders-old"}}}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/schema-new", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"tree": []map[string]string{{"path": "orders", "type": "tree", "sha": "orders-new"}}}))
	})

	retry, err := h.applyConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.ApplyConfirm})

	require.NoError(t, err)
	assert.False(t, retry, "a verified stale base is terminal")
	assert.Empty(t, locks.released, "stale rejection must use the intent-conditional release")
	assert.Equal(t, []string{"plan_confirm123"}, locks.releasedIfPending)
}

func newApplyGateContractHandler(t *testing.T, store storage.Storage) (*Handler, *http.ServeMux, <-chan string) {
	t.Helper()
	client, mux := setupGitHubServer(t)
	registerApplyDiscoveryEndpoints(t, mux, "orders")
	comments := make(chan string, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))
	h := actorAuthStorageTestHandler(actorAuthTestConfig(false), store, ghclient.NewInstallationClient(client, testLogger()))
	return h, mux, comments
}

func newApplyConfirmContractLockStore() *actorAuthLockStore {
	return &actorAuthLockStore{locks: []*storage.Lock{{
		DatabaseName: "orders", DatabaseType: storage.DatabaseTypeMySQL,
		Owner: "octocat/hello-world#1", Repository: "octocat/hello-world", PullRequest: 1,
		PendingPlanID: "plan_confirm123",
	}}}
}

func registerBaseFreshnessRef(t *testing.T, mux *http.ServeMux) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/git/ref/heads/main", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"ref": "refs/heads/main", "object": map[string]string{"type": "commit", "sha": "base-tip-sha"},
		}))
	})
}
