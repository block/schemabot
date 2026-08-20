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

// applyConfirmCommandCore returns a durability disposition (retry, err) that
// the durable issue_comment driver consumes. The synchronous goSafe wrapper
// discards it, so these tests pin the contract directly on the core.

// A command bootstrap failure (here, the per-installation GitHub client cannot be
// created) is a transient infrastructure failure the same delivery could clear on
// a later attempt, so the core must report it as retryable with a non-nil error.
func TestApplyConfirmCommandCoreBootstrapFailureIsRetryable(t *testing.T) {
	h := &Handler{
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{
			forInstallationErr: errors.New("installation token unavailable"),
		}),
		logger: testLogger(),
	}

	retry, err := h.applyConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.ApplyConfirm})

	require.Error(t, err)
	assert.True(t, retry, "a command bootstrap failure is a transient infra failure a durable driver should re-drive")
}

// Terminal outcomes are the command's answer — a static skip or a config-shape
// rejection the same input will always produce — so the core reports them as
// (retry=false, err=nil): a durable driver must not re-drive them.
func TestApplyConfirmCommandCoreTerminalDispositions(t *testing.T) {
	// An unscoped fan-out apply-confirm for a database this deployment does not
	// own is a deliberate silent no-op, not a failure.
	t.Run("unowned unscoped fan-out is terminal and silent", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())
		serveSchemaConfigForDatabase(t, mux, "orders")

		retry, err := h.applyConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.ApplyConfirm})

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

		retry, err := h.applyConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.ApplyConfirm})

		require.NoError(t, err)
		assert.False(t, retry, "a config-shape rejection is the command's answer, not a transient failure")
		body := requireComment(t, comments, "database-not-configured apply-confirm error")
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

		retry, err := h.applyConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.ApplyConfirm})

		require.NoError(t, err)
		assert.False(t, retry, "an unconfigured-environment rejection is the command's answer, not a transient failure")
		body := requireComment(t, comments, "environment-not-configured apply-confirm error")
		assert.Contains(t, body, `database &#34;orders&#34; environment &#34;staging&#34; is not configured on this server`)
	})
}

// A GitHub read failure during config discovery (here, fetching the changed
// schemabot.yaml returns a server error) is a transient infrastructure failure:
// the same delivery could succeed once GitHub recovers, so the core must report
// it as retryable rather than treating the posted error comment as terminal.
func TestApplyConfirmCommandCoreTransientConfigReadFailureIsRetryable(t *testing.T) {
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

	retry, err := h.applyConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.ApplyConfirm})

	require.Error(t, err)
	assert.True(t, retry, "a transient GitHub config read failure must stay retryable for a durable driver")
}

// Lock lookup uncertainty remains retryable, while known lock rejections are
// terminal and report a specific operator action unless fan-out requires silence.
func TestApplyConfirmCommandCoreLockPathDispositions(t *testing.T) {
	t.Run("lock lookup failure is retryable", func(t *testing.T) {
		h, mux, _ := newApplyGateContractHandler(t, &actorAuthStorage{locks: &actorAuthLockStore{getErr: errors.New("lock storage unavailable")}})
		registerCheckStatusRESTHandlers(mux, nil)

		retry, err := h.applyConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.ApplyConfirm})

		require.Error(t, err)
		assert.True(t, retry, "storage uncertainty must be re-driven")
	})

	t.Run("missing lock is terminal", func(t *testing.T) {
		h, mux, comments := newApplyGateContractHandler(t, &actorAuthStorage{locks: &actorAuthLockStore{}})
		registerCheckStatusRESTHandlers(mux, nil)

		retry, err := h.applyConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.ApplyConfirm})

		require.NoError(t, err)
		assert.False(t, retry, "no pending confirmation is the command's terminal answer")
		body := requireComment(t, comments, "missing-lock apply-confirm comment")
		assert.Contains(t, body, "No apply lock is held for this database.")
	})

	t.Run("missing lock on unscoped fan-out is terminal", func(t *testing.T) {
		cfg := actorAuthTestConfig(false)
		cfg.Repos = aggregateLeaderConfig().Repos
		h, mux, comments := newFanOutSkipHandler(t, cfg)
		registerApplyDiscoveryEndpoints(t, mux, "orders")
		registerCheckStatusRESTHandlers(mux, nil)

		retry, err := h.applyConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.ApplyConfirm})

		require.NoError(t, err)
		assert.False(t, retry, "an unscoped deployment with no pending confirmation must terminate silently")
		assert.Empty(t, comments, "the terminal skip must stay silent")
	})

	t.Run("rollback lock is terminal", func(t *testing.T) {
		lock := applyConfirmContractLock("octocat/hello-world#1", "rollback:rbplan-1")
		h, mux, comments := newApplyGateContractHandler(t, &actorAuthStorage{locks: &actorAuthLockStore{locks: []*storage.Lock{lock}}})
		registerCheckStatusRESTHandlers(mux, nil)

		retry, err := h.applyConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.ApplyConfirm})

		require.NoError(t, err)
		assert.False(t, retry, "a rollback lock requires a different command and is a terminal rejection")
		body := requireComment(t, comments, "rollback-lock apply-confirm comment")
		assert.Contains(t, body, "This lock belongs to a rollback plan. Use `schemabot rollback-confirm` to execute it")
	})

	t.Run("lock held by another PR is terminal", func(t *testing.T) {
		lock := applyConfirmContractLock("octocat/hello-world#2", "plan_confirm123")
		lock.PullRequest = 2
		h, mux, comments := newApplyGateContractHandler(t, &actorAuthStorage{locks: &actorAuthLockStore{locks: []*storage.Lock{lock}}})
		registerCheckStatusRESTHandlers(mux, nil)
		registerCurrentBaseSchema(t, mux)

		retry, err := h.applyConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.ApplyConfirm})

		require.NoError(t, err)
		assert.False(t, retry, "a confirmed lock conflict is the command's terminal answer")
		body := requireComment(t, comments, "foreign-lock apply-confirm comment")
		assert.Contains(t, body, "**Locked by**: [octocat/hello-world#2]")
	})
}

func applyConfirmContractLock(owner, pendingPlanID string) *storage.Lock {
	return &storage.Lock{
		DatabaseName: "orders", DatabaseType: storage.DatabaseTypeMySQL,
		Owner: owner, Repository: "octocat/hello-world", PullRequest: 1,
		PendingPlanID: pendingPlanID,
	}
}

func registerCurrentBaseSchema(t *testing.T, mux *http.ServeMux) {
	t.Helper()
	registerBaseFreshnessRef(t, mux)
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/base-tip-sha...abc123", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"merge_base_commit": map[string]string{"sha": "base-tip-sha"},
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/base-tip-sha", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"tree": []any{}}))
	})
}
