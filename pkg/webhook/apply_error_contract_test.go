package webhook

import (
	"context"
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
		assert.Contains(t, body, "Database Not Configured")
		assert.Contains(t, body, "`orders`")
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

// An aggregate participant that cannot enumerate a truncated repository does
// not know whether it owns the changed schema — it might be the owner and
// simply unable to prove it. A silent defer here could drop a command with no
// deployment answering, so the failure surfaces fail-closed and stays
// retryable for a durable driver.
func TestApplyCommandCoreParticipantTruncatedDiscoverySurfaces(t *testing.T) {
	h, mux, comments := newFanOutSkipHandler(t, aggregateParticipantConfig())
	serveTruncatedRepoWithChangedSchemaFile(t, mux)

	retry, err := h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "production", "", 12345, "hubot", CommandResult{Action: action.Apply})

	require.Error(t, err)
	assert.True(t, retry, "incomplete discovery is not the command's answer, so it must stay retryable")
	body := requireComment(t, comments, "participant truncated-discovery error")
	assert.Contains(t, body, "truncated repository tree")
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
	h, mux, _ := newApplyGateContractHandler(t, newApplyConfirmContractStorage(locks))
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
	h, mux, _ := newApplyGateContractHandler(t, newApplyConfirmContractStorage(locks))
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

// A pending lock without a loadable plan cannot attest which environment the
// operator reviewed. The confirmation fails closed and leaves the intent pinned
// so an operator can inspect or replace it explicitly.
func TestApplyConfirmCommandCoreMissingPlanIsTerminalAndKeepsPendingLock(t *testing.T) {
	locks := newApplyConfirmContractLockStore()
	h, mux, comments := newApplyGateContractHandler(t, &actorAuthStorage{locks: locks})
	registerCheckStatusRESTHandlers(mux, nil)
	registerCurrentBaseSchema(t, mux)

	retry, err := h.applyConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.ApplyConfirm})

	require.NoError(t, err)
	assert.False(t, retry, "an unverifiable pending confirmation is a terminal rejection")
	assert.Empty(t, locks.releasedIfPending, "the unverifiable pending intent must stay pinned")
	body := requireComment(t, comments, "unverifiable-plan apply-confirm comment")
	assert.Contains(t, body, "Apply-confirm Refused — Staging")
	assert.Contains(t, body, "could not verify which environment")
	assert.Contains(t, body, "nothing was applied")
	assert.Contains(t, body, "To replace that confirmation with a fresh plan and apply it in one step")
	assert.Contains(t, body, "```\nschemabot apply -e staging\n```")
	assert.Contains(t, body, "_Requested by @hubot_")
}

// The rejection posted for a pending confirmation with no loadable plan carries
// the rejected command's -d scope, tenant, and option flags, so the recovery
// command it recommends can be pasted as-is.
func TestApplyConfirmCommandCoreMissingPlanRecoveryCommandKeepsOperatorFlags(t *testing.T) {
	locks := newApplyConfirmContractLockStore()
	h, mux, comments := newApplyGateContractHandler(t, &actorAuthStorage{locks: locks})
	registerCheckStatusRESTHandlers(mux, nil)
	registerCurrentBaseSchema(t, mux)

	retry, err := h.applyConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "orders", 12345, "hubot",
		CommandResult{Action: action.ApplyConfirm, Database: "orders", Tenant: "acme", DeferCutover: true})

	require.NoError(t, err)
	assert.False(t, retry)
	body := requireComment(t, comments, "unverifiable-plan apply-confirm comment")
	assert.Contains(t, body, "**Database**: `orders`")
	assert.Contains(t, body, "To replace that confirmation with a fresh plan and apply it in one step, subject to the environment ordering gate and pausing for `apply-confirm` only if its plan needs it:\n\n```\nschemabot apply -e staging -d orders --tenant acme --defer-cutover\n```")
}

// A prior environment with pending changes blocks the confirm as a terminal
// answer: the durable delivery must not re-drive it, and the reviewed plan stays
// pinned for when the prior environment is clean again.
func TestApplyConfirmCommandCorePriorEnvironmentBlockIsTerminalAndKeepsPendingLock(t *testing.T) {
	locks := newApplyConfirmContractLockStore()
	store := newApplyConfirmContractStorage(locks)
	store.plan.Environment = "production"
	store.checks = &sequenceCheckStore{results: []*storage.Check{{
		Environment: "staging", DatabaseType: "mysql", DatabaseName: "orders", HeadSHA: "abc123",
		Status: checkStatusCompleted, Conclusion: checkConclusionActionRequired, HasChanges: true,
	}}}
	h, mux, comments := newApplyGateContractHandlerWithConfig(t, promotionOrderedContractConfig(), store)
	registerCheckStatusRESTHandlers(mux, nil)
	registerCurrentBaseSchema(t, mux)

	retry, err := h.applyConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "production", "", 12345, "hubot", CommandResult{Action: action.ApplyConfirm})

	require.NoError(t, err)
	assert.False(t, retry, "a prior environment with pending changes is the command's answer, not a transient failure")
	assert.Empty(t, locks.released, "an ordering block must not release the pending lock")
	assert.Empty(t, locks.releasedIfPending, "an ordering block must not conditionally release the pending lock")
	assert.Equal(t, "plan_confirm123", locks.locks[0].PendingPlanID)
	assert.Contains(t, requireComment(t, comments, "prior environment block comment"), "Apply staging first")
}

// Failure to read a prior environment leaves promotion ordering unevaluated,
// so the durable delivery retries while preserving the exact reviewed plan.
func TestApplyConfirmCommandCorePriorEnvironmentReadFailureIsRetryableAndKeepsPendingLock(t *testing.T) {
	locks := newApplyConfirmContractLockStore()
	store := newApplyConfirmContractStorage(locks)
	store.plan.Environment = "production"
	store.checks = &applyConfirmErrorCheckStore{err: errors.New("check storage unavailable")}
	h, mux, _ := newApplyGateContractHandlerWithConfig(t, promotionOrderedContractConfig(), store)
	registerCheckStatusRESTHandlers(mux, nil)
	registerCurrentBaseSchema(t, mux)

	retry, err := h.applyConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "production", "", 12345, "hubot", CommandResult{Action: action.ApplyConfirm})

	require.Error(t, err)
	assert.True(t, retry, "an unevaluated promotion gate must be re-driven")
	assert.Empty(t, locks.released, "a gate read failure must not release the pending lock")
	assert.Empty(t, locks.releasedIfPending, "a gate read failure must not conditionally release the pending lock")
	assert.Equal(t, "plan_confirm123", locks.locks[0].PendingPlanID)
}

// promotionOrderedContractConfig adds a production environment behind staging
// so the confirm-time environment ordering gate has a prior environment to
// consult.
func promotionOrderedContractConfig() *api.ServerConfig {
	return actorAuthTestConfig(false, func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.Environments["production"] = api.EnvironmentConfig{DSN: "root@tcp(localhost)/orders"}
		cfg.Databases["orders"] = db
		cfg.EnvironmentOrder = []string{"staging", "production"}
	})
}

func newApplyGateContractHandler(t *testing.T, store storage.Storage) (*Handler, *http.ServeMux, <-chan string) {
	return newApplyGateContractHandlerWithConfig(t, actorAuthTestConfig(false), store)
}

func newApplyGateContractHandlerWithConfig(t *testing.T, cfg *api.ServerConfig, store storage.Storage) (*Handler, *http.ServeMux, <-chan string) {
	t.Helper()
	client, mux := setupGitHubServer(t)
	registerApplyDiscoveryEndpoints(t, mux, "orders")
	comments := make(chan string, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))
	h := actorAuthStorageTestHandler(cfg, store, ghclient.NewInstallationClient(client, testLogger()))
	return h, mux, comments
}

type applyConfirmContractStorage struct {
	*actorAuthStorage
	checks storage.CheckStore
}

func (s *applyConfirmContractStorage) Checks() storage.CheckStore {
	if s.checks != nil {
		return s.checks
	}
	return s.actorAuthStorage.Checks()
}

type applyConfirmErrorCheckStore struct {
	storage.CheckStore
	err error
}

func (s *applyConfirmErrorCheckStore) Get(_ context.Context, _ string, _ int, _, _, _ string) (*storage.Check, error) {
	return nil, s.err
}

func newApplyConfirmContractStorage(locks *actorAuthLockStore) *applyConfirmContractStorage {
	return &applyConfirmContractStorage{actorAuthStorage: &actorAuthStorage{
		locks: locks,
		plan: &storage.Plan{
			PlanIdentifier: "plan_confirm123",
			Database:       "orders",
			DatabaseType:   storage.DatabaseTypeMySQL,
			Environment:    "staging",
			HeadSHA:        "abc123",
		},
	}}
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
