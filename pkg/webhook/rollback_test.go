package webhook

import (
	"bytes"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

func TestWebhookRollbackDispatch(t *testing.T) {
	h, _, _ := newTestHandler(t)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback apply_abc123 -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "rollback started")
}

func TestWebhookRollbackMissingEnv(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback apply_abc123",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "missing environment flag")

	select {
	case body := <-comments:
		assert.Contains(t, body, "Missing Environment")
		assert.Contains(t, body, "-e")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for comment")
	}
}

func TestWebhookRollbackConfirmDispatch(t *testing.T) {
	h, _, _ := newTestHandler(t)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback-confirm -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "rollback-confirm started")
}

func TestWebhookRollbackConfirmIgnoresApplyID(t *testing.T) {
	h, _, _ := newTestHandler(t)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback-confirm apply_abc123 -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "rollback-confirm started")
}

func TestWebhookRollbackConfirmMissingEnv(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback-confirm",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "missing environment flag")

	select {
	case body := <-comments:
		assert.Contains(t, body, "Missing Environment")
		assert.Contains(t, body, "-e")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for comment")
	}
}

func TestWebhookRollbackRejectsDatabaseFlag(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback apply_abc123 -e staging -d users_db",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "unsupported flag")

	select {
	case body := <-comments:
		assert.Contains(t, body, "`-d` flag is not supported")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for comment")
	}
}

func TestWebhookRollbackConfirmRejectsDatabaseFlag(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback-confirm -e staging -d users_db",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "unsupported flag")

	select {
	case body := <-comments:
		assert.Contains(t, body, "`-d` flag is not supported")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for comment")
	}
}

func TestWebhookRejectsDatabaseFlagForAnyCommandThatDoesNotSupportIt(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot stop apply_abc123 -e staging -d users_db",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "unsupported flag")

	select {
	case body := <-comments:
		assert.Contains(t, body, "`-d` flag is not supported")
		assert.Contains(t, body, "`stop`")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for comment")
	}
}

func TestWebhookRollbackMissingApplyID(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-comments:
		assert.Contains(t, body, "Missing Apply ID")
		assert.Contains(t, body, "schemabot rollback <apply-id> -e <environment>")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for comment")
	}
}

func TestWebhookRollbackMissingApplyIDAndEnv(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "missing rollback arguments")

	select {
	case body := <-comments:
		assert.Contains(t, body, "Missing Arguments")
		assert.Contains(t, body, "schemabot rollback <apply-id> -e <environment>")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for comment")
	}
}

func TestHandleRollbackCommandStorageUnavailablePostsError(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 1)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))

	installClient := ghclient.NewInstallationClient(client, testLogger())
	h := &Handler{
		service:   api.New(nil, &api.ServerConfig{}, nil, testLogger()),
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
		logger:    testLogger(),
	}

	h.handleRollbackCommand("octocat/hello-world", 1, 12345, "hubot", CommandResult{
		Action:      action.Rollback,
		ApplyID:     "apply_abc123",
		Environment: "staging",
	})

	body := requireComment(t, comments, "storage-unavailable rollback comment")
	assert.Contains(t, body, "Storage is not available")
}

func TestWebhookRollbackRejectsDeferCutoverOnPlanningCommand(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback apply_abc123 -e staging --defer-cutover",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "unsupported flag")

	select {
	case body := <-comments:
		assert.Contains(t, body, "--defer-cutover")
		assert.Contains(t, body, "rollback-confirm")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for comment")
	}
}

// newRollbackConfirmNoopHandler builds a handler whose rollback-confirm
// pinned rollback plan has no remaining changes, backed by the supplied lock
// store and logger, plus a channel that captures posted PR comments.
func newRollbackConfirmNoopHandler(t *testing.T, locks *actorAuthLockStore, logger *slog.Logger) (*Handler, chan string) {
	t.Helper()
	return newRollbackConfirmHandler(t, locks, logger, nil, nil)
}

// newRollbackConfirmHandler builds a handler whose rollback-confirm resolves
// the pinned rollback plan "rollback-plan-noop" for orders in staging,
// carrying the given namespace-level and per-shard changes, backed by the
// supplied lock store and logger, plus a channel that captures posted PR
// comments.
func newRollbackConfirmHandler(t *testing.T, locks *actorAuthLockStore, logger *slog.Logger, tables []storage.TableChange, shards []storage.ShardPlan) (*Handler, chan string) {
	t.Helper()
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 2)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))

	cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
		cfg.PRCommandAuthorization.AdminUsers = []string{"hubot"}
	})
	installClient := ghclient.NewInstallationClient(client, logger)
	plan := &storage.Plan{
		PlanIdentifier: "rollback-plan-noop",
		Database:       "orders",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Repository:     "octocat/hello-world",
		PullRequest:    1,
		Environment:    "staging",
		Deployment:     "orders",
		Shards:         shards,
	}
	if len(tables) > 0 {
		plan.Namespaces = map[string]*storage.NamespacePlanData{"orders": {Tables: tables}}
	}
	svc := api.New(&actorAuthStorage{locks: locks, plan: plan}, cfg, nil, logger)
	return &Handler{
		service:   svc,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
		logger:    logger,
	}, comments
}

// assertRollbackLockPinKept checks that a refused rollback-confirm left the
// lock exactly as it found it, still pinning the rollback plan, so re-issuing
// the command with --allow-unsafe confirms the same plan.
func assertRollbackLockPinKept(t *testing.T, locks *actorAuthLockStore) {
	t.Helper()
	assert.Empty(t, locks.released, "refused rollback-confirm must not release the lock")
	assert.Empty(t, locks.releasedIfPending, "refused rollback-confirm must not release the pinned rollback lock")
	assert.Empty(t, locks.releasedByID, "refused rollback-confirm must not release the lock")
	assert.Empty(t, locks.acquired, "refused rollback-confirm must not re-pin the lock")
	require.Len(t, locks.locks, 1)
	assert.Equal(t, rollbackPendingPlanID("rollback-plan-noop"), locks.locks[0].PendingPlanID)
}

// A rollback whose plan drops a table is refused when rollback-confirm is
// given without --allow-unsafe: the refusal names the dropped table and the
// exact rollback-confirm command that consents to it, no rollback apply is
// created (the fake apply store cannot create one), and the lock keeps
// pinning the rollback plan so the re-issued command confirms it.
func TestHandleRollbackConfirmBlocksUnsafeChangesWithoutAllowUnsafe(t *testing.T) {
	locks := &actorAuthLockStore{locks: []*storage.Lock{prOwnedRollbackLock()}}
	h, comments := newRollbackConfirmHandler(t, locks, testLogger(), []storage.TableChange{{
		Table:     "audit_log",
		DDL:       "DROP TABLE `audit_log`",
		Operation: "drop",
	}}, nil)

	retry, err := h.rollbackConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", 12345, "hubot",
		CommandResult{Action: action.RollbackConfirm, Environment: "staging"})
	require.NoError(t, err)
	assert.False(t, retry, "an unsafe-changes refusal is the command's answer, not a retryable failure")

	body := requireComment(t, comments, "rollback unsafe-changes refusal")
	assert.Contains(t, body, "**⛔ Rollback rejected**: 1 unsafe change detected")
	assert.Contains(t, body, "1. `audit_log`: DROP TABLE removes all data")
	assert.Contains(t, body, "DROP TABLE `audit_log`")
	assert.Contains(t, body, "```\nschemabot rollback-confirm -e staging --allow-unsafe\n```")
	assert.NotContains(t, body, "schemabot apply")
	assertRollbackLockPinKept(t, locks)
}

// A rollback of a sharded keyspace whose shards diverge carries a column drop
// only one shard needs. The namespace-level view shows only the safe column
// add, so the refusal must read the per-shard changes: it names the drop with
// the shard that carries it before any consent is asked for.
func TestHandleRollbackConfirmBlocksShardOnlyUnsafeChange(t *testing.T) {
	addNote := storage.TableChange{Namespace: "orders", Table: "orders", DDL: "ALTER TABLE `orders` ADD COLUMN `note` varchar(255)", Operation: "alter"}
	dropLegacy := storage.TableChange{Namespace: "orders", Table: "orders", DDL: "ALTER TABLE `orders` DROP COLUMN `legacy`", Operation: "alter",
		IsUnsafe: true, UnsafeReason: `Column "legacy" is dropped`}
	locks := &actorAuthLockStore{locks: []*storage.Lock{prOwnedRollbackLock()}}
	h, comments := newRollbackConfirmHandler(t, locks, testLogger(), []storage.TableChange{addNote}, []storage.ShardPlan{
		{Namespace: "orders", Shard: "-80", Changes: []storage.TableChange{addNote}},
		{Namespace: "orders", Shard: "80-", Changes: []storage.TableChange{dropLegacy}},
	})

	retry, err := h.rollbackConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", 12345, "hubot",
		CommandResult{Action: action.RollbackConfirm, Environment: "staging"})
	require.NoError(t, err)
	assert.False(t, retry)

	body := requireComment(t, comments, "rollback shard-only unsafe-changes refusal")
	assert.Contains(t, body, "**⛔ Rollback rejected**: 1 unsafe change detected")
	assert.Contains(t, body, "1. `orders` (shard `80-`)")
	assert.Contains(t, body, "`legacy`")
	assert.Contains(t, body, "```\nschemabot rollback-confirm -e staging --allow-unsafe\n```")
	assertRollbackLockPinKept(t, locks)
}

// The rollback plan comment names each unsafe change the rollback carries —
// a divergent shard's included — and states that confirming it takes
// --allow-unsafe. A rollback with no unsafe changes carries no warning.
func TestRollbackPlanCommentData_ListsUnsafeChanges(t *testing.T) {
	apply := &storage.Apply{ApplyIdentifier: "apply_5d2e8a", Database: "orders", Environment: "staging", DatabaseType: "mysql"}
	addNote := &apitypes.TableChangeResponse{TableName: "orders", Namespace: "orders", DDL: "ALTER TABLE `orders` ADD COLUMN `note` varchar(255)", ChangeType: "alter"}
	dropLegacy := &apitypes.TableChangeResponse{TableName: "orders", Namespace: "orders", DDL: "ALTER TABLE `orders` DROP COLUMN `legacy`", ChangeType: "alter",
		IsUnsafe: true, UnsafeReason: `Column "legacy" is dropped`}
	planResp := &apitypes.PlanResponse{
		PlanID:  "plan_rb_7c41f9",
		Changes: []*apitypes.SchemaChangeResponse{{Namespace: "orders", TableChanges: []*apitypes.TableChangeResponse{addNote}}},
		Shards: []*apitypes.ShardPlanResponse{
			{Namespace: "orders", Shard: "-80", Changes: []*apitypes.TableChangeResponse{addNote}},
			{Namespace: "orders", Shard: "80-", Changes: []*apitypes.TableChangeResponse{dropLegacy}},
		},
	}

	data := (&Handler{}).rollbackPlanCommentData(apply, planResp, "testuser")
	require.True(t, data.HasUnsafeChanges)
	require.Len(t, data.UnsafeChanges, 1)
	assert.Equal(t, "orders", data.UnsafeChanges[0].Table)
	assert.Equal(t, []string{"80-"}, data.UnsafeChanges[0].Shards)

	body := templates.RenderRollbackPlanComment(data)
	assert.Contains(t, body, "**Issues**: 1 unsafe change detected")
	assert.Contains(t, body, "1. `orders` (shard `80-`)")
	assert.Contains(t, body, "To confirm this rollback, add `--allow-unsafe` to confirm 1 unsafe change")

	safe := (&Handler{}).rollbackPlanCommentData(apply, &apitypes.PlanResponse{
		PlanID:  "plan_rb_safe",
		Changes: []*apitypes.SchemaChangeResponse{{Namespace: "orders", TableChanges: []*apitypes.TableChangeResponse{addNote}}},
	}, "testuser")
	assert.False(t, safe.HasUnsafeChanges)
	assert.NotContains(t, templates.RenderRollbackPlanComment(safe), "unsafe")
}

// prOwnedRollbackLock returns a lock held by the PR that issues the
// rollback-confirm command in these tests.
func prOwnedRollbackLock() *storage.Lock {
	return &storage.Lock{
		DatabaseName:  "orders",
		DatabaseType:  storage.DatabaseTypeMySQL,
		Owner:         "octocat/hello-world#1",
		Repository:    "octocat/hello-world",
		PullRequest:   1,
		PendingPlanID: rollbackPendingPlanID("rollback-plan-noop"),
	}
}

// TestHandleRollbackConfirmAlreadyRolledBackReleasesLock verifies the
// rollback-confirm no-op path: when the database already matches the original
// schema, the PR's database lock is released and the comment tells the
// operator the lock is gone.
func TestHandleRollbackConfirmAlreadyRolledBackReleasesLock(t *testing.T) {
	locks := &actorAuthLockStore{locks: []*storage.Lock{prOwnedRollbackLock()}}
	h, comments := newRollbackConfirmNoopHandler(t, locks, testLogger())

	h.handleRollbackConfirmCommand("octocat/hello-world", 1, "staging", 12345, "hubot", CommandResult{Action: action.RollbackConfirm})

	body := requireComment(t, comments, "already-rolled-back comment")
	assert.Contains(t, body, "Already Rolled Back")
	assert.Contains(t, body, "`orders`")
	assert.Contains(t, body, "Lock released")
	assert.NotContains(t, body, "failed to release")
	assert.Equal(t, []string{rollbackPendingPlanID("rollback-plan-noop")}, locks.releasedIfPending,
		"the release must be conditioned on the pinned rollback plan so a superseding same-owner intent stays intact")
}

// TestHandleRollbackConfirmAlreadyRolledBackLockReleaseFails verifies that
// when the rollback-confirm no-op path cannot release the database lock, the
// failure is logged with triage identifiers and the comment tells the
// operator the lock is still held and how to clear it, instead of claiming
// the lock was released.
func TestHandleRollbackConfirmAlreadyRolledBackLockReleaseFails(t *testing.T) {
	locks := &actorAuthLockStore{
		locks:      []*storage.Lock{prOwnedRollbackLock()},
		releaseErr: errors.New("storage unavailable"),
	}
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))
	h, comments := newRollbackConfirmNoopHandler(t, locks, logger)

	h.handleRollbackConfirmCommand("octocat/hello-world", 1, "staging", 12345, "hubot", CommandResult{Action: action.RollbackConfirm})

	body := requireComment(t, comments, "already-rolled-back lock-held comment")
	assert.Contains(t, body, "Already Rolled Back")
	assert.Contains(t, body, "failed to release the lock held by `octocat/hello-world#1`")
	assert.Contains(t, body, "Applies on this database will be blocked until the lock is released")
	assert.Contains(t, body, "schemabot unlock")
	assert.Contains(t, body, "schemabot unlock -d orders --force")
	assert.NotContains(t, body, "Lock released")
	assert.Empty(t, locks.releasedIfPending, "failed release must not be recorded as released")

	logs := logBuf.String()
	assert.Contains(t, logs, "failed to release the database lock")
	assert.Contains(t, logs, "octocat/hello-world")
	assert.Contains(t, logs, "database=orders")
	assert.Contains(t, logs, "environment=staging")
	assert.Contains(t, logs, "lock_owner=octocat/hello-world#1")
	assert.Contains(t, logs, "storage unavailable")
}

func TestWebhookApplyDispatch(t *testing.T) {
	h, _, _ := newTestHandler(t)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "apply started")
}

// A rollback plan comment carries the stored rollback plan's identifier, so
// reversal DDL cut to fit the comment names the command that prints the
// rollback plan in full rather than the schema files, which hold the desired
// schema and not the statements that reverse it.
func TestRollbackPlanCommentData_CarriesPlanID(t *testing.T) {
	apply := &storage.Apply{
		ApplyIdentifier: "apply_5d2e8a",
		Database:        "orders",
		Environment:     "production",
		DatabaseType:    "mysql",
	}
	columns := make([]string, 4000)
	for i := range columns {
		columns[i] = "DROP COLUMN `note_" + strings.Repeat("x", 8) + "`"
	}
	planResp := &apitypes.PlanResponse{
		PlanID: "plan_rb_7c41f9",
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace: "orders",
			TableChanges: []*apitypes.TableChangeResponse{{
				TableName:  "orders",
				DDL:        "ALTER TABLE `orders` " + strings.Join(columns, ", "),
				ChangeType: "alter",
			}},
		}},
	}

	data := (&Handler{}).rollbackPlanCommentData(apply, planResp, "testuser")
	assert.Equal(t, "plan_rb_7c41f9", data.PlanID)

	// The body holds the whole cut statement, so the markers are checked by
	// presence rather than dumped on failure.
	body := templates.RenderRollbackPlanComment(data)
	assert.True(t, strings.Contains(body, "the full plan is available from the CLI with `schemabot list-plans -e production plan_rb_7c41f9`."),
		"cut rollback DDL names the stored rollback plan")
	assert.False(t, strings.Contains(body, "the desired schema is in this PR's schema files"),
		"cut rollback DDL does not point at the schema files")
}
