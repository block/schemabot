package webhook

import (
	"bytes"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
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
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 2)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))

	cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
		cfg.PRCommandAuthorization.AdminUsers = []string{"hubot"}
	})
	installClient := ghclient.NewInstallationClient(client, logger)
	svc := api.New(&actorAuthStorage{
		locks: locks,
		plan: &storage.Plan{
			PlanIdentifier: "rollback-plan-noop",
			Database:       "orders",
			DatabaseType:   storage.DatabaseTypeMySQL,
			Repository:     "octocat/hello-world",
			PullRequest:    1,
			Environment:    "staging",
			Deployment:     "orders",
		},
	}, cfg, nil, logger)
	return &Handler{
		service:   svc,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
		logger:    logger,
	}, comments
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

	h.handleRollbackConfirmCommand("octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.RollbackConfirm})

	body := requireComment(t, comments, "already-rolled-back comment")
	assert.Contains(t, body, "Already Rolled Back")
	assert.Contains(t, body, "`orders`")
	assert.Contains(t, body, "Lock released")
	assert.NotContains(t, body, "failed to release")
	assert.Equal(t, []string{"orders"}, locks.released)
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

	h.handleRollbackConfirmCommand("octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.RollbackConfirm})

	body := requireComment(t, comments, "already-rolled-back lock-held comment")
	assert.Contains(t, body, "Already Rolled Back")
	assert.Contains(t, body, "failed to release the lock held by `octocat/hello-world#1`")
	assert.Contains(t, body, "Applies on this database will be blocked until the lock is released")
	assert.Contains(t, body, "schemabot unlock")
	assert.Contains(t, body, "schemabot unlock -d orders --force")
	assert.NotContains(t, body, "Lock released")
	assert.Empty(t, locks.released, "failed release must not be recorded as released")

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
