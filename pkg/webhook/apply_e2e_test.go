//go:build integration

package webhook

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	gh "github.com/google/go-github/v68/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/spirit/pkg/utils"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

func TestE2EApplyWithChanges(t *testing.T) {
	dbName := "webhook_apply_changes"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "apply started")

	// The apply handler runs as a goroutine — wait for the apply plan confirmation comment
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Schema Change Plan (Apply)")
		assert.Contains(t, body, "CREATE TABLE")
		assert.Contains(t, body, dbName)
		assert.Contains(t, body, "Lock acquired by")
		assert.Contains(t, body, "schemabot apply-confirm -e staging")
		assert.Contains(t, body, "schemabot unlock")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for apply plan comment")
	}

	// Verify lock was acquired
	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock, "expected lock to be acquired")
	assert.Equal(t, "octocat/hello-world#1", lock.Owner)
	assert.Equal(t, "octocat/hello-world", lock.Repository)
	assert.Equal(t, 1, lock.PullRequest)

	// Clean up the lock so it doesn't leak to other tests sharing the same PR
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	// Verify check run was created with action_required
	select {
	case cr := <-result.checkRuns:
		assert.Contains(t, cr.Name, "SchemaBot")
		assert.Equal(t, "completed", cr.Status)
		assert.Equal(t, "action_required", cr.Conclusion)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for check run")
	}
}

func TestE2EApplyNoChanges(t *testing.T) {
	dbName := "webhook_apply_nochanges"
	svc := setupE2EService(t, dbName)

	// Pre-create the table so there are no changes
	seedTargetTable(t, dbName,
		"CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	// No changes → should post regular plan comment (not apply plan), no lock acquired
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "No schema changes detected")
		assert.NotContains(t, body, "Lock acquired")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for comment")
	}

	// Verify no lock was acquired
	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	assert.Nil(t, lock, "expected no lock when there are no changes")
}

func TestE2EApplyLockConflictDifferentPR(t *testing.T) {
	dbName := "webhook_apply_conflict"
	svc := setupE2EService(t, dbName)

	// Pre-acquire a lock from a different PR
	err := svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: "mysql",
		Repository:   "other-org/other-repo",
		PullRequest:  42,
		Owner:        "other-org/other-repo#42",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	// Should post "blocked by other PR" comment
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Apply Blocked")
		assert.Contains(t, body, "other-org/other-repo#42")
		assert.Contains(t, body, "https://github.com/other-org/other-repo/pull/42")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for blocked comment")
	}
}

func TestE2EApplyConfirmNoLock(t *testing.T) {
	dbName := "webhook_confirm_nolock"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	// apply-confirm without a prior apply (no lock)
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply-confirm -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	// Should post "no lock found" comment
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "No Lock Found")
		assert.Contains(t, body, "schemabot apply -e staging")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for no-lock comment")
	}
}

func TestE2EUnlock(t *testing.T) {
	dbName := "webhook_unlock"
	svc := setupE2EService(t, dbName)

	// Acquire a lock from this PR
	err := svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: "mysql",
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		Owner:        "octocat/hello-world#1",
	})
	require.NoError(t, err)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// For unlock, we still need PR info for the check run
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{
				Ref: new("feature-branch"),
				SHA: new("abc123"),
			},
			Base: &gh.PullRequestBranch{
				Ref: new("main"),
				SHA: new("def456"),
			},
			User: &gh.User{Login: new("testuser")},
		})
	})

	comments := make(chan string, 10)
	reactions := make(chan string, 10)
	checkRuns := make(chan checkRunCapture, 10)

	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Content string `json:"content"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		reactions <- body.Content
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		checkRuns <- body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot unlock",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "unlock started")

	// Should post "lock released" comment
	select {
	case body := <-comments:
		assert.Contains(t, body, "Lock Released")
		assert.Contains(t, body, dbName)
		assert.Contains(t, body, "@testuser")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for unlock comment")
	}

	// Verify lock was released
	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	assert.Nil(t, lock, "expected lock to be released")

	// Verify check run updated to neutral
	select {
	case cr := <-checkRuns:
		assert.Equal(t, "completed", cr.Status)
		assert.Equal(t, "neutral", cr.Conclusion)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for check run")
	}
}

func TestE2EApplyConfirmExecutesApply(t *testing.T) {
	dbName := "webhook_confirm_apply"
	svc := setupE2EService(t, dbName)

	// Seed a check record (simulating a prior plan that created the check run)
	seedCheck(t, svc, dbName, "staging", "action_required")

	// Acquire a lock from this PR (simulating a prior `apply` command)
	err := svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: "mysql",
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		Owner:        "octocat/hello-world#1",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply-confirm -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "apply-confirm started")

	// The apply may complete synchronously (instant DDL) or asynchronously.
	// For a CREATE TABLE, it completes fast — the sync handler catches it and
	// posts the summary directly without an "In Progress" comment.
	//
	// Wait for either "Schema Change In Progress" or "Schema Change Applied/Failed"
	// as the first comment.
	select {
	case body := <-result.comments:
		hasProgress := strings.Contains(body, "Schema Change In Progress")
		hasComplete := strings.Contains(body, "Schema Change Applied")
		hasFailed := strings.Contains(body, "Schema Change Failed")
		assert.True(t, hasProgress || hasComplete || hasFailed,
			"expected progress or summary comment, got: %s", body[:min(len(body), 200)])

		// If we got a progress comment, wait for the summary too
		if hasProgress {
			select {
			case summary := <-result.comments:
				assert.True(t,
					strings.Contains(summary, "Schema Change Applied") || strings.Contains(summary, "Schema Change Failed"),
					"expected summary comment")
			case <-time.After(60 * time.Second):
				t.Fatal("timed out waiting for summary comment")
			}
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for any apply comment")
	}

	// Verify apply was created in storage
	applies, err := svc.Storage().Applies().GetByPR(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	var ourApply *storage.Apply
	for _, a := range applies {
		if a.Database == dbName {
			ourApply = a
			break
		}
	}
	require.NotNil(t, ourApply, "expected an apply record for database %s", dbName)
	assert.Equal(t, dbName, ourApply.Database)
	assert.Equal(t, "mysql", ourApply.DatabaseType)
	assert.Equal(t, "staging", ourApply.Environment)
	assert.Contains(t, ourApply.Caller, "github:testuser@octocat/hello-world#1")

	// After terminal state, the check run should be updated in storage.
	require.Eventually(t, func() bool {
		check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", dbName)
		if err != nil || check == nil {
			return false
		}
		return check.Conclusion == "success"
	}, 30*time.Second, 200*time.Millisecond,
		"expected check to transition to success after apply completion")
}

func TestE2EUnlockNoLocks(t *testing.T) {
	dbName := "webhook_unlock_none"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	comments := make(chan string, 10)
	reactions := make(chan string, 10)

	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Content string `json:"content"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		reactions <- body.Content
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot unlock",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-comments:
		assert.Contains(t, body, "No Locks Found")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for no-locks comment")
	}
}

func TestE2EApplySamePRActiveApply(t *testing.T) {
	dbName := "webhook_apply_active"
	svc := setupE2EService(t, dbName)

	// Acquire a lock from this PR
	err := svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: "mysql",
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		Owner:        "octocat/hello-world#1",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	// Get the lock to obtain its ID
	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)

	// Create a non-terminal apply record linked to this lock
	_, err = svc.Storage().Applies().Create(t.Context(), &storage.Apply{
		ApplyIdentifier: "apply-active-test-001",
		LockID:          lock.ID,
		PlanID:          0,
		Database:        dbName,
		DatabaseType:    "mysql",
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		Environment:     "staging",
		Engine:          "spirit",
		State:           "running",
	})
	require.NoError(t, err)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Apply Already In Progress")
		assert.Contains(t, body, "apply-active-test-001")
		assert.Contains(t, body, "running")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for active-apply comment")
	}
}

func TestE2EApplyConfirmLockConflict(t *testing.T) {
	dbName := "webhook_confirm_conflict"
	svc := setupE2EService(t, dbName)

	// Acquire a lock from a different PR
	err := svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: "mysql",
		Repository:   "other-org/other-repo",
		PullRequest:  42,
		Owner:        "other-org/other-repo#42",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply-confirm -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Apply Blocked")
		assert.Contains(t, body, "other-org/other-repo#42")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for blocked comment")
	}
}

func TestE2EApplyConfirmNoChanges(t *testing.T) {
	dbName := "webhook_confirm_nochange"
	svc := setupE2EService(t, dbName)

	// Pre-create the table so there are no changes when re-planning
	seedTargetTable(t, dbName,
		"CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")

	// Acquire a lock from this PR (simulating a prior `apply` command)
	err := svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: "mysql",
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		Owner:        "octocat/hello-world#1",
	})
	require.NoError(t, err)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply-confirm -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	// Should post "no changes" and release lock
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "No Changes Detected")
		assert.Contains(t, body, "already up to date")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for no-changes comment")
	}

	// Lock release happens asynchronously after the comment is posted —
	// poll until the goroutine completes the ForceRelease call.
	require.Eventually(t, func() bool {
		lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
		return err == nil && lock == nil
	}, 5*time.Second, 100*time.Millisecond, "expected lock to be released after no-changes confirm")
}

func TestE2EUnlockBlockedByActiveApply(t *testing.T) {
	dbName := "webhook_unlock_blocked"
	svc := setupE2EService(t, dbName)

	// Acquire a lock from this PR
	err := svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: "mysql",
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		Owner:        "octocat/hello-world#1",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	// Get the lock to obtain its ID
	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)

	// Create a non-terminal apply record
	_, err = svc.Storage().Applies().Create(t.Context(), &storage.Apply{
		ApplyIdentifier: "apply-blocked-unlock-001",
		LockID:          lock.ID,
		PlanID:          0,
		Database:        dbName,
		DatabaseType:    "mysql",
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		Environment:     "staging",
		Engine:          "spirit",
		State:           "running",
	})
	require.NoError(t, err)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	comments := make(chan string, 10)
	reactions := make(chan string, 10)

	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Content string `json:"content"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		reactions <- body.Content
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot unlock",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-comments:
		assert.Contains(t, body, "Cannot Unlock")
		assert.Contains(t, body, "apply-blocked-unlock-001")
		assert.Contains(t, body, "running")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for cannot-unlock comment")
	}

	// Lock should still be held
	existingLock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	assert.NotNil(t, existingLock, "expected lock to still be held")
}

func TestE2EApplyStaleLockReacquire(t *testing.T) {
	dbName := "webhook_apply_stale"
	svc := setupE2EService(t, dbName)

	// Acquire a stale lock from the same PR (no active applies)
	err := svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: "mysql",
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		Owner:        "octocat/hello-world#1",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	// Should succeed: release stale lock, re-plan, acquire new lock, post confirmation
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Schema Change Plan (Apply)")
		assert.Contains(t, body, "Lock acquired by")
		assert.Contains(t, body, "schemabot apply-confirm -e staging")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for apply plan comment")
	}

	// Verify a lock is still held (the new one)
	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock, "expected lock to be re-acquired")
	assert.Equal(t, "octocat/hello-world#1", lock.Owner)
}

// TestE2EApplyProductionBlockedByStagingFirst verifies that production apply is
// blocked when staging has unapplied changes.
func TestE2EApplyProductionBlockedByStagingFirst(t *testing.T) {
	dbName := "webhook_staging_first"
	svc := setupE2EService(t, dbName)

	// Seed a staging check that is NOT success (action_required — unapplied changes)
	seedCheck(t, svc, dbName, "staging", "action_required")

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\nenvironments:\n  - staging\n  - production\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	h := newE2EHandler(t, svc, client)

	// Try to apply production — should be blocked
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e production",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Apply Blocked")
		assert.Contains(t, body, "Staging")
		assert.Contains(t, body, "schemabot apply -e staging")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for blocked comment")
	}
}

// TestE2EApplyProductionAllowedWhenStagingSuccess verifies that production apply
// is allowed when staging check is success.
func TestE2EApplyProductionAllowedWhenStagingSuccess(t *testing.T) {
	dbName := "webhook_staging_ok"
	svc := setupE2EService(t, dbName)

	// Seed a staging check that IS success
	seedCheck(t, svc, dbName, "staging", "success")

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\nenvironments:\n  - staging\n  - production\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	h := newE2EHandler(t, svc, client)

	// Try to apply production — should be allowed (staging is success)
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e production",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "apply started")

	// Should NOT get a "blocked" message — the staging-first check passed.
	// The apply may fail for other reasons (e.g., no production deployment in test)
	// but the point is it wasn't blocked by staging-first enforcement.
	select {
	case body := <-result.comments:
		assert.NotContains(t, body, "Apply Blocked",
			"production apply should not be blocked when staging is success")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for comment")
	}

	// Clean up lock
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})
}

// TestE2EApplyThreeEnvEnforcement verifies that checkPriorEnvironments checks ALL
// prior environments, not just the immediately previous one. Uses 3 environments
// (sandbox → staging → production) and calls checkPriorEnvironments directly
// (bypassing config validation which currently restricts to staging/production).
func TestE2EApplyThreeEnvEnforcement(t *testing.T) {
	dbName := "webhook_three_env"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// Capture comments
	comments := make(chan string, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})

	h := newE2EHandler(t, svc, client)
	envs := []string{"sandbox", "staging", "production"}

	// Case 1: production blocked when sandbox is action_required
	seedCheck(t, svc, dbName, "sandbox", "action_required")

	blocked := h.checkPriorEnvironments(t.Context(), "octocat/hello-world", 1,
		dbName, "mysql", "production", envs, 1, "testuser")
	assert.True(t, blocked, "production should be blocked when sandbox is action_required")

	select {
	case body := <-comments:
		assert.Contains(t, body, "sandbox")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for blocked comment (case 1)")
	}

	// Case 2: production blocked when sandbox is success but staging is action_required
	seedCheck(t, svc, dbName, "sandbox", "success")
	seedCheck(t, svc, dbName, "staging", "action_required")

	blocked = h.checkPriorEnvironments(t.Context(), "octocat/hello-world", 1,
		dbName, "mysql", "production", envs, 1, "testuser")
	assert.True(t, blocked, "production should be blocked when staging is action_required")

	select {
	case body := <-comments:
		assert.Contains(t, body, "staging")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for blocked comment (case 2)")
	}

	// Case 3: production allowed when both sandbox and staging are success
	seedCheck(t, svc, dbName, "staging", "success")

	blocked = h.checkPriorEnvironments(t.Context(), "octocat/hello-world", 1,
		dbName, "mysql", "production", envs, 1, "testuser")
	assert.False(t, blocked, "production should not be blocked when all prior envs are success")

	// Case 4: staging only requires sandbox (not production)
	seedCheck(t, svc, dbName, "sandbox", "action_required")

	blocked = h.checkPriorEnvironments(t.Context(), "octocat/hello-world", 1,
		dbName, "mysql", "staging", envs, 1, "testuser")
	assert.True(t, blocked, "staging should be blocked when sandbox is action_required")

	// Case 5: sandbox (first env) is never blocked
	blocked = h.checkPriorEnvironments(t.Context(), "octocat/hello-world", 1,
		dbName, "mysql", "sandbox", envs, 1, "testuser")
	assert.False(t, blocked, "sandbox (first env) should never be blocked")
}

// seedTargetTable creates a table in the target database.
func seedTargetTable(t *testing.T, dbName, ddl string) {
	t.Helper()

	appDSN := strings.Replace(e2eTargetDSN, "/target_test", "/"+dbName, 1) + "&multiStatements=true"
	db, err := sql.Open("mysql", appDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	_, err = db.ExecContext(t.Context(), ddl)
	require.NoError(t, err)
}
