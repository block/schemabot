//go:build integration

package webhook

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync/atomic"
	"testing"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
)

// prFetchCountingTransport counts GET /repos/octocat/hello-world/pulls/1
// requests that flow through the InstallationClient. Layered between the
// go-github client and the fake GitHub server so it observes every
// upstream FetchPullRequest call without disturbing payloads.
type prFetchCountingTransport struct {
	base  http.RoundTripper
	count *atomic.Int32
}

func (t *prFetchCountingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodGet && req.URL.Path == "/repos/octocat/hello-world/pulls/1" {
		t.count.Add(1)
	}
	return t.base.RoundTrip(req)
}

// TestUnlockHandlerDedupesPRFetchAcrossLocks proves that a single
// "schemabot unlock" invocation collapses every FetchPullRequest call
// fanned out by updateCheckRunAfterUnlock — one per released lock — to a
// single upstream GitHub call via the WithPRInfoCache wrap on the
// handler's ctx. Without that wrap, every released lock would issue its
// own /pulls fetch.
func TestUnlockHandlerDedupesPRFetchAcrossLocks(t *testing.T) {
	dbA := "webhook_unlock_dedup_a"
	dbB := "webhook_unlock_dedup_b"
	svc := setupE2EService(t, dbA)

	for _, db := range []string{dbA, dbB} {
		require.NoError(t, svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
			DatabaseName: db,
			DatabaseType: "mysql",
			Repository:   "octocat/hello-world",
			PullRequest:  1,
			Owner:        "octocat/hello-world#1",
		}))
	}
	t.Cleanup(func() {
		for _, db := range []string{dbA, dbB} {
			_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), db, "mysql")
		}
	})

	var prFetchCalls atomic.Int32
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{Ref: new("feature-branch"), SHA: new("abc123")},
			Base: &gh.PullRequestBranch{Ref: new("main"), SHA: new("def456")},
			User: &gh.User{Login: new("testuser")},
		})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	httpClient := &http.Client{Transport: &prFetchCountingTransport{base: http.DefaultTransport, count: &prFetchCalls}}
	client := gh.NewClient(httpClient)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}
	h := NewHandler(svc, factory, nil, logger)

	// Call the handler entry point synchronously so every fan-out into
	// updateCheckRunAfterUnlock resolves within the same WithPRInfoCache
	// scope before the test asserts.
	h.handleUnlockCommand("octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

	for _, db := range []string{dbA, dbB} {
		l, err := svc.Storage().Locks().Get(t.Context(), db, "mysql")
		require.NoError(t, err)
		assert.Nil(t, l, "lock should be released for %s", db)
	}

	assert.Equal(t, int32(1), prFetchCalls.Load(),
		"handleUnlockCommand must dedupe FetchPullRequest across all per-lock check-run cleanups within one invocation")
}

// TestRollbackConfirmHandlerDoesNotFetchPRForNoLock proves that
// "schemabot rollback-confirm" does not discover schema files from the current
// PR when there is no PR-owned rollback lock. Confirmation must execute only a
// lock-pinned rollback plan created by a preceding rollback command.
func TestRollbackConfirmHandlerDoesNotFetchPRForNoLock(t *testing.T) {
	dbName := "webhook_rbconfirm_dedup"
	svc := setupE2EService(t, dbName)

	var prFetchCalls atomic.Int32
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	schemaSQL := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}
	schemabotConfig := "database: " + dbName + "\ntype: mysql\n"
	_ = setupFakeGitHubForPlan(t, mux, schemaSQL, schemabotConfig, dbName)

	httpClient := &http.Client{Transport: &prFetchCountingTransport{base: http.DefaultTransport, count: &prFetchCalls}}
	client := gh.NewClient(httpClient)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}
	h := NewHandler(svc, factory, nil, logger)

	applyIdentifier := "apply_rbconfirm_dedup"
	_, err := svc.Storage().Applies().Create(t.Context(), &storage.Apply{
		ApplyIdentifier: applyIdentifier,
		Database:        dbName,
		DatabaseType:    "mysql",
		Environment:     "staging",
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		State:           "completed",
		Engine:          "spirit",
	})
	require.NoError(t, err)

	// Call the handler entry point synchronously. No lock is acquired, so the
	// handler must bail out without fetching the PR or discovering current schema
	// files.
	h.handleRollbackConfirmCommand("octocat/hello-world", 1, "staging", 12345, "testuser", CommandResult{
		Environment: "staging",
	})

	assert.Equal(t, int32(0), prFetchCalls.Load(),
		"handleRollbackConfirmCommand must not fetch the PR when no rollback lock is pending")
}
