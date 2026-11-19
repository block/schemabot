//go:build integration

package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
)

// A deployment listens to a repository it does not serve, to measure what
// serving it would cost, so the repository's database is not in its registry.
// A pull request that changes the database's schema still reads what an
// auto-plan would read: the schema files at the head for each environment the
// deployment serves, and one base-branch comparison for all of them. Nothing
// is planned and nothing is written: no comment, no Check Run.
func TestE2EAutoPlanOnListenOnlyRepositoryReadsPlanInputsForUnregisteredDatabase(t *testing.T) {
	mux := http.NewServeMux()
	var mu sync.Mutex
	var reads, writes []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := r.Method + " " + r.URL.Path
		mu.Lock()
		if r.Method == http.MethodGet {
			reads = append(reads, request)
		} else {
			writes = append(writes, request)
		}
		mu.Unlock()
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	client := gh.NewClient(nil)
	var err error
	client.BaseURL, err = url.Parse(server.URL + "/")
	require.NoError(t, err)

	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": baseSchemaUsersTable},
		"database: listen_only_orders\ntype: mysql\n", "listen_only_orders")
	var compares atomic.Int32
	registerStaleBaseSchemaComparisonWithHook(t, mux, result, func() { compares.Add(1) })

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	svc := api.New(&emptyStorage{}, &api.ServerConfig{
		AllowedEnvironments: []string{"staging", "production"},
		Repos:               map[string]api.RepoConfig{"octocat/hello-world": {ListenOnly: true}},
	}, nil, logger)
	h := NewHandler(svc, &fakeClientFactory{client: ghclient.NewInstallationClientWithSlug(client, logger, "schemabot")}, nil, logger)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "synchronize", beforeSHA: "oldsha", headSHA: "abc123"}, nil))
	require.Equal(t, http.StatusOK, rr.Code)
	ctx, cancel := context.WithTimeout(t.Context(), webhookIntegrationPollDeadline)
	defer cancel()
	h.DrainInProcessWebhookWork(ctx)
	require.NoError(t, ctx.Err(), "the auto-plan was still running at the deadline")

	mu.Lock()
	defer mu.Unlock()
	assert.Empty(t, writes, "a listen-only repository gets no comment, reaction or Check Run")
	assert.Empty(t, result.comments)
	assert.Empty(t, result.checkRuns)
	assert.True(t, containsPrefix(reads, "GET /repos/octocat/hello-world/git/blobs/"),
		"the schema files at the head are read, as a plan would read them: %v", reads)
	assert.Equal(t, int32(1), compares.Load(), "one base-branch comparison covers both environments")

	entry := findLogEntry(t, &logs, "read a listen-only repository's plan inputs for a database this deployment does not register; nothing was planned or posted")
	assert.Equal(t, "listen_only_orders", entry["database"])
	assert.ElementsMatch(t, []any{"staging", "production"}, entry["environments"])
	assert.EqualValues(t, 2, entry["schema_files"], "users.sql, once for each environment")
}

func containsPrefix(requests []string, prefix string) bool {
	for _, request := range requests {
		if strings.HasPrefix(request, prefix) {
			return true
		}
	}
	return false
}

// findLogEntry returns the JSON log entry whose message is msg, failing the
// test when there is none.
func findLogEntry(t *testing.T, logs *bytes.Buffer, msg string) map[string]any {
	t.Helper()
	for line := range strings.SplitSeq(strings.TrimSpace(logs.String()), "\n") {
		var entry map[string]any
		if json.Unmarshal([]byte(line), &entry) == nil && entry["msg"] == msg {
			return entry
		}
	}
	t.Fatalf("no log entry %q in:\n%s", msg, logs.String())
	return nil
}
