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
	logs := runListenOnlyAutoPlan(t, []string{"staging", "production"}, nil, map[string]string{"users.sql": baseSchemaUsersTable})

	entry := findLogEntry(t, logs, listenOnlyPlanReadMessage)
	assert.ElementsMatch(t, []any{"staging", "production"}, entry["environments"])
	assert.EqualValues(t, 2, entry["schema_files"], "users.sql, once for each environment")

	// The base branch changed the schema after the PR branched. A plan warns
	// that its comment says so and that apply is blocked.
	stale := findLogEntry(t, logs, "plan is of a branch missing newer schema changes from its base branch; the plan comment says so and apply is blocked until the branch has them")
	assert.Equal(t, "DEBUG", stale["level"])
	assert.Equal(t, true, stale["listen_only"])
}

// A deployment without allowed_environments serves every environment, so it
// has none to name. It reads the environments of its promotion order, and a
// repository that keeps one directory per environment is read from each
// environment's own directory. Reading the config directory alone would find
// the environment directories as namespaces and skip the files one level
// below them.
func TestE2EAutoPlanOnListenOnlyRepositoryReadsEnvironmentDirectoriesWhenEnvironmentsAreUnscoped(t *testing.T) {
	logs := runListenOnlyAutoPlan(t, nil, []string{"staging", "production"}, map[string]string{
		"staging/listen_only_orders/users.sql":    baseSchemaUsersTable,
		"production/listen_only_orders/users.sql": baseSchemaUsersTable,
	})

	entry := findLogEntry(t, logs, listenOnlyPlanReadMessage)
	assert.ElementsMatch(t, []any{"staging", "production"}, entry["environments"])
	assert.EqualValues(t, 2, entry["schema_files"], "each environment's users.sql, from its own directory")
}

// runListenOnlyAutoPlan sends a synchronize webhook for a listen-only
// repository whose database the deployment does not register, waits for the
// auto-plan, and checks what every such auto-plan must do: write nothing,
// read schema blobs at the head, compare against the base branch once, and
// log nothing about a plan comment or an apply block as a warning. It returns
// the handler's logs, at debug level. environmentDirectories names
// the directories under schema/ that the repository keeps for environments,
// each holding the listen_only_orders namespace.
func runListenOnlyAutoPlan(t *testing.T, allowedEnvironments, environmentDirectories []string, schemaSQL map[string]string) *bytes.Buffer {
	t.Helper()
	mux := http.NewServeMux()
	for _, env := range environmentDirectories {
		dir := "schema/" + env
		mux.HandleFunc("GET /repos/octocat/hello-world/contents/"+dir, func(w http.ResponseWriter, _ *http.Request) {
			require.NoError(t, json.NewEncoder(w).Encode([]*gh.RepositoryContent{{
				Type: new("dir"),
				Name: new("listen_only_orders"),
				Path: new(dir + "/listen_only_orders"),
			}}))
		})
	}
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

	result := setupFakeGitHubForPlan(t, mux, schemaSQL, "database: listen_only_orders\ntype: mysql\n", "listen_only_orders")
	var compares atomic.Int32
	registerStaleBaseSchemaComparisonWithHook(t, mux, result, func() { compares.Add(1) })

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	svc := api.New(&emptyStorage{}, &api.ServerConfig{
		AllowedEnvironments: allowedEnvironments,
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
	assert.Equal(t, int32(1), compares.Load(), "one base-branch comparison covers every environment")

	// A plan logs the base comparison's findings as warnings about its
	// comment and a blocked apply. Neither exists on a listen-only
	// repository, so the findings are debug lines.
	for _, logged := range logEntries(&logs) {
		msg, _ := logged["msg"].(string)
		if strings.HasPrefix(msg, "plan comment will say the base branch could not be checked") || strings.HasPrefix(msg, "plan is of a branch missing newer schema changes") {
			assert.Equal(t, "DEBUG", logged["level"], "a listen-only read must not warn about a plan comment or an apply block: %s", msg)
		}
	}

	entry := findLogEntry(t, &logs, listenOnlyPlanReadMessage)
	assert.Equal(t, "listen_only_orders", entry["database"])
	return &logs
}

const listenOnlyPlanReadMessage = "read a listen-only repository's plan inputs for a database this deployment does not register; nothing was planned or posted"

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
	for _, entry := range logEntries(logs) {
		if entry["msg"] == msg {
			return entry
		}
	}
	t.Fatalf("no log entry %q in:\n%s", msg, logs.String())
	return nil
}

// logEntries parses every JSON log line in logs.
func logEntries(logs *bytes.Buffer) []map[string]any {
	var entries []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(logs.String()), "\n") {
		var entry map[string]any
		if json.Unmarshal([]byte(line), &entry) == nil {
			entries = append(entries, entry)
		}
	}
	return entries
}
