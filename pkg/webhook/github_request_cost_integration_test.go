//go:build integration

package webhook

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
)

// A pull request in a large repository changes one schema file and hundreds of
// files outside any schema directory, each by a single line. SchemaBot plans
// it with exactly the GitHub requests it uses when the PR changes one file
// outside its schema directory, so a monorepo's routine edits cannot multiply
// the requests every delivery makes and exhaust the App's rate limit for every
// repository it serves (AV-15).
func TestE2EAutoPlanGitHubRequestsDoNotGrowWithNonSchemaFiles(t *testing.T) {
	const dbName = "webhook_github_cost"
	var few, many map[string]int
	t.Run("one non-schema file", func(t *testing.T) {
		few = autoPlanGitHubRequests(t, dbName, 1)
	})
	t.Run("many non-schema files", func(t *testing.T) {
		many = autoPlanGitHubRequests(t, dbName, 400)
	})
	require.NotEmpty(t, few, "the plan flow made GitHub requests to count")
	assert.Equal(t, few, many, "GitHub requests by method and path, for one non-schema file vs many")
}

// autoPlanGitHubRequests auto-plans a PR that adds one schema file and edits
// nonSchemaFiles files outside the schema directory by one line each, and
// returns every GitHub request the delivery made, counted by method and path.
func autoPlanGitHubRequests(t *testing.T, dbName string, nonSchemaFiles int) map[string]int {
	t.Helper()
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	requests := &githubRequestCounter{counts: make(map[string]int)}
	server := httptest.NewServer(requests.wrap(mux))
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	var err error
	client.BaseURL, err = url.Parse(server.URL + "/")
	require.NoError(t, err)

	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}
	prFiles := []*gh.CommitFile{{
		Filename: new(schemaFixturePath(dbName, "users.sql")),
		Status:   new("added"),
	}}
	var otherFiles []string
	for i := range nonSchemaFiles {
		// A one-line edit several directories deep is the most common change
		// in a large repository, and the one a per-file lookup costs most for.
		path := fmt.Sprintf("services/app/src/main/java/com/example/module%03d/internal/Handler.java", i)
		otherFiles = append(otherFiles, path)
		prFiles = append(prFiles, &gh.CommitFile{
			Filename:  new(path),
			Status:    new("modified"),
			Additions: new(1),
			Deletions: new(1),
			Changes:   new(2),
			Patch:     new("@@ -1 +1 @@\n-old\n+new"),
		})
	}
	configs := map[string]string{"schema/schemabot.yaml": fmt.Sprintf("database: %s\ntype: mysql\n", dbName)}
	result := setupFakeGitHubForPlanInRepository(t, mux, schemaFiles, configs, dbName, prFiles, otherFiles)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	h := NewHandler(svc, &fakeClientFactory{client: ghclient.NewInstallationClient(client, logger)}, nil, logger)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "opened"}, nil))
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "auto-plan started")

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "CREATE TABLE")
	case <-time.After(webhookIntegrationCheckRunDeadline):
		require.Fail(t, "timed out waiting for the auto-plan comment")
	}
	select {
	case cr := <-result.checkRuns:
		assert.Equal(t, "completed", cr.Status)
	case <-time.After(webhookIntegrationCheckRunDeadline):
		require.Fail(t, "timed out waiting for the auto-plan check run")
	}

	ctx, cancel := context.WithTimeout(t.Context(), webhookIntegrationCheckRunDeadline)
	defer cancel()
	h.DrainInProcessWebhookWork(ctx)
	require.NoError(t, ctx.Err(), "auto-plan work was still running when the requests were counted")
	return requests.snapshot()
}

// githubRequestCounter counts the requests a fake GitHub server receives, by
// method and path.
type githubRequestCounter struct {
	mu     sync.Mutex
	counts map[string]int
}

func (c *githubRequestCounter) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		c.counts[r.Method+" "+r.URL.Path]++
		c.mu.Unlock()
		next.ServeHTTP(w, r)
	})
}

func (c *githubRequestCounter) snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Clone(c.counts)
}
