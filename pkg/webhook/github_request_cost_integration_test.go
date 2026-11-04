//go:build integration

package webhook

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A pull request in a large repository edits hundreds of files outside any
// schema directory, each by a single line. Every way SchemaBot plans that PR
// makes exactly the GitHub requests it makes when the PR edits one such file,
// so a monorepo's routine edits cannot multiply the requests every delivery
// makes and exhaust the App's rate limit for every repository it serves
// (AV-15). The paths covered are an auto-plan when the PR opens and when a push
// lands, a plan command, and a plan command in a repository whose tree GitHub
// truncates, where config discovery falls back to the configured schema
// directories.
func TestE2EGitHubRequestsDoNotGrowWithNonSchemaFiles(t *testing.T) {
	const dbName = "webhook_github_cost"
	for _, delivery := range githubCostDeliveries(dbName) {
		t.Run(delivery.name, func(t *testing.T) {
			var few, many map[string]int
			t.Run("one non-schema file", func(t *testing.T) {
				few = githubRequestsToPlan(t, dbName, delivery, 1)
			})
			t.Run("many non-schema files", func(t *testing.T) {
				many = githubRequestsToPlan(t, dbName, delivery, 400)
			})
			require.NotEmpty(t, few, "the plan flow made GitHub requests to count")
			// The fake lists every changed file in one page, so the listing
			// request counts the same for one file and for four hundred. Any
			// difference, the listing included, is a request made per file.
			assert.Equal(t, few, many, "GitHub requests by method and path, for one non-schema file vs many")
		})
	}
}

// githubCostDelivery is one way a webhook delivery makes SchemaBot plan a PR.
type githubCostDelivery struct {
	name string
	// monorepo serves a repository too large for GitHub to list recursively.
	// The PR then changes no schema file, so the plan command has to find the
	// database's config through the server-configured schema directories.
	monorepo bool
	// request builds the delivery once the fake GitHub serves the PR's files.
	request func(t *testing.T, mux *http.ServeMux, result *planFlowResult, prFiles []*gh.CommitFile) *http.Request
}

func githubCostDeliveries(dbName string) []githubCostDelivery {
	planCommand := func(comment string) func(*testing.T, *http.ServeMux, *planFlowResult, []*gh.CommitFile) *http.Request {
		return func(t *testing.T, _ *http.ServeMux, _ *planFlowResult, _ []*gh.CommitFile) *http.Request {
			return buildWebhookRequest(t, webhookPayloadOpts{comment: comment, isPR: true}, nil)
		}
	}
	return []githubCostDelivery{
		{
			name: "auto-plan on open",
			request: func(t *testing.T, _ *http.ServeMux, _ *planFlowResult, _ []*gh.CommitFile) *http.Request {
				return buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "opened"}, nil)
			},
		},
		{
			name: "auto-plan on push",
			request: func(t *testing.T, mux *http.ServeMux, result *planFlowResult, prFiles []*gh.CommitFile) *http.Request {
				result.HeadSHAs = []string{"newsha"}
				registerCompareFiles(t, mux, "oldsha", "newsha", prFiles)
				registerFreshBaseComparison(t, mux, "newsha")
				return buildPRWebhookRequest(t, prWebhookPayloadOpts{
					action:    "synchronize",
					beforeSHA: "oldsha",
					headSHA:   "newsha",
					headRef:   "feature-branch",
				}, nil)
			},
		},
		{
			name:    "plan command",
			request: planCommand("schemabot plan -e staging"),
		},
		{
			name:     "plan command in a monorepo",
			monorepo: true,
			request:  planCommand(fmt.Sprintf("schemabot plan -e staging -d %s", dbName)),
		},
	}
}

// githubRequestsToPlan plans a PR that edits nonSchemaFiles files outside the
// schema directory by one line each through delivery, and returns every GitHub
// request the delivery made, counted by method and path. Outside a monorepo the
// PR also adds the database's one schema file.
func githubRequestsToPlan(t *testing.T, dbName string, delivery githubCostDelivery, nonSchemaFiles int) map[string]int {
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
	var prFiles []*gh.CommitFile
	if !delivery.monorepo {
		prFiles = append(prFiles, &gh.CommitFile{
			Filename: new(schemaFixturePath(dbName, "users.sql")),
			Status:   new("added"),
		})
	}
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

	var h *Handler
	if delivery.monorepo {
		result.TruncateRecursiveTrees.Store(true)
		serverConfig := svc.Config()
		dbConfig := serverConfig.Databases[dbName]
		dbConfig.AllowedDirs = []string{"schema"}
		serverConfig.Databases[dbName] = dbConfig
		h = newE2EHandlerWithConfigDirHints(t, svc, client)
	} else {
		h = newE2EHandler(t, svc, client)
	}

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, delivery.request(t, mux, result, prFiles))
	require.Equal(t, http.StatusOK, rr.Code)

	deadline := time.After(webhookIntegrationCheckRunDeadline)
	for planned := false; !planned; {
		select {
		case body := <-result.comments:
			planned = strings.Contains(body, "CREATE TABLE")
		case <-deadline:
			require.Fail(t, "timed out waiting for the plan comment")
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), webhookIntegrationCheckRunDeadline)
	defer cancel()
	h.DrainInProcessWebhookWork(ctx)
	require.NoError(t, ctx.Err(), "plan work was still running when the requests were counted")
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
