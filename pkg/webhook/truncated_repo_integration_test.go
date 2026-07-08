//go:build integration

// Webhook integration tests for plan commands on repositories so large that
// GitHub truncates their recursive tree listing.

package webhook

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
)

// newE2EHandlerWithConfigDirHints mirrors newE2EHandler but wires the
// server-configured schema directory hints into the GitHub client, matching
// the production serve wiring so config discovery can fall back to the
// configured directories when the repository tree is truncated.
func newE2EHandlerWithConfigDirHints(t *testing.T, svc *api.Service, client *gh.Client) *Handler {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	installClient.SetConfigDirHints(svc.Config().SchemaDirHintsForRepo)
	factory := &fakeClientFactory{client: installClient}
	return NewHandler(svc, factory, nil, logger)
}

// setupFakeGitHubForPlanOnTruncatedRepo simulates the GitHub API of a
// monorepo whose whole-repo recursive tree listing is truncated: recursive
// fetches of the head commit return truncated with no entries, while shallow
// (per-level) fetches resolve, and the schema directory's own subtree lists
// completely. The PR diff contains no schema files, so config discovery must
// go through the whole-repo scan rather than the changed-file probes.
func setupFakeGitHubForPlanOnTruncatedRepo(t *testing.T, mux *http.ServeMux, schemaSQL map[string]string, schemabotConfig, ns string) *planFlowResult {
	t.Helper()
	return setupFakeGitHubForPlanOnTruncatedRepoWithPRState(t, mux, schemaSQL, schemabotConfig, ns, "open")
}

// setupFakeGitHubForPlanOnTruncatedRepoWithPRState is
// setupFakeGitHubForPlanOnTruncatedRepo with the PR's lifecycle state under
// test control, so plan-command behavior on closed PRs can be exercised.
func setupFakeGitHubForPlanOnTruncatedRepoWithPRState(t *testing.T, mux *http.ServeMux, schemaSQL map[string]string, schemabotConfig, ns, prState string) *planFlowResult {
	t.Helper()

	result := &planFlowResult{
		comments:  make(chan string, 10),
		reactions: make(chan string, 10),
		checkRuns: make(chan checkRunCapture, 10),
	}

	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			State: new(prState),
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
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]*gh.CommitFile{{
			Filename: new("docs/readme.md"),
			Status:   new("modified"),
		}})
	})

	// The schema directory's subtree, with entry paths relative to "schema".
	subtreeEntries := []*gh.TreeEntry{{
		Path: new("schemabot.yaml"),
		Mode: new("100644"),
		Type: new("blob"),
		SHA:  new("configsha001"),
		Size: new(len(schemabotConfig)),
	}}
	blobContents := map[string]string{"configsha001": schemabotConfig}
	blobIndex := 0
	for name, content := range schemaSQL {
		sha := fmt.Sprintf("blobsha%03d", blobIndex)
		blobIndex++
		blobContents[sha] = content
		subtreeEntries = append(subtreeEntries, &gh.TreeEntry{
			Path: new(ns + "/" + name),
			Mode: new("100644"),
			Type: new("blob"),
			SHA:  new(sha),
			Size: new(len(content)),
		})
	}

	// Head commit tree: the recursive listing is truncated; the shallow
	// listing resolves the top-level "schema" directory for the subtree walk.
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/abc123", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("recursive") == "1" {
			_ = json.NewEncoder(w).Encode(gh.Tree{
				SHA:       new("abc123"),
				Entries:   []*gh.TreeEntry{},
				Truncated: new(true),
			})
			return
		}
		_ = json.NewEncoder(w).Encode(gh.Tree{
			SHA: new("abc123"),
			Entries: []*gh.TreeEntry{{
				Path: new("schema"),
				Mode: new("040000"),
				Type: new("tree"),
				SHA:  new("treeschema001"),
			}},
			Truncated: new(false),
		})
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/treeschema001", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.Tree{
			SHA:       new("treeschema001"),
			Entries:   subtreeEntries,
			Truncated: new(false),
		})
	})

	mux.HandleFunc("GET /repos/octocat/hello-world/git/blobs/", func(w http.ResponseWriter, r *http.Request) {
		sha := r.URL.Path[len("/repos/octocat/hello-world/git/blobs/"):]
		content, ok := blobContents[sha]
		if !ok {
			http.NotFound(w, r)
			return
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(content))
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"sha":"%s","content":"%s","encoding":"base64","size":%d}`, sha, encoded, len(content))
	})

	// Contents API serves only the config file. Directory listings 404 so
	// schema files load through the git-tree path, like a repository whose
	// schema directory the Contents walk cannot serve.
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/", func(w http.ResponseWriter, r *http.Request) {
		filePath := r.URL.Path[len("/repos/octocat/hello-world/contents/"):]
		if filePath == "schema/schemabot.yaml" {
			_ = json.NewEncoder(w).Encode(gh.RepositoryContent{
				Name:     new("schemabot.yaml"),
				Path:     new("schema/schemabot.yaml"),
				Content:  new(base64.StdEncoding.EncodeToString([]byte(schemabotConfig))),
				Encoding: new("base64"),
			})
			return
		}
		http.NotFound(w, r)
	})

	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		result.comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Content string `json:"content"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		result.reactions <- body.Content
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		result.checkRuns <- body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})
	mux.HandleFunc("PATCH /repos/octocat/hello-world/check-runs/", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		result.checkRuns <- body
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	registerCheckStatusRESTHandlersForAnyRef(mux, func() []checkStatusNode { return nil })

	return result
}

// TestE2EPlanCommandOnTruncatedRepoDiscoversConfigViaConfiguredDirs exercises
// database-scoped discovery on a huge monorepo: a user comments
// `schemabot plan -e staging -d <db>` on a PR whose diff does not touch that
// database's schema, in a repository whose recursive tree listing GitHub
// truncates. The whole-repo config scan must recover via the
// server-configured schema directories (databases.<db>.allowed_dirs) and the
// schema files must load through the schema directory's own subtree, so the
// user gets a real plan instead of a failed command they cannot act on.
func TestE2EPlanCommandOnTruncatedRepoDiscoversConfigViaConfiguredDirs(t *testing.T) {
	dbName := "webhook_truncated_repo_plan"
	svc := setupE2EService(t, dbName)

	serverConfig := svc.Config()
	dbConfig := serverConfig.Databases[dbName]
	dbConfig.AllowedDirs = []string{"schema"}
	serverConfig.Databases[dbName] = dbConfig

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlanOnTruncatedRepo(t, mux, schemaFiles, schemabotConfig, dbName)
	h := newE2EHandlerWithConfigDirHints(t, svc, client)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: fmt.Sprintf("schemabot plan -e staging -d %s", dbName),
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.NotContains(t, body, "truncated repository tree")
		assert.NotContains(t, body, "Plan Failed")
		assert.Contains(t, body, "CREATE TABLE")
		assert.Contains(t, body, "users")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for plan comment")
	}
}

// TestE2EPlanCommandOnTruncatedRepoWithoutConfiguredDirsFailsClosed verifies
// the fail-closed boundary of the truncated-tree fallback: when the server
// config carries no schema directories for the repository, a database-scoped
// plan's whole-repo scan has nothing safe to probe and the command must fail
// with the truncation error rather than report that no config exists.
func TestE2EPlanCommandOnTruncatedRepoWithoutConfiguredDirsFailsClosed(t *testing.T) {
	dbName := "webhook_truncated_repo_no_dirs"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlanOnTruncatedRepo(t, mux, nil, schemabotConfig, dbName)
	h := newE2EHandlerWithConfigDirHints(t, svc, client)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: fmt.Sprintf("schemabot plan -e staging -d %s", dbName),
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Plan Failed")
		assert.Contains(t, body, "truncated repository tree")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for failure comment")
	}
}
