package webhook

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

// rawInfrastructureError is the kind of failure text that must never reach a
// PR comment: a dial failure naming an internal address.
const rawInfrastructureError = "dial tcp 10.1.2.3:3306: connect: connection refused"

const planControlRetryLine = "Run the command again; if it fails again, the SchemaBot server logs have the cause."

// requireNoInfrastructureDetail fails when a PR comment carries any part of
// rawInfrastructureError.
func requireNoInfrastructureDetail(t *testing.T, body string) {
	t.Helper()
	assert.NotContains(t, body, "10.1.2.3")
	assert.NotContains(t, body, "connection refused")
	assert.NotContains(t, body, "dial tcp")
}

// planControlApplyStore serves the one apply a control command looks up.
type planControlApplyStore struct {
	storage.ApplyStore
	apply *storage.Apply
	err   error
}

func (s *planControlApplyStore) GetByApplyIdentifier(context.Context, string) (*storage.Apply, error) {
	return s.apply, s.err
}

type planControlStorage struct {
	emptyStorage
	applies storage.ApplyStore
}

func (s *planControlStorage) Applies() storage.ApplyStore { return s.applies }

func planControlCommandResult() CommandResult {
	return CommandResult{ApplyID: "apply-123", Environment: "staging"}
}

// A control command whose apply cannot be read from storage tells the
// commenter which command did not run and what to do, and keeps the storage
// error, which names an internal address, in the server log.
func TestControlCommandApplyLookupFailureKeepsStorageErrorOutOfComment(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := recordComments(t, mux)
	st := &planControlStorage{applies: &planControlApplyStore{err: errors.New(rawInfrastructureError)}}
	h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

	h.handleStopCommand("octocat/hello-world", 1, 12345, "testuser", planControlCommandResult())

	body := requireComment(t, comments, "apply lookup failure comment")
	assert.Contains(t, body, "SchemaBot could not look up apply `apply-123`, so the stop command did not run. "+planControlRetryLine)
	assert.NotContains(t, body, "Failed to look up apply")
	requireNoInfrastructureDetail(t, body)
}

// servePlanPullRequest serves the reads a plan makes of an open pull request and
// its repository. The first read of the pull request succeeds and every later
// one fails with a bad gateway.
func servePlanPullRequest(t *testing.T, mux *http.ServeMux) {
	t.Helper()
	repoPath := "/repos/octocat/hello-world"
	var pullReads atomic.Int32
	mux.HandleFunc("GET "+repoPath, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"default_branch":"main"}`)
	})
	mux.HandleFunc("GET "+repoPath+"/git/ref/heads/main", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"object":{"sha":"def456"}}`)
	})
	mux.HandleFunc("GET "+repoPath+"/git/trees/def456", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"tree": []map[string]string{}}))
	})
	mux.HandleFunc("GET "+repoPath+"/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		if pullReads.Add(1) > 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"state": "open",
			"user":  map[string]any{"login": "author"},
			"head":  map[string]any{"sha": "abc123", "ref": "topic"},
			"base":  map[string]any{"sha": "def456", "ref": "main", "repo": map[string]any{"default_branch": "main"}},
		}))
	})
}

// servePlanDocsOnlyPullRequest serves a pull request in prState that changes
// one file that is not a SchemaBot input.
func servePlanDocsOnlyPullRequest(t *testing.T, mux *http.ServeMux, prState string) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"state": prState,
			"user":  map[string]any{"login": "author"},
			"head":  map[string]any{"sha": "abc123", "ref": "topic"},
			"base":  map[string]any{"sha": "def456", "ref": "main", "repo": map[string]any{"default_branch": "main"}},
		}))
	})
	serveLeaderPRFiles(t, mux, "docs/readme.md")
}

// servePlanOrdersSchema makes the pull request add a schema directory for the
// orders database, with its config and one table.
func servePlanOrdersSchema(t *testing.T, mux *http.ServeMux) {
	t.Helper()
	repoPath := "/repos/octocat/hello-world"
	schemaSQL := "CREATE TABLE `users` (`id` bigint unsigned NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`))"
	mux.HandleFunc("GET "+repoPath+"/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode([]map[string]string{{"filename": "schema/orders/users.sql", "status": "added"}}))
	})
	mux.HandleFunc("GET "+repoPath+"/contents/schema/orders/schemabot.yaml", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]string{
			"content":  base64.StdEncoding.EncodeToString([]byte("database: orders\ntype: mysql\n")),
			"encoding": "base64",
		}))
	})
	mux.HandleFunc("GET "+repoPath+"/git/trees/abc123", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"tree": []map[string]any{{
				"path": "schema/orders/users.sql", "mode": "100644", "type": "blob", "sha": "userssha", "size": len(schemaSQL),
			}},
			"truncated": false,
		}))
	})
	mux.HandleFunc("GET "+repoPath+"/git/blobs/userssha", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]string{
			"content":  base64.StdEncoding.EncodeToString([]byte(schemaSQL)),
			"encoding": "base64",
		}))
	})
}

// planControlServerConfig configures the orders database for staging, so a plan
// that names it passes the registry gate.
func planControlServerConfig() *api.ServerConfig {
	return &api.ServerConfig{
		Repos: map[string]api.RepoConfig{"octocat/hello-world": {EnableChecks: new(false)}},
		Databases: map[string]api.DatabaseConfig{"orders": {
			Type:         "mysql",
			Environments: map[string]api.EnvironmentConfig{"staging": {Deployment: "remote", Target: "orders"}},
		}},
	}
}

// A plan on a pull request with no schema files checks the stored check state
// before it finds a config. When that read fails, the comment says the plan did
// not start and what to do next, and the storage error stays in the server log.
func TestPlanReconciliationFailureKeepsErrorOutOfComment(t *testing.T) {
	newHandler := func(t *testing.T) (*Handler, chan string) {
		t.Helper()
		checks := &failAfterFirstCheckRead{err: errors.New(rawInfrastructureError)}
		h, mux, _ := newFoldHandler(t, planControlServerConfig(), checks)
		servePlanDocsOnlyPullRequest(t, mux, "open")
		comments := make(chan string, 2)
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))
		return h, comments
	}

	t.Run("single environment plan", func(t *testing.T) {
		h, comments := newHandler(t)

		h.handlePlanCommand(httptest.NewRecorder(), "octocat/hello-world", 1, "staging", "orders", "", "", 12345, "", "alice", 0)

		body := requireComment(t, comments, "plan reconciliation failure comment")
		assert.Contains(t, body, "SchemaBot could not check the schema files in this pull request for `orders`, so the plan did not start. "+planControlRetryLine)
		requireNoInfrastructureDetail(t, body)
	})

	t.Run("plan across environments", func(t *testing.T) {
		h, comments := newHandler(t)

		h.handleMultiEnvPlan("octocat/hello-world", 1, "", "", 12345, "alice", false, 1, true, 0, nil, prSchemaAdmission{})

		body := requireComment(t, comments, "multi-env plan reconciliation failure comment")
		assert.Contains(t, body, "SchemaBot could not check the schema files in this pull request, so the plan did not start. "+planControlRetryLine)
		requireNoInfrastructureDetail(t, body)
	})
}

// failAfterFirstCheckRead serves the stale check reconciliation that runs
// first, then fails the stored check read the schema file check makes.
type failAfterFirstCheckRead struct {
	storage.CheckStore
	err   error
	calls atomic.Int32
}

func (s *failAfterFirstCheckRead) GetByPR(context.Context, string, int) ([]*storage.Check, error) {
	if s.calls.Add(1) == 1 {
		return nil, nil
	}
	return nil, s.err
}

// A plan on a closed pull request is a refusal SchemaBot wrote, so the
// comment keeps its text.
func TestPlanReconciliationClosedPullRequestKeepsItsText(t *testing.T) {
	h, mux, _ := newFoldHandler(t, planControlServerConfig(), &foldCheckStore{})
	servePlanDocsOnlyPullRequest(t, mux, "closed")
	comments := make(chan string, 2)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))

	h.handlePlanCommand(httptest.NewRecorder(), "octocat/hello-world", 1, "staging", "", "", "", 12345, "", "alice", 0)

	body := requireComment(t, comments, "closed pull request comment")
	assert.Contains(t, body, "PR #1 in octocat/hello-world is closed; SchemaBot refreshes check state only for open PRs")
	assert.NotContains(t, body, "SchemaBot could not check the schema files")
}

// planHeadFixture serves a pull request that adds one managed database, so a
// plan reaches the read that confirms the schema files it loaded are still
// current. The first read of the pull request succeeds and every later one
// fails.
func planHeadFixture(t *testing.T) (*Handler, chan string) {
	t.Helper()
	h, mux, _ := newFoldHandler(t, planControlServerConfig(), &foldCheckStore{})
	servePlanPullRequest(t, mux)
	servePlanOrdersSchema(t, mux)
	comments := make(chan string, 2)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))
	return h, comments
}

// A plan confirms the pull request's latest commit after it loads the schema
// files. When that read fails, the comment says the plan did not start and what
// to do next, and the GitHub error stays in the server log.
func TestPlanHeadVerificationFailureKeepsErrorOutOfComment(t *testing.T) {
	const want = "SchemaBot could not read the latest commit of this pull request for `orders`, so the plan did not start. " + planControlRetryLine

	t.Run("single environment plan", func(t *testing.T) {
		h, comments := planHeadFixture(t)

		h.handlePlanCommand(httptest.NewRecorder(), "octocat/hello-world", 1, "staging", "orders", "", "", 12345, "", "alice", 0)

		body := requireComment(t, comments, "head verification failure comment")
		assert.Contains(t, body, want)
		assert.NotContains(t, body, "Failed to verify PR HEAD")
		assert.NotContains(t, body, "502")
	})

	t.Run("plan across environments", func(t *testing.T) {
		h, comments := planHeadFixture(t)

		h.handleMultiEnvPlan("octocat/hello-world", 1, "orders", "", 12345, "alice", false, 1, true, 0, nil, prSchemaAdmission{})

		body := requireComment(t, comments, "multi-env head verification failure comment")
		assert.Contains(t, body, want)
		assert.NotContains(t, body, "Failed to verify PR HEAD")
		assert.NotContains(t, body, "502")
	})
}
