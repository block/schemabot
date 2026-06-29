//go:build integration

// Webhook integration tests for plan/apply when the PR removes the current schema files.

package webhook

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// TestE2EPlanWithNoCurrentSchemaFilesExplainsInProgressApply verifies that a
// PR whose current diff no longer contains managed schema files does not fall
// back to whole-repo config discovery when an apply from that PR is still
// running. The command should tell the user how to reconcile the in-flight apply.
func TestE2EPlanWithNoCurrentSchemaFilesExplainsInProgressApply(t *testing.T) {
	body, treeCalls := runCommandWithNoCurrentSchemaFilesAndApplyOwnedCheck(t, "schemabot plan -e staging", state.Apply.Running, checkStatusInProgress, "")

	assert.Zero(t, treeCalls, "empty-diff plan should not use whole-repo git tree discovery")
	assert.Contains(t, body, "SchemaBot is still applying a schema change from this PR")
	assert.Contains(t, body, "The live database operation was already started")
	assert.Contains(t, body, "schemabot stop apply-empty-diff -e staging")
	assert.Contains(t, body, "schemabot rollback apply-empty-diff -e staging")
	assert.Contains(t, body, "schemabot plan -e staging -d webhook_empty_diff_apply")
	assert.Contains(t, body, "push a no-op `schemabot.yaml` edit to trigger a fresh plan")
	assert.NotContains(t, body, "ask an operator")
	assert.NotContains(t, body, "truncated repository tree")
	assert.NotContains(t, body, "schemabot status")
}

// TestE2EPlanWithNoCurrentSchemaFilesExplainsCompletedApply verifies that a PR
// whose current diff no longer contains managed schema files produces a clear
// reconciliation-required comment after the apply has reached a terminal state.
func TestE2EPlanWithNoCurrentSchemaFilesExplainsCompletedApply(t *testing.T) {
	body, treeCalls := runCommandWithNoCurrentSchemaFilesAndApplyOwnedCheck(t, "schemabot plan -e staging", state.Apply.Completed, checkStatusCompleted, checkConclusionActionRequired)

	assert.Zero(t, treeCalls, "empty-diff plan should not use whole-repo git tree discovery")
	assert.Contains(t, body, "SchemaBot already applied a schema change from this PR")
	assert.Contains(t, body, "The live database was already updated")
	assert.Contains(t, body, "Keep the live schema change")
	assert.Contains(t, body, "Undo the live schema change")
	assert.Contains(t, body, "schemabot rollback apply-empty-diff -e staging")
	assert.Contains(t, body, "schemabot plan -e staging -d webhook_empty_diff_apply")
	assert.Contains(t, body, "push a no-op `schemabot.yaml` edit to trigger a fresh plan")
	assert.NotContains(t, body, "ask an operator")
	assert.NotContains(t, body, "truncated repository tree")
	assert.NotContains(t, body, "Git reverting")
}

// TestE2EApplyWithNoCurrentSchemaFilesExplainsInProgressApply verifies that
// apply uses the same reconciliation guard as plan instead of falling back to
// whole-repo config discovery after the current PR diff no longer contains
// managed schema files.
func TestE2EApplyWithNoCurrentSchemaFilesExplainsInProgressApply(t *testing.T) {
	body, treeCalls := runCommandWithNoCurrentSchemaFilesAndApplyOwnedCheck(t, "schemabot apply -e staging", state.Apply.Running, checkStatusInProgress, "")

	assert.Zero(t, treeCalls, "empty-diff apply should not use whole-repo git tree discovery")
	assert.Contains(t, body, "SchemaBot is still applying a schema change from this PR")
	assert.Contains(t, body, "schemabot rollback apply-empty-diff -e staging")
	assert.NotContains(t, body, "truncated repository tree")
}

// TestE2EApplyConfirmWithNoCurrentSchemaFilesExplainsInProgressApply verifies
// that apply-confirm also fails closed before discovery when a PR no longer has
// managed schema files but still owns an in-flight apply.
func TestE2EApplyConfirmWithNoCurrentSchemaFilesExplainsInProgressApply(t *testing.T) {
	body, treeCalls := runCommandWithNoCurrentSchemaFilesAndApplyOwnedCheck(t, "schemabot apply-confirm -e staging", state.Apply.Running, checkStatusInProgress, "")

	assert.Zero(t, treeCalls, "empty-diff apply-confirm should not use whole-repo git tree discovery")
	assert.Contains(t, body, "SchemaBot is still applying a schema change from this PR")
	assert.Contains(t, body, "schemabot stop apply-empty-diff -e staging")
	assert.Contains(t, body, "schemabot rollback apply-empty-diff -e staging")
	assert.NotContains(t, body, "truncated repository tree")
}

func runCommandWithNoCurrentSchemaFilesAndApplyOwnedCheck(t *testing.T, comment, applyState, checkStatus, checkConclusion string) (string, int64) {
	t.Helper()

	dbName := "webhook_empty_diff_apply"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	apply := &storage.Apply{
		ApplyIdentifier: "apply-empty-diff",
		Database:        dbName,
		DatabaseType:    "mysql",
		Environment:     "staging",
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		State:           applyState,
		Engine:          "spirit",
	}
	applyID, err := svc.Storage().Applies().Create(ctx, apply)
	require.NoError(t, err)

	require.NoError(t, svc.Storage().Checks().Upsert(ctx, &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "oldsha111",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: dbName,
		CheckRunID:   100,
		ApplyID:      applyID,
		HasChanges:   true,
		Status:       checkStatus,
		Conclusion:   checkConclusion,
	}))

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{
				Ref: new("feature-branch"),
				SHA: new("newsha222"),
			},
			Base: &gh.PullRequestBranch{
				Ref: new("main"),
				SHA: new("def456"),
			},
			User: &gh.User{Login: new("testuser")},
		})
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]*gh.CommitFile{})
	})

	var treeCalls atomic.Int64
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/", func(w http.ResponseWriter, _ *http.Request) {
		treeCalls.Add(1)
		_ = json.NewEncoder(w).Encode(gh.Tree{
			SHA:       new("newsha222"),
			Entries:   []*gh.TreeEntry{},
			Truncated: new(true),
		})
	})

	comments := make(chan string, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	h := newE2EHandler(t, svc, client)
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: comment,
		isPR:    true,
	}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-comments:
		return body, treeCalls.Load()
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for reconciliation comment")
		return "", treeCalls.Load()
	}
}
