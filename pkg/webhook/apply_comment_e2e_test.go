//go:build integration

package webhook

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gh "github.com/google/go-github/v68/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// commentCapture records all GitHub comment API calls (creates and edits).
type commentCapture struct {
	creates chan commentCreate
	edits   chan commentEdit
	nextID  atomic.Int64
}

type commentCreate struct {
	Body string
	ID   int64
}

type commentEdit struct {
	CommentID int64
	Body      string
}

// setupFakeGitHubForComments creates a mock GitHub server that captures comment creates and edits.
// It handles any repo/PR combination via wildcard routing.
func setupFakeGitHubForComments(t *testing.T) (*ghclient.InstallationClient, *commentCapture) {
	t.Helper()

	capture := &commentCapture{
		creates: make(chan commentCreate, 20),
		edits:   make(chan commentEdit, 20),
	}
	capture.nextID.Store(1000)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	// Create comment — match any repo/PR via prefix
	mux.HandleFunc("POST /repos/", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		id := capture.nextID.Add(1) - 1
		capture.creates <- commentCreate{Body: body.Body, ID: id}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": id})
	})

	// Edit comment — match any repo/comment ID via prefix
	mux.HandleFunc("PATCH /repos/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		var commentID int64
		// Try to extract comment ID from paths like /repos/{owner}/{repo}/issues/comments/{id}
		parts := splitPath(path)
		if len(parts) >= 6 {
			_, _ = fmt.Sscanf(parts[5], "%d", &commentID)
		}

		var body struct {
			Body string `json:"body"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		capture.edits <- commentEdit{CommentID: commentID, Body: body.Body}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": commentID})
	})

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)

	return installClient, capture
}

// splitPath splits a URL path into segments, filtering empty strings.
func splitPath(path string) []string {
	var parts []string
	for p := range strings.SplitSeq(path, "/") {
		if p != "" {
			parts = append(parts, p)
		}
	}
	return parts
}

// TestE2EApplyCommentLifecycle tests the full comment lifecycle:
// 1. Post progress comment
// 2. Edit progress comment on state change
// 3. Edit progress comment to final state
// 4. Post summary comment
func TestE2EApplyCommentLifecycle(t *testing.T) {
	ctx := t.Context()

	// Set up SchemaBot storage
	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(schemabotDB) })

	st := mysqlstore.New(schemabotDB)

	// Clean up stale data
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM apply_comments")
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM tasks WHERE repository = 'org/repo'")
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM applies WHERE repository = 'org/repo'")
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM locks WHERE repository = 'org/repo'")

	// Create lock, apply, and tasks in storage
	lock := &storage.Lock{
		DatabaseName: "e2e_comment_db",
		DatabaseType: "mysql",
		Repository:   "org/repo",
		PullRequest:  42,
		Owner:        "org/repo#42",
	}
	require.NoError(t, st.Locks().Acquire(ctx, lock))
	lock, err = st.Locks().Get(ctx, "e2e_comment_db", "mysql")
	require.NoError(t, err)

	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply_e2e_comment_%d", time.Now().UnixNano()),
		LockID:          lock.ID,
		PlanID:          1,
		Database:        "e2e_comment_db",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     42,
		Environment:     "staging",
		InstallationID:  12345,
		Engine:          "spirit",
		State:           state.Apply.Pending,
	}
	applyID, err := st.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	// Create tasks for the apply
	now := time.Now()
	task1 := &storage.Task{
		TaskIdentifier: fmt.Sprintf("task_e2e_1_%d", now.UnixNano()),
		ApplyID:        applyID,
		PlanID:         1,
		Database:       "e2e_comment_db",
		DatabaseType:   "mysql",
		Engine:         "spirit",
		Repository:     "org/repo",
		PullRequest:    42,
		Environment:    "staging",
		State:          state.Task.Pending,
		TableName:      "users",
		DDL:            "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
		DDLAction:      "alter",
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	_, err = st.Tasks().Create(ctx, task1)
	require.NoError(t, err)

	// Set up fake GitHub and handler
	installClient, capture := setupFakeGitHubForComments(t)
	factory := &fakeClientFactory{client: installClient}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	serverConfig := &api.ServerConfig{}
	svc := api.New(st, serverConfig, map[string]tern.Client{}, logger)
	t.Cleanup(func() { _ = svc.Close() })

	h := NewHandler(svc, factory, nil, logger)

	// Step 1: Post initial progress comment
	progressCommentID := h.postAndTrackComment(ctx, "org/repo", 42, 12345, applyID, state.Comment.Progress, "Initial progress")

	// Verify create was captured
	select {
	case created := <-capture.creates:
		assert.Equal(t, "Initial progress", created.Body)
		assert.Equal(t, progressCommentID, created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for progress comment create")
	}

	// Verify it was stored in apply_comments
	comment, err := st.ApplyComments().Get(ctx, applyID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, comment)
	assert.Equal(t, progressCommentID, comment.GitHubCommentID)

	// Step 2: Edit the progress comment via observer
	obs := NewCommentObserver(CommentObserverConfig{
		GHClient:       factory,
		Storage:        st,
		Repo:           "org/repo",
		PR:             42,
		InstallationID: 12345,
		ApplyID:        applyID,
		Logger:         logger,
	})
	obs.editTrackedComment(state.Comment.Progress, "Updated progress: running 45%")

	select {
	case edited := <-capture.edits:
		assert.Equal(t, progressCommentID, edited.CommentID)
		assert.Equal(t, "Updated progress: running 45%", edited.Body)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for progress comment edit")
	}

	// Step 3: Verify active comment resolves to progress (no cutover yet)
	active, err := st.ApplyComments().Get(ctx, applyID, state.Comment.Cutover)
	require.NoError(t, err)
	if active == nil {
		active, err = st.ApplyComments().Get(ctx, applyID, state.Comment.Progress)
		require.NoError(t, err)
	}
	require.NotNil(t, active)
	assert.Equal(t, state.Comment.Progress, active.CommentState)
	assert.Equal(t, progressCommentID, active.GitHubCommentID)

	// Step 4: Post cutover comment (simulating defer_cutover)
	cutoverCommentID := h.postAndTrackComment(ctx, "org/repo", 42, 12345, applyID, state.Comment.Cutover, "Cutover ready")

	select {
	case created := <-capture.creates:
		assert.Equal(t, "Cutover ready", created.Body)
		assert.Equal(t, cutoverCommentID, created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cutover comment create")
	}

	// Step 5: Verify active comment now resolves to cutover
	active, err = st.ApplyComments().Get(ctx, applyID, state.Comment.Cutover)
	require.NoError(t, err)
	require.NotNil(t, active)
	assert.Equal(t, state.Comment.Cutover, active.CommentState)
	assert.Equal(t, cutoverCommentID, active.GitHubCommentID)

	// Step 6: Edit cutover comment via observer
	obs.editTrackedComment(state.Comment.Cutover, "Cutover in progress")

	select {
	case edited := <-capture.edits:
		assert.Equal(t, cutoverCommentID, edited.CommentID)
		assert.Equal(t, "Cutover in progress", edited.Body)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cutover comment edit")
	}

	// Step 7: Post summary comment (terminal state)
	summaryCommentID := h.postAndTrackComment(ctx, "org/repo", 42, 12345, applyID, state.Comment.Summary, "Schema change completed")

	select {
	case created := <-capture.creates:
		assert.Equal(t, "Schema change completed", created.Body)
		assert.Equal(t, summaryCommentID, created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for summary comment create")
	}

	// Step 8: Verify all three comments are stored
	allComments, err := st.ApplyComments().ListByApply(ctx, applyID)
	require.NoError(t, err)
	require.Len(t, allComments, 3)

	commentStates := make(map[string]int64)
	for _, c := range allComments {
		commentStates[c.CommentState] = c.GitHubCommentID
	}
	assert.Equal(t, progressCommentID, commentStates[state.Comment.Progress])
	assert.Equal(t, cutoverCommentID, commentStates[state.Comment.Cutover])
	assert.Equal(t, summaryCommentID, commentStates[state.Comment.Summary])
}

func TestE2EReconcileMissingSummaryCommentsPostsSummary(t *testing.T) {
	ctx := t.Context()

	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(schemabotDB) })

	st := mysqlstore.New(schemabotDB)

	// The storage database is shared by this integration package. Clear the
	// rows this scenario owns so the missing-summary query only sees this apply.
	_, err = schemabotDB.ExecContext(ctx, "DELETE FROM apply_comments")
	require.NoError(t, err)
	_, err = schemabotDB.ExecContext(ctx, "DELETE FROM tasks WHERE repository = 'org/reconcile-summary'")
	require.NoError(t, err)
	_, err = schemabotDB.ExecContext(ctx, "DELETE FROM applies WHERE repository = 'org/reconcile-summary'")
	require.NoError(t, err)
	_, err = schemabotDB.ExecContext(ctx, "DELETE FROM locks WHERE repository = 'org/reconcile-summary'")
	require.NoError(t, err)

	lock := &storage.Lock{
		DatabaseName: "e2e_reconcile_summary_db",
		DatabaseType: storage.DatabaseTypeMySQL,
		Repository:   "org/reconcile-summary",
		PullRequest:  44,
		Owner:        "org/reconcile-summary#44",
	}
	require.NoError(t, st.Locks().Acquire(ctx, lock))
	lock, err = st.Locks().Get(ctx, "e2e_reconcile_summary_db", storage.DatabaseTypeMySQL)
	require.NoError(t, err)

	// Startup reconciliation only posts summaries for GitHub-backed applies.
	// CLI applies normally do not create apply_comments rows, and any candidate
	// row still needs repository, pull request number, and installation ID so
	// the reconciler knows where to post.
	now := time.Now()
	startedAt := now.Add(-time.Minute)
	applyIdentifier := fmt.Sprintf("apply_reconcile_summary_%d", now.UnixNano())
	apply := &storage.Apply{
		ApplyIdentifier: applyIdentifier,
		LockID:          lock.ID,
		PlanID:          1,
		Database:        "e2e_reconcile_summary_db",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Repository:      "org/reconcile-summary",
		PullRequest:     44,
		Environment:     "staging",
		Caller:          "org/reconcile-summary#44",
		InstallationID:  12345,
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Completed,
	}
	applyID, err := st.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID
	apply.StartedAt = &startedAt
	apply.CompletedAt = &now
	require.NoError(t, st.Applies().Update(ctx, apply))

	// The reconciler reloads tasks from storage to render the summary comment,
	// so seed the task state that should appear in the posted body.
	task := &storage.Task{
		TaskIdentifier: fmt.Sprintf("task_reconcile_summary_%d", now.UnixNano()),
		ApplyID:        applyID,
		PlanID:         1,
		Database:       "e2e_reconcile_summary_db",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Engine:         storage.EngineSpirit,
		Repository:     "org/reconcile-summary",
		PullRequest:    44,
		Environment:    "staging",
		State:          state.Task.Completed,
		TableName:      "reconcile_users",
		DDL:            "ALTER TABLE reconcile_users ADD COLUMN email VARCHAR(255)",
		DDLAction:      "alter",
		RowsCopied:     10,
		RowsTotal:      10,
		CreatedAt:      startedAt,
		UpdatedAt:      now,
		StartedAt:      &startedAt,
		CompletedAt:    &now,
	}
	_, err = st.Tasks().Create(ctx, task)
	require.NoError(t, err)

	// A progress marker without a summary marker represents a process restart
	// between progress comment posting and terminal summary comment posting.
	require.NoError(t, st.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:         applyID,
		CommentState:    state.Comment.Progress,
		GitHubCommentID: 9001,
	}))

	installClient, capture := setupFakeGitHubForComments(t)
	factory := &fakeClientFactory{client: installClient}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	svc := api.New(st, &api.ServerConfig{}, map[string]tern.Client{}, logger)
	t.Cleanup(func() { utils.CloseAndLog(svc) })

	h := NewHandler(svc, factory, nil, logger)
	// Run startup reconciliation directly; the fake GitHub server captures the
	// summary comment that would be posted during server startup.
	h.ReconcileMissingSummaryComments(ctx)

	var created commentCreate
	select {
	case created = <-capture.creates:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for missing summary comment")
	}
	assert.Contains(t, created.Body, "Schema Change Applied")
	assert.Contains(t, created.Body, "reconcile_users")
	assert.Contains(t, created.Body, applyIdentifier)

	// Recording the summary marker keeps future startup reconciliation passes
	// from posting a duplicate terminal summary comment.
	summaryComment, err := st.ApplyComments().Get(ctx, applyID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, summaryComment)
	assert.Equal(t, created.ID, summaryComment.GitHubCommentID)
}

// TestE2EApplyCommentUpsertOnResume tests that Start/resume replaces old comment IDs.
func TestE2EApplyCommentUpsertOnResume(t *testing.T) {
	ctx := t.Context()

	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	t.Cleanup(func() { _ = schemabotDB.Close() })

	st := mysqlstore.New(schemabotDB)

	// Clean up
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM apply_comments")
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM applies WHERE repository = 'org/repo-resume'")
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM locks WHERE repository = 'org/repo-resume'")

	// Create lock and apply
	lock := &storage.Lock{
		DatabaseName: "e2e_resume_db",
		DatabaseType: "mysql",
		Repository:   "org/repo-resume",
		PullRequest:  43,
		Owner:        "org/repo-resume#43",
	}
	require.NoError(t, st.Locks().Acquire(ctx, lock))
	lock, err = st.Locks().Get(ctx, "e2e_resume_db", "mysql")
	require.NoError(t, err)

	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply_e2e_resume_%d", time.Now().UnixNano()),
		LockID:          lock.ID,
		PlanID:          1,
		Database:        "e2e_resume_db",
		DatabaseType:    "mysql",
		Repository:      "org/repo-resume",
		PullRequest:     43,
		Environment:     "staging",
		InstallationID:  12345,
		Engine:          "spirit",
		State:           state.Apply.Stopped,
	}
	applyID, err := st.Applies().Create(ctx, apply)
	require.NoError(t, err)

	// Simulate old comment IDs from previous run
	require.NoError(t, st.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID: applyID, CommentState: state.Comment.Progress, GitHubCommentID: 111,
	}))
	require.NoError(t, st.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID: applyID, CommentState: state.Comment.Summary, GitHubCommentID: 222,
	}))

	// Set up fake GitHub
	installClient, capture := setupFakeGitHubForComments(t)
	factory := &fakeClientFactory{client: installClient}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	serverConfig := &api.ServerConfig{}
	svc := api.New(st, serverConfig, map[string]tern.Client{}, logger)
	t.Cleanup(func() { _ = svc.Close() })

	h := NewHandler(svc, factory, nil, logger)

	// Resume: post new progress comment (upsert should replace old ID)
	newProgressID := h.postAndTrackComment(ctx, "org/repo-resume", 43, 12345, applyID, state.Comment.Progress, "Resumed progress")

	select {
	case created := <-capture.creates:
		assert.Equal(t, "Resumed progress", created.Body)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for resumed progress comment")
	}

	// Verify the old comment ID was replaced
	comment, err := st.ApplyComments().Get(ctx, applyID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, comment)
	assert.Equal(t, newProgressID, comment.GitHubCommentID)
	assert.NotEqual(t, int64(111), comment.GitHubCommentID, "old comment ID should be replaced")

	// Post new summary (upsert replaces old ID)
	newSummaryID := h.postAndTrackComment(ctx, "org/repo-resume", 43, 12345, applyID, state.Comment.Summary, "Resumed summary")

	select {
	case created := <-capture.creates:
		assert.Equal(t, "Resumed summary", created.Body)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for resumed summary comment")
	}

	comment, err = st.ApplyComments().Get(ctx, applyID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, comment)
	assert.Equal(t, newSummaryID, comment.GitHubCommentID)

	// Verify total comment count is still 2 (upsert, not insert)
	allComments, err := st.ApplyComments().ListByApply(ctx, applyID)
	require.NoError(t, err)
	assert.Len(t, allComments, 2, "upsert should not create duplicate entries")
}

// TestE2EEditTrackedCommentNotFound tests that editing a non-existent comment is handled gracefully.
func TestE2EEditTrackedCommentNotFound(t *testing.T) {
	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	t.Cleanup(func() { _ = schemabotDB.Close() })

	st := mysqlstore.New(schemabotDB)

	installClient, capture := setupFakeGitHubForComments(t)
	factory := &fakeClientFactory{client: installClient}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	serverConfig := &api.ServerConfig{}
	svc := api.New(st, serverConfig, map[string]tern.Client{}, logger)
	t.Cleanup(func() { _ = svc.Close() })

	_ = NewHandler(svc, factory, nil, logger)

	// Try to edit a comment for a non-existent apply via observer — should be a no-op
	obs := NewCommentObserver(CommentObserverConfig{
		GHClient:       factory,
		Storage:        st,
		Repo:           "org/repo",
		PR:             42,
		InstallationID: 12345,
		ApplyID:        99999, // non-existent
		Logger:         logger,
	})
	obs.OnProgress(
		&storage.Apply{ID: 99999, State: state.Apply.Running},
		[]*storage.Task{{RowsCopied: 100}},
	)

	// No GitHub API call should be made
	select {
	case <-capture.edits:
		t.Fatal("expected no edit call for non-existent tracked comment")
	case <-time.After(500 * time.Millisecond):
		// expected: no edit
	}
}
