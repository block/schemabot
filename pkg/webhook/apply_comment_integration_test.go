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

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/clock"
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
	apply.LeaseOwner = "comment-test-driver"
	apply.LeaseToken = "comment-test-token"
	leaseAcquiredAt := time.Now()
	apply.LeaseAcquiredAt = &leaseAcquiredAt
	_, err = schemabotDB.ExecContext(ctx, `
		UPDATE applies
		SET lease_owner = ?, lease_token = ?, lease_acquired_at = ?
		WHERE id = ?
	`, apply.LeaseOwner, apply.LeaseToken, leaseAcquiredAt, applyID)
	require.NoError(t, err)

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
	h.postAndTrackComment(ctx, "org/repo", 42, 12345, applyID, state.Comment.Progress, "Initial progress")

	// Verify create was captured
	var progressCommentID int64
	select {
	case created := <-capture.creates:
		assert.Equal(t, "Initial progress", created.Body)
		progressCommentID = created.ID
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
	obs.editTrackedComment(apply, state.Comment.Progress, "Updated progress: running 45%")

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
	h.postAndTrackComment(ctx, "org/repo", 42, 12345, applyID, state.Comment.Cutover, "Cutover ready")

	var cutoverCommentID int64
	select {
	case created := <-capture.creates:
		assert.Equal(t, "Cutover ready", created.Body)
		cutoverCommentID = created.ID
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
	obs.editTrackedComment(apply, state.Comment.Cutover, "Cutover in progress")

	select {
	case edited := <-capture.edits:
		assert.Equal(t, cutoverCommentID, edited.CommentID)
		assert.Equal(t, "Cutover in progress", edited.Body)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cutover comment edit")
	}

	// Step 7: Post summary comment (terminal state)
	h.postAndTrackComment(ctx, "org/repo", 42, 12345, applyID, state.Comment.Summary, "Schema change completed")

	var summaryCommentID int64
	select {
	case created := <-capture.creates:
		assert.Equal(t, "Schema change completed", created.Body)
		summaryCommentID = created.ID
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

// When a stopped apply resumes, the observer posts a fresh progress comment and
// tracks that as the live one, rather than re-editing the comment frozen at
// "Stopped". The prior progress comment is left in place as the record of where
// the apply paused, the stopped summary marker is consumed, and later progress
// edits land on the new comment.
func TestE2EResumeRotatesProgressComment(t *testing.T) {
	ctx := t.Context()

	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(schemabotDB) })

	st := mysqlstore.New(schemabotDB)

	for _, stmt := range []string{
		"DELETE FROM apply_comments",
		"DELETE FROM tasks WHERE repository = 'org/repo-rotate'",
		"DELETE FROM applies WHERE repository = 'org/repo-rotate'",
		"DELETE FROM locks WHERE repository = 'org/repo-rotate'",
	} {
		_, err = schemabotDB.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	lock := &storage.Lock{
		DatabaseName: "e2e_rotate_db",
		DatabaseType: "mysql",
		Repository:   "org/repo-rotate",
		PullRequest:  142,
		Owner:        "org/repo-rotate#142",
	}
	require.NoError(t, st.Locks().Acquire(ctx, lock))
	lock, err = st.Locks().Get(ctx, "e2e_rotate_db", "mysql")
	require.NoError(t, err)

	// The apply has just been started again after a stop: the data plane accepted
	// the start, so the apply is in the Resuming window (it may still report stopped
	// briefly) before it transitions to Running.
	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply_e2e_rotate_%d", time.Now().UnixNano()),
		LockID:          lock.ID,
		PlanID:          1,
		Database:        "e2e_rotate_db",
		DatabaseType:    "mysql",
		Repository:      "org/repo-rotate",
		PullRequest:     142,
		Environment:     "staging",
		InstallationID:  12345,
		Engine:          "spirit",
		State:           state.Apply.Resuming,
	}
	applyID, err := st.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID
	apply.LeaseOwner = "resume-test-driver"
	apply.LeaseToken = "resume-test-token"
	leaseAcquiredAt := time.Now()
	apply.LeaseAcquiredAt = &leaseAcquiredAt
	_, err = schemabotDB.ExecContext(ctx, `
		UPDATE applies
		SET lease_owner = ?, lease_token = ?, lease_acquired_at = ?, state = ?
		WHERE id = ?
	`, apply.LeaseOwner, apply.LeaseToken, leaseAcquiredAt, state.Apply.Resuming, applyID)
	require.NoError(t, err)

	// The task is copying again after resume.
	now := time.Now()
	task := &storage.Task{
		TaskIdentifier:  fmt.Sprintf("task_e2e_rotate_%d", now.UnixNano()),
		ApplyID:         applyID,
		PlanID:          1,
		Database:        "e2e_rotate_db",
		DatabaseType:    "mysql",
		Engine:          "spirit",
		Repository:      "org/repo-rotate",
		PullRequest:     142,
		Environment:     "staging",
		State:           state.Task.Running,
		TableName:       "users",
		DDL:             "ALTER TABLE users ADD INDEX idx_email (email)",
		DDLAction:       "alter",
		RowsCopied:      500,
		RowsTotal:       1000,
		ProgressPercent: 50,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	_, err = st.Tasks().Create(ctx, task)
	require.NoError(t, err)

	installClient, capture := setupFakeGitHubForComments(t)
	factory := &fakeClientFactory{client: installClient}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	serverConfig := &api.ServerConfig{}
	svc := api.New(st, serverConfig, map[string]tern.Client{}, logger)
	t.Cleanup(func() { _ = svc.Close() })
	h := NewHandler(svc, factory, nil, logger)

	// Seed the pre-resume comments left by the stop: the progress comment frozen at
	// "Stopped" and the stopped summary marker that signals a resume is in progress.
	h.postAndTrackComment(ctx, "org/repo-rotate", 142, 12345, applyID, state.Comment.Progress, "Stopped at 21%")
	stoppedProgressID := requireCommentCreate(t, capture)
	h.postAndTrackComment(ctx, "org/repo-rotate", 142, 12345, applyID, state.Comment.Summary, "Schema Change Stopped")
	requireCommentCreate(t, capture)

	fake := clock.NewFake(now)
	obs := NewCommentObserver(CommentObserverConfig{
		GHClient:       factory,
		Storage:        st,
		Repo:           "org/repo-rotate",
		PR:             142,
		InstallationID: 12345,
		ApplyID:        applyID,
		Logger:         logger,
		Clock:          fake,
	})

	// First progress tick after resume rotates the progress comment. While the
	// apply is in the Resuming window the comment keeps the stable in-progress
	// title and renders state-only: the row-copy percent is indeterminate
	// (continuation vs fresh copy), so no bar is shown.
	obs.OnProgress(apply, []*storage.Task{task})

	var newProgressID int64
	select {
	case created := <-capture.creates:
		newProgressID = created.ID
		assert.Contains(t, created.Body, "Schema Change In Progress — Staging")
		assert.Contains(t, created.Body, "**Status**: Resuming")
		assert.Contains(t, created.Body, "Resuming…")
		assert.NotContains(t, created.Body, "Stopped")
		assert.NotContains(t, created.Body, "50%", "the indeterminate resume window must not show a stale percent")
	case <-time.After(5 * time.Second):
		t.Fatal("expected a new progress comment to be posted on resume")
	}
	assert.NotEqual(t, stoppedProgressID, newProgressID, "resume must post a new comment, not reuse the stopped one")

	// The progress row now tracks the new comment; the stopped-summary marker is
	// consumed by being superseded — the row and its GitHub comment are kept, not
	// deleted.
	prog, err := st.ApplyComments().Get(ctx, applyID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, prog)
	assert.Equal(t, newProgressID, prog.GitHubCommentID)
	summary, err := st.ApplyComments().Get(ctx, applyID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, summary, "the stopped-summary row is retired, not deleted")
	assert.NotNil(t, summary.SupersededAt, "the stopped-summary marker is superseded after rotation")

	// Once the data plane leaves stopped the apply is Running: a later tick edits
	// the new comment in place (it does not rotate again) and now shows the bar.
	apply.State = state.Apply.Running
	fake.Advance(activeInterval + time.Second)
	task.RowsCopied = 700
	task.ProgressPercent = 70
	obs.OnProgress(apply, []*storage.Task{task})

	select {
	case edited := <-capture.edits:
		assert.Equal(t, newProgressID, edited.CommentID, "later edits land on the new comment")
		assert.Contains(t, edited.Body, "70%", "once running, the new comment shows real row-copy progress")
	case created := <-capture.creates:
		t.Fatalf("resume must rotate exactly once; got another new comment %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected an in-place edit of the new progress comment on the next tick")
	}
}

// requireCommentCreate returns the next captured comment-create id, failing if
// none arrives.
func requireCommentCreate(t *testing.T, capture *commentCapture) int64 {
	t.Helper()
	select {
	case created := <-capture.creates:
		return created.ID
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for comment create")
		return 0
	}
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
	h.postAndTrackComment(ctx, "org/repo-resume", 43, 12345, applyID, state.Comment.Progress, "Resumed progress")

	var newProgressID int64
	select {
	case created := <-capture.creates:
		assert.Equal(t, "Resumed progress", created.Body)
		newProgressID = created.ID
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
	h.postAndTrackComment(ctx, "org/repo-resume", 43, 12345, applyID, state.Comment.Summary, "Resumed summary")

	var newSummaryID int64
	select {
	case created := <-capture.creates:
		assert.Equal(t, "Resumed summary", created.Body)
		newSummaryID = created.ID
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

// This scenario covers a recovered PR observer whose operator driver has lost
// ownership before it reaches terminal notification. The stale observer must not
// edit progress, post a summary, mark summary state, or run terminal hooks.
func TestE2ECommentObserverSkipsTerminalSideEffectsAfterLeaseLoss(t *testing.T) {
	ctx := t.Context()

	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(schemabotDB) })

	st := mysqlstore.New(schemabotDB)

	_, err = schemabotDB.ExecContext(ctx, "DELETE FROM apply_comments")
	require.NoError(t, err)
	_, err = schemabotDB.ExecContext(ctx, "DELETE FROM tasks WHERE repository = 'org/stale-lease'")
	require.NoError(t, err)
	_, err = schemabotDB.ExecContext(ctx, "DELETE FROM applies WHERE repository = 'org/stale-lease'")
	require.NoError(t, err)
	_, err = schemabotDB.ExecContext(ctx, "DELETE FROM locks WHERE repository = 'org/stale-lease'")
	require.NoError(t, err)

	lock := &storage.Lock{
		DatabaseName: "e2e_stale_lease_db",
		DatabaseType: storage.DatabaseTypeMySQL,
		Repository:   "org/stale-lease",
		PullRequest:  45,
		Owner:        "org/stale-lease#45",
	}
	require.NoError(t, st.Locks().Acquire(ctx, lock))
	lock, err = st.Locks().Get(ctx, "e2e_stale_lease_db", storage.DatabaseTypeMySQL)
	require.NoError(t, err)
	require.NotNil(t, lock)

	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply_stale_lease_%d", now.UnixNano()),
		LockID:          lock.ID,
		PlanID:          1,
		Database:        "e2e_stale_lease_db",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Repository:      "org/stale-lease",
		PullRequest:     45,
		Environment:     "staging",
		Caller:          "org/stale-lease#45",
		InstallationID:  12345,
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Pending,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := st.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	task := &storage.Task{
		TaskIdentifier:  fmt.Sprintf("task_stale_lease_%d", now.UnixNano()),
		ApplyID:         applyID,
		PlanID:          apply.PlanID,
		Database:        apply.Database,
		DatabaseType:    apply.DatabaseType,
		Engine:          storage.EngineSpirit,
		Repository:      apply.Repository,
		PullRequest:     apply.PullRequest,
		Environment:     apply.Environment,
		State:           state.Task.Completed,
		TableName:       "users",
		DDL:             "ALTER TABLE `users` ADD COLUMN `stale_lease_note` varchar(255)",
		DDLAction:       "alter",
		Options:         []byte("{}"),
		ProgressPercent: 100,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	taskID, err := st.Tasks().Create(ctx, task)
	require.NoError(t, err)
	task.ID = taskID

	_, err = schemabotDB.ExecContext(ctx, `
		UPDATE applies
		SET lease_owner = ?, lease_token = ?, lease_acquired_at = NOW()
		WHERE id = ?
	`, "current-driver", "current-token", applyID)
	require.NoError(t, err)

	progressCommentID := int64(4242)
	require.NoError(t, st.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:         applyID,
		CommentState:    state.Comment.Progress,
		GitHubCommentID: progressCommentID,
	}))

	installClient, capture := setupFakeGitHubForComments(t)
	factory := &fakeClientFactory{client: installClient}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	terminalHookCalled := atomic.Bool{}
	observer := NewCommentObserver(CommentObserverConfig{
		GHClient:       factory,
		Storage:        st,
		Repo:           "org/stale-lease",
		PR:             45,
		InstallationID: 12345,
		ApplyID:        applyID,
		ApplyLease: storage.ApplyLease{
			ApplyID: applyID,
			Owner:   "stale-driver",
			Token:   "stale-token",
		},
		Logger: logger,
		OnTerminalHook: func(*storage.Apply) {
			terminalHookCalled.Store(true)
		},
	})

	terminalApply := *apply
	terminalApply.State = state.Apply.Failed
	terminalApply.ErrorMessage = "stale driver terminal state"
	terminalApply.CompletedAt = &now
	observer.OnTerminal(&terminalApply, []*storage.Task{task})

	select {
	case edited := <-capture.edits:
		t.Fatalf("expected no edit call after lease loss, got comment %d: %s", edited.CommentID, edited.Body)
	case created := <-capture.creates:
		t.Fatalf("expected no create call after lease loss, got comment %d: %s", created.ID, created.Body)
	case <-time.After(500 * time.Millisecond):
		// expected: no GitHub side effects
	}
	assert.False(t, terminalHookCalled.Load())

	summary, err := st.ApplyComments().Get(ctx, applyID, state.Comment.Summary)
	require.NoError(t, err)
	assert.Nil(t, summary)
	progress, err := st.ApplyComments().Get(ctx, applyID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, progress)
	assert.Equal(t, progressCommentID, progress.GitHubCommentID)
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
