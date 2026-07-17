//go:build integration

package webhook

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
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

// commentCapture records all GitHub comment API calls (creates and edits) and
// remembers the latest body per comment ID so GET reads return what was written.
type commentCapture struct {
	creates chan commentCreate
	edits   chan commentEdit
	nextID  atomic.Int64

	mu     sync.Mutex
	bodies map[int64]string
}

func (c *commentCapture) setBody(commentID int64, body string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bodies[commentID] = body
}

func (c *commentCapture) body(commentID int64) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	body, ok := c.bodies[commentID]
	return body, ok
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
		bodies:  make(map[int64]string),
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
		capture.setBody(id, body.Body)
		capture.creates <- commentCreate{Body: body.Body, ID: id}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": id})
	})

	// Read comment — the observer loads a superseded comment's body before
	// freezing it into a collapsed details block.
	mux.HandleFunc("GET /repos/", func(w http.ResponseWriter, r *http.Request) {
		var commentID int64
		parts := splitPath(r.URL.Path)
		if len(parts) >= 6 {
			_, _ = fmt.Sscanf(parts[5], "%d", &commentID)
		}
		body, ok := capture.body(commentID)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": commentID, "body": body})
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
		capture.setBody(commentID, body.Body)
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
	h.postAndTrackComment(ctx, "org/repo", 42, 12345, apply, state.Comment.Progress, "Initial progress")

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
	h.postAndTrackComment(ctx, "org/repo", 42, 12345, apply, state.Comment.Cutover, "Cutover ready")

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
	h.postAndTrackComment(ctx, "org/repo", 42, 12345, apply, state.Comment.Summary, "Schema change completed")

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
// "Stopped". The prior progress comment is folded into a collapsed details
// block pointing at its successor — keeping the pre-stop record on the PR
// without looking live — the stopped summary marker is consumed, and later
// progress edits land on the new comment.
func TestE2EResumeRotatesProgressComment(t *testing.T) {
	ctx := t.Context()

	// The apply has just been started again after a stop: the data plane accepted
	// the start, so the apply is in the Resuming window (it may still report stopped
	// briefly) before it transitions to Running. The task is copying again after
	// resume.
	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:       "org/repo-rotate",
		pr:         142,
		database:   "e2e_rotate_db",
		applyState: state.Apply.Resuming,
	})
	st, apply, task, capture, h := f.st, f.apply, f.task, f.capture, f.handler

	// Seed the pre-resume comments left by the stop: the progress comment frozen at
	// "Stopped" and the stopped summary marker that signals a resume is in progress.
	h.postAndTrackComment(ctx, "org/repo-rotate", 142, 12345, apply, state.Comment.Progress, "Stopped at 21%")
	stoppedProgressID := requireCommentCreate(t, capture)
	h.postAndTrackComment(ctx, "org/repo-rotate", 142, 12345, apply, state.Comment.Summary, "Schema Change Stopped")
	requireCommentCreate(t, capture)

	fake := clock.NewFake(task.CreatedAt)
	obs := f.newObserver(st, fake)

	// First progress tick after resume rotates the progress comment. While the
	// apply is in the Resuming window the comment keeps the stable in-progress
	// title and renders state-only: the row-copy percent is indeterminate
	// (continuation vs fresh copy), so no bar is shown.
	obs.OnProgress(apply, []*storage.Task{task})

	var newProgressID int64
	select {
	case created := <-capture.creates:
		newProgressID = created.ID
		assert.Contains(t, created.Body, "Schema Change Status — Staging")
		assert.Contains(t, created.Body, "**Status**: Resuming")
		assert.Contains(t, created.Body, "Resuming…")
		assert.NotContains(t, created.Body, "Stopped")
		assert.NotContains(t, created.Body, "50%", "the indeterminate resume window must not show a stale percent")
	case <-time.After(5 * time.Second):
		t.Fatal("expected a new progress comment to be posted on resume")
	}
	assert.NotEqual(t, stoppedProgressID, newProgressID, "resume must post a new comment, not reuse the stopped one")

	// The prior comment is frozen into a collapsed details block pointing at
	// its successor, with the pre-stop body preserved inside the fold.
	select {
	case edited := <-capture.edits:
		assert.Equal(t, stoppedProgressID, edited.CommentID, "the freeze edit lands on the superseded comment")
		assert.Contains(t, edited.Body, "Schema change resumed")
		assert.Contains(t, edited.Body, fmt.Sprintf("#issuecomment-%d", newProgressID), "the frozen comment links to its successor")
		assert.Contains(t, edited.Body, "<details>")
		assert.Contains(t, edited.Body, "Stopped at 21%", "the pre-stop body is preserved inside the fold")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the superseded progress comment to be frozen")
	}

	// The progress row now tracks the new comment with no freeze left owing;
	// the stopped-summary marker is consumed by being superseded — the row and
	// its GitHub comment are kept, not deleted.
	prog, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, prog)
	assert.Equal(t, newProgressID, prog.GitHubCommentID)
	assert.Nil(t, prog.PendingFreezeCommentID)
	summary, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
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

// Across repeated stop/start iterations, each resume folds the progress
// comment it supersedes, so the PR timeline keeps exactly one live progress
// comment. A resume rotation also reconciles a freeze that a prior drive owed
// but never landed: the first tick after the second resume freezes both the
// leftover comment and the one this resume supersedes, each pointing at its
// successor. The owed fold uses the generic superseded rendering — the marker
// records which comment is owed, not which rotation superseded it — while the
// fold this resume performs itself carries the resume headline.
func TestE2EResumeRotationFreezesAcrossIterations(t *testing.T) {
	ctx := t.Context()

	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:       "org/repo-rotate-iterations",
		pr:         146,
		database:   "e2e_rotate_iterations_db",
		applyState: state.Apply.Resuming,
	})
	st, apply, task, capture, h := f.st, f.apply, f.task, f.capture, f.handler

	// Seed the comments left by a first stop/start cycle whose drive died
	// before its freeze edit landed: the first cycle's progress comment (still
	// unfolded), the fresh comment that resume rotated to — now frozen at
	// "Stopped" by a second stop — and the second stop's summary marker.
	h.postAndTrackComment(ctx, "org/repo-rotate-iterations", 146, 12345, apply, state.Comment.Progress, "Stopped at 21%")
	firstProgressID := requireCommentCreate(t, capture)
	h.postAndTrackComment(ctx, "org/repo-rotate-iterations", 146, 12345, apply, state.Comment.Progress, "Stopped at 63%")
	secondProgressID := requireCommentCreate(t, capture)
	require.NoError(t, st.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:                apply.ID,
		CommentState:           state.Comment.Progress,
		GitHubCommentID:        secondProgressID,
		PendingFreezeCommentID: &firstProgressID,
	}))
	h.postAndTrackComment(ctx, "org/repo-rotate-iterations", 146, 12345, apply, state.Comment.Summary, "Schema Change Stopped")
	requireCommentCreate(t, capture)

	fake := clock.NewFake(task.CreatedAt)
	obs := f.newObserver(st, fake)

	// The first tick after the second resume reconciles the owed freeze before
	// rotating: the first cycle's comment folds pointing at the comment that
	// superseded it.
	obs.OnProgress(apply, []*storage.Task{task})

	select {
	case edited := <-capture.edits:
		assert.Equal(t, firstProgressID, edited.CommentID, "the reconciled freeze lands on the first cycle's comment")
		assert.Contains(t, edited.Body, "Progress comment superseded",
			"the owed fold uses the generic rendering since the superseding rotation is not recorded")
		assert.Contains(t, edited.Body, fmt.Sprintf("#issuecomment-%d", secondProgressID), "the frozen comment links to its successor")
		assert.Contains(t, edited.Body, "Stopped at 21%", "the superseded body is preserved inside the fold")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the owed freeze from the prior drive to be reconciled")
	}

	// The same tick rotates for the second resume: a fresh progress comment is
	// posted and the second cycle's comment folds pointing at it.
	var thirdProgressID int64
	select {
	case created := <-capture.creates:
		thirdProgressID = created.ID
		assert.Contains(t, created.Body, "Schema Change Status — Staging")
	case <-time.After(5 * time.Second):
		t.Fatal("expected a new progress comment on the second resume")
	}
	select {
	case edited := <-capture.edits:
		assert.Equal(t, secondProgressID, edited.CommentID, "the freeze edit lands on the second cycle's comment")
		assert.Contains(t, edited.Body, "Schema change resumed")
		assert.Contains(t, edited.Body, fmt.Sprintf("#issuecomment-%d", thirdProgressID), "the frozen comment links to its successor")
		assert.Contains(t, edited.Body, "Stopped at 63%", "the superseded body is preserved inside the fold")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the second cycle's progress comment to be frozen")
	}

	// The tracked row points at the live comment with no freeze left owing, and
	// the stopped-summary marker is consumed.
	prog, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, prog)
	assert.Equal(t, thirdProgressID, prog.GitHubCommentID)
	assert.Nil(t, prog.PendingFreezeCommentID)
	summary, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, summary)
	assert.NotNil(t, summary.SupersededAt, "the stopped-summary marker is superseded after rotation")
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

// applyCommentFixtureParams names the per-test identity of a comment-rotation
// fixture. A zero volume seeds the apply without volume options.
type applyCommentFixtureParams struct {
	repo       string
	pr         int
	database   string
	applyState string
	volume     int
}

// applyCommentFixture bundles the storage seeding and fake-GitHub plumbing the
// comment-rotation tests share: a leased apply mid-copy with one running task,
// and a webhook handler wired to a capturing fake GitHub.
type applyCommentFixture struct {
	st      storage.Storage
	apply   *storage.Apply
	task    *storage.Task
	capture *commentCapture
	factory *fakeClientFactory
	logger  *slog.Logger
	handler *Handler
}

// setupApplyCommentFixture cleans up any prior rows for the fixture's repo,
// seeds a lock, a leased apply, and a running copy task at 50%, and returns
// the handler and fake-GitHub capture the test drives comments through.
func setupApplyCommentFixture(t *testing.T, p applyCommentFixtureParams) *applyCommentFixture {
	t.Helper()
	ctx := t.Context()

	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	// svc.Close below owns closing the store's DB; this early-failure safety
	// close is redundant once svc exists, so discard its guaranteed
	// already-closed error.
	t.Cleanup(func() { _ = schemabotDB.Close() })

	st := mysqlstore.New(schemabotDB)

	_, err = schemabotDB.ExecContext(ctx, "DELETE FROM apply_comments")
	require.NoError(t, err)
	for _, table := range []string{"tasks", "applies", "locks"} {
		_, err = schemabotDB.ExecContext(ctx, "DELETE FROM `"+table+"` WHERE repository = ?", p.repo)
		require.NoError(t, err)
	}

	lock := &storage.Lock{
		DatabaseName: p.database,
		DatabaseType: "mysql",
		Repository:   p.repo,
		PullRequest:  p.pr,
		Owner:        fmt.Sprintf("%s#%d", p.repo, p.pr),
	}
	require.NoError(t, st.Locks().Acquire(ctx, lock))
	lock, err = st.Locks().Get(ctx, p.database, "mysql")
	require.NoError(t, err)

	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply_%s_%d", p.database, time.Now().UnixNano()),
		LockID:          lock.ID,
		PlanID:          1,
		Database:        p.database,
		DatabaseType:    "mysql",
		Repository:      p.repo,
		PullRequest:     p.pr,
		Environment:     "staging",
		InstallationID:  12345,
		Engine:          "spirit",
		State:           p.applyState,
	}
	if p.volume != 0 {
		apply.SetOptions(storage.ApplyOptions{Volume: p.volume})
	}
	applyID, err := st.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID
	apply.LeaseOwner = p.database + "-test-driver"
	apply.LeaseToken = p.database + "-test-token"
	leaseAcquiredAt := time.Now()
	apply.LeaseAcquiredAt = &leaseAcquiredAt
	_, err = schemabotDB.ExecContext(ctx, `
		UPDATE applies
		SET lease_owner = ?, lease_token = ?, lease_acquired_at = ?, state = ?
		WHERE id = ?
	`, apply.LeaseOwner, apply.LeaseToken, leaseAcquiredAt, p.applyState, applyID)
	require.NoError(t, err)

	now := time.Now()
	task := &storage.Task{
		TaskIdentifier:  fmt.Sprintf("task_%s_%d", p.database, now.UnixNano()),
		ApplyID:         applyID,
		PlanID:          1,
		Database:        p.database,
		DatabaseType:    "mysql",
		Engine:          "spirit",
		Repository:      p.repo,
		PullRequest:     p.pr,
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
	svc := api.New(st, &api.ServerConfig{}, map[string]tern.Client{}, logger)
	t.Cleanup(func() { _ = svc.Close() })

	return &applyCommentFixture{
		st:      st,
		apply:   apply,
		task:    task,
		capture: capture,
		factory: factory,
		logger:  logger,
		handler: NewHandler(svc, factory, nil, logger),
	}
}

// newObserver builds a comment observer over the fixture's apply. Tests pass
// the fixture's real storage, or a wrapper injecting failures, and their fake
// clock.
func (f *applyCommentFixture) newObserver(stor storage.Storage, clk clock.Clock) *CommentObserver {
	return NewCommentObserver(CommentObserverConfig{
		GHClient:       f.factory,
		Storage:        stor,
		Repo:           f.apply.Repository,
		PR:             f.apply.PullRequest,
		InstallationID: f.apply.InstallationID,
		ApplyID:        f.apply.ID,
		Logger:         f.logger,
		Clock:          clk,
	})
}

// When an operator's volume change takes effect on a running apply, the
// observer posts a fresh progress comment tracking the new level — a new
// comment at the bottom of the PR timeline is where the operator looks for the
// effect of the command they just issued. The prior progress comment is frozen
// into a collapsed details block pointing at its successor, later progress
// edits land on the new comment, and a tick without a level change does not
// rotate again.
func TestE2EVolumeChangeRotatesProgressComment(t *testing.T) {
	ctx := t.Context()

	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:       "org/repo-volume",
		pr:         143,
		database:   "e2e_volume_db",
		applyState: state.Apply.Running,
		volume:     3,
	})
	st, apply, task, capture, h := f.st, f.apply, f.task, f.capture, f.handler

	// Seed the tracked progress comment the apply started with; the handler
	// records the starting level from the apply's options.
	h.postAndTrackComment(ctx, "org/repo-volume", 143, 12345, apply, state.Comment.Progress, "Copying at volume 3")
	initialProgressID := requireCommentCreate(t, capture)

	fake := clock.NewFake(task.CreatedAt)
	obs := f.newObserver(st, fake)

	// A tick at the level the comment was posted with edits in place.
	fake.Advance(activeInterval + time.Second)
	obs.OnProgress(apply, []*storage.Task{task})
	select {
	case edited := <-capture.edits:
		assert.Equal(t, initialProgressID, edited.CommentID, "same-level ticks edit the tracked comment in place")
		assert.Contains(t, edited.Body, "Volume: 3/11")
	case created := <-capture.creates:
		t.Fatalf("a tick without a volume change must not post a new comment, got %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected an in-place edit of the tracked progress comment")
	}

	// The driver applied a volume change and recorded the new level on the
	// apply options. The next tick rotates: a fresh progress comment tracks
	// the new level.
	apply.SetOptions(storage.ApplyOptions{Volume: 5})
	task.RowsCopied = 600
	fake.Advance(activeInterval + time.Second)
	obs.OnProgress(apply, []*storage.Task{task})

	var newProgressID int64
	select {
	case created := <-capture.creates:
		newProgressID = created.ID
		assert.Contains(t, created.Body, "Volume: 5/11", "the fresh comment tracks the new level")
	case <-time.After(5 * time.Second):
		t.Fatal("expected a new progress comment after the volume change")
	}
	assert.NotEqual(t, initialProgressID, newProgressID, "the volume change must post a new comment, not reuse the old one")

	// The prior comment is frozen into a collapsed details block pointing at
	// its successor, with the pre-change body preserved inside the fold.
	select {
	case edited := <-capture.edits:
		assert.Equal(t, initialProgressID, edited.CommentID, "the freeze edit lands on the superseded comment")
		assert.Contains(t, edited.Body, "Volume changed to **5/11**")
		assert.Contains(t, edited.Body, fmt.Sprintf("#issuecomment-%d", newProgressID), "the frozen comment links to its successor")
		assert.Contains(t, edited.Body, "<details>")
		assert.Contains(t, edited.Body, "Volume: 3/11", "the pre-change body is preserved inside the fold")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the superseded progress comment to be frozen")
	}

	// The progress row tracks the new comment at the new level.
	prog, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, prog)
	assert.Equal(t, newProgressID, prog.GitHubCommentID)
	require.NotNil(t, prog.PostedVolume)
	assert.Equal(t, 5, *prog.PostedVolume)

	// A later tick without another level change edits the new comment in place.
	task.RowsCopied = 700
	task.ProgressPercent = 70
	fake.Advance(activeInterval + time.Second)
	obs.OnProgress(apply, []*storage.Task{task})
	select {
	case edited := <-capture.edits:
		assert.Equal(t, newProgressID, edited.CommentID, "later edits land on the new comment")
		assert.Contains(t, edited.Body, "70%")
	case created := <-capture.creates:
		t.Fatalf("a volume change must rotate exactly once; got another new comment %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected an in-place edit of the new progress comment on the next tick")
	}
}

// failingCommentUpsertStore wraps an ApplyCommentStore so every Upsert fails
// until the outage is healed, modeling a storage outage between posting a
// comment and recording its ID. Reads and every other write pass through.
type failingCommentUpsertStore struct {
	storage.ApplyCommentStore
	healed *atomic.Bool
}

func (s *failingCommentUpsertStore) Upsert(ctx context.Context, comment *storage.ApplyComment) error {
	if s.healed.Load() {
		return s.ApplyCommentStore.Upsert(ctx, comment)
	}
	return errors.New("injected comment upsert failure")
}

// failingCommentUpsertStorage serves the failing comment store while passing
// every other store through to the real storage. heal ends the outage so
// later Upserts land.
type failingCommentUpsertStorage struct {
	storage.Storage
	healed atomic.Bool
}

func (s *failingCommentUpsertStorage) ApplyComments() storage.ApplyCommentStore {
	return &failingCommentUpsertStore{ApplyCommentStore: s.Storage.ApplyComments(), healed: &s.healed}
}

func (s *failingCommentUpsertStorage) heal() { s.healed.Store(true) }

// A volume-change rotation whose fresh comment posts to the PR but fails to be
// recorded as the tracked comment must not freeze the prior comment: the
// tracked row still points at the prior comment, so later progress edits land
// there, and a frozen "superseded" body would fight those edits. While the
// outage lasts, later ticks retry only the tracking write (adoption) — never
// another post — so duplicates stay bounded at one. Once storage heals, the
// next tick adopts the already-live fresh comment: the tracked row (and the
// freeze owed to the prior comment, recorded in the same write) points at it,
// the prior comment is frozen with a link to its successor, and progress edits
// move to the adopted comment. A volume revert after adoption rotates again
// off the adopted comment, so an operator's revert always gets its own fresh
// comment.
func TestE2EVolumeRotationUntrackedFreshCommentAdoptedWhenStorageHeals(t *testing.T) {
	ctx := t.Context()

	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:       "org/repo-volume-untracked",
		pr:         144,
		database:   "e2e_volume_untracked_db",
		applyState: state.Apply.Running,
		volume:     3,
	})
	st, apply, task, capture, h := f.st, f.apply, f.task, f.capture, f.handler

	// Seed the tracked progress comment at the starting level through the real
	// store, the way the accepted-apply handler does (recording the level from
	// the apply's options).
	h.postAndTrackComment(ctx, "org/repo-volume-untracked", 144, 12345, apply, state.Comment.Progress, "Copying at volume 3")
	initialProgressID := requireCommentCreate(t, capture)

	// The observer's comment-tracking writes hit the failing store; reads and
	// every other store pass through.
	failingStorage := &failingCommentUpsertStorage{Storage: st}
	fake := clock.NewFake(task.CreatedAt)
	obs := f.newObserver(failingStorage, fake)

	// The level change posts the fresh comment, but recording it as the tracked
	// comment fails — the prior comment must not be frozen.
	apply.SetOptions(storage.ApplyOptions{Volume: 5})
	fake.Advance(activeInterval + time.Second)
	obs.OnProgress(apply, []*storage.Task{task})

	var freshProgressID int64
	select {
	case created := <-capture.creates:
		freshProgressID = created.ID
		assert.Contains(t, created.Body, "Volume: 5/11", "the fresh comment renders the new level")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the volume change to post a fresh progress comment")
	}
	select {
	case edited := <-capture.edits:
		t.Fatalf("no comment may be edited when the fresh comment was not tracked; got an edit of %d: %s", edited.CommentID, edited.Body)
	case <-time.After(100 * time.Millisecond):
	}

	// While the outage lasts, a later tick retries only the tracking write — it
	// does not post a duplicate for the same level — and its progress edit
	// lands on the prior comment, still the tracked one.
	task.RowsCopied = 700
	task.ProgressPercent = 70
	fake.Advance(activeInterval + time.Second)
	obs.OnProgress(apply, []*storage.Task{task})

	select {
	case edited := <-capture.edits:
		assert.Equal(t, initialProgressID, edited.CommentID, "progress edits continue on the prior tracked comment")
		assert.Contains(t, edited.Body, "70%")
	case created := <-capture.creates:
		t.Fatalf("an untracked rotation must not repost for the same level; got another new comment %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected an in-place edit of the prior tracked comment")
	}

	// Once storage heals, the next tick adopts the already-live fresh comment
	// instead of posting another: the prior comment is frozen pointing at its
	// successor, and the tick's progress edit lands on the adopted comment.
	failingStorage.heal()
	task.RowsCopied = 800
	task.ProgressPercent = 80
	fake.Advance(activeInterval + time.Second)
	obs.OnProgress(apply, []*storage.Task{task})

	select {
	case edited := <-capture.edits:
		assert.Equal(t, initialProgressID, edited.CommentID, "the freeze edit lands on the superseded comment")
		assert.Contains(t, edited.Body, "Volume changed to **5/11**")
		assert.Contains(t, edited.Body, fmt.Sprintf("#issuecomment-%d", freshProgressID), "the frozen comment links to its adopted successor")
		assert.Contains(t, edited.Body, "<details>")
	case created := <-capture.creates:
		t.Fatalf("adoption must not post another comment; got %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected the superseded progress comment to be frozen after adoption")
	}
	select {
	case edited := <-capture.edits:
		assert.Equal(t, freshProgressID, edited.CommentID, "progress edits move to the adopted comment")
		assert.Contains(t, edited.Body, "80%")
	case <-time.After(5 * time.Second):
		t.Fatal("expected a progress edit on the adopted comment")
	}

	// The tracked row now points at the adopted comment at the new level, with
	// no freeze left owing.
	prog, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, prog)
	assert.Equal(t, freshProgressID, prog.GitHubCommentID)
	require.NotNil(t, prog.PostedVolume)
	assert.Equal(t, 5, *prog.PostedVolume)
	assert.Nil(t, prog.PendingFreezeCommentID)

	// An operator reverting the volume after adoption rotates again: a fresh
	// comment at the reverted level, with the adopted comment frozen in turn.
	apply.SetOptions(storage.ApplyOptions{Volume: 3})
	fake.Advance(activeInterval + time.Second)
	obs.OnProgress(apply, []*storage.Task{task})

	var revertedProgressID int64
	select {
	case created := <-capture.creates:
		revertedProgressID = created.ID
		assert.Contains(t, created.Body, "Volume: 3/11", "the revert posts a fresh comment at the reverted level")
	case <-time.After(5 * time.Second):
		t.Fatal("expected a volume revert after adoption to rotate again")
	}
	select {
	case edited := <-capture.edits:
		assert.Equal(t, freshProgressID, edited.CommentID, "the adopted comment is frozen in turn")
		assert.Contains(t, edited.Body, "Volume changed to **3/11**")
		assert.Contains(t, edited.Body, fmt.Sprintf("#issuecomment-%d", revertedProgressID), "the frozen comment links to the revert's fresh comment")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the adopted comment to be frozen after the revert")
	}
}

// A rotation that tracked its fresh comment but died before the freeze edit
// landed leaves the pending-freeze marker on the tracked row. A later drive's
// observer — with no in-memory state from the rotation — reconciles it on its
// first volume check: the superseded comment is frozen pointing at its
// successor and the marker is cleared, without posting anything new. The fold
// uses the generic superseded rendering — the marker records which comment is
// owed, not which rotation superseded it.
func TestE2EVolumeRotationReconcilesPendingFreezeFromPriorDrive(t *testing.T) {
	ctx := t.Context()

	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:       "org/repo-volume-freeze",
		pr:         145,
		database:   "e2e_volume_freeze_db",
		applyState: state.Apply.Running,
		volume:     5,
	})
	st, apply, task, capture, h := f.st, f.apply, f.task, f.capture, f.handler

	// Seed the two comments the prior drive left on the PR: the superseded
	// comment still showing live progress, and the fresh comment it rotated to.
	h.postAndTrackComment(ctx, "org/repo-volume-freeze", 145, 12345, apply, state.Comment.Progress, "Copying at volume 3")
	supersededID := requireCommentCreate(t, capture)
	h.postAndTrackComment(ctx, "org/repo-volume-freeze", 145, 12345, apply, state.Comment.Progress, "Copying at volume 5")
	freshID := requireCommentCreate(t, capture)

	// The prior drive recorded the freeze it owed in the same write that
	// tracked the fresh comment, then died before the freeze edit landed.
	postedVolume := 5
	require.NoError(t, st.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:                apply.ID,
		CommentState:           state.Comment.Progress,
		GitHubCommentID:        freshID,
		PostedVolume:           &postedVolume,
		PendingFreezeCommentID: &supersededID,
	}))

	fake := clock.NewFake(task.CreatedAt)
	obs := f.newObserver(st, fake)

	// The new drive's first tick reconciles the owed freeze even though the
	// levels already match — no rotation, no new comment.
	fake.Advance(activeInterval + time.Second)
	obs.OnProgress(apply, []*storage.Task{task})

	select {
	case edited := <-capture.edits:
		assert.Equal(t, supersededID, edited.CommentID, "the reconciled freeze lands on the superseded comment")
		assert.Contains(t, edited.Body, "Progress comment superseded",
			"the owed fold uses the generic rendering since the superseding rotation is not recorded")
		assert.Contains(t, edited.Body, fmt.Sprintf("#issuecomment-%d", freshID), "the frozen comment links to its successor")
		assert.Contains(t, edited.Body, "Copying at volume 3", "the superseded body is preserved inside the fold")
	case created := <-capture.creates:
		t.Fatalf("reconciling a pending freeze must not post a new comment; got %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected the pending freeze to be reconciled on the first tick")
	}

	// The marker is cleared once the freeze lands; the row still tracks the
	// fresh comment.
	prog, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, prog)
	assert.Nil(t, prog.PendingFreezeCommentID)
	assert.Equal(t, freshID, prog.GitHubCommentID)

	// The same tick's progress edit lands on the tracked fresh comment.
	select {
	case edited := <-capture.edits:
		assert.Equal(t, freshID, edited.CommentID, "the tick's progress edit lands on the tracked comment")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the tick's progress edit on the tracked comment")
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

// seedReconcileScenario clears the shared storage rows for a repo, then seeds a
// GitHub-backed apply in the given state with one task and a tracked progress
// comment but no summary marker — a restart that lost the terminal summary.
// Stopped applies keep completed_at NULL (a stop is resumable), matching how
// stop reconciliation leaves them.
func seedReconcileScenario(t *testing.T, st storage.Storage, schemabotDB *sql.DB, repo, database, applyState, taskState string) *storage.Apply {
	t.Helper()
	ctx := t.Context()

	for _, stmt := range []string{
		"DELETE FROM apply_comments",
		"DELETE FROM tasks WHERE repository = ?",
		"DELETE FROM applies WHERE repository = ?",
		"DELETE FROM locks WHERE repository = ?",
	} {
		args := []any{repo}
		if stmt == "DELETE FROM apply_comments" {
			args = nil
		}
		_, err := schemabotDB.ExecContext(ctx, stmt, args...)
		require.NoError(t, err)
	}

	lock := &storage.Lock{
		DatabaseName: database,
		DatabaseType: storage.DatabaseTypeMySQL,
		Repository:   repo,
		PullRequest:  44,
		Owner:        repo + "#44",
	}
	require.NoError(t, st.Locks().Acquire(ctx, lock))
	lock, err := st.Locks().Get(ctx, database, storage.DatabaseTypeMySQL)
	require.NoError(t, err)

	now := time.Now()
	startedAt := now.Add(-time.Minute)
	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply_reconcile_%s_%d", applyState, now.UnixNano()),
		LockID:          lock.ID,
		PlanID:          1,
		Database:        database,
		DatabaseType:    storage.DatabaseTypeMySQL,
		Repository:      repo,
		PullRequest:     44,
		Environment:     "staging",
		Caller:          repo + "#44",
		InstallationID:  12345,
		Engine:          storage.EngineSpirit,
		State:           applyState,
	}
	applyID, err := st.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID
	apply.StartedAt = &startedAt
	if applyState != state.Apply.Stopped {
		apply.CompletedAt = &now
	}
	require.NoError(t, st.Applies().Update(ctx, apply))

	task := &storage.Task{
		TaskIdentifier: fmt.Sprintf("task_reconcile_%s_%d", applyState, now.UnixNano()),
		ApplyID:        applyID,
		PlanID:         1,
		Database:       database,
		DatabaseType:   storage.DatabaseTypeMySQL,
		Engine:         storage.EngineSpirit,
		Repository:     repo,
		PullRequest:    44,
		Environment:    "staging",
		State:          taskState,
		TableName:      "reconcile_users",
		DDL:            "ALTER TABLE reconcile_users ADD COLUMN email VARCHAR(255)",
		DDLAction:      "alter",
		RowsCopied:     5,
		RowsTotal:      10,
		CreatedAt:      startedAt,
		UpdatedAt:      now,
		StartedAt:      &startedAt,
	}
	_, err = st.Tasks().Create(ctx, task)
	require.NoError(t, err)

	require.NoError(t, st.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:         applyID,
		CommentState:    state.Comment.Progress,
		GitHubCommentID: 9001,
	}))
	return apply
}

// TestE2EReconcileMissingSummaryCommentsRepairsStoppedApply verifies startup
// reconciliation repairs a stopped apply that lost its terminal summary — the
// operator stopped the apply (stop reconciliation, driver crash) and no
// publisher posted the "⏹️ Stopped" summary before the restart. The PR must
// still get exactly one stopped summary, and the recorded marker must prevent
// a repeat on the next startup.
func TestE2EReconcileMissingSummaryCommentsRepairsStoppedApply(t *testing.T) {
	ctx := t.Context()

	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	// Redundant early-exit closer: svc owns the storage (and this handle) and
	// closes it below, so discard the guaranteed already-closed error.
	t.Cleanup(func() { _ = schemabotDB.Close() })
	st := mysqlstore.New(schemabotDB)

	apply := seedReconcileScenario(t, st, schemabotDB, "org/reconcile-stopped", "e2e_reconcile_stopped_db", state.Apply.Stopped, state.Task.Stopped)

	installClient, capture := setupFakeGitHubForComments(t)
	factory := &fakeClientFactory{client: installClient}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	svc := api.New(st, &api.ServerConfig{}, map[string]tern.Client{}, logger)
	t.Cleanup(func() { utils.CloseAndLog(svc) })

	h := NewHandler(svc, factory, nil, logger)
	h.ReconcileMissingSummaryComments(ctx)

	var created commentCreate
	select {
	case created = <-capture.creates:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for stopped summary comment")
	}
	assert.Contains(t, created.Body, "Schema Change Stopped")
	assert.Contains(t, created.Body, "reconcile_users")
	assert.Contains(t, created.Body, apply.ApplyIdentifier)

	summaryComment, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, summaryComment)
	assert.Equal(t, created.ID, summaryComment.GitHubCommentID)
}

// TestE2EReconcileMissingSummaryCommentsRespectsFreshClaim verifies the
// reconciler defers to an in-flight publisher: an apply whose summary marker is
// a fresh claim sentinel is being posted right now by another writer, so the
// reconciler must not post a duplicate and must leave the claim untouched.
func TestE2EReconcileMissingSummaryCommentsRespectsFreshClaim(t *testing.T) {
	ctx := t.Context()

	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	// Redundant early-exit closer: svc owns the storage (and this handle) and
	// closes it below, so discard the guaranteed already-closed error.
	t.Cleanup(func() { _ = schemabotDB.Close() })
	st := mysqlstore.New(schemabotDB)

	apply := seedReconcileScenario(t, st, schemabotDB, "org/reconcile-claimed", "e2e_reconcile_claimed_db", state.Apply.Completed, state.Task.Completed)

	won, err := st.ApplyComments().ClaimSummaryComment(ctx, apply.ID)
	require.NoError(t, err)
	require.True(t, won)

	installClient, capture := setupFakeGitHubForComments(t)
	factory := &fakeClientFactory{client: installClient}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	svc := api.New(st, &api.ServerConfig{}, map[string]tern.Client{}, logger)
	t.Cleanup(func() { utils.CloseAndLog(svc) })

	h := NewHandler(svc, factory, nil, logger)
	h.ReconcileMissingSummaryComments(ctx)

	select {
	case created := <-capture.creates:
		t.Fatalf("reconciler must not post while a fresh claim is held, posted: %q", created.Body)
	default:
	}

	marker, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, marker)
	assert.Equal(t, int64(0), marker.GitHubCommentID, "the in-flight claim sentinel must survive reconciliation")
}

// TestE2EAggregateTerminalObserverClaimsSummaryExactlyOnce verifies the
// summary-marker claim makes concurrent terminal publishers exactly-once: when
// two aggregate CAS-winner observers (for example stop reconciliation's
// publisher racing a still-live driver observer) both reach the terminal
// summary step for the same apply, exactly one summary comment lands on the PR
// and the marker records its comment ID.
func TestE2EAggregateTerminalObserverClaimsSummaryExactlyOnce(t *testing.T) {
	ctx := t.Context()

	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(schemabotDB) })
	st := mysqlstore.New(schemabotDB)

	apply := seedReconcileScenario(t, st, schemabotDB, "org/claim-once", "e2e_claim_once_db", state.Apply.Stopped, state.Task.Stopped)
	tasks, err := st.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)

	installClient, capture := setupFakeGitHubForComments(t)
	factory := &fakeClientFactory{client: installClient}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	newObserver := func() *CommentObserver {
		return NewAggregateTerminalCommentObserver(CommentObserverConfig{
			GHClient:       factory,
			Storage:        st,
			Repo:           apply.Repository,
			PR:             apply.PullRequest,
			InstallationID: apply.InstallationID,
			ApplyID:        apply.ID,
			Logger:         logger,
		})
	}

	newObserver().OnTerminal(apply, tasks)
	newObserver().OnTerminal(apply, tasks)

	var created commentCreate
	select {
	case created = <-capture.creates:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for the claimed summary comment")
	}
	assert.Contains(t, created.Body, "Schema Change Stopped")
	assert.Contains(t, created.Body, apply.ApplyIdentifier)

	select {
	case duplicate := <-capture.creates:
		t.Fatalf("the second publisher must lose the claim and skip, posted duplicate: %q", duplicate.Body)
	default:
	}

	marker, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, marker)
	assert.Equal(t, created.ID, marker.GitHubCommentID)
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
	apply.ID = applyID

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
	h.postAndTrackComment(ctx, "org/repo-resume", 43, 12345, apply, state.Comment.Progress, "Resumed progress")

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
	h.postAndTrackComment(ctx, "org/repo-resume", 43, 12345, apply, state.Comment.Summary, "Resumed summary")

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

// An apply can reach a terminal state before its initial progress comment is
// posted — a metadata-only DDL finishes faster than the handler's post, so the
// driver's observer has already found nothing to edit at terminal. The handler
// re-checks the apply after posting and finalizes the comment in place, so the
// PR never shows a progress comment frozen at "Starting" after the success
// summary. An apply that is still active is left to the observer to edit.
func TestE2EInitialProgressCommentFinalizedForFastApply(t *testing.T) {
	ctx := t.Context()

	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(schemabotDB) })
	st := mysqlstore.New(schemabotDB)

	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM apply_comments WHERE apply_id IN (SELECT id FROM applies WHERE repository = 'org/fastapply-repo')")
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM applies WHERE repository = 'org/fastapply-repo'")

	newApply := func(t *testing.T, applyState string) *storage.Apply {
		t.Helper()
		apply := &storage.Apply{
			ApplyIdentifier: fmt.Sprintf("apply_e2e_fast_%d", time.Now().UnixNano()),
			PlanID:          1,
			Database:        "e2e_fast_db",
			DatabaseType:    "mysql",
			Repository:      "org/fastapply-repo",
			PullRequest:     7,
			Environment:     "staging",
			InstallationID:  12345,
			Engine:          "spirit",
			State:           applyState,
		}
		applyID, err := st.Applies().Create(ctx, apply)
		require.NoError(t, err)
		apply.ID = applyID
		return apply
	}

	newHandler := func(t *testing.T) (*Handler, *commentCapture) {
		t.Helper()
		installClient, capture := setupFakeGitHubForComments(t)
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		svc := api.New(st, &api.ServerConfig{}, nil, logger)
		h := NewHandler(svc, &fakeClientFactory{client: installClient}, nil, logger)
		return h, capture
	}

	t.Run("already-terminal apply finalizes the comment in place", func(t *testing.T) {
		apply := newApply(t, state.Apply.Completed)
		h, capture := newHandler(t)

		pending := *apply
		pending.State = state.Apply.Pending
		h.postInitialProgressComment(ctx, apply.Repository, apply.PullRequest, apply.InstallationID, apply,
			formatProgressComment(&pending, nil, nil, ""))

		var created commentCreate
		select {
		case created = <-capture.creates:
			assert.Contains(t, created.Body, "**Status**: Starting")
		case <-time.After(webhookIntegrationCheckRunDeadline):
			t.Fatal("timed out waiting for the initial progress comment")
		}

		select {
		case edited := <-capture.edits:
			assert.Equal(t, created.ID, edited.CommentID, "the finalize edits the just-posted comment")
			assert.Contains(t, edited.Body, "**Status**: Applied")
			assert.NotContains(t, edited.Body, "**Status**: Starting")
		case <-time.After(webhookIntegrationCheckRunDeadline):
			t.Fatal("timed out waiting for the terminal finalize edit")
		}

		comment, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
		require.NoError(t, err)
		require.NotNil(t, comment)
		assert.Equal(t, 1, comment.EditCount)
	})

	t.Run("observer-edited comment is not overwritten by the finalize", func(t *testing.T) {
		apply := newApply(t, state.Apply.Completed)
		h, capture := newHandler(t)

		// The observer already found and edited the tracked comment; its
		// terminal edit carries the full per-operation rendering, so the
		// handler's no-operations fallback must not overwrite it.
		require.NoError(t, st.ApplyComments().Upsert(ctx, &storage.ApplyComment{
			ApplyID: apply.ID, CommentState: state.Comment.Progress, GitHubCommentID: 424242,
		}))
		require.NoError(t, st.ApplyComments().IncrementEditCount(ctx, apply.ID, state.Comment.Progress))

		h.postInitialProgressComment(ctx, apply.Repository, apply.PullRequest, apply.InstallationID, apply,
			formatProgressComment(apply, nil, nil, ""))

		select {
		case <-capture.creates:
		case <-time.After(webhookIntegrationCheckRunDeadline):
			t.Fatal("timed out waiting for the initial progress comment")
		}

		select {
		case edited := <-capture.edits:
			t.Fatalf("an observer-edited comment must not be finalized by the handler, got edit: %q", edited.Body)
		case <-time.After(500 * time.Millisecond):
			// expected: the observer's terminal edit owns the final body
		}
	})

	t.Run("active apply is left to the observer", func(t *testing.T) {
		apply := newApply(t, state.Apply.Running)
		h, capture := newHandler(t)

		h.postInitialProgressComment(ctx, apply.Repository, apply.PullRequest, apply.InstallationID, apply,
			formatProgressComment(apply, nil, nil, ""))

		select {
		case <-capture.creates:
		case <-time.After(webhookIntegrationCheckRunDeadline):
			t.Fatal("timed out waiting for the initial progress comment")
		}

		select {
		case edited := <-capture.edits:
			t.Fatalf("active apply must not be finalized by the handler, got edit: %q", edited.Body)
		case <-time.After(500 * time.Millisecond):
			// expected: the observer owns all further edits
		}
	})
}
