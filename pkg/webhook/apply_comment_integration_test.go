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
	schemabotDB, err := sql.Open("block-mysql", e2eSchemabotDSN)
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
		assert.Contains(t, edited.Body, "70.00%", "once running, the new comment shows real row-copy progress")
	case created := <-capture.creates:
		t.Fatalf("resume must rotate exactly once; got another new comment %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected an in-place edit of the new progress comment on the next tick")
	}
}

// A resume rotation whose fresh comment lands but whose summary-marker
// consumption fails must not leave the marker active for the rest of the
// drive: the marker is the durable signal that the next terminal summary needs
// posting fresh, and a rotated-away marker still recorded as active would cost
// the apply its eventual terminal summary. Later ticks retry only the
// supersede — never a second rotation — and once storage heals the marker is
// consumed.
func TestE2EResumeSummaryMarkerSupersedeRetriedUntilItLands(t *testing.T) {
	ctx := t.Context()

	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:       "org/repo-resume-marker-retry",
		pr:         156,
		database:   "e2e_resume_marker_retry_db",
		applyState: state.Apply.Resuming,
	})
	st, apply, task, capture, h := f.st, f.apply, f.task, f.capture, f.handler

	// Seed the pre-resume comments left by the stop: the progress comment frozen
	// at "Stopped" and the stopped summary marker that signals a resume rotation
	// is owed.
	h.postAndTrackComment(ctx, "org/repo-resume-marker-retry", 156, 12345, apply, state.Comment.Progress, "Stopped at 21%")
	stoppedProgressID := requireCommentCreate(t, capture)
	h.postAndTrackComment(ctx, "org/repo-resume-marker-retry", 156, 12345, apply, state.Comment.Summary, "Schema Change Stopped")
	requireCommentCreate(t, capture)

	failingStorage := &failingCommentSupersedeStorage{Storage: st}
	fake := clock.NewFake(task.CreatedAt)
	obs := f.newObserver(failingStorage, fake)

	// The first tick rotates: the fresh comment posts and the stopped one is
	// frozen, but consuming the summary marker fails at storage.
	obs.OnProgress(apply, []*storage.Task{task})

	var newProgressID int64
	select {
	case created := <-capture.creates:
		newProgressID = created.ID
		assert.Contains(t, created.Body, "**Status**: Resuming")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the resume rotation to post a fresh progress comment")
	}
	select {
	case edited := <-capture.edits:
		assert.Equal(t, stoppedProgressID, edited.CommentID, "the freeze edit lands on the superseded comment")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the superseded progress comment to be frozen")
	}
	summary, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, summary)
	assert.Nil(t, summary.SupersededAt, "the storage outage leaves the marker recorded as active")

	// While the outage lasts, later ticks retry only the supersede: progress
	// edits land in place on the fresh comment and no duplicate rotation posts.
	apply.State = state.Apply.Running
	fake.Advance(activeInterval + time.Second)
	task.RowsCopied = 700
	task.ProgressPercent = 70
	obs.OnProgress(apply, []*storage.Task{task})

	select {
	case edited := <-capture.edits:
		assert.Equal(t, newProgressID, edited.CommentID, "ticks during the outage keep editing the rotated comment")
	case created := <-capture.creates:
		t.Fatalf("the supersede retry must not rotate again; got another new comment %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected an in-place edit of the rotated comment during the outage")
	}
	summary, err = st.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, summary)
	assert.Nil(t, summary.SupersededAt, "the marker stays active until the supersede lands")

	// Storage heals: the next tick's retry consumes the marker without touching
	// the rotated comment beyond its normal in-place edit.
	failingStorage.heal()
	fake.Advance(activeInterval + time.Second)
	task.RowsCopied = 800
	task.ProgressPercent = 80
	obs.OnProgress(apply, []*storage.Task{task})

	select {
	case edited := <-capture.edits:
		assert.Equal(t, newProgressID, edited.CommentID, "the healed tick still edits the rotated comment in place")
	case created := <-capture.creates:
		t.Fatalf("the healed retry must not rotate again; got another new comment %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected an in-place edit of the rotated comment after storage heals")
	}
	summary, err = st.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, summary)
	assert.NotNil(t, summary.SupersededAt, "the retried supersede consumes the marker once storage heals")
}

// While a terminal-summary publish is in flight — the summary marker exists
// only as a claim sentinel that has not yet recorded its posted comment — a
// resumed apply's ticks must not rotate: superseding the sentinel mid-publish
// would transfer the claim and let a later writer post a duplicate summary.
// The rotation waits until the marker records its posted comment, then rotates
// on the next tick.
func TestE2EResumeRotationWaitsForSummaryClaimSentinel(t *testing.T) {
	ctx := t.Context()

	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:       "org/repo-resume-sentinel",
		pr:         157,
		database:   "e2e_resume_sentinel_db",
		applyState: state.Apply.Resuming,
	})
	st, apply, task, capture, h := f.st, f.apply, f.task, f.capture, f.handler

	h.postAndTrackComment(ctx, "org/repo-resume-sentinel", 157, 12345, apply, state.Comment.Progress, "Stopped at 21%")
	stoppedProgressID := requireCommentCreate(t, capture)

	// A stop-side publisher has claimed the summary but not yet posted it: the
	// marker row is a claim sentinel with no comment ID.
	won, err := st.ApplyComments().ClaimSummaryComment(ctx, apply.ID)
	require.NoError(t, err)
	require.True(t, won, "the claim sentinel is created for the in-flight publish")

	fake := clock.NewFake(task.CreatedAt)
	obs := f.newObserver(st, fake)

	// Ticks while the sentinel is live edit the tracked comment in place and do
	// not rotate.
	obs.OnProgress(apply, []*storage.Task{task})
	select {
	case created := <-capture.creates:
		t.Fatalf("rotation must wait for the in-flight summary publish; got new comment %d: %s", created.ID, created.Body)
	case edited := <-capture.edits:
		assert.Equal(t, stoppedProgressID, edited.CommentID, "ticks keep editing the tracked comment while the publish is in flight")
	case <-time.After(5 * time.Second):
		t.Fatal("expected an in-place edit of the tracked comment while the sentinel is live")
	}

	// The publisher finishes: the marker records its posted summary comment.
	// The next tick sees an active posted marker and rotates.
	require.NoError(t, st.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:         apply.ID,
		CommentState:    state.Comment.Summary,
		GitHubCommentID: 70001,
	}))
	fake.Advance(activeInterval + time.Second)
	obs.OnProgress(apply, []*storage.Task{task})

	select {
	case created := <-capture.creates:
		assert.Contains(t, created.Body, "**Status**: Resuming")
		assert.NotEqual(t, stoppedProgressID, created.ID, "the rotation posts a new comment, not a reuse of the stopped one")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the rotation once the summary marker records its posted comment")
	}
	summary, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, summary)
	assert.NotNil(t, summary.SupersededAt, "the rotation consumes the posted marker")
}

// An apply that is stopped, resumed, and then driven to completion must still
// get its terminal summary comment: the stop posts a summary, the resume
// rotation consumes (supersedes) that summary marker, and completion claims
// the summary again — the superseded marker converts back into a claim
// sentinel instead of blocking the claim — so the operator sees the final
// result at the bottom of the PR rather than the apply ending silently.
func TestE2EStopResumeCompleteStillPostsTerminalSummary(t *testing.T) {
	ctx := t.Context()

	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:       "org/repo-stop-resume",
		pr:         143,
		database:   "e2e_stop_resume_db",
		applyState: state.Apply.Resuming,
	})
	st, apply, task, capture := f.st, f.apply, f.task, f.capture

	// Seed the comments the stop left behind: the progress comment frozen at
	// "Stopped" and the stopped summary marker.
	f.handler.postAndTrackComment(ctx, apply.Repository, apply.PullRequest, 12345, apply, state.Comment.Progress, "Stopped at 50%")
	requireCommentCreate(t, capture)
	f.handler.postAndTrackComment(ctx, apply.Repository, apply.PullRequest, 12345, apply, state.Comment.Summary, "Schema Change Stopped")
	stoppedSummaryID := requireCommentCreate(t, capture)

	fake := clock.NewFake(task.CreatedAt)
	obs := f.newObserver(st, fake)

	// The first progress tick after resume rotates the progress comment and
	// consumes the stopped summary marker by superseding it.
	obs.OnProgress(apply, []*storage.Task{task})
	requireCommentCreate(t, capture)
	select {
	case <-capture.edits: // freeze of the superseded progress comment
	case <-time.After(5 * time.Second):
		t.Fatal("expected the superseded progress comment to be frozen")
	}
	summary, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, summary)
	require.NotNil(t, summary.SupersededAt, "resume rotation must supersede the stopped summary marker")

	// The resumed copy finishes and the apply completes.
	apply.State = state.Apply.Completed
	task.State = state.Task.Completed
	task.RowsCopied = 1000
	task.ProgressPercent = 100
	obs.OnTerminal(apply, []*storage.Task{task})

	// The terminal publish edits the progress comment to its final rendering
	// and posts a fresh terminal summary comment.
	select {
	case <-capture.edits:
	case <-time.After(5 * time.Second):
		t.Fatal("expected the progress comment to be edited to its final rendering")
	}
	var terminalSummaryID int64
	select {
	case created := <-capture.creates:
		terminalSummaryID = created.ID
		assert.Contains(t, created.Body, "Schema Change Applied")
	case <-time.After(5 * time.Second):
		t.Fatal("expected a terminal summary comment for the completed apply")
	}
	assert.NotEqual(t, stoppedSummaryID, terminalSummaryID, "the terminal summary is a fresh comment, not the stop's")

	// The summary marker now records the terminal summary and is active again.
	summary, err = st.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, summary)
	assert.Equal(t, terminalSummaryID, summary.GitHubCommentID)
	assert.Nil(t, summary.SupersededAt)
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
// fixture. An unset deferCutover seeds the apply without options.
type applyCommentFixtureParams struct {
	repo         string
	pr           int
	database     string
	applyState   string
	deferCutover bool
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

	schemabotDB, err := sql.Open("block-mysql", e2eSchemabotDSN)
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
	if p.deferCutover {
		apply.SetOptions(storage.ApplyOptions{DeferCutover: p.deferCutover})
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

// When an operator's revert takes effect on an apply in its revert window, the
// observer posts a fresh progress comment tracking the revert — a new comment
// at the bottom of the PR timeline is where the operator looks for the effect
// of the command they just issued. The prior progress comment is frozen at its
// pre-revert rendering inside a collapsed details block pointing at its
// successor, later progress edits land on the new comment, and neither a later
// tick nor a fresh observer on a later drive claim rotates again: the fresh
// comment durably records that it was posted while the apply was reverting.
func TestE2ERevertRotatesProgressComment(t *testing.T) {
	ctx := t.Context()

	// The apply completed its copy and is holding its revert window open when
	// the operator issues the revert.
	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:       "org/repo-revert",
		pr:         144,
		database:   "e2e_revert_db",
		applyState: state.Apply.RevertWindow,
	})
	st, apply, task, capture, h := f.st, f.apply, f.task, f.capture, f.handler

	// Seed the tracked progress comment as it stood during the revert window;
	// posted before the revert, it records that the apply was not yet reverting.
	h.postAndTrackComment(ctx, "org/repo-revert", 144, 12345, apply, state.Comment.Progress, "Revert window open — closes in 25m")
	preRevertID := requireCommentCreate(t, capture)

	fake := clock.NewFake(task.CreatedAt)
	obs := f.newObserver(st, fake)

	// The revert command was accepted and the driver is unwinding the change.
	// The first progress tick while reverting rotates the progress comment.
	apply.State = state.Apply.Reverting
	obs.OnProgress(apply, []*storage.Task{task})

	var newProgressID int64
	select {
	case created := <-capture.creates:
		newProgressID = created.ID
		assert.Contains(t, created.Body, "Schema Change Status — Staging")
		assert.Contains(t, created.Body, "**Status**: Reverting")
	case <-time.After(5 * time.Second):
		t.Fatal("expected a new progress comment to be posted when the revert takes effect")
	}
	assert.NotEqual(t, preRevertID, newProgressID, "the revert must post a new comment, not reuse the pre-revert one")

	// The prior comment is frozen into a collapsed details block pointing at
	// its successor, with the pre-revert body preserved inside the fold.
	select {
	case edited := <-capture.edits:
		assert.Equal(t, preRevertID, edited.CommentID, "the freeze edit lands on the superseded comment")
		assert.Contains(t, edited.Body, "Schema change reverting")
		assert.Contains(t, edited.Body, fmt.Sprintf("#issuecomment-%d", newProgressID), "the frozen comment links to its successor")
		assert.Contains(t, edited.Body, "<details>")
		assert.Contains(t, edited.Body, "Revert window open", "the pre-revert body is preserved inside the fold")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the superseded progress comment to be frozen")
	}

	// The progress row tracks the new comment with no freeze left owing, and
	// records that the comment was posted while the apply was reverting.
	prog, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, prog)
	assert.Equal(t, newProgressID, prog.GitHubCommentID)
	assert.Nil(t, prog.PendingFreezeCommentID)
	require.NotNil(t, prog.PostedPhase)
	assert.Equal(t, "reverting", *prog.PostedPhase, "the fresh comment records the reverting phase")

	// A later tick while still reverting edits the new comment in place — the
	// revert rotates exactly once.
	task.RowsCopied = 300
	task.ProgressPercent = 30
	fake.Advance(activeInterval + time.Second)
	obs.OnProgress(apply, []*storage.Task{task})
	select {
	case edited := <-capture.edits:
		assert.Equal(t, newProgressID, edited.CommentID, "later edits land on the new comment")
	case created := <-capture.creates:
		t.Fatalf("a revert must rotate exactly once; got another new comment %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected an in-place edit of the new progress comment on the next tick")
	}

	// A fresh observer — a later drive claim after a restart — sees the durable
	// record that the tracked comment already postdates the revert and edits it
	// in place instead of rotating again.
	obs2 := f.newObserver(st, fake)
	task.RowsCopied = 100
	task.ProgressPercent = 10
	fake.Advance(activeInterval + time.Second)
	obs2.OnProgress(apply, []*storage.Task{task})
	select {
	case edited := <-capture.edits:
		assert.Equal(t, newProgressID, edited.CommentID, "a fresh observer edits the tracked comment in place")
	case created := <-capture.creates:
		t.Fatalf("a fresh observer must not rotate for a revert already tracked; got new comment %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected the fresh observer to edit the tracked progress comment")
	}
}

// When an operator's skip-revert takes effect on an apply in its revert
// window, the observer posts a fresh progress comment tracking the finalizing
// schema change — the operator looks at the bottom of the PR timeline for the
// effect of the command they just issued. The operator origin is read from the
// durable skip-revert control request, so the rotation happens for commands
// and not for revert windows that close on their own. The prior progress
// comment is frozen at its revert-window rendering inside a collapsed details
// block pointing at its successor, and neither a later tick nor a fresh
// observer on a later drive claim rotates again: the fresh comment durably
// records the skipping-revert phase.
func TestE2ESkipRevertRotatesProgressComment(t *testing.T) {
	ctx := t.Context()

	// The apply completed its copy and is holding its revert window open when
	// the operator issues the skip-revert.
	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:       "org/repo-skip-revert",
		pr:         145,
		database:   "e2e_skip_revert_db",
		applyState: state.Apply.RevertWindow,
	})
	st, apply, task, capture, h := f.st, f.apply, f.task, f.capture, f.handler

	// Seed the tracked progress comment as it stood during the revert window;
	// posted before the skip-revert, it records the revert-window phase.
	h.postAndTrackComment(ctx, "org/repo-skip-revert", 145, 12345, apply, state.Comment.Progress, "Revert window open — closes in 25m")
	preSkipID := requireCommentCreate(t, capture)

	// The operator's skip-revert command was recorded durably before it was
	// acknowledged — the signal that the phase is operator-issued.
	_, _, err := st.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationSkipRevert,
		RequestedBy: "operator",
	})
	require.NoError(t, err)

	fake := clock.NewFake(task.CreatedAt)
	obs := f.newObserver(st, fake)

	// The skip-revert command was accepted and the driver is finalizing the
	// change. The first progress tick while skipping rotates the comment.
	apply.State = state.Apply.SkippingRevert
	obs.OnProgress(apply, []*storage.Task{task})

	var newProgressID int64
	select {
	case created := <-capture.creates:
		newProgressID = created.ID
		assert.Contains(t, created.Body, "Schema Change Status — Staging")
		assert.Contains(t, created.Body, "**Status**: Skipping revert")
	case <-time.After(5 * time.Second):
		t.Fatal("expected a new progress comment to be posted when the skip-revert takes effect")
	}
	assert.NotEqual(t, preSkipID, newProgressID, "the skip-revert must post a new comment, not reuse the revert-window one")

	// The prior comment is frozen into a collapsed details block pointing at
	// its successor, with the revert-window body preserved inside the fold.
	select {
	case edited := <-capture.edits:
		assert.Equal(t, preSkipID, edited.CommentID, "the freeze edit lands on the superseded comment")
		assert.Contains(t, edited.Body, "Revert skipped")
		assert.Contains(t, edited.Body, fmt.Sprintf("#issuecomment-%d", newProgressID), "the frozen comment links to its successor")
		assert.Contains(t, edited.Body, "<details>")
		assert.Contains(t, edited.Body, "Revert window open", "the revert-window body is preserved inside the fold")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the superseded progress comment to be frozen")
	}

	// The progress row tracks the new comment with no freeze left owing, and
	// records the skipping-revert phase.
	prog, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, prog)
	assert.Equal(t, newProgressID, prog.GitHubCommentID)
	assert.Nil(t, prog.PendingFreezeCommentID)
	require.NotNil(t, prog.PostedPhase)
	assert.Equal(t, "skipping_revert", *prog.PostedPhase, "the fresh comment records the skipping-revert phase")

	// A fresh observer — a later drive claim after a restart — sees the durable
	// record that the tracked comment already postdates the skip-revert and
	// edits it in place instead of rotating again.
	obs2 := f.newObserver(st, fake)
	task.RowsCopied = 700
	task.ProgressPercent = 70
	fake.Advance(activeInterval + time.Second)
	obs2.OnProgress(apply, []*storage.Task{task})
	select {
	case edited := <-capture.edits:
		assert.Equal(t, newProgressID, edited.CommentID, "a fresh observer edits the tracked comment in place")
	case created := <-capture.creates:
		t.Fatalf("a fresh observer must not rotate for a skip-revert already tracked; got new comment %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected the fresh observer to edit the tracked progress comment")
	}
}

// A revert window that closes on its own — the default end of every apply
// holding one — moves the apply through skipping-revert with no operator
// command involved. There is no durable skip-revert control request, so the
// observer does not rotate: the progress comment transitions in place instead
// of burying the timeline in a third comment for a routine apply. The same
// holds for a fresh observer on a later drive claim.
func TestE2ESkipRevertWithoutOperatorCommandEditsProgressCommentInPlace(t *testing.T) {
	ctx := t.Context()

	// The apply held its revert window open until the window expired.
	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:       "org/repo-skip-expiry",
		pr:         149,
		database:   "e2e_skip_expiry_db",
		applyState: state.Apply.RevertWindow,
	})
	st, apply, task, capture, h := f.st, f.apply, f.task, f.capture, f.handler

	// Seed the tracked progress comment as it stood during the revert window.
	// The handler stamps the posting-time phase on the tracked row — the
	// durable record control-phase rotation compares against.
	h.postAndTrackComment(ctx, "org/repo-skip-expiry", 149, 12345, apply, state.Comment.Progress, "Revert window open — closes in 25m")
	windowID := requireCommentCreate(t, capture)
	seeded, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, seeded)
	require.NotNil(t, seeded.PostedPhase)
	assert.Equal(t, "revert_window", *seeded.PostedPhase, "the handler records the phase the comment was posted in")

	fake := clock.NewFake(task.CreatedAt)
	obs := f.newObserver(st, fake)

	// The window expired and the driver is finalizing the change. No skip-revert
	// control request exists — nobody issued a command.
	apply.State = state.Apply.SkippingRevert
	obs.OnProgress(apply, []*storage.Task{task})

	select {
	case edited := <-capture.edits:
		assert.Equal(t, windowID, edited.CommentID, "the progress comment transitions in place")
		assert.Contains(t, edited.Body, "Skipping revert")
	case created := <-capture.creates:
		t.Fatalf("a window expiry must not rotate the progress comment; got new comment %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected an in-place edit of the tracked progress comment")
	}

	// A fresh observer — a later drive claim after a restart — re-checks the
	// durable record, finds no operator command, and also edits in place.
	obs2 := f.newObserver(st, fake)
	task.RowsCopied = 900
	task.ProgressPercent = 90
	fake.Advance(activeInterval + time.Second)
	obs2.OnProgress(apply, []*storage.Task{task})
	select {
	case edited := <-capture.edits:
		assert.Equal(t, windowID, edited.CommentID, "a fresh observer edits the tracked comment in place")
	case created := <-capture.creates:
		t.Fatalf("a fresh observer must not rotate for a window expiry; got new comment %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected the fresh observer to edit the tracked progress comment")
	}
}

// A control-phase rotation that tracked its fresh comment but died before the
// freeze edit landed leaves the pending-freeze marker on the tracked row. A
// later drive's observer — with no in-memory state from the rotation — finds
// the tracked comment already recording the phase, so no new rotation follows;
// it still reconciles the owed freeze: the superseded comment is frozen
// pointing at its successor and the marker is cleared, without posting
// anything new.
func TestE2EControlPhaseRotationReconcilesPendingFreezeFromPriorDrive(t *testing.T) {
	ctx := t.Context()

	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:       "org/repo-revert-freeze",
		pr:         150,
		database:   "e2e_revert_freeze_db",
		applyState: state.Apply.Reverting,
	})
	st, apply, task, capture, h := f.st, f.apply, f.task, f.capture, f.handler

	// Seed the two comments the prior drive left on the PR: the superseded
	// revert-window comment still showing its live rendering, and the fresh
	// comment the revert rotated to.
	h.postAndTrackComment(ctx, "org/repo-revert-freeze", 150, 12345, apply, state.Comment.Progress, "Revert window open — closes in 25m")
	supersededID := requireCommentCreate(t, capture)
	h.postAndTrackComment(ctx, "org/repo-revert-freeze", 150, 12345, apply, state.Comment.Progress, "Reverting schema change")
	freshID := requireCommentCreate(t, capture)

	// The prior drive recorded the freeze it owed in the same write that
	// tracked the fresh comment, then died before the freeze edit landed.
	postedPhase := "reverting"
	require.NoError(t, st.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:                apply.ID,
		CommentState:           state.Comment.Progress,
		GitHubCommentID:        freshID,
		PostedPhase:            &postedPhase,
		PendingFreezeCommentID: &supersededID,
	}))

	fake := clock.NewFake(task.CreatedAt)
	obs := f.newObserver(st, fake)

	// The new drive's first tick reconciles the owed freeze even though the
	// tracked comment already records the phase — no rotation, no new comment.
	fake.Advance(activeInterval + time.Second)
	obs.OnProgress(apply, []*storage.Task{task})

	select {
	case edited := <-capture.edits:
		assert.Equal(t, supersededID, edited.CommentID, "the reconciled freeze lands on the superseded comment")
		assert.Contains(t, edited.Body, "Progress comment superseded",
			"the owed fold uses the generic rendering since the superseding rotation is not recorded")
		assert.Contains(t, edited.Body, fmt.Sprintf("#issuecomment-%d", freshID), "the frozen comment links to its successor")
		assert.Contains(t, edited.Body, "Revert window open", "the superseded body is preserved inside the fold")
	case created := <-capture.creates:
		t.Fatalf("reconciling a pending freeze must not post a new comment; got %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected the pending freeze to be reconciled on the first tick")
	}

	// The marker is cleared once the freeze lands; the row still tracks the
	// fresh comment and its phase.
	prog, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, prog)
	assert.Nil(t, prog.PendingFreezeCommentID)
	assert.Equal(t, freshID, prog.GitHubCommentID)
	require.NotNil(t, prog.PostedPhase)
	assert.Equal(t, "reverting", *prog.PostedPhase)

	// The same tick's progress edit lands on the tracked fresh comment.
	select {
	case edited := <-capture.edits:
		assert.Equal(t, freshID, edited.CommentID, "the tick's progress edit lands on the tracked comment")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the tick's progress edit on the tracked comment")
	}
}

// When a deferred cutover completes into a still-active apply (the revert
// window), the observer unmutes and posts a fresh progress comment tracking
// the post-cutover phase — the operator issued the cutover command, so its
// effect belongs at the bottom of the PR timeline. The spent cutover prompt is
// frozen into a collapsed details block pointing at the fresh comment, its
// stored row is superseded so no later observer treats it as the live comment,
// and later progress edits land on the fresh comment.
func TestE2EDeferredCutoverRotatesProgressComment(t *testing.T) {
	ctx := t.Context()

	// The apply copied all rows and paused at the deferred-cutover gate: the
	// progress comment froze and the cutover prompt was posted.
	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:       "org/repo-cutover-rotate",
		pr:         146,
		database:   "e2e_cutover_rotate_db",
		applyState: state.Apply.WaitingForCutover,
	})
	st, apply, task, capture, h := f.st, f.apply, f.task, f.capture, f.handler

	h.postAndTrackComment(ctx, "org/repo-cutover-rotate", 146, 12345, apply, state.Comment.Progress, "Copy complete — waiting for cutover")
	preCutoverProgressID := requireCommentCreate(t, capture)
	h.postAndTrackComment(ctx, "org/repo-cutover-rotate", 146, 12345, apply, state.Comment.Cutover, "Ready for cutover — run `schemabot cutover` to proceed")
	cutoverPromptID := requireCommentCreate(t, capture)

	fake := clock.NewFake(task.CreatedAt)
	obs := f.newObserver(st, fake)

	// The operator ran the cutover and it completed into the revert window.
	// The first progress tick past the gate rotates: fresh comment in, prompt
	// folded.
	apply.State = state.Apply.RevertWindow
	task.State = state.Task.RevertWindow
	obs.OnProgress(apply, []*storage.Task{task})

	var newProgressID int64
	select {
	case created := <-capture.creates:
		newProgressID = created.ID
		assert.Contains(t, created.Body, "Schema Change Status — Staging")
		assert.Contains(t, created.Body, "**Status**: Revert Window")
	case <-time.After(5 * time.Second):
		t.Fatal("expected a new progress comment to be posted when the deferred cutover completes")
	}
	assert.NotEqual(t, cutoverPromptID, newProgressID, "the rotation must post a new comment, not reuse the cutover prompt")
	assert.NotEqual(t, preCutoverProgressID, newProgressID, "the rotation must post a new comment, not reuse the frozen progress comment")

	// The spent cutover prompt is frozen into a collapsed details block
	// pointing at its successor, with the prompt preserved inside the fold.
	select {
	case edited := <-capture.edits:
		assert.Equal(t, cutoverPromptID, edited.CommentID, "the freeze edit lands on the cutover prompt")
		assert.Contains(t, edited.Body, "Cutover complete")
		assert.Contains(t, edited.Body, fmt.Sprintf("#issuecomment-%d", newProgressID), "the frozen prompt links to its successor")
		assert.Contains(t, edited.Body, "<details>")
		assert.Contains(t, edited.Body, "Ready for cutover", "the prompt body is preserved inside the fold")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the cutover prompt to be frozen")
	}

	// The progress row tracks the new comment with no freeze left owing and
	// records the post-cutover phase; the cutover row is consumed so no later
	// observer treats the folded prompt as the live comment.
	prog, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, prog)
	assert.Equal(t, newProgressID, prog.GitHubCommentID)
	assert.Nil(t, prog.PendingFreezeCommentID)
	require.NotNil(t, prog.PostedPhase)
	assert.Equal(t, "revert_window", *prog.PostedPhase, "the fresh comment records the post-cutover phase")
	cutoverRow, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Cutover)
	require.NoError(t, err)
	require.NotNil(t, cutoverRow)
	assert.NotNil(t, cutoverRow.SupersededAt, "the cutover row is superseded once its prompt is folded")

	// A later tick edits the fresh comment in place — the observer is unmuted
	// and the rotation happened exactly once.
	fake.Advance(activeInterval + time.Second)
	obs.OnProgress(apply, []*storage.Task{task})
	select {
	case edited := <-capture.edits:
		assert.Equal(t, newProgressID, edited.CommentID, "later edits land on the fresh comment")
	case created := <-capture.creates:
		t.Fatalf("a deferred cutover must rotate exactly once; got another new comment %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected an in-place edit of the fresh progress comment on the next tick")
	}

	// A fresh observer — a later drive claim after a restart — finds the
	// superseded cutover row and the recorded post-cutover phase, stays
	// unmuted, and edits the tracked comment in place.
	obs2 := f.newObserver(st, fake)
	task.RowsCopied = 900
	task.ProgressPercent = 90
	fake.Advance(activeInterval + time.Second)
	obs2.OnProgress(apply, []*storage.Task{task})
	select {
	case edited := <-capture.edits:
		assert.Equal(t, newProgressID, edited.CommentID, "a fresh observer edits the tracked comment in place")
	case created := <-capture.creates:
		t.Fatalf("a fresh observer must not rotate for a cutover already tracked; got new comment %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected the fresh observer to edit the tracked progress comment")
	}
}

// A post-cutover rotation whose fresh comment lands but whose cutover-row
// supersede write fails must not re-mute the observer: the rotation already
// happened, so the still-live row is stale. Later ticks keep editing the
// fresh comment while retrying the supersede, and once storage heals the row
// is consumed so later observers neither re-mute on the folded prompt nor
// complete it on terminal.
func TestE2EDeferredCutoverSupersedeRetriedUntilItLands(t *testing.T) {
	ctx := t.Context()

	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:       "org/repo-cutover-supersede-retry",
		pr:         149,
		database:   "e2e_cutover_supersede_retry_db",
		applyState: state.Apply.WaitingForCutover,
	})
	st, apply, task, capture, h := f.st, f.apply, f.task, f.capture, f.handler

	h.postAndTrackComment(ctx, "org/repo-cutover-supersede-retry", 149, 12345, apply, state.Comment.Progress, "Copy complete — waiting for cutover")
	requireCommentCreate(t, capture)
	h.postAndTrackComment(ctx, "org/repo-cutover-supersede-retry", 149, 12345, apply, state.Comment.Cutover, "Ready for cutover")
	cutoverPromptID := requireCommentCreate(t, capture)

	failingStorage := &failingCommentSupersedeStorage{Storage: st}
	fake := clock.NewFake(task.CreatedAt)
	obs := f.newObserver(failingStorage, fake)

	// The cutover completes into the revert window. The rotation posts the
	// fresh comment and folds the prompt, but consuming the cutover row fails.
	apply.State = state.Apply.RevertWindow
	task.State = state.Task.RevertWindow
	obs.OnProgress(apply, []*storage.Task{task})

	var freshProgressID int64
	select {
	case created := <-capture.creates:
		freshProgressID = created.ID
		assert.Contains(t, created.Body, "**Status**: Revert Window")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the rotation to post a fresh progress comment")
	}
	select {
	case edited := <-capture.edits:
		assert.Equal(t, cutoverPromptID, edited.CommentID, "the freeze edit lands on the cutover prompt")
		assert.Contains(t, edited.Body, "Cutover complete")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the cutover prompt to be frozen")
	}
	cutoverRow, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Cutover)
	require.NoError(t, err)
	require.NotNil(t, cutoverRow)
	require.Nil(t, cutoverRow.SupersededAt, "the injected outage keeps the cutover row live")

	// While the outage lasts, later ticks keep editing the fresh comment — the
	// still-live cutover row is known-stale to this observer — and never post
	// a duplicate rotation.
	task.RowsCopied = 700
	task.ProgressPercent = 70
	fake.Advance(activeInterval + time.Second)
	obs.OnProgress(apply, []*storage.Task{task})
	select {
	case edited := <-capture.edits:
		assert.Equal(t, freshProgressID, edited.CommentID, "progress edits land on the fresh comment while the supersede is owed")
	case created := <-capture.creates:
		t.Fatalf("a deferred cutover must rotate exactly once; got another new comment %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected an in-place edit of the fresh progress comment during the outage")
	}

	// Once storage heals, the next tick's retry consumes the cutover row while
	// the tick still edits the fresh comment in place.
	failingStorage.heal()
	task.RowsCopied = 800
	task.ProgressPercent = 80
	fake.Advance(activeInterval + time.Second)
	obs.OnProgress(apply, []*storage.Task{task})
	select {
	case edited := <-capture.edits:
		assert.Equal(t, freshProgressID, edited.CommentID)
	case created := <-capture.creates:
		t.Fatalf("the supersede retry must not post a comment; got %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected an in-place edit of the fresh progress comment after healing")
	}
	cutoverRow, err = st.ApplyComments().Get(ctx, apply.ID, state.Comment.Cutover)
	require.NoError(t, err)
	require.NotNil(t, cutoverRow)
	assert.NotNil(t, cutoverRow.SupersededAt, "the retried supersede consumes the cutover row once storage heals")
}

// While the cutover prompt is live and unanswered, states that do not follow
// from a completed cutover — a restart recovery re-copying the parked apply, a
// retryable failure at the gate, the gate state itself — keep the observer
// muted: no fresh progress comment is posted and the prompt is never folded
// under a false "Cutover complete" record. Only a post-cutover phase (revert
// window, reverting, skipping revert) proves the operator's cutover happened
// and unmutes.
func TestE2EDeferredCutoverPromptStaysMutedWithoutCutover(t *testing.T) {
	ctx := t.Context()

	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:       "org/repo-cutover-muted",
		pr:         151,
		database:   "e2e_cutover_muted_db",
		applyState: state.Apply.WaitingForCutover,
	})
	st, apply, task, capture, h := f.st, f.apply, f.task, f.capture, f.handler

	h.postAndTrackComment(ctx, "org/repo-cutover-muted", 151, 12345, apply, state.Comment.Progress, "Copy complete — waiting for cutover")
	preCutoverProgressID := requireCommentCreate(t, capture)
	h.postAndTrackComment(ctx, "org/repo-cutover-muted", 151, 12345, apply, state.Comment.Cutover, "Ready for cutover")
	requireCommentCreate(t, capture)

	fake := clock.NewFake(task.CreatedAt)

	// Each state is observed by a fresh observer — a restart recovery hands
	// the apply to a new drive claim, which must rediscover the live prompt
	// and stay muted rather than mistake its own arrival for a cutover.
	for _, applyState := range []string{state.Apply.Running, state.Apply.FailedRetryable, state.Apply.WaitingForCutover} {
		obs := f.newObserver(st, fake)
		apply.State = applyState
		fake.Advance(activeInterval + time.Second)
		obs.OnProgress(apply, []*storage.Task{task})
		select {
		case created := <-capture.creates:
			t.Fatalf("state %q must not rotate while the prompt is unanswered; got new comment %d: %s", applyState, created.ID, created.Body)
		case edited := <-capture.edits:
			t.Fatalf("state %q must not edit while the prompt is unanswered; got an edit of %d: %s", applyState, edited.CommentID, edited.Body)
		case <-time.After(100 * time.Millisecond):
		}
	}

	// The prompt row stays live — the gate is still waiting for its answer —
	// and the tracked progress comment is untouched, recording no post-cutover
	// phase.
	cutoverRow, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Cutover)
	require.NoError(t, err)
	require.NotNil(t, cutoverRow)
	assert.Nil(t, cutoverRow.SupersededAt, "the prompt row stays live while no cutover happened")
	prog, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, prog)
	assert.Equal(t, preCutoverProgressID, prog.GitHubCommentID, "the tracked progress comment is unchanged")
	assert.False(t, postCutoverPhase(trackedPhase(prog)), "no post-cutover phase is recorded without a cutover")
}

// A deferred-cutover apply parks at the gate in waiting_for_cutover until the
// operator's cutover command arrives. The first observer tick that finds the
// apply parked posts the cutover prompt — carrying the pasteable cutover
// command — as a fresh comment at the bottom of the PR timeline rather than
// an edit of the tracked progress comment, tracks it as the cutover comment,
// and mutes progress edits behind it so later ticks at the gate change
// nothing.
func TestE2EDeferredCutoverParkedGatePostsPrompt(t *testing.T) {
	ctx := t.Context()

	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:         "org/repo-cutover-parked",
		pr:           154,
		database:     "e2e_cutover_parked_db",
		applyState:   state.Apply.WaitingForCutover,
		deferCutover: true,
	})
	st, apply, task, capture, h := f.st, f.apply, f.task, f.capture, f.handler

	h.postAndTrackComment(ctx, "org/repo-cutover-parked", 154, 12345, apply, state.Comment.Progress, "Copy complete — waiting for cutover")
	progressID := requireCommentCreate(t, capture)

	fake := clock.NewFake(task.CreatedAt)
	obs := f.newObserver(st, fake)

	// The first tick at the parked gate posts the prompt as a new comment.
	obs.OnProgress(apply, []*storage.Task{task})

	var promptID int64
	select {
	case created := <-capture.creates:
		promptID = created.ID
		assert.Contains(t, created.Body, "**Status**: Waiting for Cutover")
		assert.Contains(t, created.Body, "To proceed with cutover:")
		assert.Contains(t, created.Body, fmt.Sprintf("schemabot cutover %s -e staging", apply.ApplyIdentifier))
	case edited := <-capture.edits:
		t.Fatalf("the parked gate posts the prompt as a new comment, not an edit; got an edit of %d: %s", edited.CommentID, edited.Body)
	case <-time.After(5 * time.Second):
		t.Fatal("expected the cutover prompt to be posted at the parked gate")
	}
	assert.NotEqual(t, progressID, promptID, "the prompt is its own comment, not the progress comment")

	// The cutover row tracks the prompt so a restart's fresh observer finds it.
	cutoverRow, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Cutover)
	require.NoError(t, err)
	require.NotNil(t, cutoverRow)
	assert.Equal(t, promptID, cutoverRow.GitHubCommentID)
	assert.Nil(t, cutoverRow.SupersededAt, "the prompt row stays live while the gate waits for its answer")

	// Later ticks at the gate are muted behind the live prompt: nothing is
	// posted and nothing is edited.
	fake.Advance(activeInterval + time.Second)
	obs.OnProgress(apply, []*storage.Task{task})
	select {
	case created := <-capture.creates:
		t.Fatalf("a later tick at the gate must not post another comment; got %d: %s", created.ID, created.Body)
	case edited := <-capture.edits:
		t.Fatalf("a later tick at the gate must not edit while the prompt is live; got an edit of %d: %s", edited.CommentID, edited.Body)
	case <-time.After(100 * time.Millisecond):
	}

	// A pod restart or re-claimed drive hands the parked apply to a fresh
	// observer with no in-memory record of the prompt. The durable cutover row
	// is what keeps a multi-day park from re-posting the prompt on every
	// restart: the fresh observer rehydrates from the row and stays muted.
	obs2 := f.newObserver(st, fake)
	fake.Advance(activeInterval + time.Second)
	obs2.OnProgress(apply, []*storage.Task{task})
	select {
	case created := <-capture.creates:
		t.Fatalf("a fresh observer at the parked gate must not re-post the prompt; got %d: %s", created.ID, created.Body)
	case edited := <-capture.edits:
		t.Fatalf("a fresh observer at the parked gate must not edit while the prompt is live; got an edit of %d: %s", edited.CommentID, edited.Body)
	case <-time.After(100 * time.Millisecond):
	}
}

// When the operator's cutover on a parked deferred-cutover apply resolves
// directly to a terminal state, the cutover prompt is the completion comment:
// the terminal edit turns it into the applied summary and the summary marker
// records it as the terminal publish. The tracked progress comment — still
// rendering the cutover gate and its pasteable cutover command — also
// receives its final per-operation status freeze, so no comment on the
// timeline keeps advertising the spent gate as live.
func TestE2EDeferredCutoverFastTerminalFinalizesProgressComment(t *testing.T) {
	ctx := t.Context()

	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:         "org/repo-cutover-fast-terminal",
		pr:           155,
		database:     "e2e_cutover_fast_terminal_db",
		applyState:   state.Apply.WaitingForCutover,
		deferCutover: true,
	})
	st, apply, task, capture, h := f.st, f.apply, f.task, f.capture, f.handler

	h.postAndTrackComment(ctx, "org/repo-cutover-fast-terminal", 155, 12345, apply, state.Comment.Progress, "Copy complete — waiting for cutover")
	progressID := requireCommentCreate(t, capture)

	fake := clock.NewFake(task.CreatedAt)
	obs := f.newObserver(st, fake)

	// The tick at the parked gate posts and tracks the cutover prompt.
	obs.OnProgress(apply, []*storage.Task{task})
	promptID := requireCommentCreate(t, capture)

	// The operator's cutover completes the apply before another progress tick
	// lands.
	apply.State = state.Apply.Completed
	task.State = state.Task.Completed
	task.RowsCopied = 1000
	task.ProgressPercent = 100
	obs.OnTerminal(apply, []*storage.Task{task})

	// The prompt is edited into the terminal summary — it is the completion
	// comment.
	select {
	case edited := <-capture.edits:
		assert.Equal(t, promptID, edited.CommentID, "the terminal summary lands on the cutover prompt")
		assert.Contains(t, edited.Body, "✅ Schema Change Applied")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the cutover prompt to be edited into the terminal summary")
	}

	// The tracked progress comment receives its final status freeze: the
	// completed rendering, with no live cutover instruction left behind.
	select {
	case edited := <-capture.edits:
		assert.Equal(t, progressID, edited.CommentID, "the status freeze lands on the tracked progress comment")
		assert.Contains(t, edited.Body, "**Status**: Applied")
		assert.NotContains(t, edited.Body, "To proceed with cutover:", "the frozen progress comment must not advertise the spent gate")
		assert.NotContains(t, edited.Body, "schemabot cutover", "the frozen progress comment must not carry a live cutover command")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the tracked progress comment to be frozen at its final status")
	}

	// No extra comment is posted — the prompt already sits at the bottom of
	// the PR as the summary.
	select {
	case created := <-capture.creates:
		t.Fatalf("a cutover apply's terminal publish edits the prompt, it must not post a new comment; got %d: %s", created.ID, created.Body)
	case <-time.After(100 * time.Millisecond):
	}

	// The summary marker records the prompt as the terminal publish, and the
	// progress row — never superseded — still tracks its own comment.
	marker, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, marker)
	assert.Equal(t, promptID, marker.GitHubCommentID, "the summary marker records the completed prompt")
	prog, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, prog)
	assert.Equal(t, progressID, prog.GitHubCommentID)
	assert.Nil(t, prog.SupersededAt, "the frozen progress comment stays tracked, not superseded")
}

// An apply stopped at the deferred-cutover gate completes the prompt itself:
// the terminal edit turns the prompt into the stop record and consumes its
// row, so the prompt is never folded under a false "Cutover complete" record.
// When the operator resumes, the consumed row no longer mutes the fresh
// observer, and the summary marker triggers the resume rotation: the resumed
// apply is tracked in a fresh progress comment at the bottom of the PR while
// the stop record stays intact on the timeline.
func TestE2EStopAtCutoverGateThenResumeRotatesFreshComment(t *testing.T) {
	ctx := t.Context()

	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:       "org/repo-cutover-stop-resume",
		pr:         152,
		database:   "e2e_cutover_stop_resume_db",
		applyState: state.Apply.WaitingForCutover,
	})
	st, apply, task, capture, h := f.st, f.apply, f.task, f.capture, f.handler

	h.postAndTrackComment(ctx, "org/repo-cutover-stop-resume", 152, 12345, apply, state.Comment.Progress, "Copy complete — waiting for cutover")
	preStopProgressID := requireCommentCreate(t, capture)
	h.postAndTrackComment(ctx, "org/repo-cutover-stop-resume", 152, 12345, apply, state.Comment.Cutover, "Ready for cutover")
	cutoverPromptID := requireCommentCreate(t, capture)

	fake := clock.NewFake(task.CreatedAt)
	obs := f.newObserver(st, fake)

	// The operator stops the apply at the gate. The prompt is the active
	// comment, so the terminal edit lands there as the stop record.
	apply.State = state.Apply.Stopped
	task.State = state.Task.Stopped
	obs.OnTerminal(apply, []*storage.Task{task})

	select {
	case edited := <-capture.edits:
		assert.Equal(t, cutoverPromptID, edited.CommentID, "the stop record lands on the prompt")
		assert.Contains(t, edited.Body, "Schema Change Stopped")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the cutover prompt to be edited into the stop record")
	}
	// The tracked progress comment is frozen at the stopped rendering too, so
	// it stops advertising the gate's cutover command.
	select {
	case edited := <-capture.edits:
		assert.Equal(t, preStopProgressID, edited.CommentID, "the status freeze lands on the pre-stop progress comment")
		assert.Contains(t, edited.Body, "**Status**: Stopped")
		assert.NotContains(t, edited.Body, "schemabot cutover", "the frozen progress comment must not carry a live cutover command")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the tracked progress comment to be frozen at the stopped rendering")
	}
	select {
	case created := <-capture.creates:
		t.Fatalf("a stop at the gate completes the prompt itself; got a new comment %d: %s", created.ID, created.Body)
	case <-time.After(100 * time.Millisecond):
	}

	// The stop consumes the prompt row — its call to action is void — and the
	// summary marker records the stop record as the terminal publish.
	cutoverRow, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Cutover)
	require.NoError(t, err)
	require.NotNil(t, cutoverRow)
	assert.NotNil(t, cutoverRow.SupersededAt, "the stop consumes the prompt row")
	marker, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, marker)
	assert.Equal(t, cutoverPromptID, marker.GitHubCommentID, "the summary marker records the stop record")

	// The operator starts the apply again and a later drive claims it. The
	// fresh observer finds the consumed prompt row — no mute — and the live
	// summary marker triggers the resume rotation.
	apply.State = state.Apply.Resuming
	task.State = state.Task.Running
	obs2 := f.newObserver(st, fake)
	fake.Advance(activeInterval + time.Second)
	obs2.OnProgress(apply, []*storage.Task{task})

	var resumedProgressID int64
	select {
	case created := <-capture.creates:
		resumedProgressID = created.ID
		assert.Contains(t, created.Body, "Schema Change Status — Staging")
		assert.Contains(t, created.Body, "**Status**: Resuming")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the resume to post a fresh progress comment")
	}
	assert.NotEqual(t, cutoverPromptID, resumedProgressID, "the resumed apply gets its own comment; the stop record is not reused")

	// The pre-stop progress comment — not the stop record — is folded pointing
	// at the fresh comment.
	select {
	case edited := <-capture.edits:
		assert.Equal(t, preStopProgressID, edited.CommentID, "the freeze edit lands on the pre-stop progress comment")
		assert.Contains(t, edited.Body, fmt.Sprintf("#issuecomment-%d", resumedProgressID), "the frozen comment links to its successor")
		assert.NotContains(t, edited.Body, "Cutover complete", "nothing renders a cutover that never happened")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the pre-stop progress comment to be frozen")
	}

	// The summary marker is consumed and later ticks edit the fresh comment —
	// the stop record is never touched again.
	marker, err = st.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, marker)
	assert.NotNil(t, marker.SupersededAt, "the resume consumes the summary marker")

	apply.State = state.Apply.Running
	task.RowsCopied = 700
	task.ProgressPercent = 70
	fake.Advance(activeInterval + time.Second)
	obs2.OnProgress(apply, []*storage.Task{task})
	select {
	case edited := <-capture.edits:
		assert.Equal(t, resumedProgressID, edited.CommentID, "later edits land on the fresh comment")
	case created := <-capture.creates:
		t.Fatalf("a resume rotates exactly once; got another new comment %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected an in-place edit of the fresh progress comment")
	}
}

// A post-cutover rotation whose fresh comment posts to the PR but fails to be
// recorded as the tracked comment must not fold the prompt or consume the
// cutover row: the row is the durable marker that the rotation is owed, and
// the fold marker rides the tracking write. While the outage lasts, later
// ticks retry only the tracking write (adoption) — never another post — so
// duplicates stay bounded at one. Once storage heals, the next tick adopts the
// already-live fresh comment: the tracked row records it with the post-cutover
// phase, the prompt is folded pointing at it, the cutover row is consumed, and
// progress edits move to the adopted comment.
func TestE2EDeferredCutoverUntrackedFreshCommentAdoptedWhenStorageHeals(t *testing.T) {
	ctx := t.Context()

	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:       "org/repo-cutover-untracked",
		pr:         153,
		database:   "e2e_cutover_untracked_db",
		applyState: state.Apply.WaitingForCutover,
	})
	st, apply, task, capture, h := f.st, f.apply, f.task, f.capture, f.handler

	h.postAndTrackComment(ctx, "org/repo-cutover-untracked", 153, 12345, apply, state.Comment.Progress, "Copy complete — waiting for cutover")
	preCutoverProgressID := requireCommentCreate(t, capture)
	h.postAndTrackComment(ctx, "org/repo-cutover-untracked", 153, 12345, apply, state.Comment.Cutover, "Ready for cutover")
	cutoverPromptID := requireCommentCreate(t, capture)

	failingStorage := &failingCommentUpsertStorage{Storage: st}
	fake := clock.NewFake(task.CreatedAt)
	obs := f.newObserver(failingStorage, fake)

	// The cutover completes into the revert window. The fresh comment posts,
	// but recording it as the tracked comment fails — the prompt must not be
	// folded and the cutover row must stay live.
	apply.State = state.Apply.RevertWindow
	task.State = state.Task.RevertWindow
	obs.OnProgress(apply, []*storage.Task{task})

	var freshProgressID int64
	select {
	case created := <-capture.creates:
		freshProgressID = created.ID
		assert.Contains(t, created.Body, "**Status**: Revert Window")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the rotation to post a fresh progress comment")
	}
	select {
	case edited := <-capture.edits:
		t.Fatalf("no comment may be edited when the fresh comment was not tracked; got an edit of %d: %s", edited.CommentID, edited.Body)
	case <-time.After(100 * time.Millisecond):
	}
	cutoverRow, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Cutover)
	require.NoError(t, err)
	require.NotNil(t, cutoverRow)
	assert.Nil(t, cutoverRow.SupersededAt, "the cutover row stays live while the rotation is untracked")
	prog, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, prog)
	assert.Equal(t, preCutoverProgressID, prog.GitHubCommentID, "the tracked row still points at the pre-cutover comment")

	// While the outage lasts, a later tick retries only the tracking write —
	// no duplicate post, no fold, no progress edit past the still-live prompt.
	fake.Advance(activeInterval + time.Second)
	obs.OnProgress(apply, []*storage.Task{task})
	select {
	case created := <-capture.creates:
		t.Fatalf("an untracked rotation must not repost; got another new comment %d", created.ID)
	case edited := <-capture.edits:
		t.Fatalf("no comment may be edited while adoption is owed; got an edit of %d: %s", edited.CommentID, edited.Body)
	case <-time.After(100 * time.Millisecond):
	}

	// Once storage heals, the next tick adopts the already-live fresh comment:
	// the prompt is folded pointing at it and the cutover row is consumed.
	failingStorage.heal()
	fake.Advance(activeInterval + time.Second)
	obs.OnProgress(apply, []*storage.Task{task})

	select {
	case edited := <-capture.edits:
		assert.Equal(t, cutoverPromptID, edited.CommentID, "the freeze edit lands on the cutover prompt")
		assert.Contains(t, edited.Body, "Cutover complete")
		assert.Contains(t, edited.Body, fmt.Sprintf("#issuecomment-%d", freshProgressID), "the frozen prompt links to its adopted successor")
	case created := <-capture.creates:
		t.Fatalf("adoption must not post another comment; got %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected the cutover prompt to be frozen after adoption")
	}
	prog, err = st.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, prog)
	assert.Equal(t, freshProgressID, prog.GitHubCommentID, "the tracked row records the adopted comment")
	require.NotNil(t, prog.PostedPhase)
	assert.Equal(t, "revert_window", *prog.PostedPhase, "adoption records the post-cutover phase durably")
	assert.Nil(t, prog.PendingFreezeCommentID)
	cutoverRow, err = st.ApplyComments().Get(ctx, apply.ID, state.Comment.Cutover)
	require.NoError(t, err)
	require.NotNil(t, cutoverRow)
	assert.NotNil(t, cutoverRow.SupersededAt, "the cutover row is consumed once the rotation is tracked")

	// Later ticks edit the adopted comment in place.
	task.RowsCopied = 900
	task.ProgressPercent = 90
	fake.Advance(activeInterval + time.Second)
	obs.OnProgress(apply, []*storage.Task{task})
	select {
	case edited := <-capture.edits:
		assert.Equal(t, freshProgressID, edited.CommentID, "progress edits move to the adopted comment")
	case created := <-capture.creates:
		t.Fatalf("no further rotation is owed; got new comment %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected an in-place edit of the adopted comment")
	}
}

// When the apply reaches a terminal state while the cutover row still reads
// live but the tracked progress comment records a post-cutover phase, the row
// is a spent prompt whose supersede write never landed: the rotation already
// happened and the fresh progress comment is the active one. The terminal
// publish completes the fresh comment and posts the summary — it must not
// overwrite the folded prompt — and consumes the stale row.
func TestE2EDeferredCutoverStaleCutoverRowRoutesTerminalToFreshComment(t *testing.T) {
	ctx := t.Context()

	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:       "org/repo-cutover-stale-terminal",
		pr:         154,
		database:   "e2e_cutover_stale_terminal_db",
		applyState: state.Apply.WaitingForCutover,
	})
	st, apply, task, capture, h := f.st, f.apply, f.task, f.capture, f.handler

	h.postAndTrackComment(ctx, "org/repo-cutover-stale-terminal", 154, 12345, apply, state.Comment.Progress, "Copy complete — waiting for cutover")
	requireCommentCreate(t, capture)
	h.postAndTrackComment(ctx, "org/repo-cutover-stale-terminal", 154, 12345, apply, state.Comment.Cutover, "Ready for cutover")
	cutoverPromptID := requireCommentCreate(t, capture)

	// The cutover completes into the revert window and the rotation lands —
	// fresh comment tracked, prompt folded — but consuming the cutover row
	// fails, leaving it live.
	failingStorage := &failingCommentSupersedeStorage{Storage: st}
	fake := clock.NewFake(task.CreatedAt)
	obs := f.newObserver(failingStorage, fake)
	apply.State = state.Apply.RevertWindow
	task.State = state.Task.RevertWindow
	obs.OnProgress(apply, []*storage.Task{task})

	var freshProgressID int64
	select {
	case created := <-capture.creates:
		freshProgressID = created.ID
	case <-time.After(5 * time.Second):
		t.Fatal("expected the rotation to post a fresh progress comment")
	}
	select {
	case edited := <-capture.edits:
		assert.Equal(t, cutoverPromptID, edited.CommentID, "the freeze edit lands on the cutover prompt")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the cutover prompt to be frozen")
	}

	// The revert window expires and the apply completes. A fresh observer —
	// only its terminal callback runs — reads the stale live row, sees the
	// tracked comment's post-cutover phase, and routes the terminal publish to
	// the fresh comment.
	apply.State = state.Apply.Completed
	task.State = state.Task.Completed
	obs2 := f.newObserver(st, fake)
	obs2.OnTerminal(apply, []*storage.Task{task})

	select {
	case edited := <-capture.edits:
		assert.Equal(t, freshProgressID, edited.CommentID, "the terminal freeze lands on the fresh progress comment, not the folded prompt")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the fresh progress comment to be edited to its final rendering")
	}
	select {
	case created := <-capture.creates:
		assert.Contains(t, created.Body, "Schema Change Applied")
		assert.Contains(t, created.Body, apply.ApplyIdentifier)
	case <-time.After(5 * time.Second):
		t.Fatal("expected a summary comment for the completed apply")
	}
	select {
	case edited := <-capture.edits:
		t.Fatalf("the folded prompt must not be overwritten on terminal; got an edit of %d: %s", edited.CommentID, edited.Body)
	case <-time.After(100 * time.Millisecond):
	}

	// The stale row is consumed on the way through.
	cutoverRow, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Cutover)
	require.NoError(t, err)
	require.NotNil(t, cutoverRow)
	assert.NotNil(t, cutoverRow.SupersededAt, "the terminal publish consumes the stale cutover row")
}

// A fresh observer on a later drive claim that finds the cutover row still
// live but the tracked progress comment recording a post-cutover phase treats
// the row as a spent prompt whose supersede write never landed: it consumes
// the row without rotating again — the recorded phase is the durable proof the
// rotation already happened — and progress edits continue on the fresh
// comment.
func TestE2EDeferredCutoverFreshObserverConsumesStaleCutoverRow(t *testing.T) {
	ctx := t.Context()

	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:       "org/repo-cutover-stale-row",
		pr:         155,
		database:   "e2e_cutover_stale_row_db",
		applyState: state.Apply.WaitingForCutover,
	})
	st, apply, task, capture, h := f.st, f.apply, f.task, f.capture, f.handler

	h.postAndTrackComment(ctx, "org/repo-cutover-stale-row", 155, 12345, apply, state.Comment.Progress, "Copy complete — waiting for cutover")
	requireCommentCreate(t, capture)
	h.postAndTrackComment(ctx, "org/repo-cutover-stale-row", 155, 12345, apply, state.Comment.Cutover, "Ready for cutover")
	cutoverPromptID := requireCommentCreate(t, capture)

	// The cutover completes into the revert window and the rotation lands —
	// fresh comment tracked, prompt folded — but consuming the cutover row
	// fails, leaving it live for the next drive claim to find.
	failingStorage := &failingCommentSupersedeStorage{Storage: st}
	fake := clock.NewFake(task.CreatedAt)
	obs := f.newObserver(failingStorage, fake)
	apply.State = state.Apply.RevertWindow
	task.State = state.Task.RevertWindow
	obs.OnProgress(apply, []*storage.Task{task})

	var freshProgressID int64
	select {
	case created := <-capture.creates:
		freshProgressID = created.ID
	case <-time.After(5 * time.Second):
		t.Fatal("expected the rotation to post a fresh progress comment")
	}
	select {
	case edited := <-capture.edits:
		assert.Equal(t, cutoverPromptID, edited.CommentID, "the freeze edit lands on the cutover prompt")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the cutover prompt to be frozen")
	}

	// A fresh observer claims the drive. Its first tick reads the live row,
	// sees the tracked comment's recorded post-cutover phase, and consumes the
	// row instead of rotating again — no duplicate comment, no second fold.
	obs2 := f.newObserver(st, fake)
	fake.Advance(activeInterval + time.Second)
	obs2.OnProgress(apply, []*storage.Task{task})
	select {
	case created := <-capture.creates:
		t.Fatalf("a fresh observer must not rotate for a cutover already tracked; got new comment %d", created.ID)
	case edited := <-capture.edits:
		t.Fatalf("a fresh observer must not re-fold; got an edit of %d: %s", edited.CommentID, edited.Body)
	case <-time.After(100 * time.Millisecond):
	}
	cutoverRow, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Cutover)
	require.NoError(t, err)
	require.NotNil(t, cutoverRow)
	assert.NotNil(t, cutoverRow.SupersededAt, "the fresh observer consumes the stale cutover row")

	// The next tick edits the fresh comment in place — the observer is unmuted.
	task.RowsCopied = 900
	task.ProgressPercent = 90
	fake.Advance(activeInterval + time.Second)
	obs2.OnProgress(apply, []*storage.Task{task})
	select {
	case edited := <-capture.edits:
		assert.Equal(t, freshProgressID, edited.CommentID, "progress edits continue on the fresh comment")
	case created := <-capture.creates:
		t.Fatalf("no further rotation is owed; got new comment %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected an in-place edit of the fresh progress comment")
	}
}

// A control-phase rotation whose fresh comment posts to the PR but fails to
// be recorded as the tracked comment must not freeze the prior comment: the
// tracked row still points at it, so later progress edits land there. While
// the outage lasts, later ticks retry only the tracking write (adoption) —
// never another post — so duplicates stay bounded at one. Once storage heals,
// the next tick adopts the already-live fresh comment: the tracked row records
// it with the control phase, the prior comment is frozen pointing at its
// successor, and progress edits move to the adopted comment — including for a
// fresh observer on a later drive claim.
func TestE2ERevertRotationUntrackedFreshCommentAdoptedWhenStorageHeals(t *testing.T) {
	ctx := t.Context()

	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:       "org/repo-revert-untracked",
		pr:         148,
		database:   "e2e_revert_untracked_db",
		applyState: state.Apply.RevertWindow,
	})
	st, apply, task, capture, h := f.st, f.apply, f.task, f.capture, f.handler

	h.postAndTrackComment(ctx, "org/repo-revert-untracked", 148, 12345, apply, state.Comment.Progress, "Revert window open — closes in 25m")
	preRevertID := requireCommentCreate(t, capture)

	failingStorage := &failingCommentUpsertStorage{Storage: st}
	fake := clock.NewFake(task.CreatedAt)
	obs := f.newObserver(failingStorage, fake)

	// The revert takes effect: the fresh comment posts, but recording it as
	// the tracked comment fails — the prior comment must not be frozen.
	apply.State = state.Apply.Reverting
	obs.OnProgress(apply, []*storage.Task{task})

	var freshProgressID int64
	select {
	case created := <-capture.creates:
		freshProgressID = created.ID
		assert.Contains(t, created.Body, "**Status**: Reverting")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the revert to post a fresh progress comment")
	}
	select {
	case edited := <-capture.edits:
		t.Fatalf("no comment may be edited when the fresh comment was not tracked; got an edit of %d: %s", edited.CommentID, edited.Body)
	case <-time.After(100 * time.Millisecond):
	}

	// While the outage lasts, a later tick retries only the tracking write —
	// no duplicate post — and its progress edit lands on the prior comment,
	// still the tracked one.
	task.RowsCopied = 300
	task.ProgressPercent = 30
	fake.Advance(activeInterval + time.Second)
	obs.OnProgress(apply, []*storage.Task{task})
	select {
	case edited := <-capture.edits:
		assert.Equal(t, preRevertID, edited.CommentID, "progress edits continue on the prior tracked comment")
	case created := <-capture.creates:
		t.Fatalf("an untracked rotation must not repost for the same phase; got another new comment %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected an in-place edit of the prior tracked comment")
	}

	// Once storage heals, the next tick adopts the already-live fresh comment
	// instead of posting another: the prior comment is frozen pointing at its
	// successor and the tick's progress edit lands on the adopted comment.
	failingStorage.heal()
	task.RowsCopied = 200
	task.ProgressPercent = 20
	fake.Advance(activeInterval + time.Second)
	obs.OnProgress(apply, []*storage.Task{task})

	select {
	case edited := <-capture.edits:
		assert.Equal(t, preRevertID, edited.CommentID, "the freeze edit lands on the superseded comment")
		assert.Contains(t, edited.Body, "Schema change reverting")
		assert.Contains(t, edited.Body, fmt.Sprintf("#issuecomment-%d", freshProgressID), "the frozen comment links to its adopted successor")
	case created := <-capture.creates:
		t.Fatalf("adoption must not post another comment; got %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected the superseded progress comment to be frozen after adoption")
	}
	select {
	case edited := <-capture.edits:
		assert.Equal(t, freshProgressID, edited.CommentID, "progress edits move to the adopted comment")
	case <-time.After(5 * time.Second):
		t.Fatal("expected a progress edit on the adopted comment")
	}

	// The tracked row records the adopted comment with the reverting phase and
	// no freeze left owing, so no observer — this one or a fresh one on a later
	// drive claim — rotates again.
	prog, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, prog)
	assert.Equal(t, freshProgressID, prog.GitHubCommentID)
	require.NotNil(t, prog.PostedPhase)
	assert.Equal(t, "reverting", *prog.PostedPhase, "adoption records the control phase durably")
	assert.Nil(t, prog.PendingFreezeCommentID)

	obs2 := f.newObserver(st, fake)
	task.RowsCopied = 100
	task.ProgressPercent = 10
	fake.Advance(activeInterval + time.Second)
	obs2.OnProgress(apply, []*storage.Task{task})
	select {
	case edited := <-capture.edits:
		assert.Equal(t, freshProgressID, edited.CommentID, "a fresh observer edits the adopted comment in place")
	case created := <-capture.creates:
		t.Fatalf("a fresh observer must not rotate for a revert already tracked; got new comment %d", created.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("expected the fresh observer to edit the adopted comment")
	}
}

// A rotation's fresh comment that is still untracked when the apply reaches a
// terminal state gets one last adoption attempt from the terminal publish: the
// tracked row is pointed at the fresh comment, the superseded comment is
// frozen pointing at its successor, and the terminal rendering lands on the
// fresh comment — the newest one on the PR — instead of leaving it live-looking
// at a stale progress rendering above the summary.
func TestE2ERevertRotationUntrackedFreshCommentAdoptedAtTerminal(t *testing.T) {
	ctx := t.Context()

	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:       "org/repo-revert-terminal-adopt",
		pr:         151,
		database:   "e2e_revert_terminal_adopt_db",
		applyState: state.Apply.RevertWindow,
	})
	st, apply, task, capture, h := f.st, f.apply, f.task, f.capture, f.handler

	h.postAndTrackComment(ctx, "org/repo-revert-terminal-adopt", 151, 12345, apply, state.Comment.Progress, "Revert window open — closes in 25m")
	preRevertID := requireCommentCreate(t, capture)

	failingStorage := &failingCommentUpsertStorage{Storage: st}
	fake := clock.NewFake(task.CreatedAt)
	obs := f.newObserver(failingStorage, fake)

	// The revert takes effect: the fresh comment posts, but recording it as
	// the tracked comment fails.
	apply.State = state.Apply.Reverting
	obs.OnProgress(apply, []*storage.Task{task})

	var freshProgressID int64
	select {
	case created := <-capture.creates:
		freshProgressID = created.ID
		assert.Contains(t, created.Body, "**Status**: Reverting")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the revert to post a fresh progress comment")
	}

	// Storage heals, but the revert finishes before another progress tick —
	// the terminal publish is the only remaining chance to adopt.
	failingStorage.heal()
	apply.State = state.Apply.Reverted
	task.State = state.Task.Reverted
	obs.OnTerminal(apply, []*storage.Task{task})

	// The adoption freezes the superseded comment pointing at the adopted
	// successor.
	select {
	case edited := <-capture.edits:
		assert.Equal(t, preRevertID, edited.CommentID, "the freeze edit lands on the superseded comment")
		assert.Contains(t, edited.Body, fmt.Sprintf("#issuecomment-%d", freshProgressID), "the frozen comment links to its adopted successor")
		assert.Contains(t, edited.Body, "Revert window open", "the pre-revert body is preserved inside the fold")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the superseded progress comment to be frozen during the terminal publish")
	}

	// The terminal rendering lands on the adopted fresh comment.
	select {
	case edited := <-capture.edits:
		assert.Equal(t, freshProgressID, edited.CommentID, "the terminal rendering lands on the adopted comment")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the terminal rendering on the adopted comment")
	}

	// The terminal summary still posts as its own fresh comment.
	select {
	case created := <-capture.creates:
		assert.Contains(t, created.Body, apply.ApplyIdentifier)
	case <-time.After(5 * time.Second):
		t.Fatal("expected the terminal summary comment")
	}

	// The tracked row records the adopted comment with no freeze left owing.
	prog, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, prog)
	assert.Equal(t, freshProgressID, prog.GitHubCommentID)
	assert.Nil(t, prog.PendingFreezeCommentID)
}

// A cancel resolves the apply directly to a terminal state, so its effect
// surfaces through the terminal flow rather than a progress-comment rotation:
// the tracked progress comment is frozen at its final rendering in place, and
// a brand-new summary comment lands at the bottom of the PR timeline — which
// is where the operator looks for the effect of the command they just issued.
// Exactly one new comment is posted; rotating as well would duplicate it.
func TestE2ECancelSurfacesThroughTerminalSummaryComment(t *testing.T) {
	ctx := t.Context()

	f := setupApplyCommentFixture(t, applyCommentFixtureParams{
		repo:       "org/repo-cancel",
		pr:         147,
		database:   "e2e_cancel_db",
		applyState: state.Apply.Running,
	})
	st, apply, task, capture, h := f.st, f.apply, f.task, f.capture, f.handler

	// The apply is mid-copy with a tracked progress comment when the operator
	// issues the cancel.
	h.postAndTrackComment(ctx, "org/repo-cancel", 147, 12345, apply, state.Comment.Progress, "Copying rows — 50%")
	progressID := requireCommentCreate(t, capture)

	fake := clock.NewFake(task.CreatedAt)
	obs := f.newObserver(st, fake)

	apply.State = state.Apply.Cancelled
	task.State = state.Task.Cancelled
	obs.OnTerminal(apply, []*storage.Task{task})

	// The tracked progress comment is frozen at its final rendering in place —
	// no rotation.
	select {
	case edited := <-capture.edits:
		assert.Equal(t, progressID, edited.CommentID, "the terminal freeze lands on the tracked progress comment")
	case <-time.After(5 * time.Second):
		t.Fatal("expected the progress comment to be edited to its final rendering")
	}

	// The cancel's operator-visible effect is a fresh summary comment at the
	// bottom of the PR.
	var summaryID int64
	select {
	case created := <-capture.creates:
		summaryID = created.ID
		assert.Contains(t, created.Body, "🚫 Schema Change Cancelled")
		assert.Contains(t, created.Body, apply.ApplyIdentifier)
	case <-time.After(5 * time.Second):
		t.Fatal("expected a new summary comment to be posted for the cancel")
	}
	assert.NotEqual(t, progressID, summaryID, "the summary is a new comment, not an edit of the progress comment")

	// Exactly one new comment: the summary. A rotation on top of it would
	// post a duplicate.
	select {
	case duplicate := <-capture.creates:
		t.Fatalf("a cancel must surface through exactly one new comment; got another: %q", duplicate.Body)
	default:
	}

	// The summary marker records the posted comment so reconciliation and
	// later observers know the terminal publish happened.
	marker, err := st.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, marker)
	assert.Equal(t, summaryID, marker.GitHubCommentID)
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

// failingCommentSupersedeStore wraps an ApplyCommentStore so every Supersede
// fails until the outage is healed, modeling a storage outage between posting
// the post-cutover rotation comment and consuming the cutover row. Reads and
// every other write pass through.
type failingCommentSupersedeStore struct {
	storage.ApplyCommentStore
	healed *atomic.Bool
}

func (s *failingCommentSupersedeStore) Supersede(ctx context.Context, applyID int64, commentState string) error {
	if s.healed.Load() {
		return s.ApplyCommentStore.Supersede(ctx, applyID, commentState)
	}
	return errors.New("injected comment supersede failure")
}

// failingCommentSupersedeStorage serves the failing comment store while
// passing every other store through to the real storage. heal ends the outage
// so later Supersedes land.
type failingCommentSupersedeStorage struct {
	storage.Storage
	healed atomic.Bool
}

func (s *failingCommentSupersedeStorage) ApplyComments() storage.ApplyCommentStore {
	return &failingCommentSupersedeStore{ApplyCommentStore: s.Storage.ApplyComments(), healed: &s.healed}
}

func (s *failingCommentSupersedeStorage) heal() { s.healed.Store(true) }

func TestE2EReconcileMissingSummaryCommentsPostsSummary(t *testing.T) {
	ctx := t.Context()

	schemabotDB, err := sql.Open("block-mysql", e2eSchemabotDSN)
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

	schemabotDB, err := sql.Open("block-mysql", e2eSchemabotDSN)
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

	schemabotDB, err := sql.Open("block-mysql", e2eSchemabotDSN)
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

	schemabotDB, err := sql.Open("block-mysql", e2eSchemabotDSN)
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

	schemabotDB, err := sql.Open("block-mysql", e2eSchemabotDSN)
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

	schemabotDB, err := sql.Open("block-mysql", e2eSchemabotDSN)
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
	schemabotDB, err := sql.Open("block-mysql", e2eSchemabotDSN)
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

	schemabotDB, err := sql.Open("block-mysql", e2eSchemabotDSN)
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
