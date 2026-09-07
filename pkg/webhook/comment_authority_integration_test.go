//go:build integration

package webhook

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
)

// operationScopedApplyFixture is one seeded apply whose work runs under
// operation leases: two keyed non-terminal operation rows, no lease on the
// parent applies row, and a tracked progress comment ready to edit.
type operationScopedApplyFixture struct {
	st                storage.Storage
	db                *sql.DB
	apply             *storage.Apply
	tasks             []*storage.Task
	progressCommentID int64
}

// seedOperationScopedApply creates a running apply with keyed operation rows
// and a tracked progress comment, leaving the parent applies row's lease
// fields empty — the shape an operation-scoped rollout has between dispatch
// waves. repo scopes the rows so tests do not interfere with each other.
func seedOperationScopedApply(t *testing.T, repo, database string) *operationScopedApplyFixture {
	t.Helper()
	ctx := t.Context()

	schemabotDB, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(schemabotDB) })
	require.NoError(t, schemabotDB.PingContext(ctx))

	st := mysqlstore.New(schemabotDB)

	lock := &storage.Lock{
		DatabaseName: database,
		DatabaseType: "mysql",
		Repository:   repo,
		PullRequest:  42,
		Owner:        repo + "#42",
	}
	require.NoError(t, st.Locks().Acquire(ctx, lock))
	lock, err = st.Locks().Get(ctx, database, "mysql")
	require.NoError(t, err)

	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply_authority_%s_%d", database, time.Now().UnixNano()),
		LockID:          lock.ID,
		PlanID:          1,
		Database:        database,
		DatabaseType:    "mysql",
		Repository:      repo,
		PullRequest:     42,
		Environment:     "staging",
		InstallationID:  12345,
		Engine:          "spirit",
		State:           state.Apply.Running,
	}
	applyID, err := st.Applies().Create(ctx, apply)
	require.NoError(t, err)

	// Two operation-keyed rows still copying under their own operation leases,
	// while the parent applies row records no lease at all.
	var firstOpID int64
	for i, key := range []string{"orders/-80", "orders/80-"} {
		res, err := schemabotDB.ExecContext(ctx, `
			INSERT INTO apply_operations (apply_id, deployment, operation_key, state, lease_owner, lease_token)
			VALUES (?, ?, ?, ?, ?, ?)
		`, applyID, "primary", key, state.ApplyOperation.Running,
			fmt.Sprintf("driver-host/%d/op", i), fmt.Sprintf("op-token-%d", i))
		require.NoError(t, err)
		if i == 0 {
			firstOpID, err = res.LastInsertId()
			require.NoError(t, err)
		}
	}

	now := time.Now()
	task := &storage.Task{
		TaskIdentifier:   fmt.Sprintf("task_authority_%s_%d", database, now.UnixNano()),
		ApplyID:          applyID,
		ApplyOperationID: &firstOpID,
		PlanID:           1,
		Database:         database,
		DatabaseType:     "mysql",
		Engine:           "spirit",
		Repository:       repo,
		PullRequest:      42,
		Environment:      "staging",
		State:            state.Task.Running,
		TableName:        "orders",
		DDL:              "ALTER TABLE orders ADD COLUMN region VARCHAR(32)",
		DDLAction:        "alter",
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	_, err = st.Tasks().Create(ctx, task)
	require.NoError(t, err)

	// The tracked progress comment the handler posted when the apply started.
	progressCommentID := int64(700000 + time.Now().UnixNano()%100000)
	require.NoError(t, st.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:         applyID,
		CommentState:    state.Comment.Progress,
		GitHubCommentID: progressCommentID,
	}))

	apply, err = st.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	require.Empty(t, apply.LeaseToken, "fixture apply must not hold a parent lease")

	tasks, err := st.Tasks().GetByApplyID(ctx, applyID)
	require.NoError(t, err)

	return &operationScopedApplyFixture{
		st:                st,
		db:                schemabotDB,
		apply:             apply,
		tasks:             tasks,
		progressCommentID: progressCommentID,
	}
}

// progressObserverOwner reads the durable authority owner recorded on the
// apply's tracked progress comment row.
func progressObserverOwner(t *testing.T, db *sql.DB, applyID int64) sql.NullString {
	t.Helper()
	var owner sql.NullString
	err := db.QueryRowContext(t.Context(), `
		SELECT observer_owner FROM apply_comments WHERE apply_id = ? AND comment_state = ?
	`, applyID, state.Comment.Progress).Scan(&owner)
	require.NoError(t, err)
	return owner
}

// requireNoGitHubCalls asserts a skipped observer produced no GitHub side
// effects. The observer callbacks are synchronous, so an empty channel after
// the callback returns means no call was made.
func requireNoGitHubCalls(t *testing.T, capture *commentCapture) {
	t.Helper()
	select {
	case edit := <-capture.edits:
		t.Fatalf("expected no GitHub comment edit, got edit of comment %d", edit.CommentID)
	case created := <-capture.creates:
		t.Fatalf("expected no GitHub comment create, got comment %d", created.ID)
	default:
	}
}

// An apply whose operations run under operation leases holds the parent apply
// lease only transiently per dispatch wave. Between waves the observer must
// keep editing the PR progress comment — under the durable progress-comment
// authority rather than a lease — so operators watching the PR see a live
// rollout instead of a comment frozen mid-apply.
func TestObserverEditsProgressCommentForOperationScopedApply(t *testing.T) {
	fx := seedOperationScopedApply(t, "org/authority-edit", "authority_edit_db")

	installClient, capture := setupFakeGitHubForComments(t)
	capture.setBody(fx.progressCommentID, "seed progress body")

	obs := NewCommentObserver(CommentObserverConfig{
		GHClient:       &fakeClientFactory{client: installClient},
		Storage:        fx.st,
		Repo:           fx.apply.Repository,
		PR:             fx.apply.PullRequest,
		InstallationID: fx.apply.InstallationID,
		ApplyID:        fx.apply.ID,
		Logger:         &capturingLogger{},
	})

	obs.OnProgress(fx.apply, fx.tasks)

	select {
	case edited := <-capture.edits:
		assert.Equal(t, fx.progressCommentID, edited.CommentID)
		assert.Contains(t, edited.Body, fx.apply.ApplyIdentifier, "progress edit must render the live apply")
		assert.Contains(t, edited.Body, "running table copy", "progress edit must render the in-flight operations")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the progress comment edit")
	}

	// The edit was performed under the durable authority recorded on the
	// tracked comment row, owned by this process's observer identity.
	owner := progressObserverOwner(t, fx.db, fx.apply.ID)
	require.True(t, owner.Valid, "the observer must record its authority on the tracked comment row")
	assert.Equal(t, storage.LeaseOwnerProcess()+"/comment-observer", owner.String)
}

// Two observers polling the same operation-scoped apply from different
// processes must never both edit the progress comment: the first claims the
// durable authority, and the second loses the compare-and-swap, skips every
// GitHub side effect, and logs the skip with triage identifiers.
func TestConcurrentObserversShareOneProgressCommentAuthority(t *testing.T) {
	fx := seedOperationScopedApply(t, "org/authority-race", "authority_race_db")

	installClient, capture := setupFakeGitHubForComments(t)
	capture.setBody(fx.progressCommentID, "seed progress body")

	newObserver := func(owner string, logger *capturingLogger) *CommentObserver {
		obs := NewCommentObserver(CommentObserverConfig{
			GHClient:       &fakeClientFactory{client: installClient},
			Storage:        fx.st,
			Repo:           fx.apply.Repository,
			PR:             fx.apply.PullRequest,
			InstallationID: fx.apply.InstallationID,
			ApplyID:        fx.apply.ID,
			Logger:         logger,
		})
		obs.authorityOwner = owner
		return obs
	}

	winner := newObserver("pod-a/1/comment-observer", &capturingLogger{})
	winner.OnProgress(fx.apply, fx.tasks)

	select {
	case edited := <-capture.edits:
		assert.Equal(t, fx.progressCommentID, edited.CommentID)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the winner's progress comment edit")
	}

	loserLogger := &capturingLogger{}
	loser := newObserver("pod-b/2/comment-observer", loserLogger)
	loser.OnProgress(fx.apply, fx.tasks)

	requireNoGitHubCalls(t, capture)

	owner := progressObserverOwner(t, fx.db, fx.apply.ID)
	require.True(t, owner.Valid)
	assert.Equal(t, "pod-a/1/comment-observer", owner.String, "the loser must not take the authority over")

	var loggedSkip bool
	for _, entry := range loserLogger.debugs {
		if entry.msg != "observer: progress-comment authority not won (held by another observer, or no tracked comment row yet); skipping GitHub side effect" {
			continue
		}
		loggedSkip = true
		fields := fieldsOf(t, entry.args)
		assert.Equal(t, fx.apply.ApplyIdentifier, fields["apply_id"])
		assert.Equal(t, fx.apply.Repository, fields["repo"])
		assert.Equal(t, fx.apply.PullRequest, fields["pr"])
		assert.Equal(t, "pod-b/2/comment-observer", fields["authority_owner"])
	}
	assert.True(t, loggedSkip, "the losing observer must log its skipped edit with triage identifiers")
}

// An apply whose driver holds the parent apply lease keeps the lease as the
// one authority for GitHub side effects: the lease holder edits without
// touching the durable comment authority, and an observer whose captured
// lease no longer matches the row skips even though operation-keyed work is
// in flight.
func TestLeaseHeldApplyKeepsLeaseAuthoritative(t *testing.T) {
	fx := seedOperationScopedApply(t, "org/authority-lease", "authority_lease_db")
	ctx := t.Context()

	// A driver claims the parent apply for a dispatch wave.
	leaseAcquiredAt := time.Now()
	_, err := fx.db.ExecContext(ctx, `
		UPDATE applies SET lease_owner = ?, lease_token = ?, lease_acquired_at = ? WHERE id = ?
	`, "driver-host/9/dispatch", "wave-token", leaseAcquiredAt, fx.apply.ID)
	require.NoError(t, err)
	apply, err := fx.st.Applies().Get(ctx, fx.apply.ID)
	require.NoError(t, err)
	require.NotNil(t, apply)

	installClient, capture := setupFakeGitHubForComments(t)
	capture.setBody(fx.progressCommentID, "seed progress body")

	newObserver := func(lease storage.ApplyLease, logger *capturingLogger) *CommentObserver {
		return NewCommentObserver(CommentObserverConfig{
			GHClient:       &fakeClientFactory{client: installClient},
			Storage:        fx.st,
			Repo:           apply.Repository,
			PR:             apply.PullRequest,
			InstallationID: apply.InstallationID,
			ApplyID:        apply.ID,
			ApplyLease:     lease,
			Logger:         logger,
		})
	}

	holder := newObserver(apply.Lease(), &capturingLogger{})
	holder.OnProgress(apply, fx.tasks)

	select {
	case edited := <-capture.edits:
		assert.Equal(t, fx.progressCommentID, edited.CommentID)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the lease holder's progress comment edit")
	}

	owner := progressObserverOwner(t, fx.db, fx.apply.ID)
	assert.False(t, owner.Valid, "a lease-held apply must not record a progress-comment authority")

	// A stale observer whose captured lease was rotated away must skip while
	// the row is held by another driver — the lease stays authoritative.
	staleLogger := &capturingLogger{}
	stale := newObserver(storage.ApplyLease{ApplyID: apply.ID, Owner: "old-driver", Token: "rotated-away"}, staleLogger)
	stale.OnProgress(apply, fx.tasks)

	requireNoGitHubCalls(t, capture)
	var loggedSkip bool
	for _, entry := range staleLogger.errors {
		if entry.msg == "observer: apply lease no longer owns apply; skipping GitHub side effect" {
			loggedSkip = true
		}
	}
	assert.True(t, loggedSkip, "the stale observer must log its skipped edit")
}

// The observer's tick works from a poller snapshot that can predate a
// dispatch wave re-claiming the parent apply lease. The authority gate
// re-reads the apply row before claiming, so an observer holding a stale
// no-lease snapshot skips every GitHub side effect while the wave's lease
// holder owns the comment — and never records a durable authority alongside
// the live lease.
func TestParentLeaseReclaimAfterSnapshotDeniesAuthority(t *testing.T) {
	fx := seedOperationScopedApply(t, "org/authority-reclaim", "authority_reclaim_db")
	ctx := t.Context()

	installClient, capture := setupFakeGitHubForComments(t)
	capture.setBody(fx.progressCommentID, "seed progress body")

	obs := NewCommentObserver(CommentObserverConfig{
		GHClient:       &fakeClientFactory{client: installClient},
		Storage:        fx.st,
		Repo:           fx.apply.Repository,
		PR:             fx.apply.PullRequest,
		InstallationID: fx.apply.InstallationID,
		ApplyID:        fx.apply.ID,
		Logger:         &capturingLogger{},
	})

	// A dispatch wave claims the parent apply after the observer's snapshot
	// (fx.apply) was taken, so the snapshot still records no lease.
	_, err := fx.db.ExecContext(ctx, `
		UPDATE applies SET lease_owner = ?, lease_token = ?, lease_acquired_at = ? WHERE id = ?
	`, "driver-host/9/dispatch", "wave-token", time.Now(), fx.apply.ID)
	require.NoError(t, err)

	obs.OnProgress(fx.apply, fx.tasks)

	requireNoGitHubCalls(t, capture)
	owner := progressObserverOwner(t, fx.db, fx.apply.ID)
	assert.False(t, owner.Valid, "no durable authority may be recorded while a driver holds the parent lease")
}
