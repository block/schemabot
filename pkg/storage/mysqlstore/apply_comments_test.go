//go:build integration

package mysqlstore

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func TestApplyCommentStore_Upsert(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_comment_upsert", 1)

	postedVolume := 3
	comment := &storage.ApplyComment{
		ApplyID:         apply.ID,
		CommentState:    state.Comment.Progress,
		GitHubCommentID: 111222333,
		PostedVolume:    &postedVolume,
	}

	// Insert
	require.NoError(t, store.ApplyComments().Upsert(ctx, comment))

	// Verify insert
	retrieved, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, apply.ID, retrieved.ApplyID)
	assert.Equal(t, state.Comment.Progress, retrieved.CommentState)
	assert.Equal(t, int64(111222333), retrieved.GitHubCommentID)
	require.NotNil(t, retrieved.PostedVolume)
	assert.Equal(t, 3, *retrieved.PostedVolume)
	assert.NotZero(t, retrieved.ID)
	assert.NotZero(t, retrieved.CreatedAt)
	assert.NotZero(t, retrieved.UpdatedAt)

	// Upsert with new comment ID and level (simulates a volume-change rotation)
	comment.GitHubCommentID = 444555666
	newVolume := 5
	comment.PostedVolume = &newVolume
	require.NoError(t, store.ApplyComments().Upsert(ctx, comment))

	// Verify upsert updated the comment ID and recorded level
	retrieved, err = store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, int64(444555666), retrieved.GitHubCommentID)
	require.NotNil(t, retrieved.PostedVolume)
	assert.Equal(t, 5, *retrieved.PostedVolume)

	// A summary comment carries no level; the column stays NULL and reads back nil.
	summary := &storage.ApplyComment{
		ApplyID:         apply.ID,
		CommentState:    state.Comment.Summary,
		GitHubCommentID: 777888999,
	}
	require.NoError(t, store.ApplyComments().Upsert(ctx, summary))
	retrieved, err = store.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Nil(t, retrieved.PostedVolume)
}

func TestApplyCommentStore_Get(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Get non-existent should return nil
	comment, err := store.ApplyComments().Get(ctx, 99999, state.Comment.Progress)
	require.NoError(t, err)
	require.Nil(t, comment)
}

func TestApplyCommentStore_ListByApply(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_comment_getbyapply", 1)

	// ListByApply with no comments should return empty slice
	comments, err := store.ApplyComments().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Empty(t, comments)

	// Create all three comment states
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:         apply.ID,
		CommentState:    state.Comment.Progress,
		GitHubCommentID: 100,
	}))
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:         apply.ID,
		CommentState:    state.Comment.Cutover,
		GitHubCommentID: 200,
	}))
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:         apply.ID,
		CommentState:    state.Comment.Summary,
		GitHubCommentID: 300,
	}))

	// ListByApply should return all three
	comments, err = store.ApplyComments().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, comments, 3)

	// Verify ordering (by id)
	states := make([]string, len(comments))
	for i, c := range comments {
		states[i] = c.CommentState
	}
	assert.Equal(t, []string{state.Comment.Progress, state.Comment.Cutover, state.Comment.Summary}, states)
}

func TestApplyCommentStore_ListByApply_Isolation(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply1 := createTestApply(t, store, lock, "apply_iso_1", 1)
	apply2 := createTestApplyWithStateAndEnv(t, store, lock, "apply_iso_2", 2, state.Apply.Completed, "staging")

	// Create comments for both applies
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:         apply1.ID,
		CommentState:    state.Comment.Progress,
		GitHubCommentID: 100,
	}))
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:         apply2.ID,
		CommentState:    state.Comment.Progress,
		GitHubCommentID: 200,
	}))

	// ListByApply should only return comments for apply1
	comments, err := store.ApplyComments().ListByApply(ctx, apply1.ID)
	require.NoError(t, err)
	require.Len(t, comments, 1)
	assert.Equal(t, int64(100), comments[0].GitHubCommentID)
}

func TestApplyCommentStore_DeleteByApply(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply1 := createTestApply(t, store, lock, "apply_comment_del1", 1)
	apply2 := createTestApplyWithStateAndEnv(t, store, lock, "apply_comment_del2", 2, state.Apply.Completed, "staging")

	// Create comments for both applies
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID: apply1.ID, CommentState: state.Comment.Progress, GitHubCommentID: 100,
	}))
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID: apply1.ID, CommentState: state.Comment.Summary, GitHubCommentID: 101,
	}))
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID: apply2.ID, CommentState: state.Comment.Progress, GitHubCommentID: 200,
	}))

	// Delete apply1's comments
	require.NoError(t, store.ApplyComments().DeleteByApply(ctx, apply1.ID))

	// apply1 comments should be gone
	comments, err := store.ApplyComments().ListByApply(ctx, apply1.ID)
	require.NoError(t, err)
	require.Empty(t, comments)

	// apply2 comment should still exist
	comments, err = store.ApplyComments().ListByApply(ctx, apply2.ID)
	require.NoError(t, err)
	require.Len(t, comments, 1)

	// DeleteByApply on non-existent should not error (no-op)
	require.NoError(t, store.ApplyComments().DeleteByApply(ctx, 99999))
}

func TestApplyCommentStore_Supersede(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_comment_supersede", 1)

	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID: apply.ID, CommentState: state.Comment.Progress, GitHubCommentID: 100,
	}))
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID: apply.ID, CommentState: state.Comment.Summary, GitHubCommentID: 101,
	}))

	// Supersede the summary marker. The row is retired in place, not deleted, and
	// the progress comment is untouched.
	require.NoError(t, store.ApplyComments().Supersede(ctx, apply.ID, state.Comment.Summary))

	summary, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, summary, "the superseded row is kept, not deleted")
	assert.NotNil(t, summary.SupersededAt, "the row is marked superseded")
	assert.Equal(t, int64(101), summary.GitHubCommentID, "the GitHub comment id is preserved")

	progress, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, progress)
	assert.Nil(t, progress.SupersededAt)
	assert.Equal(t, int64(100), progress.GitHubCommentID)

	// Re-posting a summary (e.g. on a later stop) reactivates the row.
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID: apply.ID, CommentState: state.Comment.Summary, GitHubCommentID: 102,
	}))
	summary, err = store.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, summary)
	assert.Nil(t, summary.SupersededAt, "a re-posted summary is active again")
	assert.Equal(t, int64(102), summary.GitHubCommentID)

	// Superseding a missing or already-superseded state is a no-op, not an error.
	require.NoError(t, store.ApplyComments().Supersede(ctx, apply.ID, state.Comment.Cutover))
	require.NoError(t, store.ApplyComments().Supersede(ctx, 99999, state.Comment.Progress))
}

func TestApplyCommentStore_PendingFreeze(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_comment_pending_freeze", 1)

	// A volume-change rotation records the freeze owed to the superseded
	// comment in the same write that tracks its successor.
	postedVolume := 5
	supersededID := int64(100)
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:                apply.ID,
		CommentState:           state.Comment.Progress,
		GitHubCommentID:        200,
		PostedVolume:           &postedVolume,
		PendingFreezeCommentID: &supersededID,
	}))

	retrieved, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, int64(200), retrieved.GitHubCommentID)
	require.NotNil(t, retrieved.PendingFreezeCommentID)
	assert.Equal(t, supersededID, *retrieved.PendingFreezeCommentID)

	// Once the frozen rendering lands on GitHub, the marker is cleared; the
	// rest of the row is untouched.
	require.NoError(t, store.ApplyComments().ClearPendingFreeze(ctx, apply.ID, state.Comment.Progress))
	retrieved, err = store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Nil(t, retrieved.PendingFreezeCommentID)
	assert.Equal(t, int64(200), retrieved.GitHubCommentID)
	require.NotNil(t, retrieved.PostedVolume)
	assert.Equal(t, 5, *retrieved.PostedVolume)

	// Clearing an already-clear marker or a missing row is a no-op, not an error.
	require.NoError(t, store.ApplyComments().ClearPendingFreeze(ctx, apply.ID, state.Comment.Progress))
	require.NoError(t, store.ApplyComments().ClearPendingFreeze(ctx, 99999, state.Comment.Progress))

	// A post that supersedes nothing leaves the marker NULL.
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID: apply.ID, CommentState: state.Comment.Summary, GitHubCommentID: 300,
	}))
	summary, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, summary)
	assert.Nil(t, summary.PendingFreezeCommentID)
}

func TestApplyCommentStore_UniqueConstraint(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_comment_unique", 1)

	// Insert two different states for same apply — should succeed
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID: apply.ID, CommentState: state.Comment.Progress, GitHubCommentID: 100,
	}))
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID: apply.ID, CommentState: state.Comment.Summary, GitHubCommentID: 200,
	}))

	// Verify both exist
	comments, err := store.ApplyComments().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, comments, 2)

	// Upsert same (apply_id, comment_state) with different github_comment_id — should update
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID: apply.ID, CommentState: state.Comment.Progress, GitHubCommentID: 999,
	}))

	// Should still be 2 comments, not 3
	comments, err = store.ApplyComments().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, comments, 2)

	// Progress should have updated ID
	progress, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	assert.Equal(t, int64(999), progress.GitHubCommentID)
}

func TestApplyCommentStore_LeaseGuardsWrites(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_comment_lease", 1)
	_, err := testDB.ExecContext(ctx, `
		UPDATE applies
		SET lease_owner = ?, lease_token = ?, lease_acquired_at = NOW()
		WHERE id = ?
	`, "current-driver", "current-token", apply.ID)
	require.NoError(t, err)

	comment := &storage.ApplyComment{
		ApplyID:         apply.ID,
		CommentState:    state.Comment.Progress,
		GitHubCommentID: 100,
	}
	staleCtx := storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: apply.ID, Owner: "old-driver", Token: "stale-token"})
	require.ErrorIs(t, store.ApplyComments().Upsert(staleCtx, comment), storage.ErrApplyLeaseLost)

	missing, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	assert.Nil(t, missing)

	currentCtx := storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: apply.ID, Owner: "current-driver", Token: "current-token"})
	require.NoError(t, store.ApplyComments().Upsert(currentCtx, comment))
	require.ErrorIs(t, store.ApplyComments().IncrementEditCount(staleCtx, apply.ID, state.Comment.Progress), storage.ErrApplyLeaseLost)

	retrieved, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, int64(100), retrieved.GitHubCommentID)
	assert.Equal(t, 0, retrieved.EditCount)

	require.NoError(t, store.ApplyComments().IncrementEditCount(currentCtx, apply.ID, state.Comment.Progress))
	retrieved, err = store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, 1, retrieved.EditCount)

	// ClearPendingFreeze is a lease-guarded write like the others: a stale
	// lease fails closed, the current lease clears the marker.
	pendingFreezeID := int64(50)
	comment.PendingFreezeCommentID = &pendingFreezeID
	require.NoError(t, store.ApplyComments().Upsert(currentCtx, comment))
	require.ErrorIs(t, store.ApplyComments().ClearPendingFreeze(staleCtx, apply.ID, state.Comment.Progress), storage.ErrApplyLeaseLost)

	retrieved, err = store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.NotNil(t, retrieved.PendingFreezeCommentID, "a stale lease must not clear the marker")

	require.NoError(t, store.ApplyComments().ClearPendingFreeze(currentCtx, apply.ID, state.Comment.Progress))
	retrieved, err = store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Nil(t, retrieved.PendingFreezeCommentID)
}

// DB error tests

func TestApplyCommentStore_Upsert_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	err = store.ApplyComments().Upsert(t.Context(), &storage.ApplyComment{
		ApplyID: 1, CommentState: "progress", GitHubCommentID: 100,
	})
	require.Error(t, err)
}

func TestApplyCommentStore_Get_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.ApplyComments().Get(t.Context(), 1, "progress")
	require.Error(t, err)
}

func TestApplyCommentStore_ListByApply_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.ApplyComments().ListByApply(t.Context(), 1)
	require.Error(t, err)
}

func TestApplyCommentStore_DeleteByApply_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	err = store.ApplyComments().DeleteByApply(t.Context(), 1)
	require.Error(t, err)
}

// TestApplyCommentStore_ClaimSummaryComment verifies the atomic summary-marker
// claim is first-writer-wins: the first claim inserts the sentinel
// (github_comment_id = 0) and wins, every later claim for the same apply loses,
// and a summary marker that already records a real comment also blocks the
// claim. Claims for different applies are independent.
func TestApplyCommentStore_ClaimSummaryComment(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_comment_claim", 1)
	otherLock := createTestLock(t, store, "testdb_other", "mysql", "staging")
	other := createTestApply(t, store, otherLock, "apply_comment_claim_other", 2)

	won, err := store.ApplyComments().ClaimSummaryComment(ctx, apply.ID)
	require.NoError(t, err)
	assert.True(t, won, "first claim must win")

	claimed, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, int64(0), claimed.GitHubCommentID, "won claim is the sentinel form of the marker")

	won, err = store.ApplyComments().ClaimSummaryComment(ctx, apply.ID)
	require.NoError(t, err)
	assert.False(t, won, "second claim for the same apply must lose")

	won, err = store.ApplyComments().ClaimSummaryComment(ctx, other.ID)
	require.NoError(t, err)
	assert.True(t, won, "claims for different applies are independent")

	// A recorded real comment blocks the claim the same way a sentinel does.
	require.NoError(t, store.ApplyComments().ReleaseSummaryClaim(ctx, apply.ID))
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID: apply.ID, CommentState: state.Comment.Summary, GitHubCommentID: 9001,
	}))
	won, err = store.ApplyComments().ClaimSummaryComment(ctx, apply.ID)
	require.NoError(t, err)
	assert.False(t, won, "a posted summary must block the claim")
}

// TestApplyCommentStore_ReclaimStaleSummaryClaim verifies crashed-publisher
// takeover: a claim sentinel older than the stale window transfers to the
// reclaimer (bumping updated_at so a second reclaimer loses), while a fresh
// sentinel, a missing marker, and a recorded real comment are all not
// reclaimable.
func TestApplyCommentStore_ReclaimStaleSummaryClaim(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_comment_reclaim", 1)

	reclaimed, err := store.ApplyComments().ReclaimStaleSummaryClaim(ctx, apply.ID)
	require.NoError(t, err)
	assert.False(t, reclaimed, "missing marker is not reclaimable")

	won, err := store.ApplyComments().ClaimSummaryComment(ctx, apply.ID)
	require.NoError(t, err)
	require.True(t, won)

	reclaimed, err = store.ApplyComments().ReclaimStaleSummaryClaim(ctx, apply.ID)
	require.NoError(t, err)
	assert.False(t, reclaimed, "fresh sentinel is an in-flight publish, not reclaimable")

	// Backdate the sentinel past the stale window to simulate a publisher that
	// crashed between claiming and posting.
	backdateSummaryClaim(t, apply.ID)

	reclaimed, err = store.ApplyComments().ReclaimStaleSummaryClaim(ctx, apply.ID)
	require.NoError(t, err)
	assert.True(t, reclaimed, "stale sentinel transfers to the reclaimer")

	reclaimed, err = store.ApplyComments().ReclaimStaleSummaryClaim(ctx, apply.ID)
	require.NoError(t, err)
	assert.False(t, reclaimed, "a just-reclaimed sentinel is fresh again; a second reclaimer loses")

	// A recorded real comment is never reclaimable, however old.
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID: apply.ID, CommentState: state.Comment.Summary, GitHubCommentID: 9001,
	}))
	backdateSummaryClaim(t, apply.ID)
	reclaimed, err = store.ApplyComments().ReclaimStaleSummaryClaim(ctx, apply.ID)
	require.NoError(t, err)
	assert.False(t, reclaimed, "a posted summary must never be reclaimed")
}

// TestApplyCommentStore_ReleaseSummaryClaim verifies release deletes only the
// sentinel form of the summary marker: a released claim can be re-won, a
// missing marker releases without error, and a marker recording a real posted
// comment survives release untouched.
func TestApplyCommentStore_ReleaseSummaryClaim(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_comment_release", 1)

	require.NoError(t, store.ApplyComments().ReleaseSummaryClaim(ctx, apply.ID), "releasing a missing claim is not an error")

	won, err := store.ApplyComments().ClaimSummaryComment(ctx, apply.ID)
	require.NoError(t, err)
	require.True(t, won)
	require.NoError(t, store.ApplyComments().ReleaseSummaryClaim(ctx, apply.ID))

	won, err = store.ApplyComments().ClaimSummaryComment(ctx, apply.ID)
	require.NoError(t, err)
	assert.True(t, won, "a released claim must be re-winnable")

	// Convert the claim to a posted summary; release must not delete it.
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID: apply.ID, CommentState: state.Comment.Summary, GitHubCommentID: 9001,
	}))
	require.NoError(t, store.ApplyComments().ReleaseSummaryClaim(ctx, apply.ID))
	posted, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
	require.NoError(t, err)
	require.NotNil(t, posted, "a recorded posted summary must survive release")
	assert.Equal(t, int64(9001), posted.GitHubCommentID)
}

// backdateSummaryClaim pushes an apply's summary marker updated_at past the
// stale-claim window, simulating a publisher that crashed after claiming.
func backdateSummaryClaim(t *testing.T, applyID int64) {
	t.Helper()
	_, err := testDB.ExecContext(t.Context(), `
		UPDATE apply_comments SET updated_at = NOW() - INTERVAL ? SECOND
		WHERE apply_id = ? AND comment_state = ?
	`, int64(storage.SummaryClaimStaleAfter.Seconds())+1, applyID, state.Comment.Summary)
	require.NoError(t, err)
}
