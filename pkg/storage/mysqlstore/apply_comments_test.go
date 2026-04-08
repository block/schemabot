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

	comment := &storage.ApplyComment{
		ApplyID:         apply.ID,
		CommentState:    state.Comment.Progress,
		GitHubCommentID: 111222333,
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
	assert.NotZero(t, retrieved.ID)
	assert.NotZero(t, retrieved.CreatedAt)
	assert.NotZero(t, retrieved.UpdatedAt)

	// Upsert with new comment ID (simulates Start/resume)
	comment.GitHubCommentID = 444555666
	require.NoError(t, store.ApplyComments().Upsert(ctx, comment))

	// Verify upsert updated the comment ID
	retrieved, err = store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, int64(444555666), retrieved.GitHubCommentID)
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
	apply2 := createTestApply(t, store, lock, "apply_iso_2", 2)

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
	apply2 := createTestApply(t, store, lock, "apply_comment_del2", 2)

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
