//go:build integration

package mysqlstore

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

func insertTestPlanComment(t *testing.T, store *Storage, repo string, pr int, database, databaseType, envScope, headSHA string, commentID int64) *storage.PlanComment {
	t.Helper()
	comment := &storage.PlanComment{
		Repository:       repo,
		PullRequest:      pr,
		DatabaseName:     database,
		DatabaseType:     databaseType,
		EnvironmentScope: envScope,
		HeadSHA:          headSHA,
		GitHubCommentID:  commentID,
		GitHubNodeID:     fmt.Sprintf("IC_node%d", commentID),
	}
	require.NoError(t, store.PlanComments().Insert(t.Context(), comment))
	require.NotZero(t, comment.ID, "Insert must set the row ID")
	return comment
}

func TestPlanCommentStore_InsertAndListUnminimizedForSlot(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Two comments in the orders slot, one in a different-database slot on the
	// same PR, and one for the same database on a different PR.
	first := insertTestPlanComment(t, store, "org/repo", 42, "orders", "mysql", "production,staging", "sha1", 100)
	second := insertTestPlanComment(t, store, "org/repo", 42, "orders", "mysql", "production,staging", "sha2", 200)
	insertTestPlanComment(t, store, "org/repo", 42, "billing", "mysql", "production,staging", "sha1", 300)
	insertTestPlanComment(t, store, "org/repo", 7, "orders", "mysql", "production,staging", "sha1", 400)

	comments, err := store.PlanComments().ListUnminimizedForSlot(ctx, "org/repo", 42, "orders", "mysql")
	require.NoError(t, err)
	require.Len(t, comments, 2, "only the orders slot on PR 42 is listed")

	assert.Equal(t, first.ID, comments[0].ID, "ordered by id ascending")
	assert.Equal(t, second.ID, comments[1].ID)
	assert.Equal(t, "org/repo", comments[0].Repository)
	assert.Equal(t, 42, comments[0].PullRequest)
	assert.Equal(t, "orders", comments[0].DatabaseName)
	assert.Equal(t, "mysql", comments[0].DatabaseType)
	assert.Equal(t, "production,staging", comments[0].EnvironmentScope)
	assert.Equal(t, "sha1", comments[0].HeadSHA)
	assert.Equal(t, int64(100), comments[0].GitHubCommentID)
	assert.Equal(t, "IC_node100", comments[0].GitHubNodeID)
	assert.Nil(t, comments[0].MinimizedAt)
	assert.NotZero(t, comments[0].CreatedAt)
	assert.NotZero(t, comments[0].UpdatedAt)

	// An empty slot lists as empty, not an error.
	comments, err = store.PlanComments().ListUnminimizedForSlot(ctx, "org/repo", 42, "orders", "vitess")
	require.NoError(t, err)
	assert.Empty(t, comments)
}

func TestPlanCommentStore_MarkMinimized(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	first := insertTestPlanComment(t, store, "org/repo", 42, "orders", "mysql", "staging", "sha1", 100)
	second := insertTestPlanComment(t, store, "org/repo", 42, "orders", "mysql", "staging", "sha2", 200)

	require.NoError(t, store.PlanComments().MarkMinimized(ctx, first.ID))

	comments, err := store.PlanComments().ListUnminimizedForSlot(ctx, "org/repo", 42, "orders", "mysql")
	require.NoError(t, err)
	require.Len(t, comments, 1, "the minimized comment drops out of the unminimized list")
	assert.Equal(t, second.ID, comments[0].ID)

	var minimizedAt *time.Time
	require.NoError(t, testDB.QueryRowContext(ctx,
		"SELECT minimized_at FROM plan_comments WHERE id = ?", first.ID).Scan(&minimizedAt))
	require.NotNil(t, minimizedAt, "the row is stamped, not deleted")

	// Marking an already-minimized or missing row is a no-op, not an error, and
	// the original stamp is preserved.
	require.NoError(t, store.PlanComments().MarkMinimized(ctx, first.ID))
	require.NoError(t, store.PlanComments().MarkMinimized(ctx, 99999))

	var minimizedAtAfter *time.Time
	require.NoError(t, testDB.QueryRowContext(ctx,
		"SELECT minimized_at FROM plan_comments WHERE id = ?", first.ID).Scan(&minimizedAtAfter))
	require.NotNil(t, minimizedAtAfter)
	assert.Equal(t, *minimizedAt, *minimizedAtAfter, "a repeat mark must not move the stamp")
}

// TestApplyStore_ExistsForDatabaseHead exercises the apply-ownership guard
// used before minimizing a superseded plan comment: a plan comment whose head
// produced an apply must stay expanded, and an apply whose plan row was
// deleted counts as owning any head because the head it came from can no
// longer be proven.
func TestApplyStore_ExistsForDatabaseHead(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// No applies at all: nothing owns any head.
	exists, err := store.Applies().ExistsForDatabaseHead(ctx, "org/repo", 123, "testdb", "mysql", "shaA")
	require.NoError(t, err)
	assert.False(t, exists)

	planID, err := store.Plans().Create(ctx, &storage.Plan{
		PlanIdentifier: "plan_exists_head",
		Database:       "testdb",
		DatabaseType:   "mysql",
		Repository:     "org/repo",
		PullRequest:    123,
		Environment:    "staging",
		HeadSHA:        "shaA",
		CreatedAt:      time.Now(),
	})
	require.NoError(t, err)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	createTestApply(t, store, lock, "apply_exists_head", planID)

	exists, err = store.Applies().ExistsForDatabaseHead(ctx, "org/repo", 123, "testdb", "mysql", "shaA")
	require.NoError(t, err)
	assert.True(t, exists, "an apply from a plan at this head owns it")

	exists, err = store.Applies().ExistsForDatabaseHead(ctx, "org/repo", 123, "testdb", "mysql", "shaB")
	require.NoError(t, err)
	assert.False(t, exists, "a different head is not owned while the plan row proves the apply's head")

	// Other PRs and other databases are isolated.
	exists, err = store.Applies().ExistsForDatabaseHead(ctx, "org/repo", 999, "testdb", "mysql", "shaA")
	require.NoError(t, err)
	assert.False(t, exists)
	exists, err = store.Applies().ExistsForDatabaseHead(ctx, "org/repo", 123, "otherdb", "mysql", "shaA")
	require.NoError(t, err)
	assert.False(t, exists)

	// Deleting the plan removes the proof of which head the apply came from;
	// the apply then counts as owning every head.
	require.NoError(t, store.Plans().Delete(ctx, planID))
	exists, err = store.Applies().ExistsForDatabaseHead(ctx, "org/repo", 123, "testdb", "mysql", "shaB")
	require.NoError(t, err)
	assert.True(t, exists, "an apply without a plan row owns any head")
}

// DB error tests

func TestPlanCommentStore_Insert_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	err = store.PlanComments().Insert(t.Context(), &storage.PlanComment{
		Repository: "org/repo", PullRequest: 1, DatabaseName: "db", DatabaseType: "mysql",
	})
	require.Error(t, err)
}

func TestPlanCommentStore_ListUnminimizedForSlot_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.PlanComments().ListUnminimizedForSlot(t.Context(), "org/repo", 1, "db", "mysql")
	require.Error(t, err)
}

func TestPlanCommentStore_MarkMinimized_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	require.Error(t, store.PlanComments().MarkMinimized(t.Context(), 1))
}

func TestApplyStore_ExistsForDatabaseHead_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Applies().ExistsForDatabaseHead(t.Context(), "org/repo", 1, "db", "mysql", "sha")
	require.Error(t, err)
}
