package storagetest

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// InsertPlanComment stores a plan comment for the given repository slot and
// returns it with its stored ID.
func InsertPlanComment(t *testing.T, store storage.Storage, repo string, pr int, database, databaseType, envScope, headSHA string, commentID int64) *storage.PlanComment {
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

// TestPlanComments runs the behavioral parity suite for
// storage.PlanCommentStore, plus the ExistsForDatabaseHead apply-ownership
// guard that decides whether a superseded plan comment is minimized or
// deleted.
//
// MarkMinimized/MarkDeleted stamp preservation on repeat marks needs to read
// the raw column and is covered in each dialect's own suite (the MySQL and
// PostgreSQL sqlstore tests); the parity contract here is the visible
// behavior — a retired comment drops out of the unretired listings and repeat
// or missing-id marks are no-ops.
func TestPlanComments(t *testing.T, h Harness) {
	t.Run("CanonicalizesIdentityKeys", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		inserted := InsertPlanComment(t, store, "MixedCase/Sample-Repo", 42, "OrdersDB", "MySQL", "Production,Staging", "sha1", 100)
		// Stored-value equality is the cross-dialect check that identity keys are canonicalized before persistence.
		assert.Equal(t, "mixedcase/sample-repo", inserted.Repository)
		assert.Equal(t, "ordersdb", inserted.DatabaseName)
		assert.Equal(t, "mysql", inserted.DatabaseType)
		// EnvironmentScope is not a query predicate and is compared in Go against
		// a scope built from configured environment names, so it is stored as given.
		assert.Equal(t, "Production,Staging", inserted.EnvironmentScope)

		comments, err := store.PlanComments().ListUnretiredForSlot(ctx, "MIXEDCASE/SAMPLE-REPO", 42, "ORDERSDB", "MYSQL")
		require.NoError(t, err)
		require.Len(t, comments, 1)
		assert.Equal(t, inserted.ID, comments[0].ID)

		comments, err = store.PlanComments().ListUnretiredForRepoPR(ctx, "MIXEDCASE/SAMPLE-REPO", 42)
		require.NoError(t, err)
		require.Len(t, comments, 1)
		assert.Equal(t, inserted.ID, comments[0].ID)
	})

	t.Run("Insert_And_ListUnretiredForSlot", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		// Two comments in the orders slot, one in a different-database slot on
		// the same PR, and one for the same database on a different PR: the
		// slot listing must return only its own slot, id ascending.
		first := InsertPlanComment(t, store, "org/repo", 42, "orders", "mysql", "production,staging", "sha1", 100)
		second := InsertPlanComment(t, store, "org/repo", 42, "orders", "mysql", "production,staging", "sha2", 200)
		InsertPlanComment(t, store, "org/repo", 42, "billing", "mysql", "production,staging", "sha1", 300)
		InsertPlanComment(t, store, "org/repo", 7, "orders", "mysql", "production,staging", "sha1", 400)

		comments, err := store.PlanComments().ListUnretiredForSlot(ctx, "org/repo", 42, "orders", "mysql")
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
		comments, err = store.PlanComments().ListUnretiredForSlot(ctx, "org/repo", 42, "orders", "vitess")
		require.NoError(t, err)
		assert.Empty(t, comments)
	})

	t.Run("ListUnretiredForRepoPR", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		// The repo-PR listing spans every database slot on the PR but never
		// leaks other PRs, and both minimized and deleted comments drop out
		// of it.
		orders := InsertPlanComment(t, store, "org/repo", 42, "orders", "mysql", "staging", "sha1", 100)
		billing := InsertPlanComment(t, store, "org/repo", 42, "billing", "vitess", "staging", "sha1", 200)
		minimized := InsertPlanComment(t, store, "org/repo", 42, "orders", "mysql", "staging", "sha2", 300)
		deleted := InsertPlanComment(t, store, "org/repo", 42, "billing", "vitess", "staging", "sha2", 500)
		InsertPlanComment(t, store, "org/repo", 7, "orders", "mysql", "staging", "sha1", 400)

		require.NoError(t, store.PlanComments().MarkMinimized(ctx, minimized.ID))
		require.NoError(t, store.PlanComments().MarkDeleted(ctx, deleted.ID))

		comments, err := store.PlanComments().ListUnretiredForRepoPR(ctx, "org/repo", 42)
		require.NoError(t, err)
		require.Len(t, comments, 2, "every unretired slot on the PR, no other PRs")
		assert.Equal(t, orders.ID, comments[0].ID, "ordered by id ascending")
		assert.Equal(t, billing.ID, comments[1].ID)

		empty, err := store.PlanComments().ListUnretiredForRepoPR(ctx, "org/repo", 999)
		require.NoError(t, err)
		assert.Empty(t, empty)
	})

	t.Run("MarkMinimized", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		first := InsertPlanComment(t, store, "org/repo", 42, "orders", "mysql", "staging", "sha1", 100)
		second := InsertPlanComment(t, store, "org/repo", 42, "orders", "mysql", "staging", "sha2", 200)

		require.NoError(t, store.PlanComments().MarkMinimized(ctx, first.ID))

		comments, err := store.PlanComments().ListUnretiredForSlot(ctx, "org/repo", 42, "orders", "mysql")
		require.NoError(t, err)
		require.Len(t, comments, 1, "the minimized comment drops out of the unretired list")
		assert.Equal(t, second.ID, comments[0].ID)

		// Marking an already-minimized or missing row is a no-op, not an
		// error, and the survivor stays listed.
		require.NoError(t, store.PlanComments().MarkMinimized(ctx, first.ID))
		require.NoError(t, store.PlanComments().MarkMinimized(ctx, 99999))

		comments, err = store.PlanComments().ListUnretiredForSlot(ctx, "org/repo", 42, "orders", "mysql")
		require.NoError(t, err)
		require.Len(t, comments, 1)
		assert.Equal(t, second.ID, comments[0].ID)
	})

	t.Run("MarkDeleted", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		first := InsertPlanComment(t, store, "org/repo", 42, "orders", "mysql", "staging", "sha1", 100)
		second := InsertPlanComment(t, store, "org/repo", 42, "orders", "mysql", "staging", "sha2", 200)

		require.NoError(t, store.PlanComments().MarkDeleted(ctx, first.ID))

		comments, err := store.PlanComments().ListUnretiredForSlot(ctx, "org/repo", 42, "orders", "mysql")
		require.NoError(t, err)
		require.Len(t, comments, 1, "the deleted comment drops out of the unretired list")
		assert.Equal(t, second.ID, comments[0].ID)

		// Marking an already-deleted or missing row is a no-op, not an error,
		// and the survivor stays listed.
		require.NoError(t, store.PlanComments().MarkDeleted(ctx, first.ID))
		require.NoError(t, store.PlanComments().MarkDeleted(ctx, 99999))

		comments, err = store.PlanComments().ListUnretiredForSlot(ctx, "org/repo", 42, "orders", "mysql")
		require.NoError(t, err)
		require.Len(t, comments, 1)
		assert.Equal(t, second.ID, comments[0].ID)
	})

	t.Run("ExistsForDatabaseHead", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		// The apply-ownership guard that picks a superseded plan comment's
		// retirement: a plan comment whose head produced an apply is
		// minimized (the record stays expandable), one that did not is
		// deleted. An apply whose plan row was deleted counts as owning any
		// head because the head it came from can no longer be proven.

		// No applies at all: nothing owns any head.
		exists, err := store.Applies().ExistsForDatabaseHead(ctx, "org/repo", 123, "testdb", "mysql", "shaA")
		require.NoError(t, err)
		assert.False(t, exists)

		planID, err := store.Plans().Create(ctx, &storage.Plan{
			PlanIdentifier: "plan_exists_head",
			Database:       "testdb",
			DatabaseType:   storage.DatabaseTypeMySQL,
			Repository:     "org/repo",
			PullRequest:    123,
			Environment:    "staging",
			HeadSHA:        "shaA",
			CreatedAt:      time.Now().UTC().Truncate(time.Second),
		})
		require.NoError(t, err)

		lock := CreateLock(t, store, "testdb", "mysql")
		CreateApply(t, store, lock, "apply_exists_head", planID)

		exists, err = store.Applies().ExistsForDatabaseHead(ctx, "org/repo", 123, "testdb", "mysql", "shaA")
		require.NoError(t, err)
		assert.True(t, exists, "an apply from a plan at this head owns it")

		exists, err = store.Applies().ExistsForDatabaseHead(ctx, "org/repo", 123, "testdb", "mysql", "shaB")
		require.NoError(t, err)
		assert.False(t, exists, "a different head is not owned while the plan row proves the apply's head")

		// Other repositories, PRs, databases, and database types are isolated.
		exists, err = store.Applies().ExistsForDatabaseHead(ctx, "org/other", 123, "testdb", "mysql", "shaA")
		require.NoError(t, err)
		assert.False(t, exists)
		exists, err = store.Applies().ExistsForDatabaseHead(ctx, "org/repo", 999, "testdb", "mysql", "shaA")
		require.NoError(t, err)
		assert.False(t, exists)
		exists, err = store.Applies().ExistsForDatabaseHead(ctx, "org/repo", 123, "otherdb", "mysql", "shaA")
		require.NoError(t, err)
		assert.False(t, exists)
		exists, err = store.Applies().ExistsForDatabaseHead(ctx, "org/repo", 123, "testdb", "vitess", "shaA")
		require.NoError(t, err)
		assert.False(t, exists)

		// Deleting the plan removes the proof of which head the apply came
		// from; the apply then counts as owning every head.
		require.NoError(t, store.Plans().Delete(ctx, planID))
		exists, err = store.Applies().ExistsForDatabaseHead(ctx, "org/repo", 123, "testdb", "mysql", "shaB")
		require.NoError(t, err)
		assert.True(t, exists, "an apply without a plan row owns any head")
	})

	t.Run("Insert_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		err := store.PlanComments().Insert(t.Context(), &storage.PlanComment{
			Repository: "org/repo", PullRequest: 1, DatabaseName: "db", DatabaseType: "mysql",
		})
		require.Error(t, err)
	})

	t.Run("ListUnretiredForSlot_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.PlanComments().ListUnretiredForSlot(t.Context(), "org/repo", 1, "db", "mysql")
		require.Error(t, err)
	})

	t.Run("ListUnretiredForRepoPR_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.PlanComments().ListUnretiredForRepoPR(t.Context(), "org/repo", 1)
		require.Error(t, err)
	})

	t.Run("MarkMinimized_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		require.Error(t, store.PlanComments().MarkMinimized(t.Context(), 1))
	})

	t.Run("MarkDeleted_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		require.Error(t, store.PlanComments().MarkDeleted(t.Context(), 1))
	})

	t.Run("ExistsForDatabaseHead_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.Applies().ExistsForDatabaseHead(t.Context(), "org/repo", 1, "db", "mysql", "sha")
		require.Error(t, err)
	})
}
