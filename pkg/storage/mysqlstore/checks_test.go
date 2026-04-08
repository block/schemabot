//go:build integration

package mysqlstore

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

func TestCheckStore_Upsert(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	check := &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "vitess",
		DatabaseName: "testdb",
		CheckRunID:   999,
		HasChanges:   true,
		Status:       "pending_apply",
		Conclusion:   "action_required",
	}

	// Insert
	require.NoError(t, store.Checks().Upsert(ctx, check))

	// Verify insert
	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "vitess", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, "pending_apply", retrieved.Status)

	// Update
	check.Status = "completed"
	check.Conclusion = "success"
	require.NoError(t, store.Checks().Upsert(ctx, check))

	// Verify update
	retrieved, err = store.Checks().Get(ctx, "org/repo", 123, "staging", "vitess", "testdb")
	require.NoError(t, err)
	require.Equal(t, "completed", retrieved.Status)
}

func TestCheckStore_GetByCheckRunID(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	check := &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "vitess",
		DatabaseName: "testdb",
		CheckRunID:   999,
		HasChanges:   true,
		Status:       "pending_apply",
	}

	require.NoError(t, store.Checks().Upsert(ctx, check))

	// GetByCheckRunID should return the check
	retrieved, err := store.Checks().GetByCheckRunID(ctx, 999)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, "testdb", retrieved.DatabaseName)

	// Non-existent should return nil
	retrieved, err = store.Checks().GetByCheckRunID(ctx, 12345)
	require.NoError(t, err)
	require.Nil(t, retrieved)
}

func TestCheckStore_GetByPR(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// GetByPR on empty table should return empty slice
	checks, err := store.Checks().GetByPR(ctx, "org/repo", 999)
	require.NoError(t, err)
	require.Empty(t, checks)

	// Create checks for same PR, different envs/dbs
	checksToCreate := []*storage.Check{
		{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "staging", DatabaseType: "vitess", DatabaseName: "db1", Status: "pending"},
		{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "production", DatabaseType: "vitess", DatabaseName: "db1", Status: "pending"},
		{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "staging", DatabaseType: "mysql", DatabaseName: "db2", Status: "pending"},
	}
	for _, c := range checksToCreate {
		require.NoError(t, store.Checks().Upsert(ctx, c))
	}

	// Create check for different PR
	require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  456,
		HeadSHA:      "def",
		Environment:  "staging",
		DatabaseType: "vitess",
		DatabaseName: "db1",
		Status:       "pending",
	}))

	// GetByPR should return only checks for PR 123
	retrieved, err := store.Checks().GetByPR(ctx, "org/repo", 123)
	require.NoError(t, err)
	require.Len(t, retrieved, 3)
}

func TestCheckStore_GetByDatabase(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// GetByDatabase on empty table should return empty slice
	checks, err := store.Checks().GetByDatabase(ctx, "org/repo", "staging", "vitess", "nonexistent")
	require.NoError(t, err)
	require.Empty(t, checks)

	// Create checks for same database across different PRs
	checksToCreate := []*storage.Check{
		{Repository: "org/repo", PullRequest: 100, HeadSHA: "a", Environment: "staging", DatabaseType: "vitess", DatabaseName: "shared-db", Status: "pending"},
		{Repository: "org/repo", PullRequest: 200, HeadSHA: "b", Environment: "staging", DatabaseType: "vitess", DatabaseName: "shared-db", Status: "pending"},
		{Repository: "org/repo", PullRequest: 300, HeadSHA: "c", Environment: "staging", DatabaseType: "vitess", DatabaseName: "shared-db", Status: "pending"},
	}
	for _, c := range checksToCreate {
		require.NoError(t, store.Checks().Upsert(ctx, c))
	}

	// Create check for different database
	require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  100,
		HeadSHA:      "a",
		Environment:  "staging",
		DatabaseType: "vitess",
		DatabaseName: "other-db",
		Status:       "pending",
	}))

	// GetByDatabase should return checks for shared-db
	retrieved, err := store.Checks().GetByDatabase(ctx, "org/repo", "staging", "vitess", "shared-db")
	require.NoError(t, err)
	require.Len(t, retrieved, 3)
}

func TestCheckStore_Delete(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	check := &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "abc",
		Environment:  "staging",
		DatabaseType: "vitess",
		DatabaseName: "testdb",
		Status:       "pending",
	}
	require.NoError(t, store.Checks().Upsert(ctx, check))

	// Get to find the ID
	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "vitess", "testdb")
	require.NoError(t, err)

	// Delete should succeed
	require.NoError(t, store.Checks().Delete(ctx, retrieved.ID))

	// Verify deleted
	retrieved, err = store.Checks().Get(ctx, "org/repo", 123, "staging", "vitess", "testdb")
	require.NoError(t, err)
	require.Nil(t, retrieved)

	// Delete non-existent should fail
	require.ErrorIs(t, store.Checks().Delete(ctx, 99999), storage.ErrCheckNotFound)
}

func TestCheckStore_DeleteByPR(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Create checks for same PR
	checksToCreate := []*storage.Check{
		{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "staging", DatabaseType: "vitess", DatabaseName: "db1", Status: "pending"},
		{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "production", DatabaseType: "vitess", DatabaseName: "db1", Status: "pending"},
		{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "staging", DatabaseType: "mysql", DatabaseName: "db2", Status: "pending"},
	}
	for _, c := range checksToCreate {
		require.NoError(t, store.Checks().Upsert(ctx, c))
	}

	// Create check for different PR (should not be deleted)
	require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  456,
		HeadSHA:      "def",
		Environment:  "staging",
		DatabaseType: "vitess",
		DatabaseName: "db1",
		Status:       "pending",
	}))

	// DeleteByPR should succeed
	require.NoError(t, store.Checks().DeleteByPR(ctx, "org/repo", 123))

	// Verify PR 123 checks are deleted
	retrieved, err := store.Checks().GetByPR(ctx, "org/repo", 123)
	require.NoError(t, err)
	require.Empty(t, retrieved)

	// Verify PR 456 check still exists
	retrieved, err = store.Checks().GetByPR(ctx, "org/repo", 456)
	require.NoError(t, err)
	require.Len(t, retrieved, 1)

	// DeleteByPR on non-existent PR should not error (no-op)
	require.NoError(t, store.Checks().DeleteByPR(ctx, "org/repo", 999))
}

func TestCheckStore_GetByPR_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Checks().GetByPR(t.Context(), "org/repo", 123)
	require.Error(t, err)
}

func TestCheckStore_GetByDatabase_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Checks().GetByDatabase(t.Context(), "org/repo", "staging", "vitess", "db")
	require.Error(t, err)
}

func TestCheckStore_Delete_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	err = store.Checks().Delete(t.Context(), 123)
	require.Error(t, err)
}

func TestCheckStore_CheckRunIDZeroIsNull(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Create check without CheckRunID (zero value)
	check := &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "vitess",
		DatabaseName: "testdb",
		CheckRunID:   0,
		HasChanges:   true,
		Status:       "pending_apply",
	}

	require.NoError(t, store.Checks().Upsert(ctx, check))

	// GetByCheckRunID(0) should NOT find the check (NULL != 0)
	retrieved, err := store.Checks().GetByCheckRunID(ctx, 0)
	require.NoError(t, err)
	require.Nil(t, retrieved)

	// Get by key should return the check with CheckRunID=0
	retrieved, err = store.Checks().Get(ctx, "org/repo", 123, "staging", "vitess", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, int64(0), retrieved.CheckRunID)
}
