//go:build integration

package mysqlstore

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

func TestLockStore_Acquire(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := &storage.Lock{
		DatabaseName: "testdb",
		DatabaseType: "vitess",
		Repository:   "org/repo",
		PullRequest:  123,
		Owner:        "testuser",
	}

	// First acquire should succeed
	require.NoError(t, store.Locks().Acquire(ctx, lock))

	// Second acquire with same owner should succeed (idempotent)
	require.NoError(t, store.Locks().Acquire(ctx, lock))

	// Acquire with different owner should fail with ErrLockHeld
	differentOwner := &storage.Lock{
		DatabaseName: "testdb",
		DatabaseType: "vitess",
		Repository:   "org/other-repo",
		PullRequest:  456,
		Owner:        "otheruser",
	}
	require.ErrorIs(t, store.Locks().Acquire(ctx, differentOwner), storage.ErrLockHeld)
}

func TestLockStore_Release(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := &storage.Lock{
		DatabaseName: "testdb",
		DatabaseType: "vitess",
		Repository:   "org/repo",
		PullRequest:  123,
		Owner:        "testuser",
	}

	// Acquire lock
	require.NoError(t, store.Locks().Acquire(ctx, lock))

	// Release with wrong owner should fail
	require.ErrorIs(t, store.Locks().Release(ctx, "testdb", "vitess", "wronguser"), storage.ErrLockNotOwned)

	// Release with correct owner should succeed
	require.NoError(t, store.Locks().Release(ctx, "testdb", "vitess", "testuser"))

	// Release non-existent lock should fail
	require.ErrorIs(t, store.Locks().Release(ctx, "testdb", "vitess", "testuser"), storage.ErrLockNotFound)
}

func TestLockStore_ReleaseIsolation(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Create multiple locks for different databases
	locks := []*storage.Lock{
		{DatabaseName: "db1", DatabaseType: "vitess", Repository: "org/repo", PullRequest: 100, Owner: "userA"},
		{DatabaseName: "db2", DatabaseType: "vitess", Repository: "org/repo", PullRequest: 200, Owner: "userA"},
		{DatabaseName: "db1", DatabaseType: "mysql", Repository: "org/repo", PullRequest: 300, Owner: "userA"},
		{DatabaseName: "db3", DatabaseType: "vitess", Repository: "org/repo", PullRequest: 400, Owner: "userB"},
	}
	for _, lock := range locks {
		require.NoError(t, store.Locks().Acquire(ctx, lock))
	}

	// Verify all 4 locks exist
	allLocks, err := store.Locks().List(ctx)
	require.NoError(t, err)
	require.Len(t, allLocks, 4)

	// Release db1/vitess (owned by userA)
	require.NoError(t, store.Locks().Release(ctx, "db1", "vitess", "userA"))

	// Verify only db1/vitess was released, other 3 still exist
	allLocks, err = store.Locks().List(ctx)
	require.NoError(t, err)
	require.Len(t, allLocks, 3)

	// Verify specific locks still exist
	lock, err := store.Locks().Get(ctx, "db2", "vitess")
	require.NoError(t, err)
	require.NotNil(t, lock, "db2/vitess should still exist")

	lock, err = store.Locks().Get(ctx, "db1", "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock, "db1/mysql should still exist")

	lock, err = store.Locks().Get(ctx, "db3", "vitess")
	require.NoError(t, err)
	require.NotNil(t, lock, "db3/vitess should still exist")

	// db1/vitess should be gone
	lock, err = store.Locks().Get(ctx, "db1", "vitess")
	require.NoError(t, err)
	require.Nil(t, lock, "db1/vitess should have been released")
}

func TestLockStore_ReleaseWrongOwnerDoesNotDelete(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// UserA acquires lock
	require.NoError(t, store.Locks().Acquire(ctx, &storage.Lock{
		DatabaseName: "testdb",
		DatabaseType: "vitess",
		Repository:   "org/repo",
		PullRequest:  123,
		Owner:        "userA",
	}))

	// UserB tries to release - should fail
	require.ErrorIs(t, store.Locks().Release(ctx, "testdb", "vitess", "userB"), storage.ErrLockNotOwned)

	// Lock should still exist and be owned by userA
	lock, err := store.Locks().Get(ctx, "testdb", "vitess")
	require.NoError(t, err)
	require.NotNil(t, lock, "Lock should still exist after failed release attempt")
	require.Equal(t, "userA", lock.Owner)
}

func TestLockStore_ForceRelease(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := &storage.Lock{
		DatabaseName: "testdb",
		DatabaseType: "vitess",
		Repository:   "org/repo",
		PullRequest:  123,
		Owner:        "testuser",
	}

	// Acquire lock
	require.NoError(t, store.Locks().Acquire(ctx, lock))

	// Force release should succeed regardless of owner
	require.NoError(t, store.Locks().ForceRelease(ctx, "testdb", "vitess"))

	// Force release non-existent should fail
	require.ErrorIs(t, store.Locks().ForceRelease(ctx, "testdb", "vitess"), storage.ErrLockNotFound)
}

func TestLockStore_Get(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Get non-existent lock should return nil
	lock, err := store.Locks().Get(ctx, "testdb", "vitess")
	require.NoError(t, err)
	require.Nil(t, lock)

	// Create lock
	require.NoError(t, store.Locks().Acquire(ctx, &storage.Lock{
		DatabaseName: "testdb",
		DatabaseType: "vitess",
		Repository:   "org/repo",
		PullRequest:  123,
		Owner:        "testuser",
	}))

	// Get should return the lock
	lock, err = store.Locks().Get(ctx, "testdb", "vitess")
	require.NoError(t, err)
	require.NotNil(t, lock)
	require.Equal(t, "testdb", lock.DatabaseName)
	require.Equal(t, "testuser", lock.Owner)
}

func TestLockStore_List(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// List empty should return empty slice
	locks, err := store.Locks().List(ctx)
	require.NoError(t, err)
	require.Empty(t, locks)

	// Create some locks
	for i := range 3 {
		require.NoError(t, store.Locks().Acquire(ctx, &storage.Lock{
			DatabaseName: fmt.Sprintf("db%d", i),
			DatabaseType: "vitess",
			Repository:   "org/repo",
			PullRequest:  100 + i,
			Owner:        "testuser",
		}))
	}

	// List should return all locks
	locks, err = store.Locks().List(ctx)
	require.NoError(t, err)
	require.Len(t, locks, 3)
}

func TestLockStore_Update(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Create lock
	require.NoError(t, store.Locks().Acquire(ctx, &storage.Lock{
		DatabaseName: "testdb",
		DatabaseType: "vitess",
		Repository:   "org/repo",
		PullRequest:  123,
		Owner:        "testuser",
	}))

	// Get initial lock to check updated_at
	initial, err := store.Locks().Get(ctx, "testdb", "vitess")
	require.NoError(t, err)
	require.NotNil(t, initial)

	// Wait 1 second to ensure updated_at will change (MySQL datetime has second precision)
	time.Sleep(1 * time.Second)

	// Update lock (just touches updated_at)
	require.NoError(t, store.Locks().Update(ctx, &storage.Lock{
		DatabaseName: "testdb",
		DatabaseType: "vitess",
	}))

	// Verify updated_at changed
	updated, err := store.Locks().Get(ctx, "testdb", "vitess")
	require.NoError(t, err)
	require.True(t, updated.UpdatedAt.After(initial.UpdatedAt),
		"expected updated_at to change, initial: %v, updated: %v", initial.UpdatedAt, updated.UpdatedAt)
}

func TestLockStore_UpdateNonExistent(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	err := store.Locks().Update(ctx, &storage.Lock{
		DatabaseName: "nonexistent",
		DatabaseType: "vitess",
	})
	require.ErrorIs(t, err, storage.ErrLockNotFound)
}

func TestStorage_Close(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)

	store := New(db)

	// Verify connection works
	require.NoError(t, db.PingContext(t.Context()))

	// Close should succeed
	require.NoError(t, store.Close())

	// After close, operations should fail
	require.Error(t, db.PingContext(t.Context()))
}

func TestStorage_Ping(t *testing.T) {
	store := New(testDB)
	require.NoError(t, store.Ping(t.Context()))
}

func TestStorage_Ping_Error(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	require.Error(t, store.Ping(t.Context()))
}

func TestLockStore_Acquire_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	err = store.Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName: "testdb",
		DatabaseType: "vitess",
		Repository:   "org/repo",
		PullRequest:  123,
		Owner:        "testuser",
	})
	require.Error(t, err)
}

func TestLockStore_List_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Locks().List(t.Context())
	require.Error(t, err)
}

func TestLockStore_GetByPR_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Locks().GetByPR(t.Context(), "org/repo", 123)
	require.Error(t, err)
}

func TestLockStore_Update_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	err = store.Locks().Update(t.Context(), &storage.Lock{
		DatabaseName: "testdb",
		DatabaseType: "vitess",
	})
	require.Error(t, err)
}

func TestLockStore_Release_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	err = store.Locks().Release(t.Context(), "testdb", "vitess", "owner")
	require.Error(t, err)
}

func TestLockStore_ForceRelease_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	err = store.Locks().ForceRelease(t.Context(), "testdb", "vitess")
	require.Error(t, err)
}

func TestLockStore_GetByPR(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// GetByPR on empty table should return empty slice
	locks, err := store.Locks().GetByPR(ctx, "org/repo", 999)
	require.NoError(t, err)
	require.Empty(t, locks)

	// Create locks for same PR
	for i := range 3 {
		require.NoError(t, store.Locks().Acquire(ctx, &storage.Lock{
			DatabaseName: fmt.Sprintf("db%d", i),
			DatabaseType: "vitess",
			Repository:   "org/repo",
			PullRequest:  123,
			Owner:        "testuser",
		}))
	}

	// Create lock for different PR
	require.NoError(t, store.Locks().Acquire(ctx, &storage.Lock{
		DatabaseName: "other-db",
		DatabaseType: "vitess",
		Repository:   "org/repo",
		PullRequest:  456,
		Owner:        "testuser",
	}))

	// GetByPR should return only locks for PR 123
	locks, err = store.Locks().GetByPR(ctx, "org/repo", 123)
	require.NoError(t, err)
	require.Len(t, locks, 3)
}
