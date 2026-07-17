//go:build integration

package mysqlstore

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
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

// Re-acquiring a lock the same owner already holds succeeds and overwrites the
// stored confirmation plan with the latest apply attempt's plan, so apply-confirm
// loads the plan the human just reviewed rather than a stale one.
func TestLockStore_Acquire_SameOwnerRefreshesPendingPlanID(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	require.NoError(t, store.Locks().Acquire(ctx, &storage.Lock{
		DatabaseName:  "testdb",
		DatabaseType:  "vitess",
		Repository:    "org/repo",
		PullRequest:   123,
		Owner:         "org/repo#123",
		PendingPlanID: "plan-1",
	}))

	require.NoError(t, store.Locks().Acquire(ctx, &storage.Lock{
		DatabaseName:  "testdb",
		DatabaseType:  "vitess",
		Repository:    "org/repo",
		PullRequest:   123,
		Owner:         "org/repo#123",
		PendingPlanID: "plan-2",
	}))

	lock, err := store.Locks().Get(ctx, "testdb", "vitess")
	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, "org/repo#123", lock.Owner)
	assert.Equal(t, "plan-2", lock.PendingPlanID)

	// A re-acquire with an empty plan (rollback, CLI) leaves the stored plan intact.
	require.NoError(t, store.Locks().Acquire(ctx, &storage.Lock{
		DatabaseName: "testdb",
		DatabaseType: "vitess",
		Repository:   "org/repo",
		PullRequest:  123,
		Owner:        "org/repo#123",
	}))
	lock, err = store.Locks().Get(ctx, "testdb", "vitess")
	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, "plan-2", lock.PendingPlanID)
}

// Two applies for the same PR and database (staging and production apply-confirms
// share the owner repo#pr and the database+type lock key) may acquire the lock
// concurrently. Every same-owner Acquire must succeed; none may be turned away
// with ErrLockHeld by losing the insert race against itself.
func TestLockStore_Acquire_SameOwnerConcurrent(t *testing.T) {
	clearTables(t)
	ctx := t.Context()

	const drivers = 16
	stores := make([]*Storage, drivers)
	for i := range drivers {
		db, openErr := sql.Open("mysql", testDSNChangedRows)
		require.NoError(t, openErr)
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		t.Cleanup(func() {
			require.NoError(t, db.Close())
		})
		stores[i] = New(db)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error

	for i := range drivers {
		driverStore := stores[i]
		planID := fmt.Sprintf("plan-%d", i)
		wg.Go(func() {
			<-start
			err := driverStore.Locks().Acquire(ctx, &storage.Lock{
				DatabaseName:  "testdb",
				DatabaseType:  "vitess",
				Repository:    "org/repo",
				PullRequest:   123,
				Owner:         "org/repo#123",
				PendingPlanID: planID,
			})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
			}
		})
	}

	close(start)
	wg.Wait()

	require.Empty(t, errs, "all same-owner Acquire calls should succeed")

	lock, err := stores[0].Locks().Get(ctx, "testdb", "vitess")
	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, "org/repo#123", lock.Owner)
	assert.Contains(t, lock.PendingPlanID, "plan-", "stored plan should be one of the concurrent attempts")
}

// Under MySQL's default changed-rows semantics, a same-owner refresh whose new
// confirmation plan already matches the stored value reports zero affected rows
// even though the owner still holds the lock. This happens when a concurrent
// same-owner caller wrote the same plan first. The refresh must treat the caller
// as still holding the lock and succeed, not turn it away with ErrLockHeld.
func TestLockStore_Acquire_RefreshSameOwnerValueAlreadyMatches(t *testing.T) {
	clearTables(t)
	ctx := t.Context()

	db, err := sql.Open("mysql", testDSNChangedRows)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	require.NoError(t, db.PingContext(ctx))
	store := &lockStore{db: db}

	require.NoError(t, store.Acquire(ctx, &storage.Lock{
		DatabaseName:  "testdb",
		DatabaseType:  "vitess",
		Repository:    "org/repo",
		PullRequest:   123,
		Owner:         "org/repo#123",
		PendingPlanID: "plan-1",
	}))

	// A concurrent same-owner caller already set pending_plan_id to plan-2.
	_, err = db.ExecContext(ctx, `
		UPDATE locks
		SET pending_plan_id = ?
		WHERE database_name = ? AND database_type = ? AND owner = ?
	`, "plan-2", "testdb", "vitess", "org/repo#123")
	require.NoError(t, err)

	// This caller's refresh targets the same value. The UPDATE matches the row but
	// changes nothing, so RowsAffected is 0 under changed-rows semantics. The caller
	// still owns the lock, so the refresh must succeed.
	err = store.refreshPendingPlanID(ctx,
		&storage.Lock{
			DatabaseName:  "testdb",
			DatabaseType:  "vitess",
			Owner:         "org/repo#123",
			PendingPlanID: "plan-2",
		},
		&storage.Lock{
			DatabaseName:  "testdb",
			DatabaseType:  "vitess",
			Owner:         "org/repo#123",
			PendingPlanID: "plan-1",
		})
	require.NoError(t, err)

	lock, err := store.Get(ctx, "testdb", "vitess")
	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, "org/repo#123", lock.Owner)
	assert.Equal(t, "plan-2", lock.PendingPlanID)

	// A full same-owner Acquire whose plan already matches the stored value must
	// also succeed for the same reason.
	require.NoError(t, store.Acquire(ctx, &storage.Lock{
		DatabaseName:  "testdb",
		DatabaseType:  "vitess",
		Repository:    "org/repo",
		PullRequest:   123,
		Owner:         "org/repo#123",
		PendingPlanID: "plan-2",
	}))
}

// When the lock changes hands between reading it and refreshing its confirmation
// plan, the refresh must not silently succeed: the caller no longer holds the lock
// and must learn the accurate cause (held by another owner, or gone entirely).
func TestLockStore_Acquire_RefreshOwnerNoLongerMatches(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := &lockStore{db: testDB}

	require.NoError(t, store.Acquire(ctx, &storage.Lock{
		DatabaseName:  "testdb",
		DatabaseType:  "vitess",
		Repository:    "org/repo",
		PullRequest:   123,
		Owner:         "org/repo#123",
		PendingPlanID: "plan-1",
	}))

	// The lock is reassigned to a different owner. A refresh that targets the old
	// owner predicate now matches no rows and must report ErrLockHeld.
	_, err := testDB.ExecContext(ctx, `
		UPDATE locks
		SET owner = ?
		WHERE database_name = ? AND database_type = ?
	`, "org/repo#999", "testdb", "vitess")
	require.NoError(t, err)

	err = store.refreshPendingPlanID(ctx,
		&storage.Lock{
			DatabaseName:  "testdb",
			DatabaseType:  "vitess",
			Owner:         "org/repo#123",
			PendingPlanID: "plan-2",
		},
		&storage.Lock{
			DatabaseName:  "testdb",
			DatabaseType:  "vitess",
			Owner:         "org/repo#123",
			PendingPlanID: "plan-1",
		})
	require.ErrorIs(t, err, storage.ErrLockHeld)

	// The new owner's plan must be untouched by the missed refresh.
	lock, err := store.Get(ctx, "testdb", "vitess")
	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, "org/repo#999", lock.Owner)
	assert.Equal(t, "plan-1", lock.PendingPlanID)

	// The lock is released outright. A refresh against the gone lock must report
	// ErrLockNotFound rather than silently succeeding.
	require.NoError(t, store.ForceRelease(ctx, "testdb", "vitess"))
	err = store.refreshPendingPlanID(ctx,
		&storage.Lock{
			DatabaseName:  "testdb",
			DatabaseType:  "vitess",
			Owner:         "org/repo#123",
			PendingPlanID: "plan-3",
		},
		&storage.Lock{
			DatabaseName:  "testdb",
			DatabaseType:  "vitess",
			Owner:         "org/repo#123",
			PendingPlanID: "plan-1",
		})
	require.ErrorIs(t, err, storage.ErrLockNotFound)
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

func TestLockStore_ReleaseIfPendingPlanID(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	require.NoError(t, store.Locks().Acquire(ctx, &storage.Lock{
		DatabaseName:  "testdb",
		DatabaseType:  "vitess",
		Repository:    "org/repo",
		PullRequest:   123,
		Owner:         "org/repo#123",
		PendingPlanID: "rollback:replacement",
	}))

	released, err := store.Locks().ReleaseIfPendingPlanID(ctx, "testdb", "vitess", "org/repo#123", "apply-original")
	require.NoError(t, err)
	assert.False(t, released)
	lock, err := store.Locks().Get(ctx, "testdb", "vitess")
	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, "rollback:replacement", lock.PendingPlanID)

	released, err = store.Locks().ReleaseIfPendingPlanID(ctx, "testdb", "vitess", "org/repo#123", "rollback:replacement")
	require.NoError(t, err)
	assert.True(t, released)
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
	store := newChangedRowsStore(t)

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

	// Wait 1 second so updated_at advances (MySQL DATETIME has second precision).
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

// Touching a lock whose updated_at already equals NOW() leaves the row
// unchanged. Under production changed-rows semantics that UPDATE reports zero
// affected rows, but the lock still exists, so the touch must succeed rather
// than report the lock as missing.
//
// The session timestamp is frozen on a single pinned connection so both the
// row's stored updated_at and the touch's NOW() resolve to the same instant,
// making the UPDATE a guaranteed no-op. Without freezing, a touch that crossed
// into the next one-second DATETIME tick would change updated_at and report one
// affected row, masking the changed-rows path this exercises.
func TestLockStore_UpdateSameSecondSucceeds(t *testing.T) {
	clearTables(t)
	ctx := t.Context()

	// Pin to a single connection so the frozen session timestamp persists across
	// the seed INSERT, the touch UPDATE, and the re-read.
	db, err := sql.Open("mysql", testDSNChangedRows)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	require.NoError(t, db.PingContext(ctx))

	// Freeze NOW() so the seeded updated_at and the touch's NOW() are identical.
	_, err = db.ExecContext(ctx, "SET TIMESTAMP = 1700000000")
	require.NoError(t, err)

	store := &lockStore{db: db}

	// Acquire seeds the row via the locks table's DEFAULT CURRENT_TIMESTAMP,
	// which resolves to the frozen NOW().
	require.NoError(t, store.Acquire(ctx, &storage.Lock{
		DatabaseName: "testdb",
		DatabaseType: "vitess",
		Repository:   "org/repo",
		PullRequest:  123,
		Owner:        "testuser",
	}))

	// updated_at already equals the frozen NOW(), so the touch's
	// SET updated_at = NOW() changes nothing: zero affected rows under
	// changed-rows semantics. The lock still exists, so the touch must succeed.
	require.NoError(t, store.Update(ctx, &storage.Lock{DatabaseName: "testdb", DatabaseType: "vitess"}),
		"a no-op touch leaves updated_at unchanged but the lock still exists")

	lock, err := store.Get(ctx, "testdb", "vitess")
	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, "testuser", lock.Owner)
}

func TestLockStore_UpdateNonExistent(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := newChangedRowsStore(t)

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
