//go:build integration

package sqlstore

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"

	_ "github.com/block/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

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
		db, openErr := sql.Open("block-mysql", testDSNChangedRows)
		require.NoError(t, openErr)
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		t.Cleanup(func() {
			require.NoError(t, db.Close())
		})
		stores[i] = NewMySQL(db)
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

	db, err := sql.Open("block-mysql", testDSNChangedRows)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	require.NoError(t, db.PingContext(ctx))
	store := &lockStore{db: newRebindDB(db, MySQLDialect{}), dialect: MySQLDialect{}, classifier: NewMySQLErrorClassifier()}

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
	err = store.refreshPendingConfirmation(ctx,
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
	store := &lockStore{db: newRebindDB(testDB, MySQLDialect{}), dialect: MySQLDialect{}, classifier: NewMySQLErrorClassifier()}

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

	err = store.refreshPendingConfirmation(ctx,
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
	err = store.refreshPendingConfirmation(ctx,
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

	// Backdate the row so the touch's NOW() is guaranteed to land ahead of
	// the stored stamp even within the same one-second DATETIME tick.
	_, err := testDB.ExecContext(ctx, `
		UPDATE locks SET updated_at = NOW() - INTERVAL 1 HOUR
		WHERE database_name = 'testdb' AND database_type = 'vitess'
	`)
	require.NoError(t, err)

	initial, err := store.Locks().Get(ctx, "testdb", "vitess")
	require.NoError(t, err)
	require.NotNil(t, initial)

	// Update lock (just touches updated_at)
	require.NoError(t, store.Locks().Update(ctx, &storage.Lock{
		DatabaseName: "testdb",
		DatabaseType: "vitess",
		Owner:        "testuser",
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
	db, err := sql.Open("block-mysql", testDSNChangedRows)
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

	store := &lockStore{db: newRebindDB(db, MySQLDialect{}), dialect: MySQLDialect{}, classifier: NewMySQLErrorClassifier()}

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
	require.NoError(t, store.Update(ctx, &storage.Lock{DatabaseName: "testdb", DatabaseType: "vitess", Owner: "testuser"}),
		"a no-op touch leaves updated_at unchanged but the lock still exists")

	lock, err := store.Get(ctx, "testdb", "vitess")
	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, "testuser", lock.Owner)
}

func TestStorage_Close(t *testing.T) {
	db, err := sql.Open("block-mysql", testDSN)
	require.NoError(t, err)

	store := NewMySQL(db)

	// Verify connection works
	require.NoError(t, db.PingContext(t.Context()))

	// Close should succeed
	require.NoError(t, store.Close())

	// After close, operations should fail
	require.Error(t, db.PingContext(t.Context()))
}

func TestStorage_Ping(t *testing.T) {
	store := NewMySQL(testDB)
	require.NoError(t, store.Ping(t.Context()))
}

func TestStorage_Ping_Error(t *testing.T) {
	db, err := sql.Open("block-mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := NewMySQL(db)
	require.Error(t, store.Ping(t.Context()))
}
