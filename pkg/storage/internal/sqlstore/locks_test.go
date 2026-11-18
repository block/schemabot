//go:build integration

package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
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
	store := &lockStore{db: newRebindDB(db, MySQLDialect{}), dialect: MySQLDialect{}, identity: MySQLDialect{}, classifier: NewMySQLErrorClassifier()}

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
		}, nil)
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

// A conditional refresh scopes its UPDATE to the pending plan the caller
// observed, so a zero-row result has one more cause to tell apart: the same
// owner replaced the pin in between. Under changed-rows semantics the refresh
// still succeeds when the concurrent write set the very plan this caller
// wanted, and reports ErrLockIntentChanged when it set any other plan or
// released the lock.
func TestLockStore_AcquireIfPendingPlanID_RefreshObservedPinReplaced(t *testing.T) {
	clearTables(t)
	ctx := t.Context()

	db, err := sql.Open("block-mysql", testDSNChangedRows)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	require.NoError(t, db.PingContext(ctx))
	store := &lockStore{db: newRebindDB(db, MySQLDialect{}), dialect: MySQLDialect{}, identity: MySQLDialect{}, classifier: NewMySQLErrorClassifier()}

	require.NoError(t, store.Acquire(ctx, &storage.Lock{
		DatabaseName:  "testdb",
		DatabaseType:  "vitess",
		Repository:    "org/repo",
		PullRequest:   123,
		Owner:         "org/repo#123",
		PendingPlanID: "plan-1",
	}))
	observed := "plan-1"
	wanted := &storage.Lock{
		DatabaseName:  "testdb",
		DatabaseType:  "vitess",
		Owner:         "org/repo#123",
		PendingPlanID: "plan-2",
	}
	asObserved := &storage.Lock{
		DatabaseName:  "testdb",
		DatabaseType:  "vitess",
		Owner:         "org/repo#123",
		PendingPlanID: "plan-1",
	}
	setStoredPin := func(pin string) {
		_, err := db.ExecContext(ctx, `
			UPDATE locks
			SET pending_plan_id = ?
			WHERE database_name = ? AND database_type = ? AND owner = ?
		`, pin, "testdb", "vitess", "org/repo#123")
		require.NoError(t, err)
	}

	// A concurrent same-owner caller already wrote plan-2: the conditional
	// UPDATE matches no row, but the lock carries the plan this caller wanted.
	setStoredPin("plan-2")
	require.NoError(t, store.refreshPendingConfirmation(ctx, wanted, asObserved, &observed))

	// A concurrent same-owner caller wrote plan-3 instead: the intent this
	// caller observed is gone and plan-3 must stay.
	setStoredPin("plan-3")
	require.ErrorIs(t, store.refreshPendingConfirmation(ctx, wanted, asObserved, &observed), storage.ErrLockIntentChanged)
	lock, err := store.Get(ctx, "testdb", "vitess")
	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, "plan-3", lock.PendingPlanID)

	// The lock is released instead: the intent this caller observed is gone
	// just the same, so the refresh reports the intent change, not a missing
	// lock, and re-creates nothing.
	require.NoError(t, store.ForceRelease(ctx, "testdb", "vitess"))
	require.ErrorIs(t, store.refreshPendingConfirmation(ctx, wanted, asObserved, &observed), storage.ErrLockIntentChanged)
	lock, err = store.Get(ctx, "testdb", "vitess")
	require.NoError(t, err)
	assert.Nil(t, lock, "the refused refresh must not re-create the released lock")
}

// When the lock changes hands between reading it and refreshing its confirmation
// plan, the refresh must not silently succeed: the caller no longer holds the lock
// and must learn the accurate cause (held by another owner, or gone entirely).
func TestLockStore_Acquire_RefreshOwnerNoLongerMatches(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := &lockStore{db: newRebindDB(testDB, MySQLDialect{}), dialect: MySQLDialect{}, identity: MySQLDialect{}, classifier: NewMySQLErrorClassifier()}

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
		}, nil)
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
		}, nil)
	require.ErrorIs(t, err, storage.ErrLockNotFound)
}

// A caller that observed an unpinned lock (empty observed pin) has no pin to
// have lost, so a lock released before its write is not an intent change: the
// refresh reports the lock gone, as it does for an unconditional acquire.
func TestLockStore_RefreshObservedUnpinnedLockReleased(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := &lockStore{db: newRebindDB(testDB, MySQLDialect{}), dialect: MySQLDialect{}, identity: MySQLDialect{}, classifier: NewMySQLErrorClassifier()}

	require.NoError(t, store.Acquire(ctx, &storage.Lock{
		DatabaseName: "testdb", DatabaseType: "vitess", Repository: "org/repo", PullRequest: 123, Owner: "org/repo#123",
	}))
	observed := ""
	wanted := &storage.Lock{DatabaseName: "testdb", DatabaseType: "vitess", Owner: "org/repo#123", PendingPlanID: "plan-1"}
	asObserved := &storage.Lock{DatabaseName: "testdb", DatabaseType: "vitess", Owner: "org/repo#123"}

	require.NoError(t, store.ForceRelease(ctx, "testdb", "vitess"))
	err := store.refreshPendingConfirmation(ctx, wanted, asObserved, &observed)
	require.ErrorIs(t, err, storage.ErrLockNotFound)
	require.NotErrorIs(t, err, storage.ErrLockIntentChanged)
}

// releaseOnRefreshBinder releases the lock the moment the acquire's refresh
// UPDATE is bound, so the release lands between the acquire's read of the
// owner's lock and its write: the window in which the lock the caller read
// is gone by the time it writes.
type releaseOnRefreshBinder struct {
	t        *testing.T
	store    *lockStore
	released int
}

func (b *releaseOnRefreshBinder) Rebind(query string) string {
	if strings.Contains(query, "SET pending_plan_id") && b.released == 0 {
		b.released++
		require.NoError(b.t, b.store.ForceRelease(b.t.Context(), "testdb", "vitess"))
	}
	return MySQLDialect{}.Rebind(query)
}

// An acquire that observed no pin, or none at all, reads the owner's own
// unpinned lock and finds it released by the time it writes. A free key is the
// state the caller asked for, so the acquire claims it instead of failing: the
// lock exists again under the caller's owner and pin, created by this acquire.
func TestLockStore_Acquire_LockReleasedBetweenReadAndWriteIsClaimed(t *testing.T) {
	cases := []struct {
		name    string
		acquire func(ctx context.Context, store *lockStore, lock *storage.Lock) error
	}{
		{
			name: "unconditional acquire",
			acquire: func(ctx context.Context, store *lockStore, lock *storage.Lock) error {
				return store.Acquire(ctx, lock)
			},
		},
		{
			name: "acquire that observed no pin",
			acquire: func(ctx context.Context, store *lockStore, lock *storage.Lock) error {
				return store.AcquireIfPendingPlanID(ctx, lock, "")
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearTables(t)
			ctx := t.Context()
			releaser := &lockStore{db: newRebindDB(testDB, MySQLDialect{}), dialect: MySQLDialect{}, identity: MySQLDialect{}, classifier: NewMySQLErrorClassifier()}
			binder := &releaseOnRefreshBinder{t: t, store: releaser}
			store := &lockStore{db: newRebindDB(testDB, binder), dialect: MySQLDialect{}, identity: MySQLDialect{}, classifier: NewMySQLErrorClassifier()}

			require.NoError(t, releaser.Acquire(ctx, &storage.Lock{
				DatabaseName: "testdb", DatabaseType: "vitess", Repository: "org/repo", PullRequest: 123, Owner: "org/repo#123",
			}))
			lock := &storage.Lock{
				DatabaseName: "testdb", DatabaseType: "vitess", Repository: "org/repo", PullRequest: 123, Owner: "org/repo#123",
				PendingPlanID: "plan-1",
			}

			require.NoError(t, tc.acquire(ctx, store, lock))

			assert.Equal(t, 1, binder.released, "the release landed between the acquire's read and its write")
			assert.NotZero(t, lock.ID, "the acquire created the lock row")
			current, err := store.Get(ctx, "testdb", "vitess")
			require.NoError(t, err)
			require.NotNil(t, current)
			assert.Equal(t, lock.ID, current.ID)
			assert.Equal(t, "org/repo#123", current.Owner)
			assert.Equal(t, "plan-1", current.PendingPlanID)
		})
	}
}

// insertRaceBinder makes another owner win the acquire's INSERT and then
// release the lock before the acquire re-reads it. For each of the first
// races INSERTs the rival claims the key just before the INSERT runs, so the
// INSERT hits a duplicate key, and releases it just before the next read, so
// the re-read finds the key free.
type insertRaceBinder struct {
	t           *testing.T
	next        binder
	rival       *lockStore
	races       int
	inserts     int
	releaseNext bool
}

func (b *insertRaceBinder) Rebind(query string) string {
	switch {
	case strings.Contains(query, "INSERT INTO locks"):
		b.inserts++
		if b.inserts <= b.races {
			require.NoError(b.t, b.rival.Acquire(b.t.Context(), &storage.Lock{
				DatabaseName: "testdb", DatabaseType: "vitess", Repository: "org/other", PullRequest: 9, Owner: "org/other#9",
			}))
			b.releaseNext = true
		}
	case b.releaseNext && strings.Contains(query, "FROM locks"):
		b.releaseNext = false
		require.NoError(b.t, b.rival.ForceRelease(b.t.Context(), "testdb", "vitess"))
	}
	return b.next.Rebind(query)
}

// TestLockStore_Acquire_InsertRaceReleasedBeforeReread runs the insert-race
// scenarios against MySQL; TestPostgresStorageParity runs the same scenarios
// against PostgreSQL.
func TestLockStore_Acquire_InsertRaceReleasedBeforeReread(t *testing.T) {
	testLockAcquireInsertRaceReleasedBeforeReread(t, func(t *testing.T, b func(binder) binder) *lockStore {
		t.Helper()
		return &lockStore{db: newRebindDB(testDB, b(MySQLDialect{})), dialect: MySQLDialect{}, identity: MySQLDialect{}, classifier: NewMySQLErrorClassifier()}
	}, clearTables)
}

// Another owner wins the acquire's INSERT and releases the lock before the
// acquire re-reads it, so the key the acquire lost is free again. A caller
// that observed no pin, or none at all, asked for a free key: it inserts once
// more and holds the lock. When the retried INSERT loses the same way, the
// acquire reports the lock held rather than racing again. A caller that
// observed a pin never reaches the INSERT: a free key means the pin it planned
// against is gone, so it learns of the intent change and creates nothing.
func testLockAcquireInsertRaceReleasedBeforeReread(t *testing.T, newStore func(t *testing.T, wrap func(binder) binder) *lockStore, clear func(t *testing.T)) {
	newLock := func() *storage.Lock {
		return &storage.Lock{
			DatabaseName: "testdb", DatabaseType: "vitess", Repository: "org/repo", PullRequest: 123, Owner: "org/repo#123",
			PendingPlanID: "plan-1",
		}
	}
	unpinned := []struct {
		name    string
		acquire func(ctx context.Context, store *lockStore, lock *storage.Lock) error
	}{
		{
			name: "unconditional acquire",
			acquire: func(ctx context.Context, store *lockStore, lock *storage.Lock) error {
				return store.Acquire(ctx, lock)
			},
		},
		{
			name: "acquire that observed no pin",
			acquire: func(ctx context.Context, store *lockStore, lock *storage.Lock) error {
				return store.AcquireIfPendingPlanID(ctx, lock, "")
			},
		},
	}
	setup := func(t *testing.T, races int) (*lockStore, *insertRaceBinder) {
		t.Helper()
		clear(t)
		rival := newStore(t, func(b binder) binder { return b })
		var race *insertRaceBinder
		store := newStore(t, func(b binder) binder {
			race = &insertRaceBinder{t: t, next: b, rival: rival, races: races}
			return race
		})
		return store, race
	}

	for _, tc := range unpinned {
		t.Run(tc.name+" claims the released key", func(t *testing.T) {
			ctx := t.Context()
			store, race := setup(t, 1)
			lock := newLock()

			require.NoError(t, tc.acquire(ctx, store, lock))

			assert.Equal(t, 2, race.inserts, "the acquire inserted once more after the rival released the key")
			assert.NotZero(t, lock.ID, "the retried insert created the lock row")
			current, err := store.Get(ctx, "testdb", "vitess")
			require.NoError(t, err)
			require.NotNil(t, current)
			assert.Equal(t, lock.ID, current.ID)
			assert.Equal(t, "org/repo#123", current.Owner)
			assert.Equal(t, "plan-1", current.PendingPlanID)
		})

		t.Run(tc.name+" retries the insert only once", func(t *testing.T) {
			ctx := t.Context()
			store, race := setup(t, 2)
			lock := newLock()

			require.ErrorIs(t, tc.acquire(ctx, store, lock), storage.ErrLockHeld)

			assert.Equal(t, 2, race.inserts, "the acquire gave up after one retried insert")
			assert.Zero(t, lock.ID)
			current, err := store.Get(ctx, "testdb", "vitess")
			require.NoError(t, err)
			assert.Nil(t, current, "the acquire created no lock row")
		})
	}

	t.Run("acquire that observed a pin creates nothing", func(t *testing.T) {
		ctx := t.Context()
		store, race := setup(t, 1)
		lock := newLock()

		require.ErrorIs(t, store.AcquireIfPendingPlanID(ctx, lock, "plan-0"), storage.ErrLockIntentChanged)

		assert.Zero(t, race.inserts, "a caller that observed a pin never inserts")
		assert.Zero(t, lock.ID)
		current, err := store.Get(ctx, "testdb", "vitess")
		require.NoError(t, err)
		assert.Nil(t, current, "the acquire created no lock row")
	})
}

// insertRetryBinder makes another owner win the acquire's first INSERT and
// release the key before the re-read, so the acquire retries the INSERT, and
// then decides the retried INSERT's fate: retryWinner claims the key just
// before it runs and keeps it, or, when failRetry is set, the statement is
// rewritten to target a table that does not exist so it fails outright.
type insertRetryBinder struct {
	t           *testing.T
	next        binder
	rival       *lockStore
	retryWinner *storage.Lock
	failRetry   bool
	inserts     int
	releaseNext bool
}

func (b *insertRetryBinder) Rebind(query string) string {
	switch {
	case strings.Contains(query, "INSERT INTO locks"):
		b.inserts++
		switch b.inserts {
		case 1:
			require.NoError(b.t, b.rival.Acquire(b.t.Context(), &storage.Lock{
				DatabaseName: "testdb", DatabaseType: "vitess", Repository: "org/other", PullRequest: 9, Owner: "org/other#9",
			}))
			b.releaseNext = true
		case 2:
			if b.failRetry {
				query = strings.Replace(query, "INSERT INTO locks", "INSERT INTO locks_missing", 1)
			}
			if b.retryWinner != nil {
				require.NoError(b.t, b.rival.Acquire(b.t.Context(), b.retryWinner))
			}
		}
	case b.releaseNext && strings.Contains(query, "FROM locks"):
		b.releaseNext = false
		require.NoError(b.t, b.rival.ForceRelease(b.t.Context(), "testdb", "vitess"))
	}
	return b.next.Rebind(query)
}

// TestLockStore_Acquire_InsertRetryOutcomes runs the retried-insert scenarios
// against MySQL; TestPostgresStorageParity runs the same scenarios against
// PostgreSQL.
func TestLockStore_Acquire_InsertRetryOutcomes(t *testing.T) {
	testLockAcquireInsertRetryOutcomes(t, func(t *testing.T, b func(binder) binder) *lockStore {
		t.Helper()
		return &lockStore{db: newRebindDB(testDB, b(MySQLDialect{})), dialect: MySQLDialect{}, identity: MySQLDialect{}, classifier: NewMySQLErrorClassifier()}
	}, clearTables)
}

// The acquire's first INSERT loses to another owner who releases the key
// before the re-read, so the acquire inserts once more. That retried INSERT
// can lose again to a rival that keeps the key: another owner means the lock
// is held and the acquire creates nothing, while the same owner means the
// acquire holds the lock through the rival's row and records its intent on
// it. A retried INSERT that fails for any reason other than a duplicate key
// surfaces that failure instead of reporting the lock held or acquired.
func testLockAcquireInsertRetryOutcomes(t *testing.T, newStore func(t *testing.T, wrap func(binder) binder) *lockStore, clear func(t *testing.T)) {
	newLock := func() *storage.Lock {
		return &storage.Lock{
			DatabaseName: "testdb", DatabaseType: "vitess", Repository: "org/repo", PullRequest: 123, Owner: "org/repo#123",
			PendingPlanID: "plan-1",
		}
	}
	setup := func(t *testing.T, configure func(b *insertRetryBinder)) (*lockStore, *insertRetryBinder) {
		t.Helper()
		clear(t)
		rival := newStore(t, func(b binder) binder { return b })
		var race *insertRetryBinder
		store := newStore(t, func(b binder) binder {
			race = &insertRetryBinder{t: t, next: b, rival: rival}
			configure(race)
			return race
		})
		return store, race
	}

	// An acquire that carries no intent has nothing to write onto the winner's
	// row, so the owner comparison alone decides it; one that carries an
	// intent is also decided by the owner-scoped refresh finding no row.
	for _, pendingPlanID := range []string{"", "plan-1"} {
		t.Run("retried insert loses to another owner who keeps the key, intent "+strconv.Quote(pendingPlanID), func(t *testing.T) {
			ctx := t.Context()
			store, race := setup(t, func(b *insertRetryBinder) {
				b.retryWinner = &storage.Lock{DatabaseName: "testdb", DatabaseType: "vitess", Repository: "org/third", PullRequest: 7, Owner: "org/third#7"}
			})
			lock := newLock()
			lock.PendingPlanID = pendingPlanID

			require.ErrorIs(t, store.Acquire(ctx, lock), storage.ErrLockHeld)

			assert.Equal(t, 2, race.inserts, "the acquire gave up after one retried insert")
			assert.Zero(t, lock.ID, "the acquire created no lock row")
			current, err := store.Get(ctx, "testdb", "vitess")
			require.NoError(t, err)
			require.NotNil(t, current, "the rival's row survives the acquire")
			assert.Equal(t, "org/third#7", current.Owner)
			assert.Empty(t, current.PendingPlanID, "the acquire wrote nothing onto another owner's row")
		})
	}

	t.Run("retried insert loses to the same owner who keeps the key", func(t *testing.T) {
		ctx := t.Context()
		store, race := setup(t, func(b *insertRetryBinder) {
			b.retryWinner = &storage.Lock{DatabaseName: "testdb", DatabaseType: "vitess", Repository: "org/repo", PullRequest: 123, Owner: "org/repo#123"}
		})
		lock := newLock()

		require.NoError(t, store.Acquire(ctx, lock))

		assert.Equal(t, 2, race.inserts, "the acquire found its own row after one retried insert")
		assert.Zero(t, lock.ID, "the acquire created no row of its own")
		current, err := store.Get(ctx, "testdb", "vitess")
		require.NoError(t, err)
		require.NotNil(t, current)
		assert.Equal(t, "org/repo#123", current.Owner)
		assert.Equal(t, "plan-1", current.PendingPlanID, "the acquire recorded its intent on the same owner's row")
	})

	t.Run("retried insert failure is returned", func(t *testing.T) {
		ctx := t.Context()
		store, race := setup(t, func(b *insertRetryBinder) { b.failRetry = true })
		lock := newLock()

		err := store.Acquire(ctx, lock)

		require.Error(t, err)
		assert.NotErrorIs(t, err, storage.ErrLockHeld, "a failed insert is not a held lock")
		assert.ErrorContains(t, err, "insert lock for testdb/vitess owner=org/repo#123")
		assert.Equal(t, 2, race.inserts, "the acquire stopped at the failed retried insert")
		assert.Zero(t, lock.ID, "a failed insert never records a row id")
		current, getErr := store.Get(ctx, "testdb", "vitess")
		require.NoError(t, getErr)
		assert.Nil(t, current, "the acquire created no lock row")
	})
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

	store := &lockStore{db: newRebindDB(db, MySQLDialect{}), dialect: MySQLDialect{}, identity: MySQLDialect{}, classifier: NewMySQLErrorClassifier()}

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
