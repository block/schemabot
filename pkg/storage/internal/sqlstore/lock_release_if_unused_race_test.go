//go:build integration

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/namedlock"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// TestLockStore_ReleaseByIDIfUnusedWaitsForApplyCreate runs the release/apply
// create race against MySQL; TestPostgresStorageParity runs the same scenario
// against PostgreSQL.
func TestLockStore_ReleaseByIDIfUnusedWaitsForApplyCreate(t *testing.T) {
	testReleaseByIDIfUnusedWaitsForApplyCreate(t, func(t *testing.T) *Storage {
		t.Helper()
		clearTables(t)
		return NewMySQL(testDB)
	}, mysqlLockWaiter(testDB))
}

// testReleaseByIDIfUnusedWaitsForApplyCreate pins the serialization between a
// lock release and an apply created under that lock. The apply create has
// locked the lock row to verify its lock intent and written its apply, but has
// not committed. The release waits on the lock row, and once the create
// commits it sees the apply and keeps the lock instead of deleting it from
// under a running apply.
func testReleaseByIDIfUnusedWaitsForApplyCreate(t *testing.T, newStore func(t *testing.T) *Storage, rowWaiting lockWaiter) {
	// The bounded waits below do not cover a database call that stalls, so
	// every call shares the race deadline too.
	ctx, cancel := context.WithTimeout(t.Context(), startTakeoverRaceDeadline)
	defer cancel()
	store := newStore(t)
	lock := &storage.Lock{
		DatabaseName:  "release_race_db",
		DatabaseType:  storage.DatabaseTypeMySQL,
		Repository:    "org/repo",
		PullRequest:   42,
		Owner:         "org/repo#42",
		PendingPlanID: "plan-1",
	}
	require.NoError(t, store.Locks().Acquire(ctx, lock))
	held, err := store.Locks().Get(ctx, "release_race_db", storage.DatabaseTypeMySQL)
	require.NoError(t, err)
	require.NotNil(t, held)

	create, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := create.Rollback()
		if errors.Is(err, sql.ErrTxDone) || errors.Is(err, context.Canceled) {
			return
		}
		assert.NoError(t, err, "roll back the apply create")
	})
	apply := &storage.Apply{
		ApplyIdentifier:       "apply_release_race",
		Database:              "release_race_db",
		DatabaseType:          storage.DatabaseTypeMySQL,
		ExpectedLockOwner:     "org/repo#42",
		ExpectedPendingPlanID: "plan-1",
	}
	require.NoError(t, verifyExpectedLockIntent(ctx, create, apply), "the apply create locks the lock row")
	require.Equal(t, held.ID, apply.LockID)
	_, err = create.ExecContext(ctx, `
		INSERT INTO applies (
			apply_identifier, lock_id, plan_id, database_name, database_type,
			repository, pull_request, environment, engine, state, error_message, options
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, apply.ApplyIdentifier, apply.LockID, 1, apply.Database, apply.DatabaseType,
		"org/repo", 42, "staging", storage.EngineForType(apply.DatabaseType), state.Apply.Pending, "", "{}")
	require.NoError(t, err)

	released := make(chan error, 1)
	go func() {
		released <- store.Locks().ReleaseByIDIfUnused(ctx, held.ID, "release_race_db", storage.DatabaseTypeMySQL, "org/repo#42", "plan-1")
	}()
	require.Eventually(t, func() bool { return rowWaiting(t, "FROM locks") },
		startTakeoverRaceDeadline, 20*time.Millisecond, "the release waits on the lock row the apply create holds")
	select {
	case err := <-released:
		require.FailNow(t, "the release decided while an apply create held the lock row", "err = %v", err)
	default:
	}

	require.NoError(t, create.Commit())
	select {
	case err := <-released:
		var inUse *storage.LockInUseError
		require.ErrorAs(t, err, &inUse, "the release sees the apply committed while it waited")
		assert.Equal(t, "apply_release_race", inUse.Apply.ApplyIdentifier)
		assert.Equal(t, held.ID, inUse.LockID)
	case <-time.After(startTakeoverRaceDeadline):
		require.FailNow(t, "the release did not finish after the apply create committed")
	}

	current, err := store.Locks().Get(ctx, "release_race_db", storage.DatabaseTypeMySQL)
	require.NoError(t, err)
	require.NotNil(t, current, "the lock the new apply records stays held")
	assert.Equal(t, held.ID, current.ID)
}

// TestLockStore_ApplyCreateQueuedBehindReleaseSeesLockIntentChanged runs the
// queued release/apply create race against MySQL; TestPostgresStorageParity
// runs the same scenario against PostgreSQL.
func TestLockStore_ApplyCreateQueuedBehindReleaseSeesLockIntentChanged(t *testing.T) {
	testApplyCreateQueuedBehindReleaseSeesLockIntentChanged(t, func(t *testing.T, classifier ErrorClassifier) *Storage {
		t.Helper()
		clearTables(t)
		dialect := MySQLDialect{}
		return NewWithDependencies(Dependencies{
			DB: testDB, Binder: dialect, Dialect: dialect, Identity: dialect,
			Locker: namedlock.MySQL{}, Classifier: classifier,
		})
	}, NewMySQLErrorClassifier(), mysqlLockWaiter(testDB), 0)
}

// conflictCountingClassifier counts the errors classified as retryable
// conflicts, so a test can tell an apply create that retried from one that
// did not.
type conflictCountingClassifier struct {
	ErrorClassifier
	conflicts atomic.Int32
}

func (c *conflictCountingClassifier) IsRetryableConflict(err error) bool {
	conflict := c.ErrorClassifier.IsRetryableConflict(err)
	if conflict {
		c.conflicts.Add(1)
	}
	return conflict
}

// testApplyCreateQueuedBehindReleaseSeesLockIntentChanged covers an unlock and
// an apply create that both arrive while another transaction holds the lock
// row. The release queues on the row first, then the create. Once the row is
// free the release deletes the lock, and the create refuses with
// ErrLockIntentChanged, which the PR comment answers with the line telling
// the operator to re-issue the command: the lock it planned against is gone.
// Neither side fails with a lock conflict, and nothing is left behind.
//
// wantCreateConflicts is how many retryable conflicts the create's own retry
// absorbs on the way. On MySQL the release and the create queue on the same
// index entry, so there are none. On PostgreSQL the create's first attempt
// took its snapshot before the delete committed and fails with a serialization
// failure, which a fresh attempt resolves.
func testApplyCreateQueuedBehindReleaseSeesLockIntentChanged(t *testing.T, newStore func(t *testing.T, classifier ErrorClassifier) *Storage, baseClassifier ErrorClassifier, rowWaiting lockWaiter, wantCreateConflicts int32) {
	// The bounded waits below do not cover a database call that stalls, so
	// every call shares the race deadline too.
	ctx, cancel := context.WithTimeout(t.Context(), startTakeoverRaceDeadline)
	defer cancel()
	classifier := &conflictCountingClassifier{ErrorClassifier: baseClassifier}
	store := newStore(t, classifier)
	lock := &storage.Lock{
		DatabaseName:  "queued_release_db",
		DatabaseType:  storage.DatabaseTypeMySQL,
		Repository:    "org/repo",
		PullRequest:   42,
		Owner:         "org/repo#42",
		PendingPlanID: "plan-1",
	}
	require.NoError(t, store.Locks().Acquire(ctx, lock))
	held, err := store.Locks().Get(ctx, "queued_release_db", storage.DatabaseTypeMySQL)
	require.NoError(t, err)
	require.NotNil(t, held)

	gate, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := gate.Rollback()
		if errors.Is(err, sql.ErrTxDone) || errors.Is(err, context.Canceled) {
			return
		}
		assert.NoError(t, err, "roll back the gate")
	})
	var gatedID int64
	require.NoError(t, gate.QueryRowContext(ctx, `SELECT id FROM locks WHERE id = ? FOR UPDATE`, held.ID).Scan(&gatedID))

	released := make(chan error, 1)
	go func() {
		released <- store.Locks().ReleaseByIDIfUnused(ctx, held.ID, "queued_release_db", storage.DatabaseTypeMySQL, "org/repo#42", "plan-1")
	}()
	// Only the release runs besides the gate, so any session waiting on the
	// locks table is the release.
	require.Eventually(t, func() bool { return rowWaiting(t, "FROM locks") },
		startTakeoverRaceDeadline, 20*time.Millisecond, "the release waits on the lock row the gate holds")

	conflictsBefore := classifier.conflicts.Load()
	created := make(chan error, 1)
	go func() {
		_, err := store.Applies().CreateWithTasks(ctx, &storage.Apply{
			ApplyIdentifier:       "apply_queued_release",
			LockID:                held.ID,
			PlanID:                1,
			Database:              "queued_release_db",
			DatabaseType:          storage.DatabaseTypeMySQL,
			Repository:            "org/repo",
			PullRequest:           42,
			Environment:           "staging",
			Engine:                storage.EngineForType(storage.DatabaseTypeMySQL),
			State:                 state.Apply.Pending,
			ExpectedLockOwner:     "org/repo#42",
			ExpectedPendingPlanID: "plan-1",
		}, nil)
		created <- err
	}()
	// The lock intent check is the create's only locks read that filters on
	// the owner after the database key.
	require.Eventually(t, func() bool { return rowWaiting(t, "WHERE database_name = % AND owner = ") },
		startTakeoverRaceDeadline, 20*time.Millisecond, "the apply create waits behind the release")
	select {
	case err := <-released:
		require.FailNow(t, "the release finished while the gate held the lock row", "err = %v", err)
	case err := <-created:
		require.FailNow(t, "the apply create finished while the gate held the lock row", "err = %v", err)
	default:
	}

	require.NoError(t, gate.Commit())
	select {
	case err := <-released:
		require.NoError(t, err, "the release deletes the lock once the gate commits")
	case <-time.After(startTakeoverRaceDeadline):
		require.FailNow(t, "the release did not finish after the gate committed")
	}
	select {
	case err := <-created:
		require.ErrorIs(t, err, storage.ErrLockIntentChanged, "the apply create finds the lock it planned against gone")
	case <-time.After(startTakeoverRaceDeadline):
		require.FailNow(t, "the apply create did not finish after the release committed")
	}
	assert.Equal(t, wantCreateConflicts, classifier.conflicts.Load()-conflictsBefore,
		"retryable conflicts the apply create absorbed")

	current, err := store.Locks().Get(ctx, "queued_release_db", storage.DatabaseTypeMySQL)
	require.NoError(t, err)
	assert.Nil(t, current, "the released lock stays released")
	var applies int
	require.NoError(t, store.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM applies WHERE apply_identifier = ?`, "apply_queued_release").Scan(&applies))
	assert.Zero(t, applies, "the refused apply create stores no apply")
}
