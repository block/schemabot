//go:build integration

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	require.Eventually(t, func() bool { return rowWaiting(t, "SELECT id FROM locks") },
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
