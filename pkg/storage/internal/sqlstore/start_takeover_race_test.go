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
	"github.com/block/schemabot/pkg/storage/storagetest"
)

// startTakeoverRaceDeadline bounds each wait in the start/takeover race: a
// session reaching the lock it is meant to wait on, and each side returning
// once the race is let go.
const startTakeoverRaceDeadline = 30 * time.Second

// advisoryLockWaiter reports whether a session is waiting on an advisory lock.
type advisoryLockWaiter func(t *testing.T) bool

// mysqlAdvisoryLockWaiter reports a session still running GET_LOCK, which only
// stays in flight while another session holds the lock it asked for.
func mysqlAdvisoryLockWaiter(db *sql.DB) advisoryLockWaiter {
	return func(t *testing.T) bool {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), leaseFenceProbeDeadline)
		defer cancel()
		var waiting int
		require.NoError(t, db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM performance_schema.threads
			WHERE PROCESSLIST_INFO LIKE 'SELECT GET_LOCK%'
		`).Scan(&waiting))
		return waiting > 0
	}
}

// postgresAdvisoryLockWaiter reports a backend waiting on an advisory lock.
func postgresAdvisoryLockWaiter(db *sql.DB) advisoryLockWaiter {
	return func(t *testing.T) bool {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), leaseFenceProbeDeadline)
		defer cancel()
		var waiting int
		require.NoError(t, db.QueryRowContext(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock' AND wait_event = 'advisory'
		`).Scan(&waiting))
		return waiting > 0
	}
}

// TestStoppedApplyStartRacingItsTakeover runs the start/takeover race against
// MySQL; TestPostgresStorageParity runs the same scenario against PostgreSQL.
func TestStoppedApplyStartRacingItsTakeover(t *testing.T) {
	testStoppedApplyStartRacingItsTakeover(t, func(t *testing.T) *Storage {
		t.Helper()
		clearTables(t)
		return NewMySQL(testDB)
	}, mysqlLockWaiter(testDB), mysqlAdvisoryLockWaiter(testDB))
}

// testStoppedApplyStartRacingItsTakeover pins the lock order between a create
// that takes over a stopped apply and an operator's start of that same apply.
// The create holds the apply target lock and is held up just before it stamps
// the stopped apply, while the start's claim is made and waits for that target
// lock. Once the create is let go it stamps the stopped apply and commits, and
// the start's claim, made under the lock the create released, refuses the start
// with the successor named. Neither waits on the other: a claim that held the
// stopped apply's row while waiting for the target lock would block the create's
// stamp until the lock wait gave up.
func testStoppedApplyStartRacingItsTakeover(t *testing.T, newStore func(t *testing.T) *Storage, rowWaiting lockWaiter, advisoryWaiting advisoryLockWaiter) {
	ctx := t.Context()
	store := newStore(t)
	if limit := store.db.pool.Stats().MaxOpenConnections; limit != 0 && limit < 5 {
		require.FailNow(t, "the store's pool cannot hold the lock-row hold, the create, the claim and its target lock, and the probe at once", "MaxOpenConnections = %d", limit)
	}
	lock := storagetest.CreateLock(t, store, "start_takeover_race_db", storage.DatabaseTypeMySQL)
	holder := storagetest.CreateApplyWithStateAndEnv(t, store, lock, "apply_start_takeover_holder", 9601, state.Apply.Stopped, "staging")
	_, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:   holder.ID,
		Operation: storage.ControlOperationStart,
		Status:    storage.ControlRequestPending,
		Metadata:  []byte(`{}`),
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)

	// The create verifies the lock's apply intent under the target lock and
	// before it stamps the stopped apply, so holding the lock row keeps the
	// create inside the target lock until the test lets it go.
	lockRowHold, err := store.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := lockRowHold.Rollback()
		if errors.Is(err, sql.ErrTxDone) || errors.Is(err, context.Canceled) {
			return
		}
		assert.NoError(t, err, "roll back the lock row hold")
	})
	var lockID int64
	require.NoError(t, lockRowHold.QueryRowContext(ctx, `SELECT id FROM locks WHERE id = ? FOR UPDATE`, lock.ID).Scan(&lockID))

	successor := &storage.Apply{
		ApplyIdentifier:       "apply_start_takeover_successor",
		LockID:                lock.ID,
		PlanID:                9602,
		Database:              lock.DatabaseName,
		DatabaseType:          lock.DatabaseType,
		Repository:            lock.Repository,
		PullRequest:           lock.PullRequest,
		Environment:           "staging",
		Deployment:            "default",
		Engine:                storage.EngineForType(lock.DatabaseType),
		State:                 state.Apply.Pending,
		ExpectedLockOwner:     lock.Owner,
		ExpectedPendingPlanID: lock.PendingPlanID,
		TakesOverApplyIDs:     []int64{holder.ID},
	}
	successorTask := attachRaceTask(successor, "task_apply_start_takeover_successor")
	successorOp := &storage.ApplyOperation{Deployment: "default", OperationKey: "default", State: state.ApplyOperation.Pending}
	createErr := make(chan error, 1)
	go func() {
		_, err := store.Applies().CreateWithTasksAndOperations(ctx, successor, []*storage.Task{successorTask}, []*storage.ApplyOperation{successorOp})
		createErr <- err
	}()
	waitForStartTakeoverRace(t, createErr, "the create never waited on the lock row inside the target lock",
		func() bool { return rowWaiting(t, "FROM locks") })

	type claimResult struct {
		apply *storage.Apply
		err   error
	}
	claimed := make(chan claimResult, 1)
	go func() {
		apply, err := store.Applies().ClaimApplyByID(ctx, holder.ID, "driver-a")
		claimed <- claimResult{apply: apply, err: err}
	}()
	waitForStartTakeoverRace(t, nil, "the start's claim never waited on the apply target lock",
		func() bool { return advisoryWaiting(t) })

	require.NoError(t, lockRowHold.Commit())

	select {
	case err := <-createErr:
		require.NoError(t, err, "the create stamps the stopped apply without waiting on the start's claim")
	case <-time.After(startTakeoverRaceDeadline):
		require.FailNow(t, "the create did not return after the lock row was released")
	}
	select {
	case result := <-claimed:
		require.NoError(t, result.err, "the start's claim is made under the target lock the create released")
		assert.Nil(t, result.apply, "a stopped apply whose work the create took over must not be claimed for a start")
	case <-time.After(startTakeoverRaceDeadline):
		require.FailNow(t, "the start's claim did not return after the create committed")
	}

	stored, err := store.Applies().Get(ctx, holder.ID)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, successor.ApplyIdentifier, stored.SupersededBy, "the create stamped the stopped apply with its successor")
	assert.Equal(t, state.Apply.Stopped, stored.State, "the refused start leaves the stopped apply stopped")

	settled, err := store.ControlRequests().GetByOperation(ctx, holder.ID, storage.ControlOperationStart)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, storage.ControlRequestFailed, settled.Status)
	assert.Contains(t, settled.ErrorMessage, "superseded by "+successor.ApplyIdentifier,
		"the start is refused because of the marker, naming the successor")
}

// waitForStartTakeoverRace polls until reached reports true, failing the test
// if done returns first or the deadline passes.
func waitForStartTakeoverRace(t *testing.T, done <-chan error, failure string, reached func() bool) {
	t.Helper()
	deadline := time.Now().Add(startTakeoverRaceDeadline)
	for !reached() {
		select {
		case err := <-done:
			require.FailNow(t, failure, "returned before reaching the wait: %v", err)
		default:
		}
		require.True(t, time.Now().Before(deadline), failure)
		time.Sleep(10 * time.Millisecond)
	}
}
