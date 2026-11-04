//go:build integration

package sqlstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/testutil"
)

func TestMySQLDeferredCutoverQueue(t *testing.T) {
	testDeferredCutoverQueue(t, func(t *testing.T) *Storage {
		clearTables(t)
		return NewMySQL(testDB)
	})
}

func TestPostgresDeferredCutoverQueue(t *testing.T) {
	_, db := testutil.StartPostgres(t, "deferred_cutover_queue")
	applyPostgresTestSchema(t, db)
	testDeferredCutoverQueue(t, func(t *testing.T) *Storage {
		clearPostgresTables(t, db)
		return NewPostgres(db)
	})
}

// testDeferredCutoverQueue covers the order deferred applies take drivers in,
// on both dialects. Each case starts from apply A, which defers its cutover:
// one shard is waiting for its cutover on a driver and the other failed and is
// waiting to retry. A later apply B has a shard ready to start.
func testDeferredCutoverQueue(t *testing.T, newStore func(*testing.T) *Storage) {
	// B defers its cutover too. If B's shard took the driver A's failed shard
	// released, each apply could end up holding part of its shards with the
	// rest waiting on the other, and neither could be cut over. B waits until
	// A has every shard running.
	t.Run("a later deferred apply waits for an earlier one to start every shard", func(t *testing.T) {
		ctx := t.Context()
		store := newStore(t)
		applyA := seedHalfStartedDeferredApply(t, store, "apply_queue_first")
		applyB := createQueueApply(t, store, "apply_queue_second", state.Apply.Pending, true, time.Minute)
		insertQueueOperation(t, store, applyB, "shard-1", state.ApplyOperation.Pending, false)

		claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
		require.NoError(t, err)
		assert.Nil(t, claimed, "B's shard waits while A still has a shard to start")

		_, err = store.db.ExecContext(ctx, "UPDATE apply_operations SET state = ? WHERE apply_id = ? AND state = ?",
			state.ApplyOperation.Running, applyA.ID, state.ApplyOperation.FailedRetryable)
		require.NoError(t, err)
		claimed, err = store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
		require.NoError(t, err)
		require.NotNil(t, claimed, "B's shard starts once A has every shard running")
		assert.Equal(t, applyB.ID, claimed.ApplyID)
	})

	// An apply that does not defer its cutover never keeps a driver waiting on
	// a person, so it takes drivers as they free up.
	t.Run("an apply that does not defer its cutover is not queued", func(t *testing.T) {
		ctx := t.Context()
		store := newStore(t)
		seedHalfStartedDeferredApply(t, store, "apply_queue_first")
		applyB := createQueueApply(t, store, "apply_queue_plain", state.Apply.Pending, false, time.Minute)
		insertQueueOperation(t, store, applyB, "shard-1", state.ApplyOperation.Pending, false)

		claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
		require.NoError(t, err)
		require.NotNil(t, claimed)
		assert.Equal(t, applyB.ID, claimed.ApplyID)
	})

	// A stopped apply holds no driver and starts nothing until someone starts
	// it again, so it does not hold B back.
	t.Run("a stopped apply holds no place", func(t *testing.T) {
		ctx := t.Context()
		store := newStore(t)
		applyA := seedHalfStartedDeferredApply(t, store, "apply_queue_first")
		stopQueueApply(t, store, applyA)
		applyB := createQueueApply(t, store, "apply_queue_second", state.Apply.Pending, true, time.Minute)
		insertQueueOperation(t, store, applyB, "shard-1", state.ApplyOperation.Pending, false)

		claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
		require.NoError(t, err)
		require.NotNil(t, claimed)
		assert.Equal(t, applyB.ID, claimed.ApplyID)
	})

	// A was stopped, B started while it was, and then A is started again. A
	// released its drivers when it stopped and B may hold them now, so A goes
	// behind B: B's next shard starts ahead of A's.
	t.Run("a resumed apply queues behind the applies that started while it was stopped", func(t *testing.T) {
		ctx := t.Context()
		store := newStore(t)
		applyA := seedHalfStartedDeferredApply(t, store, "apply_queue_first")
		stopQueueApply(t, store, applyA)
		applyB := createQueueApply(t, store, "apply_queue_second", state.Apply.Running, true, time.Minute)
		insertQueueOperation(t, store, applyB, "shard-1", state.ApplyOperation.WaitingForCutover, true)
		shardB := insertQueueOperation(t, store, applyB, "shard-2", state.ApplyOperation.Pending, false)

		_, _, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
			ApplyID: applyA.ID, Operation: storage.ControlOperationStart, RequestedBy: "operator",
		})
		require.NoError(t, err)
		resumed, err := store.Applies().Get(ctx, applyA.ID)
		require.NoError(t, err)
		require.NotNil(t, resumed)
		assert.Equal(t, applyA.UpdatedAt.Unix(), resumed.UpdatedAt.Unix(), "recording the start leaves A's heartbeat alone")

		claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
		require.NoError(t, err)
		require.NotNil(t, claimed)
		assert.Equal(t, shardB, claimed.ID, "B, which started while A was stopped, is ahead of A")
	})
}

// seedHalfStartedDeferredApply creates apply A, created two minutes ago and
// deferring its cutover: one shard is waiting for its cutover on a driver, and
// the other failed and waits to retry, so neither is claimable now.
func seedHalfStartedDeferredApply(t *testing.T, store *Storage, name string) *storage.Apply {
	t.Helper()
	apply := createQueueApply(t, store, name, state.Apply.Running, true, 2*time.Minute)
	insertQueueOperation(t, store, apply, "shard-1", state.ApplyOperation.WaitingForCutover, true)
	insertQueueOperation(t, store, apply, "shard-2", state.ApplyOperation.FailedRetryable, false)
	return apply
}

// createQueueApply creates an apply in the given state, deferring its cutover
// or not, created age ago, and returns it as stored.
func createQueueApply(t *testing.T, store *Storage, name, applyState string, deferCutover bool, age time.Duration) *storage.Apply {
	t.Helper()
	ctx := t.Context()
	lock := createTestLock(t, store, name+"_db", storage.DatabaseTypeMySQL)
	id, err := store.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: name,
		LockID:          lock.ID,
		PlanID:          1,
		Database:        lock.DatabaseName,
		DatabaseType:    lock.DatabaseType,
		Repository:      lock.Repository,
		PullRequest:     lock.PullRequest,
		Environment:     "production",
		Engine:          storage.EngineForType(lock.DatabaseType),
		State:           applyState,
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: deferCutover}),
	})
	require.NoError(t, err)
	_, err = store.db.ExecContext(ctx, "UPDATE applies SET created_at = "+agedHeartbeatExpr(store, age)+" WHERE id = ?", id)
	require.NoError(t, err)
	apply, err := store.Applies().Get(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, apply)
	return apply
}

// insertQueueOperation adds a work operation for one shard of the apply's
// single rollout member, in the given state and leased to a live driver when
// leased is set.
func insertQueueOperation(t *testing.T, store *Storage, apply *storage.Apply, shard, opState string, leased bool) int64 {
	t.Helper()
	ctx := t.Context()
	id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", OperationKey: shard, State: opState,
	})
	require.NoError(t, err)
	if leased {
		_, err = store.db.ExecContext(ctx,
			"UPDATE apply_operations SET lease_owner = 'holder', lease_token = 'holder-token', lease_acquired_at = NOW(), updated_at = NOW() WHERE id = ?", id)
		require.NoError(t, err)
	}
	return id
}

// stopQueueApply stops the apply and every operation it has, releasing their
// drivers the way a stop does.
func stopQueueApply(t *testing.T, store *Storage, apply *storage.Apply) {
	t.Helper()
	ctx := t.Context()
	_, err := store.db.ExecContext(ctx, "UPDATE applies SET state = ? WHERE id = ?", state.Apply.Stopped, apply.ID)
	require.NoError(t, err)
	_, err = store.db.ExecContext(ctx,
		"UPDATE apply_operations SET state = ?, lease_owner = '', lease_token = '' WHERE apply_id = ?",
		state.ApplyOperation.Stopped, apply.ID)
	require.NoError(t, err)
}
