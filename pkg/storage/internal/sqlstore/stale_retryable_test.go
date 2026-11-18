//go:build integration

package sqlstore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/testutil"
)

func TestMySQLMarkStaleApplyRetryableLeases(t *testing.T) {
	testMarkStaleApplyRetryableLeases(t, func(t *testing.T) *Storage {
		clearTables(t)
		return NewMySQL(testDB)
	})
}

func TestPostgresMarkStaleApplyRetryableLeases(t *testing.T) {
	_, db := testutil.StartPostgres(t, "stale_retryable")
	applyPostgresTestSchema(t, db)
	testMarkStaleApplyRetryableLeases(t, func(t *testing.T) *Storage {
		clearPostgresTables(t, db)
		return NewPostgres(db)
	})
}

// The storage parity suite reaches a stale lease only through a release,
// which clears the token. A driver that crashed keeps its token on the row
// with a heartbeat that stopped, and a sibling operation can be driven while
// the parent's own heartbeat reads stale; both need the lease columns set
// directly.
func testMarkStaleApplyRetryableLeases(t *testing.T, newStore func(*testing.T) *Storage) {
	t.Run("a crashed driver's retained lease moves only for its own token", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		store := newStore(t)
		apply, opID := seedStaleRunningTree(t, ctx, store, "stale_retained")

		moved, err := store.Applies().MarkStaleApplyRetryable(ctx, storage.StaleRetryableRequest{
			ApplyID: apply.ID, ApplyLeaseToken: "token-apply-other",
			OperationID: opID, OperationLeaseToken: "token-op",
			ErrorMessage: "observed another lease",
		})
		require.NoError(t, err)
		assert.False(t, moved, "an apply token the row does not hold refuses the write")
		assertStaleRunningTree(t, ctx, store, apply.ID, opID)

		moved, err = store.Applies().MarkStaleApplyRetryable(ctx, storage.StaleRetryableRequest{
			ApplyID: apply.ID, ApplyLeaseToken: "token-apply",
			OperationID: opID, OperationLeaseToken: "token-op",
			ErrorMessage: "copy of users failed and may be retried",
		})
		require.NoError(t, err)
		require.True(t, moved)

		marked, err := store.Applies().Get(ctx, apply.ID)
		require.NoError(t, err)
		require.NotNil(t, marked)
		assert.Equal(t, state.Apply.FailedRetryable, marked.State)
		assert.Empty(t, marked.LeaseOwner)
		assert.Empty(t, marked.LeaseToken)
		assert.True(t, marked.UpdatedAt.After(time.Now().Add(-storage.ApplyLeaseStaleAfter)),
			"the transition stamps the heartbeat so the retryable claim's freshness window admits it")
		markedOp, err := store.ApplyOperations().Get(ctx, opID)
		require.NoError(t, err)
		require.NotNil(t, markedOp)
		assert.Equal(t, state.ApplyOperation.FailedRetryable, markedOp.State)
		assert.Empty(t, markedOp.LeaseOwner)
		assert.Empty(t, markedOp.LeaseToken)
	})

	// The named operation matches every guard and is written first, so the
	// refusal comes from the parent, and the operation write must not survive
	// it.
	t.Run("a sibling drive's fresh lease keeps the whole tree", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		store := newStore(t)
		apply, opID := seedStaleRunningTree(t, ctx, store, "stale_sibling")
		siblingID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: "region-b", OperationKey: "schema", State: state.ApplyOperation.Running,
		})
		require.NoError(t, err)
		_, err = store.db.ExecContext(ctx, `
			UPDATE apply_operations
			SET lease_owner = 'driver-live', lease_token = 'token-sibling', lease_acquired_at = NOW(), updated_at = NOW()
			WHERE id = ?
		`, siblingID)
		require.NoError(t, err)

		moved, err := store.Applies().MarkStaleApplyRetryable(ctx, storage.StaleRetryableRequest{
			ApplyID: apply.ID, ApplyLeaseToken: "token-apply",
			OperationID: opID, OperationLeaseToken: "token-op",
			ErrorMessage: "copy of users failed and may be retried",
		})
		require.NoError(t, err)
		assert.False(t, moved, "a parent is never moved under a live operation drive")
		assertStaleRunningTree(t, ctx, store, apply.ID, opID)
	})

	// A sibling claim that has locked its row but not yet committed a lease
	// still reads stale. Only the row lock shows that a drive is taking it,
	// so the parent must be refused while the lock is held, and the named
	// operation's write must not survive. Once the claim lets go, the same
	// request goes through.
	t.Run("a sibling row another transaction holds keeps the whole tree until it is released", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		store := newStore(t)
		apply, opID := seedStaleRunningTree(t, ctx, store, "stale_locked_sibling")
		siblingID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: "region-b", OperationKey: "schema", State: state.ApplyOperation.Running,
		})
		require.NoError(t, err)
		staleHeartbeat := agedHeartbeatExpr(store, 2*storage.ApplyLeaseStaleAfter)
		_, err = store.db.ExecContext(ctx, `
			UPDATE apply_operations
			SET lease_owner = '', lease_token = '', lease_acquired_at = NULL, updated_at = `+staleHeartbeat+`
			WHERE id = ?
		`, siblingID)
		require.NoError(t, err)

		claimTx, err := store.db.BeginTx(ctx, nil)
		require.NoError(t, err)
		claimOpen := true
		defer func() {
			if claimOpen {
				_ = claimTx.Rollback()
			}
		}()
		var lockedID int64
		require.NoError(t, claimTx.QueryRowContext(ctx, `SELECT id FROM apply_operations WHERE id = ? FOR UPDATE`, siblingID).Scan(&lockedID))

		req := storage.StaleRetryableRequest{
			ApplyID: apply.ID, ApplyLeaseToken: "token-apply",
			OperationID: opID, OperationLeaseToken: "token-op",
			ErrorMessage: "copy of users failed and may be retried",
		}
		moved, err := store.Applies().MarkStaleApplyRetryable(ctx, req)
		require.NoError(t, err)
		assert.False(t, moved, "a parent is never moved while a sibling's row is held by a claim in flight")
		assertStaleRunningTree(t, ctx, store, apply.ID, opID)

		require.NoError(t, claimTx.Rollback())
		claimOpen = false

		moved, err = store.Applies().MarkStaleApplyRetryable(ctx, req)
		require.NoError(t, err)
		assert.True(t, moved, "with the sibling released the stale tree moves")
		marked, err := store.Applies().Get(ctx, apply.ID)
		require.NoError(t, err)
		require.NotNil(t, marked)
		assert.Equal(t, state.Apply.FailedRetryable, marked.State)
		markedOp, err := store.ApplyOperations().Get(ctx, opID)
		require.NoError(t, err)
		require.NotNil(t, markedOp)
		assert.Equal(t, state.ApplyOperation.FailedRetryable, markedOp.State)
	})

	// The parent's heartbeat can read stale while the named operation is
	// still being driven: the operation refuses first, and the parent is not
	// reached.
	t.Run("the named operation's own fresh lease keeps both rows", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		store := newStore(t)
		apply, opID := seedStaleRunningTree(t, ctx, store, "stale_fresh_op")
		_, err := store.db.ExecContext(ctx, `
			UPDATE apply_operations
			SET lease_acquired_at = NOW(), updated_at = NOW()
			WHERE id = ?
		`, opID)
		require.NoError(t, err)

		moved, err := store.Applies().MarkStaleApplyRetryable(ctx, storage.StaleRetryableRequest{
			ApplyID: apply.ID, ApplyLeaseToken: "token-apply",
			OperationID: opID, OperationLeaseToken: "token-op",
			ErrorMessage: "copy of users failed and may be retried",
		})
		require.NoError(t, err)
		assert.False(t, moved, "an operation a driver is heartbeating is never reopened")
		op, err := store.ApplyOperations().Get(ctx, opID)
		require.NoError(t, err)
		require.NotNil(t, op)
		assert.Equal(t, state.ApplyOperation.Running, op.State)
		assert.Equal(t, "token-op", op.LeaseToken)
		parent, err := store.Applies().Get(ctx, apply.ID)
		require.NoError(t, err)
		require.NotNil(t, parent)
		assert.Equal(t, state.Apply.Running, parent.State)
		assert.Equal(t, "token-apply", parent.LeaseToken)
	})

	// The operation matches on id, state, token, and staleness; only its
	// parent differs from the apply named in the request.
	t.Run("an operation of another apply is refused", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		store := newStore(t)
		apply, opID := seedStaleRunningTree(t, ctx, store, "stale_own_tree")
		otherApply, otherOpID := seedStaleRunningTree(t, ctx, store, "stale_other_tree")

		moved, err := store.Applies().MarkStaleApplyRetryable(ctx, storage.StaleRetryableRequest{
			ApplyID: apply.ID, ApplyLeaseToken: "token-apply",
			OperationID: otherOpID, OperationLeaseToken: "token-op",
			ErrorMessage: "copy of users failed and may be retried",
		})
		require.NoError(t, err)
		assert.False(t, moved, "an operation is only ever moved with its own parent")
		assertStaleRunningTree(t, ctx, store, apply.ID, opID)
		assertStaleRunningTree(t, ctx, store, otherApply.ID, otherOpID)
	})

	// A row written before recovering_cutover was folded into recovering still
	// carries the old name, which the store's own writes never produce. It is
	// a recovering apply all the same: past its copy, so the retry leaves it
	// to the stale-active claim.
	t.Run("a row still named recovering_cutover is left alone", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		store := newStore(t)
		apply, opID := seedStaleRunningTree(t, ctx, store, "stale_legacy_recovering")
		staleHeartbeat := agedHeartbeatExpr(store, 2*storage.ApplyLeaseStaleAfter)
		_, err := store.db.ExecContext(ctx, `
			UPDATE applies
			SET state = 'recovering_cutover', updated_at = `+staleHeartbeat+`
			WHERE id = ?
		`, apply.ID)
		require.NoError(t, err)

		moved, err := store.Applies().MarkStaleApplyRetryable(ctx, storage.StaleRetryableRequest{
			ApplyID: apply.ID, ApplyLeaseToken: "token-apply",
			OperationID: opID, OperationLeaseToken: "token-op",
			ErrorMessage: "copy of users failed and may be retried",
		})
		require.NoError(t, err)
		assert.False(t, moved, "an apply reattaching to its cutover checkpoint is never rewound to running")
		var storedState, leaseToken string
		require.NoError(t, store.db.QueryRowContext(ctx, `SELECT state, lease_token FROM applies WHERE id = ?`, apply.ID).Scan(&storedState, &leaseToken))
		assert.Equal(t, "recovering_cutover", storedState)
		assert.Equal(t, "token-apply", leaseToken)
		op, err := store.ApplyOperations().Get(ctx, opID)
		require.NoError(t, err)
		require.NotNil(t, op)
		assert.Equal(t, state.ApplyOperation.Running, op.State, "the operation write does not survive the parent's refusal")
		assert.Equal(t, "token-op", op.LeaseToken)
	})

	// The caller observed the operation running, but it settled before the
	// write: a finished operation keeps its result and its parent stays where
	// it is.
	t.Run("a completed operation is never reopened", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		store := newStore(t)
		apply, opID := seedStaleRunningTree(t, ctx, store, "stale_completed_op")
		_, err := store.db.ExecContext(ctx, `
			UPDATE apply_operations
			SET state = ?, completed_at = NOW()
			WHERE id = ?
		`, state.ApplyOperation.Completed, opID)
		require.NoError(t, err)

		moved, err := store.Applies().MarkStaleApplyRetryable(ctx, storage.StaleRetryableRequest{
			ApplyID: apply.ID, ApplyLeaseToken: "token-apply",
			OperationID: opID, OperationLeaseToken: "token-op",
			ErrorMessage: "copy of users failed and may be retried",
		})
		require.NoError(t, err)
		assert.False(t, moved, "a settled operation keeps its result")
		op, err := store.ApplyOperations().Get(ctx, opID)
		require.NoError(t, err)
		require.NotNil(t, op)
		assert.Equal(t, state.ApplyOperation.Completed, op.State)
		assert.NotNil(t, op.CompletedAt)
		parent, err := store.Applies().Get(ctx, apply.ID)
		require.NoError(t, err)
		require.NotNil(t, parent)
		assert.Equal(t, state.Apply.Running, parent.State)
		assert.Equal(t, "token-apply", parent.LeaseToken)
		assert.Empty(t, parent.ErrorMessage)
	})
}

// seedStaleRunningTree creates a running apply with one running operation,
// both still carrying the lease of a driver whose heartbeat stopped.
func seedStaleRunningTree(t *testing.T, ctx context.Context, store *Storage, name string) (*storage.Apply, int64) {
	t.Helper()
	lock := createTestLock(t, store, name+"_db", storage.DatabaseTypeMySQL)
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_"+name, 995, state.Apply.Running, "staging")
	opID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", OperationKey: "schema", State: state.ApplyOperation.Running,
	})
	require.NoError(t, err)

	staleHeartbeat := agedHeartbeatExpr(store, 2*storage.ApplyLeaseStaleAfter)
	_, err = store.db.ExecContext(ctx, `
		UPDATE applies
		SET lease_owner = 'driver-crashed', lease_token = 'token-apply', lease_acquired_at = `+staleHeartbeat+`, updated_at = `+staleHeartbeat+`
		WHERE id = ?
	`, apply.ID)
	require.NoError(t, err)
	_, err = store.db.ExecContext(ctx, `
		UPDATE apply_operations
		SET lease_owner = 'driver-crashed', lease_token = 'token-op', lease_acquired_at = `+staleHeartbeat+`, updated_at = `+staleHeartbeat+`
		WHERE id = ?
	`, opID)
	require.NoError(t, err)
	return apply, opID
}

// assertStaleRunningTree proves a refused write left both rows as seeded.
func assertStaleRunningTree(t *testing.T, ctx context.Context, store *Storage, applyID, opID int64) {
	t.Helper()
	apply, err := store.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	assert.Equal(t, state.Apply.Running, apply.State)
	assert.Equal(t, "token-apply", apply.LeaseToken)
	assert.Empty(t, apply.ErrorMessage)
	op, err := store.ApplyOperations().Get(ctx, opID)
	require.NoError(t, err)
	require.NotNil(t, op)
	assert.Equal(t, state.ApplyOperation.Running, op.State)
	assert.Equal(t, "token-op", op.LeaseToken)
	assert.Empty(t, op.ErrorMessage)
}
