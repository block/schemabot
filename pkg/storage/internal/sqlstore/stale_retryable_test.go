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
