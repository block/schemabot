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
	"github.com/block/schemabot/pkg/testutil"
)

// claimContentionDeadline bounds each claim and reread in these tests. A row
// lock held by the test stays held until the claim returns, so a claim that
// waits on the lock instead of skipping it fails at this deadline instead of
// hanging until the package timeout.
const claimContentionDeadline = 30 * time.Second

// claimContext is a context for one claim in a test that holds a row lock.
func claimContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), claimContentionDeadline)
	t.Cleanup(cancel)
	return ctx
}

func TestMySQLOperationClaimContention(t *testing.T) {
	testOperationClaimContention(t, func(t *testing.T) *Storage {
		clearTables(t)
		return NewMySQL(testDB)
	})
}

func TestPostgresOperationClaimContention(t *testing.T) {
	_, db := testutil.StartPostgres(t, "operation_claim_contention")
	applyPostgresTestSchema(t, db)
	testOperationClaimContention(t, func(t *testing.T) *Storage {
		clearPostgresTables(t, db)
		return NewPostgres(db)
	})
}

// testOperationClaimContention covers a claim that finds the operation it
// would take row-locked by another transaction, on both dialects. The claim
// skips locked rows, so without contention reporting it cannot tell that case
// from an apply with nothing left to start, and a driver woken for that
// operation goes back to sleep while it sits claimable.
func testOperationClaimContention(t *testing.T, newStore func(*testing.T) *Storage) {
	// Another transaction holds the only pending operation's row, the way
	// SaveExternalID holds every operation of an apply while it records a
	// remote apply id. A claim that reports contention says the operation is
	// there and claimable, and one that does not reports nothing to claim.
	// Once the transaction ends, the claim takes the operation.
	t.Run("a claimable operation locked by another transaction is reported as contended", func(t *testing.T) {
		ctx := claimContext(t)
		store := newStore(t)
		apply := createQueueApply(t, store, "apply_claim_contended", state.Apply.Running, false, time.Minute)
		opID := insertQueueOperation(t, store, apply, "shard-1", state.ApplyOperation.Pending, false)

		holder := lockOperationRow(t, store, opID)

		claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a", storage.ReportClaimContention())
		require.ErrorIs(t, err, storage.ErrOperationClaimContended)
		assert.Nil(t, claimed)

		claimed, err = store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
		require.NoError(t, err, "a claim that does not report contention skips the locked row")
		assert.Nil(t, claimed)

		require.NoError(t, holder.Rollback())
		claimed, err = store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a", storage.ReportClaimContention())
		require.NoError(t, err)
		require.NotNil(t, claimed, "the operation is claimable once the transaction holding it ends")
		assert.Equal(t, opID, claimed.ID)
		assert.Equal(t, "driver-a", claimed.LeaseOwner)
	})

	// The lock-free reread applies every gate the claim does, so a locked
	// operation the claim could not take anyway is not contention. Here the
	// only other operation is already leased to a live driver, and the locked
	// one belongs to an apply that has settled.
	t.Run("a locked operation the claim could not take is not contention", func(t *testing.T) {
		ctx := claimContext(t)
		store := newStore(t)
		apply := createQueueApply(t, store, "apply_claim_settled", state.Apply.Completed, false, time.Minute)
		opID := insertQueueOperation(t, store, apply, "shard-1", state.ApplyOperation.Pending, false)
		running := createQueueApply(t, store, "apply_claim_running", state.Apply.Running, false, time.Minute)
		insertQueueOperation(t, store, running, "shard-1", state.ApplyOperation.Running, true)

		holder := lockOperationRow(t, store, opID)
		defer func() { require.NoError(t, holder.Rollback()) }()

		claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a", storage.ReportClaimContention())
		require.NoError(t, err, "nothing the claim could take is locked")
		assert.Nil(t, claimed)
	})

	// With nothing claimable at all, reporting contention changes nothing.
	t.Run("nothing to claim is reported as nothing", func(t *testing.T) {
		ctx := t.Context()
		store := newStore(t)

		claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a", storage.ReportClaimContention())
		require.NoError(t, err)
		assert.Nil(t, claimed)
	})
}

// lockOperationRow opens a transaction that holds the apply_operations row
// opID locked until the returned transaction ends. The test rolls it back to
// release the row; if the test fails first, cleanup rolls it back.
func lockOperationRow(t *testing.T, store *Storage, opID int64) *rebindTx {
	t.Helper()
	tx, err := store.db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := tx.Rollback()
		if errors.Is(err, sql.ErrTxDone) || errors.Is(err, context.Canceled) {
			return
		}
		assert.NoError(t, err, "roll back the transaction holding the operation row")
	})
	var locked int64
	require.NoError(t, tx.QueryRowContext(t.Context(), "SELECT id FROM apply_operations WHERE id = ? FOR UPDATE", opID).Scan(&locked))
	require.Equal(t, opID, locked)
	return tx
}
