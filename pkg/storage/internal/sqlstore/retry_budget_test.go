//go:build integration

package sqlstore

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/testutil"
)

func TestMySQLRetryBudget(t *testing.T) {
	testRetryBudget(t, func(t *testing.T) *Storage {
		clearTables(t)
		return NewMySQL(testDB)
	})
}

func TestPostgresRetryBudget(t *testing.T) {
	_, db := testutil.StartPostgres(t, "retry_budget")
	applyPostgresTestSchema(t, db)
	testRetryBudget(t, func(t *testing.T) *Storage {
		clearPostgresTables(t, db)
		return NewPostgres(db)
	})
}

// testRetryBudget covers each operation's own retry budget and the backoff
// between its redispatches on both dialects.
func testRetryBudget(t *testing.T, newStore func(*testing.T) *Storage) {
	// A failed_retryable operation is redispatched again only once the backoff
	// for its own attempt has passed since it failed, for every attempt the
	// budget allows. The first redispatch is immediate.
	for attempt := range maxRecoveryAttempts {
		type backoffCase struct {
			name      string
			failedAgo time.Duration
			claimable bool
		}
		backoff := storage.RecoveryBackoff(attempt)
		cases := []backoffCase{{name: "past the backoff", failedAgo: backoff + 3*time.Second, claimable: true}}
		if backoff > 0 {
			cases = append(cases, backoffCase{name: "inside the backoff", failedAgo: backoff - 3*time.Second})
		}
		for _, tc := range cases {
			t.Run(fmt.Sprintf("attempt %d %s", attempt, tc.name), func(t *testing.T) {
				ctx := t.Context()
				store := newStore(t)
				lock := createTestLock(t, store, "retry_backoff_db", storage.DatabaseTypeMySQL)
				apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_retry_backoff", 1, state.Apply.FailedRetryable, "staging")
				id := insertOperationAtAttempt(t, store, apply, "region-a", state.ApplyOperation.FailedRetryable, attempt, tc.failedAgo)
				insertOperationAtAttempt(t, store, apply, "region-b", state.ApplyOperation.WaitingForCutover, 0, 0)

				claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
				require.NoError(t, err)
				if !tc.claimable {
					assert.Nil(t, claimed, "attempt %d waits %s after its failure", attempt, backoff)
					return
				}
				require.NotNil(t, claimed)
				assert.Equal(t, id, claimed.ID)
				assert.Equal(t, attempt+1, claimed.Attempt)
			})
		}
	}

	// Shard A spent all its retries and then succeeded, which raised the
	// apply's attempt to the budget. Shard B fails for the first time
	// afterwards. B still has its whole budget: expiry passes the apply over,
	// and B's first redispatch is immediate.
	t.Run("a sibling that spent its budget leaves the other's intact", func(t *testing.T) {
		ctx := t.Context()
		store := newStore(t)
		lock := createTestLock(t, store, "retry_sibling_db", storage.DatabaseTypeMySQL)
		apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_retry_sibling", 1, state.Apply.FailedRetryable, "staging")
		setApplyAttempt(t, store, apply.ID, state.Apply.FailedRetryable, maxRecoveryAttempts, 0)
		insertOperationAtAttempt(t, store, apply, "region-a", state.ApplyOperation.Completed, maxRecoveryAttempts, 0)
		shardB := insertOperationAtAttempt(t, store, apply, "region-b", state.ApplyOperation.FailedRetryable, 0, 0)

		expired, err := store.Applies().ExpireRetryable(ctx, 10)
		require.NoError(t, err)
		assert.Empty(t, expired, "a failed_retryable shard with retries left keeps the apply alive")

		claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
		require.NoError(t, err)
		require.NotNil(t, claimed, "the shard's first redispatch is immediate")
		assert.Equal(t, shardB, claimed.ID)
		assert.Equal(t, 1, claimed.Attempt)
		assert.Equal(t, state.ApplyOperation.Running, claimed.State)

		persisted, err := store.Applies().Get(ctx, apply.ID)
		require.NoError(t, err)
		require.NotNil(t, persisted)
		assert.Equal(t, maxRecoveryAttempts, persisted.Attempt, "the apply keeps the highest attempt of any shard")
	})

	// An operation's attempt advances when it is claimed, before the parent
	// claim that raises the apply's. A parent claim that then fails leaves the
	// apply one behind, so an operation can spend its budget while the apply
	// reads less. The operation's own budget decides: no redispatch can run it
	// again, and expiry fails the apply rather than leaving it retryable until
	// the recovery window lapses.
	t.Run("an operation that spent its budget expires the apply", func(t *testing.T) {
		ctx := t.Context()
		store := newStore(t)
		lock := createTestLock(t, store, "retry_spent_db", storage.DatabaseTypeMySQL)
		apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_retry_spent", 1, state.Apply.FailedRetryable, "staging")
		setApplyAttempt(t, store, apply.ID, state.Apply.FailedRetryable, maxRecoveryAttempts-1, 0)
		insertOperationAtAttempt(t, store, apply, "region-a", state.ApplyOperation.FailedRetryable, maxRecoveryAttempts, storage.MaxRecoveryBackoff+time.Minute)

		claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
		require.NoError(t, err)
		assert.Nil(t, claimed, "an operation that spent its budget is not redispatched")

		expired, err := store.Applies().ExpireRetryable(ctx, 10)
		require.NoError(t, err)
		require.Len(t, expired, 1)
		assert.Equal(t, apply.ID, expired[0].Apply.ID)
		assert.Equal(t, storage.RetryableExpirationAttemptBudget, expired[0].Reason)
	})

	// A drive that failed back to failed_retryable hands its lease back on
	// shutdown. The failure time is what the backoff counts from, so the
	// handback leaves it in place and the next redispatch still waits.
	t.Run("handing a failed operation back keeps its backoff", func(t *testing.T) {
		ctx := t.Context()
		store := newStore(t)
		lock := createTestLock(t, store, "retry_handback_db", storage.DatabaseTypeMySQL)
		apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_retry_handback", 1, state.Apply.FailedRetryable, "staging")
		id := insertOperationAtAttempt(t, store, apply, "region-a", state.ApplyOperation.FailedRetryable, 2, 0)
		insertOperationAtAttempt(t, store, apply, "region-b", state.ApplyOperation.WaitingForCutover, 0, 0)
		_, err := store.db.ExecContext(ctx,
			"UPDATE apply_operations SET lease_owner = 'driver-a', lease_token = 'token-a', lease_acquired_at = NOW() WHERE id = ?", id)
		require.NoError(t, err)

		released, err := store.ApplyOperations().ReleaseClaim(ctx, storage.OperationLease{
			ApplyID: apply.ID, OperationID: id, Owner: "driver-a", Token: "token-a",
		})
		require.NoError(t, err)
		require.True(t, released)

		claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-b")
		require.NoError(t, err)
		assert.Nil(t, claimed, "the handed-back operation still waits out its backoff")
	})

	// A driver crashed part-way through a retry its parent had already
	// admitted: the parent is running with a stale heartbeat and the operation
	// is still failed_retryable at an attempt whose backoff has not passed.
	// Recovering the crashed drive is not a new redispatch, so it does not wait
	// out the backoff and does not spend an attempt.
	t.Run("crash recovery does not wait out the backoff", func(t *testing.T) {
		ctx := t.Context()
		store := newStore(t)
		lock := createTestLock(t, store, "retry_crash_db", storage.DatabaseTypeMySQL)
		apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_retry_crash", 1, state.Apply.FailedRetryable, "staging")
		stale := 2 * storage.ApplyLeaseStaleAfter
		require.Less(t, stale, storage.RecoveryBackoff(maxRecoveryAttempts-1), "the fixture sits inside the operation's backoff")
		setApplyAttempt(t, store, apply.ID, state.Apply.Running, maxRecoveryAttempts-1, stale)
		id := insertOperationAtAttempt(t, store, apply, "region-a", state.ApplyOperation.FailedRetryable, maxRecoveryAttempts-1, stale)

		claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "recovery-driver")
		require.NoError(t, err)
		require.NotNil(t, claimed, "a crashed retry is recovered without waiting out the backoff")
		assert.Equal(t, id, claimed.ID)
		assert.Equal(t, maxRecoveryAttempts-1, claimed.Attempt, "recovery spends no attempt")
	})
}

// insertOperationAtAttempt inserts an operation in the given state that has
// had attempt redispatches and last changed failedAgo in the past, on either
// dialect.
func insertOperationAtAttempt(t *testing.T, store *Storage, apply *storage.Apply, deployment, opState string, attempt int, failedAgo time.Duration) int64 {
	t.Helper()
	id, err := store.ApplyOperations().Insert(t.Context(), &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: deployment, State: opState,
	})
	require.NoError(t, err)
	_, err = store.db.ExecContext(t.Context(),
		"UPDATE apply_operations SET attempt = ?, updated_at = "+agedHeartbeatExpr(store, failedAgo)+" WHERE id = ?",
		attempt, id)
	require.NoError(t, err)
	return id
}

// setApplyAttempt sets the apply's state and attempt and backdates its
// heartbeat by age, on either dialect.
func setApplyAttempt(t *testing.T, store *Storage, applyID int64, applyState string, attempt int, age time.Duration) {
	t.Helper()
	_, err := store.db.ExecContext(t.Context(),
		"UPDATE applies SET state = ?, attempt = ?, updated_at = "+agedHeartbeatExpr(store, age)+" WHERE id = ?",
		applyState, attempt, applyID)
	require.NoError(t, err)
}
