//go:build integration

package sqlstore

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/testutil"
)

func TestMySQLRetryableExpiryOperationFence(t *testing.T) {
	testRetryableExpiryOperationFence(t, func(t *testing.T) *Storage {
		clearTables(t)
		return NewMySQL(testDB)
	})
}

func TestPostgresRetryableExpiryOperationFence(t *testing.T) {
	_, db := testutil.StartPostgres(t, "expiry_fence")
	applyPostgresTestSchema(t, db)
	testRetryableExpiryOperationFence(t, func(t *testing.T) *Storage {
		clearPostgresTables(t, db)
		return NewPostgres(db)
	})
}

func TestMySQLRetryableExpiryLeaseMeanings(t *testing.T) {
	testRetryableExpiryLeaseMeanings(t, func(t *testing.T) *Storage {
		clearTables(t)
		return NewMySQL(testDB)
	})
}

func TestPostgresRetryableExpiryLeaseMeanings(t *testing.T) {
	_, db := testutil.StartPostgres(t, "expiry_lease_meanings")
	applyPostgresTestSchema(t, db)
	testRetryableExpiryLeaseMeanings(t, func(t *testing.T) *Storage {
		clearPostgresTables(t, db)
		return NewPostgres(db)
	})
}

// A fresh lease on a failed_retryable operation has two readings and the row
// cannot tell them apart: a driver that just claimed the retry holds one, and a
// drive that settled into failed_retryable left one behind. Expiry reads the
// lease alone, which protects the first and defers the second until the lease
// goes stale — and a deferred apply keeps its state, so the next sweep offers it
// again rather than stranding it.
func testRetryableExpiryLeaseMeanings(t *testing.T, newStore func(*testing.T) *Storage) {
	t.Run("a claimed retry is protected", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		store := newStore(t)
		apply, opIDs, tasks := seedExhaustedRetryableTree(t, ctx, store, "expiry_running_retry", maxRecoveryAttempts-1)

		// The redispatch claim rotates a lease onto one operation and spends the
		// parent's last attempt, so the apply now matches expiry's budget arm
		// while the drive it just admitted is only starting. The row it holds
		// still reads failed_retryable: the claim transitions pending and
		// stopped operations, not this one.
		claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "retry-driver")
		require.NoError(t, err)
		require.NotNil(t, claimed, "an operation under a parent with budget left must be claimable for redispatch")
		assert.Equal(t, state.ApplyOperation.FailedRetryable, claimed.State,
			"a redispatch claim leaves the operation state alone, so state cannot distinguish it from a settled drive")
		assert.Contains(t, opIDs, claimed.ID)

		parent, err := store.Applies().Get(ctx, apply.ID)
		require.NoError(t, err)
		require.NotNil(t, parent)
		require.Equal(t, maxRecoveryAttempts, parent.Attempt,
			"the redispatch spends the last attempt, which is what puts the apply in expiry's sights")
		require.Equal(t, state.Apply.FailedRetryable, parent.State,
			"an operation-only drive never claims the parent, so its state cannot carry the signal")

		expired, err := store.Applies().ExpireRetryable(ctx, 10)
		require.NoError(t, err)
		assert.Empty(t, expired, "the lease of a drive that was just admitted must keep expiry out")
		assertRetryableTreeIntact(t, ctx, store, apply.ID, opIDs, tasks)
	})

	t.Run("a settled drive's leftover lease expires the whole tree", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		store := newStore(t)
		apply, opIDs, tasks := seedExhaustedRetryableTree(t, ctx, store, "expiry_settled_drive", maxRecoveryAttempts)

		expired, err := store.Applies().ExpireRetryable(ctx, 10)
		require.NoError(t, err)
		assert.Empty(t, expired, "a settled drive's leftover lease is indistinguishable from a live one until it goes stale")
		assertRetryableTreeIntact(t, ctx, store, apply.ID, opIDs, tasks)

		for _, id := range opIDs {
			backdateOperationLease(t, ctx, store, id, storage.ApplyLeaseStaleAfter+time.Minute)
		}

		// The declined apply kept its state, so the next sweep sees it again and
		// settles the parent, every operation, and every task in one pass.
		expired, err = store.Applies().ExpireRetryable(ctx, 10)
		require.NoError(t, err)
		require.Len(t, expired, 1)
		assert.Equal(t, storage.RetryableExpirationAttemptBudget, expired[0].Reason)

		parent, err := store.Applies().Get(ctx, apply.ID)
		require.NoError(t, err)
		require.NotNil(t, parent)
		assert.Equal(t, state.Apply.Failed, parent.State)

		for _, id := range opIDs {
			op, err := store.ApplyOperations().Get(ctx, id)
			require.NoError(t, err)
			require.NotNil(t, op)
			assert.Equal(t, state.ApplyOperation.Failed, op.State,
				"every operation must settle with the parent, not just the one the pass looked at")
			assert.NotNil(t, op.CompletedAt, "a terminalized operation stamps completed_at")
		}

		assertTaskState(t, store, tasks[0].TaskIdentifier, state.Task.Failed)
		assertTaskState(t, store, tasks[1].TaskIdentifier, state.Task.Cancelled)
	})
}

// seedExhaustedRetryableTree builds the row shape a fan-out apply comes to rest
// in after a retryable failure: a failed_retryable parent at the given attempt
// count, two failed_retryable operations each carrying the lease its drive left
// behind, and the failed and still-queued task under the first of them.
func seedExhaustedRetryableTree(t *testing.T, ctx context.Context, store *Storage, name string, attempt int) (*storage.Apply, []int64, []*storage.Task) {
	t.Helper()
	lock := createTestLock(t, store, name+"_db", storage.DatabaseTypeMySQL)
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_"+name, 993, state.Apply.FailedRetryable, "staging")
	_, err := store.db.ExecContext(ctx, "UPDATE applies SET attempt = ? WHERE id = ?", attempt, apply.ID)
	require.NoError(t, err)

	var opIDs []int64
	for _, deployment := range []string{"region-a", "region-b"} {
		id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: deployment, Target: apply.Database,
			State: state.ApplyOperation.Running,
		})
		require.NoError(t, err)
		// NOW() rather than a client timestamp: the gate compares the heartbeat
		// against database time, so a stamp written from here would carry the
		// client's clock into the comparison.
		_, err = store.db.ExecContext(ctx, `
			UPDATE apply_operations
			SET state = ?, on_failure = ?, lease_owner = ?, lease_token = ?,
			    lease_acquired_at = NOW(), updated_at = NOW()
			WHERE id = ?
		`, state.ApplyOperation.FailedRetryable, storage.OnFailureContinue,
			"driver-"+deployment, "token-"+deployment, id)
		require.NoError(t, err)
		opIDs = append(opIDs, id)
	}

	failed := createRetryableReapTask(t, store, apply, "task_"+name+"_failed", "users", state.Task.FailedRetryable, "copy failed")
	queued := createRetryableReapTask(t, store, apply, "task_"+name+"_queued", "orders", state.Task.Pending, "")
	for _, task := range []*storage.Task{failed, queued} {
		_, err := store.db.ExecContext(ctx,
			"UPDATE tasks SET apply_operation_id = ? WHERE task_identifier = ?", opIDs[0], task.TaskIdentifier)
		require.NoError(t, err)
	}
	return apply, opIDs, []*storage.Task{failed, queued}
}

// assertRetryableTreeIntact proves a declined pass wrote nothing at all: a
// partial settlement is the failure this deferral exists to prevent, and a
// parent left failed over unsettled children is how it would show.
func assertRetryableTreeIntact(t *testing.T, ctx context.Context, store *Storage, applyID int64, opIDs []int64, tasks []*storage.Task) {
	t.Helper()
	parent, err := store.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, parent)
	assert.Equal(t, state.Apply.FailedRetryable, parent.State, "a declined apply keeps its state so the next sweep offers it again")

	for _, id := range opIDs {
		op, err := store.ApplyOperations().Get(ctx, id)
		require.NoError(t, err)
		require.NotNil(t, op)
		assert.Equal(t, state.ApplyOperation.FailedRetryable, op.State)
	}
	assertTaskState(t, store, tasks[0].TaskIdentifier, state.Task.FailedRetryable)
	assertTaskState(t, store, tasks[1].TaskIdentifier, state.Task.Pending)
}

// backdateOperationLease ages the operation's heartbeat on either dialect, which
// is what both the claim path and expiry read to decide a lease is idle. The age
// is subtracted from database time for the same reason the stamp above uses
// NOW(): the reader compares against database time.
func backdateOperationLease(t *testing.T, ctx context.Context, store *Storage, operationID int64, age time.Duration) {
	t.Helper()
	dialect := store.Applies().(*applyStore).dialect
	staleHeartbeat := dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime,
		LiteralIntervalAmount(uint64(age.Microseconds())), IntervalMicrosecond)
	_, err := store.db.ExecContext(ctx,
		"UPDATE apply_operations SET updated_at = "+staleHeartbeat+" WHERE id = ?", operationID)
	require.NoError(t, err)
}

// Exercise both orderings at the candidate-scan boundary using actual row locks
// and the public operation claim. Parent claims alone cannot protect fan-out.
func testRetryableExpiryOperationFence(t *testing.T, newStore func(*testing.T) *Storage) {
	for _, scenario := range []string{"claim wins", "expiry wins", "busy sibling", "heartbeat wins", "cutover claim wins", "expiry blocks cutover"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			store := newStore(t)
			expiryStore := store.Applies().(*applyStore)
			lock := createTestLock(t, store, "expiry_fence", storage.DatabaseTypeMySQL)
			apply := createTestApplyWithStateAndEnv(t, store, lock, "expiry_fence", 991, state.Apply.FailedRetryable, "staging")
			_, err := store.db.ExecContext(ctx, "UPDATE applies SET attempt = ? WHERE id = ?", maxRecoveryAttempts, apply.ID)
			require.NoError(t, err)
			var opIDs []int64
			for _, deployment := range []string{"region-a", "region-b"} {
				id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
					ApplyID: apply.ID, Deployment: deployment, Target: apply.Database, State: state.ApplyOperation.Running,
				})
				require.NoError(t, err)
				_, err = store.db.ExecContext(ctx, `UPDATE apply_operations SET lease_owner = 'old-driver', lease_token = 'old-token', updated_at = ? WHERE id = ?`, time.Now().Add(-2*storage.ApplyLeaseStaleAfter), id)
				require.NoError(t, err)
				opIDs = append(opIDs, id)
			}
			if scenario == "cutover claim wins" || scenario == "expiry blocks cutover" {
				_, err = store.db.ExecContext(ctx, "UPDATE apply_operations SET state = ?, cutover_policy = ?, updated_at = ? WHERE id = ?", state.ApplyOperation.Completed, storage.CutoverPolicyParallel, time.Now().Add(-2*storage.ApplyLeaseStaleAfter), opIDs[0])
				require.NoError(t, err)
				_, err = store.db.ExecContext(ctx, "UPDATE apply_operations SET state = ?, cutover_policy = ?, updated_at = ? WHERE id = ?", state.ApplyOperation.WaitingForCutover, storage.CutoverPolicyParallel, time.Now().Add(-2*storage.ApplyLeaseStaleAfter), opIDs[1])
				require.NoError(t, err)
			}
			task := createRetryableReapTask(t, store, apply, "expiry_task", "users", state.Task.Running, "")
			_, err = store.db.ExecContext(ctx, "UPDATE tasks SET apply_operation_id = ? WHERE id = ?", opIDs[0], task.ID)
			require.NoError(t, err)

			if scenario == "busy sibling" {
				// A claim can hold a sibling while waiting to update the parent's retry
				// budget. Expiry must skip the whole apply rather than wait in reverse
				// lock order or settle only the unlocked siblings.
				busy, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
				require.NoError(t, err)
				defer rollbackTx(ctx, busy, "test busy expiry sibling")
				var id int64
				require.NoError(t, busy.QueryRowContext(ctx, "SELECT id FROM apply_operations WHERE id = ? FOR UPDATE", opIDs[1]).Scan(&id))
				expired, err := store.Applies().ExpireRetryable(ctx, 10)
				require.NoError(t, err)
				assert.Empty(t, expired)
				assertTaskState(t, store, task.TaskIdentifier, state.Task.Running)
				require.NoError(t, busy.Rollback())
				expired, err = store.Applies().ExpireRetryable(ctx, 10)
				require.NoError(t, err)
				require.Len(t, expired, 1)
				assertTaskState(t, store, task.TaskIdentifier, state.Task.Failed)
				return
			}

			// Pause at expiry's actual parent-selection boundary, before it locks
			// children. The same predicate admits this exhausted, stale-leased tree.
			tx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
			require.NoError(t, err)
			defer rollbackTx(ctx, tx, "test expiry selection")
			gate := undrivenApplyGate(expiryStore.dialect)
			args := []any{apply.ID, state.Apply.FailedRetryable, maxRecoveryAttempts}
			var id int64
			require.NoError(t, tx.QueryRowContext(ctx, "SELECT id FROM applies WHERE id = ? AND state = ? AND attempt >= ? AND "+gate+" FOR UPDATE", args...).Scan(&id))

			switch scenario {
			case "claim wins", "cutover claim wins":
				claim := store.ApplyOperations().FindNextApplyOperation
				if scenario == "cutover claim wins" {
					claim = store.ApplyOperations().FindNextApplyOperationCutover
				}
				claimed, err := claim(ctx, "new-driver")
				require.NoError(t, err)
				require.NotNil(t, claimed)
				admitted, err := expiryStore.lockUndrivenApply(ctx, tx, apply.ID)
				require.NoError(t, err)
				assert.False(t, admitted, "lease recheck must reject a claim made after candidate selection")
			case "heartbeat wins":
				err := store.ApplyOperations().Heartbeat(storage.WithOperationLease(ctx, storage.OperationLease{ApplyID: apply.ID, OperationID: opIDs[0], Token: "old-token"}), opIDs[0])
				require.NoError(t, err)
				admitted, err := expiryStore.lockUndrivenApply(ctx, tx, apply.ID)
				require.NoError(t, err)
				assert.False(t, admitted, "renewed lease must exclude expiry even though selection saw it stale")
			case "expiry wins", "expiry blocks cutover":
				admitted, err := expiryStore.lockUndrivenApply(ctx, tx, apply.ID)
				require.NoError(t, err)
				require.True(t, admitted)
				claim := store.ApplyOperations().FindNextApplyOperation
				if scenario == "expiry blocks cutover" {
					claim = store.ApplyOperations().FindNextApplyOperationCutover
				}
				claimed, err := claim(ctx, "new-driver")
				require.NoError(t, err)
				assert.Nil(t, claimed, "claim must skip every operation while expiry owns the tree")
			}
			require.NoError(t, tx.Rollback())
			assertTaskState(t, store, task.TaskIdentifier, state.Task.Running)
		})
	}
}
