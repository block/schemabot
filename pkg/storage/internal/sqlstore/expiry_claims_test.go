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
