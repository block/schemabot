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

func TestMySQLEndedOperationStopAndRetryExpiry(t *testing.T) {
	testEndedOperationStopAndRetryExpiry(t, func(t *testing.T) *Storage {
		clearTables(t)
		return NewMySQL(testDB)
	})
}

func TestPostgresEndedOperationStopAndRetryExpiry(t *testing.T) {
	_, db := testutil.StartPostgres(t, "ended_operation_expiry")
	applyPostgresTestSchema(t, db)
	testEndedOperationStopAndRetryExpiry(t, func(t *testing.T) *Storage {
		clearPostgresTables(t, db)
		return NewPostgres(db)
	})
}

// A deferred apply's shard -40 waits on a retry that will never come, and shard
// 40-80 is parked at the cutover with no driver holding it, as after the server
// that drove it restarted. Both the ended operation sweep and retry expiry see
// an apply no driver holds. Expiry runs between the sweep listing the apply and
// recording SchemaBot's stop, and again once the stop is pending, and passes
// over it both times. The stop then reaches the parked shard, and once the
// stop's drive no longer holds the shard, the settle fails the apply with the
// shard's copy saved and -40 named as its cause.
//
// The retry can never come for either of two reasons, and both survive the
// stop's delivery: -40 spent its own retry budget, or the failure is older
// than the recovery window, which the stop never refreshes.
func testEndedOperationStopAndRetryExpiry(t *testing.T, newStore func(*testing.T) *Storage) {
	causes := []struct {
		name  string
		spend func(t *testing.T, store *Storage, applyID int64)
	}{
		{"spent budget", func(t *testing.T, store *Storage, applyID int64) {
			_, err := store.db.ExecContext(t.Context(), "UPDATE apply_operations SET attempt = ? WHERE apply_id = ? AND state = ?",
				maxRecoveryAttempts, applyID, state.ApplyOperation.FailedRetryable)
			require.NoError(t, err)
			_, err = store.db.ExecContext(t.Context(), "UPDATE applies SET attempt = ? WHERE id = ?", maxRecoveryAttempts, applyID)
			require.NoError(t, err)
		}},
		{"recovery window passed", func(t *testing.T, store *Storage, applyID int64) {
			_, err := store.db.ExecContext(t.Context(),
				"UPDATE applies SET updated_at = "+agedHeartbeatExpr(store, 2*24*time.Hour)+" WHERE id = ?", applyID)
			require.NoError(t, err)
		}},
	}
	for _, cause := range causes {
		t.Run(cause.name, func(t *testing.T) {
			ctx := t.Context()
			store := newStore(t)
			apply, retryID, parkedID := createParkedBehindRetryApply(t, store, "apply_ended_expiry_race")
			cause.spend(t, store, apply.ID)

			listed, err := store.Applies().ListAppliesHeldByEndedOperation(ctx, endedOperationStopTypes, 10)
			require.NoError(t, err)
			require.Len(t, listed, 1)
			require.NotNil(t, listed[0].EndedOperation)
			assert.Equal(t, retryID, listed[0].EndedOperation.ID)

			expired, err := store.Applies().ExpireRetryable(ctx, endedOperationStopTypes, 10)
			require.NoError(t, err)
			assert.Empty(t, expired, "expiry passes over an apply the ended operation sweep is stopping")

			_, _, err = store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
				ApplyID: apply.ID, Operation: storage.ControlOperationStop, Status: storage.ControlRequestPending,
				RequestedBy: storage.EndedOperationStopCaller, Metadata: []byte(`{}`),
			})
			require.NoError(t, err)
			expired, err = store.Applies().ExpireRetryable(ctx, endedOperationStopTypes, 10)
			require.NoError(t, err)
			assert.Empty(t, expired, "expiry passes over an apply carrying SchemaBot's stop")

			claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-stop")
			require.NoError(t, err)
			require.NotNil(t, claimed, "the pending stop claims the parked shard")
			assert.Equal(t, parkedID, claimed.ID)
			driveCtx := storage.WithOperationLease(ctx, claimed.Lease())
			require.NoError(t, store.ApplyOperations().UpdateState(driveCtx, parkedID, state.ApplyOperation.Stopped))
			parkedTask, err := store.Tasks().Get(ctx, "task_apply_ended_expiry_race_40-80")
			require.NoError(t, err)
			require.NotNil(t, parkedTask)
			parkedTask.State = state.Task.Stopped
			require.NoError(t, store.Tasks().Update(ctx, parkedTask))
			// A stopped operation stays addressable, so its drive leaves the
			// lease in place, and the settle waits until no lease is fresh.
			settled, err := store.Applies().SettleAppliesStoppedForEndedOperation(ctx, 10)
			require.NoError(t, err)
			assert.Empty(t, settled, "the settle waits while the stop's drive still holds the shard")
			backdateOperationLease(t, ctx, store, parkedID, 2*storage.ApplyLeaseStaleAfter)

			settled, err = store.Applies().SettleAppliesStoppedForEndedOperation(ctx, 10)
			require.NoError(t, err)
			require.Len(t, settled, 1, "the stop's delivery leaves the reason the retry can never come")
			assert.Equal(t, state.Apply.FailedRetryable, settled[0].SettledFrom)
			require.NotNil(t, settled[0].EndedOperation)
			assert.Equal(t, retryID, settled[0].EndedOperation.ID)

			stopped, err := store.ApplyOperations().Get(ctx, parkedID)
			require.NoError(t, err)
			require.NotNil(t, stopped)
			assert.Equal(t, state.ApplyOperation.Failed, stopped.State)
			assert.Equal(t, retryID, stopped.EndedByOperationID)
			stoppedTask, err := store.Tasks().Get(ctx, "task_apply_ended_expiry_race_40-80")
			require.NoError(t, err)
			require.NotNil(t, stoppedTask)
			assert.Equal(t, state.Task.Stopped, stoppedTask.State, "the parked shard's copy stays saved")
		})
	}
}

// createParkedBehindRetryApply creates a failed_retryable apply deferring its
// cutover, with shard -40 waiting on a retry and shard 40-80 parked at the
// cutover, neither held by a driver, and one task under each. It returns the
// apply and the two operations' IDs.
func createParkedBehindRetryApply(t *testing.T, store *Storage, name string) (*storage.Apply, int64, int64) {
	t.Helper()
	ctx := t.Context()
	lock := createTestLock(t, store, name+"_db", storage.DatabaseTypeMySQL)
	apply := createTestApplyWithStateAndEnv(t, store, lock, name, 1, state.Apply.FailedRetryable, "production")
	apply.Options = []byte(`{"defer_cutover":true}`)
	require.NoError(t, store.Applies().Update(ctx, apply))

	insert := func(shard, opState, taskState string) int64 {
		id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: "region-a",
			OperationKey:  "commerce/" + shard + "/users",
			OperationKind: storage.ApplyOperationKindWork,
			CutoverPolicy: storage.CutoverPolicyParallel,
			State:         opState,
		})
		require.NoError(t, err)
		now := time.Now()
		_, err = store.Tasks().Create(ctx, &storage.Task{
			TaskIdentifier:   "task_" + name + "_" + shard,
			ApplyID:          apply.ID,
			ApplyOperationID: &id,
			PlanID:           apply.PlanID,
			Database:         apply.Database,
			DatabaseType:     apply.DatabaseType,
			Engine:           storage.EngineSpirit,
			Environment:      apply.Environment,
			State:            taskState,
			TableName:        "users",
			DDL:              "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
			DDLAction:        "ALTER",
			CreatedAt:        now,
			UpdatedAt:        now,
		})
		require.NoError(t, err)
		return id
	}
	retryID := insert("-40", state.ApplyOperation.FailedRetryable, state.Task.FailedRetryable)
	parkedID := insert("40-80", state.ApplyOperation.WaitingForCutover, state.Task.WaitingForCutover)
	return apply, retryID, parkedID
}
