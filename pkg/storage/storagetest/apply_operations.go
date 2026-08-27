package storagetest

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// TestApplyOperations runs the behavioral parity suite for
// storage.ApplyOperationStore: ordered operation claims, operation- and
// apply-lease guarded writes, bulk pending-stop transitions, the deployment
// remote-apply invariant, and single-winner claim contention.
func TestApplyOperations(t *testing.T, h Harness) {
	createOperation := func(t *testing.T, store storage.Storage, applyID int64, deployment, operationKey string) int64 {
		t.Helper()
		id, err := store.ApplyOperations().Insert(t.Context(), &storage.ApplyOperation{
			ApplyID: applyID, Deployment: deployment, OperationKey: operationKey,
		})
		require.NoError(t, err)
		return id
	}

	// FindNextApplyOperation_ClaimsInDeploymentOrder verifies the operation
	// ladder: a pending operation is claimed into running with a fresh lease,
	// and a later deployment remains blocked until its earlier sibling
	// completes.
	t.Run("FindNextApplyOperation_ClaimsInDeploymentOrder", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "operation_order_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_operation_order", 901)
		firstID := createOperation(t, store, apply.ID, "region-a", "schema")
		secondID := createOperation(t, store, apply.ID, "region-b", "schema")
		thirdID := createOperation(t, store, apply.ID, "region-c", "schema")

		first, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
		require.NoError(t, err)
		require.NotNil(t, first)
		assert.Equal(t, firstID, first.ID)
		assert.Equal(t, "region-a", first.Deployment)
		assert.Equal(t, state.ApplyOperation.Pending, first.State, "the claim returns the pre-transition state")
		assert.Equal(t, "driver-a", first.LeaseOwner)
		assert.NotEmpty(t, first.LeaseToken)

		blocked, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-b")
		require.NoError(t, err)
		assert.Nil(t, blocked, "a later deployment waits for its earlier sibling")

		require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, firstID))
		second, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-b")
		require.NoError(t, err)
		require.NotNil(t, second)
		assert.Equal(t, secondID, second.ID)
		assert.Equal(t, "region-b", second.Deployment)

		persisted, err := store.ApplyOperations().Get(ctx, secondID)
		require.NoError(t, err)
		require.NotNil(t, persisted)
		assert.Equal(t, state.ApplyOperation.Running, persisted.State)
		assert.NotNil(t, persisted.StartedAt)

		require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, secondID))
		third, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-c")
		require.NoError(t, err)
		require.NotNil(t, third)
		assert.Equal(t, thirdID, third.ID)
		assert.Equal(t, "region-c", third.Deployment)
		require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, thirdID))
		done, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-d")
		require.NoError(t, err)
		assert.Nil(t, done)
	})

	// FindNextApplyOperation_ShardFanOutClaimOrder verifies claim ordering
	// when several rows are claimable at once. A deployment's per-shard work
	// rows fan out — a later shard is claimable while an earlier one is still
	// running — so with two claimable siblings the claim must hand them out in
	// insertion order. The group_finalizer stays blocked
	// until every work sibling completes.
	t.Run("FindNextApplyOperation_ShardFanOutClaimOrder", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "operation_shard_order_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_operation_shard_order", 908)

		shardA, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce/-80/users", OperationKind: storage.ApplyOperationKindWork,
		})
		require.NoError(t, err)
		shardB, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce/80-/users", OperationKind: storage.ApplyOperationKindWork,
		})
		require.NoError(t, err)
		finalizerID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce/group_finalizer", OperationKind: storage.ApplyOperationKindGroupFinalizer,
		})
		require.NoError(t, err)

		first, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
		require.NoError(t, err)
		require.NotNil(t, first)
		assert.Equal(t, shardA, first.ID, "concurrently claimable siblings are handed out in insertion order")

		second, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-b")
		require.NoError(t, err)
		require.NotNil(t, second, "a sibling shard is claimable while the first still runs")
		assert.Equal(t, shardB, second.ID)
		assert.NotEqual(t, first.LeaseToken, second.LeaseToken, "each shard claim rotates its own lease")

		blocked, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-c")
		require.NoError(t, err)
		assert.Nil(t, blocked, "the group finalizer waits for every work sibling")

		require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, shardA))

		stillBlocked, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-c")
		require.NoError(t, err)
		assert.Nil(t, stillBlocked, "the finalizer waits for EVERY sibling, not just one")

		require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, shardB))

		claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
		require.NoError(t, err)
		require.NotNil(t, claimed)
		assert.Equal(t, finalizerID, claimed.ID)
		assert.Equal(t, storage.ApplyOperationKindGroupFinalizer, claimed.OperationKind)
	})

	// LeaseGuardsSingleAndJoinedWrites verifies both guarded DML shapes. An
	// operation lease fences a single-row write on the child token, while an
	// apply lease fences a joined child/parent write; stale holders lose
	// without changing state and current holders win.
	t.Run("LeaseGuardsSingleAndJoinedWrites", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		opLock := CreateLock(t, store, "operation_lease_db", storage.DatabaseTypeMySQL)
		opApply := CreateApply(t, store, opLock, "apply_operation_lease", 902)
		opID := createOperation(t, store, opApply.ID, "region-a", "schema")
		claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "current-driver")
		require.NoError(t, err)
		require.NotNil(t, claimed)
		require.NoError(t, store.ApplyOperations().UpdateState(ctx, opID, state.ApplyOperation.Pending))

		staleOpCtx := storage.WithOperationLease(ctx, storage.OperationLease{
			ApplyID: opApply.ID, OperationID: opID, Owner: "old-driver", Token: "stale-token",
		})
		ownedOpCtx := storage.WithOperationLease(ctx, storage.OperationLease{
			ApplyID: opApply.ID, OperationID: opID, Owner: claimed.LeaseOwner, Token: claimed.LeaseToken,
		})
		require.ErrorIs(t, store.ApplyOperations().UpdateState(staleOpCtx, opID, state.ApplyOperation.Running), storage.ErrApplyLeaseLost)
		operation, err := store.ApplyOperations().Get(ctx, opID)
		require.NoError(t, err)
		require.NotNil(t, operation)
		assert.Equal(t, state.ApplyOperation.Pending, operation.State)
		require.NoError(t, store.ApplyOperations().UpdateState(ownedOpCtx, opID, state.ApplyOperation.Running))
		operation, err = store.ApplyOperations().Get(ctx, opID)
		require.NoError(t, err)
		require.NotNil(t, operation)
		assert.Equal(t, state.ApplyOperation.Running, operation.State)

		applyLock := CreateLock(t, store, "apply_lease_operation_db", storage.DatabaseTypeMySQL)
		apply := CreateClaimedApply(t, store, applyLock, "apply_joined_lease", 903, "current-driver")
		joinedID := createOperation(t, store, apply.ID, "region-a", "schema")
		staleApplyCtx := storage.WithApplyLease(ctx, storage.ApplyLease{
			ApplyID: apply.ID, Owner: "old-driver", Token: "stale-token",
		})
		ownedApplyCtx := storage.WithApplyLease(ctx, storage.ApplyLease{
			ApplyID: apply.ID, Owner: apply.LeaseOwner, Token: apply.LeaseToken,
		})
		require.ErrorIs(t, store.ApplyOperations().MarkStarted(staleApplyCtx, joinedID), storage.ErrApplyLeaseLost)
		operation, err = store.ApplyOperations().Get(ctx, joinedID)
		require.NoError(t, err)
		require.NotNil(t, operation)
		assert.Equal(t, state.ApplyOperation.Pending, operation.State)
		assert.Nil(t, operation.StartedAt)
		require.NoError(t, store.ApplyOperations().MarkStarted(ownedApplyCtx, joinedID))
		operation, err = store.ApplyOperations().Get(ctx, joinedID)
		require.NoError(t, err)
		require.NotNil(t, operation)
		assert.Equal(t, state.ApplyOperation.Running, operation.State)
		assert.NotNil(t, operation.StartedAt)
	})

	// MarkPendingStoppedByApply verifies the bulk transition changes only
	// pending siblings, leaves running and terminal rows intact, preserves the
	// resumable stopped timestamp contract, and enforces the caller's lease.
	t.Run("MarkPendingStoppedByApply", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "operation_stop_db", storage.DatabaseTypeMySQL)
		apply := CreateClaimedApply(t, store, lock, "apply_operation_stop", 904, "current-driver")
		pendingID := createOperation(t, store, apply.ID, "region-a", "pending")
		runningID := createOperation(t, store, apply.ID, "region-b", "running")
		completedID := createOperation(t, store, apply.ID, "region-c", "completed")
		failedID := createOperation(t, store, apply.ID, "region-d", "failed")
		require.NoError(t, store.ApplyOperations().UpdateState(ctx, runningID, state.ApplyOperation.Running))
		require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, completedID))
		require.NoError(t, store.ApplyOperations().UpdateState(ctx, failedID, state.ApplyOperation.Failed))

		staleCtx := storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: apply.ID, Owner: "old-driver", Token: "stale-token"})
		_, err := store.ApplyOperations().MarkPendingStoppedByApply(staleCtx, apply.ID)
		require.ErrorIs(t, err, storage.ErrApplyLeaseLost)

		ownedCtx := storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: apply.ID, Owner: apply.LeaseOwner, Token: apply.LeaseToken})
		changed, err := store.ApplyOperations().MarkPendingStoppedByApply(ownedCtx, apply.ID)
		require.NoError(t, err)
		assert.Equal(t, int64(1), changed)
		again, err := store.ApplyOperations().MarkPendingStoppedByApply(ownedCtx, apply.ID)
		require.NoError(t, err)
		assert.Zero(t, again)

		pending, err := store.ApplyOperations().Get(ctx, pendingID)
		require.NoError(t, err)
		require.NotNil(t, pending)
		assert.Equal(t, state.ApplyOperation.Stopped, pending.State)
		assert.Nil(t, pending.CompletedAt)
		running, err := store.ApplyOperations().Get(ctx, runningID)
		require.NoError(t, err)
		require.NotNil(t, running)
		assert.Equal(t, state.ApplyOperation.Running, running.State)
		completed, err := store.ApplyOperations().Get(ctx, completedID)
		require.NoError(t, err)
		require.NotNil(t, completed)
		assert.Equal(t, state.ApplyOperation.Completed, completed.State)
		failed, err := store.ApplyOperations().Get(ctx, failedID)
		require.NoError(t, err)
		require.NotNil(t, failed)
		assert.Equal(t, state.ApplyOperation.Failed, failed.State)
	})

	// MarkPendingStoppedByApply_OperationLease verifies a fan-out driver can
	// stop pending siblings through its live operation lease, while stale or
	// cross-apply lease claims fail closed.
	t.Run("MarkPendingStoppedByApply_OperationLease", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "operation_stop_fanout_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_operation_stop_fanout", 907)
		ownerID := createOperation(t, store, apply.ID, "region-a", "owner")
		pendingID := createOperation(t, store, apply.ID, "region-b", "pending")

		claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "current-driver")
		require.NoError(t, err)
		require.NotNil(t, claimed)
		require.Equal(t, ownerID, claimed.ID)
		operationCtx := func(applyID int64, token string) storage.OperationLease {
			return storage.OperationLease{
				ApplyID: applyID, OperationID: ownerID, Owner: claimed.LeaseOwner, Token: token,
			}
		}

		_, err = store.ApplyOperations().MarkPendingStoppedByApply(
			storage.WithOperationLease(ctx, operationCtx(apply.ID, "stale-token")), apply.ID)
		require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
		pending, err := store.ApplyOperations().Get(ctx, pendingID)
		require.NoError(t, err)
		require.NotNil(t, pending)
		assert.Equal(t, state.ApplyOperation.Pending, pending.State)

		_, err = store.ApplyOperations().MarkPendingStoppedByApply(
			storage.WithOperationLease(ctx, operationCtx(apply.ID+1, claimed.LeaseToken)), apply.ID)
		require.ErrorIs(t, err, storage.ErrApplyLeaseLost)

		ownedCtx := storage.WithOperationLease(ctx, operationCtx(apply.ID, claimed.LeaseToken))
		changed, err := store.ApplyOperations().MarkPendingStoppedByApply(ownedCtx, apply.ID)
		require.NoError(t, err)
		assert.Equal(t, int64(1), changed)
		pending, err = store.ApplyOperations().Get(ctx, pendingID)
		require.NoError(t, err)
		require.NotNil(t, pending)
		assert.Equal(t, state.ApplyOperation.Stopped, pending.State)
		owner, err := store.ApplyOperations().Get(ctx, ownerID)
		require.NoError(t, err)
		require.NotNil(t, owner)
		assert.Equal(t, state.ApplyOperation.Running, owner.State)
		again, err := store.ApplyOperations().MarkPendingStoppedByApply(ownedCtx, apply.ID)
		require.NoError(t, err)
		assert.Zero(t, again)
	})

	// SaveExternalID_EnforcesDeploymentInvariant verifies sibling operations
	// in one deployment share one remote apply ID, a different deployment is
	// independent, and a conflicting write fails without persisting its value.
	t.Run("SaveExternalID_EnforcesDeploymentInvariant", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "operation_external_id_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_operation_external_id", 905)
		_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: "region-a", OperationKey: "shard/-80", ExternalID: "remote-a",
		})
		require.NoError(t, err)
		siblingID := createOperation(t, store, apply.ID, "region-a", "shard/80-")
		otherID := createOperation(t, store, apply.ID, "region-b", "shard/-80")

		require.NoError(t, store.ApplyOperations().SaveExternalID(ctx, apply.ID, siblingID, "remote-a"))
		require.NoError(t, store.ApplyOperations().SaveExternalID(ctx, apply.ID, otherID, "remote-b"))
		err = store.ApplyOperations().SaveExternalID(ctx, apply.ID, siblingID, "remote-conflict")
		require.ErrorIs(t, err, storage.ErrRemoteApplyDeploymentIDConflict)

		sibling, err := store.ApplyOperations().Get(ctx, siblingID)
		require.NoError(t, err)
		require.NotNil(t, sibling)
		assert.Equal(t, "remote-a", sibling.ExternalID, "a refused write preserves the deployment's shared ID")
		other, err := store.ApplyOperations().Get(ctx, otherID)
		require.NoError(t, err)
		require.NotNil(t, other)
		assert.Equal(t, "remote-b", other.ExternalID)
	})

	// FindNextApplyOperation_ConcurrentSingleWinner verifies contending drivers
	// cannot claim one pending operation more than once.
	t.Run("FindNextApplyOperation_ConcurrentSingleWinner", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "operation_contention_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_operation_contention", 906)
		opID := createOperation(t, store, apply.ID, "region-a", "schema")

		const drivers = 8
		start := make(chan struct{})
		results := make(chan *storage.ApplyOperation, drivers)
		errs := make(chan error, drivers)
		var wg sync.WaitGroup
		for i := range drivers {
			owner := fmt.Sprintf("driver-%d", i)
			wg.Go(func() {
				<-start
				claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, owner)
				if err != nil {
					errs <- err
					return
				}
				if claimed != nil {
					results <- claimed
				}
			})
		}
		close(start)
		wg.Wait()
		close(results)
		close(errs)

		for err := range errs {
			require.NoError(t, err)
		}
		claims := make([]*storage.ApplyOperation, 0, drivers)
		for claimed := range results {
			claims = append(claims, claimed)
		}
		require.Len(t, claims, 1)
		assert.Equal(t, opID, claims[0].ID)
		assert.NotEmpty(t, claims[0].LeaseToken)
	})

	t.Run("FindNextApplyOperation_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.ApplyOperations().FindNextApplyOperation(t.Context(), "driver")
		require.Error(t, err)
	})
}
