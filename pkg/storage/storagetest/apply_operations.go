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

		// Assert the finalizer's sibling gate while only one shard is occupied.
		// With both shards running the apply is at the default fan-out cap, and a
		// nil claim would prove nothing about the gate.
		blocked, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-c")
		require.NoError(t, err)
		require.NotNil(t, blocked, "a sibling shard is claimable while the first still runs")
		assert.Equal(t, shardB, blocked.ID, "the finalizer waits for every work sibling, so the next claim is the second shard")
		assert.NotEqual(t, first.LeaseToken, blocked.LeaseToken, "each shard claim rotates its own lease")

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

	// insertTargetMembers inserts one work operation per target of a single
	// deployment, the shape an environment-level targets list resolves to, and
	// returns their IDs in list order.
	insertTargetMembers := func(t *testing.T, store storage.Storage, applyID int64, cutoverPolicy, onFailure string, targets ...string) []int64 {
		t.Helper()
		ids := make([]int64, len(targets))
		for i, target := range targets {
			id, err := store.ApplyOperations().Insert(t.Context(), &storage.ApplyOperation{
				ApplyID: applyID, Deployment: "payments-a", Target: target,
				OperationKey:  storage.TargetOperationKey(target, ""),
				OperationKind: storage.ApplyOperationKindWork,
				CutoverPolicy: cutoverPolicy, OnFailure: onFailure,
			})
			require.NoError(t, err)
			ids[i] = id
		}
		return ids
	}

	// FindNextApplyOperation_RollingOrdersTargetsOfOneDeployment verifies that
	// the targets of one deployment are rollout members in their own right. A
	// rolling rollout over payments-001..003 starts one target at a time, in
	// list order, even though every target shares the deployment.
	t.Run("FindNextApplyOperation_RollingOrdersTargetsOfOneDeployment", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "operation_target_rolling_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_operation_target_rolling", 910)
		ids := insertTargetMembers(t, store, apply.ID, storage.CutoverPolicyRolling, storage.OnFailureHalt,
			"payments-001", "payments-002", "payments-003")

		for i, id := range ids {
			claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
			require.NoError(t, err)
			require.NotNil(t, claimed, "target %d is next in order", i+1)
			assert.Equal(t, id, claimed.ID)
			assert.Equal(t, fmt.Sprintf("payments-%03d", i+1), claimed.Target)

			blocked, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-b")
			require.NoError(t, err)
			assert.Nil(t, blocked, "no later target starts while payments-%03d is still running", i+1)

			require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, id))
		}
	})

	// FindNextApplyOperation_OnFailureGatesLaterTargets verifies that on_failure
	// governs the targets of one deployment. When payments-001 fails, halt and
	// pause keep payments-002 and payments-003 from starting, an operator's
	// release lets a paused rollout go on to payments-002, and continue goes on
	// to it without one.
	t.Run("FindNextApplyOperation_OnFailureGatesLaterTargets", func(t *testing.T) {
		for _, tc := range []struct {
			name          string
			onFailure     string
			release       bool
			laterAdmitted bool
		}{
			{name: "halt", onFailure: storage.OnFailureHalt},
			{name: "pause", onFailure: storage.OnFailurePause},
			{name: "pause_released", onFailure: storage.OnFailurePause, release: true, laterAdmitted: true},
			{name: "continue", onFailure: storage.OnFailureContinue, laterAdmitted: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx := t.Context()
				store := h.NewStorage(t)
				lock := CreateLock(t, store, "operation_target_failure_"+tc.name, storage.DatabaseTypeMySQL)
				apply := CreateApply(t, store, lock, "apply_operation_target_failure_"+tc.name, 911)
				ids := insertTargetMembers(t, store, apply.ID, storage.CutoverPolicyRolling, tc.onFailure,
					"payments-001", "payments-002", "payments-003")

				first, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
				require.NoError(t, err)
				require.NotNil(t, first)
				require.Equal(t, ids[0], first.ID)
				require.NoError(t, store.ApplyOperations().MarkFailed(ctx, ids[0], "duplicate key name 'idx_orders_source'"))

				if tc.release {
					_, _, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
						ApplyID:     apply.ID,
						Operation:   storage.ControlOperationRelease,
						Status:      storage.ControlRequestPending,
						RequestedBy: "operator-a",
						Metadata:    []byte(`{}`),
					})
					require.NoError(t, err)
				}

				next, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-b")
				require.NoError(t, err)
				if !tc.laterAdmitted {
					assert.Nil(t, next, "on_failure %s keeps every later target from starting after payments-001 failed", tc.onFailure)
					return
				}
				require.NotNil(t, next, "%s goes on to payments-002 after payments-001 failed", tc.name)
				assert.Equal(t, ids[1], next.ID)
				assert.Equal(t, "payments-002", next.Target)

				afterNext, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-c")
				require.NoError(t, err)
				assert.Nil(t, afterNext, "payments-003 still waits for payments-002 under rolling")
			})
		}
	})

	// FindNextApplyOperation_ParallelStartsTargetsUpToCapAndCutsOverInOrder
	// verifies a parallel rollout over the targets of one deployment. Copies
	// start without waiting on earlier targets, bounded by the per-apply driver
	// cap, so a third target queues until a slot frees. Cutover stays one target
	// at a time in list order: payments-003 parked at the barrier does not cut
	// over while payments-002, earlier in the list, is still parked.
	t.Run("FindNextApplyOperation_ParallelStartsTargetsUpToCapAndCutsOverInOrder", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "operation_target_parallel_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_operation_target_parallel", 912)
		require.GreaterOrEqual(t, storage.DefaultMaxDriversPerApply, 2,
			"the cutover half needs two targets parked behind the first")
		targets := make([]string, storage.DefaultMaxDriversPerApply+1)
		for i := range targets {
			targets[i] = fmt.Sprintf("payments-%03d", i+1)
		}
		ids := insertTargetMembers(t, store, apply.ID, storage.CutoverPolicyParallel, storage.OnFailureHalt, targets...)

		for i := range storage.DefaultMaxDriversPerApply {
			claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
			require.NoError(t, err)
			require.NotNil(t, claimed, "parallel starts %s while earlier targets still copy", targets[i])
			assert.Equal(t, ids[i], claimed.ID)
		}
		capped, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-b")
		require.NoError(t, err)
		assert.Nil(t, capped, "the last target queues behind the per-apply driver cap")

		require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, ids[0]))
		last := len(ids) - 1
		queued, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-b")
		require.NoError(t, err)
		require.NotNil(t, queued, "a freed driver slot starts the queued target")
		assert.Equal(t, ids[last], queued.ID)

		for _, id := range ids[1:] {
			require.NoError(t, store.ApplyOperations().UpdateState(ctx, id, state.ApplyOperation.WaitingForCutover))
		}
		cutover, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "driver-a")
		require.NoError(t, err)
		require.NotNil(t, cutover)
		assert.Equal(t, ids[1], cutover.ID, "the earliest parked target cuts over first")

		held, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "driver-b")
		require.NoError(t, err)
		assert.Nil(t, held, "a later target does not cut over while an earlier one is mid-cutover")

		require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, ids[1]))
		next, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "driver-b")
		require.NoError(t, err)
		require.NotNil(t, next)
		assert.Equal(t, ids[2], next.ID, "the next target in list order cuts over once the earlier one completes")
	})

	// FindNextApplyOperation_RollingKeepsOneTargetsShardsTogether verifies that
	// ordering by member leaves one member's own work unordered. Under rolling,
	// both shards of payments-001 start together, and payments-002's shards wait
	// until both have completed.
	t.Run("FindNextApplyOperation_RollingKeepsOneTargetsShardsTogether", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "operation_target_shards_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_operation_target_shards", 913)
		ids := map[string][]int64{}
		for _, target := range []string{"payments-001", "payments-002"} {
			for _, shard := range []string{"-80", "80-"} {
				id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
					ApplyID: apply.ID, Deployment: "payments-a", Target: target,
					OperationKey:  storage.TargetOperationKey(target, storage.ShardOperationKey("orders", shard, "orders")),
					OperationKind: storage.ApplyOperationKindWork,
					CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt,
				})
				require.NoError(t, err)
				ids[target] = append(ids[target], id)
			}
		}

		for _, want := range ids["payments-001"] {
			claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
			require.NoError(t, err)
			require.NotNil(t, claimed, "a target's shards start together")
			assert.Equal(t, want, claimed.ID)
		}
		require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, ids["payments-001"][0]))
		blocked, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-b")
		require.NoError(t, err)
		assert.Nil(t, blocked, "payments-002 waits for every shard of payments-001")

		require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, ids["payments-001"][1]))
		for _, want := range ids["payments-002"] {
			claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-b")
			require.NoError(t, err)
			require.NotNil(t, claimed)
			assert.Equal(t, want, claimed.ID)
		}
	})

	// FindNextApplyOperation_CapsDriversPerApply verifies that one wide fan-out
	// cannot take the whole driver pool. A sharded apply with more claimable
	// shards than the cap allows occupies exactly the cap's worth of drivers, so
	// other databases' work still gets claimed while it runs.
	t.Run("FindNextApplyOperation_CapsDriversPerApply", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "operation_cap_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_operation_cap", 909)

		shards := storage.DefaultMaxDriversPerApply + 2
		for i := range shards {
			_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
				ApplyID: apply.ID, Deployment: "region-a",
				OperationKey:  fmt.Sprintf("commerce/shard-%d/users", i),
				OperationKind: storage.ApplyOperationKindWork,
			})
			require.NoError(t, err)
		}

		claimed := 0
		for range shards + 1 {
			got, claimErr := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
			require.NoError(t, claimErr)
			if got == nil {
				break
			}
			assert.Equal(t, apply.ID, got.ApplyID)
			claimed++
		}
		assert.Equal(t, storage.DefaultMaxDriversPerApply, claimed, "a wide apply occupies exactly the cap's worth of drivers, not every claimable shard")
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

	// ApplyIdentifierForRemoteApply_NamesTheApplyThisPlaneDispatched verifies the
	// correlation an operator depends on when another change holds their
	// database: the data plane names the holder by its own identifier, and this
	// turns it back into the handle the control-plane CLI accepts. Sibling
	// operations of one deployment share the remote apply, so the shared shape is
	// still one answer.
	t.Run("ApplyIdentifierForRemoteApply_NamesTheApplyThisPlaneDispatched", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "operation_holder_lookup_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_operation_holder_lookup", 921)
		first := createOperation(t, store, apply.ID, "region-a", "shard/-80")
		sibling := createOperation(t, store, apply.ID, "region-a", "shard/80-")
		require.NoError(t, store.ApplyOperations().SaveExternalID(ctx, apply.ID, first, "remote-holder"))
		require.NoError(t, store.ApplyOperations().SaveExternalID(ctx, apply.ID, sibling, "remote-holder"))

		identifier, err := store.ApplyOperations().ApplyIdentifierForRemoteApply(ctx, "remote-holder")
		require.NoError(t, err)
		assert.Equal(t, "apply_operation_holder_lookup", identifier)
	})

	// ApplyIdentifierForRemoteApply_OffersNoHandleForWorkThisPlaneDidNotStart
	// verifies the empty answer stays empty. A remote apply this control plane
	// never dispatched belongs to a direct engine run or another control plane,
	// and an operation that recorded no remote identifier must not be matched by
	// a caller that has none either — both would name the wrong schema change.
	t.Run("ApplyIdentifierForRemoteApply_OffersNoHandleForWorkThisPlaneDidNotStart", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "operation_holder_unknown_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_operation_holder_unknown", 922)
		createOperation(t, store, apply.ID, "region-a", "shard/-80")

		identifier, err := store.ApplyOperations().ApplyIdentifierForRemoteApply(ctx, "remote-never-dispatched")
		require.NoError(t, err)
		assert.Empty(t, identifier)

		identifier, err = store.ApplyOperations().ApplyIdentifierForRemoteApply(ctx, "")
		require.NoError(t, err)
		assert.Empty(t, identifier, "an empty identifier correlates to nothing, not to every operation that recorded none")
	})

	// ApplyIdentifierForRemoteApply_RefusesToGuessBetweenTwoApplies verifies an
	// already-broken correlation is reported rather than resolved. One remote
	// apply belonging to two applies means the deployment invariant was violated
	// upstream, and naming either would send an operator to the wrong change.
	t.Run("ApplyIdentifierForRemoteApply_RefusesToGuessBetweenTwoApplies", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		firstLock := CreateLock(t, store, "operation_holder_ambiguous_db", storage.DatabaseTypeMySQL)
		secondLock := CreateLock(t, store, "operation_holder_ambiguous_other_db", storage.DatabaseTypeMySQL)
		first := CreateApply(t, store, firstLock, "apply_operation_holder_first", 923)
		second := CreateApply(t, store, secondLock, "apply_operation_holder_second", 924)
		firstOp := createOperation(t, store, first.ID, "region-a", "shard/-80")
		secondOp := createOperation(t, store, second.ID, "region-a", "shard/-80")
		require.NoError(t, store.ApplyOperations().SaveExternalID(ctx, first.ID, firstOp, "remote-shared"))
		require.NoError(t, store.ApplyOperations().SaveExternalID(ctx, second.ID, secondOp, "remote-shared"))

		identifier, err := store.ApplyOperations().ApplyIdentifierForRemoteApply(ctx, "remote-shared")
		require.ErrorIs(t, err, storage.ErrRemoteApplyDeploymentIDConflict)
		assert.Empty(t, identifier)
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

	// PlanIDRoundTrip verifies which plan each member of an apply executes. A
	// member planned together with its siblings stores no plan of its own and
	// resolves to the apply's plan, while a member planned against its own live
	// schema carries that plan on its row and keeps it across a claim.
	t.Run("PlanIDRoundTrip", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "operation_plan_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_operation_plan", 910)

		sharedID := createOperation(t, store, apply.ID, "region-a", "schema")
		ownID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: "region-b", OperationKey: "schema", PlanID: 911,
		})
		require.NoError(t, err)

		shared, err := store.ApplyOperations().Get(ctx, sharedID)
		require.NoError(t, err)
		require.NotNil(t, shared)
		assert.Zero(t, shared.PlanID, "a member planned with its siblings stores no plan of its own")
		sharedPlan, err := storage.PlanIDForOperation(apply, shared)
		require.NoError(t, err)
		assert.Equal(t, int64(910), sharedPlan)

		own, err := store.ApplyOperations().Get(ctx, ownID)
		require.NoError(t, err)
		require.NotNil(t, own)
		assert.Equal(t, int64(911), own.PlanID)
		ownPlan, err := storage.PlanIDForOperation(apply, own)
		require.NoError(t, err)
		assert.Equal(t, int64(911), ownPlan)

		claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
		require.NoError(t, err)
		require.NotNil(t, claimed)
		assert.Equal(t, sharedID, claimed.ID)
		assert.Zero(t, claimed.PlanID)
		require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, sharedID))

		claimed, err = store.ApplyOperations().FindNextApplyOperation(ctx, "driver-b")
		require.NoError(t, err)
		require.NotNil(t, claimed)
		assert.Equal(t, ownID, claimed.ID)
		assert.Equal(t, int64(911), claimed.PlanID, "a member's own plan survives the claim")
	})

	t.Run("FindNextApplyOperation_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.ApplyOperations().FindNextApplyOperation(t.Context(), "driver")
		require.Error(t, err)
	})
}
