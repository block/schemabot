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
	// ordering by member leaves one member's own copy starts unordered. Under rolling,
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

	// CutoverBlocker_FollowsDeploymentOrder verifies whose turn it is to cut
	// over. An earlier deployment holds a later one's cutover until it has
	// completed, whether it is still copying or parked at the cutover itself,
	// and the blocker named is the earliest one holding the turn.
	t.Run("CutoverBlocker_FollowsDeploymentOrder", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "cutover_turn_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_cutover_turn", 911)
		insert := func(deployment, opState string) int64 {
			t.Helper()
			id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
				ApplyID: apply.ID, Deployment: deployment, State: opState, CutoverPolicy: storage.CutoverPolicyBarrier,
			})
			require.NoError(t, err)
			return id
		}
		euID := insert("eu", state.ApplyOperation.WaitingForCutover)
		usID := insert("us", state.ApplyOperation.WaitingForCutover)
		auID := insert("au", state.ApplyOperation.Running)

		requireBlocker := func(operationID, wantBlockerID int64, msg string) {
			t.Helper()
			blocker, err := store.ApplyOperations().CutoverBlocker(ctx, operationID)
			require.NoError(t, err)
			if wantBlockerID == 0 {
				assert.Nil(t, blocker, msg)
				return
			}
			require.NotNil(t, blocker, msg)
			assert.Equal(t, wantBlockerID, blocker.ID, msg)
		}
		requireBlocker(euID, 0, "the first deployment's cutover is never held")
		requireBlocker(usID, euID, "a parked earlier deployment holds the next cutover")
		requireBlocker(auID, euID, "the earliest deployment holding the turn is the one named")

		require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, euID))
		requireBlocker(usID, 0, "a completed earlier deployment releases the turn")
		requireBlocker(auID, usID, "the turn passes one deployment at a time")
	})

	// CutoverBlocker_ShardSiblingsDoNotTakeTurns verifies that the shard work
	// rows of one deployment never hold each other's cutover. They share the
	// deployment's one data-plane apply, which a cutover addresses as a whole,
	// so only an earlier deployment can hold the turn.
	t.Run("CutoverBlocker_ShardSiblingsDoNotTakeTurns", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "cutover_turn_shard_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_cutover_turn_shard", 912)
		insert := func(deployment, operationKey, opState string) int64 {
			t.Helper()
			id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
				ApplyID: apply.ID, Deployment: deployment, OperationKey: operationKey, OperationKind: storage.ApplyOperationKindWork,
				State: opState, CutoverPolicy: storage.CutoverPolicyBarrier,
			})
			require.NoError(t, err)
			return id
		}
		copyingShard := insert("eu", "commerce/-80/users", state.ApplyOperation.Running)
		parkedShard := insert("eu", "commerce/80-/users", state.ApplyOperation.WaitingForCutover)
		laterDeployment := insert("us", "commerce/-80/users", state.ApplyOperation.WaitingForCutover)

		blocker, err := store.ApplyOperations().CutoverBlocker(ctx, parkedShard)
		require.NoError(t, err)
		assert.Nil(t, blocker, "a still-copying shard of the same deployment does not hold a parked shard's cutover")

		blocker, err = store.ApplyOperations().CutoverBlocker(ctx, laterDeployment)
		require.NoError(t, err)
		require.NotNil(t, blocker, "a later deployment waits for the earlier deployment's shards")
		assert.Equal(t, copyingShard, blocker.ID)
		assert.Equal(t, "eu", blocker.Deployment)
	})

	// CutoverBlocker_TargetsOfOneDeploymentTakeTurns verifies that the targets
	// of one deployment are separate members. Each target is its own database
	// with its own cutover, so an earlier target holds a later target's turn
	// exactly as an earlier deployment would, while the shards of one target
	// never hold each other's requested cutover.
	t.Run("CutoverBlocker_TargetsOfOneDeploymentTakeTurns", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "cutover_turn_targets_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_cutover_turn_targets", 916)
		insert := func(target, operationKey, opState string) int64 {
			t.Helper()
			id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
				ApplyID: apply.ID, Deployment: "primary", Target: target, OperationKey: operationKey,
				OperationKind: storage.ApplyOperationKindWork, State: opState, CutoverPolicy: storage.CutoverPolicyBarrier,
			})
			require.NoError(t, err)
			return id
		}
		copyingTarget := insert("orders-001", "orders-001", state.ApplyOperation.Running)
		parkedTarget := insert("orders-002", "orders-002/commerce/-80/users", state.ApplyOperation.WaitingForCutover)
		parkedShard := insert("orders-002", "orders-002/commerce/80-/users", state.ApplyOperation.WaitingForCutover)

		blocker, err := store.ApplyOperations().CutoverBlocker(ctx, parkedTarget)
		require.NoError(t, err)
		require.NotNil(t, blocker, "an earlier target of the same deployment holds a later target's cutover")
		assert.Equal(t, copyingTarget, blocker.ID)
		assert.Equal(t, "orders-001", blocker.Target)

		require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, copyingTarget))
		blocker, err = store.ApplyOperations().CutoverBlocker(ctx, parkedShard)
		require.NoError(t, err)
		assert.Nil(t, blocker, "the shards of one target never hold each other once the earlier target has completed")
	})

	// CutoverBlocker_FailedSiblingFollowsOnFailure verifies that a
	// terminal-failed earlier deployment holds the turn exactly as the
	// automatic cutover claim holds it: under halt it always holds, under
	// continue it never does, and under pause it holds until a release latches
	// the rollout open.
	t.Run("CutoverBlocker_FailedSiblingFollowsOnFailure", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		seed := func(identifier, onFailure string, planID int64) (*storage.Apply, int64, int64) {
			t.Helper()
			lock := CreateLock(t, store, identifier+"_db", storage.DatabaseTypeMySQL)
			apply := CreateApply(t, store, lock, identifier, planID)
			failedID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
				ApplyID: apply.ID, Deployment: "eu", State: state.ApplyOperation.Failed,
				CutoverPolicy: storage.CutoverPolicyBarrier, OnFailure: onFailure,
			})
			require.NoError(t, err)
			parkedID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
				ApplyID: apply.ID, Deployment: "us", State: state.ApplyOperation.WaitingForCutover,
				CutoverPolicy: storage.CutoverPolicyBarrier, OnFailure: onFailure,
			})
			require.NoError(t, err)
			return apply, failedID, parkedID
		}

		_, haltFailed, haltParked := seed("apply_cutover_turn_halt", storage.OnFailureHalt, 913)
		blocker, err := store.ApplyOperations().CutoverBlocker(ctx, haltParked)
		require.NoError(t, err)
		require.NotNil(t, blocker, "under halt a failed earlier deployment holds the cutover")
		assert.Equal(t, haltFailed, blocker.ID)

		_, _, continueParked := seed("apply_cutover_turn_continue", storage.OnFailureContinue, 914)
		blocker, err = store.ApplyOperations().CutoverBlocker(ctx, continueParked)
		require.NoError(t, err)
		assert.Nil(t, blocker, "under continue a failed earlier deployment does not hold the cutover")

		pauseApply, pauseFailed, pauseParked := seed("apply_cutover_turn_pause", storage.OnFailurePause, 915)
		blocker, err = store.ApplyOperations().CutoverBlocker(ctx, pauseParked)
		require.NoError(t, err)
		require.NotNil(t, blocker, "under pause a failed earlier deployment holds the cutover until released")
		assert.Equal(t, pauseFailed, blocker.ID)
		_, _, err = store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
			ApplyID: pauseApply.ID, Operation: storage.ControlOperationRelease,
			Status: storage.ControlRequestPending, RequestedBy: "cli:alice",
		})
		require.NoError(t, err)
		blocker, err = store.ApplyOperations().CutoverBlocker(ctx, pauseParked)
		require.NoError(t, err)
		assert.Nil(t, blocker, "a release lets the cutover proceed past the failed deployment")
	})

	// FindNextApplyOperation_OrderedCutoverTakesOneTargetsShardsOneAtATime
	// verifies what stays ordered inside one rollout member. Under barrier and
	// under parallel, both shards of payments-001 start their copies together,
	// but they cut over one at a time: shard 80- parks behind shard -80 even
	// though both belong to the same target. The target's finalizer waits until
	// both shards have completed.
	t.Run("FindNextApplyOperation_OrderedCutoverTakesOneTargetsShardsOneAtATime", func(t *testing.T) {
		for _, cutoverPolicy := range []string{storage.CutoverPolicyBarrier, storage.CutoverPolicyParallel} {
			t.Run(cutoverPolicy, func(t *testing.T) {
				testOneTargetsShardsCutOverOneAtATime(t, h, cutoverPolicy)
			})
		}
	})

	// FindNextApplyOperation_FinalizerWaitsOnEarlierMembers verifies that a
	// finalizer is ordered against earlier rollout members like a cutover. A
	// VSchema-only rollout over region-a and region-b has one finalizer per
	// member and no work. region-b's finalizer does not start while region-a's
	// is running, under any policy, and once region-a's fails it stays held
	// under halt, and under pause until an operator releases the rollout, so
	// region-b's VSchema never goes out behind a failed region-a. It starts once
	// region-a's completes.
	t.Run("FindNextApplyOperation_FinalizerWaitsOnEarlierMembers", func(t *testing.T) {
		for _, tc := range []struct {
			name          string
			cutoverPolicy string
			onFailure     string
			// earlierFails fails region-a's finalizer; otherwise it completes.
			earlierFails  bool
			release       bool
			laterAdmitted bool
		}{
			{name: "rolling_halt_failed", cutoverPolicy: storage.CutoverPolicyRolling, onFailure: storage.OnFailureHalt, earlierFails: true},
			{name: "rolling_pause_failed", cutoverPolicy: storage.CutoverPolicyRolling, onFailure: storage.OnFailurePause, earlierFails: true},
			{name: "rolling_pause_released", cutoverPolicy: storage.CutoverPolicyRolling, onFailure: storage.OnFailurePause, earlierFails: true, release: true, laterAdmitted: true},
			{name: "barrier_halt_failed", cutoverPolicy: storage.CutoverPolicyBarrier, onFailure: storage.OnFailureHalt, earlierFails: true},
			{name: "parallel_halt_failed", cutoverPolicy: storage.CutoverPolicyParallel, onFailure: storage.OnFailureHalt, earlierFails: true},
			{name: "parallel_halt_completed", cutoverPolicy: storage.CutoverPolicyParallel, onFailure: storage.OnFailureHalt, laterAdmitted: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx := t.Context()
				store := h.NewStorage(t)
				lock := CreateLock(t, store, "operation_finalizer_order_"+tc.name, storage.DatabaseTypeVitess)
				apply := CreateApply(t, store, lock, "apply_operation_finalizer_order_"+tc.name, 915)
				ids := make([]int64, 0, 2)
				for _, deployment := range []string{"region-a", "region-b"} {
					id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
						ApplyID: apply.ID, Deployment: deployment,
						OperationKey:  "group_finalizer",
						OperationKind: storage.ApplyOperationKindGroupFinalizer,
						CutoverPolicy: tc.cutoverPolicy, OnFailure: tc.onFailure,
					})
					require.NoError(t, err)
					ids = append(ids, id)
				}

				first, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
				require.NoError(t, err)
				require.NotNil(t, first)
				require.Equal(t, ids[0], first.ID, "region-a's finalizer starts first")

				held, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-b")
				require.NoError(t, err)
				assert.Nil(t, held, "region-b's finalizer waits while region-a's is running under %s", tc.cutoverPolicy)

				if tc.earlierFails {
					require.NoError(t, store.ApplyOperations().MarkFailed(ctx, ids[0], "vschema apply rejected"))
				} else {
					require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, ids[0]))
				}
				if tc.release {
					heldBeforeRelease, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-b")
					require.NoError(t, err)
					require.Nil(t, heldBeforeRelease, "a paused rollout holds region-b until an operator releases it")
					_, _, err = store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
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
					assert.Nil(t, next, "region-b's VSchema is never applied behind a failed region-a under %s/%s", tc.cutoverPolicy, tc.onFailure)
					return
				}
				require.NotNil(t, next, "%s admits region-b's finalizer", tc.name)
				assert.Equal(t, ids[1], next.ID)
				assert.Equal(t, "region-b", next.Deployment)
			})
		}
	})

	// FindNextApplyOperation_BarrierStartsLaterCopyPastParkedMemberFinalizer
	// verifies that an earlier member's pending finalizer does not turn barrier
	// into rolling. payments-001 and payments-002 each have two shards of
	// orders and an orders finalizer. Once payments-001's shards park at the
	// barrier, payments-002's copies start even though payments-001's finalizer
	// is still pending. What is visible stays ordered: payments-002 does not cut
	// over, and its finalizer does not start, until payments-001's finalizer has
	// completed.
	t.Run("FindNextApplyOperation_BarrierStartsLaterCopyPastParkedMemberFinalizer", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "operation_barrier_finalizer_db", storage.DatabaseTypeVitess)
		apply := CreateApply(t, store, lock, "apply_operation_barrier_finalizer", 916)
		shards := map[string][]int64{}
		finalizers := map[string]int64{}
		for _, target := range []string{"payments-001", "payments-002"} {
			for _, shard := range []string{"-80", "80-"} {
				id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
					ApplyID: apply.ID, Deployment: "payments-a", Target: target,
					OperationKey:  storage.TargetOperationKey(target, storage.ShardOperationKey("orders", shard, "orders")),
					OperationKind: storage.ApplyOperationKindWork,
					CutoverPolicy: storage.CutoverPolicyBarrier, OnFailure: storage.OnFailureHalt,
				})
				require.NoError(t, err)
				shards[target] = append(shards[target], id)
			}
			id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
				ApplyID: apply.ID, Deployment: "payments-a", Target: target,
				OperationKey:  storage.TargetOperationKey(target, "orders/group_finalizer"),
				OperationKind: storage.ApplyOperationKindGroupFinalizer,
				CutoverPolicy: storage.CutoverPolicyBarrier, OnFailure: storage.OnFailureHalt,
			})
			require.NoError(t, err)
			finalizers[target] = id
		}

		// parkAtBarrier moves a claimed shard to waiting_for_cutover and
		// releases its lease, as a copy drive does when it parks.
		parkAtBarrier := func(t *testing.T, claimed *storage.ApplyOperation) {
			t.Helper()
			require.NoError(t, store.ApplyOperations().UpdateState(ctx, claimed.ID, state.ApplyOperation.WaitingForCutover))
			released, err := store.ApplyOperations().ReleaseClaim(ctx, claimed.Lease())
			require.NoError(t, err)
			require.True(t, released)
		}

		for _, target := range []string{"payments-001", "payments-002"} {
			for _, want := range shards[target] {
				claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
				require.NoError(t, err)
				require.NotNil(t, claimed, "%s's copies start while every earlier member is at the barrier", target)
				assert.Equal(t, want, claimed.ID)
				parkAtBarrier(t, claimed)
			}
		}
		idle, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-b")
		require.NoError(t, err)
		assert.Nil(t, idle, "neither finalizer starts while its target's shards are parked")

		for _, want := range shards["payments-001"] {
			cutover, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "driver-a")
			require.NoError(t, err)
			require.NotNil(t, cutover)
			assert.Equal(t, want, cutover.ID, "payments-001 cuts over first, one shard at a time")
			require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, want))
		}

		heldCutover, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "driver-b")
		require.NoError(t, err)
		assert.Nil(t, heldCutover, "payments-002 does not cut over while payments-001's finalizer is pending")

		finalizer, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
		require.NoError(t, err)
		require.NotNil(t, finalizer)
		assert.Equal(t, finalizers["payments-001"], finalizer.ID, "payments-001's finalizer starts once its shards complete")

		stillHeld, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "driver-b")
		require.NoError(t, err)
		assert.Nil(t, stillHeld, "payments-002 does not cut over while payments-001's finalizer is running")

		require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, finalizers["payments-001"]))
		next, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "driver-b")
		require.NoError(t, err)
		require.NotNil(t, next)
		assert.Equal(t, shards["payments-002"][0], next.ID, "payments-002 cuts over once payments-001 has completed")
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

	// SaveExternalID_ScopesTheInvariantToTheTarget verifies that two targets of
	// one deployment are two rollout members: each records its own remote apply
	// ID, while a second ID for the same target is still refused.
	t.Run("SaveExternalID_ScopesTheInvariantToTheTarget", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "operation_external_id_targets_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_operation_external_id_targets", 906)
		firstID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: "default", OperationKey: "payments-001", Target: "payments-001",
		})
		require.NoError(t, err)
		secondID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: "default", OperationKey: "payments-002", Target: "payments-002",
		})
		require.NoError(t, err)

		require.NoError(t, store.ApplyOperations().SaveExternalID(ctx, apply.ID, firstID, "remote-001"))
		require.NoError(t, store.ApplyOperations().SaveExternalID(ctx, apply.ID, secondID, "remote-002"),
			"a second target of the deployment owns its own remote apply")
		err = store.ApplyOperations().SaveExternalID(ctx, apply.ID, secondID, "remote-001")
		require.ErrorIs(t, err, storage.ErrRemoteApplyDeploymentIDConflict,
			"a target's operation cannot be moved onto its sibling target's remote apply")

		second, err := store.ApplyOperations().Get(ctx, secondID)
		require.NoError(t, err)
		require.NotNil(t, second)
		assert.Equal(t, "remote-002", second.ExternalID)
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

// testOneTargetsShardsCutOverOneAtATime seeds one target with two shards of
// orders and an orders finalizer under an ordered cutover policy, and checks
// that the shards copy together, cut over one at a time, and that the
// finalizer waits for both.
func testOneTargetsShardsCutOverOneAtATime(t *testing.T, h Harness, cutoverPolicy string) {
	t.Helper()
	ctx := t.Context()
	store := h.NewStorage(t)
	lock := CreateLock(t, store, "operation_target_shard_cutover_"+cutoverPolicy, storage.DatabaseTypeMySQL)
	apply := CreateApply(t, store, lock, "apply_operation_target_shard_cutover_"+cutoverPolicy, 914)
	const target = "payments-001"
	var shardIDs []int64
	for _, shard := range []string{"-80", "80-"} {
		id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: "payments-a", Target: target,
			OperationKey:  storage.TargetOperationKey(target, storage.ShardOperationKey("orders", shard, "orders")),
			OperationKind: storage.ApplyOperationKindWork,
			CutoverPolicy: cutoverPolicy, OnFailure: storage.OnFailureHalt,
		})
		require.NoError(t, err)
		shardIDs = append(shardIDs, id)
	}
	finalizerID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "payments-a", Target: target,
		OperationKey:  storage.TargetOperationKey(target, "orders/group_finalizer"),
		OperationKind: storage.ApplyOperationKindGroupFinalizer,
		CutoverPolicy: cutoverPolicy, OnFailure: storage.OnFailureHalt,
	})
	require.NoError(t, err)

	for _, want := range shardIDs {
		claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
		require.NoError(t, err)
		require.NotNil(t, claimed, "one target's shards start their copies together")
		assert.Equal(t, want, claimed.ID)
	}
	for _, id := range shardIDs {
		require.NoError(t, store.ApplyOperations().UpdateState(ctx, id, state.ApplyOperation.WaitingForCutover))
	}

	first, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "driver-a")
	require.NoError(t, err)
	require.NotNil(t, first)
	assert.Equal(t, shardIDs[0], first.ID, "shard -80 cuts over first")

	held, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "driver-b")
	require.NoError(t, err)
	assert.Nil(t, held, "shard 80- does not cut over while shard -80 of the same target is mid-cutover")

	require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, shardIDs[0]))
	finalizerEarly, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-b")
	require.NoError(t, err)
	assert.Nil(t, finalizerEarly, "the finalizer waits while shard 80- has not completed")

	second, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "driver-b")
	require.NoError(t, err)
	require.NotNil(t, second)
	assert.Equal(t, shardIDs[1], second.ID, "shard 80- cuts over once shard -80 completes")

	require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, shardIDs[1]))
	finalizer, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
	require.NoError(t, err)
	require.NotNil(t, finalizer, "the finalizer starts once all of its target's work has completed")
	assert.Equal(t, finalizerID, finalizer.ID)
}
