//go:build integration

package sqlstore

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/storagetest"
)

// newShareStore returns an empty store built with opts, for one dialect.
type newShareStore func(t *testing.T, opts ...storage.Option) *Storage

// TestApplyOperationStore_FindNextApplyOperation_ProcessDriverShare runs the
// per-process driver share scenarios against MySQL.
func TestApplyOperationStore_FindNextApplyOperation_ProcessDriverShare(t *testing.T) {
	testProcessDriverShare(t, func(t *testing.T, opts ...storage.Option) *Storage {
		t.Helper()
		clearTables(t)
		return NewMySQL(testDB, opts...)
	})
}

// testProcessDriverShare verifies that one apply holds at most half of any one
// process's driver pool while starting work. The plane-wide cap is raised well
// above the shard count in every scenario, so only the share can be what
// refuses a claim.
func testProcessDriverShare(t *testing.T, newStore newShareStore) {
	// A wide sharded apply fills half of pod A's four drivers. Pod A's third
	// driver takes another database's apply instead of a third shard, and its
	// fourth finds nothing, because every claimable row left belongs to the
	// apply that is already at its share there. Other pods still take the wide
	// apply's shards, including one whose process name shares a text prefix
	// with pod A's.
	t.Run("HoldsAnApplyToHalfOfEachProcess", func(t *testing.T) {
		ctx := t.Context()
		store := newStore(t, storage.WithMaxDriversPerApply(32), storage.WithDriversPerProcess(4))
		wide := seedShardedApply(t, store, "share_wide_db", "apply_share_wide", 6)
		other := seedShardedApply(t, store, "share_other_db", "apply_share_other", 1)

		claim := func(owner string) *storage.ApplyOperation {
			t.Helper()
			got, err := store.ApplyOperations().FindNextApplyOperation(ctx, owner)
			require.NoError(t, err)
			return got
		}

		for _, owner := range []string{"pod-a/1/driver-1", "pod-a/1/driver-2"} {
			got := claim(owner)
			require.NotNil(t, got, "%s claims a shard while the apply is under its share on pod A", owner)
			assert.Equal(t, wide.ID, got.ApplyID)
		}
		third := claim("pod-a/1/driver-3")
		require.NotNil(t, third, "pod A has drivers left for another database")
		assert.Equal(t, other.ID, third.ApplyID, "pod A's third driver skips the wide apply, which holds half its pool")
		assert.Nil(t, claim("pod-a/1/driver-4"), "the wide apply's remaining shards wait for another pod")

		for _, owner := range []string{"pod-b/2/driver-1", "pod-a/11/driver-1"} {
			got := claim(owner)
			require.NotNil(t, got, "%s is a different process, so its share of the wide apply is untouched", owner)
			assert.Equal(t, wide.ID, got.ApplyID)
		}
	})

	// Every driver on one pod wakes at once and races for the same wide apply.
	// The share is exact within a process, so the pod ends up holding exactly
	// its share of the apply rather than every driver reading the pre-claim
	// count and claiming on it together.
	t.Run("ConcurrentClaimsOnOneProcessStopAtTheShare", func(t *testing.T) {
		const drivers = 8
		store := newStore(t, storage.WithMaxDriversPerApply(32), storage.WithDriversPerProcess(drivers))
		wide := seedShardedApply(t, store, "share_race_db", "apply_share_race", drivers)

		start := make(chan struct{})
		var wg sync.WaitGroup
		var mu sync.Mutex
		var claimed []*storage.ApplyOperation
		var claimErrors []error
		for i := range drivers {
			owner := fmt.Sprintf("pod-a/1/driver-%d", i)
			wg.Go(func() {
				<-start
				got, err := store.ApplyOperations().FindNextApplyOperation(t.Context(), owner)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					claimErrors = append(claimErrors, err)
					return
				}
				if got != nil {
					claimed = append(claimed, got)
				}
			})
		}
		close(start)
		wg.Wait()

		require.Empty(t, claimErrors)
		require.Len(t, claimed, storage.ProcessDriverShare(drivers), "one pod holds exactly its share of the apply")
		for _, op := range claimed {
			assert.Equal(t, wide.ID, op.ApplyID)
		}
	})

	// Starting a parked cutover takes a driver like starting a copy does, so it
	// counts against the same share. With one of its two drivers already copying
	// the apply's later shard, pod A leaves the apply's cutover to another pod
	// rather than giving the apply both of its drivers.
	t.Run("ParkedCutoverWaitsForTheShare", func(t *testing.T) {
		ctx := t.Context()
		store := newStore(t, storage.WithMaxDriversPerApply(32), storage.WithDriversPerProcess(2))
		lock := storagetest.CreateLock(t, store, "share_cutover_db", storage.DatabaseTypeMySQL)
		apply := storagetest.CreateApply(t, store, lock, "apply_share_cutover", 1)
		parkedID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce/shard-0/users",
			OperationKind: storage.ApplyOperationKindWork,
			State:         state.ApplyOperation.WaitingForCutover, CutoverPolicy: storage.CutoverPolicyBarrier,
		})
		require.NoError(t, err)
		copyingID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce/shard-1/users",
			OperationKind: storage.ApplyOperationKindWork, CutoverPolicy: storage.CutoverPolicyBarrier,
		})
		require.NoError(t, err)

		copying, err := store.ApplyOperations().FindNextApplyOperation(ctx, "pod-a/1/driver-1")
		require.NoError(t, err)
		require.NotNil(t, copying)
		require.Equal(t, copyingID, copying.ID, "pod A starts the later shard's copy")

		blocked, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "pod-a/1/driver-2")
		require.NoError(t, err)
		assert.Nil(t, blocked, "pod A already gives the apply its share, so the cutover waits for another pod")

		cutover, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "pod-b/2/driver-1")
		require.NoError(t, err)
		require.NotNil(t, cutover, "pod B has its share of the apply free")
		assert.Equal(t, parkedID, cutover.ID)
	})

	// A shard parked at the cutover barrier keeps the lease of the drive that
	// parked it until that lease goes stale, but the drive has returned and the
	// driver is free. On a two-driver pod the shard's cutover still starts there
	// at once rather than waiting out the stale lease.
	t.Run("ParkedShardsLeftoverLeaseDoesNotHoldItsCutover", func(t *testing.T) {
		ctx := t.Context()
		store := newStore(t, storage.WithMaxDriversPerApply(32), storage.WithDriversPerProcess(2))
		lock := storagetest.CreateLock(t, store, "share_parked_db", storage.DatabaseTypeMySQL)
		apply := storagetest.CreateApply(t, store, lock, "apply_share_parked", 1)
		parkedID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce/shard-0/users",
			OperationKind: storage.ApplyOperationKindWork, CutoverPolicy: storage.CutoverPolicyBarrier,
		})
		require.NoError(t, err)
		_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce/shard-1/users",
			OperationKind: storage.ApplyOperationKindWork, CutoverPolicy: storage.CutoverPolicyBarrier,
			State: state.ApplyOperation.Completed,
		})
		require.NoError(t, err)

		copied, err := store.ApplyOperations().FindNextApplyOperation(ctx, "pod-a/1/driver-1")
		require.NoError(t, err)
		require.NotNil(t, copied)
		require.Equal(t, parkedID, copied.ID)
		require.NoError(t, store.ApplyOperations().UpdateState(ctx, parkedID, state.ApplyOperation.WaitingForCutover))

		cutover, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "pod-a/1/driver-2")
		require.NoError(t, err)
		require.NotNil(t, cutover, "the parked shard's own leftover lease occupies no driver")
		assert.Equal(t, parkedID, cutover.ID)
	})

	// A claimer whose owner names no process is its own process, so it is
	// matched whole: driver-10 holding its share does not refuse driver-1,
	// whose owner is the first part of driver-10's.
	t.Run("OwnerWithoutProcessIsMatchedWhole", func(t *testing.T) {
		ctx := t.Context()
		store := newStore(t, storage.WithMaxDriversPerApply(32), storage.WithDriversPerProcess(2))
		wide := seedShardedApply(t, store, "share_bare_db", "apply_share_bare", 3)

		first, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-10")
		require.NoError(t, err)
		require.NotNil(t, first)
		assert.Equal(t, wide.ID, first.ApplyID)

		again, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-10")
		require.NoError(t, err)
		assert.Nil(t, again, "driver-10 already holds its share of the apply")

		other, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-1")
		require.NoError(t, err)
		require.NotNil(t, other, "driver-1 is a different claimer from driver-10")
		assert.Equal(t, wide.ID, other.ApplyID)
	})

	// A one-driver pool cannot be split, so the share is the whole driver: the
	// pod runs one of the apply's shards and nothing more.
	t.Run("OneDriverPoolTakesOneShard", func(t *testing.T) {
		ctx := t.Context()
		store := newStore(t, storage.WithMaxDriversPerApply(32), storage.WithDriversPerProcess(1))
		wide := seedShardedApply(t, store, "share_single_db", "apply_share_single", 3)

		first, err := store.ApplyOperations().FindNextApplyOperation(ctx, "pod-a/1/driver-1")
		require.NoError(t, err)
		require.NotNil(t, first)
		assert.Equal(t, wide.ID, first.ApplyID)

		second, err := store.ApplyOperations().FindNextApplyOperation(ctx, "pod-a/1/driver-2")
		require.NoError(t, err)
		assert.Nil(t, second, "a one-driver pool holds one shard of the apply")
	})
}

// seedShardedApply creates an apply on its own database with one pending work
// operation per shard.
func seedShardedApply(t *testing.T, store *Storage, database, applyID string, shards int) *storage.Apply {
	t.Helper()
	lock := storagetest.CreateLock(t, store, database, storage.DatabaseTypeMySQL)
	apply := storagetest.CreateApply(t, store, lock, applyID, 1)
	for i := range shards {
		_, err := store.ApplyOperations().Insert(t.Context(), &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: "region-a",
			OperationKey:  fmt.Sprintf("commerce/shard-%d/users", i),
			OperationKind: storage.ApplyOperationKindWork,
		})
		require.NoError(t, err)
	}
	return apply
}
