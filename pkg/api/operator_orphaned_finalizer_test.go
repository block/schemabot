package api

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// TestFinalizerOrphanedByFailedWork verifies which finalizers the rollout
// derivation treats as dead. payments-001's orders finalizer is orphaned once
// one of its own shards has failed, whether it is still pending or a stop
// moved it to stopped. It is not orphaned while its shards are only parked,
// once it has started, or when the failed shard belongs to payments-002, and
// a work row is never an orphan.
func TestFinalizerOrphanedByFailedWork(t *testing.T) {
	op := func(target, scoped, kind, opState string) *storage.ApplyOperation {
		return &storage.ApplyOperation{
			Deployment: "payments-a", Target: target,
			OperationKey:  storage.TargetOperationKey(target, scoped),
			OperationKind: kind,
			State:         opState,
		}
	}
	shard := func(target, shardName, opState string) *storage.ApplyOperation {
		return op(target, storage.ShardOperationKey("orders", shardName, "orders"), storage.ApplyOperationKindWork, opState)
	}
	finalizer := func(target, opState string) *storage.ApplyOperation {
		return op(target, "orders/group_finalizer", storage.ApplyOperationKindGroupFinalizer, opState)
	}

	cases := []struct {
		name      string
		candidate *storage.ApplyOperation
		siblings  []*storage.ApplyOperation
		want      bool
	}{
		{
			name:      "pending finalizer behind its own failed shard",
			candidate: finalizer("payments-001", state.ApplyOperation.Pending),
			siblings:  []*storage.ApplyOperation{shard("payments-001", "-80", state.ApplyOperation.Failed), shard("payments-001", "80-", state.ApplyOperation.Completed)},
			want:      true,
		},
		{
			name:      "stopped finalizer behind its own failed shard",
			candidate: finalizer("payments-001", state.ApplyOperation.Stopped),
			siblings:  []*storage.ApplyOperation{shard("payments-001", "-80", state.ApplyOperation.Failed)},
			want:      true,
		},
		{
			name:      "pending finalizer behind parked shards",
			candidate: finalizer("payments-001", state.ApplyOperation.Pending),
			siblings:  []*storage.ApplyOperation{shard("payments-001", "-80", state.ApplyOperation.WaitingForCutover)},
		},
		{
			name:      "running finalizer",
			candidate: finalizer("payments-001", state.ApplyOperation.Running),
			siblings:  []*storage.ApplyOperation{shard("payments-001", "-80", state.ApplyOperation.Failed)},
		},
		{
			name:      "failed shard of another target",
			candidate: finalizer("payments-001", state.ApplyOperation.Pending),
			siblings:  []*storage.ApplyOperation{shard("payments-002", "-80", state.ApplyOperation.Failed)},
		},
		{
			name:      "work row",
			candidate: shard("payments-001", "80-", state.ApplyOperation.Pending),
			siblings:  []*storage.ApplyOperation{shard("payments-001", "-80", state.ApplyOperation.Failed)},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ops := append([]*storage.ApplyOperation{tc.candidate}, tc.siblings...)
			assert.Equal(t, tc.want, finalizerOrphanedByFailedWork(tc.candidate, ops))
		})
	}
}
