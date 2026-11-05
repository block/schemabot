package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A log read for one target of a rollout resolves to that target's
// operations, names them even when the apply runs on that target alone, and
// finds none for a target the apply never ran on.
func TestLogOperationsOnTarget(t *testing.T) {
	rollout := []*ApplyOperation{
		{ID: 1, Target: "shop-001"},
		{ID: 2, Target: "shop-002"},
		{ID: 3, Target: "shop-002"},
	}

	assert.Equal(t, []int64{2, 3}, LogOperationsOnTarget(rollout, "shop-002"))
	assert.Equal(t, []int64{1}, LogOperationsOnTarget([]*ApplyOperation{{ID: 1, Target: "shop-001"}}, "shop-001"),
		"a sibling can join between listing and reading, so the only target still names its operations")
	assert.Nil(t, LogOperationsOnTarget(rollout, "shop-003"))
	assert.Equal(t, []string{"shop-001", "shop-002"}, DistinctOperationTargets(append(rollout, &ApplyOperation{ID: 4})))
}

// A log read for one shard resolves to the operations keyed to that shard,
// whether the key stands alone or behind its target, and leaves out work that
// covers no single shard: a whole-target operation and a group finalizer.
func TestLogOperationsOnShard(t *testing.T) {
	sharded := []*ApplyOperation{
		{ID: 1, Target: "shop-001", OperationKey: TargetOperationKey("shop-001", ShardOperationKey("main", "-80", "orders"))},
		{ID: 2, Target: "shop-001", OperationKey: TargetOperationKey("shop-001", ShardOperationKey("main", "80-", "orders"))},
		{ID: 3, Target: "shop-002", OperationKey: ShardOperationKey("main", "-80", "orders")},
		{ID: 4, Target: "shop-002", OperationKey: "main/group_finalizer", OperationKind: ApplyOperationKindGroupFinalizer},
		{ID: 5, Target: "shop-003", OperationKey: TargetOperationKey("shop-003", "")},
	}

	assert.Equal(t, []int64{1, 3}, LogOperationsOnShard(sharded, "-80"))
	assert.Equal(t, []int64{1}, LogOperationsOnShard(OperationsOnTarget(sharded, "shop-001"), "-80"),
		"a shard name repeats across targets, so naming the target narrows to its own")
	assert.Nil(t, LogOperationsOnShard(sharded, "c0-"))
	assert.Equal(t, []string{"-80", "80-"}, DistinctOperationShards(sharded))
	assert.Nil(t, DistinctOperationShards(OperationsOnTarget(sharded, "shop-003")))
}

func TestOperationKeyShard(t *testing.T) {
	for _, tc := range []struct {
		name   string
		key    string
		target string
		shard  string
		ok     bool
	}{
		{"shard key", ShardOperationKey("main", "-80", "orders"), "shop-001", "-80", true},
		{"shard key behind its target", TargetOperationKey("shop-001", ShardOperationKey("main", "-80", "orders")), "shop-001", "-80", true},
		{"whole-target key", TargetOperationKey("shop-001", ""), "shop-001", "", false},
		{"rollout step", TargetOperationKey("shop-001", RolloutStepOperationKey(2)), "shop-001", "", false},
		{"group finalizer", "main/group_finalizer", "shop-001", "", false},
		{"four components behind another target", "shop-002/main/-80/orders", "shop-001", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shard, ok := OperationKeyShard(tc.key, tc.target)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.shard, shard)
		})
	}
}
