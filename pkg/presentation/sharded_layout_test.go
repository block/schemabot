package presentation

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The stored plan is consulted only once the keys alone admit the single-shard
// layout, so a caller can read it lazily: a keyspace across two shards never
// reads the plan, and a finalizer with nothing beside it never does either.
func TestReadsAsSingleShardConsultsThePlanOnlyWhenTheKeysAdmitIt(t *testing.T) {
	var consulted []string
	finalizesOnly := func(ns string) bool {
		consulted = append(consulted, ns)
		return true
	}

	assert.True(t, ReadsAsSingleShard([]string{"ks/-/orders", "ks/group_finalizer"}, finalizesOnly))
	assert.Equal(t, []string{"ks"}, consulted)

	consulted = nil
	assert.False(t, ReadsAsSingleShard([]string{"ks/-80/orders", "ks/80-/orders", "ks/group_finalizer"}, finalizesOnly))
	assert.False(t, ReadsAsSingleShard([]string{"ks1/-/orders", "ks2/group_finalizer"}, finalizesOnly))
	assert.Empty(t, consulted, "keys that already decide the shard layout leave the plan unread")

	assert.False(t, ReadsAsSingleShard([]string{"ks/-/orders", "ks/group_finalizer"}, func(string) bool { return false }),
		"a finalizer with a VSchema change to show keeps the shard layout")
}
