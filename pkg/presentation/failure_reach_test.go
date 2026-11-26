package presentation

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
)

// A table's failure reach counts against every shard the change spans, so a
// table whose later shards have yet to attach does not read as failed on all
// of them.
func TestFailedReachCountsShardsYetToAttach(t *testing.T) {
	failed, running := state.Task.Failed, state.Task.Running
	assert.Equal(t, " on 1 of 3 shards", FailedReach([]string{failed, running, running}, 0))
	assert.Equal(t, " on 1 of 16 shards", FailedReach([]string{failed}, 16))
	assert.Equal(t, " on 2 of 16 shards", FailedReach([]string{failed, failed, running}, 16))
	assert.Empty(t, FailedReach([]string{failed, failed}, 2), "a failure on every shard needs no count")
	assert.Empty(t, FailedReach([]string{running}, 16), "no failure, no count")
}

// Shard names repeat across keyspaces split on the same key ranges, so a
// failure names each shard with its keyspace only when a name repeats.
func TestQualifyShardNames(t *testing.T) {
	assert.Equal(t, []string{"shop_a/-80", "shop_a/80-", "shop_b/-80"}, QualifyShardNames([]ShardRef{
		{Keyspace: "shop_a", Shard: "-80"}, {Keyspace: "shop_a", Shard: "80-"}, {Keyspace: "shop_b", Shard: "-80"},
	}))
	assert.Equal(t, []string{"-", "-40", "40-"}, QualifyShardNames([]ShardRef{
		{Keyspace: "shop", Shard: "-"}, {Keyspace: "shop_sharded", Shard: "-40"}, {Keyspace: "shop_sharded", Shard: "40-"},
	}))
}
