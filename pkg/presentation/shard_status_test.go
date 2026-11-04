package presentation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
)

// A keyspace's shards run in order: while the first copies, each later shard
// waits on it, and the count reads them by status the way the PR comment's
// Shards line does.
func TestDeriveShardsOrdersTheShardsOfAKeyspace(t *testing.T) {
	shards := DeriveShards(state.Apply.Running, []ShardWork{
		{Keyspace: "shop_001", Shard: "-40", Operations: []Operation{{State: state.ApplyOperation.Running}}},
		{Keyspace: "shop_001", Shard: "40-80", Operations: []Operation{{State: state.ApplyOperation.Pending}}},
		{Keyspace: "shop_001", Shard: "80-", Operations: []Operation{{State: state.ApplyOperation.Pending}}},
	})

	require.Len(t, shards, 3)
	assert.Equal(t, "-40", shards[0].Shard)
	assert.Equal(t, "waiting for -40", shards[1].Label)
	assert.Equal(t, "waiting for -40", shards[2].Label)
	assert.Equal(t, "1 running table copy, 2 waiting for -40", ShardCounts([]string{shards[0].Label, shards[1].Label, shards[2].Label}))
}

// A shard running more than one table's change reads as its most
// attention-worthy one, with that operation's error, and a label naming a
// shard of an apply spanning keyspaces qualifies it by keyspace, since every
// unsharded keyspace's shard is "-".
func TestDeriveShardsTakesTheFailedTableAndQualifiesAcrossKeyspaces(t *testing.T) {
	shards := DeriveShards(state.Apply.Running, []ShardWork{
		{Keyspace: "shop_001", Shard: "-", Operations: []Operation{
			{State: state.ApplyOperation.Completed},
			{State: state.ApplyOperation.Failed, Error: "duplicate key"},
		}},
		{Keyspace: "shop_002", Shard: "-", Operations: []Operation{{State: state.ApplyOperation.Pending}}},
	})

	require.Len(t, shards, 2)
	assert.Equal(t, state.ApplyOperation.Failed, shards[0].State)
	assert.Equal(t, "duplicate key", shards[0].Error)
	assert.Equal(t, "shop_002", shards[1].Keyspace)
	assert.Equal(t, "halted — shop_001/- failed", shards[1].Label)
	assert.Equal(t, "1 failed, 1 halted", ShardCounts([]string{shards[0].Label, shards[1].Label}))
}

// A start moves a sharded apply to resuming before its shards' rows leave
// stopped, so through that window the shards count as resuming, not stopped.
// Stopped shards of an apply that is still stopped count as stopped.
func TestDeriveShardsReadsAStartUnderWay(t *testing.T) {
	stopped := []ShardWork{
		{Keyspace: "shop_001", Shard: "-80", Operations: []Operation{{State: state.ApplyOperation.Stopped, Parallel: true}}},
		{Keyspace: "shop_001", Shard: "80-", Operations: []Operation{{State: state.ApplyOperation.Stopped, Parallel: true}}},
	}
	labels := func(applyState string) []string {
		var out []string
		for _, s := range DeriveShards(applyState, stopped) {
			out = append(out, s.Label)
		}
		return out
	}

	assert.Equal(t, "2 resuming", ShardCounts(labels(state.Apply.Resuming)))
	assert.Equal(t, "2 stopped", ShardCounts(labels(state.Apply.Stopped)))
}
