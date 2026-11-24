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

// A running shard whose table changes all copy no rows, such as a CREATE
// TABLE, reads as applying; a shard with any change that copies rows reads as
// copying, so the count never calls a create a table copy.
func TestDeriveShardsReadsAChangeThatCopiesNoRowsAsApplying(t *testing.T) {
	shards := DeriveShards(state.Apply.Running, []ShardWork{
		{Keyspace: "shop_001", Shard: "-80", Operations: []Operation{{State: state.ApplyOperation.Running, Parallel: true, CopiesNoRows: true}}},
		{Keyspace: "shop_001", Shard: "80-", Operations: []Operation{
			{State: state.ApplyOperation.Running, Parallel: true, CopiesNoRows: true},
			{State: state.ApplyOperation.Running, Parallel: true},
		}},
		{Keyspace: "shop_001", Shard: "c0-", Operations: []Operation{{State: state.ApplyOperation.Completed, Parallel: true, CopiesNoRows: true}}},
	})

	require.Len(t, shards, 3)
	assert.Equal(t, RunningApplying, shards[0].Label)
	assert.Equal(t, "running table copy", shards[1].Label)
	assert.Equal(t, "1 applying, 1 running table copy, 1 completed",
		ShardCounts([]string{shards[0].Label, shards[1].Label, shards[2].Label}))
}

// A shard's label follows the changes it is running. On -80 a CREATE TABLE
// runs while another table is still queued with no tasks, and on 80- a CREATE
// TABLE runs after a sibling's table copy has finished. Neither the queued
// table nor the finished copy makes the shard read as a table copy, so the
// count matches the table lines above it.
func TestDeriveShardsLabelsAShardByTheChangesItIsRunning(t *testing.T) {
	shards := DeriveShards(state.Apply.Running, []ShardWork{
		{Keyspace: "shop_001", Shard: "-80", Operations: []Operation{
			{State: state.ApplyOperation.Running, Parallel: true, CopiesNoRows: true},
			{State: state.ApplyOperation.Pending, Parallel: true},
		}},
		{Keyspace: "shop_001", Shard: "80-", Operations: []Operation{
			{State: state.ApplyOperation.Completed, Parallel: true},
			{State: state.ApplyOperation.Running, Parallel: true, CopiesNoRows: true},
		}},
	})

	require.Len(t, shards, 2)
	assert.Equal(t, RunningApplying, shards[0].Label, "the queued table does not decide")
	assert.Equal(t, RunningApplying, shards[1].Label, "the finished table copy does not decide")
}

// A shard has landed only once every table change it runs has landed. A
// shard whose first table waits out its revert window while a second table's
// change is still queued reads by its revert window state, but it is not done.
func TestDeriveShardsLandsAShardOnlyWhenEveryChangeHasLanded(t *testing.T) {
	shards := DeriveShards(state.Apply.Running, []ShardWork{
		{Keyspace: "shop_001", Shard: "-80", Operations: []Operation{
			{State: state.ApplyOperation.RevertWindow},
			{State: state.ApplyOperation.Completed},
		}},
		{Keyspace: "shop_001", Shard: "80-", Operations: []Operation{
			{State: state.ApplyOperation.RevertWindow},
			{State: state.ApplyOperation.Pending},
		}},
	})

	require.Len(t, shards, 2)
	assert.True(t, shards[0].Landed, "every change on -80 has landed")
	assert.Equal(t, state.ApplyOperation.RevertWindow, shards[1].State,
		"the shard reads as its revert window operation")
	assert.False(t, shards[1].Landed, "80- still has a queued change")
}
