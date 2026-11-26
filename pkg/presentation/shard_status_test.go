package presentation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/mysqlerr"
	"github.com/block/schemabot/pkg/state"
)

// The shards of one target copy in parallel, and the claim gate holds a
// pending shard back only at the apply's driver cap. With eight shards and a
// cap of two, two shards copy and the other six read as queued, never as
// waiting for a sibling shard.
func TestDeriveShardsQueuesTheShardsTheDriverCapHoldsBack(t *testing.T) {
	names := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	work := make([]ShardWork, 0, len(names))
	for i, name := range names {
		opState := state.ApplyOperation.Pending
		if i < 2 {
			opState = state.ApplyOperation.Running
		}
		work = append(work, ShardWork{Keyspace: "shop_001", Shard: name, Operations: []Operation{
			{Deployment: "default", State: opState, Barrier: true},
		}})
	}

	shards := DeriveShards(state.Apply.Running, work)

	require.Len(t, shards, 8)
	labels := make([]string, 0, len(shards))
	for i, s := range shards {
		assert.Equal(t, names[i], s.Shard)
		labels = append(labels, s.Label)
	}
	assert.Equal(t, "running table copy", shards[1].Label)
	assert.Equal(t, PendingQueued, shards[2].Label)
	assert.Equal(t, PendingQueued, shards[7].Label)
	assert.Equal(t, "2 running table copy, 6 queued", ShardCounts(labels))
}

// A failed shard holds the other shards of its target that have not started,
// since the claim gate holds a member's unstarted work once any of it has
// failed: the pending shard reads as halted behind the failure, while the
// apply runs and after it has settled. Under on_failure=pause it reads as
// paused with the release the operator needs, and under continue, or once the
// pause is released, it reads as queued, since the claim lets it start. A
// shard reads as its most attention-worthy table, with that table's error.
func TestDeriveShardsTakesTheFailedTableAndHoldsItsTarget(t *testing.T) {
	work := []ShardWork{
		{Keyspace: "shop_001", Shard: "-", Operations: []Operation{
			{Deployment: "default", State: state.ApplyOperation.Completed},
			{Deployment: "default", State: state.ApplyOperation.Failed, Error: "duplicate key"},
		}},
		{Keyspace: "shop_002", Shard: "-", Operations: []Operation{{Deployment: "default", State: state.ApplyOperation.Pending}}},
	}

	running := DeriveShards(state.Apply.Running, work)
	require.Len(t, running, 2)
	assert.Equal(t, state.ApplyOperation.Failed, running[0].State)
	assert.Equal(t, "duplicate key", running[0].Error)
	assert.Equal(t, "halted — shop_001/- failed", running[1].Label)
	assert.Equal(t, "1 failed, 1 halted", ShardCounts([]string{running[0].Label, running[1].Label}))

	settled := DeriveShards(state.Apply.Failed, work)
	require.Len(t, settled, 2)
	assert.Equal(t, "halted — shop_001/- failed", settled[1].Label)
	assert.Equal(t, "1 failed, 1 halted", ShardCounts([]string{settled[0].Label, settled[1].Label}))

	withPolicy := func(set func(*Operation)) []ShardWork {
		out := make([]ShardWork, len(work))
		for i, sw := range work {
			ops := make([]Operation, len(sw.Operations))
			for j, op := range sw.Operations {
				set(&op)
				ops[j] = op
			}
			out[i] = ShardWork{Keyspace: sw.Keyspace, Shard: sw.Shard, Operations: ops}
		}
		return out
	}
	paused := DeriveShards(state.Apply.Paused, withPolicy(func(op *Operation) { op.PauseOnFailure = true }))
	assert.Equal(t, "paused — shop_001/- failed; release or stop", paused[1].Label)

	released := DeriveShards(state.Apply.Running, withPolicy(func(op *Operation) { op.PauseOnFailure, op.Released = true, true }))
	assert.Equal(t, PendingQueued, released[1].Label)

	continued := DeriveShards(state.Apply.Running, withPolicy(func(op *Operation) { op.ContinueOnFailure = true }))
	assert.Equal(t, PendingQueued, continued[1].Label)
}

// Shards that span rollout members keep the ordering the claim gate applies
// across members: a later member's shard is halted behind an earlier member's
// failure, and the label qualifies the shard by keyspace, since every
// unsharded keyspace's shard is "-".
func TestDeriveShardsOrdersShardsAcrossMembers(t *testing.T) {
	shards := DeriveShards(state.Apply.Running, []ShardWork{
		{Keyspace: "shop_001", Shard: "-", Operations: []Operation{{Deployment: "us", State: state.ApplyOperation.Failed, Error: "duplicate key"}}},
		{Keyspace: "shop_002", Shard: "-", Operations: []Operation{{Deployment: "eu", State: state.ApplyOperation.Pending}}},
	})

	require.Len(t, shards, 2)
	assert.Equal(t, "halted — shop_001/- failed", shards[1].Label)
	assert.Equal(t, "1 failed, 1 halted", ShardCounts([]string{shards[0].Label, shards[1].Label}))
}

// In an apply with two rollout members, each with two shards, a member's
// shards never order one another: us's second shard reads as queued behind
// nothing while its first copies. When that first shard fails, us's second
// halts behind it, since the claim holds a member's unstarted shards once one
// of them has failed. eu's shards still wait for, or halt behind, us's shards,
// since the claim gate orders copy starts across members, and eu's second
// shard names us's shard rather than eu's own first.
func TestDeriveShardsOrdersOnlyAcrossMembers(t *testing.T) {
	derive := func(firstState string) []string {
		op := func(deployment, opState string) []Operation {
			return []Operation{{Deployment: deployment, State: opState}}
		}
		var labels []string
		for _, s := range DeriveShards(state.Apply.Running, []ShardWork{
			{Keyspace: "shop_001", Shard: "-40", Operations: op("us", firstState)},
			{Keyspace: "shop_001", Shard: "40-", Operations: op("us", state.ApplyOperation.Pending)},
			{Keyspace: "shop_002", Shard: "-40", Operations: op("eu", state.ApplyOperation.Pending)},
			{Keyspace: "shop_002", Shard: "40-", Operations: op("eu", state.ApplyOperation.Pending)},
		}) {
			labels = append(labels, s.Label)
		}
		return labels
	}

	running := derive(state.ApplyOperation.Running)
	require.Len(t, running, 4)
	assert.Equal(t, PendingQueued, running[1], "us's second shard waits only for a free driver")
	assert.Equal(t, "waiting for shop_001/-40", running[2])
	assert.Equal(t, "waiting for shop_001/-40", running[3], "eu's second shard names us's shard, not eu's own")

	failed := derive(state.ApplyOperation.Failed)
	require.Len(t, failed, 4)
	assert.Equal(t, "halted — shop_001/-40 failed", failed[1], "a failure on us's first shard holds us's second")
	assert.Equal(t, "halted — shop_001/-40 failed", failed[2])
	assert.Equal(t, "halted — shop_001/-40 failed", failed[3])
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

// A sharded apply runs the same change on every shard of a keyspace, so a
// failed shard's generic reason names the shard, on the PR comment and in the
// CLI alike, since both read a shard through DeriveShards. A reason chosen by
// its code already says what to do, so it reads the same as on any target.
func TestDeriveShardsNamesTheShardInTheGenericReason(t *testing.T) {
	duplicates := mysqlerr.ReasonFromText("Error 1062 (23000): Duplicate entry 'x' for key 'uk_token'")
	work := []ShardWork{
		{Keyspace: "commerce", Shard: "-80", Operations: []Operation{
			{Deployment: "default", State: state.ApplyOperation.Failed, Error: mysqlerr.Generic},
		}},
		{Keyspace: "commerce", Shard: "80-", Operations: []Operation{
			{Deployment: "default", State: state.ApplyOperation.Failed, Error: duplicates},
		}},
	}

	shards := DeriveShards(state.Apply.Failed, work)
	require.Len(t, shards, 2)
	assert.Equal(t, mysqlerr.GenericOnShard, shards[0].Error)
	assert.Equal(t, duplicates, shards[1].Error)
}
