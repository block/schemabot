package presentation

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
)

// A table copying on some shards and queued on the rest reads as copying, and
// its row figures cover only the shards that reported a total: a shard with
// copied rows but no total yet adds nothing, and the ETA is the slowest
// reporting shard's.
func TestRollUpShardedTable(t *testing.T) {
	got := RollUpShardedTable([]ShardCopy{
		{Status: state.Task.Completed, RowsCopied: 1000, RowsTotal: 1000, PercentComplete: 100},
		{Status: state.Task.Running, RowsCopied: 300, RowsTotal: 1200, ETASeconds: 90},
		{Status: state.Task.Running, RowsCopied: 50, ETASeconds: 400},
		{Status: state.Task.Pending},
	}, 0)

	assert.Equal(t, ShardedTableCopy{Status: state.Task.Running, RowsCopied: 1300, RowsTotal: 2200, ETASeconds: 90, ShardsReporting: 2}, got)
	assert.Equal(t, state.Task.Failed, RollUpShardedTable([]ShardCopy{{Status: state.Task.Running}, {Status: state.Task.Failed}}, 0).Status,
		"a failed shard outranks one still copying")
	assert.Equal(t, state.Task.Pending, RollUpShardedTable([]ShardCopy{{Status: state.Task.RevertWindow}, {Status: state.Task.Pending}}, 0).Status,
		"a table with an undispatched shard has work ahead of it")
}

// A table copying on five of eight shards, with three queued behind them,
// counts the plan's estimate for the three in its total, so it reads as the
// fraction of the whole table it has copied rather than of the five shards.
// The estimate stands in only while every shard still ahead of its copy has
// one and every planned shard is listed; otherwise the figures cover the
// reporting shards alone. A shard that failed before it reported a total adds
// nothing, since it will not copy its rows.
func TestRollUpShardedTableCountsPlannedRowsForShardsStillToCopy(t *testing.T) {
	rows := func(n int64) *int64 { return &n }
	eightShards := func() []ShardCopy {
		var shards []ShardCopy
		for range 5 {
			shards = append(shards, ShardCopy{Status: state.Task.Running, RowsCopied: 400, RowsTotal: 1000, ETASeconds: 60, PlannedRows: rows(1000)})
		}
		for range 3 {
			shards = append(shards, ShardCopy{Status: state.Task.Pending, PlannedRows: rows(1000)})
		}
		return shards
	}

	got := RollUpShardedTable(eightShards(), 8)
	assert.Equal(t, ShardedTableCopy{Status: state.Task.Running, RowsCopied: 2000, RowsTotal: 8000, ETASeconds: 60, ShardsReporting: 5, RowsTotalPlanned: true}, got)

	failed := eightShards()
	failed[7] = ShardCopy{Status: state.Task.Failed, PlannedRows: rows(1000)}
	got = RollUpShardedTable(failed, 8)
	assert.Equal(t, int64(7000), got.RowsTotal, "a failed shard's estimate is not counted")
	assert.True(t, got.RowsTotalPlanned)

	missing := eightShards()
	missing[6].PlannedRows = nil
	got = RollUpShardedTable(missing, 8)
	assert.Equal(t, int64(5000), got.RowsTotal, "one unreported shard without an estimate keeps the reported figures")
	assert.False(t, got.RowsTotalPlanned)

	got = RollUpShardedTable(eightShards(), 10)
	assert.Equal(t, int64(5000), got.RowsTotal, "planned shards not yet listed keep the reported figures")
	assert.False(t, got.RowsTotalPlanned)

	queued := eightShards()
	for i := range queued {
		queued[i] = ShardCopy{Status: state.Task.Pending, PlannedRows: rows(1000)}
	}
	got = RollUpShardedTable(queued, 8)
	assert.Equal(t, ShardedTableCopy{Status: state.Task.Pending}, got, "a table with no shard copying keeps no total")

	allReported := eightShards()[:5]
	got = RollUpShardedTable(allReported, 5)
	assert.Equal(t, int64(5000), got.RowsTotal)
	assert.False(t, got.RowsTotalPlanned, "a table whose every shard reported has an exact total")
}

// A shard operation reads as its most attention-worthy task, and as its own
// state while it has no task or its task has reported no state.
func TestShardOperationCopy(t *testing.T) {
	assert.Equal(t, ShardCopy{Status: state.ApplyOperation.Pending}, ShardOperationCopy(state.ApplyOperation.Pending, nil))
	assert.Equal(t, ShardCopy{Status: state.ApplyOperation.Running, RowsTotal: 10},
		ShardOperationCopy(state.ApplyOperation.Running, []ShardCopy{{RowsTotal: 10}}))
	assert.Equal(t, ShardCopy{Status: state.Task.Failed, RowsCopied: 5},
		ShardOperationCopy(state.ApplyOperation.Running, []ShardCopy{{Status: state.Task.Completed}, {Status: state.Task.Failed, RowsCopied: 5}}))
}

// A shard whose operation is running reads as running even while its task row
// still says pending: a driver has dispatched it, and the task row only
// catches up on the next progress poll. A pending task under a pending
// operation stays queued.
func TestShardOperationCopyReadsADispatchedShardAsRunning(t *testing.T) {
	assert.Equal(t, state.ApplyOperation.Running,
		ShardOperationCopy(state.ApplyOperation.Running, []ShardCopy{{Status: state.Task.Pending}}).Status)
	assert.Equal(t, state.Task.Pending,
		ShardOperationCopy(state.ApplyOperation.Pending, []ShardCopy{{Status: state.Task.Pending}}).Status)
	assert.Equal(t, state.Task.Checksumming,
		ShardOperationCopy(state.ApplyOperation.Running, []ShardCopy{{Status: state.Task.Pending}, {Status: state.Task.Checksumming}}).Status,
		"a pending sibling does not hide a task that has reported a later step")

	// 97 shards landed and 31 dispatched with pending task rows: the table
	// reads as in progress, not queued.
	var shards []ShardCopy
	for range 97 {
		shards = append(shards, ShardOperationCopy(state.ApplyOperation.Completed, []ShardCopy{{Status: state.Task.Completed}}))
	}
	for range 31 {
		shards = append(shards, ShardOperationCopy(state.ApplyOperation.Running, []ShardCopy{{Status: state.Task.Pending}}))
	}
	assert.Equal(t, state.Task.Running, RollUpShardedTable(shards, len(shards)).Status)
}

// A finalizer whose rollout ended without running it must not read as
// pending, whichever record says so: a cancelled/reverted operation row
// (written by the cancel path or mirrored by the stranded-operation reaper)
// reads as cancelled, and so does a row still pending under a parent whose
// verdict is final — a halted rollout terminalizes the apply immediately,
// while the reaper only settles the stranded row minutes after the summary
// posted. Stopped — on the operation or the parent — reads as stopped: a
// stopped apply is resumable, so its finalizer may yet run.
func TestFinalizerVSchemaStatusTerminalInertStates(t *testing.T) {
	running := state.Apply.Running
	assert.Equal(t, "cancelled", FinalizerVSchemaStatus(running, state.ApplyOperation.Cancelled))
	assert.Equal(t, "cancelled", FinalizerVSchemaStatus(running, state.ApplyOperation.Reverted))
	assert.Equal(t, "stopped", FinalizerVSchemaStatus(running, state.ApplyOperation.Stopped))
	assert.Equal(t, "", FinalizerVSchemaStatus(running, state.ApplyOperation.Pending),
		"a pending finalizer under a live apply still reads as pending")

	pending := state.ApplyOperation.Pending
	assert.Equal(t, "cancelled", FinalizerVSchemaStatus(state.Apply.Failed, pending),
		"a halted rollout's terminal summary must not promise VSchema work no claim arm will run")
	assert.Equal(t, "cancelled", FinalizerVSchemaStatus(state.Apply.Cancelled, pending))
	assert.Equal(t, "stopped", FinalizerVSchemaStatus(state.Apply.Stopped, pending),
		"a stopped apply's pending finalizer may yet run on resume")
	assert.Equal(t, "failed", FinalizerVSchemaStatus(state.Apply.Failed, state.ApplyOperation.Failed),
		"the operation's own failure outranks the parent verdict")
}
