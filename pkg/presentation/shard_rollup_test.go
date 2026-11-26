package presentation

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
)

// A table copying on some shards and queued on the rest reads as copying, and
// its row figures cover only the shards that reported a total: a shard with
// copied rows but no total yet adds nothing, and the ETA is the time the
// reporting shards take together.
func TestRollUpShardedTable(t *testing.T) {
	got := RollUpShardedTable([]ShardCopy{
		{Status: state.Task.Completed, RowsCopied: 1000, RowsTotal: 1000, PercentComplete: 100},
		{Status: state.Task.Running, RowsCopied: 300, RowsTotal: 1200, ETASeconds: 90},
		{Status: state.Task.Running, RowsCopied: 50, ETASeconds: 400},
		{Status: state.Task.Pending},
	}, 0)

	// The shard with no row total yet still reports 400s left, and the table
	// finishes when it does.
	assert.Equal(t, ShardedTableCopy{Status: state.Task.Running, RowsCopied: 1300, RowsTotal: 2200, ETASeconds: 400, ShardsReporting: 2}, got)
	assert.Equal(t, state.Task.Failed, RollUpShardedTable([]ShardCopy{{Status: state.Task.Running}, {Status: state.Task.Failed}}, 0).Status,
		"a failed shard outranks one still copying")
	assert.Equal(t, state.Task.Pending, RollUpShardedTable([]ShardCopy{{Status: state.Task.RevertWindow}, {Status: state.Task.Pending}}, 0).Status,
		"a table with an undispatched shard has work ahead of it")
}

// A table with one copying shard held back by the throttler reads as
// throttled with that shard's reason, while its ETA stays the unthrottled
// shard's. A shard that finished while throttled does not hold the table back.
func TestRollUpShardedTableReadsAThrottledShard(t *testing.T) {
	shards := []ShardCopy{
		{Status: state.Task.Running, RowsCopied: 4000, RowsTotal: 10000, ETASeconds: 60},
		{Status: state.Task.Running, RowsCopied: 4000, RowsTotal: 10000, ETASeconds: 7200, Throttled: true, ThrottleReason: "replica-lag 5s >= 2s"},
	}
	got := RollUpShardedTable(shards, 2)
	assert.True(t, got.Throttled)
	assert.Equal(t, "replica-lag 5s >= 2s", got.ThrottleReason)
	assert.Equal(t, int64(60), got.ETASeconds, "the throttled shard's ETA stays out while an unthrottled shard reports one")

	finished := RollUpShardedTable([]ShardCopy{
		{Status: state.Task.Running, RowsCopied: 4000, RowsTotal: 10000, ETASeconds: 60},
		{Status: state.Task.Completed, RowsCopied: 10000, RowsTotal: 10000, Throttled: true, ThrottleReason: "replica-lag 5s >= 2s"},
	}, 2)
	assert.False(t, finished.Throttled, "a finished shard holds nothing back")
	assert.Empty(t, finished.ThrottleReason)
}

// A 32-shard table copies 31 shards that each report a minute left, while one
// throttled shard reports two hours. Shards cannot share work, so the table
// finishes when its slowest shard does, and the ETA is the longest any
// unthrottled shard reports. The throttled shard's ETA stays out while any
// unthrottled shard reports one, since the throttle can lift at any moment
// and the table already reads "(throttled)". Once only throttled shards
// report an ETA, the longest of theirs is the table's. A queued shard, which
// reports no ETA, adds nothing, and with no ETA at all the table's is zero.
func TestShardedTableETA(t *testing.T) {
	var shards []ShardCopy
	for range 31 {
		shards = append(shards, ShardCopy{Status: state.Task.Running, RowsCopied: 4000, RowsTotal: 10000, ETASeconds: 60})
	}
	throttled := ShardCopy{Status: state.Task.Running, RowsCopied: 4000, RowsTotal: 10000, ETASeconds: 7200, Throttled: true}
	shards = append(shards, throttled)
	assert.Equal(t, int64(60), ShardedTableETA(shards))

	uneven := slices.Concat(shards, []ShardCopy{
		{Status: state.Task.Pending},
		{Status: state.Task.Running, RowsCopied: 1000, RowsTotal: 10000, ETASeconds: 540},
	})
	assert.Equal(t, int64(540), ShardedTableETA(uneven), "the slowest unthrottled shard decides, and a queued shard adds nothing")

	assert.Equal(t, int64(7200), ShardedTableETA([]ShardCopy{throttled}), "with only throttled shards reporting, the table waits on them")
	assert.Equal(t, int64(7200), ShardedTableETA([]ShardCopy{throttled, {Status: state.Task.Running, ETASeconds: 900, Throttled: true}}))
	assert.Zero(t, ShardedTableETA([]ShardCopy{{Status: state.Task.Pending}}))
	assert.Zero(t, ShardedTableETA(nil))
}

// A table copying on five of eight shards, with three queued behind them,
// counts the plan's estimate for the three in its total, so it reads as the
// fraction of the whole table it has copied rather than of the five shards.
// The estimate stands in only while every shard still ahead of its copy has
// one and every planned shard is listed; otherwise the figures cover the
// reporting shards alone, as they do when an estimate is negative or the plan
// recorded no shard count. Once every shard still queued has failed, the total
// is the reporting shards' again. A shard
// that failed before it reported a total adds nothing, since it will not copy
// its rows, and neither does a shard whose change copies no rows, such as an
// instant ALTER.
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

	negative := eightShards()
	negative[6].PlannedRows = rows(-1)
	got = RollUpShardedTable(negative, 8)
	assert.Equal(t, int64(5000), got.RowsTotal, "a negative estimate is no estimate")
	assert.False(t, got.RowsTotalPlanned)

	instant := eightShards()
	instant[7] = ShardCopy{Status: state.Task.Pending, CopiesNoRows: true}
	got = RollUpShardedTable(instant, 8)
	assert.Equal(t, int64(7000), got.RowsTotal, "a shard whose change copies no rows has no copy ahead to estimate")
	assert.True(t, got.RowsTotalPlanned)

	got = RollUpShardedTable(eightShards(), 10)
	assert.Equal(t, int64(5000), got.RowsTotal, "planned shards not yet listed keep the reported figures")
	assert.False(t, got.RowsTotalPlanned)

	got = RollUpShardedTable(eightShards(), 0)
	assert.Equal(t, int64(5000), got.RowsTotal, "a plan with no shard count cannot show every shard is listed")
	assert.False(t, got.RowsTotalPlanned)

	lastQueuedFailed := eightShards()
	for i := 5; i < 8; i++ {
		lastQueuedFailed[i] = ShardCopy{Status: state.Task.Failed, PlannedRows: rows(1000)}
	}
	got = RollUpShardedTable(lastQueuedFailed, 8)
	assert.Equal(t, int64(5000), got.RowsTotal, "once no shard is still to copy, the total is the reported shards' again")
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
