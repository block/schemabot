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
	})

	assert.Equal(t, ShardedTableCopy{Status: state.Task.Running, RowsCopied: 1300, RowsTotal: 2200, ETASeconds: 90, ShardsReporting: 2}, got)
	assert.Equal(t, state.Task.Failed, RollUpShardedTable([]ShardCopy{{Status: state.Task.Running}, {Status: state.Task.Failed}}).Status,
		"a failed shard outranks one still copying")
	assert.Equal(t, state.Task.Pending, RollUpShardedTable([]ShardCopy{{Status: state.Task.RevertWindow}, {Status: state.Task.Pending}}).Status,
		"a table with an undispatched shard has work ahead of it")
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
