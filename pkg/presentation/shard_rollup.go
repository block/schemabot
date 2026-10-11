package presentation

import "github.com/block/schemabot/pkg/state"

// ShardCopy is one shard's progress on one table's change: its status in
// task vocabulary and the copy figures its engine reported.
type ShardCopy struct {
	Status          string
	PercentComplete int
	RowsCopied      int64
	RowsTotal       int64
	ETASeconds      int64
}

// ShardOperationCopy resolves one (shard, table) operation's progress from its
// most attention-worthy task, which is where the engine reports live shard
// state. The operation state stands in for a task that has not reported a
// state, and for an operation with no tasks yet, since dispatch creates them
// when the operation's wave starts.
func ShardOperationCopy(operationState string, tasks []ShardCopy) ShardCopy {
	best := ShardCopy{}
	for _, t := range tasks {
		if t.Status == "" {
			t.Status = operationState
		}
		if best.Status == "" || TaskAttentionRank(t.Status) > TaskAttentionRank(best.Status) {
			best = t
		}
	}
	if best.Status == "" {
		return ShardCopy{Status: operationState}
	}
	return best
}

// ShardedTableCopy is one keyspace table's change rolled up across the shards
// that run it.
type ShardedTableCopy struct {
	// Status is the shard status an operator should act on first: failure
	// over active work, active work over waiting, waiting over done.
	Status string

	// RowsCopied, RowsTotal, and ETASeconds cover only the shards that have
	// reported a row total, counted in ShardsReporting: rows sum across them
	// and the ETA is the slowest one's. A shard counts only once it carries a
	// total, so a shard with copied rows but no total cannot inflate the
	// numerator alone, and shards whose wave has not started add nothing. A
	// renderer discloses the coverage rather than passing one wave's fraction
	// off as the table's.
	RowsCopied      int64
	RowsTotal       int64
	ETASeconds      int64
	ShardsReporting int
}

// RollUpShardedTable rolls one keyspace table's per-shard progress up into the
// table's, the way the PR comment and the CLI both read a sharded table. shards
// must not be empty.
func RollUpShardedTable(shards []ShardCopy) ShardedTableCopy {
	out := ShardedTableCopy{Status: shards[0].Status}
	for _, sh := range shards {
		if TaskAttentionRank(sh.Status) > TaskAttentionRank(out.Status) {
			out.Status = sh.Status
		}
		if sh.RowsTotal <= 0 {
			continue
		}
		out.ShardsReporting++
		out.RowsCopied += sh.RowsCopied
		out.RowsTotal += sh.RowsTotal
		out.ETASeconds = max(out.ETASeconds, sh.ETASeconds)
	}
	return out
}

// TaskAttentionRank orders task states by how much they demand attention,
// higher first, normalizing first so operation states stood in for a task's
// rank the same way. Failure ranks highest, then active work, then paused and
// queued work, then the settled states. Pending outranks the revert window: a
// table with undispatched shards still has work ahead of it, however its
// landed shards hold, so the rollup must not read as complete.
func TaskAttentionRank(s string) int {
	switch state.NormalizeTaskStatus(s) {
	case state.Task.Failed:
		return 17
	case state.Task.FailedRetryable:
		return 16
	case state.Task.CuttingOver:
		return 15
	case state.Task.Running:
		return 14
	case state.Task.PostChecksum:
		return 13
	case state.Task.Checksumming:
		return 12
	case state.Task.CatchingUp:
		return 11
	case state.Task.Reverting:
		return 10
	case state.Task.WaitingForCutover:
		return 9
	case state.Task.Recovering:
		return 8
	case state.Task.WaitingForDeploy:
		return 7
	case state.Task.Stopped:
		return 6
	case state.Task.Pending:
		return 5
	case state.Task.RevertWindow:
		return 4
	case state.Task.Cancelled:
		return 2
	case state.Task.Reverted:
		return 1
	case state.Task.Completed:
		return 0
	default:
		// NormalizeTaskStatus maps unrecognized statuses to Task.Running, so
		// this arm is reachable only if that mapping changes; rank it the same
		// way so an unknown state still reads as active work.
		return 14
	}
}

// FinalizerVSchemaStatus projects a finalizer operation's state onto
// the VSchema display status vocabulary the single-deployment comment uses, so
// both comment shapes describe VSchema application identically: applied when
// the finalizer completed, applying while it runs, failed on a failure
// (terminal or auto-retrying), and pending (empty) before it starts. A
// finalizer whose rollout ended without running it reads as cancelled rather
// than pending, so the terminal summary never promises VSchema work that no
// claim arm will run. That covers both routes to a dead row: the operation
// itself holds cancelled or reverted (written by the cancel path or mirrored
// from the settled parent by the stranded-operation reaper), and the row
// still pending under a parent whose verdict is already final — a halted
// rollout terminalizes the apply immediately, while the reaper only settles
// the stranded row minutes later, well after the summary posted. Stopped —
// on the operation or the parent — stays its own status: a stopped apply is
// resumable, so its finalizer may yet run, but "pending" would overpromise.
func FinalizerVSchemaStatus(applyState, opState string) string {
	switch {
	case state.IsState(opState, state.ApplyOperation.Completed):
		return "applied"
	case state.IsState(opState, state.ApplyOperation.Running):
		return "applying"
	case state.IsState(opState, state.ApplyOperation.Failed, state.ApplyOperation.FailedRetryable):
		return "failed"
	case state.IsState(opState, state.ApplyOperation.Cancelled, state.ApplyOperation.Reverted):
		return "cancelled"
	case state.IsState(opState, state.ApplyOperation.Stopped) || state.IsState(applyState, state.Apply.Stopped):
		return "stopped"
	case state.IsTerminalApplyState(applyState):
		return "cancelled"
	default:
		return ""
	}
}
