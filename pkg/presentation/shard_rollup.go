package presentation

import (
	"github.com/block/schemabot/pkg/state"
)

// ShardCopy is one shard's progress on one table's change: its status in
// task vocabulary and the copy figures its engine reported.
type ShardCopy struct {
	Status          string
	PercentComplete int
	RowsCopied      int64
	RowsTotal       int64
	ETASeconds      int64
	// ChecksumRowsChecked and ChecksumRowsTotal are how far a checksumming
	// shard's verify has got, once its engine reports a total.
	ChecksumRowsChecked int64
	ChecksumRowsTotal   int64
	// PlannedRows is the plan's row estimate for this shard's table, nil when
	// the plan has none. It stands in for the shard's row total until the
	// engine reports one (RollUpShardedTable).
	PlannedRows *int64
	// CopiesNoRows marks a shard whose engine reported that its change copies
	// no rows (TasksCopyNoRows), such as an instant ALTER. It has no copy
	// ahead of it, so the plan's estimate never stands in for its total.
	CopiesNoRows bool
	// Throttled reports that the engine's throttler is pausing this shard's
	// work, and ThrottleReason names the signal. The reason is empty when
	// Throttled is false.
	Throttled      bool
	ThrottleReason string
}

// ShardOperationCopy resolves one (shard, table) operation's progress from its
// most attention-worthy task, which is where the engine reports live shard
// state. The operation state stands in for a task that has not reported a
// state, and for an operation with no tasks yet, since dispatch creates them
// when the operation's wave starts. It also stands in for a task still
// recorded as pending under a running operation: a driver has dispatched the
// shard, and the task row only catches up on the next progress poll, so the
// shard is under way rather than queued. Tasks are ranked by the state they
// recorded, and only the chosen task is read as running, so a pending
// sibling never outranks a task that has reported a later step such as
// checksumming.
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
	if dispatchedButPending(operationState, best.Status) {
		best.Status = operationState
	}
	return best
}

// dispatchedButPending reports whether a task still reads pending although
// its operation is running, so a driver has already dispatched it.
func dispatchedButPending(operationState, taskStatus string) bool {
	return state.IsState(operationState, state.ApplyOperation.Running) &&
		state.NormalizeTaskStatus(taskStatus) == state.Task.Pending
}

// ShardedTableCopy is one keyspace table's change rolled up across the shards
// that run it.
type ShardedTableCopy struct {
	// Status is the shard status an operator should act on first: failure
	// over active work, active work over waiting, waiting over done.
	Status string

	// RowsCopied and RowsTotal cover the shards that have reported a row
	// total, counted in ShardsReporting. A shard counts only once it carries a
	// total, so a shard with copied rows but no total cannot inflate the
	// numerator alone. A renderer discloses the coverage rather than passing
	// one wave's fraction off as the table's. ETASeconds is when the slowest
	// shard finishes (ShardedTableETA), whether or not it reported a total.
	RowsCopied      int64
	RowsTotal       int64
	ETASeconds      int64
	ShardsReporting int

	// RowsTotalPlanned marks a RowsTotal that also counts the plan's estimate
	// for every shard that will still copy and has not reported a total, so
	// the total and the percent span the whole table rather than the shards
	// reporting so far. The total is then approximate.
	RowsTotalPlanned bool

	// Throttled reports that the engine's throttler is pausing at least one
	// shard still at work, and ThrottleReason names the first such shard's
	// signal. A throttled shard's ETA stays out of the table's while another
	// shard is not throttled (ShardedTableETA), so the table reads as
	// throttled instead, and the operator still sees the shard held back.
	Throttled      bool
	ThrottleReason string
}

// RollUpShardedTable rolls one keyspace table's per-shard progress up into the
// table's, the way the PR comment and the CLI both read a sharded table.
// shards must not be empty. plannedShards is how many shards the plan spans
// the table across, zero when it recorded none.
//
// While the copy is in flight, the plan's estimate stands in for each shard
// still ahead of its copy, so a table with five of eight shards reporting is
// not read as nearly done. It stands in only when every such shard has an
// estimate and every planned shard is listed: a total missing some shards
// would be neither the reporting shards' nor the table's, so the figures then
// cover the reporting shards alone. A plan that recorded no shard count cannot
// show that every shard is listed, so its estimates never stand in. A shard
// that failed, stopped, finished
// past its copy without a total, or copies no rows adds nothing, so the total
// never counts rows that will not be copied. A negative estimate is not a
// count, so it reads as no estimate.
func RollUpShardedTable(shards []ShardCopy, plannedShards int) ShardedTableCopy {
	out := ShardedTableCopy{Status: shards[0].Status}
	var planned int64
	estimatesCoverUnreported := everyPlannedShardListed(len(shards), plannedShards)
	unreported, inFlight := 0, false
	for _, sh := range shards {
		if TaskAttentionRank(sh.Status) > TaskAttentionRank(out.Status) {
			out.Status = sh.Status
		}
		inFlight = inFlight || state.IsInFlightTaskState(sh.Status)
		if sh.Throttled && !out.Throttled && state.IsInFlightTaskState(sh.Status) {
			out.Throttled, out.ThrottleReason = true, sh.ThrottleReason
		}
		if sh.RowsTotal <= 0 {
			if copyAheadWithoutTotal(sh) {
				unreported++
				if sh.PlannedRows == nil || *sh.PlannedRows < 0 {
					estimatesCoverUnreported = false
				} else {
					planned += *sh.PlannedRows
				}
			}
			continue
		}
		out.ShardsReporting++
		out.RowsCopied += sh.RowsCopied
		out.RowsTotal += sh.RowsTotal
	}
	// Every shard's ETA counts, row total or not: the data plane folds its
	// stored shards the same way (aggregateStoredShards in pkg/tern), so the
	// CLI and the PR comment read one ETA for the same shards.
	out.ETASeconds = ShardedTableETA(shards)
	if inFlight && unreported > 0 && estimatesCoverUnreported {
		out.RowsTotal += planned
		out.RowsTotalPlanned = true
	}
	return out
}

// ShardedTableETA is when a sharded table's copy finishes: when its slowest
// shard does, since each shard copies only its own rows and no shard can take
// over another's. It is the longest ETA any shard reports, zero when none
// reports one.
//
// A throttled shard's ETA is left out while some shard that is not throttled
// reports one. That ETA comes from the rate the throttler holds the shard to
// for the moment, so it says how long the pause would last if it never
// lifted, not when the table finishes. The table reads as throttled instead
// (RollUpShardedTable). When every shard that reports an ETA is throttled,
// the longest of theirs stands, since it is the only figure there is.
func ShardedTableETA(shards []ShardCopy) int64 {
	var unthrottled, throttled int64
	for _, sh := range shards {
		if sh.ETASeconds <= 0 {
			continue
		}
		if sh.Throttled {
			throttled = max(throttled, sh.ETASeconds)
			continue
		}
		unthrottled = max(unthrottled, sh.ETASeconds)
	}
	if unthrottled > 0 {
		return unthrottled
	}
	return throttled
}

// everyPlannedShardListed reports whether the listed shards are known to
// cover every shard the plan spans the table across. A plan with no shard
// count gives no way to tell, so coverage is unknown and reads as not covered.
func everyPlannedShardListed(listed, plannedShards int) bool {
	return plannedShards > 0 && listed >= plannedShards
}

// copyAheadWithoutTotal reports whether a shard that has reported no row
// total still has a row copy ahead of it, so the plan's estimate is what its
// total will be.
func copyAheadWithoutTotal(sh ShardCopy) bool {
	return CopyStillAhead(sh.Status) && !sh.CopiesNoRows
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
