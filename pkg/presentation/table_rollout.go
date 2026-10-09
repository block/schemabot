package presentation

import "github.com/block/schemabot/pkg/state"

// The ranks TableRolloutRank returns, in the order a rollout's tables are
// listed.
const (
	tableRankWorking = iota
	tableRankWaiting
	tableRankPartlyDone
	tableRankNotStarted
	tableRankSettled
)

// TableRolloutRank is where a table rolled up across the targets that run it
// sorts in a rollout's progress, lower first, given the table's status on each
// of those targets. A table some target is working on leads, then one a target
// is waiting on or retrying, then one finished on some targets and queued on the
// rest, then one no target has started, and last one that halted or finished
// everywhere it ran. The PR comment and the CLI both order their target rollups
// by it, so the two list a rollout's tables the same way.
func TableRolloutRank(statuses []string) int {
	var waiting, halted, done, queued bool
	for _, s := range statuses {
		switch state.NormalizeTaskStatus(s) {
		case state.Task.Running, state.Task.CatchingUp, state.Task.Checksumming,
			state.Task.PostChecksum, state.Task.CuttingOver, state.Task.Reverting:
			return tableRankWorking
		case state.Task.WaitingForCutover, state.Task.WaitingForDeploy,
			state.Task.Recovering, state.Task.FailedRetryable:
			waiting = true
		case state.Task.Failed, state.Task.Stopped, state.Task.Cancelled, state.Task.Reverted:
			halted = true
		case state.Task.Completed, state.Task.RevertWindow:
			done = true
		default:
			// Pending: not started. NormalizeTaskStatus maps a state this build
			// does not know to Running, so unknown work ranks as working.
			queued = true
		}
	}
	switch {
	case waiting:
		return tableRankWaiting
	case halted:
		return tableRankSettled
	case done && queued:
		return tableRankPartlyDone
	case queued || len(statuses) == 0:
		return tableRankNotStarted
	default:
		return tableRankSettled
	}
}
