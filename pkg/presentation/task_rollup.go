package presentation

import (
	"slices"

	"github.com/block/schemabot/pkg/state"
)

// inFlightRollupOrder is every task state between queued and complete, the
// least finished first: copying, then the phases in the order a target passes
// through them, then a revert still undoing the change, a revert window that
// is not final yet, and a revert that has finished.
var inFlightRollupOrder = []string{
	state.Task.Running,
	state.Task.WaitingForDeploy,
	state.Task.Recovering,
	state.Task.CatchingUp,
	state.Task.Checksumming,
	state.Task.PostChecksum,
	state.Task.WaitingForCutover,
	state.Task.CuttingOver,
	state.Task.Reverting,
	state.Task.RevertWindow,
	state.Task.Reverted,
}

// RollupTaskStatus is a table's status across the targets that run it, given
// the table's status on each of them: a failure or halt first, then the least
// finished state any target is in, so the table never reads further along
// than its slowest target, then queued, then complete. A table no target
// reports reads as complete.
func RollupTaskStatus(statuses []string) string {
	normalized := make([]string, len(statuses))
	for i, s := range statuses {
		normalized[i] = state.NormalizeTaskStatus(s)
	}
	for _, halting := range []string{state.Task.Failed, state.Task.FailedRetryable, state.Task.Stopped, state.Task.Cancelled} {
		if slices.Contains(normalized, halting) {
			return halting
		}
	}
	for _, phase := range inFlightRollupOrder {
		if slices.Contains(normalized, phase) {
			return phase
		}
	}
	if slices.Contains(normalized, state.Task.Pending) {
		return state.Task.Pending
	}
	return state.Task.Completed
}
