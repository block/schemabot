package presentation

import (
	"slices"

	"github.com/block/schemabot/pkg/state"
)

// RollupTaskStatus is a table's status across the targets that run it, given
// the table's status on each of them: a failure or halt first (in the order of
// state.HaltedTaskStates), then the first state a target reports that is
// neither queued nor complete, then queued, then complete. A table no target
// reports reads as complete.
func RollupTaskStatus(statuses []string) string {
	normalized := make([]string, len(statuses))
	for i, s := range statuses {
		normalized[i] = state.NormalizeTaskStatus(s)
	}
	for _, halting := range state.HaltedTaskStates() {
		if slices.Contains(normalized, halting) {
			return halting
		}
	}
	for _, s := range normalized {
		if s != state.Task.Pending && s != state.Task.Completed {
			return s
		}
	}
	if slices.Contains(normalized, state.Task.Pending) {
		return state.Task.Pending
	}
	return state.Task.Completed
}
