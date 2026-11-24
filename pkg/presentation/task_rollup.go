package presentation

import (
	"slices"

	"github.com/block/schemabot/pkg/state"
)

// RollupTaskStatus is a table's status across the targets that run it, given
// the table's status on each of them: a failure or halt first, then the first
// state a target reports that is neither queued nor complete, then queued,
// then complete. A table no target reports reads as complete.
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
