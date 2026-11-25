package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/state/statetest"
	"github.com/block/schemabot/pkg/ui"
)

// The tests in this file pin, for every task state, how each ranking the PR
// comment sorts or rolls tables up by treats it. A state added to pkg/state
// fails them until each ranking says where it goes, and changing a rank means
// editing the expected table here, so no ranking moves as a side effect of
// another change.

// protoForm is the proto-prefixed spelling of a canonical state.
func protoForm(s string) string { return "STATE_" + strings.ToUpper(s) }

// The progress comment lists a table by the same priority as the CLI, whether
// the engine reports the canonical state or its proto-prefixed form.
func TestTableStatePriorityMatchesTheCLIForEveryTaskState(t *testing.T) {
	for _, s := range statetest.Every(t, state.Task) {
		assert.Equalf(t, ui.TableStatePriority(s), tableStatePriority(s), "priority of task state %q", s)
		assert.Equalf(t, ui.TableStatePriority(s), tableStatePriority(protoForm(s)), "priority of task state %q", protoForm(s))
	}
}

// The terminal summary lists what went wrong first, then what landed, then
// what never ran, then any state an apply does not end in.
func TestSummaryStatePriorityRanksEveryTaskState(t *testing.T) {
	task := state.Task
	statetest.AssertRanks(t, statetest.Every(t, state.Task), map[string]int{
		task.Failed:            0,
		task.Stopped:           0,
		task.Reverted:          0,
		task.Completed:         1,
		task.Cancelled:         2,
		task.Pending:           3,
		task.Running:           3,
		task.CatchingUp:        3,
		task.Checksumming:      3,
		task.PostChecksum:      3,
		task.WaitingForDeploy:  3,
		task.WaitingForCutover: 3,
		task.Recovering:        3,
		task.CuttingOver:       3,
		task.RevertWindow:      3,
		task.Reverting:         3,
		task.FailedRetryable:   3,
	}, summaryStatePriority)
}

// The terminal summary gives a state the same priority however it is
// spelled: the proto-prefixed form lists as the canonical state, and an
// operation state that is not a task state lists as running, the state it
// stands in for.
func TestSummaryStatePriorityRanksAStateTheSameHoweverItIsSpelled(t *testing.T) {
	for _, s := range statetest.Every(t, state.Task) {
		assert.Equalf(t, summaryStatePriority(s), summaryStatePriority(protoForm(s)), "priority of task state %q", protoForm(s))
	}
	assert.Equal(t, summaryStatePriority(state.Task.Running), summaryStatePriority(state.ApplyOperation.RunningDegraded))
}
