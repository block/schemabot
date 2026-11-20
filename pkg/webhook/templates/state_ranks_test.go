package templates

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

// The tests in this file pin, for every task state, how each ranking the PR
// comment sorts or rolls tables up by treats it. A state added to pkg/state
// fails them until each ranking says where it goes, and changing a rank means
// editing the expected table here, so no ranking moves as a side effect of
// another change.

// everyTaskState lists the value of every state.Task field.
func everyTaskState(t *testing.T) []string {
	t.Helper()
	var states []string
	for _, field := range reflect.ValueOf(state.Task).Fields() {
		require.NotEmpty(t, field.String())
		states = append(states, field.String())
	}
	require.NotEmpty(t, states)
	return states
}

// The progress comment lists a table by the same priority as the CLI, whether
// the engine reports the canonical state or its proto-prefixed form.
func TestTableStatePriorityMatchesTheCLIForEveryTaskState(t *testing.T) {
	for _, s := range everyTaskState(t) {
		assert.Equalf(t, ui.TableStatePriority(s), tableStatePriority(s), "priority of task state %q", s)
		proto := "STATE_" + strings.ToUpper(s)
		assert.Equalf(t, ui.TableStatePriority(s), tableStatePriority(proto), "priority of task state %q", proto)
	}
}

// The terminal summary lists what went wrong first, then what landed, then
// what never ran, then any state an apply does not end in.
func TestSummaryStatePriorityRanksEveryTaskState(t *testing.T) {
	task := state.Task
	want := map[string]int{
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
	}
	states := everyTaskState(t)
	assert.Len(t, want, len(states), "the expected priorities must name every task state, and only those")
	for _, s := range states {
		wantPriority, ok := want[s]
		if !assert.Truef(t, ok, "no expected priority for task state %q", s) {
			continue
		}
		assert.Equalf(t, wantPriority, summaryStatePriority(s), "priority of task state %q", s)
	}
}

// rollupTier is a group of task states that a rollup prefers over every state
// in a later tier. Within a tier, the rollup prefers the state listed first,
// or, when firstReported is set, whichever state the targets report first.
type rollupTier struct {
	states        []string
	firstReported bool
}

// wantRollup is the status tiers says a table reads as when two targets
// report a and then b.
func wantRollup(t *testing.T, tiers []rollupTier, a, b string) string {
	t.Helper()
	tierOf := func(s string) (int, int) {
		for i, tier := range tiers {
			if j := slices.Index(tier.states, s); j >= 0 {
				return i, j
			}
		}
		require.FailNow(t, "task state "+s+" is in no rollup tier")
		return 0, 0
	}
	tierA, posA := tierOf(a)
	tierB, posB := tierOf(b)
	switch {
	case tierA != tierB:
		if tierA < tierB {
			return a
		}
		return b
	case tiers[tierA].firstReported || posA <= posB:
		return a
	default:
		return b
	}
}

// A table run on several targets reads as a failure or halt first, then as
// the first moving state a target reports, then queued, then complete.
func TestRollupTaskStatusPrecedenceCoversEveryTaskState(t *testing.T) {
	task := state.Task
	tiers := []rollupTier{
		{states: []string{task.Failed, task.FailedRetryable, task.Stopped, task.Cancelled}},
		{states: []string{
			task.Running, task.CatchingUp, task.Checksumming, task.PostChecksum,
			task.WaitingForDeploy, task.WaitingForCutover, task.Recovering,
			task.CuttingOver, task.RevertWindow, task.Reverting, task.Reverted,
		}, firstReported: true},
		{states: []string{task.Pending}},
		{states: []string{task.Completed}},
	}
	states := everyTaskState(t)
	var tiered []string
	for _, tier := range tiers {
		tiered = append(tiered, tier.states...)
	}
	assert.ElementsMatch(t, states, tiered, "the rollup tiers must name every task state exactly once")

	for _, a := range states {
		for _, b := range states {
			got := rollupTaskStatus([]TableProgressData{{Status: a}, {Status: b}})
			assert.Equalf(t, wantRollup(t, tiers, a, b), got, "targets report %q then %q", a, b)
		}
	}
	assert.Equal(t, task.Completed, rollupTaskStatus(nil), "a table no target reports reads as complete")
}
