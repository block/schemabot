package templates

import (
	"reflect"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
)

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

// A table run on several targets reads as the earliest in-flight phase any
// target is in, then as a failure or halt, then as the first other state a
// target reports, then queued, then complete. The test pins that precedence
// for every pair of task states, so a state added to pkg/state fails here
// until the rollup says where it goes, and changing the precedence means
// editing the tiers here.
func TestRollupTaskStatusPrecedenceCoversEveryTaskState(t *testing.T) {
	task := state.Task
	tiers := []rollupTier{
		{states: []string{
			task.Running, task.WaitingForDeploy, task.Recovering, task.CatchingUp,
			task.Checksumming, task.PostChecksum, task.WaitingForCutover,
			task.CuttingOver, task.Reverting,
		}},
		{states: []string{task.Failed, task.FailedRetryable, task.Stopped, task.Cancelled}},
		{states: []string{task.RevertWindow, task.Reverted}, firstReported: true},
		{states: []string{task.Pending}},
		{states: []string{task.Completed}},
	}
	var states []string
	for _, field := range reflect.ValueOf(state.Task).Fields() {
		require.NotEmpty(t, field.String())
		states = append(states, field.String())
	}
	var tiered []string
	for _, tier := range tiers {
		tiered = append(tiered, tier.states...)
	}
	assert.ElementsMatch(t, states, tiered, "the rollup tiers must name every task state exactly once")

	for _, a := range states {
		for _, b := range states {
			got := rollupTaskStatus([]string{a, b})
			assert.Equalf(t, wantRollup(t, tiers, a, b), got, "targets report %q then %q", a, b)
		}
	}
	assert.Equal(t, task.Completed, rollupTaskStatus(nil), "a table no target reports reads as complete")
}
