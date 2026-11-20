package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/state/statetest"
)

// A table run on several targets reads as the earliest in-flight phase any
// target is in, then as a failure or halt, then as the first other state a
// target reports, then queued, then complete. The test pins that precedence
// for every pair of task states, so a state added to pkg/state fails here
// until the rollup says where it goes, and changing the precedence means
// editing the tiers here.
func TestRollupTaskStatusPrecedenceCoversEveryTaskState(t *testing.T) {
	task := state.Task
	tiers := []statetest.RollupTier{
		{States: []string{
			task.Running, task.WaitingForDeploy, task.Recovering, task.CatchingUp,
			task.Checksumming, task.PostChecksum, task.WaitingForCutover,
			task.CuttingOver, task.Reverting,
		}},
		{States: []string{task.Failed, task.FailedRetryable, task.Stopped, task.Cancelled}},
		{States: []string{task.RevertWindow, task.Reverted}, FirstReported: true},
		{States: []string{task.Pending}},
		{States: []string{task.Completed}},
	}
	statetest.AssertRollup(t, statetest.Every(t, state.Task), tiers, func(first, second string) string {
		return rollupTaskStatus([]string{first, second})
	})
	assert.Equal(t, task.Completed, rollupTaskStatus(nil), "a table no target reports reads as complete")
}
