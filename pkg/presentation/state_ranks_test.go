package presentation

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
)

// The tests in this file pin, for every state, where each ranking in this
// package places it. A state added to pkg/state fails them until each ranking
// says where it goes, and changing a rank means editing the expected table
// here, so no ranking moves as a side effect of another change.

// everyState lists the value of every field of a state constant struct such as
// state.Task.
func everyState(t *testing.T, constants any) []string {
	t.Helper()
	var states []string
	for _, field := range reflect.ValueOf(constants).Fields() {
		require.NotEmpty(t, field.String())
		states = append(states, field.String())
	}
	require.NotEmpty(t, states)
	return states
}

// assertRanks checks that want names every state exactly and that rank gives
// each state the value want pins for it.
func assertRanks(t *testing.T, states []string, want map[string]int, rank func(string) int) {
	t.Helper()
	assert.Len(t, want, len(states), "the expected ranks must name every state, and only those")
	for _, s := range states {
		wantRank, ok := want[s]
		if !assert.Truef(t, ok, "no expected rank for state %q", s) {
			continue
		}
		assert.Equalf(t, wantRank, rank(s), "rank of state %q", s)
	}
}

// A sharded table reads as its most urgent shard: a failure first, then work
// in progress, then paused and queued work, then the settled states.
func TestTaskAttentionRankRanksEveryTaskState(t *testing.T) {
	task := state.Task
	assertRanks(t, everyState(t, state.Task), map[string]int{
		task.Failed:            17,
		task.FailedRetryable:   16,
		task.CuttingOver:       15,
		task.Running:           14,
		task.PostChecksum:      13,
		task.Checksumming:      12,
		task.CatchingUp:        11,
		task.Reverting:         10,
		task.WaitingForCutover: 9,
		task.Recovering:        8,
		task.WaitingForDeploy:  7,
		task.Stopped:           6,
		task.Pending:           5,
		task.RevertWindow:      4,
		task.Cancelled:         2,
		task.Reverted:          1,
		task.Completed:         0,
	}, TaskAttentionRank)
}

// A shard surfaces its most urgent operation. States the ranking does not name
// rank with pending.
func TestShardStateRankRanksEveryOperationState(t *testing.T) {
	op := state.ApplyOperation
	assertRanks(t, everyState(t, state.ApplyOperation), map[string]int{
		op.Failed:                  12,
		op.FailedRetryable:         11,
		op.Running:                 10,
		op.CuttingOver:             9,
		op.WaitingForCutover:       8,
		op.Recovering:              7,
		op.Resuming:                6,
		op.Stopped:                 5,
		op.RevertWindow:            4,
		op.Pending:                 3,
		op.RunningDegraded:         3,
		op.CatchingUp:              3,
		op.Checksumming:            3,
		op.PostChecksum:            3,
		op.Paused:                  3,
		op.WaitingForDeploy:        3,
		op.SkippingRevert:          3,
		op.Reverting:               3,
		op.PreparingBranch:         3,
		op.ApplyingBranchChanges:   3,
		op.ValidatingBranch:        3,
		op.CreatingDeployRequest:   3,
		op.ValidatingDeployRequest: 3,
		op.Cancelled:               2,
		op.Reverted:                1,
		op.Completed:               0,
	}, ShardStateRank)
}

// A table that one target runs sorts in the rollout by that target's state.
func TestTableRolloutRankRanksEveryTaskStateOnOneTarget(t *testing.T) {
	task := state.Task
	assertRanks(t, everyState(t, state.Task), map[string]int{
		task.Running:           tableRankWorking,
		task.CatchingUp:        tableRankWorking,
		task.Checksumming:      tableRankWorking,
		task.PostChecksum:      tableRankWorking,
		task.CuttingOver:       tableRankWorking,
		task.Reverting:         tableRankWorking,
		task.WaitingForCutover: tableRankWaiting,
		task.WaitingForDeploy:  tableRankWaiting,
		task.Recovering:        tableRankWaiting,
		task.FailedRetryable:   tableRankWaiting,
		task.Failed:            tableRankHalted,
		task.Stopped:           tableRankHalted,
		task.Cancelled:         tableRankHalted,
		task.Reverted:          tableRankHalted,
		task.Pending:           tableRankNotStarted,
		task.Completed:         tableRankFinished,
		task.RevertWindow:      tableRankFinished,
	}, func(s string) int { return TableRolloutRank([]string{s}) })
}
