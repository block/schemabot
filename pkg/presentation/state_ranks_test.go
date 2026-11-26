package presentation

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/state/statetest"
)

// The tests in this file pin, for every state, where each ranking over the
// states of pkg/state in this package places it. A state added to pkg/state
// fails them until each ranking says where it goes, and changing a rank means
// editing the expected table here, so no ranking moves as a side effect of
// another change.

// A sharded table reads as its most urgent shard: a failure first, then work
// in progress, then paused and queued work, then the settled states.
func TestTaskAttentionRankRanksEveryTaskState(t *testing.T) {
	task := state.Task
	statetest.AssertRanks(t, statetest.Every(t, state.Task), map[string]int{
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
	statetest.AssertRanks(t, statetest.Every(t, state.ApplyOperation), map[string]int{
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

// The row that leads a target's rows is the one most in need of an operator:
// a failure, then a retry, a stop, a cutover, work under way, work still
// queued, and settled work last. Active states the ranking does not name rank
// with work under way.
func TestRowLeadRankRanksEveryOperationState(t *testing.T) {
	op := state.ApplyOperation
	statetest.AssertRanks(t, statetest.Every(t, state.ApplyOperation), map[string]int{
		op.Failed:                  0,
		op.FailedRetryable:         1,
		op.Stopped:                 2,
		op.WaitingForCutover:       3,
		op.CuttingOver:             3,
		op.Running:                 4,
		op.RunningDegraded:         4,
		op.CatchingUp:              4,
		op.Checksumming:            4,
		op.PostChecksum:            4,
		op.Recovering:              4,
		op.Resuming:                4,
		op.Paused:                  4,
		op.WaitingForDeploy:        4,
		op.RevertWindow:            4,
		op.SkippingRevert:          4,
		op.Reverting:               4,
		op.PreparingBranch:         4,
		op.ApplyingBranchChanges:   4,
		op.ValidatingBranch:        4,
		op.CreatingDeployRequest:   4,
		op.ValidatingDeployRequest: 4,
		op.Pending:                 5,
		op.Cancelled:               6,
		op.Reverted:                6,
		op.Completed:               7,
	}, rowLeadRank)
}

// rolloutRankOnOneTarget is the TableRolloutRank of a table that one target
// runs, given that target's state.
func rolloutRankOnOneTarget(s string) int { return TableRolloutRank([]string{s}) }

// A table that one target runs sorts in the rollout by that target's state.
func TestTableRolloutRankRanksEveryTaskStateOnOneTarget(t *testing.T) {
	task := state.Task
	statetest.AssertRanks(t, statetest.Every(t, state.Task), map[string]int{
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
	}, rolloutRankOnOneTarget)
}

// A table that two targets run sorts by whichever target's state lists
// earlier, so a waiting target leads a halted one and both lead a target that
// has not started. The one exception is a table finished on one target and
// not started on the other, which is partly done. The order the targets
// report in makes no difference.
func TestTableRolloutRankRanksEveryPairOfTaskStates(t *testing.T) {
	states := statetest.Every(t, state.Task)
	for _, a := range states {
		for _, b := range states {
			rankA, rankB := rolloutRankOnOneTarget(a), rolloutRankOnOneTarget(b)
			want := min(rankA, rankB)
			if min(rankA, rankB) == tableRankNotStarted && max(rankA, rankB) == tableRankFinished {
				want = tableRankPartlyDone
			}
			assert.Equalf(t, want, TableRolloutRank([]string{a, b}), "targets report %q and %q", a, b)
		}
	}
}

// A table run on several targets reads as a failure or halt first, then as
// the least finished state any target is in, then queued, then complete. The
// proto-prefixed spelling of a state rolls up as the canonical state.
func TestRollupTaskStatusPrecedenceCoversEveryTaskState(t *testing.T) {
	task := state.Task
	tiers := []statetest.RollupTier{
		{States: []string{task.Failed, task.FailedRetryable, task.Stopped, task.Cancelled}},
		{States: []string{
			task.Running, task.WaitingForDeploy, task.Recovering, task.CatchingUp,
			task.Checksumming, task.PostChecksum, task.WaitingForCutover, task.CuttingOver,
			task.Reverting, task.RevertWindow, task.Reverted,
		}},
		{States: []string{task.Pending}},
		{States: []string{task.Completed}},
	}
	statetest.AssertRollup(t, statetest.Every(t, state.Task), tiers, func(first, second string) string {
		return RollupTaskStatus([]string{first, second})
	})
	assert.Equal(t, task.Completed, RollupTaskStatus(nil), "a table no target reports reads as complete")
	assert.Equal(t, task.Failed, RollupTaskStatus([]string{"STATE_RUNNING", "STATE_FAILED"}))
}

// A task ranking gives a state the same rank however it is spelled: the
// proto-prefixed form ranks as the canonical state, and an operation state
// that is not a task state ranks as running, the state it stands in for.
func TestTaskRankingsRankAStateTheSameHoweverItIsSpelled(t *testing.T) {
	rankings := map[string]func(string) int{
		"TaskAttentionRank": TaskAttentionRank,
		"TableRolloutRank":  rolloutRankOnOneTarget,
	}
	for name, rank := range rankings {
		for _, s := range statetest.Every(t, state.Task) {
			proto := "STATE_" + strings.ToUpper(s)
			assert.Equalf(t, rank(s), rank(proto), "%s of %q", name, proto)
		}
		assert.Equalf(t, rank(state.Task.Running), rank(state.ApplyOperation.RunningDegraded),
			"%s of an operation state that stands in for running", name)
	}
}
