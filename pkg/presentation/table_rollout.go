package presentation

import (
	"cmp"
	"slices"

	"github.com/block/schemabot/pkg/state"
)

// The ranks TableRolloutRank returns for a table no target has halted, in the
// order a rollout's tables are listed. A halted table ranks below all of them
// (see TableRolloutRank).
const (
	tableRankWorking = iota
	tableRankWaiting
	tableRankPartlyDone
	tableRankNotStarted
	tableRankFinished
)

// rolloutHaltedStates is the order a rollout leads with the tables that
// stopped short of completing on some target.
var rolloutHaltedStates = state.HaltedTaskStates()

// tableRankHalted is the rank of a table whose most urgent halted state on any
// target is s. It is below every other rank, and the halted states rank in the
// order of state.HaltedTaskStates.
func tableRankHalted(s string) int {
	return slices.Index(rolloutHaltedStates, s) - len(rolloutHaltedStates)
}

// TableRolloutRank is where a table rolled up across the targets that run it
// sorts in a rollout's progress, lower first, given the table's status on each
// of those targets. A table that failed or halted on any target leads, in the
// order of state.HaltedTaskStates, since that is where the rollout stopped and
// what the operator has to act on; single-target listings lead the same way
// (ui.TableStatePriority). Then comes a table some target is working on, then
// one a target is waiting on, then one finished on some targets and queued on
// the rest, then one no target has started, and last one finished everywhere
// it ran. The PR comment and the CLI both order their target rollups by it, so
// the two list a rollout's tables the same way.
func TableRolloutRank(statuses []string) int {
	halted := -1
	var working, waiting, done, queued bool
	for _, s := range statuses {
		normalized := state.NormalizeTaskStatus(s)
		if i := slices.Index(rolloutHaltedStates, normalized); i >= 0 {
			if halted < 0 || i < halted {
				halted = i
			}
			continue
		}
		switch normalized {
		case state.Task.Running, state.Task.CatchingUp, state.Task.Checksumming,
			state.Task.PostChecksum, state.Task.CuttingOver, state.Task.Reverting:
			working = true
		case state.Task.WaitingForCutover, state.Task.WaitingForDeploy, state.Task.Recovering:
			waiting = true
		case state.Task.Completed, state.Task.RevertWindow, state.Task.Reverted:
			done = true
		default:
			// Pending: not started. NormalizeTaskStatus maps a state this build
			// does not know to Running, so unknown work ranks as working.
			queued = true
		}
	}
	switch {
	case halted >= 0:
		return tableRankHalted(rolloutHaltedStates[halted])
	case working:
		return tableRankWorking
	case waiting:
		return tableRankWaiting
	case done && queued:
		return tableRankPartlyDone
	case queued || len(statuses) == 0:
		return tableRankNotStarted
	default:
		return tableRankFinished
	}
}

// TableChangeOrder is the order a rollout lists its n table changes in, given
// the table each one changes and its TableRolloutRank. Targets can need
// different statements to reach the same table, an ALTER where the table
// exists and a CREATE where it does not, and each statement has its own
// entry. A table's statements are listed together, so the listing reads as
// the tables the headline counts rather than as one table per statement.
// Tables sort by the lowest rank among their statements, and a table's
// statements by their own rank. Equal ranks keep plan order. The PR comment
// and the CLI both order their target rollups by it.
func TableChangeOrder(n int, table func(int) string, rank func(int) int) []int {
	ranks := make([]int, n)
	tableRank := make(map[string]int)
	tablePlanned := make(map[string]int)
	for i := range n {
		ranks[i] = rank(i)
		name := table(i)
		if best, ok := tableRank[name]; !ok || ranks[i] < best {
			tableRank[name] = ranks[i]
		}
		if _, ok := tablePlanned[name]; !ok {
			tablePlanned[name] = i
		}
	}
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int {
		ta, tb := table(a), table(b)
		if c := cmp.Compare(tableRank[ta], tableRank[tb]); c != 0 {
			return c
		}
		if c := cmp.Compare(tablePlanned[ta], tablePlanned[tb]); c != 0 {
			return c
		}
		return cmp.Compare(ranks[a], ranks[b])
	})
	return order
}
