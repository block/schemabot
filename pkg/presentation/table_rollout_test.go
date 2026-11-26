package presentation

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
)

func TestTableRolloutRank(t *testing.T) {
	task := state.Task
	tests := []struct {
		name     string
		statuses []string
		want     int
	}{
		{"one target copying among queued ones", []string{task.Completed, task.Running, task.Pending}, tableRankWorking},
		{"checksumming is working", []string{task.Checksumming, task.Pending}, tableRankWorking},
		{"a failure leads work beside it", []string{task.Failed, task.CuttingOver}, tableRankHalted(task.Failed)},
		{"failed and retrying on two targets reads as failed", []string{task.FailedRetryable, task.Failed}, tableRankHalted(task.Failed)},
		{"waiting for cutover", []string{task.WaitingForCutover, task.Pending}, tableRankWaiting},
		{"retrying", []string{task.FailedRetryable, task.Completed}, tableRankHalted(task.FailedRetryable)},
		{"finished on some targets, queued on the rest", []string{task.Completed, task.Pending, task.Pending}, tableRankPartlyDone},
		{"in its revert window on some targets, queued on the rest", []string{task.RevertWindow, task.Pending}, tableRankPartlyDone},
		{"no target has started", []string{task.Pending, task.Pending}, tableRankNotStarted},
		{"no target has reported", nil, tableRankNotStarted},
		{"finished everywhere", []string{task.Completed, task.Completed}, tableRankFinished},
		{"halted on one target", []string{task.Completed, task.Failed, task.Pending}, tableRankHalted(task.Failed)},
		{"stopped", []string{task.Stopped, task.Pending}, tableRankHalted(task.Stopped)},
		{"reverted everywhere is finished", []string{task.Reverted, task.Completed}, tableRankFinished},
		{"proto-prefixed status", []string{"STATE_RUNNING"}, tableRankWorking},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, TableRolloutRank(tt.statuses))
		})
	}
}

// The ranks list a halted table first, a failure ahead of a retry, a stop and a
// cancellation, since that is where the rollout stopped. Then a table some
// target is copying leads a table a target is waiting on, which leads a table
// finished on one target and queued on three, which leads a table no target
// has started, which leads one finished everywhere.
func TestTableRolloutRankOrder(t *testing.T) {
	task := state.Task
	assert.Less(t, tableRankHalted(task.Failed), tableRankHalted(task.FailedRetryable))
	assert.Less(t, tableRankHalted(task.FailedRetryable), tableRankHalted(task.Stopped))
	assert.Less(t, tableRankHalted(task.Stopped), tableRankHalted(task.Cancelled))
	assert.Less(t, tableRankHalted(task.Cancelled), tableRankWorking)
	assert.Less(t, tableRankWorking, tableRankWaiting)
	assert.Less(t, tableRankWaiting, tableRankPartlyDone)
	assert.Less(t, tableRankPartlyDone, tableRankNotStarted)
	assert.Less(t, tableRankNotStarted, tableRankFinished)
}

// A table that failed on a target and halted the rollout leads the tables the
// halt left unstarted, so the operator reads the failure first.
func TestTableRolloutRankFailureLeadsUnstartedTables(t *testing.T) {
	task := state.Task
	failed := TableRolloutRank([]string{task.Failed, task.Pending, task.Pending, task.Pending})
	unstarted := TableRolloutRank([]string{task.Pending, task.Pending, task.Pending, task.Pending})
	assert.Less(t, failed, unstarted)
}

// A rollout changes `customers` with an ALTER on the targets that have it and
// a CREATE on the ones that do not, and also alters `orders`. The CREATE is
// still working, the ALTER finished, and `orders` is finished on some targets.
// Both `customers` entries are listed together, ahead of `orders`, so the
// listing reads as the two tables the headline counts.
func TestTableChangeOrderListsATablesStatementsTogether(t *testing.T) {
	type change struct {
		table, statement string
		rank             int
	}
	changes := []change{
		{"customers", "alter", tableRankFinished},
		{"orders", "alter", tableRankPartlyDone},
		{"customers", "create", tableRankWorking},
	}
	order := TableChangeOrder(len(changes),
		func(i int) string { return changes[i].table },
		func(i int) int { return changes[i].rank })
	var got []string
	for _, i := range order {
		got = append(got, changes[i].table+" "+changes[i].statement)
	}
	assert.Equal(t, []string{"customers create", "customers alter", "orders alter"}, got)
}

// Tables of equal rank keep plan order, and so do one table's statements of
// equal rank.
func TestTableChangeOrderKeepsPlanOrderForEqualRanks(t *testing.T) {
	tables := []string{"payments", "orders", "payments", "refunds"}
	order := TableChangeOrder(len(tables),
		func(i int) string { return tables[i] },
		func(int) int { return tableRankWorking })
	assert.Equal(t, []int{0, 2, 1, 3}, order)
}

// A table is placed by the most urgent of its statements: one whose CREATE is
// working leads a table that is only partly done, even when its other
// statement has finished.
func TestTableChangeOrderPlacesATableByItsMostUrgentStatement(t *testing.T) {
	type change struct {
		table string
		rank  int
	}
	changes := []change{
		{"orders", tableRankPartlyDone},
		{"customers", tableRankFinished},
		{"customers", tableRankWorking},
	}
	order := TableChangeOrder(len(changes),
		func(i int) string { return changes[i].table },
		func(i int) int { return changes[i].rank })
	assert.Equal(t, []int{2, 1, 0}, order)
}
