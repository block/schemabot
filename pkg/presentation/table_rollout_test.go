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
		{"working outranks a failure beside it", []string{task.Failed, task.CuttingOver}, tableRankWorking},
		{"waiting for cutover", []string{task.WaitingForCutover, task.Pending}, tableRankWaiting},
		{"retrying", []string{task.FailedRetryable, task.Completed}, tableRankWaiting},
		{"finished on some targets, queued on the rest", []string{task.Completed, task.Pending, task.Pending}, tableRankPartlyDone},
		{"in its revert window on some targets, queued on the rest", []string{task.RevertWindow, task.Pending}, tableRankPartlyDone},
		{"no target has started", []string{task.Pending, task.Pending}, tableRankNotStarted},
		{"no target has reported", nil, tableRankNotStarted},
		{"finished everywhere", []string{task.Completed, task.Completed}, tableRankSettled},
		{"halted on one target", []string{task.Completed, task.Failed, task.Pending}, tableRankSettled},
		{"stopped", []string{task.Stopped, task.Pending}, tableRankSettled},
		{"proto-prefixed status", []string{"STATE_RUNNING"}, tableRankWorking},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, TableRolloutRank(tt.statuses))
		})
	}
}

// The ranks list the live tables first: a table some target is copying leads a
// table finished on one target and queued on three, which leads a table no
// target has started, which leads one finished everywhere.
func TestTableRolloutRankOrder(t *testing.T) {
	assert.Less(t, tableRankWorking, tableRankWaiting)
	assert.Less(t, tableRankWaiting, tableRankPartlyDone)
	assert.Less(t, tableRankPartlyDone, tableRankNotStarted)
	assert.Less(t, tableRankNotStarted, tableRankSettled)
}
