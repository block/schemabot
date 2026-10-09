package presentation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A rollout runs `bikes`, `docks` and `riders` on orders-001 and orders-002.
// `bikes` finished on both, `docks` finished on orders-001 and is copying on
// orders-002, and `riders` waits. One of three steps is done, though no target
// has finished its whole change.
func TestTableSteps_CountsStepsDoneOnEveryTarget(t *testing.T) {
	model := Derive([]Operation{
		tableRow("orders-001", so.Completed, 1),
		tableRow("orders-002", so.Completed, 1),
		tableRow("orders-001", so.Completed, 2),
		tableRow("orders-002", so.Running, 2),
		tableRow("orders-001", so.Pending, 3),
		tableRow("orders-002", so.Pending, 3),
	})
	groups := model.Groups()
	require.Len(t, groups, 1)

	steps, ok := model.TableSteps(groups[0])
	require.True(t, ok)
	assert.Equal(t, TableSteps{Steps: 3, Done: 1}, steps)
}

// A step a target's plan does not touch has no row for that target, so it is
// done once the targets that run it have.
func TestTableSteps_AStepDoneOnTheTargetsThatRunIt(t *testing.T) {
	model := Derive([]Operation{
		tableRow("orders-001", so.Completed, 1),
		tableRow("orders-002", so.Completed, 1),
		tableRow("orders-002", so.Completed, 2),
	})

	steps, ok := model.TableSteps(model.Groups()[0])
	require.True(t, ok)
	assert.Equal(t, TableSteps{Steps: 2, Done: 2}, steps)
}

// A rollout that does not run table by table, or runs one table step, has no
// steps to count: its targets' progress says where it is.
func TestTableSteps_NoneWithoutSeveralSteps(t *testing.T) {
	for name, ops := range map[string][]Operation{
		"rows carry no step": {tableRow("orders-001", so.Completed, 0), tableRow("orders-002", so.Running, 0)},
		"one step":           {tableRow("orders-001", so.Completed, 1), tableRow("orders-002", so.Running, 1)},
	} {
		t.Run(name, func(t *testing.T) {
			model := Derive(ops)
			_, ok := model.TableSteps(model.Groups()[0])
			assert.False(t, ok)
		})
	}
}

// orders-000 already held the change, so the rollout gave it one settled row
// in the first step. It counts toward the done first step like any completed
// row, and the group's progress counts it as having had the change, which is
// what the headlines subtract from the targets they count.
func TestTableSteps_ConvergedTargetSettlesInTheFirstStep(t *testing.T) {
	converged := tableRow("orders-000", so.Completed, 1)
	converged.AlreadyConverged = true
	model := Derive([]Operation{
		converged,
		tableRow("orders-001", so.Completed, 1),
		tableRow("orders-002", so.Completed, 1),
		tableRow("orders-001", so.Running, 2),
		tableRow("orders-002", so.Pending, 2),
	})
	groups := model.Groups()
	require.Len(t, groups, 1)

	steps, ok := model.TableSteps(groups[0])
	require.True(t, ok)
	assert.Equal(t, TableSteps{Steps: 2, Done: 1}, steps)
	progress := model.TargetProgress(groups[0])
	assert.Equal(t, 3, progress.Total)
	assert.Equal(t, 1, progress.AlreadyHad)
}

// The table-step headline names the targets in an outcome an operator acts on
// and leaves the ones still under way to the table count. Once the rollout
// settles short, it also counts the targets that finished.
func TestTargetProgress_TableStepOutcomes(t *testing.T) {
	progress := TargetProgress{Total: 4, Done: 1, Others: []StateCount{{Label: "running", Count: 1}, {Label: "waiting", Count: 1}, {Label: "failed", Count: 1}}, Unsettled: 2}
	assert.Equal(t, []StateCount{{Label: "failed", Count: 1}}, progress.TableStepOutcomes(TableSteps{Steps: 3, Done: 1}, false))

	settledShort := TargetProgress{Total: 3, Done: 2, Others: []StateCount{{Label: "failed", Count: 1}}}
	assert.Equal(t, []StateCount{{Label: "completed", Count: 2}, {Label: "failed", Count: 1}}, settledShort.TableStepOutcomes(TableSteps{Steps: 3, Done: 2}, true))

	assert.Empty(t, TargetProgress{Total: 2, Done: 2}.TableStepOutcomes(TableSteps{Steps: 2, Done: 2}, true))
}

// A rollout run table by table reads the same headline on the PR comment and
// in the CLI: the tables done and the targets they ran on, the tables alone
// once every one is done, and the targets an operator acts on after them, the
// failures first and the first count named as targets. A target that already
// had the change is not counted, and a rollback says so.
func TestTableStepsHeadline(t *testing.T) {
	failed := TargetProgress{Total: 3, Done: 2, Others: []StateCount{{Label: "failed", Count: 1}}}
	tests := []struct {
		name     string
		steps    TableSteps
		progress TargetProgress
		settled  bool
		rollback bool
		want     string
	}{
		{"running", TableSteps{Steps: 3, Done: 1}, TargetProgress{Total: 4, Unsettled: 4}, false, false, "1 of 3 tables done on 4 targets"},
		{"settled with a target still going", TableSteps{Steps: 3, Done: 3}, TargetProgress{Total: 4, Done: 3, Unsettled: 1}, true, false, "3 of 3 tables done on 4 targets"},
		{"every table done", TableSteps{Steps: 3, Done: 3}, TargetProgress{Total: 4, Done: 4}, true, false, "3 tables done on 4 targets"},
		{"failing while others run", TableSteps{Steps: 3, Done: 1}, TargetProgress{Total: 4, Unsettled: 1, Others: []StateCount{{Label: "running", Count: 1}, {Label: "failed", Count: 3}}}, false, false, "1 of 3 tables done on 4 targets · 3 targets failed"},
		{"settled short on the last table", TableSteps{Steps: 3, Done: 2}, failed, true, false, "2 of 3 tables done on 3 targets · 2 targets completed, 1 failed"},
		{"settled short, the rest halted", TableSteps{Steps: 2, Done: 0}, TargetProgress{Total: 3, Others: []StateCount{{Label: "halted", Count: 2}, {Label: "failed", Count: 1}}}, true, false, "0 of 2 tables done on 3 targets · 1 target failed, 2 halted"},
		{"a target already had the change", TableSteps{Steps: 2, Done: 2}, TargetProgress{Total: 2, Done: 1, AlreadyHad: 1}, true, false, "2 tables done on 1 target"},
		{"rolling back", TableSteps{Steps: 2, Done: 0}, TargetProgress{Total: 2, Unsettled: 2}, false, true, "0 of 2 tables rolled back on 2 targets"},
		{"rolled back", TableSteps{Steps: 2, Done: 2}, TargetProgress{Total: 2, Done: 2}, true, true, "2 tables rolled back on 2 targets"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, TableStepsHeadline(tt.steps, tt.progress, tt.settled, tt.rollback))
		})
	}
}
