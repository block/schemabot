package presentation

import "github.com/block/schemabot/pkg/state"

// TableSteps is a deployment's progress through the table steps of a rollout
// run table by table: how many steps it has, and how many of them have
// completed on every target that runs them. A table finishes on every target
// before the next one starts, so between steps a target is done with one table
// and waiting on the next, and counting steps says where the rollout is where
// counting targets would read most of them as queued.
type TableSteps struct {
	// Steps is the deployment's table steps.
	Steps int
	// Done is the steps whose every row completed.
	Done int
}

// TableSteps counts g's table steps from the rollout step its rows are stamped
// with. It reports false when g does not run in several table steps: a row
// carries no step, which is a rollout that does not run table by table, or
// every row runs the same one, where the targets' own progress says it all.
func (a Apply) TableSteps(g Group) (TableSteps, bool) {
	unfinished := make(map[int]bool)
	for _, i := range g.Members {
		for _, r := range a.Deployments[i].Rows {
			op := a.rows[r]
			if op.RolloutStep == 0 {
				return TableSteps{}, false
			}
			unfinished[op.RolloutStep] = unfinished[op.RolloutStep] || !state.IsState(op.State, state.ApplyOperation.Completed)
		}
	}
	if len(unfinished) < 2 {
		return TableSteps{}, false
	}
	steps := TableSteps{Steps: len(unfinished)}
	for _, open := range unfinished {
		if !open {
			steps.Done++
		}
	}
	return steps, true
}
