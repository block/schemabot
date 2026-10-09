package presentation

import (
	"slices"

	"github.com/block/schemabot/pkg/state"
)

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
//
// It reads g's rows as the whole rollout. Apply creation stores a row for
// every target and table step it runs before any of them starts, so a step
// with no row for a target is one that target's plan does not touch, not one
// it has yet to reach, and a step is done once every row it has completed.
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

// tableStepOutcomeStates are the target statuses a table-step headline names
// beside its table count: outcomes an operator acts on. A target copying,
// queued, waiting between tables, at cutover or in its revert window is where
// the table count already places it.
var tableStepOutcomeStates = []PresentationState{
	StateHalted, StatePaused, StateFailed, StateRetrying, StateStopped, StateCancelled, StateReverted, StateUnknown,
}

// TableStepOutcomes is the target counts a table-step headline carries beside
// its table count, which says how far the rollout got but not how its targets
// stand: the targets in an outcome an operator acts on, such as failed or
// stopped, and, once the rollout settled short of its last table, the targets
// that ran the whole change ahead of them. The PR comment and the CLI both
// read their headline's counts from it.
func (p TargetProgress) TableStepOutcomes(steps TableSteps, settled bool) []StateCount {
	var outcomes []StateCount
	if settled && p.Unsettled == 0 && steps.Done < steps.Steps && p.Done > 0 {
		outcomes = append(outcomes, StateCount{Label: "completed", Count: p.Done})
	}
	for _, count := range p.Others {
		if isTableStepOutcome(count.Label) {
			outcomes = append(outcomes, count)
		}
	}
	return outcomes
}

// isTableStepOutcome reports whether the summary category labelled label
// counts a status in tableStepOutcomeStates.
func isTableStepOutcome(label string) bool {
	for _, category := range summaryCategoryOrder {
		if category.label != label {
			continue
		}
		for _, s := range category.states {
			if slices.Contains(tableStepOutcomeStates, s) {
				return true
			}
		}
	}
	return false
}
