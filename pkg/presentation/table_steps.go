package presentation

import (
	"fmt"
	"slices"
	"strings"

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
// beside its table count: outcomes an operator acts on, the failures first,
// since the targets held or halted are held or halted by them. A target
// copying, queued, waiting between tables, at cutover or in its revert window
// is where the table count already places it.
var tableStepOutcomeStates = []PresentationState{
	StateFailed, StateRetrying, StateHalted, StatePaused, StateStopped, StateCancelled, StateReverted, StateUnknown,
}

// TableStepsHeadline states a rollout run table by table in one line, the
// same on the PR comment and in the CLI: "1 of 3 tables done on 4 targets"
// while it runs or once it settled short, and "3 tables done on 4 targets"
// once every table is done on every target. A table finishes on every target
// before the next starts, so the targets are counted only as the rollout's
// size: most of them are between tables, which a count of target states would
// read as queued. The targets in an outcome an operator acts on follow
// (TableStepOutcomes), the first count naming them as
// targets so it does not read as a count of tables: "0 of 2 tables done on 3
// targets · 1 target failed, 2 halted". A rollback says "rolled back" for
// "done". A target that already had the change ran no table, so it is not
// counted.
func TableStepsHeadline(steps TableSteps, p TargetProgress, settled, rollback bool) string {
	verb := "done"
	if rollback {
		verb = "rolled back"
	}
	targets := TargetNoun.Count(p.Total - p.AlreadyHad)
	line := fmt.Sprintf("%d of %d tables %s on %s", steps.Done, steps.Steps, verb, targets)
	if settled && p.Unsettled == 0 && steps.Done == steps.Steps {
		line = fmt.Sprintf("%d tables %s on %s", steps.Steps, verb, targets)
	}
	outcomes := p.TableStepOutcomes(steps, settled)
	if len(outcomes) == 0 {
		return line
	}
	parts := make([]string, len(outcomes))
	for i, c := range outcomes {
		parts[i] = fmt.Sprintf("%d %s", c.Count, c.Label)
	}
	parts[0] = TargetNoun.Count(outcomes[0].Count) + " " + outcomes[0].Label
	return line + " · " + strings.Join(parts, ", ")
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
	var named []StateCount
	for _, count := range p.Others {
		if tableStepOutcomeRank(count.Label) >= 0 {
			named = append(named, count)
		}
	}
	slices.SortStableFunc(named, func(a, b StateCount) int {
		return tableStepOutcomeRank(a.Label) - tableStepOutcomeRank(b.Label)
	})
	return append(outcomes, named...)
}

// tableStepOutcomeRank is where the summary category labelled label falls in
// tableStepOutcomeStates, or -1 when it counts none of them.
func tableStepOutcomeRank(label string) int {
	for _, category := range summaryCategoryOrder {
		if category.label != label {
			continue
		}
		for _, s := range category.states {
			if i := slices.Index(tableStepOutcomeStates, s); i >= 0 {
				return i
			}
		}
	}
	return -1
}
