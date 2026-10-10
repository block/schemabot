package presentation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
)

// tableRow is one target's work on the table at step of a rollout run table
// by table.
func tableRow(target, st string, step int) Operation {
	return Operation{Deployment: "primary", Target: target, Work: true, State: st, RolloutStep: step, ContinueOnFailure: true, ExternalOperationID: "op-" + target + "-" + st}
}

// A rollout runs `bikes` then `docks` on orders-001 and orders-002, one row per
// target and table, created table by table. Each target reads as one member
// over its two rows: orders-001 finished `bikes` and waits for `docks`, and
// orders-002 is still copying `bikes`, which orders-001 waits on. The
// deployment is one group of two targets, the counts are targets rather than
// rows, and the header reads the rows as the stored apply state does.
func TestDerive_FoldsATargetsTableRowsIntoOneMember(t *testing.T) {
	model := Derive([]Operation{
		tableRow("orders-001", so.Completed, 1),
		tableRow("orders-002", so.Running, 1),
		tableRow("orders-001", so.Pending, 2),
		tableRow("orders-002", so.Pending, 2),
	})

	require.Len(t, model.Deployments, 2)
	first, second := model.Deployments[0], model.Deployments[1]
	assert.Equal(t, "primary/orders-001", first.Name)
	assert.Equal(t, so.Pending, first.State)
	assert.Equal(t, []int{0, 2}, first.Rows)
	assert.Equal(t, 2, first.Row, "the queued table speaks for a target whose other table is done")
	assert.Equal(t, "op-orders-001-pending", first.ExternalOperationID)
	assert.Equal(t, "primary/orders-002", second.Name)
	assert.Equal(t, so.Running, second.State)
	assert.Equal(t, []int{1, 3}, second.Rows)
	assert.Equal(t, 1, second.Row)

	groups := model.Groups()
	require.Len(t, groups, 1)
	assert.Equal(t, []int{0, 1}, groups[0].Members)
	assert.Equal(t, state.Apply.Running, model.State)
	assert.Equal(t, []StateCount{{"running", 1}, {"waiting", 1}}, model.Counts)
	assert.Equal(t, "waiting for primary/orders-002", first.Label, "the queued table waits on the other target's copy of `bikes`")
}

// A failure on any table is its target's: orders-002 failed `bikes`, so it
// reads failed with that table's error and identifiers, and it is the
// rollout's first failure, though its `docks` row is still queued.
func TestDerive_FoldedTargetFailsOnAnyTable(t *testing.T) {
	failed := tableRow("orders-002", so.Failed, 1)
	failed.Error = "Error 1062: Duplicate entry"
	model := Derive([]Operation{
		tableRow("orders-001", so.Completed, 1),
		failed,
		tableRow("orders-001", so.Running, 2),
		tableRow("orders-002", so.Pending, 2),
	})

	require.Len(t, model.Deployments, 2)
	target := model.Deployments[1]
	assert.Equal(t, so.Failed, target.State)
	assert.Equal(t, StateFailed, target.Presentation)
	assert.Equal(t, 1, target.Row)
	assert.Equal(t, "Error 1062: Duplicate entry", target.Error)
	assert.Equal(t, "op-orders-002-failed", target.ExternalOperationID)
	require.NotNil(t, model.FirstFailure)
	assert.Equal(t, "primary/orders-002", model.FirstFailure.Name)
}

// A target whose every table finished reads completed, and one whose every
// table already held the change reads already applied. A target that had only
// some of its tables already is one that ran the rest, so it reads completed.
func TestDerive_FoldedTargetSettlesOnceEveryTableHas(t *testing.T) {
	converged := func(target string, step int) Operation {
		op := tableRow(target, so.Completed, step)
		op.NeverStarted = true
		op.AlreadyConverged = true
		return op
	}
	model := Derive([]Operation{
		tableRow("orders-001", so.Completed, 1),
		converged("orders-002", 1),
		converged("orders-003", 1),
		tableRow("orders-001", so.Completed, 2),
		tableRow("orders-002", so.Completed, 2),
		converged("orders-003", 2),
	})

	require.Len(t, model.Deployments, 3)
	assert.Equal(t, StateCompleted, model.Deployments[0].Presentation)
	assert.Equal(t, StateCompleted, model.Deployments[1].Presentation)
	assert.False(t, model.Deployments[1].NeverStarted)
	assert.Equal(t, StateAlreadyApplied, model.Deployments[2].Presentation)
	assert.Equal(t, state.Apply.Completed, model.State)
}

// Only rows stamped with a table step fold. Rows that each run their member's
// whole change keep a member each, whether several of them divide one
// target's work, several targets each run several keyed members, or they are
// the sharded fan-out, whose rows are shards under a namespace finalizer.
func TestDerive_FoldsOnlyRowsOfATableByTableRollout(t *testing.T) {
	for name, ops := range map[string][]Operation{
		"one target's keyed work": {
			{Deployment: "primary", Target: "orders-001", OperationKey: "ns_0", Work: true, State: so.Running},
			{Deployment: "primary", Target: "orders-001", OperationKey: "ns_1", Work: true, State: so.Pending},
		},
		"several targets' keyed work": {
			{Deployment: "primary", Target: "orders-001", OperationKey: "orders-001/ns_0", Work: true, State: so.Completed},
			{Deployment: "primary", Target: "orders-002", OperationKey: "orders-002/ns_0", Work: true, State: so.Running},
			{Deployment: "primary", Target: "orders-001", OperationKey: "orders-001/ns_1", Work: true, State: so.Pending},
			{Deployment: "primary", Target: "orders-002", OperationKey: "orders-002/ns_1", Work: true, State: so.Pending},
		},
		"sharded fan-out": {
			{Deployment: "primary", Target: "orders-001", OperationKey: "orders-001/ns_0/-80/t", Work: true, State: so.Running},
			{Deployment: "primary", Target: "orders-001", OperationKey: "orders-001/ns_0/80-/t", Work: true, State: so.Running},
			{Deployment: "primary", Target: "orders-002", OperationKey: "orders-002/ns_0/-80/t", Work: true, State: so.Pending},
			{Deployment: "primary", Target: "orders-001", OperationKey: "orders-001/ns_0/group_finalizer", Finalizer: true, State: so.Pending},
		},
	} {
		t.Run(name, func(t *testing.T) {
			model := Derive(ops)
			require.Len(t, model.Deployments, len(ops))
			for i, d := range model.Deployments {
				assert.Equal(t, []int{i}, d.Rows)
				assert.Equal(t, i, d.Row)
			}
		})
	}
}

// A target waiting on its next table is held by whatever holds that table's
// row, which can be a failure on a target the member order puts after it.
// Under halt, orders-002 failed `bikes`, so orders-001, done with `bikes` and
// queued for `docks`, reads halted by orders-002's failure rather than queued.
func TestDerive_FoldedTargetWaitsOnEarlierRowsOfOtherTargets(t *testing.T) {
	row := func(target, st string, step int) Operation {
		op := tableRow(target, st, step)
		op.ContinueOnFailure = false
		return op
	}
	model := Derive([]Operation{
		row("orders-001", so.Completed, 1),
		row("orders-002", so.Failed, 1),
		row("orders-001", so.Pending, 2),
		row("orders-002", so.Pending, 2),
	})

	require.Len(t, model.Deployments, 2)
	first := model.Deployments[0]
	assert.Equal(t, StateHalted, first.Presentation)
	assert.Equal(t, "halted — primary/orders-002 failed", first.Label)
	assert.Equal(t, StateFailed, model.Deployments[1].Presentation)
}

// The first failure is the first failed row, so a target that failed an
// earlier table is reported before one the member order puts first that
// failed a later table.
func TestDerive_FirstFailureIsTheEarliestFailedRow(t *testing.T) {
	model := Derive([]Operation{
		tableRow("orders-001", so.Completed, 1),
		tableRow("orders-002", so.Failed, 1),
		tableRow("orders-001", so.Failed, 2),
		tableRow("orders-002", so.Pending, 2),
	})

	require.NotNil(t, model.FirstFailure)
	assert.Equal(t, "primary/orders-002", model.FirstFailure.Name)
}

// orders-001 copied `bikes` and parked it for cutover, with `docks` still
// queued behind it. The target stands at that cutover, so it reads as ready
// for cutover and the next action offers it, rather than reading as queued
// with nothing for the operator to do. orders-002 waits on it.
func TestDerive_FoldedTargetParkedForCutoverOffersTheCutover(t *testing.T) {
	model := Derive([]Operation{
		tableRow("orders-001", so.WaitingForCutover, 1),
		tableRow("orders-002", so.Pending, 1),
		tableRow("orders-001", so.Pending, 2),
		tableRow("orders-002", so.Pending, 2),
	})

	require.Len(t, model.Deployments, 2)
	first := model.Deployments[0]
	assert.Equal(t, so.WaitingForCutover, first.State)
	assert.Equal(t, StateReadyForCutoverNext, first.Presentation)
	assert.Equal(t, "ready for cutover — next in order", first.Label)
	assert.Equal(t, 0, first.Row)
	assert.Equal(t, "waiting for primary/orders-001", model.Deployments[1].Label)
	assert.Equal(t, NextActionCutover, model.NextAction.Kind)
	assert.Equal(t, "orders-001", model.NextAction.Target)
}

// Under a parallel copy, orders-001 copied and parked `docks` while `bikes`,
// its earlier table, is still queued. Cutovers run in order, so `docks` cannot
// cut over before `bikes` has run: the target reads as queued and no cutover
// is offered.
func TestDerive_FoldedTargetWithAnEarlierTableQueuedIsNotReadyForCutover(t *testing.T) {
	row := func(st string, step int) Operation {
		op := tableRow("orders-001", st, step)
		op.Parallel = true
		return op
	}
	model := Derive([]Operation{
		row(so.Pending, 1),
		row(so.WaitingForCutover, 2),
	})

	require.Len(t, model.Deployments, 1)
	only := model.Deployments[0]
	assert.Equal(t, so.Pending, only.State)
	assert.Equal(t, StateQueuedNext, only.Presentation)
	assert.Equal(t, NextActionNone, model.NextAction.Kind)
}

// A table step waits for every target to finish the step before it, under
// every cutover policy and on_failure. orders-002 failed `bikes`: under
// continue and under an unreleased pause, orders-001's `docks` reads halted,
// not queued or paused for a release, since no release starts the next table
// on a fleet missing the last one, and the rollout settles failed. While
// orders-002 is still copying `bikes` under parallel, orders-001 waits for it
// rather than reading next in order.
func TestDerive_TableStepWaitsForTheStepBefore(t *testing.T) {
	policies := map[string]func(*Operation){
		"continue": func(*Operation) {},
		"pause":    func(op *Operation) { op.ContinueOnFailure = false; op.PauseOnFailure = true },
	}
	for name, policy := range policies {
		t.Run(name, func(t *testing.T) {
			rows := []Operation{
				tableRow("orders-001", so.Completed, 1),
				tableRow("orders-002", so.Failed, 1),
				tableRow("orders-001", so.Pending, 2),
				tableRow("orders-002", so.Pending, 2),
			}
			for i := range rows {
				rows[i].Parallel = true
				policy(&rows[i])
			}
			model := Derive(rows)

			require.Len(t, model.Deployments, 2)
			assert.Equal(t, StateHalted, model.Deployments[0].Presentation)
			assert.Equal(t, "halted — primary/orders-002 failed", model.Deployments[0].Label)
			assert.Equal(t, state.Apply.Failed, model.State)
		})
	}

	t.Run("still copying", func(t *testing.T) {
		rows := []Operation{
			tableRow("orders-001", so.Completed, 1),
			tableRow("orders-002", so.Running, 1),
			tableRow("orders-001", so.Pending, 2),
			tableRow("orders-002", so.Pending, 2),
		}
		for i := range rows {
			rows[i].Parallel = true
		}
		model := Derive(rows)

		require.Len(t, model.Deployments, 2)
		assert.Equal(t, StateWaiting, model.Deployments[0].Presentation)
		assert.Equal(t, "waiting for primary/orders-002", model.Deployments[0].Label)
	})
}

// A target whose only change is a later table is one row, and it still waits
// for the step before: orders-003 changes only `docks`, so it waits while
// orders-001 copies `bikes`.
func TestDerive_SingleTableTargetWaitsForTheStepBefore(t *testing.T) {
	model := Derive([]Operation{
		tableRow("orders-001", so.Running, 1),
		tableRow("orders-001", so.Pending, 2),
		tableRow("orders-003", so.Pending, 2),
	})

	require.Len(t, model.Deployments, 2)
	assert.Equal(t, StateWaiting, model.Deployments[1].Presentation)
	assert.Equal(t, "waiting for primary/orders-001", model.Deployments[1].Label)
}

// A rollout has settled only when its apply has and every target has too. An
// apply settles failed the moment one target fails, so a sibling still copying
// keeps the rollout unsettled and its tables still to run read as queued; a
// stopped apply can resume, so it never settles a rollout either.
func TestRolloutSettled(t *testing.T) {
	derive := func(applyState string, ops ...Operation) (Apply, Group) {
		model := Derive(ops)
		model.State = applyState
		groups := model.Groups()
		require.Len(t, groups, 1)
		return model, groups[0]
	}
	model, g := derive(so.Failed, tableRow("orders-001", so.Failed, 1), tableRow("orders-002", so.Running, 1))
	assert.False(t, model.RolloutSettled(g), "a target still copying")
	model, g = derive(so.Failed, tableRow("orders-001", so.Failed, 1), tableRow("orders-002", so.Completed, 1))
	assert.True(t, model.RolloutSettled(g), "every target final")
	model, g = derive(so.Stopped, tableRow("orders-001", so.Stopped, 1), tableRow("orders-002", so.Completed, 1))
	assert.False(t, model.RolloutSettled(g), "a stopped apply can resume")
}
