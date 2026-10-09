package presentation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
)

// tableRow is one target's work on one table of a rollout run table by table.
func tableRow(target, st string) Operation {
	return Operation{Deployment: "primary", Target: target, Work: true, State: st, ContinueOnFailure: true, ExternalOperationID: "op-" + target + "-" + st}
}

// A rollout runs `bikes` then `docks` on orders-001 and orders-002, one row per
// target and table, created table by table. Each target reads as one member
// over its two rows: orders-001 finished `bikes` and waits for `docks`, and
// orders-002 is still copying `bikes`. The deployment is one group of two
// targets, the counts are targets rather than rows, and the header reads the
// rows as the stored apply state does.
func TestDerive_FoldsATargetsTableRowsIntoOneMember(t *testing.T) {
	model := Derive([]Operation{
		tableRow("orders-001", so.Completed),
		tableRow("orders-002", so.Running),
		tableRow("orders-001", so.Pending),
		tableRow("orders-002", so.Pending),
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
	assert.Equal(t, []StateCount{{"running", 1}, {"queued", 1}}, model.Counts)
}

// A failure on any table is its target's: orders-002 failed `bikes`, so it
// reads failed with that table's error and identifiers, and it is the
// rollout's first failure, though its `docks` row is still queued.
func TestDerive_FoldedTargetFailsOnAnyTable(t *testing.T) {
	failed := tableRow("orders-002", so.Failed)
	failed.Error = "Error 1062: Duplicate entry"
	model := Derive([]Operation{
		tableRow("orders-001", so.Completed),
		failed,
		tableRow("orders-001", so.Running),
		tableRow("orders-002", so.Pending),
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
	converged := func(target string) Operation {
		op := tableRow(target, so.Completed)
		op.NeverStarted = true
		op.AlreadyConverged = true
		return op
	}
	model := Derive([]Operation{
		tableRow("orders-001", so.Completed),
		converged("orders-002"),
		converged("orders-003"),
		tableRow("orders-001", so.Completed),
		tableRow("orders-002", so.Completed),
		converged("orders-003"),
	})

	require.Len(t, model.Deployments, 3)
	assert.Equal(t, StateCompleted, model.Deployments[0].Presentation)
	assert.Equal(t, StateCompleted, model.Deployments[1].Presentation)
	assert.False(t, model.Deployments[1].NeverStarted)
	assert.Equal(t, StateAlreadyApplied, model.Deployments[2].Presentation)
	assert.Equal(t, state.Apply.Completed, model.State)
}

// Rows fold only by a target of a deployment that addresses several targets.
// Several rows dividing one target's work keep a member each, and so does the
// sharded fan-out, whose rows are shards under a namespace finalizer.
func TestDerive_FoldsOnlyTheTargetsOfAMultiTargetDeployment(t *testing.T) {
	for name, ops := range map[string][]Operation{
		"one target's keyed work": {
			{Deployment: "primary", Target: "orders-001", OperationKey: "ns_0", Work: true, State: so.Running},
			{Deployment: "primary", Target: "orders-001", OperationKey: "ns_1", Work: true, State: so.Pending},
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
