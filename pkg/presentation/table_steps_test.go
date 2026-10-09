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
