package ui

import (
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
)

func TestTableStatePriority(t *testing.T) {
	tests := []struct {
		state    string
		expected int
	}{
		{state.Task.Running, 0},
		{state.Task.CuttingOver, 0},
		{state.Task.WaitingForCutover, 1},
		{state.Task.Pending, 2},
		{state.Task.Failed, 3},
		{state.Task.Stopped, 3},
		{state.Task.Completed, 4},
		{state.Task.Cancelled, 4},
		{state.Task.Reverted, 4},
		{"unknown_state", 2}, // default
	}

	for _, tt := range tests {
		t.Run(tt.state, func(t *testing.T) {
			assert.Equal(t, tt.expected, TableStatePriority(tt.state))
		})
	}
}
