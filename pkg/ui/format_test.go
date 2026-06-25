package ui

import (
	"strings"
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
		{state.Task.Recovering, 1},
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

func TestVSchemaStatusLabel(t *testing.T) {
	assert.Equal(t, "Applying...", VSchemaStatusLabel("applying"))
	assert.Equal(t, "Applied", VSchemaStatusLabel("applied"))
	assert.Equal(t, "Pending", VSchemaStatusLabel(""))
	assert.Equal(t, "rolling_back", VSchemaStatusLabel("rolling_back"), "unknown status passes through")
}

func TestProgressBarActivity(t *testing.T) {
	bar := ProgressBarActivity()

	assert.Equal(t, 20, strings.Count(bar, ColorBlue))
	assert.Zero(t, strings.Count(bar, ColorEmpty))
}

func TestRowCopyDisplayPercent(t *testing.T) {
	assert.Equal(t, 0, RowCopyDisplayPercent(0, 0))
	assert.Equal(t, 1, RowCopyDisplayPercent(0, 42))
	assert.Equal(t, 1, RowCopyDisplayPercent(-3, 42))
	assert.Equal(t, 2, RowCopyDisplayPercent(2, 42))
	assert.Equal(t, 100, RowCopyDisplayPercent(145, 42))
}

func TestCleanLintReason(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect string
	}{
		{
			name:   "strips ERROR prefix and linter name",
			input:  "[ERROR] invisible_index_before_drop: Index 'idx_status' should be made invisible",
			expect: "Index 'idx_status' should be made invisible",
		},
		{
			name:   "strips WARNING prefix and linter name",
			input:  "[WARNING] unsafe: DROP COLUMN removes data",
			expect: "DROP COLUMN removes data",
		},
		{
			name:   "no prefix passes through",
			input:  "DROP TABLE removes all data",
			expect: "DROP TABLE removes all data",
		},
		{
			name:   "multiple reasons joined by semicolon",
			input:  "[ERROR] unsafe: DROP COLUMN removes data; [ERROR] invisible_index_before_drop: Index should be invisible first",
			expect: "DROP COLUMN removes data; Index should be invisible first",
		},
		{
			name:   "empty string",
			input:  "",
			expect: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, CleanLintReason(tt.input))
		})
	}
}
