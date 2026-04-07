package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizeState_Resolve(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"running", "running"},
		{"RUNNING", "running"},
		{"STATE_RUNNING", "running"},
		{"STATE_COMPLETED", "completed"},
		{"completed", "completed"},
		{"pending", "pending"},
		{"", "no_active_change"},
		{"STATE_NO_ACTIVE_CHANGE", "no_active_change"},
		{"NO_ACTIVE_CHANGE", "no_active_change"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, NormalizeState(tt.input))
		})
	}
}
