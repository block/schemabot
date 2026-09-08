package engine

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSanitizeThrottleReason verifies that an engine-produced throttle reason
// is safe for the operator surfaces that render it: whitespace runs collapse,
// markdown table separators are neutralized, and overlong text is clamped so
// a reason can never break a PR comment table or a CLI row.
func TestSanitizeThrottleReason(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"plain reason passes through", "replica-lag 12s > 10s", "replica-lag 12s > 10s"},
		{"empty stays empty", "", ""},
		{"newlines and runs collapse to single spaces", "replica-lag\n12s >\t\t10s", "replica-lag 12s > 10s"},
		{"table separators are neutralized", "signal a > 1 | signal b > 2", "signal a > 1 / signal b > 2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, SanitizeThrottleReason(tt.in))
		})
	}

	t.Run("overlong reason is clamped with an ellipsis", func(t *testing.T) {
		long := strings.Repeat("replica-lag 12s > 10s; ", 20)
		got := SanitizeThrottleReason(long)
		assert.LessOrEqual(t, len(got), 200)
		assert.True(t, strings.HasSuffix(got, "…"))
	})
}
