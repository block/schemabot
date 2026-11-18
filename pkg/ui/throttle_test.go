package ui

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestThrottleTip verifies that every throttle signal the engine can emit maps
// to a short operator-facing tip, composite reasons collapse shared meanings,
// and any unrecognized signal suppresses the whole tip so the raw reason
// stands alone rather than binding a partial explanation to the wrong signal.
func TestThrottleTip(t *testing.T) {
	tests := []struct {
		name   string
		reason string
		want   string
	}{
		{"redo-aware threads", "redo-aware 4 > 3",
			"backing off while the database's active threads exceed its budget"},
		{"threads-running fallback shares the thread-budget tip", "threads-running 21 > 18",
			"backing off while the database's active threads exceed its budget"},
		{"commit latency", "commit-latency 112.4ms >= 100ms",
			"backing off while database writes commit slowly"},
		{"composite reasons join their tips", "redo-aware 4 > 3; commit-latency 112.4ms >= 100ms",
			"backing off while the database's active threads exceed its budget; backing off while database writes commit slowly"},
		{"composite duplicate meanings collapse", "redo-aware 4 > 3; threads-running 21 > 18",
			"backing off while the database's active threads exceed its budget"},
		{"unrecognized signal yields no tip", "mock throttler (always throttled)", ""},
		{"unrecognized segment suppresses the whole tip", "disk-usage 95% > 90%; commit-latency 112.4ms >= 100ms", ""},
		{"signal token must match exactly", "redo-awareness 4 > 3", ""},
		{"empty segments are ignored", "commit-latency 112.4ms >= 100ms; ",
			"backing off while database writes commit slowly"},
		{"empty reason yields no tip", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ThrottleTip(tt.reason))
		})
	}
}
