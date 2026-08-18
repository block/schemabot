package ui

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestThrottleTip verifies that every engine throttle signal maps to a short
// operator-facing tip, composite reasons collapse shared meanings, and an
// unrecognized signal yields no tip so the raw reason stands alone.
func TestThrottleTip(t *testing.T) {
	tests := []struct {
		name   string
		reason string
		want   string
	}{
		{"replica lag", "replica-lag 125000ms >= 120000ms",
			"waiting for the read replica to catch up"},
		{"replica lag unobservable fails closed", "replica-lag unobservable for 45s (failing closed)",
			"replication lag cannot be measured, pausing to protect the replica"},
		{"redo-aware threads", "redo-aware 4 > 3",
			"yielding to application query load on the database"},
		{"threads-running fallback shares the thread-load tip", "threads-running 130 > 128",
			"yielding to application query load on the database"},
		{"commit latency", "commit-latency 112.4ms >= 100ms",
			"backing off while database writes commit slowly"},
		{"composite reasons join their tips", "redo-aware 4 > 3; commit-latency 112.4ms >= 100ms",
			"yielding to application query load on the database; backing off while database writes commit slowly"},
		{"composite duplicate meanings collapse", "redo-aware 4 > 3; threads-running 130 > 128",
			"yielding to application query load on the database"},
		{"unrecognized signal yields no tip", "mock throttler (always throttled)", ""},
		{"empty reason yields no tip", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ThrottleTip(tt.reason))
		})
	}
}
