package ui

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	testDay  = 24 * time.Hour
	testWeek = 7 * testDay
)

// The two duration formatters agree up to a day and then diverge: the elapsed
// style keeps two units, the exact style keeps every non-zero unit. Pinning
// both over the same inputs shows where they differ, so a change to one can
// never shift the other unnoticed.
func TestFormatDurationStyles(t *testing.T) {
	tests := []struct {
		name  string
		d     time.Duration
		human string
		exact string
	}{
		{"zero", 0, "< 1s", "0s"},
		{"sub-second", 500 * time.Millisecond, "< 1s", "0s"},
		{"just under a second", 999 * time.Millisecond, "< 1s", "0s"},
		{"one second", time.Second, "1s", "1s"},
		{"fractional second truncates", 1500 * time.Millisecond, "1s", "1s"},
		{"59 seconds", 59 * time.Second, "59s", "59s"},
		{"60 seconds", 60 * time.Second, "1m", "1m"},
		{"61 seconds", 61 * time.Second, "1m 1s", "1m 1s"},
		{"59m59s", 59*time.Minute + 59*time.Second, "59m 59s", "59m 59s"},
		{"one hour", time.Hour, "1h", "1h"},
		{"1h1m", time.Hour + time.Minute, "1h 1m", "1h 1m"},
		{"23h59m59s", 23*time.Hour + 59*time.Minute + 59*time.Second, "23h 59m", "23h 59m"},
		{"one day", testDay, "1d", "1d"},
		{"25 hours", 25 * time.Hour, "1d 1h", "1d 1h"},
		{"25h1m drops minutes in human style", 25*time.Hour + time.Minute, "1d 1h", "1d 1h 1m"},
		{"49h30m", 49*time.Hour + 30*time.Minute, "2d 1h", "2d 1h 30m"},
		{"6d23h", 6*testDay + 23*time.Hour, "6d 23h", "6d 23h"},
		{"one week", testWeek, "1w", "1w"},
		{"8d3h4m", 8*testDay + 3*time.Hour + 4*time.Minute, "1w 1d", "1w 1d 3h 4m"},
		{"15 days", 15 * testDay, "2w 1d", "2w 1d"},
		{"negative seconds", -5 * time.Second, "< 1s", "-5s"},
		{"negative minutes", -90 * time.Second, "< 1s", "-90s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.human, FormatHumanDuration(tt.d), "FormatHumanDuration(%v)", tt.d)
			assert.Equal(t, tt.exact, FormatExactDuration(tt.d), "FormatExactDuration(%v)", tt.d)
		})
	}
}

// The two seconds-based formatters agree up to a day and then diverge: the ETA
// style reports days, the remaining-time style stops at hours. Pinning both over
// the same inputs shows where they differ.
func TestFormatSecondsStyles(t *testing.T) {
	tests := []struct {
		name      string
		seconds   int64
		eta       string
		remaining string
	}{
		{"zero", 0, "0s", "< 1s"},
		{"one second", 1, "1s", "1s"},
		{"59 seconds", 59, "59s", "59s"},
		{"60 seconds", 60, "1m 0s", "1m 0s"},
		{"61 seconds", 61, "1m 1s", "1m 1s"},
		{"59m59s", 3599, "59m 59s", "59m 59s"},
		{"one hour", 3600, "1h 0m", "1h 0m"},
		{"1h1m", 3660, "1h 1m", "1h 1m"},
		{"23h59m59s", 86399, "23h 59m", "23h 59m"},
		{"one day", 86400, "1d 0h", "24h 0m"},
		{"25 hours", 90000, "1d 1h", "25h 0m"},
		{"25h1m", 90060, "1d 1h", "25h 1m"},
		{"49h30m", 178200, "2d 1h", "49h 30m"},
		{"one week", 604800, "7d 0h", "168h 0m"},
		{"15 days", 1296000, "15d 0h", "360h 0m"},
		{"negative seconds", -5, "-5s", "< 1s"},
		{"negative minutes", -90, "-90s", "< 1s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.eta, FormatETA(tt.seconds), "FormatETA(%d)", tt.seconds)
			assert.Equal(t, tt.remaining, FormatDurationSeconds(tt.seconds), "FormatDurationSeconds(%d)", tt.seconds)
		})
	}
}
