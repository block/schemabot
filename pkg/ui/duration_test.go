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

// Every duration SchemaBot shows reads the same way: the two largest non-zero
// units, with a zero unit left out. A duration under a second, zero, or
// negative reads "< 1s".
func TestFormatHumanDuration(t *testing.T) {
	tests := []struct {
		name string
		d    time.Duration
		want string
	}{
		{"zero", 0, "< 1s"},
		{"sub-second", 500 * time.Millisecond, "< 1s"},
		{"just under a second", 999 * time.Millisecond, "< 1s"},
		{"one second", time.Second, "1s"},
		{"fractional second truncates", 1500 * time.Millisecond, "1s"},
		{"59 seconds", 59 * time.Second, "59s"},
		{"60 seconds drops the zero seconds", 60 * time.Second, "1m"},
		{"90 seconds", 90 * time.Second, "1m 30s"},
		{"61 seconds", 61 * time.Second, "1m 1s"},
		{"30 minutes drops the zero seconds", 30 * time.Minute, "30m"},
		{"59m59s", 59*time.Minute + 59*time.Second, "59m 59s"},
		{"one hour", time.Hour, "1h"},
		{"1h1m", time.Hour + time.Minute, "1h 1m"},
		{"1h1m1s drops the seconds", time.Hour + time.Minute + time.Second, "1h 1m"},
		{"23h59m59s", 23*time.Hour + 59*time.Minute + 59*time.Second, "23h 59m"},
		{"one day", testDay, "1d"},
		{"25 hours", 25 * time.Hour, "1d 1h"},
		{"25h1m drops the minutes", 25*time.Hour + time.Minute, "1d 1h"},
		{"30 hours", 30 * time.Hour, "1d 6h"},
		{"49h30m", 49*time.Hour + 30*time.Minute, "2d 1h"},
		{"6d23h", 6*testDay + 23*time.Hour, "6d 23h"},
		{"one week", testWeek, "1w"},
		{"8d3h drops the hours", 8*testDay + 3*time.Hour, "1w 1d"},
		{"15 days", 15 * testDay, "2w 1d"},
		{"negative seconds", -5 * time.Second, "< 1s"},
		{"negative minutes", -90 * time.Second, "< 1s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, FormatHumanDuration(tt.d), "FormatHumanDuration(%v)", tt.d)
		})
	}
}

// FormatETA takes seconds and reads them exactly as FormatHumanDuration reads
// the same duration, so a table's ETA and a revert window's time left match
// every other duration on screen.
func TestFormatETAReadsAsHumanDuration(t *testing.T) {
	tests := []struct {
		name    string
		seconds int64
		want    string
	}{
		{"zero", 0, "< 1s"},
		{"one second", 1, "1s"},
		{"59 seconds", 59, "59s"},
		{"60 seconds", 60, "1m"},
		{"195 seconds", 195, "3m 15s"},
		{"30 minutes", 1800, "30m"},
		{"one hour", 3600, "1h"},
		{"1h1m", 3660, "1h 1m"},
		{"one day", 86400, "1d"},
		{"25h1m", 90060, "1d 1h"},
		{"30 hours", 108000, "1d 6h"},
		{"one week", 604800, "1w"},
		{"negative", -90, "< 1s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, FormatETA(tt.seconds), "FormatETA(%d)", tt.seconds)
			assert.Equal(t, FormatHumanDuration(time.Duration(tt.seconds)*time.Second), FormatETA(tt.seconds))
		})
	}
}
