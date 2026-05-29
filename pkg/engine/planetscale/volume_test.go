package planetscale

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVolumeToThrottleRatio(t *testing.T) {
	tests := []struct {
		volume   int32
		expected float64
	}{
		{0, 0.95},  // below min, clamped to max throttle
		{1, 0.95},  // max throttle
		{2, 0.85},  // default volume
		{6, 0.45},  // mid-range
		{10, 0.05}, // near no throttle
		{11, 0.0},  // no throttle
		{12, 0.0},  // above max, clamped to no throttle
	}

	for _, tt := range tests {
		ratio := volumeToThrottleRatio(tt.volume)
		assert.InDelta(t, tt.expected, ratio, 0.001, "volume %d", tt.volume)
	}
}
