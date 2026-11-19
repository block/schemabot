package serve

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestParseSynchronizeQuietPeriod pins the tuning contract for the
// synchronize quiet period: unset keeps the default, 0 turns it off, any other
// duration up to the ceiling replaces it, and a malformed, negative, or
// above-ceiling value is rejected to the default rather than disabling the
// coalescing or holding every push's Check Run back. The default is written
// out rather than read from the constant, so changing it fails here.
func TestParseSynchronizeQuietPeriod(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	for _, tc := range []struct {
		name  string
		value string
		want  time.Duration
	}{
		{"unset keeps the fifteen second default", "", 15 * time.Second},
		{"zero turns it off", "0", 0},
		{"zero with a unit turns it off", "0s", 0},
		{"duration replaces the default", "30s", 30 * time.Second},
		{"the ceiling itself is accepted", "2m", 2 * time.Minute},
		{"malformed value keeps the default", "soon", 15 * time.Second},
		{"negative value keeps the default", "-5s", 15 * time.Second},
		{"value above the ceiling keeps the default", "15m", 15 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("WEBHOOK_SYNCHRONIZE_QUIET_PERIOD", tc.value)

			require.Equal(t, tc.want, parseSynchronizeQuietPeriod(logger))
			if tc.want == 0 {
				require.Empty(t, synchronizeQuietPeriodOptions(logger))
			} else {
				require.Len(t, synchronizeQuietPeriodOptions(logger), 1)
			}
		})
	}
}
