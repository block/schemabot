package serve

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/webhook"
)

// TestParseSynchronizeQuietPeriod pins the tuning contract for the
// synchronize quiet period: unset keeps the default, 0 turns it off, any other
// duration replaces it, and a malformed or negative value is rejected to the
// default rather than disabling the coalescing it exists for.
func TestParseSynchronizeQuietPeriod(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	for _, tc := range []struct {
		name  string
		value string
		want  time.Duration
	}{
		{"unset keeps the default", "", webhook.DefaultSynchronizeQuietPeriod},
		{"zero turns it off", "0", 0},
		{"zero with a unit turns it off", "0s", 0},
		{"duration replaces the default", "30s", 30 * time.Second},
		{"malformed value keeps the default", "soon", webhook.DefaultSynchronizeQuietPeriod},
		{"negative value keeps the default", "-5s", webhook.DefaultSynchronizeQuietPeriod},
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
