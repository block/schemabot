package serve

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCheckSuiteRecoveryOptions pins the kill-switch contract: durable
// check-suite recovery defaults on, WEBHOOK_CHECK_SUITE_RECOVERY set to false
// drops the webhook endpoint to acknowledge-and-ignore for check_suite
// deliveries, and an unparseable value fails safe to disabled — setting the
// variable at all signals intent to turn recovery off, so a malformed value
// must not leave it running.
func TestCheckSuiteRecoveryOptions(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	for _, tc := range []struct {
		name    string
		value   string
		enabled bool
	}{
		{"default enabled when unset", "", true},
		{"explicitly enabled", "true", true},
		{"disabled", "false", false},
		{"invalid value fails safe to disabled", "not-a-bool", false},
		{"non-ParseBool disable spelling fails safe to disabled", "off", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("WEBHOOK_CHECK_SUITE_RECOVERY", tc.value)

			opts := checkSuiteRecoveryOptions(logger)

			if tc.enabled {
				require.Len(t, opts, 1)
			} else {
				require.Empty(t, opts)
			}
		})
	}
}
