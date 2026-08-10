package serve

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWebhookReconcileSynthesisOptions pins the kill-switch contract: the
// reconciler's missing-head synthesis defaults on, WEBHOOK_RECONCILE_SYNTHESIS
// set to false drops it back to a report-only scan, and an unparseable value
// fails safe to disabled — setting the variable at all signals intent to turn
// synthesis off, so a malformed value must not leave it running.
func TestWebhookReconcileSynthesisOptions(t *testing.T) {
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
			t.Setenv("WEBHOOK_RECONCILE_SYNTHESIS", tc.value)

			opts := webhookReconcileSynthesisOptions(logger)

			if tc.enabled {
				require.Len(t, opts, 1)
			} else {
				require.Empty(t, opts)
			}
		})
	}
}
