package serve

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestParseWebhookReconcileScanBounds pins the tuning contract for the
// reconciler's missing-delivery scan: an unset variable yields zero (keep the
// default), each variable can be set on its own, and a malformed value, a
// non-positive lookback, or a page budget below the floor the pass needs is
// rejected to zero rather than shrinking or disabling the scan.
func TestParseWebhookReconcileScanBounds(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	for _, tc := range []struct {
		name         string
		maxPages     string
		lookback     string
		wantMaxPages int
		wantLookback time.Duration
	}{
		{"unset yields zero for both", "", "", 0, 0},
		{"both set", "12", "24h", 12, 24 * time.Hour},
		{"max pages alone", "8", "", 8, 0},
		{"lookback alone", "", "36h", 0, 36 * time.Hour},
		{"non-numeric max pages rejected", "many", "24h", 0, 24 * time.Hour},
		{"zero max pages rejected", "0", "", 0, 0},
		{"one max page rejected", "1", "", 0, 0},
		{"smallest usable max pages accepted", "2", "", 2, 0},
		{"negative max pages rejected", "-3", "", 0, 0},
		{"malformed lookback rejected", "8", "2 days", 8, 0},
		{"negative lookback rejected", "", "-1h", 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("WEBHOOK_RECONCILE_MAX_PAGES", tc.maxPages)
			t.Setenv("WEBHOOK_RECONCILE_LOOKBACK", tc.lookback)

			maxPages, lookback := parseWebhookReconcileScanBounds(logger)

			require.Equal(t, tc.wantMaxPages, maxPages)
			require.Equal(t, tc.wantLookback, lookback)
		})
	}
}

// TestWebhookReconcileScanBoundsOptions pins that the environment yields a
// handler option only when at least one bound was accepted, so the package
// defaults stay untouched when nothing valid is configured.
func TestWebhookReconcileScanBoundsOptions(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("nothing configured", func(t *testing.T) {
		t.Setenv("WEBHOOK_RECONCILE_MAX_PAGES", "")
		t.Setenv("WEBHOOK_RECONCILE_LOOKBACK", "")
		require.Empty(t, webhookReconcileScanBoundsOptions(logger))
	})

	t.Run("only rejected values", func(t *testing.T) {
		t.Setenv("WEBHOOK_RECONCILE_MAX_PAGES", "0")
		t.Setenv("WEBHOOK_RECONCILE_LOOKBACK", "soon")
		require.Empty(t, webhookReconcileScanBoundsOptions(logger))
	})

	t.Run("one accepted value", func(t *testing.T) {
		t.Setenv("WEBHOOK_RECONCILE_MAX_PAGES", "")
		t.Setenv("WEBHOOK_RECONCILE_LOOKBACK", "12h")
		require.Len(t, webhookReconcileScanBoundsOptions(logger), 1)
	})
}
