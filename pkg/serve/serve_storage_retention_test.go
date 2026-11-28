package serve

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook"
)

// A retention window shorter than what the webhook reconciler still reads would
// make it synthesize deliveries it already has, and one shorter than GitHub's
// redelivery limit would let a redelivery of a purged delivery run again, so
// startup refuses both and the error names the key to change and the numbers
// behind the floor.
func TestValidateStorageRetentionWindows(t *testing.T) {
	// With the default 48h lookback the reconciler reads the lookback plus the
	// grace plus the failed-delivery cool-off, which is under GitHub's 3-day
	// redelivery limit, so the redelivery limit is the floor.
	defaultCoverage := 48*time.Hour + 15*time.Minute + storage.WebhookEventFailedCoverageCoolOff
	redeliveryFloor := 72 * time.Hour

	for _, tc := range []struct {
		name      string
		retention api.StorageRetentionConfig
		lookback  string
		wantErr   []string
	}{
		{
			name:      "not enabled is never checked",
			retention: api.StorageRetentionConfig{WebhookEvents: "1h"},
		},
		{
			name:      "default window clears the default lookback",
			retention: api.StorageRetentionConfig{Enabled: true},
		},
		{
			name:      "exactly the redelivery floor is allowed",
			retention: api.StorageRetentionConfig{Enabled: true, WebhookEvents: redeliveryFloor.String()},
		},
		{
			name:      "a window above the reconciler's reach but below the redelivery limit is refused",
			retention: api.StorageRetentionConfig{Enabled: true, WebhookEvents: "60h"},
			wantErr: []string{
				"storage_retention.webhook_events is 60h0m0s",
				"72h0m0s GitHub keeps a delivery redeliverable",
				"at least 72h0m0s",
			},
		},
		{
			name:      "exactly the reconciler's reach is still below the redelivery limit",
			retention: api.StorageRetentionConfig{Enabled: true, WebhookEvents: defaultCoverage.String()},
			wantErr:   []string{"at least 72h0m0s"},
		},
		{
			name:      "a window one minute under the redelivery floor is refused",
			retention: api.StorageRetentionConfig{Enabled: true, WebhookEvents: (redeliveryFloor - time.Minute).String()},
			wantErr:   []string{"storage_retention.webhook_events"},
		},
		{
			name:      "a raised lookback names the reconciler's terms",
			retention: api.StorageRetentionConfig{Enabled: true, WebhookEvents: "80h"},
			lookback:  "100h",
			wantErr: []string{
				"storage_retention.webhook_events is 80h0m0s",
				"its lookback is 100h0m0s",
				"WEBHOOK_RECONCILE_LOOKBACK",
				"15m0s grace",
				"30m0s failed-delivery cool-off",
				"at least 100h45m0s",
			},
		},
		{
			name:      "a raised lookback raises the floor above the default window",
			retention: api.StorageRetentionConfig{Enabled: true},
			lookback:  "200h",
			wantErr: []string{
				"storage_retention.webhook_events is 168h0m0s",
				"its lookback is 200h0m0s",
				"at least 200h45m0s",
			},
		},
		{
			name:      "a window that clears the raised lookback is allowed",
			retention: api.StorageRetentionConfig{Enabled: true, WebhookEvents: "240h"},
			lookback:  "200h",
		},
		{
			name:      "a shortened lookback cannot lower the floor below the redelivery limit",
			retention: api.StorageRetentionConfig{Enabled: true, WebhookEvents: "13h"},
			lookback:  "12h",
			wantErr:   []string{"at least 72h0m0s"},
		},
		{
			name:      "a malformed lookback leaves the default in force",
			retention: api.StorageRetentionConfig{Enabled: true, WebhookEvents: "73h"},
			lookback:  "soon",
		},
		{
			name:      "an unparseable window is reported as the config error it is",
			retention: api.StorageRetentionConfig{Enabled: true, WebhookEvents: "a week"},
			wantErr:   []string{"storage_retention.webhook_events"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("WEBHOOK_RECONCILE_LOOKBACK", tc.lookback)
			cfg := &api.ServerConfig{StorageRetention: tc.retention}

			err := validateStorageRetentionWindows(cfg)

			if len(tc.wantErr) == 0 {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			for _, want := range tc.wantErr {
				assert.ErrorContains(t, err, want)
			}
		})
	}
}

// The floor adds the reconciler's own grace and the failed-delivery cool-off to
// whichever lookback is in force, and an unset lookback means the default.
func TestReconcileInboxCoverageWindowAddsEveryTermToTheLookback(t *testing.T) {
	window := webhook.ReconcileInboxCoverageWindow(0)
	assert.Positive(t, window.Lookback, "an unset lookback means the default is in force")
	assert.Positive(t, window.Grace)
	assert.Equal(t, storage.WebhookEventFailedCoverageCoolOff, window.FailedCoverageCoolOff)
	assert.Equal(t, window.Lookback+window.Grace+window.FailedCoverageCoolOff, window.Total())

	raised := webhook.ReconcileInboxCoverageWindow(10 * time.Hour)
	assert.Equal(t, 10*time.Hour, raised.Lookback)
	assert.Equal(t, window.Grace, raised.Grace)
}

// Build is what every deployment goes through, so the window floor is checked
// there, before any storage is opened, and a server with a short window never
// starts.
func TestBuildRefusesARetentionWindowBelowTheFloor(t *testing.T) {
	t.Setenv("WEBHOOK_RECONCILE_LOOKBACK", "")
	cfg := &api.ServerConfig{StorageRetention: api.StorageRetentionConfig{Enabled: true, WebhookEvents: "24h"}}

	_, err := Build(t.Context(), cfg)

	require.ErrorContains(t, err, "storage_retention.webhook_events is 24h0m0s")
}
