package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/block/schemabot/pkg/storage"
)

// Exercise the collector through the real Prometheus exporter: failures retain
// the last good depth, while a successful empty snapshot explicitly clears it.
func TestCollectWebhookInboxClaimableDepthExport(t *testing.T) {
	registry := prometheus.NewRegistry()
	exporter, err := otelprom.New(otelprom.WithRegisterer(registry))
	require.NoError(t, err)
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(previous)
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Second)
		defer cancel()
		require.NoError(t, provider.Shutdown(ctx))
	})

	store := &fakeWebhookEventStore{stats: &storage.WebhookInboxStats{ClaimableDepth: 11}}
	svc := newInboxMetricsTestService(t, store)
	assertDepth := func(want float64) {
		t.Helper()
		families, gatherErr := registry.Gather()
		require.NoError(t, gatherErr)
		for _, family := range families {
			if family.GetName() != "schemabot_webhook_inbox_claimable_depth" {
				continue
			}
			require.Len(t, family.Metric, 1, "one shared-inbox gauge, without a state dimension")
			sample := family.Metric[0]
			require.NotNil(t, sample.Gauge)
			assert.Equal(t, want, sample.Gauge.GetValue())
			labels := map[string]string{}
			for _, label := range sample.Label {
				labels[label.GetName()] = label.GetValue()
			}
			assert.Equal(t, "unknown", labels["environment"])
			for _, forbidden := range []string{"state", "repository", "pull_request", "delivery", "scheduled"} {
				assert.NotContains(t, labels, forbidden)
			}
			return
		}
		t.Fatal("claimable-depth gauge missing from Prometheus export")
	}

	svc.CollectWebhookInboxMetrics(t.Context())
	assertDepth(11)

	store.err = errors.New("snapshot unavailable")
	svc.CollectWebhookInboxMetrics(t.Context())
	assertDepth(11)
	families, err := registry.Gather()
	require.NoError(t, err)
	var failures float64
	for _, family := range families {
		if family.GetName() == "schemabot_webhook_inbox_stats_collection_failures_total" {
			require.Len(t, family.Metric, 1)
			failures = family.Metric[0].GetCounter().GetValue()
		}
	}
	assert.Equal(t, float64(1), failures)

	store.err = nil
	store.stats = &storage.WebhookInboxStats{}
	svc.CollectWebhookInboxMetrics(t.Context())
	assertDepth(0)
}
