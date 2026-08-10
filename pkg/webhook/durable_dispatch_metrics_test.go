package webhook

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// newDispatchMetricsReader installs a manual OTel reader for the test's
// lifetime so the driver's dispatch metrics can be collected and asserted.
func newDispatchMetricsReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(t.Context()))
	})
	return reader
}

// collectDispatchHistogramPoints collects the named float64 histogram's data
// points from the reader.
func collectDispatchHistogramPoints(t *testing.T, reader *sdkmetric.ManualReader, name string) []metricdata.HistogramDataPoint[float64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	var points []metricdata.HistogramDataPoint[float64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			require.True(t, ok, "metric %s must be a float64 histogram", name)
			points = append(points, hist.DataPoints...)
		}
	}
	return points
}

// A first claim records the started status, the inbox dispatch lag measured
// from receipt, and exactly one duration observation with the completed
// outcome — the started/outcome pair is the ledger that surfaces drivers dying
// mid-claim.
func TestDurableWebhookDispatchMetricsFirstClaimCompleted(t *testing.T) {
	reader := newDispatchMetricsReader(t)
	store := newScriptedWebhookEventStore(&storage.WebhookEvent{
		Provider:   storage.WebhookProviderGitHub,
		DeliveryID: "delivery-metrics-1",
		Event:      "issue_comment",
		Payload:    []byte(`{}`),
		ReceivedAt: time.Now().Add(-90 * time.Second),
	})
	h := newDurableDriverHandler(t, store, nil, nil)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Len(t, store.completed, 1, "unsupported event must be marked completed")

	lagPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.inbox_dispatch_lag_seconds")
	require.Len(t, lagPoints, 1, "first claim must record inbox dispatch lag")
	assert.Equal(t, uint64(1), lagPoints[0].Count)
	assert.GreaterOrEqual(t, lagPoints[0].Sum, 90.0)
	assertStringAttr(t, lagPoints[0].Attributes, "event_type", "issue_comment")

	durationPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
	require.Len(t, durationPoints, 1, "the claim must record exactly one outcome")
	assert.Equal(t, uint64(1), durationPoints[0].Count)
	assertStringAttr(t, durationPoints[0].Attributes, "event_type", "issue_comment")
	assertStringAttr(t, durationPoints[0].Attributes, "outcome", "completed")
}

// A retry claim measures the retry window, not backlog latency, so only the
// first claim records inbox dispatch lag.
func TestDurableWebhookDispatchMetricsRetryClaimSkipsInboxLag(t *testing.T) {
	reader := newDispatchMetricsReader(t)
	store := newScriptedWebhookEventStore(&storage.WebhookEvent{
		Provider:   storage.WebhookProviderGitHub,
		DeliveryID: "delivery-metrics-retry",
		Event:      "issue_comment",
		Payload:    []byte(`{}`),
		Attempts:   1, // FindNext claim increments to 2
		ReceivedAt: time.Now().Add(-90 * time.Second),
	})
	h := newDurableDriverHandler(t, store, nil, nil)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Len(t, store.completed, 1)
	lagPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.inbox_dispatch_lag_seconds")
	assert.Empty(t, lagPoints, "a retry claim must not record inbox dispatch lag")

	durationPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
	require.Len(t, durationPoints, 1)
	assertStringAttr(t, durationPoints[0].Attributes, "outcome", "completed")
}

// A retryable processing failure below the attempt cap records the retrying
// outcome.
func TestDurableWebhookDispatchMetricsRetryingOutcome(t *testing.T) {
	reader := newDispatchMetricsReader(t)
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	h := newDurableDriverHandler(t, store, nil, nil)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return true, errors.New("transient auto-plan failure")
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Len(t, store.failed, 1)
	durationPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
	require.Len(t, durationPoints, 1)
	assertStringAttr(t, durationPoints[0].Attributes, "outcome", "retrying")
}

// A non-retryable processing failure records the failed outcome.
func TestDurableWebhookDispatchMetricsFailedOutcome(t *testing.T) {
	reader := newDispatchMetricsReader(t)
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	h := newDurableDriverHandler(t, store, nil, nil)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return false, errors.New("deterministic rejection")
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Len(t, store.failed, 1)
	durationPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
	require.Len(t, durationPoints, 1)
	assertStringAttr(t, durationPoints[0].Attributes, "outcome", "failed")
}

// A shutdown-interrupted claim that is refunded records the released outcome —
// not failed or retrying, since no genuine attempt completed.
func TestDurableWebhookDispatchMetricsReleasedOutcome(t *testing.T) {
	reader := newDispatchMetricsReader(t)
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	h := newDurableDriverHandler(t, store, nil, nil)

	driverCtx, cancelDriver := context.WithCancel(t.Context())
	defer cancelDriver()
	h.durableWebhookProcessOverride = func(ctx context.Context, _ *storage.WebhookEvent) (bool, error) {
		cancelDriver()
		<-ctx.Done()
		return true, ctx.Err()
	}

	h.driveNextDurableWebhook(driverCtx, 0, "test-host/1/webhook-driver-0")

	require.Len(t, store.released, 1)
	durationPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
	require.Len(t, durationPoints, 1)
	assertStringAttr(t, durationPoints[0].Attributes, "outcome", "released")
}

// Losing the lease heartbeat after successful processing leaves the row for
// reclaim and records the lease_lost outcome.
func TestDurableWebhookDispatchMetricsLeaseLostOutcome(t *testing.T) {
	reader := newDispatchMetricsReader(t)
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	store.heartbeatErr = storage.ErrWebhookEventLeaseLost
	h := newDurableDriverHandler(t, store, nil, nil)
	h.durableWebhookLeaseDuration = 30 * time.Millisecond
	h.durableWebhookProcessOverride = func(ctx context.Context, _ *storage.WebhookEvent) (bool, error) {
		<-ctx.Done()
		return false, nil
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	require.Empty(t, store.completed)
	durationPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
	require.Len(t, durationPoints, 1)
	assertStringAttr(t, durationPoints[0].Attributes, "outcome", "lease_lost")
}
