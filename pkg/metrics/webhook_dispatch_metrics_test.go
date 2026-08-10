package metrics

import (
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collectHistogramPoints collects the named float64 histogram's data points
// from the reader.
func collectHistogramPoints(t *testing.T, reader *metric.ManualReader, name string) []metricdata.HistogramDataPoint[float64] {
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
			require.True(t, ok)
			points = append(points, hist.DataPoints...)
		}
	}
	return points
}

func attrString(t *testing.T, attrs attribute.Set, key string) string {
	t.Helper()
	value, ok := attrs.Value(attribute.Key(key))
	require.True(t, ok, "attribute %q missing", key)
	return value.AsString()
}

// The dispatch-lag histogram records the wait in seconds with the event type
// and repository attributes so backlog latency can be sliced per event.
func TestRecordWebhookInboxDispatchLag(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	previousProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	defer func() {
		otel.SetMeterProvider(previousProvider)
		require.NoError(t, mp.Shutdown(t.Context()))
	}()

	RecordWebhookInboxDispatchLag(t.Context(), "pull_request", "org/repo", 90*time.Second)

	points := collectHistogramPoints(t, reader, "schemabot.webhook.inbox_dispatch_lag_seconds")
	require.Len(t, points, 1)
	assert.Equal(t, uint64(1), points[0].Count)
	assert.InDelta(t, 90.0, points[0].Sum, 0.001)
	assert.Equal(t, "pull_request", attrString(t, points[0].Attributes, "event_type"))
	assert.Equal(t, "org/repo", attrString(t, points[0].Attributes, "repository"))
}

// A negative lag (cross-pod clock skew between the enqueueing and claiming
// replica) must clamp to zero, and an unrecognized event type must fold to
// "unknown" so the histogram's cardinality stays bounded.
func TestRecordWebhookInboxDispatchLagClampsAndFolds(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	previousProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	defer func() {
		otel.SetMeterProvider(previousProvider)
		require.NoError(t, mp.Shutdown(t.Context()))
	}()

	RecordWebhookInboxDispatchLag(t.Context(), "not_a_real_event", "org/repo", -3*time.Second)

	points := collectHistogramPoints(t, reader, "schemabot.webhook.inbox_dispatch_lag_seconds")
	require.Len(t, points, 1)
	assert.Equal(t, uint64(1), points[0].Count)
	assert.InDelta(t, 0.0, points[0].Sum, 0.001)
	assert.Equal(t, "unknown", attrString(t, points[0].Attributes, "event_type"))
}

// The dispatch-duration histogram records the claim's duration in seconds
// keyed by the allowlisted outcome.
func TestRecordWebhookDispatchDuration(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	previousProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	defer func() {
		otel.SetMeterProvider(previousProvider)
		require.NoError(t, mp.Shutdown(t.Context()))
	}()

	RecordWebhookDispatchDuration(t.Context(), "pull_request", "org/repo", "completed", 2500*time.Millisecond)

	points := collectHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
	require.Len(t, points, 1)
	assert.Equal(t, uint64(1), points[0].Count)
	assert.InDelta(t, 2.5, points[0].Sum, 0.001)
	assert.Equal(t, "pull_request", attrString(t, points[0].Attributes, "event_type"))
	assert.Equal(t, "org/repo", attrString(t, points[0].Attributes, "repository"))
	assert.Equal(t, "completed", attrString(t, points[0].Attributes, "outcome"))
}

// An outcome outside the allowlist must fold to "unknown" so a typo at a call
// site can't add a new time series.
func TestRecordWebhookDispatchDurationFoldsUnknownOutcome(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	previousProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	defer func() {
		otel.SetMeterProvider(previousProvider)
		require.NoError(t, mp.Shutdown(t.Context()))
	}()

	RecordWebhookDispatchDuration(t.Context(), "pull_request", "org/repo", "not_a_real_outcome", time.Second)

	points := collectHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
	require.Len(t, points, 1)
	assert.Equal(t, "unknown", attrString(t, points[0].Attributes, "outcome"))
}
