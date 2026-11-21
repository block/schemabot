package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestApplyNotificationGauges(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() {
		PublishApplyNotificationBacklog(nil)
		require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context())))
	})
	require.NoError(t, RegisterApplyNotificationMetrics(provider))
	oldest := time.Now().Add(-48 * time.Hour)
	PublishApplyNotificationBacklog(&ApplyNotificationBacklog{Oldest: &oldest, OutOfWindow: 3})
	for range 2 {
		var data metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(t.Context(), &data))
		require.Len(t, data.ScopeMetrics, 1)
		require.Len(t, data.ScopeMetrics[0].Metrics, 2)
		for _, m := range data.ScopeMetrics[0].Metrics {
			switch m.Name {
			case "schemabot.apply_notifications.out_of_window":
				gauge, ok := m.Data.(metricdata.Gauge[int64])
				require.True(t, ok)
				require.Len(t, gauge.DataPoints, 1)
				assert.Equal(t, int64(3), gauge.DataPoints[0].Value, "gauge must not accumulate each scrape")
				assert.Equal(t, attribute.NewSet(attribute.String("environment", "unknown")), gauge.DataPoints[0].Attributes)
			case "schemabot.apply_notifications.oldest_unpublished_age_seconds":
				gauge, ok := m.Data.(metricdata.Gauge[float64])
				require.True(t, ok)
				require.Len(t, gauge.DataPoints, 1)
				assert.InDelta(t, (48 * time.Hour).Seconds(), gauge.DataPoints[0].Value, 5)
			default:
				require.FailNow(t, "unexpected metric", m.Name)
			}
		}
	}
	PublishApplyNotificationBacklog(nil)
	var cleared metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &cleared))
	assert.Empty(t, cleared.ScopeMetrics)
}
