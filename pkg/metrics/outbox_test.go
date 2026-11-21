package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestOutboxOldestUnpublishedAge(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(previous)
		require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context())))
	})
	for _, age := range []time.Duration{42 * time.Second, 0} {
		RecordOutboxOldestUnpublishedAge(t.Context(), age)
		var data metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(t.Context(), &data))
		require.Len(t, data.ScopeMetrics, 1)
		require.Len(t, data.ScopeMetrics[0].Metrics, 1)
		instrument := data.ScopeMetrics[0].Metrics[0]
		assert.Equal(t, "schemabot.outbox.oldest_unpublished_age", instrument.Name)
		assert.Equal(t, "s", instrument.Unit)
		gauge, ok := instrument.Data.(metricdata.Gauge[int64])
		require.True(t, ok)
		require.Len(t, gauge.DataPoints, 1)
		assert.Equal(t, int64(age.Seconds()), gauge.DataPoints[0].Value)
		assert.Equal(t, attribute.NewSet(EnvironmentAttribute("")), gauge.DataPoints[0].Attributes)
	}
}
