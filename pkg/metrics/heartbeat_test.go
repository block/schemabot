package metrics

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Idle collections observe a process heartbeat without any Record call or storage.
func TestTelemetryHeartbeatRegistry(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context()))) })
	require.NoError(t, RegisterTelemetryHeartbeat(provider))
	for range 2 {
		var data metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(t.Context(), &data))
		require.Len(t, data.ScopeMetrics, 1)
		require.Len(t, data.ScopeMetrics[0].Metrics, 1)
		m := data.ScopeMetrics[0].Metrics[0]
		assert.Equal(t, "schemabot.telemetry.heartbeat", m.Name)
		assert.Equal(t, "1", m.Unit)
		gauge, ok := m.Data.(metricdata.Gauge[int64])
		require.True(t, ok)
		require.Len(t, gauge.DataPoints, 1)
		assert.Equal(t, int64(1), gauge.DataPoints[0].Value)
		assert.Equal(t, attribute.NewSet(attribute.String("environment", "unknown")), gauge.DataPoints[0].Attributes)
	}
}
