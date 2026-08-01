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

// An unrecognized inbox state must fold to "unknown" so a typo or a future
// unhandled state can't blow up the depth gauge's cardinality.
func TestRecordWebhookInboxDepthFoldsUnknownState(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	previousProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	defer func() {
		otel.SetMeterProvider(previousProvider)
		require.NoError(t, mp.Shutdown(t.Context()))
	}()

	RecordWebhookInboxDepth(t.Context(), "pending", 3)
	RecordWebhookInboxDepth(t.Context(), "not_a_real_state", 7)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	depthByState := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "schemabot.webhook.inbox_depth" {
				continue
			}
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok)
			for _, dp := range gauge.DataPoints {
				state, ok := dp.Attributes.Value(attribute.Key("state"))
				require.True(t, ok)
				depthByState[state.AsString()] = dp.Value
			}
		}
	}

	assert.Equal(t, int64(3), depthByState["pending"])
	assert.Equal(t, int64(7), depthByState["unknown"])
	assert.NotContains(t, depthByState, "not_a_real_state")
}

// The backlog-age gauge reports whole seconds.
func TestRecordWebhookInboxOldestClaimableAge(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	previousProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	defer func() {
		otel.SetMeterProvider(previousProvider)
		require.NoError(t, mp.Shutdown(t.Context()))
	}()

	RecordWebhookInboxOldestClaimableAge(t.Context(), 90*time.Second)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	var found bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "schemabot.webhook.inbox_oldest_claimable_age_seconds" {
				continue
			}
			found = true
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok)
			require.Len(t, gauge.DataPoints, 1)
			assert.Equal(t, int64(90), gauge.DataPoints[0].Value)
		}
	}
	assert.True(t, found)
}
