package metrics

import (
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An unrecognized control operation must fold to "unknown" so a typo or a
// future unhandled operation can't blow up the stale-pending counter's
// cardinality.
func TestRecordControlRequestStalePendingFoldsUnknownOperation(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	previousProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	defer func() {
		otel.SetMeterProvider(previousProvider)
		require.NoError(t, mp.Shutdown(t.Context()))
	}()

	RecordControlRequestStalePending(t.Context(), "cancel", "testdb", "mysql", "tern-a", "staging")
	RecordControlRequestStalePending(t.Context(), "not_a_real_operation", "testdb", "mysql", "tern-a", "staging")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	countByOperation := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "schemabot.control_requests.stale_pending_total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				operation, ok := dp.Attributes.Value(attribute.Key("operation"))
				require.True(t, ok)
				countByOperation[operation.AsString()] = dp.Value
			}
		}
	}

	assert.Equal(t, int64(1), countByOperation["cancel"])
	assert.Equal(t, int64(1), countByOperation["unknown"])
	assert.NotContains(t, countByOperation, "not_a_real_operation")
}
