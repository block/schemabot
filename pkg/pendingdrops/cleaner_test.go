package pendingdrops

import (
	"bytes"
	"io"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newManualMetricReader swaps in a manual-reader meter provider for the test's
// duration so emitted counters can be collected and asserted on.
func newManualMetricReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previousProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(previousProvider)
	})
	return reader
}

// cleanupErrorReasonsByDatabase collects the pending-drops cleanup error
// counter and returns each datapoint's reason keyed by database.
func cleanupErrorReasonsByDatabase(t *testing.T, reader *sdkmetric.ManualReader) map[string]string {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	reasons := map[string]string{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "schemabot.pending_drops.cleanup_errors_total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				database, ok := dp.Attributes.Value(attribute.Key("database"))
				require.True(t, ok)
				reason, ok := dp.Attributes.Value(attribute.Key("reason"))
				require.True(t, ok)
				reasons[database.AsString()] = reason.AsString()
			}
		}
	}
	return reasons
}

// TestCleaner_TargetWithoutLockerFailsClosed verifies a target missing its
// advisory-lock implementation is rejected before any connection is opened:
// cleanup must not run unserialized against a target, and must not assume
// MySQL lock semantics for an engine the producer did not declare. The
// failure is counted with the locker_missing reason so a permanent producer
// wiring bug is distinguishable from transient connectivity errors on
// dashboards.
func TestCleaner_TargetWithoutLockerFailsClosed(t *testing.T) {
	reader := newManualMetricReader(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	targets := []Target{{Database: "testdb", Environment: "staging", DSN: "root:pw@tcp(127.0.0.1:1)/testdb?timeout=2s"}}
	cleaner := NewCleaner(targets, DefaultRetention, false, logger)

	err := cleaner.Run(t.Context())
	require.ErrorContains(t, err, "no advisory locker")

	reasons := cleanupErrorReasonsByDatabase(t, reader)
	assert.Equal(t, "locker_missing", reasons["testdb"])
}

// TestCleaner_RunContinuesPastFailedTarget verifies targets are independent:
// a target that fails closed must not prevent the cleaner from attempting the
// remaining targets in the same pass, and each failed target is logged and
// counted individually.
func TestCleaner_RunContinuesPastFailedTarget(t *testing.T) {
	reader := newManualMetricReader(t)
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	targets := []Target{
		{Database: "firstdb", Environment: "staging", DSN: "root:pw@tcp(127.0.0.1:1)/firstdb?timeout=2s"},
		{Database: "seconddb", Environment: "staging", DSN: "root:pw@tcp(127.0.0.1:1)/seconddb?timeout=2s"},
	}
	cleaner := NewCleaner(targets, DefaultRetention, false, logger)

	err := cleaner.Run(t.Context())

	// The returned error is the last failure, proving the pass reached the
	// second target after the first failed.
	require.ErrorContains(t, err, "seconddb")
	assert.Contains(t, logs.String(), "firstdb")
	assert.Contains(t, logs.String(), "seconddb")

	reasons := cleanupErrorReasonsByDatabase(t, reader)
	assert.Equal(t, "locker_missing", reasons["firstdb"])
	assert.Equal(t, "locker_missing", reasons["seconddb"])
}
