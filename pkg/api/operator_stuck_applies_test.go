package api

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/block/schemabot/pkg/storage"
)

// fakeStuckApplyStore serves a fixed FindStuckPendingApplies result; every other
// ApplyStore method is unused by the stuck-pending monitor.
type fakeStuckApplyStore struct {
	storage.ApplyStore
	stuck  []*storage.Apply
	err    error
	calls  int
	onScan func()
}

func (f *fakeStuckApplyStore) FindStuckPendingApplies(context.Context, time.Duration, int) ([]*storage.Apply, error) {
	f.calls++
	if f.onScan != nil {
		f.onScan()
	}
	return f.stuck, f.err
}

// stuckPendingMockStorage wires a fake ApplyStore into the mock storage so the
// monitor can read it.
type stuckPendingMockStorage struct {
	mockStorage
	applies storage.ApplyStore
}

func (m *stuckPendingMockStorage) Applies() storage.ApplyStore { return m.applies }

func newStuckPendingTestService(t *testing.T, applies storage.ApplyStore) *Service {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&stuckPendingMockStorage{applies: applies}, &ServerConfig{}, nil, logger)
}

func newStuckPendingMetricReader(t *testing.T) sdkmetric.Reader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		// Cleanup runs after the test's context is cancelled; strip the
		// cancellation so the shutdown call gets a live context.
		require.NoError(t, mp.Shutdown(context.WithoutCancel(t.Context())))
	})
	return reader
}

// gaugeValue returns the single data point value for a gauge metric, or -1 when
// the metric was not emitted.
func gaugeValue(t *testing.T, reader sdkmetric.Reader, name string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok)
			require.Len(t, gauge.DataPoints, 1)
			return gauge.DataPoints[0].Value
		}
	}
	return -1
}

func metricNames(t *testing.T, reader sdkmetric.Reader) map[string]bool {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	names := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names[m.Name] = true
		}
	}
	return names
}

// A driver should already have claimed every pending apply the scan returns, so
// the monitor records their count as the stuck-pending gauge for an operator to
// alert on.
func TestCollectOperatorStuckPendingMetricsRecordsGauge(t *testing.T) {
	reader := newStuckPendingMetricReader(t)

	store := &fakeStuckApplyStore{stuck: []*storage.Apply{
		{ApplyIdentifier: "apply_oldest", Database: "db1", Environment: "staging", CreatedAt: time.Now().Add(-40 * time.Minute)},
		{ApplyIdentifier: "apply_old", Database: "db2", Environment: "staging", CreatedAt: time.Now().Add(-20 * time.Minute)},
	}}
	svc := newStuckPendingTestService(t, store)

	svc.CollectOperatorStuckPendingMetrics(t.Context())

	assert.Equal(t, int64(2), gaugeValue(t, reader, "schemabot.operator.stuck_pending_applies"))
	assert.Equal(t, 1, store.calls)
	assert.NotContains(t, metricNames(t, reader), "schemabot.operator.stuck_pending_scan_failures",
		"a successful scan must not increment the failure counter")
}

// With nothing stuck the gauge must still be recorded as 0: it is a last-value
// instrument, so skipping the record when the backlog clears would freeze it at
// its last nonzero value and show a phantom stuck population forever.
func TestCollectOperatorStuckPendingMetricsRecordsZeroWhenHealthy(t *testing.T) {
	reader := newStuckPendingMetricReader(t)

	svc := newStuckPendingTestService(t, &fakeStuckApplyStore{stuck: nil})

	svc.CollectOperatorStuckPendingMetrics(t.Context())

	assert.Equal(t, int64(0), gaugeValue(t, reader, "schemabot.operator.stuck_pending_applies"))
}

// A scan failure must not emit the gauge: it is a last-value instrument, so
// leaving it untouched re-exports the last-good value and reads as a healthy
// operator. Instead it increments the scan-failure counter — the liveness signal
// that the gauge is stale — and must not panic the monitor loop.
func TestCollectOperatorStuckPendingMetricsCountsFailureOnStoreError(t *testing.T) {
	reader := newStuckPendingMetricReader(t)

	svc := newStuckPendingTestService(t, &fakeStuckApplyStore{err: errors.New("db down")})

	svc.CollectOperatorStuckPendingMetrics(t.Context())

	names := metricNames(t, reader)
	assert.NotContains(t, names, "schemabot.operator.stuck_pending_applies", "the gauge must not be re-emitted on error")
	assert.Contains(t, names, "schemabot.operator.stuck_pending_scan_failures", "a failed scan must increment the failure counter")
}

// A shutdown that lands mid-scan cancels the monitor context and fails the
// query with a cancellation error. That is orderly teardown, not a stale gauge:
// a routine deploy must not tick the scan-failure counter (the "gauge must not
// be trusted" liveness signal) or log the failure WARN.
func TestCollectOperatorStuckPendingMetricsIgnoresShutdownMidScan(t *testing.T) {
	reader := newStuckPendingMetricReader(t)

	ctx, cancel := context.WithCancel(t.Context())
	store := &fakeStuckApplyStore{err: context.Canceled, onScan: cancel}
	svc := newStuckPendingTestService(t, store)

	svc.CollectOperatorStuckPendingMetrics(ctx)

	names := metricNames(t, reader)
	assert.NotContains(t, names, "schemabot.operator.stuck_pending_applies", "the gauge must not be re-emitted on error")
	assert.NotContains(t, names, "schemabot.operator.stuck_pending_scan_failures",
		"a scan aborted by shutdown must not count as a scan failure")
	assert.Equal(t, 1, store.calls)
}

// The monitor is a no-op when apply storage is unavailable: there is nothing to
// scan, so it must neither emit the gauge nor panic.
func TestCollectOperatorStuckPendingMetricsNoOpWhenStorageUnavailable(t *testing.T) {
	reader := newStuckPendingMetricReader(t)

	svc := newStuckPendingTestService(t, nil)

	svc.CollectOperatorStuckPendingMetrics(t.Context())

	assert.NotContains(t, metricNames(t, reader), "schemabot.operator.stuck_pending_applies")
}
