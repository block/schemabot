package api

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/block/schemabot/pkg/storage"
)

// fakeActiveApplyStore serves a fixed CountActiveApplies result; every other
// ApplyStore method is unused by the active-applies monitor.
type fakeActiveApplyStore struct {
	storage.ApplyStore
	counts []storage.ActiveApplyCount
	err    error
	calls  int
	onScan func()
}

func (f *fakeActiveApplyStore) CountActiveApplies(context.Context) ([]storage.ActiveApplyCount, error) {
	f.calls++
	if f.onScan != nil {
		f.onScan()
	}
	return f.counts, f.err
}

// activeAppliesGaugeValues returns the active-applies gauge value per target
// attribute set, or an empty map when the metric was not emitted.
func activeAppliesGaugeValues(t *testing.T, reader sdkmetric.Reader) map[activeAppliesTarget]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	values := map[activeAppliesTarget]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "schemabot.applies.active" {
				continue
			}
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok)
			for _, dp := range gauge.DataPoints {
				database, _ := dp.Attributes.Value(attribute.Key("database"))
				deployment, _ := dp.Attributes.Value(attribute.Key("deployment"))
				environment, _ := dp.Attributes.Value(attribute.Key("environment"))
				values[activeAppliesTarget{
					database:    database.AsString(),
					deployment:  deployment.AsString(),
					environment: environment.AsString(),
				}] = dp.Value
			}
		}
	}
	return values
}

// The monitor samples the in-flight apply population from storage, so the
// gauge must report every target's count from the scan — including applies
// started before this pod existed.
func TestCollectActiveAppliesMetricsRecordsPerTargetGauge(t *testing.T) {
	reader := newMonitorMetricReader(t)

	store := &fakeActiveApplyStore{counts: []storage.ActiveApplyCount{
		{Database: "db1", Deployment: "pie", Environment: "staging", Count: 2},
		{Database: "db2", Deployment: "gap", Environment: "production", Count: 1},
	}}
	svc := newMonitorTestService(t, store)

	svc.CollectActiveAppliesMetrics(t.Context())

	assert.Equal(t, map[activeAppliesTarget]int64{
		{database: "db1", deployment: "pie", environment: "staging"}:    2,
		{database: "db2", deployment: "gap", environment: "production"}: 1,
	}, activeAppliesGaugeValues(t, reader))
	assert.Equal(t, 1, store.calls)
	assert.NotContains(t, metricNames(t, reader), "schemabot.applies.active_scan_failures",
		"a successful scan must not increment the failure counter")
}

// A target absent from the latest scan has no active applies left. The gauge
// is a last-value instrument, so the monitor must record 0 for it — otherwise
// the series freezes at its final nonzero count and shows a phantom in-flight
// apply forever.
func TestCollectActiveAppliesMetricsZeroesDepartedTargets(t *testing.T) {
	reader := newMonitorMetricReader(t)

	store := &fakeActiveApplyStore{counts: []storage.ActiveApplyCount{
		{Database: "db1", Deployment: "pie", Environment: "staging", Count: 1},
		{Database: "db2", Deployment: "pie", Environment: "staging", Count: 3},
	}}
	svc := newMonitorTestService(t, store)
	svc.CollectActiveAppliesMetrics(t.Context())

	store.counts = []storage.ActiveApplyCount{
		{Database: "db2", Deployment: "pie", Environment: "staging", Count: 2},
	}
	svc.CollectActiveAppliesMetrics(t.Context())

	assert.Equal(t, map[activeAppliesTarget]int64{
		{database: "db1", deployment: "pie", environment: "staging"}: 0,
		{database: "db2", deployment: "pie", environment: "staging"}: 2,
	}, activeAppliesGaugeValues(t, reader))
}

// A scan failure must not touch the gauge: the last-good values keep real
// in-flight applies visible through a transient storage blip. Instead it
// increments the scan-failure counter — the liveness signal that the gauge is
// stale — and a later successful scan must still zero targets that departed
// while scans were failing.
func TestCollectActiveAppliesMetricsCountsFailureOnStoreError(t *testing.T) {
	reader := newMonitorMetricReader(t)

	store := &fakeActiveApplyStore{counts: []storage.ActiveApplyCount{
		{Database: "db1", Deployment: "pie", Environment: "staging", Count: 1},
	}}
	svc := newMonitorTestService(t, store)
	svc.CollectActiveAppliesMetrics(t.Context())

	store.err = errors.New("db down")
	svc.CollectActiveAppliesMetrics(t.Context())

	assert.Contains(t, metricNames(t, reader), "schemabot.applies.active_scan_failures",
		"a failed scan must increment the failure counter")
	assert.Equal(t, map[activeAppliesTarget]int64{
		{database: "db1", deployment: "pie", environment: "staging"}: 1,
	}, activeAppliesGaugeValues(t, reader), "a failed scan must leave the last-good values untouched")

	store.err = nil
	store.counts = nil
	svc.CollectActiveAppliesMetrics(t.Context())

	assert.Equal(t, map[activeAppliesTarget]int64{
		{database: "db1", deployment: "pie", environment: "staging"}: 0,
	}, activeAppliesGaugeValues(t, reader), "a successful scan after failures must zero the departed target")
}

// A shutdown that lands mid-scan cancels the monitor context and fails the
// query with a cancellation error. That is orderly teardown, not a stale
// gauge: a routine deploy must not tick the scan-failure counter (the "gauge
// must not be trusted" liveness signal).
func TestCollectActiveAppliesMetricsIgnoresShutdownMidScan(t *testing.T) {
	reader := newMonitorMetricReader(t)

	ctx, cancel := context.WithCancel(t.Context())
	store := &fakeActiveApplyStore{err: context.Canceled, onScan: cancel}
	svc := newMonitorTestService(t, store)

	svc.CollectActiveAppliesMetrics(ctx)

	names := metricNames(t, reader)
	assert.NotContains(t, names, "schemabot.applies.active", "the gauge must not be emitted on error")
	assert.NotContains(t, names, "schemabot.applies.active_scan_failures",
		"a scan aborted by shutdown must not count as a scan failure")
	assert.Equal(t, 1, store.calls)
}

// With no active applies anywhere and none previously reported, there is no
// target to record: the gauge is simply absent rather than emitted with
// made-up attribute sets.
func TestCollectActiveAppliesMetricsEmptyPopulationEmitsNothing(t *testing.T) {
	reader := newMonitorMetricReader(t)

	svc := newMonitorTestService(t, &fakeActiveApplyStore{counts: nil})

	svc.CollectActiveAppliesMetrics(t.Context())

	assert.NotContains(t, metricNames(t, reader), "schemabot.applies.active")
}

// The monitor is a no-op when apply storage is unavailable: there is nothing
// to scan, so it must neither emit the gauge nor panic.
func TestCollectActiveAppliesMetricsNoOpWhenStorageUnavailable(t *testing.T) {
	reader := newMonitorMetricReader(t)

	svc := newMonitorTestService(t, nil)

	svc.CollectActiveAppliesMetrics(t.Context())

	assert.NotContains(t, metricNames(t, reader), "schemabot.applies.active")
}
