package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/block/schemabot/pkg/clock"
	"github.com/block/schemabot/pkg/storage"
)

// A promotion pipeline holds on schemabot_environment_frozen, so every known
// environment reports 0 or 1 on every pass: production reads 1 while a freeze
// narrowed to one database is in effect, staging reads 0, and production
// returns to 0 once the freeze is lifted.
func TestCollectFreezeMetricsReportsEachEnvironment(t *testing.T) {
	reader := newFreezeMetricsReader(t)
	freezes := &memoryFreezeStore{}
	config := freezeMetricsConfig([]string{"staging", "production"}, "staging", "production")
	svc := New(&mockStorageWithFreezes{freezes: freezes}, config, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, svc.SetClock(clock.NewFake(freezeTestNow)))

	freeze, err := freezes.Create(t.Context(), &storage.EnvironmentFreeze{
		Identifier: "freeze-0123456789abcdef", Environment: "production", Databases: []string{"orders"},
		Kind: storage.FreezeKindOnDemand, Override: storage.FreezeOverrideAdmins,
		StartsAt: freezeTestNow.Add(-time.Minute), Reason: "incident", CreatedBy: "alice", Source: storage.FreezeSourceManual,
	})
	require.NoError(t, err)

	svc.CollectFreezeMetrics(t.Context())
	assert.Equal(t, map[string]int64{"production": 1, "staging": 0}, frozenGaugeValues(t, reader))

	_, err = freezes.Lift(t.Context(), freeze.Identifier, "bob", "resolved", freezeTestNow)
	require.NoError(t, err)
	svc.CollectFreezeMetrics(t.Context())
	assert.Equal(t, map[string]int64{"production": 0, "staging": 0}, frozenGaugeValues(t, reader))
}

// A failed snapshot leaves the gauge untouched and counts the failure, so an
// operator can tell the frozen values are stale.
func TestCollectFreezeMetricsCountsFailure(t *testing.T) {
	reader := newFreezeMetricsReader(t)
	svc := New(&mockStorageWithFreezes{freezes: &failingFreezeStore{}}, freezeMetricsConfig([]string{"production"}, "production"), nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	svc.CollectFreezeMetrics(t.Context())

	assert.Empty(t, frozenGaugeValues(t, reader))
	failures, ok := freezeCollectionFailures(t, reader)
	require.True(t, ok)
	assert.Equal(t, int64(1), failures)
}

// A staging-only instance names production in its environment order, but
// production's freezes live in another store. Reporting production as 0 would
// read as "not frozen" while it may be, so the instance reports nothing for it.
func TestCollectFreezeMetricsSkipsPeerEnvironments(t *testing.T) {
	reader := newFreezeMetricsReader(t)
	config := freezeMetricsConfig([]string{"staging"}, "staging", "production")
	config.EnvironmentOrder = []string{"staging", "production"}
	svc := New(&mockStorageWithFreezes{freezes: &memoryFreezeStore{}}, config, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, svc.SetClock(clock.NewFake(freezeTestNow)))

	svc.CollectFreezeMetrics(t.Context())

	assert.Equal(t, map[string]int64{"staging": 0}, frozenGaugeValues(t, reader))
}

// Stopping the monitor while a query is in flight is an orderly shutdown and
// must not tick the failure counter that alerts on a stale gauge.
func TestCollectFreezeMetricsIgnoresCancellation(t *testing.T) {
	reader := newFreezeMetricsReader(t)
	ctx, cancel := context.WithCancel(t.Context())
	store := &cancellingFreezeStore{cancel: cancel}
	svc := New(&mockStorageWithFreezes{freezes: store}, freezeMetricsConfig([]string{"production"}, "production"), nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	svc.CollectFreezeMetrics(ctx)

	require.True(t, store.called)
	assert.Empty(t, frozenGaugeValues(t, reader))
	_, ok := freezeCollectionFailures(t, reader)
	assert.False(t, ok, "cancellation must not record a collection failure")
}

// The monitor exports the failure counter at 0 when it starts, so the first
// failure is an increase an alert can see.
func TestStartFreezeMonitorExportsFailureBaseline(t *testing.T) {
	reader := newFreezeMetricsReader(t)
	svc := New(&mockStorageWithFreezes{freezes: &memoryFreezeStore{}}, freezeMetricsConfig([]string{"production"}, "production"), nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	svc.StartFreezeMonitor(t.Context())
	t.Cleanup(svc.StopFreezeMonitor)

	failures, ok := freezeCollectionFailures(t, reader)
	require.True(t, ok, "failure counter must be exported before any failure")
	assert.Equal(t, int64(0), failures)
}

// freezeMetricsConfig allows the given environments and routes one database to
// each routed environment, the two conditions for serving an environment.
func freezeMetricsConfig(allowed []string, routed ...string) *ServerConfig {
	environments := make(map[string]EnvironmentConfig, len(routed))
	for _, environment := range routed {
		environments[environment] = EnvironmentConfig{DSN: "root@tcp(localhost)/orders_" + environment}
	}
	return &ServerConfig{
		AllowedEnvironments: allowed,
		Databases:           map[string]DatabaseConfig{"orders": {Type: "mysql", Environments: environments}},
	}
}

func freezeCollectionFailures(t *testing.T, reader *sdkmetric.ManualReader) (int64, bool) {
	t.Helper()
	var failures int64
	found := false
	for _, m := range collectFreezeMetrics(t, reader) {
		if m.Name != "schemabot.environment.frozen_collection_failures" {
			continue
		}
		sum, ok := m.Data.(metricdata.Sum[int64])
		require.True(t, ok)
		for _, dp := range sum.DataPoints {
			found = true
			failures += dp.Value
		}
	}
	return failures, found
}

type failingFreezeStore struct{ memoryFreezeStore }

func (*failingFreezeStore) ListInEffect(_ context.Context, _ string, _ time.Time) ([]*storage.EnvironmentFreeze, error) {
	return nil, errors.New("database unavailable")
}

// cancellingFreezeStore cancels the caller's context mid-query, as a monitor
// stop would, and fails the query with the resulting cancellation error.
type cancellingFreezeStore struct {
	memoryFreezeStore
	cancel context.CancelFunc
	called bool
}

func (s *cancellingFreezeStore) ListInEffect(ctx context.Context, _ string, _ time.Time) ([]*storage.EnvironmentFreeze, error) {
	s.called = true
	s.cancel()
	<-ctx.Done()
	return nil, ctx.Err()
}

func newFreezeMetricsReader(t *testing.T) *sdkmetric.ManualReader {
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

func collectFreezeMetrics(t *testing.T, reader *sdkmetric.ManualReader) []metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	var all []metricdata.Metrics
	for _, sm := range rm.ScopeMetrics {
		all = append(all, sm.Metrics...)
	}
	return all
}

func frozenGaugeValues(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	values := map[string]int64{}
	for _, m := range collectFreezeMetrics(t, reader) {
		if m.Name != "schemabot.environment.frozen" {
			continue
		}
		gauge, ok := m.Data.(metricdata.Gauge[int64])
		require.True(t, ok)
		for _, dp := range gauge.DataPoints {
			environment, ok := dp.Attributes.Value(attribute.Key("environment"))
			require.True(t, ok)
			values[environment.AsString()] = dp.Value
		}
	}
	return values
}
