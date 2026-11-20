package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/block/schemabot/pkg/clock"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

// A promotion pipeline holds on schemabot_environment_frozen, so every served
// environment reports each scope on every pass: production reads partial=1
// while a freeze narrowed to one database is in effect, environment=1 once a
// whole-environment freeze joins it, and both return to 0 after the lifts.
// Staging reads 0 throughout.
func TestCollectFreezeMetricsReportsEachEnvironment(t *testing.T) {
	reader := newFreezeMetricsReader(t)
	freezes := &memoryFreezeStore{clock: clock.NewFake(freezeTestNow)}
	config := freezeMetricsConfig([]string{"staging", "production"}, "staging", "production")
	svc := New(&mockStorageWithFreezes{freezes: freezes}, config, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	narrow := createMetricsFreeze(t, freezes, "freeze-0123456789abcdef", []string{"orders"})
	svc.CollectFreezeMetrics(t.Context())
	assert.Equal(t, map[string]int64{
		"production/environment": 0, "production/partial": 1,
		"staging/environment": 0, "staging/partial": 0,
	}, frozenGaugeValues(t, reader))

	whole := createMetricsFreeze(t, freezes, "freeze-fedcba9876543210", nil)
	svc.CollectFreezeMetrics(t.Context())
	assert.Equal(t, int64(1), frozenGaugeValues(t, reader)["production/environment"])
	assert.Equal(t, int64(1), frozenGaugeValues(t, reader)["production/partial"])

	for _, freeze := range []*storage.EnvironmentFreeze{narrow, whole} {
		_, err := freezes.Lift(t.Context(), freeze.Identifier, "bob", "resolved")
		require.NoError(t, err)
	}
	svc.CollectFreezeMetrics(t.Context())
	assert.Equal(t, map[string]int64{
		"production/environment": 0, "production/partial": 0,
		"staging/environment": 0, "staging/partial": 0,
	}, frozenGaugeValues(t, reader))
}

// schemabot_environment_freeze_open_seconds is the age, by the database clock,
// of the oldest open-ended freeze in effect. A planned freeze has an end and is
// not counted, so a long calendar freeze does not trip the forgotten-freeze
// alert. The age returns to 0 when the freeze is lifted.
func TestCollectFreezeMetricsReportsOpenAge(t *testing.T) {
	reader := newFreezeMetricsReader(t)
	databaseClock := clock.NewFake(freezeTestNow)
	freezes := &memoryFreezeStore{clock: databaseClock}
	svc := New(&mockStorageWithFreezes{freezes: freezes}, freezeMetricsConfig([]string{"production"}, "production"), nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	end := freezeTestNow.Add(7 * 24 * time.Hour)
	_, err := freezes.Create(t.Context(), &storage.EnvironmentFreeze{
		Identifier: "freeze-1111111111111111", Environment: "production",
		Kind: storage.FreezeKindPlanned, Running: storage.FreezeRunningContinue,
		StartsAt: freezeTestNow.Add(-48 * time.Hour), EndsAt: &end,
		Reason: "holiday", CreatedBy: "alice", Source: storage.FreezeSourceManual,
	})
	require.NoError(t, err)
	open := createMetricsFreeze(t, freezes, "freeze-2222222222222222", nil)
	databaseClock.Advance(90 * time.Minute)

	svc.CollectFreezeMetrics(t.Context())
	assert.Equal(t, map[string]int64{"production": int64((90*time.Minute + time.Minute).Seconds())}, freezeOpenSeconds(t, reader))

	_, err = freezes.Lift(t.Context(), open.Identifier, "bob", "resolved")
	require.NoError(t, err)
	svc.CollectFreezeMetrics(t.Context())
	assert.Equal(t, map[string]int64{"production": 0}, freezeOpenSeconds(t, reader))
}

// A failed snapshot leaves the gauge untouched and counts the failure, so an
// operator can tell the frozen values are stale.
func TestCollectFreezeMetricsCountsFailure(t *testing.T) {
	reader := newFreezeMetricsReader(t)
	svc := New(&mockStorageWithFreezes{freezes: &failingFreezeStore{memoryFreezeStore{clock: clock.NewFake(freezeTestNow)}}}, freezeMetricsConfig([]string{"production"}, "production"), nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	svc.CollectFreezeMetrics(t.Context())

	assert.Empty(t, frozenGaugeValues(t, reader))
	failures, ok := freezeCollectionFailures(t, reader)
	require.True(t, ok)
	assert.Equal(t, int64(1), failures)
}

// A staging-only instance names production in its environment order, but
// production's freezes live in another store. Reporting production as 0 would
// read as "not frozen" while it may be, so the instance reports nothing for it,
// even for a production freeze left in its own store from before production
// moved to the peer.
func TestCollectFreezeMetricsSkipsPeerEnvironments(t *testing.T) {
	reader := newFreezeMetricsReader(t)
	config := freezeMetricsConfig([]string{"staging"}, "staging", "production")
	config.EnvironmentOrder = []string{"staging", "production"}
	store := &memoryFreezeStore{clock: clock.NewFake(freezeTestNow)}
	createMetricsFreeze(t, store, "freeze-left-behind", nil)
	svc := New(&mockStorageWithFreezes{freezes: store}, config, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	svc.CollectFreezeMetrics(t.Context())

	assert.Equal(t, map[string]int64{"staging/environment": 0, "staging/partial": 0}, frozenGaugeValues(t, reader))
	assert.Equal(t, map[string]int64{"staging": 0}, freezeOpenSeconds(t, reader))
}

// Stopping the monitor while a query is in flight is an orderly shutdown and
// must not tick the failure counter that alerts on a stale gauge.
func TestCollectFreezeMetricsIgnoresCancellation(t *testing.T) {
	reader := newFreezeMetricsReader(t)
	ctx, cancel := context.WithCancel(t.Context())
	store := &cancellingFreezeStore{memoryFreezeStore: memoryFreezeStore{clock: clock.NewFake(freezeTestNow)}, cancel: cancel}
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
	svc := New(&mockStorageWithFreezes{freezes: &memoryFreezeStore{clock: clock.NewFake(freezeTestNow)}}, freezeMetricsConfig([]string{"production"}, "production"), nil,
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

func (*failingFreezeStore) ListInEffect(context.Context, string) ([]*storage.EnvironmentFreeze, error) {
	return nil, errors.New("database unavailable")
}

// cancellingFreezeStore cancels the caller's context mid-query, as a monitor
// stop would, and fails the query with the resulting cancellation error.
type cancellingFreezeStore struct {
	memoryFreezeStore
	cancel context.CancelFunc
	called bool
}

func (s *cancellingFreezeStore) ListInEffect(ctx context.Context, _ string) ([]*storage.EnvironmentFreeze, error) {
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
	// The freeze gauges read a process-wide snapshot; start each test from
	// none, as a fresh process does.
	metrics.PublishEnvironmentFreezes(nil)
	require.NoError(t, metrics.RegisterEnvironmentFreezeMetrics(mp))
	t.Cleanup(func() {
		metrics.PublishEnvironmentFreezes(nil)
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

// frozenGaugeValues keys schemabot.environment.frozen by "environment/scope".
func frozenGaugeValues(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	return gaugeValues(t, reader, "schemabot.environment.frozen", func(dp metricdata.DataPoint[int64]) string {
		environment, ok := dp.Attributes.Value(attribute.Key("environment"))
		require.True(t, ok)
		scope, ok := dp.Attributes.Value(attribute.Key("scope"))
		require.True(t, ok)
		return environment.AsString() + "/" + scope.AsString()
	})
}

// freezeOpenSeconds keys schemabot.environment.freeze_open_seconds by
// environment.
func freezeOpenSeconds(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	return gaugeValues(t, reader, "schemabot.environment.freeze_open_seconds", func(dp metricdata.DataPoint[int64]) string {
		environment, ok := dp.Attributes.Value(attribute.Key("environment"))
		require.True(t, ok)
		return environment.AsString()
	})
}

func gaugeValues(t *testing.T, reader *sdkmetric.ManualReader, name string, key func(metricdata.DataPoint[int64]) string) map[string]int64 {
	t.Helper()
	values := map[string]int64{}
	for _, m := range collectFreezeMetrics(t, reader) {
		if m.Name != name {
			continue
		}
		gauge, ok := m.Data.(metricdata.Gauge[int64])
		require.True(t, ok)
		for _, dp := range gauge.DataPoints {
			values[key(dp)] = dp.Value
		}
	}
	return values
}

// createMetricsFreeze stores an open-ended production freeze, narrowed to the
// given databases, that started a minute before freezeTestNow.
func createMetricsFreeze(t *testing.T, freezes storage.FreezeStore, identifier string, databases []string) *storage.EnvironmentFreeze {
	t.Helper()
	freeze, err := freezes.Create(t.Context(), &storage.EnvironmentFreeze{
		Identifier: identifier, Environment: "production", Databases: databases,
		Kind: storage.FreezeKindOnDemand, Running: storage.FreezeRunningContinue,
		StartsAt: freezeTestNow.Add(-time.Minute), Reason: "incident", CreatedBy: "alice", Source: storage.FreezeSourceManual,
	})
	require.NoError(t, err)
	return freeze
}

// The monitor loop itself keeps the gauge current: a planned freeze reads 0
// before its start, 1 once the database clock passes it, and 0 again after its
// end, with no API call in between.
func TestFreezeMonitorFollowsAPlannedFreeze(t *testing.T) {
	reader := newFreezeMetricsReader(t)
	databaseClock := clock.NewFake(freezeTestNow)
	freezes := &memoryFreezeStore{clock: databaseClock}
	start := freezeTestNow.Add(time.Hour)
	end := start.Add(time.Hour)
	_, err := freezes.Create(t.Context(), &storage.EnvironmentFreeze{
		Identifier: "freeze-0123456789abcdef", Environment: "production",
		Kind: storage.FreezeKindPlanned, Running: storage.FreezeRunningContinue,
		StartsAt: start, EndsAt: &end, Reason: "holiday", CreatedBy: "alice", Source: storage.FreezeSourceManual,
	})
	require.NoError(t, err)
	svc := New(&mockStorageWithFreezes{freezes: freezes}, freezeMetricsConfig([]string{"production"}, "production"), nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	svc.freezeMetricsInterval = 10 * time.Millisecond
	svc.StartFreezeMonitor(t.Context())
	t.Cleanup(svc.StopFreezeMonitor)

	frozen := func(want int64) func() bool {
		return func() bool {
			value, ok := frozenGaugeValues(t, reader)["production/environment"]
			return ok && value == want
		}
	}
	require.Eventually(t, frozen(0), 5*time.Second, 10*time.Millisecond, "not frozen before the start")
	databaseClock.Advance(90 * time.Minute)
	require.Eventually(t, frozen(1), 5*time.Second, 10*time.Millisecond, "frozen once the start passes")
	databaseClock.Advance(time.Hour)
	require.Eventually(t, frozen(0), 5*time.Second, 10*time.Millisecond, "not frozen after the end")
}

// A scrape reads one snapshot. While a whole-environment freeze and a partial
// one keep replacing each other, production is frozen throughout, so no
// scrape may read both scopes at 0.
func TestFreezeGaugesNeverReadBothScopesClearDuringATransition(t *testing.T) {
	reader := newFreezeMetricsReader(t)
	store := &alternatingFreezeStore{memoryFreezeStore: memoryFreezeStore{clock: clock.NewFake(freezeTestNow)}}
	svc := New(&mockStorageWithFreezes{freezes: store}, freezeMetricsConfig([]string{"production"}, "production"), nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	svc.CollectFreezeMetrics(t.Context())

	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Go(func() {
		for !stop.Load() {
			svc.CollectFreezeMetrics(t.Context())
		}
	})
	for range 500 {
		values := frozenGaugeValues(t, reader)
		if values["production/environment"]+values["production/partial"] == 0 {
			stop.Store(true)
			wg.Wait()
			require.FailNow(t, "a scrape read production as not frozen while a freeze held throughout", "%v", values)
		}
	}
	stop.Store(true)
	wg.Wait()
	assert.Positive(t, store.calls.Load(), "the collections ran")
}

// alternatingFreezeStore answers each ListInEffect with a whole-environment
// production freeze or a partial one, in turn.
type alternatingFreezeStore struct {
	memoryFreezeStore
	calls atomic.Int64
}

func (s *alternatingFreezeStore) ListInEffect(context.Context, string) ([]*storage.EnvironmentFreeze, error) {
	freeze := &storage.EnvironmentFreeze{
		Identifier: "freeze-0123456789abcdef", Environment: "production",
		Kind: storage.FreezeKindOnDemand, Running: storage.FreezeRunningContinue,
		StartsAt: freezeTestNow.Add(-time.Minute), Reason: "incident", CreatedBy: "alice", Source: storage.FreezeSourceManual,
	}
	if s.calls.Add(1)%2 == 0 {
		freeze.Databases = []string{"orders"}
	}
	return []*storage.EnvironmentFreeze{freeze}, nil
}
