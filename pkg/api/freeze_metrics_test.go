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
	config := &ServerConfig{AllowedEnvironments: []string{"staging", "production"}}
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
	svc := New(&mockStorageWithFreezes{freezes: &failingFreezeStore{}}, &ServerConfig{AllowedEnvironments: []string{"production"}}, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	svc.CollectFreezeMetrics(t.Context())

	assert.Empty(t, frozenGaugeValues(t, reader))
	var failures int64
	for _, m := range collectFreezeMetrics(t, reader) {
		if m.Name == "schemabot.environment.frozen_collection_failures" {
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				failures += dp.Value
			}
		}
	}
	assert.Equal(t, int64(1), failures)
}

type failingFreezeStore struct{ memoryFreezeStore }

func (*failingFreezeStore) ListInEffect(_ context.Context, _ string, _ time.Time) ([]*storage.EnvironmentFreeze, error) {
	return nil, errors.New("database unavailable")
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
