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
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/block/schemabot/pkg/storage"
)

// fakeWebhookEventStore serves a fixed InboxStats snapshot; every other method
// is unused by the inbox metrics monitor.
type fakeWebhookEventStore struct {
	stats *storage.WebhookInboxStats
	err   error
}

func (f *fakeWebhookEventStore) InboxStats(context.Context) (*storage.WebhookInboxStats, error) {
	return f.stats, f.err
}

func (f *fakeWebhookEventStore) Create(context.Context, *storage.WebhookEvent) (bool, error) {
	return false, errors.New("unused")
}

func (f *fakeWebhookEventStore) GetByDeliveryID(context.Context, string, string) (*storage.WebhookEvent, error) {
	return nil, errors.New("unused")
}

func (f *fakeWebhookEventStore) FindNext(context.Context, string, time.Duration) (*storage.WebhookEvent, error) {
	return nil, errors.New("unused")
}

func (f *fakeWebhookEventStore) Heartbeat(context.Context, int64, string, time.Duration) error {
	return errors.New("unused")
}

func (f *fakeWebhookEventStore) MarkCompleted(context.Context, int64, string) error {
	return errors.New("unused")
}

func (f *fakeWebhookEventStore) MarkFailed(context.Context, int64, string, string, *time.Time) error {
	return errors.New("unused")
}

func (f *fakeWebhookEventStore) Release(context.Context, int64, string) error {
	return errors.New("unused")
}

func newInboxMetricsTestService(t *testing.T, store storage.WebhookEventStore) *Service {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorage{webhookEvents: store}, &ServerConfig{}, nil, logger)
}

// The webhook inbox monitor emits one depth gauge per canonical state (even when
// a state is empty), a backlog-age gauge, and a stuck-processing gauge, so an
// operator can see inbox pressure without any schema change running.
func TestCollectWebhookInboxMetricsRecordsGauges(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(t.Context()))
	})

	store := &fakeWebhookEventStore{stats: &storage.WebhookInboxStats{
		CountsByState: map[string]int64{
			storage.WebhookEventPending:         3,
			storage.WebhookEventProcessing:      1,
			storage.WebhookEventFailedRetryable: 2,
			storage.WebhookEventCompleted:       10,
			storage.WebhookEventFailed:          4,
		},
		OldestClaimableAge: 90 * time.Second,
		StuckProcessing:    2,
	}}
	svc := newInboxMetricsTestService(t, store)

	svc.CollectWebhookInboxMetrics(t.Context())

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	depthByState := map[string]int64{}
	var oldestAge, stuck int64
	var oldestFound, stuckFound bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "schemabot.webhook.inbox_depth":
				gauge, ok := m.Data.(metricdata.Gauge[int64])
				require.True(t, ok)
				for _, dp := range gauge.DataPoints {
					state, ok := dp.Attributes.Value(attribute.Key("state"))
					require.True(t, ok)
					depthByState[state.AsString()] = dp.Value
				}
			case "schemabot.webhook.inbox_oldest_claimable_age_seconds":
				gauge, ok := m.Data.(metricdata.Gauge[int64])
				require.True(t, ok)
				require.Len(t, gauge.DataPoints, 1)
				oldestAge = gauge.DataPoints[0].Value
				oldestFound = true
			case "schemabot.webhook.inbox_stuck_processing":
				gauge, ok := m.Data.(metricdata.Gauge[int64])
				require.True(t, ok)
				require.Len(t, gauge.DataPoints, 1)
				stuck = gauge.DataPoints[0].Value
				stuckFound = true
			}
		}
	}

	assert.Equal(t, map[string]int64{
		"pending":          3,
		"processing":       1,
		"failed_retryable": 2,
		"completed":        10,
		"failed":           4,
		"unknown":          0,
	}, depthByState)
	assert.True(t, oldestFound)
	assert.Equal(t, int64(90), oldestAge)
	assert.True(t, stuckFound)
	assert.Equal(t, int64(2), stuck)
}

// A non-canonical state row folds into the unknown depth series, and once it is
// cleaned up the series must return to 0. inbox_depth is a last-value gauge, so
// the collector records unknown on every pass (including 0); otherwise the
// gauge would freeze at its last nonzero value and show a phantom unknown
// population that never resolves.
func TestCollectWebhookInboxMetricsUnknownReturnsToZeroAfterCleanup(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(t.Context()))
	})

	store := &fakeWebhookEventStore{stats: &storage.WebhookInboxStats{
		CountsByState: map[string]int64{
			storage.WebhookEventPending: 1,
			"bogus":                     5,
		},
	}}
	svc := newInboxMetricsTestService(t, store)

	svc.CollectWebhookInboxMetrics(t.Context())
	require.Equal(t, int64(5), inboxDepthForState(t, reader, "unknown"),
		"a non-canonical state row should fold into the unknown series")

	// The bogus row is cleaned up; the next pass must drive unknown back to 0.
	store.stats = &storage.WebhookInboxStats{
		CountsByState: map[string]int64{storage.WebhookEventPending: 1},
	}
	svc.CollectWebhookInboxMetrics(t.Context())
	require.Equal(t, int64(0), inboxDepthForState(t, reader, "unknown"),
		"unknown must return to 0 once the non-canonical row is gone")
}

// inboxDepthForState collects the current metrics and returns the inbox_depth
// gauge value for the given state, or -1 if no data point for that state exists.
func inboxDepthForState(t *testing.T, reader sdkmetric.Reader, state string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "schemabot.webhook.inbox_depth" {
				continue
			}
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok)
			for _, dp := range gauge.DataPoints {
				v, ok := dp.Attributes.Value(attribute.Key("state"))
				require.True(t, ok)
				if v.AsString() == state {
					return dp.Value
				}
			}
		}
	}
	return -1
}

// A store failure must not emit the depth/backlog gauges: they are last-value
// instruments, so leaving them untouched re-exports the last-good values and
// reads as a healthy inbox. Instead it increments the collection-failure
// counter — the liveness signal that the gauges are stale — and must not panic
// the monitor loop.
func TestCollectWebhookInboxMetricsSkipsGaugesAndCountsFailureOnStoreError(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(t.Context()))
	})

	svc := newInboxMetricsTestService(t, &fakeWebhookEventStore{err: errors.New("db down")})

	svc.CollectWebhookInboxMetrics(t.Context())

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	names := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names[m.Name] = true
		}
	}
	assert.NotContains(t, names, "schemabot.webhook.inbox_depth", "gauges must not be re-emitted on error")
	assert.NotContains(t, names, "schemabot.webhook.inbox_oldest_claimable_age_seconds")
	assert.NotContains(t, names, "schemabot.webhook.inbox_stuck_processing")
	assert.Contains(t, names, "schemabot.webhook.inbox_stats_collection_failures", "a failed snapshot must increment the failure counter")
}
