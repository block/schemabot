package notifications

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

type memoryStore struct {
	storage.NotificationStore
	mu           sync.Mutex
	applies      []*storage.Apply
	token        string
	published    bool
	failure      string
	pageErr      error
	observations []storage.NotificationObservation
	at           time.Time
	scans        chan struct{}
	afterPage    func()
	pageCalls    int
}

func (s *memoryStore) FindUnpublished(_ context.Context, _ time.Duration, limit int) ([]*storage.Apply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.scans != nil {
		select {
		case s.scans <- struct{}{}:
		default:
		}
	}
	if s.published || s.token != "" {
		return nil, nil
	}
	return s.applies[:min(limit, len(s.applies))], nil
}
func (s *memoryStore) Claim(_ context.Context, _ int64, token string, _ time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token != "" || s.published {
		return false, nil
	}
	s.token = token
	return true, nil
}
func (s *memoryStore) MarkPublished(_ context.Context, _ int64, token string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if token != s.token {
		return false, nil
	}
	s.published = true
	s.token = ""
	return true, nil
}
func (s *memoryStore) Release(_ context.Context, _ int64, token, message string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if token != s.token {
		return false, nil
	}
	s.token = ""
	s.failure = message
	return true, nil
}
func (s *memoryStore) ObservationCeiling(context.Context) (int64, time.Time, error) {
	return int64(len(s.observations)), s.at, nil
}
func (s *memoryStore) ObservePage(_ context.Context, after, through int64, limit int) ([]storage.NotificationObservation, error) {
	s.pageCalls++
	if s.pageErr != nil {
		return nil, s.pageErr
	}
	page := s.observations[after:min(through, after+int64(limit))]
	if s.afterPage != nil {
		s.afterPage()
	}
	return page, nil
}

type publisherFunc func(context.Context, Event) error

func (f publisherFunc) Publish(ctx context.Context, event Event) error { return f(ctx, event) }

func TestRestartRetriesFailedPublicationWithStableIdentifier(t *testing.T) {
	now := time.Now().Add(-time.Hour)
	store := &memoryStore{applies: []*storage.Apply{{ID: 1, ApplyIdentifier: "apply-stable", Database: "orders", DatabaseType: "mysql", Environment: "staging", CompletedAt: &now}}}
	first, err := New(store, publisherFunc(func(ctx context.Context, event Event) error {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		assert.LessOrEqual(t, time.Until(deadline), SendTimeout)
		assert.Equal(t, "apply-stable", event.ApplyIdentifier)
		return errors.New("queue unavailable")
	}), Config{}, slog.Default())
	require.NoError(t, err)
	require.ErrorContains(t, first.Pass(t.Context()), "queue unavailable")
	assert.Empty(t, store.token)
	assert.Equal(t, "queue unavailable", store.failure)
	var events []Event
	restarted, err := New(store, publisherFunc(func(_ context.Context, event Event) error { events = append(events, event); return nil }), Config{}, slog.Default())
	require.NoError(t, err)
	require.NoError(t, restarted.Pass(t.Context()))
	require.NoError(t, restarted.Pass(t.Context()))
	require.Len(t, events, 1)
	assert.Equal(t, "apply-stable", events[0].ApplyIdentifier)
	assert.Empty(t, events[0].Repository)
	assert.Zero(t, events[0].PullRequest)
	assert.True(t, store.published)
	assert.Less(t, SendTimeout+2*QueryTimeout, ClaimTTL)
}

func TestObservationPagesAllHistoryAndPublishesOnlyFinishedSweeps(t *testing.T) {
	reader := backlogReader(t)
	metrics.PublishApplyNotificationBacklog(&metrics.ApplyNotificationBacklog{OutOfWindow: 9})
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour)
	recent := now.Add(-time.Hour)
	store := &memoryStore{at: now}
	for i := range ObservationPageSize + 1 {
		store.observations = append(store.observations, storage.NotificationObservation{ID: int64(i + 1), Completed: true, CompletedAt: &recent, Published: true})
	}
	store.observations[0] = storage.NotificationObservation{ID: 1, Completed: true, CompletedAt: &old}
	store.observations[1] = storage.NotificationObservation{ID: 2, Completed: true, CompletedAt: &recent}
	runner, err := New(store, publisherFunc(func(context.Context, Event) error { return nil }), Config{}, slog.Default())
	require.NoError(t, err)
	require.NoError(t, runner.observePage(t.Context()))
	require.NotNil(t, runner.sweep)
	assert.Equal(t, int64(ObservationPageSize), runner.sweep.after)
	assert.Equal(t, int64(1), runner.sweep.outside)
	assert.Equal(t, int64(9), observedOutside(t, reader))
	// The page cursor stays on the last successful page if storage is unavailable.
	store.pageErr = errors.New("database unavailable")
	require.Error(t, runner.observePage(t.Context()))
	assert.Equal(t, int64(ObservationPageSize), runner.sweep.after)
	assert.Equal(t, int64(9), observedOutside(t, reader))
	// Appended rows do not extend the current sweep ceiling.
	store.observations = append(store.observations, storage.NotificationObservation{ID: ObservationPageSize + 2, Completed: true, CompletedAt: &old})
	store.pageErr = nil
	require.NoError(t, runner.observePage(t.Context()))
	assert.Nil(t, runner.sweep)
	assert.Equal(t, int64(1), observedOutside(t, reader))
	// A publication since the previous sweep is observed on the next one.
	store.observations[0].Published = true
	require.NoError(t, runner.observePage(t.Context()))
	require.NotNil(t, runner.sweep)
	assert.Zero(t, runner.sweep.outside)
	assert.Equal(t, recent, *runner.sweep.oldest)
}

func TestRunStartsImmediatelyAndStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	store := &memoryStore{scans: make(chan struct{}, 1), at: time.Now()}
	runner, err := New(store, publisherFunc(func(context.Context, Event) error { return nil }), Config{}, slog.Default())
	require.NoError(t, err)
	done := make(chan struct{})
	go func() { defer close(done); runner.Run(ctx) }()
	select {
	case <-store.scans:
	case <-time.After(time.Second):
		require.FailNow(t, "startup scan did not run")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		require.FailNow(t, "publisher did not stop")
	}
}

func TestConfigSelectionAndValidation(t *testing.T) {
	publisher, err := NewPublisher(t.Context(), Config{})
	require.NoError(t, err)
	assert.Nil(t, publisher)
	for _, cfg := range []Config{{Type: "kafka"}, {Type: "sqs"}, {Type: "sqs", QueueURL: "queue", Lookback: "-1h"}, {Type: "sqs", QueueURL: "queue", Interval: "bad"}, {Type: "sqs", QueueURL: "queue", BatchSize: -1}, {Type: "sqs", QueueURL: "queue", BatchSize: 1001}} {
		require.Error(t, cfg.Validate())
	}
	lookback, interval, batch, err := (Config{}).settings()
	require.NoError(t, err)
	assert.Equal(t, 24*time.Hour, lookback)
	assert.Equal(t, DefaultInterval, interval)
	assert.Equal(t, DefaultBatchSize, batch)
	require.NoError(t, (Config{Type: "sqs", QueueURL: "queue", Lookback: "48h", Interval: "1m", BatchSize: 10}).Validate())
}

func backlogReader(t *testing.T) *metric.ManualReader {
	t.Helper()
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	require.NoError(t, metrics.RegisterApplyNotificationMetrics(provider))
	t.Cleanup(func() {
		metrics.PublishApplyNotificationBacklog(nil)
		require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context())))
	})
	return reader
}

func observedOutside(t *testing.T, reader *metric.ManualReader) int64 {
	t.Helper()
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &data))
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == "schemabot.apply_notifications.out_of_window" {
				gauge, ok := m.Data.(metricdata.Gauge[int64])
				require.True(t, ok)
				require.Len(t, gauge.DataPoints, 1)
				return gauge.DataPoints[0].Value
			}
		}
	}
	require.FailNow(t, "out-of-window gauge was not emitted")
	return 0
}

func TestObservationEmitsMultiplePagesBeforeInterSweepPause(t *testing.T) {
	reader := backlogReader(t)
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour)
	store := &memoryStore{at: now}
	for i := range ObservationPageSize + 1 {
		store.observations = append(store.observations, storage.NotificationObservation{ID: int64(i + 1), Completed: true, CompletedAt: &old})
	}
	runner, err := New(store, publisherFunc(func(context.Context, Event) error { return nil }), Config{ObservationInterval: "5m"}, slog.Default())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, runner.observe(ctx))
	require.NoError(t, ctx.Err(), "a complete snapshot must not wait for the inter-sweep pause")
	assert.Equal(t, 2, store.pageCalls)
	assert.Nil(t, runner.sweep)
	assert.Equal(t, int64(ObservationPageSize+1), observedOutside(t, reader))
}

func TestRunCancelsDuringObservationPagePacing(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	pageRead := make(chan struct{}, 1)
	store := &memoryStore{at: time.Now(), afterPage: func() { pageRead <- struct{}{} }}
	for i := range ObservationPageSize + 1 {
		store.observations = append(store.observations, storage.NotificationObservation{ID: int64(i + 1)})
	}
	runner, err := New(store, publisherFunc(func(context.Context, Event) error { return nil }), Config{}, slog.Default())
	require.NoError(t, err)
	done := make(chan struct{})
	go func() { defer close(done); runner.Run(ctx) }()
	select {
	case <-pageRead:
	case <-time.After(time.Second):
		require.FailNow(t, "observer did not read its first page")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		require.FailNow(t, "page pacing did not stop on cancellation")
	}
	assert.Equal(t, 1, store.pageCalls)
	require.NotNil(t, runner.sweep, "cancellation must not finish a partial snapshot")
}

func TestLoopStopsPermanentlyOnContainedPanic(t *testing.T) {
	var logs bytes.Buffer
	runner := &Runner{logger: slog.New(slog.NewTextHandler(&logs, nil))}
	calls := 0
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	runner.loop(ctx, time.Millisecond, func(context.Context) error {
		calls++
		panic("poisoned publisher")
	})
	require.NoError(t, ctx.Err(), "contained panic must stop the loop without waiting for shutdown")
	assert.Equal(t, 1, calls)
	assert.Contains(t, logs.String(), "poisoned publisher")
	assert.Contains(t, logs.String(), "stack=")
	assert.Contains(t, logs.String(), "TestLoopStopsPermanentlyOnContainedPanic")
}

func TestLoopRetriesReturnedErrorsAfterPause(t *testing.T) {
	runner := &Runner{logger: slog.Default()}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	var calls int
	var first time.Time
	runner.loop(ctx, 20*time.Millisecond, func(context.Context) error {
		calls++
		if calls == 1 {
			first = time.Now()
			return errors.New("temporary storage failure")
		}
		assert.GreaterOrEqual(t, time.Since(first), 20*time.Millisecond)
		cancel()
		return nil
	})
	assert.Equal(t, 2, calls)
}
