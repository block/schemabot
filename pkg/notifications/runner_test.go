package notifications

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

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
	if s.pageErr != nil {
		return nil, s.pageErr
	}
	return s.observations[after:min(through, after+int64(limit))], nil
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
	defer metrics.PublishApplyNotificationBacklog(nil)
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
	require.NoError(t, runner.observe(t.Context()))
	require.NotNil(t, runner.sweep)
	assert.Equal(t, int64(ObservationPageSize), runner.sweep.after)
	assert.Equal(t, int64(1), runner.sweep.outside)
	// The page cursor stays on the last successful page if storage is unavailable.
	store.pageErr = errors.New("database unavailable")
	require.Error(t, runner.observe(t.Context()))
	assert.Equal(t, int64(ObservationPageSize), runner.sweep.after)
	store.pageErr = nil
	require.NoError(t, runner.observe(t.Context()))
	assert.Nil(t, runner.sweep)
	// A publication since the previous sweep is observed on the next one.
	store.observations[0].Published = true
	require.NoError(t, runner.observe(t.Context()))
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
