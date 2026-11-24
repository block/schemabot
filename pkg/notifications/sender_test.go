package notifications

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type memoryOutbox struct {
	events        []*storage.OutboxEvent
	published     map[int64]bool
	status        map[int64]string
	quarantineErr error
	markErr       error
	markPanic     bool
	listErr       error
	oldestErr     error
	maxIDErr      error
	limits        []int
	batchSizes    []int
}

func (m *memoryOutbox) MaxID(context.Context) (int64, error) {
	var id int64
	for _, event := range m.events {
		id = max(id, event.ID)
	}
	return id, m.maxIDErr
}

func (m *memoryOutbox) ListUnpublished(_ context.Context, afterID int64, limit int) ([]*storage.OutboxEvent, error) {
	m.limits = append(m.limits, limit)
	var events []*storage.OutboxEvent
	for _, event := range m.events {
		if event.ID > afterID && m.status[event.ID] == storage.OutboxStatusPending {
			events = append(events, event)
			if len(events) == limit {
				break
			}
		}
	}
	m.batchSizes = append(m.batchSizes, len(events))
	return events, m.listErr
}

func (m *memoryOutbox) MarkPublished(_ context.Context, id int64) error {
	if m.markPanic && id == 1 {
		panic("mark fault")
	}
	if m.markErr != nil {
		return m.markErr
	}
	m.published[id] = true
	if m.status[id] == storage.OutboxStatusPending {
		m.status[id] = storage.OutboxStatusPublished
	}
	return nil
}

func (m *memoryOutbox) OldestUnpublishedAt(context.Context) (*time.Time, error) {
	var oldest *time.Time
	for _, event := range m.events {
		if m.status[event.ID] == storage.OutboxStatusPending || m.status[event.ID] == storage.OutboxStatusQuarantined {
			if oldest == nil || event.CreatedAt.Before(*oldest) {
				oldest = &event.CreatedAt
			}
		}
	}
	return oldest, m.oldestErr
}

type publisherFunc func(context.Context, json.RawMessage) error

func (f publisherFunc) Publish(ctx context.Context, payload json.RawMessage) error {
	return f(ctx, payload)
}

func newMemoryOutbox(t *testing.T, count int) *memoryOutbox {
	t.Helper()
	outbox := &memoryOutbox{published: make(map[int64]bool), status: make(map[int64]string)}
	for i := 1; i <= count; i++ {
		outbox.status[int64(i)] = storage.OutboxStatusPending
		payload, err := json.Marshal(storage.ApplyCompletedEvent{ID: fmt.Sprintf("apply_%d", i), Database: "orders", DatabaseType: storage.DatabaseTypeMySQL, Environment: "production", CompletedAt: time.Now().UTC()})
		require.NoError(t, err)
		outbox.events = append(outbox.events, &storage.OutboxEvent{ID: int64(i), Payload: payload, CreatedAt: time.Now().Add(-time.Minute)})
	}
	return outbox
}

func TestSenderAcceptanceAndReplay(t *testing.T) {
	store := newMemoryOutbox(t, 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var accepted []string
	publisher := publisherFunc(func(_ context.Context, payload json.RawMessage) error {
		accepted = append(accepted, string(payload))
		return nil
	})
	// Acceptance without a durable mark can be replayed by a fresh sender.
	store.markErr = errors.New("storage unavailable")
	require.ErrorContains(t, NewSender(store, publisher, logger).sendBatch(t.Context()), "not marked published")
	assert.Empty(t, store.published)
	store.markErr = nil
	require.NoError(t, NewSender(store, publisher, logger).sendBatch(t.Context()))
	require.Len(t, accepted, 2)
	assert.JSONEq(t, accepted[0], accepted[1])
	var message storage.ApplyCompletedEvent
	require.NoError(t, json.Unmarshal([]byte(accepted[1]), &message))
	assert.Equal(t, "apply_1", message.ID)
	assert.True(t, store.published[1])
}

func TestSenderFailureDoesNotHideLaterBatches(t *testing.T) {
	store := newMemoryOutbox(t, batchSize+1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var attempts []string
	publisher := publisherFunc(func(_ context.Context, payload json.RawMessage) error {
		var message storage.ApplyCompletedEvent
		require.NoError(t, json.Unmarshal(payload, &message))
		attempts = append(attempts, message.ID)
		if message.ID != fmt.Sprintf("apply_%d", batchSize+1) {
			return errors.New("SQS rejected notification")
		}
		return nil
	})
	sender := NewSender(store, publisher, logger)
	require.Error(t, sender.sendBatch(t.Context()))
	assert.Empty(t, store.published)
	require.NoError(t, sender.sendBatch(t.Context()))
	assert.True(t, store.published[int64(batchSize+1)])
	assert.Len(t, attempts, batchSize+1)
	assert.Equal(t, []int{batchSize, batchSize}, store.limits)
	assert.Zero(t, sender.afterID, "a completed scan returns to retry earlier failures")
	require.Error(t, sender.sendBatch(t.Context()))
	assert.Len(t, attempts, 2*batchSize+1)
}

// Continuously full batches do not hide earlier failures or rows that commit
// behind the cursor. Both are revisited after the captured high-water ID.
func TestSenderSustainedArrivalsRevisitEarlierRows(t *testing.T) {
	store := newMemoryOutbox(t, 4*batchSize+1)
	lateCommit := store.events[0]
	arrivals := store.events[2*batchSize+1:]
	store.events = store.events[1 : 2*batchSize+1]
	var accepted []string
	recovered := false
	publisher := publisherFunc(func(_ context.Context, payload json.RawMessage) error {
		var message storage.ApplyCompletedEvent
		require.NoError(t, json.Unmarshal(payload, &message))
		if message.ID == "apply_2" && !recovered {
			return errors.New("SQS rejected notification")
		}
		accepted = append(accepted, message.ID)
		return nil
	})
	sender := NewSender(store, publisher, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.Error(t, sender.sendBatch(t.Context()))
	assert.Equal(t, int64(2*batchSize+1), sender.scanThroughID)
	store.events = append(store.events, arrivals[:batchSize]...)
	require.NoError(t, sender.sendBatch(t.Context()))
	require.Zero(t, sender.afterID, "reaching the captured bound ends even a full batch")
	store.events = append([]*storage.OutboxEvent{lateCommit}, store.events...)
	store.events = append(store.events, arrivals[batchSize:]...)
	recovered = true
	require.NoError(t, sender.sendBatch(t.Context()))
	assert.True(t, store.published[lateCommit.ID], "the row committed behind the cursor must be delivered")
	assert.True(t, store.published[2], "the earlier failure must be retried after recovery")
	assert.Contains(t, accepted, "apply_1")
	assert.Contains(t, accepted, "apply_2")
	assert.Equal(t, []int{batchSize, batchSize, batchSize}, store.batchSizes)
}

func TestSenderCancellationInterruptsPublish(t *testing.T) {
	store := newMemoryOutbox(t, 1)
	started := make(chan struct{})
	publisher := publisherFunc(func(ctx context.Context, _ json.RawMessage) error {
		_, bounded := ctx.Deadline()
		if !bounded {
			return errors.New("publish context has no deadline")
		}
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	sender := NewSender(store, publisher, slog.New(slog.NewTextHandler(io.Discard, nil)))
	go func() { defer close(done); sender.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		require.FailNow(t, "sender did not start publication")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		require.FailNow(t, "sender did not stop after cancellation")
	}
	assert.Empty(t, store.published)
}

func TestSenderStorageErrorsRemainVisible(t *testing.T) {
	store := newMemoryOutbox(t, 0)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sender := NewSender(store, publisherFunc(func(context.Context, json.RawMessage) error { return nil }), logger)
	store.maxIDErr = errors.New("bound unavailable")
	require.ErrorContains(t, sender.sendBatch(t.Context()), "bound unavailable")
	store.maxIDErr = nil
	store.listErr = errors.New("list unavailable")
	require.ErrorContains(t, sender.sendBatch(t.Context()), "list unavailable")
	store.listErr = nil
	store.oldestErr = errors.New("age unavailable")
	require.ErrorContains(t, sender.sendBatch(t.Context()), "age unavailable")
}

func (m *memoryOutbox) Quarantine(_ context.Context, id int64) error {
	if m.quarantineErr != nil {
		return m.quarantineErr
	}
	if m.status[id] == storage.OutboxStatusPending {
		m.status[id] = storage.OutboxStatusQuarantined
	}
	return nil
}

// A panicking notification is isolated durably, while later rows deliver and
// a fresh sender observes the same exclusion without losing backlog visibility.
func TestSenderPanicQuarantine(t *testing.T) {
	for _, markPanic := range []bool{false, true} {
		t.Run(fmt.Sprintf("MarkPanic=%v", markPanic), func(t *testing.T) {
			reader := metric.NewManualReader()
			provider := metric.NewMeterProvider(metric.WithReader(reader))
			previous := otel.GetMeterProvider()
			otel.SetMeterProvider(provider)
			t.Cleanup(func() {
				otel.SetMeterProvider(previous)
				require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context())))
			})
			store := newMemoryOutbox(t, 2)
			store.markPanic = markPanic
			var output bytes.Buffer
			var attempts []string
			publisher := publisherFunc(func(_ context.Context, payload json.RawMessage) error {
				var message storage.ApplyCompletedEvent
				require.NoError(t, json.Unmarshal(payload, &message))
				attempts = append(attempts, message.ID)
				if message.ID == "apply_1" && !markPanic {
					panic("publisher fault")
				}
				return nil
			})
			logger := slog.New(slog.NewJSONHandler(&output, nil))
			sender := NewSender(store, publisher, logger)
			require.NoError(t, sender.sendBatch(t.Context()))
			require.NoError(t, sender.sendBatch(t.Context()))
			require.NoError(t, NewSender(store, publisher, logger).sendBatch(t.Context()))
			assert.Equal(t, []string{"apply_1", "apply_2"}, attempts)
			assert.Equal(t, storage.OutboxStatusQuarantined, store.status[1])
			assert.False(t, store.published[1])
			assert.True(t, store.published[2])
			assert.Contains(t, output.String(), `"apply_id":"apply_1"`)
			var data metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(t.Context(), &data))
			var panicCount int64
			var oldestAge int64
			for _, scope := range data.ScopeMetrics {
				for _, instrument := range scope.Metrics {
					switch instrument.Name {
					case "schemabot.panic.recovered_total":
						sum, ok := instrument.Data.(metricdata.Sum[int64])
						require.True(t, ok)
						for _, point := range sum.DataPoints {
							operation, ok := point.Attributes.Value("operation")
							require.True(t, ok)
							assert.Equal(t, "completion_sender", operation.AsString())
							panicCount += point.Value
						}
					case "schemabot.outbox.oldest_unpublished_age":
						gauge, ok := instrument.Data.(metricdata.Gauge[int64])
						require.True(t, ok)
						require.Len(t, gauge.DataPoints, 1)
						oldestAge = gauge.DataPoints[0].Value
					}
				}
			}
			assert.Equal(t, int64(1), panicCount)
			assert.GreaterOrEqual(t, oldestAge, int64(60), "quarantined intent remains in the backlog")
		})
	}
}

func TestSenderQuarantineFailureRemainsVisible(t *testing.T) {
	store := newMemoryOutbox(t, 2)
	store.quarantineErr = errors.New("diagnostic storage unavailable")
	var output bytes.Buffer
	publisher := publisherFunc(func(_ context.Context, payload json.RawMessage) error {
		var message storage.ApplyCompletedEvent
		require.NoError(t, json.Unmarshal(payload, &message))
		if message.ID == "apply_1" {
			panic("publisher fault")
		}
		return nil
	})
	sender := NewSender(store, publisher, slog.New(slog.NewJSONHandler(&output, nil)))
	require.Error(t, sender.sendBatch(t.Context()))
	assert.Equal(t, storage.OutboxStatusPending, store.status[1])
	assert.False(t, store.published[1])
	assert.True(t, store.published[2])
	assert.Contains(t, output.String(), "completion notification quarantine failed")
	assert.Contains(t, output.String(), "diagnostic storage unavailable")
}

func TestSenderFailureIdentityLogs(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		for _, markFailure := range []bool{false, true} {
			t.Run(fmt.Sprintf("Malformed=%v/MarkFailure=%v", malformed, markFailure), func(t *testing.T) {
				store := newMemoryOutbox(t, 1)
				if malformed {
					store.events[0].Payload = json.RawMessage(`{"secret":"do-not-log",`)
				}
				if markFailure {
					store.markErr = errors.New("mark unavailable")
				}
				var output bytes.Buffer
				publisher := publisherFunc(func(context.Context, json.RawMessage) error {
					if markFailure {
						return nil
					}
					return errors.New("transport unavailable")
				})
				sender := NewSender(store, publisher, slog.New(slog.NewJSONHandler(&output, nil)))
				require.Error(t, sender.sendBatch(t.Context()))
				log := output.String()
				assert.Contains(t, log, `"outbox_id":1`)
				assert.NotContains(t, log, "do-not-log")
				if malformed {
					assert.Contains(t, log, "identity_decode_error")
				} else {
					assert.Contains(t, log, `"apply_id":"apply_1"`)
					assert.Contains(t, log, `"database":"orders"`)
					assert.Contains(t, log, `"database_type":"mysql"`)
					assert.Contains(t, log, `"environment":"production"`)
				}
			})
		}
	}
	// A malformed diagnostic payload is still handed to the transport unchanged.
	store := newMemoryOutbox(t, 1)
	store.events[0].Payload = json.RawMessage(`invalid-json`)
	var sent json.RawMessage
	sender := NewSender(store, publisherFunc(func(_ context.Context, payload json.RawMessage) error {
		sent = payload
		return nil
	}), slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, sender.sendBatch(t.Context()))
	assert.Equal(t, store.events[0].Payload, sent)
	assert.True(t, store.published[1])
}

// A disposition changed after prefetch suppresses future batches, not the send
// already selected. Acceptance remains visible without clearing a hold or skip.
func TestSenderPrefetchedDisposition(t *testing.T) {
	for _, disposition := range []string{storage.OutboxStatusQuarantined, storage.OutboxStatusSkipped} {
		t.Run(disposition, func(t *testing.T) {
			store := newMemoryOutbox(t, 2)
			var accepted []string
			publisher := publisherFunc(func(_ context.Context, payload json.RawMessage) error {
				var message storage.ApplyCompletedEvent
				require.NoError(t, json.Unmarshal(payload, &message))
				accepted = append(accepted, message.ID)
				if message.ID == "apply_1" {
					store.status[1] = disposition
				}
				return nil
			})
			sender := NewSender(store, publisher, slog.New(slog.DiscardHandler))
			require.NoError(t, sender.sendBatch(t.Context()))
			require.NoError(t, sender.sendBatch(t.Context()))
			assert.Equal(t, []string{"apply_1", "apply_2"}, accepted)
			assert.Equal(t, disposition, store.status[1])
			assert.True(t, store.published[1], "actual acceptance is retained")
			oldest, err := store.OldestUnpublishedAt(t.Context())
			require.NoError(t, err)
			if disposition == storage.OutboxStatusSkipped {
				assert.Nil(t, oldest)
			} else {
				require.NotNil(t, oldest)
				assert.Equal(t, store.events[0].CreatedAt, *oldest)
			}
		})
	}
}
