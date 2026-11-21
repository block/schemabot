package notifications

import (
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
)

type memoryOutbox struct {
	events     []*storage.OutboxEvent
	published  map[int64]bool
	markErr    error
	listErr    error
	oldestErr  error
	maxIDErr   error
	limits     []int
	batchSizes []int
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
		if event.ID > afterID && !m.published[event.ID] {
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
	if m.markErr != nil {
		return m.markErr
	}
	m.published[id] = true
	return nil
}

func (m *memoryOutbox) OldestUnpublishedAt(context.Context) (*time.Time, error) {
	for _, event := range m.events {
		if !m.published[event.ID] {
			return &event.CreatedAt, m.oldestErr
		}
	}
	return nil, m.oldestErr
}

type publisherFunc func(context.Context, json.RawMessage) error

func (f publisherFunc) Publish(ctx context.Context, payload json.RawMessage) error {
	return f(ctx, payload)
}

func newMemoryOutbox(t *testing.T, count int) *memoryOutbox {
	t.Helper()
	outbox := &memoryOutbox{published: make(map[int64]bool)}
	for i := 1; i <= count; i++ {
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
