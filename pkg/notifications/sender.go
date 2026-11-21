package notifications

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

const (
	batchSize        = 100
	operationTimeout = 10 * time.Second
	pollInterval     = time.Second
	maxBackoff       = 30 * time.Second
)

// Sender scans durable intent in bounded batches. It needs no claim: duplicate
// publication is valid, including acceptance followed by a failed storage mark.
// Each scan advances by ID even when publication fails, so a full batch of bad
// rows cannot hide later rows. At the end it returns to the oldest pending ID.
type Sender struct {
	store     storage.OutboxStore
	publisher Publisher
	logger    *slog.Logger
	afterID   int64
}

// NewSender constructs a single-loop sender; Run must not be called concurrently.
func NewSender(store storage.OutboxStore, publisher Publisher, logger *slog.Logger) *Sender {
	return &Sender{store: store, publisher: publisher, logger: logger}
}

// Run sends immediately, then polls with capped exponential backoff after a
// failed batch. Cancellation interrupts both the wait and in-flight IO.
func (s *Sender) Run(ctx context.Context) {
	delay := pollInterval
	for ctx.Err() == nil {
		if err := s.sendBatch(ctx); err != nil {
			s.logger.Warn("completion notification batch will retry", "error", err)
			delay = min(delay*2, maxBackoff)
		} else {
			delay = pollInterval
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (s *Sender) sendBatch(ctx context.Context) error {
	readCtx, cancel := context.WithTimeout(ctx, operationTimeout)
	events, err := s.store.ListUnpublished(readCtx, s.afterID, batchSize)
	cancel()
	if err != nil {
		return fmt.Errorf("load completion notification batch: %w", err)
	}
	failed := false
	for _, event := range events {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("stop completion notification batch: %w", err)
		}
		s.afterID = event.ID
		if err := s.publish(ctx, event); err != nil {
			s.logger.Warn("completion notification remains pending", "outbox_id", event.ID, "error", err)
			failed = true
		}
	}
	if len(events) < batchSize {
		s.afterID = 0
	}
	ageCtx, cancel := context.WithTimeout(ctx, operationTimeout)
	oldest, err := s.store.OldestUnpublishedAt(ageCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("observe oldest pending completion notification: %w", err)
	}
	var age time.Duration
	if oldest != nil {
		age = max(time.Duration(0), time.Since(*oldest))
	}
	metrics.RecordOutboxOldestUnpublishedAge(ctx, age)
	if failed {
		return fmt.Errorf("one or more completion notifications were not marked published")
	}
	return nil
}

func (s *Sender) publish(ctx context.Context, event *storage.OutboxEvent) error {
	sendCtx, cancel := context.WithTimeout(ctx, operationTimeout)
	err := s.publisher.Publish(sendCtx, event.Payload)
	cancel()
	if err != nil {
		return fmt.Errorf("publish notification %d: %w", event.ID, err)
	}
	markCtx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	if err := s.store.MarkPublished(markCtx, event.ID); err != nil {
		return fmt.Errorf("mark accepted notification %d published: %w", event.ID, err)
	}
	return nil
}
