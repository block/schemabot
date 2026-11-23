package notifications

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/panicsafe"
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
// rows cannot hide later rows. Each scan captures a high-water ID, then returns
// to the oldest pending ID even while new full batches keep arriving.
type Sender struct {
	store         storage.OutboxStore
	publisher     Publisher
	logger        *slog.Logger
	afterID       int64
	scanThroughID int64
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
	if s.afterID == 0 {
		boundCtx, cancel := context.WithTimeout(ctx, operationTimeout)
		bound, err := s.store.MaxID(boundCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("bound completion notification scan: %w", err)
		}
		s.scanThroughID = bound
	}
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
		if err := s.publishContained(ctx, event); err != nil {
			s.logger.Warn("completion notification remains pending", append(s.eventLogAttrs(event), "error", err)...)
			failed = true
		}
	}
	if len(events) < batchSize || s.afterID >= s.scanThroughID {
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
	defer cancel()
	err := s.publisher.Publish(sendCtx, event.Payload)
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

// eventLogAttrs uses the immutable event identity without logging its payload.
// A diagnostic decode failure must not prevent transport from accepting it.
func (s *Sender) eventLogAttrs(event *storage.OutboxEvent) []any {
	attrs := []any{"outbox_id", event.ID}
	var completion storage.ApplyCompletedEvent
	if err := json.Unmarshal(event.Payload, &completion); err != nil {
		return append(attrs, "identity_decode_error", err)
	}
	identity := &storage.Apply{ApplyIdentifier: completion.ID, Database: completion.Database,
		DatabaseType: completion.DatabaseType, Environment: completion.Environment,
		Repository: completion.Repository, PullRequest: completion.PullRequest}
	return append(attrs, identity.IdentityLogAttrs()...)
}

func (s *Sender) publishContained(ctx context.Context, event *storage.OutboxEvent) error {
	recovered, err := panicsafe.Catch(func() error { return s.publish(ctx, event) })
	if recovered == nil {
		return err
	}
	s.logger.Error("completion notification panicked; attempting durable quarantine",
		append(s.eventLogAttrs(event), "panic", fmt.Sprint(recovered.Value), "stack", string(recovered.Stack))...)
	metrics.RecordRecoveredPanic(context.WithoutCancel(ctx), "completion_sender")
	quarantineCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), operationTimeout)
	defer cancel()
	if err := s.store.Quarantine(quarantineCtx, event.ID); err != nil {
		s.logger.Error("completion notification quarantine failed; a later scan can retry this notification",
			append(s.eventLogAttrs(event), "error", err)...)
		return fmt.Errorf("persist panic quarantine for notification %d: %w", event.ID, err)
	}
	return nil
}
