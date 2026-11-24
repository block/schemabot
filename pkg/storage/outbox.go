package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Outbox status is the delivery disposition; acceptance timestamps and incident
// tokens are retained details, not state predicates.
const (
	OutboxStatusPending     = "pending"
	OutboxStatusPublished   = "published"
	OutboxStatusQuarantined = "quarantined"
	OutboxStatusSkipped     = "skipped"
)

// RecordingErrorPolicy controls whether notification recording can block completion.
type RecordingErrorPolicy string

const (
	RecordingErrorBlock    RecordingErrorPolicy = "block"
	RecordingErrorContinue RecordingErrorPolicy = "continue"
)

// OrDefault resolves an omitted recording policy to block.
func (p RecordingErrorPolicy) OrDefault() RecordingErrorPolicy {
	if p == "" {
		return RecordingErrorBlock
	}
	return p
}

// Validate rejects unsupported recording policies.
func (p RecordingErrorPolicy) Validate() error {
	switch p.OrDefault() {
	case RecordingErrorBlock, RecordingErrorContinue:
		return nil
	default:
		return fmt.Errorf("change_events_on_recording_error must be block or continue")
	}
}

// ApplyCompletedEvent identifies a completed apply. Receivers deduplicate on ID.
type ApplyCompletedEvent struct {
	ID           string    `json:"id"`
	Database     string    `json:"database"`
	DatabaseType string    `json:"database_type"`
	Environment  string    `json:"environment"`
	Repository   string    `json:"repository,omitempty"`
	PullRequest  int       `json:"pull_request,omitempty"`
	CompletedAt  time.Time `json:"completed_at"`
}

// OutboxEvent is a notification committed alongside its apply's completion.
type OutboxEvent struct {
	ID        int64
	ApplyID   int64
	EventType string
	Payload   json.RawMessage
	CreatedAt time.Time
}

// OutboxStore holds pending completion notifications. Concurrent senders may
// deliver the same event; acceptance is recorded only after transport acceptance.
type OutboxStore interface {
	MaxID(ctx context.Context) (int64, error)
	// ListUnpublished selects only pending intent for future delivery batches.
	ListUnpublished(ctx context.Context, afterID int64, limit int) ([]*OutboxEvent, error)
	// Quarantine atomically holds pending intent and records an apply log.
	Quarantine(ctx context.Context, id int64) error
	// MarkPublished retains acceptance even if a prefetched send races a hold or skip.
	// Only pending disposition becomes published; quarantined and skipped stay held.
	MarkPublished(ctx context.Context, id int64) error
	// OldestUnpublishedAt includes pending and quarantined intent, not skipped rows.
	OldestUnpublishedAt(ctx context.Context) (*time.Time, error)
}
