package storage

import (
	"context"
	"encoding/json"
	"time"
)

// Outbox status is the delivery disposition; acceptance timestamps are retained
// details, not state predicates.
const (
	OutboxStatusPending     = "pending"
	OutboxStatusPublished   = "published"
	OutboxStatusQuarantined = "quarantined"
)

// ApplyFinishedEvent records a committed successful completion. EventID
// deduplicates delivery; ID identifies the apply.
type ApplyFinishedEvent struct {
	EventID string `json:"event_id"`
	Type    string `json:"type"`
	App     string `json:"app"`
	Outcome string `json:"outcome"`
	// OccurredAtNanos is the intent's capture time in Unix nanoseconds, before
	// its state transaction commits. It is not the commit or delivery time.
	OccurredAtNanos int64     `json:"occurred_at_nanos"`
	ID              string    `json:"id"`
	Database        string    `json:"database"`
	DatabaseType    string    `json:"database_type"`
	Environment     string    `json:"environment"`
	Repository      string    `json:"repository,omitempty"`
	PullRequest     int       `json:"pull_request,omitempty"`
	CompletedAt     time.Time `json:"completed_at"`
}

// OutboxEvent is a notification committed alongside its apply's outcome.
type OutboxEvent struct {
	ID        int64
	ApplyID   int64
	EventType string
	Payload   json.RawMessage
	CreatedAt time.Time
}

// OutboxStore holds pending outcome notifications. Concurrent senders may
// deliver the same event; acceptance is recorded only after transport acceptance.
type OutboxStore interface {
	MaxID(ctx context.Context) (int64, error)
	// ListUnpublished selects only pending intent for future delivery batches.
	ListUnpublished(ctx context.Context, afterID int64, limit int) ([]*OutboxEvent, error)
	// Quarantine atomically holds pending intent and records an apply log.
	Quarantine(ctx context.Context, id int64) error
	// MarkPublished retains acceptance even if a prefetched send races a quarantine.
	// Only pending disposition becomes published; quarantined intent stays held.
	MarkPublished(ctx context.Context, id int64) error
	// OldestUnpublishedAt includes both pending and quarantined intent.
	OldestUnpublishedAt(ctx context.Context) (*time.Time, error)
}
