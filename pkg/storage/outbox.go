package storage

import (
	"context"
	"encoding/json"
	"time"
)

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
// deliver the same event; publication is recorded only after transport acceptance.
type OutboxStore interface {
	MaxID(ctx context.Context) (int64, error)
	// ListUnpublished excludes quarantined intent from future delivery batches.
	ListUnpublished(ctx context.Context, afterID int64, limit int) ([]*OutboxEvent, error)
	// Quarantine atomically isolates an unaccepted notification and records an apply log.
	Quarantine(ctx context.Context, id int64) error
	MarkPublished(ctx context.Context, id int64) error
	OldestUnpublishedAt(ctx context.Context) (*time.Time, error)
}
