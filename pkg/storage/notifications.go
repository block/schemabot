package storage

import (
	"context"
	"time"
)

// NotificationStore tracks publication independently of apply/driver ownership.
// Only completed applies are deliverable. Crashes between send and mark can
// duplicate an event; consumers deduplicate by ApplyIdentifier.
type NotificationStore interface {
	FindUnpublished(context.Context, time.Duration, int) ([]*Apply, error)
	Claim(context.Context, int64, string, time.Duration) (bool, error)
	MarkPublished(context.Context, int64, string) (bool, error)
	Release(context.Context, int64, string, string) (bool, error)
	// ObservationCeiling pins a sweep's ID ceiling and database-clock cutoff.
	ObservationCeiling(context.Context) (int64, time.Time, error)
	// ObservePage reads at most limit apply rows by primary key, including
	// non-completed/published rows so filtering cannot turn LIMIT into an
	// unbounded historical scan. The observer filters the returned samples.
	ObservePage(ctx context.Context, afterID, throughID int64, limit int) ([]NotificationObservation, error)
}

// NotificationObservation is the minimal state needed for backlog gauges.
type NotificationObservation struct {
	ID          int64
	Completed   bool
	CompletedAt *time.Time
	Published   bool
}
