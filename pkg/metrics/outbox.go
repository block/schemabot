package metrics

import (
	"context"
	"time"
)

// RecordOutboxOldestUnpublishedAge reports the durable sender backlog's age.
// A growing value indicates that completion notifications are not draining.
func RecordOutboxOldestUnpublishedAge(ctx context.Context, age time.Duration) {
	recordGauge(ctx, "schemabot.outbox.oldest_unpublished_age", int64(age.Seconds()),
		"Age of the oldest unpublished apply completion notification", "s", EnvironmentAttribute(""))
}
