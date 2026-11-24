package metrics

import (
	"context"
	"time"
)

// RecordOutboxRecordingFailure counts committed completions without notification
// intent. Any increment calls for investigating the paired recording-error log.
func RecordOutboxRecordingFailure(ctx context.Context, environment string) {
	addCounter(ctx, "schemabot.outbox.recording_failures_total",
		"Completed applies whose notification intent could not be recorded", "1", EnvironmentAttribute(environment))
}

// RecordOutboxOldestUnpublishedAge reports the durable sender backlog's age.
// A growing value indicates that completion notifications are not draining.
func RecordOutboxOldestUnpublishedAge(ctx context.Context, age time.Duration) {
	recordGauge(ctx, "schemabot.outbox.oldest_unpublished_age", int64(age.Seconds()),
		"Age of the oldest unpublished apply completion notification", "s", EnvironmentAttribute(""))
}
