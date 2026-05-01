// Package metrics provides OpenTelemetry metric recording functions for SchemaBot.
package metrics

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
)

// Meter name used for all SchemaBot metrics.
const meterName = "schemabot"

// RecordPlan increments the plans counter with database, environment, and status attributes.
// Status should be "success" or "error".
//
// The OTel SDK deduplicates instruments with the same name, so repeated calls
// to Int64Counter are cheap after the first registration.
func RecordPlan(ctx context.Context, database, environment, status string) {
	meter := otel.Meter(meterName)
	counter, err := meter.Int64Counter("schemabot.plans.total",
		otelmetric.WithDescription("Total number of plan operations"),
		otelmetric.WithUnit("{plan}"),
	)
	if err != nil {
		slog.Warn("failed to create plans counter", "error", err)
		return
	}
	counter.Add(ctx, 1,
		otelmetric.WithAttributes(
			attribute.String("database", database),
			attribute.String("environment", environment),
			attribute.String("status", status),
		),
	)
}

// RecordPlanDuration records the duration of a plan operation.
func RecordPlanDuration(ctx context.Context, duration time.Duration, database, environment, status string) {
	meter := otel.Meter(meterName)
	hist, err := meter.Float64Histogram("schemabot.plan.duration_seconds",
		otelmetric.WithDescription("Duration of plan operations"),
		otelmetric.WithUnit("s"),
	)
	if err != nil {
		slog.Warn("failed to create plan duration histogram", "error", err)
		return
	}
	hist.Record(ctx, duration.Seconds(),
		otelmetric.WithAttributes(
			attribute.String("database", database),
			attribute.String("environment", environment),
			attribute.String("status", status),
		),
	)
}

// RecordApply increments the applies counter with database, environment, and status attributes.
// Status should be "success", "error", or "rejected".
func RecordApply(ctx context.Context, database, environment, status string) {
	meter := otel.Meter(meterName)
	counter, err := meter.Int64Counter("schemabot.applies.total",
		otelmetric.WithDescription("Total number of apply operations"),
		otelmetric.WithUnit("{apply}"),
	)
	if err != nil {
		slog.Warn("failed to create applies counter", "error", err)
		return
	}
	counter.Add(ctx, 1,
		otelmetric.WithAttributes(
			attribute.String("database", database),
			attribute.String("environment", environment),
			attribute.String("status", status),
		),
	)
}

// RecordApplyDuration records the duration of an apply operation (API call time,
// not the full Spirit run which can take hours).
func RecordApplyDuration(ctx context.Context, duration time.Duration, database, environment, status string) {
	meter := otel.Meter(meterName)
	hist, err := meter.Float64Histogram("schemabot.apply.duration_seconds",
		otelmetric.WithDescription("Duration of apply operations (API call time)"),
		otelmetric.WithUnit("s"),
	)
	if err != nil {
		slog.Warn("failed to create apply duration histogram", "error", err)
		return
	}
	hist.Record(ctx, duration.Seconds(),
		otelmetric.WithAttributes(
			attribute.String("database", database),
			attribute.String("environment", environment),
			attribute.String("status", status),
		),
	)
}

// AdjustActiveApplies increments or decrements the active applies gauge.
// Use delta=1 when an apply is accepted and delta=-1 when it reaches a terminal state.
func AdjustActiveApplies(ctx context.Context, delta int64, database, environment string) {
	meter := otel.Meter(meterName)
	counter, err := meter.Int64UpDownCounter("schemabot.active_applies",
		otelmetric.WithDescription("Number of currently in-progress applies"),
		otelmetric.WithUnit("{apply}"),
	)
	if err != nil {
		slog.Warn("failed to create active applies gauge", "error", err)
		return
	}
	counter.Add(ctx, delta,
		otelmetric.WithAttributes(
			attribute.String("database", database),
			attribute.String("environment", environment),
		),
	)
}

// knownControlOperations limits metric cardinality to expected control operations.
var knownControlOperations = map[string]bool{
	"cutover":       true,
	"stop":          true,
	"start":         true,
	"volume":        true,
	"revert":        true,
	"skip_revert":   true,
	"rollback_plan": true,
}

// RecordControlOperation increments the control operations counter.
// Operation should be one of: cutover, stop, start, volume, revert, skip_revert, rollback_plan.
// Status should be "success" or "error".
func RecordControlOperation(ctx context.Context, operation, database, environment, status string) {
	if !knownControlOperations[operation] {
		operation = "unknown"
	}
	meter := otel.Meter(meterName)
	counter, err := meter.Int64Counter("schemabot.control_operations_total",
		otelmetric.WithDescription("Total number of control operations (cutover, stop, start, etc.)"),
		otelmetric.WithUnit("{operation}"),
	)
	if err != nil {
		slog.Warn("failed to create control operations counter", "error", err)
		return
	}
	counter.Add(ctx, 1,
		otelmetric.WithAttributes(
			attribute.String("operation", operation),
			attribute.String("database", database),
			attribute.String("environment", environment),
			attribute.String("status", status),
		),
	)
}

// RecordLockOperation increments the lock operations counter.
// Operation should be "acquire" or "release".
// Status should be "success", "conflict", "not_found", "not_owned", or "error".
func RecordLockOperation(ctx context.Context, operation, database, status string) {
	meter := otel.Meter(meterName)
	counter, err := meter.Int64Counter("schemabot.lock_operations_total",
		otelmetric.WithDescription("Total number of lock acquire/release operations"),
		otelmetric.WithUnit("{operation}"),
	)
	if err != nil {
		slog.Warn("failed to create lock operations counter", "error", err)
		return
	}
	counter.Add(ctx, 1,
		otelmetric.WithAttributes(
			attribute.String("operation", operation),
			attribute.String("database", database),
			attribute.String("status", status),
		),
	)
}

// RecordRecoveryCycle increments the recovery cycle counter and records
// how many applies were recovered and how many failed in this cycle.
func RecordRecoveryCycle(ctx context.Context, recovered, failed int) {
	meter := otel.Meter(meterName)
	cycleCounter, err := meter.Int64Counter("schemabot.recovery.cycles_total",
		otelmetric.WithDescription("Total number of recovery worker cycles"),
		otelmetric.WithUnit("{cycle}"),
	)
	if err != nil {
		slog.Warn("failed to create recovery cycles counter", "error", err)
		return
	}
	cycleCounter.Add(ctx, 1)

	if recovered > 0 {
		recoveredCounter, err := meter.Int64Counter("schemabot.recovery.recovered_total",
			otelmetric.WithDescription("Total number of applies recovered by the recovery worker"),
			otelmetric.WithUnit("{apply}"),
		)
		if err != nil {
			slog.Warn("failed to create recovery recovered counter", "error", err)
			return
		}
		recoveredCounter.Add(ctx, int64(recovered))
	}

	if failed > 0 {
		failedCounter, err := meter.Int64Counter("schemabot.recovery.failed_total",
			otelmetric.WithDescription("Total number of recovery attempts that failed"),
			otelmetric.WithUnit("{apply}"),
		)
		if err != nil {
			slog.Warn("failed to create recovery failed counter", "error", err)
			return
		}
		failedCounter.Add(ctx, int64(failed))
	}
}

// knownWebhookEvents limits metric cardinality to expected GitHub event types.
var knownWebhookEvents = map[string]bool{
	"issue_comment": true,
	"pull_request":  true,
	"check_run":     true,
	"ping":          true,
}

// knownWebhookActions limits metric cardinality to expected GitHub webhook actions.
var knownWebhookActions = map[string]bool{
	"created":     true, // issue_comment
	"opened":      true, // pull_request
	"synchronize": true, // pull_request
	"reopened":    true, // pull_request
	"closed":      true, // pull_request
	"requested":   true, // check_run
	"completed":   true, // check_run
	"":            true, // events without actions (e.g., ping)
}

// RecordWebhookEvent increments the webhook events counter.
// Unknown event types and actions are normalized to "unknown" to prevent unbounded cardinality.
// Repo is not allowlisted since it's bounded by the repos configured in SchemaBot.
func RecordWebhookEvent(ctx context.Context, eventType, action, repo, status string) {
	if !knownWebhookEvents[eventType] {
		eventType = "unknown"
	}
	if !knownWebhookActions[action] {
		action = "unknown"
	}
	meter := otel.Meter(meterName)
	counter, err := meter.Int64Counter("schemabot.webhook.events_total",
		otelmetric.WithDescription("Total number of GitHub webhook events received"),
		otelmetric.WithUnit("{event}"),
	)
	if err != nil {
		slog.Warn("failed to create webhook events counter", "error", err)
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("event_type", eventType),
		attribute.String("status", status),
	}
	if action != "" {
		attrs = append(attrs, attribute.String("action", action))
	}
	if repo != "" {
		attrs = append(attrs, attribute.String("repository", repo))
	}
	counter.Add(ctx, 1, otelmetric.WithAttributes(attrs...))
}
