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
