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
// Status should be "success", "error", "rejected", or "conflict".
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

// knownSchemaFreshnessActions limits metric cardinality on the
// schemabot.schema_freshness.rejected counter to the three handlers that
// load a schema snapshot at discovery and reuse it at execution.
var knownSchemaFreshnessActions = map[string]bool{
	"plan":          true,
	"apply":         true,
	"apply_confirm": true,
}

// RecordSchemaFreshnessRejected increments the counter for plan/apply/apply-confirm
// commands rejected because the PR branch HEAD advanced after discovery loaded the
// schema files. The metric name is action-neutral because the same rejection fires
// for read-only plan as well as mutating apply paths. A spike indicates aggressive
// force-pushing, webhook replay, or a regression in the schema-freshness guard.
func RecordSchemaFreshnessRejected(ctx context.Context, action string) {
	if !knownSchemaFreshnessActions[action] {
		action = "unknown"
	}
	meter := otel.Meter(meterName)
	counter, err := meter.Int64Counter("schemabot.schema_freshness.rejected.total",
		otelmetric.WithDescription("Plan/apply/apply-confirm rejected because PR HEAD advanced after discovery loaded schema files"),
		otelmetric.WithUnit("{rejection}"),
	)
	if err != nil {
		slog.Warn("failed to create schema freshness rejected counter", "error", err)
		return
	}
	counter.Add(ctx, 1,
		otelmetric.WithAttributes(
			attribute.String("action", action),
		),
	)
}

var knownSourcePolicyOperations = map[string]bool{
	"plan":  true,
	"apply": true,
}

var knownSourcePolicyBlockReasons = map[string]bool{
	"missing_server_config":   true,
	"missing_database_config": true,
	"missing_repository":      true,
	"missing_pull_request":    true,
	"missing_schema_path":     true,
	"unauthorized_repo":       true,
	"unauthorized_schema_dir": true,
	"unknown":                 true,
}

// RecordSourcePolicyBlock increments the counter for source-policy decisions
// that block a trusted GitHub source before planning or applying.
func RecordSourcePolicyBlock(ctx context.Context, operation, database, environment, reason string) {
	if !knownSourcePolicyOperations[operation] {
		operation = "unknown"
	}
	if !knownSourcePolicyBlockReasons[reason] {
		reason = "unknown"
	}
	meter := otel.Meter(meterName)
	counter, err := meter.Int64Counter("schemabot.source_policy.blocks_total",
		otelmetric.WithDescription("Total trusted-source plan/apply requests blocked by source policy"),
		otelmetric.WithUnit("{block}"),
	)
	if err != nil {
		slog.Warn("failed to create source policy block counter", "error", err)
		return
	}
	counter.Add(ctx, 1,
		otelmetric.WithAttributes(
			attribute.String("operation", operation),
			attribute.String("database", database),
			attribute.String("environment", environment),
			attribute.String("reason", reason),
		),
	)
}

// knownCheckOwnershipOperations limits metric cardinality to expected check
// ownership miss paths.
var knownCheckOwnershipOperations = map[string]bool{
	"apply_finished":    true,
	"rollback_finished": true,
}

// RecordCheckOwnershipMiss increments the counter for guarded check updates
// that did not apply because stored check state no longer belonged to the
// apply being completed.
func RecordCheckOwnershipMiss(ctx context.Context, operation, repository, database, databaseType, environment string) {
	if !knownCheckOwnershipOperations[operation] {
		operation = "unknown"
	}
	meter := otel.Meter(meterName)
	counter, err := meter.Int64Counter("schemabot.check_ownership_misses_total",
		otelmetric.WithDescription("Total stored check state ownership misses"),
		otelmetric.WithUnit("{miss}"),
	)
	if err != nil {
		slog.Warn("failed to create check ownership miss counter", "error", err)
		return
	}
	counter.Add(ctx, 1,
		otelmetric.WithAttributes(
			attribute.String("operation", operation),
			attribute.String("repository", repository),
			attribute.String("database", database),
			attribute.String("database_type", databaseType),
			attribute.String("environment", environment),
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

// RecordSchedulerResume increments the scheduler resumed counter when an apply is
// successfully claimed and resumed.
func RecordSchedulerResume(ctx context.Context, database, environment, previousState string) {
	meter := otel.Meter(meterName)
	counter, err := meter.Int64Counter("schemabot.scheduler.resumed_total",
		otelmetric.WithDescription("Total number of applies resumed by the scheduler"),
		otelmetric.WithUnit("{apply}"),
	)
	if err != nil {
		slog.Warn("failed to create scheduler resumed counter", "error", err)
		return
	}
	counter.Add(ctx, 1,
		otelmetric.WithAttributes(
			attribute.String("database", database),
			attribute.String("environment", environment),
			attribute.String("previous_state", previousState),
		),
	)
}

// RecordSchedulerResumeFailure increments the scheduler resume failure counter.
func RecordSchedulerResumeFailure(ctx context.Context, database, environment, reason string) {
	meter := otel.Meter(meterName)
	counter, err := meter.Int64Counter("schemabot.scheduler.resume_failures_total",
		otelmetric.WithDescription("Total number of scheduler resume attempts that failed"),
		otelmetric.WithUnit("{apply}"),
	)
	if err != nil {
		slog.Warn("failed to create scheduler resume failure counter", "error", err)
		return
	}
	counter.Add(ctx, 1,
		otelmetric.WithAttributes(
			attribute.String("database", database),
			attribute.String("environment", environment),
			attribute.String("reason", reason),
		),
	)
}

var knownSchedulerClaimFailureReasons = map[string]bool{
	"storage_error": true,
}

// RecordSchedulerClaimFailure increments the scheduler claim failure counter.
func RecordSchedulerClaimFailure(ctx context.Context, reason string) {
	if !knownSchedulerClaimFailureReasons[reason] {
		reason = "unknown"
	}
	meter := otel.Meter(meterName)
	counter, err := meter.Int64Counter("schemabot.scheduler.claim_failures_total",
		otelmetric.WithDescription("Total number of scheduler claim attempts that failed"),
		otelmetric.WithUnit("{attempt}"),
	)
	if err != nil {
		slog.Warn("failed to create scheduler claim failure counter", "error", err)
		return
	}
	counter.Add(ctx, 1,
		otelmetric.WithAttributes(
			attribute.String("reason", reason),
		),
	)
}

// RecordSchedulerClaimDuration records how long it took to claim and resume an apply.
func RecordSchedulerClaimDuration(ctx context.Context, duration time.Duration, database, environment, previousState string) {
	meter := otel.Meter(meterName)
	hist, err := meter.Float64Histogram("schemabot.scheduler.claim_duration_seconds",
		otelmetric.WithDescription("Duration of scheduler claim + resume operations"),
		otelmetric.WithUnit("s"),
	)
	if err != nil {
		slog.Warn("failed to create scheduler claim duration histogram", "error", err)
		return
	}
	hist.Record(ctx, duration.Seconds(),
		otelmetric.WithAttributes(
			attribute.String("database", database),
			attribute.String("environment", environment),
			attribute.String("previous_state", previousState),
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

var knownStatusCheckOperations = map[string]bool{
	"plan_check_recorded":                  true,
	"apply_started":                        true,
	"apply_finished":                       true,
	"rollback_finished":                    true,
	"aggregate_check_sync":                 true,
	"stale_check_cleanup":                  true,
	"stale_check_reconciliation":           true,
	"schema_config_discovery":              true,
	"schema_config_environment_validation": true,
}

var knownStatusCheckStatuses = map[string]bool{
	"success": true,
	"error":   true,
	"skipped": true,
	"stale":   true,
	"noop":    true,
	"blocked": true,
}

// StatusCheckOperation describes one status-check storage or GitHub operation.
type StatusCheckOperation struct {
	Operation    string
	Repository   string
	Database     string
	DatabaseType string
	Environment  string
	Status       string
}

// RecordStatusCheckOperation increments the status-check operations counter.
// Unknown operation and status values are normalized to prevent unbounded cardinality.
func RecordStatusCheckOperation(ctx context.Context, op StatusCheckOperation) {
	if !knownStatusCheckOperations[op.Operation] {
		op.Operation = "unknown"
	}
	if !knownStatusCheckStatuses[op.Status] {
		op.Status = "unknown"
	}
	meter := otel.Meter(meterName)
	counter, err := meter.Int64Counter("schemabot.status_check_operations_total",
		otelmetric.WithDescription("Total number of status-check operations"),
		otelmetric.WithUnit("{operation}"),
	)
	if err != nil {
		slog.Warn("failed to create status-check operations counter", "error", err)
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("operation", op.Operation),
		attribute.String("status", op.Status),
	}
	if op.Database != "" {
		attrs = append(attrs, attribute.String("database", op.Database))
	}
	if op.DatabaseType != "" {
		attrs = append(attrs, attribute.String("database_type", op.DatabaseType))
	}
	if op.Environment != "" {
		attrs = append(attrs, attribute.String("environment", op.Environment))
	}
	if op.Repository != "" {
		attrs = append(attrs, attribute.String("repository", op.Repository))
	}
	counter.Add(ctx, 1, otelmetric.WithAttributes(attrs...))
}
