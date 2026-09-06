// Package metrics provides OpenTelemetry metric recording functions for SchemaBot.
package metrics

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/block/schemabot/pkg/storage"
)

// Meter name used for all SchemaBot metrics.
const meterName = "schemabot"

const (
	unknownDeployment  = "unknown"
	unknownEnvironment = "unknown"
)

// addCounter increments a named Int64Counter by one with the given attributes,
// logging and skipping if the instrument cannot be created. Centralizing the
// meter/create/error/emit boilerplate keeps each Record* function to its metric
// descriptor and attribute set.
func addCounter(ctx context.Context, name, description, unit string, attrs ...attribute.KeyValue) {
	meter := otel.Meter(meterName)
	counter, err := meter.Int64Counter(name,
		otelmetric.WithDescription(description),
		otelmetric.WithUnit(unit),
	)
	if err != nil {
		slog.Warn("failed to create counter", "metric", name, "error", err)
		return
	}
	counter.Add(ctx, 1, otelmetric.WithAttributes(attrs...))
}

// addCounterN increments a named Int64Counter by n with the given attributes,
// logging and skipping if the instrument cannot be created. n <= 0 is a no-op.
func addCounterN(ctx context.Context, n int64, name, description, unit string, attrs ...attribute.KeyValue) {
	if n <= 0 {
		return
	}
	meter := otel.Meter(meterName)
	counter, err := meter.Int64Counter(name,
		otelmetric.WithDescription(description),
		otelmetric.WithUnit(unit),
	)
	if err != nil {
		slog.Warn("failed to create counter", "metric", name, "error", err)
		return
	}
	counter.Add(ctx, n, otelmetric.WithAttributes(attrs...))
}

// addUpDownCounter adjusts a named Int64UpDownCounter by delta with the given
// attributes, logging and skipping if the instrument cannot be created.
func addUpDownCounter(ctx context.Context, name string, delta int64, description, unit string, attrs ...attribute.KeyValue) {
	meter := otel.Meter(meterName)
	counter, err := meter.Int64UpDownCounter(name,
		otelmetric.WithDescription(description),
		otelmetric.WithUnit(unit),
	)
	if err != nil {
		slog.Warn("failed to create up/down counter", "metric", name, "error", err)
		return
	}
	counter.Add(ctx, delta, otelmetric.WithAttributes(attrs...))
}

// recordHistogram records a duration value in seconds into a named
// Float64Histogram with the given attributes, logging and skipping if the
// instrument cannot be created.
func recordHistogram(ctx context.Context, name string, value float64, description string, attrs ...attribute.KeyValue) {
	meter := otel.Meter(meterName)
	hist, err := meter.Float64Histogram(name,
		otelmetric.WithDescription(description),
		otelmetric.WithUnit("s"),
	)
	if err != nil {
		slog.Warn("failed to create histogram", "metric", name, "error", err)
		return
	}
	hist.Record(ctx, value, otelmetric.WithAttributes(attrs...))
}

// recordGauge records value into a named Int64Gauge with the given attributes,
// logging and skipping if the instrument cannot be created.
func recordGauge(ctx context.Context, name string, value int64, description, unit string, attrs ...attribute.KeyValue) {
	meter := otel.Meter(meterName)
	gauge, err := meter.Int64Gauge(name,
		otelmetric.WithDescription(description),
		otelmetric.WithUnit(unit),
	)
	if err != nil {
		slog.Warn("failed to create gauge", "metric", name, "error", err)
		return
	}
	gauge.Record(ctx, value, otelmetric.WithAttributes(attrs...))
}

// DeploymentAttribute returns the canonical deployment metric attribute.
// Use "unknown" when the request fails before SchemaBot resolves routing.
func DeploymentAttribute(deployment string) attribute.KeyValue {
	if deployment == "" {
		deployment = unknownDeployment
	}
	return attribute.String("deployment", deployment)
}

// EnvironmentAttribute returns the canonical environment metric attribute.
// Use "unknown" for process-wide or integration metrics that do not belong to
// a single SchemaBot environment.
func EnvironmentAttribute(environment string) attribute.KeyValue {
	if environment == "" {
		environment = unknownEnvironment
	}
	return attribute.String("environment", environment)
}

// RecordPlan increments the plans counter with database, deployment, environment, and status attributes.
// Status should be "success" or "error".
//
// The OTel SDK deduplicates instruments with the same name, so repeated calls
// to Int64Counter are cheap after the first registration.
func RecordPlan(ctx context.Context, repo, database, deployment, environment, status string) {
	addCounter(ctx, "schemabot.plans.total",
		"Total number of plan operations", "{plan}",
		attribute.String("repository", repo),
		attribute.String("database", database),
		DeploymentAttribute(deployment),
		EnvironmentAttribute(environment),
		attribute.String("status", status),
	)
}

// RecordPlanCommentRetirement counts the outcome of retiring one superseded
// plan comment. Outcomes: "minimized" (hidden on GitHub but still expandable
// as the record of what was planned), "deleted" (no apply ever acted on the
// plan and the repository opted into deletion, so the comment is removed from
// the timeline), "apply_owned" (kept fully expanded because an apply owns the
// plan's head and the repository uses the minimize-based policy),
// "guard_error" (apply-ownership lookup failed, comment left untouched fail
// closed — investigate storage), "minimize_error" / "delete_error" (the
// GitHub call failed; retried on the next supersede — investigate GitHub API
// health), "minimize_mark_error" / "delete_mark_error" (GitHub succeeded but
// the storage mark failed; the next supersede retries the mark idempotently —
// investigate storage).
func RecordPlanCommentRetirement(ctx context.Context, repo, outcome string) {
	addCounter(ctx, "schemabot.plan_comment_retirement.total",
		"Total number of superseded plan-comment retirement attempts by outcome", "{comment}",
		attribute.String("repository", repo),
		attribute.String("outcome", outcome),
	)
}

// RecordPlanChangeOwnership counts one planned destructive change's ownership
// verdict. Outcomes: "unowned" (no other pull request is attributed the table,
// so the change renders normally), "owned" (an open pull request is attributed
// it, so the disclosure carries the attribution on every comment whose reader
// still holds the apply decision), "storage_error" and "pr_state_error" (the
// lookup could not decide, so the change is attributed as unresolved —
// investigate storage or GitHub API health respectively).
// Sustained error outcomes mean operators are seeing destructive changes they
// cannot get an attribution for; a rising "owned" count means pull requests are
// applying and merging far apart.
func RecordPlanChangeOwnership(ctx context.Context, repo, database, environment, outcome string) {
	addCounter(ctx, "schemabot.plan_change_ownership.total",
		"Total number of planned destructive changes classified by table-ownership lookup outcome", "{change}",
		attribute.String("repository", repo),
		attribute.String("database", database),
		EnvironmentAttribute(environment),
		attribute.String("outcome", outcome),
	)
}

// RecordPlanDuration records the duration of a plan operation.
func RecordPlanDuration(ctx context.Context, duration time.Duration, repo, database, deployment, environment, status string) {
	recordHistogram(ctx, "schemabot.plan.duration_seconds", duration.Seconds(),
		"Duration of plan operations",
		attribute.String("repository", repo),
		attribute.String("database", database),
		DeploymentAttribute(deployment),
		EnvironmentAttribute(environment),
		attribute.String("status", status),
	)
}

// RecordDeploymentDiff counts one deployment's review-time desired-vs-live diff
// outcome. status is "ok" when the diff was computed and reported no planning
// errors, or "errored" when the PlanDiff RPC failed or the diff itself reported
// errors — the branch that makes a deployment block the review-time drift rollup
// closed. Alert on the errored count so an unreachable or un-diffable deployment
// gating merges is investigable without waiting on the rollup layer.
func RecordDeploymentDiff(ctx context.Context, database, deployment, environment, status string) {
	addCounter(ctx, "schemabot.deployment_diff.total",
		"Total number of review-time per-deployment plan diffs by outcome", "{diff}",
		attribute.String("database", database),
		DeploymentAttribute(deployment),
		EnvironmentAttribute(environment),
		attribute.String("status", status),
	)
}

// RecordApply increments the applies counter with database, deployment, environment, and status attributes.
// Status should be "success", "error", "rejected", or "conflict".
func RecordApply(ctx context.Context, repo, database, deployment, environment, status string) {
	addCounter(ctx, "schemabot.applies.total",
		"Total number of apply operations", "{apply}",
		attribute.String("repository", repo),
		attribute.String("database", database),
		DeploymentAttribute(deployment),
		EnvironmentAttribute(environment),
		attribute.String("status", status),
	)
}

// RecordAutoCutoverFailure increments the counter for a drive auto-cutover
// attempt the engine backend rejected with a hard error (not a self-clearing
// not-ready rejection). The drive is the sole cutover actor, so a spike means
// applies are pinned at waiting_for_cutover with the drive unable to complete
// them — investigate the engine backend's cutover permissions and API health
// for the database. The drive settles the apply (failed, or paused for
// operator retry) after repeated consecutive failures.
func RecordAutoCutoverFailure(ctx context.Context, database, deployment, environment string) {
	addCounter(ctx, "schemabot.apply.auto_cutover_failure.total",
		"Drive auto-cutover attempts rejected by the engine backend with a hard error", "{failure}",
		attribute.String("database", database),
		DeploymentAttribute(deployment),
		EnvironmentAttribute(environment),
	)
}

// RecordApplyDuration records the duration of an apply operation (API call time,
// not the full Spirit run which can take hours).
func RecordApplyDuration(ctx context.Context, duration time.Duration, repo, database, deployment, environment, status string) {
	recordHistogram(ctx, "schemabot.apply.duration_seconds", duration.Seconds(),
		"Duration of apply operations (API call time)",
		attribute.String("repository", repo),
		attribute.String("database", database),
		DeploymentAttribute(deployment),
		EnvironmentAttribute(environment),
		attribute.String("status", status),
	)
}

// RecordRemoteDeploymentHealth records the latest observed health for a remote
// deployment/environment pair. A value of 1 means the latest health check
// succeeded; 0 means SchemaBot could not reach or validate the remote
// deployment.
func RecordRemoteDeploymentHealth(ctx context.Context, deployment, environment string, healthy bool) {
	value := int64(0)
	if healthy {
		value = 1
	}

	recordGauge(ctx, "schemabot.remote_deployment.health", value,
		"Latest remote deployment health check result", "1",
		DeploymentAttribute(deployment),
		EnvironmentAttribute(environment),
	)
}

var knownRemoteDeploymentHealthCheckStatuses = map[string]bool{
	"success": true,
	"error":   true,
}

var knownRemoteDeploymentHealthCheckReasons = map[string]bool{
	"healthy":             true,
	"client_config_error": true,
	"timeout":             true,
	"unavailable":         true,
}

// RecordRemoteDeploymentHealthCheck increments health check attempts for remote
// deployments. Status and reason are allowlisted to keep metric cardinality
// bounded.
func RecordRemoteDeploymentHealthCheck(ctx context.Context, deployment, environment, status, reason string) {
	if !knownRemoteDeploymentHealthCheckStatuses[status] {
		status = "error"
	}
	if !knownRemoteDeploymentHealthCheckReasons[reason] {
		reason = "unavailable"
	}

	addCounter(ctx, "schemabot.remote_deployment.health_checks_total",
		"Total number of remote deployment health checks", "{check}",
		DeploymentAttribute(deployment),
		EnvironmentAttribute(environment),
		attribute.String("status", status),
		attribute.String("reason", reason),
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
func RecordSchemaFreshnessRejected(ctx context.Context, action, environment string) {
	if !knownSchemaFreshnessActions[action] {
		action = "unknown"
	}
	addCounter(ctx, "schemabot.schema_freshness.rejected.total",
		"Plan/apply/apply-confirm rejected because PR HEAD advanced after discovery loaded schema files", "{rejection}",
		attribute.String("action", action),
		EnvironmentAttribute(environment),
	)
}

// RecordStalePlanRejected increments the counter for apply-confirm commands
// rejected because the stored plan was rendered against a commit that is no
// longer the PR HEAD (the cross-delivery race: HEAD advanced between the
// confirmation plan being posted and the user clicking apply-confirm).
//
// Distinct from RecordSchemaFreshnessRejected: the schema-freshness metric
// fires when discovery loses a race within one webhook delivery. This metric
// fires when the user-approved plan itself has been outpaced by new commits
// across deliveries. A spike here indicates humans pushing aggressively
// during PR review; sustained activity suggests reviewers need a tighter
// "freeze the branch" workflow during apply confirmation.
func RecordStalePlanRejected(ctx context.Context, environment string) {
	addCounter(ctx, "schemabot.command.rejected_stale_plan.total",
		"apply-confirm rejected because PR HEAD advanced after the confirmation plan was posted", "{rejection}",
		attribute.String("action", "apply_confirm"),
		EnvironmentAttribute(environment),
	)
}

// RecordBaseSchemaFreshnessRejected increments the counter for apply commands
// rejected by the path-scoped base freshness gate. outcome distinguishes a
// confirmed stale schema directory from GitHub uncertainty that failed closed.
func RecordBaseSchemaFreshnessRejected(ctx context.Context, action, environment, outcome string) {
	if !knownSchemaFreshnessActions[action] {
		action = "unknown"
	}
	addCounter(ctx, "schemabot.command.rejected_base_schema_freshness.total",
		"Apply rejected because the base branch schema path changed after divergence or could not be verified", "{rejection}",
		attribute.String("action", action),
		EnvironmentAttribute(environment),
		attribute.String("outcome", outcome),
	)
}

// RecordTransientPlanRetry increments the counter for webhook plan retries
// after transient remote deployment unavailability. A spike with
// outcome="exhausted" for one environment means the network path to that
// remote deployment is down rather than flapping — investigate the
// connectivity between this server and the deployment.
func RecordTransientPlanRetry(ctx context.Context, database, environment, outcome string) {
	addCounter(ctx, "schemabot.webhook.plan_transient_retry.total",
		"webhook plan retries after transient remote deployment unavailability", "{retry}",
		attribute.String("database", database),
		EnvironmentAttribute(environment),
		attribute.String("outcome", outcome),
	)
}

var knownReviewDriftClassifications = map[string]bool{
	"match":    true,
	"diverged": true,
	"errored":  true,
}

// RecordReviewDrift increments the counter for a deployment's review-time drift
// classification against the reviewed primary plan. A spike with
// classification="diverged" means a deployment's live schema no longer matches
// what was reviewed — an operator must reconcile that deployment before the PR
// can apply. classification="errored" means the deployment could not be diffed
// or compared and is failing the check closed; investigate connectivity to that
// deployment or the plan input.
func RecordReviewDrift(ctx context.Context, database, environment, deployment, classification string) {
	if !knownReviewDriftClassifications[classification] {
		// An unrecognized classification is a coding gap, not a drift signal.
		// Coercing it to "errored" would inflate the blocking-failure count, so
		// bucket it as "unknown" and keep the real failure classes accurate.
		classification = "unknown"
	}
	addCounter(ctx, "schemabot.review_drift.total",
		"review-time per-deployment drift classifications against the reviewed primary plan", "{deployment}",
		attribute.String("database", database),
		EnvironmentAttribute(environment),
		attribute.String("deployment", deployment),
		attribute.String("classification", classification),
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
	addCounter(ctx, "schemabot.source_policy.blocks_total",
		"Total trusted-source plan/apply requests blocked by source policy", "{block}",
		attribute.String("operation", operation),
		attribute.String("database", database),
		EnvironmentAttribute(environment),
		attribute.String("reason", reason),
	)
}

// Scope values for RecordStorageSchemaDestructiveRefusal: whether the whole
// statement was refused or only the destructive clauses split out of a mixed
// ALTER (whose safe clauses still executed).
const (
	StorageSchemaRefusalWhole = "whole"
	StorageSchemaRefusalSplit = "split"
)

// RecordStorageSchemaDestructiveRefusal increments the counter for destructive
// storage-schema DDL statements EnsureSchema refused to execute at startup.
// A nonzero rate means a starting binary's embedded schema no longer declares
// a table or column that exists in the storage database — expected briefly
// from older pods during a rolling deploy or rollback. The scope attribute
// says whether the safe clauses of the statement still ran: "split" means a
// mixed ALTER executed its safe clauses and refused only the destructive
// remainder; "whole" means nothing in the statement ran. Operator action: if
// the removal is intended and every pod runs a binary without the table or
// column, set storage.allow_destructive_schema_changes to true for one
// deploy; otherwise investigate which binary is starting against newer
// storage state.
func RecordStorageSchemaDestructiveRefusal(ctx context.Context, table, operation, scope string) {
	addCounter(ctx, "schemabot.storage_schema.destructive_refusals_total",
		"Total destructive storage-schema DDL statements refused by EnsureSchema", "{statement}",
		attribute.String("table", table),
		attribute.String("operation", operation),
		attribute.String("scope", scope),
		// The storage-schema bootstrap precedes any schema change
		// environment, so the counter carries the canonical unknown value.
		EnvironmentAttribute(""),
	)
}

var knownPRCommandActorAuthCommands = map[string]bool{
	"apply":            true,
	"apply_confirm":    true,
	"rollback":         true,
	"rollback_confirm": true,
	"unlock":           true,
	"cutover":          true,
	"stop":             true,
	"cancel":           true,
	"start":            true,
	"revert":           true,
	"skip_revert":      true,
}

var knownPRCommandActorAuthStatuses = map[string]bool{
	"allowed": true,
	"denied":  true,
	"error":   true,
	"skipped": true,
}

var knownPRCommandActorAuthReasons = map[string]bool{
	"disabled":                true,
	"allowed_admin_team":      true,
	"allowed_admin_user":      true,
	"allowed_operator_team":   true,
	"allowed_operator_user":   true,
	"missing_actor":           true,
	"missing_server_config":   true,
	"missing_database_config": true,
	"no_configured_principal": true,
	"not_authorized":          true,
	"github_error":            true,
	"unknown":                 true,
}

// RecordPRCommandActorAuthorization increments the counter for GitHub PR
// comment actor authorization decisions. Command, status, and reason are
// allowlisted to keep metric cardinality bounded.
func RecordPRCommandActorAuthorization(ctx context.Context, command, database, environment, repository, status, reason string) {
	if !knownPRCommandActorAuthCommands[command] {
		command = "unknown"
	}
	if !knownPRCommandActorAuthStatuses[status] {
		status = "unknown"
	}
	if !knownPRCommandActorAuthReasons[reason] {
		reason = "unknown"
	}

	addCounter(ctx, "schemabot.pr_command_actor_authorization.total",
		"Total GitHub PR command actor authorization decisions", "{decision}",
		attribute.String("command", command),
		attribute.String("database", database),
		EnvironmentAttribute(environment),
		attribute.String("repository", repository),
		attribute.String("status", status),
		attribute.String("reason", reason),
	)
}

var knownDirectWriteAuthOperations = map[string]bool{
	"plan":               true,
	"apply":              true,
	"rollback_plan":      true,
	"stop":               true,
	"start":              true,
	"cutover":            true,
	"cancel":             true,
	"release":            true,
	"revert":             true,
	"skip_revert":        true,
	"lock_acquire":       true,
	"lock_release":       true,
	"lock_force_release": true,
	"checks_scan":        true,
	"checks_synthesize":  true,
	"checks_repos":       true,
	"webhook_redrive":    true,
	"settings_set":       true,
}

var knownDirectWriteAuthStatuses = map[string]bool{
	"allowed": true,
	"denied":  true,
	"skipped": true,
}

var knownDirectWriteAuthReasons = map[string]bool{
	"scoped_lane_disabled":    true,
	"admin_allow":             true,
	"scoped_allow":            true,
	"missing_identity":        true,
	"not_admin":               true,
	"not_database_operator":   true,
	"environment_not_allowed": true,
	"missing_database_config": true,
	"unknown":                 true,
}

// RecordDirectWriteAuthorization increments the counter for per-database
// direct-write (CLI/API) authorization decisions made at the handler layer,
// after the forward-auth middleware has admitted the caller to the write tier.
// A spike in denied decisions means scoped operators are attempting operations
// outside their grant — check the reason attribute to see whether the target
// database, the environment, or the group membership is what mismatched.
// Operation, status, and reason are allowlisted here; database and
// environment can arrive from request bodies, so callers bound them to
// configured names before recording. Every attribute stays low-cardinality
// even when a request names a database or environment that does not exist.
func RecordDirectWriteAuthorization(ctx context.Context, operation, database, environment, status, reason string) {
	if !knownDirectWriteAuthOperations[operation] {
		operation = "unknown"
	}
	if !knownDirectWriteAuthStatuses[status] {
		status = "unknown"
	}
	if !knownDirectWriteAuthReasons[reason] {
		reason = "unknown"
	}
	// Admin-gated operations have no single target database, and some denials
	// happen before the target resolves; record the same sentinel the
	// environment attribute uses so empty never appears as a blank label.
	if database == "" {
		database = "unknown"
	}

	addCounter(ctx, "schemabot.direct_write_authorization.total",
		"Total per-database direct write authorization decisions", "{decision}",
		attribute.String("operation", operation),
		attribute.String("database", database),
		EnvironmentAttribute(environment),
		attribute.String("status", status),
		attribute.String("reason", reason),
	)
}

// RecordAuthDecision increments the counter for API auth decisions on the
// direct request path (every API authorizer, including the allow-all one, so
// unauthenticated deployments still have an alertable write signal). Labels
// are inherently low-cardinality: tier is read/write, decision is allow/deny,
// reason is a fixed set.
func RecordAuthDecision(ctx context.Context, tier, decision, reason string) {
	addCounter(ctx, "schemabot.auth_decisions.total",
		"Total API auth decisions on the direct request path", "{decision}",
		attribute.String("tier", tier),
		attribute.String("decision", decision),
		attribute.String("reason", reason),
	)
}

// RecordRateLimitDecision increments the counter for request-budget decisions
// on the API's rate-limited endpoints. Both outcomes are counted, so the
// limited rate has a denominator and an operator can see a client approaching
// its budget before it is turned away.
//
// Labels are deliberately fixed sets: endpoint is the route, scope is which
// budget was consulted (the caller's or the target's), decision is allow/limit,
// and environment is the target environment of the request. The caller identity
// and the target database are the useful triage details but there are hundreds
// of each, so they belong in the log line that accompanies a limited request,
// not in metric attributes.
func RecordRateLimitDecision(ctx context.Context, endpoint, scope, decision, environment string) {
	addCounter(ctx, "schemabot.rate_limit_decisions.total",
		"Total API request-budget decisions on rate-limited endpoints", "{decision}",
		attribute.String("endpoint", endpoint),
		attribute.String("scope", scope),
		attribute.String("decision", decision),
		EnvironmentAttribute(environment),
	)
}

// knownCheckOwnershipOperations limits metric cardinality to expected check
// ownership miss paths.
var knownCheckOwnershipOperations = map[string]bool{
	"apply_finished":           true,
	"apply_cancelled_finished": true,
	"rollback_finished":        true,
}

// RecordCheckOwnershipMiss increments the counter for guarded check updates
// that did not apply because stored check state no longer belonged to the
// apply being completed.
func RecordCheckOwnershipMiss(ctx context.Context, operation, repository, database, databaseType, deployment, environment string) {
	if !knownCheckOwnershipOperations[operation] {
		operation = "unknown"
	}
	addCounter(ctx, "schemabot.check_ownership_misses_total",
		"Total stored check state ownership misses", "{miss}",
		attribute.String("operation", operation),
		attribute.String("repository", repository),
		attribute.String("database", database),
		attribute.String("database_type", databaseType),
		DeploymentAttribute(deployment),
		EnvironmentAttribute(environment),
	)
}

// Gates that evaluate check run trust, used as the gate attribute on
// RecordUntrustedAggregateNamedCheck.
const (
	// CheckTrustGatePassingChecks is the require_passing_checks CI-health gate.
	CheckTrustGatePassingChecks = "passing_checks"
	// CheckTrustGatePromotion is the prior-environment promotion gate.
	CheckTrustGatePromotion = "promotion"
	// CheckTrustGateLeaderFold is the cross-tenant aggregate-leader fold gate,
	// where the leader reads a participant deployment's aggregate Check Run and
	// folds it into the shared aggregate.
	CheckTrustGateLeaderFold = "leader_fold"
)

// RecordUntrustedAggregateNamedCheck counts PR checks that carry a SchemaBot
// aggregate Check Run name but were created by an untrusted GitHub App. A
// sustained spike means either a check-name spoof attempt or a sibling
// SchemaBot deployment missing from trusted-check-app-slugs; operators should
// inspect the app_slug attribute and the matching warn logs to tell which.
// The counter is an identity-mismatch signal, not a blocking signal: it fires
// whenever such a check is observed by a gate, even when required_checks
// narrowing keeps that check from gating applies — the warn log states the
// actual gating impact. The gate attribute distinguishes where the untrusted
// check surfaced: the passing-checks gate or the promotion gate (where it
// cannot verify the prior environment).
func RecordUntrustedAggregateNamedCheck(ctx context.Context, repository, environment, appSlug, gate string) {
	addCounter(ctx, "schemabot.untrusted_aggregate_named_checks_total",
		"Total PR checks with a SchemaBot aggregate name from an untrusted GitHub App", "{check}",
		attribute.String("repository", repository),
		EnvironmentAttribute(environment),
		attribute.String("app_slug", appSlug),
		attribute.String("gate", gate),
	)
}

// Outcomes for RecordLeaderParticipantGate. Each names why the
// aggregate leader resolved a participant's Check Run to the given state when
// folding it into the shared aggregate.
const (
	// LeaderParticipantGateSuccess: the participant's Check Run was trusted,
	// completed, and successful — it does not block the aggregate.
	LeaderParticipantGateSuccess = "success"
	// LeaderParticipantGateFailure: the participant's Check Run completed with a
	// failing conclusion — the aggregate fails.
	LeaderParticipantGateFailure = "failure"
	// LeaderParticipantGatePending: the participant's Check Run completed with
	// action_required — the participant has pending work (typically an apply
	// awaiting an operator) and the aggregate blocks as pending.
	LeaderParticipantGatePending = "pending"
	// LeaderParticipantGateInProgress: the participant's Check Run is not yet
	// terminal — the aggregate stays in progress.
	LeaderParticipantGateInProgress = "in_progress"
	// LeaderParticipantGateMissing: no trusted participant Check Run exists on the
	// commit — the aggregate blocks until the participant reports.
	LeaderParticipantGateMissing = "missing"
	// LeaderParticipantGateUntrusted: a same-named Check Run exists but only from
	// untrusted GitHub Apps — the aggregate blocks.
	LeaderParticipantGateUntrusted = "untrusted"
	// LeaderParticipantGateError: the GitHub Checks API lookup failed or the
	// participant's check name could not be resolved — the aggregate blocks.
	LeaderParticipantGateError = "error"
)

// RecordLeaderParticipantGate counts, per outcome, how the aggregate leader
// resolved each expected participant's Check Run when folding it into the shared
// aggregate. Every outcome other than "success" blocks the aggregate. A spike in
// "missing", "untrusted", or "error" points an operator at a participant
// deployment that is not reporting, a trusted-check-app-slugs gap or check-name
// spoof, or a GitHub API problem, respectively — the matching warn/error logs
// carry the repo, PR, environment, head SHA, and check name.
func RecordLeaderParticipantGate(ctx context.Context, repository, environment, tenant, outcome string) {
	addCounter(ctx, "schemabot.leader_participant_gate_total",
		"Total participant Check Run resolutions folded into the aggregate leader's check, by outcome", "{gate}",
		attribute.String("repository", repository),
		EnvironmentAttribute(environment),
		attribute.String("tenant", tenant),
		attribute.String("outcome", outcome),
	)
}

// RecordPromotionConfigErrorBlock counts applies blocked by the staging-first
// promotion gate because the target environment is absent from the configured
// promotion order on a scoped SchemaBot instance. This fires only on operator
// misconfiguration: the gate cannot identify the target's prior environments,
// so it fails closed. A non-zero count means an operator must add the
// environment to the database's effective promotion order (its
// environment_order override when set, otherwise the server-wide
// environment_order); the matching warn log carries the promotion_order so
// they can see what is configured.
func RecordPromotionConfigErrorBlock(ctx context.Context, repository, database, environment string) {
	addCounter(ctx, "schemabot.promotion.config_error_blocks_total",
		"Total applies blocked because the target environment is absent from the configured promotion order", "{block}",
		attribute.String("repository", repository),
		attribute.String("database", database),
		EnvironmentAttribute(environment),
	)
}

// AdjustActiveApplies increments or decrements the active applies gauge.
// Use delta=1 when an apply is accepted and delta=-1 when it reaches a terminal state.
func AdjustActiveApplies(ctx context.Context, delta int64, database, deployment, environment string) {
	addUpDownCounter(ctx, "schemabot.active_applies", delta,
		"Number of currently in-progress applies", "{apply}",
		attribute.String("database", database),
		DeploymentAttribute(deployment),
		EnvironmentAttribute(environment),
	)
}

// knownControlOperations limits metric cardinality to expected control operations.
var knownControlOperations = map[string]bool{
	"cutover":       true,
	"stop":          true,
	"cancel":        true,
	"start":         true,
	"revert":        true,
	"skip_revert":   true,
	"release":       true,
	"rollback_plan": true,
}

// RecordControlOperation increments the control operations counter.
// Operation should be one of: cutover, stop, start, revert, skip_revert, release, rollback_plan.
// Status should be "success" or "error".
func RecordControlOperation(ctx context.Context, operation, database, deployment, environment, status string) {
	if !knownControlOperations[operation] {
		operation = "unknown"
	}
	addCounter(ctx, "schemabot.control_operations_total",
		"Total number of control operations (cutover, stop, start, etc.)", "{operation}",
		attribute.String("operation", operation),
		attribute.String("database", database),
		DeploymentAttribute(deployment),
		EnvironmentAttribute(environment),
		attribute.String("status", status),
	)
}

// RecordRemoteControlRequestStale counts retransmissions of a durable
// stop/cancel control request that the data plane accepted but has not
// consumed within the stale threshold. A non-zero rate means an accepted
// operator command is not taking effect on the remote apply — the data plane's
// own driver is failing to consume it (its logs carry the failing consume
// error) — and the apply will not converge until that is resolved.
func RecordRemoteControlRequestStale(ctx context.Context, operation, database, deployment, environment string) {
	if !knownControlOperations[operation] {
		operation = "unknown"
	}
	addCounter(ctx, "schemabot.remote_control_requests.stale_resends_total",
		"Total retransmissions of remote control requests still unconsumed by the data plane past the stale threshold", "{resend}",
		attribute.String("operation", operation),
		attribute.String("database", database),
		DeploymentAttribute(deployment),
		EnvironmentAttribute(environment),
	)
}

// RecordRemoteControlRequestRejected counts control requests the data plane
// accepted and its own driver then failed — the operator was told the command
// was queued and the effect never landed. A non-zero rate names the operation
// and engine that is refusing operator commands: chart it by operation to see
// which control surface is unsupported or broken on that engine, and read the
// apply log entry recorded alongside it for the engine's own reason.
func RecordRemoteControlRequestRejected(ctx context.Context, operation, engine, database, deployment, environment string) {
	if !knownControlOperations[operation] {
		operation = "unknown"
	}
	addCounter(ctx, "schemabot.remote_control_requests.rejected_total",
		"Total remote control requests the data plane accepted and then failed", "{rejection}",
		attribute.String("operation", operation),
		attribute.String("engine", engine),
		attribute.String("database", database),
		DeploymentAttribute(deployment),
		EnvironmentAttribute(environment),
	)
}

// RecordControlRequestUnsupportedDecline counts durable control requests
// resolved terminally because the engine does not support the operation for
// its database type (e.g. stop or cancel against a PostgreSQL apply, whose
// statements commit or fail on their own). The decline itself is working as
// designed — the request is failed with the engine's reason and the schema
// change settles on its own — so a count here is an operator issuing a
// command the engine can never honor. A sustained rate on one operation means
// operators keep reaching for a control surface that does not exist on that
// engine: improve the operator-facing guidance for it rather than chasing a
// fault.
func RecordControlRequestUnsupportedDecline(ctx context.Context, operation, engine, database, deployment, environment string) {
	if !knownControlOperations[operation] {
		operation = "unknown"
	}
	addCounter(ctx, "schemabot.control.unsupported_declines_total",
		"Total durable control requests resolved terminally because the engine does not support the operation", "{decline}",
		attribute.String("operation", operation),
		attribute.String("engine", engine),
		attribute.String("database", database),
		DeploymentAttribute(deployment),
		EnvironmentAttribute(environment),
	)
}

// RecordTasklessSettleDeferred counts task-less stop/cancel settles that lost
// the parent apply lease mid-drive and settled only the drive's own leased
// operation row, deferring the apply row to the operator's state projection.
// The deferral itself converges (the projection derives the apply state from
// the settled rows and completes the pending request), so an occasional count
// is benign lease churn. A sustained rate means drives are routinely outliving
// their parent apply lease — check apply-lease heartbeating and claim
// contention on the affected database before the churn hits tasked drives too.
func RecordTasklessSettleDeferred(ctx context.Context, operation, database, deployment, environment string) {
	if !knownControlOperations[operation] {
		operation = "unknown"
	}
	addCounter(ctx, "schemabot.control.taskless_settle_deferrals_total",
		"Total task-less stop/cancel settles that lost the apply lease and deferred the apply row to the state projection", "{settle}",
		attribute.String("operation", operation),
		attribute.String("database", database),
		DeploymentAttribute(deployment),
		EnvironmentAttribute(environment),
	)
}

// RecordEngineTerminalTruthReconcile counts drive claims that read the
// engine's authoritative state before consuming a pending stop/cancel control
// request. Outcomes:
//   - "adopted_completed" / "adopted_failed" / "adopted_cancelled" /
//     "adopted_reverted": the engine's change was already terminal, so the
//     drive settled stored state to that outcome and mooted the pending
//     command instead of running it. A sustained rate means operator commands
//     are routinely losing the race with the engine — check why commands are
//     queued so late (e.g. webhook delivery lag, slow claim turnaround).
//   - "progress_error": the authoritative read failed and the drive fell back
//     to consuming the pending command as before. A sustained rate means the
//     drive cannot see the engine's backend — check engine API connectivity
//     and the persisted engine resume state for the apply in the drive logs.
func RecordEngineTerminalTruthReconcile(ctx context.Context, database, deployment, environment, outcome string) {
	addCounter(ctx, "schemabot.operator.engine_terminal_truth_reconciles_total",
		"Total drive claims that read the engine's authoritative state before consuming a pending stop/cancel command", "{reconcile}",
		attribute.String("database", database),
		DeploymentAttribute(deployment),
		EnvironmentAttribute(environment),
		attribute.String("outcome", outcome),
	)
}

// RecordConflictCheckOwnershipBlock counts conflict-check decisions that kept
// a non-terminal task blocking its database because this process could not
// establish that no driver will reach the task on its own — either the apply's
// lease says this process's engine memory is not authoritative for it, or a
// driver is still on its way to it. Reasons:
//   - "fresh_lease": a live driver holds the apply's lease, so the local
//     engine probe was skipped and the live drive stays authoritative. A
//     sustained rate means new applies are repeatedly dispatched against a
//     target that already has actively driven work — check who is submitting
//     the duplicates.
//   - "foreign_terminal_report": the lease is stale and this process's engine
//     memory reports terminal, but the lease was last held by another process,
//     so the report was refused. Driver stale-claim recovery settles the task;
//     investigate if the same task repeats here without converging.
//   - "pending_control_request": a stopped task's apply carries an operator
//     command a driver has not delivered yet, so the task still holds its
//     database. A sustained rate means commands are queued but not being
//     drained — check that drivers are claiming on this deployment.
//   - "control_request_unreadable": the control requests of a stopped task's
//     apply could not be read, so the task kept blocking rather than being
//     released on an unproven assumption. Any sustained rate is a storage
//     problem, not a workload one.
func RecordConflictCheckOwnershipBlock(ctx context.Context, database, databaseType, reason string) {
	addCounter(ctx, "schemabot.conflict_check.ownership_blocks_total",
		"Total conflict-check decisions that kept a task blocking because no driver could be ruled out for it", "{block}",
		attribute.String("database", database),
		attribute.String("database_type", databaseType),
		attribute.String("reason", reason),
	)
}

// RecordPlanetScaleUnclassifiedCancelRejection counts PlanetScale cancel
// rejections whose live deployment state has no explicit classification. Every
// known deployment state is classified explicitly; a hit here means PlanetScale
// introduced a new deployment state (or returned one SchemaBot has never
// mapped), and the rejection fell back to a plain retryable error. Add an
// explicit classification for the state — until then the durable stop/cancel
// request may be retried against a rejection that can never succeed, and only
// the drive's terminal-truth reconcile can converge it.
func RecordPlanetScaleUnclassifiedCancelRejection(ctx context.Context, database, deploymentState string) {
	addCounter(ctx, "schemabot.engine.planetscale.unclassified_cancel_rejections_total",
		"Total PlanetScale cancel rejections observed in a deployment state with no explicit classification", "{rejection}",
		attribute.String("database", database),
		attribute.String("deployment_state", deploymentState),
	)
}

// RecordUnrecognizedEngineTaskStatus counts engine- or data-plane-reported
// task statuses that have no mapping in pkg/state. An unknown status
// normalizes to Running so the work stays visible and blocking, but that
// fallback is a guess: every surface renders the affected work as Running
// regardless of what the engine is actually doing. A sustained rate here means
// an engine or data-plane version introduced a status SchemaBot cannot
// classify — add an explicit mapping in pkg/state.
//
// Which status went unmapped is deliberately not an attribute. The value is
// engine-controlled text, so a status carrying variable data — a host name, an
// identifier, an error tail — would mint a series per sighting, and this
// counter fires only when there is a mapping gap: exactly the moment a
// cardinality explosion would land on top of an incident. The paired drive
// warn carries the raw status alongside the task identifiers, deduped per task
// and status, so triage reads the status from the log while the counter stays
// alertable on a bounded set of dimensions.
func RecordUnrecognizedEngineTaskStatus(ctx context.Context, database, databaseType, engineName, environment string) {
	addCounter(ctx, "schemabot.engine.unrecognized_task_status_total",
		"Total engine- or data-plane-reported task statuses with no task-state mapping, rendered as Running by the fail-open default", "{status}",
		attribute.String("database", database),
		attribute.String("database_type", databaseType),
		attribute.String("engine", engineName),
		EnvironmentAttribute(environment),
	)
}

// RecordLockOperation increments the lock operations counter.
// Operation should be "acquire" or "release".
// Status should be "success", "conflict", "not_found", "not_owned", or "error".
func RecordLockOperation(ctx context.Context, operation, database, status string) {
	addCounter(ctx, "schemabot.lock_operations_total",
		"Total number of lock acquire/release operations", "{operation}",
		attribute.String("operation", operation),
		attribute.String("database", database),
		EnvironmentAttribute(""),
		attribute.String("status", status),
	)
}

// knownRemoteApplyDedupOutcomes limits metric cardinality to the expected
// idempotency-keyed dispatch dedup outcomes.
var knownRemoteApplyDedupOutcomes = map[string]bool{
	"hit":                   true,
	"conflict_race":         true,
	"create_race":           true,
	"key_collision_refused": true,
}

// RecordRemoteApplyDedup increments the counter for idempotency-keyed remote
// apply dispatch dedup outcomes. Outcome should be one of:
//   - "hit": a re-dispatch of the same generation returned the existing apply
//     instead of starting a duplicate (the ambiguous-reclaim event an operator
//     wants to see spike during an incident).
//   - "conflict_race" / "create_race": a concurrent same-key dispatch was
//     resolved to the winning apply instead of a spurious rejection or error.
//   - "key_collision_refused": a stored apply's environment/database/type
//     disagreed with the request, so the dispatch was refused fail-closed
//     (a safety event worth investigating).
func RecordRemoteApplyDedup(ctx context.Context, database, environment, outcome string) {
	if !knownRemoteApplyDedupOutcomes[outcome] {
		outcome = "unknown"
	}
	addCounter(ctx, "schemabot.remote_apply_dedup_total",
		"Total idempotency-keyed remote apply dispatch dedup outcomes", "{dispatch}",
		attribute.String("database", database),
		EnvironmentAttribute(environment),
		attribute.String("outcome", outcome),
	)
}

var knownRemoteApplyAttachOutcomes = map[string]bool{
	"attached":         true,
	"attach_race":      true,
	"terminal_refused": true,
	"manifest_refused": true,
	"adopted":          true,
}

// RecordRemoteApplyAttach increments the counter for dispatches that resolved
// into an existing deployment-keyed apply with a new operation. Outcome should
// be one of:
//   - "attached": a sibling dispatch added its operation to the deployment's
//     shared apply (the normal fan-out event; its rate tracks sharded
//     dispatch volume).
//   - "attach_race": a concurrent same-operation attach lost the insert and
//     was resolved to the winner's row instead of a spurious error.
//   - "terminal_refused": the shared apply was already terminal, so the
//     attach was refused fail-closed — the deployment's remaining operations
//     cannot dispatch until an operator reconciles the apply, so investigate
//     what terminalized it mid-fan-out.
//   - "manifest_refused": the dispatch's operation key is not in the apply's
//     stored generation manifest, so the attach was refused fail-closed — the
//     two planes disagree about the generation's operation set (version or
//     data skew), so compare the dispatcher's operation rows against the
//     stored manifest before retrying.
//   - "adopted": a dispatch resolved into the live apply already running its
//     exact change set instead of being refused by it. A steady rate means
//     applies are routinely outliving the identity that started them —
//     investigate what is terminalizing them while their work continues.
func RecordRemoteApplyAttach(ctx context.Context, database, environment, outcome string) {
	if !knownRemoteApplyAttachOutcomes[outcome] {
		outcome = "unknown"
	}
	addCounter(ctx, "schemabot.remote_apply_attach_total",
		"Total dispatches attaching an operation to an existing deployment-keyed remote apply", "{dispatch}",
		attribute.String("database", database),
		EnvironmentAttribute(environment),
		attribute.String("outcome", outcome),
	)
}

// RecordApplyManifestHold increments the counter for state-projection passes
// that held an apply's success verdict because operations declared in its
// generation manifest have not attached yet. A short-lived burst is the normal
// fan-out window (siblings dispatch one at a time). A sustained series on one
// apply means the dispatcher stopped sending the rest of the generation — its
// driver died or its control plane is wedged — so the operator action is to
// check the dispatcher's operation rows for that deployment and cancel the
// held apply if the generation is abandoned.
func RecordApplyManifestHold(ctx context.Context, database, deployment, environment string) {
	addCounter(ctx, "schemabot.apply_manifest_hold_total",
		"Total apply state projections held open awaiting manifest operations that have not attached", "{projection}",
		attribute.String("database", database),
		attribute.String("deployment", deployment),
		EnvironmentAttribute(environment),
	)
}

// RecordRemoteApplyKeyEchoMismatch increments the counter for remote dispatches
// whose accepted response echoed a different operation key than the request's
// shape derives to, so the control plane refused the response's remote ids and
// failed the dispatch closed. A spike means a data plane is answering
// deployment-keyed dispatches without attaching sibling operations — most often
// a data plane running a version that predates sibling-operation attach —
// so the operator action is to upgrade (or roll back) the named database's data
// plane before retrying the blocked applies.
func RecordRemoteApplyKeyEchoMismatch(ctx context.Context, database, environment string) {
	addCounter(ctx, "schemabot.remote_apply_key_echo_mismatch_total",
		"Total remote apply dispatches refused because the response's operation key did not match the dispatched operation", "{dispatch}",
		attribute.String("database", database),
		EnvironmentAttribute(environment),
	)
}

// RecordRemoteApplyDeploymentIDConflict increments the counter for remote
// dispatch results refused because the deployment already correlates to a
// different remote apply id. One deployment maps to exactly one data-plane
// apply, so a second id means the planes have diverged — an in-flight apply
// spanning a dispatch-key rollout, or a data plane that lost its keyed apply
// and minted a fresh one. The refusal fails the dispatch closed; the operator
// action is to inspect the named database's apply operations, decide which
// remote apply is authoritative, and re-dispatch under a fresh generation once
// the planes agree.
func RecordRemoteApplyDeploymentIDConflict(ctx context.Context, database, environment, deployment string) {
	addCounter(ctx, "schemabot.remote_apply_deployment_id_conflict_total",
		"Total remote apply dispatches refused because the deployment already correlates to a different remote apply id", "{dispatch}",
		attribute.String("database", database),
		EnvironmentAttribute(environment),
		attribute.String("deployment", deployment),
	)
}

// operatorMetricNames returns the canonical operator metric name alongside its
// deprecated schemabot.scheduler.* alias. Both are emitted for one release so
// dashboards and alerts can migrate before the legacy series is removed.
func operatorMetricNames(suffix string) [2]string {
	return [2]string{"schemabot.operator." + suffix, "schemabot.scheduler." + suffix}
}

// addOperatorCounter increments an operator counter and its deprecated
// scheduler-named alias by one with the same attributes.
func addOperatorCounter(ctx context.Context, suffix, description, unit string, attrs ...attribute.KeyValue) {
	for _, name := range operatorMetricNames(suffix) {
		addCounter(ctx, name, description, unit, attrs...)
	}
}

// RecordOperatorResume increments the operator resumed counter when an apply is
// successfully claimed and resumed.
func RecordOperatorResume(ctx context.Context, database, deployment, environment, previousState string) {
	addOperatorCounter(ctx, "resumed_total",
		"Total number of applies resumed by the operator", "{apply}",
		attribute.String("database", database),
		DeploymentAttribute(deployment),
		EnvironmentAttribute(environment),
		attribute.String("previous_state", previousState),
	)
}

// RecordOperatorResumeFailure increments the operator resume failure counter.
func RecordOperatorResumeFailure(ctx context.Context, database, deployment, environment, reason string) {
	addOperatorCounter(ctx, "resume_failures_total",
		"Total number of operator resume attempts that failed", "{apply}",
		attribute.String("database", database),
		DeploymentAttribute(deployment),
		EnvironmentAttribute(environment),
		attribute.String("reason", reason),
	)
}

// RecordOperatorStrandedOperationReaped counts apply operations the reaper
// settled from an already-settled parent apply — rows describing work that will
// never run, left behind non-terminal.
//
// A one-time burst is the historical backlog draining and needs no action. A
// rate that keeps climbing means something is actively stranding operations, so
// the investigation belongs on the producer — a path that terminalizes a parent
// apply without settling its children — not on the reaper.
//
// deployment is the reaped operation's own deployment, the routing key that says
// which target the settled row belonged to; stranded rows arise in exactly the
// multi-deployment applies where it differs from the parent's.
//
// This counter is emitted under the operator name only. The deprecated
// schemabot.scheduler.* alias exists to carry pre-existing series through one
// release, and a metric introduced after that rename has no legacy series to
// migrate.
func RecordOperatorStrandedOperationReaped(ctx context.Context, database, deployment, environment, parentState string) {
	addCounter(ctx, "schemabot.operator.stranded_operations_reaped_total",
		"Total number of stranded apply operations the reaper settled from their parent apply's outcome", "{operation}",
		attribute.String("database", database),
		DeploymentAttribute(deployment),
		EnvironmentAttribute(environment),
		attribute.String("parent_state", parentState),
	)
}

var knownOperatorClaimFailureReasons = map[string]bool{
	"expire_retryable_error":                   true,
	"stranded_reaper_error":                    true,
	"stranded_task_reaper_error":               true,
	"missing_lease_token":                      true,
	"operation_storage_error":                  true,
	"missing_operation_lease_token":            true,
	"operation_set_list_error":                 true,
	"operation_set_missing":                    true,
	"operation_task_inspect_error":             true,
	"operation_cutover_storage_error":          true,
	"missing_operation_cutover_lease_token":    true,
	"operation_cutover_set_list_error":         true,
	"operation_cutover_set_invalid":            true,
	"operation_parent_load_error":              true,
	"operation_parent_missing":                 true,
	"operation_parent_claim_error":             true,
	"operation_parent_not_claimable":           true,
	"operation_parent_release_error":           true,
	"operation_lease_release_error":            true,
	"operation_lease_recheck_error":            true,
	"operation_lease_recheck_missing":          true,
	"operation_lease_rotated":                  true,
	"operation_lease_released_by_peer":         true,
	"missing_operation_deployment":             true,
	"stop_reconciliation_claim_error":          true,
	"stop_reconciliation_missing_lease_token":  true,
	"operation_projection_claim_error":         true,
	"operation_projection_missing_lease_token": true,
}

// RecordOperatorClaimFailure increments the operator claim failure counter.
func RecordOperatorClaimFailure(ctx context.Context, reason string) {
	if !knownOperatorClaimFailureReasons[reason] {
		reason = "unknown"
	}
	addOperatorCounter(ctx, "claim_failures_total",
		"Total number of operator claim attempts that failed", "{attempt}",
		EnvironmentAttribute(""),
		attribute.String("reason", reason),
	)
}

// RecordOperatorShutdownHaltFailure counts engines that did not come down when
// this process shut down. The process stops renewing its applies' leases as it
// exits, so an engine still holding a target's lock will refuse every driver
// that reclaims that apply, and each refusal burns a recovery attempt until the
// apply is exhausted. A nonzero rate means shutdowns are leaving targets held:
// check the halt-failure logs for the endpoint, then whether the affected
// applies were reclaimed and refused.
func RecordOperatorShutdownHaltFailure(ctx context.Context) {
	addCounter(ctx, "schemabot.operator.shutdown_halt_failures",
		"Total number of engines that did not come down during operator shutdown", "{failure}",
		EnvironmentAttribute(""),
	)
}

// RecordOperatorStuckPendingApplies records how many pending applies are older
// than the stuck threshold while still carrying the child rows that make them
// claimable — work a driver should already have picked up. Apply creation
// rejects a concurrent apply for the same target rather than queuing it, so a
// nonzero value is not normal backpressure: it means the operator driver pool is
// not claiming (all drivers saturated on long-running applies, an
// operator-less/crash-looping deployment, or a wedged claim path). The expected
// operator action is to check driver liveness and claim-failure metrics/logs.
func RecordOperatorStuckPendingApplies(ctx context.Context, count int64) {
	recordGauge(ctx, "schemabot.operator.stuck_pending_applies", count,
		"Number of pending applies past the stuck threshold that a driver should have claimed", "{apply}",
		EnvironmentAttribute(""),
	)
}

// RecordOperatorStuckPendingScanFailure counts failed stuck-pending scans. The
// stuck-pending gauge is a last-value instrument, so a failed scan leaves it
// frozen at its last-good value — indistinguishable from a healthy operator.
// This counter is the liveness signal for the gauge: a nonzero rate means the
// stuck-pending value is stale and must not be trusted.
func RecordOperatorStuckPendingScanFailure(ctx context.Context) {
	addCounter(ctx, "schemabot.operator.stuck_pending_scan_failures",
		"Total number of failed operator stuck-pending apply scans", "{failure}",
		EnvironmentAttribute(""),
	)
}

// RecordOperatorDriverPoolSize records the size of this process's operator
// driver pool — how many drives it can hold at once. Together with
// schemabot.operator.drivers_busy it answers how much claim capacity remains:
// summed across processes, pool size minus busy is the number of drivers
// still free to claim queued work.
func RecordOperatorDriverPoolSize(ctx context.Context, size int64) {
	recordGauge(ctx, "schemabot.operator.driver_pool_size", size,
		"Size of the operator driver pool in this process", "{driver}",
		EnvironmentAttribute(""),
	)
}

// RecordOperatorDriversBusy records how many operator drivers in this process
// currently hold claimed work. A driver is busy from the moment its claim
// succeeds until the drive returns, so a long-running apply holds a driver for
// its full duration. When busy reaches the pool size on every process, newly
// queued applies wait for a drive to finish, and the stuck-pending gauge is the alarm
// that they waited too long. The count is process-local on purpose: a busy
// driver's occupancy dies with its process, and a peer recovers the work
// through stale-lease claims, so there is no durable truth to sample.
func RecordOperatorDriversBusy(ctx context.Context, busy int64) {
	recordGauge(ctx, "schemabot.operator.drivers_busy", busy,
		"Number of operator drivers currently holding claimed work", "{driver}",
		EnvironmentAttribute(""),
	)
}

// RecordOperatorOperationProjectionRepair counts applies the operator settled by
// re-deriving the parent from operation rows that had all already settled — a
// parent left behind its own children because a drive stopped between writing
// its terminal operation row and projecting the parent. Every increment is one
// target that was blocked for the lease staleness window by the one-active-apply
// guard, so a sustained rate means drives are dying mid-projection and the
// expected operator action is to look for driver crashes, evictions, or
// deploy-time terminations rather than at this repair path. derived_state names
// the verdict the repair landed on, so a spike that is all failed reads
// differently from one that is all completed.
func RecordOperatorOperationProjectionRepair(ctx context.Context, database, deployment, environment, derivedState string) {
	addOperatorCounter(ctx, "operation_projection_repairs_total",
		"Total number of applies settled by re-deriving the parent from already-settled operation rows", "{apply}",
		attribute.String("database", database),
		DeploymentAttribute(deployment),
		EnvironmentAttribute(environment),
		attribute.String("derived_state", derivedState),
	)
}

var knownOperatorTerminalSummaryFailureReasons = map[string]bool{
	"reload_apply_error":           true,
	"apply_missing":                true,
	"apply_not_terminal_after_cas": true,
	"reload_tasks_error":           true,
	"callback_error":               true,
}

// RecordOperatorTerminalSummaryFailure increments the counter for an apply
// whose aggregate terminal summary failed to publish after the projection CAS
// already terminalized the parent. A spike means terminal PR summaries / check
// refreshes are being dropped; the parent state itself is already durable, so
// the expected operator action is to check the GitHub side-effect path (App
// client, check state) and rely on summary reconciliation.
func RecordOperatorTerminalSummaryFailure(ctx context.Context, reason string) {
	if !knownOperatorTerminalSummaryFailureReasons[reason] {
		reason = "unknown"
	}
	addOperatorCounter(ctx, "terminal_summary_publish_failures_total",
		"Total number of aggregate terminal summary publishes that failed", "{apply}",
		EnvironmentAttribute(""),
		attribute.String("reason", reason),
	)
}

// knownRecoveredPanicOperations limits metric cardinality to the panic
// containment boundaries that exist in the codebase.
var knownRecoveredPanicOperations = map[string]bool{
	"apply_drive":            true,
	"operator_tick":          true,
	"summary_reconciliation": true,
	"observer_poll":          true,
	"grpc_handler":           true,
}

// RecordRecoveredPanic increments the recovered-panic counter for a background
// operation whose panic was contained instead of crashing the process. Any
// non-zero rate means a code or data bug is being converted into degraded work
// (a failed apply, a stopped poller, a skipped reconciliation pass). The paired
// error log carries the panic value, stack, and work identifiers, so the
// operator action is to find that log, fix the underlying fault, and reconcile
// the affected work.
func RecordRecoveredPanic(ctx context.Context, operation string) {
	if !knownRecoveredPanicOperations[operation] {
		operation = "unknown"
	}
	addCounter(ctx, "schemabot.panic.recovered_total",
		"Total number of panics recovered at background-work boundaries", "{panic}",
		EnvironmentAttribute(""),
		attribute.String("operation", operation),
	)
}

// RecordOperatorClaimDuration records how long it took to claim and resume an apply.
func RecordOperatorClaimDuration(ctx context.Context, duration time.Duration, database, deployment, environment, previousState string) {
	for _, name := range operatorMetricNames("claim_duration_seconds") {
		recordHistogram(ctx, name, duration.Seconds(),
			"Duration of operator claim + resume operations",
			attribute.String("database", database),
			DeploymentAttribute(deployment),
			EnvironmentAttribute(environment),
			attribute.String("previous_state", previousState),
		)
	}
}

// knownWebhookEvents limits metric cardinality to expected GitHub event types.
var knownWebhookEvents = map[string]bool{
	"check_suite":                 true,
	"create":                      true,
	"issues":                      true,
	"issue_comment":               true,
	"pull_request":                true,
	"pull_request_review":         true,
	"pull_request_review_comment": true,
	"check_run":                   true,
	"merge_group":                 true,
	"ping":                        true,
	"push":                        true,
}

// knownWebhookActions limits metric cardinality to expected GitHub webhook actions.
var knownWebhookActions = map[string]bool{
	"assigned":               true,
	"auto_merge_disabled":    true,
	"auto_merge_enabled":     true,
	"checks_requested":       true,
	"closed":                 true,
	"completed":              true,
	"converted_to_draft":     true,
	"created":                true,
	"deleted":                true,
	"demilestoned":           true,
	"dequeued":               true,
	"destroyed":              true,
	"dismissed":              true,
	"edited":                 true,
	"enqueued":               true,
	"labeled":                true,
	"locked":                 true,
	"milestoned":             true,
	"opened":                 true,
	"pinned":                 true,
	"ready_for_review":       true,
	"reopened":               true,
	"requested":              true,
	"rerequested":            true,
	"review_request_removed": true,
	"review_requested":       true,
	"submitted":              true,
	"synchronize":            true,
	"transferred":            true,
	"unassigned":             true,
	"unlabeled":              true,
	"unlocked":               true,
	"unpinned":               true,
	"":                       true, // events without actions (e.g., ping, push)
}

// RecordSchemaRequestError increments the schema request error counter.
// Reason should be a stable string: "database_not_found", "invalid_config",
// "no_config", "multiple_configs", or "unexpected".
func RecordSchemaRequestError(ctx context.Context, repo, command, database, environment, reason string) {
	addCounter(ctx, "schemabot.schema_request.errors_total",
		"Schema request errors by reason", "{error}",
		attribute.String("repository", repo),
		attribute.String("command", command),
		attribute.String("database", database),
		EnvironmentAttribute(environment),
		attribute.String("reason", reason),
	)
}

const (
	gitHubMetricValueUnknown = "unknown"

	GitHubOperationAddCommentReaction            = "add_comment_reaction"
	GitHubOperationCreateCheckRun                = "create_check_run"
	GitHubOperationCreateIssueComment            = "create_issue_comment"
	GitHubOperationCreateInstallationAccessToken = "create_installation_access_token"
	GitHubOperationDeleteIssueComment            = "delete_issue_comment"
	GitHubOperationEditIssueComment              = "edit_issue_comment"
	GitHubOperationFetchAppSlug                  = "fetch_app_slug"
	GitHubOperationFetchBlob                     = "fetch_blob"
	GitHubOperationFetchFileContent              = "fetch_file_content"
	GitHubOperationFetchGitTree                  = "fetch_git_tree"
	GitHubOperationFetchPullRequest              = "fetch_pull_request"
	GitHubOperationGetCombinedStatus             = "get_combined_status"
	GitHubOperationGetTeamMembership             = "get_team_membership"
	GitHubOperationGraphQLMinimizeComment        = "graphql_minimize_comment"
	GitHubOperationGraphQLStatusCheckRollup      = "graphql_status_check_rollup"
	GitHubOperationListCheckRunsForRef           = "list_check_runs_for_ref"
	GitHubOperationListPRFiles                   = "list_pr_files"
	GitHubOperationListReviews                   = "list_reviews"
	GitHubOperationListTeamMembers               = "list_team_members"
	GitHubOperationRequestReviewers              = "request_reviewers"
	GitHubOperationUnknown                       = gitHubMetricValueUnknown
	GitHubOperationUpdateCheckRun                = "update_check_run"
)

const (
	GitHubRequestCategoryAuth    = "auth"
	GitHubRequestCategoryRead    = "read"
	GitHubRequestCategoryUnknown = gitHubMetricValueUnknown
	GitHubRequestCategoryWrite   = "write"
)

const (
	// GitHubRequestStatusCancelled marks requests abandoned by SchemaBot
	// itself: the request context was cancelled before a response arrived,
	// typically during graceful shutdown. Keeping these out of
	// GitHubRequestStatusTransportError stops every deploy from producing a
	// blip on the unreachability signal operators alert on.
	GitHubRequestStatusCancelled = "cancelled"
	GitHubRequestStatusError     = "error"
	// GitHubRequestStatusNotFound marks read-operation requests GitHub
	// answered with HTTP 404. SchemaBot routinely asks read questions whose
	// expected answer is 404 — probing directories for schemabot.yaml config
	// files, or reloading a PR that has since been deleted — so these are
	// semantic "no" answers, not API failures. Keeping them out of
	// GitHubRequestStatusError lets dashboards and alerts track real GitHub
	// errors without excluding whole operations. A 404 on an auth or write
	// operation is a genuine failure (a suspended App installation answers
	// every token exchange with 404) and stays GitHubRequestStatusError.
	GitHubRequestStatusNotFound = "not_found"
	// GitHubRequestStatusTransportError marks requests that never produced an
	// HTTP response: dials that failed, TLS errors, and requests cut off by a
	// context deadline. These are the signature of GitHub (or the network path
	// to it) being unreachable, so they must be counted rather than dropped.
	// Requests SchemaBot cancelled itself are recorded as
	// GitHubRequestStatusCancelled instead.
	GitHubRequestStatusTransportError = "transport_error"
	GitHubRequestStatusSuccess        = "success"
	GitHubRequestStatusUnknown        = gitHubMetricValueUnknown
)

const (
	GitHubRateLimitResourceActionsRunnerRegistration = "actions_runner_registration"
	GitHubRateLimitResourceAuditLog                  = "audit_log"
	GitHubRateLimitResourceCodeScanningUpload        = "code_scanning_upload"
	GitHubRateLimitResourceCodeSearch                = "code_search"
	GitHubRateLimitResourceCore                      = "core"
	GitHubRateLimitResourceDependencySBOM            = "dependency_sbom"
	GitHubRateLimitResourceDependencySnapshots       = "dependency_snapshots"
	GitHubRateLimitResourceGraphQL                   = "graphql"
	GitHubRateLimitResourceIntegrationManifest       = "integration_manifest"
	GitHubRateLimitResourceSCIM                      = "scim"
	GitHubRateLimitResourceSearch                    = "search"
	GitHubRateLimitResourceSourceImport              = "source_import"
)

var (
	seenUnknownGitHubMetricLabels  sync.Map
	seenUnknownWebhookMetricLabels sync.Map
)

// GitHubRequestSample describes a GitHub API request attempt observed by
// SchemaBot — one sample per attempt, whether it produced an HTTP response or
// failed in transport. Category distinguishes reads from content-generating
// writes so dashboards can track pressure against GitHub's secondary write
// limits.
type GitHubRequestSample struct {
	Operation      string
	Category       string
	Resource       string
	Status         string
	Repository     string
	GitHubApp      string
	InstallationID int64
}

// RecordGitHubRequest increments the number of GitHub API request attempts
// observed, including attempts that never produced an HTTP response.
func RecordGitHubRequest(ctx context.Context, sample GitHubRequestSample) {
	sample.Operation = normalizeGitHubOperation(sample.Operation)
	sample.Category = normalizeGitHubRequestCategory(sample.Category)
	sample.Resource = normalizeGitHubRateLimitResource(sample.Resource)
	sample.Status = normalizeGitHubRequestStatus(sample.Status)

	attrs := gitHubMetricAttributes(sample.Operation, sample.Resource, sample.Repository, sample.GitHubApp, sample.InstallationID)
	attrs = append(attrs,
		attribute.String("category", sample.Category),
		attribute.String("status", sample.Status),
	)

	addCounter(ctx, "schemabot.github.requests_total",
		"Total GitHub API request attempts observed by SchemaBot", "{request}",
		attrs...)
}

// GitHubRateLimitSample describes rate-limit headers observed after a GitHub
// API call. Operation and resource are allowlisted before recording to keep
// metric cardinality bounded.
type GitHubRateLimitSample struct {
	Operation      string
	Resource       string
	Repository     string
	GitHubApp      string
	InstallationID int64
	Limit          int64
	Remaining      int64
	Used           int64
}

// RecordGitHubRateLimit records the latest GitHub primary rate-limit header
// values observed after an API call.
func RecordGitHubRateLimit(ctx context.Context, sample GitHubRateLimitSample) {
	sample.Operation = normalizeGitHubOperation(sample.Operation)
	sample.Resource = normalizeGitHubRateLimitResource(sample.Resource)

	attrs := gitHubMetricAttributes(sample.Operation, sample.Resource, sample.Repository, sample.GitHubApp, sample.InstallationID)

	recordGauge(ctx, "schemabot.github.rate_limit.limit", sample.Limit,
		"GitHub primary rate limit for the observed API resource", "{request}", attrs...)
	recordGauge(ctx, "schemabot.github.rate_limit.remaining", sample.Remaining,
		"GitHub primary rate limit requests remaining for the observed API resource", "{request}", attrs...)
	recordGauge(ctx, "schemabot.github.rate_limit.used", sample.Used,
		"GitHub primary rate limit requests used for the observed API resource", "{request}", attrs...)
}

func gitHubMetricAttributes(operation, resource, repository, githubApp string, installationID int64) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("operation", operation),
		EnvironmentAttribute(""),
		attribute.String("resource", resource),
	}
	if repository != "" {
		attrs = append(attrs, attribute.String("repository", repository))
	}
	if githubApp != "" {
		attrs = append(attrs, attribute.String("github_app", githubApp))
	}
	if installationID > 0 {
		attrs = append(attrs, attribute.String("installation_id", strconv.FormatInt(installationID, 10)))
	}
	return attrs
}

// normalizeGitHubLabel returns value when known passes, otherwise logs the
// unknown label once and returns the shared unknown sentinel, keeping each
// per-label normalizer to a single descriptor call.
func normalizeGitHubLabel(label, value string, known func(string) bool) string {
	if known(value) {
		return value
	}
	logUnknownGitHubMetricLabel(label, value)
	return gitHubMetricValueUnknown
}

func normalizeGitHubOperation(operation string) string {
	return normalizeGitHubLabel("operation", operation, isKnownGitHubOperation)
}

func normalizeGitHubRequestCategory(category string) string {
	return normalizeGitHubLabel("category", category, isKnownGitHubRequestCategory)
}

func normalizeGitHubRequestStatus(status string) string {
	return normalizeGitHubLabel("status", status, isKnownGitHubRequestStatus)
}

func normalizeGitHubRateLimitResource(resource string) string {
	return normalizeGitHubLabel("resource", resource, isKnownGitHubRateLimitResource)
}

func logUnknownGitHubMetricLabel(label, value string) {
	key := label + "\x00" + value
	if _, loaded := seenUnknownGitHubMetricLabels.LoadOrStore(key, struct{}{}); loaded {
		return
	}
	slog.Warn("GitHub metric label normalized to unknown", "label", label, "value", value)
}

func isKnownGitHubOperation(operation string) bool {
	switch operation {
	case GitHubOperationAddCommentReaction,
		GitHubOperationCreateCheckRun,
		GitHubOperationCreateIssueComment,
		GitHubOperationCreateInstallationAccessToken,
		GitHubOperationDeleteIssueComment,
		GitHubOperationEditIssueComment,
		GitHubOperationFetchAppSlug,
		GitHubOperationFetchBlob,
		GitHubOperationFetchFileContent,
		GitHubOperationFetchGitTree,
		GitHubOperationFetchPullRequest,
		GitHubOperationGetCombinedStatus,
		GitHubOperationGetTeamMembership,
		GitHubOperationGraphQLMinimizeComment,
		GitHubOperationGraphQLStatusCheckRollup,
		GitHubOperationListCheckRunsForRef,
		GitHubOperationListPRFiles,
		GitHubOperationListReviews,
		GitHubOperationListTeamMembers,
		GitHubOperationRequestReviewers,
		GitHubOperationUnknown,
		GitHubOperationUpdateCheckRun:
		return true
	default:
		return false
	}
}

func isKnownGitHubRequestCategory(category string) bool {
	switch category {
	case GitHubRequestCategoryAuth,
		GitHubRequestCategoryRead,
		GitHubRequestCategoryUnknown,
		GitHubRequestCategoryWrite:
		return true
	default:
		return false
	}
}

func isKnownGitHubRequestStatus(status string) bool {
	switch status {
	case GitHubRequestStatusCancelled,
		GitHubRequestStatusError,
		GitHubRequestStatusNotFound,
		GitHubRequestStatusTransportError,
		GitHubRequestStatusSuccess,
		GitHubRequestStatusUnknown:
		return true
	default:
		return false
	}
}

func isKnownGitHubRateLimitResource(resource string) bool {
	switch resource {
	case GitHubRateLimitResourceActionsRunnerRegistration,
		GitHubRateLimitResourceAuditLog,
		GitHubRateLimitResourceCodeScanningUpload,
		GitHubRateLimitResourceCodeSearch,
		GitHubRateLimitResourceCore,
		GitHubRateLimitResourceDependencySBOM,
		GitHubRateLimitResourceDependencySnapshots,
		GitHubRateLimitResourceGraphQL,
		GitHubRateLimitResourceIntegrationManifest,
		GitHubRateLimitResourceSCIM,
		GitHubRateLimitResourceSearch,
		GitHubRateLimitResourceSourceImport:
		return true
	default:
		return false
	}
}

// RecordWebhookEvent increments the webhook events counter.
// Unknown event types and actions are normalized to "unknown" to prevent unbounded cardinality.
// Repo is not allowlisted since it's bounded by the repos configured in SchemaBot;
// callers recording a status for a repo outside the configured set (e.g.
// repo_not_configured) must pass repo as "" so unbounded repo names never
// become attribute values.
// appName is the resolved GitHub App name (bounded by config), or "unknown" if
// the request could not be attributed to a configured App (e.g. unknown App ID
// header). Pass "" in legacy single-App mode and the metric will record
// "default".
func RecordWebhookEvent(ctx context.Context, appName, eventType, action, repo, status string) {
	if !knownWebhookEvents[eventType] {
		logUnknownWebhookMetricLabel("schemabot.webhook.events_total", "event_type", eventType, appName, repo, status)
		eventType = "unknown"
	}
	if !knownWebhookActions[action] {
		logUnknownWebhookMetricLabel("schemabot.webhook.events_total", "action", action, appName, repo, status)
		action = "unknown"
	}
	if appName == "" {
		appName = "default"
	}
	attrs := []attribute.KeyValue{
		EnvironmentAttribute(""),
		attribute.String("app_name", appName),
		attribute.String("event_type", eventType),
		attribute.String("status", status),
	}
	if action != "" {
		attrs = append(attrs, attribute.String("action", action))
	}
	if repo != "" {
		attrs = append(attrs, attribute.String("repository", repo))
	}
	addCounter(ctx, "schemabot.webhook.events_total",
		"Total number of GitHub webhook events received", "{event}",
		attrs...)
}

// logUnknownWebhookMetricLabel warns once per distinct label tuple when a
// webhook metric label is folded to "unknown". metric names the instrument the
// fold happened on; status is the webhook event status being recorded and must
// be empty for instruments that do not carry one, so the event_status log field
// only ever holds real statuses.
func logUnknownWebhookMetricLabel(metric, label, value, appName, repo, status string) {
	key := metric + "\x00" + label + "\x00" + value + "\x00" + appName + "\x00" + repo + "\x00" + status
	if _, loaded := seenUnknownWebhookMetricLabels.LoadOrStore(key, struct{}{}); loaded {
		return
	}
	attrs := []any{
		"metric", metric,
		"label", label,
		"value", value,
		"app_name", appName,
		"repo", repo,
	}
	if status != "" {
		attrs = append(attrs, "event_status", status)
	}
	slog.Warn("webhook metric label normalized to unknown", attrs...)
}

// RecordUnregisteredRepositoryWebhook increments the counter for webhook events
// ignored because the repository is outside this SchemaBot instance's configured
// ownership. A spike means GitHub is delivering events for repositories that this
// deployment will not process.
func RecordUnregisteredRepositoryWebhook(ctx context.Context, appName, eventType, action, repo string) {
	if !knownWebhookEvents[eventType] {
		logUnknownWebhookMetricLabel("schemabot.webhook.unregistered_repository_ignored_total", "event_type", eventType, appName, repo, "ignored")
		eventType = "unknown"
	}
	if !knownWebhookActions[action] {
		logUnknownWebhookMetricLabel("schemabot.webhook.unregistered_repository_ignored_total", "action", action, appName, repo, "ignored")
		action = "unknown"
	}
	if appName == "" {
		appName = "default"
	}

	attrs := []attribute.KeyValue{
		EnvironmentAttribute(""),
		attribute.String("app_name", appName),
		attribute.String("event_type", eventType),
		attribute.String("repository", repo),
	}
	if action != "" {
		attrs = append(attrs, attribute.String("action", action))
	}
	addCounter(ctx, "schemabot.webhook.unregistered_repository_ignored_total",
		"Total number of GitHub webhook events ignored because the repository is not configured", "{event}",
		attrs...)
}

// knownWebhookInboxStates allowlists the canonical durable webhook inbox states
// so the depth gauge's cardinality stays bounded. It is derived from the
// storage state list so the two cannot drift; an unrecognized value is folded to
// "unknown".
var knownWebhookInboxStates = func() map[string]bool {
	states := make(map[string]bool, len(storage.WebhookEventStatesAll))
	for _, state := range storage.WebhookEventStatesAll {
		states[state] = true
	}
	return states
}()

// RecordWebhookInboxDepth records the number of durable webhook inbox rows in a
// given state. A rising pending/processing depth means dispatch is falling
// behind ingestion; a rising failed_retryable depth means deliveries are
// retrying; a rising completed/failed/superseded depth means terminal rows are
// accumulating and retention has not reclaimed them. A rising failed_permanent
// depth means deliveries are being dead-lettered — each such row is a
// permanently dropped delivery that will never retry and needs explicit
// operator action (GitHub Redeliver for an organic delivery; a new head push
// or check re-run for a synthesized one).
//
// One caveat on pending: a row created with a not-before time sits in pending
// by design until it becomes due, so steady nonzero pending depth alone does
// not imply dispatch lag — cross-check the oldest-claimable-age gauge, which
// counts only dispatchable rows.
func RecordWebhookInboxDepth(ctx context.Context, state string, count int64) {
	if !knownWebhookInboxStates[state] {
		state = "unknown"
	}
	recordGauge(ctx, "schemabot.webhook.inbox_depth", count,
		"Number of durable webhook inbox rows by state", "{row}",
		EnvironmentAttribute(""),
		attribute.String("state", state),
	)
}

// RecordWebhookInboxOldestClaimableAge records how long the oldest
// ready-to-claim-but-unclaimed inbox row has been claimable, in seconds. Age
// counts from when the row became claimable — the later of receipt and its
// not-before/retry time — so a deliberately deferred row does not report its
// grace period as backlog. It is the inbox's backlog latency: a value climbing
// past the dispatch cadence means work is acked but not being picked up.
func RecordWebhookInboxOldestClaimableAge(ctx context.Context, age time.Duration) {
	recordGauge(ctx, "schemabot.webhook.inbox_oldest_claimable_age_seconds", int64(age.Seconds()),
		"Age in seconds of the oldest ready-to-claim durable webhook inbox row", "s",
		EnvironmentAttribute(""),
	)
}

// RecordWebhookInboxStuckProcessing records the number of inbox rows wedged in
// processing with an expired lease at the attempt cap — deliveries no driver can
// reclaim. A nonzero value means auto-plans are being lost to crashes
// mid-processing until the reconciler terminalizes them.
func RecordWebhookInboxStuckProcessing(ctx context.Context, count int64) {
	recordGauge(ctx, "schemabot.webhook.inbox_stuck_processing", count,
		"Number of durable webhook inbox rows stuck in processing past the attempt cap", "{row}",
		EnvironmentAttribute(""),
	)
}

// RecordWebhookInboxStatsCollectionFailure counts failed inbox snapshots. The
// inbox depth/backlog gauges are last-value instruments, so a failed snapshot
// leaves them frozen at their last-good values — indistinguishable from a
// healthy inbox. This counter is the liveness signal for the gauges: a nonzero
// rate means the depth/backlog/stuck values are stale and must not be trusted.
func RecordWebhookInboxStatsCollectionFailure(ctx context.Context) {
	addCounter(ctx, "schemabot.webhook.inbox_stats_collection_failures",
		"Total number of failed durable webhook inbox metric snapshots", "{failure}",
		EnvironmentAttribute(""),
	)
}

// RecordWebhookInboxDispatchLag records how long an accepted delivery waited
// dispatchable in the durable inbox before a driver first claimed it — from
// when the row became eligible (receipt, or its not-before time for a
// deliberately deferred row), so a deferral's grace period never counts as
// lag. This is the per-delivery counterpart of the oldest-claimable-age
// gauge: the gauge shows the backlog's worst case right now, while this
// histogram shows the lag every delivery actually experienced. A distribution
// drifting past the dispatch poll cadence means accepted deliveries are
// waiting on driver capacity — investigate driver-pool sizing and claim-query
// health. One caveat: a shutdown-released claim refunds its attempt, so the
// delivery's reclaim counts as the first attempt again and records another
// sample measured from the original eligibility time — deploy churn therefore
// adds extra, longer samples without any driver-capacity problem. Unknown
// event types fold to "unknown"
// and negative lags (cross-pod clock skew between the enqueueing and claiming
// replica) clamp to zero so the histogram stays trustworthy. appName is the
// resolved GitHub App name used only for fold-log attribution; it is not a
// metric attribute.
func RecordWebhookInboxDispatchLag(ctx context.Context, appName, eventType, repo string, lag time.Duration) {
	if !knownWebhookEvents[eventType] {
		logUnknownWebhookMetricLabel("schemabot.webhook.inbox_dispatch_lag_seconds", "event_type", eventType, appName, repo, "")
		eventType = "unknown"
	}
	if lag < 0 {
		lag = 0
	}
	recordHistogram(ctx, "schemabot.webhook.inbox_dispatch_lag_seconds", lag.Seconds(),
		"Time a webhook delivery spent dispatchable before its first dispatch claim",
		EnvironmentAttribute(""),
		attribute.String("event_type", eventType),
		attribute.String("repository", repo),
	)
}

// knownWebhookDispatchOutcomes allowlists how one durable webhook dispatch
// claim can end so the duration histogram's cardinality stays bounded. An
// unrecognized value folds to "unknown". Outcomes:
//   - "completed": processing succeeded and the row was marked completed.
//   - "failed": processing exhausted the retry budget; the row is terminal
//     but recoverable — GitHub Redeliver reopens an organic row, and the
//     reconciler resurrects a synthesized row. Each lever applies only to
//     its own GUID class; no single row has both.
//   - "failed_permanent": processing proved the delivery can never succeed
//     for its head, so the row was dead-lettered; GitHub Redeliver re-runs
//     an organic row, while a synthesized row needs a fresh delivery (a new
//     head push or a check re-run). A sustained rate means PRs are hitting a
//     deterministic limit (for example GitHub's per-PR file-listing cap) —
//     find the delivery in the driver logs and inspect its last_error.
//   - "retrying": processing failed retryably; the row waits for its retry
//     window.
//   - "superseded": the claimed auto-plan delivery was discarded without
//     processing because a newer covering delivery exists for the same pull
//     request; the successor performs the work.
//   - "released": the pool shut down mid-flight and refunded the claim.
//   - "lease_lost": the driver lost delivery ownership (heartbeat failure or
//     a lease-token mismatch recording the finish); another driver owns the
//     row or will reclaim it.
//   - "finish_error": processing finished but storage failed to record the
//     outcome; the row stays processing until lease expiry hands it to
//     another driver — investigate storage health.
var knownWebhookDispatchOutcomes = map[string]bool{
	"completed":        true,
	"failed":           true,
	"failed_permanent": true,
	"retrying":         true,
	"superseded":       true,
	"released":         true,
	"lease_lost":       true,
	"finish_error":     true,
}

// RecordWebhookDispatchDuration records how long one dispatch claim held a
// delivery, from claim to the recorded outcome. Every claim that starts also
// records exactly one outcome, so this histogram's count is the finish-side
// ledger for the durable_dispatch_started status on the webhook events
// counter: a sustained gap between started and the sum of outcomes means
// drivers are dying mid-claim without recording anything — investigate
// crashes. The two instruments only share the event_type dimension, so
// aggregate both sides down to it before subtracting; the exact query lives in
// this package's README. A rising duration for an event type means its
// processing path is slowing down and the driver pool drains fewer deliveries
// per lease. The histogram deliberately carries no repository label — a
// histogram costs an order of magnitude more series per attribute combination
// than a counter, and per-repo attribution belongs to the driver logs; appName
// and repo are used only for fold-log attribution.
func RecordWebhookDispatchDuration(ctx context.Context, appName, eventType, repo, outcome string, duration time.Duration) {
	if !knownWebhookEvents[eventType] {
		logUnknownWebhookMetricLabel("schemabot.webhook.dispatch_duration_seconds", "event_type", eventType, appName, repo, "")
		eventType = "unknown"
	}
	if !knownWebhookDispatchOutcomes[outcome] {
		logUnknownWebhookMetricLabel("schemabot.webhook.dispatch_duration_seconds", "outcome", outcome, appName, repo, "")
		outcome = "unknown"
	}
	recordHistogram(ctx, "schemabot.webhook.dispatch_duration_seconds", duration.Seconds(),
		"Duration of one durable webhook dispatch claim by outcome",
		EnvironmentAttribute(""),
		attribute.String("event_type", eventType),
		attribute.String("outcome", outcome),
	)
}

// RecordSummaryCommentRepaired counts terminal summary comments posted by
// startup reconciliation because no publisher had posted one — the driver's
// observer lost its lease or crashed, or a claimed publish died before
// posting. A nonzero rate means terminal summaries are being missed at apply
// time; investigate the terminal-publish paths (observer lease churn, stop
// reconciliation, aggregate CAS publisher) for the apply state on the label.
func RecordSummaryCommentRepaired(ctx context.Context, repo string, applyState string) {
	addCounter(ctx, "schemabot.webhook.summary_comments_repaired_total",
		"Total number of missing terminal summary comments posted by startup reconciliation", "{comment}",
		EnvironmentAttribute(""),
		attribute.String("repository", repo),
		attribute.String("apply_state", applyState))
}

// RecordWebhookReconcileMissingEvent counts open PR heads the webhook
// reconciler found without a live inbox delivery — no row at all, or only
// rows that cannot attest coverage (plain-failed synthesized rows, or
// superseded rows whose work was discarded by claim-time coalescing). A
// dead-lettered row or a plain-failed organic row covers its head, so neither
// counts as missing (the dead-letter is deliberately terminal; the organic
// failure has GitHub Redeliver as its lever). A nonzero rate usually means
// deliveries are being lost upstream of the inbox (edge auth, GitHub send
// failures), but a head covered only by superseded rows is a
// coalescing-created miss with no upstream loss — check the superseded
// dispatch outcome for the repo before blaming delivery. With synthesis
// enabled each miss also triggers a recovery delivery, counted by
// RecordWebhookReconcileSynthesizedEvent.
func RecordWebhookReconcileMissingEvent(ctx context.Context, repo string) {
	addCounter(ctx, "schemabot.webhook.reconcile_missing_events_total",
		"Total number of open PR heads found without a webhook inbox delivery", "{event}",
		EnvironmentAttribute(""),
		attribute.String("repository", repo))
}

// RecordWebhookReconcileSynthesizedEvent counts inbox deliveries the webhook
// reconciler synthesized for open PR heads whose organic delivery never
// reached the inbox. Each synthesized delivery re-plans the PR through the
// durable dispatch path. The outcome attribute separates recovery from
// thrash: "first" is the healthy case — a genuinely lost delivery recovered
// once, so investigate the upstream loss (edge auth, GitHub send failures)
// rather than the recovery — while "resynthesis" means a previously
// synthesized row terminally failed or was superseded and was reopened, so a
// sustained resynthesis rate is the same head repeatedly failing or being
// discarded after recovery — investigate that head's processing, not
// delivery loss.
func RecordWebhookReconcileSynthesizedEvent(ctx context.Context, repo string, resynthesis bool) {
	outcome := "first"
	if resynthesis {
		outcome = "resynthesis"
	}
	addCounter(ctx, "schemabot.webhook.reconcile_synthesized_events_total",
		"Total number of webhook inbox deliveries synthesized for open PR heads missing one", "{event}",
		EnvironmentAttribute(""),
		attribute.String("repository", repo),
		attribute.String("outcome", outcome))
}

// RecordWebhookCheckSuiteRecovery counts outcomes of durable
// check_suite.requested recovery processing. "covered", "synthesized",
// "resynthesized", and "already_queued" increment once per candidate PR (a
// delivery can carry several), while "no_pr_at_ingress", "no_open_pr", and
// "truncated" increment once per delivery; a delivery retried under its
// attempt budget re-counts PRs it already observed on an earlier attempt, so
// outcomes are per observation, not per unique PR. "no_pr_at_ingress" is the
// dominant branch by volume — a same-repository suite whose payload named no
// open PR, dropped at ingress before occupying an inbox row; it exists so
// the drop rate is comparable against "synthesized" and the reconciler's
// synthesize rate. "covered" is the healthy steady state — the organic
// pull_request delivery arrived during the recovery grace and planned the
// head, so the redundant signal no-oped. "synthesized" means the auto-plan
// delivery for an open PR head was genuinely lost and the check_suite signal
// recovered it — investigate the upstream loss (edge auth, GitHub send
// failures), not the recovery. "resynthesized" means the recovery reopened a
// terminally failed synthesized row, so a sustained rate is the same head
// failing repeatedly after recovery — investigate that head's processing
// failure. "already_queued" means another recovery producer (the reconciler
// or an earlier check_suite delivery) got there first. "no_open_pr" means no
// PR named by the delivery was still open at the suite head — its PRs closed
// or moved on during the grace, or a fork head matched no open PR.
// "truncated" means the fork-head open-PR walk exhausted its page budget, so
// coverage for the head is incomplete rather than absent — a matching PR
// beyond the budget went unresolved and the reconciler's missing-head scan
// is the backstop; a sustained rate means the repository's open-PR count has
// outgrown the page budget.
func RecordWebhookCheckSuiteRecovery(ctx context.Context, repo string, outcome string) {
	addCounter(ctx, "schemabot.webhook.check_suite_recovery_total",
		"Total number of outcomes from durable check_suite recovery processing", "{event}",
		EnvironmentAttribute(""),
		attribute.String("repository", repo),
		attribute.String("outcome", outcome))
}

// RecordWebhookReconcileStuckTerminated counts webhook inbox rows the
// reconciler terminated because they were parked in processing with an expired
// lease at the attempt cap — a driver hard-killed on its final attempt. A
// nonzero rate means deliveries are being lost to crashes mid-processing; each
// terminated row emits as a failure and becomes eligible for redelivery.
func RecordWebhookReconcileStuckTerminated(ctx context.Context, count int64) {
	addCounterN(ctx, count, "schemabot.webhook.reconcile_stuck_terminated_total",
		"Total number of stuck processing webhook inbox rows terminated by the reconciler", "{event}",
		EnvironmentAttribute(""))
}

// RecordPRFileCapExceeded counts auto-plan runs that failed closed because the
// pull request changes more files than GitHub will report for a single PR, so
// SchemaBot never saw the whole diff. This is a deterministic property of the
// PR, not an outage: the same PR fails the same way on every attempt, so the
// signal is "how often do PRs in this repository grow past the cap", not "is
// GitHub degraded".
//
// Every over-cap PR gets the same cap-specific blocking check. schemaVisible
// records whether the visible prefix of the listing showed a schema or config
// file: true means the PR plausibly carries a schema change that cannot be
// planned until it moves to a PR small enough for GitHub to report in full,
// false means the visible files look like a pure refactor (the withheld tail
// is not inspectable either way).
func RecordPRFileCapExceeded(ctx context.Context, repo string, schemaVisible bool) {
	addCounter(ctx, "schemabot.github.pr_file_cap_exceeded_total",
		"Total number of auto-plan runs that failed closed because the PR exceeds GitHub's per-PR changed-file cap", "{pull_request}",
		EnvironmentAttribute(""),
		attribute.String("repository", repo),
		attribute.Bool("schema_visible", schemaVisible))
}

var knownStatusCheckOperations = map[string]bool{
	"plan_check_recorded":                  true,
	"apply_started":                        true,
	"apply_finished":                       true,
	"apply_cancelled_finished":             true,
	"rollback_finished":                    true,
	"aggregate_check_sync":                 true,
	"stale_check_cleanup":                  true,
	"stale_check_reconciliation":           true,
	"pr_close_cleanup":                     true,
	"schema_config_discovery":              true,
	"schema_config_source_policy":          true,
	"schema_config_environment_validation": true,
	"managed_dir_missing_config":           true,
	"aggregate_participant_skip":           true,
	"aggregate_participant_fanout":         true,
	"participant_comment_nudge":            true,
	"participant_refold":                   true,
	"merge_group_check":                    true,
	"default_branch_check":                 true,
}

var knownStatusCheckStatuses = map[string]bool{
	"success":   true,
	"error":     true,
	"skipped":   true,
	"stale":     true,
	"noop":      true,
	"blocked":   true,
	"scheduled": true,
	"exhausted": true,
	"recreated": true,
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
	attrs := []attribute.KeyValue{
		EnvironmentAttribute(op.Environment),
		attribute.String("operation", op.Operation),
		attribute.String("status", op.Status),
	}
	if op.Database != "" {
		attrs = append(attrs, attribute.String("database", op.Database))
	}
	if op.DatabaseType != "" {
		attrs = append(attrs, attribute.String("database_type", op.DatabaseType))
	}
	if op.Repository != "" {
		attrs = append(attrs, attribute.String("repository", op.Repository))
	}
	addCounter(ctx, "schemabot.status_check_operations_total",
		"Total number of status-check operations", "{operation}",
		attrs...)
}

// RecordPendingDropMoved increments the counter for tables quarantined into
// the pending drops database instead of being dropped.
func RecordPendingDropMoved(ctx context.Context, database string) {
	addCounter(ctx, "schemabot.pending_drops.tables_moved_total",
		"Total number of dropped tables quarantined into the pending drops database", "{table}",
		attribute.String("database", database),
		EnvironmentAttribute(""),
	)
}

// RecordDropTableAlreadyAbsent increments the counter for a DROP TABLE target
// that was already gone when the apply reached it. The DROP phase replays from
// its first statement on resume, so a stopped and resumed apply produces these
// for the tables its earlier attempt dropped. Outside that, it means something
// other than the apply removed the table, and the schema files and the target
// have diverged.
func RecordDropTableAlreadyAbsent(ctx context.Context, database string) {
	addCounter(ctx, "schemabot.drop_table.already_absent_total",
		"Total number of DROP TABLE targets that were already absent when the apply reached them", "{table}",
		attribute.String("database", database),
		EnvironmentAttribute(""),
	)
}

// knownDirectExecutionOutcomes limits metric cardinality to the outcomes the
// direct execution path can produce. Executed statements terminate as
// completed, failed, or stopped; refused statements the policy does not route
// directly are blocked with the reason encoded in the outcome.
var knownDirectExecutionOutcomes = map[string]bool{
	"completed":               true,
	"failed":                  true,
	"stopped":                 true,
	"blocked_policy_disabled": true,
	"blocked_size_limit":      true,
	"blocked_size_unknown":    true,
}

// RecordDirectExecution increments the counter for a statement the
// schema change engine refused and the direct execution policy resolved — to a
// native MySQL DDL execution (completed/failed/stopped) or to a block
// (blocked_*). Direct executions are rare, operator-consented events: a spike
// in failed means native DDL is erroring on the target (check the apply logs
// for the statement and MySQL error), and a spike in blocked_size_unknown
// means row estimates are unavailable (check target connectivity and
// information_schema access).
func RecordDirectExecution(ctx context.Context, database, outcome string) {
	if !knownDirectExecutionOutcomes[outcome] {
		outcome = "unknown"
	}
	addCounter(ctx, "schemabot.direct_execution.statements_total",
		"Total number of engine-refused statements resolved by the direct execution policy", "{statement}",
		attribute.String("database", database),
		attribute.String("outcome", outcome),
		EnvironmentAttribute(""),
	)
}

// RecordPendingDropsCleanupDropped increments the counter for expired
// quarantined tables permanently dropped by the pending drops cleaner.
func RecordPendingDropsCleanupDropped(ctx context.Context, database, environment string) {
	addCounter(ctx, "schemabot.pending_drops.cleanup_dropped_total",
		"Total number of expired quarantined tables dropped by the pending drops cleaner", "{table}",
		attribute.String("database", database),
		EnvironmentAttribute(environment),
	)
}

// RecordPendingDropsCleanupSkipped increments the counter for quarantined
// tables the cleaner skipped because their names carry no valid timestamp
// prefix. A sustained nonzero rate means tables are accumulating that an
// operator must inspect and remove manually.
func RecordPendingDropsCleanupSkipped(ctx context.Context, database, environment string) {
	addCounter(ctx, "schemabot.pending_drops.cleanup_skipped_total",
		"Total number of quarantined tables skipped by the cleaner due to unparseable names", "{table}",
		attribute.String("database", database),
		EnvironmentAttribute(environment),
	)
}

// RecordPendingDropsCleanupLockSkipped increments the counter for cleanup
// passes skipped because another SchemaBot instance owns the per-target
// advisory lock.
func RecordPendingDropsCleanupLockSkipped(ctx context.Context, database, environment string) {
	addCounter(ctx, "schemabot.pending_drops.cleanup_lock_skipped_total",
		"Total number of pending drops cleanup target passes skipped because another instance held the cleanup lock", "{pass}",
		attribute.String("database", database),
		EnvironmentAttribute(environment),
	)
}

// RecordPendingDropsCleanupError increments the counter for pending drops
// cleanup failures. Failed targets and tables are retried on the next cleanup
// pass.
func RecordPendingDropsCleanupError(ctx context.Context, database, environment, reason string) {
	addCounter(ctx, "schemabot.pending_drops.cleanup_errors_total",
		"Total number of pending drops cleanup failures", "{error}",
		attribute.String("database", database),
		EnvironmentAttribute(environment),
		attribute.String("reason", reason),
	)
}
