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
)

// Meter name used for all SchemaBot metrics.
const meterName = "schemabot"

// RecordPlan increments the plans counter with database, environment, and status attributes.
// Status should be "success" or "error".
//
// The OTel SDK deduplicates instruments with the same name, so repeated calls
// to Int64Counter are cheap after the first registration.
func RecordPlan(ctx context.Context, repo, database, environment, status string) {
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
			attribute.String("repository", repo),
			attribute.String("database", database),
			attribute.String("environment", environment),
			attribute.String("status", status),
		),
	)
}

// RecordPlanDuration records the duration of a plan operation.
func RecordPlanDuration(ctx context.Context, duration time.Duration, repo, database, environment, status string) {
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
			attribute.String("repository", repo),
			attribute.String("database", database),
			attribute.String("environment", environment),
			attribute.String("status", status),
		),
	)
}

// RecordApply increments the applies counter with database, environment, and status attributes.
// Status should be "success", "error", "rejected", or "conflict".
func RecordApply(ctx context.Context, repo, database, environment, status string) {
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
			attribute.String("repository", repo),
			attribute.String("database", database),
			attribute.String("environment", environment),
			attribute.String("status", status),
		),
	)
}

// RecordApplyDuration records the duration of an apply operation (API call time,
// not the full Spirit run which can take hours).
func RecordApplyDuration(ctx context.Context, duration time.Duration, repo, database, environment, status string) {
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
			attribute.String("repository", repo),
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
func RecordStalePlanRejected(ctx context.Context) {
	meter := otel.Meter(meterName)
	counter, err := meter.Int64Counter("schemabot.command.rejected_stale_plan.total",
		otelmetric.WithDescription("apply-confirm rejected because PR HEAD advanced after the confirmation plan was posted"),
		otelmetric.WithUnit("{rejection}"),
	)
	if err != nil {
		slog.Warn("failed to create stale plan rejected counter", "error", err)
		return
	}
	counter.Add(ctx, 1,
		otelmetric.WithAttributes(
			attribute.String("action", "apply_confirm"),
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

var knownPRCommandActorAuthCommands = map[string]bool{
	"apply":            true,
	"apply_confirm":    true,
	"rollback":         true,
	"rollback_confirm": true,
	"unlock":           true,
	"cutover":          true,
	"stop":             true,
	"start":            true,
	"volume":           true,
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

	meter := otel.Meter(meterName)
	counter, err := meter.Int64Counter("schemabot.pr_command_actor_authorization.total",
		otelmetric.WithDescription("Total GitHub PR command actor authorization decisions"),
		otelmetric.WithUnit("{decision}"),
	)
	if err != nil {
		slog.Warn("failed to create PR command actor authorization counter", "error", err)
		return
	}
	counter.Add(ctx, 1,
		otelmetric.WithAttributes(
			attribute.String("command", command),
			attribute.String("database", database),
			attribute.String("environment", environment),
			attribute.String("repository", repository),
			attribute.String("status", status),
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

// RecordSchemaRequestError increments the schema request error counter.
// Reason should be a stable string: "database_not_found", "invalid_config",
// "no_config", "multiple_configs", or "unexpected".
func RecordSchemaRequestError(ctx context.Context, repo, command, database, environment, reason string) {
	meter := otel.Meter(meterName)
	counter, err := meter.Int64Counter("schemabot.schema_request.errors_total",
		otelmetric.WithDescription("Schema request errors by reason"),
		otelmetric.WithUnit("{error}"),
	)
	if err != nil {
		slog.Warn("failed to create schema request error counter", "error", err)
		return
	}
	counter.Add(ctx, 1,
		otelmetric.WithAttributes(
			attribute.String("repository", repo),
			attribute.String("command", command),
			attribute.String("database", database),
			attribute.String("environment", environment),
			attribute.String("reason", reason),
		),
	)
}

const (
	gitHubMetricValueUnknown = "unknown"

	GitHubOperationAddCommentReaction            = "add_comment_reaction"
	GitHubOperationCreateCheckRun                = "create_check_run"
	GitHubOperationCreateIssueComment            = "create_issue_comment"
	GitHubOperationCreateInstallationAccessToken = "create_installation_access_token"
	GitHubOperationEditIssueComment              = "edit_issue_comment"
	GitHubOperationFetchAppSlug                  = "fetch_app_slug"
	GitHubOperationFetchBlob                     = "fetch_blob"
	GitHubOperationFetchFileContent              = "fetch_file_content"
	GitHubOperationFetchGitTree                  = "fetch_git_tree"
	GitHubOperationFetchPullRequest              = "fetch_pull_request"
	GitHubOperationGetTeamMembership             = "get_team_membership"
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
	GitHubRequestStatusError   = "error"
	GitHubRequestStatusSuccess = "success"
	GitHubRequestStatusUnknown = gitHubMetricValueUnknown
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

var seenUnknownGitHubMetricLabels sync.Map

// GitHubRequestSample describes a GitHub API response observed by SchemaBot.
// Category distinguishes reads from content-generating writes so dashboards can
// track pressure against GitHub's secondary write limits.
type GitHubRequestSample struct {
	Operation      string
	Category       string
	Resource       string
	Status         string
	Repository     string
	GitHubApp      string
	InstallationID int64
}

// RecordGitHubRequest increments the number of GitHub API responses observed.
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

	meter := otel.Meter(meterName)
	counter, err := meter.Int64Counter("schemabot.github.requests_total",
		otelmetric.WithDescription("Total GitHub API responses observed by SchemaBot"),
		otelmetric.WithUnit("{request}"),
	)
	if err != nil {
		slog.Warn("failed to create GitHub request counter", "error", err)
		return
	}

	counter.Add(ctx, 1, otelmetric.WithAttributes(attrs...))
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

	meter := otel.Meter(meterName)
	limitGauge, err := meter.Int64Gauge("schemabot.github.rate_limit.limit",
		otelmetric.WithDescription("GitHub primary rate limit for the observed API resource"),
		otelmetric.WithUnit("{request}"),
	)
	if err != nil {
		slog.Warn("failed to create GitHub rate limit gauge", "error", err)
		return
	}
	remainingGauge, err := meter.Int64Gauge("schemabot.github.rate_limit.remaining",
		otelmetric.WithDescription("GitHub primary rate limit requests remaining for the observed API resource"),
		otelmetric.WithUnit("{request}"),
	)
	if err != nil {
		slog.Warn("failed to create GitHub rate remaining gauge", "error", err)
		return
	}
	usedGauge, err := meter.Int64Gauge("schemabot.github.rate_limit.used",
		otelmetric.WithDescription("GitHub primary rate limit requests used for the observed API resource"),
		otelmetric.WithUnit("{request}"),
	)
	if err != nil {
		slog.Warn("failed to create GitHub rate used gauge", "error", err)
		return
	}

	limitGauge.Record(ctx, sample.Limit, otelmetric.WithAttributes(attrs...))
	remainingGauge.Record(ctx, sample.Remaining, otelmetric.WithAttributes(attrs...))
	usedGauge.Record(ctx, sample.Used, otelmetric.WithAttributes(attrs...))
}

func gitHubMetricAttributes(operation, resource, repository, githubApp string, installationID int64) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("operation", operation),
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

func normalizeGitHubOperation(operation string) string {
	if isKnownGitHubOperation(operation) {
		return operation
	}
	logUnknownGitHubMetricLabel("operation", operation)
	return gitHubMetricValueUnknown
}

func normalizeGitHubRequestCategory(category string) string {
	if isKnownGitHubRequestCategory(category) {
		return category
	}
	logUnknownGitHubMetricLabel("category", category)
	return GitHubRequestCategoryUnknown
}

func normalizeGitHubRequestStatus(status string) string {
	if isKnownGitHubRequestStatus(status) {
		return status
	}
	logUnknownGitHubMetricLabel("status", status)
	return GitHubRequestStatusUnknown
}

func normalizeGitHubRateLimitResource(resource string) string {
	if isKnownGitHubRateLimitResource(resource) {
		return resource
	}
	logUnknownGitHubMetricLabel("resource", resource)
	return gitHubMetricValueUnknown
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
		GitHubOperationEditIssueComment,
		GitHubOperationFetchAppSlug,
		GitHubOperationFetchBlob,
		GitHubOperationFetchFileContent,
		GitHubOperationFetchGitTree,
		GitHubOperationFetchPullRequest,
		GitHubOperationGetTeamMembership,
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
	case GitHubRequestStatusError,
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
