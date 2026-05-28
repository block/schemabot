package webhook

import (
	"time"
)

// maxCheckRunTextLength is the GitHub API limit for check run output text.
const maxCheckRunTextLength = 65530

// GitHub Check Run status values.
const (
	checkStatusCompleted  = "completed"
	checkStatusInProgress = "in_progress"
	checkStatusQueued     = "queued"
)

// GitHub Check Run conclusion values.
const (
	checkConclusionSuccess        = "success"
	checkConclusionFailure        = "failure"
	checkConclusionActionRequired = "action_required"
	checkConclusionNeutral        = "neutral"
)

// aggregateCheckName is the GitHub Check Run name to require in branch protection.
// Per-database stored check state provides granular internal status per
// environment and database; the aggregate rolls it into one visible conclusion so
// branch protection only needs one stable name.
const aggregateCheckName = "SchemaBot"

const (
	// defaultPriorEnvCheckMaxAttempts bounds the apply gate wait for prior
	// environment check state. It includes the first read plus retries, so the
	// default waits up to roughly three seconds before failing closed.
	defaultPriorEnvCheckMaxAttempts   = 4
	defaultPriorEnvCheckRetryInterval = time.Second
)

// aggregateSentinel is used for database type and database name when storing
// aggregate check state in the checks table. For the environment field,
// per-environment aggregates use the real environment name while the global
// aggregate (no allowed_environments) uses aggregateSentinel.
const aggregateSentinel = "_aggregate"

type checkBlockReason struct {
	blockingReason string
	message        string
}

// schemaRemovedAfterApplyBlock is used when the latest PR commit removes a
// schema change after an apply has already started. blockingReason is the stable
// machine-readable value stored in checks.blocking_reason; message is shown to
// users in per-database check state.
var schemaRemovedAfterApplyBlock = checkBlockReason{
	blockingReason: "schema_removed_after_apply_started",
	message:        "Schema changes were removed from the PR after an apply started; operator action is required before this check can pass.",
}

// rollbackCompletedBlock is used after a rollback succeeds. The target
// environment no longer has the schema requested by the PR, so the check must
// stay blocked until the schema change is applied again or the PR is updated.
var rollbackCompletedBlock = checkBlockReason{
	blockingReason: "rollback_completed",
	message:        "Schema changes were rolled back in this environment; apply again before this check can pass.",
}

// githubConfigDiscoveryUnavailableBlock is used when GitHub is unavailable
// while SchemaBot is discovering which managed schema changes exist. The
// aggregate check must fail closed until SchemaBot can read PR metadata and
// repository contents.
var githubConfigDiscoveryUnavailableBlock = checkBlockReason{
	blockingReason: "github_schema_config_discovery_unavailable",
	message:        "SchemaBot failed this check closed because GitHub was unavailable while inspecting the PR schema files. Retry the check.",
}

// configDiscoveryFailedBlock is used when SchemaBot cannot inspect PR schema
// files well enough to know which managed schema changes exist for a reason
// other than GitHub availability. The aggregate check must fail closed until
// SchemaBot can determine the managed schema configuration.
var configDiscoveryFailedBlock = checkBlockReason{
	blockingReason: "schema_config_discovery_failed",
	message:        "SchemaBot failed this check closed because it could not determine the managed schema configuration for this PR. Review the SchemaBot configuration and retry the check.",
}

// noAllowedConfiguredEnvironmentsBlock is used when schema files changed but
// the environments declared by schemabot.yaml do not overlap this service's
// allowed_environments. SchemaBot cannot safely plan the schema change in that
// configuration, so the aggregate check must fail closed.
var noAllowedConfiguredEnvironmentsBlock = checkBlockReason{
	blockingReason: "no_allowed_configured_environments",
	message:        "SchemaBot found schema changes, but no environment declared by schemabot.yaml is allowed for this SchemaBot deployment. Align schemabot.yaml with allowed_environments, then retry the check.",
}
