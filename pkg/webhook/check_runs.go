package webhook

import (
	"time"

	"github.com/block/schemabot/pkg/api"
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

// aggregateCheckName is the default GitHub Check Run base name.
// Per-database stored check state provides granular internal status per
// environment and database; the aggregate rolls it into one visible conclusion so
// branch protection only needs one stable name per deployment.
const aggregateCheckName = api.DefaultGitHubCheckName

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
	message:        "The current PR no longer contains a schema change whose apply has already started; reconciliation is required before this check can pass.",
}

// rollbackCompletedBlock is used after a rollback succeeds. The target
// environment no longer has the schema requested by the PR, so the check must
// stay blocked until the PR and live schema are reconciled.
var rollbackCompletedBlock = checkBlockReason{
	blockingReason: "rollback_completed",
	message:        "Schema changes were rolled back in this environment; apply the PR schema changes again, or reconcile the PR and live schema before this check can pass.",
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
// the server-configured environments for the database do not overlap this
// service's allowed_environments. SchemaBot cannot safely plan the schema
// change in that configuration, so the aggregate check must fail closed.
var noAllowedConfiguredEnvironmentsBlock = checkBlockReason{
	blockingReason: "no_allowed_configured_environments",
	message:        "SchemaBot found schema changes, but no configured environment for this database is allowed for this SchemaBot deployment. Align server environment configuration with allowed_environments, then retry the check.",
}
