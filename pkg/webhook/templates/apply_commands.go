package templates

import (
	"fmt"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/ui"
)

// ApplyLockConflictData contains data for apply lock conflict comments.
type ApplyLockConflictData struct {
	Database     string
	DatabaseType string
	Environment  string
	RequestedBy  string

	// Lock holder info
	LockOwner   string
	LockRepo    string
	LockPR      int
	LockCreated time.Time

	// Active apply info (for "apply in progress" case)
	ApplyID    string
	ApplyState string
}

// ActorAuthorizationCommentData contains data for PR command actor
// authorization comments.
type ActorAuthorizationCommentData struct {
	RequestedBy string
	CommandName string
	Database    string
	Environment string
}

// RenderPRCommandNotAuthorized renders a comment when a GitHub PR command
// actor is not allowed to run a mutating SchemaBot command for the database.
func RenderPRCommandNotAuthorized(data ActorAuthorizationCommentData) string {
	var sb strings.Builder

	sb.WriteString("## SchemaBot Command Not Authorized\n\n")
	writeDBEnvLine(&sb, data.Database, data.Environment)
	sb.WriteString("\n")
	if data.RequestedBy != "" {
		fmt.Fprintf(&sb, "@%s is not authorized to run `schemabot %s` for this database.\n\n", data.RequestedBy, data.CommandName)
	} else {
		fmt.Fprintf(&sb, "The requester is not authorized to run `schemabot %s` for this database.\n\n", data.CommandName)
	}
	sb.WriteString("A configured SchemaBot admin/database operator must run this command.\n")

	return sb.String()
}

// RenderPRCommandDatabaseNotConfigured renders a comment when a mutating PR
// command targets a database that is not configured on this SchemaBot instance.
// It is distinct from a plain authorization denial so operators do not waste a
// round-trip assuming the actor lacks access when the database is simply absent.
func RenderPRCommandDatabaseNotConfigured(data ActorAuthorizationCommentData) string {
	var sb strings.Builder

	sb.WriteString("## SchemaBot Command Not Authorized\n\n")
	writeDBEnvLine(&sb, data.Database, data.Environment)
	sb.WriteString("\n")
	fmt.Fprintf(&sb, "`schemabot %s` cannot run because database `%s` is not configured on this SchemaBot instance.\n\n", data.CommandName, data.Database)
	sb.WriteString("Verify the database name, or run the command against the SchemaBot instance that manages this database.\n")

	return sb.String()
}

// RenderPRCommandAuthorizationUnavailable renders a comment when SchemaBot
// cannot verify actor authorization for a mutating PR command.
func RenderPRCommandAuthorizationUnavailable(data ActorAuthorizationCommentData) string {
	var sb strings.Builder

	sb.WriteString("## SchemaBot Authorization Check Failed\n\n")
	writeDBEnvLine(&sb, data.Database, data.Environment)
	if data.RequestedBy != "" {
		fmt.Fprintf(&sb, "**Requested by**: @%s\n", data.RequestedBy)
	}
	sb.WriteString("\n")
	fmt.Fprintf(&sb, "SchemaBot could not verify authorization for `schemabot %s`. No schema change was started.\n\n", data.CommandName)
	sb.WriteString("If access is granted through a GitHub team, verify the GitHub App can read organization members and team membership.\n\n")
	sb.WriteString("A configured SchemaBot admin/database operator should inspect SchemaBot authorization logs before retrying.\n")

	return sb.String()
}

// RenderUnsafeChangesBlocked renders a comment when unsafe changes are detected
// and --allow-unsafe was not specified. Shows the plan DDL plus a blocking message
// instructing the user to re-run with --allow-unsafe.
func RenderUnsafeChangesBlocked(data PlanCommentData) string {
	var sb strings.Builder

	// Render the full plan first (DDL, lint warnings, etc.) so the user can see
	// what would change — but without a lock or confirm footer.
	dbTypeLabel := "Vitess"
	if data.IsMySQL {
		dbTypeLabel = "MySQL"
	}
	fmt.Fprintf(&sb, "## %s Schema Change Plan\n\n", dbTypeLabel)

	writePlanMetadata(&sb, data)
	writePlanAttribution(&sb, data)
	sb.WriteString("\n")

	// Count and show changes
	totalStatements, keyspacesWithVSchema := countChanges(data.Changes)
	totalChanges := totalStatements + keyspacesWithVSchema

	if totalChanges > 0 {
		writeKeyspaceChanges(&sb, data)
	}

	writePlanSummary(&sb, data, totalStatements, keyspacesWithVSchema)

	// Unsafe changes blocked section
	sb.WriteString("---\n\n")
	sb.WriteString("**⛔ Unsafe Changes Detected:**\n")
	for _, c := range data.UnsafeChanges {
		reason := ui.CleanLintReason(c.Reason)
		if reason != "" {
			fmt.Fprintf(&sb, "- `%s`: %s\n", c.Table, reason)
		} else {
			fmt.Fprintf(&sb, "- `%s`\n", c.Table)
		}
	}
	sb.WriteString("\n")
	writeUnsafeDropGuidance(&sb, data.UnsafeChanges)

	sb.WriteString("**🚨 To proceed with these destructive changes, re-run with `--allow-unsafe`:**\n")
	fmt.Fprintf(&sb, "```\nschemabot apply -e %s --allow-unsafe\n```\n", data.Environment)

	return sb.String()
}

// RenderApplyStarted renders a comment when an apply begins.
func RenderApplyStarted(data ApplyStatusCommentData) string {
	var sb strings.Builder

	sb.WriteString("## Schema Change In Progress\n\n")
	writeApplyMetadata(&sb, data, currentTimestamp())
	sb.WriteString("\nSchema changes are being applied. Progress updates will be posted as new comments.\n")

	return sb.String()
}

// RenderApplyCompleted renders a comment when an apply finishes successfully.
func RenderApplyCompleted(data ApplyStatusCommentData) string {
	data.State = "completed"
	return RenderApplyStatusComment(data)
}

// RenderApplyFailed renders a comment when an apply fails.
func RenderApplyFailed(data ApplyStatusCommentData) string {
	data.State = "failed"
	return RenderApplyStatusComment(data)
}

// RenderApplyWaitingForCutover renders a comment when row copy is complete.
func RenderApplyWaitingForCutover(data ApplyStatusCommentData) string {
	data.State = "waiting_for_cutover"
	return RenderApplyStatusComment(data)
}

// RenderApplyStopped renders a comment when an apply is stopped.
func RenderApplyStopped(data ApplyStatusCommentData) string {
	data.State = "stopped"
	return RenderApplyStatusComment(data)
}

// RenderUnlockSuccess renders a confirmation when a lock is released.
func RenderUnlockSuccess(database, environment, releasedBy string) string {
	var sb strings.Builder

	sb.WriteString("## 🔓 Lock Released\n\n")
	writeDBEnvLine(&sb, database, environment)
	fmt.Fprintf(&sb, "\n*Released by @%s at %s*\n", releasedBy, currentTimestamp())
	sb.WriteString("\nThe database is now available for schema changes.\n")

	return sb.String()
}

// RenderApplyBlockedByOtherPR renders a comment when another entity holds the lock.
func RenderApplyBlockedByOtherPR(data ApplyLockConflictData) string {
	var sb strings.Builder

	sb.WriteString("## 🔒 Apply Blocked\n\n")
	writeDBEnvLine(&sb, data.Database, data.Environment)
	writeRequesterOrTimestamp(&sb, data.RequestedBy)
	sb.WriteString("\n")

	isCLI := data.LockPR == 0
	if isCLI {
		sb.WriteString("A CLI session currently holds the lock for this database.\n\n")
		fmt.Fprintf(&sb, "**Locked by**: `%s`\n", data.LockOwner)
	} else {
		sb.WriteString("Another PR currently holds the lock for this database.\n\n")
		if data.LockRepo != "" {
			fmt.Fprintf(&sb, "**Locked by**: [%s#%d](https://github.com/%s/pull/%d)\n",
				data.LockRepo, data.LockPR, data.LockRepo, data.LockPR)
		} else {
			fmt.Fprintf(&sb, "**Locked by**: `%s`\n", data.LockOwner)
		}
	}
	fmt.Fprintf(&sb, "**Since**: %s\n\n", data.LockCreated.UTC().Format("2006-01-02 15:04:05 UTC"))

	if isCLI {
		sb.WriteString("Ask the lock holder to run `schemabot unlock` from their CLI, or force-unlock with:\n")
		fmt.Fprintf(&sb, "```\nschemabot unlock -d %s --force\n```\n", data.Database)
	} else {
		sb.WriteString("Wait for the other PR to complete or ask the lock holder to run `schemabot unlock`.\n")
	}

	return sb.String()
}

// RenderApplyInProgress renders a comment when the same PR already has an active apply.
func RenderApplyInProgress(data ApplyLockConflictData) string {
	var sb strings.Builder

	sb.WriteString("## ⚠️ Apply Already In Progress\n\n")
	writeDBEnvLine(&sb, data.Database, data.Environment)
	writeRequesterOrTimestamp(&sb, data.RequestedBy)
	sb.WriteString("\n")
	fmt.Fprintf(&sb, "An apply is already running for this PR (apply ID: `%s`, state: `%s`).\n\n",
		data.ApplyID, data.ApplyState)
	sb.WriteString("Wait for it to complete or stop it first.\n")

	return sb.String()
}

// RenderNoLocksFound renders a comment when unlock finds no locks for this PR.
func RenderNoLocksFound() string {
	var sb strings.Builder

	sb.WriteString("## 🔓 No Locks Found\n\n")
	sb.WriteString("No schema change locks are held by this PR. Nothing to unlock.\n")

	return sb.String()
}

// RenderCannotUnlock renders a comment when unlock is blocked by an active apply.
func RenderCannotUnlock(database, environment, applyID, applyState string) string {
	var sb strings.Builder

	sb.WriteString("## ⚠️ Cannot Unlock\n\n")
	writeDBEnvLine(&sb, database, environment)
	sb.WriteString("\n")
	fmt.Fprintf(&sb, "An apply is currently active (apply ID: `%s`, state: `%s`).\n\n",
		applyID, applyState)
	sb.WriteString("Wait for it to complete or stop it first.\n")

	return sb.String()
}

// RenderApplyConfirmNoChanges renders a comment when apply-confirm finds no changes.
func RenderApplyConfirmNoChanges(database, environment string) string {
	var sb strings.Builder

	sb.WriteString("## No Changes Detected\n\n")
	writeDBEnvLine(&sb, database, environment)
	sb.WriteString("\nThe database is already up to date — no schema changes needed. Lock released.\n")

	return sb.String()
}

// StaleSchemaRejectionData contains data for the stale-schema rejection comment
// posted when the PR HEAD advanced after discovery loaded the schema files.
type StaleSchemaRejectionData struct {
	RequestedBy  string
	Database     string
	Environment  string
	DiscoverySHA string
	CurrentSHA   string
	Action       string // "plan", "apply", or "apply-confirm" — shown in the retry hint
}

// RenderStaleSchemaRejection renders a comment when plan/apply/apply-confirm is
// rejected because the PR branch HEAD advanced after the schema files were loaded
// at discovery. Tells the user which SHAs were involved and how to retry so
// discovery picks up the new HEAD.
func RenderStaleSchemaRejection(data StaleSchemaRejectionData) string {
	var sb strings.Builder

	sb.WriteString("## ⚠️ Rejected — new commits since discovery\n\n")
	writeDBEnvLine(&sb, data.Database, data.Environment)
	sb.WriteString("\n")
	fmt.Fprintf(&sb, "Schema files were loaded at `%s`, but the current PR HEAD is `%s`. ", data.DiscoverySHA, data.CurrentSHA)
	sb.WriteString("These files no longer match what is on the branch.\n\n")
	sb.WriteString("Re-run the command to use the current HEAD:\n\n")
	fmt.Fprintf(&sb, "```\nschemabot %s -e %s\n```\n", data.Action, data.Environment)

	if data.RequestedBy != "" {
		fmt.Fprintf(&sb, "\n_Requested by @%s_\n", data.RequestedBy)
	}

	return sb.String()
}

// StalePlanRejectionData contains data for the stale-plan rejection comment
// posted when apply-confirm detects that the PR HEAD has advanced since the
// confirmation plan was posted (the cross-delivery race).
type StalePlanRejectionData struct {
	RequestedBy string
	Database    string
	Environment string
	PlanSHA     string // SHA the confirmation plan was rendered against
	CurrentSHA  string // current PR HEAD at apply-confirm time
}

// RenderStalePlanRejection renders a comment when apply-confirm is rejected
// because the stored plan was rendered against a commit that is no longer the
// PR HEAD. Distinct from the within-delivery RenderStaleSchemaRejection: this
// fires across deliveries, so the operator framing is "the plan you confirmed
// is now stale" rather than "discovery just lost a race".
func RenderStalePlanRejection(data StalePlanRejectionData) string {
	var sb strings.Builder

	sb.WriteString("## ⚠️ Rejected — the plan you confirmed is stale\n\n")
	writeDBEnvLine(&sb, data.Database, data.Environment)
	sb.WriteString("\n")
	fmt.Fprintf(&sb, "The confirmation plan was rendered at `%s`, but the current PR HEAD is `%s`. ", data.PlanSHA, data.CurrentSHA)
	sb.WriteString("New commits have landed since the plan was posted, so the DDL you reviewed no longer matches what is on the branch.\n\n")
	sb.WriteString("Re-run `apply` to generate a fresh plan against the current HEAD:\n\n")
	fmt.Fprintf(&sb, "```\nschemabot apply -e %s\n```\n", data.Environment)

	if data.RequestedBy != "" {
		fmt.Fprintf(&sb, "\n_Requested by @%s_\n", data.RequestedBy)
	}

	return sb.String()
}

// RenderApplyConfirmNoLock renders a comment when apply-confirm is run without a lock.
func RenderApplyConfirmNoLock(database, environment string) string {
	var sb strings.Builder

	sb.WriteString("## 🔒 No Lock Found\n\n")
	writeDBEnvLine(&sb, database, environment)
	sb.WriteString("\n")
	sb.WriteString("No apply lock is held for this database. Run `apply` first to generate a plan and acquire the lock.\n\n")
	fmt.Fprintf(&sb, "```\nschemabot apply -e %s\n```\n", environment)

	return sb.String()
}

// RenderApplyBlockedByPriorEnv renders a comment when an apply is blocked because
// a prior environment has pending or failed changes.
func RenderApplyBlockedByPriorEnv(database, environment, priorEnv, status, action string) string {
	var sb strings.Builder

	sb.WriteString("## ❌ Apply Blocked\n\n")
	writeDBEnvLine(&sb, database, environment)
	sb.WriteString("\n")
	fmt.Fprintf(&sb, "%s %s. %s before applying to %s.\n\n", capitalizeFirst(priorEnv), status, action, environment)
	fmt.Fprintf(&sb, "```\nschemabot apply -e %s\n```\n", priorEnv)

	return sb.String()
}

// BlockingCheck represents a PR check that is blocking apply, either because
// it completed without passing or because it is still running. State holds the
// GitHub-reported conclusion (e.g. "failure", "timed_out", "cancelled") for
// completed checks, or the status (e.g. "in_progress", "queued", "pending")
// for in-progress checks.
type BlockingCheck struct {
	Name  string
	State string
}

// RenderApplyBlockedByNonPassingChecks renders a comment when apply is blocked
// because non-SchemaBot PR checks completed without passing.
func RenderApplyBlockedByNonPassingChecks(environment string, notPassing []BlockingCheck) string {
	var sb strings.Builder

	sb.WriteString("## ❌ Apply Blocked\n\n")
	fmt.Fprintf(&sb, "**Environment**: `%s`\n\n", environment)
	if len(notPassing) == 0 {
		// Defensive: callers should only invoke this template when at least
		// one non-passing check has been identified. Render a generic message
		// rather than an empty Markdown table with column headers but no rows.
		sb.WriteString("Cannot apply while PR checks are not passing.\n\n")
		sb.WriteString("Get the checks passing — fix failures and re-run cancelled or stale checks — then retry:\n")
		fmt.Fprintf(&sb, "```\nschemabot apply -e %s\n```\n", environment)
		return sb.String()
	}
	sb.WriteString("Cannot apply while PR checks are not passing:\n\n")
	sb.WriteString("| Check | Status |\n")
	sb.WriteString("|-------|--------|\n")
	for _, f := range notPassing {
		fmt.Fprintf(&sb, "| `%s` | %s |\n", f.Name, f.State)
	}
	sb.WriteString("\nGet the checks passing — fix failures and re-run cancelled or stale checks — then retry:\n")
	fmt.Fprintf(&sb, "```\nschemabot apply -e %s\n```\n", environment)

	return sb.String()
}

// RenderApplyBlockedByCheckStatusError renders a comment when apply is blocked
// because the GitHub API returned an error while fetching PR check statuses.
// The function recognises the "Resource not accessible" permission error and
// surfaces a targeted hint; all other errors are shown verbatim. Both branches
// include a fenced retry command, matching the non-passing/in-progress siblings.
type CheckStatusAccessDetails struct {
	GitHubApp              string
	MissingPermissions     []string
	ChecksReadable         bool
	CommitStatusesReadable bool
}

func RenderApplyBlockedByCheckStatusError(environment string, err error, details *CheckStatusAccessDetails) string {
	var sb strings.Builder

	sb.WriteString("## ❌ Apply Blocked\n\n")
	fmt.Fprintf(&sb, "**Environment**: `%s`\n\n", environment)

	if err != nil && strings.Contains(err.Error(), "Resource not accessible") {
		app := "SchemaBot GitHub App"
		if details != nil && details.GitHubApp != "" {
			app = fmt.Sprintf("SchemaBot GitHub App `%s`", details.GitHubApp)
		}
		fmt.Fprintf(&sb, "The %s cannot read PR check statuses for this repository.\n\n", app)

		switch {
		case details != nil && len(details.MissingPermissions) > 0:
			sb.WriteString("The diagnostic REST probes indicate the installation is missing or has not accepted:\n")
			for _, permission := range details.MissingPermissions {
				fmt.Fprintf(&sb, "- **%s**\n", permission)
			}
			sb.WriteString("\nGrant or accept those permissions, then retry:\n")
		case details != nil && details.ChecksReadable && details.CommitStatusesReadable:
			sb.WriteString("Diagnostic REST probes could read both **Checks** and **Commit statuses**, so the check-status read failed even though the underlying permissions appear readable. Retry the command; if it keeps failing, inspect the SchemaBot logs for the exact GitHub API error:\n")
		default:
			sb.WriteString("Verify the app installation has access to this repository and has permission to read both **Checks** and **Commit statuses**, then retry:\n")
		}
		fmt.Fprintf(&sb, "```\nschemabot apply -e %s\n```\n", environment)
		return sb.String()
	}

	if err != nil {
		sb.WriteString("Unable to verify PR check statuses:\n\n")
		sb.WriteString("```\n")
		fmt.Fprintf(&sb, "%s\n", err)
		sb.WriteString("```\n")
		sb.WriteString("\nResolve the issue and retry:\n")
	} else {
		// Defensive: callers should always pass a non-nil error here, but
		// rendering an empty fenced block followed by "Resolve the issue"
		// would be confusing if a nil error ever slipped through.
		sb.WriteString("Unable to verify PR check statuses.\n\n")
		sb.WriteString("Retry:\n")
	}
	fmt.Fprintf(&sb, "```\nschemabot apply -e %s\n```\n", environment)

	return sb.String()
}

// RenderApplyBlockedByInProgressChecks renders a comment when apply is blocked
// because PR checks have not finished verifying the commit. Two distinct causes
// are surfaced with distinct remediation:
//
//   - inProgress: non-SchemaBot checks GitHub has reported as still running.
//     These resolve on their own, so the guidance is to wait and retry.
//   - notReported: configured required checks that GitHub has not reported on
//     this commit at all. These will not necessarily resolve on their own — a
//     name that never reports (a typo in required_checks, a check not
//     configured on the repo, or a path-filtered workflow) means waiting is
//     futile — so the guidance is to verify the name and that the check runs on
//     this PR, not to wait indefinitely.
func RenderApplyBlockedByInProgressChecks(environment string, inProgress, notReported []BlockingCheck) string {
	var sb strings.Builder

	sb.WriteString("## ⏳ Apply Blocked\n\n")
	fmt.Fprintf(&sb, "**Environment**: `%s`\n\n", environment)
	if len(inProgress) == 0 && len(notReported) == 0 {
		// Defensive: callers should only invoke this template when at least
		// one in-progress or not-reported check has been identified. Render a
		// generic message rather than empty Markdown tables with column headers
		// but no rows.
		sb.WriteString("Cannot apply until PR checks finish verifying this commit.\n\n")
		sb.WriteString("Wait for checks to complete and retry:\n")
		fmt.Fprintf(&sb, "```\nschemabot apply -e %s\n```\n", environment)
		return sb.String()
	}

	if len(inProgress) > 0 {
		sb.WriteString("Cannot apply while PR checks are still running:\n\n")
		sb.WriteString("| Check | Status |\n")
		sb.WriteString("|-------|--------|\n")
		for _, c := range inProgress {
			fmt.Fprintf(&sb, "| `%s` | %s |\n", c.Name, c.State)
		}
		sb.WriteString("\nWait for checks to complete and retry:\n")
		fmt.Fprintf(&sb, "```\nschemabot apply -e %s\n```\n", environment)
	}

	if len(notReported) > 0 {
		if len(inProgress) > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString("These required checks have not reported on this commit:\n\n")
		sb.WriteString("| Check | Status |\n")
		sb.WriteString("|-------|--------|\n")
		for _, c := range notReported {
			fmt.Fprintf(&sb, "| `%s` | %s |\n", c.Name, c.State)
		}
		sb.WriteString("\nIf a check never reports, waiting will not unblock the apply. ")
		sb.WriteString("Verify the name in `required_checks` matches the check exactly and that it runs on this PR, then retry:\n")
		fmt.Fprintf(&sb, "```\nschemabot apply -e %s\n```\n", environment)
	}

	return sb.String()
}

// RenderApplyBlockedByPriorEnvCheckError renders a comment when apply is blocked
// because the GitHub API returned an error while verifying a prior environment's
// aggregate check status. Reason describes the operation that failed (e.g.
// "create GitHub client", "fetch PR details", "query check runs").
func RenderApplyBlockedByPriorEnvCheckError(priorEnv, reason string, err error) string {
	var sb strings.Builder

	sb.WriteString("## ❌ Apply Blocked\n\n")
	fmt.Fprintf(&sb, "Could not verify %s status: failed to %s. Retry the apply command.\n\n", priorEnv, reason)
	fmt.Fprintf(&sb, "_Error: %v_", err)

	return sb.String()
}

// RenderApplyBlockedByMissingPriorEnvCheck renders a comment when apply is
// blocked because SchemaBot cannot find a completed check for a required prior
// environment. This is distinct from a read/API error: retrying the later apply
// does not create the missing prior-environment check.
func RenderApplyBlockedByMissingPriorEnvCheck(priorEnv string) string {
	var sb strings.Builder

	sb.WriteString("## ❌ Apply Blocked\n\n")
	fmt.Fprintf(&sb, "SchemaBot could not find a completed `%s` check for this PR.\n\n", priorEnv)
	fmt.Fprintf(&sb, "SchemaBot must verify `%s` before applying a later environment. Create the missing `%s` status with:\n", priorEnv, priorEnv)
	fmt.Fprintf(&sb, "```\nschemabot plan -e %s\n```\n\n", priorEnv)
	fmt.Fprintf(&sb, "If the plan finds changes, apply `%s` and wait for the SchemaBot check to succeed. Then retry this apply.\n", priorEnv)

	return sb.String()
}

// RenderApplyBlockedByUntrustedPriorEnvCheck renders a comment when apply is
// blocked because the prior environment's check exists on the PR head but was
// created only by GitHub Apps this deployment does not trust. Re-running plan
// or apply on the prior environment cannot fix this — the remediation is a
// server config change (trusting the owning deployment's App) or removing a
// spoofed check, so the comment must not suggest the plan/apply loop.
func RenderApplyBlockedByUntrustedPriorEnvCheck(priorEnv, checkName string, untrustedApps []string) string {
	var sb strings.Builder

	sb.WriteString("## ❌ Apply Blocked\n\n")
	fmt.Fprintf(&sb, "A `%s` check named `%s` exists on this PR, but it was created by a GitHub App this SchemaBot deployment does not trust:\n\n", priorEnv, checkName)
	for _, app := range untrustedApps {
		fmt.Fprintf(&sb, "- `%s`\n", app)
	}
	sb.WriteString("\n")
	fmt.Fprintf(&sb, "SchemaBot only verifies `%s` through check runs created by trusted SchemaBot deployment Apps.\n\n", priorEnv)
	sb.WriteString("### Next steps\n")
	fmt.Fprintf(&sb, "- If the App above is the SchemaBot deployment that owns `%s`, an operator must add its slug to `github.trusted-check-app-slugs` in this deployment's server config.\n", priorEnv)
	sb.WriteString("- If you do not recognize the App, do not trust it — the check may be impersonating SchemaBot.\n\n")
	fmt.Fprintf(&sb, "Re-running `schemabot plan -e %s` will not resolve this.\n", priorEnv)

	return sb.String()
}

// RenderApplyBlockedByPriorEnvInProgress renders a comment when an apply is blocked
// because a prior environment's apply is currently running.
func RenderApplyBlockedByPriorEnvInProgress(database, environment, priorEnv string) string {
	var sb strings.Builder

	sb.WriteString("## ⏳ Apply Blocked\n\n")
	writeDBEnvLine(&sb, database, environment)
	sb.WriteString("\n")
	fmt.Fprintf(&sb, "%s is currently in progress. Wait for it to complete before applying to %s.\n\n", capitalizeFirst(priorEnv), environment)
	fmt.Fprintf(&sb, "Once %s completes, retry:\n", priorEnv)
	fmt.Fprintf(&sb, "```\nschemabot apply -e %s\n```\n", environment)

	return sb.String()
}

// RenderApplyBlockedByUnlistedEnvironment renders a comment when an apply is
// blocked because this scoped SchemaBot instance cannot enforce staging-first
// promotion ordering: the target environment is not part of the configured
// promotion order, so SchemaBot cannot determine which environments must be
// applied before it. This is a configuration error that an operator must fix.
func RenderApplyBlockedByUnlistedEnvironment(environment string, promotionOrder []string) string {
	var sb strings.Builder

	sb.WriteString("## ❌ Apply Blocked\n\n")
	fmt.Fprintf(&sb, "**Environment**: `%s`\n\n", environment)
	fmt.Fprintf(&sb, "`%s` is not in the configured promotion order, so SchemaBot cannot determine which environments must be applied before it and cannot enforce staging-first ordering.\n\n", environment)
	if len(promotionOrder) > 0 {
		fmt.Fprintf(&sb, "Configured promotion order: `%s`\n\n", strings.Join(promotionOrder, "` → `"))
	}
	fmt.Fprintf(&sb, "Add `%s` to `environment_order` so SchemaBot knows where it sits in the promotion sequence, then retry the apply.\n", environment)

	return sb.String()
}
