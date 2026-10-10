package templates

import (
	"fmt"
	"html"
	"strings"

	"github.com/block/schemabot/pkg/caller"
)

// RenderRollbackPlanComment renders the rollback plan comment markdown.
// Reuses PlanCommentData since rollback plans have the same structure as regular plans.
func RenderRollbackPlanComment(data PlanCommentData) string {
	return renderWithinCommentLimit(countPlanDDLBlocks(data.Changes), 0, func(budget *ddlBlockBudget) string {
		return renderRollbackPlanComment(data, budget)
	})
}

func renderRollbackPlanComment(data PlanCommentData, budget *ddlBlockBudget) string {
	var sb strings.Builder

	// Header
	writeEnvironmentTitle(&sb, "Schema Rollback Plan", data.Environment)

	writePlanMetadata(&sb, data)
	writeRequesterOrTimestamp(&sb, data.RequestedBy)
	sb.WriteString("\n")

	// Count changes
	totalStatements, keyspaceUpdates := countChanges(data.Changes)
	totalChanges := totalStatements + keyspaceUpdates

	// Summary
	if totalChanges == 0 {
		sb.WriteString("**No schema changes detected** — the database already matches the original schema.\n\n")
		return appendAgentHint(sb.String(), data.AgentHint)
	}

	// Detailed changes
	writeKeyspaceChanges(&sb, data, budget)

	// The rollback's unsafe changes are named here, because confirming them
	// takes --allow-unsafe and the operator should see what that destroys.
	if data.HasUnsafeChanges && len(data.UnsafeChanges) > 0 {
		writeUnsafeWarning(&sb, data.UnsafeChanges, nil, data.DatabaseType, data.IsMySQL, true)
	}

	// Lint violations. Only the lint fold names a finding's rule here: the
	// unsafe section lists changes, not findings, so error-severity findings
	// link no guide.
	if len(data.LintViolations) > 0 {
		writeLintViolations(&sb, data.LintViolations)
	}
	writeRelatedGuidance(&sb, data.disclosesNonErrorsOnly())

	// Errors
	if len(data.Errors) > 0 {
		writeErrors(&sb, data.Errors)
	}

	// Summary (after DDL, matching CLI layout)
	writePlanSummary(&sb, data, totalStatements, keyspaceUpdates)

	// Footer. Like the apply instruction, the consent requirement leads into
	// the command and the flag stays out of the pasteable command, so
	// consenting to destroy data takes typing it.
	sb.WriteString("---\n\n")
	if consent, ok := planUnsafeConsent(data); ok {
		fmt.Fprintf(&sb, "To confirm this rollback, %s:\n", consent.instruction())
	} else {
		sb.WriteString("To confirm this rollback, comment:\n")
	}
	fmt.Fprintf(&sb, "```\n%s\n```\n\n", tenantCommand("schemabot rollback-confirm", data.Environment, data.Tenant))
	writeRollbackCancel(&sb, data.Tenant)

	return appendAgentHint(sb.String(), data.AgentHint)
}

func writeRollbackCancel(sb *strings.Builder, tenant string) {
	sb.WriteString("To cancel, comment:\n")
	fmt.Fprintf(sb, "```\n%s\n```\n", appendTenantFlag("schemabot unlock", tenant))
}

// RenderRollbackUnsafeChangesBlocked renders the refusal posted when
// rollback-confirm is given without `--allow-unsafe` and the pinned rollback
// plan carries unsafe changes. It mirrors the apply refusal: the rollback plan,
// each unsafe change, and the exact rollback-confirm command that consents to
// them. Nothing ran and the lock still pins the rollback plan, so the
// re-issued command confirms the same plan, and unlock cancels it.
func RenderRollbackUnsafeChangesBlocked(data PlanCommentData) string {
	return renderWithinCommentLimit(countPlanDDLBlocks(data.Changes), 0, func(budget *ddlBlockBudget) string {
		return renderRollbackUnsafeChangesBlocked(data, budget)
	})
}

func renderRollbackUnsafeChangesBlocked(data PlanCommentData, budget *ddlBlockBudget) string {
	var sb strings.Builder

	writeEnvironmentTitle(&sb, "Schema Rollback Plan", data.Environment)
	writePlanMetadata(&sb, data)
	writeRequesterOrTimestamp(&sb, data.RequestedBy)
	sb.WriteString("\n")

	totalStatements, keyspaceUpdates := countChanges(data.Changes)
	if totalStatements+keyspaceUpdates > 0 {
		writeKeyspaceChanges(&sb, data, budget)
	}
	writePlanSummary(&sb, data, totalStatements, keyspaceUpdates)

	// The retry keeps every option the rejected command carried, so following
	// it changes only the consent, never how the rollback runs.
	retryCommand := tenantCommand("schemabot rollback-confirm", data.Environment, data.Tenant) + " --allow-unsafe"
	if data.DeferCutover {
		retryCommand += " --defer-cutover"
	}
	writeUnsafeChangesRejection(&sb, data, "Rollback rejected", retryCommand)
	sb.WriteString("\nThe lock still pins this rollback plan, so the command above confirms it.\n\n")
	writeRollbackCancel(&sb, data.Tenant)

	return appendAgentHint(offerSupportChannel(sb.String()), data.AgentHint)
}

// RenderRollbackConfirmNoLock renders a message when rollback-confirm is run
// without a held lock. Tenant is the deployment's own tenant; when set, the
// suggested command carries it so pasting the hint addresses this deployment.
func RenderRollbackConfirmNoLock(database, environment, tenant string) string {
	rollbackCmd := tenantCommand("schemabot rollback <apply-id>", environment, tenant)
	if database == "" {
		return fmt.Sprintf("## 🔒 No Lock Found\n\n"+
			"**Environment**: `%s`\n\n"+
			"No rollback lock is held by this PR. Run `%s` first to generate a rollback plan.",
			environment, rollbackCmd)
	}
	return fmt.Sprintf("## 🔒 No Lock Found\n\n"+
		"**Database**: `%s` | **Environment**: `%s`\n\n"+
		"No rollback lock is held. Run `%s` first to generate a rollback plan.",
		database, environment, rollbackCmd)
}

// RenderRollbackMissingApplyID renders the message posted when `schemabot rollback`
// is invoked without an apply ID argument. Tenant is the deployment's own
// tenant; when set, the rollback usage lines carry it so pasting a hint
// addresses this deployment. The status lookup is a CLI command, so it starts
// with cliName and is scoped to the environment the rollback named, or to the
// placeholder when it named none. It never carries --tenant: the tenant routes
// PR comments, the CLI has no such flag, and the CLI reaches this deployment
// through its endpoint or profile, which cliName's wrapper selects.
func RenderRollbackMissingApplyID(cliName, environment, tenant string) string {
	return offerSupportChannel("## Missing Apply ID\n\n" +
		fmt.Sprintf("Usage: `%s`\n\n", tenantCommand("schemabot rollback <apply-id>", "<environment>", tenant)) +
		fmt.Sprintf("Confirm a generated rollback with `%s`.\n\n", tenantCommand("schemabot rollback-confirm", "<environment>", tenant)) +
		"You can find the apply ID in the summary comment of a completed apply, " +
		fmt.Sprintf("or by running `%s`.", cliCommand(cliName, "status "+environmentFlag(environment))))
}

// RenderRollbackApplyNotFound renders the message posted when the supplied apply ID
// does not match any stored apply.
func RenderRollbackApplyNotFound(applyID string) string {
	return offerSupportChannel(fmt.Sprintf("## Apply Not Found\n\n"+
		"No apply found with ID `%s`. Check the ID and try again.", applyID))
}

// RollbackRejectedData contains the details shown when SchemaBot refuses to
// generate a rollback plan because the requested apply is not safe to target.
type RollbackRejectedData struct {
	ApplyID     string
	Database    string
	Environment string
	Reason      string
}

// RenderRollbackRejected renders a rollback-specific rejection message. These
// rejections are deliberate safety gates, not generic command failures.
func RenderRollbackRejected(data RollbackRejectedData) string {
	var sb strings.Builder
	sb.WriteString("## Rollback Not Allowed\n\n")
	if data.ApplyID != "" {
		fmt.Fprintf(&sb, "**Apply**: `%s`\n", data.ApplyID)
	}
	if data.Database != "" {
		fmt.Fprintf(&sb, "**Database**: `%s`\n", data.Database)
	}
	if data.Environment != "" {
		fmt.Fprintf(&sb, "**Environment**: `%s`\n", data.Environment)
	}
	if data.ApplyID != "" || data.Database != "" || data.Environment != "" {
		sb.WriteString("\n")
	}

	sb.WriteString("SchemaBot did not generate a rollback plan because it cannot safely prove this apply is the exact completed schema change that rollback would target.\n\n")
	if reason := sanitizedRollbackRejectionReason(data.Reason); reason != "" {
		fmt.Fprintf(&sb, "**Reason**: `%s`\n\n", reason)
	}
	sb.WriteString("Rollback is currently allowed only for the latest completed apply for this database and environment, and only when that apply has stored original schema. An operator should reconcile the target schema manually or retry with the latest eligible apply.")
	return sb.String()
}

// sanitizedRollbackRejectionReason makes an untrusted rejection reason safe
// inside the `**Reason**: `...“ code span: full inline sanitization plus
// neutralizing backticks so the reason cannot close the span.
func sanitizedRollbackRejectionReason(reason string) string {
	return strings.ReplaceAll(SanitizeInlineError(reason), "`", "'")
}

// RenderRollbackBlockedByLock renders the message posted when a rollback cannot
// acquire the database lock because another caller holds it. When lockRepo and
// lockPR are populated, the holder is rendered as a PR link; otherwise the bare
// owner string is shown. Tenant is the deployment's own tenant; when set, the
// suggested unlock command carries it so pasting the hint addresses this
// deployment. lockedApply is what SchemaBot found running on the locked
// database.
func RenderRollbackBlockedByLock(database, environment, lockOwner, lockRepo string, lockPR int, tenant string, lockedApply LockedDatabaseApply) string {
	if lockPR > 0 && lockRepo != "" {
		return offerSupportChannel(fmt.Sprintf("## Rollback Blocked\n\n"+
			"**Database**: `%s` | **Environment**: `%s`\n\n"+
			"A lock is currently held by %s.\n\n%s",
			database, environment,
			caller.PullRequestMarkdownLink(lockRepo, lockPR),
			otherPRLockReleaseHint(appendTenantFlag("schemabot unlock", tenant), lockedApply)))
	}
	return offerSupportChannel(fmt.Sprintf("## Rollback Blocked\n\n"+
		"**Database**: `%s` | **Environment**: `%s`\n\n"+
		"A lock is currently held by `%s`.\n\n"+
		"Wait for that operation to complete, or ask the lock owner to release it.",
		database, environment, caller.Short(lockOwner)))
}

// RenderRollbackNothingToDo renders the message posted when a rollback plan
// produces no schema changes for the supplied apply ID.
func RenderRollbackNothingToDo(database, environment, applyID string) string {
	return fmt.Sprintf("## Nothing to Rollback\n\n"+
		"**Database**: `%s` | **Environment**: `%s`\n\n"+
		"The database schema already matches the state before apply `%s`. No rollback needed.",
		database, environment, applyID)
}

// RenderRollbackLockNotOwned renders the message posted when rollback-confirm is
// invoked against a lock held by a different caller.
func RenderRollbackLockNotOwned(database, environment, lockOwner string) string {
	return fmt.Sprintf("## Lock Not Owned\n\n"+
		"**Database**: `%s` | **Environment**: `%s`\n\n"+
		"The lock is held by `%s`, not this PR. Cannot confirm rollback.",
		database, environment, caller.Short(lockOwner))
}

// RenderRollbackAlreadyRolledBack renders the message posted when rollback-confirm
// re-plans and finds no changes remain — typically because the rollback already
// ran in a separate path.
func RenderRollbackAlreadyRolledBack(database, environment string) string {
	return fmt.Sprintf("## Already Rolled Back\n\n"+
		"**Database**: `%s` | **Environment**: `%s`\n\n"+
		"The database schema already matches the original state. Lock released.",
		database, environment)
}

// RenderRollbackAlreadyRolledBackLockHeld renders the message posted when
// rollback-confirm finds no changes remain but the database lock could not be
// released. The lock continues to block applies on the database until an
// operator releases it. Tenant is the deployment's own tenant; when set, the
// suggested unlock commands carry it so pasting a hint addresses this
// deployment.
func RenderRollbackAlreadyRolledBackLockHeld(database, environment, lockOwner, tenant string) string {
	return fmt.Sprintf("## Already Rolled Back\n\n"+
		"**Database**: `%s` | **Environment**: `%s`\n\n"+
		"The database schema already matches the original state, but SchemaBot failed to release the lock held by `%s`. "+
		"Applies on this database will be blocked until the lock is released.\n\n"+
		"Release it by commenting:\n"+
		"```\n%s\n```\n"+
		"If the lock persists, force-release it:\n"+
		"```\n%s\n```",
		database, environment, caller.Short(lockOwner),
		appendTenantFlag("schemabot unlock", tenant),
		appendTenantFlag(fmt.Sprintf("schemabot unlock -d %s --force", database), tenant))
}

// RenderRollbackNotAccepted renders the message posted when the apply service
// rejects a rollback request (e.g. plan not found, validation error).
func RenderRollbackNotAccepted(database, environment, errorMessage string) string {
	header := fmt.Sprintf("## Rollback Not Accepted\n\n"+
		"**Database**: `%s` | **Environment**: `%s`\n\n", database, environment)
	if msg := SanitizeInlineError(errorMessage); msg != "" {
		return header + "The rollback was not accepted: " + html.EscapeString(msg)
	}
	return header + "The rollback was not accepted."
}
