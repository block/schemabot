package templates

import (
	"fmt"
	"strings"
)

// RenderRollbackPlanComment renders the rollback plan comment markdown.
// Reuses PlanCommentData since rollback plans have the same structure as regular plans.
func RenderRollbackPlanComment(data PlanCommentData) string {
	var sb strings.Builder

	// Header
	dbTypeLabel := "Vitess"
	if data.IsMySQL {
		dbTypeLabel = "MySQL"
	}
	fmt.Fprintf(&sb, "## %s Schema Rollback Plan\n\n", dbTypeLabel)

	writePlanMetadata(&sb, data)
	writeRequesterOrTimestamp(&sb, data.RequestedBy)
	sb.WriteString("\n")

	// Count changes
	totalStatements, keyspacesWithVSchema := countChanges(data.Changes)
	totalChanges := totalStatements + keyspacesWithVSchema

	// Summary
	if totalChanges == 0 {
		sb.WriteString("**No schema changes detected** — the database already matches the original schema.\n\n")
		return sb.String()
	}

	// Detailed changes
	writeKeyspaceChanges(&sb, data)

	// Unsafe warning — rollback typically produces DROP operations
	sb.WriteString("> **Warning**: Rollback may include destructive changes (e.g., DROP INDEX, DROP COLUMN). These will be applied automatically.\n\n")

	// Lint violations
	if len(data.LintViolations) > 0 {
		writeLintViolations(&sb, data.LintViolations)
	}

	// Errors
	if len(data.Errors) > 0 {
		writeErrors(&sb, data.Errors)
	}

	// Summary (after DDL, matching CLI layout)
	writePlanSummary(&sb, data, totalStatements, keyspacesWithVSchema)

	// Footer
	sb.WriteString("---\n\n")
	sb.WriteString("To confirm this rollback, comment:\n")
	fmt.Fprintf(&sb, "```\nschemabot rollback-confirm -e %s\n```\n\n", data.Environment)
	sb.WriteString("To cancel, comment:\n")
	fmt.Fprintf(&sb, "```\nschemabot unlock\n```\n")

	return sb.String()
}

// RenderRollbackNoCompletedApply renders a message when there is no completed
// schema change to roll back.
func RenderRollbackNoCompletedApply(database, environment string) string {
	return fmt.Sprintf("## ℹ️ No Completed Schema Change to Rollback\n\n"+
		"**Database**: `%s` | **Environment**: `%s`\n\n"+
		"There is no completed schema change with stored original schema to roll back to.\n"+
		"Rollback requires a previous `apply` that completed successfully.",
		database, environment)
}

// RenderRollbackConfirmNoLock renders a message when rollback-confirm is run
// without a held lock.
func RenderRollbackConfirmNoLock(database, environment string) string {
	if database == "" {
		return fmt.Sprintf("## 🔒 No Lock Found\n\n"+
			"**Environment**: `%s`\n\n"+
			"No rollback lock is held by this PR. Run `schemabot rollback <apply-id> -e %s` first to generate a rollback plan.",
			environment, environment)
	}
	return fmt.Sprintf("## 🔒 No Lock Found\n\n"+
		"**Database**: `%s` | **Environment**: `%s`\n\n"+
		"No rollback lock is held. Run `schemabot rollback <apply-id> -e %s` first to generate a rollback plan.",
		database, environment, environment)
}

// RenderRollbackMissingApplyID renders the message posted when `schemabot rollback`
// is invoked without an apply ID argument.
func RenderRollbackMissingApplyID() string {
	return "## Missing Apply ID\n\n" +
		"Usage: `schemabot rollback <apply-id> -e <environment>`\n\n" +
		"Confirm a generated rollback with `schemabot rollback-confirm -e <environment>`.\n\n" +
		"You can find the apply ID in the summary comment of a completed apply, " +
		"or by running `schemabot status`."
}

// RenderRollbackApplyNotFound renders the message posted when the supplied apply ID
// does not match any stored apply.
func RenderRollbackApplyNotFound(applyID string) string {
	return fmt.Sprintf("## Apply Not Found\n\n"+
		"No apply found with ID `%s`. Check the ID and try again.", applyID)
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

func sanitizedRollbackRejectionReason(reason string) string {
	fields := strings.Fields(reason)
	if len(fields) == 0 {
		return ""
	}
	return strings.ReplaceAll(strings.Join(fields, " "), "`", "'")
}

// RenderRollbackBlockedByLock renders the message posted when a rollback cannot
// acquire the database lock because another caller holds it. When lockRepo and
// lockPR are populated, the holder is rendered as a PR link; otherwise the bare
// owner string is shown.
func RenderRollbackBlockedByLock(database, environment, lockOwner, lockRepo string, lockPR int) string {
	if lockPR > 0 && lockRepo != "" {
		return fmt.Sprintf("## Rollback Blocked\n\n"+
			"**Database**: `%s` | **Environment**: `%s`\n\n"+
			"A lock is currently held by [%s#%d](https://github.com/%s/pull/%d).\n\n"+
			"Wait for that operation to complete, or ask the lock owner to run `schemabot unlock`.",
			database, environment,
			lockRepo, lockPR,
			lockRepo, lockPR)
	}
	return fmt.Sprintf("## Rollback Blocked\n\n"+
		"**Database**: `%s` | **Environment**: `%s`\n\n"+
		"A lock is currently held by `%s`.\n\n"+
		"Wait for that operation to complete, or ask the lock owner to release it.",
		database, environment, lockOwner)
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
		database, environment, lockOwner)
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
// operator releases it.
func RenderRollbackAlreadyRolledBackLockHeld(database, environment, lockOwner string) string {
	return fmt.Sprintf("## Already Rolled Back\n\n"+
		"**Database**: `%s` | **Environment**: `%s`\n\n"+
		"The database schema already matches the original state, but SchemaBot failed to release the lock held by `%s`. "+
		"Applies on this database will be blocked until the lock is released.\n\n"+
		"Release it by commenting:\n"+
		"```\nschemabot unlock\n```\n"+
		"If the lock persists, force-release it:\n"+
		"```\nschemabot unlock -d %s --force\n```",
		database, environment, lockOwner, database)
}

// RenderRollbackNotAccepted renders the message posted when the apply service
// rejects a rollback request (e.g. plan not found, validation error).
func RenderRollbackNotAccepted(database, environment, errorMessage string) string {
	return fmt.Sprintf("## Rollback Not Accepted\n\n"+
		"**Database**: `%s` | **Environment**: `%s`\n\n"+
		"The rollback was not accepted: %s",
		database, environment, errorMessage)
}
