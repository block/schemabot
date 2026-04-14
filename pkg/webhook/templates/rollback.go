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
	totalStatements, keyspacesWithDDL, keyspacesWithVSchema := countChanges(data.Changes)
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
	writePlanSummary(&sb, data, totalStatements, keyspacesWithDDL, keyspacesWithVSchema)

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
	return fmt.Sprintf("## 🔒 No Lock Found\n\n"+
		"**Database**: `%s` | **Environment**: `%s`\n\n"+
		"No rollback lock is held. Run `schemabot rollback -e %s` first to generate a rollback plan.",
		database, environment, environment)
}
