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
	writeRequesterOrTimestamp(&sb, data.RequestedBy)
	sb.WriteString("\n")

	// Count and show changes
	totalStatements, keyspacesWithDDL, keyspacesWithVSchema := countChanges(data.Changes)
	totalChanges := totalStatements + keyspacesWithVSchema

	if totalChanges > 0 {
		writeKeyspaceChanges(&sb, data)
	}

	writePlanSummary(&sb, data, totalStatements, keyspacesWithDDL, keyspacesWithVSchema)

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

	sb.WriteString("**🚨 To proceed with these destructive changes, re-run with `--allow-unsafe`:**\n")
	fmt.Fprintf(&sb, "```\nschemabot apply -e %s --allow-unsafe\n```\n", data.Environment)

	return sb.String()
}

// RenderApplyStarted renders a comment when an apply begins.
func RenderApplyStarted(data ApplyStatusCommentData) string {
	var sb strings.Builder

	sb.WriteString("## Schema Change In Progress\n\n")
	writeApplyMetadata(&sb, data)
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
		fmt.Fprintf(&sb, "```\nschemabot unlock -d %s -e %s --force\n```\n", data.Database, data.Environment)
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
