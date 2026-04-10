package templates

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/ui"
)

// LintWarningData represents a structured lint warning for template rendering.
type LintWarningData struct {
	Message    string
	Table      string
	LinterName string
	CanAutoFix bool
}

// UnsafeChangeData represents a destructive schema change for template rendering.
type UnsafeChangeData struct {
	Table  string
	Reason string
}

// PlanCommentData contains all data needed to render a plan comment.
type PlanCommentData struct {
	Database    string
	SchemaName  string // Schema directory name (e.g. filepath.Base of schema dir)
	Environment string
	RequestedBy string // Empty means auto-generated
	IsMySQL     bool

	Changes      []KeyspaceChangeData
	LintWarnings []LintWarningData
	Errors       []string

	// Unsafe change tracking
	HasUnsafeChanges bool
	AllowUnsafe      bool
	UnsafeChanges    []UnsafeChangeData

	// Options
	DeferCutover bool
	EnableRevert bool

	// Lock state (set when rendering apply-plan comments)
	IsLocked     bool
	LockOwner    string
	LockAcquired string // formatted timestamp
}

// KeyspaceChangeData contains changes for a single keyspace/schema.
type KeyspaceChangeData struct {
	Keyspace       string
	Statements     []string
	VSchemaChanged bool
	VSchemaDiff    string
}

// RenderPlanComment renders the plan comment markdown.
func RenderPlanComment(data PlanCommentData) string {
	var sb strings.Builder

	// Header
	dbTypeLabel := "Vitess"
	if data.IsMySQL {
		dbTypeLabel = "MySQL"
	}
	if data.IsLocked {
		fmt.Fprintf(&sb, "## %s Schema Change Plan (Apply)\n\n", dbTypeLabel)
	} else {
		fmt.Fprintf(&sb, "## %s Schema Change Plan\n\n", dbTypeLabel)
	}

	writePlanMetadata(&sb, data)
	writeRequesterOrTimestamp(&sb, data.RequestedBy)

	if data.IsLocked && data.LockOwner != "" {
		fmt.Fprintf(&sb, "\n🔒 **Lock acquired by** `%s`", data.LockOwner)
		if data.LockAcquired != "" {
			fmt.Fprintf(&sb, " at %s", data.LockAcquired)
		}
		sb.WriteString("\n")
	}

	sb.WriteString("\n")

	// Count changes
	totalStatements, keyspacesWithDDL, keyspacesWithVSchema := countChanges(data.Changes)
	totalChanges := totalStatements + keyspacesWithVSchema

	// No changes — short-circuit with a single clean message
	if totalChanges == 0 {
		sb.WriteString("✅ **No schema changes detected**\n")
		return sb.String()
	}

	// Detailed changes
	writeKeyspaceChanges(&sb, data)

	// Unsafe changes warning
	if data.HasUnsafeChanges && len(data.UnsafeChanges) > 0 {
		writeUnsafeWarning(&sb, data.UnsafeChanges, data.AllowUnsafe)
	}

	// Lint warnings
	if len(data.LintWarnings) > 0 {
		writeLintWarnings(&sb, data.LintWarnings)
	}

	// Errors
	if len(data.Errors) > 0 {
		writeErrors(&sb, data.Errors)
	}

	// Summary and options (after DDL, matching CLI layout)
	writePlanSummary(&sb, data, totalStatements, keyspacesWithDDL, keyspacesWithVSchema)
	writeOptions(&sb, data)

	// Footer
	sb.WriteString("\n---\n\n")

	if data.IsLocked {
		// Apply-plan footer: confirm instructions + cancel
		applyConfirmCmd := fmt.Sprintf("schemabot apply-confirm -e %s", data.Environment)
		if data.AllowUnsafe {
			applyConfirmCmd += " --allow-unsafe"
		}
		if data.DeferCutover {
			applyConfirmCmd += " --defer-cutover"
		}
		if data.EnableRevert {
			applyConfirmCmd += " --enable-revert"
		}
		writeApplyHint(&sb, applyConfirmCmd)
		sb.WriteString("\n🔓 To discard this plan and unlock, comment:\n")
		sb.WriteString("```\nschemabot unlock\n```\n")
	} else {
		writeApplyHint(&sb, fmt.Sprintf("schemabot apply -e %s", data.Environment))
	}

	return sb.String()
}

// writeApplyHint writes the 💡 apply hint with the given command.
func writeApplyHint(sb *strings.Builder, command string) {
	sb.WriteString("💡 **To apply** all schema changes from this PR, comment:\n")
	fmt.Fprintf(sb, "```\n%s\n```\n", command)
}

// writePlanMetadata writes the metadata line for plan comments.
// Schema name (the schema directory) is shown for MySQL. Vitess uses keyspace headers instead.
func writePlanMetadata(sb *strings.Builder, data PlanCommentData) {
	parts := []string{fmt.Sprintf("**Database**: `%s`", data.Database)}
	if data.IsMySQL && data.SchemaName != "" {
		parts = append(parts, fmt.Sprintf("**Schema Name**: `%s`", data.SchemaName))
	}
	if data.Environment != "" {
		parts = append(parts, fmt.Sprintf("**Environment**: `%s`", data.Environment))
	}
	fmt.Fprintf(sb, "%s\n", strings.Join(parts, " | "))
}

// writeOptions writes the options line if any options are enabled.
func writeOptions(sb *strings.Builder, data PlanCommentData) {
	var opts []string
	if data.DeferCutover {
		opts = append(opts, "⏸️ Defer Cutover")
	}
	if data.EnableRevert {
		opts = append(opts, "⏪ Enable Revert")
	}
	if len(opts) > 0 {
		fmt.Fprintf(sb, "\n**Options**: %s\n", strings.Join(opts, " | "))
	}
}

func countChanges(changes []KeyspaceChangeData) (totalStatements, keyspacesWithDDL, keyspacesWithVSchema int) {
	for _, ks := range changes {
		if len(ks.Statements) > 0 {
			totalStatements += len(ks.Statements)
			keyspacesWithDDL++
		}
		if ks.VSchemaChanged {
			keyspacesWithVSchema++
		}
	}
	return
}

func writePlanSummary(sb *strings.Builder, data PlanCommentData, totalStatements, keyspacesWithDDL, keyspacesWithVSchema int) {
	totalChanges := totalStatements + keyspacesWithVSchema
	if totalChanges == 0 {
		sb.WriteString("✅ **No schema changes detected**\n\n")
		return
	}

	// Count statement types (Terraform-style: X to create, Y to alter, Z to drop)
	creates, alters, drops := countStatementTypes(data.Changes)

	var parts []string
	if creates > 0 {
		parts = append(parts, fmt.Sprintf("**%d** %s to create", creates, pluralize("table", creates)))
	}
	if alters > 0 {
		parts = append(parts, fmt.Sprintf("**%d** %s to alter", alters, pluralize("table", alters)))
	}
	if drops > 0 {
		parts = append(parts, fmt.Sprintf("**%d** %s to drop", drops, pluralize("table", drops)))
	}
	if keyspacesWithVSchema > 0 && !data.IsMySQL {
		parts = append(parts, fmt.Sprintf("**%d** vschema %s", keyspacesWithVSchema, pluralize("update", keyspacesWithVSchema)))
	}

	if len(parts) > 0 {
		fmt.Fprintf(sb, "📋 **Plan**: %s\n\n", strings.Join(parts, ", "))
	} else {
		// Fallback for unrecognized statement types
		fmt.Fprintf(sb, "📋 **Plan**: %d DDL %s\n\n", totalStatements, pluralize("statement", totalStatements))
	}
}

// countStatementTypes counts CREATE, ALTER, and DROP statements across all keyspaces.
func countStatementTypes(changes []KeyspaceChangeData) (creates, alters, drops int) {
	for _, ks := range changes {
		for _, stmt := range ks.Statements {
			upper := strings.ToUpper(strings.TrimSpace(stmt))
			switch {
			case strings.HasPrefix(upper, "CREATE "):
				creates++
			case strings.HasPrefix(upper, "ALTER "):
				alters++
			case strings.HasPrefix(upper, "DROP "):
				drops++
			}
		}
	}
	return
}

func writeKeyspaceChanges(sb *strings.Builder, data PlanCommentData) {
	// Skip the schema/keyspace heading when there's only one and it matches
	// the database name — it's redundant with the metadata line.
	singleKeyspace := len(data.Changes) == 1 && data.IsMySQL && data.Changes[0].Keyspace == data.Database

	for _, ks := range data.Changes {
		hasVSchemaChanges := ks.VSchemaChanged && !data.IsMySQL
		hasDDLChanges := len(ks.Statements) > 0
		if !hasDDLChanges && !hasVSchemaChanges {
			continue
		}

		if !singleKeyspace {
			label := "Keyspace"
			if data.IsMySQL {
				label = "Schema Name"
			}
			fmt.Fprintf(sb, "#### %s: `%s`\n", label, ks.Keyspace)
		}

		if hasVSchemaChanges {
			sb.WriteString("#### VSchema\n")
			if ks.VSchemaDiff != "" {
				sb.WriteString("```diff\n")
				sb.WriteString(ks.VSchemaDiff)
				sb.WriteString("\n```\n\n")
			} else {
				sb.WriteString("_(diff not available)_\n\n")
			}
		}

		if hasDDLChanges {
			sb.WriteString("```sql\n")
			for i, stmt := range ks.Statements {
				sb.WriteString(ddl.FormatDDL(stmt))
				if i < len(ks.Statements)-1 {
					sb.WriteString("\n\n")
				} else {
					sb.WriteString("\n")
				}
			}
			sb.WriteString("```\n\n")
		}
	}
}

func writeUnsafeWarning(sb *strings.Builder, changes []UnsafeChangeData, allowUnsafe bool) {
	if allowUnsafe {
		sb.WriteString("**🚨 Unsafe Changes** (`--allow-unsafe` enabled):\n")
	} else {
		sb.WriteString("**⛔ Unsafe Changes Detected:**\n")
	}
	for _, c := range changes {
		reason := ui.CleanLintReason(c.Reason)
		if reason != "" {
			fmt.Fprintf(sb, "- `%s`: %s\n", c.Table, reason)
		} else {
			fmt.Fprintf(sb, "- `%s`\n", c.Table)
		}
	}
	sb.WriteString("\n")
}

func writeLintWarnings(sb *strings.Builder, warnings []LintWarningData) {
	sb.WriteString("\u26a0\ufe0f **Lint Warnings**:\n")
	for _, w := range warnings {
		warningText := w.Message
		if w.Table != "" {
			warningText = fmt.Sprintf("[%s] %s", w.Table, w.Message)
		}
		fmt.Fprintf(sb, "- %s\n", warningText)
	}
	sb.WriteString("\n")
}

func writeErrors(sb *strings.Builder, errors []string) {
	sb.WriteString("**Errors**:\n")
	for _, errMsg := range errors {
		fmt.Fprintf(sb, "- %s\n", errMsg)
	}
	sb.WriteString("\n")
}

func pluralize(singular string, count int) string {
	if count == 1 {
		return singular
	}
	return singular + "s"
}

// MultiEnvPlanCommentData contains data for rendering a multi-environment plan
// in a single comment. Used when `schemabot plan` is run without `-e`.
type MultiEnvPlanCommentData struct {
	Database    string
	SchemaName  string
	IsMySQL     bool
	RequestedBy string

	// Environments in display order (staging first, production second, etc.)
	Environments []string

	// Plans per environment (nil entry means that environment had no plan result)
	Plans map[string]*PlanCommentData

	// Errors per environment (if plan execution failed)
	Errors map[string]string
}

// RenderMultiEnvPlanComment renders a combined plan comment showing all environments.
// If all environments have identical plans, deduplicates into a single section.
func RenderMultiEnvPlanComment(data MultiEnvPlanCommentData) string {
	var sb strings.Builder

	// Header
	dbTypeLabel := "Vitess"
	if data.IsMySQL {
		dbTypeLabel = "MySQL"
	}
	fmt.Fprintf(&sb, "## %s Schema Change Plan\n\n", dbTypeLabel)

	writePlanMetadata(&sb, PlanCommentData{Database: data.Database, SchemaName: data.SchemaName})
	writeRequesterOrTimestamp(&sb, data.RequestedBy)
	sb.WriteString("\n")

	// Check which environments have changes
	envsWithChanges := 0
	for _, env := range data.Environments {
		if plan, ok := data.Plans[env]; ok && plan != nil && hasChanges(plan.Changes) {
			envsWithChanges++
		}
	}
	hasErrors := len(data.Errors) > 0

	// If no environments have changes and no errors, show simple message
	if envsWithChanges == 0 && !hasErrors {
		sb.WriteString("✅ **No schema changes detected** for any environment.\n")
		return sb.String()
	}

	// Check if all environments have identical plans (for deduplication)
	if !hasErrors && envsWithChanges >= 2 && allPlansIdentical(data) {
		// Identical plans: render once with combined header
		fmt.Fprintf(&sb, "### %s\n\n", capitalizeEnvNames(data.Environments))
		writeEnvironmentPlanSection(&sb, data.Plans[data.Environments[0]], data.IsMySQL)
	} else {
		// Separate sections per environment
		for _, env := range data.Environments {
			fmt.Fprintf(&sb, "### %s\n\n", capitalizeFirst(env))

			if errMsg, hasErr := data.Errors[env]; hasErr {
				writeErrorBlock(&sb, errMsg)
				sb.WriteString("\n")
				continue
			}

			plan, ok := data.Plans[env]
			if !ok || plan == nil {
				sb.WriteString("No plan result.\n\n")
				continue
			}

			writeEnvironmentPlanSection(&sb, plan, data.IsMySQL)
		}
	}

	// Footer with apply instructions
	sb.WriteString("---\n\n")
	writeMultiEnvFooter(&sb, data)

	return sb.String()
}

// writeEnvironmentPlanSection writes the plan body for a single environment within a multi-env comment.
func writeEnvironmentPlanSection(sb *strings.Builder, plan *PlanCommentData, isMySQL bool) {
	totalStatements, keyspacesWithDDL, keyspacesWithVSchema := countChanges(plan.Changes)
	totalChanges := totalStatements + keyspacesWithVSchema

	if totalChanges == 0 {
		sb.WriteString("✅ **No schema changes detected**\n\n")
		return
	}

	// Detailed changes
	writeKeyspaceChanges(sb, *plan)

	// Unsafe changes warning
	if plan.HasUnsafeChanges && len(plan.UnsafeChanges) > 0 {
		writeUnsafeWarning(sb, plan.UnsafeChanges, plan.AllowUnsafe)
	}

	// Lint warnings
	if len(plan.LintWarnings) > 0 {
		writeLintWarnings(sb, plan.LintWarnings)
	}

	// Errors
	if len(plan.Errors) > 0 {
		writeErrors(sb, plan.Errors)
	}

	// Summary (after DDL, matching CLI layout)
	writePlanSummary(sb, *plan, totalStatements, keyspacesWithDDL, keyspacesWithVSchema)
}

// writeMultiEnvFooter writes the footer with apply commands and error guidance.
func writeMultiEnvFooter(sb *strings.Builder, data MultiEnvPlanCommentData) {
	// Categorize environments
	var envsWithChanges []string
	var envsWithErrors []string
	for _, env := range data.Environments {
		if _, hasErr := data.Errors[env]; hasErr {
			envsWithErrors = append(envsWithErrors, env)
		} else if plan, ok := data.Plans[env]; ok && plan != nil && hasChanges(plan.Changes) {
			envsWithChanges = append(envsWithChanges, env)
		}
	}

	// Apply instructions for environments with changes
	switch {
	case len(envsWithChanges) >= 2:
		sb.WriteString("💡 **To apply** these changes, start with the first environment:\n")
		fmt.Fprintf(sb, "```\nschemabot apply -e %s\n```\n", envsWithChanges[0])
		for i := 1; i < len(envsWithChanges); i++ {
			fmt.Fprintf(sb, "\nAfter verifying %s, apply to %s:\n", envsWithChanges[i-1], envsWithChanges[i])
			fmt.Fprintf(sb, "```\nschemabot apply -e %s\n```\n", envsWithChanges[i])
		}
	case len(envsWithChanges) == 1:
		sb.WriteString("💡 **To apply** these changes, comment:\n")
		fmt.Fprintf(sb, "```\nschemabot apply -e %s\n```\n", envsWithChanges[0])
	case len(envsWithErrors) == 0:
		sb.WriteString("No changes to apply.\n")
	}

	// Error guidance for failed environments
	if len(envsWithErrors) > 0 {
		sb.WriteString("\n")
		for _, env := range envsWithErrors {
			fmt.Fprintf(sb, "⚠️ **%s** failed to plan. Resolve the error above and re-run:\n", capitalizeFirst(env))
			fmt.Fprintf(sb, "```\nschemabot plan -e %s\n```\n", env)
		}
	}
}

// allPlansIdentical returns true if all environments have identical changes.
func allPlansIdentical(data MultiEnvPlanCommentData) bool {
	var firstPlan *PlanCommentData
	for _, env := range data.Environments {
		plan, ok := data.Plans[env]
		if !ok || plan == nil || !hasChanges(plan.Changes) {
			return false
		}
		if firstPlan == nil {
			firstPlan = plan
			continue
		}
		if !plansIdentical(firstPlan, plan) {
			return false
		}
	}
	return firstPlan != nil
}

// plansIdentical checks if two plans have the same DDL statements.
func plansIdentical(a, b *PlanCommentData) bool {
	if len(a.Changes) != len(b.Changes) {
		return false
	}
	for i, aChange := range a.Changes {
		bChange := b.Changes[i]
		if aChange.Keyspace != bChange.Keyspace {
			return false
		}
		if len(aChange.Statements) != len(bChange.Statements) {
			return false
		}
		for j, stmt := range aChange.Statements {
			if stmt != bChange.Statements[j] {
				return false
			}
		}
		if aChange.VSchemaChanged != bChange.VSchemaChanged || aChange.VSchemaDiff != bChange.VSchemaDiff {
			return false
		}
	}
	return true
}

// capitalizeEnvNames joins environment names with " & " and capitalizes each.
func capitalizeEnvNames(envs []string) string {
	caps := make([]string, len(envs))
	for i, env := range envs {
		caps[i] = capitalizeFirst(env)
	}
	return strings.Join(caps, " & ")
}

// hasChanges returns true if there are any schema changes.
func hasChanges(changes []KeyspaceChangeData) bool {
	for _, ks := range changes {
		if len(ks.Statements) > 0 || ks.VSchemaChanged {
			return true
		}
	}
	return false
}
