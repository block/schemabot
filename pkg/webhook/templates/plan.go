package templates

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/statement"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/ui"
)

// LintViolationData represents a structured lint warning for template rendering.
type LintViolationData struct {
	Message    string
	Table      string
	LinterName string
	CanAutoFix bool
}

// UnsafeChangeData represents a destructive schema change for template rendering.
type UnsafeChangeData struct {
	Table  string
	Reason string
	// Shards names the shards this unsafe change applies to, for a sharded plan
	// where only some shards carry it. Empty for a non-sharded change (applies to
	// the whole table).
	Shards []string
}

// PlanCommentData contains all data needed to render a plan comment.
type PlanCommentData struct {
	Database     string
	SchemaName   string // Schema directory name (e.g. filepath.Base of schema dir)
	Environment  string
	Tenant       string
	HeadSHA      string
	Repository   string
	RequestedBy  string // Empty means auto-generated
	DatabaseType string
	IsMySQL      bool
	ApplyID      string

	Changes        []KeyspaceChangeData
	LintViolations []LintViolationData
	Errors         []string

	// Unsafe change tracking
	HasUnsafeChanges bool
	AllowUnsafe      bool
	UnsafeChanges    []UnsafeChangeData

	// Options
	DeferCutover bool
	SkipRevert   bool

	// Lock state (set when rendering apply-plan comments)
	IsLocked     bool
	LockOwner    string
	LockAcquired string // formatted timestamp

	// Automatic apply state
	AutoConfirmDowngradeReason string // Non-empty when automatic apply downgraded to manual confirmation

	RecoveredApplyOwnedCheckState bool
}

// KeyspaceChangeData contains changes for a single keyspace/schema.
type KeyspaceChangeData struct {
	Keyspace       string
	Statements     []string
	VSchemaChanged bool
	VSchemaDiff    string

	// Shards carries this keyspace's per-shard changes for a sharded plan. When
	// set, the DDL is rendered per shard-group ("what applies where") instead of
	// the single Statements block — so a keyspace whose shards diverge is shown
	// faithfully. Empty for a non-sharded keyspace.
	Shards []KeyspaceShardChange
}

// KeyspaceShardChange is one shard's planned statements within a keyspace.
type KeyspaceShardChange struct {
	Shard      string
	Statements []string
	// Satisfied marks a shard that already matches the desired schema while
	// sibling shards in the keyspace change — a partially-applied keyspace. It
	// carries no Statements and renders as an "already applied" group, so the
	// plan comment shows the divergent state rather than hiding the shard.
	Satisfied bool
}

// RenderPlanComment renders the plan comment markdown.
func RenderPlanComment(data PlanCommentData) string {
	var sb strings.Builder

	// Header
	if data.IsLocked {
		writeEnvironmentTitle(&sb, "Schema Change Apply", data.Environment)
	} else {
		writeEnvironmentTitle(&sb, "Schema Change Plan", data.Environment)
	}

	writePlanMetadata(&sb, data)
	writePlanAttribution(&sb, data)

	if data.IsLocked && data.LockOwner != "" {
		fmt.Fprintf(&sb, "\n🔒 **Lock acquired by** `%s`", data.LockOwner)
		if data.LockAcquired != "" {
			fmt.Fprintf(&sb, " at %s", data.LockAcquired)
		}
		sb.WriteString("\n")
	}

	sb.WriteString("\n")

	// Count changes
	totalStatements, keyspacesWithVSchema := countChanges(data.Changes)
	totalChanges := totalStatements + keyspacesWithVSchema

	// No changes — short-circuit with a single clean message
	if totalChanges == 0 {
		writeNoChangesDetected(&sb, data)
		return sb.String()
	}

	// Detailed changes
	writeKeyspaceChanges(&sb, data)

	// Unsafe changes warning — shown on the plan comment for review, omitted on
	// the locked apply comment: unsafe changes only reach an apply after the
	// operator acknowledged them with --allow-unsafe (apply-confirm re-checks
	// and blocks otherwise), so repeating them there is noise.
	if data.HasUnsafeChanges && len(data.UnsafeChanges) > 0 && !data.IsLocked {
		writeUnsafeWarning(&sb, data.UnsafeChanges, data.IsMySQL)
	}

	// Lint violations — shown on the plan comment for review, omitted on the
	// locked apply comment where they are noise (the operator already reviewed
	// them at plan time).
	if len(data.LintViolations) > 0 && !data.IsLocked {
		writeLintViolations(&sb, data.LintViolations)
	}

	// Errors
	if len(data.Errors) > 0 {
		writeErrors(&sb, data.Errors)
	}

	// Summary and options (after DDL, matching CLI layout)
	writePlanSummary(&sb, data, totalStatements, keyspacesWithVSchema)
	writeOptions(&sb, data)

	// Footer
	sb.WriteString("\n---\n\n")

	if data.IsLocked {
		applyConfirmCmd := fmt.Sprintf("schemabot apply-confirm -e %s", data.Environment)
		if data.Tenant != "" {
			applyConfirmCmd += fmt.Sprintf(" --tenant %s", data.Tenant)
		}
		if data.AllowUnsafe {
			applyConfirmCmd += " --allow-unsafe"
		}
		if data.DeferCutover {
			applyConfirmCmd += " --defer-cutover"
		}
		if data.SkipRevert {
			applyConfirmCmd += " --skip-revert"
		}

		if data.AutoConfirmDowngradeReason != "" {
			// Automatic apply was downgraded to manual confirmation — show unlock since user needs to act
			fmt.Fprintf(&sb, "⚠️ **Automatic apply paused**: %s\n\n", data.AutoConfirmDowngradeReason)
			sb.WriteString("Review the plan above, then confirm manually:\n")
			fmt.Fprintf(&sb, "```\n%s\n```\n", applyConfirmCmd)
			sb.WriteString("\n🔓 To discard this plan and unlock, comment:\n")
			sb.WriteString("```\nschemabot unlock\n```\n")
		} else {
			// Automatic apply is proceeding. No unlock hint — it's noise on the
			// happy path; the operator can still unlock from the CLI if needed.
			sb.WriteString("**Applying automatically**\n")
		}
	} else {
		applyCmd := fmt.Sprintf("schemabot apply -e %s", data.Environment)
		if data.Tenant != "" {
			applyCmd += fmt.Sprintf(" --tenant %s", data.Tenant)
		}
		writeApplyHint(&sb, applyCmd)
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
	parts = append(parts, fmt.Sprintf("**Type**: `%s`", schemaChangePlanDatabaseTypeLabel(data.DatabaseType, data.IsMySQL)))
	if data.IsMySQL && data.SchemaName != "" {
		parts = append(parts, fmt.Sprintf("**Schema Name**: `%s`", data.SchemaName))
	}
	if data.Tenant != "" {
		parts = append(parts, fmt.Sprintf("**Tenant**: `%s`", data.Tenant))
	}
	fmt.Fprintf(sb, "%s\n", strings.Join(parts, " | "))
}

func writePlanAttribution(sb *strings.Builder, data PlanCommentData) {
	writeAttributionLineWithSuffix(sb, "Requested", data.RequestedBy, planCommitSuffix(data.Repository, data.HeadSHA))
}

func planCommitSuffix(repository, sha string) string {
	if sha == "" {
		return ""
	}
	return fmt.Sprintf(" · planned from %s", formatCommitRef(repository, sha))
}

func formatCommitRef(repository, sha string) string {
	short := shortSHA(sha)
	if repository == "" {
		return fmt.Sprintf("`%s`", short)
	}
	return fmt.Sprintf("[`%s`](https://github.com/%s/commit/%s)", short, repository, sha)
}

func shortSHA(sha string) string {
	if len(sha) <= 7 {
		return sha
	}
	return sha[:7]
}

// writeOptions writes the options line if any options are enabled.
func writeOptions(sb *strings.Builder, data PlanCommentData) {
	var opts []string
	if data.DeferCutover {
		opts = append(opts, "⏸️ Defer Cutover")
	}
	if data.SkipRevert {
		opts = append(opts, "⏩ Skip Revert")
	}
	if len(opts) > 0 {
		fmt.Fprintf(sb, "\n**Options**: %s\n", strings.Join(opts, " | "))
	}
}

func countChanges(changes []KeyspaceChangeData) (totalStatements, keyspacesWithVSchema int) {
	for _, ks := range changes {
		totalStatements += keyspaceStatementCount(ks)
		if ks.VSchemaChanged {
			keyspacesWithVSchema++
		}
	}
	return
}

// keyspaceStatementCount counts a keyspace's DDL statements for the summary and
// the no-changes short-circuit. It prefers the collapsed namespace-level
// Statements; when those are absent but the keyspace carries per-shard changes,
// it counts the distinct statements across shards, so a sharded plan whose only
// DDL is per-shard is never miscounted as "no changes".
func keyspaceStatementCount(ks KeyspaceChangeData) int {
	if len(ks.Statements) > 0 {
		return len(ks.Statements)
	}
	seen := make(map[string]struct{})
	for _, sh := range ks.Shards {
		for _, stmt := range sh.Statements {
			seen[stmt] = struct{}{}
		}
	}
	return len(seen)
}

func writePlanSummary(sb *strings.Builder, data PlanCommentData, totalStatements, keyspacesWithVSchema int) {
	totalChanges := totalStatements + keyspacesWithVSchema
	if totalChanges == 0 {
		writeNoChangesDetected(sb, data)
		sb.WriteString("\n")
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

func writeNoChangesDetected(sb *strings.Builder, data PlanCommentData) {
	sb.WriteString("✅ **No schema changes detected**\n")
	if data.RecoveredApplyOwnedCheckState {
		sb.WriteString("\nℹ️ SchemaBot found stored PR check state for this database/environment that was still marked as an apply in progress. Because this fresh plan shows the target schema already matches this PR, SchemaBot updated the PR check to passing.\n")
	}
}

// SummarizeChanges renders a compact one-line summary of a plan's changes for
// the aggregate check's Change column, e.g. "5 creates, 3 alters, 1 drop ·
// 2 vschema updates". Each category is a pluralized noun so the phrasing stays
// consistent with the vschema clause. Zero categories are omitted. The vschema
// clause is only included for non-MySQL engines, matching the plan comment's
// summary. Returns "" only when the plan has no changes at all. The
// create/alter/drop and vschema counting is identical to the plan comment's
// summary (countStatementTypes / countChanges) so the two always agree.
func SummarizeChanges(data PlanCommentData) string {
	creates, alters, drops := countStatementTypes(data.Changes)
	totalStatements, keyspacesWithVSchema := countChanges(data.Changes)

	var parts []string
	if creates > 0 {
		parts = append(parts, fmt.Sprintf("%d %s", creates, pluralize("create", creates)))
	}
	if alters > 0 {
		parts = append(parts, fmt.Sprintf("%d %s", alters, pluralize("alter", alters)))
	}
	if drops > 0 {
		parts = append(parts, fmt.Sprintf("%d %s", drops, pluralize("drop", drops)))
	}
	ddlSummary := strings.Join(parts, ", ")

	// Fallback matching the plan comment: statements that classify as none of
	// create/alter/drop — or per-shard-only DDL that countStatementTypes does not
	// walk — still count. Report the raw statement total so the Change column
	// never implies "no changes" for a plan that has them.
	if ddlSummary == "" && totalStatements > 0 {
		ddlSummary = fmt.Sprintf("%d DDL %s", totalStatements, pluralize("statement", totalStatements))
	}

	if keyspacesWithVSchema > 0 && !data.IsMySQL {
		vschemaSummary := fmt.Sprintf("%d vschema %s", keyspacesWithVSchema, pluralize("update", keyspacesWithVSchema))
		if ddlSummary == "" {
			return vschemaSummary
		}
		return ddlSummary + " · " + vschemaSummary
	}
	return ddlSummary
}

// countStatementTypes counts CREATE, ALTER, and DROP statements across all keyspaces.
func countStatementTypes(changes []KeyspaceChangeData) (creates, alters, drops int) {
	for _, ks := range changes {
		for _, stmt := range ks.Statements {
			stmtType, _, err := ddl.ClassifyStatement(stmt)
			if err != nil {
				continue
			}
			switch stmtType {
			case statement.StatementCreateTable:
				creates++
			case statement.StatementAlterTable:
				alters++
			case statement.StatementDropTable:
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
		hasDDLChanges := len(ks.Statements) > 0 || len(ks.Shards) > 0
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
			if len(ks.Shards) > 0 {
				writeShardedPlanDDL(sb, ks.Shards)
			} else {
				writePlanDDLBlock(sb, ks.Statements)
			}
		}
	}
}

// writePlanDDLBlock writes a single fenced SQL block of statements.
func writePlanDDLBlock(sb *strings.Builder, statements []string) {
	sb.WriteString("```sql\n")
	for i, stmt := range statements {
		sb.WriteString(ddl.FormatDDL(stmt))
		if i < len(statements)-1 {
			sb.WriteString("\n\n")
		} else {
			sb.WriteString("\n")
		}
	}
	sb.WriteString("```\n\n")
}

// writeShardedPlanDDL renders a sharded keyspace's DDL grouped by change: shards
// that need the same statements share one block, so a uniform keyspace shows the
// DDL once and a divergent one shows "what applies where" — each distinct change
// set with the shards it applies to.
func writeShardedPlanDDL(sb *strings.Builder, shards []KeyspaceShardChange) {
	groups := groupKeyspaceShardsByStatements(shards)
	if len(groups) <= 1 {
		// A single group of changing shards shows the DDL once, but still names the
		// shards it applies to — a sharded plan must always show which shards are
		// affected, even when the change is uniform across them. A lone group of
		// satisfied shards means nothing is changing, so render nothing rather than
		// an empty code block.
		if len(groups) == 1 && !groups[0].Satisfied {
			fmt.Fprintf(sb, "**%s**\n\n", planShardList(groups[0].Shards))
			writePlanDDLBlock(sb, groups[0].Statements)
		}
		return
	}
	sb.WriteString("Shards diverge — what applies where:\n\n")
	for _, g := range groups {
		fmt.Fprintf(sb, "**%s**\n\n", planShardList(g.Shards))
		// A satisfied group already matches the desired schema; say so instead
		// of rendering an empty code block.
		if g.Satisfied {
			sb.WriteString("_Already applied — no change._\n\n")
			continue
		}
		writePlanDDLBlock(sb, g.Statements)
	}
}

type keyspaceShardGroup struct {
	Shards     []string
	Statements []string
	Satisfied  bool
}

// groupKeyspaceShardsByStatements buckets shards whose statement set and
// satisfied status are identical, preserving resolved order, so a uniform
// keyspace yields one group.
func groupKeyspaceShardsByStatements(shards []KeyspaceShardChange) []keyspaceShardGroup {
	var order []string
	bySig := make(map[string]*keyspaceShardGroup)
	for _, s := range shards {
		sig := shardGroupSignature(s)
		g := bySig[sig]
		if g == nil {
			g = &keyspaceShardGroup{Statements: s.Statements, Satisfied: s.Satisfied}
			bySig[sig] = g
			order = append(order, sig)
		}
		g.Shards = append(g.Shards, s.Shard)
	}
	groups := make([]keyspaceShardGroup, 0, len(order))
	for _, sig := range order {
		groups = append(groups, *bySig[sig])
	}
	return groups
}

// shardGroupSignature keys shards into the same group only when they carry the
// same planned statements and the same satisfied status. Keying on Satisfied —
// not just an empty statement set — keeps a satisfied shard from ever merging
// with a changing shard, and ensures only a shard explicitly marked satisfied
// renders as "already applied".
func shardGroupSignature(s KeyspaceShardChange) string {
	status := "change"
	if s.Satisfied {
		status = "satisfied"
	}
	return status + "\x02" + strings.Join(s.Statements, "\x01")
}

// planShardList renders a group's shards as "shard `x`" or "shards `x`, `y`".
func planShardList(shards []string) string {
	quoted := make([]string, len(shards))
	for i, s := range shards {
		quoted[i] = fmt.Sprintf("`%s`", s)
	}
	if len(quoted) == 1 {
		return "shard " + quoted[0]
	}
	return "shards " + strings.Join(quoted, ", ")
}

func writeUnsafeWarning(sb *strings.Builder, changes []UnsafeChangeData, isMySQL bool) {
	sb.WriteString("**⛔ Unsafe Changes Detected:**\n")
	for _, c := range changes {
		table := "`" + c.Table + "`"
		if len(c.Shards) > 0 {
			table = fmt.Sprintf("%s (%s)", table, planShardList(c.Shards))
		}
		reason := ui.CleanLintReason(c.Reason)
		if reason != "" {
			fmt.Fprintf(sb, "- %s: %s\n", table, reason)
		} else {
			fmt.Fprintf(sb, "- %s\n", table)
		}
	}
	sb.WriteString("\n")
	writeUnsafeDropGuidance(sb, changes, isMySQL)
}

func writeUnsafeDropGuidance(sb *strings.Builder, changes []UnsafeChangeData, isMySQL bool) {
	applicationUsageTarget, hasApplicationUsageTarget := unsafeDropApplicationUsageTarget(changes)
	indexActionTarget, indexInvisibleTarget, indexQueryTarget, hasIndexUsageTarget := unsafeDropIndexUsageTargets(changes)
	if !hasApplicationUsageTarget && !hasIndexUsageTarget {
		return
	}

	sb.WriteString("**Destructive drop guidance:**\n\n")
	if hasApplicationUsageTarget {
		fmt.Fprintf(sb, "Before allowing a destructive drop, first deploy application code that no longer reads from or writes to %s.\n\n", applicationUsageTarget)
	}
	if hasIndexUsageTarget {
		if isMySQL {
			fmt.Fprintf(sb, "Before dropping %s in MySQL, first make %s invisible and verify application queries no longer rely on %s for safe performance.\n\n", indexActionTarget, indexInvisibleTarget, indexQueryTarget)
		} else {
			fmt.Fprintf(sb, "Before allowing a destructive drop, verify application queries no longer rely on %s for safe performance.\n\n", indexInvisibleTarget)
		}
	}
}

func unsafeDropApplicationUsageTarget(changes []UnsafeChangeData) (string, bool) {
	dropColumns := 0
	dropTables := 0
	for _, change := range changes {
		upperReason := strings.ToUpper(change.Reason)
		dropColumns += strings.Count(upperReason, "DROP COLUMN")
		dropTables += strings.Count(upperReason, "DROP TABLE")
	}

	if dropColumns > 1 && dropTables > 1 {
		return "any dropped tables or columns", true
	}
	if dropColumns == 1 && dropTables == 1 {
		return "the dropped table and column", true
	}
	if dropColumns == 1 && dropTables > 1 {
		return "any dropped tables and the dropped column", true
	}
	if dropColumns > 1 && dropTables == 1 {
		return "the dropped table and any dropped columns", true
	}
	if dropColumns == 1 {
		return "the dropped column", true
	}
	if dropColumns > 1 {
		return "any dropped columns", true
	}
	if dropTables == 1 {
		return "the dropped table", true
	}
	if dropTables > 1 {
		return "any dropped tables", true
	}
	return "", false
}

func unsafeDropIndexUsageTargets(changes []UnsafeChangeData) (actionTarget, invisibleTarget, queryTarget string, ok bool) {
	dropIndexes := 0
	for _, change := range changes {
		dropIndexes += strings.Count(strings.ToUpper(change.Reason), "DROP INDEX")
	}

	if dropIndexes == 1 {
		return "an index", "the dropped index", "it", true
	}
	if dropIndexes > 1 {
		return "indexes", "any dropped indexes", "them", true
	}
	return "", "", "", false
}

func writeLintViolations(sb *strings.Builder, warnings []LintViolationData) {
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
	Database     string
	SchemaName   string
	HeadSHA      string
	Repository   string
	DatabaseType string
	IsMySQL      bool
	RequestedBy  string
	Tenant       string

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
	if plan, ok := singleEnvironmentPlan(data); ok {
		return RenderPlanComment(plan)
	}

	var sb strings.Builder

	// Header
	writeEnvironmentTitle(&sb, "Schema Change Plan", singleEnvironmentTitleEnvironment(data.Environments))

	writePlanMetadata(&sb, PlanCommentData{Database: data.Database, SchemaName: data.SchemaName, DatabaseType: data.DatabaseType, IsMySQL: data.IsMySQL, Tenant: data.Tenant})
	writePlanAttribution(&sb, PlanCommentData{
		HeadSHA:     data.HeadSHA,
		Repository:  data.Repository,
		RequestedBy: data.RequestedBy,
	})
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
		writeEnvironmentPlanSection(&sb, data.Plans[data.Environments[0]])
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

			writeEnvironmentPlanSection(&sb, plan)
		}
	}

	// Footer with apply instructions
	sb.WriteString("---\n\n")
	writeMultiEnvFooter(&sb, data)

	return sb.String()
}

func singleEnvironmentPlan(data MultiEnvPlanCommentData) (PlanCommentData, bool) {
	if len(data.Environments) != 1 || len(data.Errors) > 0 {
		return PlanCommentData{}, false
	}
	environment := data.Environments[0]
	plan, ok := data.Plans[environment]
	if !ok || plan == nil {
		return PlanCommentData{}, false
	}
	merged := *plan
	if merged.Database == "" {
		merged.Database = data.Database
	}
	if merged.SchemaName == "" {
		merged.SchemaName = data.SchemaName
	}
	if merged.Environment == "" {
		merged.Environment = environment
	}
	if merged.HeadSHA == "" {
		merged.HeadSHA = data.HeadSHA
	}
	if merged.Repository == "" {
		merged.Repository = data.Repository
	}
	if merged.DatabaseType == "" {
		merged.DatabaseType = data.DatabaseType
	}
	if !merged.IsMySQL {
		merged.IsMySQL = data.IsMySQL
	}
	if merged.RequestedBy == "" {
		merged.RequestedBy = data.RequestedBy
	}
	if merged.Tenant == "" {
		merged.Tenant = data.Tenant
	}
	return merged, true
}

func singleEnvironmentTitleEnvironment(environments []string) string {
	if len(environments) != 1 {
		return ""
	}
	return environments[0]
}

func schemaChangePlanDatabaseTypeLabel(databaseType string, isMySQL bool) string {
	databaseType = strings.TrimSpace(databaseType)
	switch databaseType {
	case storage.DatabaseTypeMySQL:
		return "MySQL"
	case storage.DatabaseTypeStrata:
		return "Strata"
	case storage.DatabaseTypeVitess:
		return "Vitess"
	case "postgres", "postgresql":
		return "PostgreSQL"
	}
	if databaseType != "" {
		if label := titleDatabaseType(databaseType); label != "" {
			return label
		}
	}
	if isMySQL {
		return "MySQL"
	}
	return "Vitess"
}

func titleDatabaseType(databaseType string) string {
	return titleLabel(databaseType)
}

// writeEnvironmentPlanSection writes the plan body for a single environment within a multi-env comment.
func writeEnvironmentPlanSection(sb *strings.Builder, plan *PlanCommentData) {
	totalStatements, keyspacesWithVSchema := countChanges(plan.Changes)
	totalChanges := totalStatements + keyspacesWithVSchema

	if totalChanges == 0 {
		sb.WriteString("✅ **No schema changes detected**\n\n")
		return
	}

	// Detailed changes, collapsed so the DDL doesn't dominate the comment while
	// the unsafe/lint warnings and summary below stay visible at a glance.
	writeCollapsibleKeyspaceChanges(sb, *plan, totalStatements)

	// Unsafe changes warning
	if plan.HasUnsafeChanges && len(plan.UnsafeChanges) > 0 {
		writeUnsafeWarning(sb, plan.UnsafeChanges, plan.IsMySQL)
	}

	// Lint violations
	if len(plan.LintViolations) > 0 {
		writeLintViolations(sb, plan.LintViolations)
	}

	// Errors
	if len(plan.Errors) > 0 {
		writeErrors(sb, plan.Errors)
	}

	// Summary (after DDL, matching CLI layout)
	writePlanSummary(sb, *plan, totalStatements, keyspacesWithVSchema)
}

// writeCollapsibleKeyspaceChanges renders a plan's changes — DDL, plus VSchema
// diffs for non-MySQL keyspaces — inside a collapsed <details> block. The
// summary line carries the statement count so reviewers can gauge the size of
// the change without expanding it.
func writeCollapsibleKeyspaceChanges(sb *strings.Builder, plan PlanCommentData, totalStatements int) {
	summary := "Show changes"
	if totalStatements > 0 {
		summary = fmt.Sprintf("Show SQL (%d %s)", totalStatements, pluralize("statement", totalStatements))
	}
	fmt.Fprintf(sb, "<details>\n<summary>%s</summary>\n\n", summary)
	writeKeyspaceChanges(sb, plan)
	sb.WriteString("</details>\n\n")
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
		fmt.Fprintf(sb, "```\n%s\n```\n", tenantCommand("schemabot apply", envsWithChanges[0], data.Tenant))
		for i := 1; i < len(envsWithChanges); i++ {
			fmt.Fprintf(sb, "\nAfter verifying %s, apply to %s:\n", envsWithChanges[i-1], envsWithChanges[i])
			fmt.Fprintf(sb, "```\n%s\n```\n", tenantCommand("schemabot apply", envsWithChanges[i], data.Tenant))
		}
	case len(envsWithChanges) == 1:
		sb.WriteString("💡 **To apply** these changes, comment:\n")
		fmt.Fprintf(sb, "```\n%s\n```\n", tenantCommand("schemabot apply", envsWithChanges[0], data.Tenant))
	case len(envsWithErrors) == 0:
		sb.WriteString("No changes to apply.\n")
	}

	// Error guidance for failed environments
	if len(envsWithErrors) > 0 {
		sb.WriteString("\n")
		for _, env := range envsWithErrors {
			fmt.Fprintf(sb, "⚠️ **%s** failed to plan. Resolve the error above and re-run:\n", capitalizeFirst(env))
			fmt.Fprintf(sb, "```\n%s\n```\n", tenantCommand("schemabot plan", env, data.Tenant))
		}
	}
}

func tenantCommand(baseCommand, environment, tenant string) string {
	return appendTenantFlag(fmt.Sprintf("%s -e %s", baseCommand, environment), tenant)
}

// appendTenantFlag appends the --tenant flag to a pasteable command hint when
// tenant is set. In tenant mode, commands without an explicit tenant target
// are ignored, so every command hint a user may copy-paste must carry the
// deployment's tenant.
func appendTenantFlag(command, tenant string) string {
	if tenant == "" {
		return command
	}
	return fmt.Sprintf("%s --tenant %s", command, tenant)
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
