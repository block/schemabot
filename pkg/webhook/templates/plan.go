package templates

import (
	"fmt"
	"html"
	"log/slog"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/caller"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/schema"
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
	// ChangeType is the engine's change type (e.g. "drop"), rendered when the
	// change carries no parseable reason so the finding still explains itself.
	ChangeType string
	// Shards names the shards this unsafe change applies to, for a sharded plan
	// where only some shards carry it. Empty for a non-sharded change (applies to
	// the whole table).
	Shards []string
	// TotalShards is how many shards the plan covers in the keyspace, so a
	// rendering too wide to name every shard can state coverage ("12 of 32
	// shards") instead of a bare count. Zero when unknown.
	TotalShards int
}

// BlockedChangeData is a planned change the engine deterministically refuses:
// an apply will fail on it, so the plan comment discloses it up front.
type BlockedChangeData struct {
	Table  string
	Reason string
	// Shards names the shards this blocked change applies to, for a sharded
	// plan where only some shards carry it. Empty for a non-sharded change.
	Shards []string
	// TotalShards is how many shards the plan covers in the keyspace, so a
	// rendering too wide to name every shard can state coverage ("12 of 32
	// shards") instead of a bare count. Zero when unknown.
	TotalShards int
}

// DirectChangeData is a planned change the database's direct execution policy
// routes to native MySQL DDL instead of the schema change engine. The plan
// comment discloses its semantics — blocking, not revertible — so the
// operator consents to them when confirming the apply.
type DirectChangeData struct {
	Table  string
	Reason string
	// Shards names the shards this direct change applies to, for a sharded
	// plan where only some shards carry it. Empty for a non-sharded change.
	Shards []string
	// TotalShards is how many shards the plan covers in the keyspace, so a
	// rendering too wide to name every shard can state coverage ("12 of 32
	// shards") instead of a bare count. Zero when unknown.
	TotalShards int
}

// AttributedChangeData is a table carrying a planned destructive change that
// stored task history attributes to a pull request other than the one being
// planned. Repository and PullRequest name the owner when the lookup resolved
// an open one; Unresolved marks a table whose ownership could not be
// established, which is annotated the same way — the lookup fails toward
// ownership rather than presenting a change SchemaBot cannot vouch for as one
// this pull request proposes.
type AttributedChangeData struct {
	Table       string
	Repository  string
	PullRequest int
	Unresolved  bool
	// OutsideUnsafeGate marks a destructive change the --allow-unsafe opt-in
	// never gated — one visible only on individual shards — so consent for it
	// was never solicited and the disclosure must not be dropped as already
	// consented to.
	OutsideUnsafeGate bool
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

	// AgentHint is the deployment's configured guidance for AI agents reading
	// the plan. Empty on deployments that configure none, which render an
	// unchanged comment.
	AgentHint string

	Changes        []KeyspaceChangeData
	LintViolations []LintViolationData
	Errors         []string

	// IgnoredNamespaces lists the namespaces whose schema files were excluded
	// from this plan by the repository's ignore_namespaces config — only entries
	// that actually removed a namespace, resolved and sorted. Disclosed on the
	// comment so a reviewer can tell "this namespace has no changes" apart from
	// "this namespace was withheld by config", which is what makes a PR that
	// introduces an ignore_namespaces entry visible in review.
	IgnoredNamespaces []string

	// Unsafe change tracking
	HasUnsafeChanges bool
	AllowUnsafe      bool
	UnsafeChanges    []UnsafeChangeData

	// Changes the engine refuses; the apply will fail on them.
	BlockedChanges []BlockedChangeData

	// Changes the direct execution policy routes to native MySQL DDL.
	DirectChanges []DirectChangeData

	// Unfinished copies already on the target that the apply will throw away
	// and copy again from the start.
	DiscardedCopies []ExistingCopyData

	// Unfinished copies already on the target, left behind by an apply that is
	// over, that the apply will resume.
	AdoptedCopies []ExistingCopyData

	// Unfinished copies still being made on the target right now, that the
	// apply will join rather than resume or restart.
	RunningCopies []ExistingCopyData

	// Tables carrying a destructive change that another pull request owns, or
	// whose ownership could not be established.
	AttributedChanges []AttributedChangeData

	// Options
	DeferCutover bool
	SkipRevert   bool

	// Lock state (set when rendering apply-plan comments)
	IsLocked     bool
	LockOwner    string
	LockAcquired string // formatted timestamp

	// Automatic apply state
	AutoConfirmDowngradeReason string // Non-empty when automatic apply downgraded to manual confirmation

	// StoppedConfirmedApply marks a downgrade that stopped an apply the
	// operator confirmed themselves rather than pausing an automatic one, so
	// the comment names what actually stopped.
	StoppedConfirmedApply bool

	RecoveredApplyOwnedCheckState bool

	// DeploymentDrift is the review-time rollup of how every configured
	// deployment compares to the reviewed primary plan. Nil for a single-target
	// database (nothing to compare) or when drift was not evaluated.
	DeploymentDrift *DeploymentDriftData
}

// DeploymentDriftData renders the review-time drift rollup in the PR preview: a
// uniform "same plan everywhere" line when every deployment matches, or a
// per-deployment breakdown when some deployment diverged from — or could not be
// confirmed against — the reviewed plan.
type DeploymentDriftData struct {
	// Deployments is every configured deployment in rollout order, primary first.
	Deployments []DeploymentDriftEntry
	// Clean is true only when every deployment matches the reviewed plan.
	Clean bool
	// Computed is false when the rollup itself could not be evaluated; the check
	// still fails closed, and the preview says the deployments are unverified.
	Computed bool
}

// DeploymentDriftEntry is one deployment's classification against the reviewed
// primary plan.
type DeploymentDriftEntry struct {
	Deployment string
	Primary    bool
	// Class is "match", "diverged", or "errored".
	Class string
	// Detail is a short human explanation for a diverged or errored deployment;
	// empty for a match.
	Detail string
}

// applyingWithoutConfirmation reports whether this comment announces an apply
// that is already running rather than one waiting on the operator: a locked
// comment with nothing pausing it. Nothing on such a comment is a question, so
// the disclosures above the footer state what the apply is doing instead of
// warning about what confirming would cost, and never offer a remedy that is
// already out of reach. The footer reads the same predicate, so the two cannot
// disagree about whether the reader still has a decision to make.
func (d PlanCommentData) applyingWithoutConfirmation() bool {
	return d.IsLocked && d.AutoConfirmDowngradeReason == ""
}

// downgradeHeading names what the comment stopped. An operator who issued
// apply-confirm themselves paused nothing automatic, so telling them an
// automatic apply was paused would describe a schema change that was never in
// flight.
func (d PlanCommentData) downgradeHeading() string {
	if d.StoppedConfirmedApply {
		return "Apply stopped"
	}
	return "Automatic apply paused"
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
		fmt.Fprintf(&sb, "\n🔒 **Lock acquired by** `%s`", caller.Short(data.LockOwner))
		if data.LockAcquired != "" {
			fmt.Fprintf(&sb, " at %s", data.LockAcquired)
		}
		sb.WriteString("\n")
	}

	sb.WriteString("\n")

	// Review-time deployment drift is shown before the change list — and before
	// the no-changes short-circuit — because a non-primary deployment can drift
	// even when the reviewed primary plan is a clean no-op.
	writeDeploymentDrift(&sb, data.DeploymentDrift)

	// Count changes
	totalStatements, keyspacesWithVSchema := countChanges(data.Changes)
	totalChanges := totalStatements + keyspacesWithVSchema

	// No changes — short-circuit with a single clean message. The
	// ignore_namespaces disclosure still renders: a no-changes result is
	// exactly where a reviewer needs to tell a withheld namespace apart from a
	// genuinely unchanged one.
	if totalChanges == 0 {
		writeNoChangesDetected(&sb, data)
		if len(data.IgnoredNamespaces) > 0 {
			sb.WriteString("\n")
			writeIgnoredNamespaces(&sb, data.IgnoredNamespaces)
		}
		return appendAgentHint(sb.String(), data.AgentHint)
	}

	// Detailed changes
	writeKeyspaceChanges(&sb, data)

	// Blocked changes — statements the engine refuses. Unlike unsafe changes,
	// these cannot be acknowledged away: the apply will fail on them. Shown on
	// the locked apply comment too, so the operator sees the guaranteed
	// failure before confirming.
	if len(data.BlockedChanges) > 0 {
		writeBlockedChanges(&sb, data.BlockedChanges)
	}

	// Destructive changes to tables another pull request owns — shown where the
	// reader still decides whether the apply proceeds, omitted on the
	// auto-applying locked comment: the disclosure coaches re-planning ("merge
	// that PR ... then re-plan"), which is noise once the apply is already
	// running.
	if len(data.AttributedChanges) > 0 && attributionStillActionable(data) {
		writeAttributedChanges(&sb, data.AttributedChanges)
	}

	// Direct-execution changes — statements the policy routes to native DDL.
	// Shown on the locked apply comment too: confirming the apply is the
	// operator's consent to their blocking, non-revertible semantics, so the
	// disclosure must sit on the comment the confirmation acts on.
	if len(data.DirectChanges) > 0 {
		writeDirectChanges(&sb, data.DirectChanges, data.DatabaseType, data.IsMySQL)
	}

	// Copies already on the target. Shown on the locked apply comment too:
	// discarding an unfinished copy destroys hours of work already done, so the
	// disclosure must sit on the comment the confirmation acts on. The copy is
	// read from the target at plan time, so it can appear on the apply comment
	// without having been on the plan comment that preceded it.
	if len(data.DiscardedCopies) > 0 {
		writeDiscardedCopies(&sb, data.DiscardedCopies, data.applyingWithoutConfirmation())
	}
	if len(data.AdoptedCopies) > 0 {
		writeAdoptedCopies(&sb, data.AdoptedCopies, data.applyingWithoutConfirmation())
	}
	if len(data.RunningCopies) > 0 {
		writeRunningCopies(&sb, data.RunningCopies, data.applyingWithoutConfirmation())
	}

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

	switch {
	case data.IsLocked:
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

		if !data.applyingWithoutConfirmation() {
			// Automatic apply was downgraded to manual confirmation — show unlock since user needs to act
			fmt.Fprintf(&sb, glyph.Attention+" **%s**: %s\n\n", data.downgradeHeading(), data.AutoConfirmDowngradeReason)
			sb.WriteString("Review the plan above, then confirm manually:\n")
			fmt.Fprintf(&sb, "```\n%s\n```\n", applyConfirmCmd)
			sb.WriteString("\n🔓 To discard this plan and unlock, comment:\n")
			sb.WriteString("```\nschemabot unlock\n```\n")
		} else {
			// Automatic apply is proceeding. No unlock hint — it's noise on the
			// happy path; the operator can still unlock from the CLI if needed.
			sb.WriteString("**Applying automatically**\n")
		}
	default:
		applyCmd := fmt.Sprintf("schemabot apply -e %s", data.Environment)
		if data.Tenant != "" {
			applyCmd += fmt.Sprintf(" --tenant %s", data.Tenant)
		}
		writeApplyInstruction(&sb, applyCmd)
	}

	return appendAgentHint(sb.String(), data.AgentHint)
}

// writeApplyInstruction writes the ▶️ apply instruction with the given command.
func writeApplyInstruction(sb *strings.Builder, command string) {
	sb.WriteString("▶️ **To apply** all schema changes from this PR, comment:\n")
	fmt.Fprintf(sb, "```\n%s\n```\n", command)
}

// attributionStillActionable reports whether the attributed-changes
// disclosure still informs a choice this comment's reader holds. The plan
// comment offers the apply command, and a locked comment downgraded to manual
// confirmation pauses for apply-confirm — both readers can still merge the
// owning pull request and re-plan instead of applying. Once the locked
// comment is applying automatically, that re-plan alternative is gone and the
// operator consented to the destruction through --allow-unsafe, so the
// disclosure is omitted — unless an attributed table never passed through the
// unsafe opt-in gate, where no consent was ever solicited and this comment is
// the operator's notice.
func attributionStillActionable(data PlanCommentData) bool {
	if !data.IsLocked || data.AutoConfirmDowngradeReason != "" {
		return true
	}
	for _, change := range data.AttributedChanges {
		if change.OutsideUnsafeGate {
			return true
		}
	}
	return false
}

// writeAttributedChanges writes the section for destructive changes to tables
// that stored task history attributes to another pull request. SchemaBot plans
// a full diff of the pull request's schema files against the live database, so
// what an unmerged pull request already applied reads as something this pull
// request wants gone. Reconciling the database to the declared schema is the
// operator's call to make; what the comment owes them is the attribution they
// cannot see from the DDL alone.
//
// Attribution is table-grained: stored task history records the table a task
// changed and nothing finer, so the notice names the table and the pull request
// that last changed it, never the specific column or index.
func writeAttributedChanges(sb *strings.Builder, changes []AttributedChangeData) {
	n := len(changes)
	fmt.Fprintf(sb, "🛑 **Check before applying**: %d %s SchemaBot cannot attribute to this PR\n", n, pluralize("destructive change", n))
	for _, d := range changes {
		if d.Unresolved {
			fmt.Fprintf(sb, "- `%s`: ownership could not be established; see server logs\n", d.Table)
			continue
		}
		// The owner named is the most recent open pull request that changed the
		// table, which is not necessarily the last one to change it: a later
		// change from a pull request that has since closed leaves no open claim
		// and is passed over.
		fmt.Fprintf(sb, "- `%s`: changed by %s, which is still open\n",
			d.Table, caller.PullRequestMarkdownLink(d.Repository, d.PullRequest))
	}
	sb.WriteString("\nA plan diffs this PR's schema files against the live database, so what another PR applied before merging reads here as something to remove. If that is not what you intend, merge that PR, or bring this PR's schema files up to date with it, then re-plan.\n\n")
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
		writeIgnoredNamespaces(sb, data.IgnoredNamespaces)
		return
	}

	// Count statement types (Terraform-style: X to create, Y to alter, Z to drop)
	creates, alters, drops := countStatementTypes(data.Changes, data.DatabaseType)

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
	// Last-resort total: when nothing classified as create/alter/drop — a plan
	// of only index or rename DDL, or per-shard-only DDL that
	// countStatementTypes does not walk — report the raw statement total so
	// the plan never reads as "no changes", and report it before the vschema
	// clause so a vschema update never hides DDL the plan will run. A mixed
	// plan with a non-zero typed count renders only the typed counts, so its
	// untyped statements are not reflected here; SummarizeChanges shares this
	// behavior, keeping the two surfaces in agreement.
	if len(parts) == 0 && totalStatements > 0 {
		parts = append(parts, fmt.Sprintf("%d DDL %s", totalStatements, pluralize("statement", totalStatements)))
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

	// Disclosed directly under the plan summary so the exclusion reads as
	// part of the plan result: what was counted, then what was withheld.
	writeIgnoredNamespaces(sb, data.IgnoredNamespaces)
}

// writeIgnoredNamespaces renders the ignore_namespaces disclosure line. No-op
// when nothing was excluded, so plans from repos without the config render
// unchanged.
func writeIgnoredNamespaces(sb *strings.Builder, ignored []string) {
	if len(ignored) == 0 {
		return
	}
	quoted := make([]string, len(ignored))
	for i, ns := range ignored {
		quoted[i] = fmt.Sprintf("`%s`", ns)
	}
	fmt.Fprintf(sb, glyph.Info+" Namespaces excluded from this plan by `ignore_namespaces`: %s\n\n", strings.Join(quoted, ", "))
}

// multiEnvHasIgnoredNamespaces reports whether any environment's plan excluded
// namespaces, so callers can decide whether the disclosure (and its spacing)
// renders at all.
func multiEnvHasIgnoredNamespaces(data MultiEnvPlanCommentData) bool {
	for _, env := range data.Environments {
		if plan, ok := data.Plans[env]; ok && plan != nil && len(plan.IgnoredNamespaces) > 0 {
			return true
		}
	}
	return false
}

// writeMultiEnvIgnoredNamespaces renders the ignore_namespaces disclosure for
// the all-environments-clean path, where no per-environment sections exist to
// carry it. When every environment excluded the same namespaces it renders the
// single shared line; otherwise one line per environment, since entries can
// resolve differently per environment.
func writeMultiEnvIgnoredNamespaces(sb *strings.Builder, data MultiEnvPlanCommentData) {
	anyIgnored := false
	identical := true
	var first []string
	for i, env := range data.Environments {
		var ignored []string
		if plan, ok := data.Plans[env]; ok && plan != nil {
			ignored = plan.IgnoredNamespaces
		}
		if len(ignored) > 0 {
			anyIgnored = true
		}
		if i == 0 {
			first = ignored
		} else if !slices.Equal(ignored, first) {
			identical = false
		}
	}
	if !anyIgnored {
		return
	}
	if identical {
		writeIgnoredNamespaces(sb, first)
		return
	}
	for _, env := range data.Environments {
		plan, ok := data.Plans[env]
		if !ok || plan == nil || len(plan.IgnoredNamespaces) == 0 {
			continue
		}
		quoted := make([]string, len(plan.IgnoredNamespaces))
		for i, ns := range plan.IgnoredNamespaces {
			quoted[i] = fmt.Sprintf("`%s`", ns)
		}
		fmt.Fprintf(sb, glyph.Info+" **%s**: namespaces excluded from this plan by `ignore_namespaces`: %s\n\n", capitalizeFirst(env), strings.Join(quoted, ", "))
	}
}

func writeNoChangesDetected(sb *strings.Builder, data PlanCommentData) {
	sb.WriteString("✅ **No schema changes detected**\n")
	if data.RecoveredApplyOwnedCheckState {
		sb.WriteString("\n" + glyph.Info + " SchemaBot found stored PR check state for this database/environment that was still marked as an apply in progress. Because this fresh plan shows the target schema already matches this PR, SchemaBot updated the PR check to passing.\n")
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
	creates, alters, drops := countStatementTypes(data.Changes, data.DatabaseType)
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

// countStatementTypes counts CREATE, ALTER, and DROP statements across all
// keyspaces with each dialect's parser, counting a valid greenfield create set
// as one create. A database type with no
// registered parser, or a statement its parser rejects, contributes nothing
// to the typed counts — the callers' raw statement-total fallbacks keep the
// summary honest — and each case is logged so a miscounted summary is
// triageable from server logs.
func countStatementTypes(changes []KeyspaceChangeData, databaseType string) (creates, alters, drops int) {
	parser, err := ddl.ParserForDialect(schema.DialectForDatabaseType(databaseType))
	if err != nil {
		slog.Warn("plan summary cannot classify statements; the summary will report raw statement totals instead of create/alter/drop counts",
			"database_type", databaseType, "error", err)
		return 0, 0, 0
	}
	for _, ks := range changes {
		for _, stmt := range ks.Statements {
			stmtType, _, classifyErr := parser.Classify(stmt)
			if classifyErr != nil {
				createSet, createSetErr := ddl.ParseCreateSet(parser, stmt)
				if createSetErr != nil {
					slog.Warn("plan summary could not classify a statement or parse it as a supported create set; it is left out of the create/alter/drop counts",
						"database_type", databaseType, "keyspace", ks.Keyspace,
						"classify_error", classifyErr, "create_set_error", createSetErr)
					continue
				}
				stmtType = createSet.Type
			}
			switch stmtType {
			case ddl.StatementCreateTable:
				creates++
			case ddl.StatementAlterTable:
				alters++
			case ddl.StatementDropTable:
				drops++
			}
		}
	}
	return
}

func writeKeyspaceChanges(sb *strings.Builder, data PlanCommentData) {
	// The DDL blocks below format statements under the plan's own dialect so
	// they are never reformatted under another family's grammar.
	dialect := schema.DialectForDatabaseType(data.DatabaseType)

	// PostgreSQL groups changes by schema, not keyspace, so it shares MySQL's
	// "Schema Name" label and heading suppression; Vitess and Strata keep the
	// keyspace vocabulary.
	schemaNamespaces := data.IsMySQL || dialect == schema.DialectPostgres

	// Skip the schema/keyspace heading when there's only one and it matches
	// the database name — it's redundant with the metadata line.
	singleKeyspace := len(data.Changes) == 1 && schemaNamespaces && data.Changes[0].Keyspace == data.Database

	// The VSchema diff budget is per comment, not per keyspace: split it
	// across the keyspaces that will render a diff so a multi-keyspace plan
	// stays bounded.
	diffCount := 0
	for _, ks := range data.Changes {
		if ks.VSchemaChanged && !data.IsMySQL && ks.VSchemaDiff != "" {
			diffCount++
		}
	}
	diffBudget := vschemaDiffBudget(diffCount)

	for _, ks := range data.Changes {
		hasVSchemaChanges := ks.VSchemaChanged && !data.IsMySQL
		hasDDLChanges := len(ks.Statements) > 0 || len(ks.Shards) > 0
		if !hasDDLChanges && !hasVSchemaChanges {
			continue
		}

		if !singleKeyspace {
			label := "Keyspace"
			if schemaNamespaces {
				label = "Schema Name"
			}
			fmt.Fprintf(sb, "#### %s: `%s`\n", label, ks.Keyspace)
		}

		if hasVSchemaChanges {
			sb.WriteString("#### VSchema\n")
			if ks.VSchemaDiff != "" {
				writeVSchemaDiffFence(sb, ks.VSchemaDiff, diffBudget)
			} else {
				sb.WriteString("_(diff not available)_\n\n")
			}
		}

		if hasDDLChanges {
			if len(ks.Shards) > 0 {
				writeShardedPlanDDL(sb, ks.Shards, dialect)
			} else {
				writePlanDDLBlock(sb, ks.Statements, dialect)
			}
		}
	}
}

// writePlanDDLBlock writes a single fenced SQL block of statements, formatted
// under the plan's own dialect. A greenfield create set is split so each of
// its statements is formatted on its own line; rendering is best-effort, so a
// statement that is neither a single statement nor a valid create set is
// still rendered as written, and the reason is logged for triage.
func writePlanDDLBlock(sb *strings.Builder, statements []string, dialect schema.Dialect) {
	sb.WriteString("```sql\n")
	formattedStatements := make([]string, 0, len(statements))
	parser, parserErr := ddl.ParserForDialect(dialect)
	if parserErr != nil {
		slog.Warn("plan DDL block cannot split create sets; multi-statement DDL will be rendered as written",
			"dialect", dialect, "error", parserErr)
	}
	for _, stmt := range statements {
		statementsToFormat := []string{stmt}
		if parserErr == nil {
			if _, _, classifyErr := parser.Classify(stmt); classifyErr != nil {
				createSet, createSetErr := ddl.ParseCreateSet(parser, stmt)
				if createSetErr != nil {
					slog.Warn("plan DDL block could not classify a statement or parse it as a supported create set; it will be rendered as written",
						"dialect", dialect, "classify_error", classifyErr, "create_set_error", createSetErr)
				} else {
					statementsToFormat = createSet.Statements
				}
			}
		}
		formattedCreateSet := make([]string, 0, len(statementsToFormat))
		for _, statementToFormat := range statementsToFormat {
			formattedCreateSet = append(formattedCreateSet, ddl.FormatDDLForDialect(dialect, statementToFormat))
		}
		formattedStatements = append(formattedStatements, strings.Join(formattedCreateSet, "\n"))
	}
	for i, stmt := range formattedStatements {
		sb.WriteString(stmt)
		if i < len(formattedStatements)-1 {
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
func writeShardedPlanDDL(sb *strings.Builder, shards []KeyspaceShardChange, dialect schema.Dialect) {
	groups := groupKeyspaceShardsByStatements(shards)
	if len(groups) <= 1 {
		// A single group of changing shards shows the DDL once, but still names the
		// shards it applies to — a sharded plan must always show which shards are
		// affected, even when the change is uniform across them. A lone group of
		// satisfied shards means nothing is changing, so render nothing rather than
		// an empty code block.
		if len(groups) == 1 && !groups[0].Satisfied {
			writeShardGroupHeading(sb, groups[0].Shards, len(shards))
			writePlanDDLBlock(sb, groups[0].Statements, dialect)
		}
		return
	}
	sb.WriteString("Shards diverge — what applies where:\n\n")
	for _, g := range groups {
		writeShardGroupHeading(sb, g.Shards, len(shards))
		// A satisfied group already matches the desired schema; say so instead
		// of rendering an empty code block.
		if g.Satisfied {
			sb.WriteString("_Already applied — no change._\n\n")
			continue
		}
		writePlanDDLBlock(sb, g.Statements, dialect)
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

// shardNamesInlineLimit caps how many shard names render inline in a PR
// comment. Beyond it, listing every range reads as a wall — a wide keyspace
// collapses to a count, with the names behind a collapsed block where the
// rendering has room for one.
const shardNamesInlineLimit = 8

// planShardList renders a group's shards as "shard `x`" or "shards `x`, `y`"
// when few enough to read inline, stating coverage beyond that — "12 of 32
// shards", or "all 32 shards" when the group spans the keyspace. Used where
// the list rides inside a line item and has no room for a collapsed name
// list; the full names stay reachable in the DDL section's collapsed
// shard-group blocks.
func planShardList(shards []string, totalShards int) string {
	if len(shards) > shardNamesInlineLimit {
		return shardCoveragePhrase(len(shards), totalShards)
	}
	quoted := markdownInlineCodeList(shards)
	if len(quoted) == 1 {
		return "shard " + quoted[0]
	}
	return "shards " + strings.Join(quoted, ", ")
}

// shardCoveragePhrase states how much of a keyspace a shard group covers:
// "all 32 shards" when it covers every planned shard, "12 of 32 shards" for
// a subset, or a bare count when the keyspace total is unknown — a subset
// must never read like whole-keyspace coverage.
func shardCoveragePhrase(count, totalShards int) string {
	if count == totalShards {
		return fmt.Sprintf("all %d shards", count)
	}
	if totalShards > 0 {
		return fmt.Sprintf("%d of %d shards", count, totalShards)
	}
	return fmt.Sprintf("%d shards", count)
}

// writeShardGroupHeading writes a shard group's bold heading above its DDL
// block. Few shards read inline by name; a wide group leads with how much of
// the keyspace it covers — "all 32 shards" when it covers every planned
// shard, "19 of 32 shards" for a subset — as a single collapsed line that
// expands into the full name list, so the names stay reachable without
// walling the comment.
func writeShardGroupHeading(sb *strings.Builder, shards []string, totalShards int) {
	if len(shards) <= shardNamesInlineLimit {
		fmt.Fprintf(sb, "**%s**\n\n", planShardList(shards, totalShards))
		return
	}
	fmt.Fprintf(sb, "<details>\n<summary><b>%s</b></summary>\n\n%s\n\n</details>\n\n",
		shardCoveragePhrase(len(shards), totalShards), strings.Join(markdownInlineCodeList(shards), ", "))
}

// writeDeploymentDrift renders the review-time drift rollup: a single uniform
// line when every deployment matches the reviewed plan, or a per-deployment
// breakdown naming which deployments diverged or could not be verified. It is a
// no-op for a nil rollup (single-target database or drift not evaluated).
func writeDeploymentDrift(sb *strings.Builder, drift *DeploymentDriftData) {
	if drift == nil {
		return
	}

	if !drift.Computed {
		sb.WriteString(glyph.Attention + " **Could not verify deployment drift** — the plan check is failing closed until it can be confirmed.\n\n")
		return
	}

	if drift.Clean {
		fmt.Fprintf(sb, "✅ **Same plan on all %d deployments** (%s).\n\n",
			len(drift.Deployments), joinDeploymentNames(drift.Deployments))
		return
	}

	sb.WriteString(glyph.Attention + " **Deployment drift detected** — some deployments no longer match the reviewed plan, so the plan check is failing closed:\n\n")
	for _, d := range drift.Deployments {
		name := "`" + d.Deployment + "`"
		if d.Primary {
			name += " (primary)"
		}
		switch d.Class {
		case "match":
			fmt.Fprintf(sb, "- %s ✅ matches the reviewed plan\n", name)
		case "diverged":
			fmt.Fprintf(sb, "- %s "+glyph.Attention+" diverged%s\n", name, driftDetailSuffix(d.Detail))
		default:
			fmt.Fprintf(sb, "- %s "+glyph.Failed+" could not verify%s\n", name, driftDetailSuffix(d.Detail))
		}
	}
	sb.WriteString("\n")
}

// driftDetailSuffix renders a deployment's drift detail as a trailing clause, or
// an empty string when there is no detail.
func driftDetailSuffix(detail string) string {
	if detail == "" {
		return ""
	}
	return " — " + detail
}

// joinDeploymentNames lists deployment names for the uniform drift line.
func joinDeploymentNames(deployments []DeploymentDriftEntry) string {
	names := make([]string, len(deployments))
	for i, d := range deployments {
		names[i] = d.Deployment
	}
	return strings.Join(names, ", ")
}

// writeBlockedChanges writes the section for statements the engine refuses,
// naming each table and a sanitized, Markdown-safe engine reason. There is no
// opt-in flag that lets these through — the remedy is whatever each reason
// names: an unsupported shape needs a rewrite, a missing grant needs
// provisioning.
func writeBlockedChanges(sb *strings.Builder, changes []BlockedChangeData) {
	n := len(changes)
	fmt.Fprintf(sb, glyph.Refused+" **Cannot apply**: %d %s the engine refuses to execute\n", n, pluralize("change", n))
	for _, c := range changes {
		table := "`" + c.Table + "`"
		if len(c.Shards) > 0 {
			table = fmt.Sprintf("%s (%s)", table, planShardList(c.Shards, c.TotalShards))
		}
		if c.Reason != "" {
			fmt.Fprintf(sb, "- %s: %s\n", table, escapeInlineMarkdown(SanitizeInlineError(c.Reason)))
		} else {
			fmt.Fprintf(sb, "- %s\n", table)
		}
	}
	sb.WriteString("\nAn apply will fail on these statements. Fix what each reason names — rewrite an unsupported change, or provision the stated access — or contact your SchemaBot operators for help.\n\n")
}

// directConsentCopy returns the header noun and consent footer for the
// direct-execution disclosure, keyed by database type. The footer is the
// sentence the operator consents to by confirming the apply, and what a
// direct statement does to the table while it runs is engine-specific — an
// engine that adopts direct execution adds its own copy here rather than
// inheriting another engine's semantics.
//
// The footer names the schema change engine in full rather than "the engine".
// It renders directly under a header noun that names the database's own native
// DDL, so the two sit adjacent: a reader who takes the shorter form for the
// storage engine gets the claim backwards, since the statement runs inside the
// database and outside SchemaBot. This is the sentence that carries the
// operator's consent to a change that cannot be reverted, so it spends the
// words.
func directConsentCopy(databaseType string, isMySQL bool) (headerNoun, footer string) {
	// Strata is sharded MySQL: a direct statement there is the same native
	// MySQL DDL, executed per shard.
	databaseType = strings.TrimSpace(databaseType)
	if databaseType == storage.DatabaseTypeMySQL || databaseType == storage.DatabaseTypeStrata || isMySQL {
		return "native MySQL DDL",
			"These statements run synchronously outside the schema change engine: writes to each table are blocked while its statement runs, the change is **not revertible**, and `--defer-cutover` does not apply to it. Confirming the apply consents to this."
	}
	// Deliberately conservative fallback for an engine that emits direct
	// verdicts without registering its own copy above: disclose the broadest
	// impact rather than understate what the operator is consenting to.
	return "native DDL",
		"These statements run synchronously outside the schema change engine: each table is unavailable while its statement runs, the change is **not revertible**, and `--defer-cutover` does not apply to it. Confirming the apply consents to this."
}

// writeDirectChanges writes the section for statements the direct execution
// policy routes to native DDL, naming each table and the planner's reason
// (which carries the row estimate). The fixed footer discloses the semantics
// the operator consents to by confirming the apply.
func writeDirectChanges(sb *strings.Builder, changes []DirectChangeData, databaseType string, isMySQL bool) {
	headerNoun, footer := directConsentCopy(databaseType, isMySQL)
	n := len(changes)
	fmt.Fprintf(sb, "⚙️ **Direct execution**: %d %s will run as %s\n", n, pluralize("change", n), headerNoun)
	for _, c := range changes {
		table := "`" + c.Table + "`"
		if len(c.Shards) > 0 {
			table = fmt.Sprintf("%s (%s)", table, planShardList(c.Shards, c.TotalShards))
		}
		if c.Reason != "" {
			fmt.Fprintf(sb, "- %s: %s\n", table, escapeInlineMarkdown(SanitizeInlineError(c.Reason)))
		} else {
			fmt.Fprintf(sb, "- %s\n", table)
		}
	}
	sb.WriteString("\n" + footer + "\n\n")
}

func writeUnsafeWarning(sb *strings.Builder, changes []UnsafeChangeData, isMySQL bool) {
	n := countUnsafeFindings(changes)
	fmt.Fprintf(sb, glyph.Attention+" **Issues**: %d unsafe %s detected\n", n, pluralize("change", n))
	item := 0
	for _, c := range changes {
		table := "`" + c.Table + "`"
		if len(c.Shards) > 0 {
			table = fmt.Sprintf("%s (%s)", table, planShardList(c.Shards, c.TotalShards))
		}
		writeUnsafeChangeItem(sb, &item, table, c.Reason, c.ChangeType)
	}
	sb.WriteString("\n")
	writeUnsafeDropGuidance(sb, changes, isMySQL)
}

// writeUnsafeChangeItem writes one table's unsafe findings, one numbered line
// per finding, so the rendered list is exactly as long as the heading's count
// and operators can reference a finding by its number. n carries the running
// number across tables; a change with no parseable reason still gets a line,
// carrying the engine's change type when one is known so the finding explains
// itself.
func writeUnsafeChangeItem(sb *strings.Builder, n *int, table, reason, changeType string) {
	reasons := ui.LintReasons(reason)
	if len(reasons) == 0 {
		*n++
		if changeType != "" {
			fmt.Fprintf(sb, "%d. %s: %s\n", *n, table, changeType)
		} else {
			fmt.Fprintf(sb, "%d. %s\n", *n, table)
		}
		return
	}
	for _, r := range reasons {
		*n++
		fmt.Fprintf(sb, "%d. %s: %s\n", *n, table, ui.CodeQuoteIdentifiers(r))
	}
}

// countUnsafeFindings sums the individual lint findings across changes, so
// headers count what the list below actually shows: a table whose reason
// carries several joined violations contributes each of them. A change with
// no parseable reason still counts once. The CLI's countUnsafeFindings in
// pkg/cmd/internal/templates mirrors this; the two must agree so the PR
// comment and CLI report the same count for the same plan.
func countUnsafeFindings(changes []UnsafeChangeData) int {
	n := 0
	for _, c := range changes {
		if reasons := ui.LintReasons(c.Reason); len(reasons) > 0 {
			n += len(reasons)
		} else {
			n++
		}
	}
	return n
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

// lintWarningsFoldThreshold is the warning count above which the lint section
// collapses into a details block grouped by table. Short lists stay inline so
// a single advisory finding never needs a click; long lists stop dominating
// the plan comment while the count stays visible in the header.
const lintWarningsFoldThreshold = 5

// writeLintViolations writes advisory lint findings. Lint warnings never block
// an apply, so they render with a lighter marker than the unsafe-change Issues
// section but share its visual language: a bold count in the header, backticked
// table prefixes, and identifiers as inline code.
func writeLintViolations(sb *strings.Builder, warnings []LintViolationData) {
	n := len(warnings)

	if n <= lintWarningsFoldThreshold {
		fmt.Fprintf(sb, "\U0001f4a1 **Lint Warnings**: %d advisory %s\n", n, pluralize("finding", n))
		for _, w := range warnings {
			message := ui.CodeQuoteIdentifiers(w.Message)
			if w.Table != "" {
				fmt.Fprintf(sb, "- `%s`: %s\n", w.Table, message)
			} else {
				fmt.Fprintf(sb, "- %s\n", message)
			}
		}
		sb.WriteString("\n")
		return
	}

	// GitHub renders <summary> content as HTML, not markdown, so the folded
	// header bolds with <b> tags instead of asterisks.
	fmt.Fprintf(sb, "<details>\n<summary>\U0001f4a1 <b>Lint Warnings</b>: %d advisory %s</summary>\n\n", n, pluralize("finding", n))
	for _, group := range groupLintWarningsByTable(warnings) {
		if group.table != "" {
			fmt.Fprintf(sb, "**`%s`**\n", group.table)
		}
		for _, message := range group.messages {
			fmt.Fprintf(sb, "- %s\n", ui.CodeQuoteIdentifiers(message))
		}
		sb.WriteString("\n")
	}
	sb.WriteString("</details>\n\n")
}

type lintWarningGroup struct {
	table    string
	messages []string
}

// groupLintWarningsByTable groups warnings by table in first-appearance order,
// preserving message order within each table. Warnings without a table come
// out as a leading group with an empty table name.
func groupLintWarningsByTable(warnings []LintViolationData) []lintWarningGroup {
	index := make(map[string]int)
	var groups []lintWarningGroup
	for _, w := range warnings {
		i, ok := index[w.Table]
		if !ok {
			i = len(groups)
			index[w.Table] = i
			groups = append(groups, lintWarningGroup{table: w.Table})
		}
		groups[i].messages = append(groups[i].messages, w.Message)
	}
	// Untabled warnings read as general notes; surface them first rather
	// than wherever they happened to appear in the linter output.
	for i, g := range groups {
		if g.table == "" && i > 0 {
			groups = append([]lintWarningGroup{g}, append(groups[:i:i], groups[i+1:]...)...)
			break
		}
	}
	return groups
}

func writeErrors(sb *strings.Builder, errors []string) {
	var msgs []string
	for _, errMsg := range errors {
		if msg := SanitizeInlineError(errMsg); msg != "" {
			msgs = append(msgs, msg)
		}
	}
	if len(msgs) == 0 {
		return
	}
	sb.WriteString("**Errors**:\n")
	for _, msg := range msgs {
		fmt.Fprintf(sb, "- %s\n", html.EscapeString(msg))
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

	// AgentHint is the deployment's configured guidance for AI agents reading
	// the plan. Empty on deployments that configure none, which render an
	// unchanged comment.
	AgentHint string

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

	// If no environments have changes and no errors, show simple message — unless
	// a deployment drifted, which must still surface even when the reviewed
	// primary plans are clean no-ops. The ignore_namespaces disclosure still
	// renders underneath it: an all-clean result is exactly where a reviewer
	// needs to see that a namespace was withheld rather than genuinely
	// unchanged.
	if envsWithChanges == 0 && !hasErrors && !AnyEnvHasDriftToShow(data) {
		sb.WriteString("✅ **No schema changes detected** for any environment.\n")
		if multiEnvHasIgnoredNamespaces(data) {
			sb.WriteString("\n")
			writeMultiEnvIgnoredNamespaces(&sb, data)
		}
		return appendAgentHint(sb.String(), data.AgentHint)
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
				writeErrorBlock(&sb, glyph.Failed, errMsg)
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

	return appendAgentHint(sb.String(), data.AgentHint)
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
	if merged.AgentHint == "" {
		merged.AgentHint = data.AgentHint
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
	// Deployment drift is shown before the change list and before the no-changes
	// short-circuit: a non-primary deployment can drift even when this
	// environment's reviewed primary plan is a clean no-op.
	writeDeploymentDrift(sb, plan.DeploymentDrift)

	totalStatements, keyspacesWithVSchema := countChanges(plan.Changes)
	totalChanges := totalStatements + keyspacesWithVSchema

	// The ignore_namespaces disclosure renders under each environment's
	// summary (writePlanSummary) or no-changes message, because entries can
	// resolve differently per environment.
	if totalChanges == 0 {
		sb.WriteString("✅ **No schema changes detected**\n\n")
		writeIgnoredNamespaces(sb, plan.IgnoredNamespaces)
		return
	}

	// Detailed changes. A single change is small enough to show inline; more
	// than one is collapsed so the DDL doesn't dominate the comment while the
	// unsafe/lint warnings and summary below stay visible at a glance.
	if totalChanges == 1 {
		writeKeyspaceChanges(sb, *plan)
	} else {
		writeCollapsibleKeyspaceChanges(sb, *plan, totalStatements)
	}

	// Blocked changes — statements the engine refuses; the apply will fail on
	// them, so each environment's section discloses its own.
	if len(plan.BlockedChanges) > 0 {
		writeBlockedChanges(sb, plan.BlockedChanges)
	}

	// Destructive changes to tables another pull request owns — resolved per
	// environment, since the task history that attributes them is per
	// environment.
	if len(plan.AttributedChanges) > 0 {
		writeAttributedChanges(sb, plan.AttributedChanges)
	}

	// Direct-execution changes — each environment's section discloses its own,
	// since the policy is configured per environment.
	if len(plan.DirectChanges) > 0 {
		writeDirectChanges(sb, plan.DirectChanges, plan.DatabaseType, plan.IsMySQL)
	}

	// Copies already on the target — read per environment, since each
	// environment has its own target.
	if len(plan.DiscardedCopies) > 0 {
		writeDiscardedCopies(sb, plan.DiscardedCopies, plan.applyingWithoutConfirmation())
	}
	if len(plan.AdoptedCopies) > 0 {
		writeAdoptedCopies(sb, plan.AdoptedCopies, plan.applyingWithoutConfirmation())
	}
	if len(plan.RunningCopies) > 0 {
		writeRunningCopies(sb, plan.RunningCopies, plan.applyingWithoutConfirmation())
	}

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

	// Apply instructions for environments with changes.
	switch {
	case len(envsWithChanges) >= 2:
		sb.WriteString("▶️ **To apply** these changes, start with the first environment:\n")
		fmt.Fprintf(sb, "```\n%s\n```\n", tenantCommand("schemabot apply", envsWithChanges[0], data.Tenant))
		for i := 1; i < len(envsWithChanges); i++ {
			fmt.Fprintf(sb, "\nAfter verifying %s, apply to %s:\n", envsWithChanges[i-1], envsWithChanges[i])
			fmt.Fprintf(sb, "```\n%s\n```\n", tenantCommand("schemabot apply", envsWithChanges[i], data.Tenant))
		}
	case len(envsWithChanges) == 1:
		sb.WriteString("▶️ **To apply** these changes, comment:\n")
		fmt.Fprintf(sb, "```\n%s\n```\n", tenantCommand("schemabot apply", envsWithChanges[0], data.Tenant))
	case len(envsWithErrors) == 0:
		sb.WriteString("No changes to apply.\n")
	}

	// Error guidance for failed environments
	if len(envsWithErrors) > 0 {
		sb.WriteString("\n")
		for _, env := range envsWithErrors {
			fmt.Fprintf(sb, glyph.Attention+" **%s** failed to plan. Resolve the error above and re-run:\n", capitalizeFirst(env))
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

// AnyEnvHasDriftToShow reports whether any environment has drift that must be
// surfaced even when no environment plans changes: a deployment that diverged or
// could not be verified. A clean uniform rollup is not "drift to show" — with no
// changes anywhere the simple no-changes message is clearer.
func AnyEnvHasDriftToShow(data MultiEnvPlanCommentData) bool {
	for _, env := range data.Environments {
		plan, ok := data.Plans[env]
		if !ok || plan == nil || plan.DeploymentDrift == nil {
			continue
		}
		d := plan.DeploymentDrift
		if !d.Computed || !d.Clean {
			return true
		}
	}
	return false
}

// plansIdentical reports whether two environments' plan sections are the same
// section, so one may stand in for both under a combined header.
//
// It answers that by rendering both and comparing the bytes, because every
// disclosure in a section is a promise about one environment's target and
// standing in for the other means making that promise for a target it was never
// read from. Identical DDL does not make those promises identical: execution
// policy, the task history that attributes a change, and unfinished copies on
// the target are all resolved per environment, so a section can differ on any of
// them while the statements match. Comparing what the section says is the only
// comparison that stays right as sections gain disclosures — a field-by-field
// version silently drops each one added after it was written, and drops it from
// the comment operators apply from.
func plansIdentical(a, b *PlanCommentData) bool {
	var renderedA, renderedB strings.Builder
	writeEnvironmentPlanSection(&renderedA, a)
	writeEnvironmentPlanSection(&renderedB, b)
	return renderedA.String() == renderedB.String()
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
