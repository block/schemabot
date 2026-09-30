package templates

import (
	"cmp"
	"fmt"
	"html"
	"log/slog"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/caller"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/routing"
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
	DDL    string
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
	// Targets names the rollout targets that refuse this change, for a target
	// plan group in which only some targets do: the group is keyed on the work
	// its targets would run, and whether the engine refuses that work can
	// depend on the target. Empty when every target in the group refuses it.
	Targets []string
	// TotalTargets is how many targets the group holds, so a subset too wide
	// to name reads as coverage ("3 of 12 targets").
	TotalTargets int
}

// DirectChangeData is a planned change the database's direct execution policy
// routes to native MySQL DDL instead of the schema change engine. The plan
// comment discloses what running it that way does to the table.
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
	Database string

	// ScopedDatabase is the database this comment's copy-paste commands name,
	// so the follow-up an operator is being asked for is one they can paste in
	// a repository that configures several databases rather than one rejected
	// as ambiguous. It comes from the -d on the command that produced the
	// comment, or, on a comment SchemaBot posts on its own, from the database
	// that comment plans — the comment's own identity rather than a guess at
	// what an operator meant. Empty renders the commands bare, which is what an
	// unscoped command is answered with, and what a comment SchemaBot posts on
	// its own carries when a bare command already reaches only the database the
	// comment is about.
	ScopedDatabase string

	SchemaName   string // Schema directory name (e.g. filepath.Base of schema dir)
	Environment  string
	Tenant       string
	HeadSHA      string
	Repository   string
	RequestedBy  string // Empty means auto-generated
	DatabaseType string
	IsMySQL      bool
	ApplyID      string

	// PlanID is the identifier of the stored plan this comment renders, so DDL
	// cut to fit the comment names the command that prints the plan in full.
	// Empty when the plan was not stored, which leaves a cut block pointing at
	// the PR's schema files.
	PlanID string

	// CLIName is the tool name the comment's CLI command hints start with,
	// the server's cli_name. Empty renders the CLI's own default.
	CLIName string

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

	// ExemptTables lists live tables excluded from a plan verdict by namespace.
	ExemptTables []ExemptTablesData

	// Unsafe change tracking
	HasUnsafeChanges bool
	AllowUnsafe      bool
	UnsafeChanges    []UnsafeChangeData

	// Changes the engine refuses; the apply will fail on them.
	BlockedChanges []BlockedChangeData

	// Changes the direct execution policy routes to native MySQL DDL.
	DirectChanges []DirectChangeData

	// AllChangesDirect marks a plan whose every change runs as direct
	// execution. Such a plan has no cutover to defer, so the apply-confirm
	// command a paused comment suggests leaves out --defer-cutover, which
	// apply-confirm rejects on it.
	AllChangesDirect bool

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

	// PendingManualConfirmation marks a locked comment that is waiting on the
	// operator rather than announcing an apply already under way. It is the
	// state itself, not evidence of it, so a downgrade whose cause is disclosed
	// in a section above can leave the reason below empty without the comment
	// reading as an apply in flight.
	PendingManualConfirmation bool

	// PausedApplyCause discloses why the apply is waiting, as a section among
	// the other disclosures rather than a line in the footer. Every warning on
	// this comment is then in one region, marked the same way and carrying the
	// same kind of detail, and the footer is the same sentence on every
	// comment. Nil when a disclosure above already explains the cause, never a
	// signal that the apply is proceeding.
	PausedApplyCause *PausedApplyCauseData

	RecoveredApplyOwnedCheckState bool

	// DeploymentDrift is the review-time rollup of how every configured
	// deployment compares to the reviewed primary plan. Nil for a single-target
	// database (nothing to compare) or when drift was not evaluated.
	DeploymentDrift *DeploymentDriftData

	// MemberApplyRefusal says why a PR apply cannot run the other targets'
	// plans this comment renders for a reviewed target already at the desired
	// schema, naming only targets, tables, and namespaces. Such an apply is
	// refused whatever its flags, so the comment offers no apply command in its
	// place. Empty when the apply can run them, or when the comment renders the
	// reviewed plan alone.
	MemberApplyRefusal string
}

// ExemptTablesData describes live tables exempt from a plan verdict.
type ExemptTablesData struct {
	Namespace string
	Tables    []string
	Reason    string
}

// DeploymentDriftData renders the review-time drift rollup in the PR preview: a
// uniform line when every member passed its contract, or a per-member breakdown
// when some member diverged from — or could not be confirmed against — the
// reviewed plan.
type DeploymentDriftData struct {
	// Deployments is every configured rollout member in rollout order, primary
	// first.
	Deployments []DeploymentDriftEntry
	// Clean means every member passed its contract: under mirrored members, that
	// they all match the reviewed plan; under independent members, that they all
	// produced a plan of their own.
	Clean bool
	// Computed is false when the rollup itself could not be evaluated; the check
	// still fails closed, and the preview says the deployments are unverified.
	Computed bool
	// Independent reports that each member holds its own schema and was planned
	// against it. Members are then not expected to match each other, so a clean
	// rollup means every target was planned rather than that they agree — which
	// is the opposite of what the mirrored wording says.
	Independent bool
	// Plans is the members grouped by the plan they would run, one entry per
	// distinct plan, the primary's first. It says how much the members actually
	// agree this round, which the contract alone cannot: members that are free
	// to differ usually do not. Set only for a clean rollup of independent
	// members — members expected to match each other say nothing by matching,
	// and a blocked rollup describes each member on its own instead.
	Plans []DeploymentPlanGroup
	// TableSizes is every target's size estimate for each table its own plan
	// changes with a statement whose cost scales with the table's size, in
	// rollout order, primary first. Each target applies to its own data, so
	// the size section ranks a table by its largest target rather than by the
	// reviewed plan alone. Set only for a clean rollup, where every target was
	// planned: a target missing from the list would read as one with nothing
	// to copy.
	TableSizes []TargetTableSize
}

// TargetTableSize is one rollout target's size estimate for one table its plan
// changes.
type TargetTableSize struct {
	// Target names the target the way an operator addresses it.
	Target   string
	Keyspace string
	Size     TableSizeData
}

// DeploymentPlanGroup is the members of a rollout that would run the same plan.
// Members share a group exactly when their plans are identical work, so a group
// is what the comment can describe once and attribute to all of them.
//
// Identical work is what an apply would do to each member, not what each
// member's plan looks like written down. Members are keyed on canonicalized
// table DDL and on which namespaces change their VSchema, because a VSchema is
// applied as the file the PR holds rather than as a computed delta: two members
// given the same file are doing the same work even where their recorded diffs
// differ, since a diff differs by where the member started.
type DeploymentPlanGroup struct {
	// Members names the group's members the way an operator addresses them, in
	// rollout order.
	Members []string
	// Primary marks the group the reviewed primary member belongs to. Exactly
	// one group carries it, and it is the group operators read first: the
	// reviewed plan is the one they have already seen.
	Primary bool
	// Changes is one member's plan, in the same shape the comment renders the
	// reviewed plan itself. Empty for a group whose members are already at the
	// desired schema.
	//
	// The group's members run the same work, so any member's plan describes all
	// of them — but they are grouped on canonicalized DDL, so two members can
	// legitimately share a group while spelling the same statement differently.
	// What renders is whichever member came first in rollout order, not a
	// spelling every member would produce.
	Changes []KeyspaceChangeData
	// BlockedChanges are the group's changes the engine will refuse at apply,
	// each naming the targets that refuse it when that is not all of them.
	BlockedChanges []BlockedChangeData
	// PlanID is the identifier of the stored plan the group's first member
	// would run, so DDL cut to fit the comment names the command that prints
	// the group's plan in full. Unused for the primary's group, which runs the
	// reviewed plan and points at PlanCommentData.PlanID. Empty when the
	// member's plan was not stored.
	PlanID string
}

// Empty reports that the group's members are already at the desired schema and
// would apply nothing. That is a plan in its own right, not a missing one, and
// naming it is the difference between a fleet that is converging and one the
// comment has quietly left out.
// A vschema rewrite carries no DDL and is still work, so a group is counted the
// same way the comment counts the reviewed plan: statements and vschema
// rewrites together.
func (g DeploymentPlanGroup) Empty() bool {
	statements, vschema := countChanges(g.Changes)
	return statements+vschema == 0
}

// DeploymentDriftEntry is one rollout member's classification against the
// reviewed primary plan.
type DeploymentDriftEntry struct {
	Deployment string
	// Target is the member's target within its deployment. One deployment can
	// address several targets, so the deployment name alone does not always name
	// the member.
	Target  string
	Primary bool
	// Class is "match", "planned", "diverged", or "errored".
	Class string
	// Blocked is the number of changes this member will refuse at apply.
	Blocked int
	// Detail is a short human explanation for a diverged or errored member;
	// empty for a member that passed.
	Detail string
}

// driftMemberNames renders each rollup entry the way an operator addresses it,
// index-parallel to the entries. The naming rule is shared with every other
// member-facing surface, so a deployment that addresses several targets is named
// the same way in the plan comment, the check summary, and the progress comment.
func driftMemberNames(entries []DeploymentDriftEntry) []string {
	members := make([]routing.ExecutionTarget, len(entries))
	for i, e := range entries {
		members[i] = routing.ExecutionTarget{Deployment: e.Deployment, Target: e.Target}
	}
	return routing.DisplayNames(members)
}

// applyingWithoutConfirmation reports whether this comment announces an apply
// that is already running rather than one waiting on the operator: a locked
// comment with nothing pausing it. Nothing on such a comment is a question, so
// the disclosures above the footer state what the apply is doing instead of
// warning about what confirming would cost, and never offer a remedy that is
// already out of reach. The footer reads the same predicate, so the two cannot
// disagree about whether the reader still has a decision to make.
func (d PlanCommentData) applyingWithoutConfirmation() bool {
	return d.IsLocked && !d.PendingManualConfirmation
}

// directNotesDeferCutover reports whether the direct disclosure says
// --defer-cutover leaves the direct statements alone: on an apply that passed
// the flag, and on a paused comment, where the operator can still pass it to
// apply-confirm.
func (d PlanCommentData) directNotesDeferCutover() bool {
	return d.DeferCutover || (d.IsLocked && d.PendingManualConfirmation)
}

// PausedApplyCauseData is a cause the rest of the comment does not already
// disclose, in the shape every other disclosure uses: a heading naming what is
// wrong, the specifics behind it, and what the operator can do. Entries may be
// empty where the cause has no per-table detail to give.
type PausedApplyCauseData struct {
	Heading string
	Entries []string
	Remedy  string
}

// KeyspaceChangeData contains changes for a single keyspace/schema.
type KeyspaceChangeData struct {
	Keyspace       string
	Statements     []string
	VSchemaChanged bool
	VSchemaDiff    string

	// Finalize marks a keyspace the engine asked to finalize after its DDL.
	// A keyspace with DDL or a VSchema change to show finalizes as part of that
	// work, so the finalize gets its own line and count only when it is the
	// keyspace's only work, which keeps such a plan from reading as having no
	// changes.
	Finalize bool
	// TableSizes carries plan-time size estimates for the existing tables this
	// keyspace's changes copy, rebuild, or scan, rendered in the size section
	// after the DDL. Tables being created have no size and are omitted. An
	// entry without a byte estimate renders an explicit "unavailable" so a
	// failed size probe never reads as a small table, unless no table in the
	// plan has an estimate, in which case the section is omitted (see
	// writeTableSizesSection).
	TableSizes []TableSizeData

	// Shards carries this keyspace's per-shard changes for a sharded plan. When
	// set, the DDL is rendered per shard-group ("what applies where") instead of
	// the single Statements block — so a keyspace whose shards diverge is shown
	// faithfully. Empty for a non-sharded keyspace.
	Shards []KeyspaceShardChange
}

// TableSizeData is one existing table's plan-time size estimate for display.
// The estimate is approximate — sourced from engine statistics that may be
// stale — and is rendered as such.
type TableSizeData struct {
	Table string
	// ShardCount is the number of shards the change spans. Zero when the
	// target is not sharded or the topology is unknown, which omits the shard
	// clause entirely.
	ShardCount int
	// EstimatedBytes is the table's approximate on-disk footprint (data plus
	// indexes), summed across shards for a sharded target. Nil renders as
	// explicitly unavailable.
	EstimatedBytes *int64
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

// RenderPlanComment renders the plan comment markdown. The DDL takes every
// byte the rest of the comment leaves under GitHub's size limit.
func RenderPlanComment(data PlanCommentData) string {
	return renderWithinCommentLimit(countCommentDDLBlocks(data), 0, func(budget *ddlBlockBudget) string {
		return renderPlanComment(data, budget)
	})
}

func renderPlanComment(data PlanCommentData, budget *ddlBlockBudget) string {
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
	writeDeploymentDrift(&sb, data.DeploymentDrift, data.Changes)
	targetPlans := RendersTargetPlans(data.DeploymentDrift)
	summary := data
	if targetPlans {
		writeTargetPlans(&sb, data, budget, false)
		summary.Changes = combinedTargetPlanChanges(data)
	}

	// Count changes
	totalStatements, keyspaceUpdates := countChanges(data.Changes)
	totalChanges := totalStatements + keyspaceUpdates
	summaryStatements, summaryKeyspaceUpdates := countChanges(summary.Changes)

	// No changes — short-circuit with a single clean message. The
	// ignore_namespaces disclosure still renders: a no-changes result is
	// exactly where a reviewer needs to tell a withheld namespace apart from a
	// genuinely unchanged one.
	//
	// A reviewed target with nothing to run while other targets still have work
	// is not a no-op: the comment goes on to summarize their plans and offer the
	// apply, which runs each of those targets' own plans.
	if totalChanges == 0 && !targetPlans {
		writeNoChangesDetected(&sb, data)
		if len(data.IgnoredNamespaces) > 0 || hasExemptTables(data.ExemptTables) {
			sb.WriteString("\n")
			writeIgnoredNamespaces(&sb, data.IgnoredNamespaces)
			writeExemptTables(&sb, data.ExemptTables)
		}
		return appendAgentHint(sb.String(), data.AgentHint)
	}

	// Detailed changes, unless every target's plan was already rendered above.
	if !targetPlans {
		writeKeyspaceChanges(&sb, data, budget)
	}

	// Blocked changes — statements the engine refuses. Unlike unsafe changes,
	// these cannot be acknowledged away: the apply will fail on them. Shown on
	// the locked apply comment too, so the operator sees the guaranteed
	// failure before confirming.
	if len(data.BlockedChanges) > 0 && !targetPlansDiscloseBlocked(data) {
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
	// The policy approves them, so the plan only discloses how they run; the
	// locked apply comment repeats it so the apply shows what it is running.
	if len(data.DirectChanges) > 0 {
		writeDirectChanges(&sb, data.DirectChanges, data.DatabaseType, data.IsMySQL, data.directNotesDeferCutover())
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

	// Why the apply is waiting, when no disclosure above says so. It sits here
	// rather than in the footer so every warning on the comment is in one
	// region: the reader meets them in one pass, and the footer stays the same
	// sentence whatever paused the apply.
	if data.PausedApplyCause != nil {
		writePausedApplyCause(&sb, data.PausedApplyCause)
	}

	// Unsafe changes warning — shown on the plan comment for review, omitted on
	// the locked apply comment: unsafe changes only reach an apply after the
	// operator acknowledged them with --allow-unsafe (apply-confirm re-checks
	// and blocks otherwise), so repeating them there is noise.
	if data.HasUnsafeChanges && len(data.UnsafeChanges) > 0 && !data.IsLocked {
		writeUnsafeWarning(&sb, data.UnsafeChanges, data.DatabaseType, data.IsMySQL)
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

	// Summary and options (after DDL, matching CLI layout). Target plans are
	// summarized together, as a sharded keyspace's shards are.
	writePlanSummary(&sb, summary, summaryStatements, summaryKeyspaceUpdates)
	writeOptions(&sb, data)

	// Footer
	sb.WriteString("\n---\n\n")

	switch {
	case data.IsLocked:
		applyConfirmCmd := scopedApplyCommand("schemabot apply-confirm", data.Environment, data.ScopedDatabase, ApplyCommandOptions{
			Tenant:       data.Tenant,
			AllowUnsafe:  data.AllowUnsafe,
			DeferCutover: data.DeferCutover && !data.AllChangesDirect,
			SkipRevert:   data.SkipRevert,
		})

		if !data.applyingWithoutConfirmation() {
			// Automatic apply was downgraded to manual confirmation — show unlock since user needs to act
			// The same sentence on every paused comment, whatever paused it.
			// The cause is a disclosure above, so the footer is only ever the
			// decision the reader is being asked for.
			sb.WriteString("**Confirmation required** — review the plan above, then confirm manually:\n")
			fmt.Fprintf(&sb, "```\n%s\n```\n", applyConfirmCmd)
			sb.WriteString("\n🔓 To discard this plan and unlock, comment:\n")
			unlockCmd := appendTenantFlag(appendDatabaseFlag("schemabot unlock", data.ScopedDatabase), data.Tenant)
			fmt.Fprintf(&sb, "```\n%s\n```\n", unlockCmd)
		} else {
			// Automatic apply is proceeding. No unlock hint — it's noise on the
			// happy path; the operator can still unlock from the CLI if needed.
			sb.WriteString("**Applying automatically**\n")
		}
	case data.MemberApplyRefusal != "":
		writeMemberApplyRefusal(&sb, data.MemberApplyRefusal)
	default:
		applyCmd := appendDatabaseFlag(fmt.Sprintf("schemabot apply -e %s", data.Environment), data.ScopedDatabase)
		if data.Tenant != "" {
			applyCmd += fmt.Sprintf(" --tenant %s", data.Tenant)
		}
		writeApplyInstruction(&sb, applyCmd)
	}

	return appendAgentHint(sb.String(), data.AgentHint)
}

// writeMemberApplyRefusal writes, in place of the apply instruction, why a PR
// apply cannot run the other targets' plans the comment renders. Offering the
// command there would coach an apply that is refused whatever its flags.
func writeMemberApplyRefusal(sb *strings.Builder, refusal string) {
	fmt.Fprintf(sb, glyph.Attention+" **This PR cannot apply the other targets' plans**: the reviewed target already has this schema, but %s.\n\n", escapeInlineMarkdown(strings.Join(strings.Fields(refusal), " ")))
	sb.WriteString("A PR apply whose reviewed target is already at the desired schema cannot run that or disclose it for confirmation. The schema check keeps blocking merge until every target has the change.\n")
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
	if !data.IsLocked || data.PendingManualConfirmation {
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
	fmt.Fprintf(sb, glyph.Attention+" **Check before applying**: %d %s SchemaBot cannot attribute to this PR\n", n, pluralize("destructive change", n))
	for _, d := range changes {
		if d.Unresolved {
			fmt.Fprintf(sb, "- %s: ownership could not be established; see server logs\n", inlineCode(d.Table))
			continue
		}
		// The owner named is the most recent open pull request that changed the
		// table, which is not necessarily the last one to change it: a later
		// change from a pull request that has since closed leaves no open claim
		// and is passed over.
		fmt.Fprintf(sb, "- %s: changed by %s, which is still open\n",
			inlineCode(d.Table), caller.PullRequestMarkdownLink(d.Repository, d.PullRequest))
	}
	sb.WriteString("\nA plan diffs this PR's schema files against the live database, so what another PR applied before merging reads here as something to remove. If that is not what you intend, merge that PR, or bring this PR's schema files up to date with it, then re-plan.\n\n")
}

// writePlanMetadata writes the metadata line for plan comments.
// Schema name (the schema directory) is shown for MySQL. Vitess uses keyspace headers instead.
func writePlanMetadata(sb *strings.Builder, data PlanCommentData) {
	parts := []string{fmt.Sprintf("**Database**: `%s`", data.Database)}
	parts = append(parts, fmt.Sprintf("**Type**: `%s`", schemaChangePlanDatabaseTypeLabel(data.DatabaseType, data.IsMySQL)))
	if data.IsMySQL && data.SchemaName != "" {
		parts = append(parts, fmt.Sprintf("**Schema Name**: %s", inlineCode(data.SchemaName)))
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

// countChanges counts a plan's DDL statements and its keyspace-level updates:
// each keyspace whose VSchema changes or whose only work is a finalize. A
// finalize beside DDL is part of that DDL's work, so it adds nothing.
func countChanges(changes []KeyspaceChangeData) (totalStatements, keyspaceUpdates int) {
	for _, ks := range changes {
		totalStatements += keyspaceStatementCount(ks)
		if ks.VSchemaChanged || finalizeIsOnlyWork(ks) {
			keyspaceUpdates++
		}
	}
	return
}

// countKeyspaceUpdates counts the summary's keyspace-level labels: the VSchema
// updates, and the keyspaces whose only work is a finalize.
func countKeyspaceUpdates(changes []KeyspaceChangeData) (vschemaUpdates, finalizes int) {
	for _, ks := range changes {
		switch {
		case ks.VSchemaChanged:
			vschemaUpdates++
		case finalizeIsOnlyWork(ks):
			finalizes++
		}
	}
	return
}

// finalizeIsOnlyWork reports whether a keyspace's only work is the finalize
// the engine asked for: no DDL and no VSchema change to show beside it.
func finalizeIsOnlyWork(ks KeyspaceChangeData) bool {
	return ks.Finalize && !ks.VSchemaChanged && keyspaceStatementCount(ks) == 0
}

// keyspaceStatementCount counts a keyspace's DDL statements for the summary and
// the no-changes short-circuit.
func keyspaceStatementCount(ks KeyspaceChangeData) int {
	return len(keyspaceStatements(ks))
}

// keyspaceStatements returns the DDL statements the plan comment renders for a
// keyspace, which is the set every summary count walks. A sharded keyspace
// renders its per-shard changes, so those are authoritative: the collapsed
// namespace-level Statements can omit a statement confined to one shard, and
// counting from them would drop it from the summary while the DDL block shows
// it. The per-shard statements are deduplicated in first-seen order so a
// uniform change across shards counts once, as it renders once.
func keyspaceStatements(ks KeyspaceChangeData) []string {
	if len(ks.Shards) == 0 {
		return ks.Statements
	}
	seen := make(map[string]struct{})
	var statements []string
	for _, sh := range ks.Shards {
		for _, stmt := range sh.Statements {
			if _, dup := seen[stmt]; !dup {
				seen[stmt] = struct{}{}
				statements = append(statements, stmt)
			}
		}
	}
	return statements
}

func writePlanSummary(sb *strings.Builder, data PlanCommentData, totalStatements, keyspaceUpdates int) {
	totalChanges := totalStatements + keyspaceUpdates
	if totalChanges == 0 {
		writeNoChangesDetected(sb, data)
		sb.WriteString("\n")
		writeIgnoredNamespaces(sb, data.IgnoredNamespaces)
		writeExemptTables(sb, data.ExemptTables)
		return
	}

	// Size context precedes the summary: how big the tables the plan will
	// copy, rebuild, or scan are, then what the plan does.
	writeTableSizesSection(sb, data)

	fmt.Fprintf(sb, "📋 **Plan**: %s\n\n", planSummaryText(data.Changes, data.DatabaseType, data.IsMySQL, totalStatements))

	// Disclosed directly under the plan summary so the exclusion reads as
	// part of the plan result: what was counted, then what was withheld.
	writeIgnoredNamespaces(sb, data.IgnoredNamespaces)
	writeExemptTables(sb, data.ExemptTables)
}

// planSummaryText renders what a plan would do as the plan summary counts it,
// e.g. "**2** alters, **1** vschema update", from the plan's changes and its
// counted statements.
func planSummaryText(changes []KeyspaceChangeData, databaseType string, isMySQL bool, totalStatements int) string {
	parts := ui.PlanSummaryParts(countStatementTypes(changes, databaseType), totalStatements, true)
	vschemaUpdates, finalizes := countKeyspaceUpdates(changes)
	if vschemaUpdates > 0 && !isMySQL {
		parts = append(parts, fmt.Sprintf("**%d** vschema %s", vschemaUpdates, pluralize("update", vschemaUpdates)))
	}
	if finalizes > 0 {
		parts = append(parts, fmt.Sprintf("**%d** %s to finalize", finalizes, pluralize("keyspace", finalizes)))
	}
	if len(parts) == 0 {
		// Fallback for unrecognized statement types
		return fmt.Sprintf("%d DDL %s", totalStatements, pluralize("statement", totalStatements))
	}
	return strings.Join(parts, ", ")
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
		quoted[i] = inlineCode(ns)
	}
	fmt.Fprintf(sb, glyph.Info+" Namespaces excluded from this plan by `ignore_namespaces`: %s\n\n", strings.Join(quoted, ", "))
}

// exemptTablesInlineLimit caps how many exempt table names render inline in
// the disclosure. Beyond it the names read as a wall standing above the plan
// they annotate, so the disclosure leads with its count and folds the names
// into a collapsed block. The count rides on the visible line either way: a
// reviewer has to be able to see that the plan withheld tables, and how many,
// without opening anything.
const exemptTablesInlineLimit = 5

// writeExemptTables renders one disclosure line per namespace holding live
// tables no schema file declares that the plan leaves in place rather than
// dropping, so a reviewer can tell an exempted table from a declared one.
// No-op when nothing was exempted, which is the ordinary case.
func writeExemptTables(sb *strings.Builder, groups []ExemptTablesData) {
	for _, group := range groups {
		writeExemptTablesGroup(sb, group, "")
	}
}

// writeExemptTablesGroup renders one namespace's exempt-table disclosure,
// naming the environment when the caller breaks the disclosure down per
// environment. The namespace and table names come from the target's catalog
// and the reason is engine prose, so each is escaped for the surface it lands
// on: markdown inline, HTML inside the <summary> of the collapsed form.
func writeExemptTablesGroup(sb *strings.Builder, group ExemptTablesData, env string) {
	if len(group.Tables) == 0 {
		return
	}
	names := strings.Join(inlineCodeList(group.Tables), ", ")

	if len(group.Tables) <= exemptTablesInlineLimit {
		prefix, noun := "", "Ignored tables"
		if env != "" {
			prefix, noun = fmt.Sprintf("**%s**: ", capitalizeFirst(env)), "ignored tables"
		}
		fmt.Fprintf(sb, glyph.Info+" %s%s in namespace %s (%s): %s\n\n",
			prefix, noun, inlineCode(group.Namespace), exemptReason(group.Reason), names)
		return
	}

	// GitHub renders <summary> content as HTML, not markdown, so the folded
	// header names the namespace in a <code> tag and escapes as HTML.
	prefix := ""
	if env != "" {
		prefix = fmt.Sprintf("<b>%s</b>: ", html.EscapeString(capitalizeFirst(env)))
	}
	fmt.Fprintf(sb, "<details>\n<summary>"+glyph.Info+" %s%d ignored tables in namespace <code>%s</code> (%s)</summary>\n\n%s\n\n</details>\n\n",
		prefix, len(group.Tables), html.EscapeString(flattenIdentifier(group.Namespace)),
		html.EscapeString(SanitizeInlineError(group.Reason)), names)
}

// hasExemptTables reports whether writeExemptTables would render anything for
// these groups. It counts tables rather than groups, so a namespace entry
// that carries no tables does not earn the disclosure its spacing.
func hasExemptTables(groups []ExemptTablesData) bool {
	for _, group := range groups {
		if len(group.Tables) > 0 {
			return true
		}
	}
	return false
}

func exemptReason(reason string) string {
	return escapeInlineMarkdown(SanitizeInlineError(reason))
}

// writeMultiEnvExemptTables renders the exempt-table disclosure for the
// all-environments-clean path, where no per-environment sections exist to
// carry it. When every environment exempted the same tables it renders the
// shared lines once; otherwise one set of lines per environment, since the
// live tables can differ per environment.
func writeMultiEnvExemptTables(sb *strings.Builder, data MultiEnvPlanCommentData) {
	if !multiEnvHasExemptTables(data) {
		return
	}
	first := planExemptTables(data, data.Environments[0])
	identical := true
	for _, env := range data.Environments[1:] {
		if !slices.EqualFunc(planExemptTables(data, env), first, exemptTablesEqual) {
			identical = false
			break
		}
	}
	if identical {
		writeExemptTables(sb, first)
		return
	}
	for _, env := range data.Environments {
		for _, group := range planExemptTables(data, env) {
			writeExemptTablesGroup(sb, group, env)
		}
	}
}

// multiEnvHasExemptTables reports whether any environment's plan exempted
// tables, so callers can decide whether the disclosure (and its spacing)
// renders at all.
func multiEnvHasExemptTables(data MultiEnvPlanCommentData) bool {
	for _, env := range data.Environments {
		if hasExemptTables(planExemptTables(data, env)) {
			return true
		}
	}
	return false
}

// planExemptTables returns the exempt tables for one environment, or nil when
// that environment has no plan (for example, it failed to plan).
func planExemptTables(data MultiEnvPlanCommentData, env string) []ExemptTablesData {
	plan, ok := data.Plans[env]
	if !ok || plan == nil {
		return nil
	}
	return plan.ExemptTables
}

func exemptTablesEqual(a, b ExemptTablesData) bool {
	return a.Namespace == b.Namespace && a.Reason == b.Reason && slices.Equal(a.Tables, b.Tables)
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
			quoted[i] = inlineCode(ns)
		}
		fmt.Fprintf(sb, glyph.Info+" **%s**: namespaces excluded from this plan by `ignore_namespaces`: %s\n\n", capitalizeFirst(env), strings.Join(quoted, ", "))
	}
}

// noChangesDetected is the line that closes a comment with nothing to apply.
// Its ✅ says the whole plan is done, so it is never written while any shard or
// target still has work.
const noChangesDetected = "✅ **No schema changes detected**"

// groupNoChanges is written under a shard or target group with nothing to
// apply. Such a group only renders beside a group that still has work, so it
// carries no ✅ and no emphasis: the rollout is not done, and the groups that
// have work are what the reader needs to find.
const groupNoChanges = "No schema changes detected"

// changingTargetCount counts the rollout's members whose own plan runs work.
//
// It answers only for a grouped rollup, which is a clean independent one: a
// mirrored rollup that passed has already established that every member matches
// the reviewed plan, so an empty reviewed plan is empty everywhere, and a rollup
// that did not pass carries no per-member plans to count.
func changingTargetCount(drift *DeploymentDriftData) int {
	if drift == nil {
		return 0
	}
	var changing int
	for _, g := range drift.Plans {
		if !g.Empty() {
			changing += len(g.Members)
		}
	}
	return changing
}

// writeNoChangesDetected closes a comment with nothing to apply. It is never
// reached while another target still has work: an empty reviewed plan is a
// no-op only for the reviewed target, so those comments render the other
// targets' plans and summary instead.
func writeNoChangesDetected(sb *strings.Builder, data PlanCommentData) {
	sb.WriteString(noChangesDetected + "\n")
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
	counts := countStatementTypes(data.Changes, data.DatabaseType)
	totalStatements, keyspaceUpdates := countChanges(data.Changes)

	parts := ui.AssemblePlanSummary(counts, totalStatements,
		func(count int, noun, op string) string {
			if noun == "index" {
				return fmt.Sprintf("%d index %s", count, pluralize(op, count))
			}
			return fmt.Sprintf("%d %s", count, pluralize(op, count))
		},
		func(count int, other bool) string {
			prefix := ""
			if other {
				prefix = "other "
			}
			return fmt.Sprintf("%d %sDDL %s", count, prefix, pluralize("statement", count))
		})
	ddlSummary := strings.Join(parts, ", ")

	summaries := []string{}
	if ddlSummary != "" {
		summaries = append(summaries, ddlSummary)
	}
	if keyspaceUpdates > 0 {
		vschemaUpdates, finalizes := countKeyspaceUpdates(data.Changes)
		if vschemaUpdates > 0 && !data.IsMySQL {
			summaries = append(summaries, fmt.Sprintf("%d vschema %s", vschemaUpdates, pluralize("update", vschemaUpdates)))
		}
		if finalizes > 0 {
			summaries = append(summaries, fmt.Sprintf("%d %s to finalize", finalizes, pluralize("keyspace", finalizes)))
		}
	}
	return strings.Join(summaries, " · ")
}

// countStatementTypes counts CREATE, ALTER, DROP, and other statements across all
// keyspaces with each dialect's parser, counting a valid greenfield create set
// as one create. It walks the statements the comment renders
// (keyspaceStatements), so a sharded keyspace is counted from its per-shard
// changes. The create/alter/drop counts are per table: when shards diverge,
// one table can render two different ALTER statements, and it is still one
// table to alter. An index build or drop on an existing table is counted in
// its own bucket, one per statement. A statement the parser rejects or a
// recognized statement outside every bucket contributes to other so the
// summary stays complete; the shared counter decides which bucket each
// classified statement lands in, so the CLI and the comment cannot disagree
// on it. A database type with no registered parser yields no counts at all,
// and the callers' raw-total fallback carries the statement count.
func countStatementTypes(changes []KeyspaceChangeData, databaseType string) ui.PlanCounts {
	var counts ui.PlanCounts
	parser, err := ddl.ParserForDialect(schema.DialectForDatabaseType(databaseType))
	if err != nil {
		slog.Warn("plan summary cannot classify statements; the summary will report the raw DDL statement total instead of create/alter/drop counts",
			"database_type", databaseType, "error", err)
		// The callers' raw-total fallback carries the count when classification
		// is unavailable.
		return counts
	}
	for _, ks := range changes {
		for _, stmt := range keyspaceStatements(ks) {
			stmtType, table, classifyErr := parser.Classify(stmt)
			if classifyErr != nil {
				createSet, createSetErr := ddl.ParseCreateSet(parser, stmt)
				if createSetErr != nil {
					slog.Warn("plan summary could not classify a statement or parse it as a supported create set; it is left out of the create/alter/drop counts",
						"database_type", databaseType, "keyspace", ks.Keyspace,
						"classify_error", classifyErr, "create_set_error", createSetErr)
					counts.AddOther()
					continue
				}
				stmtType, table = createSet.Type, createSet.Table
			}
			counts.AddTable(ks.Keyspace, ddl.StatementTypeToOp(stmtType), table)
		}
	}
	return counts
}

// writeKeyspaceChanges renders each keyspace's DDL and VSchema changes, with
// the DDL blocks drawing on the comment's shared budget.
func writeKeyspaceChanges(sb *strings.Builder, data PlanCommentData, budget *ddlBlockBudget) {
	defer budget.pointAt(storedPlanRef{cliName: data.CLIName, environment: data.Environment, id: data.PlanID})()

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
		if !hasDDLChanges && !hasVSchemaChanges && !ks.Finalize {
			continue
		}

		if !singleKeyspace {
			label := "Keyspace"
			if schemaNamespaces {
				label = "Schema Name"
			}
			fmt.Fprintf(sb, "#### %s: %s\n", label, inlineCode(ks.Keyspace))
		}

		if hasVSchemaChanges {
			sb.WriteString("#### VSchema\n")
			if ks.VSchemaDiff != "" {
				writeVSchemaDiffFence(sb, ks.VSchemaDiff, diffBudget)
			} else {
				sb.WriteString("_(diff not available)_\n\n")
			}
		}

		if ks.Finalize && !hasVSchemaChanges && keyspaceStatementCount(ks) == 0 {
			sb.WriteString(keyspaceFinalizeNote)
		}

		if hasDDLChanges {
			if len(ks.Shards) > 0 {
				writeShardedPlanDDL(sb, ks.Shards, dialect, budget)
			} else {
				writePlanDDLBlocks(sb, ks.Statements, dialect, budget)
			}
		}
	}
}

// keyspaceFinalizeNote is the plan comment's line for a keyspace whose only
// work is the finalize the engine asked for. What finalizing does
// is the engine's; the comment only says that it runs and when.
const keyspaceFinalizeNote = "_Finalized by the engine once every shard's DDL has landed._\n\n"

// countPlanDDLBlocks counts the DDL sections writeKeyspaceChanges renders for
// changes — one per unsharded keyspace with statements and one per group of
// shards that share a change — so the comment's DDL budget is shared across
// exactly those sections. A section renders its statements as separate
// blocks, but they draw on the section's share together.
func countPlanDDLBlocks(changes []KeyspaceChangeData) int {
	count := 0
	for _, ks := range changes {
		if len(ks.Shards) == 0 {
			if len(ks.Statements) > 0 {
				count++
			}
			continue
		}
		for _, g := range groupKeyspaceShardsByStatements(ks.Shards) {
			if !g.Satisfied {
				count++
			}
		}
	}
	return count
}

// tableSizesInlineLimit caps how many tables the size section lists one per
// line. A plan that changes many tables would otherwise stand a wall of sizes
// above the summary it introduces, so beyond the limit the section leads with
// the count and the largest tables and folds the rest into a collapsed block.
const tableSizesInlineLimit = 10

// tableSizesLargestShown is how many tables stay visible when the section is
// folded: the largest, since they bound how long the apply runs.
const tableSizesLargestShown = 5

// tableSizesPerTargetLimit is the most rollout targets a size line breaks
// down one by one. Past it the line gives the total alone, since a list of
// every target's size would bury the figure the operator reads first.
const tableSizesPerTargetLimit = 2

// tableSizesListedLimit caps how many tables a folded size section lists in
// total, visible and collapsed. Collapsed lines still count toward GitHub's
// comment size limit, and a plan that indexes thousands of tables would
// otherwise spend on sizes the room its DDL needs. Tables past the cap are
// counted in the heading and in a closing line instead.
const tableSizesListedLimit = 50

// tableSizeEntry is one line of the size section: the table's display name,
// qualified with its keyspace when the plan spans several, and its sizes.
type tableSizeEntry struct {
	name string
	// size is the table's estimate on a single-target plan.
	size TableSizeData
	// perTarget is each rollout target's estimate for the table, in rollout
	// order, on a multi-target plan. Nil on a single-target plan, which names
	// no target.
	perTarget []TargetTableSize
}

// multiTarget reports that the line is for a table a rollout changes.
func (e tableSizeEntry) multiTarget() bool {
	return e.perTarget != nil
}

// totalBytes sums the table's byte estimates across the targets that report
// one, and counts those targets.
func (e tableSizeEntry) totalBytes() (total int64, sized int) {
	for _, ts := range e.perTarget {
		if !hasSizeEstimate(ts.Size) {
			continue
		}
		total += *ts.Size.EstimatedBytes
		sized++
	}
	return total, sized
}

// rankBytes is the figure the section ranks the table by: its estimate, or on
// a multi-target plan the total across the targets that report one. Nil when
// no estimate is known.
func (e tableSizeEntry) rankBytes() *int64 {
	if !e.multiTarget() {
		return e.size.EstimatedBytes
	}
	total, sized := e.totalBytes()
	if sized == 0 {
		return nil
	}
	return &total
}

// incomplete reports that some estimate for the table is missing, so its rank
// in the section may understate it.
func (e tableSizeEntry) incomplete() bool {
	if !e.multiTarget() {
		return !hasSizeEstimate(e.size)
	}
	_, sized := e.totalBytes()
	return sized < len(e.perTarget)
}

// hasAnyEstimate reports whether the line carries a byte estimate for at
// least one target.
func (e tableSizeEntry) hasAnyEstimate() bool {
	if !e.multiTarget() {
		return hasSizeEstimate(e.size)
	}
	_, sized := e.totalBytes()
	return sized > 0
}

// writeTableSizesSection renders the plan's table-size info section: one line
// per table the plan will copy, rebuild, or scan (the comment builder
// attaches sizes only to statements whose cost scales with table size),
// across every keyspace, placed above the plan summary. A plan of only
// metadata-only statements renders no section at all, and neither does a plan
// where no table has an estimate: an engine that does not estimate sizes
// would otherwise show "unavailable" on every line, which reads as a failed
// probe when none ran. Table names carry
// their keyspace when the plan spans more than one keyspace with sizes, so a
// shared table name stays unambiguous.
func writeTableSizesSection(sb *strings.Builder, data PlanCommentData) {
	entries := tableSizeEntries(data.Changes)
	multiTarget := data.DeploymentDrift != nil && len(data.DeploymentDrift.TableSizes) > 0
	if multiTarget {
		entries = targetTableSizeEntries(data.DeploymentDrift.TableSizes)
	}
	if !slices.ContainsFunc(entries, tableSizeEntry.hasAnyEstimate) {
		return
	}
	if len(entries) <= tableSizesInlineLimit {
		sb.WriteString("📊 **Table sizes**:\n")
		writeTableSizeLines(sb, entries)
		sb.WriteString("\n")
		return
	}
	writeFoldedTableSizes(sb, entries, multiTarget)
}

// tableSizeEntries flattens every keyspace's sized tables into section lines,
// in plan order.
func tableSizeEntries(changes []KeyspaceChangeData) []tableSizeEntry {
	keyspacesWithSizes := 0
	for _, ks := range changes {
		if len(ks.TableSizes) > 0 {
			keyspacesWithSizes++
		}
	}
	qualify := keyspacesWithSizes > 1
	var entries []tableSizeEntry
	for _, ks := range changes {
		for _, ts := range ks.TableSizes {
			name := ts.Table
			if qualify {
				name = ks.Keyspace + "." + ts.Table
			}
			entries = append(entries, tableSizeEntry{name: name, size: ts})
		}
	}
	return entries
}

// targetTableSizeEntries groups every target's sizes into one section line per
// table, in the order the tables first appear in rollout order, each line
// carrying the table's estimate on every target that changes it.
func targetTableSizeEntries(sizes []TargetTableSize) []tableSizeEntry {
	keyspaces := make(map[string]struct{})
	for _, ts := range sizes {
		keyspaces[ts.Keyspace] = struct{}{}
	}
	qualify := len(keyspaces) > 1
	type tableKey struct{ keyspace, table string }
	at := make(map[tableKey]int)
	var entries []tableSizeEntry
	for _, ts := range sizes {
		key := tableKey{ts.Keyspace, ts.Size.Table}
		i, seen := at[key]
		if !seen {
			name := ts.Size.Table
			if qualify {
				name = ts.Keyspace + "." + ts.Size.Table
			}
			i = len(entries)
			at[key] = i
			entries = append(entries, tableSizeEntry{name: name, perTarget: []TargetTableSize{}})
		}
		entries[i].perTarget = append(entries[i].perTarget, ts)
	}
	return entries
}

// writeFoldedTableSizes renders the size section for a plan with more tables
// than tableSizesInlineLimit. The visible heading carries the table count and,
// when any table is missing an estimate, how many: a size probe that failed on
// a large table must stay visible even though its line is folded. The largest
// tables follow, then the rest in a collapsed block, all ordered largest first.
// Past tableSizesListedLimit the smallest tables are counted in a closing line
// rather than listed.
func writeFoldedTableSizes(sb *strings.Builder, entries []tableSizeEntry, multiTarget bool) {
	sorted := slices.Clone(entries)
	slices.SortStableFunc(sorted, compareTableSizesLargestFirst)

	incomplete := 0
	for _, e := range sorted {
		if e.incomplete() {
			incomplete++
		}
	}
	heading := fmt.Sprintf("%d tables, largest first", len(sorted))
	switch {
	case incomplete == 0:
	case multiTarget:
		heading += fmt.Sprintf("; %d missing an estimate on at least one target", incomplete)
	default:
		heading += fmt.Sprintf("; %d without a size estimate", incomplete)
	}
	fmt.Fprintf(sb, "📊 **Table sizes** (%s):\n", heading)
	writeTableSizeLines(sb, sorted[:tableSizesLargestShown])

	rest := sorted[tableSizesLargestShown:]
	fmt.Fprintf(sb, "\n<details>\n<summary>%d more %s</summary>\n\n", len(rest), pluralize("table", len(rest)))
	listed := min(len(sorted), tableSizesListedLimit)
	writeTableSizeLines(sb, sorted[tableSizesLargestShown:listed])
	if unlisted := len(sorted) - listed; unlisted > 0 {
		fmt.Fprintf(sb, "- …and %d more %s\n", unlisted, pluralize("table", unlisted))
	}
	sb.WriteString("\n</details>\n\n")
}

func writeTableSizeLines(sb *strings.Builder, entries []tableSizeEntry) {
	for _, e := range entries {
		fmt.Fprintf(sb, "- `%s`: %s\n", e.name, formatTableSizeEntry(e))
	}
}

// formatTableSizeEntry renders a section line's size clause. On a multi-target
// plan a table one target changes names that target; a table two targets
// change gives the total and each target's size; a table more targets change
// gives the total alone. A target with no estimate is counted rather than
// left out, since the total then understates the table.
func formatTableSizeEntry(e tableSizeEntry) string {
	if !e.multiTarget() {
		return formatTableSize(e.size)
	}
	targets := len(e.perTarget)
	total, sized := e.totalBytes()
	switch {
	case sized == 0 && targets == 1:
		return fmt.Sprintf("size estimate unavailable on `%s`", e.perTarget[0].Target)
	case sized == 0:
		return fmt.Sprintf("size estimate unavailable on all %d targets", targets)
	case targets == 1:
		return fmt.Sprintf("%s on `%s`", ui.FormatApproxBytes(total), e.perTarget[0].Target)
	case targets <= tableSizesPerTargetLimit:
		return formatPerTargetSizes(e.perTarget)
	case sized == targets:
		return fmt.Sprintf("%s across %d targets", ui.FormatApproxBytes(total), targets)
	default:
		unsized := targets - sized
		verb := "has"
		if unsized > 1 {
			verb = "have"
		}
		return fmt.Sprintf("%s across %d of %d targets; %d %s no estimate",
			ui.FormatApproxBytes(total), sized, targets, unsized, verb)
	}
}

// formatPerTargetSizes renders a table's size on each of a few targets, led by
// the total when every target reports one. A target with no estimate is
// named as such, and the total is left off since it would understate the
// table.
func formatPerTargetSizes(perTarget []TargetTableSize) string {
	var total int64
	complete := true
	parts := make([]string, 0, len(perTarget))
	for _, ts := range perTarget {
		if !hasSizeEstimate(ts.Size) {
			complete = false
			parts = append(parts, fmt.Sprintf("size estimate unavailable on `%s`", ts.Target))
			continue
		}
		total += *ts.Size.EstimatedBytes
		parts = append(parts, fmt.Sprintf("%s on `%s`", ui.FormatApproxBytes(*ts.Size.EstimatedBytes), ts.Target))
	}
	if !complete {
		return strings.Join(parts, ", ")
	}
	return fmt.Sprintf("%s across %d targets (%s)", ui.FormatApproxBytes(total), len(perTarget), strings.Join(parts, ", "))
}

// hasSizeEstimate reports that the table's on-disk footprint is known. The
// section shows bytes alone: they track how long a copy or index build runs
// on every engine that reports a size.
func hasSizeEstimate(ts TableSizeData) bool {
	return ts.EstimatedBytes != nil
}

// compareTableSizesLargestFirst orders two section lines for the folded size
// section, by bytes. A table with no estimate sorts after every table with
// one: its size is unknown, not small, and the heading already counts it.
func compareTableSizesLargestFirst(a, b tableSizeEntry) int {
	ab, bb := a.rankBytes(), b.rankBytes()
	switch {
	case ab == nil && bb == nil:
		return 0
	case ab == nil:
		return 1
	case bb == nil:
		return -1
	default:
		return cmp.Compare(*bb, *ab)
	}
}

// formatTableSize renders one table's size clause: the estimated bytes and,
// for a sharded target, the shard span. A table with no estimate is stated
// explicitly — operators must never mistake a failed size probe for a small
// table.
func formatTableSize(ts TableSizeData) string {
	if !hasSizeEstimate(ts) {
		if ts.ShardCount > 0 {
			return fmt.Sprintf("size estimate unavailable · %d %s", ts.ShardCount, pluralize("shard", ts.ShardCount))
		}
		return "size estimate unavailable"
	}
	size := ui.FormatApproxBytes(*ts.EstimatedBytes)
	if ts.ShardCount > 0 {
		size += fmt.Sprintf(" across %d %s", ts.ShardCount, pluralize("shard", ts.ShardCount))
	}
	return size
}

// writePlanDDLBlocks writes one fenced SQL block per statement, in plan
// order, so a reviewer reads each table's change on its own instead of
// picking it out of one run of every statement in the plan. Each statement is
// one unit of work the apply runs, so the blocks also mirror how the change
// will be applied. Blocks are formatted under the plan's own dialect and
// together draw one share of the comment's DDL budget. A greenfield create
// set stays one block — the table and the indexes it ships with — with each
// of its statements formatted on its own line. Rendering is best-effort, so a
// statement that is neither a single statement nor a valid create set is
// still rendered as written, and the reason is logged for triage.
func writePlanDDLBlocks(sb *strings.Builder, statements []string, dialect schema.Dialect, budget *ddlBlockBudget) {
	writeSQLFencedBlocks(sb, formatDDLBlocks(statements, dialect), budget)
	sb.WriteString("\n")
}

// formatDDLBlocks formats each statement as the content of its own SQL block,
// as writePlanDDLBlocks describes.
func formatDDLBlocks(statements []string, dialect schema.Dialect) []string {
	blocks := make([]string, 0, len(statements))
	parser, parserErr := ddl.ParserForDialect(dialect)
	if parserErr != nil {
		slog.Warn("DDL block cannot split create sets; multi-statement DDL will be rendered as written",
			"dialect", dialect, "error", parserErr)
	}
	for _, stmt := range statements {
		statementsToFormat := []string{stmt}
		if parserErr == nil {
			if _, _, classifyErr := parser.Classify(stmt); classifyErr != nil {
				createSet, createSetErr := ddl.ParseCreateSet(parser, stmt)
				if createSetErr != nil {
					slog.Warn("DDL block could not classify a statement or parse it as a supported create set; it will be rendered as written",
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
		blocks = append(blocks, strings.Join(formattedCreateSet, "\n"))
	}
	return blocks
}

// writeShardedPlanDDL renders a sharded keyspace's DDL grouped by change: shards
// that need the same statements share one section, so a uniform keyspace shows the
// DDL once and a divergent one shows "what applies where" — each distinct change
// set with the shards it applies to.
func writeShardedPlanDDL(sb *strings.Builder, shards []KeyspaceShardChange, dialect schema.Dialect, budget *ddlBlockBudget) {
	groups := groupKeyspaceShardsByStatements(shards)
	if len(groups) <= 1 {
		// A single group of changing shards shows the DDL once, but still names the
		// shards it applies to — a sharded plan must always show which shards are
		// affected, even when the change is uniform across them. A lone group of
		// satisfied shards means nothing is changing, so render nothing rather than
		// an empty code block.
		if len(groups) == 1 && !groups[0].Satisfied {
			writeShardGroupHeading(sb, groups[0].Shards, len(shards))
			writePlanDDLBlocks(sb, groups[0].Statements, dialect, budget)
		}
		return
	}
	sb.WriteString("Shards diverge — what applies where:\n\n")
	for _, g := range groups {
		writeShardGroupHeading(sb, g.Shards, len(shards))
		// A satisfied group already matches the desired schema; say so instead
		// of rendering an empty code block.
		if g.Satisfied {
			sb.WriteString(groupNoChanges + "\n\n")
			continue
		}
		writePlanDDLBlocks(sb, g.Statements, dialect, budget)
	}
}

type keyspaceShardGroup struct {
	Shards     []string
	Statements []string
	Satisfied  bool
}

// groupKeyspaceShardsByStatements buckets shards whose statement set and
// satisfied status are identical, so a uniform keyspace yields one group.
// Groups with work come first; within each half they keep resolved order.
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
	slices.SortStableFunc(groups, func(a, b keyspaceShardGroup) int {
		return compareWorkFirst(a.Satisfied, b.Satisfied)
	})
	return groups
}

// compareWorkFirst orders a group with work to run ahead of one already at the
// desired schema, and otherwise leaves the two where they were.
func compareWorkFirst(aDone, bDone bool) int {
	switch {
	case aDone == bDone:
		return 0
	case bDone:
		return -1
	default:
		return 1
	}
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

// shardNamesInlineLimit caps how many member names render inline in a PR
// comment, for the shards of a keyspace and the targets of a rollout alike.
// Beyond it, listing every name reads as a wall — a wide group collapses to a
// count, with the names behind a collapsed block where the rendering has room
// for one.
const shardNamesInlineLimit = 8

// planGroupNoun is what the members of a plan group are called: the shards of a
// keyspace, or the targets of a rollout. Both render through the same group
// headings, so a rollout whose targets need different work reads the way a
// keyspace whose shards do.
type planGroupNoun struct{ singular, plural string }

var (
	shardNoun      = planGroupNoun{singular: "shard", plural: "shards"}
	targetNoun     = planGroupNoun{singular: "target", plural: "targets"}
	deploymentNoun = planGroupNoun{singular: "deployment", plural: "deployments"}
)

// planShardList renders a group's shards as "shard `x`" or "shards `x`, `y`"
// when few enough to read inline, stating coverage beyond that — "12 of 32
// shards", or "all 32 shards" when the group spans the keyspace. Used where
// the list rides inside a line item and has no room for a collapsed name
// list; the full names stay reachable in the DDL section's collapsed
// shard-group blocks.
func planShardList(shards []string, totalShards int) string {
	return planGroupList(shardNoun, shards, totalShards)
}

func planGroupList(noun planGroupNoun, members []string, total int) string {
	if len(members) > shardNamesInlineLimit {
		return groupCoveragePhrase(noun, len(members), total)
	}
	quoted := inlineCodeList(members)
	if len(quoted) == 1 {
		return noun.singular + " " + quoted[0]
	}
	return noun.plural + " " + strings.Join(quoted, ", ")
}

// shardCoveragePhrase states how much of a keyspace a shard group covers:
// "all 32 shards" when it covers every planned shard, "12 of 32 shards" for
// a subset, or a bare count when the keyspace total is unknown — a subset
// must never read like whole-keyspace coverage.
func shardCoveragePhrase(count, totalShards int) string {
	return groupCoveragePhrase(shardNoun, count, totalShards)
}

func groupCoveragePhrase(noun planGroupNoun, count, total int) string {
	if count == total {
		return fmt.Sprintf("all %d %s", count, noun.plural)
	}
	if total > 0 {
		return fmt.Sprintf("%d of %d %s", count, total, noun.plural)
	}
	return fmt.Sprintf("%d %s", count, noun.plural)
}

// writeShardGroupHeading writes a shard group's bold heading above its DDL
// block. Few shards read inline by name; a wide group leads with how much of
// the keyspace it covers — "all 32 shards" when it covers every planned
// shard, "19 of 32 shards" for a subset — as a single collapsed line that
// expands into the full name list, so the names stay reachable without
// walling the comment.
func writeShardGroupHeading(sb *strings.Builder, shards []string, totalShards int) {
	writeGroupHeading(sb, shardNoun, shards, totalShards)
}

func writeGroupHeading(sb *strings.Builder, noun planGroupNoun, members []string, total int) {
	if len(members) <= shardNamesInlineLimit {
		fmt.Fprintf(sb, "**%s**\n\n", planGroupList(noun, members, total))
		return
	}
	fmt.Fprintf(sb, "<details>\n<summary><b>%s</b></summary>\n\n%s\n\n</details>\n\n",
		groupCoveragePhrase(noun, len(members), total), strings.Join(inlineCodeList(members), ", "))
}

// writeDeploymentDrift renders the review-time drift rollup: a single uniform
// line when every member passed its contract and none will refuse a change, or
// a per-member breakdown naming which members diverged, could not be planned or
// verified, or carry changes blocked at apply. It is a no-op for a nil rollup
// (single-target database or drift not evaluated).
//
// The two contracts get different wording throughout, because a clean rollup
// means a different thing under each. Mirrored members agree with the reviewed
// plan; independent members were never compared to it, so the uniform line says
// they were each planned rather than that they match.
func writeDeploymentDrift(sb *strings.Builder, drift *DeploymentDriftData, reviewed []KeyspaceChangeData) {
	if drift == nil {
		return
	}

	if !drift.Computed {
		sb.WriteString(glyph.Attention + " **Could not verify deployment drift** — the plan check is failing closed until it can be confirmed.\n\n")
		return
	}

	// A rollout with nothing left to apply anywhere is said once, by the
	// comment's no-changes line.
	if rolloutAtThisSchema(drift, reviewed) {
		return
	}

	// One deployment can address several targets, so the deployment name alone
	// does not always say which member a line belongs to. The shared naming rule
	// adds the target only where it disambiguates.
	//
	// A member name is assembled from server config, so it reaches this comment
	// as text SchemaBot did not choose. Rendering every one as a code span keeps
	// a name carrying a backtick or a line break from closing the span it sits
	// in and writing markdown of its own into a comment operators act on.
	names := inlineCodeList(driftMemberNames(drift.Deployments))
	switch {
	case drift.Clean && drift.Independent:
		// When targets still have work, each plan renders below under the
		// targets that run it, with any change a target will refuse disclosed
		// under that plan, which says everything this line and the per-target
		// list would.
		if RendersTargetPlans(drift) {
			return
		}
		// Independent members were deliberately never compared to each other, so
		// the mirrored headline would assert agreement the rollup did not check.
		// Without groups, all that is known is the contract.
		writeNamedRolloutLine(sb, fmt.Sprintf("Planned separately for all %d targets", len(drift.Deployments)),
			" — each target holds its own schema, so their plans are not expected to match.", names)
	case drift.Clean:
		writeNamedRolloutLine(sb, fmt.Sprintf("Same plan on all %d deployments", len(drift.Deployments)), ".", names)
	case drift.Independent:
		// A member that could not be planned blocks under either contract, but
		// only mirrored members can be out of agreement with each other. Calling
		// an independent environment's failure "drift" would send an operator to
		// reconcile targets that are supposed to differ.
		sb.WriteString(glyph.Attention + " **Some targets could not be planned** — every target must have a plan before an apply can run, so the plan check is failing closed:\n\n")
	default:
		sb.WriteString(glyph.Attention + " **Deployment drift detected** — some deployments no longer match the reviewed plan, so the plan check is failing closed:\n\n")
	}
	// A clean rollup has nothing more to say unless some member will refuse a
	// change at apply, which the per-member list below is the only place to
	// report.
	if drift.Clean && !anyDeploymentBlocked(drift.Deployments) {
		return
	}
	// A long list keeps the members an operator has to act on inline and folds
	// the ones with nothing to flag, so a large fleet does not bury them.
	fold := len(drift.Deployments) > shardNamesInlineLimit
	var quiet []string
	for i, d := range drift.Deployments {
		line := driftMemberLine(drift, d, names[i])
		if fold && !driftMemberNeedsAttention(d) {
			quiet = append(quiet, line)
			continue
		}
		sb.WriteString(line)
	}
	sb.WriteString("\n")
	if len(quiet) > 0 {
		fmt.Fprintf(sb, "<details>\n<summary>%s</summary>\n\n%s\n</details>\n\n", quietMembersSummary(drift, len(quiet)), strings.Join(quiet, ""))
	}
}

// writeNamedRolloutLine renders a clean rollout's one-line statement followed
// by the members it covers. Past the inline limit the names fold into a details
// block under the statement, the way a wide shard group's heading does, so the
// statement stays one line and the names stay reachable.
func writeNamedRolloutLine(sb *strings.Builder, statement, tail string, names []string) {
	if len(names) <= shardNamesInlineLimit {
		fmt.Fprintf(sb, "**%s** (%s)%s\n\n", statement, strings.Join(names, ", "), tail)
		return
	}
	fmt.Fprintf(sb, "<details>\n<summary><b>%s</b>%s</summary>\n\n%s\n\n</details>\n\n", statement, tail, strings.Join(names, ", "))
}

// driftMemberLine renders one member's line in the per-member drift breakdown.
func driftMemberLine(drift *DeploymentDriftData, d DeploymentDriftEntry, name string) string {
	if d.Primary {
		name += " (primary)"
	}
	switch d.Class {
	case "match":
		return fmt.Sprintf("- %s ✅ matches the reviewed plan%s\n", name, blockedSuffix(d.Blocked))
	case "planned":
		return fmt.Sprintf("- %s ✅ planned against its own schema%s\n", name, blockedSuffix(d.Blocked))
	case "diverged":
		return fmt.Sprintf("- %s "+glyph.Attention+" diverged%s%s\n", name, blockedSuffix(d.Blocked), driftDetailSuffix(d.Detail))
	default:
		// An errored member means different things under the two contracts:
		// a mirrored member's diff could not be confirmed against the
		// reviewed plan, while an independent member has no plan at all.
		reason := "could not verify"
		if drift.Independent {
			reason = "could not plan"
		}
		return fmt.Sprintf("- %s "+glyph.Failed+" %s%s%s\n", name, reason, blockedSuffix(d.Blocked), driftDetailSuffix(d.Detail))
	}
}

// driftMemberNeedsAttention reports whether a member's line carries something
// an operator has to act on: it diverged, could not be planned or verified, or
// carries a change the engine will refuse at apply.
func driftMemberNeedsAttention(d DeploymentDriftEntry) bool {
	switch d.Class {
	case "match", "planned":
		return d.Blocked > 0
	default:
		return true
	}
}

// quietMembersSummary labels the folded members of a long drift breakdown with
// how many there are and what they have in common. Every member of one rollup
// is classified under the same contract, so they share one outcome.
func quietMembersSummary(drift *DeploymentDriftData, count int) string {
	if drift.Independent {
		return fmt.Sprintf("%s ✅ planned against their own schemas", groupCoveragePhrase(targetNoun, count, len(drift.Deployments)))
	}
	return fmt.Sprintf("%s ✅ match the reviewed plan", groupCoveragePhrase(deploymentNoun, count, len(drift.Deployments)))
}

func anyDeploymentBlocked(deployments []DeploymentDriftEntry) bool {
	for _, d := range deployments {
		if d.Blocked > 0 {
			return true
		}
	}
	return false
}

// blockedSuffix renders a deployment's blocked change count as a trailing
// clause, or an empty string when the deployment refuses nothing.
func blockedSuffix(blocked int) string {
	if blocked == 0 {
		return ""
	}
	return fmt.Sprintf(" · blocked: %d", blocked)
}

// rolloutAtThisSchema reports whether a clean rollout has nothing left to apply
// on any member, so the comment's no-changes line speaks for all of them.
//
// Mirrored members run the reviewed plan, so the reviewed plan answers for all
// of them. Independent members answer only through their own grouped plans; a
// rollup that carries none has not shown that the other targets have nothing to
// run.
func rolloutAtThisSchema(drift *DeploymentDriftData, reviewed []KeyspaceChangeData) bool {
	if !drift.Clean || anyDeploymentBlocked(drift.Deployments) {
		return false
	}
	statements, vschema := countChanges(reviewed)
	if statements+vschema > 0 {
		return false
	}
	if drift.Independent {
		return len(drift.Plans) > 0 && changingTargetCount(drift) == 0
	}
	return true
}

// RendersTargetPlans reports whether the comment renders the rollout's plans one
// group of targets at a time instead of the reviewed plan alone: a clean
// rollout of independent targets in which some target still has work.
//
// Each such target applies its own plan, so the reviewed plan describes only
// the targets that share it. Rendering it alone would leave a reviewer to
// approve statements the comment never showed.
func RendersTargetPlans(drift *DeploymentDriftData) bool {
	return drift != nil && drift.Computed && drift.Clean && drift.Independent && changingTargetCount(drift) > 0
}

// targetPlanChanges is the plan a group of targets renders. The reviewed
// target's group renders the reviewed plan itself, so what a reviewer reads for
// it is exactly what the rest of the comment describes; every other group
// renders its own members' plan.
func targetPlanChanges(g DeploymentPlanGroup, data PlanCommentData) []KeyspaceChangeData {
	if g.Primary && hasChanges(data.Changes) {
		return data.Changes
	}
	return g.Changes
}

// targetPlanID is the stored plan a target group's DDL comes from, the one a
// reader who cannot see all of it is pointed at. The primary runs the reviewed
// plan itself and has no member plan of its own.
func targetPlanID(g DeploymentPlanGroup, data PlanCommentData) string {
	if g.Primary {
		return data.PlanID
	}
	return g.PlanID
}

// writeTargetPlans renders the rollout's plans the way a sharded keyspace
// renders its shards: one heading per group naming the targets that run it,
// with the group's DDL under it, and a group already at the desired schema
// saying so in place of DDL. More than one group is introduced as divergence,
// and a single group still names its targets, so every target is shown with
// the plan it runs. collapse folds a plan with more than one change into a
// details block, as a multi-environment section does for its own plan.
func writeTargetPlans(sb *strings.Builder, data PlanCommentData, budget *ddlBlockBudget, collapse bool) {
	drift := data.DeploymentDrift
	if len(drift.Plans) > 1 {
		sb.WriteString("Targets diverge — what applies where:\n\n")
	}
	// Targets with work lead, as changing shards do: they are what the apply
	// will run, and the targets already at the schema follow them.
	plans := slices.Clone(drift.Plans)
	slices.SortStableFunc(plans, func(a, b DeploymentPlanGroup) int {
		return compareWorkFirst(a.Empty(), b.Empty())
	})
	for _, g := range plans {
		writeGroupHeading(sb, targetNoun, g.Members, len(drift.Deployments))
		if g.Empty() {
			sb.WriteString(groupNoChanges + "\n\n")
			continue
		}
		group := data
		group.Changes = targetPlanChanges(g, data)
		group.PlanID = targetPlanID(g, data)
		statements, vschema := countChanges(group.Changes)
		restore := budget.forTargetGroup(g.Members)
		if collapse && statements+vschema > 1 {
			writeCollapsibleKeyspaceChanges(sb, group, statements, budget)
		} else {
			writeKeyspaceChanges(sb, group, budget)
		}
		restore()
		// A refused change is disclosed under the DDL it refuses, naming the
		// targets that refuse it, so the reader sees what fails and where.
		if len(g.BlockedChanges) > 0 {
			writeBlockedChanges(sb, g.BlockedChanges)
		}
	}
}

// targetPlansDiscloseBlocked reports whether the rendered target plans carry
// the plan's blocked changes under their own groups, so the plan-wide section
// would only repeat them. A reviewed plan with blocked changes that no group
// carries keeps the plan-wide section: a refused change is never left unsaid
// because the two sources disagree.
func targetPlansDiscloseBlocked(data PlanCommentData) bool {
	if !RendersTargetPlans(data.DeploymentDrift) {
		return false
	}
	for _, g := range data.DeploymentDrift.Plans {
		if len(g.BlockedChanges) > 0 {
			return true
		}
	}
	return len(data.BlockedChanges) == 0
}

// combinedTargetPlanChanges merges every target's plan into the one change list
// the plan summary counts, the way a sharded keyspace's summary counts each
// distinct statement once however many shards run it.
//
// A target whose only work in a keyspace is a finalize shows that finalize in
// its own plan, so the summary counts it even when another target runs DDL in
// the same keyspace: the keyspace is then listed a second time, with the
// finalize alone.
func combinedTargetPlanChanges(data PlanCommentData) []KeyspaceChangeData {
	var combined []KeyspaceChangeData
	byKeyspace := make(map[string]int)
	seen := make(map[string]map[string]struct{})
	finalizeOnly := make(map[string]bool)
	for _, g := range data.DeploymentDrift.Plans {
		if g.Empty() {
			continue
		}
		for _, ks := range targetPlanChanges(g, data) {
			i, ok := byKeyspace[ks.Keyspace]
			if !ok {
				i = len(combined)
				byKeyspace[ks.Keyspace] = i
				combined = append(combined, KeyspaceChangeData{Keyspace: ks.Keyspace})
				seen[ks.Keyspace] = make(map[string]struct{})
			}
			combined[i].VSchemaChanged = combined[i].VSchemaChanged || ks.VSchemaChanged
			if finalizeIsOnlyWork(ks) {
				finalizeOnly[ks.Keyspace] = true
			}
			for _, stmt := range keyspaceStatements(ks) {
				if _, dup := seen[ks.Keyspace][stmt]; dup {
					continue
				}
				seen[ks.Keyspace][stmt] = struct{}{}
				combined[i].Statements = append(combined[i].Statements, stmt)
			}
		}
	}
	for i, n := 0, len(combined); i < n; i++ {
		ks := combined[i].Keyspace
		if !finalizeOnly[ks] {
			continue
		}
		if keyspaceStatementCount(combined[i]) == 0 && !combined[i].VSchemaChanged {
			combined[i].Finalize = true
			continue
		}
		combined = append(combined, KeyspaceChangeData{Keyspace: ks, Finalize: true})
	}
	return combined
}

// countCommentDDLBlocks counts the DDL sections a plan's comment renders: the
// reviewed plan's, or every target plan's when the rollout renders them.
func countCommentDDLBlocks(data PlanCommentData) int {
	if !RendersTargetPlans(data.DeploymentDrift) {
		return countPlanDDLBlocks(data.Changes)
	}
	count := 0
	for _, g := range data.DeploymentDrift.Plans {
		if !g.Empty() {
			count += countPlanDDLBlocks(targetPlanChanges(g, data))
		}
	}
	return count
}

// driftDetailSuffix renders a deployment's drift detail as a trailing clause, or
// an empty string when there is no detail.
func driftDetailSuffix(detail string) string {
	if detail == "" {
		return ""
	}
	return " — " + detail
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
		table := inlineCode(c.Table)
		if len(c.Shards) > 0 {
			table = fmt.Sprintf("%s (%s)", table, planShardList(c.Shards, c.TotalShards))
		}
		if len(c.Targets) > 0 {
			table = fmt.Sprintf("%s on %s", table, planGroupList(targetNoun, c.Targets, c.TotalTargets))
		}
		writeEngineReasonItem(sb, table, c.Reason)
	}
	sb.WriteString("\nAn apply will fail on these statements. Fix what each reason names — rewrite an unsupported change, or provision the stated access — or contact your SchemaBot operators for help.\n\n")
}

// directDisclosureCopy returns the header noun and the consequence sentence for
// the direct-execution disclosure, keyed by database type. What a
// direct statement does to the table while it runs is engine-specific — an
// engine that adopts direct execution adds its own copy here rather than
// inheriting another engine's semantics.
//
// The consequence states only what sets a direct statement apart from the
// rest of the plan. Undoing one is the same inverse schema change as for any other
// MySQL change, none of which has a revert window, so the MySQL copy does not
// warn about reverting.
func directDisclosureCopy(databaseType string, isMySQL bool) (headerNoun, consequence string) {
	// Strata is sharded MySQL: a direct statement there is the same native
	// MySQL DDL, executed per shard.
	databaseType = strings.TrimSpace(databaseType)
	if databaseType == storage.DatabaseTypeMySQL || databaseType == storage.DatabaseTypeStrata || isMySQL {
		return "native MySQL DDL, not through Spirit",
			"Transactions blocking a table's metadata lock are killed so its statement can take the lock, and writes to each table are blocked until its statement finishes."
	}
	// Deliberately conservative fallback for an engine that emits direct
	// verdicts without registering its own copy above: disclose the broadest
	// impact rather than understate what the change does.
	return "native DDL",
		"Each table is unavailable until its statement finishes, and the change is **not revertible**."
}

// writePausedApplyCause renders the cause in the same shape as the disclosures
// around it, so a reader scanning for warnings finds this one where they find
// the rest rather than below the plan in the footer.
func writePausedApplyCause(sb *strings.Builder, cause *PausedApplyCauseData) {
	fmt.Fprintf(sb, glyph.Attention+" **%s**\n", cause.Heading)
	for _, entry := range cause.Entries {
		fmt.Fprintf(sb, "- %s\n", entry)
	}
	if cause.Remedy != "" {
		fmt.Fprintf(sb, "\n%s\n", cause.Remedy)
	}
	sb.WriteString("\n")
}

// writeDirectChanges writes the section for statements the direct execution
// policy routes to native DDL, naming each table and the planner's reason
// (the table's measured size). The footer says what running them does to the
// table. It mentions --defer-cutover only when the flag is or can still be in
// play: a direct statement has no cutover, so the flag leaves these statements
// alone even though it defers the rest of the plan's.
func writeDirectChanges(sb *strings.Builder, changes []DirectChangeData, databaseType string, isMySQL, deferCutover bool) {
	headerNoun, consequence := directDisclosureCopy(databaseType, isMySQL)
	footer := consequence
	if deferCutover {
		footer += " `--defer-cutover` does not apply to these direct statements: they have no cutover to defer."
	}
	n := len(changes)
	fmt.Fprintf(sb, "⚙️ **Direct execution**: %d %s will run as %s\n", n, pluralize("change", n), headerNoun)
	for _, c := range changes {
		table := inlineCode(c.Table)
		if len(c.Shards) > 0 {
			table = fmt.Sprintf("%s (%s)", table, planShardList(c.Shards, c.TotalShards))
		}
		writeEngineReasonItem(sb, table, c.Reason)
	}
	sb.WriteString("\n" + footer + "\n\n")
}

// writeEngineReasonItem keeps one cause in the established one-line shape and
// gives each additional independent cause its own nested Markdown bullet. A
// cause is judged empty after sanitization, so a reason made only of
// characters the sanitizer strips falls back to the bare table line instead of
// a dangling colon.
func writeEngineReasonItem(sb *strings.Builder, table, reason string) {
	causes := make([]string, 0)
	for _, cause := range engine.BlockedCauses(reason) {
		if cause = SanitizeInlineError(cause); cause != "" {
			causes = append(causes, cause)
		}
	}
	if len(causes) == 0 {
		fmt.Fprintf(sb, "- %s\n", table)
		return
	}
	fmt.Fprintf(sb, "- %s: %s\n", table, escapeInlineMarkdown(causes[0]))
	for _, cause := range causes[1:] {
		fmt.Fprintf(sb, "  - %s\n", escapeInlineMarkdown(cause))
	}
}

func writeUnsafeWarning(sb *strings.Builder, changes []UnsafeChangeData, databaseType string, isMySQL bool) {
	n := countUnsafeFindings(changes)
	fmt.Fprintf(sb, glyph.Attention+" **Issues**: %d unsafe %s detected\n", n, pluralize("change", n))
	item := 0
	for _, c := range changes {
		table := inlineCode(c.Table)
		if len(c.Shards) > 0 {
			table = fmt.Sprintf("%s (%s)", table, planShardList(c.Shards, c.TotalShards))
		}
		writeUnsafeChangeItem(sb, &item, table, c.Reason, c.ChangeType)
	}
	sb.WriteString("\n")
	writeUnsafeDropGuidance(sb, changes, databaseType, isMySQL)
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

func writeUnsafeDropGuidance(sb *strings.Builder, changes []UnsafeChangeData, databaseType string, isMySQL bool) {
	drops := countUnsafeDrops(changes, databaseType)
	applicationUsageTarget, hasApplicationUsageTarget := unsafeDropApplicationUsageTarget(drops)
	indexActionTarget, indexInvisibleTarget, indexQueryTarget, hasIndexUsageTarget := unsafeDropIndexUsageTargets(drops)
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

func unsafeDropApplicationUsageTarget(drops unsafeDropCounts) (string, bool) {
	dropColumns, dropTables := drops.columns, drops.tables
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

func unsafeDropIndexUsageTargets(drops unsafeDropCounts) (actionTarget, invisibleTarget, queryTarget string, ok bool) {
	if drops.indexes == 1 {
		return "an index", "the dropped index", "it", true
	}
	if drops.indexes > 1 {
		return "indexes", "any dropped indexes", "them", true
	}
	return "", "", "", false
}

// unsafeDropCounts is what the plan's unsafe changes drop, summed across
// changes, so the guidance can name the kind of object an application must
// stop relying on before the drop is allowed.
type unsafeDropCounts struct {
	columns int
	tables  int
	indexes int
}

func (c unsafeDropCounts) add(other unsafeDropCounts) unsafeDropCounts {
	return unsafeDropCounts{columns: c.columns + other.columns, tables: c.tables + other.tables, indexes: c.indexes + other.indexes}
}

// countUnsafeDrops classifies each unsafe change through the target dialect's
// parser. A change whose statement is unavailable or does not parse falls
// back to the words in its reason, so guidance still renders for plans that
// predate stored DDL.
func countUnsafeDrops(changes []UnsafeChangeData, databaseType string) unsafeDropCounts {
	parser, err := ddl.ParserForDialect(schema.DialectForDatabaseType(databaseType))
	if err != nil {
		slog.Warn("destructive drop guidance falls back to the unsafe reasons because no statement parser serves the database type",
			"database_type", databaseType, "error", err)
		parser = nil
	}
	var total unsafeDropCounts
	for _, change := range changes {
		total = total.add(classifyUnsafeDrops(change, databaseType, parser))
	}
	return total
}

func classifyUnsafeDrops(change UnsafeChangeData, databaseType string, parser ddl.StatementParser) unsafeDropCounts {
	if parser != nil && strings.TrimSpace(change.DDL) != "" {
		targets, err := parser.DropTargets(change.DDL)
		if err == nil {
			return dropCountsWithTableFallback(unsafeDropCounts{columns: targets.Columns, tables: targets.Tables, indexes: targets.Indexes}, change.ChangeType)
		}
		slog.Warn("destructive drop guidance falls back to the unsafe reason because the statement did not parse",
			"table", change.Table, "database_type", databaseType, "error", err)
	}

	upperReason := strings.ToUpper(change.Reason)
	return dropCountsWithTableFallback(unsafeDropCounts{
		columns: strings.Count(upperReason, "DROP COLUMN"),
		tables:  strings.Count(upperReason, "DROP TABLE"),
		indexes: strings.Count(upperReason, "DROP INDEX"),
	}, change.ChangeType)
}

// dropCountsWithTableFallback counts a table drop for a change whose type is
// the table drop even when neither its statement nor its reason named one. It
// is a backstop for a stored change that reaches the renderer with neither a
// parseable statement nor a reason that spells out DROP TABLE; every engine
// stores both, so the backstop only matters when a producer omits them.
// Index drops carry their own change type, so they never take this path.
func dropCountsWithTableFallback(drops unsafeDropCounts, changeType string) unsafeDropCounts {
	if strings.EqualFold(changeType, "drop") && drops.tables == 0 {
		drops.tables++
	}
	return drops
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
				fmt.Fprintf(sb, "- %s: %s\n", inlineCode(w.Table), message)
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
			fmt.Fprintf(sb, "**%s**\n", inlineCode(group.table))
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

	// ScopedDatabase carries the operator's -d through to this comment's
	// copy-paste commands, on the same terms as PlanCommentData.ScopedDatabase.
	ScopedDatabase string

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
	return renderWithinCommentLimit(countMultiEnvPlanDDLBlocks(data), 0, func(budget *ddlBlockBudget) string {
		return renderMultiEnvPlanComment(data, budget)
	})
}

// countEnvsWithChanges counts the environments whose plan carries a change.
func countEnvsWithChanges(data MultiEnvPlanCommentData) int {
	envsWithChanges := 0
	for _, env := range data.Environments {
		if plan, ok := data.Plans[env]; ok && plan != nil && hasChanges(plan.Changes) {
			envsWithChanges++
		}
	}
	return envsWithChanges
}

// multiEnvPlansRenderOnce reports whether the environments' plans are
// identical and so render as one section under a combined header.
func multiEnvPlansRenderOnce(data MultiEnvPlanCommentData) bool {
	return len(data.Errors) == 0 && countEnvsWithChanges(data) >= 2 && allPlansIdentical(data)
}

// countMultiEnvPlanDDLBlocks counts the DDL blocks a multi-environment plan
// comment renders — the shared section's blocks when the plans are identical,
// otherwise every rendered environment's own — so the comment's DDL budget is
// shared across exactly those blocks.
func countMultiEnvPlanDDLBlocks(data MultiEnvPlanCommentData) int {
	if multiEnvPlansRenderOnce(data) {
		return countCommentDDLBlocks(*data.Plans[data.Environments[0]])
	}
	count := 0
	for _, env := range data.Environments {
		if _, hasErr := data.Errors[env]; hasErr {
			continue
		}
		if plan, ok := data.Plans[env]; ok && plan != nil {
			count += countCommentDDLBlocks(*plan)
		}
	}
	return count
}

func renderMultiEnvPlanComment(data MultiEnvPlanCommentData, budget *ddlBlockBudget) string {
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

	envsWithChanges := countEnvsWithChanges(data)
	hasErrors := len(data.Errors) > 0

	// If no environments have changes and no errors, show simple message — unless
	// a deployment drifted, which must still surface even when the reviewed
	// primary plans are clean no-ops. The ignore_namespaces disclosure still
	// renders underneath it: an all-clean result is exactly where a reviewer
	// needs to see that a namespace was withheld rather than genuinely
	// unchanged.
	if envsWithChanges == 0 && !hasErrors && !AnyEnvHasDriftToShow(data) {
		sb.WriteString("✅ **No schema changes detected** for any environment.\n")
		if multiEnvHasIgnoredNamespaces(data) || multiEnvHasExemptTables(data) {
			sb.WriteString("\n")
			writeMultiEnvIgnoredNamespaces(&sb, data)
			writeMultiEnvExemptTables(&sb, data)
		}
		return appendAgentHint(sb.String(), data.AgentHint)
	}

	if multiEnvPlansRenderOnce(data) {
		// Identical plans: render once with combined header
		fmt.Fprintf(&sb, "### %s\n\n", capitalizeEnvNames(data.Environments))
		restore := budget.shareAcross(data.Environments)
		writeEnvironmentPlanSection(&sb, data.Plans[data.Environments[0]], budget)
		restore()
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

			writeEnvironmentPlanSection(&sb, plan, budget)
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
func writeEnvironmentPlanSection(sb *strings.Builder, plan *PlanCommentData, budget *ddlBlockBudget) {
	// Deployment drift is shown before the change list and before the no-changes
	// short-circuit: a non-primary deployment can drift even when this
	// environment's reviewed primary plan is a clean no-op.
	writeDeploymentDrift(sb, plan.DeploymentDrift, plan.Changes)
	targetPlans := RendersTargetPlans(plan.DeploymentDrift)
	summary := *plan
	if targetPlans {
		writeTargetPlans(sb, *plan, budget, true)
		summary.Changes = combinedTargetPlanChanges(*plan)
	}

	totalStatements, keyspaceUpdates := countChanges(plan.Changes)
	totalChanges := totalStatements + keyspaceUpdates
	summaryStatements, summaryKeyspaceUpdates := countChanges(summary.Changes)

	// The ignore_namespaces disclosure renders under each environment's
	// summary (writePlanSummary) or no-changes message, because entries can
	// resolve differently per environment. A reviewed target with nothing to
	// run while other targets still have work summarizes their plans instead.
	if totalChanges == 0 {
		if targetPlans {
			writePlanSummary(sb, summary, summaryStatements, summaryKeyspaceUpdates)
			if plan.MemberApplyRefusal != "" {
				sb.WriteString("\n")
				writeMemberApplyRefusal(sb, plan.MemberApplyRefusal)
			}
			return
		}
		sb.WriteString(noChangesDetected + "\n\n")
		writeIgnoredNamespaces(sb, plan.IgnoredNamespaces)
		writeExemptTables(sb, plan.ExemptTables)
		return
	}

	// Detailed changes. A single change is small enough to show inline; more
	// than one is collapsed so the DDL doesn't dominate the comment while the
	// unsafe/lint warnings and summary below stay visible at a glance.
	switch {
	case targetPlans:
	case totalChanges == 1:
		writeKeyspaceChanges(sb, *plan, budget)
	default:
		writeCollapsibleKeyspaceChanges(sb, *plan, totalStatements, budget)
	}

	// Blocked changes — statements the engine refuses; the apply will fail on
	// them, so each environment's section discloses its own.
	if len(plan.BlockedChanges) > 0 && !targetPlansDiscloseBlocked(*plan) {
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
		writeDirectChanges(sb, plan.DirectChanges, plan.DatabaseType, plan.IsMySQL, plan.DeferCutover)
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
		writeUnsafeWarning(sb, plan.UnsafeChanges, plan.DatabaseType, plan.IsMySQL)
	}

	// Lint violations
	if len(plan.LintViolations) > 0 {
		writeLintViolations(sb, plan.LintViolations)
	}

	// Errors
	if len(plan.Errors) > 0 {
		writeErrors(sb, plan.Errors)
	}

	// Summary (after DDL, matching CLI layout). Target plans are summarized
	// together, as a sharded keyspace's shards are.
	writePlanSummary(sb, summary, summaryStatements, summaryKeyspaceUpdates)
}

// writeCollapsibleKeyspaceChanges renders a plan's changes — DDL, plus VSchema
// diffs for non-MySQL keyspaces — inside a collapsed <details> block. The
// summary line carries the statement count so reviewers can gauge the size of
// the change without expanding it.
func writeCollapsibleKeyspaceChanges(sb *strings.Builder, plan PlanCommentData, totalStatements int, budget *ddlBlockBudget) {
	summary := "Show changes"
	if totalStatements > 0 {
		summary = fmt.Sprintf("Show SQL (%d %s)", totalStatements, pluralize("statement", totalStatements))
	}
	fmt.Fprintf(sb, "<details>\n<summary>%s</summary>\n\n", summary)
	writeKeyspaceChanges(sb, plan, budget)
	sb.WriteString("</details>\n\n")
}

// writeMultiEnvFooter writes the footer with apply commands and error guidance.
func writeMultiEnvFooter(sb *strings.Builder, data MultiEnvPlanCommentData) {
	// Categorize environments
	var envsWithChanges []string
	var envsWithErrors []string
	// An environment whose apply would be refused says why in its own section,
	// so the footer neither offers its apply nor calls the PR done.
	envsRefused := 0
	for _, env := range data.Environments {
		if _, hasErr := data.Errors[env]; hasErr {
			envsWithErrors = append(envsWithErrors, env)
		} else if plan, ok := data.Plans[env]; ok && plan != nil {
			switch {
			case environmentHasWork(plan):
				envsWithChanges = append(envsWithChanges, env)
			case plan.MemberApplyRefusal != "":
				envsRefused++
			}
		}
	}

	command := func(baseCommand, environment string) string {
		return scopedCommand(baseCommand, environment, data.ScopedDatabase, data.Tenant)
	}

	// Apply instructions for environments with changes.
	switch {
	case len(envsWithChanges) >= 2:
		sb.WriteString("▶️ **To apply** these changes, start with the first environment:\n")
		fmt.Fprintf(sb, "```\n%s\n```\n", command("schemabot apply", envsWithChanges[0]))
		for i := 1; i < len(envsWithChanges); i++ {
			fmt.Fprintf(sb, "\nAfter verifying %s, apply to %s:\n", envsWithChanges[i-1], envsWithChanges[i])
			fmt.Fprintf(sb, "```\n%s\n```\n", command("schemabot apply", envsWithChanges[i]))
		}
	case len(envsWithChanges) == 1:
		sb.WriteString("▶️ **To apply** these changes, comment:\n")
		fmt.Fprintf(sb, "```\n%s\n```\n", command("schemabot apply", envsWithChanges[0]))
	case len(envsWithErrors) == 0 && envsRefused == 0:
		sb.WriteString("No changes to apply.\n")
	}

	// Error guidance for failed environments
	if len(envsWithErrors) > 0 {
		sb.WriteString("\n")
		for _, env := range envsWithErrors {
			fmt.Fprintf(sb, glyph.Attention+" **%s** failed to plan. Resolve the error above and re-run:\n", capitalizeFirst(env))
			fmt.Fprintf(sb, "```\n%s\n```\n", command("schemabot plan", env))
		}
	}
}

func tenantCommand(baseCommand, environment, tenant string) string {
	return scopedCommand(baseCommand, environment, "", tenant)
}

// scopedCommand renders a pasteable command for one environment, carrying the
// database the operator named with -d and the deployment's tenant. Flags are
// ordered as an operator would type them — the target first, then the
// deployment qualifier — so a command lifted from one comment reads the same as
// one lifted from another.
func scopedCommand(baseCommand, environment, database, tenant string) string {
	command := appendDatabaseFlag(fmt.Sprintf("%s -e %s", baseCommand, environment), database)
	return appendTenantFlag(command, tenant)
}

// ApplyCommandOptions are the flags an apply or apply-confirm command carries
// beyond its target. A pasteable hint for either command has to repeat them,
// because the command reads its options from the comment that carries it and
// nothing else: a hint that drops --defer-cutover runs the cutover the operator
// chose to defer, and one that drops --allow-unsafe is blocked again.
type ApplyCommandOptions struct {
	Tenant       string
	AllowUnsafe  bool
	DeferCutover bool
	SkipRevert   bool
}

// scopedApplyCommand renders a pasteable apply or apply-confirm command for one
// environment: the target first (-e, then -d), the deployment qualifier, then
// the option flags in the order the locked plan comment lists them.
func scopedApplyCommand(baseCommand, environment, database string, opts ApplyCommandOptions) string {
	command := scopedCommand(baseCommand, environment, database, opts.Tenant)
	if opts.AllowUnsafe {
		command += " --allow-unsafe"
	}
	if opts.DeferCutover {
		command += " --defer-cutover"
	}
	if opts.SkipRevert {
		command += " --skip-revert"
	}
	return command
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

// appendDatabaseFlag scopes a copy-paste command to the database the operator
// named with -d, so the next command in a repository with several databases
// does not have to name it again. Empty leaves the command unchanged: an
// unscoped command is answered with an unscoped one, because the database this
// comment resolved to is SchemaBot's answer to the ambiguity rather than a
// choice the operator made, and a copy-paste line is not where to put words in
// their mouth.
func appendDatabaseFlag(command, database string) string {
	if database == "" {
		return command
	}
	return fmt.Sprintf("%s -d %s", command, database)
}

// ConfirmationRefusalData describes an apply-confirm the pending confirmation
// cannot vouch for. RequestedEnvironment and Options come from the rejected
// command, so the recovery commands repeat the database scope, tenant, and
// option flags the operator already chose. PlanEnvironment is the environment
// the pending confirmation was planned for; it is empty when no plan could be
// loaded.
type ConfirmationRefusalData struct {
	RequestedBy          string
	Database             string
	PlanEnvironment      string
	RequestedEnvironment string
	Options              ApplyCommandOptions
}

// RenderConfirmationPlanForOtherEnvironment refuses an apply-confirm whose -e
// names a different environment than the pending confirmation was planned for.
// The confirm command it offers keeps the pending confirmation. The apply
// command it offers for the requested environment does not: it answers to the
// environment ordering gate like any apply, and once through it releases this
// pull request's lock, so the pinned plan is gone and the requested
// environment is planned and applied in one step, pausing for apply-confirm
// only when the new plan needs one. The comment states both consequences so
// an operator who reads only the comment knows what each command costs. It is
// a comment of its own rather than a generic error, so no length clamp can cut
// the commands or the consequences however long the database name is.
func RenderConfirmationPlanForOtherEnvironment(data ConfirmationRefusalData) string {
	var sb strings.Builder

	writeConfirmationRefusalHeader(&sb, data)
	fmt.Fprintf(&sb, "The pending confirmation is for `%s`, not `%s`; nothing was applied.\n\n", data.PlanEnvironment, data.RequestedEnvironment)
	fmt.Fprintf(&sb, "To confirm the `%s` plan:\n\n", data.PlanEnvironment)
	writeCommandBlock(&sb, scopedApplyCommand("schemabot apply-confirm", data.PlanEnvironment, data.Database, data.Options))
	fmt.Fprintf(&sb, "\nTo apply `%s` instead, dropping the pending `%s` confirmation and planning and applying `%s` in one step, subject to the environment ordering gate and pausing for `apply-confirm` only if its plan needs it:\n\n",
		data.RequestedEnvironment, data.PlanEnvironment, data.RequestedEnvironment)
	writeCommandBlock(&sb, scopedApplyCommand("schemabot apply", data.RequestedEnvironment, data.Database, data.Options))
	writeConfirmationRefusalFooter(&sb, data)

	return offerSupportChannel(sb.String())
}

// RenderConfirmationPlanUnavailable refuses an apply-confirm whose pending
// confirmation pins no plan SchemaBot can load, so nothing attests which
// environment the operator reviewed. The apply command it offers for the
// requested environment answers to the environment ordering gate like any
// apply; once through, it releases this pull request's lock, replacing the
// unloadable pinned plan with a fresh one that is applied in the same step,
// pausing for apply-confirm only when the new plan needs one. Like the
// other-environment refusal, it is a comment of its own, so no length clamp
// can cut it.
func RenderConfirmationPlanUnavailable(data ConfirmationRefusalData) string {
	var sb strings.Builder

	writeConfirmationRefusalHeader(&sb, data)
	sb.WriteString("The pending confirmation is not backed by a plan SchemaBot can load, so it could not verify which environment was reviewed; nothing was applied.\n\n")
	fmt.Fprintf(&sb, "To replace that confirmation with a fresh plan and apply it in one step, subject to the environment ordering gate and pausing for `apply-confirm` only if its plan needs it:\n\n")
	writeCommandBlock(&sb, scopedApplyCommand("schemabot apply", data.RequestedEnvironment, data.Database, data.Options))
	writeConfirmationRefusalFooter(&sb, data)

	return offerSupportChannel(sb.String())
}

func writeConfirmationRefusalHeader(sb *strings.Builder, data ConfirmationRefusalData) {
	writeEnvironmentTitle(sb, glyph.Refused+" Apply-confirm Refused", data.RequestedEnvironment)
	if data.Database != "" {
		writeDBLine(sb, data.Database)
		sb.WriteString("\n")
	}
}

func writeConfirmationRefusalFooter(sb *strings.Builder, data ConfirmationRefusalData) {
	if data.RequestedBy != "" {
		fmt.Fprintf(sb, "\n_Requested by @%s_\n", data.RequestedBy)
	}
}

// writeCommandBlock writes one pasteable command in its own fenced block.
func writeCommandBlock(sb *strings.Builder, command string) {
	fmt.Fprintf(sb, "```\n%s\n```\n", command)
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
// could not be verified, or a rollout still converging. A clean uniform rollup
// is not "drift to show" — with no changes anywhere the simple no-changes
// message is clearer.
//
// A converging rollout passes its contract, so it is clean and says nothing here
// on that count. It is still the one case where no environment planning changes
// does not mean the fleet holds this schema: the reviewed target is at the
// desired schema and another target is not. Callers consult this only when
// nothing else would post a comment, so leaving it out is what decides whether
// the reviewer is told at all.
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
		if changingTargetCount(d) > 0 {
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
// the comment operators apply from. The sections are compared whole, never as
// they would be cut to fit the comment: cut to the same length, two plans that
// agree up to the cut and differ after it would read as one.
func plansIdentical(a, b *PlanCommentData) bool {
	var renderedA, renderedB strings.Builder
	writeEnvironmentPlanSection(&renderedA, a, newUnboundedDDLBudget(countCommentDDLBlocks(*a)))
	writeEnvironmentPlanSection(&renderedB, b, newUnboundedDDLBudget(countCommentDDLBlocks(*b)))
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

// environmentHasWork reports whether an environment's plan section offers an
// apply: its reviewed plan has changes, or its reviewed target is already at the
// desired schema while the section renders other targets' plans that do and a
// PR apply can run them. A section whose apply would be refused says why in the
// section itself.
func environmentHasWork(plan *PlanCommentData) bool {
	if hasChanges(plan.Changes) {
		return true
	}
	return RendersTargetPlans(plan.DeploymentDrift) && plan.MemberApplyRefusal == ""
}

// hasChanges returns true if there are any schema changes.
func hasChanges(changes []KeyspaceChangeData) bool {
	for _, ks := range changes {
		if len(ks.Statements) > 0 || ks.VSchemaChanged || ks.Finalize {
			return true
		}
	}
	return false
}
