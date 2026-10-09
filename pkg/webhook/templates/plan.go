package templates

import (
	"fmt"
	"html"
	"log/slog"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/caller"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/presentation"
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
	// Targets names the rollout targets that raised this finding when only
	// some of those it covers did: some of a target plan group's targets, or
	// some of the targets with work in the comment's merged lint section.
	// Empty when all of them raised it, or for a single target's plan.
	Targets []string
	// TotalTargets is how many targets the finding's scope holds, so a subset
	// too wide to name reads as coverage ("3 of 12 targets"). Zero when
	// Targets is empty.
	TotalTargets int
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
	// Targets names the rollout targets that carry this change, for a target
	// plan group in which only some targets do, or for a refusal that lists
	// other targets' changes beside the primary plan's. Empty when the change
	// is the primary plan's, or every target in its group carries it.
	Targets []string
	// TotalTargets is how many targets the group holds, so a subset too wide
	// to name reads as coverage ("3 of 12 targets"). Zero when Targets names
	// the whole list.
	TotalTargets int
	// VSchemaNamespace names the namespace whose VSchema this change alters.
	// Empty for a change to a table.
	VSchemaNamespace string
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
	// Targets names the rollout targets that run this change directly, for a
	// target plan group in which only some targets do: the direct execution
	// policy judges each target's own table. Empty when every target in the
	// group runs it directly.
	Targets []string
	// TotalTargets is how many targets the group holds, so a subset too wide
	// to name reads as coverage ("3 of 12 targets").
	TotalTargets int
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

	// NoCutoverToDefer marks a paused apply where no target it runs has a
	// cutover to defer, so the apply-confirm command the comment suggests
	// leaves out --defer-cutover, which apply-confirm rejects on it. Only a
	// caller that read every target's plan can say so; the primary plan alone
	// cannot on a rollout. The zero value keeps the flag, the safe side: a kept
	// flag costs a refusal, while a dropped one runs a cutover the operator
	// asked to hold.
	NoCutoverToDefer bool

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
	// deployment compares to the primary plan. Nil for a single-target
	// database (nothing to compare) or when drift was not evaluated.
	DeploymentDrift *DeploymentDriftData

	// MemberApplyRefusal says why a PR apply cannot run the plan of
	// MemberApplyRefusalTarget, one of the targets' plans this comment
	// renders, whether or not the primary target has work of its own, naming
	// only tables and namespaces. Such an apply is
	// refused whatever its flags, so the comment offers no apply command in its
	// place. Empty when the apply can run them, or when the comment renders the
	// primary plan alone.
	MemberApplyRefusal string
	// MemberApplyRefusalTarget names the target whose plan MemberApplyRefusal
	// is about.
	MemberApplyRefusalTarget string

	// summaryRollout is the rollout a summary of several targets' plans
	// covers. The summary then says which targets the diff rolls out to, so a
	// count combined across targets never reads as any one target's work. Nil
	// when the summary is one plan's.
	summaryRollout *DeploymentDriftData

	// namespaceLabelsInline renders each keyspace's label as a bold line
	// rather than a heading, for changes under a target group heading that
	// already sits at the namespace heading's level, so the label never
	// outranks the heading it sits under.
	namespaceLabelsInline bool
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
// primary plan.
type DeploymentDriftData struct {
	// Deployments is every configured rollout member in rollout order, primary
	// first.
	Deployments []DeploymentDriftEntry
	// Clean means every member passed its contract: under mirrored members, that
	// they all match the primary plan; under independent members, that they all
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
	// primary plan alone. Set only for a clean rollup, where every target was
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
	// Primary marks the group the primary target belongs to. Exactly one group
	// carries it. The primary is planned first and its plan is what the
	// comment's plan-wide sections describe, but it is no more reviewed than
	// any other group: an approval covers every target's plan the comment
	// shows, so the primary's group is ordered like any other.
	Primary bool
	// Changes is one member's plan, in the same shape the comment renders the
	// primary plan itself. Empty for a group whose members are already at the
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
	// UnsafeChanges are the group's changes that need `--allow-unsafe`, each
	// naming the targets that carry it when that is not all of them. The
	// primary group discloses the primary plan's own, so beside them it adds
	// only those its other targets carry without the primary target
	// (unsafeBeyondPrimaryPlan).
	UnsafeChanges []UnsafeChangeData
	// PlanID is the identifier of the stored plan the group's first member
	// would run, so DDL cut to fit the comment names the command that prints
	// the group's plan in full. Unused for the primary's group, which runs the
	// primary's plan and points at PlanCommentData.PlanID. Empty when the
	// member's plan was not stored.
	PlanID string
	// DirectChanges are the group's changes the direct execution policy routes
	// to native DDL, each naming the targets that run it that way when that is
	// not all of them.
	DirectChanges []DirectChangeData
	// LintViolations are the advisory lint findings the group's members' own
	// plans raised, each once however many members raise it. Lint reads each
	// target's live schema, so these are the group's, not the primary's. The
	// comment merges every group's into its one lint section (targetLint).
	LintViolations []LintViolationData
}

// Empty reports that the group's members are already at the desired schema and
// would apply nothing. That is a plan in its own right, not a missing one, and
// naming it is the difference between a fleet that is converging and one the
// comment has quietly left out.
// A vschema rewrite carries no DDL and is still work, so a group is counted the
// same way the comment counts the primary plan: statements and vschema
// rewrites together.
func (g DeploymentPlanGroup) Empty() bool {
	statements, vschema := countChanges(g.Changes)
	return statements+vschema == 0
}

// DeploymentDriftEntry is one rollout member's classification against the
// primary plan.
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
// wrong and the specifics behind it. The footer says what the operator can do,
// so the cause does not repeat it. Entries may be empty where the cause has no
// per-table detail to give.
type PausedApplyCauseData struct {
	Heading string
	Entries []string
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
	// CollationChanges lists the existing columns this keyspace's changes move
	// onto another collation, rendered under the table sizes (see
	// writeCollationChangesSection).
	CollationChanges []CollationChangeData

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
	// even when the primary plan is a clean no-op.
	writeDeploymentDrift(&sb, data.DeploymentDrift, data.Changes)
	targetPlans := RendersTargetPlans(data.DeploymentDrift)
	summary := data
	if targetPlans {
		// With each target's plan under its own heading, a cause written after
		// them would read as part of the last target's section. It leads instead,
		// so the reader knows why the apply is waiting before reading the plans.
		if data.PausedApplyCause != nil {
			writePausedApplyCause(&sb, data.PausedApplyCause)
		}
		writeTargetPlans(&sb, data, budget, false)
		summary.Changes = combinedTargetPlanChanges(data)
		summary.summaryRollout = data.DeploymentDrift
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
	// A primary target with nothing to run while other targets still have work
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

	// Sizes of the tables the DDL above copies, rebuilds, or scans.
	writeTableSizesSection(&sb, summary)

	// How the DDL above changes the way existing columns sort and compare, on
	// the primary target.
	writeCollationChangesSection(&sb, data)

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
	// When the unsafe warning below lists every attributed table, the
	// attribution rides on those findings instead of a section of its own, so
	// each change is explained once.
	unsafeShown := data.HasUnsafeChanges && len(data.UnsafeChanges) > 0
	attributionNotes, attributionFolded := unsafeAttributionNotes(data, unsafeShown && attributionStillActionable(data))
	if len(data.AttributedChanges) > 0 && attributionStillActionable(data) && !attributionFolded {
		writeAttributedChanges(&sb, data.AttributedChanges)
	}

	// Direct-execution changes — statements the policy routes to native DDL.
	// The policy approves them, so the plan only discloses how they run; the
	// locked apply comment repeats it so the apply shows what it is running.
	if len(data.DirectChanges) > 0 && !targetPlansDiscloseDirect(data) {
		writeDirectChanges(&sb, data.DirectChanges, data.DatabaseType, data.IsMySQL, data.directNotesDeferCutover())
	}

	// Copies already on the target. Shown on the locked apply comment too:
	// discarding an unfinished copy destroys hours of work already done, so the
	// disclosure must sit on the comment the confirmation acts on. The copy is
	// read from the target at plan time, so it can appear on the apply comment
	// without having been on the plan comment that preceded it. Target plans
	// disclose them under the primary target's group, the target they were
	// read from.
	if !targetPlans {
		writeExistingCopies(&sb, data)
	}

	// Why the apply is waiting, when no disclosure above says so. It sits here
	// rather than in the footer so every warning on the comment is in one
	// region: the reader meets them in one pass, and the footer stays the same
	// sentence whatever paused the apply.
	if data.PausedApplyCause != nil && !targetPlans {
		writePausedApplyCause(&sb, data.PausedApplyCause)
	}

	// Unsafe changes warning — shown on every comment, because the plan may
	// have changed since the operator opted in with --allow-unsafe: the plan
	// comment lists them for review, a paused comment lists what its
	// apply-confirm consents to, and the comment of an apply already running is
	// the record of what it destroys. The running comment drops the guidance on
	// how to make a drop safe, which is out of reach once the apply runs.
	// Target plans disclose them under the primary target's group, whose plan
	// they are from.
	if unsafeShown && !targetPlans {
		writeUnsafeWarning(&sb, data.UnsafeChanges, attributionNotes, data.DatabaseType, data.IsMySQL, !data.applyingWithoutConfirmation())
	}

	// Lint violations — shown on the plan comment for review, omitted on the
	// locked apply comment where they are noise (the operator already reviewed
	// them at plan time).
	if !data.IsLocked {
		writePlanWideLint(&sb, data.LintViolations, data.DeploymentDrift, targetPlans)
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
			DeferCutover: data.DeferCutover && !data.NoCutoverToDefer,
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
		writeMemberApplyRefusal(&sb, data.MemberApplyRefusalTarget, data.MemberApplyRefusal)
	case data.applyFailsOnRefusedChange():
		writeRefusedChangeReplan(&sb, "this plan", scopedCommand("schemabot plan", data.Environment, data.ScopedDatabase, data.Tenant))
	default:
		applyCmd := appendDatabaseFlag(fmt.Sprintf("schemabot apply -e %s", data.Environment), data.ScopedDatabase)
		if data.Tenant != "" {
			applyCmd += fmt.Sprintf(" --tenant %s", data.Tenant)
		}
		writeApplyInstruction(&sb, applyCmd, data)
	}

	return appendAgentHint(sb.String(), data.AgentHint)
}

// writeMemberApplyRefusal writes, in place of the apply instruction, why a PR
// apply cannot run every target's plan the comment renders. Offering the
// command there would coach an apply that is refused whatever its flags.
func writeMemberApplyRefusal(sb *strings.Builder, target, reason string) {
	reason = escapeInlineMarkdown(strings.Join(strings.Fields(reason), " "))
	fmt.Fprintf(sb, glyph.Attention+" **This PR cannot apply every target's plan**: target %s: %s.\n\n", inlineCode(target), reason)
	sb.WriteString("An apply runs every target or none, so nothing runs until that plan can. The schema check keeps blocking merge until every target has the change.\n")
}

// applyFailsOnRefusedChange reports whether the plan carries a change the
// engine refuses. Its apply fails on that change whatever its flags, so the
// comment offers a re-plan instead of an apply it knows will fail. A refused
// change on another target is a member apply refusal, which the footer states
// on its own.
func (d PlanCommentData) applyFailsOnRefusedChange() bool {
	return len(d.BlockedChanges) > 0
}

// writeRefusedChangeReplan writes, in place of the apply instruction, that the
// named plan's apply fails on a refused change, and the command that re-plans
// once it is fixed. The change itself is listed under Cannot apply above.
func writeRefusedChangeReplan(sb *strings.Builder, plan, planCommand string) {
	fmt.Fprintf(sb, "The engine refuses a change in %s (see **Cannot apply** above), so its apply fails whatever its flags. After fixing it, re-plan:\n", plan)
	fmt.Fprintf(sb, "```\n%s\n```\n", planCommand)
}

// writeApplyInstruction writes the ▶️ apply instruction with the given command.
func writeApplyInstruction(sb *strings.Builder, command string, data PlanCommentData) {
	if consent, ok := planUnsafeConsent(data); ok {
		fmt.Fprintf(sb, "▶️ **To apply**, %s:\n", consent.instruction())
	} else {
		sb.WriteString("▶️ **To apply**, comment:\n")
	}
	fmt.Fprintf(sb, "```\n%s\n```\n", command)
}

// unsafeConsent is what an apply of one plan would confirm with
// --allow-unsafe. The apply instruction states it in the sentence that leads
// into the command, so the reader meets the requirement before copying the
// command rather than after the gate rejects it, and without scrolling back up
// to the sections that explain it. The flag is deliberately left out of the
// pasteable command: consenting to destroy data takes typing it, which a
// copy-paste of the plan's own command never does.
type unsafeConsent struct {
	findings int
	tables   string
	// primaryOnly is set when the count covers the primary target alone: the
	// flag also consents to whatever unsafe changes the other targets carry,
	// which the comment has no per-target plan to count from.
	primaryOnly bool
}

// planUnsafeConsent reports what the plan's apply would confirm with
// --allow-unsafe, and false when the flag would not make the apply run: the
// apply needs no consent, or the plan carries a change the engine refuses,
// which fails the apply whatever its flags.
func planUnsafeConsent(data PlanCommentData) (unsafeConsent, bool) {
	if data.AllowUnsafe || data.applyFailsOnRefusedChange() {
		return unsafeConsent{}, false
	}
	// The flag consents to every target's disclosed unsafe changes, not only
	// the reviewed plan's, so the instruction counts and names them all.
	var changes []UnsafeChangeData
	if data.HasUnsafeChanges {
		changes = append(changes, data.UnsafeChanges...)
	}
	changes = append(changes, TargetPlanUnsafeChanges(data.DeploymentDrift)...)
	if len(changes) == 0 {
		return unsafeConsent{}, false
	}
	return unsafeConsent{
		findings:    countUnsafeFindings(changes),
		tables:      unsafeChangeTables(changes),
		primaryOnly: !unsafeCountCoversEveryTarget(data.DeploymentDrift),
	}, true
}

// unsafeCountCoversEveryTarget reports whether the unsafe changes the comment
// counts are every one --allow-unsafe would consent to. A single target, or a
// clean rollup, has them all: mirrored targets run the primary plan, and
// independent targets' own plans are counted when they render. A rollup that
// is blocked or could not be computed carries no plan per target, so the
// count is the primary target's alone. Such a rollup also blocks the apply,
// but the apply plans the rollout again, and it can be clean by then.
func unsafeCountCoversEveryTarget(drift *DeploymentDriftData) bool {
	return drift == nil || (drift.Computed && drift.Clean)
}

// instruction is the clause that tells the reader how to apply, lower-cased
// to continue a sentence. Where a change came from is the unsafe warning's to
// say, on the finding itself, so the instruction stays one short clause.
func (c unsafeConsent) instruction() string {
	noun := "unsafe change"
	if c.findings > 1 {
		noun = "unsafe changes"
	}
	if c.primaryOnly {
		return fmt.Sprintf("add `--allow-unsafe` to confirm %d %s on %s, and any the other targets carry", c.findings, noun, c.tables)
	}
	return fmt.Sprintf("add `--allow-unsafe` to confirm %d %s on %s", c.findings, noun, c.tables)
}

// unsafeConsentTablesShown caps how many names the consent sentence lists.
// The unsafe findings above list every one, so the rest are only counted.
const unsafeConsentTablesShown = 2

// unsafeChangeTables names what the unsafe changes touch, each once in
// first-appearance order, comma-separated: a table by its code span and a
// VSchema by its namespace. The names match the ones the unsafe findings list
// above uses, so the reader can match one to the other. Which targets and
// shards each change runs on is the findings list's to say, so the consent
// sentence stays one short clause however wide the rollout. Past
// unsafeConsentTablesShown the rest are counted, unless naming the one left
// over is as short as counting it.
func unsafeChangeTables(changes []UnsafeChangeData) string {
	seen := make(map[string]bool, len(changes))
	var names []string
	for _, c := range changes {
		name := unsafeConsentName(c)
		if seen[name] {
			continue
		}
		seen[name] = true
		names = append(names, name)
	}
	if len(names) > unsafeConsentTablesShown+1 {
		hidden := len(names) - unsafeConsentTablesShown
		names = append(names[:unsafeConsentTablesShown:unsafeConsentTablesShown], fmt.Sprintf("%d more", hidden))
	}
	return joinWithAnd(names)
}

func unsafeConsentName(c UnsafeChangeData) string {
	if c.VSchemaNamespace != "" {
		return inlineCode(c.VSchemaNamespace) + " VSchema"
	}
	return inlineCode(c.Table)
}

// attributionStillActionable reports whether the attributed-changes
// disclosure still informs a choice this comment's reader holds. The plan
// comment offers the apply command, and a locked comment downgraded to manual
// confirmation pauses for apply-confirm — both readers can still merge the
// owning pull request and re-plan instead of applying. Once the locked
// comment is applying automatically, that re-plan alternative is gone and the
// operator consented to the destruction through --allow-unsafe, so the
// disclosure is omitted. That holds for every attributed table because the
// unsafe gate reads the same set the attribution does, a divergent shard's
// drops included (PlanResponse.UnsafeChanges walks the shard rows), so no
// attributed destruction reaches an automatic apply without consent having
// been solicited for it.
func attributionStillActionable(data PlanCommentData) bool {
	return !data.IsLocked || data.PendingManualConfirmation
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

	text := planSummaryText(data.Changes, data.DatabaseType, data.IsMySQL, totalStatements)
	if data.summaryRollout != nil {
		text += " · " + rolloutScope(data.summaryRollout)
	}
	fmt.Fprintf(sb, "📋 **Plan**: %s\n\n", text)

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

// groupNoChanges is written under a shard group with nothing to apply. Such a
// group only renders beside a group that still has work, so it
// carries no ✅ and no emphasis: the rollout is not done, and the groups that
// have work are what the reader needs to find.
const groupNoChanges = "No schema changes detected"

// changingTargetCount counts the rollout's members whose own plan runs work.
//
// It answers only for a grouped rollup, which is a clean independent one: a
// mirrored rollup that passed has already established that every member matches
// the primary plan, so an empty primary plan is empty everywhere, and a rollup
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

// rolloutScope states which targets the plan rolls out to: "rolling out to
// all 4 targets" (or "both targets") when every target has work, otherwise the
// targets that have it, by name when there are few and when no plan heading
// already names them: "rolling out to targets `a`, `c`", or "rolling out to
// 12 targets". Targets already at the desired schema run nothing, so the
// comment neither names nor counts them.
// The summary is the union of every target's changes, so each target runs some
// of that work, not necessarily all of it.
func rolloutScope(drift *DeploymentDriftData) string {
	changing, total := changingTargetCount(drift), len(drift.Deployments)
	if changing == total && total == 2 {
		return "rolling out to both " + targetNoun.Plural
	}
	if changing == total {
		return fmt.Sprintf("rolling out to all %d %s", total, targetNoun.Plural)
	}
	if targetPlansHeaded(drift) {
		return "rolling out to " + presentation.CoveragePhrase(targetNoun, changing, 0)
	}
	return "rolling out to " + planGroupList(targetNoun, rolloutTargetsWithWork(drift), 0)
}

// rolloutTargetsWithWork is the rollout's targets that have work, in rollout
// order, as every other list of targets reads.
func rolloutTargetsWithWork(drift *DeploymentDriftData) []string {
	var working []string
	for _, g := range drift.Plans {
		if !g.Empty() {
			working = append(working, g.Members...)
		}
	}
	order := make(map[string]int)
	for i, name := range driftMemberNames(drift.Deployments) {
		order[name] = i
	}
	slices.SortStableFunc(working, func(a, b string) int { return order[a] - order[b] })
	return working
}

// targetPlansHeaded reports whether the rollout's plans render under headings
// naming their targets, which they do when targets with work run more than one
// plan.
func targetPlansHeaded(drift *DeploymentDriftData) bool {
	return len(workingTargetGroups(drift)) > 1
}

// workingTargetGroups is the rollout's groups of targets that have work, each
// running a plan of its own.
func workingTargetGroups(drift *DeploymentDriftData) []DeploymentPlanGroup {
	var working []DeploymentPlanGroup
	for _, g := range drift.Plans {
		if !g.Empty() {
			working = append(working, g)
		}
	}
	return working
}

// writeNoChangesDetected closes a comment with nothing to apply. It is never
// reached while another target still has work: an empty primary plan is a
// no-op only for the primary target, so those comments render the other
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
			writeNamespaceLabel(sb, data, label, inlineCode(ks.Keyspace))
		}

		if hasVSchemaChanges {
			writeNamespaceLabel(sb, data, "VSchema", "")
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

// memberSizes is each target's estimate for the table, in rollout order, in
// the shape the shared size formatter takes.
func (e tableSizeEntry) memberSizes() []presentation.MemberTableSize {
	sizes := make([]presentation.MemberTableSize, len(e.perTarget))
	for i, ts := range e.perTarget {
		sizes[i] = presentation.MemberTableSize{Member: ts.Target, EstimatedBytes: ts.Size.EstimatedBytes}
	}
	return sizes
}

// rankBytes is the figure the section ranks the table by: its estimate, or on
// a multi-target plan the total across the targets that report one. Nil when
// no estimate is known.
func (e tableSizeEntry) rankBytes() *int64 {
	if !e.multiTarget() {
		return e.size.EstimatedBytes
	}
	total, sized := presentation.SumTableSizeBytes(e.memberSizes())
	if sized == 0 {
		return nil
	}
	return &total
}

// hasAnyEstimate reports whether the line carries a byte estimate for at
// least one target.
func (e tableSizeEntry) hasAnyEstimate() bool {
	if !e.multiTarget() {
		return hasSizeEstimate(e.size)
	}
	_, sized := presentation.SumTableSizeBytes(e.memberSizes())
	return sized > 0
}

// writeTableSizesSection renders the plan's table-size info section: one line
// per table the plan will copy, rebuild, or scan (the comment builder
// attaches sizes only to statements whose cost scales with table size),
// across every keyspace, placed directly under the DDL. A plan of only
// metadata-only statements renders no section at all, and neither does a plan
// where no table has an estimate: an engine that does not estimate sizes
// would otherwise show "unavailable" on every line, which reads as a failed
// probe when none ran. Table names carry
// their keyspace when the plan spans more than one keyspace with sizes, so a
// shared table name stays unambiguous.
//
// Up to presentation.TableSizesInlineLimit tables are listed in plan order;
// past that the section collapses and lists the largest tables first. A rollout whose
// targets were all planned shows each table across every target that changes
// it. A rollout without per-target sizes shows the primary plan's sizes, and
// the heading names the primary target so they are not read as the whole
// rollout's.
//
// A locked comment that applies automatically renders no section: the
// operator already saw the sizes on the plan they chose to apply. A locked
// comment paused for apply-confirm keeps it, because its re-plan can carry
// statements the primary plan did not, and the operator should see the size
// of what they are confirming. The rule lives here so every renderer that
// shows a plan applies it the same way.
func writeTableSizesSection(sb *strings.Builder, data PlanCommentData) {
	if data.applyingWithoutConfirmation() {
		return
	}
	entries := tableSizeEntries(data.Changes)
	var drift *DeploymentDriftData
	switch {
	case data.DeploymentDrift != nil && len(data.DeploymentDrift.TableSizes) > 0:
		entries = targetTableSizeEntries(data.DeploymentDrift.TableSizes)
	case data.DeploymentDrift != nil:
		drift = data.DeploymentDrift
	}
	if !slices.ContainsFunc(entries, tableSizeEntry.hasAnyEstimate) {
		return
	}
	if len(entries) <= presentation.TableSizesInlineLimit {
		sb.WriteString("📊 **Table sizes**")
		if scope := primaryTargetSizeScope(drift, inlineCode); scope != "" {
			fmt.Fprintf(sb, " (%s)", scope)
		}
		sb.WriteString(":\n")
		writeTableSizeLines(sb, entries)
		sb.WriteString("\n")
		return
	}
	writeCollapsedTableSizes(sb, entries, primaryTargetSizeScope(drift, summaryCode))
}

// writeCollapsedTableSizes renders the size section for a plan with more
// tables than presentation.TableSizesInlineLimit as one collapsed block, its
// tables largest first. Past presentation.TableSizesShown the smallest tables
// are counted in a closing line rather than listed, and those of them with no
// estimate counted apart, since collapsed lines still count toward GitHub's
// comment size limit and a plan that indexes thousands of tables would
// otherwise spend on sizes the room its DDL needs.
func writeCollapsedTableSizes(sb *strings.Builder, entries []tableSizeEntry, scope string) {
	listed, unlisted, unlistedUnsized := presentation.ListTableSizes(entries, tableSizeEntry.rankBytes)
	sb.WriteString("<details>\n<summary>📊 <b>Table sizes</b>")
	if scope != "" {
		fmt.Fprintf(sb, " (%s)", scope)
	}
	sb.WriteString("</summary>\n\n")
	writeTableSizeLines(sb, listed)
	if unlisted > 0 {
		fmt.Fprintf(sb, "- %s\n", presentation.UnlistedTables(unlisted, unlistedUnsized))
	}
	sb.WriteString("\n</details>\n\n")
}

// summaryCode renders a name inside a <summary>, which GitHub reads as HTML
// rather than markdown: the name is escaped and set in a <code> tag, and
// flattened first so it cannot carry a line break out of the tag.
func summaryCode(name string) string {
	return "<code>" + html.EscapeString(flattenIdentifier(name)) + "</code>"
}

// primaryTargetSizeScope names the target whose sizes a rollout's section
// shows when the rollout carries no per-target sizes: the sizes come from the
// primary plan alone, and a reader would otherwise take them for every
// target's. The primary target is named, with code, when the rollup
// identifies it. A plan with no rollup, or a rollup of one member, has no
// other target, so its sizes need no scope. A rollup that could not be
// computed names no members, so it cannot say whether other targets exist and
// claims none.
func primaryTargetSizeScope(drift *DeploymentDriftData, code func(string) string) string {
	if drift == nil {
		return ""
	}
	if !drift.Computed || len(drift.Deployments) == 0 {
		return "one target only; targets could not be listed"
	}
	if len(drift.Deployments) == 1 {
		return ""
	}
	names := driftMemberNames(drift.Deployments)
	for i, d := range drift.Deployments {
		if d.Primary {
			return fmt.Sprintf("%s only; other targets not shown", code(names[i]))
		}
	}
	return "one target only; other targets not shown"
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

func writeTableSizeLines(sb *strings.Builder, entries []tableSizeEntry) {
	for _, e := range entries {
		fmt.Fprintf(sb, "- %s: %s\n", inlineCode(e.name), formatTableSizeEntry(e))
	}
}

// formatTableSizeEntry renders a section line's size clause. On a multi-target
// plan the clause is the shared rollout size clause, with each target in code.
func formatTableSizeEntry(e tableSizeEntry) string {
	if !e.multiTarget() {
		return formatTableSize(e.size)
	}
	return presentation.FormatMemberTableSize(presentation.TargetNoun, e.memberSizes(), inlineCode)
}

// hasSizeEstimate reports that the table's on-disk footprint is known. The
// section shows bytes alone: they track how long a copy or index build runs
// on every engine that reports a size.
func hasSizeEstimate(ts TableSizeData) bool {
	return ts.EstimatedBytes != nil
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

// A plan group's members render through the same group headings whatever
// they are called, so a rollout whose targets need different work reads the
// way a keyspace whose shards do.
var (
	shardNoun      = presentation.ShardNoun
	targetNoun     = presentation.TargetNoun
	deploymentNoun = presentation.DeploymentNoun
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

func planGroupList(noun presentation.Noun, members []string, total int) string {
	if len(members) > shardNamesInlineLimit {
		return presentation.CoveragePhrase(noun, len(members), total)
	}
	quoted := inlineCodeList(members)
	if len(quoted) == 1 {
		return noun.Singular + " " + quoted[0]
	}
	return noun.Plural + " " + strings.Join(quoted, ", ")
}

// shardCoveragePhrase states how much of a keyspace a shard group covers:
// "all 32 shards" when it covers every planned shard, "12 of 32 shards" for
// a subset, or a bare count when the keyspace total is unknown — a subset
// must never read like whole-keyspace coverage.
func shardCoveragePhrase(count, totalShards int) string {
	return presentation.CoveragePhrase(shardNoun, count, totalShards)
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

// writeNamespaceLabel labels one keyspace's changes, or its VSchema diff: a
// heading in a plan of its own, a bold line under a target group's heading.
// value is the keyspace the label names, empty for a bare label.
func writeNamespaceLabel(sb *strings.Builder, data PlanCommentData, label, value string) {
	switch {
	case data.namespaceLabelsInline && value != "":
		fmt.Fprintf(sb, "**%s**: %s\n\n", label, value)
	case data.namespaceLabelsInline:
		fmt.Fprintf(sb, "**%s**\n\n", label)
	case value != "":
		fmt.Fprintf(sb, "#### %s: %s\n", label, value)
	default:
		fmt.Fprintf(sb, "#### %s\n", label)
	}
}

// writeTargetGroupHeading heads the targets of a rollout that run one plan, at
// the given heading level: one level above the namespaces under it, so each
// target group reads as its own section. A lone target is named in the
// heading. A group is headed by how many targets it holds, and how many the
// rollout has when it holds only some, so the heading stays one short line
// however many targets the group holds; the names follow on their own line,
// collapsed when too many to read inline. The names stay whole, as an
// operator addresses each target.
func writeTargetGroupHeading(sb *strings.Builder, level string, members []string, total int) {
	if len(members) == 1 {
		fmt.Fprintf(sb, "%s Target %s\n\n", level, inlineCode(members[0]))
		return
	}
	count := presentation.CoveragePhrase(targetNoun, len(members), total)
	if len(members) == total {
		count = fmt.Sprintf("%d %s", total, targetNoun.Plural)
	}
	fmt.Fprintf(sb, "%s %s\n\n", level, count)
	names := strings.Join(inlineCodeList(members), ", ")
	if len(members) <= shardNamesInlineLimit {
		fmt.Fprintf(sb, "%s\n\n", names)
		return
	}
	fmt.Fprintf(sb, "<details>\n<summary>Target names</summary>\n\n%s\n\n</details>\n\n", names)
}

func writeGroupHeading(sb *strings.Builder, noun presentation.Noun, members []string, total int) {
	if len(members) <= shardNamesInlineLimit {
		fmt.Fprintf(sb, "**%s**\n\n", planGroupList(noun, members, total))
		return
	}
	fmt.Fprintf(sb, "<details>\n<summary><b>%s</b></summary>\n\n%s\n\n</details>\n\n",
		presentation.CoveragePhrase(noun, len(members), total), strings.Join(inlineCodeList(members), ", "))
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
		sb.WriteString(glyph.Attention + " **Deployment drift detected** — some deployments no longer match this plan, so the plan check is failing closed:\n\n")
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
	switch d.Class {
	case "match":
		return fmt.Sprintf("- %s ✅ matches this plan%s\n", name, blockedSuffix(d.Blocked))
	case "planned":
		return fmt.Sprintf("- %s ✅ planned against its own schema%s\n", name, blockedSuffix(d.Blocked))
	case "diverged":
		return fmt.Sprintf("- %s "+glyph.Attention+" diverged%s%s\n", name, blockedSuffix(d.Blocked), driftDetailSuffix(d.Detail))
	default:
		// An errored member means different things under the two contracts:
		// a mirrored member's diff could not be confirmed against the
		// primary plan, while an independent member has no plan at all.
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
		return fmt.Sprintf("%s ✅ planned against their own schemas", presentation.CoveragePhrase(targetNoun, count, len(drift.Deployments)))
	}
	return fmt.Sprintf("%s ✅ match this plan", presentation.CoveragePhrase(deploymentNoun, count, len(drift.Deployments)))
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
// Mirrored members run the primary plan, so the primary plan answers for all
// of them. Independent members answer only through their own grouped plans; a
// rollup that carries none has not shown that the other targets have nothing to
// run.
func rolloutAtThisSchema(drift *DeploymentDriftData, primary []KeyspaceChangeData) bool {
	if !drift.Clean || anyDeploymentBlocked(drift.Deployments) {
		return false
	}
	statements, vschema := countChanges(primary)
	if statements+vschema > 0 {
		return false
	}
	if drift.Independent {
		return len(drift.Plans) > 0 && changingTargetCount(drift) == 0
	}
	return true
}

// RendersTargetPlans reports whether the comment renders the rollout's plans one
// group of targets at a time instead of the primary plan alone: a clean
// rollout of independent targets in which some target still has work.
//
// Each such target applies its own plan, so the primary plan describes only
// the targets that share it. Rendering it alone would leave a reviewer to
// approve statements the comment never showed.
func RendersTargetPlans(drift *DeploymentDriftData) bool {
	return drift != nil && drift.Computed && drift.Clean && drift.Independent && changingTargetCount(drift) > 0
}

// targetPlanChanges is the plan a group of targets renders. The primary
// target's group renders the primary's plan itself, so what a reviewer reads
// for it is exactly what the plan-wide sections describe; every other group
// renders its own members' plan.
func targetPlanChanges(g DeploymentPlanGroup, data PlanCommentData) []KeyspaceChangeData {
	if g.Primary && hasChanges(data.Changes) {
		return data.Changes
	}
	return g.Changes
}

// targetPlanID is the stored plan a target group's DDL comes from, the one a
// reader who cannot see all of it is pointed at. The primary's group runs the
// primary's own plan and has no member plan of its own.
func targetPlanID(g DeploymentPlanGroup, data PlanCommentData) string {
	if g.Primary {
		return data.PlanID
	}
	return g.PlanID
}

// writeTargetPlans renders the rollout's plans. The operator is rolling one
// diff out everywhere, so targets are a count on the plan summary, not the
// structure of the comment: when every target with work runs the same plan,
// that plan renders once with no target heading. Only when targets run
// different plans does each plan get a heading naming the targets that run
// it, the way a sharded keyspace renders its shards. A target already at the
// desired schema gets no heading of its own; the plan summary counts it.
// collapse folds a plan with more than one change into a details block, as a
// multi-environment section does for its own plan.
func writeTargetPlans(sb *strings.Builder, data PlanCommentData, budget *ddlBlockBudget, collapse bool) {
	drift := data.DeploymentDrift
	headed := targetPlansHeaded(drift)
	// Targets with work lead, as changing shards do: they are what the apply
	// will run, and the targets already at the schema follow them. Among
	// groups with work the largest leads, since it is what most targets will
	// run; no group leads for holding the primary, as an approval covers
	// every group alike.
	plans := slices.Clone(drift.Plans)
	slices.SortStableFunc(plans, func(a, b DeploymentPlanGroup) int {
		if c := compareWorkFirst(a.Empty(), b.Empty()); c != 0 {
			return c
		}
		return len(b.Members) - len(a.Members)
	})
	// A target group sits one level above its namespaces. In a plan of its
	// own that is a section heading over namespace headings; inside an
	// environment's section it drops a level, and its namespaces become
	// bold labels so none outranks the target it sits under.
	level, namespaceLabelsInline := "###", false
	if collapse {
		level, namespaceLabelsInline = "####", true
	}
	// A plan with no target heading over it labels its namespaces as a
	// target's own plan does.
	if !headed {
		namespaceLabelsInline = data.namespaceLabelsInline
	}
	for _, g := range plans {
		if g.Empty() {
			if g.Primary {
				writePrimaryTargetDisclosures(sb, data, g)
			}
			continue
		}
		if headed {
			writeTargetGroupHeading(sb, level, g.Members, changingTargetCount(drift))
		}
		group := data
		group.namespaceLabelsInline = namespaceLabelsInline
		group.Changes = targetPlanChanges(g, data)
		group.PlanID = targetPlanID(g, data)
		statements, vschema := countChanges(group.Changes)
		// A cut block's marker names whose plan it points at. Under a heading
		// the heading names the group; a sole plan has none, so the marker
		// names the target itself.
		var restore func()
		if headed {
			restore = budget.forTargetGroup(g.Members)
		} else {
			restore = budget.forSoleTargetGroup(g.Members, 0)
		}
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
		// A direct change is disclosed the same way, naming the targets that
		// run its write-blocking DDL.
		if len(g.DirectChanges) > 0 {
			writeDirectChanges(sb, g.DirectChanges, data.DatabaseType, data.IsMySQL, data.directNotesDeferCutover())
		}
		// So is an unsafe change, which `--allow-unsafe` consents to on every
		// target. The primary's group discloses its own with the primary
		// plan's. The comment of an apply already running lists them too: a
		// target's plan can turn unsafe after the operator opted in, and that
		// comment is the only record of what the apply destroys there.
		if g.Primary {
			writePrimaryTargetDisclosures(sb, data, g)
		} else if unsafe := g.unsafeBeyondPrimaryPlan(); len(unsafe) > 0 {
			writeUnsafeWarning(sb, unsafe, nil, data.DatabaseType, data.IsMySQL, !data.applyingWithoutConfirmation())
		}
	}
}

// writePrimaryTargetDisclosures writes, under the primary target's group, the
// disclosures read from the primary target: the copies already on it and the
// unsafe changes in its plan, with any its group's other targets carry beyond
// them. Written after the last group instead, they would sit under whichever
// target's heading came last and read as that target's. Copies are read from
// the primary only, so in a group of several targets they name it. The
// findings carry the same attribution notes the plan-wide warning would, since
// the attribution section folds into them on the same condition.
func writePrimaryTargetDisclosures(sb *strings.Builder, data PlanCommentData, g DeploymentPlanGroup) {
	if hasExistingCopies(data) && !headedByItself(data.DeploymentDrift, g) {
		fmt.Fprintf(sb, "On target %s:\n\n", inlineCode(g.Members[0]))
	}
	writeExistingCopies(sb, data)
	var unsafe []UnsafeChangeData
	if data.HasUnsafeChanges {
		unsafe = append(unsafe, data.UnsafeChanges...)
	}
	unsafe = append(unsafe, g.unsafeBeyondPrimaryPlan()...)
	if len(unsafe) > 0 {
		notes, _ := unsafeAttributionNotes(data, data.HasUnsafeChanges && len(data.UnsafeChanges) > 0 && attributionStillActionable(data))
		writeUnsafeWarning(sb, unsafe, notes, data.DatabaseType, data.IsMySQL, !data.applyingWithoutConfirmation())
	}
}

// headedByItself reports whether a target group renders under a heading that
// names its one target, so a line about that target need not name it again.
func headedByItself(drift *DeploymentDriftData, g DeploymentPlanGroup) bool {
	return len(g.Members) == 1 && !g.Empty() && len(workingTargetGroups(drift)) > 1
}

func hasExistingCopies(data PlanCommentData) bool {
	return len(data.DiscardedCopies) > 0 || len(data.AdoptedCopies) > 0 || len(data.RunningCopies) > 0
}

// writeExistingCopies writes the copies already on the target: those applying
// would discard, resume, or join.
func writeExistingCopies(sb *strings.Builder, data PlanCommentData) {
	if len(data.DiscardedCopies) > 0 {
		writeDiscardedCopies(sb, data.DiscardedCopies, data.applyingWithoutConfirmation())
	}
	if len(data.AdoptedCopies) > 0 {
		writeAdoptedCopies(sb, data.AdoptedCopies, data.applyingWithoutConfirmation())
	}
	if len(data.RunningCopies) > 0 {
		writeRunningCopies(sb, data.RunningCopies, data.applyingWithoutConfirmation())
	}
}

// TargetPlanUnsafeChanges lists the unsafe changes the rendered target plans
// carry beyond the primary plan's own, each naming the targets that carry it,
// so the unsafe gate and its refusal cover every target an apply runs. Empty
// when the drift renders no target plans.
func TargetPlanUnsafeChanges(drift *DeploymentDriftData) []UnsafeChangeData {
	if !RendersTargetPlans(drift) {
		return nil
	}
	var out []UnsafeChangeData
	for _, g := range drift.Plans {
		for _, c := range g.unsafeBeyondPrimaryPlan() {
			if len(c.Targets) == 0 {
				c.Targets = slices.Clone(g.Members)
				c.TotalTargets = 0
			}
			out = append(out, c)
		}
	}
	return out
}

// unsafeBeyondPrimaryPlan lists the group's unsafe changes the primary plan
// does not already disclose. That is every one of a group other than the
// primary target's. The primary target's group shares its DDL, but each
// target's unsafe verdict is read from that target's own schema, so a sibling
// can find a statement unsafe that the primary target does not: those are the
// changes listed with targets that leave the primary target, its first
// member, out.
func (g DeploymentPlanGroup) unsafeBeyondPrimaryPlan() []UnsafeChangeData {
	if !g.Primary {
		return g.UnsafeChanges
	}
	if len(g.Members) == 0 {
		return nil
	}
	primary := g.Members[0]
	var out []UnsafeChangeData
	for _, c := range g.UnsafeChanges {
		// A change every member carries names no targets, and the primary
		// target is one of them.
		if len(c.Targets) == 0 || slices.Contains(c.Targets, primary) {
			continue
		}
		out = append(out, c)
	}
	return out
}

// targetPlansDiscloseDirect reports whether the rendered target plans carry
// the primary plan's direct changes under the primary target's own group, so
// the plan-wide section would only repeat them. The primary plan whose group
// carries none keeps the plan-wide section, so the comment never leaves a direct
// statement unsaid because the two sources disagree.
func targetPlansDiscloseDirect(data PlanCommentData) bool {
	if !RendersTargetPlans(data.DeploymentDrift) {
		return false
	}
	for _, g := range data.DeploymentDrift.Plans {
		if g.Primary {
			return len(g.DirectChanges) > 0
		}
	}
	return false
}

// targetPlansDiscloseBlocked reports whether the rendered target plans carry
// the plan's blocked changes under their own groups, so the plan-wide section
// would only repeat them. The primary plan with blocked changes that no group
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
// primary plan's, or every target plan's when the rollout renders them.
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
		if len(c.Targets) > 0 {
			table = fmt.Sprintf("%s on %s", table, planGroupList(targetNoun, c.Targets, c.TotalTargets))
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

// writeUnsafeWarning lists the unsafe findings. attributionNotes, keyed by
// table, carries the attribution for tables another pull request changed, so
// the reader learns where a change came from on the finding itself.
// dropGuidance adds how to make a destructive drop safe, which only a comment
// whose reader still decides the apply can act on.
func writeUnsafeWarning(sb *strings.Builder, changes []UnsafeChangeData, attributionNotes map[string]string, databaseType string, isMySQL, dropGuidance bool) {
	n := countUnsafeFindings(changes)
	fmt.Fprintf(sb, glyph.Attention+" **Issues**: %d unsafe %s detected\n", n, pluralize("change", n))
	item := 0
	for _, c := range changes {
		note := attributionNotes[c.Table]
		if createsItsTable(c) {
			note = ""
		}
		writeUnsafeChangeItem(sb, &item, unsafeChangeLabel(c), c.Reason, c.ChangeType, note)
	}
	if len(attributionNotes) > 0 {
		sb.WriteString("\nA plan diffs this PR's schema files against the live database, so a change another PR applied before merging shows up here as one to undo.\n")
	}
	sb.WriteString("\n")
	if dropGuidance {
		writeUnsafeDropGuidance(sb, changes, databaseType, isMySQL)
	}
}

// unsafeChangeLabel names an unsafe change's table, with the shards and the
// targets that carry it when that is not all of them.
func unsafeChangeLabel(c UnsafeChangeData) string {
	table := inlineCode(c.Table)
	if len(c.Shards) > 0 {
		table = fmt.Sprintf("%s (%s)", table, planShardList(c.Shards, c.TotalShards))
	}
	if len(c.Targets) > 0 {
		table = fmt.Sprintf("%s on %s", table, planGroupList(targetNoun, c.Targets, c.TotalTargets))
	}
	return table
}

// unsafeAttributionNotes returns, keyed by table, the note each attributed
// table's unsafe findings carry, and whether the attribution is folded into
// the unsafe warning. It folds only when that warning is shown and lists every
// attributed table with a finding that destroys something; otherwise the
// attribution keeps its own section, so no disclosure is lost. A finding on a
// change that creates its table never carries the note: such a change
// destroys nothing, so another target's change to the same table, which the
// attribution is about, would otherwise read as the creation's.
func unsafeAttributionNotes(data PlanCommentData, unsafeShown bool) (map[string]string, bool) {
	if !unsafeShown || len(data.AttributedChanges) == 0 {
		return nil, false
	}
	unsafeTables := make(map[string]bool, len(data.UnsafeChanges))
	for _, c := range data.UnsafeChanges {
		if createsItsTable(c) {
			continue
		}
		unsafeTables[c.Table] = true
	}
	notes := make(map[string]string, len(data.AttributedChanges))
	for _, a := range data.AttributedChanges {
		if !unsafeTables[a.Table] {
			return nil, false
		}
		if a.Unresolved {
			notes[a.Table] = "ownership could not be established"
			continue
		}
		notes[a.Table] = "changed by open PR " + pullRequestRef(data.Repository, a.Repository, a.PullRequest)
	}
	return notes, true
}

// createsItsTable reports whether an unsafe change creates its table, so the
// table is not on the target yet.
func createsItsTable(c UnsafeChangeData) bool {
	return ddl.OpToStatementType(c.ChangeType) == ddl.StatementCreateTable
}

// pullRequestRef links a pull request, by number alone when it is in the
// repository the comment is posted on and by repo#number otherwise.
func pullRequestRef(commentRepo, repo string, pr int) string {
	if commentRepo != "" && repo == commentRepo {
		return fmt.Sprintf("[#%d](%s)", pr, caller.PullRequestURL(repo, pr))
	}
	return caller.PullRequestMarkdownLink(repo, pr)
}

// writeUnsafeChangeItem writes one table's unsafe findings, one numbered line
// per finding, so the rendered list is exactly as long as the heading's count
// and operators can reference a finding by its number. n carries the running
// number across tables; a change with no parseable reason still gets a line,
// carrying the engine's change type when one is known so the finding explains
// itself.
func writeUnsafeChangeItem(sb *strings.Builder, n *int, table, reason, changeType, note string) {
	suffix := ""
	if note != "" {
		suffix = " (" + note + ")"
	}
	reasons := ui.LintReasons(reason)
	if len(reasons) == 0 {
		*n++
		if changeType != "" {
			fmt.Fprintf(sb, "%d. %s: %s%s\n", *n, table, changeType, suffix)
		} else {
			fmt.Fprintf(sb, "%d. %s%s\n", *n, table, suffix)
		}
		return
	}
	for _, r := range reasons {
		*n++
		fmt.Fprintf(sb, "%d. %s: %s%s\n", *n, table, ui.CodeQuoteIdentifiers(r), suffix)
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

	sb.WriteString("<details>\n<summary>Destructive drop guidance</summary>\n\n")
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
	sb.WriteString("</details>\n\n")
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
	if n == 0 {
		return
	}
	count := fmt.Sprintf("%d advisory %s", n, pluralize("finding", n))

	if n <= lintWarningsFoldThreshold {
		fmt.Fprintf(sb, "\U0001f4a1 **Lint Warnings**: %s\n", count)
		for _, w := range warnings {
			writeLintLine(sb, w.Table, w)
		}
		sb.WriteString("\n")
		return
	}

	// GitHub renders <summary> content as HTML, not markdown, so the folded
	// header bolds with <b> tags instead of asterisks.
	fmt.Fprintf(sb, "<details>\n<summary>\U0001f4a1 <b>Lint Warnings</b>: %s</summary>\n\n", count)
	for _, group := range groupLintWarningsByTable(warnings) {
		if group.table != "" {
			fmt.Fprintf(sb, "**%s**\n", inlineCode(group.table))
		}
		for _, w := range group.warnings {
			writeLintLine(sb, "", w)
		}
		sb.WriteString("\n")
	}
	sb.WriteString("</details>\n\n")
}

type lintWarningGroup struct {
	table    string
	warnings []LintViolationData
}

// writeLintLine writes one finding as a list item, led by its table, if
// given, and by the targets that raised it when not every target did. Targets
// too many to name inline read as coverage on the line, with their names
// collapsed beneath it.
func writeLintLine(sb *strings.Builder, table string, w LintViolationData) {
	message := ui.CodeQuoteIdentifiers(w.Message)
	if label := lintLabel(table, w); label != "" {
		fmt.Fprintf(sb, "- %s: %s\n", label, message)
	} else {
		fmt.Fprintf(sb, "- %s\n", message)
	}
	if len(w.Targets) > shardNamesInlineLimit {
		fmt.Fprintf(sb, "  <details>\n  <summary>Target names</summary>\n\n  %s\n\n  </details>\n\n",
			strings.Join(inlineCodeList(w.Targets), ", "))
	}
}

// lintLabel is the label a lint finding's line leads with: its table, if
// given, and the targets that raised it when not every target did.
func lintLabel(table string, w LintViolationData) string {
	var label string
	if table != "" {
		label = inlineCode(table)
	}
	if len(w.Targets) == 0 {
		return label
	}
	targets := planGroupList(targetNoun, w.Targets, w.TotalTargets)
	if label == "" {
		return "on " + targets
	}
	return label + " on " + targets
}

// writePlanWideLint writes the plan's lint section, after its DDL. A plan that
// renders target plans discloses every target's findings here, in one
// section, rather than under each target group: almost every finding is about
// the schema all targets are brought to, so the groups would repeat it.
func writePlanWideLint(sb *strings.Builder, own []LintViolationData, drift *DeploymentDriftData, targetPlans bool) {
	if !targetPlans {
		writeLintViolations(sb, own)
		return
	}
	writeLintViolations(sb, targetLint(drift))
}

// targetLint merges the target groups' lint findings into one list, sorted by
// table so a table's findings sit together. A finding every target with work
// raised names no targets, reading as a single-target plan's would. One only
// some targets raised, as drift in a table's other columns or a table's own
// AUTO_INCREMENT counter can cause, names them in rollout order. A target
// already at the desired schema has no changes to lint, so it does not keep a
// finding from naming no targets.
func targetLint(drift *DeploymentDriftData) []LintViolationData {
	var merged []LintViolationData
	var raisers [][]string
	for _, g := range drift.Plans {
		for _, f := range g.LintViolations {
			i := slices.IndexFunc(merged, func(m LintViolationData) bool { return sameLintFinding(m, f) })
			if i < 0 {
				merged = append(merged, LintViolationData{Message: f.Message, Table: f.Table, LinterName: f.LinterName, CanAutoFix: f.CanAutoFix})
				raisers = append(raisers, nil)
				i = len(merged) - 1
			}
			raisers[i] = append(raisers[i], groupLintRaisers(g, f)...)
		}
	}
	order := driftMemberNames(drift.Deployments)
	for i := range merged {
		if raisedByEveryTargetWithWork(drift.Plans, raisers[i]) {
			continue
		}
		targets := raisers[i]
		slices.SortStableFunc(targets, func(a, b string) int { return slices.Index(order, a) - slices.Index(order, b) })
		merged[i].Targets = slices.Compact(targets)
		merged[i].TotalTargets = len(drift.Deployments)
	}
	slices.SortStableFunc(merged, func(a, b LintViolationData) int { return strings.Compare(a.Table, b.Table) })
	return merged
}

// groupLintRaisers names the group's targets that raised a finding: those it
// names, or every target in the group when it names none.
func groupLintRaisers(g DeploymentPlanGroup, f LintViolationData) []string {
	if len(f.Targets) > 0 {
		return f.Targets
	}
	return g.Members
}

// raisedByEveryTargetWithWork reports whether every target in a group with
// work is among those that raised a finding.
func raisedByEveryTargetWithWork(groups []DeploymentPlanGroup, raisers []string) bool {
	for _, g := range groups {
		if g.Empty() {
			continue
		}
		for _, m := range g.Members {
			if !slices.Contains(raisers, m) {
				return false
			}
		}
	}
	return true
}

func sameLintFinding(a, b LintViolationData) bool {
	return a.Table == b.Table && a.Message == b.Message
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
		groups[i].warnings = append(groups[i].warnings, w)
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

	// UnmanagedSchema lists the schema configs the PR also changes that this
	// deployment does not manage, and UnmanagedEnvironments the environments
	// it serves. Set only by an environment-scoped deployment, which posts no
	// separate notice for them; the comment closes with a note naming them.
	UnmanagedSchema       []UnmanagedSchemaConfigNoticeData
	UnmanagedEnvironments []string
}

// RenderMultiEnvPlanComment renders a combined plan comment showing all environments.
// If all environments have identical plans, deduplicates into a single section.
func RenderMultiEnvPlanComment(data MultiEnvPlanCommentData) string {
	note := RenderUnmanagedSchemaPlanNote(data.UnmanagedEnvironments, data.UnmanagedSchema)
	if note == "" {
		return renderMultiEnvPlanCommentWithin(data, 0)
	}
	// The note goes after the plan and before the agent hint, so the hint
	// stays the comment's last line. The size limit keeps room for both.
	hint := data.AgentHint
	if plan, ok := singleEnvironmentPlan(data); ok {
		hint = plan.AgentHint
	}
	data.AgentHint = ""
	data.Plans = withoutAgentHints(data.Plans)
	body := renderMultiEnvPlanCommentWithin(data, len(note)+len(appendAgentHint("", hint))+len("\n\n"))
	return appendAgentHint(strings.TrimRight(body, "\n")+"\n\n"+note, hint)
}

func renderMultiEnvPlanCommentWithin(data MultiEnvPlanCommentData, reserve int) string {
	if plan, ok := singleEnvironmentPlan(data); ok {
		return renderWithinCommentLimit(countCommentDDLBlocks(plan), reserve, func(budget *ddlBlockBudget) string {
			return renderPlanComment(plan, budget)
		})
	}
	return renderWithinCommentLimit(countMultiEnvPlanDDLBlocks(data), reserve, func(budget *ddlBlockBudget) string {
		return renderMultiEnvPlanComment(data, budget)
	})
}

// withoutAgentHints copies plans with each one's agent hint cleared, so a
// caller appending a section after the plan can append the hint itself.
func withoutAgentHints(plans map[string]*PlanCommentData) map[string]*PlanCommentData {
	cleared := make(map[string]*PlanCommentData, len(plans))
	for env, plan := range plans {
		if plan == nil {
			cleared[env] = nil
			continue
		}
		copied := *plan
		copied.AgentHint = ""
		cleared[env] = &copied
	}
	return cleared
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
	// environment's primary plan is a clean no-op.
	writeDeploymentDrift(sb, plan.DeploymentDrift, plan.Changes)
	targetPlans := RendersTargetPlans(plan.DeploymentDrift)
	summary := *plan
	if targetPlans {
		writeTargetPlans(sb, *plan, budget, true)
		summary.Changes = combinedTargetPlanChanges(*plan)
		summary.summaryRollout = plan.DeploymentDrift
	}

	totalStatements, keyspaceUpdates := countChanges(plan.Changes)
	totalChanges := totalStatements + keyspaceUpdates
	summaryStatements, summaryKeyspaceUpdates := countChanges(summary.Changes)

	// The ignore_namespaces disclosure renders under each environment's
	// summary (writePlanSummary) or no-changes message, because entries can
	// resolve differently per environment. A primary target with nothing to
	// run while other targets still have work summarizes their plans instead.
	if totalChanges == 0 {
		if targetPlans {
			writeTableSizesSection(sb, summary)
			writePlanSummary(sb, summary, summaryStatements, summaryKeyspaceUpdates)
			if plan.MemberApplyRefusal != "" {
				writeMemberApplyRefusal(sb, plan.MemberApplyRefusalTarget, plan.MemberApplyRefusal)
				sb.WriteString("\n")
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
	writeTableSizesSection(sb, summary)
	writeCollationChangesSection(sb, *plan)

	// Blocked changes — statements the engine refuses; the apply will fail on
	// them, so each environment's section discloses its own.
	if len(plan.BlockedChanges) > 0 && !targetPlansDiscloseBlocked(*plan) {
		writeBlockedChanges(sb, plan.BlockedChanges)
	}

	// Destructive changes to tables another pull request owns — resolved per
	// environment, since the task history that attributes them is per
	// environment.
	unsafeShown := plan.HasUnsafeChanges && len(plan.UnsafeChanges) > 0
	attributionNotes, attributionFolded := unsafeAttributionNotes(*plan, unsafeShown)
	if len(plan.AttributedChanges) > 0 && !attributionFolded {
		writeAttributedChanges(sb, plan.AttributedChanges)
	}

	// Direct-execution changes — each environment's section discloses its own,
	// since the policy is configured per environment.
	if len(plan.DirectChanges) > 0 && !targetPlansDiscloseDirect(*plan) {
		writeDirectChanges(sb, plan.DirectChanges, plan.DatabaseType, plan.IsMySQL, plan.DeferCutover)
	}

	// Copies already on the target — read per environment, since each
	// environment has its own target. Target plans disclose them, and the
	// unsafe changes below, under the primary target's group.
	if !targetPlans {
		writeExistingCopies(sb, *plan)
	}

	// Unsafe changes warning
	if unsafeShown && !targetPlans {
		writeUnsafeWarning(sb, plan.UnsafeChanges, attributionNotes, plan.DatabaseType, plan.IsMySQL, true)
	}

	// Lint violations.
	writePlanWideLint(sb, plan.LintViolations, plan.DeploymentDrift, targetPlans)

	// Errors
	if len(plan.Errors) > 0 {
		writeErrors(sb, plan.Errors)
	}

	// Summary (after DDL, matching CLI layout). Target plans are summarized
	// together, as a sharded keyspace's shards are.
	writePlanSummary(sb, summary, summaryStatements, summaryKeyspaceUpdates)

	// An apply this section's other targets' plans would refuse is not offered
	// in the footer, so the section says why.
	if plan.MemberApplyRefusal != "" {
		writeMemberApplyRefusal(sb, plan.MemberApplyRefusalTarget, plan.MemberApplyRefusal)
		sb.WriteString("\n")
	}
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
	// so the footer neither offers its apply nor calls the PR done. The
	// environments after it wait on it: an apply of a later environment is
	// refused until every earlier one has succeeded, so offering one would
	// coach an apply the promotion order refuses.
	var envsRefused []string
	var envsBehindRefused []string
	// An environment whose plan carries a change the engine refuses is refused
	// too, and is offered a re-plan in place of its apply.
	var envsFailOnRefusedChange []string
	for _, env := range data.Environments {
		if _, hasErr := data.Errors[env]; hasErr {
			envsWithErrors = append(envsWithErrors, env)
		} else if plan, ok := data.Plans[env]; ok && plan != nil {
			switch {
			case environmentHasWork(plan) && len(envsRefused) > 0:
				envsBehindRefused = append(envsBehindRefused, env)
			case environmentHasWork(plan) && plan.applyFailsOnRefusedChange():
				envsRefused = append(envsRefused, env)
				envsFailOnRefusedChange = append(envsFailOnRefusedChange, env)
			case environmentHasWork(plan):
				envsWithChanges = append(envsWithChanges, env)
			case plan.MemberApplyRefusal != "":
				envsRefused = append(envsRefused, env)
			}
		}
	}

	command := func(baseCommand, environment string) string {
		return scopedCommand(baseCommand, environment, data.ScopedDatabase, data.Tenant)
	}

	// Apply instructions for environments with changes.
	switch {
	case len(envsWithChanges) >= 2:
		writeEnvApplyLeadIn(sb, "▶️ **To apply** these changes, start with the first environment", data.Plans[envsWithChanges[0]])
		fmt.Fprintf(sb, "```\n%s\n```\n", command("schemabot apply", envsWithChanges[0]))
		for i := 1; i < len(envsWithChanges); i++ {
			writeEnvApplyLeadIn(sb, fmt.Sprintf("\nAfter verifying %s, apply to %s", envsWithChanges[i-1], envsWithChanges[i]), data.Plans[envsWithChanges[i]])
			fmt.Fprintf(sb, "```\n%s\n```\n", command("schemabot apply", envsWithChanges[i]))
		}
	case len(envsWithChanges) == 1:
		if consent, ok := planUnsafeConsent(*data.Plans[envsWithChanges[0]]); ok {
			fmt.Fprintf(sb, "▶️ **To apply** these changes, %s:\n", consent.instruction())
		} else {
			sb.WriteString("▶️ **To apply** these changes, comment:\n")
		}
		fmt.Fprintf(sb, "```\n%s\n```\n", command("schemabot apply", envsWithChanges[0]))
	case len(envsWithErrors) == 0 && len(envsRefused) == 0:
		sb.WriteString("No changes to apply.\n")
	}

	for _, env := range envsFailOnRefusedChange {
		sb.WriteString("\n")
		writeRefusedChangeReplan(sb, fmt.Sprintf("the **%s** plan", env), command("schemabot plan", env))
	}

	for _, env := range envsBehindRefused {
		if slices.Contains(envsFailOnRefusedChange, envsRefused[0]) {
			fmt.Fprintf(sb, "\n"+glyph.Attention+" **%s** applies only after %s, and %s's plan carries a change the engine refuses (see above).\n",
				capitalizeFirst(env), envsRefused[0], envsRefused[0])
			continue
		}
		fmt.Fprintf(sb, "\n"+glyph.Attention+" **%s** applies only after %s, and this PR cannot apply %s's other targets' plans (see above).\n",
			capitalizeFirst(env), envsRefused[0], envsRefused[0])
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

// writeEnvApplyLeadIn writes the sentence above one environment's apply
// command, with the environment's own unsafe consent appended when its plan
// needs it.
func writeEnvApplyLeadIn(sb *strings.Builder, sentence string, plan *PlanCommentData) {
	if consent, ok := planUnsafeConsent(*plan); ok {
		fmt.Fprintf(sb, "%s. %s:\n", sentence, capitalizeFirst(consent.instruction()))
		return
	}
	sb.WriteString(sentence + ":\n")
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
// does not mean the fleet holds this schema: the primary target is at the
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
// apply: a PR apply can run the other targets' plans the section renders, and
// its primary plan has changes, or its primary target is already at the
// desired schema while those other targets' plans do. A section whose apply
// would be refused says why in the section itself.
func environmentHasWork(plan *PlanCommentData) bool {
	if plan.MemberApplyRefusal != "" {
		return false
	}
	if hasChanges(plan.Changes) {
		return true
	}
	return RendersTargetPlans(plan.DeploymentDrift)
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
