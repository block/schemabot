package templates

import (
	"fmt"
	"html"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
)

// ShardedApplyData is the input to the sharded-apply comment: an apply that fans
// out across the shards of a single keyspace within one deployment. Its unit of
// work is one operation per (shard, table). The applied comment shows shard
// status only — the DDL is already shown in the plan and apply-gate comments, so
// it is not repeated here. Shards are still grouped by their change signature so
// a divergent apply (shards that drifted to different changes) shows which shards
// moved together: a uniform apply renders one status table, a divergent one
// renders a labelled status group per distinct change set. This is distinct from
// the multi-deployment comment, whose unit is the deployment.
type ShardedApplyData struct {
	// State is the aggregate apply state (state.Apply.*), driving the headline.
	State string

	Environment string
	Database    string
	Keyspace    string
	ApplyID     string
	RequestedBy string
	StartedAt   string
	CompletedAt string

	// ErrorMessage is the failure cause to surface when the apply failed
	// without any shard in a failure state: the apply-level error when the
	// apply row carries one, otherwise a failed finalizer's operation-scoped
	// error — a failure outside shard work (e.g. a finalizer) leaves no
	// failed shard row to carry the cause.
	ErrorMessage string

	// Shards is the per-shard rollup in resolved order: one entry per shard with
	// its aggregate state. It drives the count histogram, each group's status
	// rows, and the first-failure callout.
	Shards []ShardStatus

	// Cells is one entry per (shard, table) operation — the unit that carries the
	// DDL and defines a shard's change signature for grouping.
	Cells []ShardCell

	// VSchemaChanges holds per-keyspace VSchema application state, derived from
	// the apply's finalizer operations rather than engine display metadata. Each
	// entry renders in the shared VSchema section and counts toward the outcome
	// line's grammar. Diff carries the stored plan's rendered VSchema diff —
	// the change the operator approved at plan time — and the section omits it
	// when the stored plan carries none.
	VSchemaChanges []apitypes.VSchemaChange

	// Tenant is the deployment's tenant identity, appended as --tenant to every
	// pasteable command hint so copied commands address this deployment in
	// tenant mode. Empty on single-tenant deployments, leaving hints unchanged.
	Tenant string

	// Rollback reports whether this apply reverts a previously applied schema
	// change (the apply's durable rollback option). It switches the headline
	// vocabulary from "apply" to "rollback", matching the single-deployment
	// comment.
	Rollback bool
}

// ShardStatus is one shard's aggregate status. Emoji/Label come from the same
// per-operation projection the multi-deployment comment uses, so the vocabulary
// is identical; only the unit (shard vs deployment) differs.
type ShardStatus struct {
	Shard string
	Emoji string
	Label string
	State string
	Error string
}

// ShardCell is one (shard, table) operation: the DDL for that table on that
// shard. Cells with the same (table, DDL) set across shards group those shards
// together.
type ShardCell struct {
	Shard string
	Table string
	DDL   string
}

// ShardChange is one table's DDL within a group. The DDL is not rendered in the
// applied comment; it defines the group's change signature so shards that apply
// the same change are grouped together.
type ShardChange struct {
	Table string
	DDL   string
}

// shardGroup is a set of shards that share an identical change signature, with
// the changes they all apply.
type shardGroup struct {
	Shards  []ShardStatus
	Changes []ShardChange
}

// RenderShardedApplyComment renders the PR comment for a sharded apply: the
// shared apply header and metadata, a per-shard count histogram, the first
// failed shard's error lifted to the top, then the shards grouped by change
// signature — a single group renders one status table; more than one renders a
// labelled status group per distinct change set. The comment is status-only; the
// DDL is shown in the plan and apply-gate comments, not repeated here.
func RenderShardedApplyComment(data ShardedApplyData) string {
	var sb strings.Builder
	renderedAt := currentTimestamp()

	writeApplyStatusHeader(&sb, ApplyStatusCommentData{State: data.State, Environment: data.Environment, Rollback: data.Rollback})
	writeShardedMetadata(&sb, data, renderedAt)

	writeShardCounts(&sb, data.Shards)
	writeShardedFailure(&sb, data)
	writeShardKeyspaceSection(&sb, data)
	writeVSchemaStatus(&sb, data.VSchemaChanges)

	writeShardedFooter(&sb, data)
	if !state.IsTerminalApplyState(data.State) {
		writeLastUpdatedFooter(&sb, renderedAt)
	}
	return sb.String()
}

// RenderShardedApplySummaryComment renders the final summary PR comment for a
// terminal sharded apply — the shard-unit analogue of RenderApplySummaryComment.
// It leads with the state-specific verdict header and outcome line every other
// apply shape's summary shares, then carries the same shard rollup the status
// comment shows (counts, first failure, per-shard tables grouped by change
// signature) so the outcome record names every shard's final state.
func RenderShardedApplySummaryComment(data ShardedApplyData) string {
	var sb strings.Builder

	writeApplyHeader(&sb, ApplyStatusCommentData{State: data.State, Environment: data.Environment, Rollback: data.Rollback})
	writeShardedSummaryMetadata(&sb, data)
	if state.IsState(data.State, state.Apply.Completed) {
		writeSuccessBlock(&sb, completedOutcomeMessage(shardedChangeIsSingular(data), data.Rollback))
	}
	writeShardCounts(&sb, data.Shards)
	writeShardedFailure(&sb, data)
	writeShardKeyspaceSection(&sb, data)
	writeVSchemaStatus(&sb, data.VSchemaChanges)
	writeShardedFooter(&sb, data)
	return sb.String()
}

// shardedChangeIsSingular reports whether the apply lands exactly one change —
// one table's DDL across the shards, or one VSchema update — driving the
// outcome line's singular/plural wording. Distinct tables and VSchema changes
// each count as one change; an apply with neither reads as plural, since the
// shard count alone does not prove a single change.
func shardedChangeIsSingular(data ShardedApplyData) bool {
	tables := make(map[string]struct{}, len(data.Cells))
	for _, c := range data.Cells {
		tables[c.Table] = struct{}{}
	}
	return len(tables)+len(data.VSchemaChanges) == 1
}

// writeShardedSummaryMetadata writes the terminal metadata line — database,
// engine type, apply ID, and duration when both timestamps are known — followed
// by the attribution line: the sharded analogue of writeSummaryMetadata.
func writeShardedSummaryMetadata(sb *strings.Builder, data ShardedApplyData) {
	parts := []string{
		fmt.Sprintf("**Database**: `%s`", data.Database),
		"**Type**: `Strata`",
		fmt.Sprintf("**Apply ID**: `%s`", data.ApplyID),
	}
	if d := durationDisplay(data.StartedAt, data.CompletedAt); d != "" {
		parts = append(parts, fmt.Sprintf("**Duration**: %s", d))
	}
	fmt.Fprintf(sb, "%s\n", strings.Join(parts, " | "))
	writeAppliedByOrTimestampAt(sb, data.RequestedBy, startedAtDisplay(data.StartedAt, currentTimestamp()))
}

// writeShardKeyspaceSection writes the keyspace heading and the per-shard
// status tables grouped by change signature. The section shows shard status
// only: the DDL (what changes) is already shown in the plan and apply-gate
// comments, so repeating it here adds nothing — and rendering "DDL unavailable"
// when the apply path has no per-shard DDL is pure noise. Shards are still
// grouped by change so a divergent apply shows which shards moved together.
func writeShardKeyspaceSection(sb *strings.Builder, data ShardedApplyData) {
	fmt.Fprintf(sb, "\n#### Keyspace `%s`\n", data.Keyspace)
	groups := groupShardsBySignature(data.Shards, data.Cells)
	if len(groups) <= 1 {
		writeShardStatusTable(sb, data.Shards)
		return
	}
	sb.WriteString("\nShards diverge — grouped by change:\n")
	for _, g := range groups {
		fmt.Fprintf(sb, "\n**%s**\n", shardList(g.Shards))
		writeShardStatusTable(sb, g.Shards)
	}
}

func writeShardedMetadata(sb *strings.Builder, data ShardedApplyData, renderedAt string) {
	parts := []string{
		fmt.Sprintf("**Database**: `%s`", data.Database),
		"**Type**: `Strata`",
		fmt.Sprintf("**Apply ID**: `%s`", data.ApplyID),
	}
	fmt.Fprintf(sb, "%s\n", strings.Join(parts, " | "))
	attributionAt := renderedAt
	if data.RequestedBy == "" {
		attributionAt = startedAtDisplay(data.StartedAt, renderedAt)
	}
	writeAppliedByOrTimestampAt(sb, data.RequestedBy, attributionAt)
}

// groupShardsBySignature buckets shards whose change set is identical. The
// signature is the ordered (table, DDL) pairs the shard applies, so shards
// needing different tables — or the same table with different DDL — fall into
// different groups. Groups and the shards within them keep resolved order; a
// uniform apply yields exactly one group.
func groupShardsBySignature(shards []ShardStatus, cells []ShardCell) []shardGroup {
	changesByShard := make(map[string][]ShardChange, len(shards))
	for _, c := range cells {
		changesByShard[c.Shard] = append(changesByShard[c.Shard], ShardChange{Table: c.Table, DDL: c.DDL})
	}

	var order []string
	bySig := make(map[string]*shardGroup)
	for _, s := range shards {
		changes := changesByShard[s.Shard]
		sig := signatureOf(changes)
		g := bySig[sig]
		if g == nil {
			g = &shardGroup{Changes: changes}
			bySig[sig] = g
			order = append(order, sig)
		}
		g.Shards = append(g.Shards, s)
	}

	groups := make([]shardGroup, 0, len(order))
	for _, sig := range order {
		groups = append(groups, *bySig[sig])
	}
	return groups
}

// signatureOf builds the change-set key for a shard from its ordered changes.
func signatureOf(changes []ShardChange) string {
	parts := make([]string, len(changes))
	for i, c := range changes {
		parts[i] = c.Table + "\x00" + c.DDL
	}
	return strings.Join(parts, "\x01")
}

// shardList renders a group's shards as "shard `x`" or "shards `x`, `y`".
func shardList(shards []ShardStatus) string {
	names := make([]string, len(shards))
	for i, s := range shards {
		names[i] = fmt.Sprintf("`%s`", s.Shard)
	}
	if len(names) == 1 {
		return "shard " + names[0]
	}
	return "shards " + strings.Join(names, ", ")
}

// writeShardCounts writes the per-status histogram across shards so rollout
// health is visible at a glance — the shard-unit analogue of the
// multi-deployment "Deployments:" line.
func writeShardCounts(sb *strings.Builder, shards []ShardStatus) {
	if len(shards) == 0 {
		return
	}
	order := make([]string, 0, len(shards))
	counts := make(map[string]int, len(shards))
	for _, s := range shards {
		label := shardCountLabel(s)
		if _, seen := counts[label]; !seen {
			order = append(order, label)
		}
		counts[label]++
	}
	parts := make([]string, 0, len(order))
	for _, label := range order {
		parts = append(parts, fmt.Sprintf("%d %s", counts[label], label))
	}
	fmt.Fprintf(sb, "\n**Shards**: %s\n", strings.Join(parts, ", "))
}

// shardCountLabel collapses a shard's full label to its leading state word
// ("halted — …" → "halted") for the histogram.
func shardCountLabel(s ShardStatus) string {
	if i := strings.Index(s.Label, " — "); i >= 0 {
		return s.Label[:i]
	}
	return s.Label
}

// isShardFailureState reports whether a shard's state carries an operator-facing
// error to surface — a terminal failure or an automatic retry after one. The
// retry case matters because SchemaBot holds the apply in failed_retryable while
// it retries, and the operator still needs to see what went wrong.
func isShardFailureState(opState string) bool {
	return opState == state.ApplyOperation.Failed || opState == state.ApplyOperation.FailedRetryable
}

// writeShardedFailure lifts the failure cause to the top so an operator sees
// it without scanning the table: the first failed shard's error when shard
// work failed, otherwise the apply-level error — a failure outside shard work
// (e.g. a finalizer) leaves no failed shard row to carry the cause, and a
// failed apply must still name it. Renders nothing when the apply carries no
// failure to explain.
func writeShardedFailure(sb *strings.Builder, data ShardedApplyData) {
	for _, s := range data.Shards {
		if !isShardFailureState(s.State) {
			continue
		}
		shard := html.EscapeString(s.Shard)
		if msg := SanitizeInlineError(s.Error); msg == "" {
			fmt.Fprintf(sb, "\n> ⚠️ **First failure:** shard <code>%s</code>\n", shard)
		} else {
			fmt.Fprintf(sb, "\n> ⚠️ **First failure:** shard <code>%s</code> — %s\n", shard, html.EscapeString(msg))
		}
		return
	}
	if !state.IsState(data.State, state.Apply.Failed, state.Apply.FailedRetryable) {
		return
	}
	if msg := SanitizeInlineError(data.ErrorMessage); msg != "" {
		fmt.Fprintf(sb, "\n> ⚠️ **Failure:** %s\n", html.EscapeString(msg))
	}
}

// writeShardStatusTable renders the per-shard status table for a set of shards.
func writeShardStatusTable(sb *strings.Builder, shards []ShardStatus) {
	if len(shards) == 0 {
		return
	}
	sb.WriteString("\n| Shard | Status |\n| --- | --- |\n")
	for _, s := range shards {
		fmt.Fprintf(sb, "| `%s` | %s |\n", s.Shard, shardStatusCell(s))
	}
}

// shardStatusCell renders one shard's "<emoji> <label>" cell, appending the
// error for a failed shard.
func shardStatusCell(s ShardStatus) string {
	cell := html.EscapeString(s.Label)
	if s.Emoji != "" {
		cell = fmt.Sprintf("%s %s", s.Emoji, cell)
	}
	if isShardFailureState(s.State) {
		if msg := sanitizeCellError(s.Error); msg != "" {
			cell = fmt.Sprintf("%s — %s", cell, html.EscapeString(msg))
		}
	}
	return cell
}

// writeShardedFooter renders the single next operator action, matching the
// single-deployment footer vocabulary: a failed apply is retried, an
// auto-retrying (failed_retryable) apply offers the stop-retrying command, a
// stopped apply is resumed, and a cancelled apply — permanent, unlike stopped —
// directs the operator to open a new schema change.
func writeShardedFooter(sb *strings.Builder, data ShardedApplyData) {
	switch {
	case state.IsState(data.State, state.Apply.Failed):
		writeFooterAction(sb, "To retry:", appendTenantFlag(fmt.Sprintf("schemabot apply -e %s", data.Environment), data.Tenant))
	case state.IsState(data.State, state.Apply.FailedRetryable):
		writeFooterAction(sb, "An error interrupted this schema change. SchemaBot retries automatically and marks it failed if retries are exhausted. To stop retrying:", appendTenantFlag(fmt.Sprintf("schemabot stop %s -e %s", data.ApplyID, data.Environment), data.Tenant))
	case state.IsState(data.State, state.Apply.Stopped):
		writeFooterAction(sb, "Paused — to resume from where it stopped:", appendTenantFlag(fmt.Sprintf("schemabot start %s -e %s", data.ApplyID, data.Environment), data.Tenant))
	case state.IsState(data.State, state.Apply.Cancelled):
		sb.WriteString("\n---\n\nThis schema change was cancelled and cannot be resumed. Open a new schema change to apply it again.\n")
	}
}
