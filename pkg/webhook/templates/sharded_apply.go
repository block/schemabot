package templates

import (
	"fmt"
	"html"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

// ShardedApplyData is the input to the sharded-apply comment: an apply that fans
// out across the shards of one or more keyspaces within one deployment. Its unit
// of work is one operation per (shard, table). The applied comment shows shard
// status only — the DDL is already shown in the plan and apply-gate comments, so
// it is not repeated here. Each keyspace renders as per-table rollup lines;
// per-shard status tables are exception detail, appearing only when the keyspace
// needs shard-level attention (see writeShardKeyspaceSections), grouped by
// change signature when shards diverge so a divergent apply shows which shards
// moved together. This is distinct from the multi-deployment comment, whose unit
// is the deployment.
type ShardedApplyData struct {
	// State is the aggregate apply state (state.Apply.*), driving the headline.
	State string

	Environment string
	Database    string
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

	// Keyspaces is the per-keyspace shard rollup in resolved order: each entry
	// renders its own keyspace section, and together they drive the
	// cross-keyspace count histogram and the first-failure callout.
	Keyspaces []ShardedKeyspace

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

// ShardedKeyspace is one keyspace's slice of a sharded apply: its tables in
// resolved order, each aggregated across the keyspace's shards, plus the
// per-shard rollup and the (shard, table) cells behind them.
type ShardedKeyspace struct {
	Keyspace string

	// Tables is the keyspace's per-table rollup in resolved order — the unit the
	// section renders. Each entry carries the table's aggregate state and its
	// per-shard breakdown for the compact shard summary line.
	Tables []ShardedTableStatus

	// Shards is the keyspace's per-shard rollup in resolved order: one entry per
	// shard with its aggregate state. It drives the cross-keyspace histogram and
	// first-failure callout always, and the section's per-shard status rows when
	// the keyspace needs shard detail (a failure or divergent shards).
	Shards []ShardStatus

	// Cells is one entry per (shard, table) operation — the unit that carries the
	// DDL and defines a shard's change signature for grouping.
	Cells []ShardCell
}

// ShardedTableStatus is one table's rollup within a keyspace: the aggregate
// state across the keyspace's shards (the most attention-worthy shard's state,
// in canonical task vocabulary) and the per-shard breakdown feeding the compact
// shard summary line.
type ShardedTableStatus struct {
	Table string

	// Status is the table's aggregate state in canonical task vocabulary.
	Status string

	// RowsCopied/RowsTotal/ETASeconds aggregate the shards' live copy figures:
	// rows summed across the shards that have reported, ETA from the slowest
	// shard. They drive the progress bar and rows line while the table copies;
	// zero totals fall back to the state-word rendering. The totals grow as
	// dispatch waves start their shards' copies, so ShardsReporting says how
	// many shards the figures cover and both the headline and the rows line
	// disclose partial coverage rather than passing a wave's fraction off as
	// the table's.
	RowsCopied int64
	RowsTotal  int64
	ETASeconds int64

	// ShardsReporting is the number of shards whose engine has reported row
	// figures — the shards the copy aggregates above cover. While it is below
	// len(Shards), the totals describe only the started waves: the headline
	// and rows line name the coverage and the ETA renders as a floor (the
	// unstarted shards can only add rows and time).
	ShardsReporting int

	// Shards is the per-shard state (and percent while copying) in resolved
	// order, rendered as the compact one-line summary while the table is in
	// flight.
	Shards []ShardProgressData
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
// shared apply header and metadata, a per-shard count histogram across every
// keyspace, the first failed shard's error lifted to the top, then a section
// per keyspace with its per-table rollup lines, adding per-shard status tables
// (grouped by change signature when shards diverge) only where a keyspace
// needs shard detail. The comment is status-only; the DDL is shown in the
// plan and apply-gate comments, not repeated here.
func RenderShardedApplyComment(data ShardedApplyData) string {
	var sb strings.Builder
	renderedAt := currentTimestamp()

	writeApplyStatusHeader(&sb, ApplyStatusCommentData{State: data.State, Environment: data.Environment, Rollback: data.Rollback})
	writeShardedMetadata(&sb, data, renderedAt)
	writeShardedStatusLine(&sb, data)

	writeShardCounts(&sb, allShardStatuses(data.Keyspaces))
	writeShardedFailure(&sb, data)
	writeShardKeyspaceSections(&sb, data.Keyspaces)
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
// comment shows: counts, first failure, and per-table rollup lines, with
// per-shard status tables wherever a keyspace's outcome needs shard detail —
// a failure, divergent change signatures, or divergent shard outcomes.
func RenderShardedApplySummaryComment(data ShardedApplyData) string {
	var sb strings.Builder

	writeApplyHeader(&sb, ApplyStatusCommentData{State: data.State, Environment: data.Environment, Rollback: data.Rollback})
	writeShardedSummaryMetadata(&sb, data)
	if state.IsState(data.State, state.Apply.Completed) {
		writeSuccessBlock(&sb, completedOutcomeMessage(shardedChangeIsSingular(data), data.Rollback))
	}
	writeShardCounts(&sb, allShardStatuses(data.Keyspaces))
	writeShardedFailure(&sb, data)
	writeShardKeyspaceSections(&sb, data.Keyspaces)
	writeVSchemaStatus(&sb, data.VSchemaChanges)
	writeShardedFooter(&sb, data)
	return sb.String()
}

// allShardStatuses flattens every keyspace's shards in resolved order, feeding
// the rollup elements that span keyspaces — the count histogram and the
// first-failure callout.
func allShardStatuses(keyspaces []ShardedKeyspace) []ShardStatus {
	var out []ShardStatus
	for _, ks := range keyspaces {
		out = append(out, ks.Shards...)
	}
	return out
}

// shardedChangeIsSingular reports whether the apply lands exactly one change —
// one keyspace table's DDL across the shards, or one VSchema update — driving
// the outcome line's singular/plural wording. Distinct (keyspace, table) pairs
// and VSchema changes each count as one change; an apply with neither reads as
// plural, since the shard count alone does not prove a single change.
func shardedChangeIsSingular(data ShardedApplyData) bool {
	tables := make(map[string]struct{})
	for _, ks := range data.Keyspaces {
		for _, c := range ks.Cells {
			tables[tableKey(ks.Keyspace, c.Table)] = struct{}{}
		}
	}
	return len(tables)+len(data.VSchemaChanges) == 1
}

// writeShardedStatusLine writes the status comment's at-a-glance line: the
// apply's display state and how many of the plan's changes have landed. A
// change is one (keyspace, table) unit or one VSchema update, matching the
// plan comment's arithmetic. Omitted when no keyspace carries a table rollup —
// without per-table aggregates the fraction would read 0 regardless of real
// progress.
func writeShardedStatusLine(sb *strings.Builder, data ShardedApplyData) {
	hasTables := false
	for _, ks := range data.Keyspaces {
		if len(ks.Tables) > 0 {
			hasTables = true
			break
		}
	}
	if !hasTables {
		return
	}
	applied, total := shardedChangeFraction(data)
	word := applyStatusDetail(data.State)
	if data.Rollback && state.IsState(data.State, state.Apply.Completed) {
		word = "Rolled Back"
	}
	noun := "changes"
	if total == 1 {
		noun = "change"
	}
	verb := "applied"
	if data.Rollback {
		verb = "rolled back"
	}
	fmt.Fprintf(sb, "\n**Status**: %s — %d of %d %s %s\n", word, applied, total, noun, verb)
}

// shardedChangeFraction counts the apply's changes and how many have landed. A
// change is one (keyspace, table) unit or one VSchema update.
func shardedChangeFraction(data ShardedApplyData) (applied, total int) {
	for _, ks := range data.Keyspaces {
		for _, t := range ks.Tables {
			total++
			if shardedTableChangeLanded(t) {
				applied++
			}
		}
	}
	for _, v := range data.VSchemaChanges {
		total++
		if v.Status == "applied" {
			applied++
		}
	}
	return applied, total
}

// shardedTableChangeLanded reports whether a table's DDL has landed on every
// shard: each one completed or holding in its revert window. The aggregate
// status alone cannot prove this — it surfaces the most attention-worthy
// shard, and a revert-window aggregate can sit above shards whose dispatch
// wave has not started. Without per-shard statuses the aggregate is all there
// is, so it decides.
func shardedTableChangeLanded(t ShardedTableStatus) bool {
	if len(t.Shards) == 0 {
		return shardChangeLanded(t.Status)
	}
	return landedShardCount(t.Shards) == len(t.Shards)
}

// shardChangeLanded reports whether one shard's status means the table's DDL is
// in place on that shard: completed, or applied and holding in its revert
// window.
func shardChangeLanded(status string) bool {
	normalized := state.NormalizeTaskStatus(status)
	return normalized == state.Task.Completed || normalized == state.Task.RevertWindow
}

// landedShardCount counts the shards where the table's change has landed.
func landedShardCount(shards []ShardProgressData) int {
	n := 0
	for _, sh := range shards {
		if shardChangeLanded(sh.Status) {
			n++
		}
	}
	return n
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

// writeShardKeyspaceSections writes one section per keyspace: its heading and
// its per-table rollup lines, each with the compact shard summary while in
// flight. The section shows status only: the DDL (what changes) is already
// shown in the plan and apply-gate comments, so repeating it here adds nothing.
// Per-shard status tables are exception detail — they render only when the
// keyspace needs them: a shard failure anywhere in the apply (the failed shard
// and its halted siblings need naming, and a halted sibling can sit in another
// keyspace, whose rows carry the attribution), shards that diverge by change
// signature (which shards moved together is invisible at the table level),
// divergent shard outcomes (the split the operator reconciles shard by shard),
// or a keyspace carrying no table rollup to stand in for them.
func writeShardKeyspaceSections(sb *strings.Builder, keyspaces []ShardedKeyspace) {
	applyHasShardFailure := false
	for _, ks := range keyspaces {
		if keyspaceHasShardFailure(ks.Shards) {
			applyHasShardFailure = true
			break
		}
	}
	for _, ks := range keyspaces {
		fmt.Fprintf(sb, "\n#### Keyspace `%s`\n\n", ks.Keyspace)
		for _, t := range ks.Tables {
			writeShardedTableLine(sb, t)
		}
		needsShardDetail := applyHasShardFailure || keyspaceHasDivergentOutcome(ks.Shards)
		groups := groupShardsBySignature(ks.Shards, ks.Cells)
		if len(ks.Tables) > 0 && len(groups) <= 1 && !needsShardDetail {
			continue
		}
		if len(groups) <= 1 {
			writeShardStatusTable(sb, ks.Shards)
			continue
		}
		sb.WriteString("\nShards diverge — grouped by change:\n")
		for _, g := range groups {
			fmt.Fprintf(sb, "\n**%s**\n", shardList(g.Shards, len(ks.Shards)))
			writeShardStatusTable(sb, g.Shards)
		}
	}
}

// keyspaceHasShardFailure reports whether any of the keyspace's shards is in a
// failure state, which promotes the keyspace's per-shard status table from
// noise to the failure detail an operator acts on.
func keyspaceHasShardFailure(shards []ShardStatus) bool {
	for _, s := range shards {
		if isShardFailureState(s.State) {
			return true
		}
	}
	return false
}

// keyspaceHasDivergentOutcome reports whether the keyspace's shards split
// between shards where the change landed (completed or holding in the revert
// window) and shards where it terminally did not (cancelled, reverted, or
// stopped). That split is a schema divergence the operator reconciles shard by
// shard, so it promotes the per-shard status table.
func keyspaceHasDivergentOutcome(shards []ShardStatus) bool {
	landed, missed := false, false
	for _, s := range shards {
		switch {
		case state.IsState(s.State, state.ApplyOperation.Completed, state.ApplyOperation.RevertWindow):
			landed = true
		case state.IsState(s.State, state.ApplyOperation.Cancelled, state.ApplyOperation.Reverted, state.ApplyOperation.Stopped):
			missed = true
		}
	}
	return landed && missed
}

// writeShardedTableLine writes one table's rollup line — the table name and its
// aggregate state, with the shard count on a completed sharded table — followed
// by the compact per-shard summary while the table is in flight. An actively
// copying table with reported row figures renders the live progress form (bar,
// summed rows, slowest-shard ETA); every other state names its phase, and the
// shard summary carries the per-shard percents. When the aggregate is a quiet
// state (no shard-summary breakdown) but the change has already landed on some
// shards — a partially-landed table between dispatch waves, or one cancelled
// after part of the fleet applied — the line states the landed coverage so the
// aggregate phrase alone never hides or contradicts work that happened.
func writeShardedTableLine(sb *strings.Builder, t ShardedTableStatus) {
	status := state.NormalizeTaskStatus(t.Status)
	if status == state.Task.Running && t.RowsTotal > 0 {
		writeShardedTableCopyProgress(sb, t)
	} else {
		phrase := shardedTableStatusPhrase(status)
		if landed := landedShardCount(t.Shards); landed > 0 && landed < len(t.Shards) && !shardSummaryBreakdownState(status) {
			if status == state.Task.Cancelled {
				// The pure-cancelled parenthetical ("not started") would be false
				// here: the change is live on the landed shards.
				phrase = "⊘ Cancelled"
			}
			phrase += fmt.Sprintf(" — applied on %d of %d shards", landed, len(t.Shards))
		}
		line := fmt.Sprintf("**`%s`**: %s", t.Table, phrase)
		if status == state.Task.Completed && len(t.Shards) > 1 {
			line += fmt.Sprintf(" (%d shards)", len(t.Shards))
		}
		sb.WriteString(line + "\n")
	}
	renderShardSummary(sb, TableProgressData{TableName: t.Table, Status: t.Status, Shards: t.Shards})
}

// writeShardedTableCopyProgress renders an actively copying table's live
// figures, mirroring the single-deployment row-copy idiom: a percent bar from
// the summed shard rows plus the rows/ETA line. The same honesty guards apply —
// a copy that has not reported rows yet shows a starting indicator instead of a
// stuck-looking 0% bar, and a copy past its estimated total shows finalizing
// instead of a bar pinned at 100%. While some shards have yet to report (later
// dispatch waves), the figures describe only the reporting shards, so both the
// headline and the rows line name the coverage and the ETA renders as a floor —
// nothing claims to describe shards that have not started.
func writeShardedTableCopyProgress(sb *strings.Builder, t ShardedTableStatus) {
	if ui.EstimateExceeded(t.RowsCopied, t.RowsTotal) {
		fmt.Fprintf(sb, "**`%s`**: %s Finalizing copy%s\n", t.Table, ui.ProgressBarActivity(), shardedCopyCoverageSuffix(t))
		fmt.Fprintf(sb, "- Rows copied: %s so far\n", ui.FormatNumber(t.RowsCopied))
		fmt.Fprintf(sb, "- %s _%s_\n", glyph.Info, ui.EstimateExceededTooltip)
		return
	}
	pct := ui.RowCopyDisplayPercent(int(ui.ClampRows(t.RowsCopied, t.RowsTotal)*100/t.RowsTotal), t.RowsCopied)
	if pct == 0 {
		fmt.Fprintf(sb, "**`%s`**: ⏳ Starting copy...\n", t.Table)
		writeShardedRowsAndETA(sb, t)
		return
	}
	fmt.Fprintf(sb, "**`%s`**: %s %d%%%s\n", t.Table, ui.ProgressBarRowCopy(pct), pct, shardedCopyCoverageSuffix(t))
	writeShardedRowsAndETA(sb, t)
}

// shardedCopyCoverageSuffix qualifies a copying table's headline while later
// dispatch waves have yet to report: the fraction behind the bar covers only
// the reporting shards, so the headline says so — "62% (1 of 4 shards)" —
// instead of passing a wave's fraction off as the whole table's. With every
// shard reporting the figures are the table's and no qualifier is needed.
func shardedCopyCoverageSuffix(t ShardedTableStatus) string {
	if t.ShardsReporting >= len(t.Shards) {
		return ""
	}
	return fmt.Sprintf(" (%s)", shardCoveragePhrase(t.ShardsReporting, len(t.Shards)))
}

// writeShardedRowsAndETA writes the copying table's rows/ETA line. With every
// shard reporting it matches the single-deployment line. While later waves
// have yet to start, the summed totals cover only the reporting shards and the
// slowest-reporting-shard ETA is a floor, so the line says both: it names the
// coverage and renders the ETA as "≥" — the remaining shards can only add
// rows and time.
func writeShardedRowsAndETA(sb *strings.Builder, t ShardedTableStatus) {
	if t.ShardsReporting >= len(t.Shards) {
		writeRowsAndETA(sb, TableProgressData{TableName: t.Table, RowsCopied: t.RowsCopied, RowsTotal: t.RowsTotal, ETASeconds: t.ETASeconds})
		return
	}
	line := fmt.Sprintf("- Rows: %s / %s across %d of %d shards",
		ui.FormatNumber(ui.ClampRows(t.RowsCopied, t.RowsTotal)),
		ui.FormatNumber(t.RowsTotal),
		t.ShardsReporting, len(t.Shards))
	if t.ETASeconds > 0 {
		line += fmt.Sprintf(" · ETA: ≥ %s", ui.FormatETA(t.ETASeconds))
	}
	sb.WriteString(line + "\n")
}

// shardedTableStatusPhrase maps a table's aggregate task state to its display
// phrase, reusing the single-deployment table vocabulary without the per-table
// progress bar.
func shardedTableStatusPhrase(status string) string {
	switch status {
	case state.Task.Pending:
		return "⏳ Queued"
	case state.Task.Completed:
		return "✅ Complete"
	case state.Task.Running:
		return "🔄 Row copy in progress"
	case state.Task.CatchingUp:
		return "⏩ Catching up on accumulated changes..."
	case state.Task.Checksumming:
		return "🔍 Checksumming to verify data..."
	case state.Task.PostChecksum:
		return "⏩ Data verified, applying final changes..."
	case state.Task.WaitingForCutover:
		return "🟡 Waiting for cutover"
	case state.Task.CuttingOver:
		return "🔄 Cutting over..."
	case state.Task.Recovering:
		return "🔄 Recovering state..."
	case state.Task.Failed:
		return glyph.Failed + " Failed"
	case state.Task.FailedRetryable:
		return "🔄 Interrupted — retrying automatically"
	case state.Task.Stopped:
		return "⏸ Stopped"
	case state.Task.Cancelled:
		return "⊘ Cancelled (not started)"
	case state.Task.RevertWindow:
		// Deliberately no checkmark: the change is applied but not final while
		// the revert window is open, and a checkmark reads as "done, walk away".
		return "Complete (revert window open)"
	case state.Task.Reverting:
		return "↩️ Reverting"
	case state.Task.Reverted:
		return "↩️ Reverted"
	case state.Task.WaitingForDeploy:
		return "🟡 Waiting for deploy"
	default:
		return "🔄 In progress"
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

// shardList renders a divergence group's shards as "shard `x`" or
// "shards `x`, `y`" when few enough to read inline, stating coverage against
// the keyspace's shard count beyond that ("12 of 32 shards") — the full names
// stay reachable in the group's status table below. A divergent keyspace
// always has at least two groups, so with the real total no group can read
// as "all" shards.
func shardList(shards []ShardStatus, totalShards int) string {
	names := make([]string, len(shards))
	for i, s := range shards {
		names[i] = s.Shard
	}
	return planShardList(names, totalShards)
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
	for _, s := range allShardStatuses(data.Keyspaces) {
		if !isShardFailureState(s.State) {
			continue
		}
		severity := failureSeverity(s.State == state.ApplyOperation.FailedRetryable)
		shard := html.EscapeString(s.Shard)
		if msg := SanitizeInlineError(s.Error); msg == "" {
			fmt.Fprintf(sb, "\n> %s **First failure:** shard <code>%s</code>\n", severity, shard)
		} else {
			fmt.Fprintf(sb, "\n> %s **First failure:** shard <code>%s</code> — %s\n", severity, shard, html.EscapeString(msg))
		}
		return
	}
	if !state.IsState(data.State, state.Apply.Failed, state.Apply.FailedRetryable) {
		return
	}
	if msg := SanitizeInlineError(data.ErrorMessage); msg != "" {
		fmt.Fprintf(sb, "\n> %s **Failure:** %s\n", failureSeverity(state.IsState(data.State, state.Apply.FailedRetryable)), html.EscapeString(msg))
	}
}

// failureSeverity returns the glyph for a surfaced failure: glyph.Attention
// while SchemaBot is still retrying on its own — nothing has stopped and no
// triage is due — and glyph.Failed once the system has stopped on the error.
func failureSeverity(retrying bool) string {
	if retrying {
		return glyph.Attention
	}
	return glyph.Failed
}

// writeShardStatusTable renders the per-shard status table for a set of shards.
func writeShardStatusTable(sb *strings.Builder, shards []ShardStatus) {
	if len(shards) == 0 {
		return
	}
	sb.WriteString("\n| Shard | Status |\n| --- | --- |\n")
	for _, s := range shards {
		fmt.Fprintf(sb, "| %s | %s |\n", markdownInlineCode(s.Shard), shardStatusCell(s))
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
