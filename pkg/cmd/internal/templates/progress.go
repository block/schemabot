package templates

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

// Indentation for progress rendering.
// indentTable is the prefix for table names. Aligns with keyspace name after "── " in headers.
const indentTable = "     " // 5 spaces — matches "  ── " in FormatKeyspaceHeader

// progressSymbol returns a Terraform-style prefix for the change type.
func progressSymbol(changeType string) string {
	switch ddl.OpToStatementType(changeType) {
	case ddl.StatementCreateTable:
		return "+ "
	case ddl.StatementDropTable:
		return "- "
	default:
		return "~ "
	}
}

// formatProgressDDLForDialect renders a DDL statement under the dialect's own
// grammar with syntax highlighting, indented under the table name.
func formatProgressDDLForDialect(dialect schema.Dialect, rawDDL string) string {
	if rawDDL == "" {
		return ""
	}
	return IndentSQL(ddl.FormatDDLForDialect(dialect, rawDDL), indentContent) + "\n"
}

// formatTableDDL renders a table's DDL under its name, headed by the targets
// it runs on when a rollout runs it on only some of them.
func formatTableDDL(t TableProgress) string {
	block := formatProgressDDLForDialect(t.Dialect, t.DDL)
	if block == "" || t.OnTargets == "" {
		return block
	}
	return indentContent + ANSIBold + t.OnTargets + ANSIReset + "\n" + block
}

// indentContent is the indentation for DDL lines under a table name.
var indentContent = strings.Repeat(" ", 7)

// indentDetail is the prefix for Rows/Shards detail lines (one level deeper than DDL, with bullet).
const indentDetail = "       • " // 7 spaces + bullet + space

// FormatKeyspaceHeader returns a keyspace divider line.
func FormatKeyspaceHeader(ns string) string {
	return fmt.Sprintf("\n  %s── %s ──%s\n\n", ANSIBold, ns, ANSIReset)
}

// nowFunc returns the current time. Overridden in previews for deterministic output.
var nowFunc = time.Now

// WriteProgress writes the schema change progress to stdout.
func WriteProgress(data ProgressData) {
	// No active schema change
	if state.IsState(data.State, state.NoActiveChange) {
		fmt.Println("No active schema change")
		return
	}
	if RendersOperationSections(data.Operations, data.Sharded) {
		writeMultiDeploymentProgress(data)
		return
	}

	// Build key/value pairs for the detail box
	displayState := state.Label(data.State)
	if state.IsState(data.State, state.Apply.PreparingBranch) && data.Metadata != nil && data.Metadata["existing_branch"] != "" {
		displayState = "Refreshing branch schema"
	}
	// Show latest event detail during setup phases
	if data.Metadata != nil && data.Metadata["status_detail"] != "" {
		if state.IsState(data.State, state.Apply.PreparingBranch, state.Apply.ApplyingBranchChanges, state.Apply.CreatingDeployRequest) {
			displayState = data.Metadata["status_detail"]
		}
	}
	colorFn := stateColorFunc(data.State)

	var rows []BoxRow

	if data.ApplyID != "" {
		rows = append(rows, BoxRow{"Apply ID", data.ApplyID})
	}
	if data.Database != "" {
		rows = append(rows, BoxRow{"Database", data.Database})
	}
	if data.Environment != "" {
		rows = append(rows, BoxRow{"Environment", data.Environment})
	}
	rows = append(rows, BoxRow{"State", displayState})
	if data.Sharded {
		if counts := ShardCounts(data.State, data.Operations, data.Tables, data.Released, data.MaxOperationsAtOnce); counts != "" {
			rows = append(rows, BoxRow{"Shards", counts})
		}
	}
	rows = append(rows, callerAndSourceBoxRows(data.Caller, data.PullRequestURL)...)
	if len(data.Options) > 0 {
		var opts []string
		if data.Options["defer_deploy"] == "true" {
			opts = append(opts, "⏸️ Defer Deploy")
		}
		if data.Options["defer_cutover"] == "true" {
			opts = append(opts, "⏸️ Defer Cutover")
		}
		if data.Options["skip_revert"] == "true" {
			opts = append(opts, "⏩ Skip Revert")
		}
		if len(opts) > 0 {
			rows = append(rows, BoxRow{"Options", strings.Join(opts, " | ")})
		}
	}
	if data.Metadata != nil {
		if url := data.Metadata["deploy_request_url"]; url != "" {
			rows = append(rows, BoxRow{"Deploy Request", url})
		}
	}
	if data.StartedAt != "" {
		if started, err := time.Parse(time.RFC3339, data.StartedAt); err == nil {
			rows = append(rows, BoxRow{"Started", started.Format("Jan 2 15:04:05 MST")})
		}
	}
	dur := formatApplyDuration(data.StartedAt, data.CompletedAt)
	if dur != "-" {
		rows = append(rows, BoxRow{"Duration", dur})
	}
	// Show revert window remaining time. The server provides revert_expires_at
	// in metadata based on its configured revert window duration (default 30min).
	if state.IsState(data.State, state.Apply.RevertWindow) {
		if expiresStr := data.Metadata["revert_expires_at"]; expiresStr != "" {
			if expires, err := time.Parse(time.RFC3339, expiresStr); err == nil {
				remaining := time.Until(expires)
				if remaining > 0 {
					rows = append(rows, BoxRow{"Revert expires in", ui.FormatDurationSeconds(int64(remaining.Seconds()))})
				}
			}
		}
	}

	WriteBox(rows, "State", colorFn)
	writeProgressStep(data)

	// Error below the box. A sharded apply shows it in every state, as the PR
	// comment's failure callout does: a shard or a keyspace finalize can fail
	// while other shards keep running, and nothing else on screen names it.
	// When several shards failed, their reasons grouped stand in for the one
	// error, which names only the first.
	if callout := shardFailureCallout(data); callout != "" {
		fmt.Printf("\n%s", callout)
	} else if (data.State == state.Apply.Failed || data.Sharded) && data.ErrorMessage != "" {
		fmt.Printf("\n  %s%s%s\n", ANSIRed, data.ErrorMessage, ANSIReset)
	}
	writeStoppedByFreeze(data)
	if data.Sharded {
		if note := ShardedStoppedAtCutoverNote(data.State, data.Operations, data.Tables, data.Released); note != "" {
			fmt.Printf("\n  ⏹️ %s\n", note)
		}
	}

	fmt.Println()

	// Filter out empty tables (completed schema changes with no data)
	var activeTables []TableProgress
	for _, t := range data.Tables {
		if t.TableName != "" {
			activeTables = append(activeTables, t)
		}
	}

	// Table progress (sorted: failed and halted first, then active, sharded before unsharded, finished last)
	// Hide tables during branch setup phases (all tables are Queued, not meaningful)
	if len(activeTables) > 0 && !state.IsSetupPhase(data.State) {
		sort.SliceStable(activeTables, func(i, j int) bool {
			pi := ui.TableStatePriority(state.NormalizeTaskStatus(activeTables[i].Status))
			pj := ui.TableStatePriority(state.NormalizeTaskStatus(activeTables[j].Status))
			if pi != pj {
				return pi < pj
			}
			// Within the same priority, sharded tables (have shards) sort first
			si := len(activeTables[i].Shards) > 0
			sj := len(activeTables[j].Shards) > 0
			if si != sj {
				return si
			}
			return false
		})

		// Show keyspace headers for Vitess tables (any table with a namespace)
		hasNamespaces := false
		for _, t := range activeTables {
			if t.Namespace != "" {
				hasNamespaces = true
				break
			}
		}

		if hasNamespaces {
			fmt.Print(FormatNamespacedTables(activeTables))
		} else {
			fmt.Println()
			for _, t := range activeTables {
				fmt.Print(FormatTableProgress(t))
			}
		}
	}

	fmt.Print(FormatThrottleReference(data.Tables))

	// Surface per-keyspace VSchema application status (and diff) from the engine's
	// display metadata, rather than from a synthetic task in the table list.
	if changes, err := apitypes.ParseVSchemaChanges(data.Metadata); err != nil {
		slog.Warn("failed to parse VSchema changes from progress metadata", "error", err)
	} else if vs := FormatVSchemaStatus(changes); vs != "" {
		fmt.Print(vs)
	}

	// Show deploy request info for deferred deploys
	if data.State == state.Apply.WaitingForDeploy {
		fmt.Println()
		if url := data.Metadata["deploy_request_url"]; url != "" {
			fmt.Printf("Deploy request created: %s\n", url)
		}
		if data.Metadata["is_instant"] == "true" {
			fmt.Println("⚡ This change will be applied using instant mode.")
		}
		fmt.Println()
		fmt.Println("Press Enter to deploy or proceed via the PlanetScale console (ESC to detach)")
	}

	// Show remediation guidance for failed applies
	if data.State == state.Apply.Failed {
		writeFailureGuidance()
	}
	// A shard that failed while others still run can be retried once they
	// settle. Watch offers the stop key, so no stop command is printed here.
	if data.Sharded && ShardFailedWhileRunning(data.State, data.Operations, data.Tables, data.Released) {
		fmt.Printf("\n%s\n", presentation.RetryOnceSettledNote)
	}
}

// shardFailureCallout is the grouped failure callout for a sharded apply, or
// "" when the apply is not sharded (ShardFailureCallout).
func shardFailureCallout(data ProgressData) string {
	if !data.Sharded {
		return ""
	}
	return ShardFailureCallout(data.State, data.Operations, data.Tables, data.Released)
}

func writeProgressStep(data ProgressData) {
	if !state.IsRunningApplyState(data.State) || data.Step <= 0 || data.StepsTotal <= 0 {
		return
	}
	fmt.Printf("\nstep %d of %d", data.Step, data.StepsTotal)
	if statement := ui.ClampStatement(data.Statement); statement != "" {
		fmt.Printf(" · %s", statement)
	}
	fmt.Println()
}

// FormatNamespacedTables returns tables grouped by keyspace as a string, collapsing
// keyspaces where all tables share the same terminal status.
// This prevents a wall of "Complete" lines for 30+ unsharded keyspaces.
func FormatNamespacedTables(tables []TableProgress) string {
	return FormatNamespacedTablesWithActivityBar(tables, ui.ProgressBarActivity())
}

// FormatNamespacedTablesWithActivityBar returns tables grouped by keyspace using
// the provided activity bar when row-copy progress has exceeded its estimate.
func FormatNamespacedTablesWithActivityBar(tables []TableProgress, activityBar string) string {
	return FormatNamespacedTablesWithActivity(tables, activityBar, "Finalizing copy")
}

// FormatNamespacedTablesWithActivity returns tables grouped by keyspace using
// the provided activity bar and label when row-copy progress has exceeded its
// estimate.
func FormatNamespacedTablesWithActivity(tables []TableProgress, activityBar, activityLabel string) string {
	type nsEntry struct {
		namespace string
		tables    []TableProgress
	}

	// Group tables by namespace, preserving order of first appearance.
	var ordered []nsEntry
	nsIndex := make(map[string]int)
	for _, t := range tables {
		ns := t.Namespace
		if ns == "" {
			ns = "(default)"
		}
		if idx, ok := nsIndex[ns]; ok {
			ordered[idx].tables = append(ordered[idx].tables, t)
		} else {
			nsIndex[ns] = len(ordered)
			ordered = append(ordered, nsEntry{namespace: ns, tables: []TableProgress{t}})
		}
	}

	// Collapse consecutive terminal keyspaces with identical single-table status.
	type renderGroup struct {
		namespaces []string
		tables     []TableProgress
		collapsed  bool
	}
	var groups []renderGroup
	for _, entry := range ordered {
		canCollapse := len(entry.tables) == 1 &&
			state.IsTerminalApplyState(entry.tables[0].Status) &&
			len(entry.tables[0].Shards) == 0

		// Try to merge with previous group
		if canCollapse && len(groups) > 0 {
			prev := &groups[len(groups)-1]
			if prev.collapsed && len(prev.tables) == 1 &&
				prev.tables[0].TableName == entry.tables[0].TableName &&
				prev.tables[0].Status == entry.tables[0].Status {
				prev.namespaces = append(prev.namespaces, entry.namespace)
				continue
			}
		}

		groups = append(groups, renderGroup{
			namespaces: []string{entry.namespace},
			tables:     entry.tables,
			collapsed:  canCollapse,
		})
	}

	var b strings.Builder
	for _, g := range groups {
		if g.collapsed && len(g.namespaces) > 1 {
			const maxShown = 5
			for i, ns := range g.namespaces {
				if i >= maxShown {
					fmt.Fprintf(&b, "\n  %s... and %d more keyspaces (all %s)%s\n",
						ANSIDim, len(g.namespaces)-maxShown, g.tables[0].Status, ANSIReset)
					break
				}
				b.WriteString(FormatKeyspaceHeader(ns))
				b.WriteString(FormatTableProgressWithActivity(g.tables[0], activityBar, activityLabel))
			}
		} else {
			b.WriteString(FormatKeyspaceHeader(g.namespaces[0]))
			for i, t := range g.tables {
				// A blank line sets every table after the first apart from
				// the one before it, unless that one already ended on a blank
				// line, as a table whose shards or targets are listed does.
				if i > 0 && !strings.HasSuffix(b.String(), "\n\n") {
					b.WriteString("\n")
				}
				b.WriteString(FormatTableProgressWithActivity(t, activityBar, activityLabel))
			}
		}
	}
	return b.String()
}

// FormatVSchemaStatus renders the VSchema-application status and diff of each
// keyspace surfaced on a progress response's display metadata that carries a
// diff to show. Returns empty when no change does. A keyspace's diff is a
// VSchema diff (not SQL), rendered with diff coloring via colorizeDiffLine.
func FormatVSchemaStatus(changes []apitypes.VSchemaChange) string {
	var b strings.Builder
	for _, c := range changes {
		if c.Diff == "" {
			continue
		}
		fmt.Fprintf(&b, "    ~ VSchema (%s): %s\n", c.Namespace, ui.VSchemaStatusLabel(c.Status))
		b.WriteString(FormatVSchemaDiff(c.Diff, indentContent))
	}
	if b.Len() == 0 {
		return ""
	}
	b.WriteString("\n")
	return b.String()
}

// FormatVSchemaDiff returns a VSchema diff with colorized +/- lines as a string,
// stripping ---/+++/@@ headers. Shared between plan and progress views.
func FormatVSchemaDiff(diff, indent string) string {
	var b strings.Builder
	for line := range strings.SplitSeq(strings.TrimRight(diff, "\n"), "\n") {
		if strings.HasPrefix(line, "---") || strings.HasPrefix(line, "+++") || strings.HasPrefix(line, "@@") {
			continue
		}
		fmt.Fprintf(&b, "%s%s\n", indent, colorizeDiffLine(line))
	}
	return b.String()
}

// writeFailureGuidance prints remediation instructions for failed applies.
func writeFailureGuidance() {
	fmt.Println()
	fmt.Printf("%sTo recover:%s Fix the issue above, then run a new apply.\n", ANSIBold, ANSIReset)
	fmt.Printf("The new apply will only process tables that haven't completed.\n")
}

// FormatProgressState formats the state for display with color.
// Accepts any format (proto, uppercase, or canonical lowercase) — normalizes first.
func FormatProgressState(s string) string {
	s = state.NormalizeState(s)
	switch s {
	case state.NoActiveChange:
		return "No active schema change"
	case state.Apply.Pending:
		return "⏳ Queued"
	case state.Apply.PreparingBranch:
		return ANSICyan + "🔄 Preparing branch..." + ANSIReset
	case state.Apply.ApplyingBranchChanges:
		return ANSICyan + "🔄 Applying changes to branch..." + ANSIReset
	case state.Apply.ValidatingBranch:
		return ANSICyan + "🔄 Validating branch schema..." + ANSIReset
	case state.Apply.CreatingDeployRequest:
		return ANSICyan + "🔄 Creating deploy request..." + ANSIReset
	case state.Apply.ValidatingDeployRequest:
		return ANSICyan + "🔄 Validating deploy request..." + ANSIReset
	case "idle":
		return "Idle"
	case state.Apply.Running:
		return ANSICyan + "🔄 Running" + ANSIReset
	case state.Apply.RunningDegraded:
		return ANSICyan + "🔄 Running (degraded)" + ANSIReset
	case state.Apply.CatchingUp:
		return ANSICyan + "⏩ Catching up" + ANSIReset
	case state.Apply.Checksumming:
		return ANSICyan + "🔍 Checksumming" + ANSIReset
	case state.Apply.PostChecksum:
		return ANSICyan + "⏩ Applying final changes" + ANSIReset
	case state.Apply.WaitingForDeploy:
		return ANSIYellow + "🟨 Waiting for deploy" + ANSIReset
	case state.Apply.WaitingForCutover:
		return ANSIYellow + "🟨 Waiting for cutover" + ANSIReset
	case state.Apply.Recovering:
		return ANSIYellow + "🟨 Recovering" + ANSIReset
	case state.Apply.Paused:
		return ANSIYellow + "⏸️  Paused" + ANSIReset
	case state.Apply.Resuming:
		return ANSICyan + "🔄 Resuming..." + ANSIReset
	case state.Apply.CuttingOver:
		return ANSICyan + "🔄 Cutting over..." + ANSIReset
	case state.Apply.Completed:
		return ANSIGreen + "✓ Completed" + ANSIReset
	case state.Apply.FailedRetryable:
		return ANSIYellow + "↻ Retrying" + ANSIReset
	case state.Apply.Failed:
		return ANSIRed + "✗ Failed" + ANSIReset
	case state.Apply.Stopped:
		return ANSIOrange + "⏹️  Stopped" + ANSIReset
	case state.Apply.Cancelled:
		return ANSIOrange + "🚫 Cancelled" + ANSIReset
	case state.Apply.RevertWindow:
		return ANSIYellow + "🟨 Revert window open" + ANSIReset
	case state.Apply.SkippingRevert:
		return ANSICyan + "🔄 Finalizing (closing revert window)..." + ANSIReset
	case state.Apply.Reverting:
		return ANSIYellow + "↩️ Reverting" + ANSIReset
	case state.Apply.Reverted:
		return ANSIOrange + "↩️ Reverted" + ANSIReset
	default:
		return s
	}
}

// FormatTableProgress returns progress for a single table as a string.
// Format: tablename: [progress bar] [status]
//
//	DDL statement (indented below)
//	Rows: X / Y (if applicable)
func FormatTableProgress(t TableProgress) string {
	return FormatTableProgressWithActivityBar(t, ui.ProgressBarActivity())
}

// FormatTableProgressWithActivityBar returns progress for a single table using
// the provided activity bar when row-copy progress has exceeded its estimate.
func FormatTableProgressWithActivityBar(t TableProgress, activityBar string) string {
	return FormatTableProgressWithActivity(t, activityBar, "Finalizing copy")
}

// runningWord names t's parts while they work on its change: copying, unless
// the change copies no rows (presentation.ChangeCopiesRows). The table's own
// line says "Applying..." for those, and its parts read the same way.
func runningWord(t TableProgress) string {
	return presentation.RunningWord(presentation.ChangeCopiesRows(t.ChangeType, t.IsInstant))
}

// isInstantAlter reports whether the table should be described as applying
// instantly: the engine flagged it instant and it is an ALTER. Other
// operations may complete without a row copy, but they are not instant DDL,
// so they keep their generic applying labels. ChangeType is populated for
// every stored DDL change (storage rejects a blank operation), so an empty
// value here means an unknown operation, not a missing field.
func isInstantAlter(t TableProgress) bool {
	return t.IsInstant && ddl.OpToStatementType(t.ChangeType) == ddl.StatementAlterTable
}

// writeTableLine writes a table's status line: the table, then its state as
// format renders it. A table rolled up across targets counts its targets in
// the list under it, so the line carries only the table's own state.
func writeTableLine(b *strings.Builder, t TableProgress, format string, args ...any) {
	fmt.Fprintf(b, indentTable+progressSymbol(t.ChangeType)+"%s: ", t.TableName)
	fmt.Fprintf(b, format, args...)
	b.WriteString("\n")
}

// awaitsResume reports whether a table of a resuming apply is one the start
// has yet to pick back up on any of its targets (presentation.AwaitingResume),
// and the coverage its line keeps for the targets that finished or are still
// queued. A rolled-up table reads its targets; any other its own status.
func awaitsResume(t TableProgress) (string, bool) {
	if !t.Resuming {
		return "", false
	}
	statuses := []string{t.Status}
	if len(t.Shards) > 0 {
		statuses = make([]string, len(t.Shards))
		for i, s := range t.Shards {
			statuses[i] = s.Status
		}
	}
	return presentation.AwaitingResume(statuses)
}

// formatResumingTable renders a table awaiting its resume as its state and
// DDL, as the PR comment does, until its targets report how the copy goes on.
func formatResumingTable(t TableProgress, coverage string) string {
	var b strings.Builder
	writeTableLine(&b, t, "%s%s", ANSICyan+"🔄 Resuming..."+ANSIReset, coverage)
	if t.DDL != "" {
		b.WriteString(formatTableDDL(t))
	}
	b.WriteString("\n")
	return b.String()
}

// FormatTableProgressWithActivity returns progress for a single table using the
// provided activity bar and label when row-copy progress has exceeded its
// estimate.
func FormatTableProgressWithActivity(t TableProgress, activityBar, activityLabel string) string {
	if coverage, ok := awaitsResume(t); ok {
		return formatResumingTable(t, coverage)
	}
	if t.AcrossTargets && t.BuildWork.IsConcurrentIndexBuild() {
		return formatIndexBuildAcrossTargets(t)
	}
	if keepsAcrossTargetsBar(t) {
		if out, ok := formatAcrossTargetsProgress(t); ok {
			return out
		}
	}
	if isHaltedAcrossTargets(t) {
		return formatHaltedAcrossTargets(t)
	}
	if isPartlyCompletedAcrossTargets(t) {
		return formatPartlyCompletedAcrossTargets(t)
	}
	if isNotStartedAcrossTargets(t) {
		return formatNotStartedAcrossTargets(t)
	}
	if keepsShardedBar(t) {
		return formatShardedPhaseProgress(t)
	}

	var b strings.Builder

	// Handle special states first - all use format: tablename: [bar] [status]
	switch t.Status {
	case state.Apply.Pending:
		// Pending = queued, not yet started
		writeTableLine(&b, t, "⏳ Queued")
		if t.DDL != "" {
			b.WriteString(formatTableDDL(t))
		}
		writePlannedRows(&b, t)
		b.WriteString("\n")
		b.WriteString(formatTableParts(t))
		return b.String()
	case state.Apply.Completed:
		bar := ui.ProgressBarComplete()
		label := "✓ Complete"
		if isInstantAlter(t) {
			label = "⚡ Applied instantly"
		}
		writeTableLine(&b, t, "%s %s", bar, label)
		if t.DDL != "" {
			b.WriteString(formatTableDDL(t))
		}
		b.WriteString("\n")
		b.WriteString(formatTableParts(t))
		return b.String()
	case state.Task.CatchingUp, state.Task.Checksumming, state.Task.PostChecksum:
		writeInFlightTable(&b, t, activityBar, activityLabel)
		b.WriteString("\n")
		b.WriteString(formatTableParts(t))
		return b.String()
	case state.Task.WaitingForCutover:
		bar := ui.ProgressBarWaitingCutover()
		writeTableLine(&b, t, "%s Waiting for cutover", bar)
		if t.DDL != "" {
			b.WriteString(formatTableDDL(t))
		}
		b.WriteString("\n")
		b.WriteString(formatTableParts(t))
		return b.String()
	case state.Task.WaitingForDeploy:
		writeTableLine(&b, t, "%s", ui.WaitingForDeploy)
		if t.DDL != "" {
			b.WriteString(formatTableDDL(t))
		}
		b.WriteString("\n")
		b.WriteString(formatTableParts(t))
		return b.String()
	case state.Apply.Recovering:
		if ui.RecoveringCopyingRows(t.RowsTotal, t.PercentComplete) {
			pct := ui.RowCopyDisplayPercent(t.PercentComplete, t.RowsCopied)
			bar := ui.ProgressBarRowCopy(pct)
			writeTableLine(&b, t, "%s Row copy in progress (%s%s)", bar,
				ui.FormatRowCopyPercent(t.PercentComplete, t.RowsCopied, t.RowsTotal), presentation.ShardCoverageSuffix(tableFacts(t)))
			if t.DDL != "" {
				b.WriteString(formatTableDDL(t))
			}
			writeStructuredRowsAndETA(&b, t)
			b.WriteString("\n")
			b.WriteString(formatTableParts(t))
			return b.String()
		}
		bar := ui.ProgressBarRowCopy(t.PercentComplete)
		writeTableLine(&b, t, "%s Recovering state...", bar)
		if t.DDL != "" {
			b.WriteString(formatTableDDL(t))
		}
		b.WriteString("\n")
		b.WriteString(formatTableParts(t))
		return b.String()
	case state.Apply.CuttingOver:
		bar := ui.ProgressBarRowCopy(100) // blue — still in progress
		label := "Cutting over..."
		op := ddl.OpToStatementType(t.ChangeType)
		if op == ddl.StatementCreateTable || op == ddl.StatementDropTable {
			label = "Applying..."
		}
		writeTableLine(&b, t, "%s %s", bar, label)
		if t.DDL != "" {
			b.WriteString(formatTableDDL(t))
		}
		b.WriteString("\n")
		b.WriteString(formatTableParts(t))
		return b.String()
	case state.Apply.Failed:
		if isShardedRollup(t) {
			// The shards under the line show how far each got, so the
			// table's line says how far the failure reaches instead of a
			// bar, as the PR comment's does.
			writeTableLine(&b, t, glyph.Failed+" Failed%s", presentation.FailedReach(shardTaskStatuses(t), shardTotal(t)))
		} else {
			bar := ui.ProgressBarFailed(ui.RowCopyDisplayPercent(t.PercentComplete, t.RowsCopied))
			writeTableLine(&b, t, "%s "+glyph.Failed+" Failed", bar)
		}
		if t.DDL != "" {
			b.WriteString(formatTableDDL(t))
		}
		b.WriteString("\n")
		b.WriteString(formatTableParts(t))
		return b.String()
	case state.Apply.FailedRetryable:
		if t.PercentComplete > 0 || t.RowsCopied > 0 {
			retryPercent := ui.RowCopyDisplayPercent(t.PercentComplete, t.RowsCopied)
			bar := ui.ProgressBar(retryPercent, ui.ColorYellow)
			writeTableLine(&b, t, "%s Retrying", bar)
		} else {
			writeTableLine(&b, t, "Retrying")
		}
		if t.DDL != "" {
			b.WriteString(formatTableDDL(t))
		}
		b.WriteString("\n")
		b.WriteString(formatTableParts(t))
		return b.String()
	case state.Apply.RevertWindow:
		bar := ui.ProgressBarWaitingCutover() // yellow — complete but revert available
		writeTableLine(&b, t, "%s Complete (revert window open)", bar)
		if t.DDL != "" {
			b.WriteString(formatTableDDL(t))
		}
		b.WriteString("\n")
		b.WriteString(formatTableParts(t))
		return b.String()
	case state.Apply.SkippingRevert:
		bar := ui.ProgressBarWaitingCutover() // yellow — complete, revert window closing
		writeTableLine(&b, t, "%s ✓ Complete (finalizing)", bar)
		if t.DDL != "" {
			b.WriteString(formatTableDDL(t))
		}
		b.WriteString("\n")
		b.WriteString(formatTableParts(t))
		return b.String()
	case state.Apply.Reverting:
		bar := ui.ProgressBarWaitingCutover() // yellow — undoing the change
		writeTableLine(&b, t, "%s ↩️ Reverting", bar)
		if t.DDL != "" {
			b.WriteString(formatTableDDL(t))
		}
		b.WriteString("\n")
		b.WriteString(formatTableParts(t))
		return b.String()
	case state.Apply.Reverted:
		// The change was applied, then undone at operator request — a
		// successful revert, not a failure. Full orange bar: terminal,
		// change not in effect.
		bar := ui.ProgressBar(100, ui.ColorOrange)
		writeTableLine(&b, t, "%s ↩️ Reverted", bar)
		if t.DDL != "" {
			b.WriteString(formatTableDDL(t))
		}
		b.WriteString("\n")
		b.WriteString(formatTableParts(t))
		return b.String()
	case state.Apply.Cancelled:
		if t.PercentComplete > 0 || t.RowsCopied > 0 {
			cancelledPercent := ui.RowCopyDisplayPercent(t.PercentComplete, t.RowsCopied)
			bar := ui.ProgressBar(cancelledPercent, ui.ColorOrange)
			writeTableLine(&b, t, "%s 🚫 Cancelled at %s%s", bar,
				ui.FormatRowCopyPercent(t.PercentComplete, t.RowsCopied, t.RowsTotal), presentation.ShardCoverageSuffix(tableFacts(t)))
		} else {
			writeTableLine(&b, t, "🚫 Cancelled (not started)")
		}
		if t.DDL != "" {
			b.WriteString(formatTableDDL(t))
		}
		b.WriteString("\n")
		b.WriteString(formatTableParts(t))
		return b.String()
	case state.Apply.Stopped:
		// Show orange progress bar with current progress when stopped
		stoppedPercent := ui.RowCopyDisplayPercent(t.PercentComplete, t.RowsCopied)
		bar := ui.ProgressBarStopped(stoppedPercent)
		switch {
		case t.PercentComplete >= 100:
			// At 100% = was waiting for cutover when stopped
			writeTableLine(&b, t, "%s ⏹️ Stopped (was waiting for cutover)", bar)
		case t.PercentComplete > 0 || t.RowsCopied > 0:
			writeTableLine(&b, t, "%s ⏹️ Stopped at %s%s", bar,
				ui.FormatRowCopyPercent(t.PercentComplete, t.RowsCopied, t.RowsTotal), presentation.ShardCoverageSuffix(tableFacts(t)))
		default:
			writeTableLine(&b, t, "⏹️ Stopped (not started)")
		}
		if t.DDL != "" {
			b.WriteString(formatTableDDL(t))
		}
		if ui.CopyFinishedShortOfEstimate(t.PercentComplete, t.RowsCopied, t.RowsTotal) {
			// A copy finished short of its estimate shows the rows it copied,
			// as a running one does.
			if line := presentation.FinishedCopyRowsText(t.RowsCopied); line != "" {
				b.WriteString(indentDetail + line + "\n")
			}
		} else if t.RowsTotal > 0 && (t.PercentComplete > 0 || t.RowsCopied > 0) {
			// A stopped copy is not advancing, so its rows line carries no ETA.
			f := tableFacts(t)
			f.ETASeconds = 0
			b.WriteString(indentDetail + presentation.RowsLine(f) + "\n")
		}
		b.WriteString("\n")
		b.WriteString(formatTableParts(t))
		return b.String()
	}

	// Copying, building, or applying: the reading every surface shares.
	writeInFlightTable(&b, t, activityBar, activityLabel)
	if len(t.Shards) == 0 {
		b.WriteString("\n")
	}
	b.WriteString(formatTableParts(t))
	return b.String()
}

// FormatThrottleReference renders one shared reference for a progress view.
// Table-specific throttle reasons remain beside each affected table.
func FormatThrottleReference(tables []TableProgress) string {
	for _, table := range tables {
		if !table.Throttled || ui.ThrottleTip(table.ThrottleReason) == "" {
			continue
		}
		if !state.IsState(table.Status, state.Task.Running, state.Task.Checksumming) {
			continue
		}
		return "  " + DocsLine(ui.MySQLThrottleDocURL) + "\n\n"
	}
	return ""
}

// DocsLine points a terminal surface at one documentation page, the way a PR
// comment's docs line does: the docs glyph and label at full weight, then the
// page's short reference in link blue as the hyperlink text. Where hyperlinks
// are unavailable the full URL takes the reference's place, so the line reads
// the same and the address is never lost.
func DocsLine(url string) string {
	return glyph.Docs + " Docs: " + ANSIBlue + ui.Link(ui.DocRef(url), url) + ANSIReset
}

// writeInFlightTable writes a table the engine is working on as every surface
// reads it (presentation.ReadInFlightTable): the bar and label beside the
// table name, its statement, then the detail lines. A copy that has passed its
// row estimate takes the caller's activity bar and label, which the watch view
// animates, followed by the shards the copy covers.
func writeInFlightTable(b *strings.Builder, t TableProgress, activityBar, activityLabel string) {
	f := tableFacts(t)
	r := presentation.ReadInFlightTable(f)
	headline := r.Headline()
	if r.Activity {
		headline = activityBar + " " + activityLabel + presentation.ShardCoverageSuffix(f) + ui.ThrottledSuffix(r.Throttled)
	}
	writeTableLine(b, t, "%s", headline)
	if t.DDL != "" {
		b.WriteString(formatTableDDL(t))
	}
	for _, d := range r.Details {
		writeTableDetail(b, d)
	}
}

// tableFacts is what the CLI knows about a table's change, in the form every
// surface reads it from. A sharded table whose row figures cover only the
// shards that have reported says how many.
func tableFacts(t TableProgress) presentation.TableFacts {
	f := presentation.TableFacts{
		Status:              t.Status,
		ChangeType:          t.ChangeType,
		IsInstant:           t.IsInstant,
		PercentComplete:     t.PercentComplete,
		RowsCopied:          t.RowsCopied,
		RowsTotal:           t.RowsTotal,
		RowsTotalPlanned:    t.RowsTotalPlanned,
		ETASeconds:          t.ETASeconds,
		EstimatedRows:       t.EstimatedRows,
		EstimatedBytes:      t.EstimatedBytes,
		ChecksumRowsChecked: t.ChecksumRowsChecked,
		ChecksumRowsTotal:   t.ChecksumRowsTotal,
		Throttled:           t.Throttled,
		ThrottleReason:      t.ThrottleReason,
		BuildWork:           t.BuildWork,
		PlannedShards:       t.PlannedShards,
	}
	if t.AcrossTargets {
		return f
	}
	return f.WithShardCoverage(len(t.Shards), shardsReporting(t))
}

// writeTableDetail writes one detail line under a table's statement. A note
// is dimmed after the info glyph; its docs page is named once for the whole
// view (FormatThrottleReference) rather than on every line.
func writeTableDetail(b *strings.Builder, d presentation.TableDetail) {
	if d.Note {
		b.WriteString(indentDetail + ANSIDim + d.NoteText() + ANSIReset + "\n")
		return
	}
	b.WriteString(indentDetail + d.Text + "\n")
}

// writeThrottleTooltip explains the header's "(throttled)" annotation with the
// engine's reason (presentation.ThrottleNote).
func writeThrottleTooltip(b *strings.Builder, t TableProgress) {
	if note, ok := presentation.ThrottleNote(tableFacts(t)); ok {
		writeTableDetail(b, note)
	}
}

// writeStructuredRowsAndETA writes a copy's rows line (presentation.RowsLine).
func writeStructuredRowsAndETA(b *strings.Builder, t TableProgress) {
	b.WriteString(indentDetail + presentation.RowsLine(tableFacts(t)) + "\n")
}

// writePlannedRows writes the rows line of a queued table
// (presentation.PlannedRowsText). A table rolled up across targets names them,
// since its estimate is their sum. It writes the size alone when the plan
// sized the table but could not count its rows, and nothing when the plan had
// neither. A table rolled up across targets has neither while some of its
// targets have reported nothing at all: their estimates are not known, so a
// sum of the rest would understate the table (tableAcrossTargets).
func writePlannedRows(b *strings.Builder, t TableProgress) {
	span := ""
	if t.AcrossTargets {
		span = presentation.PlannedRowsSpan(len(t.Shards))
	}
	if text := presentation.PlannedRowsText(t.EstimatedRows, t.EstimatedBytes, span); text != "" {
		b.WriteString(indentDetail + text + "\n")
	}
}

// shardsReporting is how many of a sharded table's shards have reported a
// row total.
func shardsReporting(t TableProgress) int {
	reporting := 0
	for _, sh := range t.Shards {
		if sh.RowsTotal > 0 {
			reporting++
		}
	}
	return reporting
}

// shardTotal is how many shards a sharded table's copy spans: the plan's
// count, or the shards listed when more are listed than the plan recorded or
// it recorded none, as the PR comment counts them.
func shardTotal(t TableProgress) int {
	return max(len(t.Shards), t.PlannedShards)
}

// writeTableProgress writes progress for a single table to stdout.
// StopData contains data for rendering stop command output.
type StopData struct {
	Database       string
	Environment    string
	ApplyID        string
	StoppedCount   int
	SkippedCount   int
	ProgressBefore int // Progress percentage before stop
	// StartWithdrawn reports that the schema change was still stopped and the
	// stop withdrew a start that had not taken effect, so no table was stopped.
	StartWithdrawn bool
}

// WriteStopSuccess writes the stop command success output.
func WriteStopSuccess(data StopData) {
	fmt.Printf("%s%s⏹️  Schema change stopped%s\n", ANSIBold, ANSIYellow, ANSIReset)
	fmt.Println()
	fmt.Printf("Database:    %s\n", data.Database)
	fmt.Printf("Environment: %s\n", data.Environment)
	if data.StartWithdrawn {
		fmt.Println()
		fmt.Println("The start requested earlier had not taken effect yet, so this stop withdrew it.")
	} else {
		if data.StoppedCount > 0 {
			fmt.Printf("Stopped:     %d table(s)\n", data.StoppedCount)
		}
		if data.SkippedCount > 0 {
			fmt.Printf("Skipped:     %d table(s) (already complete)\n", data.SkippedCount)
		}
	}
	fmt.Println()
	if data.ApplyID != "" {
		fmt.Printf("%sCheckpoint saved.%s\n", ANSIDim, ANSIReset)
		fmt.Print(FormatNextCommand(ResumeLabel, ControlCommand("start", data.ApplyID, data.Environment)))
	} else {
		fmt.Printf("%sCheckpoint saved. Use '%s' to resume from where you left off.%s\n", ANSIDim, cliname.Command("start"), ANSIReset)
	}
}

// CancelData contains data for rendering cancel command output.
type CancelData struct {
	Database       string
	Environment    string
	CancelledCount int
	SkippedCount   int
}

// WriteCancelSuccess writes the cancel command success output.
func WriteCancelSuccess(data CancelData) {
	fmt.Printf("%s%s✖ Schema change cancelled%s\n", ANSIBold, ANSIOrange, ANSIReset)
	fmt.Println()
	fmt.Printf("Database:    %s\n", data.Database)
	fmt.Printf("Environment: %s\n", data.Environment)
	if data.CancelledCount > 0 {
		fmt.Printf("Cancelled:   %d table(s)\n", data.CancelledCount)
	}
	if data.SkippedCount > 0 {
		fmt.Printf("Skipped:     %d table(s) (already terminal)\n", data.SkippedCount)
	}
	fmt.Println()
	fmt.Printf("%sThis schema change cannot be resumed.%s\n", ANSIDim, ANSIReset)
}

// CancelPendingData contains data for rendering a cancel the server recorded
// but the apply has not acted on yet.
type CancelPendingData struct {
	Database       string
	Environment    string
	ApplyID        string
	CancelledCount int
	// LookupErr is why the CLI could not read the apply's state while waiting,
	// or nil when the apply was still active at the deadline.
	LookupErr error
}

// WriteCancelPending writes the output of a cancel that is recorded but has
// not ended the schema change yet. The apply keeps running until a driver
// consumes the request, and a new apply is refused until it does, so the
// output must not read as done.
func WriteCancelPending(data CancelPendingData) {
	fmt.Printf("%s%s⏳ Cancel requested%s\n", ANSIBold, ANSIYellow, ANSIReset)
	fmt.Println()
	fmt.Printf("Database:    %s\n", data.Database)
	fmt.Printf("Environment: %s\n", data.Environment)
	if data.CancelledCount > 0 {
		fmt.Printf("Cancelling:  %d table(s)\n", data.CancelledCount)
	}
	fmt.Println()
	if data.LookupErr != nil {
		fmt.Printf("The cancel is recorded, but its state could not be read: %v\n", data.LookupErr)
	} else {
		fmt.Println("The cancel is recorded. The schema change is still running and ends when SchemaBot next picks it up.")
	}
	fmt.Println("A new apply is refused until it ends.")
	fmt.Println()
	fmt.Print(FormatNextCommand("To watch it end", ControlCommand("progress", data.ApplyID, data.Environment)))
}

// CancelDeclinedData contains data for rendering a cancel the schema change
// declined.
type CancelDeclinedData struct {
	Database    string
	Environment string
	ApplyID     string
	// Reason is the engine's reason the cancel did not take effect.
	Reason string
}

// WriteCancelDeclined writes the output of a cancel the schema change
// declined, such as a step that cannot be interrupted. The change keeps
// running to its own end, so the output says the cancel did nothing.
func WriteCancelDeclined(data CancelDeclinedData) {
	fmt.Printf("%s%s%s Cancel did not take effect%s\n", ANSIBold, ANSIYellow, glyph.Refused, ANSIReset)
	fmt.Println()
	fmt.Printf("Database:    %s\n", data.Database)
	fmt.Printf("Environment: %s\n", data.Environment)
	fmt.Println()
	if data.Reason != "" {
		fmt.Println(data.Reason)
	}
	fmt.Println("The schema change keeps running and finishes on its own.")
	fmt.Println()
	fmt.Print(FormatNextCommand("To watch it finish", ControlCommand("progress", data.ApplyID, data.Environment)))
}

// CancelOvertakenData contains data for rendering a cancel the schema change
// finished before acting on.
type CancelOvertakenData struct {
	Database    string
	Environment string
	ApplyID     string
	State       string
}

// WriteCancelOvertaken writes the output of a cancel that arrived after the
// schema change had already reached another terminal state.
func WriteCancelOvertaken(data CancelOvertakenData) {
	fmt.Printf("%sSchema change ended as %s before the cancel took effect%s\n", ANSIBold, data.State, ANSIReset)
	fmt.Println()
	fmt.Printf("Database:    %s\n", data.Database)
	fmt.Printf("Environment: %s\n", data.Environment)
	fmt.Println()
	fmt.Print(FormatNextCommand("To see how it ended", ControlCommand("progress", data.ApplyID, data.Environment)))
}

// StartData contains data for rendering start command output.
type StartData struct {
	Database     string
	Environment  string
	ApplyID      string
	StartedCount int
	SkippedCount int
}

// WriteStartSuccess writes the start command success output.
func WriteStartSuccess(data StartData) {
	fmt.Printf("%s%s▶️  Schema change resumed%s\n", ANSIBold, ANSIGreen, ANSIReset)
	fmt.Println()
	fmt.Printf("Database:    %s\n", data.Database)
	fmt.Printf("Environment: %s\n", data.Environment)
	if data.StartedCount > 0 {
		fmt.Printf("Resumed:     %d table(s)\n", data.StartedCount)
	}
	if data.SkippedCount > 0 {
		fmt.Printf("Skipped:     %d table(s) (already complete)\n", data.SkippedCount)
	}
	fmt.Println()
	fmt.Printf("%sResuming from checkpoint...%s\n", ANSIDim, ANSIReset)
}

// ProgressCommand is the command that watches an apply on the server and in
// the environment this run addressed. A wrapper may route an omitted
// environment to a default one, so the environment is named whenever this run
// knew it, along with the routing flags the operator passed.
func ProgressCommand(applyID, environment string) string {
	if environment == "" {
		return cliname.Command("progress", applyID)
	}
	return cliname.Command("progress", applyID, "-e", environment)
}

// WriteStartNoWatch writes the start command output when --watch=false.
func WriteStartNoWatch(applyID, database, environment string) {
	fmt.Printf("%s%s▶️  Schema change resumed%s\n", ANSIBold, ANSIGreen, ANSIReset)
	fmt.Println()
	if applyID != "" {
		fmt.Printf("To watch and manage: %s\n", ProgressCommand(applyID, environment))
	} else {
		fmt.Printf("To watch and manage: %s\n", cliname.Command("status", "-d", database, "-e", environment))
	}
}

// ReleaseData contains data for rendering release command output.
type ReleaseData struct {
	Database    string
	Environment string
	ApplyID     string
}

// WriteReleaseSuccess writes the release command success output.
func WriteReleaseSuccess(data ReleaseData) {
	fmt.Printf("%s%s▶️  Paused rollout released%s\n", ANSIBold, ANSIGreen, ANSIReset)
	fmt.Println()
	fmt.Printf("Database:    %s\n", data.Database)
	fmt.Printf("Environment: %s\n", data.Environment)
	fmt.Println()
	if data.ApplyID != "" {
		fmt.Printf("%sHeld deployments will resume. Use '%s' to follow them.%s\n", ANSIDim, ProgressCommand(data.ApplyID, data.Environment), ANSIReset)
	} else {
		fmt.Printf("%sHeld deployments will resume.%s\n", ANSIDim, ANSIReset)
	}
}

// ActiveApplyData contains data for a single apply in the status list.
type ActiveApplyData struct {
	ApplyID             string
	ExternalID          string
	ExternalOperationID string
	Database            string
	Environment         string
	Deployment          string
	State               string
	Engine              string
	Caller              string
	ErrorMessage        string
	StartedAt           string
	CompletedAt         string
	UpdatedAt           string
}

// StatusListData contains data for rendering the status list.
type StatusListData struct {
	ActiveCount    int
	Limit          int
	MaxLimit       int
	HasMore        bool
	FailuresOnly   bool
	Last           string
	StateFilter    string
	StateCounts    map[string]int
	ShowExternalID bool
	Deployment     string
	// After is the --after apply this page continues from; empty on a first
	// page.
	After   string
	Applies []ActiveApplyData
	// Sectioned renders the default view: Active above the finished applies
	// in Applies, which alone are paged by --limit and --after. Active is
	// listed in full on a first page and omitted from a continued one.
	Sectioned bool
	Active    []ActiveApplyData
	// ActiveHasMore reports more active applies than the server returns in
	// one request.
	ActiveHasMore bool
}

// statusWindowSuffix renders the active list filters (--state, --last) as a
// header suffix, or empty when the list is unfiltered.
func statusWindowSuffix(data StatusListData) string {
	parts := statusFilterPhrases(data)
	if len(parts) == 0 {
		return ""
	}
	return fmt.Sprintf(" %s(%s)%s", ANSIDim, strings.Join(parts, ", "), ANSIReset)
}

// statusFilterPhrases returns one human-readable phrase per active list
// filter, shared by the header suffix and the empty-state message.
func statusFilterPhrases(data StatusListData) []string {
	var parts []string
	if data.StateFilter != "" {
		parts = append(parts, fmt.Sprintf("in state %s", state.Label(data.StateFilter)))
	}
	if data.Last != "" {
		parts = append(parts, fmt.Sprintf("updated in the last %s", data.Last))
	}
	return parts
}

// writeStatusStateSummary renders one line tallying every apply matching the
// request's filters by state. The counts come from the server unbounded by
// limit, so the line stays truthful when the table below it is a truncated
// page. Ordered by count descending (ties by state name) so the dominant
// outcome reads first.
func writeStatusStateSummary(counts map[string]int) {
	if len(counts) == 0 {
		return
	}
	states := make([]string, 0, len(counts))
	total := 0
	for s, n := range counts {
		states = append(states, s)
		total += n
	}
	sort.Slice(states, func(i, j int) bool {
		if counts[states[i]] != counts[states[j]] {
			return counts[states[i]] > counts[states[j]]
		}
		return states[i] < states[j]
	})
	parts := make([]string, 0, len(states))
	for _, s := range states {
		parts = append(parts, fmt.Sprintf("%d %s", counts[s], state.Label(s)))
	}
	fmt.Printf("%s%d total: %s%s\n", ANSIDim, total, strings.Join(parts, " · "), ANSIReset)
}

// WriteStatusList writes the status list output.
func WriteStatusList(data StatusListData) {
	if data.Sectioned {
		writeSectionedStatusList(data)
		return
	}
	if len(data.Applies) == 0 {
		writeEmptyStatusList(data)
		return
	}
	if data.FailuresOnly {
		writeFailedStatusList(data)
		return
	}

	// Header
	if data.ActiveCount > 0 {
		writeActiveStatusHeader(data, data.ActiveCount)
	} else {
		fmt.Printf("%sRecent schema changes%s%s\n", ANSIBold, ANSIReset, statusWindowSuffix(data))
	}
	writeStatusStateSummary(data.StateCounts)
	fmt.Println()

	writeStatusTable(statusListColumns(data, data.Applies), data.Applies, func(a ActiveApplyData) string { return a.State })

	writeStatusListFooter(data)
}

// writeSectionedStatusList writes the default status view: every active apply,
// then the finished ones a page at a time. The two come from separate requests
// over an order that never changes, so paging the finished applies cannot skip
// or repeat one whose state moves between pages, and active work is always on
// screen however many applies finished since it started.
func writeSectionedStatusList(data StatusListData) {
	if len(data.Active) == 0 && len(data.Applies) == 0 {
		writeEmptyStatusList(data)
		return
	}
	if data.After == "" {
		if len(data.Active) == 0 {
			fmt.Printf("%sNo active schema changes%s%s\n", ANSIDim, ANSIReset, statusWindowSuffix(data))
		} else {
			writeActiveStatusHeader(data, len(data.Active))
			fmt.Println()
			writeStatusTable(statusListColumns(data, data.Active), data.Active, func(a ActiveApplyData) string { return a.State })
			if data.ActiveHasMore {
				fmt.Printf("\n%sShowing the %d most recent active schema changes.%s\n", ANSIDim, len(data.Active), ANSIReset)
			}
		}
		fmt.Println()
	}

	if len(data.Applies) == 0 {
		fmt.Printf("%sNo recent finished schema changes%s%s\n", ANSIDim, ANSIReset, statusWindowSuffix(data))
		writeStatusListFooter(data)
		return
	}
	fmt.Printf("%sRecent schema changes%s%s\n", ANSIBold, ANSIReset, statusWindowSuffix(data))
	writeStatusStateSummary(data.StateCounts)
	fmt.Println()
	writeStatusTable(statusListColumns(data, data.Applies), data.Applies, func(a ActiveApplyData) string { return a.State })
	writeStatusListFooter(data)
}

func writeActiveStatusHeader(data StatusListData, count int) {
	if count == 1 {
		fmt.Printf("%s1 active schema change%s%s\n", ANSIBold, ANSIReset, statusWindowSuffix(data))
		return
	}
	fmt.Printf("%s%d active schema changes%s%s\n", ANSIBold, count, ANSIReset, statusWindowSuffix(data))
}

func writeEmptyStatusList(data StatusListData) {
	filters := ""
	if parts := statusFilterPhrases(data); len(parts) > 0 {
		filters = " " + strings.Join(parts, ", ")
	}
	if data.After != "" {
		item := "schema changes"
		if data.FailuresOnly {
			item = "failed schema changes"
		}
		fmt.Printf("%sNo %s older than %s%s%s\n", ANSIDim, item, data.After, filters, ANSIReset)
		return
	}
	if data.FailuresOnly {
		fmt.Printf("%sNo recent failed schema changes%s%s\n", ANSIDim, filters, ANSIReset)
	} else {
		fmt.Printf("%sNo recent schema changes%s%s\n", ANSIDim, filters, ANSIReset)
	}
}

// writeStatusListTruncation says how much of the history a truncated page holds
// and how to see more of it. Below the server's cap a larger --limit shows
// more; once the page is at the cap, or the operator is already paging, the
// next page starts --after the last apply this one shows.
func writeStatusListTruncation(data StatusListData, item string) {
	atCap := data.MaxLimit > 0 && data.Limit >= data.MaxLimit
	last := ""
	if len(data.Applies) > 0 {
		last = data.Applies[len(data.Applies)-1].ApplyID
	}
	switch {
	case data.After != "" && last != "":
		fmt.Printf("%sShowing %d more %s. Add --after %s for the next %d.%s\n", ANSIDim, len(data.Applies), item, last, data.Limit, ANSIReset)
	case atCap && last != "":
		fmt.Printf("%sShowing the %d most recent %s. Add --after %s for the next %d.%s\n", ANSIDim, data.Limit, item, last, data.Limit, ANSIReset)
	case atCap:
		fmt.Printf("%sShowing the %d most recent %s. This server caps status history at %d.%s\n", ANSIDim, data.Limit, item, data.MaxLimit, ANSIReset)
	default:
		fmt.Printf("%sShowing the %d most recent %s. Use --limit N to show more.%s\n", ANSIDim, data.Limit, item, ANSIReset)
	}
}

func writeStatusListFooter(data StatusListData) {
	fmt.Println()
	if data.HasMore && data.Limit > 0 {
		item := "schema changes"
		if data.FailuresOnly {
			item = "failed schema changes"
		}
		writeStatusListTruncation(data, item)
	}
	fmt.Printf("%sUse '%s' to view details%s\n", ANSIDim, cliname.Command("status", "<apply_id>"), ANSIReset)
}

func writeFailedStatusList(data StatusListData) {
	fmt.Printf("%sRecent failed schema changes%s%s\n", ANSIBold, ANSIReset, statusWindowSuffix(data))
	writeStatusStateSummary(data.StateCounts)
	fmt.Println()

	for i, a := range data.Applies {
		if i > 0 {
			fmt.Println()
		}
		fmt.Printf("%s %s: %s (%s) [%s] %s\n",
			a.Database,
			a.Environment,
			state.Label(a.State),
			statusFailureActor(a, data.ShowExternalID),
			formatFailureTimestamp(a),
			compactStatusFailureReason(a.ErrorMessage))
		fmt.Println(cliname.Command("status", a.ApplyID))
	}

	if data.HasMore && data.Limit > 0 {
		fmt.Println()
		writeStatusListTruncation(data, "failed schema changes")
	}
}

// statusColumn is one column of a status table. An optional column is dropped
// when no row on the page has a value for it, so an operator only ever sees the
// columns their own fleet populates: a deployment that drives its applies
// locally has no remote handles to show, and an unfiltered list of a
// single-deployment fleet has no deployment to distinguish.
type statusColumn[Row any] struct {
	header   string
	value    func(r Row) string
	optional bool
	colored  bool
	last     bool
}

// writeStatusTable renders the aligned table every status surface shares: a
// dimmed header row, then one indented line per row with each cell padded to
// the column's widest value. rowState names a row's apply state so a colored
// column can wrap its padded cell in that state's color; the separator
// between cells stays outside the escape.
func writeStatusTable[Row any](columns []statusColumn[Row], rows []Row, rowState func(Row) string) {
	widths := statusColumnWidths(columns, rows)

	fmt.Print("  " + ANSIDim)
	for i, column := range columns {
		fmt.Print(statusCell(column.header, widths[i], column.last))
		if !column.last {
			fmt.Print("  ")
		}
	}
	fmt.Println(ANSIReset)

	for _, row := range rows {
		fmt.Print("  ")
		for i, column := range columns {
			cell := statusCell(statusColumnValue(column, row), widths[i], column.last)
			if column.colored {
				if colorFn := stateColorFunc(rowState(row)); colorFn != nil {
					cell = colorFn(cell)
				}
			}
			fmt.Print(cell)
			if !column.last {
				fmt.Print("  ")
			}
		}
		fmt.Println()
	}
}

// statusListColumns returns the columns the list renders, in order. The
// deployment-filtered list names both remote handles the way the detail views
// already do — the deployment's shared data-plane apply id and the
// per-operation remote row id — and omits DEPLOYMENT, which every row repeats
// back to the operator who named it.
func statusListColumns(data StatusListData, rows []ActiveApplyData) []statusColumn[ActiveApplyData] {
	columns := []statusColumn[ActiveApplyData]{
		{header: "APPLY ID", value: func(a ActiveApplyData) string { return a.ApplyID }},
	}
	if data.ShowExternalID {
		if data.Deployment != "" {
			columns = append(columns,
				statusColumn[ActiveApplyData]{header: "EXTERNAL APPLY ID", optional: true, value: func(a ActiveApplyData) string { return a.ExternalID }},
				statusColumn[ActiveApplyData]{header: "EXTERNAL OP ID", optional: true, value: func(a ActiveApplyData) string { return a.ExternalOperationID }},
			)
		} else {
			// Unconditional: the operator asked for this column by flag, so an
			// all-dash column positively answers "nothing recorded" — dropping
			// it would be indistinguishable from the flag doing nothing.
			columns = append(columns,
				statusColumn[ActiveApplyData]{header: "EXTERNAL ID", value: unfilteredStatusExternalID},
			)
		}
	}
	columns = append(columns,
		statusColumn[ActiveApplyData]{header: "DATABASE", value: func(a ActiveApplyData) string { return a.Database }},
		statusColumn[ActiveApplyData]{header: "ENV", value: func(a ActiveApplyData) string { return a.Environment }},
	)
	if data.Deployment == "" {
		columns = append(columns,
			statusColumn[ActiveApplyData]{header: "DEPLOYMENT", optional: true, value: func(a ActiveApplyData) string { return a.Deployment }},
		)
	}
	columns = append(columns,
		statusColumn[ActiveApplyData]{header: "STATE", colored: true, value: func(a ActiveApplyData) string { return state.Label(a.State) }},
		statusColumn[ActiveApplyData]{header: "STARTED", value: func(a ActiveApplyData) string { return formatStartedAt(a.StartedAt) }},
	)
	columns = append(columns,
		statusColumn[ActiveApplyData]{header: "SOURCE", last: true, value: func(a ActiveApplyData) string { return applySource(a.Caller) }},
	)
	return retainPopulatedStatusColumns(columns, rows)
}

// retainPopulatedStatusColumns drops every optional column no row fills in.
func retainPopulatedStatusColumns[Row any](columns []statusColumn[Row], rows []Row) []statusColumn[Row] {
	retained := make([]statusColumn[Row], 0, len(columns))
	for _, column := range columns {
		if column.optional && !anyStatusRowFillsColumn(column, rows) {
			continue
		}
		retained = append(retained, column)
	}
	return retained
}

func anyStatusRowFillsColumn[Row any](column statusColumn[Row], rows []Row) bool {
	for _, row := range rows {
		if column.value(row) != "" {
			return true
		}
	}
	return false
}

// statusColumnValue renders a row's cell, standing a dash in for a value this
// row is missing from a column other rows on the page do fill.
func statusColumnValue[Row any](column statusColumn[Row], row Row) string {
	if value := column.value(row); value != "" {
		return value
	}
	return "-"
}

// statusColumnWidths sizes each column by terminal cells rather than bytes,
// so a multi-byte value — a non-ASCII database name, a state glyph — cannot
// misalign every column to its right.
func statusColumnWidths[Row any](columns []statusColumn[Row], rows []Row) []int {
	widths := make([]int, len(columns))
	for i, column := range columns {
		widths[i] = ui.VisibleWidth(column.header)
		for _, row := range rows {
			widths[i] = maxLen(widths[i], ui.VisibleWidth(statusColumnValue(column, row)))
		}
	}
	return widths
}

// statusCell pads a cell to its column width, leaving the last column ragged so
// the row carries no trailing whitespace.
func statusCell(value string, width int, last bool) string {
	if last {
		return value
	}
	return ui.PadVisible(value, width)
}

// unfilteredStatusExternalID collapses both remote handles into the single
// EXTERNAL ID column an unfiltered list shows, preferring the per-operation row
// id when the apply has one.
func unfilteredStatusExternalID(a ActiveApplyData) string {
	if a.ExternalOperationID != "" {
		return a.ExternalOperationID
	}
	return a.ExternalID
}

func statusFailureActor(a ActiveApplyData, showExternalID bool) string {
	actor := applySource(a.Caller)
	if !showExternalID {
		return actor
	}
	externalID := unfilteredStatusExternalID(a)
	if externalID == "" {
		externalID = "-"
	}
	return actor + "; external_id=" + externalID
}

func formatFailureTimestamp(a ActiveApplyData) string {
	timestamp := a.CompletedAt
	if timestamp == "" {
		timestamp = a.UpdatedAt
	}
	if timestamp == "" {
		timestamp = a.StartedAt
	}
	if timestamp == "" {
		return "-"
	}
	t, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		return timestamp
	}
	return t.UTC().Format("2006-01-02 15:04:05 UTC")
}

func compactStatusFailureReason(reason string) string {
	reason = strings.Join(strings.Fields(reason), " ")
	if reason == "" {
		return "-"
	}
	if len(reason) <= apitypes.StatusFailureReasonWidth {
		return reason
	}
	return reason[:apitypes.StatusFailureReasonKeptWidth] + apitypes.StatusFailureReasonEllipsis
}

// formatStartedAt formats the started_at timestamp for display.
func formatStartedAt(startedAt string) string {
	if startedAt == "" {
		return "-"
	}
	t, err := time.Parse(time.RFC3339, startedAt)
	if err != nil {
		return startedAt
	}
	return ui.FormatTimeAgo(t)
}

// ApplyHistoryData contains data for a single apply in the history.
type ApplyHistoryData struct {
	ApplyID     string
	Environment string
	State       string
	Engine      string
	Caller      string
	StartedAt   string
	CompletedAt string
	Error       string
}

// DatabaseHistoryData contains data for rendering database history.
type DatabaseHistoryData struct {
	Database string
	// Environment is the environment the history was filtered to; empty when
	// it covers every environment.
	Environment string
	Applies     []ApplyHistoryData
}

// databaseHistoryColumns returns the columns the history table renders, in order.
func databaseHistoryColumns() []statusColumn[ApplyHistoryData] {
	return []statusColumn[ApplyHistoryData]{
		{header: "APPLY ID", value: func(a ApplyHistoryData) string { return a.ApplyID }},
		{header: "ENV", value: func(a ApplyHistoryData) string { return a.Environment }},
		{header: "STATE", colored: true, value: func(a ApplyHistoryData) string { return state.Label(a.State) }},
		{header: "STARTED", value: func(a ApplyHistoryData) string { return formatStartedAt(a.StartedAt) }},
		{header: "DURATION", value: func(a ApplyHistoryData) string { return formatApplyDuration(a.StartedAt, a.CompletedAt) }},
		{header: "SOURCE", last: true, value: func(a ApplyHistoryData) string { return applySource(a.Caller) }},
	}
}

// WriteDatabaseHistory writes the database history output.
func WriteDatabaseHistory(data DatabaseHistoryData) {
	if len(data.Applies) == 0 {
		fmt.Printf("%s%s%s\n", ANSIDim, emptyHistoryLine(data.Database, data.Environment), ANSIReset)
		return
	}

	fmt.Printf("%sSchema change history for %s%s\n", ANSIBold, data.Database, ANSIReset)
	fmt.Println()

	writeStatusTable(databaseHistoryColumns(), data.Applies, func(a ApplyHistoryData) string { return a.State })

	fmt.Println()
	fmt.Printf("%sUse '%s' to view details%s\n", ANSIDim, cliname.Command("status", "<apply_id>"), ANSIReset)
}

// emptyHistoryLine says that no apply has been recorded for the database, in
// the environment when the history was filtered to one. It speaks of applies,
// not schema changes, so it does not read as a claim that the database has no
// schema or is unknown.
func emptyHistoryLine(database, environment string) string {
	if environment == "" {
		return fmt.Sprintf("No applies recorded for database '%s'", database)
	}
	return fmt.Sprintf("No applies recorded for database '%s' in %s", database, environment)
}

// stateColorFunc returns an ANSI color function for the given state.
//
// Color semantics: red is reserved for Failed — the only state that means
// something broke and needs remediation. Operator-initiated terminal states
// (Stopped, Cancelled, Reverted) are orange: the change is not in effect,
// but nothing went wrong. Yellow marks states awaiting attention or an
// external event, cyan marks active work, green marks success.
func stateColorFunc(s string) func(string) string {
	switch s {
	case state.Apply.Completed:
		return colorWrap(ANSIGreen)
	case state.Apply.Failed:
		return colorWrap(ANSIRed)
	case state.Apply.FailedRetryable:
		return colorWrap(ANSIYellow)
	case state.Apply.Running, state.Apply.RunningDegraded,
		state.Apply.CatchingUp, state.Apply.Checksumming, state.Apply.PostChecksum:
		return colorWrap(ANSICyan)
	case state.Apply.WaitingForDeploy, state.Apply.WaitingForCutover, state.Apply.Recovering:
		return colorWrap(ANSIYellow)
	case state.Apply.Paused:
		return colorWrap(ANSIYellow) // the rollout holds until an operator releases it
	case state.Apply.Resuming:
		return colorWrap(ANSICyan) // a start is being processed
	case state.Apply.CuttingOver:
		return colorWrap(ANSICyan) // active work, matches the blue "Cutting over..." table bar
	case state.Apply.Stopped:
		return colorWrap(ANSIOrange)
	case state.Apply.Cancelled:
		return colorWrap(ANSIOrange)
	case state.Apply.Pending:
		return colorWrap(ANSIDim)
	case state.Apply.PreparingBranch, state.Apply.ApplyingBranchChanges, state.Apply.ValidatingBranch, state.Apply.CreatingDeployRequest, state.Apply.ValidatingDeployRequest:
		return colorWrap(ANSICyan)
	case state.Apply.Reverted:
		return colorWrap(ANSIOrange)
	case state.Apply.RevertWindow:
		return colorWrap(ANSIYellow)
	case state.Apply.SkippingRevert:
		return colorWrap(ANSICyan)
	case state.Apply.Reverting:
		return colorWrap(ANSIYellow) // matches the yellow "undoing the change" table bar
	default:
		return nil
	}
}

// formatApplyDuration returns a human-readable duration between started and completed.
// For completed applies, shows total duration. For active applies, shows elapsed time.
func formatApplyDuration(startedAt, completedAt string) string {
	if startedAt == "" {
		return "-"
	}
	started, err := time.Parse(time.RFC3339, startedAt)
	if err != nil {
		return "-"
	}
	if completedAt != "" {
		completed, err := time.Parse(time.RFC3339, completedAt)
		if err == nil {
			return ui.FormatHumanDuration(completed.Sub(started))
		}
	}
	return ui.FormatHumanDuration(nowFunc().Sub(started))
}

func maxLen(a, b int) int {
	if b > a {
		return b
	}
	return a
}
