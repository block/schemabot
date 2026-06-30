package templates

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/block/spirit/pkg/statement"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

const maxStatusFailureReasonWidth = 240

// Indentation for progress rendering.
// indentTable is the prefix for table names. Aligns with keyspace name after "── " in headers.
const indentTable = "     " // 5 spaces — matches "  ── " in FormatKeyspaceHeader

// progressSymbol returns a Terraform-style prefix for the change type.
func progressSymbol(changeType string) string {
	switch ddl.OpToStatementType(changeType) {
	case statement.StatementCreateTable:
		return "+ "
	case statement.StatementDropTable:
		return "- "
	default:
		return "~ "
	}
}

// formatProgressDDL renders a DDL statement with syntax highlighting, indented under the table name.
func formatProgressDDL(rawDDL string) string {
	if rawDDL == "" {
		return ""
	}
	return IndentSQL(ddl.FormatDDL(rawDDL), indentContent) + "\n"
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
	if len(data.Operations) > 1 {
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
	if data.Caller != "" {
		rows = append(rows, BoxRow{"Caller", data.Caller})
	}
	if data.PullRequestURL != "" {
		rows = append(rows, BoxRow{"PR", data.PullRequestURL})
	}
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
					rows = append(rows, BoxRow{"Revert expires in", FormatDurationSeconds(int64(remaining.Seconds()))})
				}
			}
		}
	}

	WriteBox(rows, "State", colorFn)

	// Error below the box
	if data.State == state.Apply.Failed && data.ErrorMessage != "" {
		fmt.Printf("\n  %s%s%s\n", ANSIRed, data.ErrorMessage, ANSIReset)
	}

	fmt.Println()

	// Filter out empty tables (completed schema changes with no data)
	var activeTables []TableProgress
	for _, t := range data.Tables {
		if t.TableName != "" {
			activeTables = append(activeTables, t)
		}
	}

	// Table progress (sorted: active first, sharded before unsharded, terminal last)
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
	return FormatNamespacedTablesWithActivity(tables, activityBar, "Active")
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
			for _, t := range g.tables {
				b.WriteString(FormatTableProgressWithActivity(t, activityBar, activityLabel))
			}
		}
	}
	return b.String()
}

// FormatVSchemaStatus renders each keyspace's VSchema-application status and
// diff surfaced on a progress response's display metadata. Returns empty when
// the apply carries no VSchema change. A keyspace's diff is a VSchema diff (not
// SQL), rendered with diff coloring via colorizeDiffLine.
func FormatVSchemaStatus(changes []apitypes.VSchemaChange) string {
	if len(changes) == 0 {
		return ""
	}
	var b strings.Builder
	for _, c := range changes {
		fmt.Fprintf(&b, "    ~ VSchema (%s): %s\n", c.Namespace, ui.VSchemaStatusLabel(c.Status))
		if c.Diff != "" {
			b.WriteString(FormatVSchemaDiff(c.Diff, indentContent))
		}
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
		return "⏳ Starting..."
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
	case state.Apply.WaitingForDeploy:
		return ANSIYellow + "🟨 Waiting for deploy" + ANSIReset
	case state.Apply.WaitingForCutover:
		return ANSIYellow + "🟨 Waiting for cutover" + ANSIReset
	case state.Apply.Recovering:
		return ANSIYellow + "🟨 Recovering" + ANSIReset
	case state.Apply.CuttingOver:
		return ANSICyan + "🔄 Cutting over..." + ANSIReset
	case state.Apply.Completed:
		return ANSIGreen + "✓ Completed" + ANSIReset
	case state.Apply.FailedRetryable:
		return ANSIYellow + "↻ Retrying" + ANSIReset
	case state.Apply.Failed:
		return ANSIRed + "✗ Failed" + ANSIReset
	case state.Apply.Stopped:
		return ANSIYellow + "⏸️  Stopped" + ANSIReset
	case state.Apply.Cancelled:
		return ANSIRed + "🚫 Cancelled" + ANSIReset
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
	return FormatTableProgressWithActivity(t, activityBar, "Active")
}

// FormatTableProgressWithActivity returns progress for a single table using the
// provided activity bar and label when row-copy progress has exceeded its
// estimate.
func FormatTableProgressWithActivity(t TableProgress, activityBar, activityLabel string) string {
	var b strings.Builder

	// Instant DDL: show "Applying instantly" for any non-terminal state.
	if t.IsInstant && !state.IsTerminalApplyState(state.NormalizeTaskStatus(t.Status)) {
		bar := ui.ProgressBarRowCopy(100)
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s Applying instantly...\n", t.TableName, bar)
		if t.DDL != "" {
			b.WriteString(formatProgressDDL(t.DDL))
		}
		b.WriteString("\n")
		return b.String()
	}

	// Handle special states first - all use format: tablename: [bar] [status]
	switch t.Status {
	case state.Apply.Pending:
		// Pending = queued, not yet started
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: ⏳ Queued\n", t.TableName)
		if t.DDL != "" {
			b.WriteString(formatProgressDDL(t.DDL))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Apply.Completed:
		bar := ui.ProgressBarComplete()
		label := "✓ Complete"
		if t.IsInstant {
			label = "⚡ Applied instantly"
		}
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s %s\n", t.TableName, bar, label)
		if t.DDL != "" {
			b.WriteString(formatProgressDDL(t.DDL))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Task.Checksumming:
		// Row copy is done; the engine is verifying the copied data against the
		// source. On a large table this can run for hours, so show how far the
		// verify has progressed once Spirit has reported a total.
		if t.ChecksumRowsTotal > 0 {
			pct := ui.ClampPercent(int(t.ChecksumRowsChecked * 100 / t.ChecksumRowsTotal))
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s 🔍 Checksumming to verify data (%d%%)\n", t.TableName, ui.ProgressBarRowCopy(pct), pct)
			if t.DDL != "" {
				b.WriteString(formatProgressDDL(t.DDL))
			}
			fmt.Fprintf(&b, indentDetail+"Rows verified: %s / %s\n",
				ui.FormatNumber(ui.ClampRows(t.ChecksumRowsChecked, t.ChecksumRowsTotal)), ui.FormatNumber(t.ChecksumRowsTotal))
		} else {
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s 🔍 Checksumming to verify data...\n", t.TableName, ui.ProgressBarRowCopy(100))
			if t.DDL != "" {
				b.WriteString(formatProgressDDL(t.DDL))
			}
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Apply.WaitingForCutover:
		bar := ui.ProgressBarRowCopy(100) // blue — in progress, row copy done
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s Waiting for cutover\n", t.TableName, bar)
		if t.DDL != "" {
			b.WriteString(formatProgressDDL(t.DDL))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Apply.Recovering:
		if recoveringIsCopyingRows(t) {
			pct := ui.RowCopyDisplayPercent(t.PercentComplete, t.RowsCopied)
			bar := ui.ProgressBarRowCopy(pct)
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s Row copy in progress (%d%%)\n", t.TableName, bar, pct)
			if t.DDL != "" {
				b.WriteString(formatProgressDDL(t.DDL))
			}
			writeStructuredRowsAndETA(&b, t)
			b.WriteString("\n")
			b.WriteString(FormatShardProgress(t.Shards))
			return b.String()
		}
		bar := ui.ProgressBarRowCopy(t.PercentComplete)
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s Recovering state...\n", t.TableName, bar)
		if t.DDL != "" {
			b.WriteString(formatProgressDDL(t.DDL))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Apply.CuttingOver:
		bar := ui.ProgressBarRowCopy(100) // blue — still in progress
		label := "Cutting over..."
		op := ddl.OpToStatementType(t.ChangeType)
		if op == statement.StatementCreateTable || op == statement.StatementDropTable {
			label = "Applying..."
		}
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s %s\n", t.TableName, bar, label)
		if t.DDL != "" {
			b.WriteString(formatProgressDDL(t.DDL))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Apply.Failed:
		bar := ui.ProgressBarFailed(ui.RowCopyDisplayPercent(t.PercentComplete, t.RowsCopied))
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s ❌ Failed\n", t.TableName, bar)
		if t.DDL != "" {
			b.WriteString(formatProgressDDL(t.DDL))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Apply.FailedRetryable:
		if t.PercentComplete > 0 || t.RowsCopied > 0 {
			retryPercent := ui.RowCopyDisplayPercent(t.PercentComplete, t.RowsCopied)
			bar := ui.ProgressBar(retryPercent, ui.ColorYellow)
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s Retrying\n", t.TableName, bar)
		} else {
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: Retrying\n", t.TableName)
		}
		if t.DDL != "" {
			b.WriteString(formatProgressDDL(t.DDL))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Apply.RevertWindow:
		bar := ui.ProgressBarWaitingCutover() // yellow — complete but revert available
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s ✓ Complete (pending revert)\n", t.TableName, bar)
		if t.DDL != "" {
			b.WriteString(formatProgressDDL(t.DDL))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Apply.SkippingRevert:
		bar := ui.ProgressBarWaitingCutover() // yellow — complete, revert window closing
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s ✓ Complete (finalizing)\n", t.TableName, bar)
		if t.DDL != "" {
			b.WriteString(formatProgressDDL(t.DDL))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Apply.Cancelled:
		if t.PercentComplete > 0 || t.RowsCopied > 0 {
			cancelledPercent := ui.RowCopyDisplayPercent(t.PercentComplete, t.RowsCopied)
			bar := ui.ProgressBarFailed(cancelledPercent)
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s ⊘ Cancelled at %d%%\n", t.TableName, bar, cancelledPercent)
		} else {
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: ⊘ Cancelled (not started)\n", t.TableName)
		}
		if t.DDL != "" {
			b.WriteString(formatProgressDDL(t.DDL))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Apply.Stopped:
		// Show orange progress bar with current progress when stopped
		stoppedPercent := ui.RowCopyDisplayPercent(t.PercentComplete, t.RowsCopied)
		bar := ui.ProgressBarStopped(stoppedPercent)
		switch {
		case t.PercentComplete >= 100:
			// At 100% = was waiting for cutover when stopped
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s ⏹️ Stopped (was waiting for cutover)\n", t.TableName, bar)
		case t.PercentComplete > 0 || t.RowsCopied > 0:
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s ⏹️ Stopped at %d%%\n", t.TableName, bar, stoppedPercent)
		default:
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: ⏹️ Stopped (not started)\n", t.TableName)
		}
		if t.DDL != "" {
			b.WriteString(formatProgressDDL(t.DDL))
		}
		if t.RowsTotal > 0 && (t.PercentComplete > 0 || t.RowsCopied > 0) {
			fmt.Fprintf(&b, indentDetail+"Rows: %s / %s\n", ui.FormatNumber(ui.ClampRows(t.RowsCopied, t.RowsTotal)), ui.FormatNumber(t.RowsTotal))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	}

	// In-progress state - try to parse Spirit's progress detail
	switch {
	case t.ProgressDetail != "":
		if info := ParseSpiritProgress(t.ProgressDetail); info != nil {
			if ui.EstimateExceeded(info.RowsCopied, info.RowsTotal) && info.State == "copyRows" {
				b.WriteString(formatEstimateExceededTable(t, info.RowsCopied, activityBar, activityLabel))
				return b.String()
			}

			// Parsed successfully - show emoji progress bar with structured data
			displayPercent := ui.RowCopyDisplayPercent(info.Percent, info.RowsCopied)
			bar := ui.ProgressBarRowCopy(displayPercent)
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s %d%%\n", t.TableName, bar, displayPercent)
			if t.DDL != "" {
				b.WriteString(formatProgressDDL(t.DDL))
			}
			// Rows and ETA on the same line, rendered from the structured ETA
			// so the CLI and PR comment show the same value via FormatETA.
			writeStructuredRowsAndETA(&b, t)
			if info.State != "" && info.State != "copyRows" {
				fmt.Fprintf(&b, indentDetail+"Status: %s\n", info.State)
			}
		} else {
			// Can't parse - show raw detail
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s:\n", t.TableName)
			if t.DDL != "" {
				b.WriteString(formatProgressDDL(t.DDL))
			}
			fmt.Fprintf(&b, "    %s\n", t.ProgressDetail)
		}
	case t.RowsTotal > 0 && t.RowsCopied == 0:
		// Row total is known but the copy hasn't reported progress yet
		// (Vitess VReplication / Spirit ramp-up — can take a while on a large
		// table). Show a starting indicator and the row total instead of a 0%
		// bar that reads as stuck.
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: ⏳ Starting copy...\n", t.TableName)
		if t.DDL != "" {
			b.WriteString(formatProgressDDL(t.DDL))
		}
		writeStructuredRowsAndETA(&b, t)
	case t.RowsTotal > 0:
		if ui.EstimateExceeded(t.RowsCopied, t.RowsTotal) {
			b.WriteString(formatEstimateExceededTable(t, t.RowsCopied, activityBar, activityLabel))
			return b.String()
		}

		// Row copy in progress — show progress bar with structured fields
		displayPercent := ui.RowCopyDisplayPercent(t.PercentComplete, t.RowsCopied)
		bar := ui.ProgressBarRowCopy(displayPercent)
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s %d%%\n", t.TableName, bar, displayPercent)

		if t.DDL != "" {
			b.WriteString(formatProgressDDL(t.DDL))
		}

		writeStructuredRowsAndETA(&b, t)

		statusLower := strings.ToLower(t.Status)
		if statusLower != "" && statusLower != "running" && statusLower != "row_copy" {
			fmt.Fprintf(&b, indentDetail+"Status: %s\n", t.Status)
		}
	default:
		// No row copy data — CREATE/DROP, instant DDL, or VSchema-only.
		// Show a full blue bar with a state label.
		bar := ui.ProgressBarRowCopy(100)
		op := ddl.OpToStatementType(t.ChangeType)
		switch {
		case t.IsInstant:
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s Applying instantly...\n", t.TableName, bar)
		case op == statement.StatementCreateTable || op == statement.StatementDropTable:
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s Applying...\n", t.TableName, bar)
		default:
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s Running...\n", t.TableName, bar)
		}
		if t.DDL != "" {
			b.WriteString(formatProgressDDL(t.DDL))
		}
	}

	if len(t.Shards) == 0 {
		b.WriteString("\n")
	}
	b.WriteString(FormatShardProgress(t.Shards))
	return b.String()
}

func recoveringIsCopyingRows(t TableProgress) bool {
	return t.RowsTotal > 0 && t.PercentComplete < 100
}

func writeStructuredRowsAndETA(b *strings.Builder, t TableProgress) {
	if t.ETASeconds > 0 {
		fmt.Fprintf(b, indentDetail+"Rows: %s / %s · ETA: %s\n", ui.FormatNumber(ui.ClampRows(t.RowsCopied, t.RowsTotal)), ui.FormatNumber(t.RowsTotal), ui.FormatETA(t.ETASeconds))
		return
	}
	fmt.Fprintf(b, indentDetail+"Rows: %s / %s\n", ui.FormatNumber(ui.ClampRows(t.RowsCopied, t.RowsTotal)), ui.FormatNumber(t.RowsTotal))
}

func formatEstimateExceededTable(t TableProgress, rowsCopied int64, activityBar, activityLabel string) string {
	var b strings.Builder
	fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s %s\n", t.TableName, activityBar, activityLabel)
	if t.DDL != "" {
		b.WriteString(formatProgressDDL(t.DDL))
	}
	fmt.Fprintf(&b, indentDetail+"Rows copied: %s so far\n", ui.FormatNumber(rowsCopied))
	fmt.Fprintf(&b, indentDetail+"%sℹ️ %s%s\n", ANSIDim, ui.EstimateExceededTooltip, ANSIReset)
	return b.String()
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
}

// WriteStopSuccess writes the stop command success output.
func WriteStopSuccess(data StopData) {
	fmt.Printf("%s%s⏸️  Schema change stopped%s\n", ANSIBold, ANSIYellow, ANSIReset)
	fmt.Println()
	fmt.Printf("Database:    %s\n", data.Database)
	fmt.Printf("Environment: %s\n", data.Environment)
	if data.StoppedCount > 0 {
		fmt.Printf("Stopped:     %d table(s)\n", data.StoppedCount)
	}
	if data.SkippedCount > 0 {
		fmt.Printf("Skipped:     %d table(s) (already complete)\n", data.SkippedCount)
	}
	fmt.Println()
	if data.ApplyID != "" {
		fmt.Printf("%sCheckpoint saved. Use 'schemabot start -e %s %s' to resume.%s\n", ANSIDim, data.Environment, data.ApplyID, ANSIReset)
	} else {
		fmt.Printf("%sCheckpoint saved. Use 'schemabot start' to resume from where you left off.%s\n", ANSIDim, ANSIReset)
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
	fmt.Printf("%s%s✖ Schema change cancelled%s\n", ANSIBold, ANSIRed, ANSIReset)
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

// WriteStartNoWatch writes the start command output when --watch=false.
func WriteStartNoWatch(applyID, database, environment string) {
	fmt.Printf("%s%s▶️  Schema change resumed%s\n", ANSIBold, ANSIGreen, ANSIReset)
	fmt.Println()
	if applyID != "" {
		fmt.Printf("To watch and manage: schemabot progress %s\n", applyID)
	} else {
		fmt.Printf("To watch and manage: schemabot status -d %s -e %s\n", database, environment)
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
		fmt.Printf("%sHeld deployments will resume. Use 'schemabot progress %s' to follow them.%s\n", ANSIDim, data.ApplyID, ANSIReset)
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
	Volume              int
}

// StatusListData contains data for rendering the status list.
type StatusListData struct {
	ActiveCount    int
	Limit          int
	MaxLimit       int
	HasMore        bool
	FailuresOnly   bool
	ShowExternalID bool
	Deployment     string
	Applies        []ActiveApplyData
}

// WriteStatusList writes the status list output.
func WriteStatusList(data StatusListData) {
	if len(data.Applies) == 0 {
		if data.FailuresOnly {
			fmt.Printf("%sNo recent failed schema changes%s\n", ANSIDim, ANSIReset)
		} else {
			fmt.Printf("%sNo recent schema changes%s\n", ANSIDim, ANSIReset)
		}
		return
	}
	if data.FailuresOnly {
		writeFailedStatusList(data)
		return
	}

	// Header
	if data.ActiveCount > 0 {
		if data.ActiveCount == 1 {
			fmt.Printf("%s1 active schema change%s\n", ANSIBold, ANSIReset)
		} else {
			fmt.Printf("%s%d active schema changes%s\n", ANSIBold, data.ActiveCount, ANSIReset)
		}
	} else {
		fmt.Printf("%sRecent schema changes%s\n", ANSIBold, ANSIReset)
	}
	fmt.Println()

	// Calculate column widths from data
	showDeployment := statusListShowsDeployment(data)
	maxID := 8 // "APPLY ID"
	maxExternal := len(statusExternalIDHeader(data))
	maxDB := 8          // "DATABASE"
	maxEnv := 3         // "ENV"
	maxDeployment := 10 // "DEPLOYMENT"
	maxState := 5       // "STATE"
	maxStarted := 7     // "STARTED"
	for _, a := range data.Applies {
		maxID = maxLen(maxID, len(a.ApplyID))
		if data.ShowExternalID {
			maxExternal = maxLen(maxExternal, len(statusExternalID(data, a)))
		}
		maxDB = maxLen(maxDB, len(a.Database))
		maxEnv = maxLen(maxEnv, len(a.Environment))
		if showDeployment {
			maxDeployment = maxLen(maxDeployment, len(a.Deployment))
		}
		maxState = maxLen(maxState, len(state.Label(a.State)))
		maxStarted = maxLen(maxStarted, len(formatStartedAt(a.StartedAt)))
	}

	// Table header
	switch {
	case data.ShowExternalID && showDeployment:
		fmt.Printf("  %s%-*s  %-*s  %-*s  %-*s  %-*s  %-*s  %-*s  %s%s\n",
			ANSIDim,
			maxID, "APPLY ID",
			maxExternal, statusExternalIDHeader(data),
			maxDB, "DATABASE",
			maxEnv, "ENV",
			maxDeployment, "DEPLOYMENT",
			maxState, "STATE",
			maxStarted, "STARTED",
			"CALLER",
			ANSIReset)
	case data.ShowExternalID:
		fmt.Printf("  %s%-*s  %-*s  %-*s  %-*s  %-*s  %-*s  %s%s\n",
			ANSIDim,
			maxID, "APPLY ID",
			maxExternal, statusExternalIDHeader(data),
			maxDB, "DATABASE",
			maxEnv, "ENV",
			maxState, "STATE",
			maxStarted, "STARTED",
			"CALLER",
			ANSIReset)
	case showDeployment:
		fmt.Printf("  %s%-*s  %-*s  %-*s  %-*s  %-*s  %-*s  %s%s\n",
			ANSIDim,
			maxID, "APPLY ID",
			maxDB, "DATABASE",
			maxEnv, "ENV",
			maxDeployment, "DEPLOYMENT",
			maxState, "STATE",
			maxStarted, "STARTED",
			"CALLER",
			ANSIReset)
	default:
		fmt.Printf("  %s%-*s  %-*s  %-*s  %-*s  %-*s  %s%s\n",
			ANSIDim,
			maxID, "APPLY ID",
			maxDB, "DATABASE",
			maxEnv, "ENV",
			maxState, "STATE",
			maxStarted, "STARTED",
			"CALLER",
			ANSIReset)
	}

	// Table rows
	for _, a := range data.Applies {
		label := state.Label(a.State)
		colorFn := stateColorFunc(a.State)
		padded := fmt.Sprintf("%-*s", maxState, label)
		coloredState := padded
		if colorFn != nil {
			coloredState = colorFn(padded)
		}

		switch {
		case data.ShowExternalID && showDeployment:
			fmt.Printf("  %-*s  %-*s  %-*s  %-*s  %-*s  %s  %-*s  %s\n",
				maxID, a.ApplyID,
				maxExternal, statusExternalID(data, a),
				maxDB, a.Database,
				maxEnv, a.Environment,
				maxDeployment, a.Deployment,
				coloredState,
				maxStarted, formatStartedAt(a.StartedAt),
				shortCaller(a.Caller))
		case data.ShowExternalID:
			fmt.Printf("  %-*s  %-*s  %-*s  %-*s  %s  %-*s  %s\n",
				maxID, a.ApplyID,
				maxExternal, statusExternalID(data, a),
				maxDB, a.Database,
				maxEnv, a.Environment,
				coloredState,
				maxStarted, formatStartedAt(a.StartedAt),
				shortCaller(a.Caller))
		case showDeployment:
			fmt.Printf("  %-*s  %-*s  %-*s  %-*s  %s  %-*s  %s\n",
				maxID, a.ApplyID,
				maxDB, a.Database,
				maxEnv, a.Environment,
				maxDeployment, a.Deployment,
				coloredState,
				maxStarted, formatStartedAt(a.StartedAt),
				shortCaller(a.Caller))
		default:
			fmt.Printf("  %-*s  %-*s  %-*s  %s  %-*s  %s\n",
				maxID, a.ApplyID,
				maxDB, a.Database,
				maxEnv, a.Environment,
				coloredState,
				maxStarted, formatStartedAt(a.StartedAt),
				shortCaller(a.Caller))
		}
	}

	writeStatusListFooter(data)
}

func writeStatusListFooter(data StatusListData) {
	fmt.Println()
	if data.HasMore && data.Limit > 0 {
		item := "schema changes"
		if data.FailuresOnly {
			item = "failed schema changes"
		}
		if data.MaxLimit > 0 && data.Limit >= data.MaxLimit {
			fmt.Printf("%sShowing the %d most recent %s. This server caps status history at %d.%s\n", ANSIDim, data.Limit, item, data.MaxLimit, ANSIReset)
		} else {
			fmt.Printf("%sShowing the %d most recent %s. Use --limit N to show more.%s\n", ANSIDim, data.Limit, item, ANSIReset)
		}
	}
	fmt.Printf("%sUse 'schemabot status <apply_id>' to view details%s\n", ANSIDim, ANSIReset)
}

func writeFailedStatusList(data StatusListData) {
	fmt.Printf("%sRecent failed schema changes%s\n", ANSIBold, ANSIReset)
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
		fmt.Printf("schemabot status %s\n", a.ApplyID)
	}

	if data.HasMore && data.Limit > 0 {
		fmt.Println()
		item := "failed schema changes"
		if data.MaxLimit > 0 && data.Limit >= data.MaxLimit {
			fmt.Printf("%sShowing the %d most recent %s. This server caps status history at %d.%s\n", ANSIDim, data.Limit, item, data.MaxLimit, ANSIReset)
		} else {
			fmt.Printf("%sShowing the %d most recent %s. Use --limit N to show more.%s\n", ANSIDim, data.Limit, item, ANSIReset)
		}
	}
}

func statusExternalID(data StatusListData, a ActiveApplyData) string {
	if a.ExternalOperationID != "" {
		return a.ExternalOperationID
	}
	if data.Deployment != "" {
		return "-"
	}
	if a.ExternalID == "" {
		return "-"
	}
	return a.ExternalID
}

func statusExternalIDHeader(data StatusListData) string {
	if data.Deployment != "" {
		return "EXTERNAL OP ID"
	}
	return "EXTERNAL ID"
}

func statusListShowsDeployment(data StatusListData) bool {
	if data.Deployment != "" {
		return true
	}
	for _, apply := range data.Applies {
		if apply.Deployment != "" {
			return true
		}
	}
	return false
}

func statusFailureActor(a ActiveApplyData, showExternalID bool) string {
	caller := shortCaller(a.Caller)
	if !showExternalID {
		return caller
	}
	return caller + "; external_id=" + statusExternalID(StatusListData{}, a)
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
	if len(reason) <= maxStatusFailureReasonWidth {
		return reason
	}
	return reason[:maxStatusFailureReasonWidth-3] + "..."
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
	Applies  []ApplyHistoryData
}

// WriteDatabaseHistory writes the database history output.
func WriteDatabaseHistory(data DatabaseHistoryData) {
	if len(data.Applies) == 0 {
		fmt.Printf("%sNo schema changes found for database '%s'%s\n", ANSIDim, data.Database, ANSIReset)
		return
	}

	// Header
	fmt.Printf("%sSchema change history for %s%s\n", ANSIBold, data.Database, ANSIReset)
	fmt.Println()

	// Calculate column widths from data
	maxID := 8      // "APPLY ID"
	maxEnv := 3     // "ENV"
	maxState := 5   // "STATE"
	maxStarted := 7 // "STARTED"
	maxDur := 8     // "DURATION"
	for _, a := range data.Applies {
		maxID = maxLen(maxID, len(a.ApplyID))
		maxEnv = maxLen(maxEnv, len(a.Environment))
		maxState = maxLen(maxState, len(state.Label(a.State)))
		maxStarted = maxLen(maxStarted, len(formatStartedAt(a.StartedAt)))
		maxDur = maxLen(maxDur, len(formatApplyDuration(a.StartedAt, a.CompletedAt)))
	}

	// Table header
	fmt.Printf("  %s%-*s  %-*s  %-*s  %-*s  %-*s  %s%s\n",
		ANSIDim,
		maxID, "APPLY ID",
		maxEnv, "ENV",
		maxState, "STATE",
		maxStarted, "STARTED",
		maxDur, "DURATION",
		"CALLER",
		ANSIReset)

	// Table rows
	for _, a := range data.Applies {
		label := state.Label(a.State)
		colorFn := stateColorFunc(a.State)
		padded := fmt.Sprintf("%-*s", maxState, label)
		coloredState := padded
		if colorFn != nil {
			coloredState = colorFn(padded)
		}

		fmt.Printf("  %-*s  %-*s  %s  %-*s  %-*s  %s\n",
			maxID, a.ApplyID,
			maxEnv, a.Environment,
			coloredState,
			maxStarted, formatStartedAt(a.StartedAt),
			maxDur, formatApplyDuration(a.StartedAt, a.CompletedAt),
			shortCaller(a.Caller))
	}

	fmt.Println()
	fmt.Printf("%sUse 'schemabot status <apply_id>' to view details%s\n", ANSIDim, ANSIReset)
}

// stateColorFunc returns an ANSI color function for the given state.
func stateColorFunc(s string) func(string) string {
	switch s {
	case state.Apply.Completed:
		return colorWrap(ANSIGreen)
	case state.Apply.Failed:
		return colorWrap(ANSIRed)
	case state.Apply.FailedRetryable:
		return colorWrap(ANSIYellow)
	case state.Apply.Running, state.Apply.RunningDegraded:
		return colorWrap(ANSICyan)
	case state.Apply.WaitingForDeploy, state.Apply.WaitingForCutover, state.Apply.Recovering, state.Apply.CuttingOver:
		return colorWrap(ANSIYellow)
	case state.Apply.Stopped:
		return colorWrap(ANSIOrange)
	case state.Apply.Cancelled:
		return colorWrap(ANSIRed)
	case state.Apply.Pending:
		return colorWrap(ANSIDim)
	case state.Apply.PreparingBranch, state.Apply.ApplyingBranchChanges, state.Apply.ValidatingBranch, state.Apply.CreatingDeployRequest, state.Apply.ValidatingDeployRequest:
		return colorWrap(ANSICyan)
	case state.Apply.Reverted:
		return colorWrap(ANSIRed)
	case state.Apply.RevertWindow:
		return colorWrap(ANSIYellow)
	case state.Apply.SkippingRevert:
		return colorWrap(ANSICyan)
	default:
		return nil
	}
}

// shortCaller strips the hostname from a caller string for compact display.
// "cli:armand@macbook.local" -> "cli:armand"
func shortCaller(caller string) string {
	if before, _, found := strings.Cut(caller, "@"); found {
		return before
	}
	return caller
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
