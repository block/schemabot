package templates

import (
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/schema"
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
	if _, err := ddl.ParserForDialect(dialect); err != nil {
		// A database type with no registered parser — empty (older server)
		// or one this CLI doesn't know (newer server) — keeps the MySQL
		// rendering rather than degrading to unformatted output.
		dialect = schema.DialectMySQL
	}
	return IndentSQL(ddl.FormatDDLForDialect(dialect, rawDDL), indentContent) + "\n"
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

// isInstantAlter reports whether the table should be described as applying
// instantly: the engine flagged it instant and it is an ALTER. Other
// operations may complete without a row copy, but they are not instant DDL,
// so they keep their generic applying labels. ChangeType is populated for
// every stored DDL change (storage rejects a blank operation), so an empty
// value here means an unknown operation, not a missing field.
func isInstantAlter(t TableProgress) bool {
	return t.IsInstant && ddl.OpToStatementType(t.ChangeType) == ddl.StatementAlterTable
}

// FormatTableProgressWithActivity returns progress for a single table using the
// provided activity bar and label when row-copy progress has exceeded its
// estimate.
func FormatTableProgressWithActivity(t TableProgress, activityBar, activityLabel string) string {
	var b strings.Builder

	// Handle special states first - all use format: tablename: [bar] [status]
	switch t.Status {
	case state.Apply.Pending:
		// Pending = queued, not yet started
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: ⏳ Queued\n", t.TableName)
		if t.DDL != "" {
			b.WriteString(formatProgressDDLForDialect(t.Dialect, t.DDL))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Apply.Completed:
		bar := ui.ProgressBarComplete()
		label := "✓ Complete"
		if isInstantAlter(t) {
			label = "⚡ Applied instantly"
		}
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s %s\n", t.TableName, bar, label)
		if t.DDL != "" {
			b.WriteString(formatProgressDDLForDialect(t.Dialect, t.DDL))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Task.CatchingUp:
		// Row copy is done; the engine is applying the changes that accumulated
		// from live traffic during the copy. On a busy source this catch-up can
		// run for a long time, so name the phase instead of showing a serene
		// full bar that looks finished.
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s ⏩ Catching up on accumulated changes...\n", t.TableName, ui.ProgressBarRowCopy(100))
		if t.DDL != "" {
			b.WriteString(formatProgressDDLForDialect(t.Dialect, t.DDL))
		}
		if t.RowsCopied > 0 {
			fmt.Fprintf(&b, indentDetail+"Rows copied: %s\n", ui.FormatNumber(t.RowsCopied))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Task.Checksumming:
		// Row copy is done; the engine is verifying the copied data against the
		// source. On a large table this can run for hours, so show how far the
		// verify has progressed once Spirit has reported a total.
		if t.ChecksumRowsTotal > 0 {
			checksumPct := int(math.Round(float64(t.ChecksumRowsChecked) * 100 / float64(t.ChecksumRowsTotal)))
			pct := ui.RowCopyDisplayPercent(checksumPct, t.ChecksumRowsChecked)
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s 🔍 Checksumming to verify data (%s)%s\n", t.TableName, ui.ProgressBarRowCopy(pct),
				ui.FormatRowCopyPercent(checksumPct, t.ChecksumRowsChecked, t.ChecksumRowsTotal), throttledSuffix(t))
			if t.DDL != "" {
				b.WriteString(formatProgressDDLForDialect(t.Dialect, t.DDL))
			}
			fmt.Fprintf(&b, indentDetail+"Rows verified: %s / %s\n",
				ui.FormatNumber(ui.ClampRows(t.ChecksumRowsChecked, t.ChecksumRowsTotal)), ui.FormatNumber(t.ChecksumRowsTotal))
		} else {
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s 🔍 Checksumming to verify data...%s\n", t.TableName, ui.ProgressBarRowCopy(100), throttledSuffix(t))
			if t.DDL != "" {
				b.WriteString(formatProgressDDLForDialect(t.Dialect, t.DDL))
			}
		}
		writeThrottleTooltip(&b, t)
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Task.PostChecksum:
		// The verify passed and the engine is applying the changes that
		// accumulated while it ran. Named separately from the pre-checksum
		// catch-up so the display doesn't rewind to an earlier phase.
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s ⏩ Data verified, applying final changes...\n", t.TableName, ui.ProgressBarRowCopy(100))
		if t.DDL != "" {
			b.WriteString(formatProgressDDLForDialect(t.Dialect, t.DDL))
		}
		if t.RowsCopied > 0 {
			fmt.Fprintf(&b, indentDetail+"Rows copied: %s\n", ui.FormatNumber(t.RowsCopied))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Task.WaitingForCutover:
		bar := ui.ProgressBarWaitingCutover()
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s Waiting for cutover\n", t.TableName, bar)
		if t.DDL != "" {
			b.WriteString(formatProgressDDLForDialect(t.Dialect, t.DDL))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Apply.Recovering:
		if recoveringIsCopyingRows(t) {
			pct := ui.RowCopyDisplayPercent(t.PercentComplete, t.RowsCopied)
			bar := ui.ProgressBarRowCopy(pct)
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s Row copy in progress (%s)\n", t.TableName, bar,
				ui.FormatRowCopyPercent(t.PercentComplete, t.RowsCopied, t.RowsTotal))
			if t.DDL != "" {
				b.WriteString(formatProgressDDLForDialect(t.Dialect, t.DDL))
			}
			writeStructuredRowsAndETA(&b, t)
			b.WriteString("\n")
			b.WriteString(FormatShardProgress(t.Shards))
			return b.String()
		}
		bar := ui.ProgressBarRowCopy(t.PercentComplete)
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s Recovering state...\n", t.TableName, bar)
		if t.DDL != "" {
			b.WriteString(formatProgressDDLForDialect(t.Dialect, t.DDL))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Apply.CuttingOver:
		bar := ui.ProgressBarRowCopy(100) // blue — still in progress
		label := "Cutting over..."
		op := ddl.OpToStatementType(t.ChangeType)
		if op == ddl.StatementCreateTable || op == ddl.StatementDropTable {
			label = "Applying..."
		}
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s %s\n", t.TableName, bar, label)
		if t.DDL != "" {
			b.WriteString(formatProgressDDLForDialect(t.Dialect, t.DDL))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Apply.Failed:
		bar := ui.ProgressBarFailed(ui.RowCopyDisplayPercent(t.PercentComplete, t.RowsCopied))
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s "+glyph.Failed+" Failed\n", t.TableName, bar)
		if t.DDL != "" {
			b.WriteString(formatProgressDDLForDialect(t.Dialect, t.DDL))
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
			b.WriteString(formatProgressDDLForDialect(t.Dialect, t.DDL))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Apply.RevertWindow:
		bar := ui.ProgressBarWaitingCutover() // yellow — complete but revert available
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s Complete (revert window open)\n", t.TableName, bar)
		if t.DDL != "" {
			b.WriteString(formatProgressDDLForDialect(t.Dialect, t.DDL))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Apply.SkippingRevert:
		bar := ui.ProgressBarWaitingCutover() // yellow — complete, revert window closing
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s ✓ Complete (finalizing)\n", t.TableName, bar)
		if t.DDL != "" {
			b.WriteString(formatProgressDDLForDialect(t.Dialect, t.DDL))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Apply.Reverting:
		bar := ui.ProgressBarWaitingCutover() // yellow — undoing the change
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s ↩️ Reverting\n", t.TableName, bar)
		if t.DDL != "" {
			b.WriteString(formatProgressDDLForDialect(t.Dialect, t.DDL))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Apply.Reverted:
		// The change was applied, then undone at operator request — a
		// successful revert, not a failure. Full orange bar: terminal,
		// change not in effect.
		bar := ui.ProgressBar(100, ui.ColorOrange)
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s ↩️ Reverted\n", t.TableName, bar)
		if t.DDL != "" {
			b.WriteString(formatProgressDDLForDialect(t.Dialect, t.DDL))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Apply.Cancelled:
		if t.PercentComplete > 0 || t.RowsCopied > 0 {
			cancelledPercent := ui.RowCopyDisplayPercent(t.PercentComplete, t.RowsCopied)
			bar := ui.ProgressBar(cancelledPercent, ui.ColorOrange)
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s 🚫 Cancelled at %s\n", t.TableName, bar,
				ui.FormatRowCopyPercent(t.PercentComplete, t.RowsCopied, t.RowsTotal))
		} else {
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: 🚫 Cancelled (not started)\n", t.TableName)
		}
		if t.DDL != "" {
			b.WriteString(formatProgressDDLForDialect(t.Dialect, t.DDL))
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
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s ⏹️ Stopped at %s\n", t.TableName, bar,
				ui.FormatRowCopyPercent(t.PercentComplete, t.RowsCopied, t.RowsTotal))
		default:
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: ⏹️ Stopped (not started)\n", t.TableName)
		}
		if t.DDL != "" {
			b.WriteString(formatProgressDDLForDialect(t.Dialect, t.DDL))
		}
		if t.RowsTotal > 0 && (t.PercentComplete > 0 || t.RowsCopied > 0) {
			fmt.Fprintf(&b, indentDetail+"Rows: %s / %s\n", ui.FormatNumber(ui.ClampRows(t.RowsCopied, t.RowsTotal)), ui.FormatNumber(t.RowsTotal))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	}

	// In-progress state — rendered from the structured copy fields, which are
	// the same source the PR comment renders from.
	switch {
	case t.RowsTotal > 0 && t.RowsCopied == 0:
		// Row total is known but the copy hasn't reported progress yet
		// (Vitess VReplication / Spirit ramp-up — can take a while on a large
		// table). Show a starting indicator and the row total instead of a 0%
		// bar that reads as stuck.
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: ⏳ Starting copy...%s\n", t.TableName, throttledSuffix(t))
		if t.DDL != "" {
			b.WriteString(formatProgressDDLForDialect(t.Dialect, t.DDL))
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
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s %s%s\n", t.TableName, bar,
			ui.FormatRowCopyPercent(t.PercentComplete, t.RowsCopied, t.RowsTotal), throttledSuffix(t))

		if t.DDL != "" {
			b.WriteString(formatProgressDDLForDialect(t.Dialect, t.DDL))
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
		case isInstantAlter(t):
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s Applying instantly...%s\n", t.TableName, bar, throttledSuffix(t))
		case op == ddl.StatementCreateTable || op == ddl.StatementDropTable:
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s Applying...%s\n", t.TableName, bar, throttledSuffix(t))
		default:
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s Running...%s\n", t.TableName, bar, throttledSuffix(t))
		}
		if t.DDL != "" {
			b.WriteString(formatProgressDDLForDialect(t.Dialect, t.DDL))
		}
	}

	writeThrottleTooltip(&b, t)
	if len(t.Shards) == 0 {
		b.WriteString("\n")
	}
	b.WriteString(FormatShardProgress(t.Shards))
	return b.String()
}

// throttledSuffix annotates a paced-phase header when the engine's throttler
// is holding the phase back, so a slow bar reads as deliberate backpressure —
// slowed, not stopped — right where the eye checks progress, and never to be
// confused with an operator stop. The drive clears the stored flag when the
// throttle lifts, so the annotation disappears on the next refresh.
func throttledSuffix(t TableProgress) string {
	if !t.Throttled {
		return ""
	}
	return " (throttled)"
}

// writeThrottleTooltip explains the header's "(throttled)" annotation with the
// engine's reason, using the same dimmed tooltip idiom as the estimate-exceeded
// note. When the engine reports throttled without a reason, the header
// annotation stands alone.
func writeThrottleTooltip(b *strings.Builder, t TableProgress) {
	if !t.Throttled || t.ThrottleReason == "" {
		return
	}
	// The raw reason names the engine signal; the tip says what the pause
	// protects. A reason whose signal has no tip renders alone so a new
	// engine signal degrades to raw text rather than a wrong explanation.
	if tip := ui.ThrottleTip(t.ThrottleReason); tip != "" {
		fmt.Fprintf(b, indentDetail+"%s"+glyph.Info+" Throttled: %s · %s%s\n", ANSIDim, t.ThrottleReason, tip, ANSIReset)
		return
	}
	fmt.Fprintf(b, indentDetail+"%s"+glyph.Info+" Throttled: %s%s\n", ANSIDim, t.ThrottleReason, ANSIReset)
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
	fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s %s%s\n", t.TableName, activityBar, activityLabel, throttledSuffix(t))
	if t.DDL != "" {
		b.WriteString(formatProgressDDLForDialect(t.Dialect, t.DDL))
	}
	fmt.Fprintf(&b, indentDetail+"Rows copied: %s so far\n", ui.FormatNumber(rowsCopied))
	fmt.Fprintf(&b, indentDetail+"%s"+glyph.Info+" %s%s\n", ANSIDim, ui.EstimateExceededTooltip, ANSIReset)
	writeThrottleTooltip(&b, t)
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
	fmt.Printf("%s%s⏹️  Schema change stopped%s\n", ANSIBold, ANSIYellow, ANSIReset)
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
		fmt.Printf("%sCheckpoint saved. Use '%s start -e %s %s' to resume.%s\n", ANSIDim, cliname.Name(), data.Environment, data.ApplyID, ANSIReset)
	} else {
		fmt.Printf("%sCheckpoint saved. Use '%s start' to resume from where you left off.%s\n", ANSIDim, cliname.Name(), ANSIReset)
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
		fmt.Printf("To watch and manage: %s progress %s\n", cliname.Name(), applyID)
	} else {
		fmt.Printf("To watch and manage: %s status -d %s -e %s\n", cliname.Name(), database, environment)
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
		fmt.Printf("%sHeld deployments will resume. Use '%s progress %s' to follow them.%s\n", ANSIDim, cliname.Name(), data.ApplyID, ANSIReset)
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
	Applies        []ActiveApplyData
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
	if len(data.Applies) == 0 {
		filters := ""
		if parts := statusFilterPhrases(data); len(parts) > 0 {
			filters = " " + strings.Join(parts, ", ")
		}
		if data.FailuresOnly {
			fmt.Printf("%sNo recent failed schema changes%s%s\n", ANSIDim, filters, ANSIReset)
		} else {
			fmt.Printf("%sNo recent schema changes%s%s\n", ANSIDim, filters, ANSIReset)
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
			fmt.Printf("%s1 active schema change%s%s\n", ANSIBold, ANSIReset, statusWindowSuffix(data))
		} else {
			fmt.Printf("%s%d active schema changes%s%s\n", ANSIBold, data.ActiveCount, ANSIReset, statusWindowSuffix(data))
		}
	} else {
		fmt.Printf("%sRecent schema changes%s%s\n", ANSIBold, ANSIReset, statusWindowSuffix(data))
	}
	writeStatusStateSummary(data.StateCounts)
	fmt.Println()

	writeStatusTable(statusListColumns(data), data.Applies, func(a ActiveApplyData) string { return a.State })

	writeStatusListFooter(data)
}

// writeStatusListTruncation says how much of the history a truncated page holds,
// and how to see more of it — or that the server will not serve more, when the
// page is already at the cap.
func writeStatusListTruncation(data StatusListData, item string) {
	if data.MaxLimit > 0 && data.Limit >= data.MaxLimit {
		fmt.Printf("%sShowing the %d most recent %s. This server caps status history at %d.%s\n", ANSIDim, data.Limit, item, data.MaxLimit, ANSIReset)
		return
	}
	fmt.Printf("%sShowing the %d most recent %s. Use --limit N to show more.%s\n", ANSIDim, data.Limit, item, ANSIReset)
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
	fmt.Printf("%sUse '%s status <apply_id>' to view details%s\n", ANSIDim, cliname.Name(), ANSIReset)
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
		fmt.Printf("%s status %s\n", cliname.Name(), a.ApplyID)
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
func statusListColumns(data StatusListData) []statusColumn[ActiveApplyData] {
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
		statusColumn[ActiveApplyData]{header: "SOURCE", last: true, value: func(a ActiveApplyData) string { return applySource(a.Caller) }},
	)
	return retainPopulatedStatusColumns(columns, data.Applies)
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
		fmt.Printf("%sNo schema changes found for database '%s'%s\n", ANSIDim, data.Database, ANSIReset)
		return
	}

	fmt.Printf("%sSchema change history for %s%s\n", ANSIBold, data.Database, ANSIReset)
	fmt.Println()

	writeStatusTable(databaseHistoryColumns(), data.Applies, func(a ApplyHistoryData) string { return a.State })

	fmt.Println()
	fmt.Printf("%sUse '%s status <apply_id>' to view details%s\n", ANSIDim, cliname.Name(), ANSIReset)
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
