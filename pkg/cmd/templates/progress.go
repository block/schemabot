package templates

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/block/spirit/pkg/statement"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

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

	// Build key/value pairs for the detail box
	displayState := StateLabel(data.State)
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
		if branch := data.Metadata["branch_name"]; branch != "" {
			rows = append(rows, BoxRow{"Branch", branch})
		}
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
	if len(activeTables) > 0 && !state.IsBranchSetupPhase(data.State) {
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
		// Strip namespace prefix from VSchema task names (e.g., "VSchema: myapp_sharded" -> "VSchema")
		// since the keyspace header already provides context.
		if strings.HasPrefix(t.TableName, "vschema:") || strings.HasPrefix(t.TableName, "VSchema:") {
			parts := strings.SplitN(t.TableName, ": ", 2)
			if len(parts) == 2 {
				t.TableName = "VSchema"
			}
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
				b.WriteString(FormatTableProgress(g.tables[0]))
			}
		} else {
			b.WriteString(FormatKeyspaceHeader(g.namespaces[0]))
			// Render VSchema tasks first, then DDL tables
			for _, t := range g.tables {
				if isVSchemaTask(t) {
					b.WriteString(FormatVSchemaProgress(t))
				}
			}
			for _, t := range g.tables {
				if !isVSchemaTask(t) {
					b.WriteString(FormatTableProgress(t))
				}
			}
		}
	}
	return b.String()
}

// writeNamespacedTables renders tables grouped by keyspace to stdout.
// isVSchemaTask returns true if this is a synthetic VSchema update task.
func isVSchemaTask(t TableProgress) bool {
	return t.TableName == "VSchema" || strings.HasPrefix(t.TableName, "vschema:") || strings.HasPrefix(t.TableName, "VSchema:")
}

// FormatVSchemaProgress returns a VSchema update rendered as a string.
// The DDL field contains a VSchema diff (not SQL), so we render it with
// diff coloring via colorizeDiffLine, stripping ---/+++/@@ headers.
func FormatVSchemaProgress(t TableProgress) string {
	var b strings.Builder
	label := vschemaStatusLabel(state.NormalizeState(t.Status))
	fmt.Fprintf(&b, "    ~ VSchema: %s\n", label)
	if t.DDL != "" {
		b.WriteString(FormatVSchemaDiff(t.DDL, indentContent))
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

// writeVSchemaDiff renders a VSchema diff to stdout.
// vschemaStatusLabel maps a task status to a VSchema-specific display label.
func vschemaStatusLabel(status string) string {
	switch status {
	case state.Apply.Pending:
		return "Pending"
	case state.Apply.Running:
		return "Applying..."
	case state.Apply.Completed:
		return ANSIGreen + "Applied" + ANSIReset
	case state.Apply.Failed:
		return ANSIRed + "Failed" + ANSIReset
	case state.Apply.RevertWindow:
		return ANSIYellow + "Applied (pending revert)" + ANSIReset
	default:
		return status
	}
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
	case state.Apply.CreatingDeployRequest:
		return ANSICyan + "🔄 Creating deploy request..." + ANSIReset
	case "idle":
		return "Idle"
	case state.Apply.Running:
		return ANSICyan + "🔄 Running" + ANSIReset
	case state.Apply.WaitingForDeploy:
		return ANSIYellow + "🟨 Waiting for deploy" + ANSIReset
	case state.Apply.WaitingForCutover:
		return ANSIYellow + "🟨 Waiting for cutover" + ANSIReset
	case state.Apply.CuttingOver:
		return ANSICyan + "🔄 Cutting over..." + ANSIReset
	case state.Apply.Completed:
		return ANSIGreen + "✓ Completed" + ANSIReset
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
	var b strings.Builder

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
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s ✓ Complete\n", t.TableName, bar)
		if t.DDL != "" {
			b.WriteString(formatProgressDDL(t.DDL))
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
	case state.Apply.CuttingOver:
		bar := ui.ProgressBarRowCopy(100) // blue — still in progress
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s 🔄 Cutting over...\n", t.TableName, bar)
		if t.DDL != "" {
			b.WriteString(formatProgressDDL(t.DDL))
		}
		b.WriteString("\n")
		b.WriteString(FormatShardProgress(t.Shards))
		return b.String()
	case state.Apply.Failed:
		bar := ui.ProgressBarFailed(t.PercentComplete)
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s ❌ Failed\n", t.TableName, bar)
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
	case state.Apply.Cancelled:
		if t.PercentComplete > 0 {
			bar := ui.ProgressBarFailed(t.PercentComplete)
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s ⊘ Cancelled at %d%%\n", t.TableName, bar, t.PercentComplete)
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
		bar := ui.ProgressBarStopped(t.PercentComplete)
		switch {
		case t.PercentComplete >= 100:
			// At 100% = was waiting for cutover when stopped
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s ⏹️ Stopped (was waiting for cutover)\n", t.TableName, bar)
		case t.PercentComplete > 0:
			stoppedPercent := min(t.PercentComplete, 100)
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s ⏹️ Stopped at %d%%\n", t.TableName, bar, stoppedPercent)
		default:
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: ⏹️ Stopped (not started)\n", t.TableName)
		}
		if t.DDL != "" {
			b.WriteString(formatProgressDDL(t.DDL))
		}
		if t.RowsTotal > 0 && t.PercentComplete > 0 {
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
			// Parsed successfully - show emoji progress bar with structured data
			bar := ui.ProgressBarRowCopy(info.Percent)
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s %d%%\n", t.TableName, bar, info.Percent)
			if t.DDL != "" {
				b.WriteString(formatProgressDDL(t.DDL))
			}
			// Rows and ETA on same line
			if info.ETA != "" && info.ETA != "TBD" {
				fmt.Fprintf(&b, indentDetail+"Rows: %s / %s · ETA: %s\n", ui.FormatNumber(ui.ClampRows(info.RowsCopied, info.RowsTotal)), ui.FormatNumber(info.RowsTotal), info.ETA)
			} else {
				fmt.Fprintf(&b, indentDetail+"Rows: %s / %s\n", ui.FormatNumber(ui.ClampRows(info.RowsCopied, info.RowsTotal)), ui.FormatNumber(info.RowsTotal))
			}
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
	case t.RowsTotal > 0 && t.RowsCopied == 0 && len(t.Shards) > 0:
		// Staging phase — shards have row totals but no rows copied yet.
		// Show "Staging schema changes..." instead of a misleading 0% bar.
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: Staging schema changes...\n", t.TableName)
		if t.DDL != "" {
			b.WriteString(formatProgressDDL(t.DDL))
		}
	case t.RowsTotal > 0:
		// Row copy in progress — show progress bar with structured fields
		bar := ui.ProgressBarRowCopy(t.PercentComplete)
		displayPercent := min(t.PercentComplete, 100)
		fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: %s %d%%\n", t.TableName, bar, displayPercent)

		if t.DDL != "" {
			b.WriteString(formatProgressDDL(t.DDL))
		}

		// Rows and ETA on same line
		if t.ETASeconds > 0 {
			fmt.Fprintf(&b, indentDetail+"Rows: %s / %s · ETA: %s\n", ui.FormatNumber(ui.ClampRows(t.RowsCopied, t.RowsTotal)), ui.FormatNumber(t.RowsTotal), ui.FormatETA(t.ETASeconds))
		} else {
			fmt.Fprintf(&b, indentDetail+"Rows: %s / %s\n", ui.FormatNumber(ui.ClampRows(t.RowsCopied, t.RowsTotal)), ui.FormatNumber(t.RowsTotal))
		}

		statusLower := strings.ToLower(t.Status)
		if statusLower != "" && statusLower != "running" && statusLower != "row_copy" {
			fmt.Fprintf(&b, indentDetail+"Status: %s\n", t.Status)
		}
	default:
		if t.IsInstant {
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: ⚡ Applying instantly...\n", t.TableName)
		} else {
			fmt.Fprintf(&b, indentTable+progressSymbol(t.ChangeType)+"%s: Running...\n", t.TableName)
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
		fmt.Printf("%sCheckpoint saved. Use 'schemabot start --apply-id %s' to resume.%s\n", ANSIDim, data.ApplyID, ANSIReset)
	} else {
		fmt.Printf("%sCheckpoint saved. Use 'schemabot start' to resume from where you left off.%s\n", ANSIDim, ANSIReset)
	}
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
		fmt.Printf("To watch and manage: schemabot progress --apply-id %s\n", applyID)
	} else {
		fmt.Printf("To watch and manage: schemabot progress -d %s -e %s\n", database, environment)
	}
}

// ActiveApplyData contains data for a single apply in the status list.
type ActiveApplyData struct {
	ApplyID     string
	Database    string
	Environment string
	State       string
	Engine      string
	Caller      string
	StartedAt   string
	CompletedAt string
	UpdatedAt   string
	Volume      int
}

// StatusListData contains data for rendering the status list.
type StatusListData struct {
	ActiveCount int
	Applies     []ActiveApplyData
}

// WriteStatusList writes the status list output.
func WriteStatusList(data StatusListData) {
	if len(data.Applies) == 0 {
		fmt.Printf("%sNo recent schema changes%s\n", ANSIDim, ANSIReset)
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
	maxID := 8      // "APPLY ID"
	maxDB := 8      // "DATABASE"
	maxEnv := 3     // "ENV"
	maxState := 5   // "STATE"
	maxStarted := 7 // "STARTED"
	maxDur := 8     // "DURATION"
	for _, a := range data.Applies {
		maxID = maxLen(maxID, len(a.ApplyID))
		maxDB = maxLen(maxDB, len(a.Database))
		maxEnv = maxLen(maxEnv, len(a.Environment))
		maxState = maxLen(maxState, len(StateLabel(a.State)))
		maxStarted = maxLen(maxStarted, len(formatStartedAt(a.StartedAt)))
		maxDur = maxLen(maxDur, len(formatApplyDuration(a.StartedAt, a.CompletedAt)))
	}

	// Table header
	fmt.Printf("  %s%-*s  %-*s  %-*s  %-*s  %-*s  %-*s  %s%s\n",
		ANSIDim,
		maxID, "APPLY ID",
		maxDB, "DATABASE",
		maxEnv, "ENV",
		maxState, "STATE",
		maxStarted, "STARTED",
		maxDur, "DURATION",
		"CALLER",
		ANSIReset)

	// Table rows
	for _, a := range data.Applies {
		label := StateLabel(a.State)
		colorFn := stateColorFunc(a.State)
		padded := fmt.Sprintf("%-*s", maxState, label)
		coloredState := padded
		if colorFn != nil {
			coloredState = colorFn(padded)
		}

		fmt.Printf("  %-*s  %-*s  %-*s  %s  %-*s  %-*s  %s\n",
			maxID, a.ApplyID,
			maxDB, a.Database,
			maxEnv, a.Environment,
			coloredState,
			maxStarted, formatStartedAt(a.StartedAt),
			maxDur, formatApplyDuration(a.StartedAt, a.CompletedAt),
			shortCaller(a.Caller))
	}

	fmt.Println()
	fmt.Printf("%sUse 'schemabot status <apply_id>' to view details%s\n", ANSIDim, ANSIReset)
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
		maxState = maxLen(maxState, len(StateLabel(a.State)))
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
		label := StateLabel(a.State)
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

// StateLabel returns the human-readable display label for an apply state.
func StateLabel(s string) string {
	switch s {
	case state.Apply.Completed:
		return "Completed"
	case state.Apply.Failed:
		return "Failed"
	case state.Apply.Running:
		return "Running"
	case state.Apply.WaitingForDeploy:
		return "Waiting for deploy"
	case state.Apply.WaitingForCutover:
		return "Waiting for cutover"
	case state.Apply.CuttingOver:
		return "Cutting over"
	case state.Apply.Stopped:
		return "Stopped"
	case state.Apply.Pending:
		return "Pending"
	case state.Apply.Cancelled:
		return "Cancelled"
	case state.Apply.PreparingBranch:
		return "Preparing branch"
	case state.Apply.ApplyingBranchChanges:
		return "Applying changes to branch"
	case state.Apply.CreatingDeployRequest:
		return "Creating deploy request"
	case state.Apply.RevertWindow:
		return "Revert window"
	case state.Apply.Reverted:
		return "Reverted"
	default:
		return s
	}
}

// stateColorFunc returns an ANSI color function for the given state.
func stateColorFunc(s string) func(string) string {
	switch s {
	case state.Apply.Completed:
		return colorWrap(ANSIGreen)
	case state.Apply.Failed:
		return colorWrap(ANSIRed)
	case state.Apply.Running:
		return colorWrap(ANSICyan)
	case state.Apply.WaitingForDeploy, state.Apply.WaitingForCutover, state.Apply.CuttingOver:
		return colorWrap(ANSIYellow)
	case state.Apply.Stopped:
		return colorWrap(ANSIOrange)
	case state.Apply.Cancelled:
		return colorWrap(ANSIRed)
	case state.Apply.Pending:
		return colorWrap(ANSIDim)
	case state.Apply.PreparingBranch, state.Apply.ApplyingBranchChanges, state.Apply.CreatingDeployRequest:
		return colorWrap(ANSICyan)
	case state.Apply.Reverted:
		return colorWrap(ANSIRed)
	case state.Apply.RevertWindow:
		return colorWrap(ANSIYellow)
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
