package templates

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

// nowFunc returns the current time. Overridden in previews for deterministic output.
var nowFunc = time.Now

// WriteProgress writes the schema change progress to stdout.
func WriteProgress(data ProgressData) {
	// No active schema change
	if data.State == "" || data.State == state.NoActiveChange {
		fmt.Println("No active schema change")
		return
	}

	// Build key/value pairs for the detail box
	displayState := StateLabel(data.State)
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
	if data.Engine != "" {
		rows = append(rows, BoxRow{"Engine", data.Engine})
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

	// Table progress (sorted: active first, terminal last)
	if len(activeTables) > 0 {
		sort.SliceStable(activeTables, func(i, j int) bool {
			return ui.TableStatePriority(state.NormalizeTaskStatus(activeTables[i].Status)) <
				ui.TableStatePriority(state.NormalizeTaskStatus(activeTables[j].Status))
		})
		fmt.Println("\nTable Progress:")
		for _, t := range activeTables {
			writeTableProgress(t)
		}
	}

	// Show remediation guidance for failed applies
	if data.State == state.Apply.Failed {
		writeFailureGuidance()
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
	case "idle":
		return "Idle"
	case state.Apply.Running:
		return ANSICyan + "🔄 Running" + ANSIReset
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
	default:
		return s
	}
}

// writeTableProgress writes progress for a single table with state-aware colors.
// Format: tablename: [progress bar] [status]
//
//	DDL statement (indented below)
//	Rows: X / Y (if applicable)
func writeTableProgress(t TableProgress) {
	// Handle special states first - all use format: tablename: [bar] [status]
	switch t.Status {
	case state.Apply.Pending:
		// Pending = queued, not yet started
		fmt.Printf("  %s: ⏳ Queued\n", t.TableName)
		if t.DDL != "" {
			fmt.Printf("    %s\n", FormatSQL(t.DDL))
		}
		fmt.Println()
		return
	case state.Apply.Completed:
		bar := ui.ProgressBarComplete()
		fmt.Printf("  %s: %s ✓ Complete\n", t.TableName, bar)
		if t.DDL != "" {
			fmt.Printf("    %s\n", FormatSQL(t.DDL))
		}
		fmt.Println()
		return
	case state.Apply.WaitingForCutover:
		bar := ui.ProgressBarWaitingCutover()
		fmt.Printf("  %s: %s Waiting for cutover\n", t.TableName, bar)
		if t.DDL != "" {
			fmt.Printf("    %s\n", FormatSQL(t.DDL))
		}
		fmt.Println()
		return
	case state.Apply.CuttingOver:
		bar := ui.ProgressBarWaitingCutover()
		fmt.Printf("  %s: %s 🔄 Cutting over...\n", t.TableName, bar)
		if t.DDL != "" {
			fmt.Printf("    %s\n", FormatSQL(t.DDL))
		}
		fmt.Println()
		return
	case state.Apply.Failed:
		bar := ui.ProgressBarFailed(t.PercentComplete)
		fmt.Printf("  %s: %s ❌ Failed\n", t.TableName, bar)
		if t.DDL != "" {
			fmt.Printf("    %s\n", FormatSQL(t.DDL))
		}
		fmt.Println()
		return
	case TaskCancelled:
		// Cancelled = task was never executed due to earlier failure
		fmt.Printf("  %s: ⊘ Cancelled (not started)\n", t.TableName)
		if t.DDL != "" {
			fmt.Printf("    %s\n", FormatSQL(t.DDL))
		}
		fmt.Println()
		return
	case state.Apply.Stopped:
		// Show orange progress bar with current progress when stopped
		bar := ui.ProgressBarStopped(t.PercentComplete)
		switch {
		case t.PercentComplete >= 100:
			// At 100% = was waiting for cutover when stopped
			fmt.Printf("  %s: %s ⏹️ Stopped (was waiting for cutover)\n", t.TableName, bar)
		case t.PercentComplete > 0:
			stoppedPercent := min(t.PercentComplete, 100)
			fmt.Printf("  %s: %s ⏹️ Stopped at %d%%\n", t.TableName, bar, stoppedPercent)
		default:
			fmt.Printf("  %s: ⏹️ Stopped (not started)\n", t.TableName)
		}
		if t.DDL != "" {
			fmt.Printf("    %s\n", FormatSQL(t.DDL))
		}
		if t.RowsTotal > 0 && t.PercentComplete > 0 {
			fmt.Printf("    Rows: %s / %s\n", ui.FormatNumber(ui.ClampRows(t.RowsCopied, t.RowsTotal)), ui.FormatNumber(t.RowsTotal))
		}
		fmt.Println()
		return
	}

	// In-progress state - try to parse Spirit's progress detail
	switch {
	case t.ProgressDetail != "":
		if info := ParseSpiritProgress(t.ProgressDetail); info != nil {
			// Parsed successfully - show emoji progress bar with structured data
			bar := ui.ProgressBarRowCopy(info.Percent)
			fmt.Printf("  %s: %s %d%%\n", t.TableName, bar, info.Percent)
			if t.DDL != "" {
				fmt.Printf("    %s\n", FormatSQL(t.DDL))
			}
			// Rows and ETA on same line
			if info.ETA != "" && info.ETA != "TBD" {
				fmt.Printf("    Rows: %s / %s · ETA: %s\n", ui.FormatNumber(ui.ClampRows(info.RowsCopied, info.RowsTotal)), ui.FormatNumber(info.RowsTotal), info.ETA)
			} else {
				fmt.Printf("    Rows: %s / %s\n", ui.FormatNumber(ui.ClampRows(info.RowsCopied, info.RowsTotal)), ui.FormatNumber(info.RowsTotal))
			}
			if info.State != "" && info.State != "copyRows" {
				fmt.Printf("    Status: %s\n", info.State)
			}
		} else {
			// Can't parse - show raw detail
			fmt.Printf("  %s:\n", t.TableName)
			if t.DDL != "" {
				fmt.Printf("    %s\n", FormatSQL(t.DDL))
			}
			fmt.Printf("    %s\n", t.ProgressDetail)
		}
	case t.RowsTotal > 0:
		// Row copy in progress — show progress bar with structured fields
		bar := ui.ProgressBarRowCopy(t.PercentComplete)
		displayPercent := min(t.PercentComplete, 100)
		fmt.Printf("  %s: %s %d%%\n", t.TableName, bar, displayPercent)

		if t.DDL != "" {
			fmt.Printf("    %s\n", FormatSQL(t.DDL))
		}

		// Rows and ETA on same line
		if t.ETASeconds > 0 {
			fmt.Printf("    Rows: %s / %s · ETA: %s\n", ui.FormatNumber(ui.ClampRows(t.RowsCopied, t.RowsTotal)), ui.FormatNumber(t.RowsTotal), ui.FormatETA(t.ETASeconds))
		} else {
			fmt.Printf("    Rows: %s / %s\n", ui.FormatNumber(ui.ClampRows(t.RowsCopied, t.RowsTotal)), ui.FormatNumber(t.RowsTotal))
		}

		statusLower := strings.ToLower(t.Status)
		if statusLower != "" && statusLower != "running" && statusLower != "row_copy" {
			fmt.Printf("    Status: %s\n", t.Status)
		}
	default:
		// No row copy data yet (initializing or instant DDL) — just show running state
		fmt.Printf("  %s: Running...\n", t.TableName)
		if t.DDL != "" {
			fmt.Printf("    %s\n", FormatSQL(t.DDL))
		}
	}

	fmt.Println()
}

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
	case state.Apply.WaitingForCutover:
		return "Waiting for cutover"
	case state.Apply.CuttingOver:
		return "Cutting over"
	case state.Apply.Stopped:
		return "Stopped"
	case state.Apply.Pending:
		return "Pending"
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
	case state.Apply.WaitingForCutover, state.Apply.CuttingOver:
		return colorWrap(ANSIYellow)
	case state.Apply.Stopped:
		return colorWrap(ANSIOrange)
	case state.Apply.Pending:
		return colorWrap(ANSIDim)
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
