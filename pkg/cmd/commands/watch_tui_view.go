package commands

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"

	"github.com/block/schemabot/pkg/cmd/templates"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

// View implements tea.Model.
func (m WatchModel) View() string {
	if m.detached {
		return m.detachedView()
	}
	if m.quitting {
		return ""
	}

	// Show nothing until we have data
	if !m.initialized {
		return m.spinner.View() + "Loading...\n"
	}

	// Handle no active schema change
	if state.IsState(m.state, StateNoActiveChange) {
		return "No active schema change for this database.\n"
	}

	return m.progressView()
}

// progressView renders the progress display.
func (m WatchModel) progressView() string {
	var b strings.Builder

	// Sort tables by status priority (running first, then pending, then completed)
	tables := make([]tableProgress, len(m.tables))
	copy(tables, m.tables)
	sortTablesByProgress(tables)

	// Check if effectively stopped (any table is stopped means the whole apply is stopped)
	effectivelyStopped := m.isEffectivelyStopped()
	if effectivelyStopped {
		sortStoppedByProgress(tables)
	}

	// Status line with spinner for active states
	// Note: spinner.Dot already includes trailing space
	switch {
	case m.volumeChanging:
		// Volume change in progress
		b.WriteString(m.spinner.View() + fmt.Sprintf("Changing volume to %d...\n", m.volumePending))
	case m.stopTriggered && !state.IsState(m.state, StateStopped):
		// Stop has been triggered but state hasn't updated yet
		b.WriteString(m.spinner.View() + "Stopping...\n")
	case effectivelyStopped:
		// Don't show status line - stopped message comes after tables
	case state.IsState(m.state, StateRunning) && !m.cutoverTriggered:
		// Find overall ETA and check if any table is actually copying rows
		eta := ""
		hasRowCopy := false
		for _, t := range tables {
			if t.RowsTotal > 0 && !isTableComplete(t.Status) {
				hasRowCopy = true
				if t.ETA != "" && t.ETA != "TBD" {
					eta = t.ETA
				}
				break
			}
		}
		if hasRowCopy {
			b.WriteString(m.spinner.View() + templates.FormatStatusLine("Copying rows...", eta))
		} else {
			b.WriteString(m.spinner.View() + "Running...")
		}
		b.WriteString("\n")
	case state.IsState(m.state, StateWaitingForCutover):
		if m.cutoverTriggered {
			b.WriteString(m.spinner.View() + "Cutover triggered, waiting for completion...\n")
		}
	case state.IsState(m.state, StateCuttingOver):
		b.WriteString(m.spinner.View() + "Cutting over...\n")
	case state.IsState(m.state, StateCompleted):
		// No status line for completed - just show completion message after tables
	case state.IsState(m.state, StateStopped):
		// No status line - show stopped message after tables
	case state.IsState(m.state, StatePending):
		b.WriteString(m.spinner.View() + "Starting...\n")
	}

	// Render each table
	for i, t := range tables {
		b.WriteString(m.renderTable(t))
		// Add spacing between tables (but not after the last one)
		if i < len(tables)-1 {
			b.WriteString("\n")
		}
	}

	// Footer based on state
	isCuttingOver := state.IsState(m.state, StateCuttingOver) || m.cutoverTriggered

	switch {
	case state.IsState(m.state, StateCompleted):
		b.WriteString("\n\n")
		b.WriteString(templates.FormatApplyComplete())
		b.WriteString("\n")
	case effectivelyStopped:
		b.WriteString("\n\n")
		b.WriteString(templates.FormatApplyStopped())
		b.WriteString("\n")
		if m.applyID != "" {
			fmt.Fprintf(&b, "Use 'schemabot start --apply-id %s' to resume.\n", m.applyID)
		} else {
			fmt.Fprintf(&b, "Use 'schemabot start -d %s -e %s' to resume.\n", m.database, m.environment)
		}
	case isCuttingOver:
		// During cutover, show minimal footer - no detach/stop allowed
		b.WriteString("\n\n")
		dimStyle := lipgloss.NewStyle().Faint(true)
		b.WriteString(dimStyle.Render("Cutover in progress - please wait..."))
		b.WriteString("\n")
	case state.IsState(m.state, StateWaitingForCutover):
		b.WriteString("\n\n")
		b.WriteString("Row copy complete. All data has been copied and new writes\n")
		b.WriteString("continue to be replicated to keep the shadow table in sync.\n\n")
		if m.allowCutover {
			b.WriteString("Press Enter to proceed with cutover (or ESC to detach)\n")
		} else {
			if m.applyID != "" {
				fmt.Fprintf(&b, "To proceed: schemabot cutover --apply-id %s\n", m.applyID)
			} else {
				fmt.Fprintf(&b, "To proceed: schemabot cutover -d %s -e %s\n", m.database, m.environment)
			}
			b.WriteString("Watching for cutover... (ESC to detach)\n")
		}
	case state.IsState(m.state, StateRunning):
		b.WriteString("\n\n")
		if m.volumeMode {
			// Volume mode - show volume adjustment UI
			b.WriteString(m.formatVolumeMode())
		} else {
			// Normal mode - simple footer without volume
			b.WriteString(m.formatFooter())
		}
		b.WriteString("\n")
	}

	// Error message if present
	if m.errorMsg != "" {
		b.WriteString("\n")
		errStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
		b.WriteString(errStyle.Render("Error: " + m.errorMsg))
		b.WriteString("\n")
	}

	return b.String()
}

// renderTable renders a single table's progress.
func (m WatchModel) renderTable(t tableProgress) string {
	var b strings.Builder

	// Format DDL with multi-line support for multiple clauses
	ddlFormatted := ddl.FormatDDL(t.DDL)

	// Calculate indentation to align with table name
	indent := strings.Repeat(" ", m.maxTableNameLen+2) // +2 for ": "

	// Helper to write DDL with proper indentation and syntax highlighting
	// Truncates if too many lines
	writeDDL := func() {
		if ddlFormatted == "" {
			return
		}
		const maxLines = 5
		lines := strings.Split(ddlFormatted, "\n")
		for i, line := range lines {
			if i >= maxLines {
				fmt.Fprintf(&b, "%s... (%d more clauses)\n", indent, len(lines)-maxLines)
				break
			}
			fmt.Fprintf(&b, "%s%s\n", indent, templates.FormatSQL(line))
		}
	}

	// Determine display based on overall state first, then table status
	// Overall state takes priority to ensure consistent display during transitions
	switch {
	// Terminal states - check overall state first
	case state.IsState(m.state, StateCompleted):
		bar := ui.ProgressBarComplete()
		fmt.Fprintf(&b, "%*s: %s ✓ Complete\n", m.maxTableNameLen, t.Name, bar)
		writeDDL()

	// Stopped state - show tables with their actual status
	// Tables that completed before the stop should show as complete, not stopped
	case m.isEffectivelyStopped() && !state.IsState(m.state, StateCompleted):
		switch {
		case state.IsState(t.Status, StateCompleted):
			bar := ui.ProgressBarComplete()
			fmt.Fprintf(&b, "%*s: %s ✓ Complete\n", m.maxTableNameLen, t.Name, bar)
		case state.IsState(t.Status, StateFailed):
			bar := ui.ProgressBarFailed(t.Percent)
			fmt.Fprintf(&b, "%*s: %s ❌ Failed\n", m.maxTableNameLen, t.Name, bar)
		default:
			pct := t.Percent
			if pct == 0 && t.RowsTotal > 0 {
				pct = int(float64(t.RowsCopied) / float64(t.RowsTotal) * 100)
			}
			bar := ui.ProgressBarStopped(pct)
			if pct > 0 {
				fmt.Fprintf(&b, "%*s: %s ⏹️ Stopped at %d%%\n", m.maxTableNameLen, t.Name, bar, pct)
			} else {
				fmt.Fprintf(&b, "%*s: %s ⏹️ Stopped\n", m.maxTableNameLen, t.Name, bar)
			}
		}
		writeDDL()

	// Cutover states - all tables show same state during cutover
	// Also handle when cutover has been triggered but state hasn't updated yet
	case state.IsState(m.state, StateCuttingOver) || (m.cutoverTriggered && !state.IsState(m.state, StateCompleted)):
		bar := ui.ProgressBarWaitingCutover()
		fmt.Fprintf(&b, "%*s: %s 🔄 Cutting over...\n", m.maxTableNameLen, t.Name, bar)
		writeDDL()

	case state.IsState(m.state, StateWaitingForCutover):
		bar := ui.ProgressBarWaitingCutover()
		fmt.Fprintf(&b, "%*s: %s ⏸️ Waiting for cutover\n", m.maxTableNameLen, t.Name, bar)
		writeDDL()

	// Per-table states - check individual table status
	case state.IsState(t.Status, StateCompleted):
		bar := ui.ProgressBarComplete()
		fmt.Fprintf(&b, "%*s: %s ✓ Complete\n", m.maxTableNameLen, t.Name, bar)
		writeDDL()

	case state.IsState(t.Status, StateWaitingForCutover):
		bar := ui.ProgressBarWaitingCutover()
		fmt.Fprintf(&b, "%*s: %s ⏸️ Waiting for cutover\n", m.maxTableNameLen, t.Name, bar)
		writeDDL()

	case state.IsState(t.Status, StateCuttingOver):
		bar := ui.ProgressBarWaitingCutover()
		fmt.Fprintf(&b, "%*s: %s 🔄 Cutting over...\n", m.maxTableNameLen, t.Name, bar)
		writeDDL()

	case state.IsState(t.Status, StatePending):
		bar := ui.ProgressBarRowCopy(0)
		fmt.Fprintf(&b, "%*s: %s queued\n", m.maxTableNameLen, t.Name, bar)
		writeDDL()

	case state.IsState(t.Status, StateStopped):
		if t.Percent > 0 {
			bar := ui.ProgressBarStopped(t.Percent)
			fmt.Fprintf(&b, "%*s: %s ⏹️ Stopped\n", m.maxTableNameLen, t.Name, bar)
		} else {
			fmt.Fprintf(&b, "%*s: ⏹️ Stopped (not started)\n", m.maxTableNameLen, t.Name)
		}
		writeDDL()

	case state.IsState(t.Status, StateFailed):
		bar := ui.ProgressBarFailed(t.Percent)
		fmt.Fprintf(&b, "%*s: %s ❌ Failed\n", m.maxTableNameLen, t.Name, bar)
		writeDDL()

	default:
		// Running state - show progress bar with percentage
		etaSuffix := ""
		if t.ETA != "" && t.ETA != "TBD" {
			etaSuffix = fmt.Sprintf(" ETA %s", strings.TrimSpace(t.ETA))
		}

		if t.RowsTotal > 0 {
			bar := ui.ProgressBarRowCopy(t.Percent)
			fmt.Fprintf(&b, "%*s: %s %d%% (%s/%s rows)%s\n",
				m.maxTableNameLen, t.Name, bar, t.Percent,
				ui.FormatNumber(t.RowsCopied),
				ui.FormatNumber(t.RowsTotal),
				etaSuffix)
		} else {
			// No row data yet (initializing or instant DDL) — just show running
			fmt.Fprintf(&b, "%*s: Running...\n", m.maxTableNameLen, t.Name)
		}
		writeDDL()
	}

	return b.String()
}

// formatFooter returns the standard footer (no volume shown by default).
func (m WatchModel) formatFooter() string {
	dimStyle := lipgloss.NewStyle().Faint(true)
	return dimStyle.Render("ESC detach • " + templates.StopKeyHint + " • v volume")
}

// formatVolumeMode returns the footer when in volume adjustment mode.
func (m WatchModel) formatVolumeMode() string {
	var b strings.Builder
	dimStyle := lipgloss.NewStyle().Faint(true)

	// Simple volume display: just the number and a simple bar
	vol := max(min(m.currentVolume, 11), 1)

	// Simple bar using block characters
	filled := strings.Repeat("█", vol)
	empty := strings.Repeat("░", 11-vol)

	fmt.Fprintf(&b, "Volume: %s%s %d/11\n", filled, empty, vol)
	b.WriteString(dimStyle.Render("↑↓ adjust • 1-9 direct • ESC done"))
	return b.String()
}

// detachedView returns the message shown when user detaches.
func (m WatchModel) detachedView() string {
	var b strings.Builder
	b.WriteString("\n")
	b.WriteString("Detached from progress view.\n")
	b.WriteString("The schema change continues running in the background.\n")
	b.WriteString("\n")
	if m.applyID != "" {
		fmt.Fprintf(&b, "To watch and manage: schemabot progress --apply-id %s\n", m.applyID)
		fmt.Fprintf(&b, "To stop:             schemabot stop --apply-id %s\n", m.applyID)
	} else {
		fmt.Fprintf(&b, "To watch and manage: schemabot progress -d %s -e %s\n", m.database, m.environment)
		fmt.Fprintf(&b, "To stop:             schemabot stop -d %s -e %s\n", m.database, m.environment)
	}
	return b.String()
}
