package commands

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/block/schemabot/pkg/cmd/templates"
	"github.com/block/schemabot/pkg/state"
)

// View implements tea.Model.
func (m WatchModel) View() string {
	if m.detached {
		return m.detachedView()
	}
	if m.quitting {
		return ""
	}

	// Show spinner until we have data. If there's a fetch error,
	// display it below the spinner so the user knows why it's still loading.
	if !m.initialized {
		s := m.spinner.View() + "Loading...\n"
		if m.errorMsg != "" {
			s += m.fetchErrorLine()
		}
		return s
	}

	// Permanent fetch error (not_found, invalid_request, etc.). Show and exit.
	if m.errorMsg != "" && m.state == "" {
		errStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
		return errStyle.Render("Error: "+m.errorMsg) + "\n"
	}

	// Handle no active schema change
	if state.IsState(m.state, state.NoActiveChange) {
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
	case m.stopTriggered && !state.IsState(m.state, state.Apply.Stopped, state.Apply.Cancelled):
		// Stop/cancel has been triggered but state hasn't updated yet
		if m.isPlanetScale() {
			b.WriteString(m.spinner.View() + "Cancelling...\n")
		} else {
			b.WriteString(m.spinner.View() + "Stopping...\n")
		}
	case effectivelyStopped:
		// Don't show status line - stopped message comes after tables
	case state.IsState(m.state, state.Apply.Running) && !m.cutoverTriggered:
		b.WriteString(m.spinner.View() + "Running...")
		b.WriteString("\n")
	case state.IsState(m.state, state.Apply.WaitingForCutover):
		if m.cutoverTriggered {
			b.WriteString(m.spinner.View() + "Cutover triggered, waiting for completion...\n")
		}
	case state.IsState(m.state, state.Apply.CuttingOver):
		b.WriteString(m.spinner.View() + "Cutting over...\n")
	case state.IsState(m.state, state.Apply.Completed):
		// No status line for completed - just show completion message after tables
	case state.IsState(m.state, state.Apply.Stopped):
		// No status line - show stopped message after tables
	case state.IsState(m.state, state.Apply.CreatingBranch):
		label := "Creating branch..."
		if m.metadata != nil && m.metadata["existing_branch"] != "" {
			label = "Refreshing branch schema..."
		}
		if m.metadata != nil && m.metadata["status_detail"] != "" {
			label = m.metadata["status_detail"]
		}
		b.WriteString(m.spinner.View() + label + m.elapsed() + "\n")
	case state.IsState(m.state, state.Apply.ApplyingBranchChanges):
		label := "Applying changes to branch..."
		if m.metadata != nil && m.metadata["status_detail"] != "" {
			label = m.metadata["status_detail"]
		}
		b.WriteString(m.spinner.View() + label + m.elapsed() + "\n")
	case state.IsState(m.state, state.Apply.CreatingDeployRequest):
		msg := "Creating deploy request..."
		if m.deployRequestURL != "" {
			msg = fmt.Sprintf("Deploy request created  %s", m.deployRequestURL)
		}
		b.WriteString(m.spinner.View() + msg + m.elapsed() + "\n")
	case state.IsState(m.state, state.Apply.Pending):
		b.WriteString(m.spinner.View() + "Starting...\n")
	}

	// Hide table list during setup phases — all tables are "Queued" and the
	// status line already shows per-keyspace progress.
	if !state.IsState(m.state, state.Apply.CreatingBranch, state.Apply.ApplyingBranchChanges, state.Apply.CreatingDeployRequest, state.Apply.Pending) {
		m.renderTables(&b, tables)
	}

	// Footer based on state
	isCuttingOver := state.IsState(m.state, state.Apply.CuttingOver) || m.cutoverTriggered

	switch {
	case state.IsState(m.state, state.Apply.Completed):
		b.WriteString("\n\n")
		b.WriteString(templates.FormatApplyComplete())
		b.WriteString("\n")
	case state.IsState(m.state, state.Apply.Failed):
		b.WriteString("\n\n")
		if m.errorMsg != "" {
			errStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
			b.WriteString(errStyle.Render("Error: "+m.errorMsg) + "\n\n")
		}
		b.WriteString(templates.FormatApplyFailed())
		b.WriteString("\n")
	case state.IsState(m.state, state.Apply.Cancelled):
		b.WriteString("\n\n")
		b.WriteString("🚫 Schema change cancelled.\n")
		b.WriteString("The deploy request has been cancelled. Start a new apply to retry.\n")
	case state.IsState(m.state, state.Apply.RevertWindow):
		b.WriteString("\n\n")
		if m.skipRevertTriggered || (m.metadata != nil && m.metadata["revert_skipped"] == "true") {
			elapsed := ""
			if !m.skipRevertAt.IsZero() {
				elapsed = fmt.Sprintf(" (%ds)", int(time.Since(m.skipRevertAt).Seconds()))
			}
			b.WriteString(m.spinner.View() + "Finalizing — closing revert window..." + elapsed + "\n")
		} else {
			b.WriteString("Schema change deployed. Revert window is open.\n\n")
			b.WriteString("Press Enter to skip revert or ESC to detach\n")
		}
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
	case state.IsState(m.state, state.Apply.WaitingForCutover):
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
	case state.IsState(m.state, state.Apply.Running):
		b.WriteString("\n\n")
		if m.volumeMode {
			// Volume mode - show volume adjustment UI
			b.WriteString(m.formatVolumeMode())
		} else {
			// Normal mode - simple footer without volume
			b.WriteString(m.formatFooter())
		}
		b.WriteString("\n")
	case state.IsState(m.state, state.Apply.CreatingBranch, state.Apply.ApplyingBranchChanges, state.Apply.CreatingDeployRequest):
		b.WriteString("\n\n")
		dimStyle := lipgloss.NewStyle().Faint(true)
		b.WriteString(dimStyle.Render("ESC to detach"))
		b.WriteString("\n")
	}

	// Fetch error during active progress (mid-flight). State and tables are
	// preserved from the last successful poll; the error tells the user
	// the server is currently unreachable.
	if m.errorMsg != "" && m.consecutiveErrors > 0 {
		b.WriteString("\n")
		b.WriteString(m.fetchErrorLine())
	}

	return b.String()
}

// fetchErrorLine formats the fetch error with consecutive failure count.
func (m WatchModel) fetchErrorLine() string {
	errStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
	label := "Error"
	if m.consecutiveErrors > 1 {
		label = fmt.Sprintf("Error (attempt %d)", m.consecutiveErrors)
	}
	return errStyle.Render(label+": "+m.errorMsg) + "\n"
}

// toTemplateTables converts TUI tableProgress slices to template TableProgress
// so the shared rendering functions can be used.
func toTemplateTables(tables []tableProgress) []templates.TableProgress {
	result := make([]templates.TableProgress, len(tables))
	for i, t := range tables {
		// Derive table-level ETA from max shard ETA for Vitess tables
		var etaSeconds int64
		for _, sh := range t.Shards {
			if sh.ETASeconds > etaSeconds {
				etaSeconds = sh.ETASeconds
			}
		}
		tp := templates.TableProgress{
			TableName:       t.Name,
			Namespace:       t.Keyspace,
			DDL:             t.DDL,
			Status:          state.NormalizeTaskStatus(t.Status),
			RowsCopied:      t.RowsCopied,
			RowsTotal:       t.RowsTotal,
			PercentComplete: t.Percent,
			ETASeconds:      etaSeconds,
			ProgressDetail:  t.ProgressDetail,
		}
		for _, sh := range t.Shards {
			tp.Shards = append(tp.Shards, templates.ShardProgress{
				Shard:      sh.Shard,
				Status:     state.NormalizeShardStatus(sh.Status),
				RowsCopied: sh.RowsCopied,
				RowsTotal:  sh.RowsTotal,
				ETASeconds: sh.ETASeconds,
			})
		}
		result[i] = tp
	}
	return result
}

// renderTables converts TUI tables to template types and uses the shared
// FormatNamespacedTables / FormatTableProgress rendering from the CLI templates.
func (m WatchModel) renderTables(b *strings.Builder, tables []tableProgress) {
	tplTables := toTemplateTables(tables)

	hasNamespaces := false
	for _, t := range tplTables {
		if t.Namespace != "" {
			hasNamespaces = true
			break
		}
	}

	if hasNamespaces {
		b.WriteString(templates.FormatNamespacedTables(tplTables))
	} else {
		b.WriteString("\n")
		for _, t := range tplTables {
			b.WriteString(templates.FormatTableProgress(t))
		}
	}
}

// formatFooter returns the standard footer (no volume shown by default).
func (m WatchModel) formatFooter() string {
	dimStyle := lipgloss.NewStyle().Faint(true)
	stopHint := templates.StopKeyHint
	if m.isPlanetScale() {
		stopHint = "c cancel"
	}
	return dimStyle.Render("ESC detach • " + stopHint + " • v volume")
}

// isPlanetScale returns true if the current apply is using the PlanetScale engine.
func (m WatchModel) isPlanetScale() bool {
	return strings.EqualFold(m.engine, "PlanetScale") || strings.EqualFold(m.engine, "planetscale")
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

// elapsed returns a formatted elapsed time string for the status line.
// Shows nothing for the first few seconds to avoid visual noise.
func (m WatchModel) elapsed() string {
	if m.startedAt.IsZero() {
		return ""
	}
	d := time.Since(m.startedAt).Round(time.Second)
	if d < 3*time.Second {
		return ""
	}
	return fmt.Sprintf(" (%s)", d)
}

// detachedView returns the message shown when user detaches.
func (m WatchModel) detachedView() string {
	var b strings.Builder
	b.WriteString("\n")
	b.WriteString("Detached from progress view.\n")
	b.WriteString("The schema change continues running in the background.\n")
	b.WriteString("\n")
	if m.applyID != "" {
		fmt.Fprintf(&b, "To reattach: schemabot progress %s\n", m.applyID)
		fmt.Fprintf(&b, "To stop:     schemabot stop %s\n", m.applyID)
	} else {
		fmt.Fprintf(&b, "To reattach: schemabot progress -d %s -e %s\n", m.database, m.environment)
		fmt.Fprintf(&b, "To stop:     schemabot stop -d %s -e %s\n", m.database, m.environment)
	}
	return b.String()
}
