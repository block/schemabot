package commands

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
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
	if len(m.operations) > 1 {
		return m.multiDeploymentProgressView()
	}

	return m.progressView()
}

// progressView renders the progress display.
func (m WatchModel) progressView() string {
	var b strings.Builder

	// Sort tables by status priority (running first, then pending, then completed)
	tables := make([]templates.TableProgress, len(m.tables))
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
	case m.stopTriggered && !state.IsState(m.state, state.Apply.Stopped, state.Apply.Cancelled):
		// Stop/cancel has been triggered but state hasn't updated yet
		if m.isPlanetScale() {
			b.WriteString(m.spinner.View() + "Cancelling...\n")
		} else {
			b.WriteString(m.spinner.View() + "Stopping...\n")
		}
	case effectivelyStopped:
		// Don't show status line - stopped message comes after tables
	case state.IsState(m.state, state.Apply.Running) && !m.cutoverTriggered && !m.deployTriggered:
		b.WriteString(m.spinner.View() + "Running...")
		b.WriteString("\n")
	case state.IsState(m.state, state.Apply.CatchingUp) && !m.cutoverTriggered && !m.deployTriggered:
		b.WriteString(m.spinner.View() + "Catching up on accumulated changes...\n")
	case state.IsState(m.state, state.Apply.Checksumming) && !m.cutoverTriggered && !m.deployTriggered:
		b.WriteString(m.spinner.View() + "Checksumming to verify data...\n")
	case state.IsState(m.state, state.Apply.PostChecksum) && !m.cutoverTriggered && !m.deployTriggered:
		b.WriteString(m.spinner.View() + "Data verified, applying final changes...\n")
	case state.IsState(m.state, state.Apply.WaitingForCutover):
		if m.cutoverTriggered {
			b.WriteString(m.spinner.View() + "Cutover triggered, waiting for completion...\n")
		}
	case state.IsState(m.state, state.Apply.Recovering):
		b.WriteString(m.spinner.View() + "Recovering state...\n")
	case state.IsState(m.state, state.Apply.CuttingOver):
		b.WriteString(m.spinner.View() + "Cutting over...\n")
	case state.IsState(m.state, state.Apply.Completed):
		// No status line for completed - just show completion message after tables
	case state.IsState(m.state, state.Apply.Stopped):
		// No status line - show stopped message after tables
	case state.IsState(m.state, state.Apply.PreparingBranch):
		label := "Preparing branch..."
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
	case state.IsState(m.state, state.Apply.ValidatingBranch):
		b.WriteString(m.spinner.View() + "Validating branch schema..." + m.elapsed() + "\n")
	case state.IsState(m.state, state.Apply.CreatingDeployRequest):
		msg := "Creating deploy request..."
		if m.deployRequestURL != "" {
			msg = fmt.Sprintf("Deploy request created  %s", m.deployRequestURL)
		}
		b.WriteString(m.spinner.View() + msg + m.elapsed() + "\n")
	case state.IsState(m.state, state.Apply.ValidatingDeployRequest):
		msg := "Validating deploy request..."
		if m.deployRequestURL != "" {
			msg = fmt.Sprintf("Validating deploy request  %s", m.deployRequestURL)
		}
		b.WriteString(m.spinner.View() + msg + m.elapsed() + "\n")
	case state.IsState(m.state, state.Apply.Pending) && !m.pastPending:
		b.WriteString(m.spinner.View() + "Starting...\n")
	}

	// Keep the PlanetScale deploy request link visible throughout the apply,
	// not only during deploy-request setup, so operators can open the console
	// to inspect a copy in flight. Setup phases already show it inline above;
	// terminal states surface it via the exit context.
	if !state.IsSetupPhase(m.state) && !state.IsTerminalApplyState(m.state) && m.metadata != nil {
		if url := m.metadata["deploy_request_url"]; url != "" {
			fmt.Fprintf(&b, "  Deploy Request:  %s\n", url)
		}
	}

	// Show table progress once past branch setup phases.
	if !state.IsSetupPhase(m.state) {
		m.renderTables(&b, tables)
	}

	// Footer based on state
	isCuttingOver := state.IsState(m.state, state.Apply.CuttingOver) || m.cutoverTriggered

	switch {
	case state.IsState(m.state, state.Apply.Completed):
		b.WriteString("\n\n")
		b.WriteString(templates.FormatApplyCompleteWithSummary(countTableProgressChanges(m.tables).summary(), m.applyID))
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
	case state.IsState(m.state, state.Apply.Reverting):
		b.WriteString("\n\n")
		b.WriteString(m.spinner.View() + "Reverting — undoing the schema change...\n")
	case state.IsState(m.state, state.Apply.RevertWindow, state.Apply.SkippingRevert):
		b.WriteString("\n\n")
		if state.IsState(m.state, state.Apply.SkippingRevert) || m.skipRevertTriggered || (m.metadata != nil && m.metadata["revert_skipped"] == "true") {
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
		if m.errorMsg != "" {
			errStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
			b.WriteString(errStyle.Render("Error: "+m.errorMsg) + "\n\n")
		}
		b.WriteString(templates.FormatApplyStopped())
		b.WriteString("\n")
		if m.applyID != "" {
			fmt.Fprintf(&b, "Use '%s start -e %s %s' to resume.\n", cliname.Name(), m.environment, m.applyID)
		} else {
			fmt.Fprintf(&b, "Use '%s status -d %s -e %s' to find the apply ID.\n", cliname.Name(), m.database, m.environment)
		}
	case isCuttingOver:
		// During cutover, show minimal footer - no detach/stop allowed
		b.WriteString("\n\n")
		dimStyle := lipgloss.NewStyle().Faint(true)
		b.WriteString(dimStyle.Render("Cutover in progress - please wait..."))
		b.WriteString("\n")
	case m.deployTriggered:
		b.WriteString("\n\n")
		b.WriteString(m.spinner.View() + "Deploying...\n")
	case state.IsState(m.state, state.Apply.WaitingForDeploy):
		b.WriteString("\n\n")
		if m.deployRequestURL != "" {
			b.WriteString("Deploy request created: " + m.deployRequestURL + "\n")
		} else {
			b.WriteString("Deploy request created.\n")
		}
		if m.metadata != nil && m.metadata["is_instant"] == "true" {
			b.WriteString("⚡ This change will be applied using instant mode.\n")
		}
		b.WriteString("\n")
		if m.allowControlActions {
			b.WriteString("Press Enter to deploy or proceed via the PlanetScale console (ESC to detach)\n")
		} else {
			if m.applyID != "" {
				fmt.Fprintf(&b, "To proceed: %s start -e %s %s\n", cliname.Name(), m.environment, m.applyID)
			} else {
				fmt.Fprintf(&b, "To find the apply ID: %s status -d %s -e %s\n", cliname.Name(), m.database, m.environment)
			}
			b.WriteString("Watching for deploy... (ESC to detach)\n")
		}
	case state.IsState(m.state, state.Apply.WaitingForCutover):
		b.WriteString("\n\n")
		b.WriteString("Row copy complete. All data has been copied and new writes\n")
		b.WriteString("continue to be replicated to keep the shadow table in sync.\n\n")
		if m.allowControlActions {
			b.WriteString("Press Enter to proceed with cutover (or ESC to detach)\n")
		} else {
			if m.applyID != "" {
				fmt.Fprintf(&b, "To proceed: %s cutover -e %s %s\n", cliname.Name(), m.environment, m.applyID)
			} else {
				fmt.Fprintf(&b, "To find the apply ID: %s status -d %s -e %s\n", cliname.Name(), m.database, m.environment)
			}
			b.WriteString("Watching for cutover... (ESC to detach)\n")
		}
	case state.IsState(m.state, state.Apply.Recovering):
		b.WriteString("\n\n")
		if pct, ok := recoveringCopyPercent(m.tables); ok {
			fmt.Fprintf(&b, "SchemaBot is recovering after restart.\nRow copy is in progress (%s); once recovery completes, progress returns to the normal row-copy view. (ESC to detach)\n", pct)
		} else {
			b.WriteString("SchemaBot is recovering after restart.\n")
			b.WriteString("Cutover will be available once recovery completes. (ESC to detach)\n")
		}
	case state.IsRunningApplyState(m.state):
		b.WriteString("\n\n")
		b.WriteString(m.formatFooter())
		b.WriteString("\n")
	case state.IsSetupPhase(m.state):
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

// recoveringCopyPercent returns the least-progressed recovering table's copy
// percent as display text, so the recovery footer never overstates how far
// the slowest table has come.
func recoveringCopyPercent(tables []templates.TableProgress) (string, bool) {
	fraction := 0.0
	text := ""
	found := false
	for _, table := range tables {
		if state.NormalizeTaskStatus(table.Status) != state.Task.Recovering || table.RowsTotal <= 0 || table.PercentComplete >= 100 {
			continue
		}
		if frac := ui.RowCopyFraction(table.PercentComplete, table.RowsCopied, table.RowsTotal); !found || frac < fraction {
			fraction = frac
			text = ui.FormatRowCopyPercent(table.PercentComplete, table.RowsCopied, table.RowsTotal)
		}
		found = true
	}
	return text, found
}

// renderTables renders tables with the shared FormatNamespacedTables /
// FormatTableProgress rendering from the CLI templates.
func (m WatchModel) renderTables(b *strings.Builder, tables []templates.TableProgress) {
	hasNamespaces := false
	for _, t := range tables {
		if t.Namespace != "" {
			hasNamespaces = true
			break
		}
	}

	activityBar := ui.ProgressBarActivity()
	activityLabelStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("14"))
	activityLabel := activityLabelStyle.Render("Finalizing copy " + activityLabelFrames[m.activityLabelFrame%len(activityLabelFrames)])
	if hasNamespaces {
		b.WriteString(templates.FormatNamespacedTablesWithActivity(tables, activityBar, activityLabel))
	} else {
		b.WriteString("\n")
		for _, t := range tables {
			b.WriteString(templates.FormatTableProgressWithActivity(t, activityBar, activityLabel))
		}
	}
}

// formatFooter returns the standard footer.
func (m WatchModel) formatFooter() string {
	dimStyle := lipgloss.NewStyle().Faint(true)
	stopHint := templates.StopKeyHint
	if m.isPlanetScale() {
		stopHint = "c cancel"
	}
	return dimStyle.Render("ESC detach • " + stopHint)
}

// isPlanetScale returns true if the current apply is using the PlanetScale engine.
func (m WatchModel) isPlanetScale() bool {
	return strings.EqualFold(m.engine, "PlanetScale") || strings.EqualFold(m.engine, "planetscale")
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
	if state.IsState(m.state, state.Apply.WaitingForDeploy) {
		b.WriteString("The deploy request is waiting for you to trigger it.\n")
	} else {
		b.WriteString("The schema change continues running in the background.\n")
	}
	b.WriteString("\n")
	if m.applyID != "" {
		fmt.Fprintf(&b, "To reattach: %s progress %s\n", cliname.Name(), m.applyID)
		if state.IsState(m.state, state.Apply.WaitingForDeploy) {
			fmt.Fprintf(&b, "To deploy:   %s start -e %s %s\n", cliname.Name(), m.environment, m.applyID)
		}
		fmt.Fprintf(&b, "To stop:     %s stop -e %s %s\n", cliname.Name(), m.environment, m.applyID)
	} else {
		fmt.Fprintf(&b, "Find apply:  %s status -d %s -e %s\n", cliname.Name(), m.database, m.environment)
	}
	return b.String()
}
