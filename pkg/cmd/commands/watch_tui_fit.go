package commands

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"

	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/ui"
)

// fitToWindow joins a view's body and footer into a frame no taller than the
// terminal. The inline renderer keeps only a frame's last rows, so a frame
// taller than the window loses its top: the status line and the first tables
// scroll away and go stale on every repaint. Instead the body gives up its
// last lines for one that says how many are hidden and how to see them all,
// and the footer, which carries the keys and the next command, stays whole.
// A footer too tall to leave room for any body is shown as it is.
func (m WatchModel) fitToWindow(body, footer string) string {
	frame := body + footer
	// The renderer splits the frame on newlines, so a trailing newline is a
	// row of its own; counting the same way keeps the frame within the window.
	// It truncates a line wider than the window rather than wrapping it, so
	// every line is one row whatever its width.
	if m.windowHeight <= 0 || strings.Count(frame, "\n")+1 <= m.windowHeight {
		return frame
	}
	bodyLines := strings.Split(strings.TrimSuffix(body, "\n"), "\n")
	noteRows := m.hiddenLinesNoteRows()
	keep := m.windowHeight - footerRows(footer) - noteRows
	if keep < 1 {
		return frame
	}
	hidden := len(bodyLines) - keep
	fitted := strings.Join(bodyLines[:keep], "\n") + "\n" + m.hiddenLinesNote(hidden)
	if footer == "" {
		// The note is the frame's last row, so no newline follows it: one
		// would be a blank row the budget did not reserve.
		return fitted
	}
	return fitted + "\n" + footer
}

// footerRows is how many rows a footer takes below the hidden-lines note: none
// for a view that writes no footer, so its body keeps that row.
func footerRows(footer string) int {
	if footer == "" {
		return 0
	}
	return strings.Count(footer, "\n") + 1
}

// hiddenLinesNoteRows is how many rows hiddenLinesNote takes: one, plus one
// for the status command when the view names an apply.
func (m WatchModel) hiddenLinesNoteRows() int {
	if m.hasStatusCommand() {
		return 2
	}
	return 1
}

func (m WatchModel) hasStatusCommand() bool {
	return m.applyID != "" && m.environment != ""
}

// hiddenLinesNote says how many lines the window could not fit and how to see
// them: a taller window, or the status command, which prints the whole view.
// Once the watch has ended its last frame stays on screen and no longer
// redraws, so a taller window shows nothing more and only the command is
// offered. The command sits on a line of its own, the way the footer's
// commands do, so a narrow window that truncates the note's first line still
// shows it whole.
func (m WatchModel) hiddenLinesNote(hidden int) string {
	dimStyle := lipgloss.NewStyle().Faint(true)
	count := fmt.Sprintf("⋯ %d more %s.", hidden, ui.Pluralize("line", hidden))
	ended := m.watchHasEnded()
	switch {
	case !m.hasStatusCommand() && ended:
		return dimStyle.Render(count)
	case !m.hasStatusCommand():
		return dimStyle.Render(count + " Enlarge the window")
	case ended:
		return dimStyle.Render(count+" To see them all, run:") + "\n" + dimStyle.Render(m.statusCommand())
	default:
		return dimStyle.Render(count+" Enlarge the window, or run:") + "\n" + dimStyle.Render(m.statusCommand())
	}
}

func (m WatchModel) statusCommand() string {
	return fmt.Sprintf("  %s status %s -e %s", cliname.Name(), m.applyID, m.environment)
}
