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
	footerRows := strings.Count(footer, "\n") + 1
	noteRows := m.hiddenLinesNoteRows()
	keep := m.windowHeight - footerRows - noteRows
	if keep < 1 {
		return frame
	}
	hidden := len(bodyLines) - keep
	return strings.Join(bodyLines[:keep], "\n") + "\n" + m.hiddenLinesNote(hidden) + "\n" + footer
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
// The command sits on a line of its own, the way the footer's commands do, so
// a narrow window that truncates the note's first line still shows it whole.
func (m WatchModel) hiddenLinesNote(hidden int) string {
	dimStyle := lipgloss.NewStyle().Faint(true)
	note := fmt.Sprintf("⋯ %d more %s. Enlarge the window", hidden, ui.Pluralize("line", hidden))
	if !m.hasStatusCommand() {
		return dimStyle.Render(note)
	}
	command := fmt.Sprintf("  %s status %s -e %s", cliname.Name(), m.applyID, m.environment)
	return dimStyle.Render(note+", or run:") + "\n" + dimStyle.Render(command)
}
