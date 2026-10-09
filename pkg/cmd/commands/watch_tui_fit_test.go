package commands

import (
	"fmt"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/state"
)

// tallRollout is a running rollout of many deployments, each copying a table,
// so its view runs well past a short terminal.
func tallRollout(deployments int) WatchModel {
	tables := make([]templates.TableProgress, deployments)
	operations := make([]templates.ProgressOperation, deployments)
	for i := range deployments {
		name := fmt.Sprintf("shard_%02d", i)
		tables[i] = templates.TableProgress{TableName: "orders", Deployment: name, Status: state.Task.Running, RowsTotal: 100, RowsCopied: 50, PercentComplete: 50}
		operations[i] = templates.ProgressOperation{Deployment: name, State: state.ApplyOperation.Running}
	}
	return WatchModel{
		initialized: true,
		state:       state.Apply.Running,
		applyID:     "apply-abc123",
		environment: "staging",
		tables:      tables,
		operations:  operations,
	}
}

func frameRows(frame string) int {
	return strings.Count(frame, "\n") + 1
}

// An operator watching a rollout in a short terminal sees the top of the view,
// where the status line is, and the footer with the keys, with a note in
// between saying how many lines did not fit and how to print them all.
func TestWatchViewFitsTheWindow(t *testing.T) {
	m := tallRollout(12)
	full := m.View()
	require.Greater(t, frameRows(full), 20, "the fixture must be taller than the window")

	updated, _ := m.Update(tea.WindowSizeMsg{Width: 120, Height: 20})
	fitted := updated.(WatchModel).View()
	plain := stripANSI(fitted)

	assert.Equal(t, 20, frameRows(fitted))
	assert.Equal(t, strings.SplitN(stripANSI(full), "\n", 2)[0], strings.SplitN(plain, "\n", 2)[0], "the status line stays on top")
	assert.Contains(t, plain, "Apply ID: apply-abc123")
	assert.True(t, strings.HasSuffix(plain, "ESC to detach\n"), plain)

	_, footer := m.multiDeploymentProgressSections()
	assert.True(t, strings.HasSuffix(fitted, footer), "the footer stays whole")
	// The note takes two rows: the count, then the status command.
	hidden := frameRows(full) - frameRows(fitted) + 2
	assert.Contains(t, plain, fmt.Sprintf("⋯ %d more lines. Enlarge the window, or run:\n  %s status apply-abc123 -e staging\n", hidden, cliname.Name()))
}

// A view that fits, or one drawn before the terminal reports its size, is
// shown whole.
func TestWatchViewUntrimmedWhenItFits(t *testing.T) {
	m := tallRollout(12)
	full := m.View()

	m.windowHeight = frameRows(full)
	assert.Equal(t, full, m.View())

	m.windowHeight = 0
	assert.Equal(t, full, m.View())
}

// A window too short to hold the footer and a line of body shows the frame
// as it is rather than dropping the keys the operator needs.
func TestWatchViewUntrimmedWhenTheFooterFillsTheWindow(t *testing.T) {
	m := tallRollout(12)
	full := m.View()
	_, footer := m.multiDeploymentProgressSections()

	m.windowHeight = frameRows(footer) + m.hiddenLinesNoteRows()
	assert.Equal(t, full, m.View())

	m.windowHeight++
	assert.Equal(t, m.windowHeight, frameRows(m.View()), "one more row leaves room for a line of body")
}

// A rollout that finished while trimmed leaves its last frame on screen after
// the watch exits. That frame no longer redraws, so the note offers only the
// status command, which prints the whole view, and not a taller window.
func TestWatchViewEndedOffersOnlyTheStatusCommand(t *testing.T) {
	m := tallRollout(12)
	m.state = state.Apply.Completed
	full := m.View()
	m.windowHeight = 20

	fitted := m.View()
	plain := stripANSI(fitted)

	assert.Equal(t, 20, frameRows(fitted))
	hidden := frameRows(full) - frameRows(fitted) + 2
	assert.Contains(t, plain, fmt.Sprintf("⋯ %d more lines. To see them all, run:\n  %s status apply-abc123 -e staging\n", hidden, cliname.Name()))
	assert.NotContains(t, plain, "Enlarge the window")
}

// The note names the hidden lines in the singular or plural, offers a taller
// window only while the watch still redraws, and leaves out the status
// command when the view has no apply to name.
func TestHiddenLinesNote(t *testing.T) {
	cmd := "  " + cliname.Name() + " status apply-abc123 -e staging"
	cases := []struct {
		name  string
		model WatchModel
		want  string
	}{
		{name: "watching, no apply", model: WatchModel{state: state.Apply.Running}, want: "⋯ 1 more line. Enlarge the window"},
		{name: "ended, no apply", model: WatchModel{state: state.Apply.Failed}, want: "⋯ 1 more line."},
		{name: "watching", model: WatchModel{state: state.Apply.Running, applyID: "apply-abc123", environment: "staging"}, want: "⋯ 1 more line. Enlarge the window, or run:\n" + cmd},
		{name: "ended", model: WatchModel{state: state.Apply.Stopped, applyID: "apply-abc123", environment: "staging"}, want: "⋯ 1 more line. To see them all, run:\n" + cmd},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, stripANSI(tc.model.hiddenLinesNote(1)))
		})
	}
}

// A view that writes no footer spends no row on one: the body keeps that row
// and the note is the frame's last line, with no blank row below it.
func TestFitToWindowWithoutAFooter(t *testing.T) {
	m := WatchModel{state: state.Apply.Running, applyID: "apply-abc123", environment: "staging", windowHeight: 6}
	body := strings.Repeat("line\n", 10)

	fitted := m.fitToWindow(body, "")

	require.Equal(t, 6, frameRows(fitted))
	lines := strings.Split(stripANSI(fitted), "\n")
	assert.Equal(t, []string{"line", "line", "line", "line"}, lines[:4], "the body keeps every row the note does not take")
	assert.Equal(t, "  "+cliname.Name()+" status apply-abc123 -e staging", lines[5], "the note's command is the last row")
}
