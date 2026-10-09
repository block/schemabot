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

// The note names one hidden line in the singular and leaves out the status
// command when the view has no apply to name.
func TestHiddenLinesNote(t *testing.T) {
	assert.Equal(t, "⋯ 1 more line. Enlarge the window", stripANSI(WatchModel{}.hiddenLinesNote(1)))
}
