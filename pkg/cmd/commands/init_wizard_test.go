package commands

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/stretchr/testify/require"
)

func wizardKey(m *initWizard, k tea.KeyType) { m.Update(tea.KeyMsg{Type: k}) }

func TestInitWizardNavigationAndValidation(t *testing.T) {
	t.Setenv("DATABASE_URL", "test-only")
	t.Setenv("SCHEMABOT_STORAGE_DSN", "test-only")
	m := newInitWizard(&InitCmd{}, "default", io.Discard)
	wizardKey(m, tea.KeyDown)
	require.Equal(t, "postgres", m.fields[0].value)
	wizardKey(m, tea.KeyEnter)
	m.input.SetValue("Bad Name")
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, 1, m.step)
	require.NotEmpty(t, m.err)
	m.input.SetValue("shop")
	wizardKey(m, tea.KeyEnter)
	wizardKey(m, tea.KeyShiftTab)
	require.Equal(t, "shop", m.input.Value())
	wizardKey(m, tea.KeyEnter)
	wizardKey(m, tea.KeyEnter)
	m.input.SetValue("postgres://secret")
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, 3, m.step)
	require.Contains(t, m.err, "env:VARIABLE")
	m.input.SetValue("env:DATABASE_URL")
	wizardKey(m, tea.KeyEnter)
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, "public", m.input.Value())
	for m.step < len(m.fields) {
		wizardKey(m, tea.KeyEnter)
	}
	require.False(t, m.confirmed)
	require.Contains(t, strings.Join(strings.Fields(m.View()), " "), "We won’t change your application’s schema")
	wizardKey(m, tea.KeyEnter)
	require.True(t, m.confirmed)
}
func TestInitWizardReviewExistingFilesAndCancel(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "users.sql")
	require.NoError(t, os.WriteFile(path, []byte("existing"), 0600))
	m := newInitWizard(&InitCmd{SchemaDir: root}, "default", io.Discard)
	m.step = len(m.fields)
	require.Contains(t, strings.Join(strings.Fields(m.View()), " "), "verify them and keep your edits")
	wizardKey(m, tea.KeyEsc)
	require.False(t, m.confirmed)
	require.True(t, m.cancelled)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "existing", string(data))
}
func TestInitWizardNonInteractive(t *testing.T) {
	cmd := InitCmd{NonInteractive: true}
	err := cmd.collectInputs(t.Context(), &Globals{})
	require.ErrorContains(t, err, "--database")
	require.ErrorContains(t, err, "--namespace")
}
func TestInitWizardMissingVariableAndNarrowTerminal(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	m := newInitWizard(&InitCmd{}, "default", io.Discard)
	m.step = 3
	m.loadField()
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, 3, m.step)
	require.Contains(t, m.err, "This variable is empty")
	m.Update(tea.WindowSizeMsg{Width: 40, Height: 24})
	require.NotEmpty(t, m.View())
}
func TestInitProgressCancellationWaitsForCleanup(t *testing.T) {
	cancelled := false
	m := &initProgress{cancel: func() { cancelled = true }}
	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyCtrlC})
	require.True(t, cancelled)
	require.True(t, m.stopping)
	require.Nil(t, cmd)
	require.Nil(t, m.finished)
	_, cmd = m.Update(initFinishedMsg{})
	require.NotNil(t, cmd)
}
func TestInitCompletionQuotesNextCommand(t *testing.T) {
	output := initCompletion(&initResult{SchemaDir: "my schema", Profile: "dev's profile"}, "development")
	require.Contains(t, output, "-s 'my schema'")
	require.Contains(t, output, "--profile 'dev'\"'\"'s profile'")
}

func TestInitWizardContextCancellationLeavesInputsUntouched(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	input, writer := io.Pipe()
	t.Cleanup(func() { require.NoError(t, input.Close()); require.NoError(t, writer.Close()) })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	output := initSignalWriter{started: make(chan struct{}, 1)}
	original := InitCmd{Database: "shop"}
	cmd := original
	done := make(chan error, 1)
	go func() { done <- cmd.promptInputs(ctx, input, output, &Globals{}) }()
	<-output.started
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Equal(t, original.Database, cmd.Database)
	require.Empty(t, cmd.Type)
}

type initSignalWriter struct{ started chan struct{} }

func (w initSignalWriter) Write(p []byte) (int, error) {
	select {
	case w.started <- struct{}{}:
	default:
	}
	return len(p), nil
}

func TestInitWizardNarrowReviewCanScroll(t *testing.T) {
	m := newInitWizard(&InitCmd{}, "default", io.Discard)
	m.step = len(m.fields)
	m.Update(tea.WindowSizeMsg{Width: 40, Height: 24})
	before := m.View()
	require.Contains(t, before, "scroll")
	for line := range strings.SplitSeq(before, "\n") {
		require.LessOrEqual(t, lipgloss.Width(line), 40)
	}
	wizardKey(m, tea.KeyDown)
	require.NotEqual(t, before, m.View())
	require.False(t, m.confirmed)
}
