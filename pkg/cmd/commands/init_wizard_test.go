package commands

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
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
	require.Contains(t, m.View(), "No application schema changes")
	wizardKey(m, tea.KeyEnter)
	require.True(t, m.confirmed)
}
func TestInitWizardReviewExistingFilesAndCancel(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "users.sql")
	require.NoError(t, os.WriteFile(path, []byte("existing"), 0600))
	m := newInitWizard(&InitCmd{SchemaDir: root}, "default", io.Discard)
	m.step = len(m.fields)
	require.Contains(t, m.View(), "verified and preserved")
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
	require.Contains(t, m.err, "Set this environment variable")
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
