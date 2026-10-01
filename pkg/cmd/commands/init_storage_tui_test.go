package commands

import (
	"io"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/stretchr/testify/require"
)

func TestInitStorageChoices(t *testing.T) {
	for _, engine := range []string{"mysql", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			cmd := InitCmd{Type: engine, Namespaces: []string{"app"}, SchemaDir: t.TempDir(), ReuseSchema: true}
			m := newInitWizard(&cmd, "default", io.Discard)
			m.step = stepStorageDSN
			m.loadField()
			require.True(t, m.integrated)
			require.Contains(t, m.contentView(), "└── schemabot (new database)")
			require.Contains(t, m.contentView(), "single database project")
			wizardKey(m, tea.KeyDown)
			require.False(t, m.integrated)
			require.Contains(t, m.contentView(), "Separate server")
			wizardKey(m, tea.KeyEnter)
			require.False(t, m.choosingStorage)
			require.Contains(t, m.contentView(), "Paste a connection string")
			m.loadField()
			wizardKey(m, tea.KeyUp)
			wizardKey(m, tea.KeyEnter)
			require.Equal(t, len(m.fields), m.step)
			require.False(t, m.confirmed)
			require.ErrorIs(t, m.copyToCommand(&cmd, &Globals{}), ErrSilent)
			require.False(t, cmd.Integrated)
			wizardKey(m, tea.KeyEnter)
			require.NoError(t, m.copyToCommand(&cmd, &Globals{}))
			require.True(t, cmd.Integrated)
			require.Empty(t, cmd.StorageDSN)
			require.True(t, cmd.ReuseSchema)
		})
	}
}

func TestInitVitessLocalStorageChoice(t *testing.T) {
	cmd := InitCmd{Type: "vitess", Database: "shop", Organization: "demo", Namespaces: []string{"shop"}, SchemaDir: t.TempDir()}
	m := newInitWizard(&cmd, "default", io.Discard)
	m.step = stepStorageDSN
	m.loadField()
	require.True(t, m.choosingStorage)
	require.Contains(t, m.View(), "Local (Docker)")
	require.Contains(t, m.View(), "PlanetScale")
	require.NotContains(t, m.View(), "Integrated")
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, stepAPIToken, m.step) // organization supplied via flag
	wizardKey(m, tea.KeyShiftTab)
	require.Equal(t, stepOrganization, m.step)
	wizardKey(m, tea.KeyShiftTab)
	require.Equal(t, stepStorageDSN, m.step)
	require.True(t, m.localStorage)
	wizardKey(m, tea.KeyDown)
	require.False(t, m.localStorage)
	wizardKey(m, tea.KeyEnter)
	require.Contains(t, m.View(), "Paste a connection string")
	m.localStorage = true
	m.confirmed = true
	require.NoError(t, m.copyToCommand(&cmd, &Globals{}))
	require.True(t, cmd.LocalStorage)
	require.False(t, cmd.Integrated)
	require.Empty(t, cmd.StorageDSN)
}
