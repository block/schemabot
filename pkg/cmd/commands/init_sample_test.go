package commands

import (
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/stretchr/testify/require"
)

func TestSampleChoiceAndCancellation(t *testing.T) {
	m := &initStartChoice{}
	m.Update(tea.KeyMsg{Type: tea.KeyDown})
	m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	require.False(t, m.confirmed)
	require.True(t, m.choosingEngine)
	require.Equal(t, "mysql", m.engine())
	m.Update(tea.KeyMsg{Type: tea.KeyDown})
	require.Equal(t, "postgres", m.engine())
	m.Update(tea.KeyMsg{Type: tea.KeyShiftTab})
	require.False(t, m.choosingEngine)
	m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m.Update(tea.KeyMsg{Type: tea.KeyDown})
	m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	require.True(t, m.confirmed)
	require.True(t, m.sample)
	require.Equal(t, "postgres", m.engine())

	cancelled := &initStartChoice{}
	cancelled.Update(tea.KeyMsg{Type: tea.KeyEsc})
	require.False(t, cancelled.confirmed)
	connect := &initStartChoice{}
	connect.Update(tea.KeyMsg{Type: tea.KeyEnter})
	require.True(t, connect.confirmed)
	require.False(t, connect.sample)
}

func TestSampleRejectsExistingConnections(t *testing.T) {
	for _, cmd := range []InitCmd{{Sample: true, DSN: "env:DATABASE_URL"}, {Sample: true, StorageDSN: "env:STATE"}, {Sample: true, Database: "real"}, {Sample: true, Integrated: true}, {Sample: true, Namespaces: []string{"real"}}, {Sample: true, Type: "vitess"}, {Sample: true, Environment: "production"}} {
		require.Error(t, cmd.prepareSample(t.Context(), &Globals{}))
	}
}
