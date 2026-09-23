package commands

import (
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/stretchr/testify/require"
)

func TestSampleChoiceAndCancellation(t *testing.T) {
	m := &initStartChoice{}
	m.Update(tea.KeyMsg{Type: tea.KeyDown})
	require.Equal(t, "mysql", m.engine())
	m.Update(tea.KeyMsg{Type: tea.KeyDown})
	require.Equal(t, "postgres", m.engine())
	m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	require.False(t, m.confirmed)
	m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	require.True(t, m.confirmed)
}

func TestSampleRejectsExistingConnections(t *testing.T) {
	for _, cmd := range []InitCmd{{Sample: true, DSN: "env:DATABASE_URL"}, {Sample: true, StorageDSN: "env:STATE"}, {Sample: true, Database: "real"}, {Sample: true, Integrated: true}, {Sample: true, Namespaces: []string{"real"}}, {Sample: true, Type: "vitess"}, {Sample: true, Environment: "production"}} {
		require.Error(t, cmd.prepareSample(t.Context(), &Globals{}))
	}
}
