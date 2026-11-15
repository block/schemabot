package commands

import (
	"io"
	"testing"

	"github.com/block/schemabot/pkg/cmd/client"
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

// An operator presses Ctrl-C or Esc on init's first screen, which asks whether
// to connect a database or try a sample. Nothing has been acted on, so the
// command says so and exits with the interrupt status.
func TestInitStartChoiceCancelKeyExitsInterrupted(t *testing.T) {
	for name, key := range map[string]string{"ctrl-c": "\x03", "esc": "\x1b"} {
		t.Run(name, func(t *testing.T) {
			input, writer := io.Pipe()
			t.Cleanup(func() { require.NoError(t, input.Close()); require.NoError(t, writer.Close()) })
			output := initSignalWriter{started: make(chan struct{}, 1)}
			done := make(chan error, 1)
			go func() {
				_, err := chooseInitStart(t.Context(), tea.WithInput(input), tea.WithOutput(output))
				done <- err
			}()
			awaitSignal(t, output.started, "the choice to be drawn")
			_, err := io.WriteString(writer, key)
			require.NoError(t, err)

			err = awaitSignal(t, done, "the choice to close")

			var status error
			stderr := captureStderr(t, func() { status = initStopped(err) })
			require.ErrorIs(t, status, ErrSilent)
			require.Equal(t, ExitInterrupted, ExitCodeFor(status))
			require.Equal(t, "Setup cancelled; nothing was changed.\n", stderr)
		})
	}
}

func TestSampleRejectsExistingConnections(t *testing.T) {
	for _, cmd := range []InitCmd{{Sample: true, DSN: "env:DATABASE_URL"}, {Sample: true, StorageDSN: "env:STATE"}, {Sample: true, Database: "real"}, {Sample: true, Integrated: true}, {Sample: true, Namespaces: []string{"real"}}, {Sample: true, Type: "vitess"}, {Sample: true, Environment: "production"}} {
		message := "--sample creates its own database and connections"
		if cmd.Type == "vitess" {
			message = "choose --type mysql or --type postgres"
		}
		if cmd.Environment == "production" {
			message = "sample databases use the development environment"
		}
		require.ErrorContains(t, cmd.prepareSample(t.Context(), &Globals{}), message)
	}
}

func TestInitAfterSampleUsesIndependentProfile(t *testing.T) {
	t.Setenv("SCHEMABOT_PROFILE", "")
	cfg := &client.Config{DefaultProfile: "sample", Profiles: map[string]client.Profile{"sample": {LocalRuntime: "schemabot-sample-example"}}}
	require.Equal(t, "local", initProfileName(cfg, "", "local"))
	require.Equal(t, "sample", initProfileName(cfg, "", "schemabot-sample-example"))
	require.Equal(t, "sample", initProfileName(cfg, "sample", "local"))
	t.Setenv("SCHEMABOT_PROFILE", "explicit")
	require.Equal(t, "explicit", initProfileName(cfg, "", "local"))
}
