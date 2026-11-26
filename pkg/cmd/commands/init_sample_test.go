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
	for _, cmd := range []InitCmd{{Sample: true, DSN: "env:DATABASE_URL"}, {Sample: true, StorageDSN: "env:STATE"}, {Sample: true, Database: "real"}, {Sample: true, Integrated: true}, {Sample: true, Namespaces: []string{"real"}}, {Sample: true, Type: "sqlite"}, {Sample: true, Environment: "production"}} {
		message := "--sample creates its own database and connections"
		if cmd.Type == "sqlite" {
			message = "choose --type mysql, --type postgres, or --type vitess"
		}
		if cmd.Environment == "production" {
			message = "sample databases use the development environment"
		}
		require.ErrorContains(t, cmd.prepareSample(t.Context(), &Globals{}), message)
	}
}

// The engine screen offers every engine a sample can be provisioned for, and
// the arrow keys wrap around that list rather than a fixed pair, so the last
// engine is reachable from the first with one press in either direction.
func TestSampleEngineChoiceWrapsAroundEveryEngine(t *testing.T) {
	m := &initStartChoice{}
	m.Update(tea.KeyMsg{Type: tea.KeyDown})
	m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	require.True(t, m.choosingEngine)
	require.Len(t, sampleEngines, m.choices())

	seen := []string{m.engine()}
	for range sampleEngines[1:] {
		m.Update(tea.KeyMsg{Type: tea.KeyDown})
		seen = append(seen, m.engine())
	}
	require.Equal(t, []string{"mysql", "postgres", "vitess"}, seen)

	m.Update(tea.KeyMsg{Type: tea.KeyDown})
	require.Equal(t, "mysql", m.engine(), "down from the last engine returns to the first")
	m.Update(tea.KeyMsg{Type: tea.KeyUp})
	require.Equal(t, "vitess", m.engine(), "up from the first engine reaches the last")

	m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	require.True(t, m.confirmed)
	require.True(t, m.sample)
	require.Equal(t, "vitess", m.engine())
}

// Sample setup keeps SchemaBot's own state in a database it can hold tables
// in. A Vitess target is reached through a PlanetScale-compatible API, so its
// state goes to MySQL rather than to the target.
func TestStorageDialectForTargetEngine(t *testing.T) {
	require.Equal(t, "mysql", storageDialect("mysql"))
	require.Equal(t, "postgres", storageDialect("postgres"))
	require.Equal(t, "mysql", storageDialect("vitess"))
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
