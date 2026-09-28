package commands

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/block/mysql"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestInitAdaptivePasteOnlySavesAfterConfirmation(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("DATABASE_URL", "")
	cmd := InitCmd{Type: "mysql", Database: "shop", Runtime: "demo", Namespaces: []string{"shop"}, SchemaDir: filepath.Join(t.TempDir(), "schema")}
	m := newInitWizard(&cmd, "demo", io.Discard)
	require.Equal(t, stepDSN, m.step)
	require.Contains(t, m.contentView(), "Paste a connection string")
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, textinput.EchoPassword, m.input.EchoMode)
	secret := "mysql://demo:very-secret@localhost:3306/shop"
	m.input.SetValue(secret)
	require.NotContains(t, m.View(), "very-secret")
	wizardKey(m, tea.KeyEnter)
	require.True(t, m.checkingConnection)
	require.NotContains(t, m.View(), "very-secret")
	m.Update(initConnectionMsg{generation: m.generation})
	wizardKey(m, tea.KeyEnter) // storage choice
	wizardKey(m, tea.KeyEnter) // integrated -> review
	require.Equal(t, len(m.fields), m.step)
	require.Contains(t, m.contentView(), "unencrypted")
	require.NotContains(t, m.contentView(), "very-secret")
	require.NoDirExists(t, filepath.Join(home, ".schemabot"))
	require.ErrorIs(t, m.copyToCommand(&cmd, &Globals{}), ErrSilent)
	wizardKey(m, tea.KeyEnter)
	require.NoError(t, m.copyToCommand(&cmd, &Globals{}))
	require.True(t, strings.HasPrefix(cmd.DSN, "file:"))
	dsn, err := resolveInitConnection(cmd.DSN)
	require.NoError(t, err)
	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)
	require.Equal(t, "very-secret", cfg.Passwd)
	path := strings.TrimPrefix(cmd.DSN, "file:")
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	ref, err := saveInitConnection("demo", "shop", "development", "application", dsn)
	require.NoError(t, err)
	require.Equal(t, cmd.DSN, ref)
	_, err = saveInitConnection("demo", "shop", "development", "application", "different")
	require.ErrorContains(t, err, "different connection")
}

func TestInitAdaptiveDetailsAndCancellation(t *testing.T) {
	for _, engine := range []string{"mysql", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			t.Setenv("DATABASE_URL", "")
			m := newInitWizard(&InitCmd{Type: engine, Database: "shop"}, "default", io.Discard)
			wizardKey(m, tea.KeyDown)
			wizardKey(m, tea.KeyEnter)
			for _, value := range []string{"localhost", "1234", "shop", "demo", "secret@:/"} {
				m.input.SetValue(value)
				wizardKey(m, tea.KeyEnter)
			}
			dsn, err := m.resolveConnection(m.input.Value())
			require.NoError(t, err)
			if engine == "mysql" {
				cfg, err := mysql.ParseDSN(dsn)
				require.NoError(t, err)
				require.Equal(t, "secret@:/", cfg.Passwd)
			} else {
				cfg, err := pgx.ParseConfig(dsn)
				require.NoError(t, err)
				require.Equal(t, "secret@:/", cfg.Password)
				require.Equal(t, "shop", cfg.Database)
			}
			require.NotContains(t, m.View(), "secret@:/")
			wizardKey(m, tea.KeyEsc)
			require.True(t, m.cancelled)
			require.NoDirExists(t, filepath.Join(home, ".schemabot"))
		})
	}
}

func TestInitConnectionFileAndPrivateStorage(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	p := filepath.Join(t.TempDir(), "connection")
	require.NoError(t, os.WriteFile(p, []byte("root@tcp(localhost:3306)/shop\n"), 0600))
	dsn, err := resolveInitConnection("file:" + p)
	require.NoError(t, err)
	require.Equal(t, "root@tcp(localhost:3306)/shop", dsn)
	require.NoError(t, os.MkdirAll(filepath.Join(home, ".schemabot"), 0700))
	require.NoError(t, os.Symlink(t.TempDir(), filepath.Join(home, ".schemabot", "credentials")))
	_, err = saveInitConnection("local", "shop", "dev", "application", dsn)
	require.ErrorContains(t, err, "not a symlink")
}

func TestInitConnectionScreenStates(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	m := newInitWizard(&InitCmd{Type: "mysql", Database: "shop"}, "default", io.Discard)
	wizardKey(m, tea.KeyEnter)
	m.input.SetValue("mysql://demo:secret@127.0.0.1:13361/shop")
	wizardKey(m, tea.KeyEnter)
	checking := stripANSI(m.View())
	require.Contains(t, checking, "Checking connection")
	require.NotContains(t, checking, "enter continue")
	require.NotContains(t, checking, "unencrypted")
	m.Update(initConnectionMsg{generation: m.generation, err: fmt.Errorf("could not connect: connection refused")})
	failed := stripANSI(m.View())
	require.Contains(t, failed, "Couldn’t connect.")
	require.Contains(t, failed, "enter retry")
	require.NotContains(t, failed, "VPN")
	require.NotContains(t, failed, "enter continue")
	require.Less(t, strings.Index(failed, "Couldn’t connect"), strings.Index(failed, "enter retry"))
	wizardKey(m, tea.KeyEnter)
	require.Empty(t, m.err)
	require.True(t, m.checkingConnection)
	require.NotContains(t, stripANSI(m.View()), "Couldn’t connect")
	m.Update(initConnectionMsg{generation: m.generation})
	connected := stripANSI(m.View())
	require.Contains(t, connected, "✓ Connected")
	require.NotContains(t, connected, "enter continue")
	require.NotContains(t, connected, "retry")
	require.NotContains(t, connected, "secret")
}

func TestInitPasteBackToConnectionChoices(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	m := newInitWizard(&InitCmd{Type: "mysql", Database: "shop"}, "default", io.Discard)
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, "paste", m.connectionEditor.mode)
	m.input.SetValue("mysql://demo:private-password@localhost/shop")
	require.Contains(t, stripANSI(m.View()), "shift+tab back")
	wizardKey(m, tea.KeyShiftTab)
	require.Equal(t, "menu", m.connectionEditor.mode)
	require.Equal(t, stepDSN, m.step)
	require.False(t, m.cancelled)
	require.NotContains(t, m.View(), "private-password")
	wizardKey(m, tea.KeyDown)
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, "details", m.connectionEditor.mode)
	require.Contains(t, m.View(), "Host")
}

func TestInitReferenceEntryCursorStartsAtEnd(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	m := newInitWizard(&InitCmd{Type: "mysql", Database: "shop"}, "default", io.Discard)
	wizardKey(m, tea.KeyDown)
	wizardKey(m, tea.KeyDown)
	wizardKey(m, tea.KeyEnter)
	require.Equal(t, "reference", m.connectionEditor.mode)
	require.Empty(t, m.input.Value())
	require.Equal(t, "env:DATABASE_URL", m.input.Placeholder)
	wizardKey(m, tea.KeyTab)
	require.Equal(t, "env:DATABASE_URL", m.input.Value())
	require.Equal(t, len([]rune(m.input.Value())), m.input.Position())
	require.NotContains(t, m.View(), "Choose a connection source")
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("_TEST")})
	require.Equal(t, "env:DATABASE_URL_TEST", m.input.Value())
}

func TestInitConnectionAutoAdvance(t *testing.T) {
	for _, action := range []string{"advance", "back", "cancel", "edit"} {
		t.Run(action, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "root@tcp(localhost:3306)/shop")
			m := newInitWizard(&InitCmd{Type: "mysql", Database: "shop"}, "default", io.Discard)
			wizardKey(m, tea.KeyEnter)
			generation := m.generation
			_, cmd := m.Update(initConnectionMsg{generation: generation})
			require.NotNil(t, cmd)
			require.Contains(t, stripANSI(m.View()), "✓ Connected")
			switch action {
			case "back":
				wizardKey(m, tea.KeyShiftTab)
			case "cancel":
				wizardKey(m, tea.KeyEsc)
			case "edit":
				m.connectionChecked = false
			}
			m.Update(initConnectionAdvanceMsg{generation: generation, step: 3})
			if action == "advance" {
				require.Equal(t, stepStorageDSN, m.step)
				require.Contains(t, stripANSI(m.View()), "✓ Database connected")
				require.Equal(t, "env:DATABASE_URL", m.fields[stepDSN].value)
				require.False(t, m.confirmed)
				m.Update(initConnectionAdvanceMsg{generation: generation, step: 3})
				require.Equal(t, stepStorageDSN, m.step)
			} else {
				require.Equal(t, stepDSN, m.step)
			}
		})
	}
}

func TestInitDetailsPlanetScaleTLS(t *testing.T) {
	for _, tc := range []struct {
		name, engine, host string
		wantTLS            bool
	}{
		{"vitess optimized", "vitess", "aws.connect.psdb.cloud", true},
		{"vitess direct", "vitess", "us-east.connect.psdb.cloud", true},
		{"mysql selection", "mysql", "aws.connect.psdb.cloud", true},
		{"case insensitive", "vitess", "AWS.CONNECT.PSDB.CLOUD", true},
		{"local vitess", "vitess", "localhost", false},
		{"local mysql", "mysql", "127.0.0.1", false},
		{"unrelated suffix", "vitess", "psdb.cloud.example.com", false},
		{"lookalike", "vitess", "notpsdb.cloud", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "")
			m := newInitWizard(&InitCmd{Type: tc.engine, Database: "shop"}, "default", io.Discard)
			wizardKey(m, tea.KeyDown)
			wizardKey(m, tea.KeyEnter)
			for _, value := range []string{tc.host, "3306", "shop", "demo", "secret@:/"} {
				m.input.SetValue(value)
				wizardKey(m, tea.KeyEnter)
			}
			dsn, err := m.resolveConnection(m.input.Value())
			require.NoError(t, err)
			cfg, err := mysql.ParseDSN(dsn)
			require.NoError(t, err)
			require.Equal(t, "secret@:/", cfg.Passwd)
			if tc.wantTLS {
				require.Equal(t, "true", cfg.TLSConfig)
				require.NotNil(t, cfg.TLS)
				require.False(t, cfg.TLS.InsecureSkipVerify)
				require.Equal(t, tc.host, cfg.TLS.ServerName)
				require.False(t, cfg.AllowFallbackToPlaintext)
			} else {
				require.Empty(t, cfg.TLSConfig)
			}
		})
	}
}

func TestInitAdaptivePlanetScaleToken(t *testing.T) {
	for _, details := range []bool{false, true} {
		t.Run(fmt.Sprint(details), func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			t.Setenv("PLANETSCALE_TOKEN", "")
			cmd := InitCmd{Type: "vitess", Database: "shop", Organization: "demo", Runtime: "demo", Namespaces: []string{"shop"}, SchemaDir: filepath.Join(t.TempDir(), "schema")}
			m := newInitWizard(&cmd, "demo", io.Discard)
			m.step = stepAPIToken
			m.loadField()
			if details {
				wizardKey(m, tea.KeyDown)
			}
			wizardKey(m, tea.KeyEnter)
			if details {
				m.input.SetValue("token-id")
				wizardKey(m, tea.KeyEnter)
			}
			require.Equal(t, textinput.EchoPassword, m.input.EchoMode)
			value := "token-id:private-secret"
			if details {
				value = "private-secret"
			}
			m.input.SetValue(value)
			require.NotContains(t, m.View(), "private-secret")
			wizardKey(m, tea.KeyEnter)
			require.True(t, m.checkingConnection)
			resolved, err := m.resolveConnection(m.input.Value())
			require.NoError(t, err)
			require.Equal(t, "token-id:private-secret", resolved)
			require.NotContains(t, m.View(), "private-secret")
			require.NoDirExists(t, filepath.Join(home, ".schemabot"))
			m.fields[stepAPIToken].value = m.input.Value()
			require.ErrorIs(t, m.copyToCommand(&cmd, &Globals{}), ErrSilent)
			m.confirmed = true
			require.NoError(t, m.copyToCommand(&cmd, &Globals{}))
			stored, err := resolveInitConnection(cmd.APIToken)
			require.NoError(t, err)
			require.Equal(t, resolved, stored)
			info, err := os.Stat(strings.TrimPrefix(cmd.APIToken, "file:"))
			require.NoError(t, err)
			require.Equal(t, os.FileMode(0600), info.Mode().Perm())
		})
	}
}

func TestInitConnectionDetailsSuggestions(t *testing.T) {
	for _, engine := range []string{"mysql", "postgres", "vitess"} {
		t.Run(engine, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "")
			m := newInitWizard(&InitCmd{Type: engine, Database: "shop"}, "demo", io.Discard)
			m.step = stepDSN
			m.loadField()
			wizardKey(m, tea.KeyDown)
			wizardKey(m, tea.KeyEnter)
			require.Empty(t, m.input.Value())
			require.Equal(t, "localhost", m.input.Placeholder)
			m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("db.example.com")})
			require.Equal(t, "db.example.com", m.input.Value())
			wizardKey(m, tea.KeyEnter)
			require.Empty(t, m.input.Value())
			port := "3306"
			if engine == "postgres" {
				port = "5432"
			}
			require.Equal(t, port, m.input.Placeholder)
			wizardKey(m, tea.KeyTab)
			require.Equal(t, port, m.input.Value())
			wizardKey(m, tea.KeyEnter)
			require.Empty(t, m.input.Placeholder)
		})
	}
}

func TestInitWizardSuggestionsPreserveEdits(t *testing.T) {
	m := newInitWizard(&InitCmd{}, "default", io.Discard)
	for _, step := range []int{stepEnvironment, stepSchemaDir, stepProfile} {
		m.step = step
		m.loadField()
		require.Empty(t, m.input.Value())
		require.Equal(t, m.suggestions[step], m.input.Placeholder)
		m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("custom")})
		require.Equal(t, "custom", m.input.Value())
		m.fields[step].value = m.input.Value()
		m.loadField()
		require.Equal(t, "custom", m.input.Value())
		require.Empty(t, m.input.Placeholder)
	}
}
