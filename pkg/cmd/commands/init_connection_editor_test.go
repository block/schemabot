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
	require.Equal(t, 3, m.step)
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
	require.Contains(t, connected, "enter continue")
	require.NotContains(t, connected, "retry")
	require.NotContains(t, connected, "secret")
}
