package commands

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInitWizardCollectsInputsWithoutInitializing(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("SCHEMABOT_PROFILE", "")
	cmd := InitCmd{SchemaDir: filepath.Join(t.TempDir(), "schema")}
	var output bytes.Buffer
	g := &Globals{}
	input := "postgres\nshop\n\n\n\n\n\n\ny\n"
	require.NoError(t, cmd.promptInputs(strings.NewReader(input), &output, g))
	require.Empty(t, cmd.missingInputs())
	require.Equal(t, []string{"public"}, cmd.Namespaces)
	require.Equal(t, "env:DATABASE_URL", cmd.DSN)
	require.Equal(t, "default", g.Profile)
	require.Contains(t, output.String(), "No application schema changes will be applied")
	_, err := os.Stat(filepath.Join(home, ".schemabot"))
	require.True(t, os.IsNotExist(err))
}

func TestInitWizardCancellationAndNonInteractive(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	cmd := InitCmd{NonInteractive: true}
	err := cmd.collectInputs(&Globals{})
	require.ErrorContains(t, err, "--database")
	require.ErrorContains(t, err, "--namespace")
	cmd = InitCmd{SchemaDir: filepath.Join(t.TempDir(), "schema")}
	var output bytes.Buffer
	err = cmd.promptInputs(strings.NewReader("mysql\nshop\n\n\n\n\n\n\nn\n"), &output, &Globals{})
	require.ErrorContains(t, err, "nothing was initialized")
}
