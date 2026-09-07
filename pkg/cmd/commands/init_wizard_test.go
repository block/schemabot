package commands

import (
	"bytes"
	"context"
	"io"
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
	require.NoError(t, cmd.promptInputs(t.Context(), strings.NewReader(input), &output, g))
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
	err := cmd.collectInputs(t.Context(), &Globals{})
	require.ErrorContains(t, err, "--database")
	require.ErrorContains(t, err, "--namespace")
	cmd = InitCmd{SchemaDir: filepath.Join(t.TempDir(), "schema")}
	var output bytes.Buffer
	err = cmd.promptInputs(t.Context(), strings.NewReader("mysql\nshop\n\n\n\n\n\n\nn\n"), &output, &Globals{})
	require.ErrorContains(t, err, "nothing was initialized")
}

func TestInitWizardContextCancelsBlockedPrompt(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	input, writer := io.Pipe()
	t.Cleanup(func() { require.NoError(t, input.Close()); require.NoError(t, writer.Close()) })
	ctx, cancel := context.WithCancel(t.Context())
	cmd := InitCmd{}
	started := make(chan struct{}, 1)
	output := promptSignalWriter{started: started}
	done := make(chan error, 1)
	go func() { done <- cmd.promptInputs(ctx, input, output, &Globals{}) }()
	<-started
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

type promptSignalWriter struct{ started chan struct{} }

func (w promptSignalWriter) Write(p []byte) (int, error) {
	select {
	case w.started <- struct{}{}:
	default:
	}
	return len(p), nil
}
