package commands

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPublishInitSchemaPreservesExistingFiles(t *testing.T) {
	parent := t.TempDir()
	stage, root := filepath.Join(parent, "stage"), filepath.Join(parent, "schema")
	require.NoError(t, os.MkdirAll(stage, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(stage, "schemabot.yaml"), []byte("verified"), 0600))
	require.NoError(t, publishInitSchema(stage, root))
	require.NoError(t, os.MkdirAll(stage, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(stage, "schemabot.yaml"), []byte("verified"), 0600))
	require.NoError(t, publishInitSchema(stage, root))
	require.NoError(t, os.WriteFile(filepath.Join(root, "schemabot.yaml"), []byte("user edits"), 0600))
	require.ErrorContains(t, publishInitSchema(stage, root), "existing files were preserved")
	data, err := os.ReadFile(filepath.Join(root, "schemabot.yaml"))
	require.NoError(t, err)
	require.Equal(t, "user edits", string(data))
}

func TestPublishInitSchemaRefusesSymlinkDestination(t *testing.T) {
	parent := t.TempDir()
	stage, root := filepath.Join(parent, "stage"), filepath.Join(parent, "schema")
	require.NoError(t, os.Mkdir(stage, 0700))
	require.NoError(t, os.Symlink(stage, root))
	require.ErrorContains(t, publishInitSchema(stage, root), "symlinks")
	info, err := os.Lstat(root)
	require.NoError(t, err)
	require.NotZero(t, info.Mode()&os.ModeSymlink)
}

func TestPublishInitSchemaReportsUnsupportedFilesystem(t *testing.T) {
	unsupported := errors.New("filesystem does not support exclusive rename")
	root := filepath.Join(t.TempDir(), "new-schema")
	err := publishInitSchemaWithRename(t.TempDir(), root, func(string, string) error { return unsupported })
	require.ErrorIs(t, err, unsupported)
	require.NotContains(t, err.Error(), "no such file")
	require.NoDirExists(t, root)
	// Even an identical existing directory must not hide other rename failures.
	existing := t.TempDir()
	err = publishInitSchemaWithRename(existing, existing, func(string, string) error { return unsupported })
	require.ErrorIs(t, err, unsupported)
}
