package commands

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/block/schemabot/pkg/apitypes"

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

func TestPublishInitRequiresVerifiedStoredBaseline(t *testing.T) {
	for _, baseline := range []*apitypes.PlanResponse{nil, {}, {PlanID: "plan", Errors: []string{"cannot verify"}}, {PlanID: "plan", Changes: []*apitypes.SchemaChangeResponse{{}}}} {
		root := filepath.Join(t.TempDir(), "schema")
		stage := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(stage, "table.sql"), []byte("CREATE TABLE t (id bigint);"), 0644))
		require.Error(t, publishVerifiedInitSchema(stage, root, baseline, "app", "dev"))
		require.NoDirExists(t, root)
		require.FileExists(t, filepath.Join(stage, "table.sql"))
	}
	stage := t.TempDir()
	root := filepath.Join(t.TempDir(), "schema")
	require.NoError(t, publishVerifiedInitSchema(stage, root, &apitypes.PlanResponse{PlanID: "plan"}, "app", "dev"))
	info, err := os.Stat(root)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0755), info.Mode().Perm())
}

func TestInitSnapshotRejectsOversizedDirectory(t *testing.T) {
	root := t.TempDir()
	f, err := os.Create(filepath.Join(root, "dump.sql"))
	require.NoError(t, err)
	require.NoError(t, f.Truncate(initSnapshotMaxBytes+1))
	require.NoError(t, f.Close())
	_, err = initSchemaSnapshot(root)
	require.ErrorContains(t, err, "choose a dedicated schema directory")
}
