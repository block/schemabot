package commands

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStageExistingInitSchemaPreservesAndChecksScope(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, "public"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "schemabot.yaml"), []byte("database: app\ntype: postgres\n"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "public", "tables.sql"), []byte("-- owned by the user\nCREATE TABLE widgets (id bigint);\n"), 0600))
	stage := t.TempDir()
	_, err := stageExistingInitSchema(root, stage, "app", "postgres", "development", []string{"public"})
	require.NoError(t, err)
	original, err := initSchemaSnapshot(root)
	require.NoError(t, err)
	copied, err := initSchemaSnapshot(stage)
	require.NoError(t, err)
	require.Equal(t, original, copied)
	_, err = stageExistingInitSchema(root, t.TempDir(), "app", "postgres", "development", []string{"other"})
	require.ErrorContains(t, err, "do not match selected scope")
	_, err = stageExistingInitSchema(root, t.TempDir(), "other", "postgres", "development", []string{"public"})
	require.ErrorContains(t, err, "different database or engine")
}
