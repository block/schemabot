package commands

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"

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
	for _, baseline := range []*apitypes.PlanResponse{nil, {}, {PlanID: "plan", Errors: []string{"cannot verify"}}, {PlanID: "plan", Changes: []*apitypes.SchemaChangeResponse{{TableChanges: []*apitypes.TableChangeResponse{{TableName: "orders", ChangeType: "create", DDL: "CREATE TABLE orders (id bigint);"}}}}}} {
		root := filepath.Join(t.TempDir(), "schema")
		stage := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(stage, "table.sql"), []byte("CREATE TABLE t (id bigint);"), 0644))
		require.Error(t, publishVerifiedInitSchema(stage, root, baseline, "app", "dev"))
		require.NoDirExists(t, root)
		require.FileExists(t, filepath.Join(stage, "table.sql"))
	}
	stage := t.TempDir()
	require.NoError(t, os.Chmod(stage, 0700))
	root := filepath.Join(t.TempDir(), "schema")
	require.NoError(t, publishVerifiedInitSchema(stage, root, &apitypes.PlanResponse{PlanID: "plan"}, "app", "dev"))
	info, err := os.Stat(root)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0700), info.Mode().Perm())
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

func TestInitPublicationPreflight(t *testing.T) {
	require.NoError(t, checkInitPublication(t.TempDir(), renameInitSchema))
	unsupported := errors.New("operation not supported")
	require.ErrorContains(t, checkInitPublication(t.TempDir(), func(string, string) error { return unsupported }), "choose --schema-dir")
}

func TestInitEmptyDestinationAndUnrelatedFiles(t *testing.T) {
	parent := t.TempDir()
	stage, root := filepath.Join(parent, "stage"), filepath.Join(parent, "schema")
	require.NoError(t, os.Mkdir(stage, 0700))
	require.NoError(t, os.Mkdir(root, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(stage, "table.sql"), []byte("schema"), 0600))
	require.NoError(t, publishInitSchema(stage, root))
	require.NoError(t, os.Mkdir(stage, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(stage, "table.sql"), []byte("schema"), 0600))
	for _, name := range []string{"README.md", ".DS_Store"} {
		require.NoError(t, os.WriteFile(filepath.Join(root, name), []byte("keep"), 0600))
	}
	require.NoError(t, publishInitSchema(stage, root))
	for _, name := range []string{"README.md", ".DS_Store"} {
		data, err := os.ReadFile(filepath.Join(root, name))
		require.NoError(t, err)
		require.Equal(t, "keep", string(data))
	}
}

func TestInitRejectsOverridesBeforeSideEffects(t *testing.T) {
	for _, which := range []string{"endpoint", "token", "endpoint-env", "token-env", "target-literal", "storage-literal", "empty-reference", "profile"} {
		t.Run(which, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			t.Setenv("SCHEMABOT_ENDPOINT", "")
			t.Setenv("SCHEMABOT_TOKEN", "")
			cmd := InitCmd{Database: "app", Environment: "dev", Type: "mysql", DSN: "env:TARGET", StorageDSN: "env:STATE", Runtime: "local", Namespaces: []string{"app"}, SchemaDir: filepath.Join(home, "schema")}
			g := &Globals{Profile: "default"}
			want := "overrides"
			switch which {
			case "endpoint":
				g.Endpoint = "https://example.com"
			case "token":
				g.Token = "token"
			case "endpoint-env":
				t.Setenv("SCHEMABOT_ENDPOINT", "https://example.com")
			case "token-env":
				t.Setenv("SCHEMABOT_TOKEN", "token")
			case "target-literal":
				cmd.DSN = "literal"
				want = "env:VARIABLE"
			case "storage-literal":
				cmd.StorageDSN = "literal"
				want = "env:VARIABLE"
			case "empty-reference":
				cmd.DSN = "env:"
				want = "env:VARIABLE"
			case "profile":
				require.NoError(t, client.SaveConfig(&client.Config{Profiles: map[string]client.Profile{"default": {Endpoint: "https://example.com"}}}))
				want = "different connection"
			}
			_, err := cmd.initialize(t.Context(), g)
			require.ErrorContains(t, err, want)
			require.NoDirExists(t, cmd.SchemaDir)
			require.NoDirExists(t, filepath.Join(home, ".schemabot", "runtimes", "local"))
		})
	}
}
