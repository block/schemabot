package commands

import (
	"context"
	"errors"
	"fmt"
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

func TestInitRetryPreservesUnrelatedSubdirectory(t *testing.T) {
	root, stage := t.TempDir(), t.TempDir()
	for _, dir := range []string{root, stage} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "table.sql"), []byte("schema"), 0600))
	}
	require.NoError(t, os.Mkdir(filepath.Join(root, "docs"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "docs", "design.md"), []byte("keep"), 0600))
	require.NoError(t, publishInitSchema(stage, root))
	require.FileExists(t, filepath.Join(root, "docs", "design.md"))
}

func TestInitFailedConnectionDoesNotRegister(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("SCHEMABOT_ENDPOINT", "")
	t.Setenv("SCHEMABOT_TOKEN", "")
	t.Setenv("INIT_BAD_TARGET", "invalid-dsn")
	t.Setenv("INIT_STATE", "user@tcp(localhost:3306)/state")
	cmd := InitCmd{Database: "app", Environment: "dev", Type: "mysql", DSN: "env:INIT_BAD_TARGET", StorageDSN: "env:INIT_STATE", Runtime: "local", Namespaces: []string{"app"}, SchemaDir: filepath.Join(home, "schema")}
	_, err := cmd.initialize(t.Context(), &Globals{})
	require.ErrorContains(t, err, "before registering runtime")
	require.NoDirExists(t, filepath.Join(home, ".schemabot", "runtimes", "local"))
	require.NoDirExists(t, cmd.SchemaDir)
}

func TestInitReuseRejectsInvalidRootBeforeRegistration(t *testing.T) {
	for _, which := range []string{"missing", "empty", "wrong-scope", "wrong-database"} {
		t.Run(which, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			t.Setenv("SCHEMABOT_ENDPOINT", "")
			t.Setenv("SCHEMABOT_TOKEN", "")
			// Connections are deliberately unset: reuse validation must happen first.
			t.Setenv("INIT_REUSE_TARGET", "")
			t.Setenv("INIT_REUSE_STATE", "")
			root := filepath.Join(home, "schema")
			if which != "missing" {
				require.NoError(t, os.Mkdir(root, 0700))
			}
			if which == "wrong-scope" || which == "wrong-database" {
				require.NoError(t, os.WriteFile(filepath.Join(root, "schemabot.yaml"), []byte("database: app\ntype: mysql\n"), 0600))
				require.NoError(t, os.Mkdir(filepath.Join(root, "app"), 0700))
				require.NoError(t, os.WriteFile(filepath.Join(root, "app", "tables.sql"), []byte("CREATE TABLE t (id bigint);"), 0600))
			}
			cmd := InitCmd{ReuseSchema: true, Database: "app", Environment: "dev", Type: "mysql", DSN: "env:INIT_REUSE_TARGET", StorageDSN: "env:INIT_REUSE_STATE", Runtime: "local", Namespaces: []string{"app"}, SchemaDir: root}
			if which == "wrong-scope" {
				cmd.Namespaces = []string{"other"}
			}
			if which == "wrong-database" {
				cmd.Database = "other"
			}
			_, err := cmd.initialize(t.Context(), &Globals{})
			require.ErrorContains(t, err, "cannot reuse schema directory")
			require.NotContains(t, err.Error(), "connection environment")
			require.NoDirExists(t, filepath.Join(home, ".schemabot", "runtimes", "local"))
		})
	}
}

func TestInitCancellationPreservesCauseAndRecovery(t *testing.T) {
	err := retainedInitError(fmt.Errorf("import live schema: %w", context.Canceled))
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "setup cancelled")
	require.ErrorContains(t, err, "retained for retry")
	require.NotContains(t, err.Error(), "connection")
	err = retainedInitError(errors.New("schema files changed"))
	require.ErrorContains(t, err, "schema files changed")
	require.NotContains(t, err.Error(), "cancelled")
}
