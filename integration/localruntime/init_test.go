//go:build integration

package localruntime

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	runtimehost "github.com/block/schemabot/pkg/localruntime"
)

// Initialization runs the installed CLI, creates its own private runtime and
// profile, and verifies imported files on each supported native engine.
func TestInitEngines(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "schemabot")
	buildCtx, cancelBuild := context.WithTimeout(t.Context(), runtimeDeadline)
	defer cancelBuild()
	output, err := exec.CommandContext(buildCtx, "go", "build", "-o", binary, "../../pkg/cmd").CombinedOutput()
	require.NoError(t, err, string(output))
	for _, engine := range []string{"mysql", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			storageDSN, targetDSN, db := supervisorDatabase(t, engine)
			execSQL(t, db, "CREATE TABLE widgets (id bigint NOT NULL PRIMARY KEY, name text NOT NULL)")
			execSQL(t, db, "INSERT INTO widgets VALUES (1, 'keep me')")
			home := t.TempDir()
			root := filepath.Join(t.TempDir(), "nested", "schema")
			namespace := "app"
			if engine == "postgres" {
				namespace = "public"
			}
			manager := runtimehost.Manager{Dir: filepath.Join(home, ".schemabot", "runtimes", "local"), Binary: binary, Version: "dev"}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), runtimeDeadline)
				defer cancel()
				assert.NoError(t, manager.Stop(ctx))
			})
			run := func(args ...string) ([]byte, error) {
				ctx, cancel := context.WithTimeout(t.Context(), runtimeDeadline)
				defer cancel()
				cmd := exec.CommandContext(ctx, binary, args...)
				cmd.Env = append(os.Environ(), "HOME="+home, "SCHEMABOT_ENDPOINT=", "SCHEMABOT_TOKEN=", "SCHEMABOT_PROFILE=", "INIT_TARGET="+targetDSN, "INIT_STORAGE="+storageDSN)
				return cmd.CombinedOutput()
			}
			args := []string{"init", "--database", "app", "--environment", "development", "--type", engine, "--dsn", "env:INIT_TARGET", "--storage-dsn", "env:INIT_STORAGE", "--schema-dir", root, "--namespace", namespace, "--profile", "project", "--json"}
			// A fresh installation works with the normal default profile too.
			defaultArgs := slices.Clone(args)
			profileIndex := slices.Index(defaultArgs, "--profile")
			defaultArgs = append(defaultArgs[:profileIndex], defaultArgs[profileIndex+2:]...)
			output, err := run(defaultArgs...)
			require.NoError(t, err, string(output))
			output, err = run("databases")
			require.NoError(t, err, string(output))
			require.Contains(t, string(output), "app")
			for range 2 {
				output, err := run(args...)
				require.NoError(t, err, string(output))
				var result struct {
					Verified bool   `json:"verified"`
					PlanID   string `json:"plan_id"`
					Tables   int    `json:"tables"`
				}
				require.NoError(t, json.Unmarshal(output, &result), string(output))
				require.True(t, result.Verified)
				require.NotEmpty(t, result.PlanID)
				require.Equal(t, 1, result.Tables)
			}
			// A normal invocation resolves the saved connection after init exits.
			output, err = run("plan", "--profile", "project", "-e", "development", "-s", root, "--json")
			require.NoError(t, err, string(output))
			var plans map[string]apitypes.PlanResponse
			require.NoError(t, json.Unmarshal(output, &plans), string(output))
			require.Empty(t, plans["development"].Changes)
			require.NoError(t, os.WriteFile(filepath.Join(root, namespace, "notes.sql"), []byte("CREATE TABLE notes (id bigint NOT NULL PRIMARY KEY);"), 0600))
			output, err = run("plan", "--profile", "project", "-e", "development", "-s", root, "--json")
			require.NoError(t, err, string(output))
			require.NoError(t, json.Unmarshal(output, &plans), string(output))
			require.NotEmpty(t, plans["development"].Changes)
			// A rejected import never publishes a schema directory.
			rejectedRoot := filepath.Join(t.TempDir(), "schema")
			rejectedArgs := slices.Clone(args)
			rejectedArgs[slices.Index(rejectedArgs, "--schema-dir")+1] = rejectedRoot
			rejectedArgs[slices.Index(rejectedArgs, "--namespace")+1] = "missing_namespace"
			output, err = run(rejectedArgs...)
			require.Error(t, err, string(output))
			_, err = os.Stat(rejectedRoot)
			require.True(t, os.IsNotExist(err))
			config, err := os.ReadFile(filepath.Join(manager.Dir, "runtime.yaml"))
			require.NoError(t, err)
			require.Contains(t, string(config), "env:INIT_TARGET")
			require.NotContains(t, string(config), targetDSN)
			schemaPath := filepath.Join(root, namespace, "widgets.sql")
			schema, err := os.ReadFile(schemaPath)
			require.NoError(t, err)
			edited := []byte(string(schema) + "\n-- keep my edit\n")
			require.NoError(t, os.WriteFile(schemaPath, edited, 0600))
			output, err = run(args...)
			require.Error(t, err, string(output))
			require.Contains(t, string(output), "existing files were preserved")
			after, err := os.ReadFile(schemaPath)
			require.NoError(t, err)
			require.Equal(t, edited, after)
			ctx, cancel := context.WithTimeout(t.Context(), runtimeDeadline)
			defer cancel()
			var name string
			require.NoError(t, db.QueryRowContext(ctx, "SELECT name FROM widgets WHERE id = 1").Scan(&name))
			require.Equal(t, "keep me", name)
		})
	}
}
