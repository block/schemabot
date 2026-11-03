package client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

// A schema root whose environment directory is a symlink to a shared base
// plans the base for that environment, the schema a pull request plans.
func TestCallPlanAPIPlansSymlinkedEnvironmentSchemaRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "orders")
	writeTestFile(t, filepath.Join(root, "schemabot.yaml"), "database: orders\ntype: mysql\n")
	writeTestFile(t, filepath.Join(root, "base", "orders_001", "orders.sql"), "CREATE TABLE orders (id bigint primary key);\n")
	require.NoError(t, os.Symlink("base", filepath.Join(root, "production")))

	got := capturePlanRequest(t, root, "production")

	require.Contains(t, got.SchemaFiles, "orders_001")
	assert.Equal(t, "CREATE TABLE orders (id bigint primary key);\n", got.SchemaFiles["orders_001"].Files["orders.sql"])
	assert.Len(t, got.SchemaFiles, 1)
}

// An environment directory made of namespace symlinks into a shared tree
// lists the namespaces that environment has: staging plans both shards and
// production plans the one it holds, though the symlinks point outside their
// environment directory into the tree beside them.
func TestCallPlanAPIPlansEnvironmentDirectoryOfNamespaceSymlinks(t *testing.T) {
	root := writeNamespaceSymlinkEnvironmentRoots(t)

	staging := capturePlanRequest(t, root, "staging")
	assert.Equal(t, map[string]map[string]string{
		"app_staging":   {"items.sql": itemsSQL},
		"app_1_staging": {"items.sql": itemsSQL},
	}, planRequestFiles(staging))

	production := capturePlanRequest(t, root, "production")
	assert.Equal(t, map[string]map[string]string{
		"app_production": {"items.sql": itemsSQL},
	}, planRequestFiles(production))
}

// With no entry named for the environment, the config directory is the schema
// root for every environment.
func TestCallPlanAPIFallsBackToConfigDirectoryWithoutEnvironmentRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "orders")
	writeTestFile(t, filepath.Join(root, "schemabot.yaml"), "database: orders\ntype: mysql\n")
	writeTestFile(t, filepath.Join(root, "orders_001", "orders.sql"), "CREATE TABLE orders (id bigint primary key);\n")

	got := capturePlanRequest(t, root, "staging")

	assert.Equal(t, map[string]map[string]string{
		"orders_001": {"orders.sql": "CREATE TABLE orders (id bigint primary key);\n"},
	}, planRequestFiles(got))
}

// An environment symlink that leaves the config directory is refused before
// any file is read: the environment's schema would otherwise come from a tree
// its schemabot.yaml does not govern.
func TestCallPlanAPIRefusesEnvironmentSymlinkOutsideConfigDirectory(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "orders")
	writeTestFile(t, filepath.Join(root, "schemabot.yaml"), "database: orders\ntype: mysql\n")
	writeTestFile(t, filepath.Join(parent, "elsewhere", "orders_001", "orders.sql"), "CREATE TABLE orders (id bigint primary key);\n")
	require.NoError(t, os.Symlink(filepath.Join("..", "elsewhere"), filepath.Join(root, "production")))

	_, _, err := CallPlanAPI("http://unreachable.invalid", "orders", "mysql", "production", root, "", 0, PlanExclusions{}, false)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "points outside schema directory")
}

const itemsSQL = "CREATE TABLE items (id bigint primary key);\n"

// writeNamespaceSymlinkEnvironmentRoots lays out one shared table tree and two
// environment directories whose namespaces are symlinks into it:
//
//	app/
//	├── schemabot.yaml
//	├── tables/app_{env}/items.sql
//	├── staging/
//	│   ├── app_{env}   -> ../tables/app_{env}
//	│   └── app_1_{env} -> ../tables/app_{env}
//	└── production/
//	    └── app_{env}   -> ../tables/app_{env}
func writeNamespaceSymlinkEnvironmentRoots(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "app")
	writeTestFile(t, filepath.Join(root, "schemabot.yaml"), "database: app\ntype: mysql\n")
	writeTestFile(t, filepath.Join(root, "tables", "app_{env}", "items.sql"), itemsSQL)
	shared := filepath.Join("..", "tables", "app_{env}")
	require.NoError(t, os.MkdirAll(filepath.Join(root, "staging"), 0o755))
	require.NoError(t, os.Symlink(shared, filepath.Join(root, "staging", "app_{env}")))
	require.NoError(t, os.Symlink(shared, filepath.Join(root, "staging", "app_1_{env}")))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "production"), 0o755))
	require.NoError(t, os.Symlink(shared, filepath.Join(root, "production", "app_{env}")))
	return root
}

func planRequestFiles(req apitypes.PlanRequest) map[string]map[string]string {
	files := make(map[string]map[string]string, len(req.SchemaFiles))
	for namespace, schemaFiles := range req.SchemaFiles {
		files[namespace] = schemaFiles.Files
	}
	return files
}

func capturePlanRequest(t *testing.T, schemaDir, environment string) apitypes.PlanRequest {
	t.Helper()
	var got apitypes.PlanRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(apitypes.PlanResponse{}))
	}))
	t.Cleanup(server.Close)

	_, _, err := CallPlanAPI(server.URL, "orders", "mysql", environment, schemaDir, "", 0, PlanExclusions{}, false)
	require.NoError(t, err)
	return got
}

func writeTestFile(t *testing.T, filePath, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(filePath), 0o755))
	require.NoError(t, os.WriteFile(filePath, []byte(content), 0o644))
}
