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

func TestCallPullSchemaAPI(t *testing.T) {
	var gotReq apitypes.PullSchemaRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/pull", r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotReq))

		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(apitypes.PullSchemaResponse{
			Database:    "orders",
			Type:        "mysql",
			Environment: "production",
			TableCount:  1,
			Namespaces: map[string]*apitypes.PulledNamespace{
				"orders": {Tables: map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"}},
			},
		}))
	}))
	t.Cleanup(server.Close)

	result, err := CallPullSchemaAPIWithOptions(server.URL, "orders", "mysql", "production", PullSchemaOptions{
		Namespaces:    []string{"orders_production", "orders_audit_production"},
		CatalogDetail: "detailed",
	})
	require.NoError(t, err)

	assert.Equal(t, apitypes.PullSchemaRequest{Database: "orders", Type: "mysql", Environment: "production", Namespaces: []string{"orders_production", "orders_audit_production"}, CatalogDetail: "detailed"}, gotReq)
	require.NotNil(t, result)
	assert.Equal(t, "orders", result.Database)
	assert.Equal(t, "mysql", result.Type)
	assert.Equal(t, "production", result.Environment)
	assert.Equal(t, int32(1), result.TableCount)
	assert.Equal(t, "CREATE TABLE `users` (`id` bigint NOT NULL);\n", result.Namespaces["orders"].Tables["users"])
}

func TestLogRequestQueryValuesAreEncoded(t *testing.T) {
	var requests []*http.Request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Clone(t.Context()))
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(`{"logs":[],"sources":[],"errors":[]}`))
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	_, err := GetLogs(server.URL, "", "", "apply/a & b", 17)
	require.NoError(t, err)
	_, err = GetDeploymentLogs(server.URL, "apply/a & b", "data plane/one", 23)
	require.NoError(t, err)
	require.Len(t, requests, 2)
	assert.Equal(t, "apply/a & b", requests[0].URL.Query().Get("apply_id"))
	assert.Equal(t, "17", requests[0].URL.Query().Get("limit"))
	assert.Equal(t, "apply/a & b", requests[1].URL.Query().Get("apply_id"))
	assert.Equal(t, "data plane/one", requests[1].URL.Query().Get("deployment"))
	assert.Equal(t, "23", requests[1].URL.Query().Get("limit"))
}

func TestCallPullSchemaAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte(`{"error":"database not configured"}`))
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	result, err := CallPullSchemaAPI(server.URL, "orders", "mysql", "production")
	require.Error(t, err)

	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "database not configured")
}

func TestListDatabases(t *testing.T) {
	var gotType string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/api/databases", r.URL.Path)
		gotType = r.URL.Query().Get("type")
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(apitypes.DatabaseListResponse{
			Databases: []*apitypes.DatabaseResponse{
				{
					Database: "orders",
					Type:     "mysql",
					Environments: []*apitypes.DatabaseEnvironmentResponse{
						{Environment: "production", Deployments: []string{"pie"}},
					},
				},
			},
		}))
	}))
	t.Cleanup(server.Close)

	result, err := ListDatabases(server.URL, ListDatabasesOptions{Type: "mysql"})
	require.NoError(t, err)

	assert.Equal(t, "mysql", gotType)
	require.NotNil(t, result)
	require.Len(t, result.Databases, 1)
	assert.Equal(t, "orders", result.Databases[0].Database)
	assert.Equal(t, "mysql", result.Databases[0].Type)
	assert.Equal(t, []string{"pie"}, result.Databases[0].Environments[0].Deployments)
}

func TestGetStatusWithOptions(t *testing.T) {
	var gotLimit, gotEnvironment, gotFailed string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotLimit = r.URL.Query().Get("limit")
		gotEnvironment = r.URL.Query().Get("environment")
		gotFailed = r.URL.Query().Get("failed")
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(`{"active_count":0,"limit":50,"applies":[]}`))
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	result, err := GetStatus(server.URL, StatusOptions{
		Limit:       50,
		Environment: "staging",
		Failed:      true,
	})
	require.NoError(t, err)

	assert.Equal(t, "50", gotLimit)
	assert.Equal(t, "staging", gotEnvironment)
	assert.Equal(t, "true", gotFailed)
	assert.Equal(t, 50, result.Limit)
}

func TestReadSchemaFiles_RegularDirectories(t *testing.T) {
	dir := t.TempDir()

	// Create two keyspace subdirectories with .sql files
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "ks_unsharded"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ks_unsharded", "users.sql"), []byte("CREATE TABLE users (id INT)"), 0o644))

	require.NoError(t, os.MkdirAll(filepath.Join(dir, "ks_sharded"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ks_sharded", "orders.sql"), []byte("CREATE TABLE orders (id INT)"), 0o644))

	result, err := ReadSchemaFiles(dir, "")
	require.NoError(t, err)

	require.Contains(t, result, "ks_unsharded")
	require.Contains(t, result, "ks_sharded")
	assert.Equal(t, "CREATE TABLE users (id INT)", result["ks_unsharded"].Files["users.sql"])
	assert.Equal(t, "CREATE TABLE orders (id INT)", result["ks_sharded"].Files["orders.sql"])
}

func TestReadSchemaFiles_SymlinkedDirectories(t *testing.T) {
	dir := t.TempDir()

	// Create a real keyspace directory
	realDir := filepath.Join(dir, "ks_real")
	require.NoError(t, os.MkdirAll(realDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(realDir, "users.sql"), []byte("CREATE TABLE users (id INT)"), 0o644))

	// Create a symlink to the real directory (simulates shared schema across keyspaces)
	symlinkDir := filepath.Join(dir, "ks_symlink")
	require.NoError(t, os.Symlink(realDir, symlinkDir))

	result, err := ReadSchemaFiles(dir, "")
	require.NoError(t, err)

	// Both the real directory and the symlink should be read
	require.Contains(t, result, "ks_real", "real directory should be read")
	require.Contains(t, result, "ks_symlink", "symlinked directory should be read")
	assert.Equal(t, "CREATE TABLE users (id INT)", result["ks_real"].Files["users.sql"])
	assert.Equal(t, "CREATE TABLE users (id INT)", result["ks_symlink"].Files["users.sql"])
}

func TestReadSchemaFiles_MixedRealAndSymlinked(t *testing.T) {
	dir := t.TempDir()

	// Real keyspace with its own schema
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "commerce"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "commerce", "settings.sql"), []byte("CREATE TABLE settings (id INT)"), 0o644))

	// Real sharded keyspace
	shardedDir := filepath.Join(dir, "commerce_sharded")
	require.NoError(t, os.MkdirAll(shardedDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(shardedDir, "orders.sql"), []byte("CREATE TABLE orders (id INT)"), 0o644))

	// Two more sharded keyspaces sharing the same schema via symlinks
	require.NoError(t, os.Symlink(shardedDir, filepath.Join(dir, "commerce_sharded_001")))
	require.NoError(t, os.Symlink(shardedDir, filepath.Join(dir, "commerce_sharded_002")))

	result, err := ReadSchemaFiles(dir, "")
	require.NoError(t, err)

	// All four keyspaces should be present
	require.Len(t, result, 4)
	require.Contains(t, result, "commerce")
	require.Contains(t, result, "commerce_sharded")
	require.Contains(t, result, "commerce_sharded_001")
	require.Contains(t, result, "commerce_sharded_002")

	// Symlinked keyspaces should have the same content as the real one
	assert.Equal(t, result["commerce_sharded"].Files["orders.sql"], result["commerce_sharded_001"].Files["orders.sql"])
	assert.Equal(t, result["commerce_sharded"].Files["orders.sql"], result["commerce_sharded_002"].Files["orders.sql"])
}

func TestReadSchemaFiles_SkipsNonSchemaFiles(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, os.MkdirAll(filepath.Join(dir, "mydb"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "mydb", "users.sql"), []byte("CREATE TABLE users (id INT)"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "mydb", "README.md"), []byte("ignore me"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "mydb", "vschema.json"), []byte("{}"), 0o644))

	result, err := ReadSchemaFiles(dir, "")
	require.NoError(t, err)

	require.Contains(t, result, "mydb")
	assert.Contains(t, result["mydb"].Files, "users.sql")
	assert.Contains(t, result["mydb"].Files, "vschema.json")
	assert.NotContains(t, result["mydb"].Files, "README.md")
}
