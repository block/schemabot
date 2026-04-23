package client

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadSchemaFiles_RegularDirectories(t *testing.T) {
	dir := t.TempDir()

	// Create two keyspace subdirectories with .sql files
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "ks_unsharded"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ks_unsharded", "users.sql"), []byte("CREATE TABLE users (id INT)"), 0o644))

	require.NoError(t, os.MkdirAll(filepath.Join(dir, "ks_sharded"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ks_sharded", "orders.sql"), []byte("CREATE TABLE orders (id INT)"), 0o644))

	result, err := ReadSchemaFiles(dir)
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

	result, err := ReadSchemaFiles(dir)
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
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "inventory2"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "inventory2", "settings.sql"), []byte("CREATE TABLE settings (id INT)"), 0o644))

	// Real sharded keyspace
	shardedDir := filepath.Join(dir, "inventory2_sharded")
	require.NoError(t, os.MkdirAll(shardedDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(shardedDir, "orders.sql"), []byte("CREATE TABLE orders (id INT)"), 0o644))

	// Two more sharded keyspaces sharing the same schema via symlinks
	require.NoError(t, os.Symlink(shardedDir, filepath.Join(dir, "inventory2_sharded_001")))
	require.NoError(t, os.Symlink(shardedDir, filepath.Join(dir, "inventory2_sharded_002")))

	result, err := ReadSchemaFiles(dir)
	require.NoError(t, err)

	// All four keyspaces should be present
	require.Len(t, result, 4)
	require.Contains(t, result, "inventory2")
	require.Contains(t, result, "inventory2_sharded")
	require.Contains(t, result, "inventory2_sharded_001")
	require.Contains(t, result, "inventory2_sharded_002")

	// Symlinked keyspaces should have the same content as the real one
	assert.Equal(t, result["inventory2_sharded"].Files["orders.sql"], result["inventory2_sharded_001"].Files["orders.sql"])
	assert.Equal(t, result["inventory2_sharded"].Files["orders.sql"], result["inventory2_sharded_002"].Files["orders.sql"])
}

func TestReadSchemaFiles_SkipsNonSchemaFiles(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, os.MkdirAll(filepath.Join(dir, "mydb"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "mydb", "users.sql"), []byte("CREATE TABLE users (id INT)"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "mydb", "README.md"), []byte("ignore me"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "mydb", "vschema.json"), []byte("{}"), 0o644))

	result, err := ReadSchemaFiles(dir)
	require.NoError(t, err)

	require.Contains(t, result, "mydb")
	assert.Contains(t, result["mydb"].Files, "users.sql")
	assert.Contains(t, result["mydb"].Files, "vschema.json")
	assert.NotContains(t, result["mydb"].Files, "README.md")
}
