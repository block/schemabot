package commands

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/client"
)

// fixableTable has an INT AUTO_INCREMENT primary key, which fix-lint rewrites
// to BIGINT and leaves no issue that needs a manual fix.
const fixableTable = "CREATE TABLE `users` (\n  `id` int NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n"

func writeSchemaFile(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(content)
}

// A flat schema directory has its .sql files fixed in place.
func TestFixLint_FlatLayout(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "app")
	usersPath := filepath.Join(dir, "users.sql")
	writeSchemaFile(t, usersPath, fixableTable)

	files, err := readSchemaFiles(dir)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"users.sql": fixableTable}, files)

	require.NoError(t, (&FixLintCmd{SchemaDir: dir}).Run(&Globals{}))

	fixed := strings.ToLower(readFile(t, usersPath))
	assert.Contains(t, fixed, "bigint")
	assert.Contains(t, fixed, "`id`")
	assert.NotContains(t, fixed, "`id` int ")
}

// A namespaced schema directory (one subdirectory per namespace) has the .sql
// files in every namespace fixed and written back to their own paths, while
// non-SQL schema files such as vschema.json are left untouched.
func TestFixLint_NamespacedLayout(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "app")
	ordersPath := filepath.Join(dir, "orders", "users.sql")
	billingPath := filepath.Join(dir, "billing", "users.sql")
	vschemaPath := filepath.Join(dir, "orders", "vschema.json")
	vschema := `{"sharded": false}`
	writeSchemaFile(t, ordersPath, fixableTable)
	writeSchemaFile(t, billingPath, fixableTable)
	writeSchemaFile(t, vschemaPath, vschema)

	files, err := readSchemaFiles(dir)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"orders/users.sql":  fixableTable,
		"billing/users.sql": fixableTable,
	}, files)

	require.NoError(t, (&FixLintCmd{SchemaDir: dir}).Run(&Globals{}))

	for _, p := range []string{ordersPath, billingPath} {
		fixed := strings.ToLower(readFile(t, p))
		assert.Contains(t, fixed, "bigint", p)
		assert.NotContains(t, fixed, "`id` int ", p)
	}
	assert.Equal(t, vschema, readFile(t, vschemaPath))
	assert.NoFileExists(t, filepath.Join(dir, "users.sql"))
}

// Dry-run on a namespaced layout reports the fixes without writing them.
func TestFixLint_NamespacedLayoutDryRun(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "app")
	ordersPath := filepath.Join(dir, "orders", "users.sql")
	writeSchemaFile(t, ordersPath, fixableTable)

	require.NoError(t, (&FixLintCmd{SchemaDir: dir, DryRun: true}).Run(&Globals{}))

	assert.Equal(t, fixableTable, readFile(t, ordersPath))
}

// Loose .sql files beside namespace subdirectories are a layout plan refuses
// to read, so fix-lint refuses it with plan's error and rewrites nothing.
func TestFixLint_MixedLayoutRejectedLikePlan(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "app")
	flatPath := filepath.Join(dir, "users.sql")
	ordersPath := filepath.Join(dir, "orders", "users.sql")
	writeSchemaFile(t, flatPath, fixableTable)
	writeSchemaFile(t, ordersPath, fixableTable)

	_, _, planErr := client.ReadSchemaFiles(dir, "", nil)
	require.Error(t, planErr)

	err := (&FixLintCmd{SchemaDir: dir}).Run(&Globals{})
	require.Error(t, err)
	assert.Equal(t, "read schema files: "+planErr.Error(), err.Error())
	assert.Contains(t, err.Error(), "both flat files and namespace subdirectories")
	assert.Equal(t, fixableTable, readFile(t, flatPath))
	assert.Equal(t, fixableTable, readFile(t, ordersPath))
}

// A schema directory with no .sql files is almost certainly a wrong path, so
// fix-lint fails instead of reporting success.
func TestFixLint_NoSQLFilesFails(t *testing.T) {
	dir := t.TempDir()
	writeSchemaFile(t, filepath.Join(dir, "README.md"), "not a schema")

	err := (&FixLintCmd{SchemaDir: dir}).Run(&Globals{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no .sql files found in "+dir)
}
