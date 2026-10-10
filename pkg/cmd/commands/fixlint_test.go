package commands

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/schema"
)

// fixableTable has an INT AUTO_INCREMENT primary key, which fix-lint rewrites
// to BIGINT and leaves no issue that needs a manual fix.
const fixableTable = "CREATE TABLE `users` (\n  `id` int NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n"

// cleanTable is a lint-clean table in SHOW CREATE TABLE form, the form
// onboard writes, so fix-lint has nothing to fix in it.
const cleanTable = "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `email` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n"

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

// The fixer parses and restores with the MySQL grammar, under which simple
// PostgreSQL DDL parses and would be written back as backtick-quoted MySQL. A
// schema directory whose schemabot.yaml declares a non-MySQL type is refused
// before any file is read, while every MySQL-family type is still fixed.
func TestFixLint_DatabaseTypeGate(t *testing.T) {
	const pg = "CREATE TABLE users (\n    id bigint PRIMARY KEY,\n    email text\n);\n"

	t.Run("postgres directory is refused untouched", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "app")
		usersPath := filepath.Join(dir, "public", "users.sql")
		writeSchemaFile(t, filepath.Join(dir, "schemabot.yaml"), "database: app\ntype: postgres\n")
		writeSchemaFile(t, usersPath, pg)

		err := (&FixLintCmd{SchemaDir: dir}).Run(&Globals{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "MySQL-family schema directories only")
		assert.Contains(t, err.Error(), `type "postgres"`)
		assert.Equal(t, pg, readFile(t, usersPath))
	})

	for _, typ := range []string{"mysql", "vitess", "strata"} {
		t.Run(typ+" directory is fixed", func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "app")
			usersPath := filepath.Join(dir, "orders", "users.sql")
			writeSchemaFile(t, filepath.Join(dir, "schemabot.yaml"), "database: app\ntype: "+typ+"\n")
			writeSchemaFile(t, usersPath, fixableTable)

			require.NoError(t, (&FixLintCmd{SchemaDir: dir}).Run(&Globals{}))

			assert.Contains(t, strings.ToLower(readFile(t, usersPath)), "bigint")
		})
	}

	t.Run("broken schemabot.yaml is refused untouched", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "app")
		usersPath := filepath.Join(dir, "orders", "users.sql")
		writeSchemaFile(t, filepath.Join(dir, "schemabot.yaml"), "type: mysql\n")
		writeSchemaFile(t, usersPath, fixableTable)

		err := (&FixLintCmd{SchemaDir: dir}).Run(&Globals{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "database is required")
		assert.Equal(t, fixableTable, readFile(t, usersPath))
	})
}

// onboard writes an empty-namespace marker for a namespace with no tables;
// plan treats it as metadata, so fix-lint skips it and fixes the rest.
func TestFixLint_EmptyNamespaceMarkerSkipped(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "app")
	markerPath := filepath.Join(dir, "empty_ns", "schema.sql")
	writeSchemaFile(t, markerPath, schema.EmptyNamespaceDeclaration)
	usersPath := filepath.Join(dir, "orders", "users.sql")
	writeSchemaFile(t, usersPath, fixableTable)

	files, err := readSchemaFiles(dir)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"orders/users.sql": fixableTable}, files)

	require.NoError(t, (&FixLintCmd{SchemaDir: dir}).Run(&Globals{}))

	assert.NotEqual(t, fixableTable, readFile(t, usersPath))
	assert.Equal(t, schema.EmptyNamespaceDeclaration, readFile(t, markerPath))
}

// A namespace listed in ignore_namespaces is one plan never reads, so fix-lint
// leaves its files alone too.
func TestFixLint_IgnoredNamespaceLeftUntouched(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "app")
	writeSchemaFile(t, filepath.Join(dir, "schemabot.yaml"), "database: app\ntype: mysql\nignore_namespaces:\n  - localtest\n")
	ordersPath := filepath.Join(dir, "orders", "users.sql")
	writeSchemaFile(t, ordersPath, fixableTable)
	ignoredPath := filepath.Join(dir, "localtest", "users.sql")
	writeSchemaFile(t, ignoredPath, fixableTable)

	files, err := readSchemaFiles(dir)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"orders/users.sql": fixableTable}, files)

	require.NoError(t, (&FixLintCmd{SchemaDir: dir}).Run(&Globals{}))

	assert.NotEqual(t, fixableTable, readFile(t, ordersPath))
	assert.Equal(t, fixableTable, readFile(t, ignoredPath))
}

// A lint-clean file in SHOW CREATE TABLE form has nothing to fix, so fix-lint
// leaves it byte-for-byte as written and reports nothing to fix.
func TestFixLint_CleanNamespacedFileLeftUntouched(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "app")
	usersPath := filepath.Join(dir, "testapp", "users.sql")
	writeSchemaFile(t, usersPath, cleanTable)

	var runErr error
	out := captureStdout(func() {
		runErr = (&FixLintCmd{SchemaDir: dir}).Run(&Globals{})
	})
	require.NoError(t, runErr)

	assert.Equal(t, cleanTable, readFile(t, usersPath))
	assert.Equal(t, "✓ No lint issues found.\n", out)
}

// In a directory where one file needs a fix, fix-lint rewrites that file with
// the fix, lists only that file, and leaves every clean file byte-for-byte as
// written.
func TestFixLint_OnlyFileNeedingFixRewritten(t *testing.T) {
	const orders = "CREATE TABLE `orders` (\n  `id` int NOT NULL AUTO_INCREMENT,\n  `user_id` bigint unsigned NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n"
	dir := filepath.Join(t.TempDir(), "app")
	usersPath := filepath.Join(dir, "testapp", "users.sql")
	ordersPath := filepath.Join(dir, "testapp", "orders.sql")
	billingPath := filepath.Join(dir, "billing", "users.sql")
	writeSchemaFile(t, usersPath, cleanTable)
	writeSchemaFile(t, ordersPath, orders)
	writeSchemaFile(t, billingPath, cleanTable)

	var runErr error
	out := captureStdout(func() {
		runErr = (&FixLintCmd{SchemaDir: dir}).Run(&Globals{})
	})
	require.NoError(t, runErr)

	assert.Equal(t, "CREATE TABLE `orders` (`id` BIGINT NOT NULL AUTO_INCREMENT,`user_id` BIGINT UNSIGNED NOT NULL,PRIMARY KEY(`id`)) ENGINE = InnoDB DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_0900_AI_CI", readFile(t, ordersPath))
	assert.Equal(t, cleanTable, readFile(t, usersPath))
	assert.Equal(t, cleanTable, readFile(t, billingPath))
	assert.Equal(t, "✅ Fixed 1 issue(s):\n"+
		"  - [testapp/orders.sql] INT → BIGINT for primary key\n"+
		"\n"+
		"Run '"+cliname.Name()+" plan' to see full validation results.\n", out)
}
