package schema

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// postgresMaxIdentifierLength is PostgreSQL's NAMEDATALEN-1 limit: identifiers
// longer than this are silently truncated by the server, so index names must
// stay within it to keep names stable and collision-free.
const postgresMaxIdentifierLength = 63

func readSchemaDir(t *testing.T, dir string) map[string]string {
	t.Helper()

	var fsys = MySQLFS
	if dir == "postgres" {
		fsys = PostgresFS
	}
	entries, err := fsys.ReadDir(dir)
	require.NoError(t, err)
	require.NotEmpty(t, entries)

	files := make(map[string]string, len(entries))
	for _, entry := range entries {
		content, err := fsys.ReadFile(dir + "/" + entry.Name())
		require.NoError(t, err)
		files[entry.Name()] = string(content)
	}
	return files
}

// TestPostgresFilesMirrorMySQLFiles verifies the postgres schema directory
// contains exactly the same set of table files as the mysql directory, so a
// table added to one dialect cannot be silently missed in the other.
func TestPostgresFilesMirrorMySQLFiles(t *testing.T) {
	mysqlFiles := readSchemaDir(t, "mysql")
	postgresFiles := readSchemaDir(t, "postgres")

	mysqlNames := make([]string, 0, len(mysqlFiles))
	for name := range mysqlFiles {
		mysqlNames = append(mysqlNames, name)
	}
	postgresNames := make([]string, 0, len(postgresFiles))
	for name := range postgresFiles {
		postgresNames = append(postgresNames, name)
	}
	assert.ElementsMatch(t, mysqlNames, postgresNames)
}

// TestPostgresFilesContainNoMySQLSyntax lints every postgres schema file for
// MySQL-only constructs that PostgreSQL rejects or silently misinterprets.
func TestPostgresFilesContainNoMySQLSyntax(t *testing.T) {
	forbidden := []string{
		"`",
		"auto_increment",
		"on update current_timestamp",
		"engine=",
		"unsigned",
		"tinyint",
		"datetime",
		"charset",
		"collate",
	}

	for name, content := range readSchemaDir(t, "postgres") {
		lower := strings.ToLower(content)
		for _, token := range forbidden {
			assert.NotContains(t, lower, token, "%s contains MySQL-only construct %q", name, token)
		}
	}
}

// TestPostgresTableNamesMatchFilenames verifies each postgres file defines
// exactly one table named after the file, matching the mysql convention the
// schema bootstrapper relies on.
func TestPostgresTableNamesMatchFilenames(t *testing.T) {
	createTableRe := regexp.MustCompile(`(?m)^CREATE TABLE (\S+) \(`)

	for name, content := range readSchemaDir(t, "postgres") {
		matches := createTableRe.FindAllStringSubmatch(content, -1)
		require.Len(t, matches, 1, "%s must contain exactly one CREATE TABLE", name)
		assert.Equal(t, strings.TrimSuffix(name, ".sql"), matches[0][1], "%s table name must match filename", name)
	}
}

// TestPostgresIndexNames verifies index naming across the postgres schema.
// Unlike MySQL, PostgreSQL index names share one schema-wide namespace, so
// every index name must be globally unique, carry its table's name as a
// prefix, and fit within the identifier length limit.
func TestPostgresIndexNames(t *testing.T) {
	createIndexRe := regexp.MustCompile(`(?m)^CREATE (?:UNIQUE )?INDEX (\S+) ON (\S+) `)

	seen := make(map[string]string)
	for name, content := range readSchemaDir(t, "postgres") {
		table := strings.TrimSuffix(name, ".sql")
		for _, match := range createIndexRe.FindAllStringSubmatch(content, -1) {
			indexName, indexTable := match[1], match[2]
			assert.Equal(t, table, indexTable, "%s: index %s must target its own file's table", name, indexName)
			assert.True(t, strings.HasPrefix(indexName, "idx_"+table+"_"),
				"%s: index %s must be prefixed idx_%s_", name, indexName, table)
			assert.LessOrEqual(t, len(indexName), postgresMaxIdentifierLength,
				"%s: index %s exceeds the PostgreSQL identifier length limit", name, indexName)
			if prev, dup := seen[indexName]; dup {
				t.Errorf("index name %s in %s collides with %s (PostgreSQL index names are schema-wide)", indexName, name, prev)
			}
			seen[indexName] = name
		}
	}
	assert.NotEmpty(t, seen)
}
