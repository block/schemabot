package schema

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/statement"
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
// MySQL constructs that must not appear in the postgres DDL. Most are syntax
// PostgreSQL rejects outright; charset and collate are policy bans — the
// storage DDL uses the database's default collation, and any collation
// decision (e.g. case-insensitive matching) belongs to the store layer where
// its query semantics can be tested, not hidden in the bootstrap DDL.
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

// TestPostgresFilesContainOnlyPlainStatements pins the PostgresFS invariant
// that every file holds plain semicolon-separated statements: no SQL comments,
// dollar-quoted blocks, or E-strings. The statement-level lints in this
// package (and the format assumptions in the bootstrap) parse the files
// assuming exactly this shape, so a stray comment header would silently
// escape them.
func TestPostgresFilesContainOnlyPlainStatements(t *testing.T) {
	// \W keeps a closing quote after a word ending in "e" (e.g. 'none') from
	// matching; a real E-string is always preceded by a non-word character.
	eStringRe := regexp.MustCompile(`(?i)(^|\W)e'`)

	for name, content := range readSchemaDir(t, "postgres") {
		assert.NotContains(t, content, "--", "%s contains a SQL comment", name)
		assert.NotContains(t, content, "/*", "%s contains a SQL comment", name)
		assert.NotContains(t, content, "$$", "%s contains a dollar-quoted block", name)
		assert.NotRegexp(t, eStringRe, content, "%s contains an E-string literal", name)
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

// postgresIndex is one CREATE INDEX statement parsed from a postgres schema file.
type postgresIndex struct {
	file    string
	name    string
	table   string
	unique  bool
	columns []string
}

var postgresCreateIndexRe = regexp.MustCompile(`(?m)^CREATE (UNIQUE )?INDEX (\S+) ON (\S+) \(([^)]+)\);`)

// readPostgresIndexes parses every CREATE INDEX statement in the postgres
// schema directory. The parse count per file is cross-checked against literal
// CREATE INDEX occurrences, so an index written in a format the regex does
// not recognize fails loudly instead of silently escaping the schema lints.
func readPostgresIndexes(t *testing.T) []postgresIndex {
	t.Helper()

	var indexes []postgresIndex
	for name, content := range readSchemaDir(t, "postgres") {
		matches := postgresCreateIndexRe.FindAllStringSubmatch(content, -1)
		literal := strings.Count(content, "CREATE INDEX ") + strings.Count(content, "CREATE UNIQUE INDEX ")
		require.Len(t, matches, literal, "%s: every CREATE INDEX must be a single-line statement of the form CREATE [UNIQUE] INDEX name ON table (cols);", name)
		for _, match := range matches {
			columns := strings.Split(match[4], ",")
			for i, col := range columns {
				columns[i] = strings.TrimSpace(col)
			}
			indexes = append(indexes, postgresIndex{
				file:    name,
				name:    match[2],
				table:   match[3],
				unique:  match[1] != "",
				columns: columns,
			})
		}
	}
	require.NotEmpty(t, indexes)
	return indexes
}

// TestPostgresIndexNames verifies index naming across the postgres schema.
// Unlike MySQL, PostgreSQL index names share one schema-wide namespace, so
// every index name must be globally unique, carry its table's name as a
// prefix, and fit within the identifier length limit.
func TestPostgresIndexNames(t *testing.T) {
	seen := make(map[string]string)
	for _, idx := range readPostgresIndexes(t) {
		table := strings.TrimSuffix(idx.file, ".sql")
		assert.Equal(t, table, idx.table, "%s: index %s must target its own file's table", idx.file, idx.name)
		assert.True(t, strings.HasPrefix(idx.name, "idx_"+table+"_"),
			"%s: index %s must be prefixed idx_%s_", idx.file, idx.name, table)
		assert.LessOrEqual(t, len(idx.name), postgresMaxIdentifierLength,
			"%s: index %s exceeds the PostgreSQL identifier length limit", idx.file, idx.name)
		if prev, dup := seen[idx.name]; dup {
			t.Errorf("index name %s in %s collides with %s (PostgreSQL index names are schema-wide)", idx.name, idx.file, prev)
		}
		seen[idx.name] = idx.file
	}
}

// indexShape is the dialect-neutral identity of an index: the table it
// covers, its ordered column list, and whether it is unique. Names are
// excluded — they legitimately differ across dialects (postgres names are
// table-prefixed and occasionally renamed for clarity) and are linted by
// TestPostgresIndexNames instead.
func indexShape(table string, unique bool, columns []string) string {
	kind := "INDEX"
	if unique {
		kind = "UNIQUE"
	}
	return fmt.Sprintf("%s %s (%s)", kind, table, strings.Join(columns, ", "))
}

// TestPostgresIndexesMirrorMySQLIndexes verifies every MySQL index has a
// postgres counterpart on the same table with the same ordered column list
// and the same uniqueness, and that postgres defines no extra indexes. The
// PostgreSQL store's ON CONFLICT upserts and duplicate-rejection guarantees
// depend on the unique indexes existing with exactly the MySQL semantics, so
// a dropped or de-uniquified index must fail here rather than surface as a
// runtime error or silent duplicate acceptance.
func TestPostgresIndexesMirrorMySQLIndexes(t *testing.T) {
	pgShapes := make([]string, 0)
	for _, idx := range readPostgresIndexes(t) {
		pgShapes = append(pgShapes, indexShape(idx.table, idx.unique, idx.columns))
	}

	mysqlShapes := make([]string, 0, len(pgShapes))
	for name, content := range readSchemaDir(t, "mysql") {
		parsed, err := statement.ParseCreateTable(content)
		require.NoError(t, err, "parse mysql schema file %s", name)
		for _, idx := range parsed.Indexes {
			if idx.Type == "PRIMARY KEY" {
				continue
			}
			mysqlShapes = append(mysqlShapes, indexShape(parsed.TableName, idx.Type == "UNIQUE", idx.Columns))
		}
	}

	assert.ElementsMatch(t, mysqlShapes, pgShapes,
		"index sets differ between dialects: every mysql index needs a postgres counterpart with the same table, ordered columns, and uniqueness, and vice versa")
}
