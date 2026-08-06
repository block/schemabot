package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// splitPostgresStatements must split embedded schema files on statement
// boundaries only: semicolons inside single-quoted string literals — including
// doubled-quote escapes — belong to the statement, and surrounding whitespace
// or a trailing semicolon must not produce empty statements.
func TestSplitPostgresStatements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		want    []string
	}{
		{
			name:    "two statements with trailing semicolon and whitespace",
			content: "CREATE TABLE t (id bigint);\n\nCREATE INDEX idx_t ON t (id);\n",
			want:    []string{"CREATE TABLE t (id bigint)", "CREATE INDEX idx_t ON t (id)"},
		},
		{
			name:    "semicolon inside a string literal",
			content: "CREATE TABLE t (s varchar(10) DEFAULT 'a;b');\nCREATE INDEX idx_t ON t (s);",
			want:    []string{"CREATE TABLE t (s varchar(10) DEFAULT 'a;b')", "CREATE INDEX idx_t ON t (s)"},
		},
		{
			name:    "doubled-quote escape keeps literal state balanced",
			content: "CREATE TABLE t (s varchar(10) DEFAULT 'it''s;ok');\nCREATE INDEX idx_t ON t (s);",
			want:    []string{"CREATE TABLE t (s varchar(10) DEFAULT 'it''s;ok')", "CREATE INDEX idx_t ON t (s)"},
		},
		{
			name:    "empty content",
			content: " \n ",
			want:    nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, splitPostgresStatements(tt.content))
		})
	}
}

// The embedded PostgreSQL schema files are the source of truth for the
// storage tables the bootstrapper converges: reading them must yield one
// table per file with non-empty content and sorted table names.
func TestReadEmbeddedPostgresSchemaFiles(t *testing.T) {
	t.Parallel()

	tables, files, err := readEmbeddedPostgresSchemaFiles()
	require.NoError(t, err)
	require.NotEmpty(t, tables)
	require.Len(t, files, len(tables))
	require.IsIncreasing(t, tables)
	for _, table := range tables {
		require.NotEmpty(t, files[table], "schema file for table %q is empty", table)
		require.Contains(t, files[table], "CREATE TABLE "+table+" (",
			"schema file for table %q must create the table it is named after", table)
	}
}
