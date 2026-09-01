package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/schema"
)

// The identity-key column map must stay in lockstep with the embedded
// PostgreSQL schema files: every mapped table must have a schema file, and
// every mapped column must exist in that table's CREATE TABLE. A renamed or
// dropped column would otherwise surface only when the canonicalization runs
// against a live storage database.
func TestPostgresIdentityKeyColumns_MatchEmbeddedSchema(t *testing.T) {
	tables, files, err := readEmbeddedPostgresSchemaFiles()
	require.NoError(t, err)
	embedded := make(map[string]bool, len(tables))
	for _, table := range tables {
		embedded[table] = true
	}

	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)

	for table, columns := range postgresIdentityKeyColumns {
		require.True(t, embedded[table], "mapped table %q has no embedded schema file", table)

		statements, err := parser.Split(files[table])
		require.NoError(t, err)
		require.NotEmpty(t, statements, "schema file for table %q has no statements", table)
		tableColumns, err := parser.CreateTableColumns(statements[0])
		require.NoError(t, err)
		existing := make(map[string]bool, len(tableColumns))
		for _, column := range tableColumns {
			existing[column] = true
		}

		for _, column := range columns {
			assert.True(t, existing[column],
				"mapped column %s.%s does not exist in the embedded schema", table, column)
		}
	}
}
