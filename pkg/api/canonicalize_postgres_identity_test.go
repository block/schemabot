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

// The converse direction: every schema column whose name marks it as an
// identity string must have a fold-map entry, or a deliberate exclusion with
// a rationale. Without this direction, a new table or column that needs
// folding would be silently omitted — its rows never folded — while the
// canonicalization still reports success.
func TestPostgresIdentityKeyColumns_CoverEmbeddedSchema(t *testing.T) {
	// The column names under which identity strings are stored anywhere in
	// the storage schema. Pod identities use distinct names (lease_owner,
	// observer_owner) and never appear here.
	identityColumnNames := map[string]bool{
		"database_name":     true,
		"database_type":     true,
		"deployment":        true,
		"environment":       true,
		"environment_scope": true,
		"owner":             true,
		"repository":        true,
	}
	// table.column entries carrying an identity-shaped name that are
	// deliberately not folded; the value states why.
	excluded := map[string]string{
		"plan_comments.environment_scope": "not a query predicate; the store persists it as given and consumers compare it in Go against a scope built from configured environment names",
	}

	tables, files, err := readEmbeddedPostgresSchemaFiles()
	require.NoError(t, err)
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)

	for _, table := range tables {
		statements, err := parser.Split(files[table])
		require.NoError(t, err)
		require.NotEmpty(t, statements, "schema file for table %q has no statements", table)
		columns, err := parser.CreateTableColumns(statements[0])
		require.NoError(t, err)

		mapped := make(map[string]bool, len(postgresIdentityKeyColumns[table]))
		for _, column := range postgresIdentityKeyColumns[table] {
			mapped[column] = true
		}
		for _, column := range columns {
			if !identityColumnNames[column] {
				continue
			}
			key := table + "." + column
			if rationale, ok := excluded[key]; ok {
				assert.False(t, mapped[column],
					"%s is excluded (%s) but also mapped; drop one", key, rationale)
				continue
			}
			assert.True(t, mapped[column],
				"schema column %s holds an identity string but has no fold-map entry; map it, or exclude it here with a rationale", key)
		}
	}
}
