package api

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/schema"
)

// The bootstrapper opens its pool through postgresconn, which parses the DSN
// eagerly: a malformed DSN must fail at open, before any dial or ping is
// attempted, so a misconfigured deployment fails startup with a parse error
// rather than a confusing connection failure.
func TestEnsurePostgresSchema_MalformedDSNFailsAtOpen(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := ensurePostgresSchema("postgres://user@host:notaport/db", logger, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "open storage database")
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

func TestPostgresCreateTableColumns_EmbeddedFiles(t *testing.T) {
	t.Parallel()

	_, files, err := readEmbeddedPostgresSchemaFiles()
	require.NoError(t, err)
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)

	settingsStatements, err := parser.Split(files["settings"])
	require.NoError(t, err)
	settings, err := parser.CreateTableColumns(settingsStatements[0])
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "setting_key", "setting_value", "created_at", "updated_at"}, settings)

	appliesStatements, err := parser.Split(files["applies"])
	require.NoError(t, err)
	applies, err := parser.CreateTableColumns(appliesStatements[0])
	require.NoError(t, err)
	assert.Equal(t, []string{
		"id", "apply_identifier", "lock_id", "plan_id", "database_name", "database_type",
		"repository", "pull_request", "environment", "deployment", "caller", "installation_id",
		"external_id", "idempotency_key", "engine", "state", "error_message", "options", "attempt",
		"lease_owner", "lease_token", "lease_acquired_at", "started_at", "completed_at",
		"revert_skipped_at", "created_at", "updated_at",
	}, applies)

	for table, content := range files {
		statements, err := parser.Split(content)
		require.NoError(t, err, "table %s", table)
		columns, err := parser.CreateTableColumns(statements[0])
		require.NoError(t, err, "table %s", table)
		assert.NotEmpty(t, columns, "table %s", table)
	}
}
