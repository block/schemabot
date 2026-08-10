package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	settings, err := postgresCreateTableColumns(files["settings"])
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "setting_key", "setting_value", "created_at", "updated_at"}, settings)

	applies, err := postgresCreateTableColumns(files["applies"])
	require.NoError(t, err)
	assert.Equal(t, []string{
		"id", "apply_identifier", "lock_id", "plan_id", "database_name", "database_type",
		"repository", "pull_request", "environment", "deployment", "caller", "installation_id",
		"external_id", "idempotency_key", "engine", "state", "error_message", "options", "attempt",
		"lease_owner", "lease_token", "lease_acquired_at", "started_at", "completed_at",
		"revert_skipped_at", "created_at", "updated_at",
	}, applies)

	for table, content := range files {
		columns, err := postgresCreateTableColumns(content)
		require.NoError(t, err, "table %s", table)
		assert.NotEmpty(t, columns, "table %s", table)
	}
}

func TestPostgresCreateTableColumns_SkipsConstraints(t *testing.T) {
	t.Parallel()

	content := `CREATE TABLE example (
  id bigint,
  "display name" text,
  PRIMARY KEY (id),
  UNIQUE (id),
  CONSTRAINT positive CHECK (id > 0),
  CHECK (id > 0),
  FOREIGN KEY (id) REFERENCES other (id),
  EXCLUDE USING gist (id WITH =),
  INDEX (id)
);`
	columns, err := postgresCreateTableColumns(content)
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "display name"}, columns)
}

func TestPostgresCreateTableColumns_RejectsUnknownEntry(t *testing.T) {
	t.Parallel()

	_, err := postgresCreateTableColumns("CREATE TABLE example (id bigint, PARTITION BY id)")
	require.ErrorContains(t, err, "unrecognized CREATE TABLE entry")
}
