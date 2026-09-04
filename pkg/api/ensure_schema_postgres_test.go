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
		"external_id", "idempotency_key", "expected_operation_keys", "engine", "state", "error_message", "options", "attempt",
		"lease_owner", "lease_token", "lease_acquired_at", "started_at", "completed_at",
		"revert_skipped_at", "superseded_by", "created_at", "updated_at",
	}, applies)

	for table, content := range files {
		statements, err := parser.Split(content)
		require.NoError(t, err, "table %s", table)
		columns, err := parser.CreateTableColumns(statements[0])
		require.NoError(t, err, "table %s", table)
		assert.NotEmpty(t, columns, "table %s", table)
	}
}

// The manual-remediation gate must name every problem across every table in
// one error, so an operator fixes them all in one pass instead of one per
// startup attempt. An all-automatic drift set passes the gate untouched.
func TestPostgresManualRemediation(t *testing.T) {
	t.Parallel()

	tables := []string{"applies", "settings"}
	automatic := postgresSchemaDrift{
		"applies": {{operation: "add_column", object: "caller", ddl: "ALTER TABLE applies ADD COLUMN caller text"}},
	}
	require.NoError(t, postgresManualRemediation(tables, automatic))

	mixed := postgresSchemaDrift{
		"applies": {
			{operation: "add_column", object: "caller", ddl: "ALTER TABLE applies ADD COLUMN caller text"},
			{operation: "add_column", object: "lock_id", manualReason: "definition is NOT NULL without a DEFAULT; add it manually or ship the column with a DEFAULT"},
		},
		"settings": {
			{operation: "add_column", object: "setting_value", manualReason: "definition is NOT NULL without a DEFAULT; add it manually or ship the column with a DEFAULT"},
			{operation: "create_index", object: "idx_settings_setting_key", manualReason: "live state is non-unique where the embedded schema requires a unique index; replace it manually"},
		},
	}
	err := postgresManualRemediation(tables, mixed)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `storage table "applies" is missing column "lock_id"`)
	assert.Contains(t, err.Error(), `storage table "settings" is missing column "setting_value"`)
	assert.Contains(t, err.Error(), "add it manually or ship the column with a DEFAULT")
	assert.Contains(t, err.Error(), `storage table "settings" has index "idx_settings_setting_key" whose live state is non-unique`)
}

// A live index satisfies an expectation only when PostgreSQL can use it: an
// invalid index is reported regardless of uniqueness, a non-unique index
// cannot stand in for a unique one, and a unique index answers a non-unique
// expectation's reads. An invalid index still under construction is told
// apart from one a failed build abandoned, since only the latter needs the
// operator to act.
func TestPostgresLiveIndexManualReason(t *testing.T) {
	t.Parallel()

	unique := postgresIndexExpectation{name: "idx_settings_setting_key", unique: true}
	nonUnique := postgresIndexExpectation{name: "idx_apply_logs_level"}

	assert.Empty(t, postgresLiveIndexManualReason(unique, postgresLiveIndex{unique: true, valid: true}))
	assert.Empty(t, postgresLiveIndexManualReason(nonUnique, postgresLiveIndex{unique: false, valid: true}))
	assert.Empty(t, postgresLiveIndexManualReason(nonUnique, postgresLiveIndex{unique: true, valid: true}))

	assert.Contains(t, postgresLiveIndexManualReason(unique, postgresLiveIndex{unique: false, valid: true}), "live state is non-unique")
	failedBuild := postgresLiveIndexManualReason(unique, postgresLiveIndex{unique: true, valid: false})
	assert.Contains(t, failedBuild, "live state is invalid and no CREATE INDEX CONCURRENTLY is visible building it")
	assert.Contains(t, failedBuild, "DROP INDEX it so startup recreates it")
	assert.Contains(t, postgresLiveIndexManualReason(nonUnique, postgresLiveIndex{unique: false, valid: false}), "live state is invalid and no CREATE INDEX CONCURRENTLY is visible building it")

	inFlight := postgresLiveIndexManualReason(unique, postgresLiveIndex{unique: true, valid: false, building: true})
	assert.Contains(t, inFlight, "live state is invalid because a CREATE INDEX CONCURRENTLY is still building it")
	assert.Contains(t, inFlight, "startup succeeds once that build completes")
	assert.NotContains(t, inFlight, "DROP INDEX")
}

// The expectations parser fails closed on schema-file statements the additive
// convergence cannot create or verify — an unnamed index or a non-index
// trailing statement — so a schema file can never silently stop being the
// source of truth for the live schema.
func TestPostgresExpectationsFor_RejectsUntrackableStatements(t *testing.T) {
	t.Parallel()

	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)

	tests := []struct {
		name string
		file string
		want string
	}{
		{
			name: "unnamed index",
			file: "CREATE TABLE settings (id bigint);\nCREATE INDEX ON settings (id);",
			want: "cannot track",
		},
		{
			name: "non-index trailing statement",
			file: "CREATE TABLE settings (id bigint);\nCOMMENT ON TABLE settings IS 'x';",
			want: "cannot track",
		},
		{
			name: "index on another table",
			file: "CREATE TABLE settings (id bigint);\nCREATE INDEX idx_other ON other (id);",
			want: `declares index "idx_other" on table "other"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := postgresExpectationsFor(parser, "settings", tt.file)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

// Every embedded PostgreSQL schema file must parse into trackable
// expectations: a CREATE TABLE followed only by named CREATE INDEX statements
// on the file's own table.
func TestPostgresExpectationsFor_EmbeddedFiles(t *testing.T) {
	t.Parallel()

	tables, files, err := readEmbeddedPostgresSchemaFiles()
	require.NoError(t, err)
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	for _, table := range tables {
		expected, err := postgresExpectationsFor(parser, table, files[table])
		require.NoError(t, err, "table %s", table)
		assert.NotEmpty(t, expected.columns, "table %s", table)
	}
}
