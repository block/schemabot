package api

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
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
