package api

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/schema"
)

// EnsureSchema routes to the schema bootstrapper for the storage database's
// dialect and fails closed for a dialect that has none: it must return an
// error naming the dialect before touching the database, never fall back to
// running the MySQL flow against another family's storage database. The DSN
// points at an unroutable port so any accidental connection attempt fails the
// test rather than hanging.
func TestEnsureSchemaFailsClosedForUnsupportedDialect(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)

	err := EnsureSchema("user:pass@tcp(127.0.0.1:1)/schemabot", logger, WithDialect(schema.DialectPostgres))

	require.Error(t, err)
	require.ErrorContains(t, err, "no schema bootstrapper")
	require.ErrorContains(t, err, string(schema.DialectPostgres))
}
