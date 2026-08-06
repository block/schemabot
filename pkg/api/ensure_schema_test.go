package api

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/schema"
)

// closedPortDSN points at a closed loopback port: any connection attempt fails
// instantly with a refused connection rather than hanging, so tests using it
// never depend on a real database and stay fast.
const closedPortDSN = "user:pass@tcp(127.0.0.1:1)/schemabot"

// EnsureSchema routes to the schema bootstrapper for the storage database's
// dialect and fails closed for a dialect that has none: it must return an
// error naming the dialect before touching the database, never fall back to
// running another family's flow against the storage database. The DSN
// points at a closed loopback port so any accidental connection attempt fails
// the test rather than hanging.
func TestEnsureSchemaFailsClosedForUnsupportedDialect(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)

	unsupported := schema.Dialect("sqlite")
	err := EnsureSchema(closedPortDSN, logger, WithDialect(unsupported))

	require.Error(t, err)
	require.ErrorContains(t, err, "no schema bootstrapper")
	require.ErrorContains(t, err, string(unsupported))
}

// EnsureSchema with the postgres dialect routes into the PostgreSQL
// bootstrapper. The closed-port DSN makes its connection attempt fail
// instantly, and the resulting error proves the dialect routed into the
// PostgreSQL flow rather than the fail-closed branch.
func TestEnsureSchemaRoutesPostgresDialect(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)

	err := EnsureSchema("postgres://user:pass@127.0.0.1:1/schemabot", logger, WithDialect(schema.DialectPostgres))

	require.Error(t, err)
	require.NotContains(t, err.Error(), "no schema bootstrapper")
	require.ErrorContains(t, err, "ping storage database")
}

// EnsureSchema without a dialect option defaults to the MySQL bootstrapper —
// the behavior every existing call site relies on. The closed-port DSN makes
// the MySQL flow's connection attempt fail instantly, and the resulting error
// proves the default routed into the MySQL flow rather than the fail-closed
// branch: a zero-value dialect would return "no schema bootstrapper" and
// crash-loop every pod at startup.
func TestEnsureSchemaDefaultsToMySQLDialect(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)

	err := EnsureSchema(closedPortDSN, logger)

	require.Error(t, err)
	require.NotContains(t, err.Error(), "no schema bootstrapper")
	require.ErrorContains(t, err, "plan schema")
}
