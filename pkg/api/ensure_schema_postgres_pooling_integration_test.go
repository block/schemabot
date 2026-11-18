//go:build integration

package api

import (
	"database/sql"
	"log/slog"
	"os"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/block/schemabot/pkg/namedlock"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// openPostgres opens a connection to a PostgreSQL DSN for assertions.
func openPostgres(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(t.Context()))
	return db
}

// A transaction-mode connection pooler leaves SchemaBot's bootstrap advisory
// lock on a backend the pod no longer reaches, so two pods starting at once
// would both converge the same storage schema believing they held the lock.
// Startup refuses that configuration instead: neither bootstrapper proceeds,
// the storage database is left untouched, and the error names the remedy.
func TestEnsureSchemaPostgres_RefusesTransactionPooledConnection(t *testing.T) {
	pooledDSN, directDSN := testutil.StartPostgresBehindPgBouncer(t, "schemabot", testutil.PgBouncerTransactionPooling)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	var pods errgroup.Group
	errs := make([]error, 2)
	for i := range errs {
		pods.Go(func() error {
			errs[i] = EnsureSchema(pooledDSN, logger, WithDialect(schema.DialectPostgres))
			return nil
		})
	}
	require.NoError(t, pods.Wait())

	for i, err := range errs {
		require.Error(t, err, "bootstrapper %d converged storage over a transaction-mode pooler", i)
		assert.ErrorIs(t, err, namedlock.ErrNoSessionAffinity)
		assert.Contains(t, err.Error(), "direct session endpoint",
			"the refusal should tell the operator which endpoint to use")
	}

	direct := openPostgres(t, directDSN)
	assert.False(t, testutil.PostgresTableExists(t, direct, "public", "applies"),
		"a refused bootstrap must not have executed DDL")
}

// Session pooling gives a client connection its own server session, which is
// all the advisory lock needs, so a pooled connection string is not refused on
// the strength of being pooled: the bootstrap converges through it normally.
func TestEnsureSchemaPostgres_BootstrapsThroughSessionPooledConnection(t *testing.T) {
	pooledDSN, directDSN := testutil.StartPostgresBehindPgBouncer(t, "schemabot", testutil.PgBouncerSessionPooling)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	require.NoError(t, EnsureSchema(pooledDSN, logger, WithDialect(schema.DialectPostgres)))

	requireStorageTables(t, openPostgres(t, directDSN))
}
