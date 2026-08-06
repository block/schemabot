//go:build integration

package api

import (
	"database/sql"
	"log/slog"
	"os"
	"testing"

	"github.com/block/spirit/pkg/utils"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"golang.org/x/sync/errgroup"

	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// startPostgresStorage starts a PostgreSQL container playing the role of
// SchemaBot's storage database and returns its DSN plus an open connection
// for assertions.
func startPostgresStorage(t *testing.T) (string, *sql.DB) {
	t.Helper()
	ctx := t.Context()

	container, err := postgres.Run(ctx,
		"postgres:16",
		postgres.WithDatabase("schemabot"),
		postgres.WithUsername("schemabot"),
		postgres.WithPassword("test"),
		postgres.BasicWaitStrategies(),
	)
	require.NoError(t, err, "failed to start postgres")
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	})

	dsn, err := testutil.ContainerConnectionString(ctx, container, "sslmode=disable")
	require.NoError(t, err, "failed to get connection string")

	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(ctx))

	return dsn, db
}

// pgTableExists reports whether table exists in the public schema. It is a
// PostgreSQL-side counterpart to testutil.TableExists, whose `?` placeholders
// only bind on MySQL.
func pgTableExists(t *testing.T, db *sql.DB, table string) bool {
	t.Helper()
	var count int
	err := db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1",
		table,
	).Scan(&count)
	require.NoError(t, err)
	return count > 0
}

// requireStorageTables asserts that every embedded PostgreSQL schema file's
// table exists in the storage database.
func requireStorageTables(t *testing.T, db *sql.DB) {
	t.Helper()
	tables, _, err := readEmbeddedPostgresSchemaFiles()
	require.NoError(t, err)
	for _, table := range tables {
		require.True(t, pgTableExists(t, db, table),
			"storage table %q missing after bootstrap", table)
	}
}

// A fresh PostgreSQL storage database bootstraps to the full storage schema:
// every embedded schema file's table exists afterwards, so the server can
// start accepting traffic against a database that began empty.
func TestEnsureSchemaPostgres_BootstrapsFreshDatabase(t *testing.T) {
	dsn, db := startPostgresStorage(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))

	requireStorageTables(t, db)
}

// A second bootstrap of an already-converged storage database is a no-op:
// the fast path finds every table present and returns without acquiring the
// advisory lock or executing DDL.
func TestEnsureSchemaPostgres_Idempotent(t *testing.T) {
	dsn, db := startPostgresStorage(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))
	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))

	requireStorageTables(t, db)
}

// A storage database missing a subset of tables converges back to the full
// schema: the bootstrapper creates only the missing tables and leaves the
// existing ones — and their data — untouched.
func TestEnsureSchemaPostgres_CreatesMissingTables(t *testing.T) {
	ctx := t.Context()
	dsn, db := startPostgresStorage(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))

	// Seed a row in a surviving table, then drop another table.
	_, err := db.ExecContext(ctx, "INSERT INTO settings (setting_key, setting_value) VALUES ('k', 'v')")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "DROP TABLE tasks")
	require.NoError(t, err)

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))

	requireStorageTables(t, db)
	var value string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT setting_value FROM settings WHERE setting_key = 'k'").Scan(&value),
		"existing table data must survive re-convergence")
	require.Equal(t, "v", value)
}

// Concurrent pod startups bootstrap the same empty storage database safely:
// the advisory lock serializes table creation, the trailing pod re-checks
// under the lock and finds the leader's tables, and both startups succeed.
func TestEnsureSchemaPostgres_ConcurrentPods(t *testing.T) {
	dsn, db := startPostgresStorage(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	var g errgroup.Group
	for range 4 {
		g.Go(func() error {
			return EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres))
		})
	}
	require.NoError(t, g.Wait())

	requireStorageTables(t, db)
}
