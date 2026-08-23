//go:build integration

package api

import (
	"database/sql"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/block/schemabot/pkg/namedlock"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// startPostgresStorage starts a PostgreSQL container playing the role of
// SchemaBot's storage database and returns its DSN plus an open connection
// for assertions.
func startPostgresStorage(t *testing.T) (string, *sql.DB) {
	t.Helper()
	return testutil.StartPostgres(t, "schemabot")
}

// requireStorageTables asserts that every embedded PostgreSQL schema file's
// table exists in the storage database.
func requireStorageTables(t *testing.T, db *sql.DB) {
	t.Helper()
	tables, _, err := readEmbeddedPostgresSchemaFiles()
	require.NoError(t, err)
	for _, table := range tables {
		require.True(t, testutil.PostgresTableExists(t, db, "public", table),
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

// Startup refuses an existing storage table that is missing an expected
// column and identifies the exact table shape operators need to restore.
func TestEnsureSchemaPostgres_RejectsMissingColumn(t *testing.T) {
	ctx := t.Context()
	dsn, db := startPostgresStorage(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))
	_, err := db.ExecContext(ctx, "ALTER TABLE settings DROP COLUMN setting_value")
	require.NoError(t, err)

	err = EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres))
	require.ErrorContains(t, err, `storage table "settings" is missing expected columns: setting_value`)
}

// Startup tolerates columns unknown to the running binary so an older binary
// can use a storage database previously bootstrapped with a newer shape.
func TestEnsureSchemaPostgres_AllowsExtraColumn(t *testing.T) {
	ctx := t.Context()
	dsn, db := startPostgresStorage(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))
	_, err := db.ExecContext(ctx, "ALTER TABLE settings ADD COLUMN future_value text")
	require.NoError(t, err)

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))
}

// Startup tolerates a missing non-unique index because it affects query
// performance rather than the storage model's write semantics.
func TestEnsureSchemaPostgres_AllowsMissingIndex(t *testing.T) {
	ctx := t.Context()
	dsn, db := startPostgresStorage(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))
	_, err := db.ExecContext(ctx, "DROP INDEX idx_apply_logs_level")
	require.NoError(t, err)

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))
}

// Startup refuses a missing unique index and identifies the exact table and
// index operators must restore before writes can safely resume.
func TestEnsureSchemaPostgres_RejectsMissingUniqueIndex(t *testing.T) {
	ctx := t.Context()
	dsn, db := startPostgresStorage(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))
	_, err := db.ExecContext(ctx, "DROP INDEX idx_settings_setting_key")
	require.NoError(t, err)

	err = EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres))
	require.ErrorContains(t, err, `storage table "settings" is missing expected unique indexes: idx_settings_setting_key`)
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

// A bootstrap that finds tables missing must park on the advisory lock while
// another pod holds it — creating nothing in the meantime — then proceed
// under the lock once the holder releases and converge the schema. The lock
// contention is pinned deterministically: the test holds the production lock,
// proves the bootstrap is waiting on it via pg_locks, and only then releases.
func TestEnsureSchemaPostgres_WaitsForAdvisoryLock(t *testing.T) {
	ctx := t.Context()
	dsn, db := startPostgresStorage(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Hold the EnsureSchema advisory lock the way a leading pod would.
	lockConn, err := acquirePostgresEnsureSchemaLock(ctx, dsn, logger, namedlock.Postgres{})
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres))
	}()

	// Wait until the bootstrap is provably parked on the advisory lock: its
	// pg_advisory_lock call shows up in pg_locks as an ungranted advisory
	// lock request.
	require.Eventually(t, func() bool {
		var waiters int
		if err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted").Scan(&waiters); err != nil {
			t.Logf("poll pg_locks for advisory-lock waiters: %v", err)
			return false
		}
		return waiters > 0
	}, 30*time.Second, 50*time.Millisecond, "bootstrap never waited on the advisory lock")

	// While parked, the bootstrap must not have created any tables.
	require.False(t, testutil.PostgresTableExists(t, db, "public", "tasks"),
		"bootstrap created tables while waiting for the advisory lock")

	// Closing the lock connection ends its session, releasing the lock.
	utils.CloseAndLog(lockConn)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("bootstrap did not complete after the advisory lock was released")
	}
	requireStorageTables(t, db)
}
