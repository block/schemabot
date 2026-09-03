//go:build integration

package api

import (
	"bytes"
	"database/sql"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
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

// Startup restores an additive column from the embedded CREATE TABLE and a
// second startup observes the converged shape without executing more DDL.
func TestEnsureSchemaPostgres_ConvergesMissingColumn(t *testing.T) {
	ctx := t.Context()
	dsn, db := startPostgresStorage(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))
	_, err := db.ExecContext(ctx, "ALTER TABLE applies DROP COLUMN caller")
	require.NoError(t, err)

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))
	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))
	columns, err := postgresTableColumns(ctx, db, "applies")
	require.NoError(t, err)
	assert.True(t, columns["caller"])
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
	_, err = db.ExecContext(ctx, "CREATE INDEX idx_settings_future_value ON settings (future_value)")
	require.NoError(t, err)

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))
	indexes, err := postgresTableIndexes(ctx, db, "settings")
	require.NoError(t, err)
	assert.Contains(t, indexes, "idx_settings_future_value")
}

// Startup recreates a missing non-unique index from the schema file's own
// CREATE INDEX statement.
func TestEnsureSchemaPostgres_ConvergesMissingNonUniqueIndex(t *testing.T) {
	ctx := t.Context()
	dsn, db := startPostgresStorage(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))
	_, err := db.ExecContext(ctx, "DROP INDEX idx_apply_logs_level")
	require.NoError(t, err)

	var logs bytes.Buffer
	convergeLogger := slog.New(slog.NewTextHandler(&logs, nil))
	require.NoError(t, EnsureSchema(dsn, convergeLogger, WithDialect(schema.DialectPostgres)))
	indexes, err := postgresTableIndexes(ctx, db, "apply_logs")
	require.NoError(t, err)
	assert.Contains(t, indexes, "idx_apply_logs_level")
	assert.Contains(t, logs.String(), "CREATE INDEX idx_apply_logs_level ON apply_logs (level)")
}

// Startup recreates a missing unique index transactionally before accepting
// traffic that depends on its write constraint.
func TestEnsureSchemaPostgres_ConvergesMissingUniqueIndex(t *testing.T) {
	ctx := t.Context()
	dsn, db := startPostgresStorage(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))
	_, err := db.ExecContext(ctx, "DROP INDEX idx_settings_setting_key")
	require.NoError(t, err)

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))
	indexes, err := postgresTableIndexes(ctx, db, "settings")
	require.NoError(t, err)
	assert.Equal(t, postgresLiveIndex{unique: true, valid: true}, indexes["idx_settings_setting_key"])
}

// Startup refuses automatic convergence when the desired missing column is
// NOT NULL without a DEFAULT and gives the operator a safe remediation.
func TestEnsureSchemaPostgres_RejectsMissingNotNullColumnWithoutDefault(t *testing.T) {
	ctx := t.Context()
	dsn, db := startPostgresStorage(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))
	_, err := db.ExecContext(ctx, "ALTER TABLE settings DROP COLUMN setting_value")
	require.NoError(t, err)

	err = EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres))
	require.ErrorContains(t, err, `storage table "settings" is missing column "setting_value" whose definition is NOT NULL without a DEFAULT`)
	require.ErrorContains(t, err, "add it manually or ship the column with a DEFAULT")
}

// A change that needs manual remediation aborts the whole convergence before
// any DDL executes: automatic drift on another table must stay untouched
// rather than being half-applied ahead of the failure, so a crashloop never
// repeats a partial convergence.
func TestEnsureSchemaPostgres_ManualRemediationBlocksAllDDL(t *testing.T) {
	ctx := t.Context()
	dsn, db := startPostgresStorage(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))
	// "applies" sorts before "settings": without the whole-set gate its
	// automatic change would commit before the settings failure surfaced.
	_, err := db.ExecContext(ctx, "ALTER TABLE applies DROP COLUMN caller")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "ALTER TABLE settings DROP COLUMN setting_value")
	require.NoError(t, err)

	err = EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres))
	require.ErrorContains(t, err, `storage table "settings" is missing column "setting_value"`)

	columns, err := postgresTableColumns(ctx, db, "applies")
	require.NoError(t, err)
	assert.False(t, columns["caller"], "no DDL may run when any change needs manual remediation")
}

// A live non-unique index under a name the embedded schema requires to be
// unique cannot be converged automatically — CREATE UNIQUE INDEX would
// collide by name — so startup fails closed with a manual remediation rather
// than silently accepting the weaker index. The problem goes through the same
// whole-set gate as column remediation: automatic drift elsewhere stays
// untouched, and every manual problem is named in the one error.
func TestEnsureSchemaPostgres_RejectsNonUniqueIndexWhereUniqueRequired(t *testing.T) {
	ctx := t.Context()
	dsn, db := startPostgresStorage(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))
	_, err := db.ExecContext(ctx, "DROP INDEX idx_settings_setting_key")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE INDEX idx_settings_setting_key ON settings (setting_key)")
	require.NoError(t, err)
	// "apply_logs" sorts before "settings": without the whole-set gate its
	// automatic index change would commit before the settings failure surfaced.
	_, err = db.ExecContext(ctx, "DROP INDEX idx_apply_logs_level")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "ALTER TABLE settings DROP COLUMN setting_value")
	require.NoError(t, err)

	err = EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres))
	require.ErrorContains(t, err, `storage table "settings" has index "idx_settings_setting_key" whose live index is non-unique where the embedded schema requires a unique index`)
	require.ErrorContains(t, err, "replace it manually")
	require.ErrorContains(t, err, `storage table "settings" is missing column "setting_value"`)

	indexes, err := postgresTableIndexes(ctx, db, "apply_logs")
	require.NoError(t, err)
	assert.NotContains(t, indexes, "idx_apply_logs_level", "no DDL may run when any change needs manual remediation")
}

// A CREATE INDEX CONCURRENTLY that fails part-way leaves an invalid index
// under the expected name. PostgreSQL never uses it for reads and it may not
// cover every row, so startup must not read it as converged: it fails closed
// naming the index and the remediation, and does not attempt a CREATE INDEX
// that would only collide with the invalid one.
func TestEnsureSchemaPostgres_RejectsInvalidIndex(t *testing.T) {
	ctx := t.Context()
	dsn, db := startPostgresStorage(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))
	_, err := db.ExecContext(ctx, "DROP INDEX idx_settings_setting_key")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO settings (setting_key, setting_value) VALUES ('dup', 'a'), ('dup', 'b')")
	require.NoError(t, err)
	// Duplicate keys make the concurrent unique build fail after the index has
	// been catalogued, which is exactly how an invalid index arises in practice.
	_, err = db.ExecContext(ctx, "CREATE UNIQUE INDEX CONCURRENTLY idx_settings_setting_key ON settings (setting_key)")
	require.Error(t, err)
	indexes, err := postgresTableIndexes(ctx, db, "settings")
	require.NoError(t, err)
	require.Equal(t, postgresLiveIndex{unique: true, valid: false}, indexes["idx_settings_setting_key"])

	err = EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres))
	require.ErrorContains(t, err, `storage table "settings" has index "idx_settings_setting_key" whose live index is invalid`)
	require.ErrorContains(t, err, "DROP INDEX it so startup recreates it")

	indexes, err = postgresTableIndexes(ctx, db, "settings")
	require.NoError(t, err)
	assert.Equal(t, postgresLiveIndex{unique: true, valid: false}, indexes["idx_settings_setting_key"], "startup must leave the invalid index for the operator")
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
