//go:build integration

package api

import (
	"bytes"
	"database/sql"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/block/schemabot/pkg/namedlock"
	"github.com/block/schemabot/pkg/postgresconn"
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
	require.ErrorContains(t, err, `storage table "settings" has index "idx_settings_setting_key" whose live state is non-unique where the embedded schema requires a unique index`)
	require.ErrorContains(t, err, "replace it manually")
	require.ErrorContains(t, err, `storage table "settings" is missing column "setting_value"`)

	indexes, err := postgresTableIndexes(ctx, db, "apply_logs")
	require.NoError(t, err)
	assert.NotContains(t, indexes, "idx_apply_logs_level", "no DDL may run when any change needs manual remediation")
}

// Drift that needs manual remediation is refused before the bootstrap queues
// for the EnsureSchema advisory lock: the decision depends only on the scan,
// so a pod that will fail anyway must not park behind a leader for the whole
// lock timeout first. The refusal still logs every detected change with its
// DDL, because the joined error names the object but not the statement, and
// those lines are what the operator triages the crashloop from.
func TestEnsureSchemaPostgres_RefusesManualRemediationWithoutWaitingForLock(t *testing.T) {
	ctx := t.Context()
	dsn, db := startPostgresStorage(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))
	_, err := db.ExecContext(ctx, "ALTER TABLE settings DROP COLUMN setting_value")
	require.NoError(t, err)
	// An automatic change alongside the manual one proves the refusal logs
	// the whole drift set, not only the problem it names.
	_, err = db.ExecContext(ctx, "ALTER TABLE applies DROP COLUMN caller")
	require.NoError(t, err)

	// Hold the EnsureSchema advisory lock the way a leading pod would, for
	// the rest of the test; a bootstrap that queued for it could not return
	// before EnsureSchemaTimeout.
	lockConn, err := acquirePostgresEnsureSchemaLock(ctx, dsn, logger, namedlock.Postgres{})
	require.NoError(t, err)
	defer utils.CloseAndLog(lockConn)

	var logs bytes.Buffer
	refusalLogger := slog.New(slog.NewTextHandler(&logs, nil))
	done := make(chan error, 1)
	go func() {
		done <- EnsureSchema(dsn, refusalLogger, WithDialect(schema.DialectPostgres))
	}()
	select {
	case err := <-done:
		require.ErrorContains(t, err, `storage table "settings" is missing column "setting_value"`)
		require.NotContains(t, err.Error(), "advisory lock")
	case <-time.After(30 * time.Second):
		t.Fatal("bootstrap did not refuse manual remediation while the advisory lock was held")
	}

	assert.Contains(t, logs.String(), "schema change detected (pre-lock)")
	assert.Contains(t, logs.String(), "ALTER TABLE settings ADD COLUMN setting_value")
	assert.Contains(t, logs.String(), "ALTER TABLE applies ADD COLUMN caller")
	assert.NotContains(t, logs.String(), "acquiring EnsureSchema advisory lock")
	columns, err := postgresTableColumns(ctx, db, "applies")
	require.NoError(t, err)
	assert.False(t, columns["caller"], "no DDL may run when any change needs manual remediation")
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
	require.ErrorContains(t, err, `storage table "settings" has index "idx_settings_setting_key" whose live state is invalid and no CREATE INDEX CONCURRENTLY is visible building it`)
	require.ErrorContains(t, err, "DROP INDEX it so startup recreates it")

	indexes, err = postgresTableIndexes(ctx, db, "settings")
	require.NoError(t, err)
	assert.Equal(t, postgresLiveIndex{unique: true, valid: false}, indexes["idx_settings_setting_key"], "startup must leave the invalid index for the operator")
}

// While an operator pre-creates an expected index with CREATE INDEX
// CONCURRENTLY, the index sits invalid under its name until the build ends.
// Startup still fails closed — the planner cannot use it yet — but the error
// says a build is in progress and that no operator action is needed, instead
// of prescribing the failed-build recovery; once the build completes, the
// next startup converges with no DDL of its own.
func TestEnsureSchemaPostgres_ReportsIndexBuildInProgress(t *testing.T) {
	ctx := t.Context()
	dsn, db := startPostgresStorage(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))
	_, err := db.ExecContext(ctx, "DROP INDEX idx_settings_setting_key")
	require.NoError(t, err)

	// An open write transaction on the table parks the concurrent build in
	// its wait for writers, after the index has been catalogued invalid.
	writer, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = writer.ExecContext(ctx, "INSERT INTO settings (setting_key, setting_value) VALUES ('held', 'open')")
	require.NoError(t, err)

	build := make(chan error, 1)
	go func() {
		_, err := db.ExecContext(ctx, "CREATE UNIQUE INDEX CONCURRENTLY idx_settings_setting_key ON settings (setting_key)")
		build <- err
	}()
	require.Eventually(t, func() bool {
		indexes, err := postgresTableIndexes(ctx, db, "settings")
		if err != nil {
			t.Logf("poll live indexes: %v", err)
			return false
		}
		return indexes["idx_settings_setting_key"] == postgresLiveIndex{unique: true, valid: false, building: true}
	}, 30*time.Second, 50*time.Millisecond, "concurrent build never became visible as in progress")

	err = EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres))
	require.ErrorContains(t, err, `storage table "settings" has index "idx_settings_setting_key" whose live state is invalid because a CREATE INDEX CONCURRENTLY is still building it`)
	require.ErrorContains(t, err, "startup succeeds once that build completes")
	require.NotContains(t, err.Error(), "DROP INDEX")

	require.NoError(t, writer.Commit())
	select {
	case err := <-build:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent index build did not finish after the writer committed")
	}

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))
	indexes, err := postgresTableIndexes(ctx, db, "settings")
	require.NoError(t, err)
	assert.Equal(t, postgresLiveIndex{unique: true, valid: true}, indexes["idx_settings_setting_key"])
}

// The post-convergence shape check is the last line before the server takes
// traffic, so every expected index it cannot accept is named with its cause:
// an absent index, a non-unique index where the schema requires uniqueness,
// and an invalid index each read differently to the operator recovering the
// crashloop, and a healthy database passes.
func TestVerifyPostgresSchemaShape_ReportsUnsatisfiedIndexes(t *testing.T) {
	ctx := t.Context()
	dsn, db := startPostgresStorage(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	require.NoError(t, EnsureSchema(dsn, logger, WithDialect(schema.DialectPostgres)))
	tables, files, err := readEmbeddedPostgresSchemaFiles()
	require.NoError(t, err)
	require.NoError(t, verifyPostgresSchemaShape(ctx, db, tables, files))

	_, err = db.ExecContext(ctx, "DROP INDEX idx_settings_setting_key")
	require.NoError(t, err)
	err = verifyPostgresSchemaShape(ctx, db, tables, files)
	require.ErrorContains(t, err, `storage table "settings" has expected indexes that are missing, invalid, or mismatched: idx_settings_setting_key (missing)`)

	_, err = db.ExecContext(ctx, "CREATE INDEX idx_settings_setting_key ON settings (setting_key)")
	require.NoError(t, err)
	err = verifyPostgresSchemaShape(ctx, db, tables, files)
	require.ErrorContains(t, err, `storage table "settings" has expected indexes that are missing, invalid, or mismatched: idx_settings_setting_key (live state is non-unique where the embedded schema requires a unique index`)

	_, err = db.ExecContext(ctx, "DROP INDEX idx_settings_setting_key")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO settings (setting_key, setting_value) VALUES ('dup', 'a'), ('dup', 'b')")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE UNIQUE INDEX CONCURRENTLY idx_settings_setting_key ON settings (setting_key)")
	require.Error(t, err)
	err = verifyPostgresSchemaShape(ctx, db, tables, files)
	require.ErrorContains(t, err, `storage table "settings" has expected indexes that are missing, invalid, or mismatched: idx_settings_setting_key (live state is invalid and no CREATE INDEX CONCURRENTLY is visible building it`)
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

// A platform-imposed statement_timeout must not reach SchemaBot's bootstrap.
// Hosted PostgreSQL providers set one at the role or database level and tune
// it for API queries; a value far below what a CREATE TABLE run needs would
// otherwise cancel the bootstrap and crashloop the pod. SchemaBot's own
// budgets take precedence, so a fresh database still converges to the full
// storage schema under a deliberately hostile 1ms database default.
func TestEnsureSchemaPostgres_OverridesHostileDatabaseStatementTimeout(t *testing.T) {
	dsn, db := startPostgresStorage(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// 1ms is below what the bootstrap's own catalog reads take, so a pod that
	// inherited this value could not even complete its drift scan. That is what
	// makes the bootstrap's success below evidence of precedence rather than of
	// a budget that simply never had to bite.
	_, err := db.ExecContext(t.Context(),
		`ALTER DATABASE schemabot SET statement_timeout = '1ms'`)
	require.NoError(t, err)

	// Baseline: the hostile default really does reach a fresh session, so the
	// bootstrap below is proving precedence rather than a no-op.
	fresh, err := postgresconn.Open(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(fresh) })
	var inherited string
	require.NoError(t, fresh.QueryRowContext(t.Context(), "SHOW statement_timeout").Scan(&inherited))
	require.Equal(t, "1ms", inherited)

	// No statement timeout option: the bootstrap's own default has to be what
	// displaces the hostile value. This is the surface an embedder gets when
	// it calls EnsureSchema without opting in, so a default of "inherit
	// whatever the platform set" would fail here rather than in production.
	require.NoError(t, EnsureSchema(dsn, logger,
		WithDialect(schema.DialectPostgres)))

	requireStorageTables(t, db)
}

// The bootstrap's advisory-lock connection runs with no statement budget so a
// trailing pod can block inside pg_advisory_lock for the leader's whole
// bootstrap. A platform statement_timeout below that wait would otherwise
// cancel a legitimate queue and fail the trailing pod's startup. Both pods
// bootstrap the same database concurrently under a hostile 50ms default; both
// must succeed.
func TestEnsureSchemaPostgres_HostileTimeoutDoesNotTruncateLockWait(t *testing.T) {
	dsn, db := startPostgresStorage(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	_, err := db.ExecContext(t.Context(),
		`ALTER DATABASE schemabot SET statement_timeout = '50ms'`)
	require.NoError(t, err)

	var g errgroup.Group
	for range 2 {
		g.Go(func() error {
			return EnsureSchema(dsn, logger,
				WithDialect(schema.DialectPostgres),
				WithPostgresStatementTimeout(DefaultPostgresStatementTimeout))
		})
	}
	require.NoError(t, g.Wait())

	requireStorageTables(t, db)
}

// A genuine statement timeout is reported as one. When a bootstrap statement
// exhausts the budget in force, the error names the timeout and the budget
// instead of surfacing a bare failure, so an operator sees the cause from the
// message alone rather than having to recognize SQLSTATE 57014.
func TestEnsureSchemaPostgres_StatementTimeoutIsClassified(t *testing.T) {
	dsn, _ := startPostgresStorage(t)

	db, err := postgresconn.Open(dsn, postgresconn.WithStatementTimeout(100*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(t.Context()))

	const budget = 100 * time.Millisecond
	start := time.Now()
	_, execErr := db.ExecContext(t.Context(), "SELECT pg_sleep(5)")
	require.Error(t, execErr)

	classified := postgresStatementTimeoutError(execErr, budget, time.Since(start))
	assert.ErrorContains(t, classified, "statement timed out")
	assert.ErrorContains(t, classified, budget.String())
	// The server's own error is still reachable for anyone matching on it.
	var pgErr *pgconn.PgError
	require.ErrorAs(t, classified, &pgErr)
	assert.Equal(t, "57014", pgErr.Code)
}

// A convergence DDL statement cancelled with SQLSTATE 57014 reports the budget
// it ran under, both in the returned error and in the log. The log is the half
// that matters most: a bootstrap failure crashloops the pod before it serves
// anything, so the structured budget and elapsed fields are the only artifact
// left to say whether the statement exhausted SchemaBot's own budget or was
// cut short by something outside it.
func TestExecPostgresChanges_ClassifiesCancelledStatement(t *testing.T) {
	dsn, _ := startPostgresStorage(t)

	db, err := postgresconn.Open(dsn, postgresconn.WithStatementTimeout(DefaultPostgresStatementTimeout))
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(t.Context()))

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// The convergence raises statement_timeout to its own budget before
	// running a change, so a statement is only cancelled once something
	// tightens it again — which is what a role- or database-level value
	// imposed outside SchemaBot does. Reproduce that from inside the change.
	changes := []postgresSchemaChange{{
		operation: postgresOpCreateTable,
		object:    "cancelled_change",
		ddl:       "SET statement_timeout = '50ms'; SELECT pg_sleep(5);",
	}}

	execErr := execPostgresChanges(t.Context(), db, "cancelled_change", changes, logger)
	require.Error(t, execErr)

	// The rendered error names the budget the convergence set, so an operator
	// can compare it against how long the statement actually ran.
	assert.ErrorContains(t, execErr, postgresBootstrapDDLStatementTimeout.String())
	var pgErr *pgconn.PgError
	require.ErrorAs(t, execErr, &pgErr, "the server's own error must stay reachable")
	assert.Equal(t, "57014", pgErr.Code)

	assert.Contains(t, logs.String(), "storage schema change failed")
	assert.Contains(t, logs.String(), `"statement_timeout":`)
	assert.Contains(t, logs.String(), `"elapsed":`)
}

// A cancellation that arrives before the budget could have fired did not come
// from the budget. An operator's pg_cancel_backend raises the same SQLSTATE as
// statement_timeout, so reporting it as budget exhaustion would send triage
// after the wrong cause; elapsed time separates them.
func TestEnsureSchemaPostgres_ExternalCancelIsNotReportedAsTimeout(t *testing.T) {
	dsn, admin := startPostgresStorage(t)

	db, err := postgresconn.Open(dsn, postgresconn.WithStatementTimeout(0))
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })

	// Pin one session so the cancel targets the sleeping backend.
	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(conn) })
	var pid int
	require.NoError(t, conn.QueryRowContext(t.Context(), "SELECT pg_backend_pid()").Scan(&pid))

	// Run the sleep on the pinned session and cancel it from the test
	// goroutine, which is the only one that may assert. The sleeper reports
	// only its error and how long it ran.
	type sleepResult struct {
		err     error
		elapsed time.Duration
	}
	slept := make(chan sleepResult, 1)
	go func() {
		start := time.Now()
		_, err := conn.ExecContext(t.Context(), "SELECT pg_sleep(10)")
		slept <- sleepResult{err: err, elapsed: time.Since(start)}
	}()

	// Cancel only once the backend is actually running the sleep: signalling
	// an idle backend succeeds without cancelling anything, and the sleep
	// would then run to completion.
	require.Eventually(t, func() bool {
		var cancelled bool
		err := admin.QueryRowContext(t.Context(), `
			SELECT pg_cancel_backend(pid)
			FROM pg_stat_activity
			WHERE pid = $1 AND state = 'active' AND query LIKE '%pg_sleep(10)%'`, pid).Scan(&cancelled)
		return err == nil && cancelled
	}, 15*time.Second, 50*time.Millisecond, "backend never started the sleep")

	var res sleepResult
	select {
	case res = <-slept:
	case <-time.After(15 * time.Second):
		require.Fail(t, "cancelled statement did not return")
	}
	require.Error(t, res.err)

	// A budget far larger than the elapsed time: the cancel cannot be the
	// budget firing, and the message must say so.
	classified := postgresStatementTimeoutError(res.err, time.Hour, res.elapsed)
	assert.ErrorContains(t, classified, "something outside SchemaBot cancelled it")
	assert.NotContains(t, classified.Error(), "statement timed out")
}

// Convergence DDL runs under its own raised budget, not the connection's
// ordinary query budget. An index build legitimately takes far longer than a
// catalog read, so a bootstrap that executed DDL under the query budget would
// cancel healthy work. The probe runs as real convergence DDL and records the
// budget the server had in force while executing it; pg_settings reports the
// value in its base unit (milliseconds), so the assertion is exact rather than
// dependent on how PostgreSQL formats units.
func TestExecPostgresChanges_RaisesStatementTimeoutForDDL(t *testing.T) {
	dsn, admin := startPostgresStorage(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// A deliberately small session budget, so the value observed inside the
	// transaction can only come from the DDL raise.
	db, err := postgresconn.Open(dsn, postgresconn.WithStatementTimeout(time.Second))
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(t.Context()))

	probe := postgresSchemaChange{
		operation: postgresOpCreateTable,
		object:    "ddl_budget_probe",
		ddl: `CREATE TABLE ddl_budget_probe AS
		      SELECT setting AS observed FROM pg_settings WHERE name = 'statement_timeout'`,
	}
	require.NoError(t, execPostgresChanges(t.Context(), db, "ddl_budget_probe", []postgresSchemaChange{probe}, logger))

	var observed string
	require.NoError(t, admin.QueryRowContext(t.Context(), "SELECT observed FROM ddl_budget_probe").Scan(&observed))
	assert.Equal(t, strconv.FormatInt(postgresBootstrapDDLStatementTimeout.Milliseconds(), 10), observed)

	// The raise is transaction-local: the pooled connection goes back to its
	// ordinary query budget rather than carrying the DDL budget into later use.
	var after string
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT setting FROM pg_settings WHERE name = 'statement_timeout'").Scan(&after))
	assert.Equal(t, "1000", after)
}
