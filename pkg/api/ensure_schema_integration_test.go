//go:build integration

package api

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/namedlock"
	"github.com/block/schemabot/pkg/testutil"
)

func TestEnsureSchema(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	sdb, db := openEnsureSchemaDatabase(t)
	dsn := sdb.DSN

	// First call should create all tables using Spirit
	require.NoError(t, EnsureSchema(dsn, logger), "First EnsureSchema failed")

	// Verify tables exist
	tables := []string{"tasks", "plans", "locks", "checks", "settings", "apply_operations"}
	for _, table := range tables {
		assert.True(t, testutil.TableExists(t, db, sdb.Name, table), "Table %s not found", table)
	}

	// tasks gains a nullable apply_operation_id column that is not
	// written by any caller yet. Verify the column landed so future PRs can
	// rely on it.
	assert.True(t, testutil.ColumnExists(t, db, sdb.Name, "tasks", "apply_operation_id"),
		"tasks.apply_operation_id column not found")
}

func TestEnsureSchema_Idempotent(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	dsn := newStorageDatabase(t).DSN

	// First call creates the tables.
	require.NoError(t, EnsureSchema(dsn, logger), "First EnsureSchema failed")

	// Second call should succeed without error (idempotent - no changes needed)
	require.NoError(t, EnsureSchema(dsn, logger), "Second EnsureSchema failed (not idempotent)")

	// Third call for good measure
	require.NoError(t, EnsureSchema(dsn, logger), "Third EnsureSchema failed (not idempotent)")
}

func TestEnsureSchema_CleansStaleSpiritTables(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	sdb, db := openEnsureSchemaDatabase(t)
	dsn := sdb.DSN

	// Bootstrap the schema first so real tables exist.
	require.NoError(t, EnsureSchema(dsn, logger))

	// Seed stale Spirit internal tables as if a previous pod was killed mid-apply.
	staleTables := []string{
		"_tasks_old",
		"_tasks_new",
		"_tasks_chkpnt",
		"_spirit_sentinel",
		"_spirit_checkpoint",
	}
	for _, tbl := range staleTables {
		_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE `%s` (id INT PRIMARY KEY)", tbl))
		require.NoError(t, err, "seed stale table %s", tbl)
	}

	// EnsureSchema should clean them up and succeed.
	require.NoError(t, EnsureSchema(dsn, logger))

	// Verify all stale tables were dropped.
	for _, tbl := range staleTables {
		assert.False(t, testutil.TableExists(t, db, sdb.Name, tbl),
			"stale Spirit table %s should have been dropped", tbl)
	}

	// Verify real tables still exist.
	assert.True(t, testutil.TableExists(t, db, sdb.Name, "tasks"),
		"real tasks table should still exist")

	assertEnsureSchemaDoesNotCleanSpiritTablesWhileWaitingForLock(t, ctx, sdb, db, logger)
}

func assertEnsureSchemaDoesNotCleanSpiritTablesWhileWaitingForLock(
	t *testing.T,
	ctx context.Context,
	sdb storageDatabase,
	db *sql.DB,
	logger *slog.Logger,
) {
	t.Helper()
	// Simulate pod A actively running EnsureSchema. The lock is the production
	// coordination mechanism, and the shadow table represents Spirit work that
	// must not be cleaned up by a second pod before it acquires the lock.
	lockConn, err := acquireMySQLEnsureSchemaLock(ctx, sdb.DSN, logger, namedlock.MySQL{})
	require.NoError(t, err)
	lockReleased := false
	defer func() {
		if !lockReleased {
			utils.CloseAndLog(lockConn)
		}
	}()

	const shadowTable = "_tasks_new"
	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE `%s` (id INT PRIMARY KEY)", shadowTable))
	require.NoError(t, err)

	errs := startEnsureSchema(t, sdb.DSN, logger)

	waitForEnsureSchemaLockWaiter(t, db, sdb.Name)
	assert.True(t, testutil.TableExists(t, db, sdb.Name, shadowTable),
		"Spirit shadow table should not be cleaned while another pod holds the EnsureSchema lock")

	utils.CloseAndLog(lockConn)
	lockReleased = true

	select {
	case err := <-errs:
		require.NoError(t, err)
	case <-time.After(ensureSchemaFinishDeadline):
		t.Fatal("timed out waiting for EnsureSchema to finish after releasing lock")
	}

	assert.False(t, testutil.TableExists(t, db, sdb.Name, shadowTable),
		"stale Spirit shadow table should be cleaned after EnsureSchema acquires the lock")
}

func TestEnsureSchema_ConcurrentPods(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	sdb, db := openEnsureSchemaDatabase(t)
	dsn := sdb.DSN

	// Simulate two pods starting simultaneously, both calling EnsureSchema.
	// The advisory lock should serialize them — both should succeed without
	// colliding on Spirit's shadow tables.
	podA := startEnsureSchema(t, dsn, logger)
	podB := startEnsureSchema(t, dsn, logger)

	// Collect both outcomes before asserting so a failure in one pod never
	// leaves the other running past the end of the test.
	errA, errB := <-podA, <-podB
	require.NoError(t, errA, "concurrent EnsureSchema failed")
	require.NoError(t, errB, "concurrent EnsureSchema failed")

	// Verify tables exist after concurrent execution.
	assert.True(t, testutil.TableExists(t, db, sdb.Name, "tasks"),
		"tasks table should exist after concurrent EnsureSchema")
}

// ensureSchemaFinishDeadline bounds how long a test waits for a background
// EnsureSchema to return once nothing is holding it back.
const ensureSchemaFinishDeadline = 30 * time.Second

// startEnsureSchema runs EnsureSchema in the background and returns the channel
// its result arrives on. The advisory lock EnsureSchema takes is server-wide,
// so the test always waits for the goroutine to finish before it ends: a
// straggler left running after a failed assertion would hold the lock against
// every later test in the package and stall them until it completed.
func startEnsureSchema(t *testing.T, dsn string, logger *slog.Logger) <-chan error {
	t.Helper()
	errs := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		errs <- EnsureSchema(dsn, logger)
	}()
	t.Cleanup(func() {
		select {
		case <-done:
		case <-time.After(ensureSchemaFinishDeadline):
			t.Error("background EnsureSchema still running at test end; it holds the ensure-schema lock against later tests")
		}
	})
	return errs
}

// waitForEnsureSchemaLockWaiter blocks until a session connected to database
// is waiting on an advisory lock. The database predicate matters: PROCESSLIST
// is server-wide, so without it a waiter from any other test on the shared
// server would satisfy the check.
func waitForEnsureSchemaLockWaiter(t *testing.T, db *sql.DB, database string) {
	t.Helper()
	var count int
	require.Eventually(t, func() bool {
		err := db.QueryRowContext(t.Context(),
			`SELECT COUNT(*) FROM information_schema.PROCESSLIST
			 WHERE ID <> CONNECTION_ID()
			   AND DB = ?
			   AND INFO LIKE '%GET_LOCK%'`,
			database,
		).Scan(&count)
		require.NoError(t, err)
		return count > 0
	}, 10*time.Second, 100*time.Millisecond,
		"expected EnsureSchema to wait for the advisory lock in %s, waiter count: %d", database, count)
}

// openEnsureSchemaDatabase gives the test an empty database on the shared
// MySQL server with an open handle; running EnsureSchema is left to the test.
func openEnsureSchemaDatabase(t *testing.T) (storageDatabase, *sql.DB) {
	t.Helper()
	sdb := newStorageDatabase(t)
	return sdb, openStorageDB(t, sdb.DSN)
}

// A deployment that predates this change still has a live vitess_tasks table.
// Now that the embedded schema no longer declares it, an operator who opts in
// to destructive storage-schema changes can have EnsureSchema reconcile the
// obsolete table away cleanly — succeeding, removing it, and staying
// idempotent on the next run.
func TestEnsureSchema_RemovesObsoleteVitessTasks(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	sdb, db := openEnsureSchemaDatabase(t)
	dsn := sdb.DSN

	// Bring the schema up to date, then simulate a pre-existing deployment by
	// recreating the obsolete table the embedded schema no longer declares.
	require.NoError(t, EnsureSchema(dsn, logger))
	_, err := db.ExecContext(ctx,
		"CREATE TABLE `vitess_tasks` (`id` bigint unsigned NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)
	require.True(t, testutil.TableExists(t, db, sdb.Name, "vitess_tasks"))

	// EnsureSchema reconciles the obsolete table away without error...
	require.NoError(t, EnsureSchema(dsn, logger, WithAllowDestructiveSchemaChanges(true)),
		"EnsureSchema with an obsolete vitess_tasks table failed")
	assert.False(t, testutil.TableExists(t, db, sdb.Name, "vitess_tasks"), "obsolete vitess_tasks should be removed")

	// ...and the next run is a clean no-op.
	require.NoError(t, EnsureSchema(dsn, logger, WithAllowDestructiveSchemaChanges(true)),
		"second EnsureSchema not idempotent")
}

// seedSurplusStorageState simulates storage state written by a newer binary:
// a column and a table that exist in the live storage database but that the
// starting binary's embedded schema does not declare. The Spirit diff turns
// each into destructive DDL (ALTER ... DROP COLUMN and DROP TABLE).
func seedSurplusStorageState(t *testing.T, db *sql.DB) (surplusColumn, surplusTable string) {
	t.Helper()
	surplusColumn = "newer_binary_col"
	surplusTable = "newer_binary_feature"

	_, err := db.ExecContext(t.Context(),
		fmt.Sprintf("ALTER TABLE `tasks` ADD COLUMN `%s` varchar(64) DEFAULT NULL", surplusColumn))
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(),
		fmt.Sprintf("CREATE TABLE `%s` (`id` bigint unsigned NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", surplusTable))
	require.NoError(t, err)
	return surplusColumn, surplusTable
}

// During a rolling deploy or rollback, an older binary's pod starts against a
// storage database that a newer binary already converged: the database holds a
// column and a table the older binary's embedded schema does not declare. By
// default EnsureSchema must refuse the destructive statements the diff emits
// for that surplus state (so the old binary cannot destroy the newer schema),
// warn with the exact DDL, and still apply the additive changes the older
// binary needs — startup proceeds either way.
func TestEnsureSchema_RefusesDestructiveChangesByDefault(t *testing.T) {
	ctx := t.Context()
	var logBuf syncBuffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	sdb, db := openEnsureSchemaDatabase(t)
	dsn := sdb.DSN

	require.NoError(t, EnsureSchema(dsn, logger))
	surplusColumn, surplusTable := seedSurplusStorageState(t, db)

	// Give EnsureSchema additive work alongside the destructive diff: drop an
	// embedded table so the diff must re-create it.
	_, err := db.ExecContext(ctx, "DROP TABLE `locks`")
	require.NoError(t, err)

	require.NoError(t, EnsureSchema(dsn, logger),
		"EnsureSchema with a destructive diff must not fail startup")

	// The additive change applied; the surplus state survived.
	assert.True(t, testutil.TableExists(t, db, sdb.Name, "locks"),
		"additive CREATE TABLE should still be applied when destructive changes are refused")
	assert.True(t, testutil.ColumnExists(t, db, sdb.Name, "tasks", surplusColumn),
		"surplus column from the newer schema must not be dropped by default")
	assert.True(t, testutil.TableExists(t, db, sdb.Name, surplusTable),
		"surplus table from the newer schema must not be dropped by default")

	// Each refusal is logged with the exact DDL so an operator can see what was
	// skipped and how to opt in.
	logs := logBuf.String()
	assert.Contains(t, logs, "refusing destructive storage-schema change")
	assert.Contains(t, logs, "allow_destructive_schema_changes")
	assert.Contains(t, logs, "DROP COLUMN")
	assert.Contains(t, logs, surplusColumn)
	assert.Contains(t, logs, "DROP TABLE")
	assert.Contains(t, logs, surplusTable)

	// A repeat run keeps refusing without error or changes.
	require.NoError(t, EnsureSchema(dsn, logger), "repeat EnsureSchema with refused changes failed")
	assert.True(t, testutil.ColumnExists(t, db, sdb.Name, "tasks", surplusColumn))
	assert.True(t, testutil.TableExists(t, db, sdb.Name, surplusTable))
}

// When the live storage database drifts from the embedded schema on the same
// table in both directions — it misses a column the starting binary requires
// and holds a surplus column a newer binary wrote — Spirit's diff emits one
// combined ALTER mixing an ADD COLUMN with a DROP COLUMN. EnsureSchema must
// split that statement: the required column is added so the binary can run,
// while the destructive clause is refused and the surplus column survives.
func TestEnsureSchema_MixedAlterAppliesSafeClauses(t *testing.T) {
	ctx := t.Context()
	var logBuf syncBuffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	sdb, db := openEnsureSchemaDatabase(t)
	dsn := sdb.DSN

	require.NoError(t, EnsureSchema(dsn, logger))

	// Same-table drift in both directions: `tasks` misses an embedded column
	// the binary requires and holds a surplus column it does not declare.
	const missingColumn = "throttle_reason"
	surplusColumn, _ := seedSurplusStorageState(t, db)
	_, err := db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE `tasks` DROP COLUMN `%s`", missingColumn))
	require.NoError(t, err)
	require.False(t, testutil.ColumnExists(t, db, sdb.Name, "tasks", missingColumn))

	require.NoError(t, EnsureSchema(dsn, logger),
		"EnsureSchema with a mixed additive/destructive ALTER must not fail startup")

	// The required column was added; the surplus column survived.
	assert.True(t, testutil.ColumnExists(t, db, sdb.Name, "tasks", missingColumn),
		"missing embedded column must be added even when the same ALTER carries destructive clauses")
	assert.True(t, testutil.ColumnExists(t, db, sdb.Name, "tasks", surplusColumn),
		"surplus column from the newer schema must not be dropped by default")

	// The refusal names only the destructive clauses, not the additive ones,
	// and carries the combined ALTER it was split from.
	logs := logBuf.String()
	assert.Contains(t, logs, "refusing destructive clauses of a mixed storage-schema ALTER")
	assert.Contains(t, logs, "split_from_ddl")
	assert.Contains(t, logs, surplusColumn)

	// A repeat run converges: the additive work is done, the destructive
	// remainder keeps being refused without error.
	require.NoError(t, EnsureSchema(dsn, logger), "repeat EnsureSchema after mixed split failed")
	assert.True(t, testutil.ColumnExists(t, db, sdb.Name, "tasks", missingColumn))
	assert.True(t, testutil.ColumnExists(t, db, sdb.Name, "tasks", surplusColumn))
}

// A live storage table whose primary key is wider than the embedded schema's
// makes the Spirit diff emit a combined DROP PRIMARY KEY, ADD PRIMARY KEY
// statement. The ADD half cannot run without the refused DROP, so the change
// is refused whole: startup succeeds and the wider primary key survives.
func TestEnsureSchema_RefusesPrimaryKeyChangeWhole(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	sdb, db := openEnsureSchemaDatabase(t)
	dsn := sdb.DSN

	require.NoError(t, EnsureSchema(dsn, logger))

	// Widen the primary key beyond the embedded schema's declaration. The
	// AUTO_INCREMENT column stays leftmost so the live table remains valid.
	_, err := db.ExecContext(ctx, "ALTER TABLE `tasks` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `apply_id`)")
	require.NoError(t, err)

	primaryKeyColumns := func() int {
		var n int
		require.NoError(t, db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'tasks' AND INDEX_NAME = 'PRIMARY'", sdb.Name).Scan(&n))
		return n
	}
	require.Equal(t, 2, primaryKeyColumns())

	require.NoError(t, EnsureSchema(dsn, logger),
		"EnsureSchema with a refused primary-key change must not fail startup")
	assert.Equal(t, 2, primaryKeyColumns(),
		"the wider live primary key must survive: an ADD PRIMARY KEY cannot execute without the refused DROP PRIMARY KEY")
}

// An operator who intentionally removed a storage table and column opts in to
// destructive storage-schema changes; EnsureSchema then executes the DROP
// statements and converges the database to the embedded schema.
func TestEnsureSchema_AllowDestructiveExecutesDrops(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	sdb, db := openEnsureSchemaDatabase(t)
	dsn := sdb.DSN

	require.NoError(t, EnsureSchema(dsn, logger))
	surplusColumn, surplusTable := seedSurplusStorageState(t, db)

	require.NoError(t, EnsureSchema(dsn, logger, WithAllowDestructiveSchemaChanges(true)),
		"EnsureSchema with destructive changes allowed failed")

	assert.False(t, testutil.ColumnExists(t, db, sdb.Name, "tasks", surplusColumn),
		"surplus column should be dropped when destructive changes are allowed")
	assert.False(t, testutil.TableExists(t, db, sdb.Name, surplusTable),
		"surplus table should be dropped when destructive changes are allowed")

	require.NoError(t, EnsureSchema(dsn, logger, WithAllowDestructiveSchemaChanges(true)),
		"second EnsureSchema not idempotent")
}

// syncBuffer is an io.Writer safe for concurrent log writes from EnsureSchema
// and Spirit's background goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
