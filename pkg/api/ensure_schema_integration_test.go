//go:build integration

package api

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/namedlock"
	"github.com/block/schemabot/pkg/schema"
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

// A cold bootstrap must refuse nothing. The refusal gate reads the plan's
// unsafe verdict, which the engine sets when any of its linters reports an
// error against a statement — not only the removals this gate exists to stop.
// So a new embedded schema file that trips any other error-level rule would be
// refused rather than created, and because refusing is deliberately not a
// startup failure, the table would simply never exist while every pod reported
// a healthy boot. Walking the embedded schema rather than a hand-listed set of
// tables is what makes that fail here, at the point the file is added.
func TestEnsureSchema_ColdBootstrapCreatesEveryEmbeddedTable(t *testing.T) {
	var logBuf syncBuffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	sdb, db := openEnsureSchemaDatabase(t)

	require.NoError(t, EnsureSchema(sdb.DSN, logger), "cold EnsureSchema failed")

	entries, err := schema.MySQLFS.ReadDir("mysql")
	require.NoError(t, err)
	require.NotEmpty(t, entries, "the embedded MySQL schema must not be empty")
	for _, entry := range entries {
		table := strings.TrimSuffix(entry.Name(), ".sql")
		assert.True(t, testutil.TableExists(t, db, sdb.Name, table),
			"embedded schema declares %s, so a cold bootstrap must create it", table)
	}

	assert.NotContains(t, logBuf.String(), "refusing destructive storage-schema change",
		"nothing in the embedded schema may be refused against an empty database")
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
	lockConn, err := acquireMySQLEnsureSchemaLock(ctx, sdb.DSN, logger, namedlock.MySQL{}, EnsureSchemaTimeout)
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
	require.NoError(t, EnsureSchema(dsn, logger, WithDestructiveSchemaChangePolicy(true, false)),
		"EnsureSchema with an obsolete vitess_tasks table failed")
	assert.False(t, testutil.TableExists(t, db, sdb.Name, "vitess_tasks"), "obsolete vitess_tasks should be removed")

	// ...and the next run is a clean no-op.
	require.NoError(t, EnsureSchema(dsn, logger, WithDestructiveSchemaChangePolicy(true, false)),
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
// combined ALTER mixing an ADD COLUMN with a DROP COLUMN. The ADD executes and
// the DROP does not: the starting binary gets the column its own queries name,
// the surplus column survives for the newer binary, and startup continues.
func TestEnsureSchema_RunsTheAdditiveClausesOfAMixedAlter(t *testing.T) {
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
		"a mixed ALTER with withheld clauses must not fail startup")

	// The addition ran and the removal did not.
	assert.True(t, testutil.ColumnExists(t, db, sdb.Name, "tasks", missingColumn),
		"the column the starting binary requires must be added, not withheld because a drop rode along with it")
	assert.True(t, testutil.ColumnExists(t, db, sdb.Name, "tasks", surplusColumn),
		"surplus column from the newer schema must not be dropped by default")

	// The warning names the clauses that did not run and the opt-in that would
	// run them, and says the additions did run so an operator is not left
	// looking for a column that is already there.
	logs := logBuf.String()
	assert.Contains(t, logs, "withholding the destructive clauses of a storage-schema change")
	assert.Contains(t, logs, "allow_destructive_schema_changes")
	assert.Contains(t, logs, surplusColumn)

	// A repeat run withholds the same clause without error or changes, and the
	// converged column stays converged.
	require.NoError(t, EnsureSchema(dsn, logger), "repeat EnsureSchema with a withheld clause failed")
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

// surplusIndexName and surplusIndexColumns describe an index that exists in
// the live storage database but that the starting binary's embedded schema does
// not declare — the shape a newer binary's index takes to an older one during a
// rolling deploy or rollback.
const surplusIndexName = "idx_newer_binary"

var surplusIndexColumns = []string{"environment", "created_at"}

// seedSurplusIndex adds the surplus index to the live `tasks` table. The Spirit
// diff turns it into an ALTER ... DROP INDEX, which loses no data and so is
// invisible to Spirit's unsafe vocabulary.
func seedSurplusIndex(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(t.Context(),
		fmt.Sprintf("ALTER TABLE `tasks` ADD INDEX `%s` (`%s`)", surplusIndexName, strings.Join(surplusIndexColumns, "`,`")))
	require.NoError(t, err)
}

// During a rolling deploy or rollback, an older binary's pod starts against a
// storage database holding an index its embedded schema does not declare. The
// drop the diff emits loses no data, completes in milliseconds because it is
// metadata-only, and can still take the database down by regressing the plan of
// a query the rest of the fleet is running. EnsureSchema must refuse it by
// default, leave the index intact, and let startup proceed.
func TestEnsureSchema_RefusesIndexDropByDefault(t *testing.T) {
	var logBuf syncBuffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	sdb, db := openEnsureSchemaDatabase(t)
	dsn := sdb.DSN

	require.NoError(t, EnsureSchema(dsn, logger))
	seedSurplusIndex(t, db)
	require.Equal(t, surplusIndexColumns, testutil.IndexColumns(t, db, sdb.Name, "tasks", surplusIndexName))

	require.NoError(t, EnsureSchema(dsn, logger),
		"EnsureSchema with an index drop in the diff must not fail startup")

	assert.Equal(t, surplusIndexColumns, testutil.IndexColumns(t, db, sdb.Name, "tasks", surplusIndexName),
		"a surplus index from the newer schema must survive intact: dropping it loses no data but can regress the fleet's query plans")

	// The refusal names the index and the opt-in, so an operator who removed it
	// on purpose can see how to proceed.
	logs := logBuf.String()
	assert.Contains(t, logs, "refusing destructive storage-schema change")
	assert.Contains(t, logs, "allow_destructive_schema_changes")
	assert.Contains(t, logs, "DROP INDEX")
	assert.Contains(t, logs, surplusIndexName)

	// A repeat run keeps refusing without error or changes.
	require.NoError(t, EnsureSchema(dsn, logger), "repeat EnsureSchema with a refused index drop failed")
	assert.Equal(t, surplusIndexColumns, testutil.IndexColumns(t, db, sdb.Name, "tasks", surplusIndexName))
}

// Spirit's diff emits one combined ALTER per table, so an index drop reaches
// the bootstrap bundled with whatever else that table drifted by — here a
// column the starting binary requires. The index drop is withheld and the
// column is added: the index survives intact, which is the refusal's whole
// purpose, and the binary still gets the column it needs to serve.
func TestEnsureSchema_RunsTheAdditiveClausesBesideAnIndexDrop(t *testing.T) {
	ctx := t.Context()
	var logBuf syncBuffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	sdb, db := openEnsureSchemaDatabase(t)
	dsn := sdb.DSN

	require.NoError(t, EnsureSchema(dsn, logger))

	// Same-table drift in both directions: `tasks` misses an embedded column
	// the binary requires and holds a surplus index it does not declare.
	const missingColumn = "throttle_reason"
	seedSurplusIndex(t, db)
	_, err := db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE `tasks` DROP COLUMN `%s`", missingColumn))
	require.NoError(t, err)
	require.False(t, testutil.ColumnExists(t, db, sdb.Name, "tasks", missingColumn))

	require.NoError(t, EnsureSchema(dsn, logger),
		"a mixed ALTER with withheld clauses must not fail startup")

	assert.True(t, testutil.ColumnExists(t, db, sdb.Name, "tasks", missingColumn),
		"the column the starting binary requires must be added, not withheld because an index drop rode along with it")
	assert.Equal(t, surplusIndexColumns, testutil.IndexColumns(t, db, sdb.Name, "tasks", surplusIndexName),
		"a surplus index must survive intact: dropping it loses no data but can regress the fleet's query plans")

	// The warning names the index that was protected, so an operator who
	// removed it on purpose can see how to proceed.
	logs := logBuf.String()
	assert.Contains(t, logs, "withholding the destructive clauses of a storage-schema change")
	assert.Contains(t, logs, "allow_destructive_schema_changes")
	assert.Contains(t, logs, surplusIndexName)

	// A repeat run withholds the same clause without error or changes.
	require.NoError(t, EnsureSchema(dsn, logger), "repeat EnsureSchema with a withheld clause failed")
	assert.True(t, testutil.ColumnExists(t, db, sdb.Name, "tasks", missingColumn))
	assert.Equal(t, surplusIndexColumns, testutil.IndexColumns(t, db, sdb.Name, "tasks", surplusIndexName))
}

// MySQL keeps column names and index names in separate namespaces, so a table
// can carry an index named the same as one of its columns. When the surplus
// index a newer binary left behind shares its name with a column the starting
// binary requires, nothing collides: the column is added while the index drop
// is withheld. Reading the two names as one namespace would withhold the
// addition too, and the pod would report a healthy boot and then serve against
// storage missing a column its own queries name.
func TestEnsureSchema_AddsAColumnNamedLikeAWithheldIndex(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	sdb, db := openEnsureSchemaDatabase(t)
	dsn := sdb.DSN

	require.NoError(t, EnsureSchema(dsn, logger))

	// The column the starting binary requires is missing, and a surplus index
	// stands under that same name.
	const missingColumn = "throttle_reason"
	_, err := db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE `tasks` DROP COLUMN `%s`", missingColumn))
	require.NoError(t, err)
	_, err = db.ExecContext(ctx,
		fmt.Sprintf("ALTER TABLE `tasks` ADD INDEX `%s` (`%s`)", missingColumn, strings.Join(surplusIndexColumns, "`,`")))
	require.NoError(t, err)
	require.False(t, testutil.ColumnExists(t, db, sdb.Name, "tasks", missingColumn))

	require.NoError(t, EnsureSchema(dsn, logger),
		"a withheld index drop sharing a name with a required column must not fail startup")

	assert.True(t, testutil.ColumnExists(t, db, sdb.Name, "tasks", missingColumn),
		"the column the starting binary requires must be added: an index of the same name occupies a different namespace and collides with nothing")
	assert.Equal(t, surplusIndexColumns, testutil.IndexColumns(t, db, sdb.Name, "tasks", missingColumn),
		"the surplus index must survive intact, whatever it shares its name with")
}

// A withheld clause stays in the diff for as long as the drift stands, and pods
// restart for reasons that have nothing to do with a deploy: a node drain, an
// OOM kill, a scale-up. So the boot after a split must recognize that the
// additions already landed and reach Spirit with nothing at all. Re-running the
// additions would put SchemaBot's own tables through a copy on every pod start,
// against the database the whole fleet reads, which is a worse outage than the
// drop being refused.
func TestEnsureSchema_WithheldClauseConvergesWithoutFurtherDDL(t *testing.T) {
	ctx := t.Context()
	sdb, db := openEnsureSchemaDatabase(t)
	dsn := sdb.DSN

	var boot syncBuffer
	bootLogger := slog.New(slog.NewTextHandler(&boot, &slog.HandlerOptions{Level: slog.LevelDebug}))
	require.NoError(t, EnsureSchema(dsn, bootLogger))

	const missingColumn = "throttle_reason"
	seedSurplusIndex(t, db)
	_, err := db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE `tasks` DROP COLUMN `%s`", missingColumn))
	require.NoError(t, err)

	var first syncBuffer
	require.NoError(t, EnsureSchema(dsn, slog.New(slog.NewTextHandler(&first, &slog.HandlerOptions{Level: slog.LevelDebug}))))
	require.True(t, testutil.ColumnExists(t, db, sdb.Name, "tasks", missingColumn))
	require.Contains(t, first.String(), "applying storage schema changes",
		"the first boot after the drift must apply the additions it partitioned out")

	var second syncBuffer
	require.NoError(t, EnsureSchema(dsn, slog.New(slog.NewTextHandler(&second, &slog.HandlerOptions{Level: slog.LevelDebug}))))

	logs := second.String()
	assert.Contains(t, logs, "all planned storage schema changes are destructive and refused",
		"the withheld drop is still outstanding, so every later boot must still refuse it")
	assert.NotContains(t, logs, "applying storage schema changes",
		"a converged split must reach Spirit with no DDL on later boots, or every pod restart copies SchemaBot's own tables")
	assert.Equal(t, surplusIndexColumns, testutil.IndexColumns(t, db, sdb.Name, "tasks", surplusIndexName))
}

// A rollback starts many pods at once, each running the bootstrap against the
// same storage database. With a mixed statement in the diff, whichever pod wins
// the advisory lock partitions it and applies the additions; the others re-plan
// once the lock frees and find only the refusal left. Every pod must start, the
// column they all need must exist, and none of them may take the surplus index
// the rest of the fleet plans around.
func TestEnsureSchema_ConcurrentPodsDuringRollbackWithholdTheSameDrop(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	sdb, db := openEnsureSchemaDatabase(t)
	dsn := sdb.DSN
	require.NoError(t, EnsureSchema(dsn, logger))

	const missingColumn = "throttle_reason"
	seedSurplusIndex(t, db)
	_, err := db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE `tasks` DROP COLUMN `%s`", missingColumn))
	require.NoError(t, err)

	podA := startEnsureSchema(t, dsn, logger)
	podB := startEnsureSchema(t, dsn, logger)
	podC := startEnsureSchema(t, dsn, logger)

	// Collect every outcome before asserting so a failure in one pod never
	// leaves another holding the server-wide advisory lock past the test.
	errA, errB, errC := <-podA, <-podB, <-podC
	require.NoError(t, errA, "a pod must start while a mixed statement is being partitioned")
	require.NoError(t, errB, "a pod must start while a mixed statement is being partitioned")
	require.NoError(t, errC, "a pod must start while a mixed statement is being partitioned")

	assert.True(t, testutil.ColumnExists(t, db, sdb.Name, "tasks", missingColumn),
		"the column every one of these binaries requires must exist once they have all started")
	assert.Equal(t, surplusIndexColumns, testutil.IndexColumns(t, db, sdb.Name, "tasks", surplusIndexName),
		"no pod may drop the surplus index, however many of them raced to converge")
}

// An operator who intentionally removed an index from the embedded schema opts
// in to destructive storage-schema changes; EnsureSchema then executes the drop
// and converges the database to the embedded schema. Without this the flag
// would be unreachable for indexes and a deliberate removal would have no
// supported path.
func TestEnsureSchema_AllowDestructiveExecutesIndexDrop(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	sdb, db := openEnsureSchemaDatabase(t)
	dsn := sdb.DSN

	require.NoError(t, EnsureSchema(dsn, logger))
	seedSurplusIndex(t, db)
	require.Equal(t, surplusIndexColumns, testutil.IndexColumns(t, db, sdb.Name, "tasks", surplusIndexName))

	require.NoError(t, EnsureSchema(dsn, logger, WithDestructiveSchemaChangePolicy(true, false)),
		"EnsureSchema with destructive changes allowed failed")

	assert.Empty(t, testutil.IndexColumns(t, db, sdb.Name, "tasks", surplusIndexName),
		"surplus index should be dropped when destructive changes are allowed")

	require.NoError(t, EnsureSchema(dsn, logger, WithDestructiveSchemaChangePolicy(true, false)),
		"second EnsureSchema not idempotent")
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

	require.NoError(t, EnsureSchema(dsn, logger, WithDestructiveSchemaChangePolicy(true, false)),
		"EnsureSchema with destructive changes allowed failed")

	assert.False(t, testutil.ColumnExists(t, db, sdb.Name, "tasks", surplusColumn),
		"surplus column should be dropped when destructive changes are allowed")
	assert.False(t, testutil.TableExists(t, db, sdb.Name, surplusTable),
		"surplus table should be dropped when destructive changes are allowed")

	require.NoError(t, EnsureSchema(dsn, logger, WithDestructiveSchemaChangePolicy(true, false)),
		"second EnsureSchema not idempotent")
}

// A convergence changes the shape of SchemaBot's storage and nothing else in
// it: it records no row about itself in the database it is converging. That is
// what lets one implementation serve both a boot against a database with no
// schema at all — where a table to record into does not exist yet — and a
// deliberate convergence against tables Spirit is copying under live traffic,
// where a write about the convergence would land in a table mid-copy.
//
// The census reads the live catalog rather than a list of tables, so a table
// added to the embedded schema is covered the day it lands rather than when
// someone remembers this test.
func TestEnsureSchema_RecordsNothingInTheStorageItConverges(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	sdb, db := openEnsureSchemaDatabase(t)
	require.NoError(t, EnsureSchema(sdb.DSN, logger), "first EnsureSchema")

	// Rows for a spurious write to be visible against, on the table the
	// convergence below copies — so the census also proves the copy carried
	// them across rather than merely that nothing new appeared.
	seedStorageSettings(t, db, 3)
	before := storageRowCensus(t, db, sdb.Name)

	// Drift that makes the convergence do real DDL. Re-adding the column is a
	// Spirit table copy of a seeded table, which is the case that would show a
	// row gained or lost.
	_, err := db.ExecContext(t.Context(), "ALTER TABLE `settings` DROP COLUMN `updated_at`")
	require.NoError(t, err, "introduce drift on settings")

	require.NoError(t, EnsureSchema(sdb.DSN, logger), "second EnsureSchema")
	require.True(t, testutil.ColumnExists(t, db, sdb.Name, "settings", "updated_at"),
		"the convergence must have run the DDL whose side effects this test measures")

	assert.Equal(t, before, storageRowCensus(t, db, sdb.Name),
		"a convergence must not add, remove, or lose a row in the storage it converges")
}

// seedStorageSettings writes count rows a convergence has to carry across a
// table copy untouched.
func seedStorageSettings(t *testing.T, db *sql.DB, count int) {
	t.Helper()
	for i := range count {
		_, err := db.ExecContext(t.Context(),
			"INSERT INTO `settings` (`setting_key`, `setting_value`) VALUES (?, ?)",
			fmt.Sprintf("census-key-%d", i), fmt.Sprintf("census-value-%d", i))
		require.NoError(t, err, "seed settings row %d", i)
	}
}

// storageRowCensus counts the rows in every table the storage database holds,
// Spirit's own internal tables aside — those are the convergence's scaffolding
// and come and go with it.
func storageRowCensus(t *testing.T, db *sql.DB, database string) map[string]int64 {
	t.Helper()

	rows, err := db.QueryContext(t.Context(),
		"SELECT table_name FROM information_schema.tables WHERE table_schema = ?", database)
	require.NoError(t, err, "list tables in %s", database)
	var tables []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name), "scan table name")
		if ddl.IsSpiritInternalTable(name) {
			continue
		}
		tables = append(tables, name)
	}
	require.NoError(t, rows.Err(), "iterate tables in %s", database)
	require.NoError(t, rows.Close(), "close table listing")
	require.NotEmpty(t, tables, "a converged storage database has tables to count")

	census := make(map[string]int64, len(tables))
	for _, table := range tables {
		var count int64
		require.NoError(t, db.QueryRowContext(t.Context(),
			fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)).Scan(&count),
			"count rows in %s", table)
		census[table] = count
	}
	return census
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
