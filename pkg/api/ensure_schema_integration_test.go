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

	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/block/schemabot/pkg/testutil"
)

func TestEnsureSchema(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	container, dsn, db := startEnsureSchemaContainer(t, ctx)
	defer func() { _ = container.Terminate(ctx) }()
	defer utils.CloseAndLog(db)

	// First call should create all tables using Spirit
	require.NoError(t, EnsureSchema(dsn, logger), "First EnsureSchema failed")

	// Verify tables exist
	tables := []string{"tasks", "plans", "locks", "checks", "settings", "apply_operations"}
	for _, table := range tables {
		assert.True(t, testutil.TableExists(t, db, "schemabot", table), "Table %s not found", table)
	}

	// tasks gains a nullable apply_operation_id column that is not
	// written by any caller yet. Verify the column landed so future PRs can
	// rely on it.
	assert.True(t, testutil.ColumnExists(t, db, "schemabot", "tasks", "apply_operation_id"),
		"tasks.apply_operation_id column not found")
}

func TestEnsureSchema_Idempotent(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	container, dsn, db := startEnsureSchemaContainer(t, ctx)
	defer func() { _ = container.Terminate(ctx) }()
	defer utils.CloseAndLog(db)

	// First call (tables may or may not exist from previous test)
	require.NoError(t, EnsureSchema(dsn, logger), "First EnsureSchema failed")

	// Second call should succeed without error (idempotent - no changes needed)
	require.NoError(t, EnsureSchema(dsn, logger), "Second EnsureSchema failed (not idempotent)")

	// Third call for good measure
	require.NoError(t, EnsureSchema(dsn, logger), "Third EnsureSchema failed (not idempotent)")
}

func TestEnsureSchema_CleansStaleSpiritTables(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	container, dsn, db := startEnsureSchemaContainer(t, ctx)
	defer func() { _ = container.Terminate(ctx) }()
	defer utils.CloseAndLog(db)

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
		assert.False(t, testutil.TableExists(t, db, "schemabot", tbl),
			"stale Spirit table %s should have been dropped", tbl)
	}

	// Verify real tables still exist.
	assert.True(t, testutil.TableExists(t, db, "schemabot", "tasks"),
		"real tasks table should still exist")

	assertEnsureSchemaDoesNotCleanSpiritTablesWhileWaitingForLock(t, ctx, dsn, db, logger)
}

func assertEnsureSchemaDoesNotCleanSpiritTablesWhileWaitingForLock(
	t *testing.T,
	ctx context.Context,
	dsn string,
	db *sql.DB,
	logger *slog.Logger,
) {
	t.Helper()
	// Simulate pod A actively running EnsureSchema. The lock is the production
	// coordination mechanism, and the shadow table represents Spirit work that
	// must not be cleaned up by a second pod before it acquires the lock.
	lockConn, err := acquireEnsureSchemaLock(ctx, dsn, logger)
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

	errs := make(chan error, 1)
	go func() {
		errs <- EnsureSchema(dsn, logger)
	}()

	waitForEnsureSchemaLockWaiter(t, db)
	assert.True(t, testutil.TableExists(t, db, "schemabot", shadowTable),
		"Spirit shadow table should not be cleaned while another pod holds the EnsureSchema lock")

	utils.CloseAndLog(lockConn)
	lockReleased = true

	select {
	case err := <-errs:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for EnsureSchema to finish after releasing lock")
	}

	assert.False(t, testutil.TableExists(t, db, "schemabot", shadowTable),
		"stale Spirit shadow table should be cleaned after EnsureSchema acquires the lock")
}

func TestEnsureSchema_ConcurrentPods(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	container, dsn, db := startEnsureSchemaContainer(t, ctx)
	defer func() { _ = container.Terminate(ctx) }()
	defer utils.CloseAndLog(db)

	// Simulate two pods starting simultaneously, both calling EnsureSchema.
	// The advisory lock should serialize them — both should succeed without
	// colliding on Spirit's shadow tables.
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			errs <- EnsureSchema(dsn, logger)
		}()
	}

	for range 2 {
		require.NoError(t, <-errs, "concurrent EnsureSchema failed")
	}

	// Verify tables exist after concurrent execution.
	assert.True(t, testutil.TableExists(t, db, "schemabot", "tasks"),
		"tasks table should exist after concurrent EnsureSchema")
}

func waitForEnsureSchemaLockWaiter(t *testing.T, db *sql.DB) {
	t.Helper()
	var count int
	require.Eventually(t, func() bool {
		err := db.QueryRowContext(t.Context(),
			`SELECT COUNT(*) FROM information_schema.PROCESSLIST
			 WHERE ID <> CONNECTION_ID()
			   AND INFO LIKE '%GET_LOCK%'`,
		).Scan(&count)
		require.NoError(t, err)
		return count > 0
	}, 10*time.Second, 100*time.Millisecond,
		"expected EnsureSchema to wait for the advisory lock, waiter count: %d", count)
}

// startEnsureSchemaContainer starts a MySQL container and returns the container, DSN, and DB.
func startEnsureSchemaContainer(t *testing.T, ctx context.Context) (testcontainers.Container, string, *sql.DB) {
	t.Helper()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "mysql:8.0",
			ExposedPorts: []string{"3306/tcp"},
			Env: map[string]string{
				"MYSQL_ROOT_PASSWORD": "testpassword",
				"MYSQL_DATABASE":      "schemabot",
			},
			WaitingFor: wait.ForAll(
				wait.ForLog("ready for connections").WithOccurrence(2).WithStartupTimeout(120*time.Second),
				wait.ForListeningPort("3306/tcp"),
			),
		},
		Started: true,
	})
	require.NoError(t, err, "Failed to start MySQL container")

	host, err := testutil.ContainerHost(ctx, container)
	require.NoError(t, err, "Failed to get container host")

	port, err := testutil.ContainerPort(ctx, container, "3306")
	require.NoError(t, err, "Failed to get container port")

	dsn := fmt.Sprintf("root:testpassword@tcp(%s:%d)/schemabot?parseTime=true", host, port)

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "Failed to connect to MySQL")

	// Wait for MySQL to be ready
	require.Eventually(t, func() bool {
		return db.PingContext(ctx) == nil
	}, 30*time.Second, time.Second, "MySQL did not become ready")

	return container, dsn, db
}

// A deployment that predates this change still has a live vitess_tasks table.
// Now that the embedded schema no longer declares it, an operator who opts in
// to destructive storage-schema changes can have EnsureSchema reconcile the
// obsolete table away cleanly — succeeding, removing it, and staying
// idempotent on the next run.
func TestEnsureSchema_RemovesObsoleteVitessTasks(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	container, dsn, db := startEnsureSchemaContainer(t, ctx)
	// Cleanup runs after the test, when t.Context() is already cancelled.
	defer func() { _ = container.Terminate(t.Context()) }()
	defer utils.CloseAndLog(db)

	// Bring the schema up to date, then simulate a pre-existing deployment by
	// recreating the obsolete table the embedded schema no longer declares.
	require.NoError(t, EnsureSchema(dsn, logger))
	_, err := db.ExecContext(ctx,
		"CREATE TABLE `vitess_tasks` (`id` bigint unsigned NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)
	require.True(t, testutil.TableExists(t, db, "schemabot", "vitess_tasks"))

	// EnsureSchema reconciles the obsolete table away without error...
	require.NoError(t, EnsureSchema(dsn, logger, WithAllowDestructiveSchemaChanges(true)),
		"EnsureSchema with an obsolete vitess_tasks table failed")
	assert.False(t, testutil.TableExists(t, db, "schemabot", "vitess_tasks"), "obsolete vitess_tasks should be removed")

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

	container, dsn, db := startEnsureSchemaContainer(t, ctx)
	defer func() { _ = container.Terminate(t.Context()) }()
	defer utils.CloseAndLog(db)

	require.NoError(t, EnsureSchema(dsn, logger))
	surplusColumn, surplusTable := seedSurplusStorageState(t, db)

	// Give EnsureSchema additive work alongside the destructive diff: drop an
	// embedded table so the diff must re-create it.
	_, err := db.ExecContext(ctx, "DROP TABLE `locks`")
	require.NoError(t, err)

	require.NoError(t, EnsureSchema(dsn, logger),
		"EnsureSchema with a destructive diff must not fail startup")

	// The additive change applied; the surplus state survived.
	assert.True(t, testutil.TableExists(t, db, "schemabot", "locks"),
		"additive CREATE TABLE should still be applied when destructive changes are refused")
	assert.True(t, testutil.ColumnExists(t, db, "schemabot", "tasks", surplusColumn),
		"surplus column from the newer schema must not be dropped by default")
	assert.True(t, testutil.TableExists(t, db, "schemabot", surplusTable),
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
	assert.True(t, testutil.ColumnExists(t, db, "schemabot", "tasks", surplusColumn))
	assert.True(t, testutil.TableExists(t, db, "schemabot", surplusTable))
}

// An operator who intentionally removed a storage table and column opts in to
// destructive storage-schema changes; EnsureSchema then executes the DROP
// statements and converges the database to the embedded schema.
func TestEnsureSchema_AllowDestructiveExecutesDrops(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	container, dsn, db := startEnsureSchemaContainer(t, ctx)
	defer func() { _ = container.Terminate(t.Context()) }()
	defer utils.CloseAndLog(db)

	require.NoError(t, EnsureSchema(dsn, logger))
	surplusColumn, surplusTable := seedSurplusStorageState(t, db)

	require.NoError(t, EnsureSchema(dsn, logger, WithAllowDestructiveSchemaChanges(true)),
		"EnsureSchema with destructive changes allowed failed")

	assert.False(t, testutil.ColumnExists(t, db, "schemabot", "tasks", surplusColumn),
		"surplus column should be dropped when destructive changes are allowed")
	assert.False(t, testutil.TableExists(t, db, "schemabot", surplusTable),
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
