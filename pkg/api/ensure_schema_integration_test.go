//go:build integration

package api

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
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
	tables := []string{"tasks", "plans", "locks", "checks", "settings", "vitess_apply_data", "vitess_tasks"}
	for _, table := range tables {
		var count int
		err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'schemabot' AND table_name = ?", table).Scan(&count)
		require.NoError(t, err, "Failed to check table %s", table)
		assert.Equal(t, 1, count, "Table %s not found", table)
	}
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
		var count int
		err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'schemabot' AND table_name = ?", tbl,
		).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count, "stale Spirit table %s should have been dropped", tbl)
	}

	// Verify real tables still exist.
	var count int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'schemabot' AND table_name = 'tasks'",
	).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "real tasks table should still exist")
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
	var count int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'schemabot' AND table_name = 'tasks'",
	).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "tasks table should exist after concurrent EnsureSchema")
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
