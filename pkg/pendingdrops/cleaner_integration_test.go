//go:build integration

package pendingdrops

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/block/schemabot/pkg/testutil"

	_ "github.com/go-sql-driver/mysql"
)

var sharedDSN string

func TestMain(m *testing.M) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "mysql:8.0",
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "testpassword",
			"MYSQL_DATABASE":      "testdb",
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("ready for connections").WithOccurrence(2).WithStartupTimeout(30*time.Second),
			wait.ForListeningPort("3306/tcp"),
		),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Fatalf("start mysql container: %v", err)
	}

	host, err := testutil.ContainerHost(ctx, container)
	if err != nil {
		log.Fatalf("get container host: %v", err)
	}
	port, err := testutil.ContainerPort(ctx, container, "3306")
	if err != nil {
		log.Fatalf("get container port: %v", err)
	}
	sharedDSN = fmt.Sprintf("root:testpassword@tcp(%s:%d)/testdb?parseTime=true", host, port)

	var db *sql.DB
	for range 30 {
		db, err = sql.Open("mysql", sharedDSN)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if err = db.PingContext(ctx); err == nil {
			if err := db.Close(); err != nil {
				log.Fatalf("close mysql readiness connection: %v", err)
			}
			break
		}
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("close mysql readiness connection: %v", closeErr)
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		log.Fatalf("connect to mysql: %v", err)
	}

	code := m.Run()

	if err := container.Terminate(ctx); err != nil {
		log.Printf("terminate mysql container: %v", err)
	}
	os.Exit(code)
}

func setupCleanerTest(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("mysql", sharedDSN)
	require.NoError(t, err, "connect to mysql")
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(t.Context()), "ping mysql")

	_, err = db.ExecContext(t.Context(), fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", Database))
	require.NoError(t, err, "reset quarantine database")
	return db
}

// createQuarantinedTable creates a table in the quarantine database named as
// if it was quarantined at the given time.
func createQuarantinedTable(t *testing.T, db *sql.DB, table string, quarantinedAt time.Time) string {
	t.Helper()
	_, err := db.ExecContext(t.Context(), fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", Database))
	require.NoError(t, err, "create quarantine database")

	name := TableName("testdb", table, quarantinedAt)
	_, err = db.ExecContext(t.Context(), fmt.Sprintf("CREATE TABLE `%s`.`%s` (id INT)", Database, name))
	require.NoError(t, err, "create quarantined table")
	return name
}

// createUnparseableQuarantineTable creates a table in the quarantine database
// whose name carries no timestamp prefix.
func createUnparseableQuarantineTable(t *testing.T, db *sql.DB, name string) {
	t.Helper()
	_, err := db.ExecContext(t.Context(), fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", Database))
	require.NoError(t, err, "create quarantine database")
	_, err = db.ExecContext(t.Context(), fmt.Sprintf("CREATE TABLE `%s`.`%s` (id INT)", Database, name))
	require.NoError(t, err, "create unparseable quarantined table")
}

func quarantinedTables(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(),
		"SELECT table_name FROM information_schema.tables WHERE table_schema = ? ORDER BY table_name",
		Database)
	require.NoError(t, err, "query quarantined tables")
	defer utils.CloseAndLog(rows)

	var tables []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name), "scan table name")
		tables = append(tables, name)
	}
	require.NoError(t, rows.Err(), "iterate quarantined tables")
	return tables
}

func testCleaner(t *testing.T, retention time.Duration, dryRun bool) *Cleaner {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	targets := []Target{{Database: "testdb", Environment: "staging", DSN: sharedDSN}}
	return NewCleaner(targets, retention, dryRun, logger)
}

// The cleaner drops quarantined tables older than the retention period and
// keeps fresh ones, so an accidental drop stays recoverable for the full
// retention window while expired tables are reclaimed.
func TestCleaner_DropsExpiredKeepsFresh(t *testing.T) {
	db := setupCleanerTest(t)

	now := time.Now()
	expired := createQuarantinedTable(t, db, "old_table", now.Add(-8*24*time.Hour))
	fresh := createQuarantinedTable(t, db, "new_table", now.Add(-1*24*time.Hour))

	cleaner := testCleaner(t, DefaultRetention, false)
	require.NoError(t, cleaner.Run(t.Context()))

	remaining := quarantinedTables(t, db)
	assert.NotContains(t, remaining, expired, "expired table should be dropped")
	assert.Contains(t, remaining, fresh, "fresh table should be kept")
}

// Tables whose names carry no valid timestamp prefix are never dropped: their
// age is unknown, so deleting them could destroy data that was quarantined
// recently or placed there manually.
func TestCleaner_NeverDropsUnparseableNames(t *testing.T) {
	db := setupCleanerTest(t)

	createUnparseableQuarantineTable(t, db, "manually_parked_table")

	cleaner := testCleaner(t, DefaultRetention, false)
	require.NoError(t, cleaner.Run(t.Context()))

	remaining := quarantinedTables(t, db)
	assert.Contains(t, remaining, "manually_parked_table")
}

// Dry-run mode reports expired tables without dropping them so operators can
// preview a retention change safely.
func TestCleaner_DryRunDropsNothing(t *testing.T) {
	db := setupCleanerTest(t)

	expired := createQuarantinedTable(t, db, "old_table", time.Now().Add(-30*24*time.Hour))

	cleaner := testCleaner(t, DefaultRetention, true)
	require.NoError(t, cleaner.Run(t.Context()))

	remaining := quarantinedTables(t, db)
	assert.Contains(t, remaining, expired, "dry run must not drop tables")
}

// A target without a quarantine database is a no-op, so the cleaner is safe to
// run against databases that never had a schema change drop a table.
func TestCleaner_NoQuarantineDatabase(t *testing.T) {
	db := setupCleanerTest(t)

	cleaner := testCleaner(t, DefaultRetention, false)
	require.NoError(t, cleaner.Run(t.Context()))

	var dbCount int
	err := db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?", Database).Scan(&dbCount)
	require.NoError(t, err)
	assert.Equal(t, 0, dbCount, "cleaner must not create the quarantine database")
}

// A custom retention shorter than the default is honored.
func TestCleaner_CustomRetention(t *testing.T) {
	db := setupCleanerTest(t)

	now := time.Now()
	expired := createQuarantinedTable(t, db, "two_days_old", now.Add(-2*24*time.Hour))
	fresh := createQuarantinedTable(t, db, "hours_old", now.Add(-2*time.Hour))

	cleaner := testCleaner(t, 24*time.Hour, false)
	require.NoError(t, cleaner.Run(t.Context()))

	remaining := quarantinedTables(t, db)
	assert.NotContains(t, remaining, expired)
	assert.Contains(t, remaining, fresh)
}

// A table-level cleanup failure is surfaced to the caller while the table
// remains quarantined for the next cleanup pass.
func TestCleaner_DropFailureReturnsError(t *testing.T) {
	db := setupCleanerTest(t)

	expired := createQuarantinedTable(t, db, "parent_table", time.Now().Add(-8*24*time.Hour))
	_, err := db.ExecContext(t.Context(), fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD PRIMARY KEY (id)", Database, expired))
	require.NoError(t, err, "add parent primary key")
	_, err = db.ExecContext(t.Context(), fmt.Sprintf("CREATE TABLE `%s`.`child_table` (id INT PRIMARY KEY, parent_id INT, CONSTRAINT fk_pending_parent FOREIGN KEY (parent_id) REFERENCES `%s`.`%s` (id))", Database, Database, expired))
	require.NoError(t, err, "create referencing child table")
	t.Cleanup(func() {
		_, cleanupErr := db.ExecContext(context.WithoutCancel(t.Context()), fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`child_table`", Database))
		require.NoError(t, cleanupErr, "drop child table")
	})

	cleaner := testCleaner(t, DefaultRetention, false)
	require.Error(t, cleaner.Run(t.Context()))

	remaining := quarantinedTables(t, db)
	assert.Contains(t, remaining, expired, "failed drop should remain quarantined")
}

// An unreachable target fails the pass with an error but does not panic; the
// failed target is retried on the next pass.
func TestCleaner_UnreachableTargetReturnsError(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	targets := []Target{{Database: "downdb", Environment: "staging", DSN: "root:wrong@tcp(127.0.0.1:1)/missing?timeout=2s"}}
	cleaner := NewCleaner(targets, DefaultRetention, false, logger)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.Error(t, cleaner.Run(ctx))
}
