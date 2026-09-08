//go:build integration

package api

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mysqldriver "github.com/block/mysql"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/block/schemabot/pkg/testutil"
)

// sharedMySQL is the single MySQL server every MySQL-backed integration test in
// this package shares. Booting a MySQL container costs seconds, so the server
// starts on first use — a run that selects only unit or PostgreSQL tests never
// pays for it — and a test that needs MySQL storage carves out its own database
// on it with newStorageDatabase, which costs a CREATE DATABASE. TestMain
// terminates the server once the package finishes.
var sharedMySQL struct {
	once      sync.Once
	container *mysql.MySQLContainer
	dsn       string
	err       error
}

// storageDatabaseSeq numbers the per-test databases so each test works in a
// database no other test has touched.
var storageDatabaseSeq atomic.Int64

// storageSetupTimeout bounds each administrative statement newStorageDatabase
// runs against the shared server, so an unreachable server fails the test with
// a named cause instead of hanging until the binary timeout.
const storageSetupTimeout = 30 * time.Second

func TestMain(m *testing.M) {
	os.Exit(runWithSharedMySQL(m))
}

// runWithSharedMySQL runs the package tests and then terminates the shared
// MySQL server if any test started it. TerminateContainer is nil-safe, so the
// deferred call is a no-op when no test needed MySQL, and it also reaps a
// container that startSharedMySQL handed back alongside an error.
func runWithSharedMySQL(m *testing.M) int {
	defer func() {
		if err := testcontainers.TerminateContainer(sharedMySQL.container); err != nil {
			log.Printf("terminate shared mysql container: %v", err)
		}
	}()
	return m.Run()
}

// sharedMySQLDSN returns the root DSN of the shared server, booting it on the
// first call. A boot failure is recorded and reported to every later caller,
// so each MySQL-backed test fails with the same cause rather than retrying a
// Docker daemon that already refused.
func sharedMySQLDSN(t *testing.T) string {
	t.Helper()
	sharedMySQL.once.Do(func() {
		// The booting test's context only bounds the start and readiness
		// calls; the running container is not tied to it, so the server
		// outlives that test for the rest of the package.
		sharedMySQL.container, sharedMySQL.dsn, sharedMySQL.err = startSharedMySQL(t.Context())
	})
	require.NoError(t, sharedMySQL.err, "shared mysql server")
	return sharedMySQL.dsn
}

// startSharedMySQL boots the shared server and confirms it answers through the
// mapped port. The container is returned even when err is set: a failed start
// can still hand back a running container (for example on a wait-strategy
// timeout), and runWithSharedMySQL terminates whatever came back.
func startSharedMySQL(ctx context.Context) (*mysql.MySQLContainer, string, error) {
	container, err := mysql.Run(ctx,
		"mysql:8.0",
		mysql.WithDatabase("schemabot_test"),
		mysql.WithUsername("root"),
		mysql.WithPassword("test"),
		testutil.MySQLTmpfsDatadir(),
	)
	if err != nil {
		return container, "", fmt.Errorf("start shared mysql container: %w", err)
	}

	dsn, err := testutil.ContainerConnectionString(ctx, container, "parseTime=true")
	if err != nil {
		return container, "", fmt.Errorf("shared mysql connection string: %w", err)
	}
	if err := pingMySQL(ctx, dsn); err != nil {
		return container, "", fmt.Errorf("shared mysql readiness: %w", err)
	}
	return container, dsn, nil
}

// pingMySQL opens a throwaway pool on dsn and runs the bounded readiness ping.
func pingMySQL(ctx context.Context, dsn string) error {
	db, err := sql.Open("block-mysql", dsn)
	if err != nil {
		return fmt.Errorf("open shared mysql: %w", err)
	}
	defer utils.CloseAndLog(db)
	return testutil.PingMySQL(ctx, db)
}

// storageDatabase is a fresh database on the shared MySQL server, scoped to one
// test. Name is the database (schema) name for information_schema lookups; DSN
// selects it as the default database.
type storageDatabase struct {
	Name string
	DSN  string
}

// newStorageDatabase creates an empty database on the shared server for the
// calling test. The database is left in place when the test finishes: dropping
// it could block behind connections the test leaked, and the whole server is
// discarded with the container once the package finishes.
//
// The database isolates tables and nothing else. Three things remain
// server-wide and are shared with every other test in the package: advisory
// lock names (EnsureSchema serializes on one fixed name), the
// pendingdrops.Database quarantine schema (its table names carry no source
// database), and information_schema.PROCESSLIST. The package's MySQL-backed
// tests run sequentially because of this; do not add t.Parallel to one without
// first scoping whatever it touches on that list.
func newStorageDatabase(t *testing.T) storageDatabase {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), storageSetupTimeout)
	defer cancel()

	rootDSN := sharedMySQLDSN(t)
	name := fmt.Sprintf("api_test_%d", storageDatabaseSeq.Add(1))

	admin, err := sql.Open("block-mysql", rootDSN)
	require.NoError(t, err, "open shared mysql")
	defer utils.CloseAndLog(admin)
	_, err = admin.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE `%s`", name))
	require.NoError(t, err, "create database %s", name)

	cfg, err := mysqldriver.ParseDSN(rootDSN)
	require.NoError(t, err, "parse shared mysql DSN")
	cfg.DBName = name

	return storageDatabase{Name: name, DSN: cfg.FormatDSN()}
}

// newStorageDatabaseWithSchema is newStorageDatabase followed by EnsureSchema,
// so the database carries the storage tables the same way a freshly started
// server's would.
func newStorageDatabaseWithSchema(t *testing.T) storageDatabase {
	t.Helper()
	sdb := newStorageDatabase(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	require.NoError(t, EnsureSchema(sdb.DSN, logger), "ensure storage schema in %s", sdb.Name)
	return sdb
}

// openStorageDB opens a handle on dsn that the test owns — it is closed when
// the test finishes — and confirms it answers with the bounded readiness ping.
// Tests that hand the handle to a component which closes it itself (a Service
// built over mysqlstore.New that the test later closes) must open their own
// handle instead so the handle has a single owner.
func openStorageDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err, "open storage database")
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, testutil.PingMySQL(t.Context(), db), "ping storage database")
	return db
}
