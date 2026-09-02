//go:build integration

package api

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"

	"github.com/block/spirit/pkg/utils"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/block/schemabot/pkg/testutil"
)

// sharedMySQLDSN points at the single MySQL server that every integration test
// in this package shares. Booting a MySQL container costs seconds; a test that
// needs MySQL storage carves out its own database on the shared server with
// newStorageDatabase instead, which costs a CREATE DATABASE.
var sharedMySQLDSN string

// storageDatabaseSeq numbers the per-test databases so each test works in a
// database no other test has touched.
var storageDatabaseSeq atomic.Int64

func TestMain(m *testing.M) {
	os.Exit(runWithSharedMySQL(m))
}

// runWithSharedMySQL boots the shared MySQL server, runs the package tests
// against it, and returns the process exit code. The container is terminated
// on every exit path — including a failed readiness check — so an aborted run
// never leaves a server behind on the host.
func runWithSharedMySQL(m *testing.M) int {
	ctx := context.Background()

	container, err := mysql.Run(ctx,
		"mysql:8.0",
		mysql.WithDatabase("schemabot_test"),
		mysql.WithUsername("root"),
		mysql.WithPassword("test"),
		testutil.MySQLTmpfsDatadir(),
	)
	if err != nil {
		log.Printf("start shared mysql container: %v", err)
		return 1
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			log.Printf("terminate shared mysql container: %v", err)
		}
	}()

	sharedMySQLDSN, err = testutil.ContainerConnectionString(ctx, container, "parseTime=true")
	if err != nil {
		log.Printf("shared mysql connection string: %v", err)
		return 1
	}
	if err := pingSharedMySQL(ctx); err != nil {
		log.Printf("shared mysql readiness: %v", err)
		return 1
	}

	return m.Run()
}

// pingSharedMySQL confirms the shared server answers through the mapped port
// before any test opens a pool against it.
func pingSharedMySQL(ctx context.Context) error {
	db, err := sql.Open("mysql", sharedMySQLDSN)
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
func newStorageDatabase(t *testing.T) storageDatabase {
	t.Helper()
	ctx := t.Context()

	name := fmt.Sprintf("api_test_%d", storageDatabaseSeq.Add(1))

	admin, err := sql.Open("mysql", sharedMySQLDSN)
	require.NoError(t, err, "open shared mysql")
	defer utils.CloseAndLog(admin)
	_, err = admin.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE `%s`", name))
	require.NoError(t, err, "create database %s", name)

	cfg, err := mysqldriver.ParseDSN(sharedMySQLDSN)
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

// openStorageDB opens and pings a handle on dsn that the test owns: it is
// closed when the test finishes. Tests that hand the handle to a component
// which closes it itself (a Service built over mysqlstore.New) must open their
// own handle instead so the handle has a single owner.
func openStorageDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "open storage database")
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(t.Context()), "ping storage database")
	return db
}
