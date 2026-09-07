package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"strings"
	"time"

	_ "github.com/block/mysql" // database/sql driver for the readiness probe below
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// driverName is the database/sql driver the readiness probe below opens with.
//
// It has to name whatever the blank import above registers, and block/mysql
// registers "block-mysql" rather than "mysql" so a binary still reaching
// upstream go-sql-driver can link both. Nothing here registers "mysql" any
// more, so the old literal failed the wait strategy with `unknown driver
// "mysql"` before a single test ran — and it failed inside a container start
// hook, which surfaces as the whole package failing rather than as a bad
// driver name. Named, so the import and the string that depends on it cannot
// drift apart again.
const driverName = "block-mysql"

// mysqlRootPassword is the root password every test MySQL container is started
// with, and that MySQLDSN builds connection strings against.
const mysqlRootPassword = "testpassword"

// mysqlPort is the container port MySQL serves on.
const mysqlPort = "3306"

// mysqlStartupTimeout bounds the wait for a test MySQL container to accept
// queries. A container that has not become usable by then is a failed setup
// rather than one worth waiting longer on.
//
// The budget assumes the in-memory data directory below. Building a fresh one
// on a contended disk does not reliably finish inside it, so a change that
// puts the directory back on the block device has to revisit this number
// too -- otherwise setup starts failing again, and does it before any test
// runs, which reports the whole package as failed.
const mysqlStartupTimeout = 30 * time.Second

// The two budgets below bound the setup steps this file performs once a
// container has started. Both exist because their callers run in TestMain,
// which has no *testing.T to hang a deadline on and so passes a background
// context: whatever stops answering would otherwise wedge package setup with
// nothing to read, and reaching the budget fails the setup instead. They are
// separate numbers because they wait on different things, so one can be
// tightened without dragging the other.

// mysqlLookupTimeout bounds the Docker API lookups MySQLDSN needs, which
// answer from the daemon's own state and involve no MySQL.
const mysqlLookupTimeout = 30 * time.Second

// mysqlPingTimeout bounds the readiness ping PingMySQL runs, which dials the
// server and completes a handshake.
const mysqlPingTimeout = 30 * time.Second

// mysqlDatadirSize caps the in-memory data directory. It is a ceiling, not a
// reservation: the tmpfs occupies only what MySQL has written. Sized to leave
// room above what the heaviest package's tables, redo log and binary logs
// reach, so a test that writes without bound fails on its own data directory
// instead of pressuring the host.
//
// The cap is per container, and a tmpfs with no container memory limit is
// charged onward to the host, so what a CI runner must hold is this times the
// number of package binaries live at once -- which Go's package parallelism
// governs, not anything here. Raising that parallelism, or adding another
// MySQL-starting package to a shard already running several, is a reason to
// revisit this rather than inherit it.
const mysqlDatadirSize = "2g"

// mysqlDatadir is where the MySQL images keep their data directory.
const mysqlDatadir = "/var/lib/mysql"

// mysqlDatadirTmpfs is the mount that holds the data directory in memory. The
// entrypoint builds a fresh one before the final server binds TCP, and that
// work is dominated by fsyncs of the system tablespaces and the redo log.
// Integration packages that each start their own MySQL share one CI runner's
// disk, so on a saturated device initialization stretches past the readiness
// budget and the package fails during setup, before any test runs. Nothing in
// the directory outlives the container, so holding it in memory costs nothing
// and takes both that initialization and the tests' own writes off the device.
func mysqlDatadirTmpfs() map[string]string {
	return map[string]string{mysqlDatadir: "rw,size=" + mysqlDatadirSize}
}

// MySQLTmpfsDatadir holds a MySQL container's data directory in memory, for
// containers built through the testcontainers MySQL module rather than through
// MySQLContainerRequest. The module's own readiness gate is already accurate --
// it waits on the line carrying the real port, which the throwaway init server
// does not emit -- so this adds the mount and leaves the gate alone.
func MySQLTmpfsDatadir() testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		if req.Tmpfs == nil {
			req.Tmpfs = map[string]string{}
		}
		maps.Copy(req.Tmpfs, mysqlDatadirTmpfs())
		return nil
	}
}

// MySQLContainerRequest returns the request for a throwaway MySQL container
// serving the named database, for the caller to start and terminate. The data
// directory is held in memory, as described on mysqlDatadirTmpfs.
//
// Readiness is gated on a real query through the mapped port, the same
// handshake the tests themselves perform. The MySQL entrypoint runs a
// throwaway init server that also logs "ready for connections" and binds
// nothing on TCP, so log- and port-based waits can be satisfied before the
// final server accepts clients.
func MySQLContainerRequest(image, database string) testcontainers.ContainerRequest {
	return testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{mysqlPort + "/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": mysqlRootPassword,
			"MYSQL_DATABASE":      database,
		},
		Tmpfs: mysqlDatadirTmpfs(),
		WaitingFor: wait.ForSQL(mysqlPort+"/tcp", driverName, func(host string, port network.Port) string {
			return fmt.Sprintf("root:%s@tcp(%s:%s)/%s", mysqlRootPassword, host, port.Port(), database)
		}).WithStartupTimeout(mysqlStartupTimeout),
	}
}

// boundSetup caps a setup step at budget. A caller that brings its own
// deadline keeps it, so a test's context still governs the step.
func boundSetup(ctx context.Context, budget time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, budget)
}

// PingMySQL verifies that a pool opened against a started test container
// reaches the server. Go's SQL driver dials lazily, so this is the first call
// that exercises the DSN. The wait is bounded at mysqlPingTimeout.
func PingMySQL(ctx context.Context, db *sql.DB) error {
	ctx, cancel := boundSetup(ctx, mysqlPingTimeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping mysql: %w", err)
	}
	return nil
}

// MySQLDSN returns the DSN addressing the named database on a started MySQL
// test container, with params appended to the query string. The lookups it
// needs are bounded at mysqlLookupTimeout.
func MySQLDSN(ctx context.Context, c testcontainers.Container, database string, params ...string) (string, error) {
	ctx, cancel := boundSetup(ctx, mysqlLookupTimeout)
	defer cancel()

	host, err := ContainerHost(ctx, c)
	if err != nil {
		return "", fmt.Errorf("get mysql container host: %w", err)
	}
	port, err := ContainerPort(ctx, c, mysqlPort)
	if err != nil {
		return "", fmt.Errorf("get mysql container port %s: %w", mysqlPort, err)
	}

	dsn := fmt.Sprintf("root:%s@tcp(%s:%d)/%s", mysqlRootPassword, host, port, database)
	if len(params) > 0 {
		dsn += "?" + strings.Join(params, "&")
	}
	return dsn, nil
}
