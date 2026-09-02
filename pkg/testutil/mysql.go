package testutil

import (
	"context"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql" // database/sql driver for the readiness probe below
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// mysqlRootPassword is the root password every test MySQL container is started
// with, and that MySQLDSN builds connection strings against.
const mysqlRootPassword = "testpassword"

// mysqlPort is the container port MySQL serves on.
const mysqlPort = "3306"

// mysqlStartupTimeout bounds the wait for a test MySQL container to accept
// queries. A container that has not become usable by then is a failed setup
// rather than one worth waiting longer on.
const mysqlStartupTimeout = 30 * time.Second

// mysqlDatadirSize caps the in-memory data directory. It is a ceiling, not a
// reservation: the tmpfs occupies only what MySQL has written. Sized to leave
// room above what the heaviest package's tables, redo log and binary logs
// reach, so a test that writes without bound fails on its own data directory
// instead of pressuring the host.
const mysqlDatadirSize = "2g"

// MySQLContainerRequest returns the request for a throwaway MySQL container
// serving the named database, for the caller to start and terminate.
//
// The data directory is mounted on tmpfs. The entrypoint builds a fresh data
// directory before the real server binds TCP, and that work is dominated by
// fsyncs of the system tablespaces and the redo log. Several integration
// packages each start their own MySQL and share one CI runner's disk, so when
// the device is saturated initialization stretches far past the readiness
// budget and the package fails during setup, before any test runs. Nothing in
// the directory outlives the container, so holding it in memory costs nothing
// and takes both that initialization and the tests' own writes out of the
// contention.
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
		Tmpfs: map[string]string{"/var/lib/mysql": "rw,size=" + mysqlDatadirSize},
		WaitingFor: wait.ForSQL(mysqlPort+"/tcp", "mysql", func(host string, port network.Port) string {
			return fmt.Sprintf("root:%s@tcp(%s:%s)/%s", mysqlRootPassword, host, port.Port(), database)
		}).WithStartupTimeout(mysqlStartupTimeout),
	}
}

// MySQLDSN returns the DSN addressing the named database on a started MySQL
// test container, with params appended to the query string.
func MySQLDSN(ctx context.Context, c testcontainers.Container, database string, params ...string) (string, error) {
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
