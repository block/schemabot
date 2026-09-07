//go:build integration

package namedlock

import (
	"context"
	"database/sql"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/block/schemabot/pkg/testutil"

	_ "github.com/block/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	mysqlDSN    string
	postgresDSN string
)

func TestMain(m *testing.M) {
	os.Exit(run(context.Background(), m))
}

// run starts one MySQL and one PostgreSQL container shared by every locker
// integration test in the package, returning the exit code for TestMain.
// Each termination is deferred before its start error is checked: a failed
// start can still hand back a running container (for example on a
// wait-strategy timeout), and the nil-safe TerminateContainer makes the
// deferred call a no-op when no container exists at all.
func run(ctx context.Context, m *testing.M) int {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testutil.MySQLContainerRequest("mysql:8.4", "testdb"),
		Started:          true,
	})
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			log.Printf("terminate mysql container: %v", err)
		}
	}()
	if err != nil {
		log.Printf("start mysql container: %v", err)
		return 1
	}

	mysqlDSN, err = testutil.MySQLDSN(ctx, container, "testdb", "parseTime=true")
	if err != nil {
		log.Printf("build mysql dsn: %v", err)
		return 1
	}

	pgContainer, err := postgres.Run(ctx,
		"postgres:16",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("test"),
		postgres.WithPassword("testpassword"),
		postgres.BasicWaitStrategies(),
	)
	defer func() {
		if err := testcontainers.TerminateContainer(pgContainer); err != nil {
			log.Printf("terminate postgres container: %v", err)
		}
	}()
	if err != nil {
		log.Printf("start postgres container: %v", err)
		return 1
	}

	postgresDSN, err = testutil.ContainerConnectionString(ctx, pgContainer, "sslmode=disable")
	if err != nil {
		log.Printf("get postgres connection string: %v", err)
		return 1
	}

	return m.Run()
}

// lockerCase binds a Locker implementation to the driver and DSN of its
// engine's shared container, so the same scenarios run identically against
// both engines.
type lockerCase struct {
	name   string
	locker Locker
	driver string
	dsn    string
}

// lockerCases is called from inside each test, after TestMain has populated
// the DSNs.
func lockerCases() []lockerCase {
	return []lockerCase{
		{name: "mysql", locker: MySQL{}, driver: "mysql", dsn: mysqlDSN},
		{name: "postgres", locker: Postgres{}, driver: "pgx", dsn: postgresDSN},
	}
}

// openLockConn opens a dedicated *sql.DB with one pinned connection and
// closes both when the test ends.
func openLockConn(t *testing.T, driverName, dsn string) *sql.Conn {
	t.Helper()
	conn, terminate := openTerminableLockConn(t, driverName, dsn)
	t.Cleanup(func() { terminate(t) })
	return conn
}

// openTerminableLockConn opens a dedicated *sql.DB with one pinned connection
// and returns the connection plus a terminate func that ends the database
// session by closing the connection and its pool. Tests that exercise
// session-end behavior call terminate mid-test; the registered cleanup only
// guards an early exit before terminate runs, so as the redundant closer it
// discards the already-closed errors that follow a normal terminate.
func openTerminableLockConn(t *testing.T, driverName, dsn string) (*sql.Conn, func(t *testing.T)) {
	t.Helper()
	db, err := sql.Open(driverName, dsn)
	require.NoError(t, err)
	require.NoError(t, testutil.PingMySQL(t.Context(), db))
	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
		_ = db.Close()
	})
	terminate := func(t *testing.T) {
		t.Helper()
		assert.NoError(t, conn.Close())
		assert.NoError(t, db.Close())
	}
	return conn, terminate
}

// A named lock is exclusive across sessions: the second connection to request
// it with a zero wait is refused, and can only take it after the holder
// releases it.
func TestLockExclusiveAcrossSessions(t *testing.T) {
	for _, tc := range lockerCases() {
		t.Run(tc.name, func(t *testing.T) {
			name := "namedlock_test_exclusive"

			holder := openLockConn(t, tc.driver, tc.dsn)
			acquired, err := tc.locker.Acquire(t.Context(), holder, name, 5*time.Second)
			require.NoError(t, err)
			require.True(t, acquired, "first session should take the lock")

			contender := openLockConn(t, tc.driver, tc.dsn)
			acquired, err = tc.locker.Acquire(t.Context(), contender, name, 0)
			require.NoError(t, err)
			assert.False(t, acquired, "second session should be refused while the lock is held")

			released, err := tc.locker.Release(t.Context(), holder, name)
			require.NoError(t, err)
			assert.True(t, released, "holder should report releasing the lock it held")

			acquired, err = tc.locker.Acquire(t.Context(), contender, name, 5*time.Second)
			require.NoError(t, err)
			assert.True(t, acquired, "second session should take the lock after release")

			released, err = tc.locker.Release(t.Context(), contender, name)
			require.NoError(t, err)
			assert.True(t, released)
		})
	}
}

// Releasing a lock this session never held reports released=false without an
// error, so a best-effort release of an unheld lock is a safe no-op.
func TestReleaseUnheldLock(t *testing.T) {
	for _, tc := range lockerCases() {
		t.Run(tc.name, func(t *testing.T) {
			conn := openLockConn(t, tc.driver, tc.dsn)

			released, err := tc.locker.Release(t.Context(), conn, "namedlock_test_never_held")
			require.NoError(t, err)
			assert.False(t, released, "releasing a lock this session never held reports not-held")
		})
	}
}

// A bounded wait that elapses while another session holds the lock reports
// acquired=false without an error, and the contender's connection stays
// usable: a later acquire of an uncontended lock on the same connection
// succeeds.
func TestBoundedWaitElapsesWhileHeld(t *testing.T) {
	for _, tc := range lockerCases() {
		t.Run(tc.name, func(t *testing.T) {
			name := "namedlock_test_bounded_wait"

			holder := openLockConn(t, tc.driver, tc.dsn)
			acquired, err := tc.locker.Acquire(t.Context(), holder, name, 5*time.Second)
			require.NoError(t, err)
			require.True(t, acquired, "first session should take the lock")

			contender := openLockConn(t, tc.driver, tc.dsn)
			acquired, err = tc.locker.Acquire(t.Context(), contender, name, time.Second)
			require.NoError(t, err, "an elapsed wait is not an error")
			assert.False(t, acquired, "second session should time out while the lock is held")

			acquired, err = tc.locker.Acquire(t.Context(), contender, "namedlock_test_bounded_wait_other", time.Second)
			require.NoError(t, err)
			assert.True(t, acquired, "the connection should stay usable after a timed-out acquire")
		})
	}
}

// Distinct lock names do not contend: a session holding one lock does not
// block another session taking a different one.
func TestDistinctNamesDoNotContend(t *testing.T) {
	for _, tc := range lockerCases() {
		t.Run(tc.name, func(t *testing.T) {
			first := openLockConn(t, tc.driver, tc.dsn)
			acquired, err := tc.locker.Acquire(t.Context(), first, "namedlock_test_name_a", 0)
			require.NoError(t, err)
			require.True(t, acquired)

			second := openLockConn(t, tc.driver, tc.dsn)
			acquired, err = tc.locker.Acquire(t.Context(), second, "namedlock_test_name_b", 0)
			require.NoError(t, err)
			assert.True(t, acquired, "a different lock name should not contend with the held lock")
		})
	}
}

// A lock is auto-released when its session ends without an explicit Release:
// after the holder's connection and pool close, a contender can take the
// lock. This is the invariant crash-safety relies on — a pod that dies
// holding a lock must not block every other pod forever.
func TestLockAutoReleasedWhenSessionEnds(t *testing.T) {
	for _, tc := range lockerCases() {
		t.Run(tc.name, func(t *testing.T) {
			name := "namedlock_test_auto_release"

			holder, terminate := openTerminableLockConn(t, tc.driver, tc.dsn)
			acquired, err := tc.locker.Acquire(t.Context(), holder, name, 5*time.Second)
			require.NoError(t, err)
			require.True(t, acquired, "first session should take the lock")

			contender := openLockConn(t, tc.driver, tc.dsn)
			acquired, err = tc.locker.Acquire(t.Context(), contender, name, 0)
			require.NoError(t, err)
			require.False(t, acquired, "the lock should be held while the first session lives")

			terminate(t)

			acquired, err = tc.locker.Acquire(t.Context(), contender, name, 5*time.Second)
			require.NoError(t, err)
			assert.True(t, acquired, "ending the holder's session should release the lock")
		})
	}
}
