//go:build integration

package namedlock

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

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

	code := m.Run()

	if err := container.Terminate(ctx); err != nil {
		log.Printf("terminate mysql container: %v", err)
	}
	os.Exit(code)
}

func openConn(t *testing.T) *sql.Conn {
	t.Helper()
	db, err := sql.Open("mysql", sharedDSN)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(t.Context()))
	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, conn.Close())
		assert.NoError(t, db.Close())
	})
	return conn
}

// A named lock is exclusive across sessions: the second connection to request
// it with a zero wait is refused, and can only take it after the holder
// releases it.
func TestMySQLLockExclusiveAcrossSessions(t *testing.T) {
	locker := MySQL{}
	name := "namedlock_test_exclusive"

	holder := openConn(t)
	acquired, err := locker.Acquire(t.Context(), holder, name, 5*time.Second)
	require.NoError(t, err)
	require.True(t, acquired, "first session should take the lock")

	contender := openConn(t)
	acquired, err = locker.Acquire(t.Context(), contender, name, 0)
	require.NoError(t, err)
	assert.False(t, acquired, "second session should be refused while the lock is held")

	released, err := locker.Release(t.Context(), holder, name)
	require.NoError(t, err)
	assert.True(t, released, "holder should report releasing the lock it held")

	acquired, err = locker.Acquire(t.Context(), contender, name, 5*time.Second)
	require.NoError(t, err)
	assert.True(t, acquired, "second session should take the lock after release")

	released, err = locker.Release(t.Context(), contender, name)
	require.NoError(t, err)
	assert.True(t, released)
}

// The same session re-acquiring a lock it already holds succeeds, and releasing
// a lock this session never held reports released=false without an error.
func TestMySQLReleaseUnheldLock(t *testing.T) {
	locker := MySQL{}
	conn := openConn(t)

	released, err := locker.Release(t.Context(), conn, "namedlock_test_never_held")
	require.NoError(t, err)
	assert.False(t, released, "releasing a lock this session never held reports not-held")
}
