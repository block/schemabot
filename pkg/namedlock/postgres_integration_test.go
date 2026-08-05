//go:build integration

package namedlock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The lock_timeout that bounds a PostgreSQL acquire wait is scoped to the
// acquire attempt and must not leak past it: after a timed-out attempt the
// connection reverts to its default lock_timeout, so later statements on the
// same connection wait normally instead of inheriting the short bound.
func TestPostgresLockTimeoutDoesNotLeakPastAcquire(t *testing.T) {
	locker := Postgres{}
	name := "namedlock_test_pg_timeout_leak"

	holder := openLockConn(t, "pgx", postgresDSN)
	acquired, err := locker.Acquire(t.Context(), holder, name, 5*time.Second)
	require.NoError(t, err)
	require.True(t, acquired, "first session should take the lock")

	contender := openLockConn(t, "pgx", postgresDSN)
	acquired, err = locker.Acquire(t.Context(), contender, name, 200*time.Millisecond)
	require.NoError(t, err, "an elapsed wait is not an error")
	require.False(t, acquired, "second session should time out while the lock is held")

	var lockTimeout string
	require.NoError(t, contender.QueryRowContext(t.Context(), "SHOW lock_timeout").Scan(&lockTimeout))
	assert.Equal(t, "0", lockTimeout, "lock_timeout should revert to its default after the acquire attempt")
}
