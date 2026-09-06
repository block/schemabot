//go:build integration

package namedlock

import (
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/testutil"
)

// openPool opens a PostgreSQL pool that hands out more than one connection,
// which is what a session affinity probe needs.
func openPool(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(t.Context()))
	return db
}

// A PostgreSQL advisory lock is only exclusive while a client connection keeps
// one server session. Behind a transaction-mode pooler it does not: the pooler
// hands the backend back at the end of the statement that took the lock, and a
// second client connection reaching the same backend takes the same lock while
// the first still believes it holds it. This is the mechanism that removes
// cross-instance exclusion from every flow built on these locks, so it is
// pinned directly rather than only through the guard that detects it.
func TestPostgresAdvisoryLockLosesExclusionBehindTransactionPooling(t *testing.T) {
	pooledDSN, _ := testutil.StartPostgresBehindPgBouncer(t, "testdb", testutil.PgBouncerTransactionPooling)
	db := openPool(t, pooledDSN)
	locker := Postgres{}
	name := "namedlock_test_pg_pooled_exclusion"

	holder, err := db.Conn(t.Context())
	require.NoError(t, err)
	defer func() { _ = holder.Close() }()
	acquired, err := locker.Acquire(t.Context(), holder, name, 0)
	require.NoError(t, err)
	require.True(t, acquired, "the first connection should take the lock")

	contender, err := db.Conn(t.Context())
	require.NoError(t, err)
	defer func() { _ = contender.Close() }()
	acquired, err = locker.Acquire(t.Context(), contender, name, 0)
	require.NoError(t, err)
	assert.True(t, acquired,
		"two pooled connections should both hold the same advisory lock, which is the lost exclusion this guard exists to refuse")
}

// The session affinity guard refuses a pool that cannot keep a connection on
// one server session, so a caller that needs the advisory lock for mutual
// exclusion fails closed instead of running without it.
func TestPostgresVerifySessionAffinityRefusesTransactionPooling(t *testing.T) {
	pooledDSN, _ := testutil.StartPostgresBehindPgBouncer(t, "testdb", testutil.PgBouncerTransactionPooling)

	err := Postgres{}.VerifySessionAffinity(t.Context(), openPool(t, pooledDSN))

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoSessionAffinity)
}

// A pooler that hands a client its own server session for the connection's
// lifetime preserves advisory locks, so the guard passes it: the refusal is
// keyed on the property SchemaBot depends on, not on the presence of a pooler.
func TestPostgresVerifySessionAffinityAcceptsSessionPooling(t *testing.T) {
	pooledDSN, directDSN := testutil.StartPostgresBehindPgBouncer(t, "testdb", testutil.PgBouncerSessionPooling)

	assert.NoError(t, Postgres{}.VerifySessionAffinity(t.Context(), openPool(t, pooledDSN)),
		"session pooling keeps one backend per client connection")
	assert.NoError(t, Postgres{}.VerifySessionAffinity(t.Context(), openPool(t, directDSN)),
		"a direct connection is the case the advisory lock contract is written for")
}
