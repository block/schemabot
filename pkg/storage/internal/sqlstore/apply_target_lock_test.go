package sqlstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAcquireApplyTargetLockConn_NilLockerFailsClosed verifies an apply
// target without an advisory-lock implementation is rejected before any
// connection is opened: applies must not proceed unserialized across
// instances, and must not assume MySQL lock semantics for an engine the
// caller did not declare. The nil *sql.DB pins the ordering — if the guard
// ran after the connection was acquired, this test would panic instead of
// returning the fail-closed error.
func TestAcquireApplyTargetLockConn_NilLockerFailsClosed(t *testing.T) {
	conn, lockName, err := acquireApplyTargetLockConn(t.Context(), nil, nil, "testdb", "mysql", "staging")
	require.ErrorContains(t, err, "requires an advisory locker")
	require.Nil(t, conn)
	require.Empty(t, lockName)
}
