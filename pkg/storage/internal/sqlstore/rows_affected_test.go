package sqlstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

var errRowCountUnavailable = errors.New("row count unavailable")

// unreadableResult is a sql.Result whose row count cannot be read, like a
// driver that does not report affected rows.
type unreadableResult struct{}

func (unreadableResult) LastInsertId() (int64, error) { return 0, nil }
func (unreadableResult) RowsAffected() (int64, error) { return 0, errRowCountUnavailable }

// unreadableDriver opens connections whose statements succeed but return an
// unreadableResult.
type unreadableDriver struct{}

func (unreadableDriver) Open(string) (driver.Conn, error) { return unreadableConn{}, nil }

type unreadableConn struct{}

func (unreadableConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare unsupported")
}
func (unreadableConn) Close() error              { return nil }
func (unreadableConn) Begin() (driver.Tx, error) { return nil, errors.New("begin unsupported") }
func (unreadableConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return unreadableResult{}, nil
}

func newUnreadableLockStore(t *testing.T) *lockStore {
	t.Helper()
	db := sql.OpenDB(unreadableConnector{})
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return &lockStore{db: newRebindDB(db, MySQLDialect{}), dialect: MySQLDialect{}}
}

type unreadableConnector struct{}

func (unreadableConnector) Connect(context.Context) (driver.Conn, error) {
	return unreadableConn{}, nil
}
func (unreadableConnector) Driver() driver.Driver { return unreadableDriver{} }

// A driver that cannot report the affected row count leaves the caller unable
// to tell a missing row from a deleted one, so the helper returns the driver's
// error instead of the not-found sentinel.
func TestCheckRowsAffected_UnreadableRowCountReturnsWrappedError(t *testing.T) {
	err := checkRowsAffected(unreadableResult{}, storage.ErrSettingNotFound)

	require.Error(t, err)
	require.ErrorIs(t, err, errRowCountUnavailable)
	assert.NotErrorIs(t, err, storage.ErrSettingNotFound)
	assert.Contains(t, err.Error(), "read rows affected")
}

func TestCheckRowsAffected_CountsRows(t *testing.T) {
	assert.ErrorIs(t, checkRowsAffected(driver.RowsAffected(0), storage.ErrSettingNotFound), storage.ErrSettingNotFound)
	assert.NoError(t, checkRowsAffected(driver.RowsAffected(1), storage.ErrSettingNotFound))
}

// Releasing a lock against a driver that cannot report the row count must fail
// with an error naming the lock, never report the lock as not held.
func TestLockStore_ReleaseUnreadableRowCountReturnsWrappedError(t *testing.T) {
	store := newUnreadableLockStore(t)

	err := store.Release(t.Context(), "testdb", "mysql", "org/repo#123")

	require.Error(t, err)
	require.ErrorIs(t, err, errRowCountUnavailable)
	assert.NotErrorIs(t, err, storage.ErrLockNotFound)
	assert.NotErrorIs(t, err, storage.ErrLockNotOwned)
	assert.Contains(t, err.Error(), "testdb/mysql")
	assert.Contains(t, err.Error(), "org/repo#123")
}

func TestLockStore_ForceReleaseUnreadableRowCountReturnsWrappedError(t *testing.T) {
	store := newUnreadableLockStore(t)

	err := store.ForceRelease(t.Context(), "testdb", "mysql")

	require.Error(t, err)
	require.ErrorIs(t, err, errRowCountUnavailable)
	assert.NotErrorIs(t, err, storage.ErrLockNotFound)
	assert.Contains(t, err.Error(), "testdb/mysql")
}

func TestLockStore_ReleaseIfPendingPlanIDUnreadableRowCountReturnsWrappedError(t *testing.T) {
	store := newUnreadableLockStore(t)

	released, err := store.ReleaseIfPendingPlanID(t.Context(), "testdb", "mysql", "org/repo#123", "plan-1")

	require.ErrorIs(t, err, errRowCountUnavailable)
	assert.False(t, released)
	assert.Contains(t, err.Error(), "testdb/mysql")
	assert.Contains(t, err.Error(), "plan-1")
}
