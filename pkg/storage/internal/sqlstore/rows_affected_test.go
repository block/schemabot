package sqlstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

var (
	errRowCountUnavailable = errors.New("row count unavailable")
	errStub                = errors.New("stub driver failure")
)

// unreadableResult is a sql.Result whose row count cannot be read, like a
// driver that does not report affected rows.
type unreadableResult struct{}

func (unreadableResult) LastInsertId() (int64, error) { return 0, nil }
func (unreadableResult) RowsAffected() (int64, error) { return 0, errRowCountUnavailable }

// stubConn is a SQL driver connection whose statements return what the test
// sets, so the store's error paths run without a database. A query returns no
// rows unless queryErr or rowsErr is set.
type stubConn struct {
	execResult driver.Result
	execErr    error
	queryErr   error
	rowsErr    error
}

func (stubConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare unsupported")
}
func (stubConn) Close() error              { return nil }
func (stubConn) Begin() (driver.Tx, error) { return nil, errors.New("begin unsupported") }

func (c stubConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	if c.execErr != nil {
		return nil, c.execErr
	}
	return c.execResult, nil
}

func (c stubConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	return stubRows{nextErr: c.rowsErr}, nil
}

// stubRows ends with nextErr, or with no rows when nextErr is nil.
type stubRows struct{ nextErr error }

func (stubRows) Columns() []string { return nil }
func (stubRows) Close() error      { return nil }
func (r stubRows) Next([]driver.Value) error {
	if r.nextErr != nil {
		return r.nextErr
	}
	return io.EOF
}

type stubConnector struct{ conn stubConn }

func (c stubConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (stubConnector) Driver() driver.Driver                          { return stubDriver{} }

type stubDriver struct{}

func (stubDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("open unsupported: use stubConnector")
}

func newStubLockStore(t *testing.T, conn stubConn) *lockStore {
	t.Helper()
	db := sql.OpenDB(stubConnector{conn: conn})
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return &lockStore{db: newRebindDB(db, MySQLDialect{}), dialect: MySQLDialect{}}
}

func newUnreadableLockStore(t *testing.T) *lockStore {
	t.Helper()
	return newStubLockStore(t, stubConn{execResult: unreadableResult{}})
}

// A driver that cannot report the affected row count leaves the caller unable
// to tell a missing row from a deleted one, so the helper returns the driver's
// error, naming the statement, instead of the not-found sentinel.
func TestCheckRowsAffected_UnreadableRowCountReturnsWrappedError(t *testing.T) {
	err := checkRowsAffected(unreadableResult{}, storage.ErrSettingNotFound, "deleting setting cursor")

	require.Error(t, err)
	require.ErrorIs(t, err, errRowCountUnavailable)
	assert.NotErrorIs(t, err, storage.ErrSettingNotFound)
	assert.Contains(t, err.Error(), "read rows affected deleting setting cursor")
}

func TestCheckRowsAffected_CountsRows(t *testing.T) {
	assert.ErrorIs(t, checkRowsAffected(driver.RowsAffected(0), storage.ErrSettingNotFound, "deleting setting cursor"), storage.ErrSettingNotFound)
	assert.NoError(t, checkRowsAffected(driver.RowsAffected(1), storage.ErrSettingNotFound, "deleting setting cursor"))
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

func TestLockStore_ForceReleaseByIDUnreadableRowCountReturnsWrappedError(t *testing.T) {
	store := newUnreadableLockStore(t)

	err := store.ForceReleaseByID(t.Context(), 7, "testdb", "mysql")

	require.Error(t, err)
	require.ErrorIs(t, err, errRowCountUnavailable)
	assert.NotErrorIs(t, err, storage.ErrLockNotFound)
	assert.NotErrorIs(t, err, storage.ErrLockReplaced)
	assert.Contains(t, err.Error(), "lock row 7 for testdb/mysql")
}

func TestLockStore_ReleaseIfPendingPlanIDUnreadableRowCountReturnsWrappedError(t *testing.T) {
	store := newUnreadableLockStore(t)

	released, err := store.ReleaseIfPendingPlanID(t.Context(), "testdb", "mysql", "org/repo#123", "plan-1")

	require.ErrorIs(t, err, errRowCountUnavailable)
	assert.False(t, released)
	assert.Contains(t, err.Error(), "testdb/mysql")
	assert.Contains(t, err.Error(), "plan-1")
}

// A failed DELETE keeps the driver's error and names the lock it was releasing.
func TestLockStore_ReleaseDeleteErrorNamesTheLock(t *testing.T) {
	store := newStubLockStore(t, stubConn{execErr: errStub})

	releaseErr := store.Release(t.Context(), "testdb", "mysql", "org/repo#123")
	forceErr := store.ForceRelease(t.Context(), "testdb", "mysql")
	_, pendingErr := store.ReleaseIfPendingPlanID(t.Context(), "testdb", "mysql", "org/repo#123", "plan-1")

	for name, err := range map[string]error{"Release": releaseErr, "ForceRelease": forceErr, "ReleaseIfPendingPlanID": pendingErr} {
		require.ErrorIs(t, err, errStub, name)
		assert.Contains(t, err.Error(), "testdb/mysql", name)
	}
	assert.Contains(t, releaseErr.Error(), "org/repo#123")
	assert.Contains(t, pendingErr.Error(), "plan-1")
}

// When a release deletes nothing and the re-read of the lock fails, the
// caller cannot tell whether the lock exists, so the error is not
// ErrLockNotFound.
func TestLockStore_ReleaseReReadErrorIsNotLockNotFound(t *testing.T) {
	store := newStubLockStore(t, stubConn{execResult: driver.RowsAffected(0), queryErr: errStub})

	err := store.Release(t.Context(), "testdb", "mysql", "org/repo#123")

	require.ErrorIs(t, err, errStub)
	assert.NotErrorIs(t, err, storage.ErrLockNotFound)
	assert.NotErrorIs(t, err, storage.ErrLockNotOwned)
	assert.Contains(t, err.Error(), "testdb/mysql")
}

// A pinned force release that deletes nothing and cannot re-read the lock
// cannot tell a missing lock from a replaced one, so it reports neither.
func TestLockStore_ForceReleaseByIDReReadErrorIsNeitherNotFoundNorReplaced(t *testing.T) {
	store := newStubLockStore(t, stubConn{execResult: driver.RowsAffected(0), queryErr: errStub})

	err := store.ForceReleaseByID(t.Context(), 7, "testdb", "mysql")

	require.ErrorIs(t, err, errStub)
	assert.NotErrorIs(t, err, storage.ErrLockNotFound)
	assert.NotErrorIs(t, err, storage.ErrLockReplaced)
	assert.Contains(t, err.Error(), "row 7")
	assert.Contains(t, err.Error(), "testdb/mysql")
}

// A lock that does not exist reads as nil with no error, so acquiring a free
// key works and releasing an absent lock reports ErrLockNotFound.
func TestLockStore_MissingLockReadsAsNil(t *testing.T) {
	store := newStubLockStore(t, stubConn{execResult: driver.RowsAffected(0)})

	lock, err := store.Get(t.Context(), "testdb", "mysql")
	require.NoError(t, err)
	assert.Nil(t, lock)

	err = store.Release(t.Context(), "testdb", "mysql", "org/repo#123")
	assert.ErrorIs(t, err, storage.ErrLockNotFound)
}

func TestLockStore_GetErrorNamesTheLock(t *testing.T) {
	store := newStubLockStore(t, stubConn{queryErr: errStub})

	lock, err := store.Get(t.Context(), "testdb", "mysql")

	require.ErrorIs(t, err, errStub)
	assert.Nil(t, lock)
	assert.Contains(t, err.Error(), "testdb/mysql")
}

// List returns no locks at all when the query or the row iteration fails, so
// a caller never acts on a partial list.
func TestLockStore_ListErrorReturnsNoLocks(t *testing.T) {
	t.Run("query", func(t *testing.T) {
		locks, err := newStubLockStore(t, stubConn{queryErr: errStub}).List(t.Context())
		require.ErrorIs(t, err, errStub)
		assert.Nil(t, locks)
		assert.Contains(t, err.Error(), "query all locks")
	})
	t.Run("iterate", func(t *testing.T) {
		locks, err := newStubLockStore(t, stubConn{rowsErr: errStub}).List(t.Context())
		require.ErrorIs(t, err, errStub)
		assert.Nil(t, locks)
		assert.Contains(t, err.Error(), "iterate lock rows")
	})
}
