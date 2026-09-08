package sqlstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingConnector is a database/sql driver stub that records every SQL
// statement text the pool hands to the driver, so tests can assert exactly
// what reached the wire after rebinding.
type recordingConnector struct {
	queries []string
	args    [][]driver.Value
	txOpts  []driver.TxOptions
	closes  int
}

func (c *recordingConnector) Connect(context.Context) (driver.Conn, error) { return c, nil }
func (c *recordingConnector) Driver() driver.Driver                        { return nil }

func (c *recordingConnector) Prepare(query string) (driver.Stmt, error) {
	c.queries = append(c.queries, query)
	return &recordingStmt{connector: c}, nil
}
func (c *recordingConnector) Close() error {
	c.closes++
	return nil
}
func (c *recordingConnector) Begin() (driver.Tx, error) { return noopTx{}, nil }

// BeginTx records the transaction options the pool hands to the driver, so
// tests can exercise the isolation-level-bearing BeginTx paths and assert the
// options survive the wrapper types intact.
func (c *recordingConnector) BeginTx(_ context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.txOpts = append(c.txOpts, opts)
	return noopTx{}, nil
}

type recordingStmt struct {
	connector *recordingConnector
}

func (s *recordingStmt) Close() error  { return nil }
func (s *recordingStmt) NumInput() int { return -1 }

func (s *recordingStmt) Exec(args []driver.Value) (driver.Result, error) {
	s.connector.args = append(s.connector.args, args)
	return driver.RowsAffected(0), nil
}

func (s *recordingStmt) Query(args []driver.Value) (driver.Rows, error) {
	s.connector.args = append(s.connector.args, args)
	return emptyRows{}, nil
}

type emptyRows struct{}

func (emptyRows) Columns() []string              { return nil }
func (emptyRows) Close() error                   { return nil }
func (emptyRows) Next(dest []driver.Value) error { return io.EOF }

type noopTx struct{}

func (noopTx) Commit() error   { return nil }
func (noopTx) Rollback() error { return nil }

// rebindMarker is appended by countingBinder so tests can assert, on the SQL
// the driver actually received, that a statement was rebound exactly once.
const rebindMarker = " /* rebound */"

// countingBinder rewrites the query to a recognizable form and counts how many
// times it was invoked, so tests can prove each execution rebinds exactly once
// and forwards the rebound SQL downstream. It also records whether it was ever
// handed a query that was already rebound, which would mean a double rebind.
type countingBinder struct {
	calls         int
	doubleRebound bool
}

func (b *countingBinder) Rebind(query string) string {
	b.calls++
	if strings.Contains(query, rebindMarker) {
		b.doubleRebound = true
	}
	return query + rebindMarker
}

// assertReboundExactlyOnce verifies every statement the driver received was
// rebound exactly once: the binder never saw already-rebound SQL, and each
// recorded query carries exactly one marker.
func assertReboundExactlyOnce(t *testing.T, b *countingBinder, connector *recordingConnector) {
	t.Helper()
	assert.False(t, b.doubleRebound, "binder received already-rebound SQL")
	for _, q := range connector.queries {
		assert.Equal(t, 1, strings.Count(q, rebindMarker), "query not rebound exactly once: %s", q)
	}
}

// newRecordingRebindDB returns a rebindDB whose pool is backed by the
// recording driver stub, so tests observe the exact SQL reaching the driver.
func newRecordingRebindDB(t *testing.T, b binder) (*rebindDB, *recordingConnector) {
	connector := &recordingConnector{}
	pool := sql.OpenDB(connector)
	t.Cleanup(func() {
		require.NoError(t, pool.Close())
	})
	return newRebindDB(pool, b), connector
}

func TestRebindDBExecContextRebindsOnce(t *testing.T) {
	b := &countingBinder{}
	rdb, connector := newRecordingRebindDB(t, b)

	_, err := rdb.ExecContext(t.Context(), "UPDATE applies SET state = ? WHERE id = ?", "running", int64(7))
	require.NoError(t, err)

	assert.Equal(t, 1, b.calls)
	assert.Equal(t, []string{"UPDATE applies SET state = ? WHERE id = ? /* rebound */"}, connector.queries)
	assert.Equal(t, [][]driver.Value{{"running", int64(7)}}, connector.args)
}

func TestRebindDBQueryContextRebindsOnce(t *testing.T) {
	b := &countingBinder{}
	rdb, connector := newRecordingRebindDB(t, b)

	rows, err := rdb.QueryContext(t.Context(), "SELECT id FROM applies WHERE state = ?", "running")
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	assert.Equal(t, 1, b.calls)
	assert.Equal(t, []string{"SELECT id FROM applies WHERE state = ? /* rebound */"}, connector.queries)
	assert.Equal(t, [][]driver.Value{{"running"}}, connector.args)
}

func TestRebindDBQueryRowContextRebindsOnce(t *testing.T) {
	b := &countingBinder{}
	rdb, connector := newRecordingRebindDB(t, b)

	var id int64
	err := rdb.QueryRowContext(t.Context(), "SELECT id FROM applies WHERE id = ?", int64(7)).Scan(&id)
	require.ErrorIs(t, err, sql.ErrNoRows)

	assert.Equal(t, 1, b.calls)
	assert.Equal(t, []string{"SELECT id FROM applies WHERE id = ? /* rebound */"}, connector.queries)
	assert.Equal(t, [][]driver.Value{{int64(7)}}, connector.args)
}

// Statements executed inside a pool transaction pass through the binder
// exactly once, the same as direct pool execution.
func TestRebindTxRebindsOnce(t *testing.T) {
	b := &countingBinder{}
	rdb, connector := newRecordingRebindDB(t, b)
	ctx := t.Context()

	tx, err := rdb.BeginTx(ctx, nil)
	require.NoError(t, err)

	_, err = tx.ExecContext(ctx, "UPDATE applies SET state = ? WHERE id = ?", "stopped", int64(7))
	require.NoError(t, err)

	rows, err := tx.QueryContext(ctx, "SELECT id FROM tasks WHERE apply_id = ?", int64(7))
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	var id int64
	err = tx.QueryRowContext(ctx, "SELECT id FROM applies WHERE id = ?", int64(7)).Scan(&id)
	require.ErrorIs(t, err, sql.ErrNoRows)

	require.NoError(t, tx.Rollback())

	assert.Equal(t, 3, b.calls)
	assert.Equal(t, []string{
		"UPDATE applies SET state = ? WHERE id = ?" + rebindMarker,
		"SELECT id FROM tasks WHERE apply_id = ?" + rebindMarker,
		"SELECT id FROM applies WHERE id = ?" + rebindMarker,
	}, connector.queries)
	assertReboundExactlyOnce(t, b, connector)
}

// A transaction begun on a pinned connection rebinds its statements the same
// way as one begun on the pool, so the advisory-lock apply-write path cannot
// leak native placeholders past the binder.
func TestRebindConnTxRebindsOnce(t *testing.T) {
	b := &countingBinder{}
	rdb, connector := newRecordingRebindDB(t, b)
	ctx := t.Context()

	conn, err := rdb.Conn(ctx)
	require.NoError(t, err)

	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "UPDATE applies SET state = ? WHERE id = ?", "running", int64(7))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.NoError(t, conn.Close())

	assert.Equal(t, 1, b.calls)
	assert.Equal(t, []string{"UPDATE applies SET state = ? WHERE id = ?" + rebindMarker}, connector.queries)
	assertReboundExactlyOnce(t, b, connector)
}

// Transactions begun with an explicit isolation level — as the apply-write
// (RepeatableRead) and read-only (ReadCommitted) paths do — carry the level
// through the wrapper types to the driver and still rebind exactly once,
// whether begun on the pool or on a pinned connection.
func TestRebindTxIsolationLevelReachesDriver(t *testing.T) {
	b := &countingBinder{}
	rdb, connector := newRecordingRebindDB(t, b)
	ctx := t.Context()

	tx, err := rdb.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "UPDATE applies SET state = ? WHERE id = ?", "running", int64(7))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	conn, err := rdb.Conn(ctx)
	require.NoError(t, err)
	connTx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	require.NoError(t, err)
	var id int64
	err = connTx.QueryRowContext(ctx, "SELECT id FROM applies WHERE id = ?", int64(7)).Scan(&id)
	require.ErrorIs(t, err, sql.ErrNoRows)
	require.NoError(t, connTx.Rollback())
	require.NoError(t, conn.Close())

	require.Len(t, connector.txOpts, 2)
	assert.Equal(t, driver.IsolationLevel(sql.LevelRepeatableRead), connector.txOpts[0].Isolation)
	assert.Equal(t, driver.IsolationLevel(sql.LevelReadCommitted), connector.txOpts[1].Isolation)
	assertReboundExactlyOnce(t, b, connector)
}

// The lockerConn() escape hands the advisory locker the pinned session
// without the binder in the way: locker SQL reaches the driver with its
// engine-native placeholders untouched, and the binder is never invoked.
func TestRebindConnLockerConnBypassesBinder(t *testing.T) {
	b := &countingBinder{}
	rdb, connector := newRecordingRebindDB(t, b)
	ctx := t.Context()

	conn, err := rdb.Conn(ctx)
	require.NoError(t, err)

	var result sql.NullInt64
	err = conn.lockerConn().QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", "lock", int64(0)).Scan(&result)
	require.ErrorIs(t, err, sql.ErrNoRows)
	require.NoError(t, conn.Close())

	assert.Equal(t, 0, b.calls)
	assert.Equal(t, []string{"SELECT GET_LOCK(?, ?)"}, connector.queries)
}

// discardBadConn retires the pinned session on the spot without surfacing the
// sentinel it injects: the pool destroys the driver connection immediately,
// and a later Close reports the session as already retired.
func TestRebindConnDiscardBadConnRetiresSession(t *testing.T) {
	b := &countingBinder{}
	rdb, connector := newRecordingRebindDB(t, b)
	ctx := t.Context()

	conn, err := rdb.Conn(ctx)
	require.NoError(t, err)

	require.NoError(t, conn.discardBadConn())
	require.ErrorIs(t, conn.Close(), sql.ErrConnDone, "the discarded session is already retired")

	assert.Equal(t, 1, connector.closes, "the pool should destroy the discarded session")
	assert.Equal(t, 0, b.calls, "discarding a session must not touch the binder")
	assert.Empty(t, connector.queries, "discarding a session must not execute SQL")
}

func TestMySQLDialectRebindIsIdentity(t *testing.T) {
	query := "SELECT id FROM applies WHERE state = ? AND database_name = ?"
	assert.Equal(t, query, MySQLDialect{}.Rebind(query))
}
