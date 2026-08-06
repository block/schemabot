package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeResult is a sql.Result whose LastInsertId / RowsAffected values and errors
// are set by the test.
type fakeResult struct {
	lastInsertID    int64
	lastInsertIDErr error
	rowsAffected    int64
	rowsAffectedErr error
}

func (r fakeResult) LastInsertId() (int64, error) { return r.lastInsertID, r.lastInsertIDErr }
func (r fakeResult) RowsAffected() (int64, error) { return r.rowsAffected, r.rowsAffectedErr }

// fakeExecer is a queryExecer that records the last ExecContext call and returns
// a scripted result/error. QueryRowContext is unused by MySQLDialect and panics
// if reached.
type fakeExecer struct {
	result    sql.Result
	err       error
	gotQuery  string
	gotArgs   []any
	execCalls int
}

func (e *fakeExecer) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	e.execCalls++
	e.gotQuery = query
	e.gotArgs = args
	return e.result, e.err
}

func (e *fakeExecer) QueryRowContext(_ context.Context, _ string, _ ...any) *sql.Row {
	panic("QueryRowContext must not be called by MySQLDialect identity methods")
}

func TestMySQLDialectInsertID(t *testing.T) {
	d := MySQLDialect{}

	t.Run("returns generated id", func(t *testing.T) {
		exec := &fakeExecer{result: fakeResult{lastInsertID: 42}}
		id, err := d.InsertID(t.Context(), exec, "INSERT INTO plans (a) VALUES (?)", "x")
		require.NoError(t, err)
		assert.Equal(t, int64(42), id)
		assert.Equal(t, 1, exec.execCalls)
		assert.Equal(t, "INSERT INTO plans (a) VALUES (?)", exec.gotQuery)
		assert.Equal(t, []any{"x"}, exec.gotArgs)
	})

	t.Run("propagates exec error", func(t *testing.T) {
		wantErr := errors.New("exec boom")
		exec := &fakeExecer{err: wantErr}
		id, err := d.InsertID(t.Context(), exec, "INSERT INTO plans (a) VALUES (?)", "x")
		require.ErrorIs(t, err, wantErr)
		assert.Zero(t, id)
	})

	t.Run("propagates last-insert-id error", func(t *testing.T) {
		wantErr := errors.New("id boom")
		exec := &fakeExecer{result: fakeResult{lastInsertIDErr: wantErr}}
		id, err := d.InsertID(t.Context(), exec, "INSERT INTO plans (a) VALUES (?)", "x")
		require.ErrorIs(t, err, wantErr)
		assert.Zero(t, id)
	})
}

func TestMySQLDialectInsertGuardedID(t *testing.T) {
	d := MySQLDialect{}

	t.Run("row written returns id and inserted", func(t *testing.T) {
		exec := &fakeExecer{result: fakeResult{rowsAffected: 1, lastInsertID: 7}}
		id, inserted, err := d.InsertGuardedID(t.Context(), exec, "INSERT ... SELECT ... WHERE lease = ?", "tok")
		require.NoError(t, err)
		assert.True(t, inserted)
		assert.Equal(t, int64(7), id)
	})

	t.Run("no row written reports not inserted", func(t *testing.T) {
		exec := &fakeExecer{result: fakeResult{rowsAffected: 0}}
		id, inserted, err := d.InsertGuardedID(t.Context(), exec, "INSERT ... SELECT ... WHERE lease = ?", "tok")
		require.NoError(t, err)
		assert.False(t, inserted)
		assert.Zero(t, id)
	})

	t.Run("propagates exec error", func(t *testing.T) {
		wantErr := errors.New("exec boom")
		exec := &fakeExecer{err: wantErr}
		id, inserted, err := d.InsertGuardedID(t.Context(), exec, "INSERT ... SELECT ... WHERE lease = ?", "tok")
		require.ErrorIs(t, err, wantErr)
		assert.False(t, inserted)
		assert.Zero(t, id)
	})

	t.Run("propagates rows-affected error", func(t *testing.T) {
		wantErr := errors.New("rows boom")
		exec := &fakeExecer{result: fakeResult{rowsAffectedErr: wantErr}}
		id, inserted, err := d.InsertGuardedID(t.Context(), exec, "INSERT ... SELECT ... WHERE lease = ?", "tok")
		require.ErrorIs(t, err, wantErr)
		assert.False(t, inserted)
		assert.Zero(t, id)
	})

	t.Run("propagates last-insert-id error after a row is written", func(t *testing.T) {
		wantErr := errors.New("id boom")
		exec := &fakeExecer{result: fakeResult{rowsAffected: 1, lastInsertIDErr: wantErr}}
		id, inserted, err := d.InsertGuardedID(t.Context(), exec, "INSERT ... SELECT ... WHERE lease = ?", "tok")
		require.ErrorIs(t, err, wantErr)
		assert.False(t, inserted)
		assert.Zero(t, id)
	})
}
