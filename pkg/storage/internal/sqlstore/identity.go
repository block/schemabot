package sqlstore

import (
	"context"
	"database/sql"
	"errors"
)

// queryExecer is the execution subset needed to run an INSERT and read back
// the generated identity, either from a driver Result (MySQL) or a RETURNING
// clause (Postgres). Both the rebind-aware pool (*rebindDB) and an in-flight
// transaction (*rebindTx) satisfy it, so identity inserts always execute
// behind the placeholder-rebind boundary.
type queryExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// identityInserter runs INSERTs that return the new row's auto-generated id.
// This is a separate capability from Dialect (which only renders SQL syntax):
// identity retrieval differs by engine in execution, not just syntax — MySQL
// reads Result.LastInsertId(), while Postgres appends RETURNING id and scans
// it. Stores that only need syntax depend on Dialect; stores that create rows
// depend on this.
type identityInserter interface {
	// InsertID runs an INSERT that creates exactly one row and returns its
	// generated id. A statement that inserts no row is an error.
	InsertID(ctx context.Context, exec queryExecer, query string, args ...any) (int64, error)
	// InsertGuardedID runs a guarded INSERT ... SELECT ... WHERE <guard> that
	// inserts zero or one row. inserted reports whether a row was written; when
	// it is false and err is nil the caller interprets the guard (for example a
	// lost lease). id is meaningful only when inserted is true and err is nil.
	//
	// Callers must check err before inserted. When err is non-nil, inserted
	// reports only what the dialect can tell about the write: MySQL learns the
	// row count before it reads the id back, so it reports inserted=true when
	// the write landed and only the id read-back failed; PostgreSQL reads the
	// id in the same statement (RETURNING), so a failed read-back is not
	// separable from a failed write and it reports inserted=false.
	InsertGuardedID(ctx context.Context, exec queryExecer, query string, args ...any) (id int64, inserted bool, err error)
}

// InsertID appends RETURNING id and scans the generated PostgreSQL identity.
func (PostgresDialect) InsertID(ctx context.Context, exec queryExecer, query string, args ...any) (int64, error) {
	var id int64
	if err := exec.QueryRowContext(ctx, query+" RETURNING id", args...).Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

// InsertGuardedID distinguishes a guarded INSERT that selected no row from a
// successful insert by the absence of a RETURNING row. The write and the id
// read-back are one statement, so any other error reports inserted=false: the
// dialect cannot tell a write that failed from one that landed and then
// failed to hand its id back.
func (PostgresDialect) InsertGuardedID(ctx context.Context, exec queryExecer, query string, args ...any) (int64, bool, error) {
	id, err := (PostgresDialect{}).InsertID(ctx, exec, query, args...)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return id, true, nil
}

// InsertID executes the INSERT and returns Result.LastInsertId().
func (MySQLDialect) InsertID(ctx context.Context, exec queryExecer, query string, args ...any) (int64, error) {
	result, err := exec.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// InsertGuardedID executes the guarded INSERT and reports whether a row was
// written via RowsAffected, returning its id only when one was.
func (MySQLDialect) InsertGuardedID(ctx context.Context, exec queryExecer, query string, args ...any) (int64, bool, error) {
	result, err := exec.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, false, err
	}
	if rows == 0 {
		return 0, false, nil
	}
	// The row is written; report inserted=true even if reading its id back
	// fails, so callers can't misclassify a successful write as a guard miss.
	id, err := result.LastInsertId()
	if err != nil {
		return 0, true, err
	}
	return id, true, nil
}
