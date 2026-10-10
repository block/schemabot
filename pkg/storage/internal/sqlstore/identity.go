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
	// inserts zero or one row. inserted has one meaning on every path: the
	// dialect proved that a row was written. id is meaningful only when
	// inserted is true and err is nil.
	//
	// When err is nil, inserted=false is the guard's answer (for example a
	// lost lease) and the caller interprets it. When err is non-nil,
	// inserted=false means unproven, never rejected: an errored statement
	// says nothing about the guard, so callers check err first and prove
	// ownership with a read rather than concluding it from the failure. A
	// dialect reports inserted=true with an error only when the write is
	// already confirmed and a later step failed. MySQL learns the row count
	// before it reads the id back, so it reports inserted=true when only the
	// id read-back failed; a RowsAffected failure leaves the write unproven.
	// PostgreSQL writes and reads the id in one statement (RETURNING), so any
	// error leaves the write unproven.
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
// read-back are one statement, so any other error leaves the write unproven
// and reports inserted=false: the dialect cannot tell a write that failed
// from one that landed and then failed to hand its id back.
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
// written via RowsAffected, returning its id only when one was. Until
// RowsAffected has answered, the write is unproven and every error reports
// inserted=false.
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
	// RowsAffected proved the write; report inserted=true even if reading
	// the id back fails, so the proven write is never reported as a miss.
	id, err := result.LastInsertId()
	if err != nil {
		return 0, true, err
	}
	return id, true, nil
}
