package mysqlstore

import (
	"context"
	"database/sql"
)

// queryExecer is the subset of *sql.DB / *sql.Tx needed to run an INSERT and
// read back the generated identity, either from a driver Result (MySQL) or a
// RETURNING clause (Postgres). Both the connection pool and an in-flight
// transaction satisfy it.
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
	// it is false the caller interprets the guard (for example a lost lease).
	// id is meaningful only when inserted is true.
	InsertGuardedID(ctx context.Context, exec queryExecer, query string, args ...any) (id int64, inserted bool, err error)
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
	id, err := result.LastInsertId()
	if err != nil {
		return 0, false, err
	}
	return id, true, nil
}
