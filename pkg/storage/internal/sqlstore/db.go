package sqlstore

import (
	"context"
	"database/sql"
)

// binder rewrites a statement's parameter placeholders into the wire syntax
// the target engine expects. The store builds all SQL with MySQL-style "?"
// placeholders; engines that use a different syntax (Postgres "$n") rebind at
// the execution boundary, while MySQL's binder is the identity.
type binder interface {
	// Rebind returns the query with its placeholders rewritten for the target
	// engine. It must be a pure string transformation: same input, same output,
	// no side effects.
	Rebind(query string) string
}

// Rebind returns the query unchanged: MySQL consumes the store's native "?"
// placeholders directly.
func (MySQLDialect) Rebind(query string) string {
	return query
}

// rebindDB wraps the connection pool and owns placeholder rebinding: every
// statement executed directly on the pool passes through the dialect's binder
// exactly once before reaching the SQL driver. Stores hold a *rebindDB rather
// than a raw *sql.DB so no store can bypass the rebind boundary.
//
// Transactions and pinned connections obtained via BeginTx / Conn are handed
// back as raw handles: statements executed on them do not pass through the
// binder yet, so those paths remain MySQL-placeholder only.
type rebindDB struct {
	pool   *sql.DB
	binder binder
}

// newRebindDB wraps pool so all direct execution rebinds placeholders with b.
func newRebindDB(pool *sql.DB, b binder) *rebindDB {
	return &rebindDB{pool: pool, binder: b}
}

// ExecContext rebinds the query's placeholders and executes it on the pool.
func (d *rebindDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return d.pool.ExecContext(ctx, d.binder.Rebind(query), args...)
}

// QueryContext rebinds the query's placeholders and runs it on the pool.
func (d *rebindDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return d.pool.QueryContext(ctx, d.binder.Rebind(query), args...)
}

// QueryRowContext rebinds the query's placeholders and runs it on the pool.
func (d *rebindDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return d.pool.QueryRowContext(ctx, d.binder.Rebind(query), args...)
}

// BeginTx starts a transaction on the underlying pool. The returned *sql.Tx is
// a raw handle: statements executed on it are not rebound.
func (d *rebindDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return d.pool.BeginTx(ctx, opts)
}

// Conn pins a single connection from the underlying pool. The returned
// *sql.Conn is a raw handle: statements executed on it are not rebound.
func (d *rebindDB) Conn(ctx context.Context) (*sql.Conn, error) {
	return d.pool.Conn(ctx)
}

// PingContext verifies the underlying pool's connectivity.
func (d *rebindDB) PingContext(ctx context.Context) error {
	return d.pool.PingContext(ctx)
}

// Close closes the underlying pool.
func (d *rebindDB) Close() error {
	return d.pool.Close()
}
