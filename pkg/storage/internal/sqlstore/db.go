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
// Transactions and pinned connections obtained via BeginTx / Conn are wrapped
// the same way, so every execution path — pool, transaction, or pinned
// connection — rebinds exactly once on the final assembled SQL. The only
// sanctioned escape is rebindConn.raw() for the advisory-lock boundary.
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

// BeginTx starts a transaction on the underlying pool, wrapped so statements
// executed on it rebind their placeholders like direct pool execution.
func (d *rebindDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*rebindTx, error) {
	tx, err := d.pool.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &rebindTx{tx: tx, binder: d.binder}, nil
}

// Conn pins a single connection from the underlying pool, wrapped so
// transactions begun on it rebind their placeholders.
func (d *rebindDB) Conn(ctx context.Context) (*rebindConn, error) {
	conn, err := d.pool.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &rebindConn{conn: conn, binder: d.binder}, nil
}

// PingContext verifies the underlying pool's connectivity.
func (d *rebindDB) PingContext(ctx context.Context) error {
	return d.pool.PingContext(ctx)
}

// Close closes the underlying pool.
func (d *rebindDB) Close() error {
	return d.pool.Close()
}

// rebindTx wraps an in-flight transaction so every statement executed on it
// passes through the dialect's binder exactly once, matching direct pool
// execution. Statements must reach the binder as final assembled SQL: a query
// is rebound at the moment it executes, never earlier and never twice.
type rebindTx struct {
	tx     *sql.Tx
	binder binder
}

// ExecContext rebinds the query's placeholders and executes it in the
// transaction.
func (t *rebindTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.tx.ExecContext(ctx, t.binder.Rebind(query), args...)
}

// QueryContext rebinds the query's placeholders and runs it in the
// transaction.
func (t *rebindTx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, t.binder.Rebind(query), args...)
}

// QueryRowContext rebinds the query's placeholders and runs it in the
// transaction.
func (t *rebindTx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return t.tx.QueryRowContext(ctx, t.binder.Rebind(query), args...)
}

// Commit commits the transaction.
func (t *rebindTx) Commit() error {
	return t.tx.Commit()
}

// Rollback aborts the transaction.
func (t *rebindTx) Rollback() error {
	return t.tx.Rollback()
}

// rebindConn wraps a pinned pool connection. It exists for the advisory-lock
// flow, which holds a session-scoped lock on the pinned session and runs its
// apply writes in transactions begun on that same session.
type rebindConn struct {
	conn   *sql.Conn
	binder binder
}

// BeginTx starts a transaction on the pinned connection, wrapped so statements
// executed on it rebind their placeholders.
func (c *rebindConn) BeginTx(ctx context.Context, opts *sql.TxOptions) (*rebindTx, error) {
	tx, err := c.conn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &rebindTx{tx: tx, binder: c.binder}, nil
}

// Raw runs f against the pinned driver connection, for lifecycle control such
// as discarding a session whose advisory-lock state is uncertain.
func (c *rebindConn) Raw(f func(driverConn any) error) error {
	return c.conn.Raw(f)
}

// Close returns the pinned session to the pool.
func (c *rebindConn) Close() error {
	return c.conn.Close()
}

// raw exposes the pinned *sql.Conn for the advisory-lock boundary. It is the
// only sanctioned binder escape: namedlock.Locker implementations emit their
// engine's native placeholders and must execute on the pinned session that
// holds the lock. No store SQL may execute through it.
func (c *rebindConn) raw() *sql.Conn {
	return c.conn
}
