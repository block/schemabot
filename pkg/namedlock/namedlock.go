// Package namedlock abstracts the session-scoped advisory ("named") locks that
// serialize SchemaBot's coordination points — apply-target execution,
// pending-drops cleanup, and schema bootstrap — so those flows can run on
// engines other than MySQL. MySQL implements them with GET_LOCK / RELEASE_LOCK,
// which are bound to a single connection's session, so every caller holds the
// lock on a pinned *sql.Conn for the lock's whole lifetime and passes that same
// connection to Release (or closes it) to drop the lock.
package namedlock

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Locker acquires and releases session-scoped advisory locks on a pinned
// connection. Implementations are engine-specific: MySQL uses GET_LOCK /
// RELEASE_LOCK, a future Postgres implementation would use pg_advisory_lock.
type Locker interface {
	// Acquire attempts to take the named lock on conn, waiting up to wait for
	// it. acquired reports whether the lock was taken; it is false when the
	// wait elapses without obtaining the lock. err is non-nil for any driver or
	// SQL failure. The lock lives on conn's session and is dropped by Release
	// or when conn closes.
	Acquire(ctx context.Context, conn *sql.Conn, name string, wait time.Duration) (acquired bool, err error)
	// Release drops the named lock previously taken on conn. released reports
	// whether this session held the lock; it is false when the lock was not
	// held by this session (already released or expired). err is non-nil for
	// any driver or SQL failure.
	Release(ctx context.Context, conn *sql.Conn, name string) (released bool, err error)
}

// MySQL implements Locker with MySQL's GET_LOCK / RELEASE_LOCK. The lock is
// bound to the connection's session, so callers must pass the same *sql.Conn to
// Acquire and Release and keep it open for as long as the lock is held.
type MySQL struct{}

var _ Locker = MySQL{}

// Acquire runs GET_LOCK(name, wait). GET_LOCK returns 1 when the lock is
// obtained, 0 when the wait elapses first, and NULL on error (for example the
// session was killed); a NULL result is surfaced as an error.
func (MySQL) Acquire(ctx context.Context, conn *sql.Conn, name string, wait time.Duration) (bool, error) {
	var result sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", name, int(wait.Seconds())).Scan(&result); err != nil {
		return false, err
	}
	if !result.Valid {
		return false, fmt.Errorf("GET_LOCK(%q) returned NULL", name)
	}
	return result.Int64 == 1, nil
}

// Release runs RELEASE_LOCK(name). RELEASE_LOCK returns 1 when this session
// held the lock, 0 when another session holds it, and NULL when the lock does
// not exist (never taken or already released); both 0 and NULL report
// released=false, since neither released a lock this session was holding.
func (MySQL) Release(ctx context.Context, conn *sql.Conn, name string) (bool, error) {
	var result sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", name).Scan(&result); err != nil {
		return false, err
	}
	return result.Valid && result.Int64 == 1, nil
}
