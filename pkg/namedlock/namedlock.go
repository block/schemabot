// Package namedlock abstracts the session-scoped advisory ("named") locks that
// serialize SchemaBot's coordination points — apply-target execution,
// pending-drops cleanup, and schema bootstrap — so those flows can run on
// engines other than MySQL. MySQL implements them with GET_LOCK / RELEASE_LOCK,
// which are bound to a single connection's session, so every caller holds the
// lock on a pinned *sql.Conn for the lock's whole lifetime and passes that same
// connection to Release (or closes it) to drop the lock.
//
// Engine selection is per-target, not per-process: a single server can serve
// multiple engines and the pending-drops cleaner iterates heterogeneous targets
// in one pass. Implementations are therefore supplied as injected dependencies
// chosen per target (a struct field or parameter), never as package-level
// bindings; a caller without a locker for its target fails closed rather than
// assuming MySQL semantics.
package namedlock

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"time"
)

// Locker acquires and releases session-scoped advisory locks on a pinned
// connection. Implementations are engine-specific: MySQL uses GET_LOCK /
// RELEASE_LOCK, a future Postgres implementation would use pg_advisory_lock.
//
// name identifies the lock within an engine but is not guaranteed to round-trip
// verbatim: MySQL uses it as the literal lock string, whereas Postgres advisory
// locks are keyed by int64, so a Postgres implementation would hash name into a
// key. Callers should treat name as an opaque identity, not a value they can
// read back.
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
// Acquire and Release and keep it open for as long as the lock is held. MySQL 8
// rejects lock names longer than 64 characters; callers are responsible for
// keeping name within that bound.
type MySQL struct{}

var _ Locker = MySQL{}

// Acquire runs GET_LOCK(name, wait). GET_LOCK returns 1 when the lock is
// obtained, 0 when the wait elapses first, and NULL on error (for example the
// session was killed); a NULL result is surfaced as an error. MySQL only
// supports whole-second waits, so sub-second waits round up to the next
// second. A negative wait is rejected rather than passed through, because
// GET_LOCK treats a negative timeout as an infinite wait.
func (MySQL) Acquire(ctx context.Context, conn *sql.Conn, name string, wait time.Duration) (bool, error) {
	if wait < 0 {
		return false, fmt.Errorf("acquire named lock %q: negative wait %s", name, wait)
	}
	waitSeconds := int64(math.Ceil(wait.Seconds()))
	var result sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", name, waitSeconds).Scan(&result); err != nil {
		return false, fmt.Errorf("acquire named lock %q: %w", name, err)
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
		return false, fmt.Errorf("release named lock %q: %w", name, err)
	}
	return result.Valid && result.Int64 == 1, nil
}
