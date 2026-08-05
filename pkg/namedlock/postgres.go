package namedlock

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// lockNotAvailable is the PostgreSQL SQLSTATE reported when lock_timeout
// elapses before the requested lock is granted.
const lockNotAvailable = "55P03"

// undoAcquireUnlockTimeout bounds the best-effort pg_advisory_unlock issued
// when an acquire fails after the advisory lock may already have been
// granted, so undoing the acquisition cannot hang on a session whose state is
// already suspect.
const undoAcquireUnlockTimeout = 5 * time.Second

// Postgres implements Locker with PostgreSQL session-level advisory locks
// (pg_advisory_lock / pg_advisory_unlock). Like MySQL's GET_LOCK, the lock is
// bound to the connection's session and is server-wide (advisory lock keys are
// shared across every database of the instance), so callers must pass the same
// *sql.Conn to Acquire and Release and keep it open for as long as the lock is
// held.
//
// Advisory locks are keyed by int64, so name is hashed into the key (see
// advisoryLockKey). A hash collision between two distinct names makes them
// contend on the same lock — extra serialization, never lost mutual
// exclusion.
//
// Connections must use the pgx driver: the bounded-wait path inspects the
// driver's typed error to distinguish an elapsed wait from a real failure.
type Postgres struct{}

var _ Locker = Postgres{}

// Acquire takes the advisory lock for name, waiting up to wait for it. A zero
// wait attempts the lock without waiting (pg_try_advisory_lock). A positive
// wait is enforced server-side by a lock_timeout scoped to a transaction
// around pg_advisory_lock: set_config(..., is_local => true) reverts the
// timeout at transaction end — including the abort when the wait elapses — so
// it can never leak into later statements on the connection, while the
// session-level advisory lock itself survives the commit and is held until
// Release or until the session ends. Sub-millisecond waits round up to the next
// millisecond. A negative wait is rejected rather than passed through,
// mirroring the MySQL implementation.
func (Postgres) Acquire(ctx context.Context, conn *sql.Conn, name string, wait time.Duration) (bool, error) {
	if wait < 0 {
		return false, fmt.Errorf("acquire named lock %q: negative wait %s", name, wait)
	}
	key := advisoryLockKey(name)

	if wait == 0 {
		var acquired bool
		if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&acquired); err != nil {
			return false, fmt.Errorf("acquire named lock %q: %w", name, err)
		}
		return acquired, nil
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("acquire named lock %q: begin lock_timeout transaction: %w", name, err)
	}
	waitMillis := int64((wait + time.Millisecond - 1) / time.Millisecond)
	if _, err := tx.ExecContext(ctx, "SELECT set_config('lock_timeout', $1, true)", strconv.FormatInt(waitMillis, 10)); err != nil {
		return false, rollbackAfter(tx, fmt.Errorf("acquire named lock %q: set lock_timeout: %w", name, err))
	}
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_lock($1)", key); err != nil {
		if isLockNotAvailable(err) {
			// The wait elapsed and aborted the transaction; roll it back so
			// the connection is usable for a retry.
			if rbErr := tx.Rollback(); rbErr != nil {
				return false, fmt.Errorf("acquire named lock %q: rollback after lock wait elapsed: %w", name, rbErr)
			}
			return false, nil
		}
		// A cancellation can race the grant: the server may have granted the
		// lock even though the statement returned an error, so undo the
		// acquisition after the rollback.
		return false, undoAcquire(ctx, conn, key, rollbackAfter(tx, fmt.Errorf("acquire named lock %q: %w", name, err)))
	}
	if err := tx.Commit(); err != nil {
		// The commit only ends the transaction that scoped lock_timeout — the
		// session-level advisory lock is already granted and survives the
		// failed commit, so reporting failure without undoing it would let a
		// pooled connection carry the lock on an idle session where nothing
		// can ever release it.
		return false, undoAcquire(ctx, conn, key, fmt.Errorf("acquire named lock %q: commit lock_timeout transaction: %w", name, err))
	}
	return true, nil
}

// Release runs pg_advisory_unlock for name. It returns true when this session
// held the lock and false when it did not (the server also emits a warning in
// that case, not an error), which maps directly onto released.
func (Postgres) Release(ctx context.Context, conn *sql.Conn, name string) (bool, error) {
	var released bool
	if err := conn.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", advisoryLockKey(name)).Scan(&released); err != nil {
		return false, fmt.Errorf("release named lock %q: %w", name, err)
	}
	return released, nil
}

// advisoryLockKey hashes name into the int64 key space of PostgreSQL advisory
// locks, reinterpreting the first eight bytes of sha256(name) as a big-endian
// two's-complement int64. The derivation must stay stable across releases:
// pods running different binary versions coordinate through these keys during
// a rolling deploy, so changing the hash would silently break mutual exclusion
// between them.
func advisoryLockKey(name string) int64 {
	sum := sha256.Sum256([]byte(name))
	return int64(binary.BigEndian.Uint64(sum[:8]))
}

// undoAcquire releases the advisory lock for key that may have been granted
// before err interrupted the acquire, preserving the contract that a failed
// Acquire leaves the lock unheld — otherwise a pooled connection would carry
// the lock on an idle session where nothing can ever release it. The unlock
// is issued on a fresh bounded context because the likely cause of the
// failure is ctx itself being cancelled; unlocking a lock that was never
// granted is a safe no-op. If the unlock also fails, the connection is marked
// bad so the pool destroys it — ending the session server-side drops the lock
// with it. err is always returned, with any unlock failure folded in.
func undoAcquire(ctx context.Context, conn *sql.Conn, key int64, err error) error {
	unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), undoAcquireUnlockTimeout)
	defer cancel()
	if _, unlockErr := conn.ExecContext(unlockCtx, "SELECT pg_advisory_unlock($1)", key); unlockErr != nil {
		if rawErr := conn.Raw(func(any) error { return driver.ErrBadConn }); rawErr != nil && !errors.Is(rawErr, driver.ErrBadConn) {
			return fmt.Errorf("%w (unlock: %w; discard connection: %w)", err, unlockErr, rawErr)
		}
		return fmt.Errorf("%w (unlock: %w; connection discarded)", err, unlockErr)
	}
	return err
}

// isLockNotAvailable reports whether err is the server-side lock_timeout
// expiry, meaning the bounded wait elapsed without the lock being granted.
func isLockNotAvailable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == lockNotAvailable
}

// rollbackAfter rolls tx back after err and folds a rollback failure into the
// returned error, so a connection left inside an aborted transaction is not
// reported as only the original failure.
func rollbackAfter(tx *sql.Tx, err error) error {
	if rbErr := tx.Rollback(); rbErr != nil {
		return fmt.Errorf("%w (rollback: %w)", err, rbErr)
	}
	return err
}
