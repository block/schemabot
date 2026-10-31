package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/block/schemabot/pkg/namedlock"
)

// lockReleaseTimeout bounds how long a pinned session may spend releasing its
// advisory lock. A release stuck on a half-open connection would otherwise
// never return, and ConnMaxLifetime cannot retire a connection that is in use.
const lockReleaseTimeout = 5 * time.Second

// releaseLockConn releases the advisory lock held on conn's session within
// lockReleaseTimeout and hands the session back. A session whose release
// failed, timed out, or reported the lock was not held may still hold the lock
// or be in an unknown state, so it is discarded rather than returned to the
// pool, where its lock would block every other holder.
//
// label names the lock in log messages, and attrs identify it in every log line
// this helper emits. A nil conn is a no-op.
func releaseLockConn(ctx context.Context, locker namedlock.Locker, conn *rebindConn, lockName, label string, attrs ...any) {
	if conn == nil {
		return
	}

	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), lockReleaseTimeout)
	defer cancel()

	released, err := locker.Release(releaseCtx, conn.lockerConn(), lockName)
	switch {
	case err != nil:
		slog.WarnContext(releaseCtx, "failed to release "+label+"; discarding connection",
			append(attrs, "error", err)...)
		discardLockConn(releaseCtx, conn, label, attrs...)
	case !released:
		slog.WarnContext(releaseCtx, label+" was not held by this session; discarding connection", attrs...)
		discardLockConn(releaseCtx, conn, label, attrs...)
	}

	closeLockConn(releaseCtx, conn, label, attrs...)
}

// discardLockConn marks the pinned connection as bad so the pool destroys it
// instead of reusing a session whose advisory-lock state is uncertain.
func discardLockConn(ctx context.Context, conn *rebindConn, label string, attrs ...any) {
	if err := conn.discardBadConn(); err != nil {
		slog.WarnContext(ctx, "failed to discard "+label+" connection",
			append(attrs, "error", err)...)
	}
}

// closeLockConn closes the pinned connection, returning a healthy session to
// the pool.
func closeLockConn(ctx context.Context, conn *rebindConn, label string, attrs ...any) {
	err := conn.Close()
	if err == nil || errors.Is(err, sql.ErrConnDone) {
		// ErrConnDone means the session was already retired by a preceding
		// discard; there is nothing left to release.
		return
	}
	slog.WarnContext(ctx, "failed to close "+label+" connection",
		append(attrs, "error", err)...)
}
