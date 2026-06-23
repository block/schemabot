// retry.go centralizes retry handling for transient InnoDB lock contention.
// Concurrent callers racing for the same rows can be chosen as a deadlock victim
// or time out waiting for a lock; both roll the failed work back — the offending
// statement, or the whole transaction when the work runs inside one — so the
// operation can be safely re-run.
package mysqlstore

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
)

// lockRetryMaxAttempts bounds how many times a transient lock conflict is
// retried before giving up.
const lockRetryMaxAttempts = 5

// lockRetryBaseBackoff is the first retry delay; it grows exponentially with
// full jitter on each subsequent attempt.
const lockRetryBaseBackoff = 5 * time.Millisecond

// MySQL error codes for transient lock contention. InnoDB picks a deadlock
// victim (1213) or times out a lock wait (1205) when callers race for the same
// rows; both roll back the failed attempt (statement or enclosing transaction),
// so the caller can safely retry.
const (
	mysqlErrDeadlock        = 1213
	mysqlErrLockWaitTimeout = 1205
)

// withLockRetry runs fn, retrying transient InnoDB lock conflicts (deadlock
// victim 1213, lock-wait timeout 1205) with bounded attempts and jittered
// backoff. It returns promptly if the context is cancelled between attempts.
//
// fn must be idempotent: a retried attempt was fully rolled back — the offending
// statement, or the entire transaction when fn runs one — so re-running it from a
// clean read is safe. Non-retryable errors (including genuine application
// conflicts) are returned immediately, unchanged.
func withLockRetry(ctx context.Context, op string, fn func() error) error {
	var lastErr error
	for attempt := range lockRetryMaxAttempts {
		if attempt > 0 {
			if err := sleepBackoff(ctx, attempt); err != nil {
				return err
			}
		}
		err := fn()
		if err == nil {
			return nil
		}
		if !isRetryableLockError(err) {
			return err
		}
		lastErr = err
	}
	return fmt.Errorf("%s after %d attempts: %w", op, lockRetryMaxAttempts, lastErr)
}

// isRetryableLockError reports whether err is a transient InnoDB lock conflict
// that resolves on retry: a deadlock victim or a lock-wait timeout. Both leave
// the database unchanged, so re-running the operation is safe.
func isRetryableLockError(err error) bool {
	var mysqlErr *gomysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	return mysqlErr.Number == mysqlErrDeadlock || mysqlErr.Number == mysqlErrLockWaitTimeout
}

// sleepBackoff waits before the next retry using exponential backoff with full
// jitter, returning early if the context is cancelled.
func sleepBackoff(ctx context.Context, attempt int) error {
	maxDelay := lockRetryBaseBackoff << (attempt - 1)
	delay := time.Duration(rand.Int64N(int64(maxDelay) + 1))
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
