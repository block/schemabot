// retry.go centralizes retry handling for transient database conflicts.
package sqlstore

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

// lockRetryMaxAttempts bounds how many times a transient lock conflict is
// retried before giving up.
const lockRetryMaxAttempts = 5

// lockRetryBaseBackoff is the first retry delay; it grows exponentially with
// full jitter on each subsequent attempt.
const lockRetryBaseBackoff = 5 * time.Millisecond

// lockRetryMaxBackoff caps the delay before any one retry, so raising
// lockRetryMaxAttempts adds attempts without letting the exponential backoff
// grow into seconds.
const lockRetryMaxBackoff = 100 * time.Millisecond

// withLockRetry runs fn, retrying conflicts selected by the backend classifier
// with bounded attempts and jittered backoff. It returns promptly if the context
// is cancelled between attempts, wrapping the context error with the operation
// and the lock conflict it was retrying so a caller can tell lock starvation
// from any other cancellation.
//
// fn must be idempotent and either run one autocommit statement or own its entire
// transaction. It must not run inside a caller-owned transaction: PostgreSQL
// aborts the transaction after a statement error, while MySQL lock-wait timeouts
// roll back only the statement. Non-retryable errors are returned unchanged.
func withLockRetry(ctx context.Context, classifier ErrorClassifier, op string, fn func() error) error {
	var lastErr error
	for attempt := range lockRetryMaxAttempts {
		if attempt > 0 {
			if err := sleepBackoff(ctx, attempt); err != nil {
				return fmt.Errorf("%s: stopped retrying after %d attempts (last lock conflict: %s): %w", op, attempt, lastErr.Error(), err)
			}
		}
		err := fn()
		if err == nil {
			return nil
		}
		if !classifier.IsRetryableConflict(err) {
			return err
		}
		lastErr = err
	}
	return fmt.Errorf("%s after %d attempts: %w", op, lockRetryMaxAttempts, lastErr)
}

// sleepBackoff waits before the next retry using exponential backoff with full
// jitter, capped at lockRetryMaxBackoff, returning early if the context is
// cancelled. Cancellation is checked before waiting because full jitter can
// pick a near-zero delay, and a select between an already-cancelled context
// and an already-fired timer picks either.
func sleepBackoff(ctx context.Context, attempt int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	delay := time.Duration(rand.Int64N(int64(lockRetryBackoffCeiling(attempt)) + 1))
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// lockRetryBackoffCeiling is the longest delay before the given retry attempt:
// the base backoff doubled per attempt, capped at lockRetryMaxBackoff.
func lockRetryBackoffCeiling(attempt int) time.Duration {
	ceiling := lockRetryBaseBackoff
	for range attempt - 1 {
		if ceiling >= lockRetryMaxBackoff {
			break
		}
		ceiling *= 2
	}
	return min(ceiling, lockRetryMaxBackoff)
}
