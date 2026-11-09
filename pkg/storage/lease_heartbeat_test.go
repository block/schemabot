package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hangUntilDone is a heartbeat write stuck on a half-open storage connection:
// it returns only once its context ends, the way the SQL driver unblocks a read
// when the context it was given is done.
func hangUntilDone(ctx context.Context) error {
	<-ctx.Done()
	return fmt.Errorf("heartbeat apply 41: %w", ctx.Err())
}

// TestHeartbeatLeaseAttempt covers the per-attempt deadline every lease
// heartbeat loop runs its write under. A write that hangs is cut off and
// reported as a timed-out heartbeat, so the loop counts it toward the staleness
// window instead of blocking forever. The attempt that straddles the end of the
// window returns at the window's end, so the loop's presumed-lost check runs the
// moment a peer may reclaim the row. A drive that is shutting down gets the
// write's own error back, so shutdown is never mistaken for a slow store.
func TestHeartbeatLeaseAttempt(t *testing.T) {
	t.Run("a write that hangs times out after the attempt timeout", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			err := HeartbeatLeaseAttempt(t.Context(), start, start, hangUntilDone)
			require.ErrorIs(t, err, ErrLeaseHeartbeatTimedOut)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			assert.Equal(t, LeaseHeartbeatAttemptTimeout, time.Since(start))
		})
	})

	t.Run("the attempt never runs past the end of the staleness window", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			now := time.Now()
			lastSuccess := now.Add(-(ApplyLeaseStaleAfter - time.Second))
			err := HeartbeatLeaseAttempt(t.Context(), now, lastSuccess, hangUntilDone)
			require.ErrorIs(t, err, ErrLeaseHeartbeatTimedOut)
			assert.Equal(t, lastSuccess.Add(ApplyLeaseStaleAfter), time.Now(),
				"the attempt returns when a peer may first reclaim the row")
		})
	})

	t.Run("an attempt that starts with the window spent fails at once", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			now := time.Now()
			err := HeartbeatLeaseAttempt(t.Context(), now, now.Add(-2*ApplyLeaseStaleAfter), hangUntilDone)
			require.ErrorIs(t, err, ErrLeaseHeartbeatTimedOut)
			assert.Equal(t, now, time.Now())
		})
	})

	t.Run("a drive shutting down gets the write's own error", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			time.AfterFunc(time.Second, cancel)
			now := time.Now()
			err := HeartbeatLeaseAttempt(ctx, now, now, hangUntilDone)
			require.ErrorIs(t, err, context.Canceled)
			assert.NotErrorIs(t, err, ErrLeaseHeartbeatTimedOut)
		})
	})

	t.Run("a write that returns is passed through", func(t *testing.T) {
		now := time.Now()
		require.NoError(t, HeartbeatLeaseAttempt(t.Context(), now, now, func(context.Context) error { return nil }))

		err := HeartbeatLeaseAttempt(t.Context(), now, now, func(context.Context) error { return ErrApplyLeaseLost })
		require.ErrorIs(t, err, ErrApplyLeaseLost)
		assert.NotErrorIs(t, err, ErrLeaseHeartbeatTimedOut)
	})

	t.Run("the write gets a deadline", func(t *testing.T) {
		now := time.Now()
		err := HeartbeatLeaseAttempt(t.Context(), now, now, func(ctx context.Context) error {
			if _, ok := ctx.Deadline(); !ok {
				return errors.New("heartbeat write has no deadline")
			}
			return nil
		})
		require.NoError(t, err)
	})
}
