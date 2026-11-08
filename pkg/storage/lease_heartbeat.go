package storage

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// LeaseHeartbeatAttemptTimeout bounds one lease heartbeat write. It is a
// fraction of ApplyLeaseStaleAfter so that several attempts fit inside the
// window a driver measures its presumed-lost bound against.
const LeaseHeartbeatAttemptTimeout = ApplyLeaseStaleAfter / 4

// ErrLeaseHeartbeatTimedOut wraps a lease heartbeat write that did not return
// before its attempt deadline. It is a failed heartbeat like any other: it says
// nothing about who holds the lease, and it counts toward the staleness window.
var ErrLeaseHeartbeatTimedOut = errors.New("lease heartbeat attempt timed out")

// HeartbeatLeaseAttempt runs one lease heartbeat write under its own deadline,
// so a write that hangs is counted as a failure rather than blocking the
// heartbeat loop. A write stuck on a half-open storage connection would
// otherwise never return, the presumed-lost check that runs after it would never
// run, and the driver would keep driving after a peer reclaimed the stale row.
//
// The deadline is LeaseHeartbeatAttemptTimeout from now, and never later than
// the moment heartbeats starting at lastSuccess have failed for the full
// ApplyLeaseStaleAfter window. The attempt that straddles the end of the window
// therefore returns at that moment, and the caller's presumed-lost check runs
// when a peer may first reclaim the row, not up to an attempt timeout later.
// lastSuccess should be when the last successful attempt started: the row's
// heartbeat can be no older than that, so measuring from it never lets the
// driver outlast the window a peer measures from the row.
//
// now and lastSuccess come from the caller's clock. The deadline is applied as
// a duration from now so a caller on an injected clock stays consistent with
// itself. An attempt that starts with the window already spent gets no time at
// all, so it fails at once and the caller stops on it. A timed-out attempt returns an error wrapping
// ErrLeaseHeartbeatTimedOut. When ctx itself ends, the write's error is returned
// unchanged so the caller can tell shutdown from a slow store.
func HeartbeatLeaseAttempt(ctx context.Context, now, lastSuccess time.Time, heartbeat func(context.Context) error) error {
	timeout := max(min(LeaseHeartbeatAttemptTimeout, lastSuccess.Add(ApplyLeaseStaleAfter).Sub(now)), 0)
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := heartbeat(attemptCtx)
	if err == nil {
		return nil
	}
	if ctx.Err() == nil && errors.Is(attemptCtx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%w after %s: %w", ErrLeaseHeartbeatTimedOut, timeout, err)
	}
	return err
}
