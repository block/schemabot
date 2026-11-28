package tern

import (
	"context"
	"time"

	"github.com/block/schemabot/pkg/storage"
)

// controlReadOutage tracks a run of failed reads of an apply's pending control
// requests during one drive's progress poll.
//
// A control request waits in storage until a drive reads it (CO-1), so a read
// that fails loses no request; it only delays one. The drive therefore keeps
// its engine work running and reads again at the next poll, rather than
// exiting and halting a schema change that a brief storage outage would
// otherwise throw away. A run of failures that lasts as long as the lease
// staleness window means the drive can no longer promise to deliver an
// operator's command, so it hands the apply back for another driver to claim.
// When storage itself is down, the drive's heartbeat has failed for that same
// window and already cancelled the drive.
type controlReadOutage struct {
	// since is when the current run of failed reads began.
	since time.Time
	// failures counts the failed reads in the current run; zero once a read
	// succeeds. It paces the next poll.
	failures int
}

// recordSuccess ends the current run of failed reads.
func (o *controlReadOutage) recordSuccess() {
	*o = controlReadOutage{}
}

// recordFailure records a read that started at attemptStart and failed at
// now, and reports whether the run has lasted the full window, so the drive
// must hand the apply back. A run is measured from the start of its first
// failed read, so the time a read spent hanging counts toward the window.
func (o *controlReadOutage) recordFailure(attemptStart, now time.Time, window time.Duration) bool {
	if o.failures == 0 {
		o.since = attemptStart
	}
	o.failures++
	return o.failingFor(now) >= window
}

// failingFor is how long the current run of failed reads has lasted at now.
func (o *controlReadOutage) failingFor(now time.Time) time.Duration {
	if o.failures == 0 {
		return 0
	}
	return now.Sub(o.since)
}

// remaining is how much of the window is left at now: all of it when no run
// is open, and nothing once the run has lasted the window.
func (o *controlReadOutage) remaining(now time.Time, window time.Duration) time.Duration {
	return max(window-o.failingFor(now), 0)
}

// nextPollWait caps the backoff before the next read at what is left of the
// window, so the read that settles the run is not scheduled past it.
func (o *controlReadOutage) nextPollWait(backoff time.Duration, now time.Time, window time.Duration) time.Duration {
	return min(backoff, o.remaining(now, window))
}

// loadPendingControlOperationsWithin reads the apply's pending control
// requests under a deadline of what is left of the outage window, so a read
// that hangs cannot hold the drive past the window while its heartbeat still
// succeeds. A read that starts with the window spent gets no time and fails
// at once. It returns when the read started, which recordFailure measures a
// run from.
func (c *LocalClient) loadPendingControlOperationsWithin(ctx context.Context, apply *storage.Apply, outage *controlReadOutage) (pendingControlOperations, time.Time, error) {
	start := time.Now()
	readCtx, cancel := context.WithTimeout(ctx, outage.remaining(start, c.controlReadOutageWindow()))
	defer cancel()
	pending, err := loadPendingControlOperations(readCtx, c.storage, apply)
	return pending, start, err
}

// controlReadOutageWindow returns how long a drive keeps polling while it
// cannot read its apply's pending control requests.
func (c *LocalClient) controlReadOutageWindow() time.Duration {
	if c.controlReadOutageWindowOverride > 0 {
		return c.controlReadOutageWindowOverride
	}
	return storage.ApplyLeaseStaleAfter
}
