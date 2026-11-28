package tern

import (
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

// recordFailure records a failed read at now and reports whether the run has
// lasted the full window, so the drive must hand the apply back.
func (o *controlReadOutage) recordFailure(now time.Time, window time.Duration) bool {
	if o.failures == 0 {
		o.since = now
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

// controlReadOutageWindow returns how long a drive keeps polling while it
// cannot read its apply's pending control requests.
func (c *LocalClient) controlReadOutageWindow() time.Duration {
	if c.controlReadOutageWindowOverride > 0 {
		return c.controlReadOutageWindowOverride
	}
	return storage.ApplyLeaseStaleAfter
}
