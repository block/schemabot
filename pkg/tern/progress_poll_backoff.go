package tern

import (
	"math/rand/v2"
	"time"
)

// progressPollBackoffCapMultiple caps a failing drive's wait between progress
// polls at this multiple of its plain poll interval. At the default interval
// the waits before a drive's error budget is spent total at most 48s, well
// inside storage.ApplyDriveStallAfter, and no single wait comes near
// storage.ApplyLeaseStaleAfter. That bound covers the waits only: the engine
// and storage calls a failing poll makes take their own time, and a provider
// that takes about 30s to fail each call can reach the stall bound before the
// budget is spent, which ends the drive in a stall cancel instead. The drive's lease is renewed by its own heartbeat goroutine, which
// keeps running through every wait.
const progressPollBackoffCapMultiple = 16

// progressPollWait returns how long a progress drive waits before its next
// poll, given how many polls in a row have failed. A drive whose last poll
// answered, or whose first failure is the only one, waits the plain interval.
// Each further failure doubles the wait, up to progressPollBackoffCapMultiple
// intervals, so a drive polling a provider that rate-limits it backs off
// instead of spending its error budget in a few seconds. A backed-off wait is
// shortened by a random amount of up to a quarter, so drives claimed together
// do not keep polling in step; the jitter never lengthens a wait past the cap
// or shortens one below the plain interval.
//
// Every progress drive paces its polls through this one function, so the
// sequential, grouped, and group_finalizer drives cannot drift apart. Each
// drive measures the wait from when its last poll began, so the time a poll
// takes is not added to the interval, and draws it once per failure, so the
// next_poll_in its failure log reports is the wait it keeps.
func (c *LocalClient) progressPollWait(consecutiveErrors int) time.Duration {
	interval := c.taskPollInterval()
	limit := interval * progressPollBackoffCapMultiple
	ceiling := interval
	for range consecutiveErrors - 1 {
		if ceiling >= limit {
			break
		}
		ceiling *= 2
	}
	ceiling = min(ceiling, limit)
	if ceiling <= interval {
		return interval
	}
	jitter := c.progressPollJitter
	if jitter == nil {
		jitter = rand.Int64N
	}
	return ceiling - time.Duration(jitter(int64(ceiling/4)+1))
}
