package tern

import (
	"math/rand/v2"
	"time"
)

// progressPollBackoffCapMultiple caps a failing drive's wait between progress
// polls at this multiple of its plain poll interval. A drive that spends its
// whole error budget at the cap still settles well inside
// storage.ApplyDriveStallAfter, and no single wait comes near
// storage.ApplyLeaseStaleAfter. The drive's lease is renewed by its own
// heartbeat goroutine, which keeps running through every wait.
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
// sequential, grouped, and group_finalizer drives cannot drift apart.
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
