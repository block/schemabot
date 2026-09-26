// Package drain bounds how long shutdown waits for background work.
//
// Every stage of SchemaBot's shutdown that waits on goroutines it started waits
// through this package, so no stage can wait forever. A wait that expires is
// not a failure to handle so much as a decision already made: the work being
// waited on is either redone by the next process to start or reclaimed from
// this one by a peer, and holding the process open past that point delays the
// recovery rather than assisting it. Callers say what they gave up in a log,
// and move on.
package drain

import (
	"sync"
	"time"
)

// Wait waits for wg, reporting whether it finished within timeout.
//
// The goroutine it starts outlives a wait that expires, by exactly as long as
// the work does: it is parked on wg.Wait() and holds nothing else, so it costs
// a stack until the last goroutine in the group returns, if one ever does.
func Wait(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	return waitUntil(done, timer.C)
}

// WaitFor waits for done to close, reporting whether it closed within timeout.
//
// It is Wait for work that signals its own completion on a channel rather than
// through a group, and it makes the same trade: a caller past the timeout
// carries on without the work, and says what it left.
func WaitFor(done <-chan struct{}, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	return waitUntil(done, timer.C)
}

// waitUntil reports whether done closed before deadline elapsed.
//
// The two can be ready together, and Go picks between two ready cases at
// random. A group that finished is finished whichever one it picked, so the
// deadline branch asks again before reporting work abandoned that is not.
func waitUntil(done <-chan struct{}, deadline <-chan time.Time) bool {
	select {
	case <-done:
		return true
	case <-deadline:
		select {
		case <-done:
			return true
		default:
			return false
		}
	}
}
