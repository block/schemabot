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

	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}
