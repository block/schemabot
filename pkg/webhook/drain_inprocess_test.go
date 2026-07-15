package webhook

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// drainNotDoneProbe is a short window used to assert that a drain has NOT
// returned yet while its work is still in flight. It is deliberately small: it
// only needs to give the drain goroutine a chance to return erroneously.
const drainNotDoneProbe = 50 * time.Millisecond

// A non-durable webhook path acks 200 and finishes its work in a detached
// goSafe goroutine. On shutdown the server must wait for that already-acked work
// to finish rather than dropping it, so DrainInProcessWebhookWork blocks until
// the in-flight goroutine returns.
func TestDrainInProcessWebhookWorkWaitsForGoroutines(t *testing.T) {
	h := &Handler{logger: testLogger()}

	release := make(chan struct{})
	started := make(chan struct{})
	var ran atomic.Bool
	h.goSafe("octocat/hello-world", 1, 1, func() {
		close(started)
		<-release
		ran.Store(true)
	})
	<-started

	drained := make(chan struct{})
	go func() {
		h.DrainInProcessWebhookWork(t.Context())
		close(drained)
	}()

	select {
	case <-drained:
		require.FailNow(t, "DrainInProcessWebhookWork returned before in-flight work finished")
	case <-time.After(drainNotDoneProbe):
	}

	close(release)
	select {
	case <-drained:
	case <-time.After(durableWebhookTestDeadline):
		require.FailNow(t, "DrainInProcessWebhookWork did not return after in-flight work finished")
	}
	assert.True(t, ran.Load(), "expected the in-flight goroutine to complete before drain returned")
}

// Nested goSafe work (for example issue_comment fanning out per participant)
// spawned before the drain begins must also be waited for: the child registers
// on the WaitGroup while draining is still false, so the drain sees it too.
func TestDrainInProcessWebhookWorkWaitsForNestedGoroutines(t *testing.T) {
	h := &Handler{logger: testLogger()}

	release := make(chan struct{})
	outerStarted := make(chan struct{})
	var nestedRan atomic.Bool
	h.goSafe("octocat/hello-world", 1, 1, func() {
		h.goSafe("octocat/hello-world", 1, 1, func() {
			<-release
			nestedRan.Store(true)
		})
		close(outerStarted)
	})
	<-outerStarted

	drained := make(chan struct{})
	go func() {
		h.DrainInProcessWebhookWork(t.Context())
		close(drained)
	}()

	select {
	case <-drained:
		require.FailNow(t, "DrainInProcessWebhookWork returned before nested work finished")
	case <-time.After(drainNotDoneProbe):
	}

	close(release)
	select {
	case <-drained:
	case <-time.After(durableWebhookTestDeadline):
		require.FailNow(t, "DrainInProcessWebhookWork did not return after nested work finished")
	}
	assert.True(t, nestedRan.Load(), "expected the nested goroutine to complete before drain returned")
}

// A goroutine that outlives the drain budget must not hang shutdown: the drain
// returns once its context deadline fires, leaving the straggler to be dropped
// on process exit.
func TestDrainInProcessWebhookWorkTimesOut(t *testing.T) {
	h := &Handler{logger: testLogger()}

	release := make(chan struct{})
	started := make(chan struct{})
	h.goSafe("octocat/hello-world", 1, 1, func() {
		close(started)
		<-release
	})
	<-started

	ctx, cancel := context.WithTimeout(t.Context(), drainNotDoneProbe)
	defer cancel()

	drained := make(chan struct{})
	go func() {
		h.DrainInProcessWebhookWork(ctx)
		close(drained)
	}()

	select {
	case <-drained:
	case <-time.After(durableWebhookTestDeadline):
		require.FailNow(t, "DrainInProcessWebhookWork did not return after its context deadline")
	}

	// Release the straggler and wait so the test leaves no goroutine behind.
	close(release)
	h.inProcessWebhookWg.Wait()
}

// A delayed timer (participant re-fold) can call goSafe after the drain has
// already begun. That work must run untracked — never Add to the WaitGroup once
// the drain flipped the flag — so it cannot trigger "Add called concurrently
// with Wait" misuse, and the drain must not block on it.
func TestDrainInProcessWebhookWorkGoSafeAfterDrainRunsUntracked(t *testing.T) {
	h := &Handler{logger: testLogger()}

	// Start draining with no in-flight work: the drain returns immediately.
	h.DrainInProcessWebhookWork(t.Context())

	ran := make(chan struct{})
	h.goSafe("octocat/hello-world", 1, 1, func() {
		close(ran)
	})

	select {
	case <-ran:
	case <-time.After(durableWebhookTestDeadline):
		require.FailNow(t, "expected goSafe work started after drain to still run (untracked)")
	}
}
