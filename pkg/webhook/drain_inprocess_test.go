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
// spawned before the drain begins must also be waited for: the child is counted
// while draining is still false, so the drain sees it too.
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

	// Release the straggler and drain again so the test leaves no goroutine
	// behind: the second drain returns once the count drops to zero.
	close(release)
	h.DrainInProcessWebhookWork(t.Context())
}

// A delayed timer (participant re-fold) can call goSafe after the drain has
// already reached empty. With no tracked work in flight (count zero) that work
// runs untracked so late timers cannot keep the drain alive forever, and the
// drain must not block on it.
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

// Tracked work that spawns a child goSafe *after* the drain has begun — but
// while the parent is still in flight (count nonzero) — must have that child
// drained too. The drain is a promise over the whole acked work chain, so a
// child spawned mid-drain (for example handleMultiEnvPlan acknowledging its
// command, which fans out an eyes-reaction goSafe) cannot be dropped while the
// drain reports success.
func TestDrainInProcessWebhookWorkWaitsForChildSpawnedDuringDrain(t *testing.T) {
	h := &Handler{logger: testLogger()}

	parentStarted := make(chan struct{})
	spawnChild := make(chan struct{})
	releaseChild := make(chan struct{})
	var childRan atomic.Bool

	// The parent stays in flight until spawnChild fires, then registers a child
	// and returns. The child outlives the parent and only finishes on release.
	h.goSafe("octocat/hello-world", 1, 1, func() {
		close(parentStarted)
		<-spawnChild
		h.goSafe("octocat/hello-world", 1, 1, func() {
			<-releaseChild
			childRan.Store(true)
		})
	})
	<-parentStarted

	// Begin draining while only the parent is counted.
	drained := make(chan struct{})
	go func() {
		h.DrainInProcessWebhookWork(t.Context())
		close(drained)
	}()

	// Spawn the child mid-drain (parent still counted, so count is nonzero and
	// the child registers rather than running untracked).
	close(spawnChild)

	select {
	case <-drained:
		require.FailNow(t, "DrainInProcessWebhookWork returned before the mid-drain child finished")
	case <-time.After(drainNotDoneProbe):
	}

	close(releaseChild)
	select {
	case <-drained:
	case <-time.After(durableWebhookTestDeadline):
		require.FailNow(t, "DrainInProcessWebhookWork did not return after the mid-drain child finished")
	}
	assert.True(t, childRan.Load(), "expected the mid-drain child goroutine to complete before drain returned")
}
