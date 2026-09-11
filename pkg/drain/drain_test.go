package drain

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A group that finishes is waited for, not timed out: shutdown must not abandon
// work that was about to return.
func TestWaitReportsWorkThatFinishes(t *testing.T) {
	var wg sync.WaitGroup
	finished := make(chan struct{})
	wg.Go(func() { <-finished })

	go func() {
		time.Sleep(10 * time.Millisecond)
		close(finished)
	}()

	start := time.Now()
	require.True(t, Wait(&wg, 30*time.Second))
	assert.Less(t, time.Since(start), 5*time.Second, "Wait returns with the work, not on the timeout")
}

// A group that does not finish is given up on at its bound, which is the whole
// point: the caller's shutdown continues instead of inheriting the stuck
// goroutine's lifetime.
func TestWaitGivesUpAtTheTimeout(t *testing.T) {
	var wg sync.WaitGroup
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	wg.Go(func() { <-release })

	const timeout = 100 * time.Millisecond
	start := time.Now()
	require.False(t, Wait(&wg, timeout))
	elapsed := time.Since(start)

	assert.GreaterOrEqual(t, elapsed, timeout)
	assert.Less(t, elapsed, 5*time.Second)
}
