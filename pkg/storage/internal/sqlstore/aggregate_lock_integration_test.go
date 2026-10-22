//go:build integration

package sqlstore

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Independent publishers sharing storage serialize the complete fold. The next
// publisher reads only after its predecessor has finished publishing, and an
// error releases the session lock so the next fold can proceed.
func TestAggregatePublicationLockSerializesPublishers(t *testing.T) {
	testAggregatePublicationLock(t, NewMySQL(testDB), NewMySQL(testDB), mysqlAdvisoryLockWaiter(testDB))
}

func testAggregatePublicationLock(t *testing.T, first, second *Storage, waiting advisoryLockWaiter) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	entered := make(chan struct{})
	release := make(chan struct{})
	firstResult := make(chan error, 1)
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	go func() {
		firstResult <- first.checks.WithAggregateCheckLock(ctx, "org/repo", 42, func(ctx context.Context) error {
			close(entered)
			select {
			case <-release:
				return assert.AnError
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		require.FailNow(t, "first fold did not acquire lock")
	}
	secondEntered := make(chan struct{})
	secondResult := make(chan error, 1)
	go func() {
		secondResult <- second.checks.WithAggregateCheckLock(ctx, "org/repo", 42, func(context.Context) error { close(secondEntered); return nil })
	}()
	require.Eventually(t, func() bool { return waiting(t) }, time.Second, 10*time.Millisecond)
	select {
	case <-secondEntered:
		require.FailNow(t, "second fold overlapped the first")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-firstResult:
		require.ErrorIs(t, err, assert.AnError)
	case <-ctx.Done():
		require.FailNow(t, "first fold did not finish")
	}
	select {
	case err := <-secondResult:
		require.NoError(t, err)
	case <-ctx.Done():
		require.FailNow(t, "second fold did not finish")
	}
}
