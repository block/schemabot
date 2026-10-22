//go:build integration

package sqlstore

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/checkstate"
	"github.com/block/schemabot/pkg/storage"

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

// Each publisher can read and write while holding the only available pool
// connection. Filling a larger pool with independent PR folds also converges.
func TestAggregatePublicationBoundedPool(t *testing.T) {
	testAggregatePublicationBoundedPool(t, NewMySQL(testDB))
}

func testAggregatePublicationBoundedPool(t *testing.T, store *Storage) {
	t.Helper()
	pool := store.db.pool
	oldLimit := pool.Stats().MaxOpenConnections
	defer pool.SetMaxOpenConns(oldLimit)
	for _, capacity := range []int{1, 2} {
		t.Run(fmt.Sprintf("connections_%d", capacity), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			pool.SetMaxOpenConns(capacity)
			entered := make(chan struct{}, capacity)
			release := make(chan struct{})
			results := make(chan error, capacity)
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			for pr := 1; pr <= capacity; pr++ {
				go func() {
					results <- store.checks.WithAggregateCheckLock(ctx, "org/bounded-pool", pr, func(ctx context.Context) error {
						entered <- struct{}{}
						select {
						case <-release:
						case <-ctx.Done():
							return ctx.Err()
						}
						check := &storage.Check{Repository: "org/bounded-pool", PullRequest: pr, HeadSHA: "head", Environment: "production", DatabaseType: "mysql", DatabaseName: "orders", Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess}
						if err := store.checks.Upsert(ctx, check); err != nil {
							return err
						}
						// Duplicate result takes the transaction and row-lock path.
						if _, err := store.checks.UpsertPlanResult(ctx, check, storage.PlanDriftClean); err != nil {
							return err
						}
						if _, err := store.checks.GetByPR(ctx, check.Repository, pr); err != nil {
							return err
						}
						if _, err := store.checks.Get(ctx, check.Repository, pr, check.Environment, check.DatabaseType, check.DatabaseName); err != nil {
							return err
						}
						_, err := store.applies.GetByPR(ctx, check.Repository, pr)
						return err
					})
				}()
			}
			for range capacity {
				select {
				case <-entered:
				case err := <-results:
					require.NoError(t, err)
					t.Fatal("fold returned before entering")
				case <-ctx.Done():
					t.Fatal("folds did not acquire pool connections")
				}
			}
			releaseOnce.Do(func() { close(release) })
			for range capacity {
				select {
				case err := <-results:
					require.NoError(t, err)
				case <-ctx.Done():
					t.Fatal("folds exhausted their own connection pool")
				}
			}
			assert.Equal(t, 0, pool.Stats().InUse, "all pinned sessions are released")
		})
	}
}
