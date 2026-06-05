package github

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckStatusSingleflight_CollapsesConcurrentFetches(t *testing.T) {
	c := NewCheckStatusSingleflight()

	const concurrency = 25
	var calls atomic.Int32
	release := make(chan struct{})
	fetch := func(_ context.Context) ([]CheckStatusRow, error) {
		calls.Add(1)
		<-release // hold open until all goroutines have joined the flight
		return []CheckStatusRow{{Name: "ci/lint"}}, nil
	}

	var wg sync.WaitGroup
	results := make([][]CheckStatusRow, concurrency)
	errs := make([]error, concurrency)
	for i := range concurrency {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := c.Do(t.Context(), "octo/repo", "abc", fetch)
			results[i] = res
			errs[i] = err
		}(i)
	}

	// Give all goroutines time to enqueue on the singleflight group, then
	// release the in-flight fetch.
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	assert.Equal(t, int32(1), calls.Load(), "all concurrent callers should collapse to one fetch")
	for i := range concurrency {
		require.NoError(t, errs[i])
		assert.Equal(t, []CheckStatusRow{{Name: "ci/lint"}}, results[i])
	}
}

// TestCheckStatusSingleflight_SequentialCallsRefetch locks in the
// invariant that the coalescer never memoises across calls: once a fetch
// completes, the next call issues a fresh request. This is the property
// that distinguishes singleflight-only from a TTL cache and what makes
// it safe against stale-success for mutable check state.
func TestCheckStatusSingleflight_SequentialCallsRefetch(t *testing.T) {
	c := NewCheckStatusSingleflight()

	var calls atomic.Int32
	fetch := func(_ context.Context) ([]CheckStatusRow, error) {
		calls.Add(1)
		return []CheckStatusRow{{Name: "ci/lint"}}, nil
	}

	for range 3 {
		_, err := c.Do(t.Context(), "octo/repo", "abc", fetch)
		require.NoError(t, err)
	}
	assert.Equal(t, int32(3), calls.Load(), "sequential calls must each fetch fresh — no memoisation across calls")
}

// TestCheckStatusSingleflight_WaiterRespectsItsOwnContext locks in the
// invariant that a caller waiting on another caller's in-flight fetch
// returns promptly when its own ctx is cancelled, rather than blocking
// until the shared fetch completes. The shared fetch is not aborted —
// the first caller still receives its result.
func TestCheckStatusSingleflight_WaiterRespectsItsOwnContext(t *testing.T) {
	c := NewCheckStatusSingleflight()

	var calls atomic.Int32
	fetchEntered := make(chan struct{})
	releaseFetch := make(chan struct{})
	fetch := func(_ context.Context) ([]CheckStatusRow, error) {
		calls.Add(1)
		close(fetchEntered)
		<-releaseFetch
		return []CheckStatusRow{{Name: "ci/lint"}}, nil
	}

	// First caller: long-lived ctx, owns the in-flight fetch.
	firstDone := make(chan struct{})
	var firstResult []CheckStatusRow
	var firstErr error
	go func() {
		defer close(firstDone)
		firstResult, firstErr = c.Do(t.Context(), "octo/repo", "abc", fetch)
	}()

	// Wait until the fetch is actually in flight before launching the
	// second caller, so we know it will join the singleflight group as a
	// waiter rather than running the fetch itself.
	<-fetchEntered

	// Second caller: its own cancellable ctx. Cancel before the fetch
	// completes and assert the caller returns promptly with ctx.Err().
	secondCtx, secondCancel := context.WithCancel(t.Context())
	secondDone := make(chan struct{})
	var secondErr error
	go func() {
		defer close(secondDone)
		_, secondErr = c.Do(secondCtx, "octo/repo", "abc", fetch)
	}()
	secondCancel()

	select {
	case <-secondDone:
	case <-time.After(2 * time.Second):
		t.Fatal("second caller did not return promptly after its ctx was cancelled — likely still blocked on the shared singleflight fetch")
	}
	assert.ErrorIs(t, secondErr, context.Canceled, "second caller should return its own ctx.Err()")

	// First caller must still get the shared fetch's result — its ctx is
	// alive and the second caller's cancellation must not have aborted
	// the shared fetch.
	close(releaseFetch)
	<-firstDone
	require.NoError(t, firstErr)
	assert.Equal(t, []CheckStatusRow{{Name: "ci/lint"}}, firstResult)
	assert.Equal(t, int32(1), calls.Load(), "fetch should still be invoked exactly once")
}

// TestCheckStatusSingleflight_LeaderCancellationDoesNotFailWaiters locks
// in the invariant that the singleflight shared fetch is decoupled from
// any individual caller's ctx: the caller that wins the singleflight may
// cancel or time out without aborting the shared GitHub request and
// without failing unrelated waiters whose own contexts are still valid.
func TestCheckStatusSingleflight_LeaderCancellationDoesNotFailWaiters(t *testing.T) {
	c := NewCheckStatusSingleflight()

	fetchEntered := make(chan struct{})
	releaseFetch := make(chan struct{})
	var fetchCtxCancelled atomic.Bool
	var calls atomic.Int32
	fetch := func(fetchCtx context.Context) ([]CheckStatusRow, error) {
		calls.Add(1)
		close(fetchEntered)
		// If the coalescer were still passing the leader's ctx straight
		// to fetch, fetchCtx would be cancelled when the leader cancels
		// below. Observe its state after the cancellation point to
		// confirm the coalescer is feeding us a decoupled ctx.
		<-releaseFetch
		if fetchCtx.Err() != nil {
			fetchCtxCancelled.Store(true)
		}
		return []CheckStatusRow{{Name: "ci/lint", Status: "completed", Conclusion: "success"}}, nil
	}

	// Leader: cancellable ctx. Wins the singleflight, then cancels
	// before the shared fetch completes.
	leaderCtx, leaderCancel := context.WithCancel(t.Context())
	leaderDone := make(chan struct{})
	var leaderResult []CheckStatusRow
	var leaderErr error
	go func() {
		defer close(leaderDone)
		leaderResult, leaderErr = c.Do(leaderCtx, "octo/repo", "abc", fetch)
	}()

	// Wait until the fetch is actually in flight before starting the
	// waiter (so we are sure the waiter joins the singleflight group as
	// a follower, not as the leader).
	<-fetchEntered

	// Waiter: a separate long-lived ctx. Must observe the shared fetch's
	// result even though the leader is about to cancel.
	waiterDone := make(chan struct{})
	var waiterResult []CheckStatusRow
	var waiterErr error
	go func() {
		defer close(waiterDone)
		waiterResult, waiterErr = c.Do(t.Context(), "octo/repo", "abc", fetch)
	}()

	// Ensure the waiter has actually entered DoChan and joined the
	// singleflight group as a follower before we release the fetch.
	// Without this the goroutine may not be scheduled in time: the
	// leader's cancellation chain (cancel → leaderDone → releaseFetch)
	// can complete, the singleflight key gets deleted, and a
	// late-scheduled waiter would then start a brand new invocation —
	// calling fetch a second time and panicking on the already-closed
	// fetchEntered channel.
	time.Sleep(50 * time.Millisecond)

	// Cancel the leader before the shared fetch completes. With a
	// decoupled fetchCtx the shared fetch is unaffected; the leader
	// itself returns ctx.Canceled via its outer select.
	leaderCancel()

	<-leaderDone
	assert.ErrorIs(t, leaderErr, context.Canceled, "leader should observe its own cancellation")
	assert.Nil(t, leaderResult)

	// Release the shared fetch. The waiter must receive the result
	// despite the leader having cancelled.
	close(releaseFetch)

	select {
	case <-waiterDone:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not receive a result — likely failed by the leader's cancellation")
	}
	require.NoError(t, waiterErr, "waiter must not be affected by the leader's cancellation")
	assert.Equal(t, []CheckStatusRow{{Name: "ci/lint", Status: "completed", Conclusion: "success"}}, waiterResult)
	assert.False(t, fetchCtxCancelled.Load(),
		"shared fetch's ctx must remain alive after the leader cancels — otherwise the GitHub request would have aborted")
	assert.Equal(t, int32(1), calls.Load(), "fetch should run exactly once across leader + waiter")
}

// TestGetPRCheckStatuses_RecomputesIsSchemaBotPerWaiter locks in the
// invariant that IsSchemaBot is derived from each calling
// InstallationClient's appSlug snapshot at projection time, never baked
// into the shared singleflight result at fetch time. Without this, two
// InstallationClients that share the same singleflight fetch (e.g.
// concurrent webhook deliveries within the same coalesced flight, one
// constructed before slug recovery and one after) would receive
// identical IsSchemaBot bits — spuriously blocking applies on the bot's
// own checks for the pre-recovery client, or spuriously trusting them
// for the post-recovery one.
func TestGetPRCheckStatuses_RecomputesIsSchemaBotPerWaiter(t *testing.T) {
	sf := NewCheckStatusSingleflight()

	// Two InstallationClients share the same singleflight but have
	// different appSlug snapshots: one was constructed before slug
	// recovery, one after.
	preRecovery := &InstallationClient{checkStatusSingleflight: sf}
	preRecovery.storeAppSlug("")
	postRecovery := &InstallationClient{checkStatusSingleflight: sf}
	postRecovery.storeAppSlug("schemabot")

	// Intercept the per-client fetch via a stub so the test can orchestrate
	// when the shared fetch returns without exercising the real GitHub API.
	const repo, sha = "octo/repo", "abc123"
	sharedRows := []CheckStatusRow{
		{Name: "schemabot/apply staging", Status: "completed", Conclusion: "success", AppSlug: "schemabot"},
		{Name: "ci/lint", Status: "completed", Conclusion: "failure", AppSlug: "other-ci"},
	}
	fetchEntered := make(chan struct{})
	releaseFetch := make(chan struct{})
	var calls atomic.Int32
	fetch := func(_ context.Context) ([]CheckStatusRow, error) {
		calls.Add(1)
		close(fetchEntered)
		<-releaseFetch
		return sharedRows, nil
	}

	// Launch both clients concurrently against the same singleflight
	// key so they share a single fetch invocation. Each client projects
	// the shared rows to PRCheckStatus independently against its own
	// appSlug.
	type result struct {
		statuses []PRCheckStatus
		err      error
	}
	preDone := make(chan result, 1)
	postDone := make(chan result, 1)
	go func() {
		rows, err := sf.Do(t.Context(), repo, sha, fetch)
		if err != nil {
			preDone <- result{err: err}
			return
		}
		preDone <- result{statuses: projectRowsForTest(preRecovery, rows)}
	}()

	<-fetchEntered // ensure pre-recovery wins the flight

	go func() {
		rows, err := sf.Do(t.Context(), repo, sha, fetch)
		if err != nil {
			postDone <- result{err: err}
			return
		}
		postDone <- result{statuses: projectRowsForTest(postRecovery, rows)}
	}()

	// Tiny pause so the second goroutine has time to enqueue as a
	// waiter on the singleflight group before we release the fetch.
	time.Sleep(50 * time.Millisecond)
	close(releaseFetch)

	pre := <-preDone
	post := <-postDone
	require.NoError(t, pre.err)
	require.NoError(t, post.err)

	assert.Equal(t, int32(1), calls.Load(), "shared fetch should run exactly once across both waiters")

	for _, s := range pre.statuses {
		assert.False(t, s.IsSchemaBot, "pre-recovery client must not classify any row as own check (slug not known) — got %+v", s)
	}
	require.Len(t, post.statuses, 2)
	assert.True(t, post.statuses[0].IsSchemaBot, "post-recovery client must classify the bot's own check as IsSchemaBot=true via its own appSlug")
	assert.False(t, post.statuses[1].IsSchemaBot, "third-party check must remain IsSchemaBot=false")
}

// projectRowsForTest mirrors the projection step inside
// GetPRCheckStatuses so the per-waiter classification test can verify the
// IsSchemaBot invariant without spinning up a real GitHub API.
func projectRowsForTest(ic *InstallationClient, rows []CheckStatusRow) []PRCheckStatus {
	out := make([]PRCheckStatus, len(rows))
	for i, r := range rows {
		out[i] = PRCheckStatus{
			Name:        r.Name,
			Status:      r.Status,
			Conclusion:  r.Conclusion,
			IsSchemaBot: ic.isOwnAppSlug(r.AppSlug),
		}
	}
	return out
}
