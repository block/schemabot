//go:build integration

package sqlstore

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/namedlock"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// The behavioral suite for ControlRequestStore lives in
// pkg/storage/storagetest and runs against every dialect via parity_test.go.
// The test here additionally pins a physical invariant the storage interface
// cannot express: the unique key converges concurrent requests on a single
// row, verified with a raw row count.

// Concurrent operator requests for the same apply operation should converge on
// one durable pending row so retries and double-clicks do not create extra work.
func TestControlRequestStore_RequestPendingConcurrentFirstRequests(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	applyID := createControlRequestTestApply(t, store, "apply_control_request_concurrent")
	const requestCount = 8
	type requestResult struct {
		req            *storage.ApplyControlRequest
		alreadyPending bool
		err            error
	}
	start := make(chan struct{})
	results := make(chan requestResult, requestCount)
	var wg sync.WaitGroup
	for range requestCount {
		wg.Go(func() {
			<-start
			req, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
				ApplyID:     applyID,
				Operation:   storage.ControlOperationStart,
				Status:      storage.ControlRequestPending,
				RequestedBy: "operator",
				Metadata:    []byte(`{"started_count":1}`),
			})
			results <- requestResult{req: req, alreadyPending: alreadyPending, err: err}
		})
	}
	close(start)
	wg.Wait()
	close(results)

	var requestID int64
	var createdCount int
	var alreadyPendingCount int
	for result := range results {
		require.NoError(t, result.err)
		require.NotNil(t, result.req)
		assert.Equal(t, storage.ControlRequestPending, result.req.Status)
		if requestID == 0 {
			requestID = result.req.ID
		}
		assert.Equal(t, requestID, result.req.ID)
		if result.alreadyPending {
			alreadyPendingCount++
		} else {
			createdCount++
		}
	}
	assert.Equal(t, 1, createdCount)
	assert.Equal(t, requestCount-1, alreadyPendingCount)

	var rowCount int
	require.NoError(t, store.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM apply_control_requests
		WHERE apply_id = ? AND operation = ?
	`, applyID, storage.ControlOperationStart).Scan(&rowCount))
	assert.Equal(t, 1, rowCount)
}

func createControlRequestTestApply(t *testing.T, store *Storage, applyIdentifier string) int64 {
	t.Helper()
	lock := createTestLock(t, store, "testdb", "mysql")
	applyID, err := store.Applies().Create(t.Context(), &storage.Apply{
		ApplyIdentifier: applyIdentifier,
		LockID:          lock.ID,
		PlanID:          801,
		Database:        "testdb",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "staging",
		Engine:          "spirit",
		State:           state.Apply.Stopped,
		Options:         []byte(`{}`),
	})
	require.NoError(t, err)
	return applyID
}

// requestPendingControl records a pending control request of the given
// operation for an apply, as an operator's command would.
func requestPendingControl(t *testing.T, store *Storage, applyID int64, op storage.ControlOperation) *storage.ApplyControlRequest {
	t.Helper()
	req, alreadyPending, err := store.ControlRequests().RequestPending(t.Context(), &storage.ApplyControlRequest{
		ApplyID:     applyID,
		Operation:   op,
		RequestedBy: "operator",
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)
	return req
}

// A pending request is stranded only once its apply has settled: a drive of a
// resumable apply — stopped, failed_retryable, or still running — is the one
// that answers it, and a stopped apply's pending start is the request that
// resumes it.
func TestControlRequestStore_ListStrandedPending_RequiresASettledApply(t *testing.T) {
	tests := []struct {
		name       string
		applyState string
		stranded   bool
	}{
		{"completed apply strands its start", state.Apply.Completed, true},
		{"failed apply strands its start", state.Apply.Failed, true},
		{"cancelled apply strands its start", state.Apply.Cancelled, true},
		{"reverted apply strands its start", state.Apply.Reverted, true},
		{"stopped apply keeps its start for the resume", state.Apply.Stopped, false},
		{"failed_retryable apply keeps its start for the retry", state.Apply.FailedRetryable, false},
		{"running apply keeps its start for its driver", state.Apply.Running, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearTables(t)
			ctx := t.Context()
			store := NewMySQL(testDB)

			lock := createTestLock(t, store, "stranded_control_db", "mysql")
			apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_stranded_control_"+tt.applyState, 1, tt.applyState, "staging")
			backdateApplyUpdatedAt(t, apply.ID, strandedParentQuiescence+time.Minute)
			req := requestPendingControl(t, store, apply.ID, storage.ControlOperationStart)

			listed, err := store.controlRequests.listStrandedPending(ctx, 10)
			require.NoError(t, err)

			if !tt.stranded {
				assert.Empty(t, listed, "a %s apply has not settled, so its pending request is not stranded", tt.applyState)
				return
			}
			require.Len(t, listed, 1)
			assert.Equal(t, req.ID, listed[0].Request.ID)
			assert.Equal(t, storage.ControlRequestPending, listed[0].Request.Status, "the listing reads; settlement is the caller's")
			assert.Equal(t, apply.ApplyIdentifier, listed[0].Apply.ApplyIdentifier, "the apply travels with the request so the caller can settle against its outcome")
			assert.Equal(t, tt.applyState, listed[0].Apply.State)
		})
	}
}

// An apply that settled moments ago may still have its drive running the
// post-drive settlement that answers the request, so the listing waits for the
// apply to go quiescent before calling the request stranded.
func TestControlRequestStore_ListStrandedPending_WaitsForApplyQuiescence(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "stranded_control_fresh_db", "mysql")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_stranded_control_fresh", 1, state.Apply.Completed, "staging")
	req := requestPendingControl(t, store, apply.ID, storage.ControlOperationStart)

	listed, err := store.controlRequests.listStrandedPending(ctx, 10)
	require.NoError(t, err)
	assert.Empty(t, listed, "an apply that settled just now is not yet quiescent")

	backdateApplyUpdatedAt(t, apply.ID, strandedParentQuiescence+time.Minute)

	listed, err = store.controlRequests.listStrandedPending(ctx, 10)
	require.NoError(t, err)
	require.Len(t, listed, 1, "once the apply is quiescent the request is stranded")
	assert.Equal(t, req.ID, listed[0].Request.ID)
}

// Under a fan-out rollout a sibling drive holding only an operation lease is
// still a drive of the apply, and its own post-drive settlement answers the
// request. The listing defers to that lease until it goes stale by the claim
// path's reckoning, the same test a driver applies before taking the operation
// from a peer.
func TestControlRequestStore_ListStrandedPending_DefersToAFreshOperationLease(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "stranded_control_leased_db", "mysql")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_stranded_control_leased", 1, state.Apply.Failed, "staging")
	backdateApplyUpdatedAt(t, apply.ID, strandedParentQuiescence+time.Minute)
	op := insertPendingOperation(t, store, apply.ID, "region-b")
	leaseApplyOperation(t, op.ID, "host/1/driver-0")
	req := requestPendingControl(t, store, apply.ID, storage.ControlOperationCancel)

	listed, err := store.controlRequests.listStrandedPending(ctx, 10)
	require.NoError(t, err)
	assert.Empty(t, listed, "a sibling drive holds an operation of this apply, so the request is that drive's to answer")

	_, err = testDB.ExecContext(ctx,
		"UPDATE apply_operations SET updated_at = NOW() - INTERVAL ? SECOND WHERE id = ?",
		int64((storage.ApplyLeaseStaleAfter + time.Minute).Seconds()), op.ID)
	require.NoError(t, err)

	listed, err = store.controlRequests.listStrandedPending(ctx, 10)
	require.NoError(t, err)
	require.Len(t, listed, 1, "a lease a peer could reclaim no longer speaks for the apply")
	assert.Equal(t, req.ID, listed[0].Request.ID)
}

// The listing is a bounded maintenance read: at most the requested number of
// rows per call, oldest first, so a backlog drains across passes.
func TestControlRequestStore_ListStrandedPending_RespectsLimit(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "stranded_control_limit_db", "mysql")
	first := createTestApplyWithStateAndEnv(t, store, lock, "apply_stranded_control_1", 1, state.Apply.Completed, "staging")
	second := createTestApplyWithStateAndEnv(t, store, lock, "apply_stranded_control_2", 2, state.Apply.Completed, "staging")
	backdateApplyUpdatedAt(t, first.ID, strandedParentQuiescence+time.Minute)
	backdateApplyUpdatedAt(t, second.ID, strandedParentQuiescence+time.Minute)
	oldest := requestPendingControl(t, store, first.ID, storage.ControlOperationStart)
	requestPendingControl(t, store, second.ID, storage.ControlOperationStart)

	listed, err := store.controlRequests.listStrandedPending(ctx, 1)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	assert.Equal(t, oldest.ID, listed[0].Request.ID, "the oldest stranded request is listed first")

	_, err = store.controlRequests.listStrandedPending(ctx, 0)
	require.Error(t, err, "a non-positive limit is a caller bug, not an unbounded scan")
}

// One instance lists per pass: a second instance finding the lock held steps
// aside rather than re-scanning, and the lock is released after each pass.
func TestControlRequestStore_ListStrandedPending_ElectsOneReaperPerPass(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "stranded_control_reaper_db", "mysql")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_stranded_control_reaper", 1, state.Apply.Completed, "staging")
	backdateApplyUpdatedAt(t, apply.ID, strandedParentQuiescence+time.Minute)
	requestPendingControl(t, store, apply.ID, storage.ControlOperationStart)

	other := NewMySQL(testDB)
	held, err := other.controlRequests.db.Conn(ctx)
	require.NoError(t, err)
	acquired, err := namedlock.MySQL{}.Acquire(ctx, held.lockerConn(), strandedControlRequestReaperLockName, 0)
	require.NoError(t, err)
	require.True(t, acquired, "the stand-in instance should take the reaper lock")

	_, err = store.ControlRequests().ListStrandedPending(ctx, 10)
	require.ErrorIs(t, err, storage.ErrStrandedControlRequestReaperBusy)

	released, err := namedlock.MySQL{}.Release(ctx, held.lockerConn(), strandedControlRequestReaperLockName)
	require.NoError(t, err)
	require.True(t, released)
	require.NoError(t, held.Close())

	listed, err := store.ControlRequests().ListStrandedPending(ctx, 10)
	require.NoError(t, err)
	require.Len(t, listed, 1)

	listed, err = store.ControlRequests().ListStrandedPending(ctx, 10)
	require.NoError(t, err)
	require.Len(t, listed, 1, "the reaper lock is released after each pass")
}
