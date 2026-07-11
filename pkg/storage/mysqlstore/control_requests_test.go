//go:build integration

package mysqlstore

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func TestControlRequestStore_RequestPendingReturnsExistingPending(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	applyID := createControlRequestTestApply(t, store, "apply_control_request_pending")
	first, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     applyID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator",
		Metadata:    []byte(`{"started_count":1}`),
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)

	second, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     applyID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator",
		Metadata:    []byte(`{"started_count":2}`),
	})
	require.NoError(t, err)
	require.True(t, alreadyPending)

	assert.Equal(t, first.ID, second.ID)
	assert.JSONEq(t, string(first.Metadata), string(second.Metadata))
}

// Concurrent operator requests for the same apply operation should converge on
// one durable pending row so retries and double-clicks do not create extra work.
func TestControlRequestStore_RequestPendingConcurrentFirstRequests(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

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

func TestControlRequestStore_RequestPendingResetsCompletedRequest(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	applyID := createControlRequestTestApply(t, store, "apply_control_request_restart")
	first, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     applyID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator-a",
		Metadata:    []byte(`{"started_count":1}`),
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)

	require.NoError(t, store.ControlRequests().CompletePending(ctx, applyID, storage.ControlOperationStart))

	second, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     applyID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator-b",
		Metadata:    []byte(`{"started_count":2}`),
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)

	assert.Equal(t, first.ID, second.ID)
	assert.Equal(t, storage.ControlRequestPending, second.Status)
	assert.Equal(t, "operator-b", second.RequestedBy)
	assert.Nil(t, second.CompletedAt)
	assert.JSONEq(t, `{"started_count":2}`, string(second.Metadata))
}

func TestControlRequestStore_CompletePending(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	applyID := createControlRequestTestApply(t, store, "apply_control_request_complete")
	created, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:   applyID,
		Operation: storage.ControlOperationStart,
		Status:    storage.ControlRequestPending,
		Metadata:  []byte(`{}`),
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)

	pending, err := store.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationStart)
	require.NoError(t, err)
	require.NotNil(t, pending)
	assert.Equal(t, created.ID, pending.ID)

	require.NoError(t, store.ControlRequests().CompletePending(ctx, applyID, storage.ControlOperationStart))

	pending, err = store.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationStart)
	require.NoError(t, err)
	assert.Nil(t, pending)

	completed := getControlRequestByID(t, store, created.ID)
	require.NotNil(t, completed)
	assert.Equal(t, storage.ControlRequestCompleted, completed.Status)
	assert.NotNil(t, completed.CompletedAt)
}

func TestControlRequestStore_FailPending(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	applyID := createControlRequestTestApply(t, store, "apply_control_request_fail")
	created, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:   applyID,
		Operation: storage.ControlOperationStart,
		Status:    storage.ControlRequestPending,
		Metadata:  []byte(`{}`),
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)

	require.NoError(t, store.ControlRequests().FailPending(ctx, applyID, storage.ControlOperationStart, "remote start failed"))

	pending, err := store.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationStart)
	require.NoError(t, err)
	assert.Nil(t, pending)

	failed := getControlRequestByID(t, store, created.ID)
	require.NotNil(t, failed)
	assert.Equal(t, storage.ControlRequestFailed, failed.Status)
	assert.Equal(t, "remote start failed", failed.ErrorMessage)
	assert.NotNil(t, failed.CompletedAt)
}

func TestControlRequestStore_LeaseGuardsPendingResolution(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	applyID := createControlRequestTestApply(t, store, "apply_control_request_lease")
	_, err := testDB.ExecContext(ctx, `
		UPDATE applies
		SET lease_owner = ?, lease_token = ?, lease_acquired_at = NOW()
		WHERE id = ?
	`, "current-driver", "current-token", applyID)
	require.NoError(t, err)

	created, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:   applyID,
		Operation: storage.ControlOperationStart,
		Status:    storage.ControlRequestPending,
		Metadata:  []byte(`{}`),
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)

	staleCtx := storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: applyID, Owner: "old-driver", Token: "stale-token"})
	require.ErrorIs(t, store.ControlRequests().CompletePending(staleCtx, applyID, storage.ControlOperationStart), storage.ErrApplyLeaseLost)
	require.ErrorIs(t, store.ControlRequests().FailPending(staleCtx, applyID, storage.ControlOperationStart, "stale failure"), storage.ErrApplyLeaseLost)

	pending, err := store.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationStart)
	require.NoError(t, err)
	require.NotNil(t, pending)
	assert.Equal(t, created.ID, pending.ID)

	currentCtx := storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: applyID, Owner: "current-driver", Token: "current-token"})
	require.NoError(t, store.ControlRequests().CompletePending(currentCtx, applyID, storage.ControlOperationStart))
	completed := getControlRequestByID(t, store, created.ID)
	require.NotNil(t, completed)
	assert.Equal(t, storage.ControlRequestCompleted, completed.Status)
}

// A release is a one-way latch: requesting it twice (an operator double-click or
// a retried release call) converges on the single durable row rather than
// creating extra control work.
func TestControlRequestStore_RequestPendingReleaseLatchIdempotent(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	applyID := createControlRequestTestApply(t, store, "apply_control_request_release")
	first, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     applyID,
		Operation:   storage.ControlOperationRelease,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator-a",
		Metadata:    []byte(`{}`),
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)

	second, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     applyID,
		Operation:   storage.ControlOperationRelease,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator-b",
		Metadata:    []byte(`{}`),
	})
	require.NoError(t, err)
	require.True(t, alreadyPending)
	assert.Equal(t, first.ID, second.ID)
}

// GetByOperation reads the latch regardless of status, so callers can observe a
// completed (or failed) release that GetPending hides. The release latch holds a
// paused rollout open while pending or completed; a failed release does not
// latch (fail-closed), matching ApplyControlRequest.ReleasesPausedRollout.
func TestControlRequestStore_GetByOperationReturnsRegardlessOfStatus(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	applyID := createControlRequestTestApply(t, store, "apply_control_request_get_by_op")

	none, err := store.ControlRequests().GetByOperation(ctx, applyID, storage.ControlOperationRelease)
	require.NoError(t, err)
	assert.Nil(t, none)

	created, _, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:   applyID,
		Operation: storage.ControlOperationRelease,
		Status:    storage.ControlRequestPending,
		Metadata:  []byte(`{}`),
	})
	require.NoError(t, err)

	pending, err := store.ControlRequests().GetByOperation(ctx, applyID, storage.ControlOperationRelease)
	require.NoError(t, err)
	require.NotNil(t, pending)
	assert.Equal(t, created.ID, pending.ID)
	assert.Equal(t, storage.ControlRequestPending, pending.Status)
	assert.True(t, pending.ReleasesPausedRollout())

	require.NoError(t, store.ControlRequests().CompletePending(ctx, applyID, storage.ControlOperationRelease))

	gone, err := store.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationRelease)
	require.NoError(t, err)
	assert.Nil(t, gone)

	completed, err := store.ControlRequests().GetByOperation(ctx, applyID, storage.ControlOperationRelease)
	require.NoError(t, err)
	require.NotNil(t, completed)
	assert.Equal(t, created.ID, completed.ID)
	assert.Equal(t, storage.ControlRequestCompleted, completed.Status)
	assert.True(t, completed.ReleasesPausedRollout())
}

// A failed release does not latch the rollout open: GetByOperation still returns
// the row for audit, but ReleasesPausedRollout reports false so the rollout
// stays paused until a fresh release succeeds.
func TestControlRequestStore_GetByOperationFailedReleaseDoesNotLatch(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	applyID := createControlRequestTestApply(t, store, "apply_control_request_failed_release")
	_, _, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:   applyID,
		Operation: storage.ControlOperationRelease,
		Status:    storage.ControlRequestPending,
		Metadata:  []byte(`{}`),
	})
	require.NoError(t, err)
	require.NoError(t, store.ControlRequests().FailPending(ctx, applyID, storage.ControlOperationRelease, "remote release failed"))

	failed, err := store.ControlRequests().GetByOperation(ctx, applyID, storage.ControlOperationRelease)
	require.NoError(t, err)
	require.NotNil(t, failed)
	assert.Equal(t, storage.ControlRequestFailed, failed.Status)
	assert.Equal(t, "remote release failed", failed.ErrorMessage)
	assert.False(t, failed.ReleasesPausedRollout())
}

func getControlRequestByID(t *testing.T, store *Storage, id int64) *storage.ApplyControlRequest {
	t.Helper()
	row := store.db.QueryRowContext(t.Context(), `
		SELECT `+controlRequestColumns+`
		FROM apply_control_requests
		WHERE id = ?
	`, id)
	req, err := scanControlRequest(row)
	require.NoError(t, err)
	return req
}

func createControlRequestTestApply(t *testing.T, store *Storage, applyIdentifier string) int64 {
	t.Helper()
	lock := createTestLock(t, store, "testdb", "mysql", "staging")
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

// A volume control request carries its desired level in the row's metadata so
// the driving instance can retune the engine from storage alone. The level
// must survive the full request/pending/complete lifecycle, and re-requesting
// after resolution must reset the row with the new level — a completed
// adjustment never pins the payload of a later one.
func TestControlRequestStore_VolumeRequestLifecycleRoundTripsLevel(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	applyID := createControlRequestTestApply(t, store, "apply_control_request_volume")
	firstMetadata, err := storage.EncodeVolumeControlRequestMetadata(3)
	require.NoError(t, err)
	first, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     applyID,
		Operation:   storage.ControlOperationVolume,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator-a",
		Metadata:    firstMetadata,
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)

	pending, err := store.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationVolume)
	require.NoError(t, err)
	require.NotNil(t, pending)
	level, err := storage.DecodeVolumeControlRequestMetadata(pending.Metadata)
	require.NoError(t, err)
	assert.Equal(t, int32(3), level)

	require.NoError(t, store.ControlRequests().CompletePending(ctx, applyID, storage.ControlOperationVolume))
	pending, err = store.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationVolume)
	require.NoError(t, err)
	assert.Nil(t, pending)
	completed := getControlRequestByID(t, store, first.ID)
	require.NotNil(t, completed)
	assert.Equal(t, storage.ControlRequestCompleted, completed.Status)

	secondMetadata, err := storage.EncodeVolumeControlRequestMetadata(9)
	require.NoError(t, err)
	second, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     applyID,
		Operation:   storage.ControlOperationVolume,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator-b",
		Metadata:    secondMetadata,
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)
	assert.Equal(t, first.ID, second.ID)
	assert.Equal(t, storage.ControlRequestPending, second.Status)
	level, err = storage.DecodeVolumeControlRequestMetadata(second.Metadata)
	require.NoError(t, err)
	assert.Equal(t, int32(9), level)
}

// While a volume request is pending it is immutable: a second request returns
// the existing row and its original level untouched. The tern layer relies on
// this to reject a different-level request instead of silently changing the
// level the driver is about to read, apply, and complete.
func TestControlRequestStore_VolumeRequestPendingKeepsOriginalLevel(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	applyID := createControlRequestTestApply(t, store, "apply_control_request_volume_pending")
	firstMetadata, err := storage.EncodeVolumeControlRequestMetadata(5)
	require.NoError(t, err)
	first, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     applyID,
		Operation:   storage.ControlOperationVolume,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator-a",
		Metadata:    firstMetadata,
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)

	secondMetadata, err := storage.EncodeVolumeControlRequestMetadata(11)
	require.NoError(t, err)
	second, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     applyID,
		Operation:   storage.ControlOperationVolume,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator-b",
		Metadata:    secondMetadata,
	})
	require.NoError(t, err)
	require.True(t, alreadyPending)
	assert.Equal(t, first.ID, second.ID)
	level, err := storage.DecodeVolumeControlRequestMetadata(second.Metadata)
	require.NoError(t, err)
	assert.Equal(t, int32(5), level, "a pending volume request must keep the level it was queued with")
}
