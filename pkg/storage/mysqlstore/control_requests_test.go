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
