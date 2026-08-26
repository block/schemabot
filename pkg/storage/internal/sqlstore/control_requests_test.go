//go:build integration

package sqlstore

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
