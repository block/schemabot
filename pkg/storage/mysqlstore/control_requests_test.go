//go:build integration

package mysqlstore

import (
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
