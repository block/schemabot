//go:build integration

package sqlstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// The behavioral suite for MergeGateRequestStore lives in
// pkg/storage/storagetest and runs against every dialect via parity_test.go.
// The tests here cover only MySQL changed-rows behavior, which requires a
// connection configuration the storage interface cannot express.

// recordChangedRowsRequest records one pending request against a synthetic
// apply row id. The merge gate table carries no foreign key, so these tests
// need no applies row to exercise the lease writes.
func recordChangedRowsRequest(t *testing.T, store *Storage, applyID int64) *storage.MergeGateRequest {
	t.Helper()
	req := &storage.MergeGateRequest{
		ApplyID:         applyID,
		ApplyIdentifier: "apply_changed_rows",
		Environment:     "staging",
		DatabaseType:    storage.DatabaseTypeMySQL,
		DatabaseName:    "testdb",
		Repository:      "org/repo",
	}
	recorded, err := store.MergeGateRequests().Record(t.Context(), req)
	require.NoError(t, err)
	require.True(t, recorded)
	return req
}

// A driver that re-records the retryable failure it already recorded — a
// redelivered result, or a retry after an unacknowledged write — rewrites the
// values the row already holds, which reports no changed rows under MySQL's
// default row-count semantics. The request is still under its attempt cap and
// still owed its remaining attempts, so that count must not be read as a
// reason to end it.
func TestMergeGateRequestStore_RepeatedRetryableFailureStaysRetryableUnderChangedRows(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := newChangedRowsStore(t)

	req := recordChangedRowsRequest(t, store, 9001)
	claimed, err := store.MergeGateRequests().ClaimNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Less(t, claimed.Attempts, storage.MaxMergeGateAttempts)

	retryAt := time.Now().UTC().Add(time.Minute).Truncate(time.Second)
	require.NoError(t, store.MergeGateRequests().MarkFailed(ctx, claimed.ID, claimed.LeaseToken, "engine unavailable", &retryAt))
	require.NoError(t, store.MergeGateRequests().MarkFailed(ctx, claimed.ID, claimed.LeaseToken, "engine unavailable", &retryAt))

	got, err := store.MergeGateRequests().GetByApplyID(ctx, req.ApplyID)
	require.NoError(t, err)
	assert.Equal(t, storage.MergeGateFailed, got.State)
	assert.NotNil(t, got.RetryAfter, "the request is still retryable")
	assert.Nil(t, got.CompletedAt, "the repeated write must not terminalize the request")
}

// A completion retried with the retained lease token rewrites the values the
// row already holds and reports no changed rows. The write matched and the
// request is complete, so the retry must report success rather than a lost
// lease, which the driver would surface as a fan-out that needs recovery.
func TestMergeGateRequestStore_RepeatedCompletionLandsUnderChangedRows(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := newChangedRowsStore(t)

	recordChangedRowsRequest(t, store, 9002)
	claimed, err := store.MergeGateRequests().ClaimNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	require.NoError(t, store.MergeGateRequests().MarkCompleted(ctx, claimed.ID, claimed.LeaseToken))
	require.NoError(t, store.MergeGateRequests().MarkCompleted(ctx, claimed.ID, claimed.LeaseToken))
}

// A heartbeat on a completed request reports lease loss even under
// changed-rows semantics, where the row-count alone cannot distinguish "the
// state guard excluded this row" from "nothing changed". The completed row
// retains its lease token, so only the state read settles it.
func TestMergeGateRequestStore_HeartbeatOnCompletedRequestReportsLeaseLostUnderChangedRows(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := newChangedRowsStore(t)

	recordChangedRowsRequest(t, store, 9003)
	claimed, err := store.MergeGateRequests().ClaimNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	require.NoError(t, store.MergeGateRequests().MarkCompleted(ctx, claimed.ID, claimed.LeaseToken))

	err = store.MergeGateRequests().Heartbeat(ctx, claimed.ID, claimed.LeaseToken, time.Hour)
	assert.ErrorIs(t, err, storage.ErrMergeGateLeaseLost)

	err = store.MergeGateRequests().Release(ctx, claimed.ID, claimed.LeaseToken)
	assert.ErrorIs(t, err, storage.ErrMergeGateLeaseLost)
}
