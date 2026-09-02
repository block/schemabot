//go:build integration

package sqlstore

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// The behavioral suite for CheckStore lives in pkg/storage/storagetest and
// runs against every dialect via parity_test.go. The tests here cover only
// MySQL changed-rows behavior, which requires a connection configuration the
// storage interface cannot express.

func TestCheckStore_MarkStalePlanSuccessfulIsIdempotentUnderChangedRows(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := newChangedRowsStore(t)

	require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
		Repository: "org/repo", PullRequest: 123, HeadSHA: "oldsha",
		Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "testdb",
		HasChanges: true, Status: "completed", Conclusion: "action_required",
		BlockingReason: "schema_change_pending", ErrorMessage: "schema change pending apply",
	}))
	success := &storage.Check{
		Repository: "org/repo", PullRequest: 123, HeadSHA: "newsha",
		Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "testdb",
		Status: "completed", Conclusion: "success",
	}

	marked, err := store.Checks().MarkStalePlanSuccessful(ctx, success)
	require.NoError(t, err)
	assert.True(t, marked)
	marked, err = store.Checks().MarkStalePlanSuccessful(ctx, success)
	require.NoError(t, err)
	assert.True(t, marked)
}

func TestCheckStore_MarkStalePlanSuccessfulLeavesInProgressApplyBlockingUnderChangedRows(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := newChangedRowsStore(t)

	require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
		Repository: "org/repo", PullRequest: 123, HeadSHA: "oldsha",
		Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "testdb",
		ApplyID: 42, HasChanges: true, Status: "in_progress",
	}))
	marked, err := store.Checks().MarkStalePlanSuccessful(ctx, &storage.Check{
		Repository: "org/repo", PullRequest: 123, HeadSHA: "newsha",
		Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "testdb",
		Status: "completed", Conclusion: "success",
	})
	require.NoError(t, err)
	assert.False(t, marked)

	stored, err := store.Checks().Get(ctx, "org/repo", 123, "staging", storage.DatabaseTypeMySQL, "testdb")
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, "in_progress", stored.Status)
	assert.Equal(t, int64(42), stored.ApplyID)
}

// A plan result whose target row is deleted mid-write — the PR closed and its
// check state was cleaned up — is reported as its own outcome, not as the
// ownership guard refusing: the guard retains apply-owned rows, so a target it
// refused still exists. The race is driven directly here because the storage
// interface offers no way to delete the row between a write's own statements.
func TestCheckStore_PlanWriteOnDeletedRowReportsCheckNotFound(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := newChangedRowsStore(t)

	check := &storage.Check{
		Repository: "org/repo", PullRequest: 123, HeadSHA: "newsha",
		Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "testdb",
		Status: "completed", Conclusion: "success",
	}

	// An update against the now-absent row yields the same zero-row result the
	// guarded plan write leaves behind once the row is gone.
	result, err := store.checks.db.ExecContext(ctx, `
		UPDATE checks SET head_sha = ?
		WHERE repository = ? AND pull_request = ?
		  AND environment = ? AND database_type = ? AND database_name = ?
	`, check.HeadSHA, check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName)
	require.NoError(t, err)

	landed, err := store.checks.planWriteLanded(ctx, check, result)
	require.ErrorIs(t, err, storage.ErrCheckNotFound)
	assert.False(t, landed)
}

func newChangedRowsStore(t *testing.T) *Storage {
	t.Helper()
	db, err := sql.Open("mysql", testDSNChangedRows)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(t.Context()))
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	return NewMySQL(db)
}
