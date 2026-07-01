//go:build integration

package mysqlstore

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func TestCheckStore_Upsert(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	check := &storage.Check{
		Repository:     "org/repo",
		PullRequest:    123,
		HeadSHA:        "abc123",
		Environment:    "staging",
		DatabaseType:   "vitess",
		DatabaseName:   "testdb",
		CheckRunID:     999,
		HasChanges:     true,
		Status:         "pending_apply",
		Conclusion:     "action_required",
		BlockingReason: "schema_removed_after_apply_started",
		ErrorMessage:   "operator action required",
	}

	// Insert
	require.NoError(t, store.Checks().Upsert(ctx, check))

	// Verify insert
	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "vitess", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, "pending_apply", retrieved.Status)
	require.Equal(t, "schema_removed_after_apply_started", retrieved.BlockingReason)
	require.Equal(t, "operator action required", retrieved.ErrorMessage)

	// Update
	check.Status = "completed"
	check.Conclusion = "success"
	check.BlockingReason = ""
	check.ErrorMessage = ""
	require.NoError(t, store.Checks().Upsert(ctx, check))

	// Verify update
	retrieved, err = store.Checks().Get(ctx, "org/repo", 123, "staging", "vitess", "testdb")
	require.NoError(t, err)
	require.Equal(t, "completed", retrieved.Status)
	require.Empty(t, retrieved.BlockingReason)
	require.Empty(t, retrieved.ErrorMessage)
}

func TestCheckStore_GetByCheckRunID(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	check := &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "vitess",
		DatabaseName: "testdb",
		CheckRunID:   999,
		HasChanges:   true,
		Status:       "pending_apply",
	}

	require.NoError(t, store.Checks().Upsert(ctx, check))

	// GetByCheckRunID should return the check
	retrieved, err := store.Checks().GetByCheckRunID(ctx, 999)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, "testdb", retrieved.DatabaseName)

	// Non-existent should return nil
	retrieved, err = store.Checks().GetByCheckRunID(ctx, 12345)
	require.NoError(t, err)
	require.Nil(t, retrieved)
}

func TestCheckStore_GetByPR(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// GetByPR on empty table should return empty slice
	checks, err := store.Checks().GetByPR(ctx, "org/repo", 999)
	require.NoError(t, err)
	require.Empty(t, checks)

	// Create checks for same PR, different envs/dbs
	checksToCreate := []*storage.Check{
		{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "staging", DatabaseType: "vitess", DatabaseName: "db1", Status: "pending"},
		{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "production", DatabaseType: "vitess", DatabaseName: "db1", Status: "pending"},
		{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "staging", DatabaseType: "mysql", DatabaseName: "db2", Status: "pending"},
	}
	for _, c := range checksToCreate {
		require.NoError(t, store.Checks().Upsert(ctx, c))
	}

	// Create check for different PR
	require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  456,
		HeadSHA:      "def",
		Environment:  "staging",
		DatabaseType: "vitess",
		DatabaseName: "db1",
		Status:       "pending",
	}))

	// GetByPR should return only checks for PR 123
	retrieved, err := store.Checks().GetByPR(ctx, "org/repo", 123)
	require.NoError(t, err)
	require.Len(t, retrieved, 3)
}

func TestCheckStore_GetByDatabase(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// GetByDatabase on empty table should return empty slice
	checks, err := store.Checks().GetByDatabase(ctx, "org/repo", "staging", "vitess", "nonexistent")
	require.NoError(t, err)
	require.Empty(t, checks)

	// Create checks for same database across different PRs
	checksToCreate := []*storage.Check{
		{Repository: "org/repo", PullRequest: 100, HeadSHA: "a", Environment: "staging", DatabaseType: "vitess", DatabaseName: "shared-db", Status: "pending"},
		{Repository: "org/repo", PullRequest: 200, HeadSHA: "b", Environment: "staging", DatabaseType: "vitess", DatabaseName: "shared-db", Status: "pending"},
		{Repository: "org/repo", PullRequest: 300, HeadSHA: "c", Environment: "staging", DatabaseType: "vitess", DatabaseName: "shared-db", Status: "pending"},
	}
	for _, c := range checksToCreate {
		require.NoError(t, store.Checks().Upsert(ctx, c))
	}

	// Create check for different database
	require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  100,
		HeadSHA:      "a",
		Environment:  "staging",
		DatabaseType: "vitess",
		DatabaseName: "other-db",
		Status:       "pending",
	}))

	// GetByDatabase should return checks for shared-db
	retrieved, err := store.Checks().GetByDatabase(ctx, "org/repo", "staging", "vitess", "shared-db")
	require.NoError(t, err)
	require.Len(t, retrieved, 3)
}

func TestCheckStore_Delete(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	check := &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "abc",
		Environment:  "staging",
		DatabaseType: "vitess",
		DatabaseName: "testdb",
		Status:       "pending",
	}
	require.NoError(t, store.Checks().Upsert(ctx, check))

	// Get to find the ID
	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "vitess", "testdb")
	require.NoError(t, err)

	// Delete should succeed
	require.NoError(t, store.Checks().Delete(ctx, retrieved.ID))

	// Verify deleted
	retrieved, err = store.Checks().Get(ctx, "org/repo", 123, "staging", "vitess", "testdb")
	require.NoError(t, err)
	require.Nil(t, retrieved)

	// Delete non-existent should fail
	require.ErrorIs(t, store.Checks().Delete(ctx, 99999), storage.ErrCheckNotFound)
}

// TestCheckStore_DeleteByPRExcludingApplyOwned verifies PR-close cleanup at the
// storage layer: all of a PR's stored check state is deleted except rows owned
// by an in-flight apply (apply_id set and status in_progress), which must keep
// blocking the PR until the apply reaches a terminal state. Rows for other PRs
// are untouched.
func TestCheckStore_DeleteByPRExcludingApplyOwned(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Checks for the closed PR: two deletable rows, one apply-owned in-flight
	// row that must survive, and one terminal apply-owned row that is deletable.
	checksToCreate := []*storage.Check{
		{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "staging", DatabaseType: "vitess", DatabaseName: "db1", Status: "pending"},
		{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "staging", DatabaseType: "mysql", DatabaseName: "db2", Status: "pending"},
		{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "production", DatabaseType: "mysql", DatabaseName: "db3", Status: "in_progress", ApplyID: 77},
		{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "production", DatabaseType: "mysql", DatabaseName: "db4", Status: "completed", ApplyID: 88, Conclusion: "success"},
	}
	for _, c := range checksToCreate {
		require.NoError(t, store.Checks().Upsert(ctx, c))
	}

	// Check for a different PR (must not be deleted)
	require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  456,
		HeadSHA:      "def",
		Environment:  "staging",
		DatabaseType: "vitess",
		DatabaseName: "db1",
		Status:       "pending",
	}))

	require.NoError(t, store.Checks().DeleteByPRExcludingApplyOwned(ctx, "org/repo", 123))

	// Only the apply-owned in-flight row survives for PR 123.
	retrieved, err := store.Checks().GetByPR(ctx, "org/repo", 123)
	require.NoError(t, err)
	require.Len(t, retrieved, 1)
	assert.Equal(t, "db3", retrieved[0].DatabaseName)
	assert.Equal(t, "in_progress", retrieved[0].Status)
	assert.Equal(t, int64(77), retrieved[0].ApplyID)

	// PR 456's check still exists.
	retrieved, err = store.Checks().GetByPR(ctx, "org/repo", 456)
	require.NoError(t, err)
	require.Len(t, retrieved, 1)
	assert.Equal(t, "db1", retrieved[0].DatabaseName)

	// Deleting for a non-existent PR is a no-op, not an error.
	require.NoError(t, store.Checks().DeleteByPRExcludingApplyOwned(ctx, "org/repo", 999))
}

func TestCheckStore_GetByPR_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Checks().GetByPR(t.Context(), "org/repo", 123)
	require.Error(t, err)
}

func TestCheckStore_GetByDatabase_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Checks().GetByDatabase(t.Context(), "org/repo", "staging", "vitess", "db")
	require.Error(t, err)
}

func TestCheckStore_Delete_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	err = store.Checks().Delete(t.Context(), 123)
	require.Error(t, err)
}

func TestCheckStore_CheckRunIDZeroIsNull(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Create check without CheckRunID (zero value)
	check := &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "vitess",
		DatabaseName: "testdb",
		CheckRunID:   0,
		HasChanges:   true,
		Status:       "pending_apply",
	}

	require.NoError(t, store.Checks().Upsert(ctx, check))

	// GetByCheckRunID(0) should NOT find the check (NULL != 0)
	retrieved, err := store.Checks().GetByCheckRunID(ctx, 0)
	require.NoError(t, err)
	require.Nil(t, retrieved)

	// Get by key should return the check with CheckRunID=0
	retrieved, err = store.Checks().Get(ctx, "org/repo", 123, "staging", "vitess", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, int64(0), retrieved.CheckRunID)
}

func TestCheckStore_ApplyIDRoundTrip(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	check := &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "vitess",
		DatabaseName: "testdb",
		ApplyID:      42,
		HasChanges:   true,
		Status:       "in_progress",
	}

	require.NoError(t, store.Checks().Upsert(ctx, check))

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "vitess", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, int64(42), retrieved.ApplyID)

	check.ApplyID = 0
	require.NoError(t, store.Checks().Upsert(ctx, check))

	retrieved, err = store.Checks().Get(ctx, "org/repo", 123, "staging", "vitess", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, int64(0), retrieved.ApplyID)
}

func TestCheckStore_UpsertPlanResultPreservesInProgressApply(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	apply := createCheckStoreApply(t, store, "apply-running", state.Apply.Running)
	require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "same-sha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		ApplyID:      apply.ID,
		HasChanges:   true,
		Status:       "in_progress",
		Conclusion:   "",
	}))

	err := store.Checks().UpsertPlanResult(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "same-sha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		HasChanges:   true,
		Status:       "completed",
		Conclusion:   "action_required",
	})
	require.NoError(t, err)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, "same-sha", retrieved.HeadSHA)
	require.Equal(t, "in_progress", retrieved.Status)
	require.Empty(t, retrieved.Conclusion)
	require.Equal(t, apply.ID, retrieved.ApplyID)
}

func TestCheckStore_RecoverApplyOwnedCheckWithNoOpPlanRecoversStoredCheckState(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	apply := createCheckStoreApply(t, store, "apply-noop-success", state.Apply.Running)
	require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
		Repository:     "org/repo",
		PullRequest:    123,
		HeadSHA:        "same-sha",
		Environment:    "staging",
		DatabaseType:   "mysql",
		DatabaseName:   "testdb",
		ApplyID:        apply.ID,
		HasChanges:     true,
		Status:         "in_progress",
		Conclusion:     "",
		BlockingReason: "schema_change_running",
		ErrorMessage:   "schema change is still running",
	}))

	err := store.Checks().UpsertPlanResult(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "same-sha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		HasChanges:   false,
		Status:       "completed",
		Conclusion:   "success",
	})
	require.NoError(t, err)

	recovered, err := store.Checks().RecoverApplyOwnedCheckWithNoOpPlan(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "same-sha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		HasChanges:   false,
		Status:       "completed",
		Conclusion:   "success",
	})
	require.NoError(t, err)
	require.True(t, recovered)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, "same-sha", retrieved.HeadSHA)
	require.Equal(t, "completed", retrieved.Status)
	require.Equal(t, "success", retrieved.Conclusion)
	require.False(t, retrieved.HasChanges)
	require.Equal(t, int64(0), retrieved.ApplyID)
	require.Empty(t, retrieved.BlockingReason)
	require.Empty(t, retrieved.ErrorMessage)
}

func TestCheckStore_UpsertPlanResultPreservesApplyOwnedCheckStateOnNoOpSuccess(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	apply := createCheckStoreApply(t, store, "apply-noop-recovery-disabled", state.Apply.Running)
	require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "same-sha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		ApplyID:      apply.ID,
		HasChanges:   true,
		Status:       "in_progress",
		Conclusion:   "",
	}))

	err := store.Checks().UpsertPlanResult(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "same-sha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		HasChanges:   false,
		Status:       "completed",
		Conclusion:   "success",
	})
	require.NoError(t, err)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, "same-sha", retrieved.HeadSHA)
	require.Equal(t, "in_progress", retrieved.Status)
	require.Empty(t, retrieved.Conclusion)
	require.Equal(t, apply.ID, retrieved.ApplyID)
}

func TestCheckStore_RecoverApplyOwnedCheckWithNoOpPlanRequiresNoOpSuccess(t *testing.T) {
	for _, tc := range []struct {
		name       string
		applyID    string
		hasChanges bool
		status     string
		conclusion string
	}{
		{
			name:       "plan has changes",
			applyID:    "apply-plan-has-changes",
			hasChanges: true,
			status:     "completed",
			conclusion: "action_required",
		},
		{
			name:       "plan failed without changes",
			applyID:    "apply-plan-failed-without-changes",
			hasChanges: false,
			status:     "completed",
			conclusion: "failure",
		},
		{
			name:       "plan failed with changes",
			applyID:    "apply-plan-failed-with-changes",
			hasChanges: true,
			status:     "completed",
			conclusion: "failure",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearTables(t)
			ctx := t.Context()
			store := New(testDB)

			apply := createCheckStoreApply(t, store, tc.applyID, state.Apply.Running)
			require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
				Repository:   "org/repo",
				PullRequest:  123,
				HeadSHA:      "same-sha",
				Environment:  "staging",
				DatabaseType: "mysql",
				DatabaseName: "testdb",
				ApplyID:      apply.ID,
				HasChanges:   true,
				Status:       "in_progress",
				Conclusion:   "",
			}))

			err := store.Checks().UpsertPlanResult(ctx, &storage.Check{
				Repository:   "org/repo",
				PullRequest:  123,
				HeadSHA:      "same-sha",
				Environment:  "staging",
				DatabaseType: "mysql",
				DatabaseName: "testdb",
				HasChanges:   tc.hasChanges,
				Status:       tc.status,
				Conclusion:   tc.conclusion,
			})
			require.NoError(t, err)

			recovered, err := store.Checks().RecoverApplyOwnedCheckWithNoOpPlan(ctx, &storage.Check{
				Repository:   "org/repo",
				PullRequest:  123,
				HeadSHA:      "same-sha",
				Environment:  "staging",
				DatabaseType: "mysql",
				DatabaseName: "testdb",
				HasChanges:   tc.hasChanges,
				Status:       tc.status,
				Conclusion:   tc.conclusion,
			})
			require.NoError(t, err)
			require.False(t, recovered)

			retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
			require.NoError(t, err)
			require.NotNil(t, retrieved)
			require.Equal(t, "same-sha", retrieved.HeadSHA)
			require.Equal(t, "in_progress", retrieved.Status)
			require.Empty(t, retrieved.Conclusion)
			require.Equal(t, apply.ID, retrieved.ApplyID)
		})
	}
}

// When a database drops out of a PR and its stored check state is a plan-only
// result with no started apply, stale cleanup marks it successful so the PR is
// no longer blocked by a database it no longer touches.
func TestCheckStore_MarkStalePlanSuccessfulMarksPlanOnlyCheck(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
		Repository:     "org/repo",
		PullRequest:    123,
		HeadSHA:        "oldsha",
		Environment:    "staging",
		DatabaseType:   "mysql",
		DatabaseName:   "testdb",
		HasChanges:     true,
		Status:         "completed",
		Conclusion:     "action_required",
		BlockingReason: "schema_change_pending",
		ErrorMessage:   "schema change pending apply",
	}))

	marked, err := store.Checks().MarkStalePlanSuccessful(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "newsha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		HasChanges:   false,
		Status:       "completed",
		Conclusion:   "success",
	})
	require.NoError(t, err)
	require.True(t, marked)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, "newsha", retrieved.HeadSHA)
	require.Equal(t, "completed", retrieved.Status)
	require.Equal(t, "success", retrieved.Conclusion)
	require.False(t, retrieved.HasChanges)
	require.Equal(t, int64(0), retrieved.ApplyID)
	require.Empty(t, retrieved.BlockingReason)
	require.Empty(t, retrieved.ErrorMessage)
}

// A database can drop out of a PR at the same moment an apply for it begins. If
// the apply claims the stored check state after stale cleanup read it, the row
// is in_progress and apply-owned. Stale cleanup must not convert that into a
// passing check: the apply may already have reached the live database, so the
// row stays blocking until an operator reconciles the target.
func TestCheckStore_MarkStalePlanSuccessfulLeavesInProgressApplyBlocking(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	apply := createCheckStoreApply(t, store, "apply-claimed-after-cleanup-read", state.Apply.Running)
	require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "oldsha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		CheckRunID:   100,
		ApplyID:      apply.ID,
		HasChanges:   true,
		Status:       "in_progress",
		Conclusion:   "",
	}))

	marked, err := store.Checks().MarkStalePlanSuccessful(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "newsha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		HasChanges:   false,
		Status:       "completed",
		Conclusion:   "success",
	})
	require.NoError(t, err)
	require.False(t, marked, "in-flight apply-owned check must not be marked successful by stale cleanup")

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, "oldsha", retrieved.HeadSHA)
	require.Equal(t, "in_progress", retrieved.Status)
	require.Empty(t, retrieved.Conclusion)
	require.True(t, retrieved.HasChanges)
	require.Equal(t, apply.ID, retrieved.ApplyID)
}

// A terminal apply-owned row (apply ID still set after the apply finished) keeps
// blocking under stale cleanup. The apply already touched the live database, so a
// later commit dropping the database must not derive a passing check by cleanup
// alone.
func TestCheckStore_MarkStalePlanSuccessfulLeavesTerminalApplyOwnedBlocking(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	apply := createCheckStoreApply(t, store, "apply-terminal-owned", state.Apply.Completed)
	require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "oldsha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		ApplyID:      apply.ID,
		HasChanges:   true,
		Status:       "completed",
		Conclusion:   "success",
	}))

	marked, err := store.Checks().MarkStalePlanSuccessful(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "newsha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		HasChanges:   false,
		Status:       "completed",
		Conclusion:   "success",
	})
	require.NoError(t, err)
	require.False(t, marked, "terminal apply-owned check must not be cleaned to success by stale cleanup")

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, "oldsha", retrieved.HeadSHA)
	require.Equal(t, apply.ID, retrieved.ApplyID)
}

// Stale cleanup runs more than once for the same dropped database: the webhook
// can re-deliver, or a later commit re-triggers cleanup for a database that is
// already cleaned. Re-marking a row that already holds the plan-only successful
// values is idempotent and must report success, not falsely report the row as
// blocked by an in-flight apply. Under MySQL's production changed-rows
// semantics the no-op update affects zero rows, so this is the case that proves
// the re-read distinguishes "already successful" from "apply-owned". The test
// runs on a changed-rows connection (no clientFoundRows) to exercise that path.
func TestCheckStore_MarkStalePlanSuccessfulIsIdempotentUnderChangedRows(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := newChangedRowsStore(t)

	require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
		Repository:     "org/repo",
		PullRequest:    123,
		HeadSHA:        "oldsha",
		Environment:    "staging",
		DatabaseType:   "mysql",
		DatabaseName:   "testdb",
		HasChanges:     true,
		Status:         "completed",
		Conclusion:     "action_required",
		BlockingReason: "schema_change_pending",
		ErrorMessage:   "schema change pending apply",
	}))

	successCheck := &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "newsha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		HasChanges:   false,
		Status:       "completed",
		Conclusion:   "success",
	}

	marked, err := store.Checks().MarkStalePlanSuccessful(ctx, successCheck)
	require.NoError(t, err)
	require.True(t, marked, "first stale cleanup must mark the plan-only check successful")

	// Re-marking the already-successful row is a no-op write under changed-rows
	// semantics, but the row is genuinely successful and must still report so.
	marked, err = store.Checks().MarkStalePlanSuccessful(ctx, successCheck)
	require.NoError(t, err)
	require.True(t, marked, "re-marking an already-successful plan check must report success, not blocking")

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, "newsha", retrieved.HeadSHA)
	require.Equal(t, "completed", retrieved.Status)
	require.Equal(t, "success", retrieved.Conclusion)
	require.False(t, retrieved.HasChanges)
	require.Equal(t, int64(0), retrieved.ApplyID)
	require.Empty(t, retrieved.BlockingReason)
	require.Empty(t, retrieved.ErrorMessage)
}

// Under changed-rows semantics, a row claimed by a started apply between the
// cleanup read and the write must still be reported as blocking. The guard
// excludes it (zero rows affected) and the re-read finds an in_progress,
// apply-owned row, so cleanup must not derive a passing check from it.
func TestCheckStore_MarkStalePlanSuccessfulLeavesInProgressApplyBlockingUnderChangedRows(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := newChangedRowsStore(t)

	apply := createCheckStoreApply(t, store, "apply-claimed-under-changed-rows", state.Apply.Running)
	require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "oldsha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		CheckRunID:   100,
		ApplyID:      apply.ID,
		HasChanges:   true,
		Status:       "in_progress",
		Conclusion:   "",
	}))

	marked, err := store.Checks().MarkStalePlanSuccessful(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "newsha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		HasChanges:   false,
		Status:       "completed",
		Conclusion:   "success",
	})
	require.NoError(t, err)
	require.False(t, marked, "in-flight apply-owned check must not be marked successful by stale cleanup")

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, "oldsha", retrieved.HeadSHA)
	require.Equal(t, "in_progress", retrieved.Status)
	require.True(t, retrieved.HasChanges)
	require.Equal(t, apply.ID, retrieved.ApplyID)
}

// newChangedRowsStore opens a Storage on a connection without clientFoundRows so
// UPDATE ... RowsAffected reflects changed rows, matching production semantics.
func newChangedRowsStore(t *testing.T) *Storage {
	t.Helper()
	db, err := sql.Open("mysql", testDSNChangedRows)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(t.Context()))
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	return New(db)
}

func TestCheckStore_UpsertPlanResultReplacesUnownedInProgressCheck(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "same-sha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		HasChanges:   true,
		Status:       "in_progress",
		Conclusion:   "",
	}))

	err := store.Checks().UpsertPlanResult(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "same-sha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		HasChanges:   true,
		Status:       "completed",
		Conclusion:   "action_required",
	})
	require.NoError(t, err)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, "same-sha", retrieved.HeadSHA)
	require.Equal(t, "completed", retrieved.Status)
	require.Equal(t, "action_required", retrieved.Conclusion)
	require.Equal(t, int64(0), retrieved.ApplyID)
}

// An apply starts on one commit and a newer commit reverts the schema change
// in-file. The auto-plan for the newer commit diffs against the mid-apply
// database and reports a successful no-op, but it must not overwrite or take
// ownership of the in-progress apply-owned row: the stored state keeps blocking
// the PR until the apply itself completes and writes its real result.
func TestCheckStore_UpsertPlanResultPreservesInProgressApplyOnNewHead(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	apply := createCheckStoreApply(t, store, "apply-running-old-head", state.Apply.Running)
	require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "oldsha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		ApplyID:      apply.ID,
		HasChanges:   true,
		Status:       "in_progress",
		Conclusion:   "",
	}))

	err := store.Checks().UpsertPlanResult(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "newsha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		HasChanges:   false,
		Status:       "completed",
		Conclusion:   "success",
	})
	require.NoError(t, err)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, "oldsha", retrieved.HeadSHA)
	assert.Equal(t, "in_progress", retrieved.Status)
	assert.Empty(t, retrieved.Conclusion)
	assert.True(t, retrieved.HasChanges)
	assert.Equal(t, apply.ID, retrieved.ApplyID)

	// The apply still owns the row, so its completion lands the real result.
	completion := *retrieved
	completion.Status = "completed"
	completion.Conclusion = "success"
	completion.HasChanges = false
	updated, err := store.Checks().CompleteForApply(ctx, &completion, apply)
	require.NoError(t, err)
	require.True(t, updated)

	retrieved, err = store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, "completed", retrieved.Status)
	assert.Equal(t, "success", retrieved.Conclusion)
	assert.Equal(t, apply.ID, retrieved.ApplyID)
}

// A plan result for a newer commit replaces an in-progress row that no apply
// owns: without a started apply there is nothing authoritative to protect, so
// the plan write proceeds normally.
func TestCheckStore_UpsertPlanResultReplacesUnownedInProgressCheckOnNewHead(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "oldsha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		HasChanges:   true,
		Status:       "in_progress",
		Conclusion:   "",
	}))

	err := store.Checks().UpsertPlanResult(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "newsha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		HasChanges:   true,
		Status:       "completed",
		Conclusion:   "action_required",
	})
	require.NoError(t, err)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, "newsha", retrieved.HeadSHA)
	assert.Equal(t, "completed", retrieved.Status)
	assert.Equal(t, "action_required", retrieved.Conclusion)
	assert.Equal(t, int64(0), retrieved.ApplyID)
}

func TestCheckStore_UpsertPlanResultClearsApplyIDWhenNotInProgress(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	apply := createCheckStoreApply(t, store, "apply-completed-plan", state.Apply.Completed)
	require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "oldsha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		ApplyID:      apply.ID,
		HasChanges:   false,
		Status:       "completed",
		Conclusion:   "success",
	}))

	err := store.Checks().UpsertPlanResult(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "newsha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		HasChanges:   true,
		Status:       "completed",
		Conclusion:   "action_required",
	})
	require.NoError(t, err)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, "newsha", retrieved.HeadSHA)
	require.Equal(t, "completed", retrieved.Status)
	require.Equal(t, "action_required", retrieved.Conclusion)
	require.Equal(t, int64(0), retrieved.ApplyID)
}

func TestCheckStore_CompleteForApply(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	apply := createCheckStoreApply(t, store, "apply-complete", state.Apply.Completed)

	check := &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		ApplyID:      apply.ID,
		HasChanges:   true,
		Status:       "in_progress",
	}
	require.NoError(t, store.Checks().Upsert(ctx, check))

	check.Status = "completed"
	check.Conclusion = "success"
	check.HasChanges = false
	updated, err := store.Checks().CompleteForApply(ctx, check, apply)
	require.NoError(t, err)
	require.True(t, updated)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, "completed", retrieved.Status)
	require.Equal(t, "success", retrieved.Conclusion)
	require.Equal(t, apply.ID, retrieved.ApplyID)
}

// A plan-time change summary must round-trip through storage and survive the
// apply lifecycle: once the plan records "N created, M altered", later
// apply-state transitions (in_progress, terminal completion) must not blank it,
// so the aggregate check's Change column keeps showing what the PR changes.
func TestCheckStore_ChangeSummaryRoundTripsAndSurvivesApply(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Plan records the summary.
	require.NoError(t, store.Checks().UpsertPlanResult(ctx, &storage.Check{
		Repository:    "octocat/hello-world",
		PullRequest:   7,
		HeadSHA:       "abc123",
		Environment:   "staging",
		DatabaseType:  "vitess",
		DatabaseName:  "orders",
		HasChanges:    true,
		Status:        "completed",
		Conclusion:    "action_required",
		ChangeSummary: "5 created, 3 altered · 2 vschema updates",
	}))

	checks, err := store.Checks().GetByPR(ctx, "octocat/hello-world", 7)
	require.NoError(t, err)
	require.Len(t, checks, 1)
	assert.Equal(t, "5 created, 3 altered · 2 vschema updates", checks[0].ChangeSummary)

	// Apply starts and completes. Neither transition carries a summary, and both
	// must preserve the plan-time value rather than blanking it.
	apply := createCheckStoreApply(t, store, "apply-change-summary", state.Apply.Completed)

	require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  7,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "vitess",
		DatabaseName: "orders",
		ApplyID:      apply.ID,
		HasChanges:   true,
		Status:       "in_progress",
	}))

	afterStart, err := store.Checks().Get(ctx, "octocat/hello-world", 7, "staging", "vitess", "orders")
	require.NoError(t, err)
	require.NotNil(t, afterStart)
	assert.Equal(t, "in_progress", afterStart.Status)
	assert.Equal(t, "5 created, 3 altered · 2 vschema updates", afterStart.ChangeSummary,
		"apply-start upsert must preserve the plan-time change summary")

	updated, err := store.Checks().CompleteForApply(ctx, &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  7,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "vitess",
		DatabaseName: "orders",
		ApplyID:      apply.ID,
		HasChanges:   false,
		Status:       "completed",
		Conclusion:   "success",
	}, apply)
	require.NoError(t, err)
	require.True(t, updated)

	afterComplete, err := store.Checks().Get(ctx, "octocat/hello-world", 7, "staging", "vitess", "orders")
	require.NoError(t, err)
	require.NotNil(t, afterComplete)
	assert.Equal(t, "completed", afterComplete.Status)
	assert.Equal(t, "success", afterComplete.Conclusion)
	assert.Equal(t, "5 created, 3 altered · 2 vschema updates", afterComplete.ChangeSummary,
		"terminal apply completion must preserve the plan-time change summary")
}

// The plan is authoritative for its own summary on a plan-only row: a re-plan
// overwrites the summary, and a re-plan that finds no changes clears it (rather
// than leaving a stale summary on a now-up-to-date check).
func TestCheckStore_ReplanUpdatesChangeSummary(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	planCheck := func(summary string) *storage.Check {
		hasChanges := summary != ""
		conclusion := "success"
		if hasChanges {
			conclusion = "action_required"
		}
		return &storage.Check{
			Repository: "octocat/hello-world", PullRequest: 7, HeadSHA: "abc123",
			Environment: "staging", DatabaseType: "mysql", DatabaseName: "orders",
			HasChanges: hasChanges, Status: "completed", Conclusion: conclusion,
			ChangeSummary: summary,
		}
	}
	get := func() *storage.Check {
		c, err := store.Checks().Get(ctx, "octocat/hello-world", 7, "staging", "mysql", "orders")
		require.NoError(t, err)
		require.NotNil(t, c)
		return c
	}

	require.NoError(t, store.Checks().UpsertPlanResult(ctx, planCheck("5 created")))
	assert.Equal(t, "5 created", get().ChangeSummary)

	// A new plan with different changes overwrites the summary.
	require.NoError(t, store.Checks().UpsertPlanResult(ctx, planCheck("2 altered")))
	assert.Equal(t, "2 altered", get().ChangeSummary)

	// A new plan that finds no changes clears the summary.
	require.NoError(t, store.Checks().UpsertPlanResult(ctx, planCheck("")))
	assert.Empty(t, get().ChangeSummary)
}

func TestCheckStore_CompleteForApplyLeaseGuard(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	apply := createCheckStoreApply(t, store, "apply-complete-lease", state.Apply.Completed)
	_, err := testDB.ExecContext(ctx, `
		UPDATE applies
		SET lease_owner = ?, lease_token = ?, lease_acquired_at = NOW()
		WHERE id = ?
	`, "current-driver", "current-token", apply.ID)
	require.NoError(t, err)

	check := &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		ApplyID:      apply.ID,
		HasChanges:   true,
		Status:       "in_progress",
	}
	require.NoError(t, store.Checks().Upsert(ctx, check))

	check.Status = "completed"
	check.Conclusion = "success"
	check.HasChanges = false
	staleApply := *apply
	staleApply.LeaseOwner = "old-driver"
	staleApply.LeaseToken = "stale-token"
	updated, err := store.Checks().CompleteForApply(ctx, check, &staleApply)
	require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
	assert.False(t, updated)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, "in_progress", retrieved.Status)
	assert.Empty(t, retrieved.Conclusion)
	assert.Equal(t, apply.ID, retrieved.ApplyID)

	currentApply := *apply
	currentApply.LeaseOwner = "current-driver"
	currentApply.LeaseToken = "current-token"
	updated, err = store.Checks().CompleteForApply(ctx, check, &currentApply)
	require.NoError(t, err)
	assert.True(t, updated)
}

func TestCheckStore_CompleteForApplySkipsNewerRunningApply(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	oldApply := createCheckStoreApply(t, store, "apply-old", state.Apply.Completed)
	newApply := createCheckStoreApply(t, store, "apply-new", state.Apply.Running)
	require.Greater(t, newApply.ID, oldApply.ID)

	check := &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		ApplyID:      oldApply.ID,
		HasChanges:   true,
		Status:       "in_progress",
	}
	require.NoError(t, store.Checks().Upsert(ctx, check))

	check.Status = "completed"
	check.Conclusion = "success"
	check.HasChanges = false
	updated, err := store.Checks().CompleteForApply(ctx, check, oldApply)
	require.NoError(t, err)
	require.False(t, updated)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, "in_progress", retrieved.Status)
	require.Empty(t, retrieved.Conclusion)
	require.Equal(t, oldApply.ID, retrieved.ApplyID)
}

func TestCheckStore_CompleteForApplySkipsNewerTerminalApply(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	oldApply := createCheckStoreApply(t, store, "apply-old-terminal", state.Apply.Completed)
	newApply := createCheckStoreApply(t, store, "apply-new-terminal", state.Apply.Failed)
	require.Greater(t, newApply.ID, oldApply.ID)

	check := &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		ApplyID:      oldApply.ID,
		HasChanges:   true,
		Status:       "in_progress",
	}
	require.NoError(t, store.Checks().Upsert(ctx, check))

	check.Status = "completed"
	check.Conclusion = "success"
	check.HasChanges = false
	updated, err := store.Checks().CompleteForApply(ctx, check, oldApply)
	require.NoError(t, err)
	require.False(t, updated)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, "in_progress", retrieved.Status)
	require.Empty(t, retrieved.Conclusion)
	require.Equal(t, oldApply.ID, retrieved.ApplyID)
}

func TestCheckStore_CompleteForApplySkipsUnownedCheck(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	apply := createCheckStoreApply(t, store, "apply-unowned", state.Apply.Completed)

	check := &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		HasChanges:   true,
		Status:       "in_progress",
	}
	require.NoError(t, store.Checks().Upsert(ctx, check))

	check.Status = "completed"
	check.Conclusion = "success"
	check.HasChanges = false
	updated, err := store.Checks().CompleteForApply(ctx, check, apply)
	require.NoError(t, err)
	require.False(t, updated)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, "in_progress", retrieved.Status)
	require.Empty(t, retrieved.Conclusion)
	require.Equal(t, int64(0), retrieved.ApplyID)
}

func TestCheckStore_MarkActionRequiredForApply(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	apply := createCheckStoreApply(t, store, "rollback-complete", state.Apply.Completed)

	check := &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		ApplyID:      apply.ID,
		HasChanges:   false,
		Status:       "completed",
		Conclusion:   "success",
	}
	require.NoError(t, store.Checks().Upsert(ctx, check))

	check.HasChanges = true
	check.Conclusion = "action_required"
	updated, err := store.Checks().MarkActionRequiredForApply(ctx, check, apply)
	require.NoError(t, err)
	require.True(t, updated)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, "completed", retrieved.Status)
	require.Equal(t, "action_required", retrieved.Conclusion)
	require.True(t, retrieved.HasChanges)
	require.Equal(t, int64(0), retrieved.ApplyID)
}

func TestCheckStore_MarkActionRequiredForApplyLeaseGuard(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	apply := createCheckStoreApply(t, store, "rollback-complete-lease", state.Apply.Completed)
	_, err := testDB.ExecContext(ctx, `
		UPDATE applies
		SET lease_owner = ?, lease_token = ?, lease_acquired_at = NOW()
		WHERE id = ?
	`, "current-driver", "current-token", apply.ID)
	require.NoError(t, err)

	check := &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		ApplyID:      apply.ID,
		HasChanges:   false,
		Status:       "completed",
		Conclusion:   "success",
	}
	require.NoError(t, store.Checks().Upsert(ctx, check))

	check.HasChanges = true
	check.Conclusion = "action_required"
	staleApply := *apply
	staleApply.LeaseOwner = "old-driver"
	staleApply.LeaseToken = "stale-token"
	updated, err := store.Checks().MarkActionRequiredForApply(ctx, check, &staleApply)
	require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
	assert.False(t, updated)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, "completed", retrieved.Status)
	assert.Equal(t, "success", retrieved.Conclusion)
	assert.False(t, retrieved.HasChanges)
	assert.Equal(t, apply.ID, retrieved.ApplyID)

	currentApply := *apply
	currentApply.LeaseOwner = "current-driver"
	currentApply.LeaseToken = "current-token"
	updated, err = store.Checks().MarkActionRequiredForApply(ctx, check, &currentApply)
	require.NoError(t, err)
	assert.True(t, updated)
}

func TestCheckStore_MarkActionRequiredForApplySkipsNewerRunningApply(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	rollbackApply := createCheckStoreApply(t, store, "rollback-complete-old", state.Apply.Completed)
	newApply := createCheckStoreApply(t, store, "apply-running-new", state.Apply.Running)
	require.Greater(t, newApply.ID, rollbackApply.ID)

	check := &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		ApplyID:      rollbackApply.ID,
		HasChanges:   false,
		Status:       "completed",
		Conclusion:   "success",
	}
	require.NoError(t, store.Checks().Upsert(ctx, check))

	check.HasChanges = true
	check.Conclusion = "action_required"
	updated, err := store.Checks().MarkActionRequiredForApply(ctx, check, rollbackApply)
	require.NoError(t, err)
	require.False(t, updated)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, "completed", retrieved.Status)
	require.Equal(t, "success", retrieved.Conclusion)
	require.False(t, retrieved.HasChanges)
	require.Equal(t, rollbackApply.ID, retrieved.ApplyID)
}

func TestCheckStore_MarkActionRequiredForApplySkipsNewerTerminalApply(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	rollbackApply := createCheckStoreApply(t, store, "rollback-complete-old", state.Apply.Completed)
	newApply := createCheckStoreApply(t, store, "apply-terminal-new", state.Apply.Failed)
	require.Greater(t, newApply.ID, rollbackApply.ID)

	check := &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		ApplyID:      rollbackApply.ID,
		HasChanges:   false,
		Status:       "completed",
		Conclusion:   "success",
	}
	require.NoError(t, store.Checks().Upsert(ctx, check))

	check.HasChanges = true
	check.Conclusion = "action_required"
	updated, err := store.Checks().MarkActionRequiredForApply(ctx, check, rollbackApply)
	require.NoError(t, err)
	require.False(t, updated)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, "completed", retrieved.Status)
	require.Equal(t, "success", retrieved.Conclusion)
	require.False(t, retrieved.HasChanges)
	require.Equal(t, rollbackApply.ID, retrieved.ApplyID)
}

func createCheckStoreApply(t *testing.T, store storage.Storage, applyIdentifier, applyState string) *storage.Apply {
	t.Helper()
	apply := &storage.Apply{
		ApplyIdentifier: applyIdentifier,
		Database:        "testdb",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "staging",
		Engine:          storage.EngineSpirit,
		State:           applyState,
	}
	id, err := store.Applies().Create(t.Context(), apply)
	require.NoError(t, err)

	created, err := store.Applies().Get(t.Context(), id)
	require.NoError(t, err)
	require.NotNil(t, created)
	return created
}
