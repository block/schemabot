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

// seedPRCloseRetentionChecks stores one row of every retention shape PR-close
// cleanup distinguishes for PR 123, plus a row on another PR that cleanup must
// never touch.
func seedPRCloseRetentionChecks(t *testing.T, store storage.Storage) {
	t.Helper()
	ctx := t.Context()

	checksToCreate := []*storage.Check{
		// Plan-only rows: deleted on every close kind regardless of status or conclusion.
		{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "staging", DatabaseType: "vitess", DatabaseName: "db1", Status: "pending"},
		{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "staging", DatabaseType: "mysql", DatabaseName: "db2", Status: "completed", Conclusion: "action_required"},
		// Apply-owned in-flight row: survives every close kind.
		{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "production", DatabaseType: "mysql", DatabaseName: "db3", Status: "in_progress", ApplyID: 77},
		// Apply-owned row that concluded successfully: retention depends on the close kind.
		{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "production", DatabaseType: "mysql", DatabaseName: "db4", Status: "completed", ApplyID: 88, Conclusion: "success"},
		// Apply-owned terminal rows that still block: survive every close kind.
		{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "production", DatabaseType: "mysql", DatabaseName: "db5", Status: "completed", ApplyID: 99, Conclusion: "action_required", BlockingReason: "schema_removed_after_apply_started"},
		{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "production", DatabaseType: "mysql", DatabaseName: "db6", Status: "completed", ApplyID: 111, Conclusion: "failure"},
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
}

// retainedChecksByDatabase asserts how many of PR 123's rows survived cleanup
// and returns them keyed by database name.
func retainedChecksByDatabase(t *testing.T, store storage.Storage, wantRetained int) map[string]*storage.Check {
	t.Helper()
	ctx := t.Context()

	retrieved, err := store.Checks().GetByPR(ctx, "org/repo", 123)
	require.NoError(t, err)
	require.Len(t, retrieved, wantRetained)

	byDatabase := make(map[string]*storage.Check, len(retrieved))
	for _, c := range retrieved {
		byDatabase[c.DatabaseName] = c
	}
	return byDatabase
}

// assertAlwaysBlockingRowsRetained verifies the apply-owned rows every close
// kind must retain: the in-flight row and the terminal rows that concluded
// without success (action_required, failure).
func assertAlwaysBlockingRowsRetained(t *testing.T, byDatabase map[string]*storage.Check) {
	t.Helper()

	inFlight, ok := byDatabase["db3"]
	require.True(t, ok, "apply-owned in-flight row must survive PR close")
	assert.Equal(t, "in_progress", inFlight.Status)
	assert.Equal(t, int64(77), inFlight.ApplyID)

	removedAfterApply, ok := byDatabase["db5"]
	require.True(t, ok, "apply-owned action_required row must survive PR close")
	assert.Equal(t, "completed", removedAfterApply.Status)
	assert.Equal(t, "action_required", removedAfterApply.Conclusion)
	assert.Equal(t, int64(99), removedAfterApply.ApplyID)
	assert.Equal(t, "schema_removed_after_apply_started", removedAfterApply.BlockingReason)

	failed, ok := byDatabase["db6"]
	require.True(t, ok, "apply-owned failed row must survive PR close")
	assert.Equal(t, "completed", failed.Status)
	assert.Equal(t, "failure", failed.Conclusion)
	assert.Equal(t, int64(111), failed.ApplyID)
}

// assertOtherPRUntouched verifies that PR-close cleanup for PR 123 never
// touches another PR's stored check state.
func assertOtherPRUntouched(t *testing.T, store storage.Storage) {
	t.Helper()

	retrieved, err := store.Checks().GetByPR(t.Context(), "org/repo", 456)
	require.NoError(t, err)
	require.Len(t, retrieved, 1)
	assert.Equal(t, "db1", retrieved[0].DatabaseName)
}

// TestCheckStore_DeleteByPRRetainingBlockingApplyOwned verifies PR-close cleanup
// at the storage layer. On every close kind, plan-only rows are deleted, rows
// for other PRs are untouched, and apply-owned rows that still block survive:
// an in_progress row must keep blocking until the apply reaches a terminal
// state, and a terminal row without a successful conclusion (action_required,
// failure) must keep blocking until an operator reconciles the target
// environment — closing and reopening the PR must not bypass either block.
//
// The close kinds differ on apply-owned rows that concluded successfully. A
// merged close deletes them: the merged PR carries the applied schema, so
// nothing remains to block. An unmerged close retains them: the stored success
// may predate a commit that removed the applied change (the PR can close
// before stale cleanup converts the row), so only reopen-time cleanup may
// decide whether the row still matches the PR contents.
func TestCheckStore_DeleteByPRRetainingBlockingApplyOwned(t *testing.T) {
	t.Run("merged close deletes successful apply-owned rows", func(t *testing.T) {
		clearTables(t)
		ctx := t.Context()
		store := New(testDB)
		seedPRCloseRetentionChecks(t, store)

		require.NoError(t, store.Checks().DeleteByPRRetainingBlockingApplyOwned(ctx, "org/repo", 123, true))

		byDatabase := retainedChecksByDatabase(t, store, 3)
		assertAlwaysBlockingRowsRetained(t, byDatabase)
		_, ok := byDatabase["db4"]
		assert.False(t, ok, "apply-owned success row is deleted on merged close")
		assertOtherPRUntouched(t, store)

		// Deleting for a non-existent PR is a no-op, not an error.
		require.NoError(t, store.Checks().DeleteByPRRetainingBlockingApplyOwned(ctx, "org/repo", 999, true))
	})

	t.Run("unmerged close retains successful apply-owned rows", func(t *testing.T) {
		clearTables(t)
		ctx := t.Context()
		store := New(testDB)
		seedPRCloseRetentionChecks(t, store)

		require.NoError(t, store.Checks().DeleteByPRRetainingBlockingApplyOwned(ctx, "org/repo", 123, false))

		byDatabase := retainedChecksByDatabase(t, store, 4)
		assertAlwaysBlockingRowsRetained(t, byDatabase)
		successful, ok := byDatabase["db4"]
		require.True(t, ok, "apply-owned success row must survive unmerged close")
		assert.Equal(t, "completed", successful.Status)
		assert.Equal(t, "success", successful.Conclusion)
		assert.Equal(t, int64(88), successful.ApplyID)
		assertOtherPRUntouched(t, store)

		// Deleting for a non-existent PR is a no-op, not an error.
		require.NoError(t, store.Checks().DeleteByPRRetainingBlockingApplyOwned(ctx, "org/repo", 999, false))
	})
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
	}, storage.PlanDriftClean)
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
	}, storage.PlanDriftClean)
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
	}, storage.PlanDriftClean)
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
			}, storage.PlanDriftClean)
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

// seedBlockedAggregateRow stores an aggregate check row carrying a blocking
// reason, as recorded when a PR-level guard fails the aggregate closed.
func seedBlockedAggregateRow(t *testing.T, store *Storage, headSHA, blockingReason string) {
	t.Helper()
	require.NoError(t, store.Checks().Upsert(t.Context(), &storage.Check{
		Repository:     "org/repo",
		PullRequest:    123,
		HeadSHA:        headSHA,
		Environment:    "_aggregate",
		DatabaseType:   "_aggregate",
		DatabaseName:   "_aggregate",
		CheckRunID:     200,
		HasChanges:     false,
		Status:         "completed",
		Conclusion:     "failure",
		BlockingReason: blockingReason,
		ErrorMessage:   "guard failed the aggregate closed",
	}))
}

// After the auto-plan guards re-verify a PR, the blocking reason on the stored
// aggregate row is released so fresh plan results can drive the aggregate
// again. The clear is pinned to the head SHA and reason the caller read.
func TestCheckStore_ClearAggregateBlockClearsMatchingRow(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	seedBlockedAggregateRow(t, store, "abc123", "managed_dir_missing_config")

	existing, err := store.Checks().Get(ctx, "org/repo", 123, "_aggregate", "_aggregate", "_aggregate")
	require.NoError(t, err)
	require.NotNil(t, existing)

	cleared, err := store.Checks().ClearAggregateBlock(ctx, existing)
	require.NoError(t, err)
	require.True(t, cleared)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "_aggregate", "_aggregate", "_aggregate")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Empty(t, retrieved.BlockingReason)
	assert.Empty(t, retrieved.ErrorMessage)
	assert.Equal(t, "failure", retrieved.Conclusion, "the clear releases only the block; conclusion is replaced by the next aggregate write")
}

// The clear is an optimistic-concurrency write: when another writer records a
// block for a newer commit between the caller's read and the clear, the head
// SHA no longer matches and the newer block stays authoritative.
func TestCheckStore_ClearAggregateBlockPreservesConcurrentlyMovedRow(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	seedBlockedAggregateRow(t, store, "abc123", "managed_dir_missing_config")

	stale, err := store.Checks().Get(ctx, "org/repo", 123, "_aggregate", "_aggregate", "_aggregate")
	require.NoError(t, err)
	require.NotNil(t, stale)

	// Another writer re-records the block on a newer commit before the clear.
	seedBlockedAggregateRow(t, store, "def456", "managed_dir_missing_config")

	cleared, err := store.Checks().ClearAggregateBlock(ctx, stale)
	require.NoError(t, err)
	require.False(t, cleared)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "_aggregate", "_aggregate", "_aggregate")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, "def456", retrieved.HeadSHA)
	assert.Equal(t, "managed_dir_missing_config", retrieved.BlockingReason)
}

// A block recorded for a different reason after the caller's read is a newer
// guard decision; the clear must not release it.
func TestCheckStore_ClearAggregateBlockPreservesDifferentReason(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	seedBlockedAggregateRow(t, store, "abc123", "managed_dir_missing_config")

	stale, err := store.Checks().Get(ctx, "org/repo", 123, "_aggregate", "_aggregate", "_aggregate")
	require.NoError(t, err)
	require.NotNil(t, stale)

	seedBlockedAggregateRow(t, store, "abc123", "schema_config_discovery_failed")

	cleared, err := store.Checks().ClearAggregateBlock(ctx, stale)
	require.NoError(t, err)
	require.False(t, cleared)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "_aggregate", "_aggregate", "_aggregate")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, "schema_config_discovery_failed", retrieved.BlockingReason)
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
	}, storage.PlanDriftClean)
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
	}, storage.PlanDriftClean)
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
	}, storage.PlanDriftClean)
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
	}, storage.PlanDriftClean)
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
	}, storage.PlanDriftClean))

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

	require.NoError(t, store.Checks().UpsertPlanResult(ctx, planCheck("5 created"), storage.PlanDriftClean))
	assert.Equal(t, "5 created", get().ChangeSummary)

	// A new plan with different changes overwrites the summary.
	require.NoError(t, store.Checks().UpsertPlanResult(ctx, planCheck("2 altered"), storage.PlanDriftClean))
	assert.Equal(t, "2 altered", get().ChangeSummary)

	// A new plan that finds no changes clears the summary.
	require.NoError(t, store.Checks().UpsertPlanResult(ctx, planCheck(""), storage.PlanDriftClean))
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

// A rollback that never claimed the stored check row (its claim failed or the
// driver crashed before it landed) must still be able to block the stale
// successful row left over from the apply it reverted; only a newer apply
// protects the row from the rollback's terminal write.
func TestCheckStore_MarkActionRequiredForApplyConvergesPriorApplyOwnedCheck(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	priorApply := createCheckStoreApply(t, store, "apply-succeeded", state.Apply.Completed)
	rollbackApply := createCheckStoreApply(t, store, "rollback-unclaimed", state.Apply.Completed)
	require.Greater(t, rollbackApply.ID, priorApply.ID)

	check := &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		ApplyID:      priorApply.ID,
		HasChanges:   false,
		Status:       "completed",
		Conclusion:   "success",
	}
	require.NoError(t, store.Checks().Upsert(ctx, check))

	check.HasChanges = true
	check.Conclusion = "action_required"
	updated, err := store.Checks().MarkActionRequiredForApply(ctx, check, rollbackApply)
	require.NoError(t, err)
	require.True(t, updated)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, "completed", retrieved.Status)
	assert.Equal(t, "action_required", retrieved.Conclusion)
	assert.True(t, retrieved.HasChanges)
	assert.Equal(t, int64(0), retrieved.ApplyID, "the rollback's terminal write releases check ownership")
}

// An unowned successful row (for example after a deliberate stale-cleanup
// unblock) still yields to a completed rollback's terminal write when the
// rollback is the newest apply for the target.
func TestCheckStore_MarkActionRequiredForApplyConvergesUnownedCheck(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	rollbackApply := createCheckStoreApply(t, store, "rollback-unclaimed", state.Apply.Completed)

	check := &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		HasChanges:   false,
		Status:       "completed",
		Conclusion:   "success",
	}
	require.NoError(t, store.Checks().Upsert(ctx, check))

	check.HasChanges = true
	check.Conclusion = "action_required"
	updated, err := store.Checks().MarkActionRequiredForApply(ctx, check, rollbackApply)
	require.NoError(t, err)
	require.True(t, updated)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "staging", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, "completed", retrieved.Status)
	assert.Equal(t, "action_required", retrieved.Conclusion)
	assert.True(t, retrieved.HasChanges)
	assert.Equal(t, int64(0), retrieved.ApplyID)
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

// A review-time deployment drift block is stored on a plan-only row as a first-
// class BlockingReason with conclusion=failure, so the aggregate fails closed.
func TestCheckStore_UpsertPlanResultStoresDriftBlock(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	err := store.Checks().UpsertPlanResult(ctx, &storage.Check{
		Repository:     "org/repo",
		PullRequest:    123,
		HeadSHA:        "sha1",
		Environment:    "production",
		DatabaseType:   "mysql",
		DatabaseName:   "testdb",
		HasChanges:     false,
		Status:         "completed",
		Conclusion:     "failure",
		BlockingReason: storage.ReviewTimeDeploymentDriftBlockingReason,
		ChangeSummary:  "drift blocks apply — diverged: au",
	}, storage.PlanDriftBlocked)
	require.NoError(t, err)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "production", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, "failure", retrieved.Conclusion)
	assert.Equal(t, storage.ReviewTimeDeploymentDriftBlockingReason, retrieved.BlockingReason)
}

// An apply-time plan (a not-evaluated write) must not clear a stored drift
// block: drift depends on live deployment state, not PR content, so an apply on
// the same head cannot re-open the gate. The head SHA is refreshed but the block
// is preserved.
func TestCheckStore_UpsertPlanResultNotEvaluatedPreservesDriftBlock(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	require.NoError(t, store.Checks().UpsertPlanResult(ctx, &storage.Check{
		Repository:     "org/repo",
		PullRequest:    123,
		HeadSHA:        "sha1",
		Environment:    "production",
		DatabaseType:   "mysql",
		DatabaseName:   "testdb",
		Status:         "completed",
		Conclusion:     "failure",
		BlockingReason: storage.ReviewTimeDeploymentDriftBlockingReason,
		ChangeSummary:  "drift blocks apply — diverged: au",
	}, storage.PlanDriftBlocked))

	// Apply-time plan on the same head: no drift evaluated, primary plan clean.
	err := store.Checks().UpsertPlanResult(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "sha1",
		Environment:  "production",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		HasChanges:   false,
		Status:       "completed",
		Conclusion:   "success",
	}, storage.PlanDriftNotEvaluated)
	require.NoError(t, err)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "production", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, "failure", retrieved.Conclusion, "drift block conclusion preserved")
	assert.Equal(t, storage.ReviewTimeDeploymentDriftBlockingReason, retrieved.BlockingReason, "drift block preserved")
	assert.Equal(t, "drift blocks apply — diverged: au", retrieved.ChangeSummary, "drift summary preserved")
}

// A fresh clean rollup (an evaluated write) clears a stored drift block: the
// deployments now match the reviewed plan, so the gate may pass.
func TestCheckStore_UpsertPlanResultCleanReplanClearsDriftBlock(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	require.NoError(t, store.Checks().UpsertPlanResult(ctx, &storage.Check{
		Repository:     "org/repo",
		PullRequest:    123,
		HeadSHA:        "sha1",
		Environment:    "production",
		DatabaseType:   "mysql",
		DatabaseName:   "testdb",
		Status:         "completed",
		Conclusion:     "failure",
		BlockingReason: storage.ReviewTimeDeploymentDriftBlockingReason,
		ChangeSummary:  "drift blocks apply — diverged: au",
	}, storage.PlanDriftBlocked))

	err := store.Checks().UpsertPlanResult(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "sha1",
		Environment:  "production",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		HasChanges:   false,
		Status:       "completed",
		Conclusion:   "success",
	}, storage.PlanDriftClean)
	require.NoError(t, err)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "production", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, "success", retrieved.Conclusion, "clean re-plan cleared the block")
	assert.Empty(t, retrieved.BlockingReason, "clean re-plan cleared the blocking reason")
}

// A not-evaluated write on a row that is not drift-blocked behaves like a normal
// plan-result replacement (no drift block to preserve).
func TestCheckStore_UpsertPlanResultNotEvaluatedReplacesNonDriftRow(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	require.NoError(t, store.Checks().UpsertPlanResult(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "sha1",
		Environment:  "production",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		HasChanges:   true,
		Status:       "completed",
		Conclusion:   "action_required",
	}, storage.PlanDriftClean))

	err := store.Checks().UpsertPlanResult(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "sha2",
		Environment:  "production",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		HasChanges:   false,
		Status:       "completed",
		Conclusion:   "success",
	}, storage.PlanDriftNotEvaluated)
	require.NoError(t, err)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "production", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, "sha2", retrieved.HeadSHA)
	assert.Equal(t, "success", retrieved.Conclusion, "non-drift row is replaced normally")
	assert.False(t, retrieved.HasChanges)
}

// Stale cleanup clears a plan-only drift block: once a later commit removes the
// database from the PR and no apply has started, the reviewed plan no longer
// gates the merge.
func TestCheckStore_MarkStalePlanSuccessfulClearsDriftBlock(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	require.NoError(t, store.Checks().UpsertPlanResult(ctx, &storage.Check{
		Repository:     "org/repo",
		PullRequest:    123,
		HeadSHA:        "sha1",
		Environment:    "production",
		DatabaseType:   "mysql",
		DatabaseName:   "testdb",
		Status:         "completed",
		Conclusion:     "failure",
		BlockingReason: storage.ReviewTimeDeploymentDriftBlockingReason,
		ChangeSummary:  "drift blocks apply — diverged: au",
	}, storage.PlanDriftBlocked))

	ok, err := store.Checks().MarkStalePlanSuccessful(ctx, &storage.Check{
		Repository:   "org/repo",
		PullRequest:  123,
		HeadSHA:      "sha1",
		Environment:  "production",
		DatabaseType: "mysql",
		DatabaseName: "testdb",
		HasChanges:   false,
		Status:       "completed",
		Conclusion:   "success",
	})
	require.NoError(t, err)
	require.True(t, ok)

	retrieved, err := store.Checks().Get(ctx, "org/repo", 123, "production", "mysql", "testdb")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, "success", retrieved.Conclusion)
	assert.Empty(t, retrieved.BlockingReason)
}
