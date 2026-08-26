package storagetest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// TestChecks runs the behavioral parity suite for storage.CheckStore: keyed
// upserts, aggregate and per-database row isolation, apply ownership, and
// review-time deployment drift disposition.
func TestChecks(t *testing.T, h Harness) {
	t.Run("Upsert", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		check := &storage.Check{
			Repository:     "org/repo",
			PullRequest:    123,
			HeadSHA:        "abc123",
			Environment:    "staging",
			DatabaseType:   storage.DatabaseTypeVitess,
			DatabaseName:   "orders",
			CheckRunID:     101,
			HasChanges:     true,
			Status:         "pending_apply",
			Conclusion:     "action_required",
			BlockingReason: "schema_change_pending",
			ErrorMessage:   "apply required",
			ChangeSummary:  "1 altered",
		}
		require.NoError(t, store.Checks().Upsert(ctx, check))

		stored, err := store.Checks().Get(ctx, "org/repo", 123, "staging", storage.DatabaseTypeVitess, "orders")
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Positive(t, stored.ID)
		assert.Equal(t, check.Repository, stored.Repository)
		assert.Equal(t, check.PullRequest, stored.PullRequest)
		assert.Equal(t, check.HeadSHA, stored.HeadSHA)
		assert.Equal(t, check.CheckRunID, stored.CheckRunID)
		assert.True(t, stored.HasChanges)
		assert.Equal(t, check.Status, stored.Status)
		assert.Equal(t, check.Conclusion, stored.Conclusion)
		assert.Equal(t, check.BlockingReason, stored.BlockingReason)
		assert.Equal(t, check.ErrorMessage, stored.ErrorMessage)
		assert.Equal(t, check.ChangeSummary, stored.ChangeSummary)
		assert.NotZero(t, stored.CreatedAt)
		assert.NotZero(t, stored.UpdatedAt)

		check.HeadSHA = "def456"
		check.CheckRunID = 202
		check.HasChanges = false
		check.Status = "completed"
		check.Conclusion = "success"
		check.BlockingReason = ""
		check.ErrorMessage = ""
		check.ChangeSummary = ""
		require.NoError(t, store.Checks().Upsert(ctx, check))

		stored, err = store.Checks().Get(ctx, "org/repo", 123, "staging", storage.DatabaseTypeVitess, "orders")
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "def456", stored.HeadSHA)
		assert.Equal(t, int64(202), stored.CheckRunID)
		assert.False(t, stored.HasChanges)
		assert.Equal(t, "completed", stored.Status)
		assert.Equal(t, "success", stored.Conclusion)
		assert.Empty(t, stored.BlockingReason)
		assert.Empty(t, stored.ErrorMessage)
		assert.Equal(t, "1 altered", stored.ChangeSummary, "ordinary state transitions preserve the plan summary")

		byRunID, err := store.Checks().GetByCheckRunID(ctx, 202)
		require.NoError(t, err)
		require.NotNil(t, byRunID)
		assert.Equal(t, stored.ID, byRunID.ID)
		missingByRunID, err := store.Checks().GetByCheckRunID(ctx, 999)
		require.NoError(t, err)
		assert.Nil(t, missingByRunID)

		missing, err := store.Checks().Get(ctx, "org/repo", 999, "staging", storage.DatabaseTypeVitess, "orders")
		require.NoError(t, err)
		assert.Nil(t, missing)
	})

	t.Run("AggregateAndPerDatabaseRows", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		emptyByPR, err := store.Checks().GetByPR(ctx, "org/repo", 123)
		require.NoError(t, err)
		assert.Empty(t, emptyByPR)
		emptyByDatabase, err := store.Checks().GetByDatabase(ctx, "org/repo", "staging", storage.DatabaseTypeMySQL, "orders")
		require.NoError(t, err)
		assert.Empty(t, emptyByDatabase)

		rows := []*storage.Check{
			{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "_aggregate", DatabaseType: "_aggregate", DatabaseName: "_aggregate", CheckRunID: 500, Status: "completed", Conclusion: "failure"},
			{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "production", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "orders", Status: "completed", Conclusion: "action_required", HasChanges: true},
			{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "orders", Status: "completed", Conclusion: "success"},
			{Repository: "org/repo", PullRequest: 456, HeadSHA: "def", Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "orders", Status: "completed", Conclusion: "success"},
		}
		for _, row := range rows {
			require.NoError(t, store.Checks().Upsert(ctx, row))
		}
		zeroRunID, err := store.Checks().GetByCheckRunID(ctx, 0)
		require.NoError(t, err)
		assert.Nil(t, zeroRunID)

		checks, err := store.Checks().GetByPR(ctx, "org/repo", 123)
		require.NoError(t, err)
		require.Len(t, checks, 3)
		assert.Equal(t, []string{"_aggregate", "production", "staging"}, []string{checks[0].Environment, checks[1].Environment, checks[2].Environment})

		byDatabase, err := store.Checks().GetByDatabase(ctx, "org/repo", "staging", storage.DatabaseTypeMySQL, "orders")
		require.NoError(t, err)
		require.Len(t, byDatabase, 2)
		assert.Equal(t, []int{123, 456}, []int{byDatabase[0].PullRequest, byDatabase[1].PullRequest})

		aggregate, err := store.Checks().Get(ctx, "org/repo", 123, "_aggregate", "_aggregate", "_aggregate")
		require.NoError(t, err)
		require.NotNil(t, aggregate)
		assert.Equal(t, int64(500), aggregate.CheckRunID)

		aggregateID := aggregate.ID
		require.NoError(t, store.Checks().Delete(ctx, aggregateID))
		aggregate, err = store.Checks().Get(ctx, "org/repo", 123, "_aggregate", "_aggregate", "_aggregate")
		require.NoError(t, err)
		assert.Nil(t, aggregate)

		perDatabase, err := store.Checks().Get(ctx, "org/repo", 123, "production", storage.DatabaseTypeMySQL, "orders")
		require.NoError(t, err)
		assert.NotNil(t, perDatabase, "deleting the aggregate row must not remove per-database state")
		require.ErrorIs(t, store.Checks().Delete(ctx, aggregateID), storage.ErrCheckNotFound)
	})

	t.Run("UpsertPlanResultPreservesApplyOwnership", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "ownership_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "apply-owned-check", 700, state.Apply.Running, "staging")
		require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
			Repository: "org/repo", PullRequest: 123, HeadSHA: "old-sha",
			Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "ownership_db",
			ApplyID: apply.ID, HasChanges: true, Status: "in_progress",
		}))

		require.NoError(t, store.Checks().UpsertPlanResult(ctx, &storage.Check{
			Repository: "org/repo", PullRequest: 123, HeadSHA: "new-sha",
			Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "ownership_db",
			HasChanges: false, Status: "completed", Conclusion: "success",
		}, storage.PlanDriftClean))

		stored, err := store.Checks().Get(ctx, "org/repo", 123, "staging", storage.DatabaseTypeMySQL, "ownership_db")
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "old-sha", stored.HeadSHA)
		assert.Equal(t, "in_progress", stored.Status)
		assert.Empty(t, stored.Conclusion)
		assert.True(t, stored.HasChanges)
		assert.Equal(t, apply.ID, stored.ApplyID)

		completion := *stored
		completion.Status = "completed"
		completion.Conclusion = "success"
		completion.HasChanges = false
		updated, err := store.Checks().CompleteForApply(ctx, &completion, apply)
		require.NoError(t, err)
		require.True(t, updated)

		stored, err = store.Checks().Get(ctx, "org/repo", 123, "staging", storage.DatabaseTypeMySQL, "ownership_db")
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "completed", stored.Status)
		assert.Equal(t, "success", stored.Conclusion)
		assert.Equal(t, apply.ID, stored.ApplyID)
	})

	t.Run("UpsertPlanResultReplacesUnownedState", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		key := &storage.Check{
			Repository: "org/repo", PullRequest: 123, HeadSHA: "old-sha",
			Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "unowned_db",
			HasChanges: true, Status: "in_progress",
		}
		require.NoError(t, store.Checks().Upsert(ctx, key))

		key.HeadSHA = "new-sha"
		key.HasChanges = false
		key.Status = "completed"
		key.Conclusion = "success"
		require.NoError(t, store.Checks().UpsertPlanResult(ctx, key, storage.PlanDriftClean))

		stored, err := store.Checks().Get(ctx, "org/repo", 123, "staging", storage.DatabaseTypeMySQL, "unowned_db")
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "new-sha", stored.HeadSHA)
		assert.Equal(t, "completed", stored.Status)
		assert.Equal(t, "success", stored.Conclusion)
		assert.Zero(t, stored.ApplyID)
	})

	t.Run("RecoverApplyOwnedCheckWithNoOpPlan", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "recovery_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "apply-noop-check", 701, state.Apply.Running, "staging")
		require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
			Repository: "org/repo", PullRequest: 123, HeadSHA: "same-sha",
			Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "recovery_db",
			ApplyID: apply.ID, HasChanges: true, Status: "in_progress", BlockingReason: "schema_change_running",
		}))

		noOp := &storage.Check{
			Repository: "org/repo", PullRequest: 123, HeadSHA: "same-sha",
			Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "recovery_db",
			HasChanges: false, Status: "completed", Conclusion: "success",
		}
		require.NoError(t, store.Checks().UpsertPlanResult(ctx, noOp, storage.PlanDriftClean))
		recovered, err := store.Checks().RecoverApplyOwnedCheckWithNoOpPlan(ctx, noOp)
		require.NoError(t, err)
		require.True(t, recovered)

		stored, err := store.Checks().Get(ctx, "org/repo", 123, "staging", storage.DatabaseTypeMySQL, "recovery_db")
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "completed", stored.Status)
		assert.Equal(t, "success", stored.Conclusion)
		assert.False(t, stored.HasChanges)
		assert.Zero(t, stored.ApplyID)
		assert.Empty(t, stored.BlockingReason)
	})

	t.Run("RecoverApplyOwnedCheckRequiresNoOpSuccess", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "recovery_guard_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "recovery-guard", 702, state.Apply.Running, "staging")
		require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
			Repository: "org/repo", PullRequest: 123, HeadSHA: "same-sha",
			Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "recovery_guard_db",
			ApplyID: apply.ID, HasChanges: true, Status: "in_progress", BlockingReason: "schema_change_running",
		}))

		// Only a successful no-op plan proves the target already matches the
		// PR schema; any other plan result must leave the apply owning the row.
		for name, plan := range map[string]*storage.Check{
			"PlanWithChanges": {Status: "completed", Conclusion: "success", HasChanges: true},
			"FailedPlan":      {Status: "completed", Conclusion: "failure", HasChanges: false},
			"InProgressPlan":  {Status: "in_progress", HasChanges: false},
		} {
			plan.Repository = "org/repo"
			plan.PullRequest = 123
			plan.HeadSHA = "same-sha"
			plan.Environment = "staging"
			plan.DatabaseType = storage.DatabaseTypeMySQL
			plan.DatabaseName = "recovery_guard_db"
			recovered, err := store.Checks().RecoverApplyOwnedCheckWithNoOpPlan(ctx, plan)
			require.NoError(t, err, name)
			assert.False(t, recovered, name)
		}

		stored, err := store.Checks().Get(ctx, "org/repo", 123, "staging", storage.DatabaseTypeMySQL, "recovery_guard_db")
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "in_progress", stored.Status)
		assert.Equal(t, apply.ID, stored.ApplyID)
		assert.Equal(t, "schema_change_running", stored.BlockingReason)
	})

	t.Run("CompleteForApplyGuards", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "complete_guard_db", storage.DatabaseTypeMySQL)
		owner := CreateApplyWithStateAndEnv(t, store, lock, "complete-owner", 703, state.Apply.Running, "staging")
		check := &storage.Check{
			Repository: lock.Repository, PullRequest: lock.PullRequest, HeadSHA: "abc",
			Environment: "staging", DatabaseType: lock.DatabaseType, DatabaseName: lock.DatabaseName,
			ApplyID: owner.ID, HasChanges: true, Status: "in_progress",
		}
		require.NoError(t, store.Checks().Upsert(ctx, check))

		completion := *check
		completion.Status = "completed"
		completion.Conclusion = "success"
		completion.HasChanges = false

		// An apply that does not own the row must not complete it.
		intruder := CreateApplyWithStateAndEnv(t, store, lock, "complete-intruder", 704, state.Apply.Completed, "staging")
		updated, err := store.Checks().CompleteForApply(ctx, &completion, intruder)
		require.NoError(t, err)
		assert.False(t, updated, "completion by a non-owning apply must not land")

		// Once a newer apply exists for the same key, the owner's completion
		// must not land either: the newer apply's outcome is authoritative.
		updated, err = store.Checks().CompleteForApply(ctx, &completion, owner)
		require.NoError(t, err)
		assert.False(t, updated, "completion must not overwrite state a newer apply owns")

		stored, err := store.Checks().Get(ctx, check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "in_progress", stored.Status)
		assert.Empty(t, stored.Conclusion)
		assert.Equal(t, owner.ID, stored.ApplyID)
	})

	t.Run("MarkActionRequiredForApplyGuards", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "action_guard_db", storage.DatabaseTypeMySQL)
		rollback := CreateApplyWithStateAndEnv(t, store, lock, "action-rollback", 705, state.Apply.Completed, "staging")
		check := &storage.Check{
			Repository: lock.Repository, PullRequest: lock.PullRequest, HeadSHA: "abc",
			Environment: "staging", DatabaseType: lock.DatabaseType, DatabaseName: lock.DatabaseName,
			ApplyID: rollback.ID + 1000, Status: "completed", Conclusion: "success",
		}
		require.NoError(t, store.Checks().Upsert(ctx, check))

		demand := *check
		demand.HasChanges = true
		demand.Conclusion = "action_required"

		// A row owned by an apply newer than the rollback stays untouched even
		// when the applies table holds nothing newer for the key.
		updated, err := store.Checks().MarkActionRequiredForApply(ctx, &demand, rollback)
		require.NoError(t, err)
		assert.False(t, updated, "a rollback must not overwrite a row owned by a newer apply")

		// Release ownership, then start a newer apply for the same key: the
		// rollback's action_required must not pull the rug out from under a
		// re-apply that started after it.
		check.ApplyID = 0
		require.NoError(t, store.Checks().Upsert(ctx, check))
		CreateApplyWithStateAndEnv(t, store, lock, "action-reapply", 706, state.Apply.Running, "staging")
		updated, err = store.Checks().MarkActionRequiredForApply(ctx, &demand, rollback)
		require.NoError(t, err)
		assert.False(t, updated, "a rollback must not overwrite state once a newer apply exists")

		stored, err := store.Checks().Get(ctx, check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "success", stored.Conclusion)
	})

	t.Run("PlanDriftDisposition", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		blocked := &storage.Check{
			Repository: "org/repo", PullRequest: 123, HeadSHA: "sha-1",
			Environment: "production", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "drift_db",
			Status: "completed", Conclusion: "failure",
			BlockingReason: storage.ReviewTimeDeploymentDriftBlockingReason,
			ChangeSummary:  "drift blocks apply",
		}
		require.NoError(t, store.Checks().UpsertPlanResult(ctx, blocked, storage.PlanDriftBlocked))

		notEvaluated := &storage.Check{
			Repository: "org/repo", PullRequest: 123, HeadSHA: "sha-2",
			Environment: "production", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "drift_db",
			Status: "completed", Conclusion: "success",
		}
		require.NoError(t, store.Checks().UpsertPlanResult(ctx, notEvaluated, storage.PlanDriftNotEvaluated))

		stored, err := store.Checks().Get(ctx, "org/repo", 123, "production", storage.DatabaseTypeMySQL, "drift_db")
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "sha-2", stored.HeadSHA)
		assert.Equal(t, "failure", stored.Conclusion)
		assert.Equal(t, storage.ReviewTimeDeploymentDriftBlockingReason, stored.BlockingReason)
		assert.Equal(t, "drift blocks apply", stored.ChangeSummary)

		predicateBase := *notEvaluated
		predicateBase.DatabaseName = "drift_predicate_db"
		predicateBase.ErrorMessage = "old error"
		require.NoError(t, store.Checks().UpsertPlanResult(ctx, &predicateBase, storage.PlanDriftClean))
		incomingBlock := predicateBase
		incomingBlock.HeadSHA = "sha-block"
		incomingBlock.BlockingReason = storage.ReviewTimeDeploymentDriftBlockingReason
		incomingBlock.ErrorMessage = "new error"
		require.NoError(t, store.Checks().UpsertPlanResult(ctx, &incomingBlock, storage.PlanDriftNotEvaluated))
		stored, err = store.Checks().Get(ctx, "org/repo", 123, "production", storage.DatabaseTypeMySQL, "drift_predicate_db")
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "new error", stored.ErrorMessage, "preservation keys on the stored blocking reason, not the incoming write")
		assert.Equal(t, storage.ReviewTimeDeploymentDriftBlockingReason, stored.BlockingReason)

		clean := *notEvaluated
		clean.HeadSHA = "sha-3"
		require.NoError(t, store.Checks().UpsertPlanResult(ctx, &clean, storage.PlanDriftClean))

		stored, err = store.Checks().Get(ctx, "org/repo", 123, "production", storage.DatabaseTypeMySQL, "drift_db")
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "sha-3", stored.HeadSHA)
		assert.Equal(t, "success", stored.Conclusion)
		assert.Empty(t, stored.BlockingReason)
		assert.Empty(t, stored.ChangeSummary)
	})

	t.Run("MarkStalePlanSuccessful", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		planOnly := &storage.Check{Repository: "org/repo", PullRequest: 123, HeadSHA: "old", Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "stale_db", HasChanges: true, Status: "completed", Conclusion: "action_required"}
		require.NoError(t, store.Checks().Upsert(ctx, planOnly))
		success := *planOnly
		success.HeadSHA = "new"
		success.HasChanges = false
		success.Conclusion = "success"
		marked, err := store.Checks().MarkStalePlanSuccessful(ctx, &success)
		require.NoError(t, err)
		assert.True(t, marked)

		lock := CreateLock(t, store, "guarded_stale_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "stale-guard", 710, state.Apply.Running, "staging")
		guarded := &storage.Check{Repository: "org/repo", PullRequest: lock.PullRequest, HeadSHA: "old", Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "guarded_stale_db", ApplyID: apply.ID, HasChanges: true, Status: "in_progress"}
		require.NoError(t, store.Checks().Upsert(ctx, guarded))
		guarded.HeadSHA = "new"
		guarded.ApplyID = 0
		guarded.HasChanges = false
		guarded.Status = "completed"
		guarded.Conclusion = "success"
		marked, err = store.Checks().MarkStalePlanSuccessful(ctx, guarded)
		require.NoError(t, err)
		assert.False(t, marked)

		// Apply ownership guards independently of status: a completed
		// apply-owned row still records that an apply reached the live
		// database, so stale cleanup must not convert it to a passing check.
		owned := &storage.Check{Repository: "org/repo", PullRequest: 123, HeadSHA: "old", Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "owned_completed_stale_db", ApplyID: apply.ID, Status: "completed", Conclusion: "action_required"}
		require.NoError(t, store.Checks().Upsert(ctx, owned))
		owned.HeadSHA = "new"
		owned.ApplyID = 0
		owned.Conclusion = "success"
		marked, err = store.Checks().MarkStalePlanSuccessful(ctx, owned)
		require.NoError(t, err)
		assert.False(t, marked)
		stored, err := store.Checks().Get(ctx, "org/repo", 123, "staging", storage.DatabaseTypeMySQL, "owned_completed_stale_db")
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "action_required", stored.Conclusion)
		assert.Equal(t, apply.ID, stored.ApplyID)
	})

	t.Run("ClearAggregateBlock", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		check := &storage.Check{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "_aggregate", DatabaseType: "_aggregate", DatabaseName: "_aggregate", Status: "completed", Conclusion: "failure", BlockingReason: "guard_failed", ErrorMessage: "blocked"}
		require.NoError(t, store.Checks().Upsert(ctx, check))
		stale := *check
		stale.HeadSHA = "old"
		cleared, err := store.Checks().ClearAggregateBlock(ctx, &stale)
		require.NoError(t, err)
		assert.False(t, cleared)

		// The clear is pinned to the blocking reason the caller read: a block
		// recorded concurrently for a different reason must be preserved.
		mismatchedReason := *check
		mismatchedReason.BlockingReason = "other_guard_failed"
		cleared, err = store.Checks().ClearAggregateBlock(ctx, &mismatchedReason)
		require.NoError(t, err)
		assert.False(t, cleared)
		stored, err := store.Checks().Get(ctx, "org/repo", 123, "_aggregate", "_aggregate", "_aggregate")
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "guard_failed", stored.BlockingReason)

		cleared, err = store.Checks().ClearAggregateBlock(ctx, check)
		require.NoError(t, err)
		assert.True(t, cleared)
	})

	t.Run("ApplyLeaseGuards", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "lease_check_db", storage.DatabaseTypeMySQL)
		apply := CreateClaimedApply(t, store, lock, "lease-check", 711, "current-driver")
		check := &storage.Check{Repository: lock.Repository, PullRequest: lock.PullRequest, HeadSHA: "abc", Environment: apply.Environment, DatabaseType: lock.DatabaseType, DatabaseName: lock.DatabaseName, ApplyID: apply.ID, HasChanges: true, Status: "in_progress"}
		require.NoError(t, store.Checks().Upsert(ctx, check))
		completion := *check
		completion.HasChanges = false
		completion.Status = "completed"
		completion.Conclusion = "success"
		staleApply := *apply
		staleApply.LeaseToken = "stale-token"
		updated, err := store.Checks().CompleteForApply(ctx, &completion, &staleApply)
		require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
		assert.False(t, updated)
		updated, err = store.Checks().CompleteForApply(ctx, &completion, apply)
		require.NoError(t, err)
		assert.True(t, updated)
	})

	t.Run("MarkActionRequiredForApply", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "action_required_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "rollback-check", 712, state.Apply.Completed, "staging")
		check := &storage.Check{Repository: lock.Repository, PullRequest: lock.PullRequest, HeadSHA: "abc", Environment: "staging", DatabaseType: lock.DatabaseType, DatabaseName: lock.DatabaseName, ApplyID: apply.ID, Status: "completed", Conclusion: "success"}
		require.NoError(t, store.Checks().Upsert(ctx, check))
		check.HasChanges = true
		check.Conclusion = "action_required"
		updated, err := store.Checks().MarkActionRequiredForApply(ctx, check, apply)
		require.NoError(t, err)
		assert.True(t, updated)
		stored, err := store.Checks().Get(ctx, check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "action_required", stored.Conclusion)
		assert.Zero(t, stored.ApplyID)
	})

	t.Run("DeleteByPRRetainingBlockingApplyOwned", func(t *testing.T) {
		for _, merged := range []bool{true, false} {
			t.Run(map[bool]string{true: "Merged", false: "Unmerged"}[merged], func(t *testing.T) {
				ctx := t.Context()
				store := h.NewStorage(t)
				rows := []*storage.Check{
					{Repository: "org/repo", PullRequest: 123, Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "plan_only", Status: "completed"},
					{Repository: "org/repo", PullRequest: 123, Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "blocking", ApplyID: 1, Status: "completed", Conclusion: "failure"},
					{Repository: "org/repo", PullRequest: 123, Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "successful", ApplyID: 2, Status: "completed", Conclusion: "success"},
					{Repository: "org/repo", PullRequest: 123, Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "applying", ApplyID: 3, Status: "in_progress", Conclusion: "success"},
					{Repository: "org/repo", PullRequest: 456, Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "other_pr", Status: "completed"},
				}
				for _, row := range rows {
					require.NoError(t, store.Checks().Upsert(ctx, row))
				}
				require.NoError(t, store.Checks().DeleteByPRRetainingBlockingApplyOwned(ctx, "org/repo", 123, merged))
				remaining, err := store.Checks().GetByPR(ctx, "org/repo", 123)
				require.NoError(t, err)
				names := make([]string, len(remaining))
				for i, row := range remaining {
					names[i] = row.DatabaseName
				}
				if merged {
					assert.Equal(t, []string{"applying", "blocking"}, names, "an in_progress apply-owned row is retained on a merged close even when concluded success")
				} else {
					assert.Equal(t, []string{"applying", "blocking", "successful"}, names)
				}

				otherPR, err := store.Checks().GetByPR(ctx, "org/repo", 456)
				require.NoError(t, err)
				require.Len(t, otherPR, 1, "closing one PR must not touch another PR's stored check state")
			})
		}
	})

	t.Run("Upsert_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		err := store.Checks().Upsert(t.Context(), &storage.Check{
			Repository: "org/repo", PullRequest: 123, Environment: "staging",
			DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "errors_db",
		})
		require.Error(t, err)
	})
	check := &storage.Check{Repository: "org/repo", PullRequest: 123, Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "errors_db", Status: "completed", Conclusion: "success"}
	apply := &storage.Apply{ID: 1}
	t.Run("UpsertPlanResult_DBError", func(t *testing.T) {
		require.Error(t, h.NewUnreachableStorage(t).Checks().UpsertPlanResult(t.Context(), check, storage.PlanDriftClean))
	})
	t.Run("RecoverApplyOwnedCheckWithNoOpPlan_DBError", func(t *testing.T) {
		_, err := h.NewUnreachableStorage(t).Checks().RecoverApplyOwnedCheckWithNoOpPlan(t.Context(), check)
		require.Error(t, err)
	})
	t.Run("MarkStalePlanSuccessful_DBError", func(t *testing.T) {
		_, err := h.NewUnreachableStorage(t).Checks().MarkStalePlanSuccessful(t.Context(), check)
		require.Error(t, err)
	})
	t.Run("ClearAggregateBlock_DBError", func(t *testing.T) {
		_, err := h.NewUnreachableStorage(t).Checks().ClearAggregateBlock(t.Context(), check)
		require.Error(t, err)
	})
	t.Run("CompleteForApply_DBError", func(t *testing.T) {
		_, err := h.NewUnreachableStorage(t).Checks().CompleteForApply(t.Context(), check, apply)
		require.Error(t, err)
	})
	t.Run("MarkActionRequiredForApply_DBError", func(t *testing.T) {
		_, err := h.NewUnreachableStorage(t).Checks().MarkActionRequiredForApply(t.Context(), check, apply)
		require.Error(t, err)
	})
	t.Run("Get_DBError", func(t *testing.T) {
		_, err := h.NewUnreachableStorage(t).Checks().Get(t.Context(), "org/repo", 123, "staging", storage.DatabaseTypeMySQL, "errors_db")
		require.Error(t, err)
	})
	t.Run("GetByCheckRunID_DBError", func(t *testing.T) {
		_, err := h.NewUnreachableStorage(t).Checks().GetByCheckRunID(t.Context(), 1)
		require.Error(t, err)
	})
	t.Run("GetByPR_DBError", func(t *testing.T) {
		_, err := h.NewUnreachableStorage(t).Checks().GetByPR(t.Context(), "org/repo", 123)
		require.Error(t, err)
	})
	t.Run("GetByDatabase_DBError", func(t *testing.T) {
		_, err := h.NewUnreachableStorage(t).Checks().GetByDatabase(t.Context(), "org/repo", "staging", storage.DatabaseTypeMySQL, "errors_db")
		require.Error(t, err)
	})
	t.Run("Delete_DBError", func(t *testing.T) { require.Error(t, h.NewUnreachableStorage(t).Checks().Delete(t.Context(), 1)) })
	t.Run("DeleteByPRRetainingBlockingApplyOwned_DBError", func(t *testing.T) {
		require.Error(t, h.NewUnreachableStorage(t).Checks().DeleteByPRRetainingBlockingApplyOwned(t.Context(), "org/repo", 123, false))
	})
}
