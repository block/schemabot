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

		missing, err := store.Checks().Get(ctx, "org/repo", 999, "staging", storage.DatabaseTypeVitess, "orders")
		require.NoError(t, err)
		assert.Nil(t, missing)
	})

	t.Run("AggregateAndPerDatabaseRows", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		rows := []*storage.Check{
			{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "_aggregate", DatabaseType: "_aggregate", DatabaseName: "_aggregate", CheckRunID: 500, Status: "completed", Conclusion: "failure"},
			{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "production", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "orders", Status: "completed", Conclusion: "action_required", HasChanges: true},
			{Repository: "org/repo", PullRequest: 123, HeadSHA: "abc", Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "orders", Status: "completed", Conclusion: "success"},
			{Repository: "org/repo", PullRequest: 456, HeadSHA: "def", Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "orders", Status: "completed", Conclusion: "success"},
		}
		for _, row := range rows {
			require.NoError(t, store.Checks().Upsert(ctx, row))
		}

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

	t.Run("Errors", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)

		err := store.Checks().Upsert(t.Context(), &storage.Check{
			Repository: "org/repo", PullRequest: 123, Environment: "staging",
			DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "errors_db",
		})
		require.Error(t, err)

		_, err = store.Checks().GetByPR(t.Context(), "org/repo", 123)
		require.Error(t, err)
	})
}
