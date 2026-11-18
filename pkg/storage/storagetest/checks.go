package storagetest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/checkstate"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// TestChecks runs the behavioral parity suite for storage.CheckStore: keyed
// upserts, aggregate and per-database row isolation, apply ownership, and
// review-time deployment drift disposition.
func TestChecks(t *testing.T, h Harness) {
	t.Run("CanonicalIdentityKey", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		check := &storage.Check{
			Repository: "MixedCase/Sample-Repo", PullRequest: 123, HeadSHA: "abc123",
			Environment: "Staging", DatabaseType: "MySQL", DatabaseName: "Orders_DB",
			Status: "completed", Conclusion: "success",
		}
		require.NoError(t, store.Checks().Upsert(ctx, check))

		stored, err := store.Checks().Get(ctx, "MIXEDCASE/SAMPLE-REPO", 123, "STAGING", "MYSQL", "ORDERS_DB")
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "mixedcase/sample-repo", stored.Repository)
		assert.Equal(t, "staging", stored.Environment)
		assert.Equal(t, "mysql", stored.DatabaseType)
		assert.Equal(t, "orders_db", stored.DatabaseName)

		check.Repository = "MIXEDCASE/SAMPLE-REPO"
		check.Environment = "STAGING"
		check.DatabaseType = "MYSQL"
		check.DatabaseName = "ORDERS_DB"
		check.HeadSHA = "def456"
		require.NoError(t, store.Checks().Upsert(ctx, check))

		checks, err := store.Checks().GetByDatabase(ctx, "MixedCase/Sample-Repo", "Staging", "MySQL", "Orders_DB")
		require.NoError(t, err)
		require.Len(t, checks, 1)
		assert.Equal(t, "def456", checks[0].HeadSHA)

		checks, err = store.Checks().GetByPR(ctx, "MIXEDCASE/SAMPLE-REPO", 123)
		require.NoError(t, err)
		require.Len(t, checks, 1)
		require.NoError(t, store.Checks().DeleteByPRRetainingBlockingApplyOwned(ctx, "MixedCase/Sample-Repo", 123, false))
		checks, err = store.Checks().GetByPR(ctx, "mixedcase/sample-repo", 123)
		require.NoError(t, err)
		assert.Empty(t, checks)
	})

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

		planStored, err := store.Checks().UpsertPlanResult(ctx, &storage.Check{
			Repository: "org/repo", PullRequest: 123, HeadSHA: "new-sha",
			Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "ownership_db",
			HasChanges: false, Status: "completed", Conclusion: "success",
		}, storage.PlanDriftClean)
		require.NoError(t, err)
		assert.False(t, planStored, "a write the ownership guard refuses must report that it did not land")

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
		planStored, err := store.Checks().UpsertPlanResult(ctx, key, storage.PlanDriftClean)
		require.NoError(t, err)
		assert.True(t, planStored)

		// Rewriting the row with the values it already holds changes no columns.
		// The write still landed, and must not be reported as a refusal.
		planStored, err = store.Checks().UpsertPlanResult(ctx, key, storage.PlanDriftClean)
		require.NoError(t, err)
		assert.True(t, planStored, "a repeated plan write that changes nothing still landed")

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
		// The ownership guard refuses this write — which is what the explicit
		// recovery path below exists to resolve for a no-op plan on the same head.
		planStored, err := store.Checks().UpsertPlanResult(ctx, noOp, storage.PlanDriftClean)
		require.NoError(t, err)
		require.False(t, planStored)

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

	// A completed task under the cancelled apply is durable evidence that the
	// target may have changed, so the ownership-releasing write must fail closed.
	t.Run("MarkActionRequiredSkipsCancelledApplyWithCompletedTask", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "cancel_guard_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "cancel-partial", 707, state.Apply.Cancelled, "staging")
		createCompletedTask(t, store, apply, "task-completed-before-cancel")
		check := &storage.Check{
			Repository: lock.Repository, PullRequest: lock.PullRequest, HeadSHA: "abc",
			Environment: "staging", DatabaseType: lock.DatabaseType, DatabaseName: lock.DatabaseName,
			ApplyID: apply.ID, HasChanges: true, Status: "in_progress",
		}
		require.NoError(t, store.Checks().Upsert(ctx, check))

		check.Status = "completed"
		check.Conclusion = "action_required"
		updated, err := store.Checks().MarkActionRequiredForApply(ctx, check, apply)
		require.NoError(t, err)
		assert.False(t, updated, "an ownership release must fail closed while completed forward work exists")

		stored, err := store.Checks().Get(ctx, lock.Repository, lock.PullRequest, "staging", lock.DatabaseType, lock.DatabaseName)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, apply.ID, stored.ApplyID)
		assert.Equal(t, "in_progress", stored.Status)
	})

	// A driver that cancels a forward apply before any task completed releases
	// the check while still holding the apply lease, so the write carries both
	// the no-completed-task guard and the lease fence. The check is released.
	t.Run("MarkActionRequiredReleasesCancelledApplyUnderItsLease", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "cancel_release_lease_db", storage.DatabaseTypeMySQL)
		apply := CreateClaimedApply(t, store, lock, "cancel-release-under-lease", 713, "driver-a")
		require.True(t, apply.Lease().Valid(), "the cancelled apply's driver still holds its lease")
		apply.State = state.Apply.Cancelled
		check := &storage.Check{
			Repository: lock.Repository, PullRequest: lock.PullRequest, HeadSHA: "abc",
			Environment: apply.Environment, DatabaseType: lock.DatabaseType, DatabaseName: lock.DatabaseName,
			ApplyID: apply.ID, HasChanges: true, Status: "in_progress",
		}
		require.NoError(t, store.Checks().Upsert(ctx, check))

		check.Status = "completed"
		check.Conclusion = "action_required"
		updated, err := store.Checks().MarkActionRequiredForApply(ctx, check, apply)
		require.NoError(t, err)
		assert.True(t, updated)

		stored, err := store.Checks().Get(ctx, lock.Repository, lock.PullRequest, apply.Environment, lock.DatabaseType, lock.DatabaseName)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "action_required", stored.Conclusion)
		assert.Zero(t, stored.ApplyID, "a safely cancelled forward apply releases its ownership of the check")
	})

	// Retained cancellation state requires completed forward-task evidence and
	// remains owned by the cancelled apply, including when reconciliation
	// repairs an older completed failure row.
	t.Run("MarkCancelledApplyFailedRetainsOwnership", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "cancel_retain_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "cancel-retained", 708, state.Apply.Cancelled, "staging")
		createCompletedTask(t, store, apply, "task-completed-retained")
		check := &storage.Check{
			Repository: lock.Repository, PullRequest: lock.PullRequest, HeadSHA: "abc",
			Environment: "staging", DatabaseType: lock.DatabaseType, DatabaseName: lock.DatabaseName,
			ApplyID: apply.ID, HasChanges: true, Status: "completed", Conclusion: "failure",
		}
		require.NoError(t, store.Checks().Upsert(ctx, check))

		check.BlockingReason = "apply_cancelled_after_task_completed"
		check.ErrorMessage = "reconciliation required"
		updated, err := store.Checks().MarkCancelledApplyFailed(ctx, check, apply)
		require.NoError(t, err)
		assert.True(t, updated)

		stored, err := store.Checks().Get(ctx, lock.Repository, lock.PullRequest, "staging", lock.DatabaseType, lock.DatabaseName)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, apply.ID, stored.ApplyID)
		assert.Equal(t, "completed", stored.Status)
		assert.Equal(t, "failure", stored.Conclusion)
		assert.Equal(t, check.BlockingReason, stored.BlockingReason)
		assert.Equal(t, check.ErrorMessage, stored.ErrorMessage)
	})

	// A cancelled apply's claim on stored check state can fail to land — the
	// claim write errored, or the driver died before it. The cancelled apply is
	// still the newest one for the target, so its terminal blocked state must
	// take the row from the older apply that owns it rather than leaving that
	// apply's stale state in place with nothing able to repair it.
	t.Run("MarkCancelledApplyFailedClaimsRowOwnedByOlderApply", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "cancel_claim_db", storage.DatabaseTypeMySQL)
		earlier := CreateApplyWithStateAndEnv(t, store, lock, "cancel-earlier-failed", 709, state.Apply.Failed, "staging")
		createCompletedTask(t, store, earlier, "task-completed-before-cancel")
		cancelled := CreateApplyWithStateAndEnv(t, store, lock, "cancel-unclaimed", 710, state.Apply.Cancelled, "staging")
		require.Greater(t, cancelled.ID, earlier.ID)
		check := &storage.Check{
			Repository: lock.Repository, PullRequest: lock.PullRequest, HeadSHA: "abc",
			Environment: "staging", DatabaseType: lock.DatabaseType, DatabaseName: lock.DatabaseName,
			ApplyID: earlier.ID, HasChanges: true, Status: "completed", Conclusion: "failure",
		}
		require.NoError(t, store.Checks().Upsert(ctx, check))

		check.BlockingReason = "apply_cancelled_after_task_completed"
		check.ErrorMessage = "reconciliation required"
		updated, err := store.Checks().MarkCancelledApplyFailed(ctx, check, cancelled)
		require.NoError(t, err)
		assert.True(t, updated, "the newest apply's terminal outcome must repair a row owned by an older apply")

		stored, err := store.Checks().Get(ctx, lock.Repository, lock.PullRequest, "staging", lock.DatabaseType, lock.DatabaseName)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, cancelled.ID, stored.ApplyID)
		assert.Equal(t, "failure", stored.Conclusion)
		assert.Equal(t, check.BlockingReason, stored.BlockingReason)
	})

	// Completed rollback tasks do not prove that a later cancelled forward
	// apply changed the target, so they must not prevent the safe ownership
	// release.
	t.Run("MarkActionRequiredIgnoresCompletedRollbackTask", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "cancel_rollback_db", storage.DatabaseTypeMySQL)
		rollback := &storage.Apply{
			ApplyIdentifier: "cancel-rollback-prior", LockID: lock.ID, PlanID: 711,
			Database: lock.DatabaseName, DatabaseType: lock.DatabaseType,
			Repository: lock.Repository, PullRequest: lock.PullRequest,
			Environment: "staging", Engine: storage.EngineForType(lock.DatabaseType),
			State:   state.Apply.Completed,
			Options: storage.MarshalApplyOptions(storage.ApplyOptions{Rollback: true}),
		}
		rollbackID, err := store.Applies().Create(ctx, rollback)
		require.NoError(t, err)
		rollback.ID = rollbackID
		createCompletedTask(t, store, rollback, "task-rollback-completed")
		apply := CreateApplyWithStateAndEnv(t, store, lock, "cancel-after-rollback", 712, state.Apply.Cancelled, "staging")
		check := &storage.Check{
			Repository: lock.Repository, PullRequest: lock.PullRequest, HeadSHA: "abc",
			Environment: "staging", DatabaseType: lock.DatabaseType, DatabaseName: lock.DatabaseName,
			ApplyID: apply.ID, HasChanges: true, Status: "in_progress",
		}
		require.NoError(t, store.Checks().Upsert(ctx, check))

		check.Status = "completed"
		check.Conclusion = "action_required"
		updated, err := store.Checks().MarkActionRequiredForApply(ctx, check, apply)
		require.NoError(t, err)
		assert.True(t, updated)

		stored, err := store.Checks().Get(ctx, lock.Repository, lock.PullRequest, "staging", lock.DatabaseType, lock.DatabaseName)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Zero(t, stored.ApplyID)
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
		requirePlanResultStored(t, ctx, store, blocked, storage.PlanDriftBlocked)

		notEvaluated := &storage.Check{
			Repository: "org/repo", PullRequest: 123, HeadSHA: "sha-2",
			Environment: "production", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "drift_db",
			Status: "completed", Conclusion: "success",
		}
		requirePlanResultStored(t, ctx, store, notEvaluated, storage.PlanDriftNotEvaluated)

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
		requirePlanResultStored(t, ctx, store, &predicateBase, storage.PlanDriftClean)
		incomingBlock := predicateBase
		incomingBlock.HeadSHA = "sha-block"
		incomingBlock.BlockingReason = storage.ReviewTimeDeploymentDriftBlockingReason
		incomingBlock.ErrorMessage = "new error"
		requirePlanResultStored(t, ctx, store, &incomingBlock, storage.PlanDriftNotEvaluated)
		stored, err = store.Checks().Get(ctx, "org/repo", 123, "production", storage.DatabaseTypeMySQL, "drift_predicate_db")
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "new error", stored.ErrorMessage, "preservation keys on the stored blocking reason, not the incoming write")
		assert.Equal(t, storage.ReviewTimeDeploymentDriftBlockingReason, stored.BlockingReason)

		clean := *notEvaluated
		clean.HeadSHA = "sha-3"
		requirePlanResultStored(t, ctx, store, &clean, storage.PlanDriftClean)

		stored, err = store.Checks().Get(ctx, "org/repo", 123, "production", storage.DatabaseTypeMySQL, "drift_db")
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "sha-3", stored.HeadSHA)
		assert.Equal(t, "success", stored.Conclusion)
		assert.Empty(t, stored.BlockingReason)
		assert.Empty(t, stored.ChangeSummary)
	})

	// The rollout target counts describe the change summary, so every write
	// that replaces the summary replaces them and every write that keeps the
	// summary keeps them, including a write that keeps a preserved block.
	t.Run("RolloutTargetsTravelWithChangeSummary", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		get := func(database string) *storage.Check {
			t.Helper()
			stored, err := store.Checks().Get(ctx, "org/repo", 321, "production", storage.DatabaseTypeMySQL, database)
			require.NoError(t, err)
			require.NotNil(t, stored)
			return stored
		}

		plan := &storage.Check{
			Repository: "org/repo", PullRequest: 321, HeadSHA: "sha-1",
			Environment: "production", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "rollout_db",
			HasChanges: true, Status: "completed", Conclusion: "action_required",
			ChangeSummary: "1 alter · pending on all 4 targets", RolloutTargets: 4, RolloutTargetsWithWork: 4,
		}
		requirePlanResultStored(t, ctx, store, plan, storage.PlanDriftClean)
		stored := get("rollout_db")
		assert.Equal(t, 4, stored.RolloutTargets)
		assert.Equal(t, 4, stored.RolloutTargetsWithWork)

		replan := *plan
		replan.HeadSHA = "sha-2"
		replan.ChangeSummary = "1 alter · pending on 1 of 4 targets: `shop-004`"
		replan.RolloutTargetsWithWork = 1
		requirePlanResultStored(t, ctx, store, &replan, storage.PlanDriftClean)
		stored = get("rollout_db")
		assert.Equal(t, replan.ChangeSummary, stored.ChangeSummary)
		assert.Equal(t, 4, stored.RolloutTargets)
		assert.Equal(t, 1, stored.RolloutTargetsWithWork, "a plan's write replaces the counts with its summary")

		transition := replan
		transition.ChangeSummary = ""
		transition.RolloutTargets, transition.RolloutTargetsWithWork = 0, 0
		require.NoError(t, store.Checks().Upsert(ctx, &transition))
		stored = get("rollout_db")
		assert.Equal(t, replan.ChangeSummary, stored.ChangeSummary)
		assert.Equal(t, 4, stored.RolloutTargets, "a write that keeps the summary keeps the counts")
		assert.Equal(t, 1, stored.RolloutTargetsWithWork)

		stale := transition
		stale.HasChanges = false
		stale.Conclusion = "success"
		marked, err := store.Checks().MarkStalePlanSuccessful(ctx, &stale)
		require.NoError(t, err)
		require.True(t, marked)
		stored = get("rollout_db")
		assert.Equal(t, replan.ChangeSummary, stored.ChangeSummary)
		assert.Equal(t, 4, stored.RolloutTargets, "an update that keeps the summary keeps the counts")
		assert.Equal(t, 1, stored.RolloutTargetsWithWork)

		single := replan
		single.HeadSHA = "sha-3"
		single.ChangeSummary = "1 alter"
		single.RolloutTargets, single.RolloutTargetsWithWork = 0, 0
		require.NoError(t, store.Checks().Upsert(ctx, &single))
		stored = get("rollout_db")
		assert.Equal(t, "1 alter", stored.ChangeSummary)
		assert.Zero(t, stored.RolloutTargets, "a summary naming no rollout clears the counts")
		assert.Zero(t, stored.RolloutTargetsWithWork)

		blocked := &storage.Check{
			Repository: "org/repo", PullRequest: 321, HeadSHA: "sha-1",
			Environment: "production", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "rollout_blocked_db",
			HasChanges: true, Status: "completed", Conclusion: "failure",
			BlockingReason: storage.ReviewTimeDeploymentDriftBlockingReason, ChangeSummary: "targets diverged at review time",
		}
		requirePlanResultStored(t, ctx, store, blocked, storage.PlanDriftBlocked)
		notEvaluated := *plan
		notEvaluated.DatabaseName = "rollout_blocked_db"
		notEvaluated.HeadSHA = "sha-2"
		requirePlanResultStored(t, ctx, store, &notEvaluated, storage.PlanDriftNotEvaluated)
		stored = get("rollout_blocked_db")
		assert.Equal(t, "targets diverged at review time", stored.ChangeSummary)
		assert.Zero(t, stored.RolloutTargets, "a preserved block keeps its summary and the counts that go with it")
		assert.Zero(t, stored.RolloutTargetsWithWork)
	})

	// A namespace placement refusal is a review-time block too: a write that did
	// not re-evaluate the review keeps it, and one that did clears it.
	t.Run("PlanPlacementBlockDisposition", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		blocked := &storage.Check{
			Repository: "org/repo", PullRequest: 124, HeadSHA: "sha-1",
			Environment: "production", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "placement_db",
			Status: "completed", Conclusion: "failure",
			BlockingReason: storage.NamespacePlacementRefusedBlockingReason,
			ChangeSummary:  "namespace placement refused the plan",
		}
		requirePlanResultStored(t, ctx, store, blocked, storage.PlanDriftBlocked)

		notEvaluated := &storage.Check{
			Repository: "org/repo", PullRequest: 124, HeadSHA: "sha-2",
			Environment: "production", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "placement_db",
			Status: "completed", Conclusion: "success",
		}
		requirePlanResultStored(t, ctx, store, notEvaluated, storage.PlanDriftNotEvaluated)

		stored, err := store.Checks().Get(ctx, "org/repo", 124, "production", storage.DatabaseTypeMySQL, "placement_db")
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "sha-2", stored.HeadSHA)
		assert.Equal(t, "failure", stored.Conclusion, "a write that did not re-plan the placement keeps the block")
		assert.Equal(t, storage.NamespacePlacementRefusedBlockingReason, stored.BlockingReason)
		assert.Equal(t, "namespace placement refused the plan", stored.ChangeSummary)

		clean := *notEvaluated
		clean.HeadSHA = "sha-3"
		requirePlanResultStored(t, ctx, store, &clean, storage.PlanDriftClean)

		stored, err = store.Checks().Get(ctx, "org/repo", 124, "production", storage.DatabaseTypeMySQL, "placement_db")
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "success", stored.Conclusion)
		assert.Empty(t, stored.BlockingReason)
	})

	// A guard block never replaces a block the writer asked to preserve: the row
	// keeps its reason, conclusion, summary, and apply ownership, and only its
	// head SHA moves so the current-head aggregate still reads it. Any other
	// row takes the guard block, a row an in-progress apply owns refuses it,
	// and a missing row is created with it.
	t.Run("UpsertGuardBlockDisposition", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "guard_db", storage.DatabaseTypeMySQL)
		finished := CreateApplyWithStateAndEnv(t, store, lock, "apply-guard-finished", 710, state.Apply.Completed, "production")
		guard := func(database, headSHA string) *storage.Check {
			return &storage.Check{
				Repository: "org/repo", PullRequest: 125, HeadSHA: headSHA,
				Environment: "production", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: database,
				HasChanges: true, Status: "completed", Conclusion: "failure",
				BlockingReason: "guard_reason", ChangeSummary: "guard summary",
			}
		}
		get := func(database string) *storage.Check {
			stored, err := store.Checks().Get(ctx, "org/repo", 125, "production", storage.DatabaseTypeMySQL, database)
			require.NoError(t, err)
			require.NotNil(t, stored)
			return stored
		}

		require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
			Repository: "org/repo", PullRequest: 125, HeadSHA: "sha-1",
			Environment: "production", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "kept_db",
			ApplyID: finished.ID, HasChanges: true, Status: "completed", Conclusion: "action_required",
			BlockingReason: "kept_reason", ErrorMessage: "kept message", ChangeSummary: "kept summary",
		}))
		stored, err := store.Checks().UpsertGuardBlock(ctx, guard("kept_db", "sha-2"), []string{"other_reason", "kept_reason"})
		require.NoError(t, err)
		assert.True(t, stored)
		kept := get("kept_db")
		assert.Equal(t, "sha-2", kept.HeadSHA)
		assert.Equal(t, "kept_reason", kept.BlockingReason, "a preserved block is never replaced by a guard")
		assert.Equal(t, "action_required", kept.Conclusion)
		assert.Equal(t, "kept message", kept.ErrorMessage)
		assert.Equal(t, "kept summary", kept.ChangeSummary)
		assert.Equal(t, finished.ID, kept.ApplyID)

		require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
			Repository: "org/repo", PullRequest: 125, HeadSHA: "sha-1",
			Environment: "production", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "replaced_db",
			ApplyID: finished.ID, Status: "completed", Conclusion: "success",
		}))
		stored, err = store.Checks().UpsertGuardBlock(ctx, guard("replaced_db", "sha-2"), []string{"kept_reason"})
		require.NoError(t, err)
		assert.True(t, stored)
		replaced := get("replaced_db")
		assert.Equal(t, "guard_reason", replaced.BlockingReason)
		assert.Equal(t, "failure", replaced.Conclusion)
		assert.Equal(t, "guard summary", replaced.ChangeSummary)
		assert.Zero(t, replaced.ApplyID)

		running := CreateApplyWithStateAndEnv(t, store, lock, "apply-guard-running", 711, state.Apply.Running, "production")
		require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
			Repository: "org/repo", PullRequest: 125, HeadSHA: "sha-1",
			Environment: "production", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "owned_db",
			ApplyID: running.ID, HasChanges: true, Status: "in_progress",
		}))
		stored, err = store.Checks().UpsertGuardBlock(ctx, guard("owned_db", "sha-2"), []string{"kept_reason"})
		require.NoError(t, err)
		assert.False(t, stored, "a row an in-progress apply owns refuses the guard block")
		owned := get("owned_db")
		assert.Equal(t, "in_progress", owned.Status)
		assert.Empty(t, owned.BlockingReason)
		assert.Equal(t, running.ID, owned.ApplyID)

		stored, err = store.Checks().UpsertGuardBlock(ctx, guard("fresh_db", "sha-2"), []string{"kept_reason"})
		require.NoError(t, err)
		assert.True(t, stored)
		assert.Equal(t, "guard_reason", get("fresh_db").BlockingReason)
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

	// Stale cleanup reads an in-progress apply-owned row, the apply completes,
	// and only then does cleanup record that the schema change left the PR. The
	// block must settle the completed row as action_required on the new commit,
	// never rewind it to in_progress as if the apply were still running.
	t.Run("BlockStaleStartedApplyAfterApplyCompleted", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "removed_after_complete_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "removed-after-complete", 713, state.Apply.Running, "staging")
		require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
			Repository: lock.Repository, PullRequest: lock.PullRequest, HeadSHA: "sha-a",
			Environment: "staging", DatabaseType: lock.DatabaseType, DatabaseName: lock.DatabaseName,
			ApplyID: apply.ID, HasChanges: true, Status: "in_progress",
		}))

		cleanupRead := getCheck(t, store, lock.DatabaseName)
		require.Equal(t, "in_progress", cleanupRead.Status)

		completion := *getCheck(t, store, lock.DatabaseName)
		completion.Status = "completed"
		completion.Conclusion = "success"
		completion.HasChanges = false
		updated, err := store.Checks().CompleteForApply(ctx, &completion, apply)
		require.NoError(t, err)
		require.True(t, updated)

		cleanupRead.BlockingReason = storage.SchemaRemovedAfterApplyStartedBlockingReason
		cleanupRead.ErrorMessage = "schema change removed after apply started"
		blocked, err := store.Checks().BlockStaleStartedApply(ctx, cleanupRead, "sha-b")
		require.NoError(t, err)
		require.True(t, blocked)
		assert.Equal(t, "sha-b", cleanupRead.HeadSHA)
		assert.Equal(t, "completed", cleanupRead.Status)
		assert.Equal(t, "action_required", cleanupRead.Conclusion)

		stored := getCheck(t, store, lock.DatabaseName)
		assert.Equal(t, "completed", stored.Status, "a completed apply must not be shown as running again")
		assert.Equal(t, "action_required", stored.Conclusion)
		assert.Equal(t, "sha-b", stored.HeadSHA)
		assert.Equal(t, storage.SchemaRemovedAfterApplyStartedBlockingReason, stored.BlockingReason)
		assert.Equal(t, "schema change removed after apply started", stored.ErrorMessage)
		assert.True(t, stored.HasChanges)
		assert.Equal(t, apply.ID, stored.ApplyID)
	})

	// The driver reads the row, stale cleanup blocks it on a newer commit, and
	// the driver then completes from its earlier read. The completion must not
	// write the block away: the row lands action_required on the newer commit
	// with the schema-removed reason the operator reconciles from.
	t.Run("CompleteForApplyKeepsStaleCleanupBlock", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "removed_before_complete_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "removed-before-complete", 714, state.Apply.Running, "staging")
		require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
			Repository: lock.Repository, PullRequest: lock.PullRequest, HeadSHA: "sha-a",
			Environment: "staging", DatabaseType: lock.DatabaseType, DatabaseName: lock.DatabaseName,
			ApplyID: apply.ID, HasChanges: true, Status: "in_progress",
		}))

		driverRead := getCheck(t, store, lock.DatabaseName)

		cleanup := *getCheck(t, store, lock.DatabaseName)
		cleanup.BlockingReason = storage.SchemaRemovedAfterApplyStartedBlockingReason
		cleanup.ErrorMessage = "schema change removed after apply started"
		blocked, err := store.Checks().BlockStaleStartedApply(ctx, &cleanup, "sha-b")
		require.NoError(t, err)
		require.True(t, blocked)
		assert.Equal(t, "in_progress", cleanup.Status, "a running apply stays in_progress while it owns the row")

		driverRead.Status = "completed"
		driverRead.Conclusion = "success"
		driverRead.HasChanges = false
		driverRead.BlockingReason = ""
		driverRead.ErrorMessage = ""
		updated, err := store.Checks().CompleteForApply(ctx, driverRead, apply)
		require.NoError(t, err)
		require.True(t, updated)
		assert.Equal(t, "action_required", driverRead.Conclusion, "the caller sees the block that was kept, not the success it asked for")
		assert.Equal(t, "sha-b", driverRead.HeadSHA)
		assert.Equal(t, storage.SchemaRemovedAfterApplyStartedBlockingReason, driverRead.BlockingReason)
		assert.Equal(t, "schema change removed after apply started", driverRead.ErrorMessage)
		assert.True(t, driverRead.HasChanges)

		stored := getCheck(t, store, lock.DatabaseName)
		assert.Equal(t, "completed", stored.Status)
		assert.Equal(t, "action_required", stored.Conclusion, "a completion must not turn a schema-removed block green")
		assert.Equal(t, "sha-b", stored.HeadSHA)
		assert.Equal(t, storage.SchemaRemovedAfterApplyStartedBlockingReason, stored.BlockingReason)
		assert.Equal(t, "schema change removed after apply started", stored.ErrorMessage)
		assert.True(t, stored.HasChanges)
		assert.Equal(t, apply.ID, stored.ApplyID)
	})

	// An apply that released the row after stale cleanup read it — here a
	// completed rollback — owns the outcome: the cleanup block writes nothing.
	t.Run("BlockStaleStartedApplyLeavesReleasedRow", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "removed_after_release_db", storage.DatabaseTypeMySQL)
		rollback := CreateApplyWithStateAndEnv(t, store, lock, "removed-after-release", 715, state.Apply.Completed, "staging")
		require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
			Repository: lock.Repository, PullRequest: lock.PullRequest, HeadSHA: "sha-a",
			Environment: "staging", DatabaseType: lock.DatabaseType, DatabaseName: lock.DatabaseName,
			ApplyID: rollback.ID, HasChanges: true, Status: "in_progress",
		}))
		cleanupRead := getCheck(t, store, lock.DatabaseName)

		release := *getCheck(t, store, lock.DatabaseName)
		release.Status = "completed"
		release.Conclusion = "action_required"
		release.BlockingReason = "rollback_completed"
		updated, err := store.Checks().MarkActionRequiredForApply(ctx, &release, rollback)
		require.NoError(t, err)
		require.True(t, updated)

		cleanupRead.BlockingReason = storage.SchemaRemovedAfterApplyStartedBlockingReason
		blocked, err := store.Checks().BlockStaleStartedApply(ctx, cleanupRead, "sha-b")
		require.NoError(t, err)
		assert.False(t, blocked)

		stored := getCheck(t, store, lock.DatabaseName)
		assert.Equal(t, "sha-a", stored.HeadSHA)
		assert.Equal(t, "action_required", stored.Conclusion)
		assert.Equal(t, "rollback_completed", stored.BlockingReason)
		assert.Zero(t, stored.ApplyID)

		_, err = store.Checks().BlockStaleStartedApply(ctx, &storage.Check{
			Repository: lock.Repository, PullRequest: lock.PullRequest, HeadSHA: "sha-a",
			Environment: "staging", DatabaseType: lock.DatabaseType, DatabaseName: "missing_db",
		}, "sha-b")
		require.ErrorIs(t, err, storage.ErrCheckNotFound)
	})

	// Two pushes are handled on two pods. Both cleanups read the row on commit
	// A; the cleanup for the newer commit C writes first. The slower cleanup
	// for the older commit B must not move the row back onto B, which is no
	// longer the PR head.
	t.Run("BlockStaleStartedApplyYieldsToNewerCommitsCleanup", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "removed_late_cleanup_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "removed-late-cleanup", 716, state.Apply.Running, "staging")
		seedApplyOwnedCheck(t, store, lock, apply.ID)
		cleanupB := getCheck(t, store, lock.DatabaseName)
		cleanupC := getCheck(t, store, lock.DatabaseName)
		redeliveredC := getCheck(t, store, lock.DatabaseName)

		cleanupC.BlockingReason = storage.SchemaRemovedAfterApplyStartedBlockingReason
		blocked, err := store.Checks().BlockStaleStartedApply(ctx, cleanupC, "sha-c")
		require.NoError(t, err)
		require.True(t, blocked)

		cleanupB.BlockingReason = storage.SchemaRemovedAfterApplyStartedBlockingReason
		blocked, err = store.Checks().BlockStaleStartedApply(ctx, cleanupB, "sha-b")
		require.NoError(t, err)
		assert.False(t, blocked, "the older commit's cleanup must not move the row back")

		stored := getCheck(t, store, lock.DatabaseName)
		assert.Equal(t, "sha-c", stored.HeadSHA)
		assert.Equal(t, "in_progress", stored.Status)
		assert.Equal(t, storage.SchemaRemovedAfterApplyStartedBlockingReason, stored.BlockingReason)
		assert.Equal(t, apply.ID, stored.ApplyID)

		// A redelivery of C's push that read the row on A before C's cleanup
		// wrote still lands: the row already names the head it is writing.
		redeliveredC.BlockingReason = storage.SchemaRemovedAfterApplyStartedBlockingReason
		blocked, err = store.Checks().BlockStaleStartedApply(ctx, redeliveredC, "sha-c")
		require.NoError(t, err)
		assert.True(t, blocked, "a cleanup for the head the row already names must still land")
	})

	// A different apply claims the row after stale cleanup read it. The block
	// belongs to the apply cleanup read, so it must not land on the new
	// apply's row, and the new apply's success must still land green.
	t.Run("BlockStaleStartedApplyYieldsToNewerApplyClaim", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "removed_new_claim_db", storage.DatabaseTypeMySQL)
		first := CreateApplyWithStateAndEnv(t, store, lock, "removed-new-claim-first", 717, state.Apply.Completed, "staging")
		seedApplyOwnedCheck(t, store, lock, first.ID)
		cleanupRead := getCheck(t, store, lock.DatabaseName)

		second := CreateApplyWithStateAndEnv(t, store, lock, "removed-new-claim-second", 718, state.Apply.Running, "staging")
		seedApplyOwnedCheck(t, store, lock, second.ID)

		cleanupRead.BlockingReason = storage.SchemaRemovedAfterApplyStartedBlockingReason
		blocked, err := store.Checks().BlockStaleStartedApply(ctx, cleanupRead, "sha-b")
		require.NoError(t, err)
		assert.False(t, blocked, "a block read under one apply must not land on another apply's row")

		stored := getCheck(t, store, lock.DatabaseName)
		assert.Equal(t, "sha-a", stored.HeadSHA)
		assert.Empty(t, stored.BlockingReason)
		assert.Equal(t, second.ID, stored.ApplyID)

		stored.Status = "completed"
		stored.Conclusion = "success"
		stored.HasChanges = false
		updated, err := store.Checks().CompleteForApply(ctx, stored, second)
		require.NoError(t, err)
		require.True(t, updated)
		completed := getCheck(t, store, lock.DatabaseName)
		assert.Equal(t, "success", completed.Conclusion)
		assert.Empty(t, completed.BlockingReason)
	})

	// The apply fails after stale cleanup blocked its row. The failure lands
	// as written, and the schema-removed block stays with the newer commit.
	t.Run("CompleteForApplyKeepsStaleCleanupBlockOnFailure", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "removed_then_failed_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "removed-then-failed", 719, state.Apply.Running, "staging")
		seedApplyOwnedCheck(t, store, lock, apply.ID)
		driverRead := getCheck(t, store, lock.DatabaseName)

		cleanup := getCheck(t, store, lock.DatabaseName)
		cleanup.BlockingReason = storage.SchemaRemovedAfterApplyStartedBlockingReason
		cleanup.ErrorMessage = "schema change removed after apply started"
		blocked, err := store.Checks().BlockStaleStartedApply(ctx, cleanup, "sha-b")
		require.NoError(t, err)
		require.True(t, blocked)

		driverRead.Status = "completed"
		driverRead.Conclusion = "failure"
		driverRead.ErrorMessage = "apply failed"
		updated, err := store.Checks().CompleteForApply(ctx, driverRead, apply)
		require.NoError(t, err)
		require.True(t, updated)

		stored := getCheck(t, store, lock.DatabaseName)
		assert.Equal(t, "completed", stored.Status)
		assert.Equal(t, "failure", stored.Conclusion, "a failed completion lands as the caller wrote it")
		assert.Equal(t, "sha-b", stored.HeadSHA)
		assert.Equal(t, storage.SchemaRemovedAfterApplyStartedBlockingReason, stored.BlockingReason)
		assert.Equal(t, "schema change removed after apply started", stored.ErrorMessage)
	})

	// A completion that itself records the schema-removed block on a row that
	// does not carry it yet is an ordinary completion: every column takes the
	// caller's value, because each one is decided from the reason stored
	// before the write, not the one the write sets.
	t.Run("CompleteForApplyDecidesFromStoredBlockingReason", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "removed_by_completion_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "removed-by-completion", 720, state.Apply.Running, "staging")
		seedApplyOwnedCheck(t, store, lock, apply.ID)

		completion := getCheck(t, store, lock.DatabaseName)
		completion.HeadSHA = "sha-b"
		completion.Status = "completed"
		completion.Conclusion = "action_required"
		completion.HasChanges = false
		completion.BlockingReason = storage.SchemaRemovedAfterApplyStartedBlockingReason
		completion.ErrorMessage = "recorded by the completion"
		updated, err := store.Checks().CompleteForApply(ctx, completion, apply)
		require.NoError(t, err)
		require.True(t, updated)

		stored := getCheck(t, store, lock.DatabaseName)
		assert.Equal(t, "sha-b", stored.HeadSHA)
		assert.False(t, stored.HasChanges)
		assert.Equal(t, "action_required", stored.Conclusion)
		assert.Equal(t, storage.SchemaRemovedAfterApplyStartedBlockingReason, stored.BlockingReason)
		assert.Equal(t, "recorded by the completion", stored.ErrorMessage)
	})

	// Retrying a failed plan replaces old success with blocking evidence, while
	// preserving both live and completed apply provenance and stronger blocks.
	t.Run("MarkPlanPending", func(t *testing.T) {
		for _, scenario := range []string{"missing", "success", "completed apply", "running apply", "guard block"} {
			t.Run(scenario, func(t *testing.T) {
				ctx := t.Context()
				store := h.NewStorage(t)
				lock := CreateLock(t, store, "pending_plan_db", storage.DatabaseTypeMySQL)
				check := &storage.Check{Repository: lock.Repository, PullRequest: lock.PullRequest, HeadSHA: "same-head", Environment: "staging", DatabaseType: lock.DatabaseType, DatabaseName: lock.DatabaseName, Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess}
				if scenario == "completed apply" || scenario == "running apply" {
					apply := CreateApplyWithStateAndEnv(t, store, lock, "pending-plan", 730, state.Apply.Running, "staging")
					check.ApplyID = apply.ID
					if scenario == "running apply" {
						check.Status, check.Conclusion = checkstate.StatusInProgress, ""
					}
				}
				if scenario == "guard block" {
					check.Conclusion, check.BlockingReason = checkstate.ConclusionActionRequired, storage.SchemaRemovedAfterApplyStartedBlockingReason
				}
				if scenario != "missing" {
					require.NoError(t, store.Checks().Upsert(ctx, check))
				}
				require.NoError(t, store.Checks().MarkPlanPending(ctx, check))
				stored, err := store.Checks().Get(ctx, check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName)
				require.NoError(t, err)
				require.NotNil(t, stored)
				assert.Equal(t, check.ApplyID, stored.ApplyID)
				assert.Equal(t, check.BlockingReason, stored.BlockingReason)
				if scenario == "running apply" {
					assert.Equal(t, checkstate.StatusInProgress, stored.Status)
					assert.Empty(t, stored.Conclusion)
				} else {
					assert.Equal(t, checkstate.StatusCompleted, stored.Status)
					assert.Equal(t, checkstate.ConclusionActionRequired, stored.Conclusion)
				}
				if scenario == "success" || scenario == "missing" || scenario == "completed apply" {
					assert.Equal(t, "Waiting for a fresh plan", stored.ChangeSummary)
					assert.True(t, stored.HasChanges)
					stored.Conclusion = checkstate.ConclusionSuccess
					stored.HasChanges = false
					written, err := store.Checks().UpsertPlanResult(ctx, stored, storage.PlanDriftClean)
					require.NoError(t, err)
					require.True(t, written)
					refreshed, err := store.Checks().Get(ctx, check.Repository, check.PullRequest, check.Environment, check.DatabaseType, check.DatabaseName)
					require.NoError(t, err)
					assert.Equal(t, checkstate.ConclusionSuccess, refreshed.Conclusion)
				}
			})
		}
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
		_, err := h.NewUnreachableStorage(t).Checks().UpsertPlanResult(t.Context(), check, storage.PlanDriftClean)
		require.Error(t, err)
	})
	t.Run("UpsertGuardBlock_DBError", func(t *testing.T) {
		_, err := h.NewUnreachableStorage(t).Checks().UpsertGuardBlock(t.Context(), &storage.Check{
			Repository: "org/repo", PullRequest: 123, HeadSHA: "sha",
			Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "errors_db",
			Status: "completed", Conclusion: "failure", BlockingReason: "guard_reason",
		}, []string{"kept_reason"})
		require.Error(t, err)
	})
	t.Run("RecoverApplyOwnedCheckWithNoOpPlan_DBError", func(t *testing.T) {
		_, err := h.NewUnreachableStorage(t).Checks().RecoverApplyOwnedCheckWithNoOpPlan(t.Context(), check)
		require.Error(t, err)
	})
	t.Run("MarkStalePlanSuccessful_DBError", func(t *testing.T) {
		_, err := h.NewUnreachableStorage(t).Checks().MarkStalePlanSuccessful(t.Context(), check)
		require.Error(t, err)
	})
	t.Run("BlockStaleStartedApply_DBError", func(t *testing.T) {
		_, err := h.NewUnreachableStorage(t).Checks().BlockStaleStartedApply(t.Context(), check, "sha-b")
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

// getCheck reads the staging per-database row for databaseName on org/repo#123
// and requires that it exists.
func getCheck(t *testing.T, store storage.Storage, databaseName string) *storage.Check {
	t.Helper()
	check, err := store.Checks().Get(t.Context(), "org/repo", 123, "staging", storage.DatabaseTypeMySQL, databaseName)
	require.NoError(t, err)
	require.NotNil(t, check, "stored check state for %s", databaseName)
	return check
}

// seedApplyOwnedCheck stores the staging per-database row for lock's database
// as an in-progress row the apply with row ID applyID owns on commit sha-a, the
// way an apply start claims it.
func seedApplyOwnedCheck(t *testing.T, store storage.Storage, lock *storage.Lock, applyID int64) {
	t.Helper()
	require.NoError(t, store.Checks().Upsert(t.Context(), &storage.Check{
		Repository: lock.Repository, PullRequest: lock.PullRequest, HeadSHA: "sha-a",
		Environment: "staging", DatabaseType: lock.DatabaseType, DatabaseName: lock.DatabaseName,
		ApplyID: applyID, HasChanges: true, Status: "in_progress",
	}))
}

// requirePlanResultStored writes a plan result and asserts it landed, for the
// cases that exercise something other than the ownership guard and would be
// meaningless if the write had been refused.
func requirePlanResultStored(t *testing.T, ctx context.Context, store storage.Storage, check *storage.Check, drift storage.PlanDriftState) {
	t.Helper()
	stored, err := store.Checks().UpsertPlanResult(ctx, check, drift)
	require.NoError(t, err)
	require.True(t, stored, "plan result must land for %s/%s/%s", check.Environment, check.DatabaseType, check.DatabaseName)
}

// createCompletedTask persists a completed forward task under the given apply,
// the durable evidence the check store's ownership-release guards key on.
func createCompletedTask(t *testing.T, store storage.Storage, apply *storage.Apply, identifier string) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	_, err := store.Tasks().Create(t.Context(), &storage.Task{
		TaskIdentifier: identifier,
		ApplyID:        apply.ID,
		PlanID:         apply.PlanID,
		Database:       apply.Database,
		DatabaseType:   apply.DatabaseType,
		Engine:         apply.Engine,
		Repository:     apply.Repository,
		PullRequest:    apply.PullRequest,
		Environment:    apply.Environment,
		State:          state.Task.Completed,
		Namespace:      apply.Database,
		TableName:      "users",
		DDL:            "ALTER TABLE `users` ADD COLUMN `email` VARCHAR(255)",
		DDLAction:      "alter",
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	require.NoError(t, err)
}
