package storagetest

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// TestApplies runs the behavioral parity suite for storage.ApplyStore: the
// create/get/update lifecycle, claimability and lease rotation, stale-lease
// recovery, lease-guarded writes, and concurrent claim exclusion.
func TestApplies(t *testing.T, h Harness) {
	t.Run("Create_Get_Update", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "apply_round_trip_db", storage.DatabaseTypeMySQL)
		apply := &storage.Apply{
			ApplyIdentifier: "apply_round_trip",
			LockID:          lock.ID,
			PlanID:          901,
			Database:        lock.DatabaseName,
			DatabaseType:    lock.DatabaseType,
			Repository:      lock.Repository,
			PullRequest:     lock.PullRequest,
			Environment:     "staging",
			Deployment:      "primary",
			Caller:          "cli:testuser",
			InstallationID:  12345,
			Engine:          storage.EngineSpirit,
			State:           state.Apply.Pending,
			Options:         []byte(`{"defer_cutover":true}`),
		}
		// Create owns these timestamps through the schema clock defaults.
		id, err := store.Applies().Create(ctx, apply)
		require.NoError(t, err)
		require.Positive(t, id)

		got, err := store.Applies().Get(ctx, id)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, "apply_round_trip", got.ApplyIdentifier)
		assert.Equal(t, lock.ID, got.LockID)
		assert.Equal(t, int64(901), got.PlanID)
		assert.Equal(t, "apply_round_trip_db", got.Database)
		assert.Equal(t, storage.DatabaseTypeMySQL, got.DatabaseType)
		assert.Equal(t, "org/repo", got.Repository)
		assert.Equal(t, 123, got.PullRequest)
		assert.Equal(t, "staging", got.Environment)
		assert.Equal(t, "primary", got.Deployment)
		assert.Equal(t, "cli:testuser", got.Caller)
		assert.Equal(t, int64(12345), got.InstallationID)
		assert.Equal(t, storage.EngineSpirit, got.Engine)
		assert.Equal(t, state.Apply.Pending, got.State)
		assert.JSONEq(t, `{"defer_cutover":true}`, string(got.Options))
		assert.NotZero(t, got.CreatedAt)
		assert.NotZero(t, got.UpdatedAt)

		got.State = state.Apply.Failed
		got.ErrorMessage = "engine failed"
		got.ExternalID = "remote-apply-1"
		got.Options = []byte(`{"defer_cutover":false}`)
		started := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
		completed := started.Add(time.Minute)
		got.StartedAt = &started
		got.CompletedAt = &completed
		require.NoError(t, store.Applies().Update(ctx, got))

		updated, err := store.Applies().GetByApplyIdentifier(ctx, got.ApplyIdentifier)
		require.NoError(t, err)
		require.NotNil(t, updated)
		assert.Equal(t, state.Apply.Failed, updated.State)
		assert.Equal(t, "engine failed", updated.ErrorMessage)
		assert.Equal(t, "remote-apply-1", updated.ExternalID)
		assert.JSONEq(t, `{"defer_cutover":false}`, string(updated.Options))
		require.NotNil(t, updated.StartedAt)
		assert.WithinDuration(t, started, *updated.StartedAt, time.Second)
		require.NotNil(t, updated.CompletedAt)
		assert.WithinDuration(t, completed, *updated.CompletedAt, time.Second)

		missing, err := store.Applies().Get(ctx, id+1000)
		require.NoError(t, err)
		assert.Nil(t, missing)
	})

	t.Run("ClaimPredicates", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "apply_claim_predicates_db", storage.DatabaseTypeMySQL)

		taskless := CreateApply(t, store, lock, "apply_taskless", 902)
		claimed, err := store.Applies().ClaimApplyByID(ctx, taskless.ID, "driver-a")
		require.NoError(t, err)
		assert.Nil(t, claimed, "a half-created pending apply is not claimable")

		terminal := CreateApplyWithStateAndEnv(t, store, lock, "apply_terminal", 903, state.Apply.Completed, "production")
		claimed, err = store.Applies().ClaimApplyByID(ctx, terminal.ID, "driver-a")
		require.NoError(t, err)
		assert.Nil(t, claimed, "a terminal apply is not claimable")

		missing, err := store.Applies().ClaimApplyByID(ctx, terminal.ID+1000, "driver-a")
		require.NoError(t, err)
		assert.Nil(t, missing)

		retryable := CreateApplyWithStateAndEnv(t, store, lock, "apply_retryable", 904, state.Apply.FailedRetryable, "production")
		retryable.ErrorMessage = "transient failure"
		require.NoError(t, store.Applies().Update(ctx, retryable))
		claimed, err = store.Applies().ClaimApplyByID(ctx, retryable.ID, "driver-a")
		require.NoError(t, err)
		require.NotNil(t, claimed)
		assert.Equal(t, state.Apply.FailedRetryable, claimed.State)
		assert.Equal(t, 1, claimed.Attempt)
		assert.Empty(t, claimed.ErrorMessage)
		assert.Equal(t, "driver-a", claimed.LeaseOwner)
		assert.NotEmpty(t, claimed.LeaseToken)

		persisted, err := store.Applies().Get(ctx, retryable.ID)
		require.NoError(t, err)
		require.NotNil(t, persisted)
		assert.Equal(t, state.Apply.Running, persisted.State)
		assert.Equal(t, 1, persisted.Attempt)
		assert.Empty(t, persisted.ErrorMessage)
		assert.NotNil(t, persisted.LeaseAcquiredAt, "a claim records when the lease was acquired")

		claimed, err = store.Applies().ClaimApplyByID(ctx, retryable.ID, "driver-b")
		require.NoError(t, err)
		assert.Nil(t, claimed, "a fresh heartbeat prevents lease theft")

		_, err = store.Applies().ClaimApplyByID(ctx, retryable.ID, "")
		require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
	})

	t.Run("RetryableClaim_ExhaustsAttemptBudget", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "apply_attempt_budget_db", storage.DatabaseTypeMySQL)
		retryable := CreateApplyWithStateAndEnv(t, store, lock, "apply_attempt_budget", 908, state.Apply.FailedRetryable, "production")

		// Each recovery cycle charges one attempt: a driver claims the
		// retryable apply, records another retryable failure, and releases
		// the lease back to the pool. The budget bounds how many such cycles
		// may run before the apply stops being claimable.
		for i := 1; i <= storage.MaxRecoveryAttempts; i++ {
			claimed, err := store.Applies().ClaimApplyByID(ctx, retryable.ID, "driver-a")
			require.NoError(t, err)
			require.NotNil(t, claimed)
			require.Equal(t, i, claimed.Attempt)

			failed, err := store.Applies().Get(ctx, retryable.ID)
			require.NoError(t, err)
			require.NotNil(t, failed)
			failed.State = state.Apply.FailedRetryable
			failed.ErrorMessage = "transient failure"
			require.NoError(t, store.Applies().Update(storage.WithApplyLease(ctx, claimed.Lease()), failed))

			released, err := store.Applies().ReleaseClaim(ctx, claimed.Lease())
			require.NoError(t, err)
			require.True(t, released)
		}

		claimed, err := store.Applies().ClaimApplyByID(ctx, retryable.ID, "driver-b")
		require.NoError(t, err)
		assert.Nil(t, claimed, "an exhausted attempt budget stops recovery claims")

		persisted, err := store.Applies().Get(ctx, retryable.ID)
		require.NoError(t, err)
		require.NotNil(t, persisted)
		assert.Equal(t, state.Apply.FailedRetryable, persisted.State)
		assert.Equal(t, storage.MaxRecoveryAttempts, persisted.Attempt)
	})

	t.Run("LeaseRotation_And_Staleness", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "apply_lease_rotation_db", storage.DatabaseTypeMySQL)
		first := CreateClaimedApply(t, store, lock, "apply_lease_rotation", 905, "driver-a")

		released, err := store.Applies().ReleaseClaim(ctx, first.Lease())
		require.NoError(t, err)
		require.True(t, released)

		stale, err := store.Applies().Get(ctx, first.ID)
		require.NoError(t, err)
		require.NotNil(t, stale)
		assert.False(t, stale.HasFreshLease(time.Now()), "release clears the lease owner")
		assert.True(t, stale.UpdatedAt.Before(time.Now().Add(-storage.ApplyLeaseStaleAfter)), "release backdates the heartbeat beyond ApplyLeaseStaleAfter")
		assert.Empty(t, stale.LeaseToken)
		assert.Nil(t, stale.LeaseAcquiredAt)
		assert.Equal(t, state.Apply.Running, stale.State)

		second, err := store.Applies().ClaimApplyByID(ctx, first.ID, "driver-b")
		require.NoError(t, err)
		require.NotNil(t, second)
		assert.Equal(t, "driver-b", second.LeaseOwner)
		assert.NotEmpty(t, second.LeaseToken)
		assert.NotEqual(t, first.LeaseToken, second.LeaseToken)
		persisted, err := store.Applies().Get(ctx, first.ID)
		require.NoError(t, err)
		require.NotNil(t, persisted)
		assert.True(t, persisted.HasFreshLease(time.Now()))

		released, err = store.Applies().ReleaseClaim(ctx, first.Lease())
		require.NoError(t, err)
		assert.False(t, released, "a stale token cannot release the rotated lease")
		persisted, err = store.Applies().Get(ctx, first.ID)
		require.NoError(t, err)
		require.NotNil(t, persisted)
		assert.Equal(t, "driver-b", persisted.LeaseOwner)
		assert.Equal(t, second.LeaseToken, persisted.LeaseToken)
		third, err := store.Applies().ClaimApplyByID(ctx, first.ID, "driver-c")
		require.NoError(t, err)
		assert.Nil(t, third, "a mismatched release leaves the current lease fresh")
	})

	t.Run("LeaseGuardsWrites", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "apply_lease_guard_db", storage.DatabaseTypeMySQL)
		claimed := CreateClaimedApply(t, store, lock, "apply_lease_guard", 906, "driver-a")
		task, err := store.Tasks().Get(ctx, "task_apply_lease_guard")
		require.NoError(t, err)
		require.NotNil(t, task)

		staleCtx := storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: claimed.ID, Owner: "old-driver", Token: "stale-token"})
		claimed.State = state.Apply.Failed
		claimed.ErrorMessage = "stale failure"
		require.ErrorIs(t, store.Applies().Update(staleCtx, claimed), storage.ErrApplyLeaseLost)
		require.ErrorIs(t, store.Applies().Heartbeat(staleCtx, claimed.ID), storage.ErrApplyLeaseLost)
		task.State = state.Task.Completed
		require.ErrorIs(t, store.Tasks().Update(staleCtx, task), storage.ErrApplyLeaseLost)
		require.ErrorIs(t, store.ApplyLogs().Append(staleCtx, &storage.ApplyLog{
			ApplyID: claimed.ID, Level: storage.LogLevelInfo, EventType: storage.LogEventStateTransition,
			Source: storage.LogSourceSchemaBot, Message: "stale driver log",
		}), storage.ErrApplyLeaseLost)

		persisted, err := store.Applies().Get(ctx, claimed.ID)
		require.NoError(t, err)
		require.NotNil(t, persisted)
		assert.Equal(t, state.Apply.Running, persisted.State)
		assert.Empty(t, persisted.ErrorMessage)
		persistedTask, err := store.Tasks().Get(ctx, task.TaskIdentifier)
		require.NoError(t, err)
		require.NotNil(t, persistedTask)
		assert.Equal(t, state.Task.Pending, persistedTask.State)
		logs, err := store.ApplyLogs().GetByApply(ctx, claimed.ID)
		require.NoError(t, err)
		assert.Empty(t, logs)

		ownedCtx := storage.WithApplyLease(ctx, claimed.Lease())
		claimed.State = state.Apply.Completed
		claimed.ErrorMessage = ""
		require.NoError(t, store.Applies().Update(ownedCtx, claimed))
		require.NoError(t, store.Applies().Heartbeat(ownedCtx, claimed.ID))
		task.State = state.Task.Completed
		require.NoError(t, store.Tasks().Update(ownedCtx, task))
		require.NoError(t, store.ApplyLogs().Append(ownedCtx, &storage.ApplyLog{
			ApplyID: claimed.ID, Level: storage.LogLevelInfo, EventType: storage.LogEventStateTransition,
			Source: storage.LogSourceSchemaBot, Message: "owned driver log",
		}))

		persisted, err = store.Applies().Get(ctx, claimed.ID)
		require.NoError(t, err)
		require.NotNil(t, persisted)
		assert.Equal(t, state.Apply.Completed, persisted.State)
	})

	// A stopped apply whose unfinished work another apply took over must never be
	// started again: its statements would replay against a target where that work
	// already happened. The takeover is recorded on the apply that handed off, and
	// the refusal survives the successor settling — at which point the target is
	// clear again and nothing else distinguishes this apply from one that merely
	// stopped. The operator learns why from the failed start request.
	t.Run("SupersededMarker_RefusesStoppedStart", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "apply_superseded_db", storage.DatabaseTypeMySQL)
		holder := CreateApplyWithStateAndEnv(t, store, lock, "apply_superseded_holder", 909, state.Apply.Stopped, "staging")
		successor := CreateApplyWithStateAndEnv(t, store, lock, "apply_superseded_successor", 910, state.Apply.Completed, "staging")

		beforeMark, err := store.Applies().Get(ctx, holder.ID)
		require.NoError(t, err)
		require.NotNil(t, beforeMark)
		assert.Empty(t, beforeMark.SupersededBy, "an apply starts with nothing having taken its work over")

		require.NoError(t, store.Applies().MarkSuperseded(ctx, holder.ID, successor.ApplyIdentifier))
		marked, err := store.Applies().Get(ctx, holder.ID)
		require.NoError(t, err)
		require.NotNil(t, marked)
		assert.Equal(t, successor.ApplyIdentifier, marked.SupersededBy)
		assert.Equal(t, state.Apply.Stopped, marked.State, "the handoff does not change the apply's own state")

		// Recording the same handoff again is the same fact, so a redelivery is
		// not an error. A different successor is an ambiguous takeover and must
		// not overwrite the marker.
		require.NoError(t, store.Applies().MarkSuperseded(ctx, holder.ID, successor.ApplyIdentifier))
		require.ErrorIs(t, store.Applies().MarkSuperseded(ctx, holder.ID, "apply_other_successor"), storage.ErrApplyAlreadySuperseded)
		require.Error(t, store.Applies().MarkSuperseded(ctx, holder.ID, ""))
		stillMarked, err := store.Applies().Get(ctx, holder.ID)
		require.NoError(t, err)
		require.NotNil(t, stillMarked)
		assert.Equal(t, successor.ApplyIdentifier, stillMarked.SupersededBy)

		// A marker that names nothing helps no operator: a row that does not
		// exist is an error, and an apply cannot take over its own work — the
		// marker is never cleared, so a self-referential handoff would refuse
		// the apply forever while pointing back at itself.
		require.ErrorIs(t, store.Applies().MarkSuperseded(ctx, holder.ID+100000, successor.ApplyIdentifier), storage.ErrApplyNotFound)
		require.Error(t, store.Applies().MarkSuperseded(ctx, successor.ID, successor.ApplyIdentifier))
		unmarked, err := store.Applies().Get(ctx, successor.ID)
		require.NoError(t, err)
		require.NotNil(t, unmarked)
		assert.Empty(t, unmarked.SupersededBy, "a rejected self-referential handoff writes nothing")

		_, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
			ApplyID:   holder.ID,
			Operation: storage.ControlOperationStart,
			Status:    storage.ControlRequestPending,
			Metadata:  []byte(`{}`),
		})
		require.NoError(t, err)
		require.False(t, alreadyPending)

		claimed, err := store.Applies().ClaimApplyByID(ctx, holder.ID, "driver-a")
		require.NoError(t, err)
		assert.Nil(t, claimed, "an apply whose work was taken over must not be claimed for a start")

		refused, err := store.Applies().Get(ctx, holder.ID)
		require.NoError(t, err)
		require.NotNil(t, refused)
		assert.Equal(t, state.Apply.Stopped, refused.State, "the refused claim leaves the apply stopped")

		stillPending, err := store.ControlRequests().GetPending(ctx, holder.ID, storage.ControlOperationStart)
		require.NoError(t, err)
		assert.Nil(t, stillPending, "the refusal resolves the start request instead of stranding it pending")

		settled, err := store.ControlRequests().GetByOperation(ctx, holder.ID, storage.ControlOperationStart)
		require.NoError(t, err)
		require.NotNil(t, settled)
		assert.Equal(t, storage.ControlRequestFailed, settled.Status)
		assert.Contains(t, settled.ErrorMessage, successor.ApplyIdentifier,
			"the failed start request names the apply that took the work over")
	})

	// Automatic retry is the one start surface with no operator request behind
	// it: a fresh failed_retryable apply within budget is claimed and re-driven
	// on the next poll, with nobody in the loop. Once another apply has taken
	// over its unfinished work, that retry would replay statements against a
	// target where the work already happened, so a superseded apply must never
	// be selected for recovery — it stays failed_retryable for an operator to
	// reconcile.
	t.Run("SupersededMarker_ExcludesRetryableRecovery", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "apply_superseded_retry_db", storage.DatabaseTypeMySQL)
		// One target holds one active apply, so the unmarked baseline and the
		// marked apply live in sibling environments; the retry claim arm keys on
		// the row alone, not the target.
		retryable := CreateApplyWithStateAndEnv(t, store, lock, "apply_retryable_unmarked", 911, state.Apply.FailedRetryable, "staging")
		superseded := CreateApplyWithStateAndEnv(t, store, lock, "apply_retryable_superseded", 912, state.Apply.FailedRetryable, "production")
		require.NoError(t, store.Applies().MarkSuperseded(ctx, superseded.ID, "apply_superseded_retry_successor"))

		claimed, err := store.Applies().ClaimApplyByID(ctx, retryable.ID, "driver-a")
		require.NoError(t, err)
		require.NotNil(t, claimed, "an unmarked fresh failed_retryable apply is claimable for automatic retry")

		refused, err := store.Applies().ClaimApplyByID(ctx, superseded.ID, "driver-a")
		require.NoError(t, err)
		assert.Nil(t, refused, "an apply whose work was taken over must not be selected for automatic retry")

		final, err := store.Applies().Get(ctx, superseded.ID)
		require.NoError(t, err)
		require.NotNil(t, final)
		assert.Equal(t, state.Apply.FailedRetryable, final.State, "the excluded apply keeps its state for an operator to reconcile")
	})

	t.Run("ConcurrentClaim_SingleWinner", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "apply_concurrent_claim_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithTask(t, store, lock, "apply_concurrent_claim", 907)

		const drivers = 16
		start := make(chan struct{})
		results := make(chan *storage.Apply, drivers)
		errors := make(chan error, drivers)
		var wg sync.WaitGroup
		for i := range drivers {
			owner := "driver-" + string(rune('a'+i))
			wg.Go(func() {
				<-start
				claimed, claimErr := store.Applies().ClaimApplyByID(ctx, apply.ID, owner)
				errors <- claimErr
				results <- claimed
			})
		}
		close(start)
		wg.Wait()
		close(results)
		close(errors)

		for claimErr := range errors {
			require.NoError(t, claimErr)
		}
		var winners []*storage.Apply
		for claimed := range results {
			if claimed != nil {
				winners = append(winners, claimed)
			}
		}
		require.Len(t, winners, 1, "only one driver may claim the apply")
		assert.Equal(t, apply.ApplyIdentifier, winners[0].ApplyIdentifier)
		assert.Equal(t, state.Apply.Pending, winners[0].State)
		assert.NotEmpty(t, winners[0].LeaseToken)
		persisted, err := store.Applies().Get(ctx, apply.ID)
		require.NoError(t, err)
		require.NotNil(t, persisted)
		assert.Equal(t, state.Apply.Running, persisted.State)
	})

	dbErrorTests := map[string]func(t *testing.T, store storage.ApplyStore) error{
		"Create_DBError": func(t *testing.T, store storage.ApplyStore) error {
			_, err := store.Create(t.Context(), &storage.Apply{})
			return err
		},
		"CreateWithTasks_DBError": func(t *testing.T, store storage.ApplyStore) error {
			_, err := store.CreateWithTasks(t.Context(), &storage.Apply{}, []*storage.Task{{}})
			return err
		},
		"CreateWithTasksAndOperations_DBError": func(t *testing.T, store storage.ApplyStore) error {
			_, err := store.CreateWithTasksAndOperations(t.Context(), &storage.Apply{}, []*storage.Task{{}}, []*storage.ApplyOperation{{}})
			return err
		},
		"CreateWithGroupedOperations_DBError": func(t *testing.T, store storage.ApplyStore) error {
			_, err := store.CreateWithGroupedOperations(t.Context(), &storage.Apply{}, []*storage.ApplyOperationWithTasks{{Operation: &storage.ApplyOperation{}}})
			return err
		},
		"AttachOperationWithTasks_DBError": func(t *testing.T, store storage.ApplyStore) error {
			return store.AttachOperationWithTasks(t.Context(), &storage.Apply{ID: 1}, &storage.ApplyOperation{}, []*storage.Task{{}})
		},
		"Get_DBError": func(t *testing.T, store storage.ApplyStore) error {
			_, err := store.Get(t.Context(), 1)
			return err
		},
		"GetByApplyIdentifier_DBError": func(t *testing.T, store storage.ApplyStore) error {
			_, err := store.GetByApplyIdentifier(t.Context(), "apply")
			return err
		},
		"GetByIdempotencyKey_DBError": func(t *testing.T, store storage.ApplyStore) error {
			_, err := store.GetByIdempotencyKey(t.Context(), "key")
			return err
		},
		"GetByPlan_DBError": func(t *testing.T, store storage.ApplyStore) error {
			_, err := store.GetByPlan(t.Context(), 1)
			return err
		},
		"GetByLock_DBError": func(t *testing.T, store storage.ApplyStore) error {
			_, err := store.GetByLock(t.Context(), 1)
			return err
		},
		"GetByDatabase_DBError": func(t *testing.T, store storage.ApplyStore) error {
			_, err := store.GetByDatabase(t.Context(), "db", storage.DatabaseTypeMySQL, "staging")
			return err
		},
		"Update_DBError": func(t *testing.T, store storage.ApplyStore) error {
			return store.Update(t.Context(), &storage.Apply{ID: 1})
		},
		"UpdateDerivedState_DBError": func(t *testing.T, store storage.ApplyStore) error {
			_, err := store.UpdateDerivedState(t.Context(), 1, state.Apply.Pending, state.Apply.Running, "", nil, nil)
			return err
		},
		"GetRecent_DBError": func(t *testing.T, store storage.ApplyStore) error {
			_, err := store.GetRecent(t.Context(), storage.RecentAppliesFilter{Limit: 1})
			return err
		},
		"CountRecentByState_DBError": func(t *testing.T, store storage.ApplyStore) error {
			_, err := store.CountRecentByState(t.Context(), storage.RecentAppliesFilter{})
			return err
		},
		"GetInProgress_DBError": func(t *testing.T, store storage.ApplyStore) error {
			_, err := store.GetInProgress(t.Context())
			return err
		},
		"FindStuckPendingApplies_DBError": func(t *testing.T, store storage.ApplyStore) error {
			_, err := store.FindStuckPendingApplies(t.Context(), time.Minute, 1)
			return err
		},
		"ClaimApplyByID_DBError": func(t *testing.T, store storage.ApplyStore) error {
			_, err := store.ClaimApplyByID(t.Context(), 1, "driver")
			return err
		},
		"FindNextApplyForStopReconciliation_DBError": func(t *testing.T, store storage.ApplyStore) error {
			_, err := store.FindNextApplyForStopReconciliation(t.Context(), "driver")
			return err
		},
		"FindNextApplyForOperationProjection_DBError": func(t *testing.T, store storage.ApplyStore) error {
			_, err := store.FindNextApplyForOperationProjection(t.Context(), "driver")
			return err
		},
		"Heartbeat_DBError": func(t *testing.T, store storage.ApplyStore) error {
			return store.Heartbeat(t.Context(), 1)
		},
		"ReleaseClaim_DBError": func(t *testing.T, store storage.ApplyStore) error {
			_, err := store.ReleaseClaim(t.Context(), storage.ApplyLease{ApplyID: 1, Owner: "driver", Token: "token"})
			return err
		},
		"SetRevertSkipped_DBError": func(t *testing.T, store storage.ApplyStore) error {
			return store.SetRevertSkipped(t.Context(), 1, time.Now())
		},
		"MarkSuperseded_DBError": func(t *testing.T, store storage.ApplyStore) error {
			return store.MarkSuperseded(t.Context(), 1, "apply_successor")
		},
		"CheckLease_DBError": func(t *testing.T, store storage.ApplyStore) error {
			return store.CheckLease(t.Context(), storage.ApplyLease{ApplyID: 1, Owner: "driver", Token: "token"})
		},
		"ExpireRetryable_DBError": func(t *testing.T, store storage.ApplyStore) error {
			_, err := store.ExpireRetryable(t.Context())
			return err
		},
		"FindMissingSummaryComment_DBError": func(t *testing.T, store storage.ApplyStore) error {
			_, err := store.FindMissingSummaryComment(t.Context())
			return err
		},
		"GetByPR_DBError": func(t *testing.T, store storage.ApplyStore) error {
			_, err := store.GetByPR(t.Context(), "org/repo", 1)
			return err
		},
		"ExistsForDatabaseHead_DBError": func(t *testing.T, store storage.ApplyStore) error {
			_, err := store.ExistsForDatabaseHead(t.Context(), "org/repo", 1, "db", storage.DatabaseTypeMySQL, "head")
			return err
		},
		"Delete_DBError": func(t *testing.T, store storage.ApplyStore) error {
			return store.Delete(t.Context(), 1)
		},
		"DeleteByPR_DBError": func(t *testing.T, store storage.ApplyStore) error {
			return store.DeleteByPR(t.Context(), "org/repo", 1)
		},
	}
	for name, test := range dbErrorTests {
		t.Run(name, func(t *testing.T) {
			require.Error(t, test(t, h.NewUnreachableStorage(t).Applies()))
		})
	}
}
