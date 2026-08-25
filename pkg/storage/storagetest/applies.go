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
		created := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
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
			CreatedAt:       created,
			UpdatedAt:       created,
		}
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
		started := created.Add(time.Minute)
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

		claimed, err = store.Applies().ClaimApplyByID(ctx, retryable.ID, "driver-b")
		require.NoError(t, err)
		assert.Nil(t, claimed, "a fresh heartbeat prevents lease theft")

		_, err = store.Applies().ClaimApplyByID(ctx, retryable.ID, "")
		require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
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
		assert.False(t, stale.HasFreshLease(time.Now()), "release backdates the heartbeat beyond ApplyLeaseStaleAfter")

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
	})

	t.Run("LeaseGuardsWrites", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "apply_lease_guard_db", storage.DatabaseTypeMySQL)
		claimed := CreateClaimedApply(t, store, lock, "apply_lease_guard", 906, "driver-a")

		staleCtx := storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: claimed.ID, Owner: "old-driver", Token: "stale-token"})
		claimed.State = state.Apply.Failed
		claimed.ErrorMessage = "stale failure"
		require.ErrorIs(t, store.Applies().Update(staleCtx, claimed), storage.ErrApplyLeaseLost)
		require.ErrorIs(t, store.Applies().Heartbeat(staleCtx, claimed.ID), storage.ErrApplyLeaseLost)

		persisted, err := store.Applies().Get(ctx, claimed.ID)
		require.NoError(t, err)
		require.NotNil(t, persisted)
		assert.Equal(t, state.Apply.Running, persisted.State)
		assert.Empty(t, persisted.ErrorMessage)

		ownedCtx := storage.WithApplyLease(ctx, claimed.Lease())
		claimed.State = state.Apply.Completed
		claimed.ErrorMessage = ""
		require.NoError(t, store.Applies().Update(ownedCtx, claimed))
		require.NoError(t, store.Applies().Heartbeat(ownedCtx, claimed.ID))

		persisted, err = store.Applies().Get(ctx, claimed.ID)
		require.NoError(t, err)
		require.NotNil(t, persisted)
		assert.Equal(t, state.Apply.Completed, persisted.State)
	})

	t.Run("ConcurrentClaim_SingleWinner", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "apply_concurrent_claim_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_concurrent_claim", 907)
		now := time.Now().UTC().Truncate(time.Second)
		_, err := store.Tasks().Create(ctx, &storage.Task{
			TaskIdentifier: "task_apply_concurrent_claim", ApplyID: apply.ID, PlanID: apply.PlanID,
			Database: apply.Database, DatabaseType: apply.DatabaseType, Engine: apply.Engine,
			Repository: apply.Repository, PullRequest: apply.PullRequest, Environment: apply.Environment,
			State: state.Task.Pending, Namespace: apply.Database, TableName: "users",
			DDL: "CREATE TABLE users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY)", DDLAction: "create",
			CreatedAt: now, UpdatedAt: now,
		})
		require.NoError(t, err)

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
	})
}
