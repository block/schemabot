package storagetest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// TestLocks runs the behavioral parity suite for storage.LockStore.
//
// Timestamp-advance assertions (Update bumping updated_at, Acquire refresh
// bumping updated_at) are intentionally absent: proving a timestamp moved
// requires backdating the stored row via SQL, which is dialect-specific and
// lives in each implementation's own integration tests.
func TestLocks(t *testing.T, h Harness) {
	t.Run("Acquire", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := &storage.Lock{
			DatabaseName: "acquire_db",
			DatabaseType: storage.DatabaseTypeMySQL,
			Repository:   "org/repo",
			PullRequest:  123,
			Owner:        "owner-a",
		}
		require.NoError(t, store.Locks().Acquire(ctx, lock))

		// Same-owner re-acquire is idempotent: staging and production
		// apply-confirms for the same PR share the lock key and both succeed.
		require.NoError(t, store.Locks().Acquire(ctx, lock))

		// A different owner must be rejected while the lock is held — even
		// from another repository, since the lock key is (database, type)
		// and nothing else.
		other := &storage.Lock{
			DatabaseName: "acquire_db",
			DatabaseType: storage.DatabaseTypeMySQL,
			Repository:   "org/other-repo",
			PullRequest:  456,
			Owner:        "owner-b",
		}
		require.ErrorIs(t, store.Locks().Acquire(ctx, other), storage.ErrLockHeld)

		// The stored row still belongs to the original owner.
		stored, err := store.Locks().Get(ctx, "acquire_db", storage.DatabaseTypeMySQL)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "owner-a", stored.Owner)
		assert.Equal(t, 123, stored.PullRequest)
	})

	t.Run("Acquire_SameOwnerRefreshesPendingPlanID", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := &storage.Lock{
			DatabaseName:  "refresh_db",
			DatabaseType:  storage.DatabaseTypeMySQL,
			Repository:    "org/repo",
			PullRequest:   123,
			Owner:         "owner-a",
			PendingPlanID: "plan-1",
		}
		require.NoError(t, store.Locks().Acquire(ctx, lock))

		// A re-acquire carrying a new pending plan overwrites the stored one:
		// the latest apply or rollback attempt's confirmation plan must be the
		// one the corresponding confirm command loads.
		refresh := &storage.Lock{
			DatabaseName:  "refresh_db",
			DatabaseType:  storage.DatabaseTypeMySQL,
			Repository:    "org/repo",
			PullRequest:   123,
			Owner:         "owner-a",
			PendingPlanID: "plan-2",
		}
		require.NoError(t, store.Locks().Acquire(ctx, refresh))

		stored, err := store.Locks().Get(ctx, "refresh_db", storage.DatabaseTypeMySQL)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "plan-2", stored.PendingPlanID)

		// A re-acquire with an empty pending plan (CLI path) leaves the stored
		// value intact.
		empty := &storage.Lock{
			DatabaseName: "refresh_db",
			DatabaseType: storage.DatabaseTypeMySQL,
			Repository:   "org/repo",
			PullRequest:  123,
			Owner:        "owner-a",
		}
		require.NoError(t, store.Locks().Acquire(ctx, empty))

		stored, err = store.Locks().Get(ctx, "refresh_db", storage.DatabaseTypeMySQL)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "plan-2", stored.PendingPlanID)

		// A re-acquire whose pending plan already matches the stored value
		// succeeds as a read-side no-op: the store sees the match and skips
		// the write. (The write-side race — a concurrent same-owner caller
		// storing the same plan between this caller's read and its write —
		// depends on the dialect's rows-affected semantics and lives in each
		// implementation's own integration tests.)
		require.NoError(t, store.Locks().Acquire(ctx, refresh))

		stored, err = store.Locks().Get(ctx, "refresh_db", storage.DatabaseTypeMySQL)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "plan-2", stored.PendingPlanID)
		assert.Equal(t, "owner-a", stored.Owner)
	})

	t.Run("Release", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		CreateLock(t, store, "release_db", storage.DatabaseTypeMySQL)

		// A non-owner cannot release, and the lock survives the attempt.
		err := store.Locks().Release(ctx, "release_db", storage.DatabaseTypeMySQL, "intruder")
		require.ErrorIs(t, err, storage.ErrLockNotOwned)

		stored, err := store.Locks().Get(ctx, "release_db", storage.DatabaseTypeMySQL)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "testuser", stored.Owner)

		// The owner releases successfully.
		require.NoError(t, store.Locks().Release(ctx, "release_db", storage.DatabaseTypeMySQL, "testuser"))

		// Releasing an already-released lock reports it missing.
		err = store.Locks().Release(ctx, "release_db", storage.DatabaseTypeMySQL, "testuser")
		require.ErrorIs(t, err, storage.ErrLockNotFound)
	})

	t.Run("Release_Isolation", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		// The lock key is (database name, database type): releasing one target
		// must not touch locks that share only the name or only the type.
		CreateLock(t, store, "iso_db1", storage.DatabaseTypeVitess)
		CreateLock(t, store, "iso_db2", storage.DatabaseTypeVitess)
		CreateLock(t, store, "iso_db1", storage.DatabaseTypeMySQL)
		CreateLock(t, store, "iso_db3", storage.DatabaseTypeVitess)

		require.NoError(t, store.Locks().Release(ctx, "iso_db1", storage.DatabaseTypeVitess, "testuser"))

		remaining, err := store.Locks().List(ctx)
		require.NoError(t, err)
		require.Len(t, remaining, 3)

		released, err := store.Locks().Get(ctx, "iso_db1", storage.DatabaseTypeVitess)
		require.NoError(t, err)
		assert.Nil(t, released)

		sameName, err := store.Locks().Get(ctx, "iso_db1", storage.DatabaseTypeMySQL)
		require.NoError(t, err)
		assert.NotNil(t, sameName)
	})

	t.Run("ReleaseIfPendingPlanID", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := &storage.Lock{
			DatabaseName:  "plan_release_db",
			DatabaseType:  storage.DatabaseTypeMySQL,
			Repository:    "org/repo",
			PullRequest:   123,
			Owner:         "owner-a",
			PendingPlanID: "plan-1",
		}
		require.NoError(t, store.Locks().Acquire(ctx, lock))

		// A stale caller whose observed pending plan no longer matches must
		// not release: a superseding apply or rollback intent stays intact.
		released, err := store.Locks().ReleaseIfPendingPlanID(ctx, "plan_release_db", storage.DatabaseTypeMySQL, "owner-a", "plan-stale")
		require.NoError(t, err)
		assert.False(t, released)

		// A matching owner with the wrong observed plan is the same no-op as a
		// wrong owner with the right plan.
		released, err = store.Locks().ReleaseIfPendingPlanID(ctx, "plan_release_db", storage.DatabaseTypeMySQL, "intruder", "plan-1")
		require.NoError(t, err)
		assert.False(t, released)

		stored, err := store.Locks().Get(ctx, "plan_release_db", storage.DatabaseTypeMySQL)
		require.NoError(t, err)
		require.NotNil(t, stored)

		// Owner and pending plan both match: the lock is released.
		released, err = store.Locks().ReleaseIfPendingPlanID(ctx, "plan_release_db", storage.DatabaseTypeMySQL, "owner-a", "plan-1")
		require.NoError(t, err)
		assert.True(t, released)

		stored, err = store.Locks().Get(ctx, "plan_release_db", storage.DatabaseTypeMySQL)
		require.NoError(t, err)
		assert.Nil(t, stored)
	})

	t.Run("ForceRelease", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		CreateLock(t, store, "force_db", storage.DatabaseTypeMySQL)

		// Admin override releases regardless of owner.
		require.NoError(t, store.Locks().ForceRelease(ctx, "force_db", storage.DatabaseTypeMySQL))

		stored, err := store.Locks().Get(ctx, "force_db", storage.DatabaseTypeMySQL)
		require.NoError(t, err)
		assert.Nil(t, stored)

		// Force-releasing a lock that does not exist reports it missing.
		err = store.Locks().ForceRelease(ctx, "force_db", storage.DatabaseTypeMySQL)
		require.ErrorIs(t, err, storage.ErrLockNotFound)
	})

	t.Run("Get", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		// Missing lock is nil, not an error.
		stored, err := store.Locks().Get(ctx, "get_db", storage.DatabaseTypeMySQL)
		require.NoError(t, err)
		require.Nil(t, stored)

		lock := &storage.Lock{
			DatabaseName:  "get_db",
			DatabaseType:  storage.DatabaseTypeMySQL,
			Repository:    "org/repo",
			PullRequest:   321,
			Owner:         "owner-a",
			PendingPlanID: "plan-9",
		}
		require.NoError(t, store.Locks().Acquire(ctx, lock))

		stored, err = store.Locks().Get(ctx, "get_db", storage.DatabaseTypeMySQL)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Positive(t, stored.ID)
		assert.Equal(t, "get_db", stored.DatabaseName)
		assert.Equal(t, storage.DatabaseTypeMySQL, stored.DatabaseType)
		assert.Equal(t, "org/repo", stored.Repository)
		assert.Equal(t, 321, stored.PullRequest)
		assert.Equal(t, "owner-a", stored.Owner)
		assert.Equal(t, "plan-9", stored.PendingPlanID)
		assert.False(t, stored.CreatedAt.IsZero())
		assert.False(t, stored.UpdatedAt.IsZero())
	})

	t.Run("List", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		// Empty store lists no locks.
		locks, err := store.Locks().List(ctx)
		require.NoError(t, err)
		require.Empty(t, locks)

		CreateLock(t, store, "list_db1", storage.DatabaseTypeMySQL)
		CreateLock(t, store, "list_db2", storage.DatabaseTypeVitess)
		CreateLock(t, store, "list_db3", storage.DatabaseTypeMySQL)

		locks, err = store.Locks().List(ctx)
		require.NoError(t, err)
		require.Len(t, locks, 3)

		names := make(map[string]bool, len(locks))
		for _, l := range locks {
			names[l.DatabaseName] = true
		}
		assert.True(t, names["list_db1"])
		assert.True(t, names["list_db2"])
		assert.True(t, names["list_db3"])
	})

	t.Run("GetByPR", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		// No locks for the PR yet.
		locks, err := store.Locks().GetByPR(ctx, "org/repo", 123)
		require.NoError(t, err)
		require.Empty(t, locks)

		// Three locks for PR 123, one for PR 456, one for another repository
		// with the same PR number.
		CreateLockWithPR(t, store, "pr_db1", storage.DatabaseTypeMySQL, "org/repo", 123)
		CreateLockWithPR(t, store, "pr_db2", storage.DatabaseTypeVitess, "org/repo", 123)
		CreateLockWithPR(t, store, "pr_db3", storage.DatabaseTypeMySQL, "org/repo", 123)
		CreateLockWithPR(t, store, "pr_db4", storage.DatabaseTypeMySQL, "org/repo", 456)
		CreateLockWithPR(t, store, "pr_db5", storage.DatabaseTypeMySQL, "org/other", 123)

		locks, err = store.Locks().GetByPR(ctx, "org/repo", 123)
		require.NoError(t, err)
		require.Len(t, locks, 3)
		for _, l := range locks {
			assert.Equal(t, "org/repo", l.Repository)
			assert.Equal(t, 123, l.PullRequest)
		}

		locks, err = store.Locks().GetByPR(ctx, "org/repo", 456)
		require.NoError(t, err)
		require.Len(t, locks, 1)
		assert.Equal(t, "pr_db4", locks[0].DatabaseName)
	})

	t.Run("Update", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "update_db", storage.DatabaseTypeMySQL)

		// Touching an existing lock as its owner succeeds and keeps the row
		// intact.
		require.NoError(t, store.Locks().Update(ctx, lock))

		stored, err := store.Locks().Get(ctx, "update_db", storage.DatabaseTypeMySQL)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "testuser", stored.Owner)

		// The touch is owner-scoped: a caller whose lock was re-acquired by
		// somebody else must not refresh the new owner's row.
		intruder := &storage.Lock{
			DatabaseName: "update_db",
			DatabaseType: storage.DatabaseTypeMySQL,
			Owner:        "other-owner",
		}
		require.ErrorIs(t, store.Locks().Update(ctx, intruder), storage.ErrLockNotOwned)

		stored, err = store.Locks().Get(ctx, "update_db", storage.DatabaseTypeMySQL)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, "testuser", stored.Owner, "a refused touch must not change ownership")

		// Touching a lock that does not exist reports it missing.
		missing := &storage.Lock{
			DatabaseName: "update_missing_db",
			DatabaseType: storage.DatabaseTypeMySQL,
			Owner:        "testuser",
		}
		require.ErrorIs(t, store.Locks().Update(ctx, missing), storage.ErrLockNotFound)
	})

	t.Run("Acquire_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		err := store.Locks().Acquire(t.Context(), &storage.Lock{
			DatabaseName: "err_db",
			DatabaseType: storage.DatabaseTypeMySQL,
			Repository:   "org/repo",
			PullRequest:  1,
			Owner:        "owner-a",
		})
		require.Error(t, err)
	})

	t.Run("Release_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		err := store.Locks().Release(t.Context(), "err_db", storage.DatabaseTypeMySQL, "owner-a")
		require.Error(t, err)
	})

	t.Run("ReleaseIfPendingPlanID_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.Locks().ReleaseIfPendingPlanID(t.Context(), "err_db", storage.DatabaseTypeMySQL, "owner-a", "plan-1")
		require.Error(t, err)
	})

	t.Run("ForceRelease_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		err := store.Locks().ForceRelease(t.Context(), "err_db", storage.DatabaseTypeMySQL)
		require.Error(t, err)
	})

	t.Run("Get_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.Locks().Get(t.Context(), "err_db", storage.DatabaseTypeMySQL)
		require.Error(t, err)
	})

	t.Run("List_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.Locks().List(t.Context())
		require.Error(t, err)
	})

	t.Run("GetByPR_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.Locks().GetByPR(t.Context(), "org/repo", 1)
		require.Error(t, err)
	})

	t.Run("Update_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		err := store.Locks().Update(t.Context(), &storage.Lock{
			DatabaseName: "err_db",
			DatabaseType: storage.DatabaseTypeMySQL,
		})
		require.Error(t, err)
	})
}
