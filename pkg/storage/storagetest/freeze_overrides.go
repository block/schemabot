package storagetest

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/storage"
)

// TestFreezeOverrides runs the behavioral parity suite for
// storage.FreezeOverrideStore.
func TestFreezeOverrides(t *testing.T, h Harness) {
	t.Run("CreateAndGet", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		want := overrideRequest("Production", "plan-Ab12", time.Now().UTC().Add(24*time.Hour))
		want.Reason = "  SEV-1234 hotfix index  "
		created, err := store.FreezeOverrides().Create(ctx, want)
		require.NoError(t, err)
		require.NotZero(t, created.ID)

		got, err := store.FreezeOverrides().Get(ctx, want.Identifier)
		require.NoError(t, err)
		assert.Equal(t, created.ID, got.ID)
		assert.Equal(t, want.Identifier, got.Identifier)
		assert.Equal(t, "production", got.Environment, "environment is stored canonical")
		assert.Equal(t, storage.FreezeOverrideActionApply, got.Action)
		assert.Equal(t, "plan-Ab12", got.TargetID, "the target identifier keeps its case")
		assert.Equal(t, "orders", got.Database, "database is stored canonical")
		assert.Equal(t, "block/orders", got.Repository, "repository is stored canonical")
		assert.Equal(t, 42, got.PullRequest)
		assert.Equal(t, "carol", got.PRAuthor)
		assert.Equal(t, []string{"freeze frz-1", "freeze frz-2"}, got.Blockers)
		assert.Equal(t, "SEV-1234 hotfix index", got.Reason, "the reason is stored trimmed")
		assert.Equal(t, "alice", got.RequestedBy)
		assert.Empty(t, got.ApprovedBy)
		assert.Nil(t, got.ApprovedAt)
		assert.WithinDuration(t, want.ExpiresAt, got.ExpiresAt, time.Millisecond)
		assert.False(t, got.CreatedAt.IsZero())
	})

	t.Run("Create_RejectsAnInvalidRequest", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		request := overrideRequest("production", "plan-1", time.Now().UTC().Add(time.Hour))
		request.Blockers = nil
		_, err := store.FreezeOverrides().Create(ctx, request)
		require.ErrorContains(t, err, "at least one blocker")

		_, err = store.FreezeOverrides().Get(ctx, request.Identifier)
		require.ErrorIs(t, err, storage.ErrFreezeOverrideNotFound, "an invalid request is never stored")
	})

	t.Run("Create_DuplicateIdentifier", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		request := overrideRequest("production", "plan-1", time.Now().UTC().Add(time.Hour))
		_, err := store.FreezeOverrides().Create(ctx, request)
		require.NoError(t, err)
		_, err = store.FreezeOverrides().Create(ctx, request)
		require.ErrorIs(t, err, storage.ErrFreezeOverrideExists)
	})

	t.Run("Get_NotFound", func(t *testing.T) {
		_, err := h.NewStorage(t).FreezeOverrides().Get(t.Context(), "ovr-missing")
		require.ErrorIs(t, err, storage.ErrFreezeOverrideNotFound)
	})

	t.Run("Approve", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		request := overrideRequest("production", "plan-1", time.Now().UTC().Add(time.Hour))
		_, err := store.FreezeOverrides().Create(ctx, request)
		require.NoError(t, err)

		approved, err := store.FreezeOverrides().Approve(ctx, request.Identifier, "bob")
		require.NoError(t, err)
		assert.Equal(t, "bob", approved.ApprovedBy)
		require.NotNil(t, approved.ApprovedAt)

		_, err = store.FreezeOverrides().Approve(ctx, request.Identifier, "dave")
		require.ErrorIs(t, err, storage.ErrFreezeOverrideAlreadyApproved)
		got, err := store.FreezeOverrides().Get(ctx, request.Identifier)
		require.NoError(t, err)
		assert.Equal(t, "bob", got.ApprovedBy, "a second approval does not replace the first")
	})

	t.Run("Approve_RefusesTheRequesterAndThePullRequestAuthor", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		request := overrideRequest("production", "plan-1", time.Now().UTC().Add(time.Hour))
		_, err := store.FreezeOverrides().Create(ctx, request)
		require.NoError(t, err)

		for _, who := range []string{"alice", "Alice", " carol ", "CAROL"} {
			_, err := store.FreezeOverrides().Approve(ctx, request.Identifier, who)
			require.ErrorIs(t, err, storage.ErrFreezeOverrideSelfApproval, "approver %q", who)
		}
		got, err := store.FreezeOverrides().Get(ctx, request.Identifier)
		require.NoError(t, err)
		assert.Nil(t, got.ApprovedAt, "a refused approval records nothing")
	})

	t.Run("Approve_RefusesAnExpiredRequest", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		request := overrideRequest("production", "plan-1", time.Now().UTC().Add(-time.Hour))
		_, err := store.FreezeOverrides().Create(ctx, request)
		require.NoError(t, err)

		_, err = store.FreezeOverrides().Approve(ctx, request.Identifier, "bob")
		require.ErrorIs(t, err, storage.ErrFreezeOverrideExpired)
	})

	t.Run("Approve_NotFound", func(t *testing.T) {
		_, err := h.NewStorage(t).FreezeOverrides().Approve(t.Context(), "ovr-missing", "bob")
		require.ErrorIs(t, err, storage.ErrFreezeOverrideNotFound)
	})

	t.Run("Approve_ConcurrentApprovalsLandOnce", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		request := overrideRequest("production", "plan-1", time.Now().UTC().Add(time.Hour))
		_, err := store.FreezeOverrides().Create(ctx, request)
		require.NoError(t, err)

		const approvers = 5
		errs := make([]error, approvers)
		var wg sync.WaitGroup
		for i := range approvers {
			wg.Go(func() {
				_, errs[i] = store.FreezeOverrides().Approve(ctx, request.Identifier, "approver")
			})
		}
		wg.Wait()

		var succeeded int
		for _, err := range errs {
			if err == nil {
				succeeded++
				continue
			}
			require.ErrorIs(t, err, storage.ErrFreezeOverrideAlreadyApproved)
		}
		assert.Equal(t, 1, succeeded)
	})

	t.Run("ListApproved", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		later := time.Now().UTC().Add(time.Hour)

		pending := overrideRequest("production", "plan-1", later)
		first := overrideRequest("production", "plan-1", later)
		second := overrideRequest("Production", "plan-1", later)
		otherTarget := overrideRequest("production", "PLAN-1", later)
		otherAction := overrideRequest("production", "plan-1", later)
		otherAction.Action = storage.FreezeOverrideActionCutover
		otherEnvironment := overrideRequest("staging", "plan-1", later)
		for _, request := range []*storage.FreezeOverrideRequest{pending, first, second, otherTarget, otherAction, otherEnvironment} {
			_, err := store.FreezeOverrides().Create(ctx, request)
			require.NoError(t, err)
		}
		for _, request := range []*storage.FreezeOverrideRequest{first, second, otherTarget, otherAction, otherEnvironment} {
			_, err := store.FreezeOverrides().Approve(ctx, request.Identifier, "bob")
			require.NoError(t, err)
		}

		approved, err := store.FreezeOverrides().ListApproved(ctx, storage.FreezeOverrideTarget{
			Environment: "PRODUCTION",
			Action:      storage.FreezeOverrideActionApply,
			TargetID:    "plan-1",
		})
		require.NoError(t, err)
		require.Len(t, approved, 2, "only approved requests for this environment, action and target")
		assert.Equal(t, second.Identifier, approved[0].Identifier, "most recently approved first")
		assert.Equal(t, first.Identifier, approved[1].Identifier)
	})

	t.Run("ListApproved_LeavesOutExpiredRequests", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		// Approve while unexpired, then let it lapse: a request created
		// already expired cannot be approved at all.
		now, err := store.Freezes().Now(ctx)
		require.NoError(t, err)
		request := overrideRequest("production", "plan-1", now.Add(2*time.Second))
		_, err = store.FreezeOverrides().Create(ctx, request)
		require.NoError(t, err)
		_, err = store.FreezeOverrides().Approve(ctx, request.Identifier, "bob")
		require.NoError(t, err)

		target := storage.FreezeOverrideTarget{Environment: "production", Action: storage.FreezeOverrideActionApply, TargetID: "plan-1"}
		require.Eventually(t, func() bool {
			approved, err := store.FreezeOverrides().ListApproved(ctx, target)
			return err == nil && len(approved) == 0
		}, 10*time.Second, 200*time.Millisecond, "an expired approval stops counting")
	})

	t.Run("UnreachableStorage", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewUnreachableStorage(t).FreezeOverrides()

		_, err := store.Create(ctx, overrideRequest("production", "plan-1", time.Now().UTC().Add(time.Hour)))
		require.Error(t, err)
		_, err = store.Get(ctx, "ovr-1")
		require.Error(t, err)
		require.NotErrorIs(t, err, storage.ErrFreezeOverrideNotFound)
		_, err = store.Approve(ctx, "ovr-1", "bob")
		require.Error(t, err)
		_, err = store.ListApproved(ctx, storage.FreezeOverrideTarget{Environment: "production", Action: storage.FreezeOverrideActionApply, TargetID: "plan-1"})
		require.Error(t, err)
	})
}

func overrideRequest(environment, targetID string, expiresAt time.Time) *storage.FreezeOverrideRequest {
	return &storage.FreezeOverrideRequest{
		Identifier:  engine.NewFreezeOverrideID(),
		Environment: environment,
		Action:      storage.FreezeOverrideActionApply,
		TargetID:    targetID,
		Database:    "Orders",
		Repository:  "Block/Orders",
		PullRequest: 42,
		PRAuthor:    "carol",
		Blockers:    []string{"freeze frz-1", "freeze frz-2"},
		Reason:      "SEV-1234 hotfix index",
		RequestedBy: "alice",
		ExpiresAt:   expiresAt,
	}
}
