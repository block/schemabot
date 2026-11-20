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

// TestFreezes runs the behavioral parity suite for storage.FreezeStore.
func TestFreezes(t *testing.T, h Harness) {
	t.Run("CreateAndGet", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		now := time.Now().UTC()

		want := onDemandFreeze("Production", now.Add(-time.Minute))
		want.Deployments = []string{"Cash", "Square"}
		want.Databases = []string{"Orders"}
		created, err := store.Freezes().Create(ctx, want)
		require.NoError(t, err)
		require.NotZero(t, created.ID)

		got, err := store.Freezes().Get(ctx, want.Identifier)
		require.NoError(t, err)
		assert.Equal(t, created.ID, got.ID)
		assert.Equal(t, want.Identifier, got.Identifier)
		assert.Equal(t, "production", got.Environment, "environment is stored canonical")
		assert.Equal(t, []string{"cash", "square"}, got.Deployments, "deployments are stored canonical")
		assert.Equal(t, []string{"orders"}, got.Databases, "databases are stored canonical")
		assert.Equal(t, storage.FreezeKindOnDemand, got.Kind)
		assert.Equal(t, storage.FreezeOverrideAdmins, got.Override)
		assert.WithinDuration(t, want.StartsAt, got.StartsAt, time.Millisecond)
		assert.Nil(t, got.EndsAt, "an on-demand freeze may be open-ended")
		assert.Equal(t, want.Reason, got.Reason)
		assert.Equal(t, want.CreatedBy, got.CreatedBy)
		assert.Equal(t, storage.FreezeSourceManual, got.Source)
		assert.Empty(t, got.ExternalID)
		assert.Empty(t, got.LiftedBy)
		assert.Nil(t, got.LiftedAt)
		assert.False(t, got.CreatedAt.IsZero())
	})

	t.Run("Create_RejectsAnInvalidFreeze", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		freeze := onDemandFreeze("production", time.Now().UTC())
		freeze.Reason = "  "
		_, err := store.Freezes().Create(ctx, freeze)
		require.ErrorContains(t, err, "reason is required")

		_, err = store.Freezes().Get(ctx, freeze.Identifier)
		require.ErrorIs(t, err, storage.ErrFreezeNotFound, "an invalid freeze is never stored")
	})

	t.Run("Create_DuplicateIdentifier", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		freeze := onDemandFreeze("production", time.Now().UTC())
		_, err := store.Freezes().Create(ctx, freeze)
		require.NoError(t, err)
		_, err = store.Freezes().Create(ctx, freeze)
		require.ErrorIs(t, err, storage.ErrFreezeExists)
	})

	t.Run("Create_DuplicateSourceAndExternalID", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		start := time.Now().UTC().Add(time.Hour)

		freeze := plannedFreeze("production", start, start.Add(time.Hour), "chill-calendar", "event-1")
		created, err := store.Freezes().Create(ctx, freeze)
		require.NoError(t, err)
		require.NotNil(t, created.EndsAt)
		assert.WithinDuration(t, *freeze.EndsAt, *created.EndsAt, time.Millisecond)
		assert.Equal(t, storage.FreezeOverrideAnyone, created.Override)
		assert.Equal(t, "event-1", created.ExternalID)

		_, err = store.Freezes().Create(ctx, plannedFreeze("staging", start, start.Add(time.Hour), "chill-calendar", "event-1"))
		require.ErrorIs(t, err, storage.ErrFreezeExists, "a source owns each external id once")

		_, err = store.Freezes().Create(ctx, plannedFreeze("production", start, start.Add(time.Hour), "manual-chill", "event-1"))
		require.NoError(t, err, "another source may reuse the external id")

		// External ids belong to the source and compare byte for byte on
		// both dialects, so ids differing only in case are distinct entries.
		_, err = store.Freezes().Create(ctx, plannedFreeze("production", start, start.Add(time.Hour), "chill-calendar", "Event-1"))
		require.NoError(t, err, "external ids are case-sensitive")
	})

	t.Run("Create_ManyManualFreezes", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		now := time.Now().UTC()

		// Manual freezes have no external id, so the source and external id
		// key must not treat them as duplicates.
		for range 3 {
			created := createFreeze(t, store, onDemandFreeze("production", now))
			assert.Nil(t, created.Deployments, "a whole-environment scope reads back nil")
		}
		inEffect, err := store.Freezes().ListInEffect(ctx, "production", now.Add(time.Second))
		require.NoError(t, err)
		assert.Len(t, inEffect, 3)
	})

	t.Run("ListInEffect", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		now := time.Now().UTC()

		active := createFreeze(t, store, onDemandFreeze("production", now.Add(-time.Hour)))
		windowed := createFreeze(t, store, plannedFreeze("production", now.Add(-30*time.Minute), now.Add(time.Hour), "chill-calendar", "now"))
		createFreeze(t, store, plannedFreeze("production", now.Add(time.Hour), now.Add(2*time.Hour), "chill-calendar", "later"))
		createFreeze(t, store, plannedFreeze("production", now.Add(-2*time.Hour), now.Add(-time.Hour), "chill-calendar", "ended"))
		lifted := createFreeze(t, store, onDemandFreeze("production", now.Add(-time.Hour)))
		_, err := store.Freezes().Lift(ctx, lifted.Identifier, "alice", "resolved", now.Add(-time.Minute))
		require.NoError(t, err)
		staging := createFreeze(t, store, onDemandFreeze("staging", now.Add(-time.Hour)))

		inEffect, err := store.Freezes().ListInEffect(ctx, "Production", now)
		require.NoError(t, err)
		assert.Equal(t, []string{active.Identifier, windowed.Identifier}, freezeIdentifiers(inEffect),
			"only started, unended, unlifted freezes of the environment, oldest start first")

		all, err := store.Freezes().ListInEffect(ctx, "", now)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{active.Identifier, windowed.Identifier, staging.Identifier}, freezeIdentifiers(all))

		later, err := store.Freezes().ListInEffect(ctx, "production", now.Add(90*time.Minute))
		require.NoError(t, err)
		assert.Len(t, later, 2, "the windowed freeze ends and the later one starts on their own")
	})

	t.Run("Lift", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		now := time.Now().UTC()

		freeze := createFreeze(t, store, onDemandFreeze("production", now.Add(-time.Hour)))
		lifted, err := store.Freezes().Lift(ctx, freeze.Identifier, "bob", "SEV resolved", now)
		require.NoError(t, err)
		assert.Equal(t, "bob", lifted.LiftedBy)
		assert.Equal(t, "SEV resolved", lifted.LiftReason)
		require.NotNil(t, lifted.LiftedAt)
		assert.WithinDuration(t, now, *lifted.LiftedAt, time.Millisecond)
		assert.False(t, lifted.InEffectAt(now.Add(time.Second)))

		got, err := store.Freezes().Get(ctx, freeze.Identifier)
		require.NoError(t, err)
		assert.Equal(t, "bob", got.LiftedBy, "a lifted freeze stays as the audit trail")
	})

	t.Run("Lift_RequiresTheCaller", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		now := time.Now().UTC()

		freeze := createFreeze(t, store, onDemandFreeze("production", now.Add(-time.Hour)))
		_, err := store.Freezes().Lift(ctx, freeze.Identifier, " ", "", now)
		require.Error(t, err)
		got, err := store.Freezes().Get(ctx, freeze.Identifier)
		require.NoError(t, err)
		assert.Nil(t, got.LiftedAt)

		lifted, err := store.Freezes().Lift(ctx, freeze.Identifier, "bob", "", now)
		require.NoError(t, err, "the lift reason is optional")
		assert.Empty(t, lifted.LiftReason)
	})

	t.Run("NotFound", func(t *testing.T) {
		store := h.NewStorage(t)
		_, err := store.Freezes().Get(t.Context(), "freeze-missing")
		require.ErrorIs(t, err, storage.ErrFreezeNotFound)
		_, err = store.Freezes().Lift(t.Context(), "freeze-missing", "bob", "", time.Now().UTC())
		require.ErrorIs(t, err, storage.ErrFreezeNotFound)
	})

	t.Run("Lift_AlreadyLifted", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		now := time.Now().UTC()

		freeze := createFreeze(t, store, onDemandFreeze("production", now.Add(-time.Hour)))
		_, err := store.Freezes().Lift(ctx, freeze.Identifier, "alice", "first", now)
		require.NoError(t, err)
		_, err = store.Freezes().Lift(ctx, freeze.Identifier, "bob", "second", now.Add(time.Second))
		require.ErrorIs(t, err, storage.ErrFreezeAlreadyLifted)

		got, err := store.Freezes().Get(ctx, freeze.Identifier)
		require.NoError(t, err)
		assert.Equal(t, "alice", got.LiftedBy, "the second lift changes nothing")
		assert.Equal(t, "first", got.LiftReason)
	})

	t.Run("Lift_Ended", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		now := time.Now().UTC()

		freeze := createFreeze(t, store, plannedFreeze("production", now.Add(-2*time.Hour), now.Add(-time.Hour), "chill-calendar", "ended"))
		_, err := store.Freezes().Lift(ctx, freeze.Identifier, "bob", "", now)
		require.ErrorIs(t, err, storage.ErrFreezeEnded)
	})

	t.Run("Lift_ScheduledFreeze", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		now := time.Now().UTC()

		// Lifting a freeze before it starts cancels it.
		freeze := createFreeze(t, store, plannedFreeze("production", now.Add(time.Hour), now.Add(2*time.Hour), "chill-calendar", "later"))
		_, err := store.Freezes().Lift(ctx, freeze.Identifier, "bob", "launch moved", now)
		require.NoError(t, err)

		inEffect, err := store.Freezes().ListInEffect(ctx, "production", now.Add(90*time.Minute))
		require.NoError(t, err)
		assert.Empty(t, inEffect, "a freeze lifted before it starts never takes effect")
	})

	t.Run("Lift_ConcurrentLiftsEndItOnce", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		now := time.Now().UTC()

		freeze := createFreeze(t, store, onDemandFreeze("production", now.Add(-time.Hour)))
		const lifters = 5
		errs := make([]error, lifters)
		var wg sync.WaitGroup
		for i := range lifters {
			wg.Go(func() {
				_, errs[i] = store.Freezes().Lift(ctx, freeze.Identifier, "lifter", "", now)
			})
		}
		wg.Wait()

		var succeeded int
		for _, err := range errs {
			if err == nil {
				succeeded++
				continue
			}
			require.ErrorIs(t, err, storage.ErrFreezeAlreadyLifted)
		}
		assert.Equal(t, 1, succeeded)
	})

	t.Run("UnreachableStorageSurfacesErrors", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewUnreachableStorage(t)
		now := time.Now().UTC()

		_, err := store.Freezes().Create(ctx, onDemandFreeze("production", now))
		require.Error(t, err)
		_, err = store.Freezes().Get(ctx, "freeze-any")
		require.Error(t, err)
		require.NotErrorIs(t, err, storage.ErrFreezeNotFound, "a connection failure is not a missing freeze")
		_, err = store.Freezes().Lift(ctx, "freeze-any", "bob", "", now)
		require.Error(t, err)
		_, err = store.Freezes().ListInEffect(ctx, "production", now)
		require.Error(t, err)
	})
}

func onDemandFreeze(environment string, startsAt time.Time) *storage.EnvironmentFreeze {
	return &storage.EnvironmentFreeze{
		Identifier:  engine.NewFreezeID(),
		Environment: environment,
		Kind:        storage.FreezeKindOnDemand,
		Override:    storage.FreezeOverrideAdmins,
		StartsAt:    startsAt,
		Reason:      "SEV-1234 payments latency",
		CreatedBy:   "alice",
		Source:      storage.FreezeSourceManual,
	}
}

func plannedFreeze(environment string, startsAt, endsAt time.Time, source, externalID string) *storage.EnvironmentFreeze {
	return &storage.EnvironmentFreeze{
		Identifier:  engine.NewFreezeID(),
		Environment: environment,
		Kind:        storage.FreezeKindPlanned,
		Override:    storage.FreezeOverrideAnyone,
		StartsAt:    startsAt,
		EndsAt:      &endsAt,
		Reason:      "Planned code freeze",
		CreatedBy:   source,
		Source:      source,
		ExternalID:  externalID,
	}
}

func createFreeze(t *testing.T, store storage.Storage, freeze *storage.EnvironmentFreeze) *storage.EnvironmentFreeze {
	t.Helper()
	created, err := store.Freezes().Create(t.Context(), freeze)
	require.NoError(t, err)
	return created
}

func freezeIdentifiers(freezes []*storage.EnvironmentFreeze) []string {
	identifiers := make([]string, 0, len(freezes))
	for _, freeze := range freezes {
		identifiers = append(identifiers, freeze.Identifier)
	}
	return identifiers
}
