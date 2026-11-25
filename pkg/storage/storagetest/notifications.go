package storagetest

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func TestNotifications(t *testing.T, h Harness) {
	t.Run("ScanClaimReleasePublishAndObservation", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		now := time.Now().UTC().Truncate(time.Second)
		var ids []int64
		for i, fixture := range []struct {
			state  string
			age    time.Duration
			github bool
		}{
			{state.Apply.Completed, time.Hour, false},
			{state.Apply.Completed, 2 * time.Hour, true},
			{state.Apply.Completed, 25 * time.Hour, false},
			{state.Apply.Stopped, time.Hour, false},
			{state.Apply.Failed, time.Hour, false},
		} {
			completedAt := now.Add(-fixture.age)
			apply := &storage.Apply{ApplyIdentifier: fmt.Sprintf("notify-%d", i), Database: "notifydb", DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging", State: fixture.state, CompletedAt: &completedAt}
			if fixture.github {
				apply.Repository = "example/schema"
				apply.PullRequest = 1
			}
			id, err := store.Applies().Create(ctx, apply)
			require.NoError(t, err)
			apply.ID = id
			require.NoError(t, store.Applies().Update(ctx, apply))
			ids = append(ids, id)
		}
		notifications := store.Notifications()
		applies, err := notifications.FindUnpublished(ctx, 24*time.Hour, 1)
		require.NoError(t, err)
		require.Len(t, applies, 1)
		assert.Equal(t, ids[1], applies[0].ID, "oldest first, with a bounded batch")
		applies, err = notifications.FindUnpublished(ctx, 24*time.Hour, 100)
		require.NoError(t, err)
		require.Len(t, applies, 2, "non-GitHub included; old/failed/stopped excluded")
		won, err := notifications.Claim(ctx, ids[0], "first", time.Minute)
		require.NoError(t, err)
		require.True(t, won)
		won, err = notifications.Claim(ctx, ids[0], "second", time.Minute)
		require.NoError(t, err)
		assert.False(t, won)
		applies, err = notifications.FindUnpublished(ctx, 24*time.Hour, 100)
		require.NoError(t, err)
		require.Len(t, applies, 1, "fresh claim excludes delivery, not observation")
		ceiling, at, err := notifications.ObservationCeiling(ctx)
		require.NoError(t, err)
		assert.Equal(t, ids[4], ceiling)
		assert.WithinDuration(t, time.Now(), at, 5*time.Second)
		samples, err := notifications.ObservePage(ctx, 0, ceiling, 2)
		require.NoError(t, err)
		require.Len(t, samples, 2)
		assert.False(t, samples[0].Published)
		assert.True(t, samples[0].Completed)
		changed, err := notifications.Release(ctx, ids[0], "second", "must not clear another claim")
		require.NoError(t, err)
		assert.False(t, changed)
		changed, err = notifications.MarkPublished(ctx, ids[0], "second")
		require.NoError(t, err)
		assert.False(t, changed)
		changed, err = notifications.Release(ctx, ids[0], "first", "SQS unavailable")
		require.NoError(t, err)
		assert.True(t, changed)
		won, err = notifications.Claim(ctx, ids[0], "retry", time.Minute)
		require.NoError(t, err)
		require.True(t, won)
		changed, err = notifications.MarkPublished(ctx, ids[0], "retry")
		require.NoError(t, err)
		require.True(t, changed)
		won, err = notifications.Claim(ctx, ids[0], "after-publish", time.Minute)
		require.NoError(t, err)
		assert.False(t, won)
		samples, err = notifications.ObservePage(ctx, 0, ceiling, 10)
		require.NoError(t, err)
		require.Len(t, samples, 5)
		assert.True(t, samples[0].Published)
		assert.False(t, samples[3].Completed)
	})
	t.Run("ConcurrentClaimHasOneWinner", func(t *testing.T) {
		store := h.NewStorage(t)
		now := time.Now().Add(-time.Hour)
		id, err := store.Applies().Create(t.Context(), &storage.Apply{ApplyIdentifier: "concurrent-notification", Database: "notifydb", DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging", State: state.Apply.Completed, CompletedAt: &now})
		require.NoError(t, err)
		var wins atomic.Int32
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for i := range 8 {
			wg.Go(func() {
				won, err := store.Notifications().Claim(t.Context(), id, fmt.Sprintf("pod-%d", i), time.Minute)
				if err != nil {
					errs <- err
				}
				if won {
					wins.Add(1)
				}
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		assert.Equal(t, int32(1), wins.Load())
	})
	t.Run("ErrorsSurface", func(t *testing.T) {
		n := h.NewUnreachableStorage(t).Notifications()
		_, err := n.FindUnpublished(t.Context(), time.Hour, 1)
		require.Error(t, err)
		_, err = n.Claim(t.Context(), 1, "claim", time.Minute)
		require.Error(t, err)
		_, err = n.MarkPublished(t.Context(), 1, "claim")
		require.Error(t, err)
		_, err = n.Release(t.Context(), 1, "claim", "failed")
		require.Error(t, err)
		_, _, err = n.ObservationCeiling(t.Context())
		require.Error(t, err)
		_, err = n.ObservePage(t.Context(), 0, 1, 1)
		require.Error(t, err)
	})
}
