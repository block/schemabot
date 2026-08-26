package storagetest

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// TestWebhookEvents runs the behavioral parity suite for
// storage.WebhookEventStore: delivery deduplication and redelivery, claim
// ordering and retry windows, expired-lease recovery, terminal transitions,
// and claim contention.
func TestWebhookEvents(t *testing.T, h Harness) {
	newEvent := func(deliveryID string) *storage.WebhookEvent {
		return &storage.WebhookEvent{
			DeliveryID:  deliveryID,
			Event:       "pull_request",
			Action:      "synchronize",
			Repository:  "block/example",
			PullRequest: 123,
			HeadSHA:     "abc123",
			TenantID:    "456",
			Payload:     []byte(`{"action":"synchronize"}`),
		}
	}

	t.Run("Create_DeduplicatesDeliveryID", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		event := newEvent("delivery-dedup")
		inserted, err := store.WebhookEvents().Create(ctx, event)
		require.NoError(t, err)
		require.True(t, inserted)
		require.Positive(t, event.ID)

		duplicate := newEvent("delivery-dedup")
		duplicate.Payload = []byte(`{"duplicate":true}`)
		inserted, err = store.WebhookEvents().Create(ctx, duplicate)
		require.NoError(t, err)
		assert.False(t, inserted)

		stored, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-dedup")
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, event.ID, stored.ID)
		assert.Equal(t, storage.WebhookProviderGitHub, stored.Provider)
		assert.Equal(t, storage.WebhookEventPending, stored.State)
		assert.Equal(t, "block/example", stored.Repository)
		assert.Equal(t, 123, stored.PullRequest)
		assert.Equal(t, "456", stored.TenantID)
		assert.JSONEq(t, `{"action":"synchronize"}`, string(stored.Payload))

		otherProvider := newEvent("delivery-dedup")
		otherProvider.Provider = "gitlab"
		inserted, err = store.WebhookEvents().Create(ctx, otherProvider)
		require.NoError(t, err)
		assert.True(t, inserted, "delivery IDs are unique within a provider")
	})

	t.Run("FindNext_OrdersOldestEligibleFirst", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		future := time.Now().UTC().Add(time.Hour)
		deferred := newEvent("delivery-deferred")
		deferred.RetryAfter = &future
		inserted, err := store.WebhookEvents().Create(ctx, deferred)
		require.NoError(t, err)
		require.True(t, inserted)

		for _, deliveryID := range []string{"delivery-first", "delivery-second"} {
			inserted, err = store.WebhookEvents().Create(ctx, newEvent(deliveryID))
			require.NoError(t, err)
			require.True(t, inserted)
		}

		first, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, first)
		assert.Equal(t, "delivery-first", first.DeliveryID)
		assert.Equal(t, storage.WebhookEventProcessing, first.State)
		assert.Equal(t, 1, first.Attempts)
		assert.Equal(t, "driver-a", first.LeaseOwner)
		assert.NotEmpty(t, first.LeaseToken)

		second, err := store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, second)
		assert.Equal(t, "delivery-second", second.DeliveryID)

		none, err := store.WebhookEvents().FindNext(ctx, "driver-c", time.Minute)
		require.NoError(t, err)
		assert.Nil(t, none, "a future not-before row is not claimable")
	})

	t.Run("RetryWindow", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		inserted, err := store.WebhookEvents().Create(ctx, newEvent("delivery-retry"))
		require.NoError(t, err)
		require.True(t, inserted)

		claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claimed)

		future := time.Now().UTC().Add(time.Hour)
		require.NoError(t, store.WebhookEvents().MarkFailed(ctx, claimed.ID, claimed.LeaseToken, "temporary failure", &future))

		none, err := store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
		require.NoError(t, err)
		require.Nil(t, none)

		past := time.Now().UTC().Add(-time.Hour)
		require.NoError(t, store.WebhookEvents().MarkFailed(ctx, claimed.ID, claimed.LeaseToken, "retry now", &past))
		retryable, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-retry")
		require.NoError(t, err)
		require.NotNil(t, retryable)
		require.NotNil(t, retryable.RetryAfter)
		assert.Equal(t, storage.WebhookEventFailedRetryable, retryable.State)
		assert.Equal(t, "retry now", retryable.LastError)
		assert.WithinDuration(t, past, *retryable.RetryAfter, time.Second)

		reclaimed, err := store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, reclaimed)
		assert.Equal(t, claimed.ID, reclaimed.ID)
		assert.Equal(t, 2, reclaimed.Attempts)
		assert.NotEqual(t, claimed.LeaseToken, reclaimed.LeaseToken)

		persisted, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-retry")
		require.NoError(t, err)
		require.NotNil(t, persisted)
		assert.Nil(t, persisted.RetryAfter, "claiming consumes the persisted retry window, not just the returned mirror")
	})

	t.Run("ExpiredProcessingLeaseIsReclaimed", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		inserted, err := store.WebhookEvents().Create(ctx, newEvent("delivery-expired"))
		require.NoError(t, err)
		require.True(t, inserted)

		claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Millisecond)
		require.NoError(t, err)
		require.NotNil(t, claimed)

		var reclaimed *storage.WebhookEvent
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			reclaimed, err = store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
			if !assert.NoError(collect, err) {
				return
			}
			assert.NotNil(collect, reclaimed)
		}, 5*time.Second, 10*time.Millisecond)
		assert.Equal(t, claimed.ID, reclaimed.ID)
		assert.Equal(t, 2, reclaimed.Attempts)
		assert.Equal(t, "driver-b", reclaimed.LeaseOwner)
		assert.NotEqual(t, claimed.LeaseToken, reclaimed.LeaseToken)
	})

	t.Run("Create_ReopensExpiredProcessingDelivery", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		event := newEvent("delivery-reopen-stuck")
		inserted, err := store.WebhookEvents().Create(ctx, event)
		require.NoError(t, err)
		require.True(t, inserted)
		claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Millisecond)
		require.NoError(t, err)
		require.NotNil(t, claimed)

		redelivery := newEvent("delivery-reopen-stuck")
		redelivery.Payload = []byte(`{"redelivered":true}`)
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			inserted, err = store.WebhookEvents().Create(ctx, redelivery)
			if !assert.NoError(collect, err) {
				return
			}
			assert.True(collect, inserted, "the redelivery must reopen the expired processing row")
		}, 5*time.Second, 10*time.Millisecond)

		reopened, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-reopen-stuck")
		require.NoError(t, err)
		require.NotNil(t, reopened)
		assert.Equal(t, claimed.ID, reopened.ID)
		assert.Equal(t, storage.WebhookEventPending, reopened.State)
		assert.Zero(t, reopened.Attempts)
		assert.Empty(t, reopened.LeaseToken)
		assert.Nil(t, reopened.StartedAt)
		assert.JSONEq(t, `{"redelivered":true}`, string(reopened.Payload))
	})

	t.Run("Create_DeduplicatesProcessingDeliveryWithLiveLease", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		event := newEvent("delivery-live-lease")
		inserted, err := store.WebhookEvents().Create(ctx, event)
		require.NoError(t, err)
		require.True(t, inserted)

		claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claimed)

		redelivery := newEvent("delivery-live-lease")
		redelivery.Payload = []byte(`{"redelivered":true}`)
		inserted, err = store.WebhookEvents().Create(ctx, redelivery)
		require.NoError(t, err)
		assert.False(t, inserted, "a processing row with a live lease must deduplicate")

		current, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-live-lease")
		require.NoError(t, err)
		require.NotNil(t, current)
		assert.Equal(t, storage.WebhookEventProcessing, current.State)
		assert.Equal(t, claimed.LeaseOwner, current.LeaseOwner)
		assert.Equal(t, claimed.LeaseToken, current.LeaseToken)
		assert.JSONEq(t, `{"action":"synchronize"}`, string(current.Payload))
	})

	t.Run("MarkFailed_TerminalFailure", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		inserted, err := store.WebhookEvents().Create(ctx, newEvent("delivery-failed"))
		require.NoError(t, err)
		require.True(t, inserted)
		claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claimed)

		require.NoError(t, store.WebhookEvents().MarkFailed(ctx, claimed.ID, claimed.LeaseToken, "terminal failure", nil))

		failed, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-failed")
		require.NoError(t, err)
		require.NotNil(t, failed)
		assert.Equal(t, storage.WebhookEventFailed, failed.State)
		assert.Equal(t, "terminal failure", failed.LastError)
		assert.NotNil(t, failed.CompletedAt)
	})

	t.Run("TerminalTransitionsAndRedelivery", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		inserted, err := store.WebhookEvents().Create(ctx, newEvent("delivery-terminal"))
		require.NoError(t, err)
		require.True(t, inserted)
		claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claimed)

		require.ErrorIs(t, store.WebhookEvents().MarkCompleted(ctx, claimed.ID, "stale-token"), storage.ErrWebhookEventLeaseLost)
		require.NoError(t, store.WebhookEvents().MarkCompleted(ctx, claimed.ID, claimed.LeaseToken))
		require.NoError(t, store.WebhookEvents().MarkCompleted(ctx, claimed.ID, claimed.LeaseToken))

		completed, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-terminal")
		require.NoError(t, err)
		require.NotNil(t, completed)
		assert.Equal(t, storage.WebhookEventCompleted, completed.State)
		assert.NotNil(t, completed.CompletedAt)
		assert.Equal(t, claimed.LeaseToken, completed.LeaseToken)

		redelivery := newEvent("delivery-terminal")
		redelivery.Payload = []byte(`{"redelivered":true}`)
		inserted, err = store.WebhookEvents().Create(ctx, redelivery)
		require.NoError(t, err)
		require.True(t, inserted)
		assert.Zero(t, redelivery.ID, "a reopen does not report a fresh insertion ID")

		reopened, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-terminal")
		require.NoError(t, err)
		require.NotNil(t, reopened)
		assert.Equal(t, storage.WebhookEventPending, reopened.State)
		assert.Zero(t, reopened.Attempts)
		assert.Nil(t, reopened.CompletedAt)
	})

	t.Run("ConcurrentClaim_SingleClaimerWins", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		inserted, err := store.WebhookEvents().Create(ctx, newEvent("delivery-contended"))
		require.NoError(t, err)
		require.True(t, inserted)

		const claimerCount = 8
		type claimResult struct {
			event *storage.WebhookEvent
			err   error
		}
		start := make(chan struct{})
		results := make(chan claimResult, claimerCount)
		var wg sync.WaitGroup
		for i := range claimerCount {
			wg.Go(func() {
				<-start
				event, claimErr := store.WebhookEvents().FindNext(ctx, "driver-"+string(rune('a'+i)), time.Minute)
				results <- claimResult{event: event, err: claimErr}
			})
		}
		close(start)
		wg.Wait()
		close(results)

		var winners []*storage.WebhookEvent
		for result := range results {
			require.NoError(t, result.err)
			if result.event != nil {
				winners = append(winners, result.event)
			}
		}
		require.Len(t, winners, 1)
		assert.Equal(t, "delivery-contended", winners[0].DeliveryID)
		assert.Equal(t, 1, winners[0].Attempts)
	})

	t.Run("Create_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.WebhookEvents().Create(t.Context(), newEvent("delivery-error"))
		require.Error(t, err)
	})

	t.Run("FindNext_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.WebhookEvents().FindNext(t.Context(), "driver-a", time.Minute)
		require.Error(t, err)
	})
}
