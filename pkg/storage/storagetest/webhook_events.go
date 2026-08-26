package storagetest

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// pollDeadline and pollInterval bound the suite's asynchronous waits, such as
// polling for a short lease to expire.
const (
	pollDeadline = 5 * time.Second
	pollInterval = 10 * time.Millisecond
)

// TestWebhookEvents runs the behavioral parity suite for
// storage.WebhookEventStore: delivery deduplication and redelivery, claim
// ordering and retry windows, expired-lease recovery, lease maintenance and
// release, terminal transitions and dead-lettering, coalescing (covering
// successors and supersede), head coverage, inbox observability, the
// stuck-processing sweep, and claim contention.
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

	// newPullRequestEvent builds an auto-plannable (or closed) pull_request
	// delivery for the coalescing subtests, with receivedAt controlling its
	// position in the received_at newness ordering — Create honors a
	// caller-set ReceivedAt.
	newPullRequestEvent := func(deliveryID, action string, pr int, headSHA string, receivedAt time.Time) *storage.WebhookEvent {
		return &storage.WebhookEvent{
			DeliveryID:  deliveryID,
			Event:       "pull_request",
			Action:      action,
			Repository:  "block/example",
			PullRequest: pr,
			HeadSHA:     headSHA,
			Payload:     []byte(`{}`),
			ReceivedAt:  receivedAt,
		}
	}

	createEvent := func(t *testing.T, store storage.Storage, event *storage.WebhookEvent) {
		t.Helper()
		inserted, err := store.WebhookEvents().Create(t.Context(), event)
		require.NoError(t, err)
		require.True(t, inserted)
	}

	// claimExpecting claims the next delivery and asserts it is the one the
	// test expects, so later assertions never run against the wrong claim.
	claimExpecting := func(t *testing.T, store storage.Storage, wantDeliveryID string) *storage.WebhookEvent {
		t.Helper()
		claimed, err := store.WebhookEvents().FindNext(t.Context(), "driver-a", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claimed)
		require.Equal(t, wantDeliveryID, claimed.DeliveryID)
		return claimed
	}

	// driveToCapExhausted creates a delivery and drives it to the attempt cap
	// through the interface alone: each attempt but the last is claimed and
	// failed retryably with an elapsed retry window, and the final attempt is
	// claimed with finalLease. A finalLease short enough to expire leaves the
	// row in the stuck-processing shape a hard-killed driver produces. The
	// target must be the only claimable row while this runs, or the loop
	// claims a bystander.
	driveToCapExhausted := func(t *testing.T, store storage.Storage, deliveryID string, finalLease time.Duration) {
		t.Helper()
		ctx := t.Context()
		createEvent(t, store, newEvent(deliveryID))
		past := time.Now().UTC().Add(-time.Hour)
		for range storage.MaxWebhookEventAttempts - 1 {
			claimed := claimExpecting(t, store, deliveryID)
			require.NoError(t, store.WebhookEvents().MarkFailed(ctx, claimed.ID, claimed.LeaseToken, "attempt failed", &past))
		}
		claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", finalLease)
		require.NoError(t, err)
		require.NotNil(t, claimed)
		require.Equal(t, deliveryID, claimed.DeliveryID)
		require.Equal(t, storage.MaxWebhookEventAttempts, claimed.Attempts)
	}

	// claimOldAgainstSuccessor seeds the coalescing shape the supersede tests
	// start from: an older delivery "old" and the given newer successor for
	// the same coalescing key, returning the live claim on the older row.
	claimOldAgainstSuccessor := func(t *testing.T, store storage.Storage, successor *storage.WebhookEvent) *storage.WebhookEvent {
		t.Helper()
		createEvent(t, store, newPullRequestEvent("old", "synchronize", 7, "old-head", time.Now().UTC().Add(-time.Minute)))
		createEvent(t, store, successor)
		return claimExpecting(t, store, "old")
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

		// An empty provider defaults to GitHub and finds the same row.
		defaulted, err := store.WebhookEvents().GetByDeliveryID(ctx, "", "delivery-dedup")
		require.NoError(t, err)
		require.NotNil(t, defaulted)
		assert.Equal(t, event.ID, defaulted.ID)
	})

	// Create rejects rows missing identity fields before touching storage: the
	// delivery GUID is the dedup key and the event type routes processing.
	t.Run("Create_RequiresDeliveryIDAndEvent", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		missingDelivery := newEvent("")
		inserted, err := store.WebhookEvents().Create(ctx, missingDelivery)
		require.ErrorContains(t, err, "webhook delivery ID is required")
		require.False(t, inserted)

		missingEvent := newEvent("delivery-no-event")
		missingEvent.Event = ""
		inserted, err = store.WebhookEvents().Create(ctx, missingEvent)
		require.ErrorContains(t, err, "webhook event type is required")
		require.False(t, inserted)
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
		}, pollDeadline, pollInterval)
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
		}, pollDeadline, pollInterval)

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

	t.Run("Create_RejectsNonPendingState", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		// A row created directly in "processing" would have NULL lease columns:
		// never claimable, never expiring, yet deduplicating every future
		// redelivery. Create must only ever persist pending rows.
		for _, state := range []string{
			storage.WebhookEventProcessing,
			storage.WebhookEventCompleted,
			storage.WebhookEventFailedRetryable,
			storage.WebhookEventFailed,
			"bogus",
		} {
			event := newEvent("delivery-nonpending")
			event.State = state
			inserted, err := store.WebhookEvents().Create(ctx, event)
			require.ErrorContains(t, err, "must be pending", "state %q", state)
			require.False(t, inserted)
		}

		stored, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-nonpending")
		require.NoError(t, err)
		require.Nil(t, stored)
	})

	// A duplicate Create whose GUID matches a terminally failed row reopens
	// that row immediately claimable even when the incoming event carries a
	// future not-before time: redelivery is an operator recovery lever, so the
	// reopened row must never be re-deferred behind the very deferral whose
	// loss it recovers from.
	t.Run("Create_ReopenIgnoresIncomingNotBefore", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		createEvent(t, store, newEvent("delivery-reopen-defer"))
		claimed := claimExpecting(t, store, "delivery-reopen-defer")
		require.NoError(t, store.WebhookEvents().MarkFailed(ctx, claimed.ID, claimed.LeaseToken, "permanent failure", nil))

		future := time.Now().UTC().Add(time.Hour)
		redelivery := newEvent("delivery-reopen-defer")
		redelivery.RetryAfter = &future
		inserted, err := store.WebhookEvents().Create(ctx, redelivery)
		require.NoError(t, err)
		require.True(t, inserted)

		reopened, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-reopen-defer")
		require.NoError(t, err)
		require.NotNil(t, reopened)
		assert.Equal(t, storage.WebhookEventPending, reopened.State)
		assert.Nil(t, reopened.RetryAfter, "the reopen discards the incoming not-before time")

		reclaimed, err := store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, reclaimed)
		assert.Equal(t, "delivery-reopen-defer", reclaimed.DeliveryID, "the reopened row is immediately claimable")
	})

	t.Run("FindNext_RequiresOwnerAndPositiveLease", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		claimed, err := store.WebhookEvents().FindNext(ctx, "", time.Minute)
		require.Nil(t, claimed)
		require.Error(t, err)
		assert.NotErrorIs(t, err, storage.ErrWebhookEventLeaseLost)

		claimed, err = store.WebhookEvents().FindNext(ctx, "driver-a", 0)
		require.Nil(t, claimed)
		require.Error(t, err)
	})

	// A sub-second lease must not collapse to an already-expired lease: a
	// second driver must not immediately reclaim an event the first driver is
	// processing.
	t.Run("FindNext_SubSecondLeaseIsNotImmediatelyReclaimable", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		createEvent(t, store, newEvent("delivery-subsecond"))

		claimed, err := store.WebhookEvents().FindNext(ctx, "driver-a", 200*time.Millisecond)
		require.NoError(t, err)
		require.NotNil(t, claimed)

		none, err := store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
		require.NoError(t, err)
		assert.Nil(t, none)
	})

	// Once a delivery's attempts reach the ceiling it must stop being
	// reclaimed instead of blocking the queue forever, while one attempt below
	// the ceiling stays claimable.
	t.Run("FindNext_StopsReclaimingAtAttemptsCeiling", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		createEvent(t, store, newEvent("delivery-ceiling"))
		past := time.Now().UTC().Add(-time.Hour)
		for attempt := 1; attempt <= storage.MaxWebhookEventAttempts; attempt++ {
			claimed := claimExpecting(t, store, "delivery-ceiling")
			require.Equal(t, attempt, claimed.Attempts, "a row below the ceiling stays claimable")
			require.NoError(t, store.WebhookEvents().MarkFailed(ctx, claimed.ID, claimed.LeaseToken, "attempt failed", &past))
		}

		none, err := store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
		require.NoError(t, err)
		require.Nil(t, none, "a retryable row at the attempts ceiling must not be reclaimed")
	})

	// Heartbeat is a lease-guarded write: it rewrites the lease expiry from
	// the requested duration, rejects stale or missing tokens, and requires a
	// positive duration.
	t.Run("Heartbeat_ExtendsLeaseAndGuardsToken", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		createEvent(t, store, newEvent("delivery-heartbeat"))
		claimed := claimExpecting(t, store, "delivery-heartbeat")

		require.ErrorIs(t, store.WebhookEvents().Heartbeat(ctx, claimed.ID, "stale-token", time.Minute), storage.ErrWebhookEventLeaseLost)
		require.NoError(t, store.WebhookEvents().Heartbeat(ctx, claimed.ID, claimed.LeaseToken, time.Minute))
		require.Error(t, store.WebhookEvents().Heartbeat(ctx, claimed.ID, "", time.Minute))
		require.Error(t, store.WebhookEvents().Heartbeat(ctx, claimed.ID, claimed.LeaseToken, 0))

		// A heartbeat that shortens the lease proves the write landed: the
		// row becomes reclaimable once the shortened lease expires.
		require.NoError(t, store.WebhookEvents().Heartbeat(ctx, claimed.ID, claimed.LeaseToken, time.Millisecond))
		var reclaimed *storage.WebhookEvent
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			var err error
			reclaimed, err = store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
			if !assert.NoError(collect, err) {
				return
			}
			assert.NotNil(collect, reclaimed)
		}, pollDeadline, pollInterval)
		assert.Equal(t, claimed.ID, reclaimed.ID)
		assert.NotEqual(t, claimed.LeaseToken, reclaimed.LeaseToken)

		require.ErrorIs(t, store.WebhookEvents().Heartbeat(ctx, claimed.ID, claimed.LeaseToken, time.Minute), storage.ErrWebhookEventLeaseLost,
			"a reclaim rotates the token, so the previous holder's heartbeat reports the lost lease")
	})

	t.Run("Release_RefundsAttemptAndRequeues", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		createEvent(t, store, newEvent("delivery-release"))
		claimed := claimExpecting(t, store, "delivery-release")
		require.Equal(t, 1, claimed.Attempts)

		require.ErrorIs(t, store.WebhookEvents().Release(ctx, claimed.ID, "stale-token"), storage.ErrWebhookEventLeaseLost)
		require.NoError(t, store.WebhookEvents().Release(ctx, claimed.ID, claimed.LeaseToken))

		released, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-release")
		require.NoError(t, err)
		require.NotNil(t, released)
		assert.Equal(t, storage.WebhookEventPending, released.State)
		assert.Equal(t, 0, released.Attempts, "release must refund the attempt consumed by the claim")
		assert.Empty(t, released.LeaseOwner)
		assert.Empty(t, released.LeaseToken)
		assert.Nil(t, released.StartedAt, "releasing the first claim must clear started_at so it re-derives on reclaim")

		reclaimed := claimExpecting(t, store, "delivery-release")
		assert.Equal(t, 1, reclaimed.Attempts)
		require.NotNil(t, reclaimed.StartedAt, "reclaim must set started_at to the actual processing start")
	})

	// A release that undoes a later claim (attempts > 1) must keep started_at,
	// which records when the earliest attempt began processing.
	t.Run("Release_KeepsStartedAtAfterFirstAttempt", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		createEvent(t, store, newEvent("delivery-release-later"))

		first := claimExpecting(t, store, "delivery-release-later")
		require.NotNil(t, first.StartedAt)
		firstStartedAt := *first.StartedAt
		retryAfter := time.Now().UTC().Add(-time.Minute)
		require.NoError(t, store.WebhookEvents().MarkFailed(ctx, first.ID, first.LeaseToken, "boom", &retryAfter))

		second := claimExpecting(t, store, "delivery-release-later")
		require.Equal(t, 2, second.Attempts)
		require.NoError(t, store.WebhookEvents().Release(ctx, second.ID, second.LeaseToken))

		released, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-release-later")
		require.NoError(t, err)
		require.NotNil(t, released)
		assert.Equal(t, storage.WebhookEventPending, released.State, "release must requeue the row regardless of which attempt it undoes")
		assert.Empty(t, released.LeaseToken)
		assert.Equal(t, 1, released.Attempts, "release must refund only the second attempt")
		assert.Nil(t, released.RetryAfter)
		require.NotNil(t, released.StartedAt, "releasing a later claim must preserve the original started_at")
		assert.WithinDuration(t, firstStartedAt, *released.StartedAt, time.Second)
	})

	// A dead-lettered delivery is terminal and sticky: MarkFailedPermanent
	// records the error that proved the head can never succeed, FindNext never
	// reclaims the row, and only a redelivery of the same GUID (GitHub
	// Redeliver) reopens it with a fresh claim budget.
	t.Run("MarkFailedPermanent_DeadLetters", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		createEvent(t, store, newEvent("delivery-deadletter"))
		claimed := claimExpecting(t, store, "delivery-deadletter")

		require.ErrorIs(t, store.WebhookEvents().MarkFailedPermanent(ctx, claimed.ID, "stale-token", "unused"), storage.ErrWebhookEventLeaseLost)
		require.NoError(t, store.WebhookEvents().MarkFailedPermanent(ctx, claimed.ID, claimed.LeaseToken, "PR file listing hit the GitHub cap"))

		deadLettered, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-deadletter")
		require.NoError(t, err)
		require.NotNil(t, deadLettered)
		assert.Equal(t, storage.WebhookEventFailedPermanent, deadLettered.State)
		assert.Equal(t, "PR file listing hit the GitHub cap", deadLettered.LastError)
		assert.Nil(t, deadLettered.RetryAfter)
		assert.NotNil(t, deadLettered.CompletedAt)
		// The lease token is retained on terminal rows so a
		// committed-but-unacked retry of MarkFailedPermanent stays idempotent
		// rather than reporting a lost lease.
		assert.Equal(t, claimed.LeaseToken, deadLettered.LeaseToken)
		require.NoError(t, store.WebhookEvents().MarkFailedPermanent(ctx, claimed.ID, claimed.LeaseToken, "PR file listing hit the GitHub cap"))

		reclaimed, err := store.WebhookEvents().FindNext(ctx, "driver-b", time.Minute)
		require.NoError(t, err)
		require.Nil(t, reclaimed, "a dead-lettered row must never be reclaimed")

		// GitHub Redeliver reuses the delivery GUID and is the one lever that
		// revives a dead-lettered delivery.
		redelivery := newEvent("delivery-deadletter")
		redelivery.Payload = []byte(`{"redelivered":true}`)
		inserted, err := store.WebhookEvents().Create(ctx, redelivery)
		require.NoError(t, err)
		require.True(t, inserted)

		reopened, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "delivery-deadletter")
		require.NoError(t, err)
		require.NotNil(t, reopened)
		assert.Equal(t, storage.WebhookEventPending, reopened.State)
		assert.Equal(t, 0, reopened.Attempts)
		assert.Empty(t, reopened.LeaseToken)
		assert.Nil(t, reopened.CompletedAt)
		assert.JSONEq(t, `{"redelivered":true}`, string(reopened.Payload),
			"the reopen must store the redelivery's payload, not keep the dead-lettered one")

		revived := claimExpecting(t, store, "delivery-deadletter")
		assert.Equal(t, 1, revived.Attempts, "a redelivered dead-lettered row must be claimable again")
	})

	// Only auto-plannable pull_request rows cover a head. Rows of other event
	// types or actions can carry the same PR + head SHA without planning it —
	// a pull_request.closed row from before a reopen, or a check_run row — so
	// they must not suppress the reconciler's recovery of a lost auto-plan
	// delivery. Coverage is keyed by (provider, repository, PR, head SHA).
	t.Run("HasEventForHead_CoversOnlyAutoPlanRows", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		for i, tc := range []struct {
			name   string
			event  string
			action string
			covers bool
		}{
			{"opened plans the head", "pull_request", "opened", true},
			{"synchronize plans the head", "pull_request", "synchronize", true},
			{"reopened plans the head", "pull_request", "reopened", true},
			{"closed does not plan the head", "pull_request", "closed", false},
			{"check_run does not plan the head", "check_run", "rerequested", false},
			{"check_run with an auto-plan action does not plan the head", "check_run", "synchronize", false},
			{"issue_comment does not plan the head", "issue_comment", "created", false},
		} {
			headSHA := fmt.Sprintf("event-head-%d", i)
			event := newPullRequestEvent(fmt.Sprintf("delivery-event-%d", i), tc.action, 7, headSHA, time.Time{})
			event.Event = tc.event
			createEvent(t, store, event)

			found, err := store.WebhookEvents().HasEventForHead(ctx, storage.WebhookProviderGitHub, "block/example", 7, headSHA)
			require.NoError(t, err, tc.name)
			assert.Equal(t, tc.covers, found, tc.name)
		}

		// Provider defaults to GitHub.
		found, err := store.WebhookEvents().HasEventForHead(ctx, "", "block/example", 7, "event-head-0")
		require.NoError(t, err)
		assert.True(t, found)

		for _, tc := range []struct {
			name    string
			repo    string
			pr      int
			headSHA string
		}{
			{"different head SHA", "block/example", 7, "unseen-head"},
			{"different PR", "block/example", 8, "event-head-0"},
			{"different repo", "block/other", 7, "event-head-0"},
		} {
			found, err = store.WebhookEvents().HasEventForHead(ctx, storage.WebhookProviderGitHub, tc.repo, tc.pr, tc.headSHA)
			require.NoError(t, err, tc.name)
			assert.False(t, found, tc.name)
		}

		_, err = store.WebhookEvents().HasEventForHead(ctx, storage.WebhookProviderGitHub, "", 7, "event-head-0")
		require.ErrorContains(t, err, "repository, pull request, and head SHA are required", "missing repository must be rejected")
		_, err = store.WebhookEvents().HasEventForHead(ctx, storage.WebhookProviderGitHub, "block/example", 0, "event-head-0")
		require.ErrorContains(t, err, "repository, pull request, and head SHA are required", "missing pull request must be rejected")
		_, err = store.WebhookEvents().HasEventForHead(ctx, storage.WebhookProviderGitHub, "block/example", 7, "")
		require.ErrorContains(t, err, "repository, pull request, and head SHA are required", "missing head SHA must be rejected")
	})

	// A terminally failed row's coverage depends on its origin: a failed
	// organic delivery still covers its head because the operator's GitHub
	// Redeliver lever exists for it, while a failed synthesized row must not
	// cover, because there is no Redeliver lever for a synthesized GUID and
	// re-synthesis is the only recovery path. A dead-lettered row covers
	// regardless of origin: the driver proved the head can never succeed. A
	// superseded row never covers: its work was discarded on the promise that
	// a covering successor performs it.
	t.Run("HasEventForHead_FailedCoverageDependsOnOrigin", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		coversHead := func(headSHA string) bool {
			t.Helper()
			found, err := store.WebhookEvents().HasEventForHead(ctx, storage.WebhookProviderGitHub, "block/example", 7, headSHA)
			require.NoError(t, err)
			return found
		}

		// Each delivery reaches a terminal state before the next is created,
		// so the FIFO claim always lands on the delivery under test.
		organic := newPullRequestEvent("delivery-failed-organic", "synchronize", 7, "head-organic", time.Time{})
		createEvent(t, store, organic)
		claimed := claimExpecting(t, store, "delivery-failed-organic")
		require.NoError(t, store.WebhookEvents().MarkFailed(ctx, claimed.ID, claimed.LeaseToken, "boom", nil))
		assert.True(t, coversHead("head-organic"), "a failed organic delivery covers: Redeliver exists for it")

		synthesized := newPullRequestEvent(storage.SynthesizedWebhookDeliveryIDPrefix+"delivery-failed-synth", "synchronize", 7, "head-synth", time.Time{})
		createEvent(t, store, synthesized)
		claimed = claimExpecting(t, store, storage.SynthesizedWebhookDeliveryIDPrefix+"delivery-failed-synth")
		require.NoError(t, store.WebhookEvents().MarkFailed(ctx, claimed.ID, claimed.LeaseToken, "boom", nil))
		assert.False(t, coversHead("head-synth"), "a failed synthesized delivery must not cover: re-synthesis is its only recovery path")

		deadLettered := newPullRequestEvent(storage.SynthesizedWebhookDeliveryIDPrefix+"delivery-dead-synth", "synchronize", 7, "head-dead", time.Time{})
		createEvent(t, store, deadLettered)
		claimed = claimExpecting(t, store, storage.SynthesizedWebhookDeliveryIDPrefix+"delivery-dead-synth")
		require.NoError(t, store.WebhookEvents().MarkFailedPermanent(ctx, claimed.ID, claimed.LeaseToken, "proven permanent"))
		assert.True(t, coversHead("head-dead"), "a dead-lettered delivery covers regardless of origin")

		// A superseded row never covers: its claim was discarded in favor of
		// the newer delivery, which covers its own head instead.
		older := newPullRequestEvent("delivery-superseded", "synchronize", 7, "head-superseded", time.Now().UTC().Add(-time.Minute))
		createEvent(t, store, older)
		claimed = claimExpecting(t, store, "delivery-superseded")
		newer := newPullRequestEvent("delivery-successor", "synchronize", 7, "head-successor", time.Now().UTC())
		createEvent(t, store, newer)
		superseded, err := store.WebhookEvents().SupersedeIfCovered(ctx, claimed)
		require.NoError(t, err)
		require.True(t, superseded)
		assert.False(t, coversHead("head-superseded"), "a superseded row must not cover its head")
		assert.True(t, coversHead("head-successor"))
	})

	// A claimed auto-plan delivery is discarded only when a strictly newer
	// delivery for the same PR will perform the work: the advisory probe and
	// the guarded write agree, the superseded row lands terminal, and an
	// uncovered claim stays processing.
	t.Run("SupersedeIfCovered_DiscardsCoveredClaim", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		claimed := claimOldAgainstSuccessor(t, store, newPullRequestEvent("new", "synchronize", 7, "new-head", time.Now().UTC()))
		covered, err := store.WebhookEvents().HasCoveringSuccessor(ctx, claimed)
		require.NoError(t, err)
		assert.True(t, covered, "the advisory probe must agree with the guarded write's predicate")
		superseded, err := store.WebhookEvents().SupersedeIfCovered(ctx, claimed)
		require.NoError(t, err)
		require.True(t, superseded)
		assert.Equal(t, storage.WebhookEventSuperseded, claimed.State, "the claimed struct must reflect the supersede")

		got, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "old")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, storage.WebhookEventSuperseded, got.State)
		assert.NotNil(t, got.CompletedAt)

		// The newest delivery has no successor, so its claim is not discarded.
		claimedNew := claimExpecting(t, store, "new")
		covered, err = store.WebhookEvents().HasCoveringSuccessor(ctx, claimedNew)
		require.NoError(t, err)
		assert.False(t, covered)
		superseded, err = store.WebhookEvents().SupersedeIfCovered(ctx, claimedNew)
		require.NoError(t, err)
		assert.False(t, superseded)

		got, err = store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "new")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, storage.WebhookEventProcessing, got.State, "an uncovered claim must stay processing")
	})

	// Newness is a strictly greater received_at: deliveries received in the
	// same instant never cover each other, regardless of insertion order, so
	// two simultaneous deliveries both process instead of one being discarded
	// on an ordering guess.
	t.Run("SupersedeIfCovered_IgnoresEqualTimestamps", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		// Both deliveries share one second-truncated receipt instant so the
		// stored timestamps are equal on every dialect's column precision.
		receivedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
		createEvent(t, store, newPullRequestEvent("first", "synchronize", 7, "head-1", receivedAt))
		createEvent(t, store, newPullRequestEvent("second", "synchronize", 7, "head-2", receivedAt))

		claimed := claimExpecting(t, store, "first")
		covered, err := store.WebhookEvents().HasCoveringSuccessor(ctx, claimed)
		require.NoError(t, err)
		assert.False(t, covered, "an equal-timestamp delivery must not count as a covering successor")
		superseded, err := store.WebhookEvents().SupersedeIfCovered(ctx, claimed)
		require.NoError(t, err)
		assert.False(t, superseded, "an equal-timestamp delivery must not cover")

		claimedSecond := claimExpecting(t, store, "second")
		covered, err = store.WebhookEvents().HasCoveringSuccessor(ctx, claimedSecond)
		require.NoError(t, err)
		assert.False(t, covered, "equal-timestamp deliveries must not count as covering in either direction")
		superseded, err = store.WebhookEvents().SupersedeIfCovered(ctx, claimedSecond)
		require.NoError(t, err)
		assert.False(t, superseded, "equal-timestamp deliveries must not cover in either direction")
	})

	// Coalescing is keyed by (provider, repository, pull request): deliveries
	// for a different PR or repository never cover, while a newer delivery for
	// the same PR covers even when it targets a different head — the PR's
	// newest head is the only one worth planning.
	t.Run("SupersedeIfCovered_ScopesToPullRequest", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			repo    string
			pr      int
			headSHA string
			covers  bool
		}{
			{"different PR does not cover", "block/example", 8, "old-head", false},
			{"different repository does not cover", "block/other", 7, "old-head", false},
			{"same PR with a different head covers", "block/example", 7, "other-head", true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx := t.Context()
				store := h.NewStorage(t)

				successor := newPullRequestEvent("new", "synchronize", tc.pr, tc.headSHA, time.Now().UTC())
				successor.Repository = tc.repo
				claimed := claimOldAgainstSuccessor(t, store, successor)
				covered, err := store.WebhookEvents().HasCoveringSuccessor(ctx, claimed)
				require.NoError(t, err)
				assert.Equal(t, tc.covers, covered, "the advisory probe must agree with the guarded write's predicate")
				superseded, err := store.WebhookEvents().SupersedeIfCovered(ctx, claimed)
				require.NoError(t, err)
				assert.Equal(t, tc.covers, superseded)
			})
		}
	})

	// Closed deliveries participate asymmetrically: a newer closed delivery
	// covers older auto-plan work (planning a closed PR is pointless), but a
	// claimed closed delivery is never itself superseded — its cleanup must
	// always run. Likewise a claimed non-pull_request row that carries the
	// coalescing key must never be superseded, even when a newer auto-plan
	// delivery exists.
	t.Run("SupersedeIfCovered_NeverSupersedesClosedOrNonPullRequest", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		createEvent(t, store, newPullRequestEvent("closed-old", "closed", 7, "old-head", time.Now().UTC().Add(-time.Minute)))
		createEvent(t, store, newPullRequestEvent("reopened-new", "reopened", 7, "new-head", time.Now().UTC()))

		claimed := claimExpecting(t, store, "closed-old")
		// The probe reports only successor existence — it knows nothing about
		// the claimed row's action — so a true answer here is advisory and the
		// guarded write still refuses to supersede the closed claim.
		covered, err := store.WebhookEvents().HasCoveringSuccessor(ctx, claimed)
		require.NoError(t, err)
		assert.True(t, covered, "a newer auto-plan delivery is a covering successor by the successor predicate")
		superseded, err := store.WebhookEvents().SupersedeIfCovered(ctx, claimed)
		require.NoError(t, err)
		assert.False(t, superseded, "a closed delivery must never be superseded")

		got, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "closed-old")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, storage.WebhookEventProcessing, got.State)

		// Terminalize the closed claim so the next claim lands on the
		// non-pull_request row under test.
		require.NoError(t, store.WebhookEvents().MarkCompleted(ctx, claimed.ID, claimed.LeaseToken))
		reopenedClaim := claimExpecting(t, store, "reopened-new")
		require.NoError(t, store.WebhookEvents().MarkCompleted(ctx, reopenedClaim.ID, reopenedClaim.LeaseToken))

		// The action would auto-plan on a pull_request row, so only the
		// event-type guard protects this claim from being superseded.
		checkRun := newPullRequestEvent("check-run-old", "synchronize", 7, "old-head", time.Now().UTC().Add(-time.Minute))
		checkRun.Event = "check_run"
		createEvent(t, store, checkRun)
		createEvent(t, store, newPullRequestEvent("plan-new", "synchronize", 7, "newer-head", time.Now().UTC()))

		claimedCheckRun := claimExpecting(t, store, "check-run-old")
		superseded, err = store.WebhookEvents().SupersedeIfCovered(ctx, claimedCheckRun)
		require.NoError(t, err)
		assert.False(t, superseded, "a non-pull_request claim must never be superseded")

		got, err = store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "check-run-old")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, storage.WebhookEventProcessing, got.State, "the non-pull_request claim must stay processing")
	})

	// SupersedeIfCovered is a lease-guarded write: a stale token reports the
	// lost lease and a claim without the coalescing key fields is rejected
	// before touching storage. The read-only probe carries no lease semantics.
	t.Run("SupersedeIfCovered_GuardsLeaseAndInputs", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		claimed := claimOldAgainstSuccessor(t, store, newPullRequestEvent("new", "synchronize", 7, "new-head", time.Now().UTC()))

		missingToken := *claimed
		missingToken.LeaseToken = ""
		_, err := store.WebhookEvents().SupersedeIfCovered(ctx, &missingToken)
		require.ErrorContains(t, err, "lease token is required")

		// The read-only probe answers without a lease token but still rejects
		// a claim missing the coalescing key.
		covered, err := store.WebhookEvents().HasCoveringSuccessor(ctx, &missingToken)
		require.NoError(t, err)
		assert.True(t, covered, "the probe must answer without a lease token")

		missingKey := *claimed
		missingKey.Repository = ""
		_, err = store.WebhookEvents().SupersedeIfCovered(ctx, &missingKey)
		require.ErrorContains(t, err, "repository and pull request are required")
		_, err = store.WebhookEvents().HasCoveringSuccessor(ctx, &missingKey)
		require.ErrorContains(t, err, "repository and pull request are required")

		staleToken := *claimed
		staleToken.LeaseToken = "some-other-driver's-token"
		_, err = store.WebhookEvents().SupersedeIfCovered(ctx, &staleToken)
		require.ErrorIs(t, err, storage.ErrWebhookEventLeaseLost)

		got, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "old")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, storage.WebhookEventProcessing, got.State, "a stale token must not supersede the row")
	})

	// Redelivering a superseded delivery reopens it as the PR's newest
	// delivery: the reopen refreshes received_at, so pre-existing rows cannot
	// immediately supersede the operator's explicit request — instead the
	// reopened row now covers the rows that predate it.
	t.Run("SupersedeIfCovered_RedeliveryReopensAndWins", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		// delivery-b's receipt sits a few whole seconds in the past so the
		// second-precision received_at comparison is deterministic: it is
		// strictly newer than delivery-a's original receipt and strictly older
		// than delivery-a's redelivery below.
		createEvent(t, store, newPullRequestEvent("delivery-a", "synchronize", 7, "head-a", time.Now().UTC().Add(-time.Minute)))
		createEvent(t, store, newPullRequestEvent("delivery-b", "synchronize", 7, "head-b", time.Now().UTC().Add(-5*time.Second)))

		claimed := claimExpecting(t, store, "delivery-a")
		superseded, err := store.WebhookEvents().SupersedeIfCovered(ctx, claimed)
		require.NoError(t, err)
		require.True(t, superseded)

		// Operator Redeliver reuses the GUID; the superseded row reopens
		// pending with a fresh received_at.
		inserted, err := store.WebhookEvents().Create(ctx, newPullRequestEvent("delivery-a", "synchronize", 7, "head-a", time.Time{}))
		require.NoError(t, err)
		require.True(t, inserted)

		// The reopened row is claimed first (FIFO by created_at) and is now
		// the PR's newest delivery, so the still-pending delivery-b cannot
		// cover it.
		reclaimed := claimExpecting(t, store, "delivery-a")
		covered, err := store.WebhookEvents().HasCoveringSuccessor(ctx, reclaimed)
		require.NoError(t, err)
		assert.False(t, covered, "rows predating the redelivery must not count as covering successors")
		superseded, err = store.WebhookEvents().SupersedeIfCovered(ctx, reclaimed)
		require.NoError(t, err)
		assert.False(t, superseded, "a redelivered row must not be re-superseded by rows that predate the redelivery")

		// The reopened row's live claim now covers delivery-b instead.
		claimedB := claimExpecting(t, store, "delivery-b")
		covered, err = store.WebhookEvents().HasCoveringSuccessor(ctx, claimedB)
		require.NoError(t, err)
		assert.True(t, covered, "the redelivered row must count as delivery-b's covering successor")
		superseded, err = store.WebhookEvents().SupersedeIfCovered(ctx, claimedB)
		require.NoError(t, err)
		assert.True(t, superseded, "the redelivered row's live claim must cover the older delivery")
	})

	// InboxStats gives operators a steady-state view of the durable inbox:
	// every canonical state is present (zero when empty), the backlog age
	// tracks the oldest ready-to-claim row, and a deferred row is not backlog
	// because no driver would take it yet.
	t.Run("InboxStats_CountsClaimableBacklog", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		empty, err := store.WebhookEvents().InboxStats(ctx)
		require.NoError(t, err)
		require.NotNil(t, empty)
		for _, state := range storage.WebhookEventStatesAll {
			count, ok := empty.CountsByState[state]
			require.True(t, ok, "state %q must be present even when empty", state)
			assert.Equal(t, int64(0), count, "state %q", state)
		}
		assert.Zero(t, empty.OldestClaimableAge)
		assert.Equal(t, int64(0), empty.StuckProcessing)

		// Two pending rows; the older one drives the backlog age.
		older := newEvent("pending-old")
		older.ReceivedAt = time.Now().UTC().Add(-2 * time.Minute)
		createEvent(t, store, older)
		createEvent(t, store, newEvent("pending-new"))

		stats, err := store.WebhookEvents().InboxStats(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(2), stats.CountsByState[storage.WebhookEventPending])
		assert.GreaterOrEqual(t, stats.OldestClaimableAge, time.Minute, "the oldest pending row drives the backlog age")
		assert.Less(t, stats.OldestClaimableAge, 10*time.Minute)

		// Claiming the oldest row moves it out of the backlog; the remaining
		// pending row is young, so the age drops.
		claimed := claimExpecting(t, store, "pending-old")
		stats, err = store.WebhookEvents().InboxStats(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(1), stats.CountsByState[storage.WebhookEventPending])
		assert.Equal(t, int64(1), stats.CountsByState[storage.WebhookEventProcessing])
		assert.Less(t, stats.OldestClaimableAge, time.Minute)

		require.NoError(t, store.WebhookEvents().MarkCompleted(ctx, claimed.ID, claimed.LeaseToken))

		// A deferred row counts as pending but not as backlog: its not-before
		// time has not elapsed, so no driver would claim it. Its receipt is
		// well in the past so measuring the age from received_at instead of
		// the not-before time would inflate the backlog age past the bound.
		future := time.Now().UTC().Add(time.Hour)
		deferred := newEvent("pending-deferred")
		deferred.ReceivedAt = time.Now().UTC().Add(-10 * time.Minute)
		deferred.RetryAfter = &future
		createEvent(t, store, deferred)

		stats, err = store.WebhookEvents().InboxStats(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(2), stats.CountsByState[storage.WebhookEventPending])
		assert.Equal(t, int64(1), stats.CountsByState[storage.WebhookEventCompleted])
		assert.Less(t, stats.OldestClaimableAge, time.Minute, "a deferred pending row is not claimable backlog")
	})

	// TerminateStuckProcessing sweeps out rows a hard-killed driver left
	// parked in processing at the attempt cap with an expired lease — FindNext
	// never reclaims those, so the reconciler must terminalize them. Rows
	// below the cap, rows whose lease is still fresh, and pending rows must be
	// left alone.
	t.Run("TerminateStuckProcessing_SweepsCapExhaustedExpiredRows", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		// Build each row's state sequentially so the FIFO claim inside the
		// cap-drive helper always lands on its own delivery: a row at the cap
		// or holding a live lease is invisible to later claims.
		driveToCapExhausted(t, store, "stuck", time.Millisecond)
		driveToCapExhausted(t, store, "fresh-at-cap", time.Minute)

		createEvent(t, store, newEvent("below-cap"))
		belowCap, err := store.WebhookEvents().FindNext(ctx, "driver-a", time.Millisecond)
		require.NoError(t, err)
		require.NotNil(t, belowCap)
		require.Equal(t, "below-cap", belowCap.DeliveryID)

		createEvent(t, store, newEvent("pending-row"))

		// The stuck row surfaces on the gauge once its final lease expires.
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			stats, statsErr := store.WebhookEvents().InboxStats(ctx)
			if !assert.NoError(collect, statsErr) {
				return
			}
			assert.Equal(collect, int64(1), stats.StuckProcessing)
		}, pollDeadline, pollInterval)

		terminated, err := store.WebhookEvents().TerminateStuckProcessing(ctx, "reconciler sweep")
		require.NoError(t, err)
		assert.Equal(t, int64(1), terminated, "only the cap-exhausted expired-lease row should be terminated")

		got, err := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "stuck")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, storage.WebhookEventFailed, got.State)
		assert.Equal(t, "reconciler sweep", got.LastError)
		assert.Empty(t, got.LeaseToken)
		assert.Nil(t, got.LeaseExpiresAt)
		assert.NotNil(t, got.CompletedAt)

		for deliveryID, wantState := range map[string]string{
			"fresh-at-cap": storage.WebhookEventProcessing,
			"below-cap":    storage.WebhookEventProcessing,
			"pending-row":  storage.WebhookEventPending,
		} {
			row, rowErr := store.WebhookEvents().GetByDeliveryID(ctx, storage.WebhookProviderGitHub, deliveryID)
			require.NoError(t, rowErr, deliveryID)
			require.NotNil(t, row, deliveryID)
			assert.Equal(t, wantState, row.State, deliveryID)
		}

		terminated, err = store.WebhookEvents().TerminateStuckProcessing(ctx, "reconciler sweep")
		require.NoError(t, err)
		assert.Equal(t, int64(0), terminated, "a second sweep finds nothing left to terminate")
	})

	t.Run("Create_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.WebhookEvents().Create(t.Context(), newEvent("delivery-error"))
		require.Error(t, err)
	})

	t.Run("GetByDeliveryID_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.WebhookEvents().GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, "delivery-error")
		require.Error(t, err)
	})

	t.Run("FindNext_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.WebhookEvents().FindNext(t.Context(), "driver-a", time.Minute)
		require.Error(t, err)
	})

	t.Run("Heartbeat_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		require.Error(t, store.WebhookEvents().Heartbeat(t.Context(), 1, "token", time.Minute))
	})

	t.Run("MarkCompleted_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		require.Error(t, store.WebhookEvents().MarkCompleted(t.Context(), 1, "token"))
	})

	t.Run("MarkFailed_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		require.Error(t, store.WebhookEvents().MarkFailed(t.Context(), 1, "token", "boom", nil))
	})

	t.Run("MarkFailedPermanent_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		require.Error(t, store.WebhookEvents().MarkFailedPermanent(t.Context(), 1, "token", "boom"))
	})

	t.Run("Release_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		require.Error(t, store.WebhookEvents().Release(t.Context(), 1, "token"))
	})

	t.Run("HasEventForHead_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.WebhookEvents().HasEventForHead(t.Context(), storage.WebhookProviderGitHub, "block/example", 7, "abc123")
		require.Error(t, err)
	})

	t.Run("HasCoveringSuccessor_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		event := newEvent("delivery-error")
		event.ID = 1
		event.LeaseToken = "token"
		_, err := store.WebhookEvents().HasCoveringSuccessor(t.Context(), event)
		require.Error(t, err)
	})

	t.Run("SupersedeIfCovered_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		event := newEvent("delivery-error")
		event.ID = 1
		event.LeaseToken = "token"
		_, err := store.WebhookEvents().SupersedeIfCovered(t.Context(), event)
		require.Error(t, err)
	})

	t.Run("InboxStats_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.WebhookEvents().InboxStats(t.Context())
		require.Error(t, err)
	})

	t.Run("TerminateStuckProcessing_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.WebhookEvents().TerminateStuckProcessing(t.Context(), "sweep")
		require.Error(t, err)
	})
}
