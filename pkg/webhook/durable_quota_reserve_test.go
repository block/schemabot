package webhook

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

// quotaReserveRefusalError wraps a quota reserve refusal the way it reaches
// the driver from a plan: the transport's refusal inside the HTTP client's
// url.Error, inside the plan's own context.
func quotaReserveRefusalError(resetAt time.Time) error {
	refusal := &ghclient.QuotaReservedError{Operation: "fetch_file_content", Remaining: 40, Reserve: 100, ResetAt: resetAt}
	httpErr := &url.Error{Op: "Get", URL: "https://api.github.com/repos/octocat/hello-world/contents/schema/orders.sql", Err: refusal}
	return fmt.Errorf("auto-plan for durable delivery delivery-driver-1 (octocat/hello-world#7): %w", httpErr)
}

// driveQuotaReserveDelivery drives one claim of event whose processing fails
// retryably with processErr.
func driveQuotaReserveDelivery(t *testing.T, event *storage.WebhookEvent, processErr error) *scriptedWebhookEventStore {
	t.Helper()
	store := newScriptedWebhookEventStore(event)
	h := newDurableDriverHandler(t, store, nil, nil)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return true, processErr
	}
	require.True(t, h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0"))
	return store
}

// An auto-plan the installation's quota reserve refused is retried when the
// quota resets, and the wait costs the delivery no attempt, even on the claim
// that reached the attempt cap: retrying sooner would be refused the same
// way, so the check converges after the reset instead of going terminal.
func TestDurableWebhookDriverRetriesQuotaReserveRefusalAtResetWithoutChargingAttempt(t *testing.T) {
	for _, priorAttempts := range []int{0, maxDurableWebhookAttempts - 1} {
		t.Run(fmt.Sprintf("claim %d", priorAttempts+1), func(t *testing.T) {
			event := durablePullRequestEvent(t)
			event.Attempts = priorAttempts
			resetAt := time.Now().Add(40 * time.Minute).Truncate(time.Second)

			store := driveQuotaReserveDelivery(t, event, quotaReserveRefusalError(resetAt))

			require.Empty(t, store.failed, "a quota reserve refusal must not be recorded as a charged failure")
			require.Empty(t, store.completed)
			require.Empty(t, store.failedPermanent)
			require.Len(t, store.failedUncharged, 1)
			failure := <-store.failedUncharged
			require.NotNil(t, failure.retryAfter)
			assert.True(t, resetAt.Equal(*failure.retryAfter), "retry at the quota reset %s, got %s", resetAt, failure.retryAfter)
			assert.Contains(t, failure.errMsg, ghclient.ErrGitHubQuotaReserved.Error())
		})
	}
}

// A delivery that fails for any other reason keeps the one-minute retry and
// spends the attempt its claim consumed.
func TestDurableWebhookDriverChargesOtherRetryableFailures(t *testing.T) {
	before := time.Now()
	store := driveQuotaReserveDelivery(t, durablePullRequestEvent(t), errors.New("GitHub returned 502"))

	require.Empty(t, store.failedUncharged, "only a quota reserve refusal is retried without charging an attempt")
	require.Len(t, store.failed, 1)
	failure := <-store.failed
	require.NotNil(t, failure.retryAfter)
	assert.WithinDuration(t, before.Add(durableWebhookRetryDelay), *failure.retryAfter, 5*time.Second)
}

// A delivery the reserve keeps refusing stops being free once it is older
// than the uncharged window: it still waits for the reset, but each refusal
// spends an attempt, so it goes terminal when its attempts run out rather
// than being rescheduled forever.
func TestDurableWebhookDriverChargesQuotaReserveRefusalsAfterUnchargedWindow(t *testing.T) {
	t.Run("retries at the reset and charges the attempt", func(t *testing.T) {
		event := durablePullRequestEvent(t)
		event.ReceivedAt = time.Now().Add(-quotaReserveUnchargedWindow - time.Minute)
		resetAt := time.Now().Add(25 * time.Minute).Truncate(time.Second)

		store := driveQuotaReserveDelivery(t, event, quotaReserveRefusalError(resetAt))

		require.Empty(t, store.failedUncharged)
		require.Len(t, store.failed, 1)
		failure := <-store.failed
		require.NotNil(t, failure.retryAfter)
		assert.True(t, resetAt.Equal(*failure.retryAfter), "retry at the quota reset %s, got %s", resetAt, failure.retryAfter)
	})

	t.Run("goes terminal at the attempt cap", func(t *testing.T) {
		event := durablePullRequestEvent(t)
		event.ReceivedAt = time.Now().Add(-quotaReserveUnchargedWindow - time.Minute)
		event.Attempts = maxDurableWebhookAttempts - 1

		store := driveQuotaReserveDelivery(t, event, quotaReserveRefusalError(time.Now().Add(25*time.Minute)))

		require.Empty(t, store.failedUncharged)
		require.Len(t, store.failed, 1)
		failure := <-store.failed
		assert.Nil(t, failure.retryAfter, "an exhausted delivery fails terminally")
	})

	t.Run("a delivery without a receipt time is charged", func(t *testing.T) {
		event := durablePullRequestEvent(t)
		event.ReceivedAt = time.Time{}

		store := driveQuotaReserveDelivery(t, event, quotaReserveRefusalError(time.Now().Add(25*time.Minute)))

		require.Empty(t, store.failedUncharged)
		require.Len(t, store.failed, 1)
	})
}

// The retry time follows the reported reset, with a short floor for a reset
// that has passed and a cap for a reset further out than GitHub's hourly
// window allows.
func TestQuotaReserveRetryAt(t *testing.T) {
	now := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		resetAt time.Time
		want    time.Time
	}{
		{name: "reset ahead", resetAt: now.Add(37 * time.Minute), want: now.Add(37 * time.Minute)},
		{name: "reset already passed", resetAt: now.Add(-5 * time.Minute), want: now.Add(quotaReserveRetryFloor)},
		{name: "reset sooner than the floor", resetAt: now.Add(time.Second), want: now.Add(quotaReserveRetryFloor)},
		{name: "zero reset", resetAt: time.Time{}, want: now.Add(quotaReserveRetryFloor)},
		{name: "reset at the horizon", resetAt: now.Add(ghclient.QuotaResetHorizon), want: now.Add(ghclient.QuotaResetHorizon)},
		{name: "reset past the horizon", resetAt: now.Add(30 * 24 * time.Hour), want: now.Add(ghclient.QuotaResetHorizon)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, quotaReserveRetryAt(tc.resetAt, now))
		})
	}
}

// A refused delivery whose reset has passed is retried after the floor, not
// at once and not a minute later.
func TestDurableWebhookDriverRetriesPastQuotaResetAfterFloor(t *testing.T) {
	before := time.Now()
	store := driveQuotaReserveDelivery(t, durablePullRequestEvent(t), quotaReserveRefusalError(before.Add(-time.Minute)))

	require.Len(t, store.failedUncharged, 1)
	failure := <-store.failedUncharged
	require.NotNil(t, failure.retryAfter)
	assert.WithinDuration(t, before.Add(quotaReserveRetryFloor), *failure.retryAfter, 5*time.Second)
}
