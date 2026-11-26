package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
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

// A delivery that fails for any other reason keeps the backoff retry, which
// waits the base delay plus up to a quarter more after its first claim, and
// spends the attempt its claim consumed.
func TestDurableWebhookDriverChargesOtherRetryableFailures(t *testing.T) {
	before := time.Now()
	store := driveQuotaReserveDelivery(t, durablePullRequestEvent(t), errors.New("GitHub returned 502"))
	after := time.Now()

	require.Empty(t, store.failedUncharged, "only a quota reserve refusal is retried without charging an attempt")
	require.Len(t, store.failed, 1)
	failure := <-store.failed
	require.NotNil(t, failure.retryAfter)
	delay := durableWebhookRetryDelay(1)
	assert.False(t, failure.retryAfter.Before(before.Add(delay)), "retry must wait at least %s", delay)
	assert.False(t, failure.retryAfter.After(after.Add(delay+delay/4)), "retry must not wait longer than %s", delay+delay/4)
}

// A charged quota reserve refusal on a later claim is retried at the reset,
// not at the backoff time for that attempt, whether the reset comes before or
// after it: any retry before the reset would be refused the same way.
func TestDurableWebhookDriverRetriesChargedQuotaReserveRefusalAtResetOnLaterAttempts(t *testing.T) {
	tests := []struct {
		name       string
		attempt    int
		resetAfter time.Duration
	}{
		{name: "reset after the backoff time", attempt: 3, resetAfter: 25 * time.Minute},
		{name: "reset before the backoff time", attempt: 4, resetAfter: 90 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			event := durablePullRequestEvent(t)
			event.ReceivedAt = time.Now().Add(-unchargedRefusalWindow - time.Minute)
			event.Attempts = tc.attempt - 1 // FindNext claim increments to attempt
			resetAt := time.Now().Add(tc.resetAfter).Truncate(time.Second)

			store := driveQuotaReserveDelivery(t, event, quotaReserveRefusalError(resetAt))

			require.Empty(t, store.failedUncharged)
			require.Len(t, store.failed, 1)
			failure := <-store.failed
			require.NotNil(t, failure.retryAfter)
			assert.True(t, resetAt.Equal(*failure.retryAfter), "retry at the quota reset %s, got %s", resetAt, failure.retryAfter)
		})
	}
}

// A delivery the reserve keeps refusing stops being free once it is older
// than the uncharged window: it still waits for the reset, but each refusal
// spends an attempt, so it goes terminal when its attempts run out rather
// than being rescheduled forever.
func TestDurableWebhookDriverChargesQuotaReserveRefusalsAfterUnchargedWindow(t *testing.T) {
	t.Run("retries at the reset and charges the attempt", func(t *testing.T) {
		event := durablePullRequestEvent(t)
		event.ReceivedAt = time.Now().Add(-unchargedRefusalWindow - time.Minute)
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
		event.ReceivedAt = time.Now().Add(-unchargedRefusalWindow - time.Minute)
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

// A quota reserve refusal is visible to an operator in three places: the
// dispatch duration outcome, the webhook event status, and one log line that
// says when the delivery runs again and which installation's quota refused it.
// A refusal inside the uncharged window reads as quota_reserve_deferred; one
// past it reads as retrying, because it spent an attempt.
func TestDurableWebhookDriverRecordsQuotaReserveRetry(t *testing.T) {
	tests := []struct {
		name        string
		receivedAgo time.Duration
		wantOutcome string
		wantLevel   string
		wantMessage string
	}{
		{
			name:        "inside the uncharged window",
			wantOutcome: "quota_reserve_deferred",
			wantLevel:   "INFO",
			wantMessage: "durable webhook delivery was refused by the GitHub quota reserve; it will be retried when the quota resets without spending an attempt",
		},
		{
			name:        "past the uncharged window",
			receivedAgo: unchargedRefusalWindow + time.Minute,
			wantOutcome: "retrying",
			wantLevel:   "WARN",
			wantMessage: "durable webhook delivery was refused by the GitHub quota reserve more than the uncharged window after it was received; it will be retried when the quota resets and this refusal spent an attempt",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := newDispatchMetricsReader(t)
			event := durablePullRequestEvent(t)
			event.ReceivedAt = time.Now().Add(-tc.receivedAgo)
			resetAt := time.Now().Add(40 * time.Minute).Truncate(time.Second)
			store := newScriptedWebhookEventStore(event)
			h := newDurableDriverHandler(t, store, nil, nil)
			var logs bytes.Buffer
			h.logger = slog.New(slog.NewJSONHandler(&logs, nil))
			h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
				return true, quotaReserveRefusalError(resetAt)
			}

			require.True(t, h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0"))

			durationPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
			require.Len(t, durationPoints, 1)
			assertStringAttr(t, durationPoints[0].Attributes, "outcome", tc.wantOutcome)
			var statuses []string
			for _, dp := range collectCounterPoints(t, reader, "schemabot.webhook.events_total") {
				status, _ := dp.Attributes.Value("status")
				statuses = append(statuses, status.AsString())
			}
			assert.Contains(t, statuses, "durable_dispatch_"+tc.wantOutcome)

			var matched int
			for line := range strings.Lines(logs.String()) {
				var entry map[string]any
				require.NoError(t, json.Unmarshal([]byte(line), &entry))
				if entry["msg"] != tc.wantMessage {
					continue
				}
				matched++
				assert.Equal(t, tc.wantLevel, entry["level"])
				assert.Equal(t, event.DeliveryID, entry["delivery_id"])
				assert.Equal(t, event.Repository, entry["repo"])
				assert.Equal(t, float64(event.PullRequest), entry["pr"])
				assert.Equal(t, event.HeadSHA, entry["head_sha"])
				assert.Equal(t, event.TenantID, entry["installation_id"])
				assert.Equal(t, "fetch_file_content", entry["github_operation"])
				assert.Equal(t, resetAt.UTC().Format(time.RFC3339), entry["reset_time"])
				assert.Equal(t, resetAt.UTC().Format(time.RFC3339), entry["retry_at"])
			}
			assert.Equal(t, 1, matched, "exactly one quota reserve retry log line: %s", logs.String())
		})
	}
}

// Config discovery wraps the refusal from the installation's quota reserve
// without dropping it, so the durable driver can find the reset time on the
// error it gets back and wait for it.
func TestDiscoveryKeepsQuotaRefusalOnTheErrorChain(t *testing.T) {
	h, _, client, reserve := newQuotaReserveFoldHandler(t, &foldCheckStore{})

	_, err := h.readPRPlanDiscovery(t.Context(), client, "octocat/hello-world", 1, &ghclient.PullRequestInfo{HeadSHA: "abc123"}, "pull_request", "d1")

	require.Error(t, err)
	assert.EqualValues(t, 1, reserve.refused.Load())
	var refusal *ghclient.QuotaReservedError
	require.ErrorAs(t, err, &refusal, "the refusal must survive the discovery wrapping")
	assert.EqualValues(t, 100, refusal.Reserve)
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
