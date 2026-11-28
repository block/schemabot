package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gofri/go-github-ratelimit/v2/github_ratelimit/github_primary_ratelimit"
	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

// rateLimitJitter is the extra the tests' jitter adds to the reset time.
const rateLimitJitter = 7 * time.Second

// primaryRateLimitError wraps GitHub's primary rate limit the way it reaches
// the driver from a plan: the rate limit transport's error inside the HTTP
// client's url.Error, inside the plan's own context. A nil resetAt is a rate
// limit whose reset time was not reported.
func primaryRateLimitError(resetAt *time.Time) error {
	request := &http.Request{URL: &url.URL{Scheme: "https", Host: "api.github.com", Path: "/repos/octocat/hello-world/pulls/7"}}
	limit := &github_primary_ratelimit.RateLimitReachedError{Request: request, Category: "core", ResetTime: resetAt}
	httpErr := &url.Error{Op: "Get", URL: request.URL.String(), Err: limit}
	return fmt.Errorf("auto-plan for durable delivery delivery-driver-1 (octocat/hello-world#7): %w", httpErr)
}

// driveRateLimitedDelivery drives one claim of event whose processing fails
// retryably with processErr, with the rate limit's retry jitter pinned to
// rateLimitJitter and the backoff's to zero.
func driveRateLimitedDelivery(t *testing.T, event *storage.WebhookEvent, processErr error) *scriptedWebhookEventStore {
	t.Helper()
	store := newScriptedWebhookEventStore(event)
	h := newDurableDriverHandler(t, store, nil, nil)
	h.durableWebhookRetryJitterOverride = func(limit time.Duration) time.Duration {
		if limit == primaryRateLimitRetryJitter {
			return rateLimitJitter
		}
		return 0
	}
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return true, processErr
	}
	require.True(t, h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0"))
	return store
}

// A delivery whose GitHub requests are refused by the primary rate limit waits
// for the reset time GitHub reported and keeps its retry attempts. This holds
// on the claim that reached the attempt cap too: any sooner retry would be
// refused the same way, and an hour-long limit would otherwise outlast the
// backoff schedule and leave the PR's check failing until a push.
func TestDurableWebhookDriverRetriesPrimaryRateLimitAtResetWithoutChargingAttempt(t *testing.T) {
	for _, priorAttempts := range []int{0, maxDurableWebhookAttempts - 1} {
		t.Run(fmt.Sprintf("claim %d", priorAttempts+1), func(t *testing.T) {
			event := durablePullRequestEvent(t)
			event.Attempts = priorAttempts
			resetAt := time.Now().Add(50 * time.Minute).Truncate(time.Second)

			store := driveRateLimitedDelivery(t, event, primaryRateLimitError(&resetAt))

			require.Empty(t, store.failed, "a primary rate limit must not be recorded as a charged failure")
			require.Empty(t, store.completed)
			require.Empty(t, store.failedPermanent)
			require.Len(t, store.failedUncharged, 1)
			failure := <-store.failedUncharged
			require.NotNil(t, failure.retryAfter)
			assert.True(t, resetAt.Add(rateLimitJitter).Equal(*failure.retryAfter),
				"retry at the reset %s plus the jitter, got %s", resetAt, failure.retryAfter)
		})
	}
}

// The wait is bounded by the same horizon as the quota reserve's: a reset
// further out than GitHub's hourly window allows is a garbled reading, and one
// that passed after the delivery became due is retried after the short floor
// instead of at once.
func TestDurableWebhookDriverBoundsPrimaryRateLimitWait(t *testing.T) {
	tests := []struct {
		name    string
		resetAt time.Time
		wantMin time.Duration
		wantMax time.Duration
	}{
		{
			name:    "reset past the horizon",
			resetAt: time.Now().Add(30 * 24 * time.Hour),
			wantMin: ghclient.QuotaResetHorizon + rateLimitJitter - 5*time.Second,
			wantMax: ghclient.QuotaResetHorizon + rateLimitJitter + 5*time.Second,
		},
		{
			name:    "reset already passed",
			resetAt: time.Now().Add(-time.Minute),
			wantMin: quotaReserveRetryFloor + rateLimitJitter - 5*time.Second,
			wantMax: quotaReserveRetryFloor + rateLimitJitter + 5*time.Second,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			before := time.Now()
			event := durablePullRequestEvent(t)
			event.ReceivedAt = before.Add(-5 * time.Minute)
			store := driveRateLimitedDelivery(t, event, primaryRateLimitError(&tc.resetAt))

			require.Len(t, store.failedUncharged, 1)
			failure := <-store.failedUncharged
			require.NotNil(t, failure.retryAfter)
			wait := failure.retryAfter.Sub(before)
			assert.GreaterOrEqual(t, wait, tc.wantMin)
			assert.LessOrEqual(t, wait, tc.wantMax)
		})
	}
}

// skewedClaim is the result of one claim driven by driveRateLimitedClaimAt, with
// its retry time on the scenario's pod clock.
type skewedClaim struct {
	disposition string // "uncharged", "charged", or "completed"
	retryAt     time.Time
	logs        string
}

// driveRateLimitedClaimAt drives one claim of event as if the pod's clock read
// podNow. The driver reads the real clock, so the event's times and GitHub's
// reset are moved by the gap between the two clocks, and the recorded retry
// time is moved back. A nil resetAt means GitHub serves the claim. The rate
// limit's retry jitter is pinned to rateLimitJitter.
func driveRateLimitedClaimAt(t *testing.T, podNow time.Time, event storage.WebhookEvent, resetAt *time.Time) skewedClaim {
	t.Helper()
	shift := time.Since(podNow)
	event.ReceivedAt = event.ReceivedAt.Add(shift)
	if event.RetryAfter != nil {
		retryAfter := event.RetryAfter.Add(shift)
		event.RetryAfter = &retryAfter
	}
	processErr := error(nil)
	if resetAt != nil {
		shiftedReset := resetAt.Add(shift)
		processErr = primaryRateLimitError(&shiftedReset)
	}

	store := newScriptedWebhookEventStore(&event)
	h := newDurableDriverHandler(t, store, nil, nil)
	var logs bytes.Buffer
	h.logger = slog.New(slog.NewJSONHandler(&logs, nil))
	h.durableWebhookRetryJitterOverride = func(limit time.Duration) time.Duration {
		if limit == primaryRateLimitRetryJitter {
			return rateLimitJitter
		}
		return 0
	}
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return true, processErr
	}
	require.True(t, h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0"))

	claim := skewedClaim{logs: logs.String()}
	switch {
	case len(store.completed) == 1:
		claim.disposition = "completed"
		return claim
	case len(store.failedUncharged) == 1:
		claim.disposition = "uncharged"
		failure := <-store.failedUncharged
		require.NotNil(t, failure.retryAfter)
		claim.retryAt = failure.retryAfter.Add(-shift)
	case len(store.failed) == 1:
		failure := <-store.failed
		if failure.retryAfter == nil {
			claim.disposition = "terminal"
			return claim
		}
		claim.disposition = "charged"
		claim.retryAt = failure.retryAfter.Add(-shift)
	default:
		require.Fail(t, "the claim recorded no completion or retryable failure")
	}
	return claim
}

// clockSkewLogLine returns the one log line that reports the pod's clock is
// ahead of GitHub's, or nil when there is none.
func clockSkewLogLine(t *testing.T, logs string) map[string]any {
	t.Helper()
	var found map[string]any
	for line := range strings.Lines(logs) {
		var entry map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &entry))
		msg, _ := entry["msg"].(string)
		if !strings.Contains(msg, "this pod's clock is ahead of GitHub's") {
			continue
		}
		require.Nil(t, found, "more than one clock skew line: %s", logs)
		found = entry
	}
	return found
}

// The pod's clock runs two minutes ahead of GitHub's, and GitHub says the
// installation's limit resets at 12:00:00 by its own clock. The first refusal
// arrives at 12:01:00 on the pod, so the reset looks a minute past, and the
// delivery retries after the short floor without spending an attempt. GitHub
// refuses the retry with the same 12:00:00 reset: the delivery already waited
// out that reset, so the retry now waits as long as the reset is overdue
// rather than polling again after the floor, still without spending an
// attempt, and a warning names the skew. A retry that GitHub refuses with a
// later reset has met a new rate limit window and waits for that reset.
func TestDurableWebhookDriverBacksOffPrimaryRateLimitResetAlreadyWaitedOut(t *testing.T) {
	reset := time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)
	event := *durablePullRequestEvent(t)
	event.ReceivedAt = reset.Add(-30 * time.Second)

	firstClaimAt := reset.Add(time.Minute)
	first := driveRateLimitedClaimAt(t, firstClaimAt, event, &reset)
	require.Equal(t, "uncharged", first.disposition)
	assert.WithinDuration(t, firstClaimAt.Add(quotaReserveRetryFloor+rateLimitJitter), first.retryAt, time.Second,
		"a reset that passed after the delivery became due is retried after the floor")
	assert.Nil(t, clockSkewLogLine(t, first.logs), "the first refusal of a reset is not a sign of skew")

	retried := event
	retried.RetryAfter = &first.retryAt
	secondClaimAt := first.retryAt

	t.Run("the same reset again waits as long as it is overdue", func(t *testing.T) {
		second := driveRateLimitedClaimAt(t, secondClaimAt, retried, &reset)

		require.Equal(t, "uncharged", second.disposition)
		overdue := secondClaimAt.Sub(reset)
		assert.WithinDuration(t, secondClaimAt.Add(overdue+rateLimitJitter), second.retryAt, time.Second,
			"the retry waits the %s the reset is overdue, not the %s floor", overdue, quotaReserveRetryFloor)

		line := clockSkewLogLine(t, second.logs)
		require.NotNil(t, line, "the repeated reset logs the skew: %s", second.logs)
		assert.Equal(t, "WARN", line["level"])
		assert.Equal(t, event.DeliveryID, line["delivery_id"])
		assert.Equal(t, event.Repository, line["repo"])
		assert.Equal(t, event.TenantID, line["installation_id"])
		assert.Equal(t, "rate_limit_deferred", line["disposition"])
		loggedTime := func(key string) time.Time {
			value, ok := line[key].(string)
			require.True(t, ok, "%s is logged", key)
			parsed, err := time.Parse(time.RFC3339, value)
			require.NoError(t, err)
			return parsed
		}
		resetLogged := loggedTime("reset_time")
		assert.WithinDuration(t, resetLogged.Add(overdue), loggedTime("pod_now"), time.Second)
		assert.WithinDuration(t, resetLogged.Add(overdue), loggedTime("due_since"), time.Second)
	})

	t.Run("the same reset on the last attempt says the delivery will not be retried", func(t *testing.T) {
		last := retried
		last.ReceivedAt = secondClaimAt.Add(-unchargedRefusalWindow - time.Minute)
		last.Attempts = maxDurableWebhookAttempts - 1 // the claim increments to the cap
		second := driveRateLimitedClaimAt(t, secondClaimAt, last, &reset)

		require.Equal(t, "terminal", second.disposition)
		line := clockSkewLogLine(t, second.logs)
		require.NotNil(t, line, "the repeated reset logs the skew: %s", second.logs)
		assert.Equal(t, "failed", line["disposition"])
		assert.NotContains(t, line, "retry_at")
		msg, _ := line["msg"].(string)
		assert.Contains(t, msg, "will not be retried")
		assert.NotContains(t, msg, "the retry will wait")
	})

	t.Run("a later reset is a new window and waits for it", func(t *testing.T) {
		nextReset := reset.Add(time.Hour)
		second := driveRateLimitedClaimAt(t, secondClaimAt, retried, &nextReset)

		require.Equal(t, "uncharged", second.disposition)
		assert.WithinDuration(t, nextReset.Add(rateLimitJitter), second.retryAt, time.Second)
		assert.Nil(t, clockSkewLogLine(t, second.logs), "a later reset is not a sign of skew")
	})
}

// GitHub says the installation's limit resets at 12:00:00 by its clock, 59
// minutes away, and the pod's clock runs an hour ahead, so on the pod that
// reset is already past when the delivery arrives and stays past for the whole
// hour GitHub keeps refusing. The delivery is claimed a handful of times in
// that hour, never spending an attempt, and runs once GitHub's clock reaches
// the reset, instead of being claimed every few seconds until then.
func TestDurableWebhookDriverBoundsClaimsUnderClockSkew(t *testing.T) {
	const skew = time.Hour
	githubReset := time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)
	event := *durablePullRequestEvent(t)
	event.ReceivedAt = githubReset.Add(-59 * time.Minute).Add(skew)

	const maxClaims = 12
	podNow := event.ReceivedAt
	claims := 0
	for {
		claims++
		require.LessOrEqual(t, claims, maxClaims, "a pod clock %s ahead of GitHub's took more than %d claims", skew, maxClaims)
		var refusal *time.Time
		if podNow.Add(-skew).Before(githubReset) {
			refusal = &githubReset
		}
		claim := driveRateLimitedClaimAt(t, podNow, event, refusal)
		if claim.disposition == "completed" {
			break
		}
		require.Equal(t, "uncharged", claim.disposition, "claim %d", claims)
		require.True(t, claim.retryAt.After(podNow), "claim %d retries later", claims)
		event.RetryAfter = &claim.retryAt
		podNow = claim.retryAt
	}
	assert.False(t, podNow.Add(-skew).Before(githubReset), "the delivery ran once GitHub's clock reached the reset")
}

// A delivery that GitHub keeps refusing cannot be rescheduled forever. Within
// three hours of its receipt the refusals cost no attempt. After that each one
// spends an attempt but still waits for the reset, so the delivery goes
// terminal when its attempts run out.
func TestDurableWebhookDriverChargesPrimaryRateLimitAfterUnchargedWindow(t *testing.T) {
	t.Run("each claim retries at the reset and spends an attempt until the cap", func(t *testing.T) {
		for claim := 1; claim <= maxDurableWebhookAttempts; claim++ {
			event := durablePullRequestEvent(t)
			event.ReceivedAt = time.Now().Add(-unchargedRefusalWindow - time.Minute)
			event.Attempts = claim - 1 // FindNext claim increments to claim
			resetAt := time.Now().Add(25 * time.Minute).Truncate(time.Second)

			store := driveRateLimitedDelivery(t, event, primaryRateLimitError(&resetAt))

			require.Empty(t, store.failedUncharged, "claim %d is past the uncharged window", claim)
			require.Len(t, store.failed, 1, "claim %d", claim)
			failure := <-store.failed
			if claim == maxDurableWebhookAttempts {
				assert.Nil(t, failure.retryAfter, "the claim at the attempt cap fails terminally")
				continue
			}
			require.NotNil(t, failure.retryAfter, "claim %d", claim)
			assert.True(t, resetAt.Add(rateLimitJitter).Equal(*failure.retryAfter),
				"claim %d retries at the reset plus the jitter, got %s", claim, failure.retryAfter)
		}
	})

	t.Run("a delivery without a receipt time is charged", func(t *testing.T) {
		event := durablePullRequestEvent(t)
		event.ReceivedAt = time.Time{}
		resetAt := time.Now().Add(25 * time.Minute)

		store := driveRateLimitedDelivery(t, event, primaryRateLimitError(&resetAt))

		require.Empty(t, store.failedUncharged)
		require.Len(t, store.failed, 1)
	})
}

// A rate limit with no reset time to wait for is retried like any other
// failure: on the backoff schedule, spending the attempt its claim consumed.
func TestDurableWebhookDriverChargesPrimaryRateLimitWithoutResetTime(t *testing.T) {
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	h := newDurableDriverHandler(t, store, nil, nil)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return true, primaryRateLimitError(nil)
	}

	before := time.Now()
	require.True(t, h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0"))

	require.Empty(t, store.failedUncharged)
	require.Len(t, store.failed, 1)
	failure := <-store.failed
	require.NotNil(t, failure.retryAfter)
	delay := durableWebhookRetryDelay(1)
	assert.False(t, failure.retryAfter.Before(before.Add(delay)), "retry must wait at least %s", delay)
	assert.False(t, failure.retryAfter.After(time.Now().Add(delay+delay/4)), "retry must not wait longer than %s", delay+delay/4)
}

// A failure the processor proved permanent is dead-lettered even if the error
// chain carries a rate limit, and a non-retryable failure never waits.
func TestDurableWebhookDriverDoesNotWaitForRateLimitOnPermanentFailure(t *testing.T) {
	store := newScriptedWebhookEventStore(durablePullRequestEvent(t))
	h := newDurableDriverHandler(t, store, nil, nil)
	resetAt := time.Now().Add(25 * time.Minute)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return false, primaryRateLimitError(&resetAt)
	}

	require.True(t, h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0"))

	require.Empty(t, store.failedUncharged)
	require.Empty(t, store.failed)
	require.Len(t, store.failedPermanent, 1)
}

// A primary rate limit is visible to an operator in three places: the dispatch
// duration outcome, the webhook event status, and one log line that says when
// the delivery runs again and which installation's quota refused it. A refusal
// inside the uncharged window reads as rate_limit_deferred; one past it reads
// as retrying, because it spent an attempt. The failure line reports the
// attempts the row holds afterwards: a deferral gives the claim's attempt back.
func TestDurableWebhookDriverRecordsPrimaryRateLimitRetry(t *testing.T) {
	tests := []struct {
		name         string
		receivedAgo  time.Duration
		wantOutcome  string
		wantLevel    string
		wantMessage  string
		wantAttempts float64
	}{
		{
			name:         "inside the uncharged window",
			wantAttempts: 2,
			wantOutcome:  "rate_limit_deferred",
			wantLevel:    "INFO",
			wantMessage:  "durable webhook delivery was refused by the GitHub primary rate limit; it will be retried when the limit resets without spending an attempt",
		},
		{
			name:         "past the uncharged window",
			receivedAgo:  unchargedRefusalWindow + time.Minute,
			wantAttempts: 3,
			wantOutcome:  "retrying",
			wantLevel:    "WARN",
			wantMessage:  "durable webhook delivery was refused by the GitHub primary rate limit more than the uncharged window after it was received; it will be retried when the limit resets and this refusal spent an attempt",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := newDispatchMetricsReader(t)
			event := durablePullRequestEvent(t)
			event.ReceivedAt = time.Now().Add(-tc.receivedAgo)
			event.Attempts = 2
			resetAt := time.Now().Add(40 * time.Minute).Truncate(time.Second)
			store := newScriptedWebhookEventStore(event)
			h := newDurableDriverHandler(t, store, nil, nil)
			var logs bytes.Buffer
			h.logger = slog.New(slog.NewJSONHandler(&logs, nil))
			h.durableWebhookRetryJitterOverride = func(time.Duration) time.Duration { return 0 }
			h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
				return true, primaryRateLimitError(&resetAt)
			}

			require.True(t, h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0"))

			durationPoints := collectDispatchHistogramPoints(t, reader, "schemabot.webhook.dispatch_duration_seconds")
			require.Len(t, durationPoints, 1)
			assertStringAttr(t, durationPoints[0].Attributes, "outcome", tc.wantOutcome)

			var matched, failureLines int
			for line := range strings.Lines(logs.String()) {
				var entry map[string]any
				require.NoError(t, json.Unmarshal([]byte(line), &entry))
				if entry["msg"] == "durable webhook driver recorded delivery failure" {
					failureLines++
					assert.Equal(t, tc.wantAttempts, entry["attempts"], "the failure line reports the attempts the row holds")
				}
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
				assert.Equal(t, resetAt.UTC().Format(time.RFC3339), entry["reset_time"])
				assert.Equal(t, resetAt.UTC().Format(time.RFC3339), entry["retry_at"])
			}
			assert.Equal(t, 1, matched, "exactly one primary rate limit retry log line: %s", logs.String())
			assert.Equal(t, 1, failureLines, "exactly one delivery failure line: %s", logs.String())
		})
	}
}

// A quota reserve refusal keeps its own handling when a delivery's error chain
// also carries a rate limit: the reserve's reset time and outcome win.
func TestDurableWebhookDriverPrefersQuotaReserveRefusalOverRateLimit(t *testing.T) {
	reserveResetAt := time.Now().Add(30 * time.Minute).Truncate(time.Second)
	rateLimitResetAt := time.Now().Add(50 * time.Minute)

	store := driveRateLimitedDelivery(t, durablePullRequestEvent(t),
		errors.Join(quotaReserveRefusalError(reserveResetAt), primaryRateLimitError(&rateLimitResetAt)))

	require.Len(t, store.failedUncharged, 1)
	failure := <-store.failedUncharged
	require.NotNil(t, failure.retryAfter)
	assert.True(t, reserveResetAt.Equal(*failure.retryAfter), "retry at the quota reserve's reset %s, got %s", reserveResetAt, failure.retryAfter)
}

// rateLimitSchemaReads stands in for an installation whose quota is spent: it
// answers every schema file, blob and tree read with the rate limit
// transport's error.
type rateLimitSchemaReads struct {
	resetAt time.Time
}

func (rt *rateLimitSchemaReads) RoundTrip(req *http.Request) (*http.Response, error) {
	schemaRead := req.Method == http.MethodGet &&
		(strings.Contains(req.URL.Path, "/contents/") || strings.Contains(req.URL.Path, "/git/trees/") || strings.Contains(req.URL.Path, "/git/blobs/"))
	if !schemaRead {
		return http.DefaultTransport.RoundTrip(req)
	}
	return nil, &github_primary_ratelimit.RateLimitReachedError{Request: req, Category: "core", ResetTime: &rt.resetAt}
}

// Config discovery wraps a primary rate limit without dropping it, so the
// durable driver can find the reset time on the error it gets back and wait
// for it.
func TestDiscoveryKeepsPrimaryRateLimitOnTheErrorChain(t *testing.T) {
	resetAt := time.Now().Add(40 * time.Minute).Truncate(time.Second)
	plain, mux := setupGitHubServer(t)
	client := gh.NewClient(&http.Client{Transport: &rateLimitSchemaReads{resetAt: resetAt}})
	client.BaseURL = plain.BaseURL
	installClient := ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot")
	h := &Handler{
		service:   api.New(&foldStorage{checks: &foldCheckStore{}}, &api.ServerConfig{Repos: map[string]api.RepoConfig{"octocat/hello-world": {}}}, nil, testLogger()),
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
		logger:    testLogger(),
	}
	serveHeadSHA(t, mux, "abc123")
	serveAggregateScopeInputs(t, mux, []string{"schemabot.yaml"}, "database: orders\ntype: mysql\n")

	_, err := h.readPRPlanDiscovery(t.Context(), installClient, "octocat/hello-world", 1, &ghclient.PullRequestInfo{HeadSHA: "abc123"}, "pull_request", "d1")

	require.Error(t, err)
	got, ok := ghclient.PrimaryRateLimitResetAt(err)
	require.True(t, ok, "the rate limit must survive the discovery wrapping: %v", err)
	assert.True(t, resetAt.Equal(got), "reset time %s, got %s", resetAt, got)
}
