package metrics

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func newQuotaRegistry(t *testing.T, options ...sdkmetric.Option) (*githubRateLimitRecorder, func() string) {
	t.Helper()
	registry := prometheus.NewRegistry()
	exporter, err := otelprom.New(otelprom.WithRegisterer(registry))
	require.NoError(t, err)
	provider := sdkmetric.NewMeterProvider(append(options, sdkmetric.WithReader(exporter))...)
	previous := activeGitHubRateLimits.Load()
	require.NoError(t, RegisterGitHubRateLimitMetrics(provider))
	recorder := activeGitHubRateLimits.Load()
	t.Cleanup(func() {
		activeGitHubRateLimits.Store(previous)
		require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context())))
	})
	return recorder, func() string {
		response := httptest.NewRecorder()
		promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(response, httptest.NewRequestWithContext(t.Context(), "GET", "/metrics", nil))
		require.Equal(t, 200, response.Code, response.Body.String())
		return response.Body.String()
	}
}

func quotaSample(now time.Time, remaining int64) GitHubRateLimitSample {
	return GitHubRateLimitSample{
		Operation: GitHubOperationFetchPullRequest, Resource: GitHubRateLimitResourceCore,
		Repository: "octocat/hello-world", GitHubApp: "example-app", InstallationID: 42,
		Limit: 15000, Remaining: remaining, Used: 15000 - remaining,
		ObservedAt: now, ResetAt: now.Truncate(time.Hour).Add(time.Hour),
	}
}

func snapshotLines(body, metric string) []string {
	var lines []string
	for line := range strings.SplitSeq(body, "\n") {
		if strings.HasPrefix(line, metric+"{") && strings.Contains(line, `operation="rate_limit_snapshot"`) {
			lines = append(lines, line)
		}
	}
	return lines
}

// Real Prometheus scrapes retain diagnostic gauges, but remove stale/reset
// snapshots while observation age keeps increasing even without new requests.
func TestGitHubQuotaSnapshotRegistryFreshness(t *testing.T) {
	recorder, scrape := newQuotaRegistry(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	recorder.now = func() time.Time { return now }
	sample := quotaSample(now, 6001)
	RecordGitHubRateLimit(t.Context(), sample)
	for _, remaining := range []int64{6001, 6000, 2000, 1999, 0} {
		sample.Remaining = remaining
		sample.Repository = fmt.Sprintf("octocat/repo-%d", remaining)
		RecordGitHubRateLimit(t.Context(), sample)
		lines := snapshotLines(scrape(), "schemabot_github_rate_limit_remaining")
		require.Len(t, lines, 1)
		assert.True(t, strings.HasSuffix(lines[0], fmt.Sprintf(" %d", remaining)))
		assert.NotContains(t, lines[0], "repository=")
		assert.NotContains(t, lines[0], "reset=")
	}
	now = now.Add(120 * time.Second)
	assert.Len(t, snapshotLines(scrape(), "schemabot_github_rate_limit_remaining"), 1)
	now = now.Add(time.Nanosecond)
	body := scrape()
	assert.Empty(t, snapshotLines(body, "schemabot_github_rate_limit_remaining"))
	assert.Contains(t, body, `operation="fetch_pull_request"`)
	lines := snapshotLines(body, "schemabot_github_rate_limit_observation_age_seconds")
	require.Len(t, lines, 1)
	assert.True(t, strings.HasSuffix(lines[0], " 120.000000001"), lines[0])
	// Scraping does not refresh receipt time; a new response does.
	now = sample.ResetAt
	sample.ObservedAt = now
	sample.ResetAt = now.Add(time.Hour)
	sample.Remaining = 15000
	RecordGitHubRateLimit(t.Context(), sample)
	lines = snapshotLines(scrape(), "schemabot_github_rate_limit_remaining")
	require.Len(t, lines, 1)
	assert.True(t, strings.HasSuffix(lines[0], " 15000"))
	// Reset expiry removes a snapshot even when it is younger than 120 seconds.
	now = sample.ResetAt.Add(-time.Second)
	sample.ObservedAt = now
	RecordGitHubRateLimit(t.Context(), sample)
	now = sample.ResetAt
	assert.Empty(t, snapshotLines(scrape(), "schemabot_github_rate_limit_remaining"))
	assert.Len(t, snapshotLines(scrape(), "schemabot_github_rate_limit_observation_age_seconds"), 1)
}

func TestGitHubQuotaSnapshotOrderingAndIdentity(t *testing.T) {
	recorder, scrape := newQuotaRegistry(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	recorder.now = func() time.Time { return now }
	sample := quotaSample(now, 1999)
	RecordGitHubRateLimit(t.Context(), sample)
	now = now.Add(time.Second)
	delayed := sample
	delayed.ObservedAt = now
	delayed.Remaining = 6000
	RecordGitHubRateLimit(t.Context(), delayed)
	assert.Equal(t, sample, recorder.snapshots[githubRateLimitIdentity{"example-app", 42, "core"}])
	// A newer reset window supersedes the depleted window, even with more quota.
	newer := delayed
	newer.ResetAt = sample.ResetAt.Add(time.Hour)
	RecordGitHubRateLimit(t.Context(), newer)
	olderWindow := sample
	olderWindow.ObservedAt = now.Add(time.Second)
	RecordGitHubRateLimit(t.Context(), olderWindow)
	assert.Equal(t, newer, recorder.snapshots[githubRateLimitIdentity{"example-app", 42, "core"}])
	delayed = newer
	delayed.ObservedAt = delayed.ObservedAt.Add(-time.Millisecond)
	delayed.Remaining = 1000
	RecordGitHubRateLimit(t.Context(), delayed)
	assert.Equal(t, delayed, recorder.snapshots[githubRateLimitIdentity{"example-app", 42, "core"}])
	for _, change := range []func(*GitHubRateLimitSample){
		func(s *GitHubRateLimitSample) { s.InstallationID++ },
		func(s *GitHubRateLimitSample) { s.GitHubApp = "another-app" },
		func(s *GitHubRateLimitSample) { s.Resource = "graphql" },
	} {
		other := newer
		change(&other)
		RecordGitHubRateLimit(t.Context(), other)
	}
	assert.Len(t, snapshotLines(scrape(), "schemabot_github_rate_limit_remaining"), 4)
}

func TestGitHubQuotaSnapshotRejectsInvalidObservations(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := map[string]func(*GitHubRateLimitSample){
		"no app":                   func(s *GitHubRateLimitSample) { s.GitHubApp = "" },
		"no installation":          func(s *GitHubRateLimitSample) { s.InstallationID = 0 },
		"negative installation":    func(s *GitHubRateLimitSample) { s.InstallationID = -1 },
		"unknown resource":         func(s *GitHubRateLimitSample) { s.Resource = "other" },
		"zero limit":               func(s *GitHubRateLimitSample) { s.Limit = 0 },
		"zero limit and remaining": func(s *GitHubRateLimitSample) { s.Limit = 0; s.Remaining = 0 },
		"negative remaining":       func(s *GitHubRateLimitSample) { s.Remaining = -1 },
		"remaining exceeds limit":  func(s *GitHubRateLimitSample) { s.Remaining = s.Limit + 1 },
		"missing receipt":          func(s *GitHubRateLimitSample) { s.ObservedAt = time.Time{} },
		"expired reset":            func(s *GitHubRateLimitSample) { s.ResetAt = now },
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			recorder, scrape := newQuotaRegistry(t)
			recorder.now = func() time.Time { return now }
			sample := quotaSample(now, 1999)
			change(&sample)
			RecordGitHubRateLimit(t.Context(), sample)
			assert.Empty(t, snapshotLines(scrape(), "schemabot_github_rate_limit_remaining"))
			assert.Empty(t, snapshotLines(scrape(), "schemabot_github_rate_limit_observation_age_seconds"))
		})
	}
}

func TestGitHubQuotaConcurrentResponsesAndCollection(t *testing.T) {
	_, scrape := newQuotaRegistry(t)
	now := time.Now()
	var group sync.WaitGroup
	for i := range 8 {
		group.Go(func() {
			for j := range 10 {
				sample := quotaSample(now, int64(1000-i*10-j))
				RecordGitHubRateLimit(t.Context(), sample)
				scrape()
			}
		})
	}
	group.Wait()
	lines := snapshotLines(scrape(), "schemabot_github_rate_limit_remaining")
	require.Len(t, lines, 1)
	assert.True(t, strings.HasSuffix(lines[0], " 921"), lines[0])
}

// Repository diagnostics must not crowd the paging snapshot out of the SDK's
// cardinality budget for the shared remaining metric.
func TestGitHubQuotaSnapshotSurvivesDiagnosticCardinality(t *testing.T) {
	recorder, scrape := newQuotaRegistry(t, sdkmetric.WithCardinalityLimit(2))
	now := time.Now()
	recorder.now = func() time.Time { return now }
	for i := range 20 {
		sample := quotaSample(now, 1999)
		sample.Repository = fmt.Sprintf("octocat/repo-%d", i)
		RecordGitHubRateLimit(t.Context(), sample)
	}
	lines := snapshotLines(scrape(), "schemabot_github_rate_limit_remaining")
	require.Len(t, lines, 1)
	assert.True(t, strings.HasSuffix(lines[0], " 1999"), lines[0])
}

// Malformed responses and local request decisions cannot make an earlier
// observation fresh, nor can they clear a depleted snapshot.
func TestGitHubQuotaInvalidResponsesDoNotRefreshAge(t *testing.T) {
	recorder, scrape := newQuotaRegistry(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	recorder.now = func() time.Time { return now }
	sample := quotaSample(now, 1999)
	RecordGitHubRateLimit(t.Context(), sample)
	now = now.Add(121 * time.Second)
	sample.ObservedAt = now
	sample.ResetAt = time.Time{}
	RecordGitHubRateLimit(t.Context(), sample)
	RecordGitHubRequest(t.Context(), GitHubRequestSample{GitHubApp: "example-app", InstallationID: 42, Status: GitHubRequestStatusTransportError})
	body := scrape()
	assert.Empty(t, snapshotLines(body, "schemabot_github_rate_limit_remaining"))
	lines := snapshotLines(body, "schemabot_github_rate_limit_observation_age_seconds")
	require.Len(t, lines, 1)
	assert.True(t, strings.HasSuffix(lines[0], " 121"), lines[0])
}

// Concurrent responses report the same quota window. A lower allowance must
// survive either recording order, with freshness measured from its own receipt.
func TestGitHubQuotaSnapshotLowerAllowanceAcrossRecordingOrders(t *testing.T) {
	for _, lowFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("low_first_%t", lowFirst), func(t *testing.T) {
			recorder, scrape := newQuotaRegistry(t)
			received := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			low := quotaSample(received, 1999)
			high := quotaSample(received.Add(time.Second), 6000)
			now := high.ObservedAt
			recorder.now = func() time.Time { return now }
			if lowFirst {
				RecordGitHubRateLimit(t.Context(), low)
				RecordGitHubRateLimit(t.Context(), high)
			} else {
				RecordGitHubRateLimit(t.Context(), high)
				RecordGitHubRateLimit(t.Context(), low)
			}
			key := githubRateLimitIdentity{"example-app", 42, "core"}
			assert.Equal(t, low, recorder.snapshots[key])
			lines := snapshotLines(scrape(), "schemabot_github_rate_limit_remaining")
			require.Len(t, lines, 1)
			assert.True(t, strings.HasSuffix(lines[0], " 1999"), lines[0])
			// An equally low but older receipt cannot move the timestamp backward.
			olderEqual := low
			olderEqual.ObservedAt = received.Add(-time.Second)
			RecordGitHubRateLimit(t.Context(), olderEqual)
			assert.Equal(t, low, recorder.snapshots[key])
			now = received.Add(120*time.Second + time.Nanosecond)
			body := scrape()
			assert.Empty(t, snapshotLines(body, "schemabot_github_rate_limit_remaining"))
			ages := snapshotLines(body, "schemabot_github_rate_limit_observation_age_seconds")
			require.Len(t, ages, 1)
			assert.True(t, strings.HasSuffix(ages[0], " 120.000000001"), ages[0])
			// A fresh response confirming the same allowance restores visibility.
			low.ObservedAt = now
			RecordGitHubRateLimit(t.Context(), low)
			assert.Len(t, snapshotLines(scrape(), "schemabot_github_rate_limit_remaining"), 1)
		})
	}
}

// An observation cannot describe current headroom before its receipt time.
func TestGitHubQuotaSnapshotFutureObservation(t *testing.T) {
	recorder, scrape := newQuotaRegistry(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	recorder.now = func() time.Time { return now }
	sample := quotaSample(now.Add(time.Second), 1999)
	RecordGitHubRateLimit(t.Context(), sample)
	assert.Empty(t, snapshotLines(scrape(), "schemabot_github_rate_limit_remaining"))
	now = sample.ObservedAt
	lines := snapshotLines(scrape(), "schemabot_github_rate_limit_remaining")
	require.Len(t, lines, 1)
	assert.True(t, strings.HasSuffix(lines[0], " 1999"), lines[0])
	ages := snapshotLines(scrape(), "schemabot_github_rate_limit_observation_age_seconds")
	require.Len(t, ages, 1)
	assert.True(t, strings.HasSuffix(ages[0], " 0"), ages[0])
}
