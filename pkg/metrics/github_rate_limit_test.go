package metrics

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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
	// A reset does not remove a snapshot; only its age does.
	now = sample.ResetAt.Add(-time.Second)
	sample.ObservedAt = now
	RecordGitHubRateLimit(t.Context(), sample)
	now = sample.ResetAt
	assert.Len(t, snapshotLines(scrape(), "schemabot_github_rate_limit_remaining"), 1)
	now = sample.ObservedAt.Add(GitHubRateLimitSnapshotMaxAge + time.Nanosecond)
	assert.Empty(t, snapshotLines(scrape(), "schemabot_github_rate_limit_remaining"))
	assert.Len(t, snapshotLines(scrape(), "schemabot_github_rate_limit_observation_age_seconds"), 1)
}

// An installation's hourly window resets between two quota probe reads. The
// installation was read seconds before the reset and is read again a probe
// interval later, so its budget is never unknown: across the reset it keeps
// exporting the allowance read before it, which is no more than the full
// budget GitHub restores, and the next read replaces it with the new window.
// An installation that is not read again still turns unobserved once that
// reading ages out, so a stalled probe or a failing read keeps a quota gate
// closed.
func TestGitHubQuotaResetBetweenProbeReadsKeepsInstallationObserved(t *testing.T) {
	recorder, scrape := newQuotaRegistry(t)
	reset := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	now := reset.Add(-5 * time.Second)
	recorder.now = func() time.Time { return now }
	unobserved := func() string {
		lines := snapshotLines(scrape(), "schemabot_github_rate_limit_unobserved_installations")
		require.Len(t, lines, 1)
		return lines[0]
	}
	remaining := func() []string {
		return snapshotLines(scrape(), "schemabot_github_rate_limit_remaining")
	}

	beforeReset := quotaSample(now, 9000)
	require.Equal(t, reset, beforeReset.ResetAt)
	RecordGitHubRateLimit(t.Context(), beforeReset)
	SetGitHubQuotaProbeTargets(1, "example-app", []int64{42}, 0)
	assert.True(t, strings.HasSuffix(unobserved(), " 0"), unobserved())

	// Between the reset and the next probe read, which comes once per probe
	// interval: half the export age.
	now = reset.Add(GitHubRateLimitSnapshotMaxAge/2 - 5*time.Second)
	assert.True(t, strings.HasSuffix(unobserved(), " 0"), "the window reset, but the installation was read moments ago: %s", unobserved())
	lines := remaining()
	require.Len(t, lines, 1)
	assert.True(t, strings.HasSuffix(lines[0], " 9000"), "the allowance read before the reset: %s", lines[0])

	// The next probe read reports the new window.
	afterReset := quotaSample(now, 14999)
	require.Equal(t, reset.Add(time.Hour), afterReset.ResetAt)
	RecordGitHubRateLimit(t.Context(), afterReset)
	SetGitHubQuotaProbeTargets(1, "example-app", []int64{42}, 0)
	lines = remaining()
	require.Len(t, lines, 1)
	assert.True(t, strings.HasSuffix(lines[0], " 14999"), lines[0])
	assert.True(t, strings.HasSuffix(unobserved(), " 0"), unobserved())

	// Without another read the installation turns unobserved once that
	// reading ages out, even though its probe keeps reporting it as a target.
	now = afterReset.ObservedAt.Add(GitHubRateLimitSnapshotMaxAge + time.Nanosecond)
	SetGitHubQuotaProbeTargets(1, "example-app", []int64{42}, 0)
	assert.True(t, strings.HasSuffix(unobserved(), " 1"), "an installation that is not read again is unknown: %s", unobserved())
	assert.Empty(t, remaining())
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
	// A higher allowance in the same window keeps the lower remaining but
	// refreshes when the installation was observed.
	refreshed := sample
	refreshed.ObservedAt = delayed.ObservedAt
	assert.Equal(t, refreshed, recorder.snapshots[githubRateLimitIdentity{"example-app", 42, "core"}])
	// A newer reset window supersedes the depleted window, even with more quota.
	newer := delayed
	newer.ResetAt = sample.ResetAt.Add(time.Hour)
	RecordGitHubRateLimit(t.Context(), newer)
	// The earlier window, in a response received before the newer one, is
	// stale and cannot replace it.
	olderWindow := sample
	RecordGitHubRateLimit(t.Context(), olderWindow)
	assert.Equal(t, newer, recorder.snapshots[githubRateLimitIdentity{"example-app", 42, "core"}])
	delayed = newer
	delayed.ObservedAt = delayed.ObservedAt.Add(-time.Millisecond)
	delayed.Remaining = 1000
	RecordGitHubRateLimit(t.Context(), delayed)
	// A lower allowance recorded late is kept, without moving the receipt time
	// back to its own.
	lowered := delayed
	lowered.ObservedAt = newer.ObservedAt
	assert.Equal(t, lowered, recorder.snapshots[githubRateLimitIdentity{"example-app", 42, "core"}])
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
// survive either recording order, with freshness measured from the later
// receipt and never moved backward by an older one.
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
			want := low
			want.ObservedAt = high.ObservedAt
			assert.Equal(t, want, recorder.snapshots[key])
			lines := snapshotLines(scrape(), "schemabot_github_rate_limit_remaining")
			require.Len(t, lines, 1)
			assert.True(t, strings.HasSuffix(lines[0], " 1999"), lines[0])
			// An equally low but older receipt cannot move the timestamp backward.
			olderEqual := low
			olderEqual.ObservedAt = received.Add(-time.Second)
			RecordGitHubRateLimit(t.Context(), olderEqual)
			assert.Equal(t, want, recorder.snapshots[key])
			now = high.ObservedAt.Add(120*time.Second + time.Nanosecond)
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

// The unobserved-installations gauge counts the probe's targets whose core
// budget is unknown. A serving installation with a fresh snapshot is observed;
// one with no snapshot, and an owner whose installation could not be resolved,
// are not, so a gate that requires zero fails closed instead of reading only
// the budgets that happen to be known. A snapshot that ages out turns its
// installation unobserved without another probe pass.
func TestGitHubQuotaUnobservedInstallations(t *testing.T) {
	recorder, scrape := newQuotaRegistry(t)
	now := time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC)
	recorder.now = func() time.Time { return now }
	RecordGitHubRateLimit(t.Context(), quotaSample(now, 9000))

	SetGitHubQuotaProbeTargets(1, "example-app", []int64{42, 43}, 1)
	lines := snapshotLines(scrape(), "schemabot_github_rate_limit_unobserved_installations")
	require.Len(t, lines, 1)
	assert.Contains(t, lines[0], `github_app="example-app"`)
	assert.Contains(t, lines[0], `resource="core"`)
	assert.NotContains(t, lines[0], "installation_id=")
	assert.True(t, strings.HasSuffix(lines[0], " 2"), "installation 43 and the unresolved owner: %s", lines[0])

	SetGitHubQuotaProbeTargets(1, "example-app", []int64{42}, 0)
	lines = snapshotLines(scrape(), "schemabot_github_rate_limit_unobserved_installations")
	require.Len(t, lines, 1)
	assert.True(t, strings.HasSuffix(lines[0], " 0"), lines[0])

	now = now.Add(GitHubRateLimitSnapshotMaxAge + time.Second)
	SetGitHubQuotaProbeTargets(1, "example-app", []int64{42}, 0)
	lines = snapshotLines(scrape(), "schemabot_github_rate_limit_unobserved_installations")
	require.Len(t, lines, 1)
	assert.True(t, strings.HasSuffix(lines[0], " 1"), "the aged-out snapshot leaves installation 42 unobserved: %s", lines[0])
}

// A probe that stops reporting its targets counts one more unobserved
// installation once its targets are older than a snapshot may be, even while
// webhook traffic keeps the known installations' snapshots fresh: an
// installation added since the probe's last pass would otherwise never be
// counted.
func TestGitHubQuotaProbeTargetsGoStaleWhenTheProbeStopsReporting(t *testing.T) {
	recorder, scrape := newQuotaRegistry(t)
	now := time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC)
	recorder.now = func() time.Time { return now }
	SetGitHubQuotaProbeTargets(1, "example-app", []int64{42}, 0)

	now = now.Add(GitHubRateLimitSnapshotMaxAge + time.Second)
	RecordGitHubRateLimit(t.Context(), quotaSample(now, 9000))
	lines := snapshotLines(scrape(), "schemabot_github_rate_limit_unobserved_installations")
	require.Len(t, lines, 1)
	assert.True(t, strings.HasSuffix(lines[0], " 1"), "installation 42 is fresh but the targets are stale: %s", lines[0])

	SetGitHubQuotaProbeTargets(1, "example-app", []int64{42}, 0)
	lines = snapshotLines(scrape(), "schemabot_github_rate_limit_unobserved_installations")
	require.Len(t, lines, 1)
	assert.True(t, strings.HasSuffix(lines[0], " 0"), "a fresh report clears the stale count: %s", lines[0])
}

// A probe keeps one set of targets per App even when the label it reports
// under changes, as it does once the App's slug is learned after startup, so
// an earlier label does not linger as a permanently unobserved series.
func TestGitHubQuotaProbeTargetsReplacedPerApp(t *testing.T) {
	_, scrape := newQuotaRegistry(t)
	SetGitHubQuotaProbeTargets(1, "", []int64{42}, 0)
	SetGitHubQuotaProbeTargets(1, "example-app", []int64{42}, 0)
	SetGitHubQuotaProbeTargets(2, "other-app", nil, 0)

	lines := snapshotLines(scrape(), "schemabot_github_rate_limit_unobserved_installations")
	require.Len(t, lines, 2)
	body := strings.Join(lines, "\n")
	assert.Contains(t, body, `github_app="example-app"`)
	assert.Contains(t, body, `github_app="other-app"`)
}

// Until their slugs are known, two Apps report under the same empty label. Their
// counts are summed into the one series, so an App with nothing unobserved
// cannot overwrite another App's unobserved installations.
func TestGitHubQuotaUnobservedInstallationsSumAppsSharingALabel(t *testing.T) {
	_, scrape := newQuotaRegistry(t)
	SetGitHubQuotaProbeTargets(1, "", nil, 0)
	SetGitHubQuotaProbeTargets(2, "", nil, 2)

	lines := snapshotLines(scrape(), "schemabot_github_rate_limit_unobserved_installations")
	require.Len(t, lines, 1)
	assert.True(t, strings.HasSuffix(lines[0], " 2"), lines[0])
}

// GitHub can report more remaining later in the same window. An installation
// the probe reads every minute stays observed: the snapshot keeps the lower
// remaining, since depletion takes precedence, but each reading refreshes when
// the installation was last observed, so the budget does not age out while
// every read succeeds.
func TestGitHubQuotaSnapshotHigherReadingInWindowKeepsInstallationObserved(t *testing.T) {
	recorder, scrape := newQuotaRegistry(t)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := start
	recorder.now = func() time.Time { return now }
	key := githubRateLimitIdentity{"example-app", 42, "core"}

	RecordGitHubRateLimit(t.Context(), quotaSample(start, 14995))
	higher := quotaSample(start.Add(60*time.Second), 14999)
	now = higher.ObservedAt
	RecordGitHubRateLimit(t.Context(), higher)
	assert.Equal(t, int64(14995), recorder.snapshots[key].Remaining)
	assert.Equal(t, higher.ObservedAt, recorder.snapshots[key].ObservedAt)

	// Past the export age of the first reading, but not of the second.
	now = start.Add(150 * time.Second)
	body := scrape()
	lines := snapshotLines(body, "schemabot_github_rate_limit_remaining")
	require.Len(t, lines, 1)
	assert.True(t, strings.HasSuffix(lines[0], " 14995"), lines[0])
	ages := snapshotLines(body, "schemabot_github_rate_limit_observation_age_seconds")
	require.Len(t, ages, 1)
	assert.True(t, strings.HasSuffix(ages[0], " 90"), ages[0])

	// Without another reading, the budget ages out from the later one.
	now = higher.ObservedAt.Add(GitHubRateLimitSnapshotMaxAge + time.Nanosecond)
	assert.Empty(t, snapshotLines(scrape(), "schemabot_github_rate_limit_remaining"))
}

// GitHub can report a reset time later than the one it reports for the rest of
// the window: one reading shows a nearly full budget resetting in an hour,
// and every reading after it shows the real, lower budget resetting sooner.
// The installation stays observed on those later readings and exports the
// lower budget, rather than holding the one outlier until it ages out and
// leaving the installation with no budget at all until the real window resets.
func TestGitHubQuotaSnapshotFollowsNewerResponseWithEarlierReset(t *testing.T) {
	recorder, scrape := newQuotaRegistry(t)
	start := time.Date(2026, 1, 1, 0, 50, 0, 0, time.UTC)
	now := start
	recorder.now = func() time.Time { return now }
	key := githubRateLimitIdentity{"example-app", 42, "core"}

	outlier := quotaSample(start, 14996)
	outlier.ResetAt = start.Add(time.Hour)
	RecordGitHubRateLimit(t.Context(), outlier)

	var last GitHubRateLimitSample
	for i, remaining := range []int64{14745, 14736, 14720} {
		last = quotaSample(start.Add(time.Duration(i+1)*time.Minute), remaining)
		require.True(t, last.ResetAt.Before(outlier.ResetAt))
		now = last.ObservedAt
		RecordGitHubRateLimit(t.Context(), last)
	}
	assert.Equal(t, last, recorder.snapshots[key])

	// Past the export age of the outlier, the newest reading is exported.
	now = last.ObservedAt.Add(30 * time.Second)
	require.True(t, now.After(outlier.ObservedAt.Add(GitHubRateLimitSnapshotMaxAge)))
	body := scrape()
	lines := snapshotLines(body, "schemabot_github_rate_limit_remaining")
	require.Len(t, lines, 1)
	assert.True(t, strings.HasSuffix(lines[0], " 14720"), lines[0])
	ages := snapshotLines(body, "schemabot_github_rate_limit_observation_age_seconds")
	require.Len(t, ages, 1)
	assert.True(t, strings.HasSuffix(ages[0], " 30"), ages[0])
}

// A window that becomes current again still carries the depletion already
// observed in it. Three responses arrive in order: A reports 100 remaining for
// the earlier reset, B reports 4,990 for a later reset, and C reports 3,100 for
// the earlier reset again. Whichever order they are recorded in, the exported
// snapshot is C's window with A's lower remaining, observed at C's receipt.
func TestGitHubQuotaSnapshotKeepsDepletionWhenAWindowBecomesCurrentAgain(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC)
	a := quotaSample(start, 100)
	b := quotaSample(start.Add(10*time.Second), 4990)
	b.ResetAt = a.ResetAt.Add(18 * time.Minute)
	c := quotaSample(start.Add(20*time.Second), 3100)
	require.Equal(t, a.ResetAt, c.ResetAt)

	want := a
	want.ObservedAt = c.ObservedAt
	key := githubRateLimitIdentity{"example-app", 42, "core"}
	for name, order := range map[string][]GitHubRateLimitSample{
		"A B C": {a, b, c}, "A C B": {a, c, b}, "B A C": {b, a, c},
		"B C A": {b, c, a}, "C A B": {c, a, b}, "C B A": {c, b, a},
	} {
		t.Run(name, func(t *testing.T) {
			recorder, scrape := newQuotaRegistry(t)
			recorder.now = func() time.Time { return c.ObservedAt }
			for _, sample := range order {
				RecordGitHubRateLimit(t.Context(), sample)
			}
			assert.Equal(t, want, recorder.snapshots[key])
			lines := snapshotLines(scrape(), "schemabot_github_rate_limit_remaining")
			require.Len(t, lines, 1)
			assert.True(t, strings.HasSuffix(lines[0], " 100"), lines[0])
		})
	}
}

// A window whose reset has passed by the newest response is dropped, so the
// per-reset record does not grow with every window an installation lives
// through.
func TestGitHubQuotaSnapshotDropsWindowsThatHaveReset(t *testing.T) {
	recorder, _ := newQuotaRegistry(t)
	first := quotaSample(time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC), 200)
	RecordGitHubRateLimit(t.Context(), first)
	next := quotaSample(first.ResetAt.Add(time.Minute), 14990)
	RecordGitHubRateLimit(t.Context(), next)

	key := githubRateLimitIdentity{"example-app", 42, "core"}
	assert.Equal(t, next, recorder.snapshots[key])
	assert.Equal(t, map[int64]GitHubRateLimitSample{next.ResetAt.UnixNano(): next}, recorder.windows[key])
}

// GitHub can keep alternating between two reset times for one installation.
// The first switch to the earlier reset is logged at Info, so an operator can
// see GitHub doing it; switching back to that reset again is not, so an
// alternating installation does not log on every response.
func TestGitHubQuotaEarlierResetLogsOncePerResetTime(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
	})
	newQuotaRegistry(t)

	start := time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC)
	for i, remaining := range []int64{4990, 3100, 4980, 3090, 4970, 3080} {
		sample := quotaSample(start.Add(time.Duration(i)*time.Second), remaining)
		if i%2 == 0 {
			sample.ResetAt = sample.ResetAt.Add(18 * time.Minute)
		}
		RecordGitHubRateLimit(t.Context(), sample)
	}

	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	require.Len(t, lines, 1, logs.String())
	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &entry))
	assert.Equal(t, "INFO", entry["level"])
	assert.Equal(t, "GitHub reported an earlier quota reset than the last response; following the newer response", entry["msg"])
	assert.Equal(t, float64(3100), entry["remaining"])
	assert.Equal(t, float64(4990), entry["previous_remaining"])
}

// Reset times that jitter between responses each become a window. The
// recorder keeps a bounded number per installation, dropping the least
// recently reported, so the work done under the lock on every response stays
// small. The current window and the most recent ones keep their readings.
func TestGitHubQuotaSnapshotBoundsWindowsPerInstallation(t *testing.T) {
	recorder, _ := newQuotaRegistry(t)
	start := time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC)
	var samples []GitHubRateLimitSample
	for i := range maxGitHubQuotaWindowsPerIdentity + 3 {
		sample := quotaSample(start.Add(time.Duration(i)*time.Second), int64(5000-i))
		sample.ResetAt = sample.ResetAt.Add(time.Duration(i) * time.Second)
		samples = append(samples, sample)
		RecordGitHubRateLimit(t.Context(), sample)
	}

	key := githubRateLimitIdentity{"example-app", 42, "core"}
	windows := recorder.windows[key]
	require.Len(t, windows, maxGitHubQuotaWindowsPerIdentity)
	for _, sample := range samples[len(samples)-maxGitHubQuotaWindowsPerIdentity:] {
		assert.Equal(t, sample, windows[sample.ResetAt.UnixNano()])
	}
	assert.Equal(t, samples[len(samples)-1], recorder.snapshots[key])
}
