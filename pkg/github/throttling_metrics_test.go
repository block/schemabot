package github

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/gofri/go-github-ratelimit/v2/github_ratelimit/github_primary_ratelimit"
	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const (
	quotaMetric           = "schemabot.github.primary_rate_limit_rejections_total"
	reportMetric          = "schemabot.github.secondary_rate_limit_cooldown_reports_total"
	reportedSecondsMetric = "schemabot.github.secondary_rate_limit_cooldown_reported_seconds_total"
	waitMetric            = "schemabot.github.wait_duration_seconds"
)

func throttlingData(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	out := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

func assertThrottlingIdentity(t *testing.T, attrs attribute.Set, operation string) {
	t.Helper()
	for key, want := range map[string]string{"operation": operation, "github_app": "test-app", "installation_id": "42", "resource": "core"} {
		got, ok := attrs.Value(attribute.Key(key))
		require.True(t, ok, key)
		assert.Equal(t, want, got.AsString(), key)
	}
	for _, key := range []string{"repository", "pr", "sha", "reset_time", "error"} {
		_, ok := attrs.Value(attribute.Key(key))
		assert.False(t, ok, key)
	}
}

func throttlingRequest(t *testing.T, ctx context.Context, method, target string) *http.Request {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, method, target, nil)
	require.NoError(t, err)
	return req
}

func metricTestTransport(base http.RoundTripper) http.RoundTripper {
	return newGitHubRateLimitTransport(newGitHubMetricsTransport(base, 42, func() string { return "test-app" }))
}

// A primary quota response and a later locally refused call have separate
// origins; only the first reaches HTTP and contributes to HTTP failures.
func TestGitHubPrimaryQuotaMetrics(t *testing.T) {
	reader := newGitHubMetricsTestReader(t)
	var calls atomic.Int64
	reset := time.Now().Add(2 * time.Second).Unix()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("X-RateLimit-Resource", "core")
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(reset, 10))
		w.WriteHeader(http.StatusForbidden)
		_, err := io.WriteString(w, `{"message":"API rate limit exceeded"}`)
		assert.NoError(t, err)
	}))
	defer server.Close()
	rt := metricTestTransport(http.DefaultTransport)
	req := throttlingRequest(t, t.Context(), http.MethodGet, server.URL+"/repos/octocat/example/pulls/1")
	for range 2 {
		resp, err := rt.RoundTrip(req)
		if resp != nil {
			defer resp.Body.Close()
		}
		require.Nil(t, resp)
		var quotaErr *github_primary_ratelimit.RateLimitReachedError
		require.ErrorAs(t, err, &quotaErr)
		require.NoError(t, quotaErr.Response.Body.Close())
	}
	assert.Equal(t, int64(1), calls.Load())
	data := throttlingData(t, reader)
	points := data[quotaMetric].Data.(metricdata.Sum[int64]).DataPoints
	require.Len(t, points, 2)
	origins := map[string]int64{}
	for _, p := range points {
		assertThrottlingIdentity(t, p.Attributes, metrics.GitHubOperationFetchPullRequest)
		v, _ := p.Attributes.Value("origin")
		origins[v.AsString()] += p.Value
	}
	assert.Equal(t, map[string]int64{"http": 1, "local": 1}, origins)
	httpPoints := data["schemabot.github.requests_total"].Data.(metricdata.Sum[int64]).DataPoints
	require.Len(t, httpPoints, 1)
	assert.Equal(t, int64(1), httpPoints[0].Value)
	status, _ := httpPoints[0].Attributes.Value("status")
	assert.Equal(t, "error", status.AsString())

	// The bypass callback is advisory, so a real HTTP permission failure must
	// neither count as local nor be mistaken for another primary rejection.
	bypassCtx := github_primary_ratelimit.WithOverrideConfig(t.Context(), github_primary_ratelimit.WithBypassLimit())
	bypass := throttlingRequest(t, bypassCtx, http.MethodGet, server.URL+"/repos/octocat/example/pulls/1")
	// This call still receives a real quota response: count HTTP again, not local.
	bypassResp, err := rt.RoundTrip(bypass)
	if bypassResp != nil {
		defer bypassResp.Body.Close()
	}
	var quotaErr *github_primary_ratelimit.RateLimitReachedError
	require.ErrorAs(t, err, &quotaErr)
	require.NoError(t, quotaErr.Response.Body.Close())
	points = throttlingData(t, reader)[quotaMetric].Data.(metricdata.Sum[int64]).DataPoints
	origins = map[string]int64{}
	for _, p := range points {
		v, _ := p.Attributes.Value("origin")
		origins[v.AsString()] += p.Value
	}
	assert.Equal(t, map[string]int64{"http": 2, "local": 1}, origins)
}

// Quota responses remain countable when their bucket is absent or unfamiliar.
// The limiter still returns these responses without caching a local refusal.
func TestGitHubPrimaryQuotaUnknownResource(t *testing.T) {
	for _, resource := range []string{"", "search_v2"} {
		for _, tc := range []struct {
			name, remaining, httpStatus string
			status                      int
			quota                       bool
		}{
			{"forbidden", "0", "error", http.StatusForbidden, true},
			{"too_many_requests", "0", "error", http.StatusTooManyRequests, true},
			{"permission", "100", "error", http.StatusForbidden, false},
			{"success_at_zero", "0", "success", http.StatusOK, false},
		} {
			t.Run(resource+"/"+tc.name, func(t *testing.T) {
				reader := newGitHubMetricsTestReader(t)
				calls := 0
				rt := metricTestTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
					calls++
					header := http.Header{}
					if resource != "" {
						header.Set("X-RateLimit-Resource", resource)
					}
					header.Set("X-RateLimit-Remaining", tc.remaining)
					header.Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Minute).Unix(), 10))
					return &http.Response{StatusCode: tc.status, Header: header, Request: req, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
				}))
				req := throttlingRequest(t, t.Context(), http.MethodGet, "https://api.github.com/repos/octocat/example/pulls/1")
				for range 2 {
					resp, err := rt.RoundTrip(req)
					require.NoError(t, err)
					require.Equal(t, tc.status, resp.StatusCode)
					require.NoError(t, resp.Body.Close())
				}
				assert.Equal(t, 2, calls)
				data := throttlingData(t, reader)
				quota, exists := data[quotaMetric]
				if tc.quota {
					require.True(t, exists)
					points := quota.Data.(metricdata.Sum[int64]).DataPoints
					require.Len(t, points, 1)
					assert.Equal(t, int64(2), points[0].Value)
					for key, want := range map[string]string{"origin": "http", "resource": "unknown", "operation": metrics.GitHubOperationFetchPullRequest, "github_app": "test-app", "installation_id": "42"} {
						value, ok := points[0].Attributes.Value(attribute.Key(key))
						require.True(t, ok, key)
						assert.Equal(t, want, value.AsString(), key)
					}
				} else {
					assert.False(t, exists)
				}
				httpPoints := data["schemabot.github.requests_total"].Data.(metricdata.Sum[int64]).DataPoints
				require.Len(t, httpPoints, 1)
				assert.Equal(t, int64(2), httpPoints[0].Value)
				status, _ := httpPoints[0].Attributes.Value("status")
				assert.Equal(t, tc.httpStatus, status.AsString())
			})
		}
	}
}

func secondaryResponse(req *http.Request, retryAfter string) *http.Response {
	header := http.Header{}
	header.Set("X-RateLimit-Resource", "core")
	header.Set("X-RateLimit-Remaining", "100")
	if retryAfter != "" {
		header.Set("Retry-After", retryAfter)
	}
	return &http.Response{StatusCode: 403, Header: header, Request: req, Body: io.NopCloser(strings.NewReader(`{"message":"You have exceeded a secondary rate limit","documentation_url":"https://docs.github.com/rest/using-the-rest-api/rate-limits-for-the-rest-api#about-secondary-rate-limits"}`))}
}

// The same callback emits both counters, including long cooldowns on writes
// that never enter an outer read retry and the optional total-allowance branch.
func TestGitHubSecondaryCooldownReports(t *testing.T) {
	cases := []struct {
		name, retryAfter, method, disposition string
		allowance                             *time.Duration
		calls                                 int
		seconds                               float64
	}{
		{"accepted", "1", http.MethodGet, metrics.GitHubCooldownAccepted, nil, 2, 1},
		{"declined_write", "10", http.MethodPost, metrics.GitHubCooldownSingleSleepLimitExceeded, nil, 1, 10},
		{"total_allowance", "1", http.MethodGet, metrics.GitHubCooldownTotalSleepLimitExceeded, new(time.Duration), 1, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := newGitHubMetricsTestReader(t)
			calls := 0
			base := newGitHubMetricsTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls++
				if calls == 1 {
					return secondaryResponse(req, tc.retryAfter), nil
				}
				return &http.Response{StatusCode: 200, Header: http.Header{}, Request: req, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
			}), 42, func() string { return "test-app" })
			rt := newGitHubRateLimitTransportWithTotalAllowance(base, tc.allowance)
			target := "https://api.github.com/repos/octocat/example/pulls/1"
			operation := metrics.GitHubOperationFetchPullRequest
			if tc.method == http.MethodPost {
				target = "https://api.github.com/repos/octocat/example/issues/1/comments"
				operation = metrics.GitHubOperationCreateIssueComment
			}
			req := throttlingRequest(t, t.Context(), tc.method, target)
			resp, err := rt.RoundTrip(req)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			assert.Equal(t, tc.calls, calls)
			data := throttlingData(t, reader)
			reports := data[reportMetric].Data.(metricdata.Sum[int64]).DataPoints
			seconds := data[reportedSecondsMetric].Data.(metricdata.Sum[float64]).DataPoints
			require.Len(t, reports, 1)
			require.Len(t, seconds, 1)
			assert.Equal(t, int64(1), reports[0].Value)
			assert.InDelta(t, tc.seconds, seconds[0].Value, 0.1)
			assert.Equal(t, reports[0].Attributes, seconds[0].Attributes)
			disposition, _ := reports[0].Attributes.Value("disposition")
			assert.Equal(t, tc.disposition, disposition.AsString())
			assertThrottlingIdentity(t, reports[0].Attributes, operation)
			_, hasWait := data[waitMetric]
			assert.False(t, hasWait)
		})
	}
}

func TestGitHubRepeatedDeclinedCooldowns(t *testing.T) {
	reader := newGitHubMetricsTestReader(t)
	rt := metricTestTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) { return secondaryResponse(req, "10"), nil }))
	req := throttlingRequest(t, t.Context(), http.MethodGet, "https://api.github.com/repos/octocat/example/pulls/1")
	for range 3 {
		resp, err := rt.RoundTrip(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
	}
	data := throttlingData(t, reader)
	assert.Equal(t, int64(3), data[reportMetric].Data.(metricdata.Sum[int64]).DataPoints[0].Value)
	assert.InDelta(t, 30, data[reportedSecondsMetric].Data.(metricdata.Sum[float64]).DataPoints[0].Value, 0.1)
}

func TestGitHubNonQuotaResponses(t *testing.T) {
	for _, tc := range []struct {
		name, body, retryAfter, reset string
		remaining                     string
	}{
		{"permission", `{"message":"Resource not accessible by integration"}`, "10", "", "100"},
		{"secondary_without_reset", `{"message":"You have exceeded a secondary rate limit","documentation_url":"https://docs.github.com/rest/using-the-rest-api/rate-limits-for-the-rest-api#about-secondary-rate-limits"}`, "", "", "100"},
		{"malformed_body", `broken`, "10", "", "100"},
		{"success_at_zero", `{}`, "", "", "0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := newGitHubMetricsTestReader(t)
			rt := metricTestTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
				resp := secondaryResponse(req, tc.retryAfter)
				resp.Header.Set("X-RateLimit-Remaining", tc.remaining)
				if tc.name == "success_at_zero" {
					resp.StatusCode = 200
				}
				resp.Body = io.NopCloser(strings.NewReader(tc.body))
				return resp, nil
			}))
			req := throttlingRequest(t, t.Context(), http.MethodGet, "https://api.github.com/repos/octocat/example/pulls/1")
			resp, err := rt.RoundTrip(req)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			data := throttlingData(t, reader)
			for _, name := range []string{quotaMetric, reportMetric, reportedSecondsMetric, waitMetric} {
				_, ok := data[name]
				assert.False(t, ok, name)
			}
			assert.Equal(t, int64(1), data["schemabot.github.requests_total"].Data.(metricdata.Sum[int64]).DataPoints[0].Value)
		})
	}
}

// Successful retries and cancelled waits both record actual elapsed time;
// secondary Retry-After hints remain distinct from generic transient failures.
func TestGitHubOuterRetryWaitMetrics(t *testing.T) {
	for _, secondary := range []bool{false, true} {
		t.Run(fmt.Sprint(secondary), func(t *testing.T) {
			reader := newGitHubMetricsTestReader(t)
			t.Cleanup(SetUnavailableReadRetryDelayForTest(5 * time.Millisecond))
			ic := &InstallationClient{installationID: 42}
			ic.storeAppSlug("test-app")
			calls := 0
			failure := classifyGitHubAPIError(&gh.ErrorResponse{Response: &http.Response{StatusCode: 502}})
			reason := metrics.GitHubWaitReasonTransientError
			if secondary {
				hint := 10 * time.Millisecond
				failure = classifyGitHubAPIError(&gh.AbuseRateLimitError{RetryAfter: &hint})
				reason = metrics.GitHubWaitReasonSecondaryLimit
			}
			got, err := retryGitHubUnavailableRead(t.Context(), ic, metrics.GitHubOperationFetchPullRequest, "fetch pull request", nil, func(context.Context) (int, error) {
				calls++
				if calls == 1 {
					return 0, failure
				}
				return 7, nil
			})
			require.NoError(t, err)
			assert.Equal(t, 7, got)
			assert.Equal(t, 2, calls)
			data := throttlingData(t, reader)
			points := data[waitMetric].Data.(metricdata.Histogram[float64]).DataPoints
			require.Len(t, points, 1)
			assert.Equal(t, uint64(1), points[0].Count)
			assert.GreaterOrEqual(t, points[0].Sum, 0.005)
			assertThrottlingIdentity(t, points[0].Attributes, metrics.GitHubOperationFetchPullRequest)
			value, _ := points[0].Attributes.Value("reason")
			assert.Equal(t, reason, value.AsString())
			value, _ = points[0].Attributes.Value("layer")
			assert.Equal(t, "read_retry", value.AsString())
			_, hasReport := data[reportMetric]
			assert.False(t, hasReport)
		})
	}
	t.Run("interrupted", func(t *testing.T) {
		reader := newGitHubMetricsTestReader(t)
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		sample := metrics.GitHubClientMetricContext{Operation: metrics.GitHubOperationFetchPullRequest}
		err := waitToRetryGitHubRead(ctx, sample, time.Second, errors.New("network error"))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		points := throttlingData(t, reader)[waitMetric].Data.(metricdata.Histogram[float64]).DataPoints
		require.Len(t, points, 1)
		assert.Equal(t, uint64(1), points[0].Count)
		assert.Greater(t, points[0].Sum, 0.0)
		assert.Less(t, points[0].Sum, 1.0)
		for _, key := range []string{"github_app", "installation_id", "resource"} {
			v, ok := points[0].Attributes.Value(attribute.Key(key))
			require.True(t, ok)
			assert.Equal(t, "unknown", v.AsString())
		}
	})
}

func TestGitHubPrimaryQuotaResetResumesHTTP(t *testing.T) {
	reader := newGitHubMetricsTestReader(t)
	var calls atomic.Int64
	rt := metricTestTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		header := http.Header{}
		header.Set("X-RateLimit-Resource", "core")
		status := 200
		if calls.Add(1) == 1 {
			status = 403
			header.Set("X-RateLimit-Remaining", "0")
			header.Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Second).Unix(), 10))
		}
		return &http.Response{StatusCode: status, Header: header, Request: req, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	}))
	req := throttlingRequest(t, t.Context(), http.MethodGet, "https://api.github.com/repos/octocat/example/pulls/1")
	firstResp, err := rt.RoundTrip(req)
	if firstResp != nil {
		defer firstResp.Body.Close()
	}
	var quotaErr *github_primary_ratelimit.RateLimitReachedError
	require.ErrorAs(t, err, &quotaErr)
	require.NoError(t, quotaErr.Response.Body.Close())
	require.Eventually(t, func() bool {
		resp, err := rt.RoundTrip(req)
		if err != nil {
			if errors.As(err, &quotaErr) {
				require.NoError(t, quotaErr.Response.Body.Close())
			}
			return false
		}
		require.NoError(t, resp.Body.Close())
		return resp.StatusCode == 200
	}, 2*time.Second, 10*time.Millisecond)
	assert.Equal(t, int64(2), calls.Load())
	points := throttlingData(t, reader)[quotaMetric].Data.(metricdata.Sum[int64]).DataPoints
	var httpRejections int64
	for _, p := range points {
		origin, _ := p.Attributes.Value("origin")
		if origin.AsString() == "http" {
			httpRejections += p.Value
		}
	}
	assert.Equal(t, int64(1), httpRejections)
}

// A concurrent caller joins the accepted shared cooldown without creating
// another report; cancelling that caller does not retract the original report.
func TestGitHubSharedCooldownCancellation(t *testing.T) {
	reader := newGitHubMetricsTestReader(t)
	var calls atomic.Int64
	rt := metricTestTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if err := req.Context().Err(); err != nil {
			return nil, err
		}
		if calls.Add(1) == 1 {
			return secondaryResponse(req, "1"), nil
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Request: req, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	}))
	type result struct {
		resp *http.Response
		err  error
	}
	done := make(chan result, 1)
	req := throttlingRequest(t, t.Context(), http.MethodGet, "https://api.github.com/repos/octocat/example/pulls/1")
	go func() {
		resp, err := rt.RoundTrip(req)
		if resp != nil {
			defer resp.Body.Close()
		}
		done <- result{resp, err}
	}()
	require.Eventually(t, func() bool { _, ok := throttlingData(t, reader)[reportMetric]; return ok }, time.Second, time.Millisecond)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	waiter := throttlingRequest(t, ctx, http.MethodGet, req.URL.String())
	resp, err := rt.RoundTrip(waiter)
	if resp != nil {
		defer resp.Body.Close()
	}
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, resp)
	select {
	case got := <-done:
		require.NoError(t, got.err)
		require.Equal(t, http.StatusOK, got.resp.StatusCode)
	case <-time.After(2 * time.Second):
		t.Fatal("original shared-cooldown caller did not complete")
	}
	data := throttlingData(t, reader)
	assert.Equal(t, int64(1), data[reportMetric].Data.(metricdata.Sum[int64]).DataPoints[0].Value)
	assert.InDelta(t, 1, data[reportedSecondsMetric].Data.(metricdata.Sum[float64]).DataPoints[0].Value, 0.1)
	_, hasWait := data[waitMetric]
	assert.False(t, hasWait)
}

// A cooldown longer than the inner allowance returns through the composed
// client, then a successful outer read retry records its real six-second wait.
func TestGitHubDeclinedCooldownThenSuccessfulOuterRetry(t *testing.T) {
	reader := newGitHubMetricsTestReader(t)
	calls := 0
	rt := metricTestTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return secondaryResponse(req, "6"), nil
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Request: req, Body: io.NopCloser(strings.NewReader(`{"encoding":"base64","content":"aGVsbG8="}`))}, nil
	}))
	client := gh.NewClient(&http.Client{Transport: rt})
	client.DisableRateLimitCheck = true
	ic := NewInstallationClientWithSlug(client, nil, "test-app")
	ic.installationID = 42
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	content, err := ic.FetchBlobContent(ctx, "octocat/example", "abc")
	require.NoError(t, err)
	assert.Equal(t, "hello", content)
	assert.Equal(t, 2, calls)
	data := throttlingData(t, reader)
	reports := data[reportMetric].Data.(metricdata.Sum[int64]).DataPoints
	require.Len(t, reports, 1)
	assert.Equal(t, int64(1), reports[0].Value)
	disposition, _ := reports[0].Attributes.Value("disposition")
	assert.Equal(t, metrics.GitHubCooldownSingleSleepLimitExceeded, disposition.AsString())
	assert.InDelta(t, 6, data[reportedSecondsMetric].Data.(metricdata.Sum[float64]).DataPoints[0].Value, 0.1)
	waits := data[waitMetric].Data.(metricdata.Histogram[float64]).DataPoints
	require.Len(t, waits, 1)
	assert.Equal(t, uint64(1), waits[0].Count)
	assert.GreaterOrEqual(t, waits[0].Sum, 6.0)
	assert.Less(t, waits[0].Sum, 10.0)
	assertThrottlingIdentity(t, waits[0].Attributes, metrics.GitHubOperationFetchBlob)
	reason, _ := waits[0].Attributes.Value("reason")
	assert.Equal(t, metrics.GitHubWaitReasonSecondaryLimit, reason.AsString())
	var attempts int64
	for _, p := range data["schemabot.github.requests_total"].Data.(metricdata.Sum[int64]).DataPoints {
		attempts += p.Value
	}
	assert.Equal(t, int64(2), attempts)
}

func TestGitHubExpiredCooldownProducesNoReport(t *testing.T) {
	reader := newGitHubMetricsTestReader(t)
	calls := 0
	rt := metricTestTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			resp := secondaryResponse(req, "")
			resp.Header.Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(-time.Second).Unix(), 10))
			return resp, nil
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Request: req, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	}))
	req := throttlingRequest(t, t.Context(), http.MethodGet, "https://api.github.com/repos/octocat/example/pulls/1")
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, 2, calls)
	data := throttlingData(t, reader)
	_, hasReport := data[reportMetric]
	assert.False(t, hasReport)
	_, hasWait := data[waitMetric]
	assert.False(t, hasWait)
}
