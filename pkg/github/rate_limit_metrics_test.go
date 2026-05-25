package github

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestGitHubRateLimitTransportRecordsHeaders(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(context.WithoutCancel(t.Context())))
	})

	rt := newGitHubMetricsTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		header := http.Header{}
		header.Set(headerRateLimitLimit, "15000")
		header.Set(headerRateLimitRemaining, "14991")
		header.Set(headerRateLimitResource, "core")
		header.Set(headerRateLimitUsed, "9")
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     header,
			Body:       io.NopCloser(strings.NewReader("{}")),
			Request:    req,
		}, nil
	}), 12345, func() string { return "schemabot-block" })

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://api.github.com/repos/octocat/hello-world/pulls/1", nil)
	require.NoError(t, err)

	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	values, attrs := collectGitHubRateLimitMetrics(t, reader)
	assert.Equal(t, int64(15000), values["schemabot.github.rate_limit.limit"])
	assert.Equal(t, int64(14991), values["schemabot.github.rate_limit.remaining"])
	assert.Equal(t, int64(9), values["schemabot.github.rate_limit.used"])

	assert.Equal(t, metrics.GitHubOperationFetchPullRequest, attrs["operation"])
	assert.Equal(t, metrics.GitHubRateLimitResourceCore, attrs["resource"])
	assert.Equal(t, "octocat/hello-world", attrs["repository"])
	assert.Equal(t, "schemabot-block", attrs["github_app"])
	assert.Equal(t, "12345", attrs["installation_id"])
}

func TestGitHubRateLimitTransportInfersGraphQLResourceAndUsed(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(context.WithoutCancel(t.Context())))
	})

	rt := newGitHubMetricsTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		header := http.Header{}
		header.Set(headerRateLimitLimit, "5000")
		header.Set(headerRateLimitRemaining, "4997")
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     header,
			Body:       io.NopCloser(strings.NewReader("{}")),
			Request:    req,
		}, nil
	}), 0, func() string { return "schemabot-block-staging" })

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://api.github.com/graphql", nil)
	require.NoError(t, err)
	req = req.WithContext(withGitHubRateLimitContext(req.Context(), metrics.GitHubOperationGraphQLStatusCheckRollup, "octocat/hello-world"))

	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	values, attrs := collectGitHubRateLimitMetrics(t, reader)
	assert.Equal(t, int64(5000), values["schemabot.github.rate_limit.limit"])
	assert.Equal(t, int64(4997), values["schemabot.github.rate_limit.remaining"])
	assert.Equal(t, int64(3), values["schemabot.github.rate_limit.used"])

	assert.Equal(t, metrics.GitHubOperationGraphQLStatusCheckRollup, attrs["operation"])
	assert.Equal(t, metrics.GitHubRateLimitResourceGraphQL, attrs["resource"])
	assert.Equal(t, "schemabot-block-staging", attrs["github_app"])
	assert.NotContains(t, attrs, "installation_id")
}

func TestGitHubRateLimitContextFromRequestClassifiesRESTRoutes(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		url        string
		operation  string
		repository string
	}{
		{
			name:       "fetch pull request",
			method:     http.MethodGet,
			url:        "https://api.github.com/repos/octocat/hello-world/pulls/1",
			operation:  metrics.GitHubOperationFetchPullRequest,
			repository: "octocat/hello-world",
		},
		{
			name:       "create issue comment",
			method:     http.MethodPost,
			url:        "https://api.github.com/repos/octocat/hello-world/issues/1/comments",
			operation:  metrics.GitHubOperationCreateIssueComment,
			repository: "octocat/hello-world",
		},
		{
			name:       "edit issue comment",
			method:     http.MethodPatch,
			url:        "https://api.github.com/repos/octocat/hello-world/issues/comments/123",
			operation:  metrics.GitHubOperationEditIssueComment,
			repository: "octocat/hello-world",
		},
		{
			name:       "add comment reaction",
			method:     http.MethodPost,
			url:        "https://api.github.com/repos/octocat/hello-world/issues/comments/123/reactions",
			operation:  metrics.GitHubOperationAddCommentReaction,
			repository: "octocat/hello-world",
		},
		{
			name:       "list pull request files",
			method:     http.MethodGet,
			url:        "https://api.github.com/repos/octocat/hello-world/pulls/1/files",
			operation:  metrics.GitHubOperationListPRFiles,
			repository: "octocat/hello-world",
		},
		{
			name:       "list reviews",
			method:     http.MethodGet,
			url:        "https://api.github.com/repos/octocat/hello-world/pulls/1/reviews",
			operation:  metrics.GitHubOperationListReviews,
			repository: "octocat/hello-world",
		},
		{
			name:       "request reviewers",
			method:     http.MethodPost,
			url:        "https://api.github.com/repos/octocat/hello-world/pulls/1/requested_reviewers",
			operation:  metrics.GitHubOperationRequestReviewers,
			repository: "octocat/hello-world",
		},
		{
			name:       "create check run",
			method:     http.MethodPost,
			url:        "https://api.github.com/repos/octocat/hello-world/check-runs",
			operation:  metrics.GitHubOperationCreateCheckRun,
			repository: "octocat/hello-world",
		},
		{
			name:       "update check run",
			method:     http.MethodPatch,
			url:        "https://api.github.com/repos/octocat/hello-world/check-runs/123",
			operation:  metrics.GitHubOperationUpdateCheckRun,
			repository: "octocat/hello-world",
		},
		{
			name:       "list check runs",
			method:     http.MethodGet,
			url:        "https://api.github.com/repos/octocat/hello-world/commits/main/check-runs",
			operation:  metrics.GitHubOperationListCheckRunsForRef,
			repository: "octocat/hello-world",
		},
		{
			name:       "fetch git tree",
			method:     http.MethodGet,
			url:        "https://api.github.com/repos/octocat/hello-world/git/trees/abc123",
			operation:  metrics.GitHubOperationFetchGitTree,
			repository: "octocat/hello-world",
		},
		{
			name:       "fetch blob",
			method:     http.MethodGet,
			url:        "https://api.github.com/repos/octocat/hello-world/git/blobs/abc123",
			operation:  metrics.GitHubOperationFetchBlob,
			repository: "octocat/hello-world",
		},
		{
			name:       "fetch file content",
			method:     http.MethodGet,
			url:        "https://api.github.com/repos/octocat/hello-world/contents/path/to/file.sql",
			operation:  metrics.GitHubOperationFetchFileContent,
			repository: "octocat/hello-world",
		},
		{
			name:       "fetch root content",
			method:     http.MethodGet,
			url:        "https://api.github.com/repos/octocat/hello-world/contents",
			operation:  metrics.GitHubOperationFetchFileContent,
			repository: "octocat/hello-world",
		},
		{
			name:      "list team members",
			method:    http.MethodGet,
			url:       "https://api.github.com/orgs/octocat/teams/db-team/members",
			operation: metrics.GitHubOperationListTeamMembers,
		},
		{
			name:      "get team membership",
			method:    http.MethodGet,
			url:       "https://api.github.com/orgs/octocat/teams/db-team/memberships/monalisa",
			operation: metrics.GitHubOperationGetTeamMembership,
		},
		{
			name:      "create installation access token",
			method:    http.MethodPost,
			url:       "https://api.github.com/app/installations/123/access_tokens",
			operation: metrics.GitHubOperationCreateInstallationAccessToken,
		},
		{
			name:      "fetch app slug",
			method:    http.MethodGet,
			url:       "https://api.github.com/app",
			operation: metrics.GitHubOperationFetchAppSlug,
		},
		{
			name:       "enterprise api prefix",
			method:     http.MethodGet,
			url:        "https://github.example.com/api/v3/repos/octocat/hello-world/pulls/1",
			operation:  metrics.GitHubOperationFetchPullRequest,
			repository: "octocat/hello-world",
		},
		{
			name:       "unknown repo route preserves repository",
			method:     http.MethodGet,
			url:        "https://api.github.com/repos/octocat/hello-world/actions/runs",
			operation:  metrics.GitHubOperationUnknown,
			repository: "octocat/hello-world",
		},
		{
			name:      "unknown route",
			method:    http.MethodGet,
			url:       "https://api.github.com/rate_limit",
			operation: metrics.GitHubOperationUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), tt.method, tt.url, nil)
			require.NoError(t, err)

			got := githubRateLimitContextFromRequest(req)
			assert.Equal(t, tt.operation, got.operation)
			assert.Equal(t, tt.repository, got.repository)
		})
	}
}

func TestGitHubRateLimitTransportSkipsGaugeWhenRequiredHeadersAreMissing(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		require.NoError(t, mp.Shutdown(context.WithoutCancel(t.Context())))
	})

	rt := newGitHubMetricsTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		header := http.Header{}
		header.Set(headerRateLimitRemaining, "4997")
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     header,
			Body:       io.NopCloser(strings.NewReader("{}")),
			Request:    req,
		}, nil
	}), 0, nil)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://api.github.com/graphql", nil)
	require.NoError(t, err)
	req = req.WithContext(withGitHubRateLimitContext(req.Context(), metrics.GitHubOperationGraphQLStatusCheckRollup, "octocat/hello-world"))

	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	values := collectGitHubRateLimitMetricValues(t, reader)
	assert.Empty(t, values)
}

func collectGitHubRateLimitMetrics(t *testing.T, reader *sdkmetric.ManualReader) (map[string]int64, map[string]string) {
	t.Helper()

	values, attrs := collectGitHubRateLimitMetricValuesAndAttributes(t, reader)
	require.Contains(t, values, "schemabot.github.rate_limit.limit")
	require.Contains(t, values, "schemabot.github.rate_limit.remaining")
	require.Contains(t, values, "schemabot.github.rate_limit.used")
	return values, attrs
}

func collectGitHubRateLimitMetricValues(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()

	values, _ := collectGitHubRateLimitMetricValuesAndAttributes(t, reader)
	return values
}

func collectGitHubRateLimitMetricValuesAndAttributes(t *testing.T, reader *sdkmetric.ManualReader) (map[string]int64, map[string]string) {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	values := make(map[string]int64)
	attrs := make(map[string]string)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if !strings.HasPrefix(m.Name, "schemabot.github.rate_limit.") {
				continue
			}
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok, "%s should be an int64 gauge", m.Name)
			require.Len(t, gauge.DataPoints, 1, "%s should have one data point", m.Name)
			dp := gauge.DataPoints[0]
			values[m.Name] = dp.Value
			for _, kv := range dp.Attributes.ToSlice() {
				attrs[string(kv.Key)] = attributeValueString(kv.Value)
			}
		}
	}

	return values, attrs
}

func attributeValueString(value attribute.Value) string {
	switch value.Type() {
	case attribute.STRING:
		return value.AsString()
	case attribute.INT64:
		return strconv.FormatInt(value.AsInt64(), 10)
	default:
		return value.Emit()
	}
}
