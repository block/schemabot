package metrics

import (
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeGitHubOperation(t *testing.T) {
	assert.Equal(t, GitHubOperationFetchPullRequest, normalizeGitHubOperation(GitHubOperationFetchPullRequest))
	assert.Equal(t, gitHubMetricValueUnknown, normalizeGitHubOperation("new_github_operation"))
}

func TestNormalizeGitHubRequestCategory(t *testing.T) {
	assert.Equal(t, GitHubRequestCategoryWrite, normalizeGitHubRequestCategory(GitHubRequestCategoryWrite))
	assert.Equal(t, GitHubRequestCategoryUnknown, normalizeGitHubRequestCategory("new_category"))
}

func TestNormalizeGitHubRequestStatus(t *testing.T) {
	assert.Equal(t, GitHubRequestStatusSuccess, normalizeGitHubRequestStatus(GitHubRequestStatusSuccess))
	assert.Equal(t, GitHubRequestStatusUnknown, normalizeGitHubRequestStatus("new_status"))
}

func TestNormalizeGitHubRateLimitResource(t *testing.T) {
	assert.Equal(t, GitHubRateLimitResourceCore, normalizeGitHubRateLimitResource(GitHubRateLimitResourceCore))
	assert.Equal(t, gitHubMetricValueUnknown, normalizeGitHubRateLimitResource("new_resource"))
}

func TestUnknownGitHubMetricLabelsAreTrackedOncePerDistinctValue(t *testing.T) {
	seenUnknownGitHubMetricLabels = sync.Map{}
	t.Cleanup(func() {
		seenUnknownGitHubMetricLabels = sync.Map{}
	})

	assert.Equal(t, gitHubMetricValueUnknown, normalizeGitHubOperation("new_github_operation"))
	assert.Equal(t, gitHubMetricValueUnknown, normalizeGitHubOperation("new_github_operation"))
	assert.Equal(t, gitHubMetricValueUnknown, normalizeGitHubOperation("another_github_operation"))

	var seen int
	seenUnknownGitHubMetricLabels.Range(func(_, _ any) bool {
		seen++
		return true
	})
	assert.Equal(t, 2, seen)
}

func TestRecordUnregisteredRepositoryWebhook(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	previousProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	defer func() {
		otel.SetMeterProvider(previousProvider)
		require.NoError(t, mp.Shutdown(t.Context()))
	}()

	RecordUnregisteredRepositoryWebhook(t.Context(), "default", "issue_comment", "created", "octocat/hello-world")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	var found bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "schemabot.webhook.unregistered_repository_ignored_total" {
				continue
			}
			found = true
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			require.Len(t, sum.DataPoints, 1)
			assert.Equal(t, int64(1), sum.DataPoints[0].Value)
			assertMetricAttribute(t, sum.DataPoints[0].Attributes, "environment", "unknown")
			assertMetricAttribute(t, sum.DataPoints[0].Attributes, "app_name", "default")
			assertMetricAttribute(t, sum.DataPoints[0].Attributes, "event_type", "issue_comment")
			assertMetricAttribute(t, sum.DataPoints[0].Attributes, "action", "created")
			assertMetricAttribute(t, sum.DataPoints[0].Attributes, "repository", "octocat/hello-world")
		}
	}
	assert.True(t, found)
}

func assertMetricAttribute(t *testing.T, attrs attribute.Set, key string, want string) {
	t.Helper()
	got, ok := attrs.Value(attribute.Key(key))
	require.True(t, ok)
	assert.Equal(t, want, got.AsString())
}
