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

func TestRecordWebhookEventKnownObservedGitHubLabels(t *testing.T) {
	seenUnknownWebhookMetricLabels = sync.Map{}
	t.Cleanup(func() {
		seenUnknownWebhookMetricLabels = sync.Map{}
	})

	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	previousProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	defer func() {
		otel.SetMeterProvider(previousProvider)
		require.NoError(t, mp.Shutdown(t.Context()))
	}()

	RecordWebhookEvent(t.Context(), "default", "check_suite", "requested", "octocat/hello-world", "ignored")
	RecordWebhookEvent(t.Context(), "default", "pull_request", "enqueued", "octocat/hello-world", "processed")
	RecordWebhookEvent(t.Context(), "default", "pull_request", "dequeued", "octocat/hello-world", "processed")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "schemabot.webhook.events_total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				key := metricAttributeString(t, dp.Attributes, "event_type") + ":" +
					metricAttributeString(t, dp.Attributes, "action") + ":" +
					metricAttributeString(t, dp.Attributes, "status")
				got[key] = dp.Value
			}
		}
	}
	assert.Equal(t, map[string]int64{
		"check_suite:requested:ignored":   1,
		"pull_request:enqueued:processed": 1,
		"pull_request:dequeued:processed": 1,
	}, got)

	var unknownLabels int
	seenUnknownWebhookMetricLabels.Range(func(_, _ any) bool {
		unknownLabels++
		return true
	})
	assert.Zero(t, unknownLabels)
}

func assertMetricAttribute(t *testing.T, attrs attribute.Set, key string, want string) {
	t.Helper()
	got := metricAttributeString(t, attrs, key)
	assert.Equal(t, want, got)
}

func metricAttributeString(t *testing.T, attrs attribute.Set, key string) string {
	t.Helper()
	got, ok := attrs.Value(attribute.Key(key))
	require.True(t, ok)
	return got.AsString()
}
