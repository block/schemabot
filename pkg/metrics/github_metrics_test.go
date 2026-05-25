package metrics

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
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
