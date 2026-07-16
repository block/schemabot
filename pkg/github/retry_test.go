package github

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ghErrorResponse(status int, message string) *gh.ErrorResponse {
	return &gh.ErrorResponse{
		Response: &http.Response{StatusCode: status},
		Message:  message,
	}
}

// TestIsGitHubUnavailableClassification pins down which GitHub API failures
// count as availability failures (retryable) versus semantic answers about
// the request (never retried). The 404 and message-bearing 400 rows are
// safety-relevant: config discovery treats 404 as "this path does not
// exist", so retrying it would turn every discovery probe into a slow path.
func TestIsGitHubUnavailableClassification(t *testing.T) {
	t.Parallel()

	retryAfter := 5 * time.Second
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "500 internal server error", err: ghErrorResponse(http.StatusInternalServerError, "boom"), want: true},
		{name: "503 service unavailable", err: ghErrorResponse(http.StatusServiceUnavailable, ""), want: true},
		{name: "429 too many requests", err: ghErrorResponse(http.StatusTooManyRequests, "slow down"), want: true},
		{name: "empty-body 400 from the edge", err: ghErrorResponse(http.StatusBadRequest, ""), want: true},
		{name: "400 with a real validation message", err: ghErrorResponse(http.StatusBadRequest, "invalid request"), want: false},
		{name: "400 with field errors", err: &gh.ErrorResponse{Response: &http.Response{StatusCode: http.StatusBadRequest}, Errors: []gh.Error{{Code: "invalid"}}}, want: false},
		{name: "404 not found", err: ghErrorResponse(http.StatusNotFound, "Not Found"), want: false},
		{name: "401 unauthorized", err: ghErrorResponse(http.StatusUnauthorized, "Bad credentials"), want: false},
		{name: "403 forbidden", err: ghErrorResponse(http.StatusForbidden, "Resource not accessible"), want: false},
		{name: "primary rate limit means the hourly budget is exhausted", err: &gh.RateLimitError{Rate: gh.Rate{Reset: gh.Timestamp{Time: time.Now().Add(time.Minute)}}}, want: false},
		{name: "secondary rate limit clears within seconds", err: &gh.AbuseRateLimitError{RetryAfter: &retryAfter}, want: true},
		{name: "wrapped 502", err: fmt.Errorf("fetch file: %w", ghErrorResponse(http.StatusBadGateway, "")), want: true},
		{name: "context deadline exceeded", err: context.DeadlineExceeded, want: true},
		{name: "plain error", err: errors.New("something else"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, isGitHubUnavailable(tt.err))
			assert.Equal(t, tt.want, IsUnavailableError(classifyGitHubAPIError(tt.err)))
		})
	}
}

// TestGitHubReadRetryDelay verifies the wait schedule: exponential growth
// from the base delay, raised to a rate-limit retry-after hint when the
// server provides one, and always capped so a long rate-limit window cannot
// stall a caller indefinitely.
func TestGitHubReadRetryDelay(t *testing.T) {
	t.Parallel()

	shortHint := 100 * time.Millisecond
	longHint := 5 * time.Second
	transient := ghErrorResponse(http.StatusServiceUnavailable, "")

	tests := []struct {
		name    string
		attempt int
		err     error
		want    time.Duration
	}{
		{name: "first attempt uses the base delay", attempt: 1, err: transient, want: 500 * time.Millisecond},
		{name: "delay doubles per attempt", attempt: 3, err: transient, want: 2 * time.Second},
		{name: "fifth attempt reaches eight seconds", attempt: 5, err: transient, want: 8 * time.Second},
		{name: "retry-after hint raises the wait", attempt: 1, err: &gh.AbuseRateLimitError{RetryAfter: &longHint}, want: 5 * time.Second},
		{name: "retry-after hint survives error classification", attempt: 1, err: classifyGitHubAPIError(&gh.AbuseRateLimitError{RetryAfter: &longHint}), want: 5 * time.Second},
		{name: "schedule wins over a shorter hint", attempt: 3, err: &gh.AbuseRateLimitError{RetryAfter: &shortHint}, want: 2 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, githubReadRetryDelay(tt.attempt, tt.err))
		})
	}

	t.Run("a long retry-after hint is capped", func(t *testing.T) {
		t.Parallel()

		farHint := 10 * time.Minute
		abuseErr := &gh.AbuseRateLimitError{RetryAfter: &farHint}
		assert.Equal(t, githubUnavailableReadRetryMaxDelay, githubReadRetryDelay(1, abuseErr))
	})
}

// TestGetIssueCommentRetriesEmptyBodyBadRequest exercises the transient
// empty-body 400 GitHub's edge intermittently returns for valid reads: the
// read is retried and the caller sees the successful result, not the blip.
func TestGetIssueCommentRetriesEmptyBodyBadRequest(t *testing.T) {
	setGitHubUnavailableReadRetryDelay(t, time.Millisecond)

	client, mux := setupConfigTestGitHubServer(t)
	requests := 0
	mux.HandleFunc("GET /repos/octocat/hello-world/issues/comments/77", func(w http.ResponseWriter, _ *http.Request) {
		requests++
		if requests == 1 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, err := w.Write([]byte(`{"id": 77, "body": "progress comment body"}`))
		require.NoError(t, err)
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	body, err := ic.GetIssueComment(t.Context(), "octocat/hello-world", 77)

	require.NoError(t, err)
	assert.Equal(t, "progress comment body", body)
	assert.Equal(t, 2, requests)
}

// TestGetIssueCommentDoesNotRetrySemanticBadRequest verifies a 400 that
// carries a real validation message fails immediately: it is GitHub's
// answer about the request, and retrying it would only delay the caller.
func TestGetIssueCommentDoesNotRetrySemanticBadRequest(t *testing.T) {
	setGitHubUnavailableReadRetryDelay(t, time.Millisecond)

	client, mux := setupConfigTestGitHubServer(t)
	requests := 0
	mux.HandleFunc("GET /repos/octocat/hello-world/issues/comments/77", func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte(`{"message": "problems parsing JSON"}`))
		require.NoError(t, err)
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, err := ic.GetIssueComment(t.Context(), "octocat/hello-world", 77)

	require.Error(t, err)
	assert.False(t, IsUnavailableError(err))
	assert.Equal(t, 1, requests)
}

// TestListReviewsRetriesUnavailableRead verifies the paginated review read
// that gates approvals survives a transient 5xx on one page.
func TestListReviewsRetriesUnavailableRead(t *testing.T) {
	setGitHubUnavailableReadRetryDelay(t, time.Millisecond)

	client, mux := setupConfigTestGitHubServer(t)
	requests := 0
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/reviews", func(w http.ResponseWriter, _ *http.Request) {
		requests++
		if requests == 1 {
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			return
		}
		_, err := w.Write([]byte(`[{"user": {"login": "alice"}, "state": "APPROVED"}]`))
		require.NoError(t, err)
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	reviews, err := ic.ListReviews(t.Context(), "octocat/hello-world", 1)

	require.NoError(t, err)
	require.Len(t, reviews, 1)
	assert.Equal(t, "alice", reviews[0].User)
	assert.Equal(t, ReviewApproved, reviews[0].State)
	assert.Equal(t, 2, requests)
}
