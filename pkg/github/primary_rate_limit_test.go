package github

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rateLimitedPullRequestRead sends one pull request read through transport to
// a server that answers it with GitHub's primary rate limit response. An empty
// reset leaves the X-RateLimit-Reset header out.
func rateLimitedPullRequestRead(t *testing.T, transport http.RoundTripper, reset string) error {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RateLimit-Resource", "core")
		w.Header().Set("X-RateLimit-Remaining", "0")
		if reset != "" {
			w.Header().Set("X-RateLimit-Reset", reset)
		}
		w.WriteHeader(http.StatusForbidden)
		_, err := io.WriteString(w, `{"message":"API rate limit exceeded"}`)
		assert.NoError(t, err)
	}))
	t.Cleanup(server.Close)
	client := gh.NewClient(&http.Client{Transport: transport})
	baseURL, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client.BaseURL = baseURL

	_, _, err = client.PullRequests.Get(t.Context(), "octocat", "example", 1)
	require.Error(t, err)
	return err
}

// The reset time comes out of both shapes a primary rate limit reaches a
// caller in: the client's rate limit transport, which the installation client
// uses, and go-github's own error for a response the transport does not
// handle. A caller sees either one inside the wrapping its own layers add.
func TestPrimaryRateLimitResetAt(t *testing.T) {
	resetAt := time.Now().Add(40 * time.Minute).Truncate(time.Second)
	reset := strconv.FormatInt(resetAt.Unix(), 10)

	tests := []struct {
		name      string
		transport http.RoundTripper
	}{
		{name: "rate limit transport", transport: newGitHubRateLimitTransport(http.DefaultTransport)},
		{name: "go-github error", transport: http.DefaultTransport},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := rateLimitedPullRequestRead(t, tc.transport, reset)

			got, ok := PrimaryRateLimitResetAt(fmt.Errorf("auto-plan for delivery d1 (octocat/example#1): %w", err))

			require.True(t, ok)
			assert.True(t, resetAt.Equal(got), "reset time %s, got %s", resetAt, got)
		})
	}
}

// A request the rate limit transport refuses without sending carries the
// reset time of the response that exhausted the budget.
func TestPrimaryRateLimitResetAtOfRequestRefusedLocally(t *testing.T) {
	resetAt := time.Now().Add(40 * time.Minute).Truncate(time.Second)
	reset := strconv.FormatInt(resetAt.Unix(), 10)
	transport := newGitHubRateLimitTransport(http.DefaultTransport)
	require.Error(t, rateLimitedPullRequestRead(t, transport, reset))

	err := rateLimitedPullRequestRead(t, transport, reset)

	got, ok := PrimaryRateLimitResetAt(err)
	require.True(t, ok)
	assert.True(t, resetAt.Equal(got), "reset time %s, got %s", resetAt, got)
}

// A rate limit whose reset time was not reported has nothing to wait for, so
// it does not report one.
func TestPrimaryRateLimitResetAtWithoutReportedReset(t *testing.T) {
	for name, transport := range map[string]http.RoundTripper{
		"rate limit transport": newGitHubRateLimitTransport(http.DefaultTransport),
		"go-github error":      http.DefaultTransport,
	} {
		t.Run(name, func(t *testing.T) {
			err := rateLimitedPullRequestRead(t, transport, "")

			got, ok := PrimaryRateLimitResetAt(err)

			assert.False(t, ok)
			assert.True(t, got.IsZero())
		})
	}
}

// Only the primary rate limit reports a reset time. A secondary rate limit
// clears on its own within seconds and is retried by the read retry.
func TestPrimaryRateLimitResetAtIgnoresOtherErrors(t *testing.T) {
	retryAfter := time.Minute
	for name, err := range map[string]error{
		"plain error":          errors.New("boom"),
		"secondary rate limit": &gh.AbuseRateLimitError{RetryAfter: &retryAfter},
		"quota reserve":        &QuotaReservedError{ResetAt: time.Now().Add(time.Hour)},
		"nil":                  nil,
	} {
		t.Run(name, func(t *testing.T) {
			_, ok := PrimaryRateLimitResetAt(err)
			assert.False(t, ok)
		})
	}
}
