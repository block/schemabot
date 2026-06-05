package github

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateCheckRunRetriesSecondaryRateLimitedWrite(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	var attempts atomic.Int64
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		assert.Contains(t, string(body), "schemabot/staging")
		if attempts.Add(1) == 1 {
			writeGitHubSecondaryRateLimitError(t, w, http.StatusTooManyRequests)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(gh.CheckRun{ID: new(int64(1234))}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	checkRunID, err := ic.CreateCheckRun(t.Context(), "octocat/hello-world", "abc123", CheckRunOptions{
		Name:   "schemabot/staging",
		Status: "in_progress",
	})

	require.NoError(t, err)
	assert.Equal(t, int64(1234), checkRunID)
	assert.Equal(t, int64(2), attempts.Load())
}

func TestUpdateCheckRunRetriesSecondaryRateLimitWrite(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	var attempts atomic.Int64
	mux.HandleFunc("PATCH /repos/octocat/hello-world/check-runs/1234", func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			writeGitHubSecondaryRateLimitError(t, w, http.StatusForbidden)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(gh.CheckRun{ID: new(int64(1234))}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	err := ic.UpdateCheckRun(t.Context(), "octocat/hello-world", 1234, CheckRunOptions{
		Name:   "schemabot/staging",
		Status: "completed",
	})

	require.NoError(t, err)
	assert.Equal(t, int64(2), attempts.Load())
}

func TestCreateIssueCommentRetriesSecondaryRateLimitedWrite(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	var attempts atomic.Int64
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			writeGitHubSecondaryRateLimitError(t, w, http.StatusTooManyRequests)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(gh.IssueComment{ID: new(int64(5678))}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	commentID, err := ic.CreateIssueComment(t.Context(), "octocat/hello-world", 1, "hello")

	require.NoError(t, err)
	assert.Equal(t, int64(5678), commentID)
	assert.Equal(t, int64(2), attempts.Load())
}

func TestAddReactionToCommentRetriesSecondaryRateLimitedWrite(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	var attempts atomic.Int64
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/5678/reactions", func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			writeGitHubSecondaryRateLimitError(t, w, http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(gh.Reaction{ID: new(int64(9012))}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	err := ic.AddReactionToComment(t.Context(), "octocat/hello-world", 5678, "eyes")

	require.NoError(t, err)
	assert.Equal(t, int64(2), attempts.Load())
}

func TestEditIssueCommentDoesNotRetryNonRateLimitWriteError(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	var attempts atomic.Int64
	mux.HandleFunc("PATCH /repos/octocat/hello-world/issues/comments/5678", func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
		require.NoError(t, json.NewEncoder(w).Encode(gh.ErrorResponse{Message: "bad credentials"}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	err := ic.EditIssueComment(t.Context(), "octocat/hello-world", 5678, "hello")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "401")
	assert.Equal(t, int64(1), attempts.Load())
}

func setupRateLimitedTestGitHubServer(t *testing.T) (*gh.Client, *http.ServeMux) {
	t.Helper()

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(&http.Client{Transport: newGitHubRateLimitTransport(http.DefaultTransport)})
	client.DisableRateLimitCheck = true
	baseURL, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client.BaseURL = baseURL

	return client, mux
}

func writeGitHubSecondaryRateLimitError(t *testing.T, w http.ResponseWriter, statusCode int) {
	t.Helper()

	w.Header().Set("X-RateLimit-Remaining", "1")
	w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(-time.Second).Unix(), 10))
	w.WriteHeader(statusCode)
	require.NoError(t, json.NewEncoder(w).Encode(gh.ErrorResponse{
		Message:          "You have exceeded a secondary rate limit",
		DocumentationURL: "https://docs.github.com/rest/using-the-rest-api/rate-limits-for-the-rest-api#about-secondary-rate-limits",
	}))
}
