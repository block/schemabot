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

// TestFindCheckRunByNameIgnoresForeignAppCheckRuns covers a PR commit that
// carries two completed check runs with the same name: a passing run created
// by another GitHub App and SchemaBot's own run, which has not passed. The
// lookup must return SchemaBot's own run so a same-named foreign run can
// never satisfy a gate that relies on this lookup.
func TestFindCheckRunByNameIgnoresForeignAppCheckRuns(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "SchemaBot (staging)", r.URL.Query().Get("check_name"))
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"total_count": 2,
			"check_runs": []map[string]any{
				{"id": 7, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "success", "app": map[string]any{"slug": "github-actions"}},
				{"id": 3, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "action_required", "app": map[string]any{"slug": "schemabot"}},
			},
		}))
	})

	ic := NewInstallationClientWithSlug(client, slog.New(slog.NewTextHandler(io.Discard, nil)), "schemabot")
	result, err := ic.FindCheckRunByName(t.Context(), "octocat/hello-world", "abc123", "SchemaBot (staging)")

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int64(3), result.ID)
	assert.Equal(t, "SchemaBot (staging)", result.Name)
	assert.Equal(t, "completed", result.Status)
	assert.Equal(t, "action_required", result.Conclusion)
}

// TestFindCheckRunByNameReturnsNilWhenOnlyForeignAppRunsExist covers a PR
// commit where the only check run with the requested name was created by
// another GitHub App. The lookup must report the check run as missing so
// callers treat the gate as unsatisfied.
func TestFindCheckRunByNameReturnsNilWhenOnlyForeignAppRunsExist(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"total_count": 1,
			"check_runs": []map[string]any{
				{"id": 7, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "success", "app": map[string]any{"slug": "github-actions"}},
			},
		}))
	})

	ic := NewInstallationClientWithSlug(client, slog.New(slog.NewTextHandler(io.Discard, nil)), "schemabot")
	result, err := ic.FindCheckRunByName(t.Context(), "octocat/hello-world", "abc123", "SchemaBot (staging)")

	require.NoError(t, err)
	assert.Nil(t, result)
}

// TestFindCheckRunByNameReturnsMostRecentOwnAppRun covers a PR commit with
// several own-app check runs sharing the same name. The lookup must return
// the most recently created run (highest check run ID) regardless of the
// order GitHub lists them in, so a stale earlier run cannot mask the current
// gate state.
func TestFindCheckRunByNameReturnsMostRecentOwnAppRun(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"total_count": 2,
			"check_runs": []map[string]any{
				{"id": 3, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "failure", "app": map[string]any{"slug": "schemabot"}},
				{"id": 9, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "success", "app": map[string]any{"slug": "schemabot"}},
			},
		}))
	})

	ic := NewInstallationClientWithSlug(client, slog.New(slog.NewTextHandler(io.Discard, nil)), "schemabot")
	result, err := ic.FindCheckRunByName(t.Context(), "octocat/hello-world", "abc123", "SchemaBot (staging)")

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int64(9), result.ID)
	assert.Equal(t, "success", result.Conclusion)
}

// TestFindCheckRunByNameErrorsWhenOwnAppSlugUnknown covers the case where the
// client does not know its own GitHub App slug, so check run ownership cannot
// be verified. The lookup must return an error without querying GitHub —
// ownership ambiguity must never be converted into a result a gate could
// trust.
func TestFindCheckRunByNameErrorsWhenOwnAppSlugUnknown(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	var requests atomic.Int64
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"total_count": 1,
			"check_runs": []map[string]any{
				{"id": 7, "name": "SchemaBot (staging)", "status": "completed", "conclusion": "success", "app": map[string]any{"slug": "schemabot"}},
			},
		}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	result, err := ic.FindCheckRunByName(t.Context(), "octocat/hello-world", "abc123", "SchemaBot (staging)")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "check run ownership cannot be verified")
	assert.Contains(t, err.Error(), "octocat/hello-world")
	assert.Contains(t, err.Error(), "abc123")
	assert.Nil(t, result)
	assert.Equal(t, int64(0), requests.Load())
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
