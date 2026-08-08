package github

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mergeQueueEntryGraphQLRequest mirrors the merge-queue-entry query payload so
// the fake server can assert on the repository and PR number the client sends.
type mergeQueueEntryGraphQLRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables"`
}

func TestFetchMergeQueueEntryReturnsQueuedEntry(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, r *http.Request) {
		var req mergeQueueEntryGraphQLRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Contains(t, req.Query, "mergeQueueEntry")
		assert.Equal(t, "octocat", req.Variables["owner"])
		assert.Equal(t, "hello-world", req.Variables["name"])
		assert.Equal(t, float64(1), req.Variables["number"])
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"repository": map[string]any{
					"pullRequest": map[string]any{
						"mergeQueueEntry": map[string]any{
							"headCommit": map[string]any{"oid": "mergegroupsha1"},
						},
					},
				},
			},
		}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	entry, err := ic.FetchMergeQueueEntry(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, "mergegroupsha1", entry.HeadSHA)
}

func TestFetchMergeQueueEntryReturnsNilWhenNotQueued(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"repository": map[string]any{
					"pullRequest": map[string]any{"mergeQueueEntry": nil},
				},
			},
		}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	entry, err := ic.FetchMergeQueueEntry(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	assert.Nil(t, entry)
}

// A queued PR whose merge group has not been built yet reports an entry with
// no head commit; the caller distinguishes that from "not queued" by the
// empty HeadSHA.
func TestFetchMergeQueueEntryReturnsEmptyHeadSHAWhenGroupNotBuilt(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"repository": map[string]any{
					"pullRequest": map[string]any{
						"mergeQueueEntry": map[string]any{"headCommit": nil},
					},
				},
			},
		}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	entry, err := ic.FetchMergeQueueEntry(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Empty(t, entry.HeadSHA)
}

func TestFetchMergeQueueEntryReturnsGraphQLError(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"data": nil,
			"errors": []map[string]any{
				{"message": "API rate limit exceeded"},
			},
		}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	entry, err := ic.FetchMergeQueueEntry(t.Context(), "octocat/hello-world", 1)
	require.Error(t, err)
	assert.Nil(t, entry)
	assert.Contains(t, err.Error(), "API rate limit exceeded")
	assert.Contains(t, err.Error(), "octocat/hello-world#1")
}

func TestFetchMergeQueueEntryErrorsWhenPRNotFound(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"repository": map[string]any{"pullRequest": nil},
			},
		}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	entry, err := ic.FetchMergeQueueEntry(t.Context(), "octocat/hello-world", 1)
	require.Error(t, err)
	assert.Nil(t, entry)
	assert.Contains(t, err.Error(), "pull request not found")
}

func TestFetchMergeQueueEntryReturnsHTTPError(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	entry, err := ic.FetchMergeQueueEntry(t.Context(), "octocat/hello-world", 1)
	require.Error(t, err)
	assert.Nil(t, entry)
	assert.Contains(t, err.Error(), "502")
}
