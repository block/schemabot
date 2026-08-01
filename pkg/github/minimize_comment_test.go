package github

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// minimizeGraphQLRequest mirrors the minimizeComment mutation payload so the
// fake server can assert on the node ID and classifier the client sends.
type minimizeGraphQLRequest struct {
	Query     string            `json:"query"`
	Variables map[string]string `json:"variables"`
}

func TestMinimizeCommentSendsOutdatedMutation(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, r *http.Request) {
		var req minimizeGraphQLRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Contains(t, req.Query, "minimizeComment")
		assert.Contains(t, req.Query, "OUTDATED")
		assert.Equal(t, "IC_node1234", req.Variables["id"])
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"minimizeComment": map[string]any{
					"minimizedComment": map[string]any{"isMinimized": true},
				},
			},
		}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, ic.MinimizeComment(t.Context(), "octocat/hello-world", "IC_node1234"))
}

func TestMinimizeCommentReturnsGraphQLError(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"data": nil,
			"errors": []map[string]any{
				{"message": "Could not resolve to a node with the global id"},
			},
		}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	err := ic.MinimizeComment(t.Context(), "octocat/hello-world", "IC_bogus")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Could not resolve to a node")
	assert.Contains(t, err.Error(), "IC_bogus")
}

func TestMinimizeCommentRejectsUnminimizedResult(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"minimizeComment": map[string]any{
					"minimizedComment": map[string]any{"isMinimized": false},
				},
			},
		}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	err := ic.MinimizeComment(t.Context(), "octocat/hello-world", "IC_node1234")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "did not report the comment as minimized")
}

// GitHub Enterprise Server serves REST under /api/v3/ and GraphQL at
// /api/graphql on the same host; a client configured with an enterprise base
// URL must send the minimize mutation to the enterprise GraphQL path.
func TestMinimizeCommentUsesEnterpriseGraphQLPath(t *testing.T) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(&http.Client{Transport: newGitHubRateLimitTransport(http.DefaultTransport)})
	client.DisableRateLimitCheck = true
	baseURL, err := url.Parse(server.URL + "/api/v3/")
	require.NoError(t, err)
	client.BaseURL = baseURL

	mux.HandleFunc("POST /api/graphql", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"minimizeComment": map[string]any{
					"minimizedComment": map[string]any{"isMinimized": true},
				},
			},
		}))
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, ic.MinimizeComment(t.Context(), "octocat/hello-world", "IC_node1234"))
}

func TestMinimizeCommentReturnsHTTPError(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	err := ic.MinimizeComment(t.Context(), "octocat/hello-world", "IC_node1234")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "502")
}
