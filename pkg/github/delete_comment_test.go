package github

import (
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteIssueCommentSendsDelete(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	var deleted bool
	mux.HandleFunc("DELETE /repos/octocat/hello-world/issues/comments/1234", func(w http.ResponseWriter, _ *http.Request) {
		deleted = true
		w.WriteHeader(http.StatusNoContent)
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, ic.DeleteIssueComment(t.Context(), "octocat/hello-world", 1234))
	assert.True(t, deleted, "the DELETE must reach GitHub")
}

// A comment GitHub no longer has counts as deleted: retirement sweeps retry
// failed deletes, and a retry after a crash — or a comment a human already
// removed — must converge instead of erroring forever.
func TestDeleteIssueCommentTreatsNotFoundAsDeleted(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("DELETE /repos/octocat/hello-world/issues/comments/1234", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, ic.DeleteIssueComment(t.Context(), "octocat/hello-world", 1234))
}

func TestDeleteIssueCommentPropagatesOtherErrors(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("DELETE /repos/octocat/hello-world/issues/comments/1234", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	err := ic.DeleteIssueComment(t.Context(), "octocat/hello-world", 1234)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "delete issue comment 1234")
}
