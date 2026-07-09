package webhook

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
)

// A Check Run backfill on a closed PR is refused: close-time cleanup owns a
// closed PR's stored check state, so recreating Check Runs there would
// resurrect what cleanup settled.
func TestBackfillPRCheckRunsSkipsClosedPR(t *testing.T) {
	client, mux := setupGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"state": "closed",
			"head":  map[string]any{"sha": "abc123", "ref": "feature-branch"},
			"base":  map[string]any{"sha": "def456", "ref": "main"},
			"user":  map[string]any{"login": "testuser"},
		}))
	})
	h := actorAuthStorageTestHandler(actorAuthTestConfig(false), &emptyStorage{}, ghclient.NewInstallationClient(client, testLogger()))

	outcome, err := h.BackfillPRCheckRuns(t.Context(), "octocat/hello-world", 1, 12345)

	require.NoError(t, err)
	assert.Equal(t, "skipped: PR is closed", outcome)
}

// A backfill on an open PR replays the auto-plan flow against the PR's
// current head — for a PR with no managed schema changes, that is the
// passing-aggregate path a real check-creating delivery would have taken.
func TestBackfillPRCheckRunsReplaysAutoPlanFlow(t *testing.T) {
	client, mux := setupGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"state": "open",
			"head":  map[string]any{"sha": "abc123", "ref": "feature-branch"},
			"base":  map[string]any{"sha": "def456", "ref": "main"},
			"user":  map[string]any{"login": "testuser"},
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode([]map[string]string{{
			"filename": "docs/readme.md",
			"status":   "modified",
		}}))
	})
	h := actorAuthStorageTestHandler(actorAuthTestConfig(false), &emptyStorage{}, ghclient.NewInstallationClient(client, testLogger()))

	outcome, err := h.BackfillPRCheckRuns(t.Context(), "octocat/hello-world", 1, 12345)

	require.NoError(t, err)
	assert.Equal(t, "no schema files in PR", outcome)
}
