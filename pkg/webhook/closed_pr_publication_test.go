package webhook

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
)

// newClosedPRTestHandler builds a handler whose GitHub reports PR #1 at
// head abc123 in the given state.
func newClosedPRTestHandler(t *testing.T, state string, merged bool) (*Handler, *ghclient.InstallationClient) {
	t.Helper()
	client, mux := setupGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"number": 1,
			"state":  state,
			"merged": merged,
			"head":   map[string]any{"sha": "abc123", "ref": "feature"},
			"base":   map[string]any{"sha": "def456", "ref": "main", "repo": map[string]any{"default_branch": "main"}},
			"user":   map[string]any{"login": "octocat"},
		}))
	})
	installClient := ghclient.NewInstallationClient(client, testLogger())
	h := &Handler{
		service:   api.New(&emptyStorage{}, &api.ServerConfig{}, nil, testLogger()),
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
		logger:    testLogger(),
	}
	return h, installClient
}

func prInfoInState(state string) *ghclient.PullRequestInfo {
	return &ghclient.PullRequestInfo{State: state, HeadSHA: "abc123", BaseRef: "main", DefaultBranch: "main"}
}

// A plan that finishes after its PR was closed stores no check state. Close
// cleanup already removed the PR's rows and does not run again, so a row
// written now would be orphaned.
func TestPlanCheckRecordRefusesClosedPR(t *testing.T) {
	h, client := newClosedPRTestHandler(t, "closed", false)
	schema := &ghclient.SchemaRequestResult{Database: "orders", Type: "mysql", HeadSHA: "abc123"}

	_, check, err := h.upsertPlanCheckRecord(t.Context(), client, "octocat/hello-world", 1, schema, &apitypes.PlanResponse{}, "staging", reviewDriftOutcome{})

	require.ErrorIs(t, err, errPRClosed)
	assert.Nil(t, check)
}

// A check publication from work that started while the PR was open is
// discarded once the PR is closed, so a plan racing the close cannot
// recreate check state close cleanup removed. Work that started on a PR
// already closed, such as an apply's terminal outcome after a merge, still
// publishes.
func TestCheckPublicationAfterClose(t *testing.T) {
	t.Run("work that started while the PR was open is discarded", func(t *testing.T) {
		h, client := newClosedPRTestHandler(t, "closed", false)
		ctx := withExpectedPRTarget(t.Context(), "octocat/hello-world", 1, prInfoInState("open"))

		current, err := h.verifyHeadSHACurrency(ctx, client, "octocat/hello-world", 1, "abc123", "aggregate_check_sync")

		require.NoError(t, err)
		assert.False(t, current)
	})

	t.Run("work that started after a merge still publishes", func(t *testing.T) {
		h, client := newClosedPRTestHandler(t, "closed", true)
		ctx := withExpectedPRTarget(t.Context(), "octocat/hello-world", 1, prInfoInState("closed"))

		current, err := h.verifyHeadSHACurrency(ctx, client, "octocat/hello-world", 1, "abc123", "aggregate_check_sync")

		require.NoError(t, err)
		assert.True(t, current)
	})
}
