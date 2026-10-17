package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

const startWithdrawnPostDeadline = 5 * time.Second

// startWithdrawnHandler returns a handler whose GitHub client records the
// comments posted on octocat/hello-world#7.
func startWithdrawnHandler(t *testing.T) (*Handler, func() []string) {
	t.Helper()
	client, mux := setupGitHubServer(t)
	var mu sync.Mutex
	var posted []string
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/7/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		mu.Lock()
		posted = append(posted, body.Body)
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 99}))
	})
	h := &Handler{
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}),
		logger:    testLogger(),
	}
	return h, func() []string {
		ctx, cancel := context.WithTimeout(t.Context(), startWithdrawnPostDeadline)
		defer cancel()
		h.DrainInProcessWebhookWork(ctx)
		require.NoError(t, ctx.Err(), "the withdrawal comment post did not finish")
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), posted...)
	}
}

func startWithdrawnApply() *storage.Apply {
	return &storage.Apply{
		ApplyIdentifier: "apply-a1b2c3d4e5f67890",
		Database:        "orders",
		Environment:     "production",
		Repository:      "octocat/hello-world",
		PullRequest:     7,
		InstallationID:  42,
	}
}

// An operator starts a stopped schema change from its PR and then stops it
// from the CLI before any driver acts on the start. The apply never leaves
// stopped, so no drive updates the PR; SchemaBot tells the PR the start was
// withdrawn, so its timeline does not end on a start acknowledgement that
// promised a resume.
func TestReportQueuedStartWithdrawnTellsThePR(t *testing.T) {
	h, posted := startWithdrawnHandler(t)

	h.reportQueuedStartWithdrawn(startWithdrawnApply(), "cli:bob@laptop.local")

	assert.Equal(t, []string{templates.RenderStartWithdrawn(templates.StartWithdrawnData{
		ApplyID:     "apply-a1b2c3d4e5f67890",
		Environment: "production",
	})}, posted(), "one comment, with no stopper named for a caller that is not a GitHub user")
}

// A stop sent as a command on the same PR already gets a reply saying it
// withdrew the start, so the PR is not told a second time.
func TestReportQueuedStartWithdrawnSkipsAStopCommandOnThePR(t *testing.T) {
	h, posted := startWithdrawnHandler(t)

	h.reportQueuedStartWithdrawn(startWithdrawnApply(), formatGitHubCaller("alice", "octocat/hello-world", 7))

	assert.Empty(t, posted())
}

// An apply with no PR, such as one started from the CLI, has nowhere to post.
func TestReportQueuedStartWithdrawnSkipsAnApplyWithoutAPR(t *testing.T) {
	h, posted := startWithdrawnHandler(t)
	apply := startWithdrawnApply()
	apply.Repository, apply.PullRequest, apply.InstallationID = "", 0, 0

	h.reportQueuedStartWithdrawn(apply, "cli:bob@laptop.local")

	assert.Empty(t, posted())
}

func TestCallerIsPRCommand(t *testing.T) {
	assert.True(t, callerIsPRCommand("github:alice@octocat/hello-world#7", "octocat/hello-world", 7))
	assert.False(t, callerIsPRCommand("github:alice@octocat/hello-world#8", "octocat/hello-world", 7), "a command on another PR")
	assert.False(t, callerIsPRCommand("github:alice@octocat/other#7", "octocat/hello-world", 7), "a command on another repo")
	assert.False(t, callerIsPRCommand("cli:alice@laptop.local", "octocat/hello-world", 7))
	assert.False(t, callerIsPRCommand("", "octocat/hello-world", 7))
}
