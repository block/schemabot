package webhook

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// postCommandError must always render a non-empty timestamp in the comment
// footer. A regression of the original bug renders "at  UTC" (two spaces) and
// is what the helper exists to prevent.
func TestPostCommandError_RendersTimestamp(t *testing.T) {
	original := templates.NowFunc
	t.Cleanup(func() { templates.NowFunc = original })
	templates.NowFunc = func() time.Time {
		return time.Date(2026, 5, 26, 12, 34, 56, 0, time.UTC)
	}

	client, mux := setupGitHubServer(t)
	bodies := make(chan string, 1)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		bodies <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})

	h := &Handler{
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}),
		logger:    testLogger(),
	}

	h.postCommandError(
		"octocat/hello-world", 1, 12345,
		"plan", "staging", "alice",
		"boom",
	)

	select {
	case body := <-bodies:
		assert.NotContains(t, body, "at  UTC",
			"comment must not render empty timestamp")
		assert.Contains(t, body, "at 2026-05-26 12:34:56 UTC",
			"comment must render the stubbed timestamp from templates.NowFunc")
		assert.Contains(t, body, "*Requested by @alice")
		assert.Contains(t, body, "Plan Failed")
		assert.Contains(t, body, "`staging`")
		assert.Contains(t, body, "> boom")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for posted comment")
	}
}

func TestPostCommandErrorExplainsRemoteUnavailable(t *testing.T) {
	client, mux := setupGitHubServer(t)
	bodies := make(chan string, 1)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		bodies <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})

	h := &Handler{
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}),
		logger:    testLogger(),
	}

	h.postCommandError(
		"octocat/hello-world", 1, 12345,
		"plan", "staging", "alice",
		"rpc error: code = Unavailable desc = no healthy upstream",
	)

	select {
	case body := <-bodies:
		assert.Contains(t, body, "SchemaBot could not reach the remote schema change service")
		assert.Contains(t, body, "No healthy upstream is available")
		assert.Contains(t, body, "Raw error: rpc error: code = Unavailable desc = no healthy upstream")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for posted comment")
	}
}
