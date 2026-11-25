package webhook

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/tern"
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
		require.FailNow(t, "timed out waiting for posted comment")
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
		assert.Contains(t, body, "SchemaBot could not reach the remote schema change service. Run the command again once the service is healthy; if it fails again, the SchemaBot server logs have the cause.")
		assert.NotContains(t, body, "no healthy upstream")
		assert.NotContains(t, body, "rpc error: code =")
	case <-time.After(2 * time.Second):
		require.FailNow(t, "timed out waiting for posted comment")
	}
}

// A shared schema check that cannot learn which schemas a database's
// namespaces address fails the command with a fixed line naming the database,
// target, and deployment. The data plane's own error stays in the server logs,
// a data plane too old to answer is told to upgrade, and a data plane whose
// engine cannot resolve namespaces is told a retry will not help.
func TestUserFacingErrorHidesNamespaceResolutionCause(t *testing.T) {
	resolution := func(cause error) error {
		return fmt.Errorf("compare ignore_tables of %q with the databases sharing its targets in staging: %w", "shop",
			&api.NamespaceResolutionError{Database: "shop", Environment: "staging", Deployment: "east", Target: "shop-001", Namespaces: []string{"shop"}, Cause: cause})
	}

	t.Run("a data plane that predates the RPC", func(t *testing.T) {
		message := userFacingError(resolution(fmt.Errorf("resolve namespaces: %w", tern.ErrResolveNamespacesUnimplemented)))
		assert.Equal(t, "SchemaBot could not check which schemas `shop` shares with other databases on target `shop-001` of deployment `east`, because that deployment's data plane cannot report which schema each namespace addresses. Upgrade that data plane, then run the command again.", message)
	})
	t.Run("an engine that cannot resolve namespaces", func(t *testing.T) {
		message := userFacingError(resolution(fmt.Errorf("resolve namespaces: %w", tern.ErrResolveNamespacesUnsupported)))
		assert.Equal(t, "SchemaBot could not check which schemas `shop` shares with other databases on target `shop-001` of deployment `east`, because that deployment's data plane cannot resolve namespaces for this database's engine. Running the command again will not help; the SchemaBot server logs have the cause.", message)
	})
	t.Run("any other failure", func(t *testing.T) {
		message := userFacingError(resolution(errors.New("dial tcp db-internal.example:3306: connection refused")))
		assert.Equal(t, "SchemaBot could not check which schemas `shop` shares with other databases on target `shop-001` of deployment `east`, so the command was refused. Run the command again; if it fails again, the SchemaBot server logs have the cause.", message)
		assert.NotContains(t, message, "db-internal")
	})
}
