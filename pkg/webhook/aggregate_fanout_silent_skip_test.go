package webhook

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/webhook/action"
)

// aggregateLeaderConfig returns a server config where the test repo has the
// aggregate leader role and the databases registry has no entry for the
// participant-owned database "orders" — the shape of an untenanted leader
// receiving unscoped fan-out commands for work another deployment owns.
func aggregateLeaderConfig() *api.ServerConfig {
	return &api.ServerConfig{
		Repos: map[string]api.RepoConfig{
			"octocat/hello-world": {Aggregate: &api.AggregateConfig{
				Role:            api.AggregateRoleLeader,
				ExpectedTenants: []api.ExpectedTenant{{Tenant: "tenant-b", Paths: []string{"tenant-b/schema"}, CheckName: "SchemaBot Tenant B"}},
			}},
		},
	}
}

// nonAggregateConfig returns a server config where the test repo has no
// aggregate role, so every "nothing to do" answer stays a visible PR comment.
func nonAggregateConfig() *api.ServerConfig {
	return &api.ServerConfig{
		Repos: map[string]api.RepoConfig{"octocat/hello-world": {}},
	}
}

// newFanOutSkipHandler builds a handler backed by a fake GitHub server and
// empty storage, returning the handler, the GitHub mux for extra routes, and a
// channel capturing posted PR comments. The handlers under test post comments
// synchronously, so after a direct handler call the channel holds every
// comment the command produced.
func newFanOutSkipHandler(t *testing.T, cfg *api.ServerConfig) (*Handler, *http.ServeMux, chan string) {
	t.Helper()
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))

	installClient := ghclient.NewInstallationClient(client, testLogger())
	h := &Handler{
		service:   api.New(&emptyStorage{}, cfg, nil, testLogger()),
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
		logger:    testLogger(),
	}
	return h, mux, comments
}

// serveSchemaConfigForDatabase registers the GitHub content routes config
// discovery needs so the PR resolves to a schemabot.yaml for the given
// database.
func serveSchemaConfigForDatabase(t *testing.T, mux *http.ServeMux, database string) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"head": map[string]any{"sha": "abc123", "ref": "feature-branch"},
			"base": map[string]any{"sha": "def456", "ref": "main"},
			"user": map[string]any{"login": "testuser"},
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode([]map[string]string{{
			"filename": "schemabot.yaml",
			"status":   "modified",
		}}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schemabot.yaml", func(w http.ResponseWriter, _ *http.Request) {
		content := "database: " + database + "\ntype: mysql\n"
		require.NoError(t, json.NewEncoder(w).Encode(map[string]string{
			"type":     "file",
			"encoding": "base64",
			"content":  base64.StdEncoding.EncodeToString([]byte(content)),
		}))
	})
}

// On an aggregate repo, an unscoped `apply -e <env>` fans out to every
// installed deployment. A deployment whose databases registry has no entry for
// the database discovered from the PR's schemabot.yaml is not the owner under
// the aggregate contract, so it stays silent instead of posting a "database is
// not configured on this server" failure comment. A -t-scoped command and a
// non-aggregate repo still surface the error.
func TestUnscopedApplyOnUnregisteredDatabaseStaysSilent(t *testing.T) {
	t.Run("unscoped apply on aggregate repo stays silent", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())
		serveSchemaConfigForDatabase(t, mux, "orders")

		h.handleApplyCommand("octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.Apply})

		assert.Empty(t, comments, "non-owning deployment must not post a comment for an unscoped fan-out apply")
	})

	t.Run("tenant-scoped apply still reports the error", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())
		serveSchemaConfigForDatabase(t, mux, "orders")

		h.handleApplyCommand("octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.Apply, Tenant: "tenant-b"})

		body := requireComment(t, comments, "database-not-configured apply error")
		assert.Contains(t, body, `database "orders" is not configured on this server`)
	})

	t.Run("non-aggregate repo still reports the error", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, nonAggregateConfig())
		serveSchemaConfigForDatabase(t, mux, "orders")

		h.handleApplyCommand("octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.Apply})

		body := requireComment(t, comments, "database-not-configured apply error")
		assert.Contains(t, body, `database "orders" is not configured on this server`)
	})
}

// On an aggregate repo, an unscoped `rollback <apply-id> -e <env>` fans out to
// every deployment, but the apply lives in exactly one tenant's storage. A
// deployment whose storage has no such apply stays silent so only the owning
// deployment answers — it must not post "Apply Not Found" for another tenant's
// apply. A -t-scoped rollback and a non-aggregate repo still get the comment.
func TestUnscopedRollbackUnknownApplyStaysSilentOnAggregateRepo(t *testing.T) {
	rollbackResult := func(tenant string) CommandResult {
		return CommandResult{Action: action.Rollback, ApplyID: "apply_a1b2c3", Environment: "staging", Tenant: tenant}
	}

	t.Run("unscoped rollback on aggregate repo stays silent", func(t *testing.T) {
		h, _, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())

		h.handleRollbackCommand("octocat/hello-world", 1, 12345, "hubot", rollbackResult(""))

		assert.Empty(t, comments, "deployment without the apply must not post Apply Not Found on an unscoped fan-out rollback")
	})

	t.Run("tenant-scoped rollback still posts apply not found", func(t *testing.T) {
		h, _, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())

		h.handleRollbackCommand("octocat/hello-world", 1, 12345, "hubot", rollbackResult("tenant-b"))

		body := requireComment(t, comments, "apply-not-found rollback comment")
		assert.Contains(t, body, "Apply Not Found")
		assert.Contains(t, body, "`apply_a1b2c3`")
	})

	t.Run("non-aggregate repo still posts apply not found", func(t *testing.T) {
		h, _, comments := newFanOutSkipHandler(t, nonAggregateConfig())

		h.handleRollbackCommand("octocat/hello-world", 1, 12345, "hubot", rollbackResult(""))

		body := requireComment(t, comments, "apply-not-found rollback comment")
		assert.Contains(t, body, "Apply Not Found")
		assert.Contains(t, body, "`apply_a1b2c3`")
	})
}

// On an aggregate repo, an unscoped `rollback-confirm -e <env>` fans out to
// every deployment, but only the deployment holding the pinned rollback lock
// has anything to confirm. A deployment with no pending rollback stays silent
// so only the owning deployment answers — it must not post "No Lock Found" for
// another tenant's rollback. A -t-scoped rollback-confirm and a non-aggregate
// repo still get the comment.
func TestUnscopedRollbackConfirmNoLockStaysSilentOnAggregateRepo(t *testing.T) {
	t.Run("unscoped rollback-confirm on aggregate repo stays silent", func(t *testing.T) {
		h, _, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())

		h.handleRollbackConfirmCommand("octocat/hello-world", 1, "staging", 12345, "hubot", CommandResult{Action: action.RollbackConfirm})

		assert.Empty(t, comments, "deployment without a pending rollback must not post No Lock Found on an unscoped fan-out rollback-confirm")
	})

	t.Run("tenant-scoped rollback-confirm still posts no lock found", func(t *testing.T) {
		h, _, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())

		h.handleRollbackConfirmCommand("octocat/hello-world", 1, "staging", 12345, "hubot", CommandResult{Action: action.RollbackConfirm, Tenant: "tenant-b"})

		body := requireComment(t, comments, "no-lock rollback-confirm comment")
		assert.Contains(t, body, "No Lock Found")
		assert.Contains(t, body, "`staging`")
	})

	t.Run("non-aggregate repo still posts no lock found", func(t *testing.T) {
		h, _, comments := newFanOutSkipHandler(t, nonAggregateConfig())

		h.handleRollbackConfirmCommand("octocat/hello-world", 1, "staging", 12345, "hubot", CommandResult{Action: action.RollbackConfirm})

		body := requireComment(t, comments, "no-lock rollback-confirm comment")
		assert.Contains(t, body, "No Lock Found")
		assert.Contains(t, body, "`staging`")
	})
}
