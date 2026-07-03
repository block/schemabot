package webhook

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"
	"time"

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

	// A participant deployment receives the same fan-out for a PR touching
	// only a database another deployment owns; the skip is role-agnostic, so
	// it stays just as silent as a leader would.
	t.Run("unscoped apply on participant deployment stays silent", func(t *testing.T) {
		cfg := aggregateLeaderConfig()
		repoCfg := cfg.Repos["octocat/hello-world"]
		repoCfg.Aggregate = &api.AggregateConfig{Role: api.AggregateRoleParticipant}
		cfg.Repos["octocat/hello-world"] = repoCfg
		h, mux, comments := newFanOutSkipHandler(t, cfg)
		serveSchemaConfigForDatabase(t, mux, "orders")

		h.handleApplyCommand("octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.Apply})

		assert.Empty(t, comments, "a participant must not post Apply Failed for a database another deployment owns")
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

// registerReactionRecorder captures acknowledgment reactions posted to the
// command comment.
func registerReactionRecorder(t *testing.T, mux *http.ServeMux) chan string {
	t.Helper()
	reactions := make(chan string, 4)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Content string `json:"content"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		reactions <- body.Content
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 1}))
	})
	return reactions
}

// The acknowledgment reaction means "this deployment is acting on your
// command": a deployment that owns the discovered database reacts with eyes,
// while a fan-out deployment that silently skips unowned work leaves only its
// log — so the reaction count on a shared repo reflects the deployments doing
// work, not everyone who heard the command.
func TestCommandAcknowledgmentFollowsOwnership(t *testing.T) {
	t.Run("owning deployment reacts", func(t *testing.T) {
		cfg := aggregateLeaderConfig()
		cfg.Databases = map[string]api.DatabaseConfig{
			"orders": {Environments: map[string]api.EnvironmentConfig{
				"staging": {Deployment: "default", Target: "orders"},
			}},
		}
		h, mux, _ := newFanOutSkipHandler(t, cfg)
		serveSchemaConfigForDatabase(t, mux, "orders")
		// Discovery loads the schema files next to schemabot.yaml: serve the
		// root directory listing and one table file so the command proceeds
		// past discovery to the acknowledgment point.
		mux.HandleFunc("GET /repos/octocat/hello-world/contents/", func(w http.ResponseWriter, _ *http.Request) {
			require.NoError(t, json.NewEncoder(w).Encode([]map[string]any{
				{"type": "file", "name": "schemabot.yaml", "path": "schemabot.yaml"},
				{"type": "dir", "name": "staging", "path": "staging"},
			}))
		})
		mux.HandleFunc("GET /repos/octocat/hello-world/contents/staging", func(w http.ResponseWriter, _ *http.Request) {
			require.NoError(t, json.NewEncoder(w).Encode([]map[string]any{
				{"type": "file", "name": "users.sql", "path": "staging/users.sql"},
			}))
		})
		mux.HandleFunc("GET /repos/octocat/hello-world/contents/staging/users.sql", func(w http.ResponseWriter, _ *http.Request) {
			ddl := "CREATE TABLE `users` (`id` bigint unsigned NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
			require.NoError(t, json.NewEncoder(w).Encode(map[string]string{
				"type": "file", "encoding": "base64",
				"content": base64.StdEncoding.EncodeToString([]byte(ddl)),
			}))
		})
		reactions := registerReactionRecorder(t, mux)

		h.handleApplyCommand("octocat/hello-world", 1, "staging", "", 12345, "hubot",
			CommandResult{Action: action.Apply, CommentID: 42})

		select {
		case content := <-reactions:
			assert.Equal(t, "eyes", content)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for the acknowledgment reaction")
		}
	})

	// The acknowledgment must not wait for schema files to load: ownership is
	// decidable from config discovery alone, so on databases with large schema
	// directories the user still sees the reaction promptly. Failing the file
	// fetch proves the reaction fired before it.
	t.Run("owning deployment acknowledges before schema files load", func(t *testing.T) {
		cfg := aggregateLeaderConfig()
		cfg.Databases = map[string]api.DatabaseConfig{
			"orders": {Environments: map[string]api.EnvironmentConfig{
				"staging": {Deployment: "default", Target: "orders"},
			}},
		}
		h, mux, _ := newFanOutSkipHandler(t, cfg)
		serveSchemaConfigForDatabase(t, mux, "orders")
		mux.HandleFunc("GET /repos/octocat/hello-world/contents/staging", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "schema listing unavailable", http.StatusInternalServerError)
		})
		reactions := registerReactionRecorder(t, mux)

		h.handleApplyCommand("octocat/hello-world", 1, "staging", "", 12345, "hubot",
			CommandResult{Action: action.Apply, CommentID: 42})

		select {
		case content := <-reactions:
			assert.Equal(t, "eyes", content)
		case <-time.After(2 * time.Second):
			t.Fatal("the acknowledgment must fire from config discovery, before schema files load")
		}
	})

	// A bare multi-env plan on a repo without a schema-dir allowlist resolves
	// config discovery successfully even on a deployment that does not own the
	// database — ownership is only decided at the registry lookup. A fan-out
	// participant in that position silently skips, so it must not acknowledge.
	t.Run("multi-env plan on unowned database does not react", func(t *testing.T) {
		h, mux, _ := newFanOutSkipHandler(t, aggregateLeaderConfig())
		serveSchemaConfigForDatabase(t, mux, "orders")
		reactions := registerReactionRecorder(t, mux)

		h.handleMultiEnvPlan("octocat/hello-world", 1, "", "", 12345, "hubot", false, true, 42)

		select {
		case <-reactions:
			t.Fatal("a deployment that cannot resolve the database in its registry must not acknowledge")
		case <-time.After(100 * time.Millisecond):
		}
	})

	t.Run("silently skipping deployment does not react", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())
		serveSchemaConfigForDatabase(t, mux, "orders")
		reactions := registerReactionRecorder(t, mux)

		h.handleApplyCommand("octocat/hello-world", 1, "staging", "", 12345, "hubot",
			CommandResult{Action: action.Apply, CommentID: 42})

		assert.Empty(t, comments, "the silent skip posts nothing")
		select {
		case <-reactions:
			t.Fatal("a silently skipping deployment must not acknowledge the command")
		case <-time.After(100 * time.Millisecond):
		}
	})
}
