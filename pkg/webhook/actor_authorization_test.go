package webhook

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
)

func TestAuthorizePRCommandActor(t *testing.T) {
	tests := []struct {
		name       string
		config     *api.ServerConfig
		actor      string
		database   string
		wantAllow  bool
		wantReason string
		wantMatch  string
	}{
		{
			name:       "disabled authorization allows without checking principals",
			config:     actorAuthTestConfig(false),
			actor:      "mona",
			database:   "orders",
			wantAllow:  true,
			wantReason: api.ActorAuthReasonDisabled,
		},
		{
			name: "admin user allows any configured database",
			config: actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
				cfg.PRCommandAuthorization.AdminUsers = []string{"mona"}
			}),
			actor:      "Mona",
			database:   "orders",
			wantAllow:  true,
			wantReason: api.ActorAuthReasonAllowedAdminUser,
			wantMatch:  "mona",
		},
		{
			name: "database operator user allows that database",
			config: actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
				db := cfg.Databases["orders"]
				db.OperatorUsers = []string{"mona"}
				cfg.Databases["orders"] = db
			}),
			actor:      "mona",
			database:   "orders",
			wantAllow:  true,
			wantReason: api.ActorAuthReasonAllowedOperatorUser,
			wantMatch:  "mona",
		},
		{
			name: "missing actor denies",
			config: actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
				cfg.PRCommandAuthorization.AdminUsers = []string{"mona"}
			}),
			database:   "orders",
			wantReason: api.ActorAuthReasonMissingActor,
		},
		{
			name: "missing database config denies",
			config: actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
				cfg.PRCommandAuthorization.AdminUsers = []string{"mona"}
			}),
			actor:      "mona",
			database:   "unknown",
			wantReason: api.ActorAuthReasonMissingDatabaseConfig,
		},
		{
			name:       "enabled with no principals denies",
			config:     actorAuthTestConfig(true),
			actor:      "mona",
			database:   "orders",
			wantReason: api.ActorAuthReasonNoConfiguredPrincipal,
		},
		{
			name: "users only denial does not need GitHub",
			config: actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
				cfg.PRCommandAuthorization.AdminUsers = []string{"hubot"}
			}),
			actor:      "mona",
			database:   "orders",
			wantReason: api.ActorAuthReasonNotAuthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := actorAuthTestHandler(tt.config, nil)
			result, err := h.authorizePRCommandActor(t.Context(), nil, tt.actor, tt.database)
			require.NoError(t, err)
			assert.Equal(t, tt.wantAllow, result.Allowed)
			assert.Equal(t, tt.wantReason, result.Reason)
			assert.Equal(t, tt.wantMatch, result.MatchedPrincipal)
		})
	}
}

func TestAuthorizePRCommandActorTeams(t *testing.T) {
	tests := []struct {
		name                  string
		configure             func(*api.ServerConfig)
		teamMembersStatusCode int
		teamMembers           []string
		wantAllow             bool
		wantReason            string
		wantMatch             string
		wantErr               bool
	}{
		{
			name: "admin team allows",
			configure: func(cfg *api.ServerConfig) {
				cfg.PRCommandAuthorization.AdminTeams = []string{"octocat/schema-admins"}
			},
			teamMembers: []string{"mona"},
			wantAllow:   true,
			wantReason:  api.ActorAuthReasonAllowedAdminTeam,
			wantMatch:   "octocat/schema-admins",
		},
		{
			name: "admin team takes precedence over admin user",
			configure: func(cfg *api.ServerConfig) {
				cfg.PRCommandAuthorization.AdminTeams = []string{"octocat/schema-admins"}
				cfg.PRCommandAuthorization.AdminUsers = []string{"mona"}
			},
			teamMembers: []string{"mona"},
			wantAllow:   true,
			wantReason:  api.ActorAuthReasonAllowedAdminTeam,
			wantMatch:   "octocat/schema-admins",
		},
		{
			name: "admin user allows after admin team non-member",
			configure: func(cfg *api.ServerConfig) {
				cfg.PRCommandAuthorization.AdminTeams = []string{"octocat/schema-admins"}
				cfg.PRCommandAuthorization.AdminUsers = []string{"mona"}
			},
			teamMembers: []string{"hubot"},
			wantAllow:   true,
			wantReason:  api.ActorAuthReasonAllowedAdminUser,
			wantMatch:   "mona",
		},
		{
			name: "operator team allows",
			configure: func(cfg *api.ServerConfig) {
				db := cfg.Databases["orders"]
				db.OperatorTeams = []string{"octocat/orders-operators"}
				cfg.Databases["orders"] = db
			},
			teamMembers: []string{"mona"},
			wantAllow:   true,
			wantReason:  api.ActorAuthReasonAllowedOperatorTeam,
			wantMatch:   "octocat/orders-operators",
		},
		{
			name: "not a member denies",
			configure: func(cfg *api.ServerConfig) {
				cfg.PRCommandAuthorization.AdminTeams = []string{"octocat/schema-admins"}
			},
			teamMembers: []string{"hubot"},
			wantReason:  api.ActorAuthReasonNotAuthorized,
		},
		{
			name: "admin team membership visibility error fails closed",
			configure: func(cfg *api.ServerConfig) {
				cfg.PRCommandAuthorization.AdminTeams = []string{"octocat/schema-admins"}
			},
			teamMembersStatusCode: http.StatusForbidden,
			wantReason:            api.ActorAuthReasonGitHubError,
			wantErr:               true,
		},
		{
			name: "GitHub error fails closed",
			configure: func(cfg *api.ServerConfig) {
				cfg.PRCommandAuthorization.AdminTeams = []string{"octocat/schema-admins"}
			},
			teamMembersStatusCode: http.StatusInternalServerError,
			wantReason:            api.ActorAuthReasonGitHubError,
			wantErr:               true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux := setupGitHubServer(t)
			teamMembersStatusCode := tt.teamMembersStatusCode
			if teamMembersStatusCode == 0 {
				teamMembersStatusCode = http.StatusOK
			}
			mux.HandleFunc("GET /orgs/octocat/teams/schema-admins/members", teamMembersHandler(t, teamMembersStatusCode, tt.teamMembers...))
			mux.HandleFunc("GET /orgs/octocat/teams/orders-operators/members", teamMembersHandler(t, teamMembersStatusCode, tt.teamMembers...))
			installClient := ghclient.NewInstallationClient(client, testLogger())

			cfg := actorAuthTestConfig(true, tt.configure)
			h := actorAuthTestHandler(cfg, installClient)
			result, err := h.authorizePRCommandActor(t.Context(), installClient, "mona", "orders")
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantAllow, result.Allowed)
			assert.Equal(t, tt.wantReason, result.Reason)
			assert.Equal(t, tt.wantMatch, result.MatchedPrincipal)
		})
	}
}

func TestAuthorizePRCommandActorUsersOnlyDoesNotCallGitHub(t *testing.T) {
	client, mux := setupGitHubServer(t)
	var teamCalls atomic.Int32
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		teamCalls.Add(1)
		http.NotFound(w, r)
	})
	installClient := ghclient.NewInstallationClient(client, testLogger())

	cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
		cfg.PRCommandAuthorization.AdminUsers = []string{"hubot"}
	})
	h := actorAuthTestHandler(cfg, installClient)

	result, err := h.authorizePRCommandActor(t.Context(), installClient, "mona", "orders")
	require.NoError(t, err)
	assert.False(t, result.Allowed)
	assert.Equal(t, api.ActorAuthReasonNotAuthorized, result.Reason)
	assert.Equal(t, int32(0), teamCalls.Load(), "users-only config should not query GitHub team membership")
}

func TestAuthorizePRCommandActorMissingServiceFailsClosed(t *testing.T) {
	h := &Handler{logger: testLogger()}
	result, err := h.authorizePRCommandActor(t.Context(), nil, "mona", "orders")
	require.Error(t, err)
	assert.False(t, result.Allowed)
	assert.Equal(t, api.ActorAuthReasonMissingServerConfig, result.Reason)
}

func TestEnforcePRCommandActorAuthorizationComments(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 2)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 99}))
	})
	installClient := ghclient.NewInstallationClient(client, testLogger())
	cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
		cfg.PRCommandAuthorization.AdminUsers = []string{"hubot"}
	})
	h := actorAuthTestHandler(cfg, installClient)

	blocked := h.enforcePRCommandActorAuthorization(t.Context(), installClient, "octocat/hello-world", 1, 12345, "mona", "orders", storage.DatabaseTypeMySQL, "staging", action.Apply)
	assert.True(t, blocked)

	select {
	case body := <-comments:
		assert.Contains(t, body, "SchemaBot Command Not Authorized")
		assert.Contains(t, body, "@mona is not authorized")
		assert.Contains(t, body, "`schemabot apply`")
	case <-time.After(2 * time.Second):
		require.FailNow(t, "timed out waiting for authorization comment")
	}
}

func TestEnforcePRCommandActorAuthorizationTeamLookupErrorComment(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 2)
	mux.HandleFunc("GET /orgs/octocat/teams/schema-admins/members", teamMembersHandler(t, http.StatusForbidden))
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 99}))
	})
	installClient := ghclient.NewInstallationClient(client, testLogger())
	cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
		cfg.PRCommandAuthorization.AdminTeams = []string{"octocat/schema-admins"}
	})
	h := actorAuthTestHandler(cfg, installClient)

	blocked := h.enforcePRCommandActorAuthorization(t.Context(), installClient, "octocat/hello-world", 1, 12345, "mona", "orders", storage.DatabaseTypeMySQL, "staging", action.Apply)
	assert.True(t, blocked)

	select {
	case body := <-comments:
		assert.Contains(t, body, "SchemaBot Authorization Check Failed")
		assert.Contains(t, body, "could not verify authorization")
		assert.Contains(t, body, "No schema change was started")
		assert.Contains(t, body, "GitHub App can read organization members")
		assert.NotContains(t, body, "is not authorized")
	case <-time.After(2 * time.Second):
		require.FailNow(t, "timed out waiting for authorization failure comment")
	}
}

// TestHandleApplyCommandBlocksUnauthorizedActorBeforePlanning exercises the
// PR apply flow through schema discovery and actor authorization. An actor who
// is not a configured admin/operator should receive a denial comment before
// SchemaBot reconciles checks, creates a plan, or acquires a lock.
func TestHandleApplyCommandBlocksUnauthorizedActorBeforePlanning(t *testing.T) {
	client, mux := setupGitHubServer(t)
	registerApplyDiscoveryEndpoints(t, mux, "orders")

	comments := make(chan string, 2)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 99}))
	})

	installClient := ghclient.NewInstallationClient(client, testLogger())
	cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
		cfg.PRCommandAuthorization.AdminUsers = []string{"hubot"}
	})
	h := actorAuthTestHandler(cfg, installClient)

	h.handleApplyCommand("octocat/hello-world", 1, "staging", "", 12345, "mona", CommandResult{Action: action.Apply})

	select {
	case body := <-comments:
		assert.Contains(t, body, "SchemaBot Command Not Authorized")
		assert.Contains(t, body, "@mona is not authorized")
	case <-time.After(2 * time.Second):
		require.FailNow(t, "timed out waiting for unauthorized apply comment")
	}
}

// Stop is a mutating PR comment command, so the full webhook path uses the same
// configured admin/operator authorization as apply before recording durable stop intent.
func TestWebhookStopCommandBlocksUnauthorizedActor(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 2)
	reactions := make(chan string, 2)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 99}))
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Content string `json:"content"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		reactions <- body.Content
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 1}))
	})

	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply_abcd1234",
		Database:        "orders",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		Environment:     "staging",
		Deployment:      "orders",
	}
	cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
		cfg.PRCommandAuthorization.AdminUsers = []string{"hubot"}
	})
	service := api.New(&stopActorAuthStorage{apply: apply}, cfg, nil, testLogger())
	h := &Handler{
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}),
		logger:    testLogger(),
	}

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment:   "schemabot stop " + apply.ApplyIdentifier + " -e staging",
		userLogin: "mona",
		isPR:      true,
	}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "stop started")

	select {
	case reaction := <-reactions:
		assert.Equal(t, "eyes", reaction)
	case <-time.After(2 * time.Second):
		require.FailNow(t, "timed out waiting for stop acknowledgement reaction")
	}

	select {
	case body := <-comments:
		assert.Contains(t, body, "SchemaBot Command Not Authorized")
		assert.Contains(t, body, "@mona is not authorized")
		assert.Contains(t, body, "`schemabot stop`")
	case <-time.After(2 * time.Second):
		require.FailNow(t, "timed out waiting for unauthorized stop comment")
	}
}

type stopActorAuthStorage struct {
	emptyStorage
	apply *storage.Apply
}

func (s *stopActorAuthStorage) Applies() storage.ApplyStore {
	return &stopActorAuthApplyStore{apply: s.apply}
}

type stopActorAuthApplyStore struct {
	storage.ApplyStore
	apply *storage.Apply
}

func (s *stopActorAuthApplyStore) GetByApplyIdentifier(_ context.Context, applyIdentifier string) (*storage.Apply, error) {
	if s.apply == nil || s.apply.ApplyIdentifier != applyIdentifier {
		return nil, nil
	}
	return s.apply, nil
}

func actorAuthTestConfig(enabled bool, opts ...func(*api.ServerConfig)) *api.ServerConfig {
	cfg := &api.ServerConfig{
		PRCommandAuthorization: api.PRCommandAuthorizationConfig{Enabled: enabled},
		Databases: map[string]api.DatabaseConfig{
			"orders": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]api.EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost)/orders"},
				},
			},
		},
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

func actorAuthTestHandler(cfg *api.ServerConfig, installClient *ghclient.InstallationClient) *Handler {
	service := api.New(nil, cfg, nil, testLogger())
	return &Handler{
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
		logger:    testLogger(),
	}
}

func teamMembersHandler(t *testing.T, statusCode int, members ...string) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(statusCode)
		if statusCode == http.StatusOK {
			users := make([]map[string]string, 0, len(members))
			for _, member := range members {
				users = append(users, map[string]string{"login": member})
			}
			require.NoError(t, json.NewEncoder(w).Encode(users))
		}
		if statusCode >= http.StatusBadRequest {
			_, err := fmt.Fprint(w, http.StatusText(statusCode))
			require.NoError(t, err)
		}
	}
}

func registerApplyDiscoveryEndpoints(t *testing.T, mux *http.ServeMux, database string) {
	t.Helper()

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\nenvironments:\n  - staging\n", database)
	schemaSQL := "CREATE TABLE `users` (`id` bigint unsigned NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`))"

	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"head": map[string]any{"sha": "abc123", "ref": "feature-branch"},
			"base": map[string]any{"sha": "def456", "ref": "main"},
			"user": map[string]any{"login": "testuser"},
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode([]map[string]string{{
			"filename": "schema/" + database + "/users.sql",
			"status":   "added",
		}}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema/"+database+"/schemabot.yaml", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]string{
			"content":  base64.StdEncoding.EncodeToString([]byte(schemabotConfig)),
			"encoding": "base64",
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/abc123", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"tree": []map[string]any{{
				"path": "schema/" + database + "/users.sql",
				"mode": "100644",
				"type": "blob",
				"sha":  "userssha",
				"size": len(schemaSQL),
			}},
			"truncated": false,
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/blobs/userssha", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]string{
			"content":  base64.StdEncoding.EncodeToString([]byte(schemaSQL)),
			"encoding": "base64",
		}))
	})
}
