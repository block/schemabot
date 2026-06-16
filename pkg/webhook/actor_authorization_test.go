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
	"github.com/block/schemabot/pkg/state"
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
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))
	installClient := ghclient.NewInstallationClient(client, testLogger())
	cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
		cfg.PRCommandAuthorization.AdminUsers = []string{"hubot"}
	})
	h := actorAuthTestHandler(cfg, installClient)

	blocked := h.enforcePRCommandActorAuthorization(t.Context(), installClient, "octocat/hello-world", 1, 12345, "mona", "orders", storage.DatabaseTypeMySQL, "staging", action.Apply)
	assert.True(t, blocked)

	body := requireComment(t, comments, "authorization comment")
	assert.Contains(t, body, "SchemaBot Command Not Authorized")
	assert.Contains(t, body, "@mona is not authorized")
	assert.Contains(t, body, "`schemabot apply`")
}

// A mutating PR command targeting a database that is not configured on this
// instance fails closed, but the comment distinguishes the missing-database
// case from a plain access denial so operators do not retry assuming an
// authorization problem.
func TestEnforcePRCommandActorAuthorizationUnconfiguredDatabaseComment(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 2)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))
	installClient := ghclient.NewInstallationClient(client, testLogger())
	cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
		cfg.PRCommandAuthorization.AdminUsers = []string{"hubot"}
	})
	h := actorAuthTestHandler(cfg, installClient)

	blocked := h.enforcePRCommandActorAuthorization(t.Context(), installClient, "octocat/hello-world", 1, 12345, "mona", "payments", storage.DatabaseTypeMySQL, "", action.Unlock)
	assert.True(t, blocked)

	body := requireComment(t, comments, "unconfigured-database authorization comment")
	assert.Contains(t, body, "database `payments` is not configured on this SchemaBot instance")
	assert.NotContains(t, body, "is not authorized")
}

func TestEnforcePRCommandActorAuthorizationTeamLookupErrorComment(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 2)
	mux.HandleFunc("GET /orgs/octocat/teams/schema-admins/members", teamMembersHandler(t, http.StatusForbidden))
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))
	installClient := ghclient.NewInstallationClient(client, testLogger())
	cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
		cfg.PRCommandAuthorization.AdminTeams = []string{"octocat/schema-admins"}
	})
	h := actorAuthTestHandler(cfg, installClient)

	blocked := h.enforcePRCommandActorAuthorization(t.Context(), installClient, "octocat/hello-world", 1, 12345, "mona", "orders", storage.DatabaseTypeMySQL, "staging", action.Apply)
	assert.True(t, blocked)

	body := requireComment(t, comments, "authorization failure comment")
	assert.Contains(t, body, "SchemaBot Authorization Check Failed")
	assert.Contains(t, body, "could not verify authorization")
	assert.Contains(t, body, "No schema change was started")
	assert.Contains(t, body, "GitHub App can read organization members")
	assert.NotContains(t, body, "is not authorized")
}

// TestHandleApplyCommandBlocksUnauthorizedActorBeforePlanning exercises the
// PR apply flow through schema discovery and actor authorization. An actor who
// is not a configured admin/operator should receive a denial comment before
// SchemaBot reconciles checks, creates a plan, or acquires a lock.
func TestHandleApplyCommandBlocksUnauthorizedActorBeforePlanning(t *testing.T) {
	client, mux := setupGitHubServer(t)
	registerApplyDiscoveryEndpoints(t, mux, "orders")

	comments := make(chan string, 2)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))

	installClient := ghclient.NewInstallationClient(client, testLogger())
	cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
		cfg.PRCommandAuthorization.AdminUsers = []string{"hubot"}
	})
	h := actorAuthTestHandler(cfg, installClient)

	h.handleApplyCommand("octocat/hello-world", 1, "staging", "", 12345, "mona", CommandResult{Action: action.Apply})

	body := requireComment(t, comments, "unauthorized apply comment")
	assert.Contains(t, body, "SchemaBot Command Not Authorized")
	assert.Contains(t, body, "@mona is not authorized")
}

// Apply-scoped control commands are mutating PR comments, so the full webhook
// path uses the same configured admin/operator authorization as apply before
// recording durable operator intent.
func TestWebhookApplyScopedControlCommandBlocksUnauthorizedActor(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 2)
	reactions := make(chan string, 2)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))
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
	service := api.New(&actorAuthStorage{apply: apply, locks: &actorAuthLockStore{}}, cfg, nil, testLogger())
	h := &Handler{
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}),
		logger:    testLogger(),
	}

	for _, command := range []string{action.Stop, action.Start} {
		t.Run(command, func(t *testing.T) {
			req := buildWebhookRequest(t, webhookPayloadOpts{
				comment:   "schemabot " + command + " " + apply.ApplyIdentifier + " -e staging",
				userLogin: "mona",
				isPR:      true,
			}, nil)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			require.Equal(t, http.StatusOK, rr.Code)
			assert.Contains(t, rr.Body.String(), command+" started")

			select {
			case reaction := <-reactions:
				assert.Equal(t, "eyes", reaction)
			case <-time.After(2 * time.Second):
				require.FailNow(t, "timed out waiting for "+command+" acknowledgement reaction")
			}

			body := requireComment(t, comments, "unauthorized "+command+" comment")
			assert.Contains(t, body, "SchemaBot Command Not Authorized")
			assert.Contains(t, body, "@mona is not authorized")
			assert.Contains(t, body, "`schemabot "+command+"`")
		})
	}
}

// TestHandleRollbackCommandBlocksUnauthorizedActor exercises the PR rollback
// flow with actor authorization enabled. Rollback executes DDL against the
// target database, so an actor who is not a configured admin/operator receives
// a denial comment before SchemaBot generates a rollback plan or acquires a
// lock. A conflicting lock is seeded so the test also proves the authorization
// gate runs before the lock-conflict probe: an unauthorized actor must not be
// able to learn lock ownership by probing apply IDs.
func TestHandleRollbackCommandBlocksUnauthorizedActor(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 2)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))

	locks := &actorAuthLockStore{locks: []*storage.Lock{{
		DatabaseName: "orders",
		DatabaseType: storage.DatabaseTypeMySQL,
		Owner:        "octocat/other-repo#9",
		Repository:   "octocat/other-repo",
		PullRequest:  9,
	}}}
	cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
		cfg.PRCommandAuthorization.AdminUsers = []string{"hubot"}
	})
	installClient := ghclient.NewInstallationClient(client, testLogger())
	h := actorAuthStorageTestHandler(cfg, &actorAuthStorage{apply: actorAuthRollbackApply(), locks: locks}, installClient)

	h.handleRollbackCommand("octocat/hello-world", 1, 12345, "mona", CommandResult{Action: action.Rollback, ApplyID: "apply_abcd1234", Environment: "staging"})

	body := requireComment(t, comments, "unauthorized rollback comment")
	assert.Contains(t, body, "SchemaBot Command Not Authorized")
	assert.Contains(t, body, "@mona is not authorized")
	assert.Contains(t, body, "`schemabot rollback`")
	assert.NotContains(t, body, "Rollback Blocked", "denied rollback must not leak lock-conflict detail")
	assert.NotContains(t, body, "octocat/other-repo#9", "denied rollback must not reveal the conflicting lock owner")
	assert.Empty(t, locks.acquired, "denied rollback must not acquire a lock")
}

// TestHandleRollbackCommandAllowsAuthorizedActor verifies that a configured
// admin proceeds past the actor authorization gate for rollback: the command
// reaches the lock check and reports the conflicting lock owner.
func TestHandleRollbackCommandAllowsAuthorizedActor(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 2)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))

	locks := &actorAuthLockStore{locks: []*storage.Lock{{
		DatabaseName: "orders",
		DatabaseType: storage.DatabaseTypeMySQL,
		Owner:        "octocat/other-repo#9",
		Repository:   "octocat/other-repo",
		PullRequest:  9,
	}}}
	cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
		cfg.PRCommandAuthorization.AdminUsers = []string{"hubot"}
	})
	installClient := ghclient.NewInstallationClient(client, testLogger())
	h := actorAuthStorageTestHandler(cfg, &actorAuthStorage{apply: actorAuthRollbackApply(), locks: locks}, installClient)

	h.handleRollbackCommand("octocat/hello-world", 1, 12345, "hubot", CommandResult{Action: action.Rollback, ApplyID: "apply_abcd1234", Environment: "staging"})

	body := requireComment(t, comments, "rollback blocked-by-lock comment")
	assert.Contains(t, body, "Rollback Blocked")
	assert.Contains(t, body, "octocat/other-repo#9")
	assert.NotContains(t, body, "is not authorized")
}

// TestHandleRollbackConfirmCommandBlocksUnauthorizedActor exercises the PR
// rollback-confirm flow with actor authorization enabled. Rollback-confirm
// executes DDL with unsafe changes allowed, so an actor who is not a
// configured admin/operator receives a denial comment before SchemaBot reads
// lock state or executes the rollback.
func TestHandleRollbackConfirmCommandBlocksUnauthorizedActor(t *testing.T) {
	client, mux := setupGitHubServer(t)
	registerApplyDiscoveryEndpoints(t, mux, "orders")
	comments := make(chan string, 2)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))

	locks := &actorAuthLockStore{locks: []*storage.Lock{{
		DatabaseName: "orders",
		DatabaseType: storage.DatabaseTypeMySQL,
		Owner:        "octocat/hello-world#1",
		Repository:   "octocat/hello-world",
		PullRequest:  1,
	}}}
	cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
		cfg.PRCommandAuthorization.AdminUsers = []string{"hubot"}
	})
	installClient := ghclient.NewInstallationClient(client, testLogger())
	h := actorAuthStorageTestHandler(cfg, &actorAuthStorage{locks: locks}, installClient)

	h.handleRollbackConfirmCommand("octocat/hello-world", 1, "staging", "", 12345, "mona", CommandResult{Action: action.RollbackConfirm})

	body := requireComment(t, comments, "unauthorized rollback-confirm comment")
	assert.Contains(t, body, "SchemaBot Command Not Authorized")
	assert.Contains(t, body, "@mona is not authorized")
	assert.Contains(t, body, "`schemabot rollback-confirm`")
	assert.Empty(t, locks.released, "denied rollback-confirm must not release the lock")
}

// TestHandleRollbackConfirmCommandAllowsAuthorizedActor verifies that a
// configured admin proceeds past the actor authorization gate for
// rollback-confirm: the command reaches the lock-ownership check and reports
// that no rollback lock is held.
func TestHandleRollbackConfirmCommandAllowsAuthorizedActor(t *testing.T) {
	client, mux := setupGitHubServer(t)
	registerApplyDiscoveryEndpoints(t, mux, "orders")
	comments := make(chan string, 2)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))

	cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
		cfg.PRCommandAuthorization.AdminUsers = []string{"hubot"}
	})
	installClient := ghclient.NewInstallationClient(client, testLogger())
	h := actorAuthStorageTestHandler(cfg, &actorAuthStorage{locks: &actorAuthLockStore{}}, installClient)

	h.handleRollbackConfirmCommand("octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.RollbackConfirm})

	body := requireComment(t, comments, "rollback-confirm no-lock comment")
	assert.Contains(t, body, "No Lock Found")
	assert.Contains(t, body, "`orders`")
	assert.NotContains(t, body, "is not authorized")
}

// TestHandleUnlockCommandBlocksUnauthorizedActor exercises the PR force-unlock
// flow with actor authorization enabled. Unlock releases lock state — with
// --force it can clear locks owned by CLI sessions — so an actor who is not a
// configured admin/operator receives a denial comment and every lock stays
// held.
func TestHandleUnlockCommandBlocksUnauthorizedActor(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 2)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))

	locks := &actorAuthLockStore{locks: []*storage.Lock{{
		DatabaseName: "orders",
		DatabaseType: storage.DatabaseTypeMySQL,
		Owner:        "cli:dev@workstation",
	}}}
	cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
		cfg.PRCommandAuthorization.AdminUsers = []string{"hubot"}
	})
	installClient := ghclient.NewInstallationClient(client, testLogger())
	h := actorAuthStorageTestHandler(cfg, &actorAuthStorage{locks: locks}, installClient)

	h.handleUnlockCommand("octocat/hello-world", 1, 12345, "mona", CommandResult{Action: action.Unlock, Force: true, Database: "orders"})

	body := requireComment(t, comments, "unauthorized unlock comment")
	assert.Contains(t, body, "SchemaBot Command Not Authorized")
	assert.Contains(t, body, "@mona is not authorized")
	assert.Contains(t, body, "`schemabot unlock`")
	assert.Empty(t, locks.forceReleased, "denied unlock must not force-release any lock")
	assert.Empty(t, locks.released, "denied unlock must not release any lock")
}

// TestHandleUnlockCommandAllowsAuthorizedActor verifies that a configured
// admin proceeds past the actor authorization gate for force unlock: the
// CLI-owned lock is force-released and a release comment is posted.
func TestHandleUnlockCommandAllowsAuthorizedActor(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 2)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))

	locks := &actorAuthLockStore{locks: []*storage.Lock{{
		DatabaseName: "orders",
		DatabaseType: storage.DatabaseTypeMySQL,
		Owner:        "cli:dev@workstation",
	}}}
	cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
		cfg.PRCommandAuthorization.AdminUsers = []string{"hubot"}
	})
	installClient := ghclient.NewInstallationClient(client, testLogger())
	h := actorAuthStorageTestHandler(cfg, &actorAuthStorage{locks: locks}, installClient)

	h.handleUnlockCommand("octocat/hello-world", 1, 12345, "hubot", CommandResult{Action: action.Unlock, Force: true, Database: "orders"})

	body := requireComment(t, comments, "unlock success comment")
	assert.Contains(t, body, "Lock Released")
	assert.Contains(t, body, "@hubot")
	assert.Equal(t, []string{"orders"}, locks.forceReleased)
}

// TestHandleUnlockCommandUnconfiguredDatabaseHint exercises the PR force-unlock
// flow against a database that is not configured on this SchemaBot instance.
// The actor authorization gate fails closed and no lock is released, but the
// operator-facing comment differentiates the missing-database case from a plain
// access denial so the operator does not assume an authorization problem.
func TestHandleUnlockCommandUnconfiguredDatabaseHint(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 2)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))

	locks := &actorAuthLockStore{locks: []*storage.Lock{{
		DatabaseName: "payments",
		DatabaseType: storage.DatabaseTypeMySQL,
		Owner:        "cli:dev@workstation",
	}}}
	cfg := actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
		cfg.PRCommandAuthorization.AdminUsers = []string{"hubot"}
	})
	installClient := ghclient.NewInstallationClient(client, testLogger())
	h := actorAuthStorageTestHandler(cfg, &actorAuthStorage{locks: locks}, installClient)

	h.handleUnlockCommand("octocat/hello-world", 1, 12345, "hubot", CommandResult{Action: action.Unlock, Force: true, Database: "payments"})

	body := requireComment(t, comments, "unconfigured-database unlock comment")
	assert.Contains(t, body, "database `payments` is not configured on this SchemaBot instance")
	assert.NotContains(t, body, "is not authorized", "unconfigured database must not render a plain access denial")
	assert.Empty(t, locks.forceReleased, "unconfigured database unlock must not force-release any lock")
	assert.Empty(t, locks.released, "unconfigured database unlock must not release any lock")
}

func actorAuthRollbackApply() *storage.Apply {
	completedAt := time.Now()
	return &storage.Apply{
		ID:              42,
		PlanID:          24,
		ApplyIdentifier: "apply_abcd1234",
		Database:        "orders",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		Environment:     "staging",
		Deployment:      "orders",
		State:           state.Apply.Completed,
		CompletedAt:     &completedAt,
	}
}

// actorAuthStorage backs actor authorization handler tests with a stored apply
// and a lock store that records mutations, so tests can assert that denied
// commands have no lock side effects.
type actorAuthStorage struct {
	emptyStorage
	apply *storage.Apply
	locks *actorAuthLockStore
}

func (s *actorAuthStorage) Applies() storage.ApplyStore {
	return &actorAuthApplyStore{apply: s.apply}
}

func (s *actorAuthStorage) Locks() storage.LockStore {
	return s.locks
}

func (s *actorAuthStorage) Plans() storage.PlanStore {
	return &actorAuthPlanStore{apply: s.apply}
}

func (s *actorAuthStorage) Tasks() storage.TaskStore {
	return &actorAuthTaskStore{apply: s.apply}
}

type actorAuthApplyStore struct {
	storage.ApplyStore
	apply *storage.Apply
}

func (s *actorAuthApplyStore) GetByApplyIdentifier(_ context.Context, applyIdentifier string) (*storage.Apply, error) {
	if s.apply == nil || s.apply.ApplyIdentifier != applyIdentifier {
		return nil, nil
	}
	return s.apply, nil
}

func (s *actorAuthApplyStore) GetByDatabase(_ context.Context, _, _, _ string) ([]*storage.Apply, error) {
	return nil, nil
}

type actorAuthPlanStore struct {
	storage.PlanStore
	apply *storage.Apply
}

func (s *actorAuthPlanStore) GetByID(_ context.Context, id int64) (*storage.Plan, error) {
	if s.apply == nil || s.apply.PlanID != id {
		return nil, nil
	}
	return &storage.Plan{
		ID:           s.apply.PlanID,
		Database:     s.apply.Database,
		DatabaseType: s.apply.DatabaseType,
		Environment:  s.apply.Environment,
		Namespaces: map[string]*storage.NamespacePlanData{
			s.apply.Database: {
				OriginalFiles:         map[string]string{"users.sql": "CREATE TABLE `users` (`id` bigint unsigned NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"},
				OriginalFilesCaptured: true,
			},
		},
	}, nil
}

type actorAuthTaskStore struct {
	storage.TaskStore
	apply *storage.Apply
}

func (s *actorAuthTaskStore) GetByDatabase(_ context.Context, database string) ([]*storage.Task, error) {
	if s.apply == nil || s.apply.Database != database {
		return nil, nil
	}
	return []*storage.Task{{
		ApplyID:      s.apply.ID,
		PlanID:       s.apply.PlanID,
		Database:     s.apply.Database,
		DatabaseType: s.apply.DatabaseType,
		Repository:   s.apply.Repository,
		PullRequest:  s.apply.PullRequest,
		Environment:  s.apply.Environment,
		State:        state.Task.Completed,
		CompletedAt:  s.apply.CompletedAt,
		CreatedAt:    s.apply.CreatedAt,
	}}, nil
}

// actorAuthLockStore serves locks from a fixed set and records every lock
// mutation so tests can assert which releases and acquisitions happened.
// Setting releaseErr makes every Release call fail with that error, simulating
// a storage outage during lock release.
type actorAuthLockStore struct {
	storage.LockStore
	locks         []*storage.Lock
	acquired      []*storage.Lock
	released      []string
	forceReleased []string
	releaseErr    error
}

func (s *actorAuthLockStore) Get(_ context.Context, database, dbType string) (*storage.Lock, error) {
	for _, lock := range s.locks {
		if lock.DatabaseName == database && lock.DatabaseType == dbType {
			return lock, nil
		}
	}
	return nil, nil
}

func (s *actorAuthLockStore) GetByPR(_ context.Context, repo string, pr int) ([]*storage.Lock, error) {
	var matches []*storage.Lock
	for _, lock := range s.locks {
		if lock.Repository == repo && lock.PullRequest == pr {
			matches = append(matches, lock)
		}
	}
	return matches, nil
}

func (s *actorAuthLockStore) List(_ context.Context) ([]*storage.Lock, error) {
	return s.locks, nil
}

func (s *actorAuthLockStore) Acquire(_ context.Context, lock *storage.Lock) error {
	s.acquired = append(s.acquired, lock)
	return nil
}

func (s *actorAuthLockStore) Release(_ context.Context, database, _, _ string) error {
	if s.releaseErr != nil {
		return s.releaseErr
	}
	s.released = append(s.released, database)
	return nil
}

func (s *actorAuthLockStore) ForceRelease(_ context.Context, database, _ string) error {
	s.forceReleased = append(s.forceReleased, database)
	return nil
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
	return actorAuthStorageTestHandler(cfg, nil, installClient)
}

func actorAuthStorageTestHandler(cfg *api.ServerConfig, store storage.Storage, installClient *ghclient.InstallationClient) *Handler {
	service := api.New(store, cfg, nil, testLogger())
	return &Handler{
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
		logger:    testLogger(),
	}
}

// commentRecorder returns a mux handler that captures posted PR comment bodies
// on the given channel and responds like the GitHub create-comment API.
func commentRecorder(t *testing.T, comments chan<- string) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 99}))
	}
}

// requireComment waits for the next captured PR comment and fails the test if
// none arrives before the deadline.
func requireComment(t *testing.T, comments <-chan string, failureContext string) string {
	t.Helper()
	select {
	case body := <-comments:
		return body
	case <-time.After(2 * time.Second):
		require.FailNow(t, "timed out waiting for comment", failureContext)
		return ""
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

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", database)
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
