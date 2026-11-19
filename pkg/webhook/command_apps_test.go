package webhook

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

const (
	testCommandApp      = "deploy-gate"
	testCommandAppLogin = testCommandApp + "[bot]"
	testOwnAppSlug      = "schemabot-dev"
)

// withCommandApps lists apps in the orders database's command_apps.
func withCommandApps(apps ...string) func(*api.ServerConfig) {
	return func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.CommandApps = apps
		cfg.Databases["orders"] = db
	}
}

// withAllowedRepos limits the orders database to repos.
func withAllowedRepos(repos ...string) func(*api.ServerConfig) {
	return func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.AllowedRepos = repos
		cfg.Databases["orders"] = db
	}
}

// An App's authority to run a PR command is exactly its command_apps entries:
// the database must list it and accept the repo. Turning PR command
// authorization off does not widen it, and human principals never apply to it.
func TestAuthorizePRCommandActorCommandApps(t *testing.T) {
	tests := []struct {
		name       string
		config     *api.ServerConfig
		actor      string
		repo       string
		database   string
		wantAllow  bool
		wantReason string
		wantMatch  string
	}{
		{
			name:       "listed App is allowed",
			config:     actorAuthTestConfig(true, withCommandApps(testCommandApp)),
			actor:      testCommandAppLogin,
			database:   "orders",
			wantAllow:  true,
			wantReason: api.ActorAuthReasonAllowedCommandApp,
			wantMatch:  testCommandAppLogin,
		},
		{
			name:       "listed App matches case-insensitively",
			config:     actorAuthTestConfig(true, withCommandApps("Deploy-Gate")),
			actor:      testCommandAppLogin,
			database:   "orders",
			wantAllow:  true,
			wantReason: api.ActorAuthReasonAllowedCommandApp,
			wantMatch:  testCommandAppLogin,
		},
		{
			name:       "listed App is allowed with authorization disabled",
			config:     actorAuthTestConfig(false, withCommandApps(testCommandApp)),
			actor:      testCommandAppLogin,
			database:   "orders",
			wantAllow:  true,
			wantReason: api.ActorAuthReasonAllowedCommandApp,
			wantMatch:  testCommandAppLogin,
		},
		{
			name:       "unlisted App is denied with authorization disabled",
			config:     actorAuthTestConfig(false),
			actor:      testCommandAppLogin,
			database:   "orders",
			wantReason: api.ActorAuthReasonNotAuthorized,
		},
		{
			name:       "another App is denied",
			config:     actorAuthTestConfig(true, withCommandApps(testCommandApp)),
			actor:      "other-app[bot]",
			database:   "orders",
			wantReason: api.ActorAuthReasonNotAuthorized,
		},
		{
			name: "App listed only for another database is denied",
			config: actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
				cfg.Databases["payments"] = api.DatabaseConfig{
					Type:         storage.DatabaseTypeMySQL,
					Environments: map[string]api.EnvironmentConfig{"staging": {DSN: "root@tcp(localhost)/payments"}},
					CommandApps:  []string{testCommandApp},
				}
			}),
			actor:      testCommandAppLogin,
			database:   "orders",
			wantReason: api.ActorAuthReasonNotAuthorized,
		},
		{
			name:       "App is denied from a repo the database does not accept",
			config:     actorAuthTestConfig(true, withCommandApps(testCommandApp), withAllowedRepos("octocat/schemas")),
			actor:      testCommandAppLogin,
			repo:       "octocat/hello-world",
			database:   "orders",
			wantReason: api.ActorAuthReasonNotAuthorized,
		},
		{
			name:       "App is allowed from a repo the database accepts",
			config:     actorAuthTestConfig(true, withCommandApps(testCommandApp), withAllowedRepos("octocat/schemas")),
			actor:      testCommandAppLogin,
			repo:       "octocat/schemas",
			database:   "orders",
			wantAllow:  true,
			wantReason: api.ActorAuthReasonAllowedCommandApp,
			wantMatch:  testCommandAppLogin,
		},
		{
			name: "App login in operator_users is not authorized",
			config: actorAuthTestConfig(true, func(cfg *api.ServerConfig) {
				cfg.PRCommandAuthorization.AdminUsers = []string{testCommandAppLogin}
				db := cfg.Databases["orders"]
				db.OperatorUsers = []string{testCommandAppLogin}
				cfg.Databases["orders"] = db
			}),
			actor:      testCommandAppLogin,
			database:   "orders",
			wantReason: api.ActorAuthReasonNotAuthorized,
		},
		{
			name:       "App is denied for an unconfigured database",
			config:     actorAuthTestConfig(true, withCommandApps(testCommandApp)),
			actor:      testCommandAppLogin,
			database:   "missing",
			wantReason: api.ActorAuthReasonMissingDatabaseConfig,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := tt.repo
			if repo == "" {
				repo = "octocat/hello-world"
			}
			h := actorAuthTestHandler(tt.config, nil)
			result, err := h.authorizePRCommandActor(t.Context(), nil, tt.actor, repo, tt.database)
			require.NoError(t, err)
			assert.Equal(t, tt.wantAllow, result.Allowed)
			assert.Equal(t, tt.wantReason, result.Reason)
			assert.Equal(t, tt.wantMatch, result.MatchedPrincipal)
		})
	}
}

// A bot comment is parsed only when its App is listed for a database that
// accepts the repo, and never when this deployment cannot rule out that the
// comment is its own.
func TestIsCommandAppComment(t *testing.T) {
	tests := []struct {
		name    string
		config  *api.ServerConfig
		ownSlug string
		login   string
		repo    string
		want    bool
	}{
		{
			name:    "listed App is admitted",
			config:  actorAuthTestConfig(true, withCommandApps(testCommandApp)),
			ownSlug: testOwnAppSlug,
			login:   testCommandAppLogin,
			want:    true,
		},
		{
			name:    "unlisted App is ignored",
			config:  actorAuthTestConfig(true, withCommandApps(testCommandApp)),
			ownSlug: testOwnAppSlug,
			login:   "other-app[bot]",
		},
		{
			name:    "listed App from a repo no database accepts is ignored",
			config:  actorAuthTestConfig(true, withCommandApps(testCommandApp), withAllowedRepos("octocat/schemas")),
			ownSlug: testOwnAppSlug,
			login:   testCommandAppLogin,
			repo:    "octocat/hello-world",
		},
		{
			name:   "unknown own App slug admits nothing",
			config: actorAuthTestConfig(true, withCommandApps(testCommandApp)),
			login:  testCommandAppLogin,
		},
		{
			name:    "own App listed in command_apps is ignored",
			config:  actorAuthTestConfig(true, withCommandApps(testCommandApp)),
			ownSlug: "Deploy-Gate",
			login:   testCommandAppLogin,
		},
		{
			name:    "login without the bot suffix is not an App",
			config:  actorAuthTestConfig(true, withCommandApps(testCommandApp)),
			ownSlug: testOwnAppSlug,
			login:   testCommandApp,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := tt.repo
			if repo == "" {
				repo = "octocat/hello-world"
			}
			client, _ := setupGitHubServer(t)
			h := actorAuthTestHandler(tt.config, ghclient.NewInstallationClientWithSlug(client, testLogger(), tt.ownSlug))
			assert.Equal(t, tt.want, h.isCommandAppComment(repo, 12345, tt.login))
		})
	}
}

// A stored delivery is checked against command_apps like a live one. A
// delivery whose installation is unknown is not admitted, so the bot comment
// is ignored rather than retried; a listed App whose comment cannot be decided
// yet because this deployment's own App slug is unknown is not admitted
// either, but reports that it can be retried.
func TestDurableCommandAppAdmission(t *testing.T) {
	client, _ := setupGitHubServer(t)
	cfg := actorAuthTestConfig(true, withCommandApps(testCommandApp))
	h := actorAuthTestHandler(cfg, ghclient.NewInstallationClientWithSlug(client, testLogger(), testOwnAppSlug))

	var payload webhookPayload
	require.NoError(t, json.Unmarshal(commandAppCommentPayload(t, "schemabot apply -e staging"), &payload))

	event := &storage.WebhookEvent{DeliveryID: "d-1", Event: "issue_comment", Repository: "octocat/hello-world", PullRequest: 7, TenantID: "12345"}
	admitted, err := h.durableCommandAppAdmission(event, &payload)
	require.NoError(t, err)
	assert.True(t, admitted)

	unknown := actorAuthTestHandler(cfg, ghclient.NewInstallationClientWithSlug(client, testLogger(), ""))
	admitted, err = unknown.durableCommandAppAdmission(event, &payload)
	require.ErrorIs(t, err, errCommandAppIdentityUnknown)
	assert.False(t, admitted)

	event.TenantID = ""
	admitted, err = h.durableCommandAppAdmission(event, &payload)
	require.NoError(t, err)
	assert.False(t, admitted)
}

// A command App's queued command survives a driver that starts before it
// knows its own App slug: the delivery stays pending and is retried, rather
// than completing as an ignored bot comment, and once the slug resolves the
// retry admits the comment and runs the command.
func TestDurableCommandAppCommentRetriedUntilOwnAppKnown(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/7/comments", commentRecorder(t, comments))
	event := durableIssueCommentEvent("schemabot unlock")
	event.Payload = commandAppCommentPayload(t, "schemabot unlock")
	store := newScriptedWebhookEventStore(event, event)
	factory := &fakeClientFactory{client: ghclient.NewInstallationClientWithSlug(client, testLogger(), "")}
	h := newDurableDriverHandler(t, store, actorAuthTestConfig(true, withCommandApps(testCommandApp)), factory)

	before := time.Now()
	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")
	select {
	case failure := <-store.failed:
		require.NotNil(t, failure.retryAfter, "a command App delivery must stay retryable while the own App slug is unknown")
		assert.True(t, failure.retryAfter.After(before), "retry must be scheduled in the future")
		assert.Contains(t, failure.errMsg, errCommandAppIdentityUnknown.Error())
	default:
		t.Fatal("expected the delivery to be marked failed for retry")
	}
	require.Empty(t, store.completed, "the delivery must not complete as an ignored bot comment")

	factory.client = ghclient.NewInstallationClientWithSlug(client, testLogger(), testOwnAppSlug)
	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")
	select {
	case <-store.completed:
	case failure := <-store.failed:
		t.Fatalf("the retried delivery must run once the own App slug is known, got failure %q", failure.errMsg)
	default:
		t.Fatal("expected the retried delivery to complete")
	}
	assert.Contains(t, requireComment(t, comments, "unlock reply to the command App"), "No Locks Found")
}

// A listed App's comment reaches the command parser: `schemabot help` gets
// SchemaBot's help reply, where any other bot's comment is ignored.
func TestWebhookCommandAppCommentIsParsed(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, err := w.Write([]byte(`{"id": 1}`))
		assert.NoError(t, err)
	})
	installClient := ghclient.NewInstallationClientWithSlug(client, testLogger(), testOwnAppSlug)
	h := actorAuthStorageTestHandler(actorAuthTestConfig(true, withCommandApps(testCommandApp)), &emptyStorage{}, installClient)
	drainWebhookWorkOnCleanup(t, h)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment:   "schemabot help",
		userType:  "Bot",
		userLogin: testCommandAppLogin,
		isPR:      true,
	}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.NotContains(t, rr.Body.String(), "event ignored")
	body := requireComment(t, comments, "help reply to a command App")
	assert.Contains(t, body, "schemabot plan")
}

// withPaymentsDatabase adds a payments database that lists apps in its
// command_apps.
func withPaymentsDatabase(apps ...string) func(*api.ServerConfig) {
	return func(cfg *api.ServerConfig) {
		cfg.Databases["payments"] = api.DatabaseConfig{
			Type: storage.DatabaseTypeMySQL,
			Environments: map[string]api.EnvironmentConfig{
				"staging": {DSN: "root@tcp(localhost)/payments"},
			},
			CommandApps: apps,
		}
	}
}

// A command App may run commands, but its approval never satisfies the review
// gate, even when its login is also configured as a reviewer, and even for a
// database that did not list it. It is not offered as a reviewer to ask, and
// the gate reports it apart from unauthorized reviewers, since no team or
// reviewer setting can make its approval count.
func TestCheckReviewGate_CommandAppApprovalNeverCounts(t *testing.T) {
	tests := []struct {
		name   string
		listed func(*api.ServerConfig)
	}{
		{name: "listed by the gated database", listed: withCommandApps(testCommandApp)},
		{name: "listed by another database", listed: withPaymentsDatabase(testCommandApp)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, mux := setupReviewGateHandler(t, reviewGateTestConfig(tt.listed, func(cfg *api.ServerConfig) {
				cfg.ReviewPolicy.AdminUsers = []string{testCommandAppLogin, "carol"}
				db := cfg.Databases["orders"]
				db.OperatorUsers = []string{testCommandAppLogin, "dave"}
				cfg.Databases["orders"] = db
			}))

			registerPREndpoint(mux, "alice")
			registerReviewsEndpoint(mux, []*gh.PullRequestReview{
				{
					User:        &gh.User{Login: new(testCommandAppLogin)},
					State:       new(ghclient.ReviewApproved),
					CommitID:    new(reviewGateTestHeadSHA),
					SubmittedAt: &gh.Timestamp{Time: time.Now()},
				},
			})

			client, err := h.clientForRepo("octocat/hello-world", 12345)
			require.NoError(t, err)

			result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "staging", reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.False(t, result.Approved)
			assert.Equal(t, []string{testCommandAppLogin}, result.CommandAppApprovers)
			assert.Empty(t, result.UnauthorizedApprovers)
			assert.Equal(t, []string{"dave"}, result.OperatorReviewers)
			assert.Equal(t, []string{"carol"}, result.OtherReviewers)
		})
	}
}

// An App's plan is gated by its command_apps entries even with PR command
// authorization off: an App listed only for orders cannot plan payments, in a
// single-environment plan or a plan across every environment.
func TestPlanForResolvedDatabaseGatesCommandAppWithAuthorizationDisabled(t *testing.T) {
	tests := []struct {
		name        string
		database    string
		environment string
		wantBlocked bool
	}{
		{name: "listed database, one environment", database: "orders", environment: "staging"},
		{name: "listed database, every environment", database: "orders"},
		{name: "unlisted database, one environment", database: "payments", environment: "staging", wantBlocked: true},
		{name: "unlisted database, every environment", database: "payments", wantBlocked: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux := setupGitHubServer(t)
			comments := make(chan string, 2)
			mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))
			installClient := ghclient.NewInstallationClient(client, testLogger())
			cfg := actorAuthTestConfig(false, withCommandApps(testCommandApp), withPaymentsDatabase())
			h := actorAuthTestHandler(cfg, installClient)

			blocked := h.planForResolvedDatabaseBlocked(t.Context(), "octocat/hello-world", 1, 12345, testCommandAppLogin, tt.database, tt.environment)

			assert.Equal(t, tt.wantBlocked, blocked)
			if !tt.wantBlocked {
				select {
				case body := <-comments:
					t.Fatalf("no comment should be posted for a listed command App, got %q", body)
				default:
				}
				return
			}
			body := requireComment(t, comments, "unauthorized command App plan comment")
			assert.Contains(t, body, "SchemaBot Command Not Authorized")
			assert.Contains(t, body, "@"+testCommandAppLogin+" is not authorized")
		})
	}
}

func commandAppCommentPayload(t *testing.T, body string) []byte {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"action":       "created",
		"issue":        map[string]any{"number": 7, "pull_request": map[string]any{"url": "https://api.github.com/repos/octocat/hello-world/pulls/7"}},
		"comment":      map[string]any{"id": 42, "body": body, "user": map[string]any{"login": testCommandAppLogin, "type": "Bot"}},
		"repository":   map[string]any{"full_name": "octocat/hello-world"},
		"installation": map[string]any{"id": 12345},
	})
	require.NoError(t, err)
	return payload
}
