package webhook

import (
	"context"
	"encoding/json"
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

// appScopeStorage backs app-scoped command tests with a fixed set of stored
// plans, so tests can control which app databases participate in an expansion.
type appScopeStorage struct {
	emptyStorage
	plans    []*storage.Plan
	plansErr error
}

func (s *appScopeStorage) Plans() storage.PlanStore {
	return &appScopePlanStore{plans: s.plans, err: s.plansErr}
}

type appScopePlanStore struct {
	storage.PlanStore
	plans []*storage.Plan
	err   error
}

func (s *appScopePlanStore) GetByPR(_ context.Context, repo string, pr int) ([]*storage.Plan, error) {
	if s.err != nil {
		return nil, s.err
	}
	var matches []*storage.Plan
	for _, plan := range s.plans {
		if plan.Repository == repo && plan.PullRequest == pr {
			matches = append(matches, plan)
		}
	}
	return matches, nil
}

// appScopeTestConfig builds a config with three databases in app "tenants"
// plus one unrelated database, all with a staging environment unless a test
// mutates them.
func appScopeTestConfig(authEnabled bool, opts ...func(*api.ServerConfig)) *api.ServerConfig {
	cfg := &api.ServerConfig{
		PRCommandAuthorization: api.PRCommandAuthorizationConfig{Enabled: authEnabled},
		Databases: map[string]api.DatabaseConfig{
			"tenants-shard-01": {
				Type: storage.DatabaseTypeMySQL,
				App:  "tenants",
				Environments: map[string]api.EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost)/tenants_01"},
				},
			},
			"tenants-shard-02": {
				Type: storage.DatabaseTypeMySQL,
				App:  "tenants",
				Environments: map[string]api.EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost)/tenants_02"},
				},
			},
			"tenants-shard-03": {
				Type: storage.DatabaseTypeMySQL,
				App:  "tenants",
				Environments: map[string]api.EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost)/tenants_03"},
				},
			},
			"standalone": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]api.EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost)/standalone"},
				},
			},
		},
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// appScopePlan returns a stored staging plan for one database on the test PR.
func appScopePlan(id int64, database string) *storage.Plan {
	return &storage.Plan{
		ID:             id,
		PlanIdentifier: "plan_" + database,
		Database:       database,
		DatabaseType:   storage.DatabaseTypeMySQL,
		Repository:     "octocat/hello-world",
		PullRequest:    1,
		SchemaPath:     "schema/" + database,
		Environment:    "staging",
		CreatedAt:      time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	}
}

func appScopeTestHandler(t *testing.T, cfg *api.ServerConfig, store storage.Storage) (*Handler, <-chan string, *http.ServeMux) {
	t.Helper()
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))
	installClient := ghclient.NewInstallationClient(client, testLogger())
	return actorAuthStorageTestHandler(cfg, store, installClient), comments, mux
}

// registerAppScopePREndpoint stubs the PR fetch the dispatch head-pin uses.
// headSHA is called per request so tests can advance the PR head mid-dispatch.
func registerAppScopePREndpoint(t *testing.T, mux *http.ServeMux, headSHA func() string) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"state": "open",
			"head":  map[string]any{"sha": headSHA(), "ref": "feature-branch"},
			"base":  map[string]any{"sha": "base456", "ref": "main"},
			"user":  map[string]any{"login": "testuser"},
		}))
	})
}

// An app no configured database declares must reject with a clear comment and
// start nothing — zero matches is an error, not a no-op.
func TestAppScopedApplyUnknownAppRejected(t *testing.T) {
	cfg := appScopeTestConfig(false)
	h, comments, _ := appScopeTestHandler(t, cfg, &appScopeStorage{})

	retry, err := h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "mona",
		CommandResult{Action: action.Apply, App: "unknown-app"})
	require.NoError(t, err)
	assert.False(t, retry)

	body := requireComment(t, comments, "unknown app rejection")
	assert.Contains(t, body, "App-Scoped Command Rejected")
	assert.Contains(t, body, "`schemabot apply --app unknown-app` was rejected")
	assert.Contains(t, body, "no configured database declares `app: unknown-app`")
	assert.Contains(t, body, "No schema change was started for any database.")
	assert.Empty(t, comments, "rejection must be the only comment; nothing may dispatch")
}

// An app whose databases have no stored plan for this PR and environment must
// reject rather than silently applying to nothing.
func TestAppScopedApplyNoPlansRejected(t *testing.T) {
	cfg := appScopeTestConfig(false)
	h, comments, _ := appScopeTestHandler(t, cfg, &appScopeStorage{})

	retry, err := h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "mona",
		CommandResult{Action: action.Apply, App: "tenants"})
	require.NoError(t, err)
	assert.False(t, retry)

	body := requireComment(t, comments, "empty expansion rejection")
	assert.Contains(t, body, "App-Scoped Command Rejected")
	assert.Contains(t, body, "no database in the app has a stored `staging` plan for this PR")
	assert.Contains(t, body, "schemabot plan -e staging")
	assert.Empty(t, comments, "rejection must be the only comment; nothing may dispatch")
}

// A storage failure while loading the PR's plans is not the command's answer:
// the command fails closed and stays retryable, and a durable attempt posts no
// per-retry error comment.
func TestAppScopedApplyPlanLookupErrorRetries(t *testing.T) {
	cfg := appScopeTestConfig(false)
	h, comments, _ := appScopeTestHandler(t, cfg, &appScopeStorage{plansErr: assert.AnError})

	retry, err := h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "mona",
		CommandResult{Action: action.Apply, App: "tenants", SuppressRetryComments: true})
	require.Error(t, err)
	assert.True(t, retry)
	assert.Contains(t, err.Error(), "tenants")
	assert.Empty(t, comments, "a durable attempt must not post per-retry error comments")
}

// A source-policy denial by any database in the expansion rejects the whole
// command: config narrowed after plan time must not let the rest of the app
// proceed as a partial apply.
func TestAppScopedApplySourcePolicyDenialRejectsAll(t *testing.T) {
	cfg := appScopeTestConfig(false, func(cfg *api.ServerConfig) {
		db := cfg.Databases["tenants-shard-02"]
		db.AllowedRepos = []string{"octocat/other-repo"}
		cfg.Databases["tenants-shard-02"] = db
	})
	store := &appScopeStorage{plans: []*storage.Plan{
		appScopePlan(1, "tenants-shard-01"),
		appScopePlan(2, "tenants-shard-02"),
	}}
	h, comments, _ := appScopeTestHandler(t, cfg, store)

	retry, err := h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "mona",
		CommandResult{Action: action.Apply, App: "tenants"})
	require.NoError(t, err)
	assert.False(t, retry)

	body := requireComment(t, comments, "source policy rejection")
	assert.Contains(t, body, "App-Scoped Command Rejected")
	assert.Contains(t, body, "database `tenants-shard-02` denied the PR's source")
	assert.Contains(t, body, "No schema change was started for any database.")
	assert.Empty(t, comments, "rejection must be the only comment; nothing may dispatch")
}

// The actor must be authorized for every database in the expansion. Being an
// operator of one shard is not enough — a denial on any shard denies the whole
// command before anything dispatches, and the comment names the denying
// database and its operators.
func TestAppScopedApplyActorDenialOnOneDatabaseDeniesAll(t *testing.T) {
	cfg := appScopeTestConfig(true, func(cfg *api.ServerConfig) {
		shard01 := cfg.Databases["tenants-shard-01"]
		shard01.OperatorUsers = []string{"mona"}
		cfg.Databases["tenants-shard-01"] = shard01
		shard02 := cfg.Databases["tenants-shard-02"]
		shard02.OperatorUsers = []string{"hubot"}
		cfg.Databases["tenants-shard-02"] = shard02
	})
	store := &appScopeStorage{plans: []*storage.Plan{
		appScopePlan(1, "tenants-shard-01"),
		appScopePlan(2, "tenants-shard-02"),
	}}
	h, comments, _ := appScopeTestHandler(t, cfg, store)

	retry, err := h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "mona",
		CommandResult{Action: action.Apply, App: "tenants"})
	require.NoError(t, err)
	assert.False(t, retry)

	body := requireComment(t, comments, "app-scoped authorization denial")
	assert.Contains(t, body, "SchemaBot Command Not Authorized")
	assert.Contains(t, body, "@mona is not authorized to run `schemabot apply` for database `tenants-shard-02`")
	assert.Contains(t, body, "authorization for **every** database in the app")
	assert.Contains(t, body, "no database in `tenants` was applied")
	assert.Contains(t, body, "`hubot`")
	assert.Empty(t, comments, "denial must be the only comment; nothing may dispatch")
}

// A fully authorized expansion posts the dispatch summary — targeted databases
// in name order, skipped app databases with their reasons — before the
// per-database applies run. The per-database cores then run exactly as `-d`
// commands, so this test only pins the expansion summary and lets the cores
// fail on the deliberately unstubbed GitHub discovery endpoints (a retryable
// disposition, silent under SuppressRetryComments).
func TestAppScopedApplyDispatchSummaryListsTargetsAndSkips(t *testing.T) {
	cfg := appScopeTestConfig(true, func(cfg *api.ServerConfig) {
		cfg.PRCommandAuthorization.AdminUsers = []string{"mona"}
		shard02 := cfg.Databases["tenants-shard-02"]
		shard02.Environments = map[string]api.EnvironmentConfig{
			"production": {DSN: "root@tcp(localhost)/tenants_02"},
		}
		cfg.Databases["tenants-shard-02"] = shard02
	})
	store := &appScopeStorage{plans: []*storage.Plan{
		appScopePlan(1, "tenants-shard-01"),
	}}
	h, comments, mux := appScopeTestHandler(t, cfg, store)
	registerAppScopePREndpoint(t, mux, func() string { return "abc123" })

	retry, err := h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "mona",
		CommandResult{Action: action.Apply, App: "tenants", SuppressRetryComments: true})
	require.Error(t, err, "the per-database core has no discovery endpoints stubbed, so its failure must surface")
	assert.True(t, retry)
	assert.Contains(t, err.Error(), "tenants-shard-01")

	body := requireComment(t, comments, "dispatch summary")
	assert.Contains(t, body, "Schema Change Status — Staging")
	assert.Contains(t, body, "**App**: `tenants` | **Commit**: `abc123`")
	assert.Contains(t, body, "Applying to **1** database")
	assert.Contains(t, body, "- `tenants-shard-01`")
	assert.Contains(t, body, "**Skipped** (2):")
	assert.Contains(t, body, "- `tenants-shard-02` — environment `staging` is not configured")
	assert.Contains(t, body, "- `tenants-shard-03` — no plan for this PR")
}

// Only the newest stored plan per database counts for participation, so a
// database whose older plan targeted another environment still participates
// through its newer one, and superseded plans do not resurrect databases.
func TestAppScopedApplyUsesNewestPlanPerDatabase(t *testing.T) {
	older := appScopePlan(1, "tenants-shard-01")
	older.CreatedAt = older.CreatedAt.Add(-time.Hour)
	older.SchemaPath = "schema/old-path"
	newer := appScopePlan(2, "tenants-shard-01")

	cfg := appScopeTestConfig(false, func(cfg *api.ServerConfig) {
		// Pin source policy to the newer plan's schema path: if expansion read
		// the older plan, the policy check would reject its stale path.
		db := cfg.Databases["tenants-shard-01"]
		db.AllowedDirs = []string{"schema/tenants-shard-01"}
		cfg.Databases["tenants-shard-01"] = db
	})
	store := &appScopeStorage{plans: []*storage.Plan{older, newer}}
	h, comments, mux := appScopeTestHandler(t, cfg, store)
	registerAppScopePREndpoint(t, mux, func() string { return "abc123" })

	retry, err := h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "mona",
		CommandResult{Action: action.Apply, App: "tenants", SuppressRetryComments: true})
	require.Error(t, err, "the per-database core has no discovery endpoints stubbed, so its failure must surface")
	assert.True(t, retry)

	body := requireComment(t, comments, "dispatch summary")
	assert.Contains(t, body, "- `tenants-shard-01`")
}

// A commit landing on the PR while an app-scoped dispatch is mid-loop halts
// the remaining databases: every database in the app must apply the same
// commit, so the rest wait for a fresh command against the new head. The halt
// comment names the pinned and current commits and the databases not started.
func TestAppScopedApplyHaltsWhenHeadAdvancesMidDispatch(t *testing.T) {
	cfg := appScopeTestConfig(false)
	store := &appScopeStorage{plans: []*storage.Plan{
		appScopePlan(1, "tenants-shard-01"),
		appScopePlan(2, "tenants-shard-02"),
	}}
	h, comments, mux := appScopeTestHandler(t, cfg, store)
	var prFetches atomic.Int32
	registerAppScopePREndpoint(t, mux, func() string {
		if prFetches.Add(1) == 1 {
			return "abc123"
		}
		return "def789"
	})

	retry, err := h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "mona",
		CommandResult{Action: action.Apply, App: "tenants", SuppressRetryComments: true})
	require.Error(t, err, "the dispatched database's core has no discovery endpoints stubbed, so its failure must surface")
	assert.True(t, retry)
	assert.Contains(t, err.Error(), "tenants-shard-01")
	assert.NotContains(t, err.Error(), "tenants-shard-02", "the halted database was never dispatched")

	summary := requireComment(t, comments, "dispatch summary")
	assert.Contains(t, summary, "Schema Change Status — Staging")
	halt := requireComment(t, comments, "dispatch halt")
	assert.Contains(t, halt, "App-Scoped Dispatch Halted")
	assert.Contains(t, halt, "`abc123`")
	assert.Contains(t, halt, "`def789`")
	assert.Contains(t, halt, "- `tenants-shard-02`")
	assert.Contains(t, halt, "this database was **not** started")
}

// App preflight (source policy, all-or-nothing actor authorization) is
// evaluated per deployment, so an unscoped app command on an aggregate repo —
// which fans out to every deployment — cannot uphold the all-or-nothing
// guarantee fleet-wide and is rejected. The leader posts the rejection once;
// participants stay silent. A `-t`-scoped command addresses one deployment,
// where the guarantee holds, and proceeds into normal expansion.
func TestAppScopedApplyUnscopedRejectedOnAggregateRepo(t *testing.T) {
	role := func(role string) func(*api.ServerConfig) {
		return func(cfg *api.ServerConfig) {
			cfg.Repos = map[string]api.RepoConfig{
				"octocat/hello-world": {Aggregate: &api.AggregateConfig{Role: role}},
			}
		}
	}

	t.Run("leader posts the rejection", func(t *testing.T) {
		h, comments, _ := appScopeTestHandler(t, appScopeTestConfig(false, role(api.AggregateRoleLeader)), &appScopeStorage{})

		retry, err := h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "mona",
			CommandResult{Action: action.Apply, App: "tenants"})
		require.NoError(t, err)
		assert.False(t, retry)

		body := requireComment(t, comments, "aggregate rejection")
		assert.Contains(t, body, "App-Scoped Command Rejected")
		assert.Contains(t, body, "cannot be coordinated across")
		assert.Contains(t, body, "-t <tenant>")
	})

	t.Run("participant stays silent", func(t *testing.T) {
		h, comments, _ := appScopeTestHandler(t, appScopeTestConfig(false, role(api.AggregateRoleParticipant)), &appScopeStorage{})

		retry, err := h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "mona",
			CommandResult{Action: action.Apply, App: "tenants"})
		require.NoError(t, err)
		assert.False(t, retry)
		assert.Empty(t, comments, "the leader posts the rejection exactly once")
	})

	t.Run("tenant-scoped command proceeds into expansion", func(t *testing.T) {
		h, comments, _ := appScopeTestHandler(t, appScopeTestConfig(false, role(api.AggregateRoleParticipant)), &appScopeStorage{})

		retry, err := h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "mona",
			CommandResult{Action: action.Apply, App: "tenants", Tenant: "tenant-a"})
		require.NoError(t, err)
		assert.False(t, retry)

		body := requireComment(t, comments, "tenant-scoped expansion answer")
		assert.Contains(t, body, "no database in the app has a stored `staging` plan for this PR",
			"a -t-scoped command reaches normal expansion instead of the aggregate gate")
	})
}

// The per-database cores enforce the pinned head themselves: even when a
// core's own discovery is internally consistent at a newer commit, a live head
// that moved off the app dispatch's pin is rejected. An unpinned command
// (empty pin) is unaffected.
func TestAssertPinnedHeadStillCurrent(t *testing.T) {
	h, comments, _ := appScopeTestHandler(t, appScopeTestConfig(false), &appScopeStorage{})
	schema := &ghclient.SchemaRequestResult{Database: "tenants-shard-01", Type: storage.DatabaseTypeMySQL, HeadSHA: "abc123"}

	assert.False(t, h.assertPinnedHeadStillCurrent(t.Context(), "octocat/hello-world", 1, 12345, schema,
		"", "def789", "staging", "mona", action.Apply),
		"an unpinned command is never rejected by the pin check")
	assert.False(t, h.assertPinnedHeadStillCurrent(t.Context(), "octocat/hello-world", 1, 12345, schema,
		"abc123", "abc123", "staging", "mona", action.Apply),
		"a live head still on the pin proceeds")

	assert.True(t, h.assertPinnedHeadStillCurrent(t.Context(), "octocat/hello-world", 1, 12345, schema,
		"abc123", "def789", "staging", "mona", action.Apply),
		"a live head off the pin is rejected")
	body := requireComment(t, comments, "pinned head rejection")
	assert.Contains(t, body, "abc123")
	assert.Contains(t, body, "def789")
	assert.Contains(t, body, "tenants-shard-01")
}

// apply-confirm shares the app-scoped expansion, so its rejections name the
// confirm command.
func TestAppScopedApplyConfirmRoutesThroughExpansion(t *testing.T) {
	cfg := appScopeTestConfig(false)
	h, comments, _ := appScopeTestHandler(t, cfg, &appScopeStorage{})

	retry, err := h.applyConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "mona",
		CommandResult{Action: action.ApplyConfirm, App: "tenants"})
	require.NoError(t, err)
	assert.False(t, retry)

	body := requireComment(t, comments, "apply-confirm expansion rejection")
	assert.Contains(t, body, "`schemabot apply-confirm --app tenants` was rejected")
}

// TestIssueCommentGateBlockAppFlags pins the shared gate ladder's usage gates
// for `--app`: a malformed value, the flag on a command that does not support
// it, and the two conflicting-flag combinations are all blocked before
// dispatch on both the request path and the durable driver.
func TestIssueCommentGateBlockAppFlags(t *testing.T) {
	h := &Handler{logger: testLogger()}
	parser := NewCommandParser()

	tests := []struct {
		name string
		body string
		want issueCommentGateBlockReason
	}{
		{
			name: "malformed app value",
			body: "schemabot apply -e staging --app billing@service",
			want: issueCommentGateInvalidApp,
		},
		{
			name: "missing app value",
			body: "schemabot apply -e staging --app",
			want: issueCommentGateInvalidApp,
		},
		{
			name: "app flag on a command without app support",
			body: "schemabot plan -e staging --app billing-service",
			want: issueCommentGateApp,
		},
		{
			name: "app with database flag",
			body: "schemabot apply -e staging --app billing-service -d billing-ledger",
			want: issueCommentGateAppWithDatabase,
		},
		{
			name: "app with dangling database flag",
			body: "schemabot apply -e staging --app billing-service -d",
			want: issueCommentGateAppWithDatabase,
		},
		{
			name: "app with defer-cutover",
			body: "schemabot apply -e staging --app billing-service --defer-cutover",
			want: issueCommentGateAppDeferCutover,
		},
		{
			name: "app-scoped apply passes",
			body: "schemabot apply -e staging --app billing-service",
			want: issueCommentGatePass,
		},
		{
			name: "app-scoped apply-confirm with allow-unsafe passes",
			body: "schemabot apply-confirm -e staging --app billing-service --allow-unsafe",
			want: issueCommentGatePass,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := parser.ParseCommand(tc.body)
			assert.Equal(t, tc.want, h.issueCommentGateBlock("octocat/hello-world", result, parser, tc.body))
		})
	}
}

// The request path posts a distinct usage comment for each `--app` gate, so
// the operator learns exactly which flag combination was rejected.
func TestWebhookAppFlagUsageComments(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 4)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))
	installClient := ghclient.NewInstallationClient(client, testLogger())
	h := actorAuthStorageTestHandler(appScopeTestConfig(false), &appScopeStorage{}, installClient)

	tests := []struct {
		name    string
		comment string
		want    []string
	}{
		{
			name:    "invalid app value",
			comment: "schemabot apply -e staging --app billing@service",
			want:    []string{"Missing or Invalid App", "--app <app>"},
		},
		{
			name:    "unsupported command",
			comment: "schemabot plan -e staging --app billing-service",
			want:    []string{"The `--app` flag is not supported for `plan`."},
		},
		{
			name:    "app with database flag",
			comment: "schemabot apply -e staging --app billing-service -d billing-ledger",
			want:    []string{"Conflicting Flags", "`--app` and `-d` cannot be combined"},
		},
		{
			name:    "app with defer-cutover",
			comment: "schemabot apply -e staging --app billing-service --defer-cutover",
			want:    []string{"Conflicting Flags", "`--defer-cutover` is not supported with `--app`"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := buildWebhookRequest(t, webhookPayloadOpts{
				comment:   tc.comment,
				userLogin: "mona",
				isPR:      true,
			}, nil)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			require.Equal(t, http.StatusOK, rr.Code)

			body := requireComment(t, comments, tc.name)
			for _, want := range tc.want {
				assert.Contains(t, body, want)
			}
		})
	}
}
