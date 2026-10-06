package webhook

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
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

// aggregateLeaderWithAllowlistConfig returns aggregateLeaderConfig with one
// database of the leader's own, "billing", registered for the test repo under
// billing/schema — the shape of a leader whose ownership is partitioned by
// allowed_dirs rather than by registry alone.
func aggregateLeaderWithAllowlistConfig() *api.ServerConfig {
	cfg := aggregateLeaderConfig()
	cfg.Databases = map[string]api.DatabaseConfig{
		"billing": {
			Type:         "mysql",
			AllowedRepos: []string{"octocat/hello-world"},
			AllowedDirs:  []string{"billing/schema"},
			Environments: map[string]api.EnvironmentConfig{
				"staging": {Deployment: "default", Target: "billing"},
			},
		},
	}
	return cfg
}

// nonAggregateConfig returns a server config where the test repo has no
// aggregate role, so every "nothing to do" answer stays a visible PR comment.
func nonAggregateConfig() *api.ServerConfig {
	return &api.ServerConfig{
		Repos: map[string]api.RepoConfig{"octocat/hello-world": {}},
	}
}

// aggregateParticipantConfig returns a server config where the test repo has
// the aggregate participant role — the shape of a tenant deployment receiving
// unscoped fan-out commands on a shared repo.
func aggregateParticipantConfig() *api.ServerConfig {
	return &api.ServerConfig{
		Repos: map[string]api.RepoConfig{
			"octocat/hello-world": {Aggregate: &api.AggregateConfig{Role: api.AggregateRoleParticipant}},
		},
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
	installClient.SetConfigDirHints(cfg)
	h := &Handler{
		service:   api.New(&emptyStorage{}, cfg, nil, testLogger()),
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
		logger:    testLogger(),
	}
	return h, mux, comments
}

// serveTruncatedRepoWithChangedSchemaFile registers a PR whose changed schema
// file has no reachable config and whose recursive repository tree is
// truncated. A participant cannot resolve ownership from this local view.
func serveTruncatedRepoWithChangedSchemaFile(t *testing.T, mux *http.ServeMux) {
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
			"filename": "schema/users.sql",
			"status":   "modified",
		}}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/abc123", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"truncated": true,
			"tree":      []any{},
		}))
	})
}

func serveCompleteRootConfigTree(t *testing.T, mux *http.ServeMux) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/abc123", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"truncated": false,
			"tree": []map[string]string{{
				"path": "schemabot.yaml",
				"type": "blob",
				"sha":  "config-sha",
			}},
		}))
	})
}

// serveSchemaConfigForDatabase registers the GitHub content routes config
// discovery needs so the PR resolves to a schemabot.yaml at the repository
// root for the given database.
func serveSchemaConfigForDatabase(t *testing.T, mux *http.ServeMux, database string) {
	t.Helper()
	serveSchemaConfigForDatabaseUnder(t, mux, database, "")
}

// serveSchemaConfigForDatabaseUnder is serveSchemaConfigForDatabase with the
// schemabot.yaml under dir ("" for the repository root), so a test can place
// the config inside or outside a participant's schema directory.
func serveSchemaConfigForDatabaseUnder(t *testing.T, mux *http.ServeMux, database, dir string) {
	t.Helper()
	serveSchemaConfigs(t, mux, schemaConfigFixture{database: database, dir: dir})
}

// schemaConfigFixture places one schemabot.yaml declaring database under dir
// ("" for the repository root).
type schemaConfigFixture struct {
	database string
	dir      string
}

// serveSchemaConfigs registers the GitHub routes config discovery needs for a
// PR that changes every given schemabot.yaml. Discovery orders the configs by
// path, whatever order the PR lists them in.
func serveSchemaConfigs(t *testing.T, mux *http.ServeMux, configs ...schemaConfigFixture) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"head": map[string]any{"sha": "abc123", "ref": "feature-branch"},
			"base": map[string]any{"sha": "def456", "ref": "main"},
			"user": map[string]any{"login": "testuser"},
		}))
	})
	files := make([]map[string]string, 0, len(configs))
	for _, cfg := range configs {
		configPath := "schemabot.yaml"
		if cfg.dir != "" {
			configPath = cfg.dir + "/schemabot.yaml"
		}
		files = append(files, map[string]string{"filename": configPath, "status": "modified"})
		content := "database: " + cfg.database + "\ntype: mysql\n"
		mux.HandleFunc("GET /repos/octocat/hello-world/contents/"+configPath, func(w http.ResponseWriter, _ *http.Request) {
			require.NoError(t, json.NewEncoder(w).Encode(map[string]string{
				"type":     "file",
				"encoding": "base64",
				"content":  base64.StdEncoding.EncodeToString([]byte(content)),
			}))
		})
	}
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(files))
	})
}

// On an aggregate repo, an unscoped `apply -e <env>` fans out to every
// installed deployment. A deployment whose databases registry has no entry for
// the database discovered from the PR's schemabot.yaml is not the owner under
// the aggregate contract, so it stays silent while the owner acts — unless it
// is the leader and the config is under no expected participant's paths: a
// database the leader has not registered there is one it answers for, once,
// with Database Not Registered rather than leaving the command
// unanswered. A -t-scoped command and a non-aggregate repo still surface the
// error.
func TestUnscopedApplyOnUnregisteredDatabase(t *testing.T) {
	apply := func(h *Handler, tenant string) {
		h.handleApplyCommand("octocat/hello-world", 1, "staging", "", 12345, "hubot", CommandResult{Action: action.Apply, Tenant: tenant})
	}

	t.Run("leader answers for a database it has not registered", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())
		serveSchemaConfigForDatabase(t, mux, "orders")

		apply(h, "")

		body := requireComment(t, comments, "database-not-registered apply error")
		assert.Contains(t, body, "Database Not Registered")
		assert.Contains(t, body, "**Database**: `orders` | **Schema directory**: `.` | **Environment**: `staging`")
		assert.Contains(t, body, "ask a SchemaBot operator to register it with this schema directory")
	})

	t.Run("leader with an allowlist answers for a database it has not registered", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateLeaderWithAllowlistConfig())
		serveSchemaConfigForDatabaseUnder(t, mux, "orders", "orders/schema")

		apply(h, "")

		body := requireComment(t, comments, "database-not-registered apply error")
		assert.Contains(t, body, "Database Not Registered")
		assert.Contains(t, body, "**Database**: `orders` | **Schema directory**: `orders/schema`")
	})

	// The leader's own database declared outside its allowed_dirs, where no
	// participant manages either, is misplaced for the whole fleet: the
	// allowed_dirs remedy is the right one and the leader is the one to give it.
	t.Run("leader answers for its own database misplaced outside every managed directory", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateLeaderWithAllowlistConfig())
		serveSchemaConfigForDatabaseUnder(t, mux, "billing", "misc/schema")

		apply(h, "")

		body := requireComment(t, comments, "config-not-authorized apply error")
		assert.Contains(t, body, "SchemaBot Configuration Not Authorized")
		assert.Contains(t, body, "**Schema directory**: `misc/schema`")
		assert.Contains(t, body, "`databases.billing.allowed_dirs`")
	})

	// A PR that adds several configs the leader has not registered gets one
	// reply naming all of them, so the author does not learn about the second
	// one only after fixing the first and re-running the command.
	t.Run("leader names every unregistered config in one reply", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())
		serveSchemaConfigs(t, mux,
			schemaConfigFixture{database: "orders", dir: "orders/schema"},
			schemaConfigFixture{database: "ledger", dir: "ledger/schema"})

		apply(h, "")

		body := requireComment(t, comments, "databases-not-registered apply error")
		assert.Contains(t, body, "Databases Not Registered")
		assert.Contains(t, body, "- `ledger/schema` declares database `ledger`\n- `orders/schema` declares database `orders`\n")
		assert.NotContains(t, body, "**Database**:", "several configs have no one database for the header")
		assert.Empty(t, comments, "one reply answers the command")
	})

	// A config a participant owns is left to it even when discovery lists it
	// first (tenant-b/schema sorts before unowned/schema), and the config the
	// leader has not registered is still answered for.
	t.Run("leader answers for an unregistered config listed after a participant's", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())
		serveSchemaConfigs(t, mux,
			schemaConfigFixture{database: "payments", dir: "tenant-b/schema"},
			schemaConfigFixture{database: "orders", dir: "unowned/schema"})

		apply(h, "")

		body := requireComment(t, comments, "database-not-registered apply error")
		assert.Contains(t, body, "Database Not Registered")
		assert.Contains(t, body, "**Database**: `orders` | **Schema directory**: `unowned/schema`")
		assert.NotContains(t, body, "payments", "the participant answers for its own config")
		assert.Empty(t, comments, "one reply answers the command")
	})

	t.Run("leader stays silent for a database under a participant's directory", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())
		serveSchemaConfigForDatabaseUnder(t, mux, "orders", "tenant-b/schema")

		apply(h, "")

		assert.Empty(t, comments, "the participant managing tenant-b/schema answers; the leader must not")
	})

	// A participant deployment receives the same fan-out for a PR touching
	// only a database another deployment owns. Its view covers only its own
	// slice of the fleet, so it cannot tell whether anyone else owns the
	// config and stays silent.
	t.Run("participant deployment stays silent", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateParticipantConfig())
		serveSchemaConfigForDatabase(t, mux, "orders")

		apply(h, "")

		assert.Empty(t, comments, "a participant must not post Apply Failed for a database another deployment owns")
	})

	t.Run("tenant-scoped apply on a participant still reports the error", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateParticipantConfig())
		serveSchemaConfigForDatabase(t, mux, "orders")

		apply(h, "tenant-b")

		body := requireComment(t, comments, "database-not-configured apply error")
		assert.Contains(t, body, "Database Not Configured")
		assert.Contains(t, body, "`orders`")
	})

	t.Run("non-aggregate repo still reports the error", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, nonAggregateConfig())
		serveSchemaConfigForDatabase(t, mux, "orders")

		apply(h, "")

		body := requireComment(t, comments, "database-not-configured apply error")
		assert.Contains(t, body, "Database Not Configured")
		assert.Contains(t, body, "`orders`")
	})
}

// A bare `schemabot plan` (no -e) on an aggregate repo fans out to every
// installed deployment just like its -e sibling. A deployment whose databases
// registry has no entry for the database discovered from the PR's
// schemabot.yaml is not the owner under the aggregate contract, so it stays
// silent instead of posting a failure next to the owning deployment's real
// plan — unless it is the leader and the config is under no expected
// participant's paths, in which case the leader answers once with Database Not
// Registered.
// A -t-scoped command and a non-aggregate repo still surface the error.
func TestUnscopedMultiEnvPlanOnUnregisteredDatabase(t *testing.T) {
	barePlan := func(h *Handler, databaseName, tenant string) {
		h.handleMultiEnvPlan("octocat/hello-world", 1, databaseName, tenant, 12345, "hubot", false, 0, true, 0)
	}

	t.Run("leader answers for a database it has not registered", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())
		serveSchemaConfigForDatabase(t, mux, "orders")

		barePlan(h, "", "")

		body := requireComment(t, comments, "database-not-registered plan error")
		assert.Contains(t, body, "Database Not Registered")
		assert.Contains(t, body, "**Database**: `orders` | **Schema directory**: `.`")
	})

	t.Run("leader stays silent for a database under a participant's directory", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())
		serveSchemaConfigForDatabaseUnder(t, mux, "orders", "tenant-b/schema")

		barePlan(h, "", "")

		assert.Empty(t, comments, "the participant managing tenant-b/schema answers; the leader must not")
	})

	t.Run("participant deployment stays silent", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateParticipantConfig())
		serveSchemaConfigForDatabase(t, mux, "orders")

		barePlan(h, "", "")

		assert.Empty(t, comments, "a participant must not post a plan failure for a database another deployment owns")
	})

	// Naming the database with -d does not name a deployment: the discovered
	// config still belongs to whichever deployment registers the database. A
	// participant defers as silently as it does for the unscoped form; the
	// leader, which keeps searching the repository for a -d database precisely
	// so a database no deployment it knows of serves is still reported,
	// answers.
	t.Run("database-scoped bare plan on a participant stays silent", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateParticipantConfig())
		serveSchemaConfigForDatabase(t, mux, "orders")

		barePlan(h, "orders", "")

		assert.Empty(t, comments, "a -d-scoped fan-out plan on an unowned database must stay silent on a participant")
	})

	t.Run("database-scoped bare plan on the leader answers for a database it has not registered", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())
		serveSchemaConfigForDatabase(t, mux, "orders")

		barePlan(h, "orders", "")

		body := requireComment(t, comments, "database-not-registered plan error")
		assert.Contains(t, body, "Database Not Registered")
		assert.Contains(t, body, "`orders`")
	})

	t.Run("tenant-scoped plan on a participant still reports the error", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateParticipantConfig())
		serveSchemaConfigForDatabase(t, mux, "orders")

		barePlan(h, "", "tenant-b")

		body := requireComment(t, comments, "database-not-configured plan error")
		assert.Contains(t, body, "Database Not Configured")
		assert.Contains(t, body, "`orders`")
	})

	t.Run("non-aggregate repo still reports the error", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, nonAggregateConfig())
		serveSchemaConfigForDatabase(t, mux, "orders")

		barePlan(h, "", "")

		body := requireComment(t, comments, "database-not-configured plan error")
		assert.Contains(t, body, "Database Not Configured")
		assert.Contains(t, body, "`orders`")
	})
}

// splitLeaderConfig returns the config of one of two aggregate leaders that
// split the test repo's environments: both expect the same participant, each
// serves only env, and each registers only the given databases.
func splitLeaderConfig(env string, databases map[string]api.DatabaseConfig) *api.ServerConfig {
	cfg := aggregateLeaderConfig()
	cfg.AllowedEnvironments = []string{env}
	cfg.Databases = databases
	return cfg
}

// ledgerOnStaging registers ledger with the staging leader only, under the
// schema directory the PR's schemabot.yaml sits in.
func ledgerOnStaging() map[string]api.DatabaseConfig {
	return map[string]api.DatabaseConfig{
		"ledger": {
			Type:         "mysql",
			AllowedRepos: []string{"octocat/hello-world"},
			AllowedDirs:  []string{"services/ledger/schema"},
			Environments: map[string]api.EnvironmentConfig{
				"staging": {Deployment: "default", Target: "ledger"},
			},
		},
	}
}

// fleetLeader is one deployment of a split-environment fleet, reached through
// its webhook endpoint the way GitHub reaches it, with its own fake GitHub so
// every comment, reaction, and log line is attributable to it.
type fleetLeader struct {
	h         *Handler
	mux       *http.ServeMux
	comments  chan string
	reactions chan string
	logs      *syncBuffer
}

// fleetLeaderWorkDeadline bounds the wait for a leader's dispatched webhook
// work so a hang fails the test instead of stalling the suite.
const fleetLeaderWorkDeadline = 5 * time.Second

func newFleetLeader(t *testing.T, cfg *api.ServerConfig) fleetLeader {
	t.Helper()
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))
	reactions := registerReactionRecorder(t, mux)
	logs := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	installClient := ghclient.NewInstallationClient(client, logger)
	installClient.SetConfigDirHints(cfg)
	h := NewHandler(api.New(&emptyStorage{}, cfg, nil, logger), &fakeClientFactory{client: installClient}, nil, logger)
	t.Cleanup(func() { h.DrainInProcessWebhookWork(t.Context()) })
	return fleetLeader{h: h, mux: mux, comments: comments, reactions: reactions, logs: logs}
}

// comment delivers a PR comment to the leader's webhook endpoint and waits for
// the work it dispatched to finish, so every reply it would post has been
// posted.
func (l fleetLeader) comment(t *testing.T, body string) {
	t.Helper()
	rr := httptest.NewRecorder()
	l.h.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{comment: body, isPR: true}, nil))
	require.Equal(t, http.StatusOK, rr.Code)
	ctx, cancel := context.WithTimeout(t.Context(), fleetLeaderWorkDeadline)
	defer cancel()
	l.h.DrainInProcessWebhookWork(ctx)
	require.NoError(t, ctx.Err(), "the leader's webhook work did not finish")
}

// syncBuffer is a bytes.Buffer safe for a logger writing from the handler's
// goroutines while the test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// A repository can have one aggregate leader per environment, each with its
// own registry. Every leader receives an unscoped command, and each one that
// lacks the PR's database in its own registry would reach the same "not
// registered" conclusion, so only the leader serving the first environment in
// environment_order answers. A -e command is answered by the leader serving
// that environment, and a single leader serving every environment always
// answers. The reply speaks for the leader's own registry and names it.
func TestUnregisteredDatabaseAnsweredOncePerFleet(t *testing.T) {
	const ledgerDir = "services/ledger/schema"

	t.Run("unscoped plan registered on neither gets one reply from the first environment's leader", func(t *testing.T) {
		staging := newFleetLeader(t, splitLeaderConfig("staging", nil))
		production := newFleetLeader(t, splitLeaderConfig("production", nil))
		for _, leader := range []fleetLeader{staging, production} {
			serveSchemaConfigForDatabaseUnder(t, leader.mux, "ledger", ledgerDir)
		}

		staging.comment(t, "schemabot plan")
		production.comment(t, "schemabot plan")

		body := requireComment(t, staging.comments, "staging leader's Database Not Registered")
		assert.Contains(t, body, "## ⚠️ Database Not Registered\n")
		assert.Contains(t, body, "**Database**: `ledger` | **Schema directory**: `services/ledger/schema` | **Deployment**: `staging`")
		assert.Contains(t, body, "The staging SchemaBot deployment has no `ledger` entry under `databases`")
		assert.Empty(t, staging.comments, "the staging leader replies once")
		require.Len(t, staging.reactions, 1, "the leader that answers acknowledges")
		assert.Equal(t, "eyes", <-staging.reactions)

		assert.Empty(t, production.comments, "the production leader leaves the reply to the staging leader")
		assert.Empty(t, production.reactions, "a leader that does not answer does not acknowledge")
		assert.Contains(t, production.logs.String(), "database not registered on this leader; the leader serving the responder environment answers the command")
		assert.Contains(t, production.logs.String(), "responder_environment=staging")
	})

	t.Run("unscoped plan registered on staging only is planned by staging and ignored by production", func(t *testing.T) {
		staging := newFleetLeader(t, splitLeaderConfig("staging", ledgerOnStaging()))
		production := newFleetLeader(t, splitLeaderConfig("production", nil))
		for _, leader := range []fleetLeader{staging, production} {
			serveSchemaConfigForDatabaseUnder(t, leader.mux, "ledger", ledgerDir)
		}

		staging.comment(t, "schemabot plan")
		production.comment(t, "schemabot plan")

		require.Len(t, staging.reactions, 1, "the staging leader owns ledger and acts on the command")
		assert.Equal(t, "eyes", <-staging.reactions)
		for len(staging.comments) > 0 {
			assert.NotContains(t, <-staging.comments, "Not Registered", "the staging leader registers ledger")
		}
		assert.Empty(t, production.comments, "the production leader is not the one to answer an unscoped command")
		assert.Empty(t, production.reactions)
	})

	t.Run("plan -e production registered on neither gets one reply from the production leader", func(t *testing.T) {
		staging := newFleetLeader(t, splitLeaderConfig("staging", nil))
		production := newFleetLeader(t, splitLeaderConfig("production", nil))
		for _, leader := range []fleetLeader{staging, production} {
			serveSchemaConfigForDatabaseUnder(t, leader.mux, "ledger", ledgerDir)
		}

		staging.comment(t, "schemabot plan -e production")
		production.comment(t, "schemabot plan -e production")

		assert.Empty(t, staging.comments, "environment routing keeps a production command from the staging leader")
		assert.Empty(t, staging.reactions)
		body := requireComment(t, production.comments, "production leader's Database Not Registered")
		assert.Contains(t, body, "**Database**: `ledger` | **Schema directory**: `services/ledger/schema` | **Deployment**: `production`")
		assert.Contains(t, body, "The production SchemaBot deployment has no `ledger` entry under `databases`")
		assert.Empty(t, production.comments, "the production leader replies once")
	})

	t.Run("single leader serving every environment answers", func(t *testing.T) {
		leader := newFleetLeader(t, aggregateLeaderConfig())
		serveSchemaConfigForDatabaseUnder(t, leader.mux, "ledger", ledgerDir)

		leader.comment(t, "schemabot plan")

		body := requireComment(t, leader.comments, "single leader's Database Not Registered")
		assert.Contains(t, body, "## ⚠️ Database Not Registered\n")
		assert.Contains(t, body, "This SchemaBot deployment has no `ledger` entry under `databases`")
		assert.Empty(t, leader.comments, "the leader replies once")
	})
}

// Auto-plan on an aggregate repo leaves a schema config it does not manage to
// the deployment that does, so the unmanaged-config notice stays a log line on
// every deployment there, leader or participant. A leader cannot see a sibling
// leader's registry, so a config it dropped may be one a leader serving another
// environment plans.
func TestNotifyUnmanagedDiscoveredConfigsOnAggregateRepo(t *testing.T) {
	ledger := []ghclient.DiscoveredConfig{{
		Config:    &ghclient.SchemabotConfig{Database: "ledger", Type: "mysql"},
		SchemaDir: "services/ledger/schema",
	}}
	notify := func(h *Handler, managed []ghclient.DiscoveredConfig) {
		h.notifyUnmanagedDiscoveredConfigs("octocat/hello-world", 1, 12345, "pull_request", "abc123", func() bool { return true }, ledger, managed)
	}

	t.Run("split-environment leaders post no notice for a database one of them registers", func(t *testing.T) {
		staging, _, stagingComments := newFanOutSkipHandler(t, splitLeaderConfig("staging", ledgerOnStaging()))
		production, _, productionComments := newFanOutSkipHandler(t, splitLeaderConfig("production", nil))

		notify(staging, ledger)
		notify(production, nil)

		assert.Empty(t, stagingComments, "the staging leader plans ledger")
		assert.Empty(t, productionComments, "the production leader cannot see that the staging leader registers ledger")
	})

	t.Run("participant posts no notice", func(t *testing.T) {
		h, _, comments := newFanOutSkipHandler(t, aggregateParticipantConfig())

		notify(h, nil)

		assert.Empty(t, comments)
	})

	t.Run("non-aggregate repo notices a dropped config", func(t *testing.T) {
		h, _, comments := newFanOutSkipHandler(t, nonAggregateConfig())

		notify(h, nil)

		body := requireComment(t, comments, "unmanaged schema config notice")
		assert.Contains(t, body, "Schema Changes Not Managed by SchemaBot")
		assert.Contains(t, body, "`services/ledger/schema`")
	})
}

// A database-scoped plan still fans out when it does not name a tenant. If a
// participant's exhaustive local discovery cannot find that database, only
// the leader may publish Database Not Found as the fleet-authoritative answer.
func TestMultiEnvPlanDatabaseNotFoundParticipantDefersToLeader(t *testing.T) {
	barePlan := func(h *Handler) {
		h.handleMultiEnvPlan("octocat/hello-world", 1, "orders", "", 12345, "hubot", false, 0, true, 0)
	}

	t.Run("participant stays silent", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateParticipantConfig())
		serveSchemaConfigForDatabase(t, mux, "inventory")
		serveCompleteRootConfigTree(t, mux)

		barePlan(h)

		assert.Empty(t, comments, "a participant database discovery miss must defer silently to the leader")
	})

	t.Run("leader reports database not found", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())
		serveSchemaConfigForDatabase(t, mux, "inventory")
		serveCompleteRootConfigTree(t, mux)

		barePlan(h)

		body := requireComment(t, comments, "database-not-found leader plan error")
		assert.Contains(t, body, "Database Not Found")
		assert.Contains(t, body, "orders")
	})
}

// `apply -d` and `apply-confirm -d` fan out exactly like a database-scoped
// plan: naming a database does not name a deployment. So a participant whose
// exhaustive local discovery cannot resolve that database defers, and only the
// leader publishes Database Not Found as the fleet-authoritative answer —
// otherwise every participant on the repo posts the same failure beside it.
func TestDatabaseScopedApplyDatabaseNotFoundParticipantDefersToLeader(t *testing.T) {
	commands := []struct {
		name string
		run  func(*Handler)
	}{
		{"apply", func(h *Handler) {
			h.handleApplyCommand("octocat/hello-world", 1, "staging", "orders", 12345, "hubot",
				CommandResult{Action: action.Apply})
		}},
		{"apply-confirm", func(h *Handler) {
			h.handleApplyConfirmCommand("octocat/hello-world", 1, "staging", "orders", 12345, "hubot",
				CommandResult{Action: action.ApplyConfirm})
		}},
	}

	for _, command := range commands {
		t.Run(command.name, func(t *testing.T) {
			t.Run("participant stays silent", func(t *testing.T) {
				h, mux, comments := newFanOutSkipHandler(t, aggregateParticipantConfig())
				serveSchemaConfigForDatabase(t, mux, "inventory")
				serveCompleteRootConfigTree(t, mux)

				command.run(h)

				assert.Empty(t, comments, "a participant database discovery miss must defer silently to the leader")
			})

			t.Run("leader reports database not found", func(t *testing.T) {
				h, mux, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())
				serveSchemaConfigForDatabase(t, mux, "inventory")
				serveCompleteRootConfigTree(t, mux)

				command.run(h)

				body := requireComment(t, comments, "database-not-found leader "+command.name+" error")
				assert.Contains(t, body, "Database Not Found")
				assert.Contains(t, body, "orders")
			})
		})
	}
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

// On an aggregate repo, an unscoped `unlock` fans out to every deployment, but
// locks live in each tenant's own storage. A deployment holding no locks for
// the PR stays silent so only the deployment with locks to release answers —
// it must not post "No Locks Found" next to another tenant's real release. A
// -t-scoped unlock and a non-aggregate repo still get the comment.
func TestUnscopedUnlockNoLocksStaysSilentOnAggregateRepo(t *testing.T) {
	unlockResult := func(tenant string) CommandResult {
		return CommandResult{Action: action.Unlock, Tenant: tenant}
	}

	t.Run("unscoped unlock on aggregate repo stays silent", func(t *testing.T) {
		h, _, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())

		h.handleUnlockCommand("octocat/hello-world", 1, 12345, "hubot", unlockResult(""))

		assert.Empty(t, comments, "deployment without locks must not post No Locks Found on an unscoped fan-out unlock")
	})

	t.Run("unscoped unlock on participant deployment stays silent", func(t *testing.T) {
		h, _, comments := newFanOutSkipHandler(t, aggregateParticipantConfig())

		h.handleUnlockCommand("octocat/hello-world", 1, 12345, "hubot", unlockResult(""))

		assert.Empty(t, comments, "a participant without locks must not post No Locks Found on an unscoped fan-out unlock")
	})

	t.Run("tenant-scoped unlock still posts no locks found", func(t *testing.T) {
		h, _, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())

		h.handleUnlockCommand("octocat/hello-world", 1, 12345, "hubot", unlockResult("tenant-b"))

		body := requireComment(t, comments, "no-locks unlock comment")
		assert.Contains(t, body, "No Locks Found")
	})

	t.Run("non-aggregate repo still posts no locks found", func(t *testing.T) {
		h, _, comments := newFanOutSkipHandler(t, nonAggregateConfig())

		h.handleUnlockCommand("octocat/hello-world", 1, 12345, "hubot", unlockResult(""))

		body := requireComment(t, comments, "no-locks unlock comment")
		assert.Contains(t, body, "No Locks Found")
	})
}

// On an aggregate repo, an unscoped lifecycle control command (stop, cancel,
// cutover, ...) fans out to every deployment, but the target apply lives in
// exactly one tenant's storage. A deployment whose storage has no such apply
// stays silent so only the owning deployment answers — it must not post
// "Apply not found" for another tenant's apply. A -t-scoped command and a
// non-aggregate repo still get the comment.
func TestUnscopedControlUnknownApplyStaysSilentOnAggregateRepo(t *testing.T) {
	stopResult := func(tenant string) CommandResult {
		return CommandResult{Action: action.Stop, ApplyID: "apply_a1b2c3", Environment: "staging", Tenant: tenant}
	}

	t.Run("unscoped stop on aggregate repo stays silent", func(t *testing.T) {
		h, _, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())

		h.handleStopCommand("octocat/hello-world", 1, 12345, "hubot", stopResult(""))

		assert.Empty(t, comments, "deployment without the apply must not post Apply not found on an unscoped fan-out control command")
	})

	t.Run("unscoped stop on participant deployment stays silent", func(t *testing.T) {
		h, _, comments := newFanOutSkipHandler(t, aggregateParticipantConfig())

		h.handleStopCommand("octocat/hello-world", 1, 12345, "hubot", stopResult(""))

		assert.Empty(t, comments, "a participant without the apply must not post Apply not found on an unscoped fan-out control command")
	})

	t.Run("tenant-scoped stop still posts apply not found", func(t *testing.T) {
		h, _, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())

		h.handleStopCommand("octocat/hello-world", 1, 12345, "hubot", stopResult("tenant-b"))

		body := requireComment(t, comments, "apply-not-found control comment")
		assert.Contains(t, body, "Apply not found")
		assert.Contains(t, body, "apply_a1b2c3")
	})

	t.Run("non-aggregate repo still posts apply not found", func(t *testing.T) {
		h, _, comments := newFanOutSkipHandler(t, nonAggregateConfig())

		h.handleStopCommand("octocat/hello-world", 1, 12345, "hubot", stopResult(""))

		body := requireComment(t, comments, "apply-not-found control comment")
		assert.Contains(t, body, "Apply not found")
		assert.Contains(t, body, "apply_a1b2c3")
	})
}

// A control command without an apply ID is a usage error every deployment can
// detect from the comment text alone, so on an unscoped fan-out participants
// defer the reply to the leader, which posts it exactly once. A -t-scoped
// command and a non-aggregate repo answer directly as the single addressee.
func TestControlMissingApplyIDDefersToLeader(t *testing.T) {
	missingID := func(tenant string) CommandResult {
		return CommandResult{Action: action.Stop, Environment: "staging", Tenant: tenant}
	}

	t.Run("participant stays silent on the unscoped usage error", func(t *testing.T) {
		h, _, comments := newFanOutSkipHandler(t, aggregateParticipantConfig())

		h.handleStopCommand("octocat/hello-world", 1, 12345, "hubot", missingID(""))

		assert.Empty(t, comments, "a participant must defer the missing-apply-id reply to the leader")
	})

	t.Run("leader posts the usage error once", func(t *testing.T) {
		h, _, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())

		h.handleStopCommand("octocat/hello-world", 1, 12345, "hubot", missingID(""))

		body := requireComment(t, comments, "missing-apply-id control comment")
		assert.Contains(t, body, "Missing Apply ID")
		assert.Contains(t, body, "schemabot stop <apply-id> -e <environment>")
	})

	t.Run("tenant-scoped command gets the usage error from the addressee", func(t *testing.T) {
		h, _, comments := newFanOutSkipHandler(t, aggregateParticipantConfig())

		h.handleStopCommand("octocat/hello-world", 1, 12345, "hubot", missingID("tenant-b"))

		body := requireComment(t, comments, "missing-apply-id control comment")
		assert.Contains(t, body, "Missing Apply ID")
	})
}

// A rollback command without an apply ID is a usage error every deployment can
// detect from the comment text alone, so on an unscoped fan-out participants
// defer the reply to the leader, which posts it exactly once. A -t-scoped
// command answers directly as the single addressee.
func TestRollbackMissingApplyIDDefersToLeader(t *testing.T) {
	missingID := func(tenant string) CommandResult {
		return CommandResult{Action: action.Rollback, Environment: "staging", Tenant: tenant}
	}

	t.Run("participant stays silent on the unscoped usage error", func(t *testing.T) {
		h, _, comments := newFanOutSkipHandler(t, aggregateParticipantConfig())

		h.handleRollbackCommand("octocat/hello-world", 1, 12345, "hubot", missingID(""))

		assert.Empty(t, comments, "a participant must defer the missing-apply-id reply to the leader")
	})

	t.Run("leader posts the usage error once", func(t *testing.T) {
		h, _, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())

		h.handleRollbackCommand("octocat/hello-world", 1, 12345, "hubot", missingID(""))

		body := requireComment(t, comments, "missing-apply-id rollback comment")
		assert.Contains(t, body, "Missing Apply ID")
		assert.Contains(t, body, "schemabot rollback <apply-id> -e <environment>")
	})

	t.Run("tenant-scoped command gets the usage error from the addressee", func(t *testing.T) {
		h, _, comments := newFanOutSkipHandler(t, aggregateParticipantConfig())

		h.handleRollbackCommand("octocat/hello-world", 1, 12345, "hubot", missingID("tenant-b"))

		body := requireComment(t, comments, "missing-apply-id rollback comment")
		assert.Contains(t, body, "Missing Apply ID")
	})
}

// On an aggregate repo, an -e value this deployment does not recognize may be
// a perfectly valid environment served by a sibling deployment — a
// participant's config holds only its own slice of the fleet's environments.
// A participant therefore defers silently instead of posting "Invalid
// Environment", even when the command names its own tenant, since the same
// tenant name can be served in other environments by sibling deployments. The
// leader, whose environment order spans the fleet, still rejects a genuinely
// unknown value exactly once, and a non-aggregate deployment keeps rejecting
// directly.
func TestUnknownEnvironmentDefersOnAggregateParticipant(t *testing.T) {
	serveUnknownEnvCommand := func(t *testing.T, cfg *api.ServerConfig, comment string) (*httptest.ResponseRecorder, chan string) {
		t.Helper()
		cfg.AllowedEnvironments = []string{"production"}
		h, _, comments := newFanOutSkipHandler(t, cfg)

		req := buildWebhookRequest(t, webhookPayloadOpts{comment: comment, isPR: true}, nil)
		rr := httpResponseRecorder()
		h.ServeHTTP(rr, req)
		require.Equal(t, http.StatusOK, rr.Code)
		return rr, comments
	}

	// The participant declares only its own slice of the fleet's environments,
	// so the sibling's environment is neither allowed nor known here.
	participantConfig := func() *api.ServerConfig {
		cfg := aggregateParticipantConfig()
		cfg.Tenant = "alpha"
		cfg.EnvironmentOrder = []string{"production"}
		return cfg
	}

	t.Run("tenant-scoped command for a sibling environment defers silently", func(t *testing.T) {
		rr, comments := serveUnknownEnvCommand(t, participantConfig(), "schemabot apply -e staging --tenant alpha")

		assert.Contains(t, rr.Body.String(), "environment deferred to sibling deployments")
		assert.Empty(t, comments, "a participant must not reject an environment a sibling deployment may serve")
	})

	t.Run("unscoped command with an unrecognized environment defers silently", func(t *testing.T) {
		rr, comments := serveUnknownEnvCommand(t, participantConfig(), "schemabot apply -e staging")

		assert.Contains(t, rr.Body.String(), "environment deferred to sibling deployments")
		assert.Empty(t, comments, "a participant must not reject an environment a sibling deployment may serve")
	})

	t.Run("leader still rejects an unknown environment once", func(t *testing.T) {
		rr, comments := serveUnknownEnvCommand(t, aggregateLeaderConfig(), "schemabot apply -e prodction")

		assert.Contains(t, rr.Body.String(), "unknown environment")
		body := requireComment(t, comments, "invalid environment comment")
		assert.Contains(t, body, "Invalid Environment")
		assert.Contains(t, body, "`production`")
	})

	t.Run("non-aggregate deployment still rejects an unknown environment", func(t *testing.T) {
		rr, comments := serveUnknownEnvCommand(t, nonAggregateConfig(), "schemabot apply -e prodction")

		assert.Contains(t, rr.Body.String(), "unknown environment")
		body := requireComment(t, comments, "invalid environment comment")
		assert.Contains(t, body, "Invalid Environment")
	})
}

// A malformed -e value (for example a flag glued onto the value) is a usage
// error every deployment can detect from the comment text alone, so on an
// aggregate repo participants defer the reply to the leader, which posts it
// exactly once. A -t-scoped command answers directly as the single addressee.
func TestMalformedEnvironmentDefersToLeader(t *testing.T) {
	serveMalformedEnvCommand := func(t *testing.T, cfg *api.ServerConfig, comment string) (*httptest.ResponseRecorder, chan string) {
		t.Helper()
		h, _, comments := newFanOutSkipHandler(t, cfg)

		req := buildWebhookRequest(t, webhookPayloadOpts{comment: comment, isPR: true}, nil)
		rr := httpResponseRecorder()
		h.ServeHTTP(rr, req)
		require.Equal(t, http.StatusOK, rr.Code)
		return rr, comments
	}

	t.Run("participant stays silent on the unscoped usage error", func(t *testing.T) {
		rr, comments := serveMalformedEnvCommand(t, aggregateParticipantConfig(), "schemabot apply -e production--allow-unsafe")

		assert.Contains(t, rr.Body.String(), "usage error deferred to leader")
		assert.Empty(t, comments, "a participant must defer the malformed-environment reply to the leader")
	})

	t.Run("leader posts the usage error once", func(t *testing.T) {
		rr, comments := serveMalformedEnvCommand(t, aggregateLeaderConfig(), "schemabot apply -e production--allow-unsafe")

		assert.Contains(t, rr.Body.String(), "invalid environment value")
		body := requireComment(t, comments, "invalid environment comment")
		assert.Contains(t, body, "Invalid Environment")
	})

	t.Run("tenant-scoped command gets the usage error from the addressee", func(t *testing.T) {
		cfg := aggregateParticipantConfig()
		cfg.Tenant = "alpha"
		rr, comments := serveMalformedEnvCommand(t, cfg, "schemabot apply -e production--allow-unsafe --tenant alpha")

		assert.Contains(t, rr.Body.String(), "invalid environment value")
		body := requireComment(t, comments, "invalid environment comment")
		assert.Contains(t, body, "Invalid Environment")
	})
}

// A command that requires -e but omits it is a usage error every deployment
// can detect from the comment text alone, so on an aggregate repo participants
// defer the reply to the leader, which posts it exactly once. A -t-scoped
// command answers directly as the single addressee.
func TestMissingEnvironmentDefersToLeader(t *testing.T) {
	serveMissingEnvCommand := func(t *testing.T, cfg *api.ServerConfig, comment string) (*httptest.ResponseRecorder, chan string) {
		t.Helper()
		h, _, comments := newFanOutSkipHandler(t, cfg)

		req := buildWebhookRequest(t, webhookPayloadOpts{comment: comment, isPR: true}, nil)
		rr := httpResponseRecorder()
		h.ServeHTTP(rr, req)
		require.Equal(t, http.StatusOK, rr.Code)
		return rr, comments
	}

	t.Run("participant stays silent on the unscoped usage error", func(t *testing.T) {
		rr, comments := serveMissingEnvCommand(t, aggregateParticipantConfig(), "schemabot rollback apply-123")

		assert.Contains(t, rr.Body.String(), "usage error deferred to leader")
		assert.Empty(t, comments, "a participant must defer the missing-environment reply to the leader")
	})

	t.Run("leader posts the usage error once", func(t *testing.T) {
		rr, comments := serveMissingEnvCommand(t, aggregateLeaderConfig(), "schemabot rollback apply-123")

		assert.Contains(t, rr.Body.String(), "missing environment flag")
		body := requireComment(t, comments, "missing environment comment")
		assert.Contains(t, body, "Missing Environment")
	})

	t.Run("tenant-scoped command gets the usage error from the addressee", func(t *testing.T) {
		cfg := aggregateParticipantConfig()
		cfg.Tenant = "alpha"
		rr, comments := serveMissingEnvCommand(t, cfg, "schemabot rollback apply-123 --tenant alpha")

		assert.Contains(t, rr.Body.String(), "missing environment flag")
		body := requireComment(t, comments, "missing environment comment")
		assert.Contains(t, body, "Missing Environment")
	})

	t.Run("non-aggregate repo still posts the usage error", func(t *testing.T) {
		rr, comments := serveMissingEnvCommand(t, nonAggregateConfig(), "schemabot rollback")

		assert.Contains(t, rr.Body.String(), "missing rollback arguments")
		body := requireComment(t, comments, "missing arguments comment")
		assert.Contains(t, body, "Missing Arguments")
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
	t.Run("multi-env plan on unowned database does not react on a participant", func(t *testing.T) {
		h, mux, _ := newFanOutSkipHandler(t, aggregateParticipantConfig())
		serveSchemaConfigForDatabase(t, mux, "orders")
		reactions := registerReactionRecorder(t, mux)

		h.handleMultiEnvPlan("octocat/hello-world", 1, "", "", 12345, "hubot", false, 0, true, 42)

		select {
		case <-reactions:
			t.Fatal("a deployment that cannot resolve the database in its registry must not acknowledge")
		case <-time.After(100 * time.Millisecond):
		}
	})

	t.Run("silently skipping deployment does not react", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())
		serveSchemaConfigForDatabaseUnder(t, mux, "orders", "tenant-b/schema")
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

	// The leader that answers for a database it has not registered is acting on
	// the command, so its answer carries the acknowledgment: the user sees the
	// reaction and the comment together, never a comment from nowhere.
	t.Run("leader answering for an unregistered database reacts", func(t *testing.T) {
		for name, run := range map[string]func(h *Handler){
			"apply": func(h *Handler) {
				h.handleApplyCommand("octocat/hello-world", 1, "staging", "", 12345, "hubot",
					CommandResult{Action: action.Apply, CommentID: 42})
			},
			"apply-confirm": func(h *Handler) {
				h.handleApplyConfirmCommand("octocat/hello-world", 1, "staging", "", 12345, "hubot",
					CommandResult{Action: action.ApplyConfirm, CommentID: 42})
			},
			"multi-env plan": func(h *Handler) {
				h.handleMultiEnvPlan("octocat/hello-world", 1, "", "", 12345, "hubot", false, 0, true, 42)
			},
		} {
			t.Run(name, func(t *testing.T) {
				h, mux, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())
				serveSchemaConfigForDatabase(t, mux, "orders")
				reactions := registerReactionRecorder(t, mux)

				run(h)

				body := requireComment(t, comments, "database-not-registered answer")
				assert.Contains(t, body, "Database Not Registered")
				select {
				case content := <-reactions:
					assert.Equal(t, "eyes", content)
				case <-time.After(2 * time.Second):
					t.Fatal("the leader that answers must acknowledge the command")
				}
			})
		}
	})
}
