//go:build integration

package webhook

import (
	"database/sql"
	"encoding/json"
	"html"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"slices"
	"strings"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

const (
	sharedSchemaUnshardedDB = "webhook_shared_shop"
	sharedSchemaShardedDB   = "webhook_shared_shop_sharded"
)

func sharedSchemaTable(name string) string {
	return "CREATE TABLE `" + name + "` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
}

// sharedSchemaHarness serves a repository holding two databases that split
// one schema: the unsharded database owns `sequences`, which lives in shard
// 0's schema, and the sharded database owns `orders` in every shard's schema.
// In production both route shard 0's schema to the same target; in staging
// each has a target of its own.
type sharedSchemaHarness struct {
	store      storage.Storage
	handler    *Handler
	github     *planFlowResult
	mux        *http.ServeMux
	production *namespaceSelectingTernClient
}

// newSharedSchemaHarness lays the repository out with the given unsharded
// config and extra sharded-database files. Every file the default branch
// already holds is listed in onBase; the rest are what the pull request adds.
// configure, when given, adjusts the server config before the service starts.
func newSharedSchemaHarness(t *testing.T, unshardedConfig string, extraFiles map[string]string, onBase []string, configure ...func(*api.ServerConfig)) *sharedSchemaHarness {
	t.Helper()
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	schemabotDB, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	// Redundant close for early-exit leak safety: svc.Close below owns the
	// handle, so this close sees an already-closed DB and discards the error.
	t.Cleanup(func() { _ = schemabotDB.Close() })
	_, err = schemabotDB.ExecContext(ctx, "DELETE FROM checks WHERE repository = 'octocat/hello-world' AND pull_request = 1")
	require.NoError(t, err)
	for _, db := range []string{sharedSchemaUnshardedDB, sharedSchemaShardedDB} {
		_, err = schemabotDB.ExecContext(ctx, "DELETE FROM plans WHERE database_name = ?", db)
		require.NoError(t, err)
	}
	st := mysqlstore.New(schemabotDB)

	serverConfig := &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			sharedSchemaUnshardedDB: {Type: storage.DatabaseTypeMySQL, Environments: map[string]api.EnvironmentConfig{
				"staging": {Deployment: "us", Target: "shop-staging-unsharded"},
				"production": {Deployment: "us", Targets: []api.TargetEntry{
					{Target: "shop-001", Namespaces: []string{"shop_0"}},
				}},
			}},
			sharedSchemaShardedDB: {Type: storage.DatabaseTypeMySQL, Environments: map[string]api.EnvironmentConfig{
				"staging": {Deployment: "us", Target: "shop-staging-sharded"},
				"production": {Deployment: "us", Targets: []api.TargetEntry{
					{Target: "shop-001", Namespaces: []string{"shop_0"}},
					{Target: "shop-002", Namespaces: []string{"shop_1"}},
				}},
			}},
		},
		TernDeployments: api.TernConfig{"us": {"staging": "tern-us-staging:9090", "production": "tern-us:9090"}},
		Repos:           map[string]api.RepoConfig{"octocat/hello-world": {}},
	}
	for _, apply := range configure {
		apply(serverConfig)
	}
	production := &namespaceSelectingTernClient{planIDPrefix: "plan-production"}
	svc := api.New(st, serverConfig, map[string]tern.Client{
		"us/staging":    &namespaceSelectingTernClient{planIDPrefix: "plan-staging"},
		"us/production": production,
	}, logger)
	t.Cleanup(func() { _ = svc.Close() })

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	configs := map[string]string{
		"schema/shop/schemabot.yaml":         unshardedConfig,
		"schema/shop_sharded/schemabot.yaml": "database: " + sharedSchemaShardedDB + "\ntype: mysql\nignore_tables:\n  - sequences\n",
	}
	files := map[string]string{
		"shop/shop_0/sequences.sql":      sharedSchemaTable("sequences"),
		"shop_sharded/shop_0/orders.sql": sharedSchemaTable("orders"),
		"shop_sharded/shop_1/orders.sql": sharedSchemaTable("orders"),
	}
	maps.Copy(files, extraFiles)
	result := setupFakeGitHubForPlanWithConfigs(t, mux, files, configs, "shop", nil)
	result.baseHolds(onBase...)

	installClient := ghclient.NewInstallationClientWithSlug(client, logger, "schemabot")
	installClient.SetConfigDirHints(serverConfig)
	return &sharedSchemaHarness{
		store:      st,
		handler:    NewHandler(svc, &fakeClientFactory{client: installClient}, nil, logger, WithInsecureSkipSignatureVerification()),
		github:     result,
		mux:        mux,
		production: production,
	}
}

const sharedSchemaUnshardedConfig = "database: " + sharedSchemaUnshardedDB + "\ntype: mysql\nignore_tables:\n  - orders\n"

// sharedSchemaBaseFiles is every file of the repository before the pull
// request: both configs and the tables each database already declares.
var sharedSchemaBaseFiles = []string{
	"schema/shop/schemabot.yaml",
	"schema/shop_sharded/schemabot.yaml",
	"schema/shop/shop_0/sequences.sql",
	"schema/shop_sharded/shop_0/orders.sql",
	"schema/shop_sharded/shop_1/orders.sql",
}

func (p *sharedSchemaHarness) open(t *testing.T) string {
	t.Helper()
	rr := httptest.NewRecorder()
	p.handler.ServeHTTP(rr, buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "opened"}, nil))
	require.Equal(t, http.StatusOK, rr.Code)
	return p.nextComment(t, "the auto-plan comment")
}

// openAndAwaitAggregate opens the pull request and waits for the aggregate
// check run, for a plan that posts no comment.
func (p *sharedSchemaHarness) openAndAwaitAggregate(t *testing.T) *checkRunCapture {
	t.Helper()
	rr := httptest.NewRecorder()
	p.handler.ServeHTTP(rr, buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "opened"}, nil))
	require.Equal(t, http.StatusOK, rr.Code)
	deadline := time.After(webhookIntegrationPollDeadline)
	for {
		select {
		case cr := <-p.github.checkRuns:
			if cr.Name == aggregateCheckName && cr.Status == checkStatusCompleted {
				return &cr
			}
		case <-deadline:
			require.FailNow(t, "timed out waiting for the aggregate check run")
			return nil
		}
	}
}

func (p *sharedSchemaHarness) comment(t *testing.T, command string) string {
	t.Helper()
	rr := httptest.NewRecorder()
	p.handler.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{comment: command, isPR: true}, nil))
	require.Equal(t, http.StatusOK, rr.Code)
	return p.nextComment(t, "the reply to "+command)
}

func (p *sharedSchemaHarness) nextComment(t *testing.T, what string) string {
	t.Helper()
	select {
	case body := <-p.github.comments:
		return html.UnescapeString(body)
	case <-time.After(webhookIntegrationPollDeadline):
		require.FailNow(t, "timed out waiting for "+what)
		return ""
	}
}

func (p *sharedSchemaHarness) checksByEnv(t *testing.T, database string) map[string]*storage.Check {
	t.Helper()
	checks, err := p.store.Checks().GetByPR(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	byEnv := map[string]*storage.Check{}
	for _, c := range checks {
		if c.DatabaseName == database {
			byEnv[c.Environment] = c
		}
	}
	return byEnv
}

func (p *sharedSchemaHarness) lastAggregate() *checkRunCapture {
	var aggregate *checkRunCapture
	for {
		select {
		case cr := <-p.github.checkRuns:
			if cr.Name == aggregateCheckName {
				aggregate = &cr
			}
			continue
		default:
		}
		return aggregate
	}
}

// A pull request adds a sharded table and changes nothing else. Its own plan
// would pass: the sharded database declares the table and withholds the
// unsharded one. But the unsharded database shares shard 0's schema in
// production and does not ignore the new table, so once the table exists
// every plan of the unsharded database would propose dropping it. Production
// fails at review time, naming the entry to add and the file to add it to;
// staging, where the two databases have targets of their own, passes.
func TestE2EAutoPlanRefusesSharedSchemaMissingIgnoreEntry(t *testing.T) {
	p := newSharedSchemaHarness(t, sharedSchemaUnshardedConfig, map[string]string{
		"shop_sharded/shop_0/refunds.sql": sharedSchemaTable("refunds"),
		"shop_sharded/shop_1/refunds.sql": sharedSchemaTable("refunds"),
	}, sharedSchemaBaseFiles)

	body := p.open(t)
	assert.Contains(t, body, `"`+sharedSchemaShardedDB+`" declares "refunds" in namespace "shop_0", which "`+sharedSchemaUnshardedDB+`" shares on target "shop-001"`)
	assert.Contains(t, body, `Add "refunds" to ignore_tables in schema/shop/schemabot.yaml`)
	assert.Nil(t, p.production.planNamespaces, "production plans nothing while the ignore lists disagree")

	byEnv := p.checksByEnv(t, sharedSchemaShardedDB)
	require.Contains(t, byEnv, "staging")
	assert.Equal(t, checkConclusionSuccess, byEnv["staging"].Conclusion)
	require.Contains(t, byEnv, "production", "the refused environment stores a failing check")
	assert.Equal(t, checkConclusionFailure, byEnv["production"].Conclusion)
	aggregate := p.lastAggregate()
	require.NotNil(t, aggregate, "the aggregate check run is published")
	assert.Equal(t, checkConclusionFailure, aggregate.Conclusion, "staging passing alone must not pass the aggregate")
}

// The same pull request also lists the new table in the unsharded database's
// ignore_tables. The lists agree at the pull request's head, so production
// plans and its check passes. The plan finds no changes, so no comment is
// posted; the checks are the outcome.
func TestE2EAutoPlanPassesSharedSchemaWhenTheEntryLandsTogether(t *testing.T) {
	onBase := []string{
		"schema/shop_sharded/schemabot.yaml",
		"schema/shop/shop_0/sequences.sql",
		"schema/shop_sharded/shop_0/orders.sql",
		"schema/shop_sharded/shop_1/orders.sql",
	}
	p := newSharedSchemaHarness(t, sharedSchemaUnshardedConfig+"  - refunds\n", map[string]string{
		"shop_sharded/shop_0/refunds.sql": sharedSchemaTable("refunds"),
		"shop_sharded/shop_1/refunds.sql": sharedSchemaTable("refunds"),
	}, onBase)

	aggregate := p.openAndAwaitAggregate(t)
	assert.Equal(t, checkConclusionSuccess, aggregate.Conclusion)
	assert.Equal(t, []string{"shop_0"}, p.production.planNamespaces, "production plans shard 0's schema once the lists agree")
	byEnv := p.checksByEnv(t, sharedSchemaShardedDB)
	require.Contains(t, byEnv, "production")
	assert.Equal(t, checkConclusionSuccess, byEnv["production"].Conclusion)
	select {
	case body := <-p.github.comments:
		assert.NotContains(t, html.UnescapeString(body), "ignore_tables in schema/")
	default:
	}
}

// A plan command of production alone takes its own path through the handler,
// and is refused the same way, replacing an earlier passing row. The
// disagreement is in the schema files rather than in any one target, so a
// plan narrowed to one target fails the environment's check too.
func TestE2ESingleEnvPlanRefusesSharedSchemaMissingIgnoreEntry(t *testing.T) {
	for name, command := range map[string]string{
		"every target":    "schemabot plan -e production -d " + sharedSchemaShardedDB,
		"narrowed to one": "schemabot plan -e production -d " + sharedSchemaShardedDB + " --target shop-002",
	} {
		t.Run(name, func(t *testing.T) {
			p := newSharedSchemaHarness(t, sharedSchemaUnshardedConfig, map[string]string{
				"shop_sharded/shop_0/refunds.sql": sharedSchemaTable("refunds"),
			}, sharedSchemaBaseFiles)
			require.NoError(t, p.store.Checks().Upsert(t.Context(), &storage.Check{
				Repository: "octocat/hello-world", PullRequest: 1, HeadSHA: "abc123",
				Environment: "production", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: sharedSchemaShardedDB,
				CheckRunID: 1, Status: checkStatusCompleted, Conclusion: checkConclusionSuccess,
			}))

			body := p.comment(t, command)
			assert.Contains(t, body, `Add "refunds" to ignore_tables in schema/shop/schemabot.yaml`)
			assert.Nil(t, p.production.planNamespaces, "production plans nothing while the ignore lists disagree")

			byEnv := p.checksByEnv(t, sharedSchemaShardedDB)
			require.Contains(t, byEnv, "production")
			assert.Equal(t, checkConclusionFailure, byEnv["production"].Conclusion, "the old success row is replaced by the refusal")
		})
	}
}

// The repository allows schema under schema/shop, but the server allows the
// unsharded database's own schema only from another directory, so its files in
// this pull request are not a source the server trusts for that database. The
// sharded database's production plan fails closed rather than comparing
// against files the unsharded database's own plan would refuse, and the
// failure is stored as the sharded database's own production row.
func TestE2EAutoPlanFailsClosedWhenSharedSchemaPeerSourceIsNotAuthorized(t *testing.T) {
	p := newSharedSchemaHarness(t, sharedSchemaUnshardedConfig+"  - refunds\n", map[string]string{
		"shop_sharded/shop_0/refunds.sql": sharedSchemaTable("refunds"),
	}, sharedSchemaBaseFiles, func(c *api.ServerConfig) {
		for db, dirs := range map[string][]string{
			sharedSchemaUnshardedDB: {"schema/shop_elsewhere"},
			sharedSchemaShardedDB:   {"schema/shop_sharded", "schema/shop"},
		} {
			cfg := c.Databases[db]
			cfg.AllowedRepos = []string{"octocat/hello-world"}
			cfg.AllowedDirs = dirs
			c.Databases[db] = cfg
		}
	})

	body := p.open(t)
	assert.Contains(t, body, "could not read the schema files of `"+sharedSchemaUnshardedDB+"`")
	assert.Contains(t, body, `schema path "schema/shop" is not authorized for database "`+sharedSchemaUnshardedDB+`"`)
	assert.Nil(t, p.production.planNamespaces, "production plans nothing while the peer's source is refused")

	byEnv := p.checksByEnv(t, sharedSchemaShardedDB)
	require.Contains(t, byEnv, "staging")
	assert.Equal(t, checkConclusionSuccess, byEnv["staging"].Conclusion)
	require.Contains(t, byEnv, "production", "the peer's refusal is stored as this database's failing row")
	assert.Equal(t, checkConclusionFailure, byEnv["production"].Conclusion)
}

// The unsharded database takes schema changes only from another repository,
// so the copy of its files in this one is not its source and cannot show what
// it declares. Its ignore_tables are not compared with this pull request, and
// the sharded database's production plan runs instead of failing on a
// disagreement with files no plan of the unsharded database would read.
func TestE2EAutoPlanSkipsSharedSchemaPeerManagedFromAnotherRepository(t *testing.T) {
	p := newSharedSchemaHarness(t, sharedSchemaUnshardedConfig, map[string]string{
		"shop_sharded/shop_0/refunds.sql": sharedSchemaTable("refunds"),
	}, sharedSchemaBaseFiles, func(c *api.ServerConfig) {
		for db, policy := range map[string]struct{ repos, dirs []string }{
			sharedSchemaUnshardedDB: {repos: []string{"octocat/shop-schema"}, dirs: []string{"schema/shop"}},
			sharedSchemaShardedDB:   {repos: []string{"octocat/hello-world"}, dirs: []string{"schema/shop_sharded"}},
		} {
			cfg := c.Databases[db]
			cfg.AllowedRepos = policy.repos
			cfg.AllowedDirs = policy.dirs
			c.Databases[db] = cfg
		}
	})

	aggregate := p.openAndAwaitAggregate(t)
	assert.Equal(t, checkConclusionSuccess, aggregate.Conclusion)
	assert.Equal(t, []string{"shop_0"}, p.production.planNamespaces, "production plans once the peer is known to be managed elsewhere")
	byEnv := p.checksByEnv(t, sharedSchemaShardedDB)
	require.Contains(t, byEnv, "production")
	assert.Equal(t, checkConclusionSuccess, byEnv["production"].Conclusion)
}

// The unsharded database's schema file cannot be parsed, so the check cannot
// tell which tables it declares. The sharded database's production plan fails
// closed rather than passing a pair it could not compare.
func TestE2EAutoPlanFailsClosedWhenSharedSchemaPeerCannotBeRead(t *testing.T) {
	p := newSharedSchemaHarness(t, sharedSchemaUnshardedConfig, map[string]string{
		"shop/shop_0/broken.sql":          "CREATE TABLE (",
		"shop_sharded/shop_0/refunds.sql": sharedSchemaTable("refunds"),
	}, append(slices.Clone(sharedSchemaBaseFiles), "schema/shop/shop_0/broken.sql"))

	body := p.open(t)
	assert.Contains(t, body, "split schema file shop_0/broken.sql: failed to parse SQL statements")
	assert.NotContains(t, body, "compare ignore_tables of")
	assert.Nil(t, p.production.planNamespaces, "production plans nothing while the peer cannot be compared")

	byEnv := p.checksByEnv(t, sharedSchemaShardedDB)
	require.Contains(t, byEnv, "production")
	assert.Equal(t, checkConclusionFailure, byEnv["production"].Conclusion)
}

// Both databases share shard 0's schema in staging, and the pull request adds
// a sharded table together with the unsharded database's ignore_tables entry,
// so the two agree at the pull request's head. After the branch diverged, the
// base branch changed the unsharded database's schema and nothing of the
// sharded database's. The apply is refused as stale and names the unsharded
// database's files: its ignore_tables were compared with a version the base
// branch has since replaced, so the comparison no longer holds.
func TestE2EApplyRefusesBranchMissingABaseChangeToASharedSchemaPeer(t *testing.T) {
	onBase := []string{
		"schema/shop_sharded/schemabot.yaml",
		"schema/shop/shop_0/sequences.sql",
		"schema/shop_sharded/shop_0/orders.sql",
		"schema/shop_sharded/shop_1/orders.sql",
	}
	p := newSharedSchemaHarness(t, sharedSchemaUnshardedConfig+"  - refunds\n", map[string]string{
		"shop_sharded/shop_0/refunds.sql": sharedSchemaTable("refunds"),
	}, onBase, func(c *api.ServerConfig) {
		for db, targets := range map[string][]api.TargetEntry{
			sharedSchemaUnshardedDB: {{Target: "shop-staging-001", Namespaces: []string{"shop_0"}}},
			sharedSchemaShardedDB: {
				{Target: "shop-staging-001", Namespaces: []string{"shop_0"}},
				{Target: "shop-staging-002", Namespaces: []string{"shop_1"}},
			},
		} {
			cfg := c.Databases[db]
			cfg.Environments["staging"] = api.EnvironmentConfig{Deployment: "us", Targets: targets}
			c.Databases[db] = cfg
		}
	})
	registerBaseChangeSinceDivergence(t, p.mux, p.github, "schema/shop/shop_0/sequences.sql")

	rr := httptest.NewRecorder()
	p.handler.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{comment: "schemabot apply -e staging -d " + sharedSchemaShardedDB, isPR: true}, nil))
	require.Equal(t, http.StatusOK, rr.Code)

	body := p.commentContaining(t, "Apply rejected — base schema is newer")
	assert.Contains(t, body, "newer changes to `schema/shop`, the schema of `"+sharedSchemaUnshardedDB+"`, which shares a schema with this database")
	assert.Contains(t, body, "Merge or rebase")
	assert.NotContains(t, body, "schema/shop_sharded")
}

// commentContaining waits for the next comment that contains want, skipping
// the acknowledgements a command posts before its answer.
func (p *sharedSchemaHarness) commentContaining(t *testing.T, want string) string {
	t.Helper()
	deadline := time.After(webhookIntegrationPollDeadline)
	for {
		select {
		case body := <-p.github.comments:
			if body = html.UnescapeString(body); strings.Contains(body, want) {
				return body
			}
		case <-deadline:
			require.FailNowf(t, "timed out waiting for a comment", "want a comment containing %q", want)
			return ""
		}
	}
}

// registerBaseChangeSinceDivergence moves the default branch past the pull
// request's merge base by one change to changedPath. The merge base holds the
// default branch's files with changedPath at an older blob, so the directories
// above it differ between the two commits and every other directory is the
// same tree in both.
func registerBaseChangeSinceDivergence(t *testing.T, mux *http.ServeMux, result *planFlowResult, changedPath string) {
	t.Helper()
	tip := func() string { return "base-tip-sha" }
	result.BaseTip.Store(&tip)
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/base-tip-sha...abc123", func(w http.ResponseWriter, r *http.Request) {
		if wantsDiff(r) {
			writeCommitFilesDiff(t, w, servedPRFiles(t, mux))
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(gh.CommitsComparison{
			MergeBaseCommit: &gh.RepositoryCommit{SHA: new("merge-base-sha")},
		}))
	})
	const mergeBasePrefix = "mergebase~"
	mergeBaseLevel := func(dir string) []*gh.TreeEntry {
		level := treeLevelFrom(result.baseTreeEntries(), dir, baseSubtreeSHAPrefix)
		for _, entry := range level {
			full := entry.GetPath()
			if dir != "" {
				full = dir + "/" + full
			}
			switch {
			case full == changedPath:
				entry.SHA = new(mergeBasePrefix + "blob")
			case strings.HasPrefix(changedPath, full+"/"):
				entry.SHA = new(mergeBasePrefix + strings.ReplaceAll(full, "/", "~"))
			}
		}
		return level
	}
	serveLevel := func(dir string) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			require.NoError(t, json.NewEncoder(w).Encode(gh.Tree{Entries: mergeBaseLevel(dir)}))
		}
	}
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/merge-base-sha", serveLevel(""))
	for dir := path.Dir(changedPath); dir != "."; dir = path.Dir(dir) {
		mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/"+mergeBasePrefix+strings.ReplaceAll(dir, "/", "~"), serveLevel(dir))
	}
}
