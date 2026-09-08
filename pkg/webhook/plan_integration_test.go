//go:build integration

// Plan and auto-plan webhook integration tests.

package webhook

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

func TestE2EPlanWithChanges(t *testing.T) {
	dbName := "webhook_plan_changes"
	svc := setupE2EService(t, dbName)
	dbConfig := svc.Config().Databases[dbName]
	dbConfig.AllowedRepos = []string{"octocat/hello-world"}
	dbConfig.AllowedDirs = []string{"schema"}
	svc.Config().Databases[dbName] = dbConfig

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "plan generated successfully")

	// Verify plan comment was posted
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "## Schema Change Plan")
		assert.Contains(t, body, "CREATE TABLE")
		assert.Contains(t, body, dbName)
		assert.Contains(t, body, "staging")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for plan comment")
	}

	// Verify check run was created (per-database + aggregate)
	select {
	case cr := <-result.checkRuns:
		assert.Contains(t, cr.Name, "SchemaBot")
		assert.Equal(t, "completed", cr.Status)
		assert.Equal(t, "action_required", cr.Conclusion)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for check run")
	}

	// Verify plan was persisted to SchemaBot storage
	ctx := t.Context()
	plans, err := svc.Storage().Plans().GetByPR(ctx, "octocat/hello-world", 1)
	require.NoError(t, err)
	require.NotEmpty(t, plans, "expected at least one plan record")
	// Find the plan for this database (shared storage may have data from prior tests)
	var plan *storage.Plan
	for _, p := range plans {
		if p.Database == dbName {
			plan = p
			break
		}
	}
	require.NotNil(t, plan, "expected a plan record for database %s", dbName)
	assert.Equal(t, dbName, plan.Database)
	assert.Equal(t, "mysql", plan.DatabaseType)
	assert.Equal(t, "staging", plan.Environment)
	assert.Equal(t, "octocat/hello-world", plan.Repository)
	assert.Equal(t, 1, plan.PullRequest)
	assert.Equal(t, "schema", plan.SchemaPath)
	assert.NotEmpty(t, plan.PlanIdentifier, "plan should have an identifier")
	assert.NotNil(t, plan.Namespaces, "plan should have namespace data")
	assert.NotEmpty(t, plan.Namespaces[dbName].Tables, "plan should have DDL changes")

	// Verify check record was persisted to SchemaBot storage
	check, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check, "expected a check record")
	assert.Equal(t, "octocat/hello-world", check.Repository)
	assert.Equal(t, 1, check.PullRequest)
	assert.Equal(t, "abc123", check.HeadSHA)
	assert.Equal(t, "staging", check.Environment)
	assert.Equal(t, "mysql", check.DatabaseType)
	assert.Equal(t, dbName, check.DatabaseName)
	assert.True(t, check.HasChanges, "check should indicate changes detected")
	assert.Equal(t, "completed", check.Status)
	assert.Equal(t, "action_required", check.Conclusion)
}

// TestE2EPlanUnscopedRejectsMultipleManagedDatabases verifies that an unscoped
// plan command on a PR whose changes span two databases this deployment manages
// is rejected with the "Multiple Databases Detected" comment instead of
// arbitrarily picking one of the databases. One command drives one database, so
// the user must disambiguate with -d.
func TestE2EPlanUnscopedRejectsMultipleManagedDatabases(t *testing.T) {
	dbName := "webhook_plan_multidb"
	svc := setupE2EService(t, dbName)
	dbConfig := svc.Config().Databases[dbName]
	dbConfig.AllowedRepos = []string{"octocat/hello-world"}
	dbConfig.AllowedDirs = []string{"schema"}
	svc.Config().Databases[dbName] = dbConfig
	// The second database is genuinely this deployment's: registered and with
	// its own allowed dir, so the ambiguity is real rather than an ownership
	// artifact.
	svc.Config().Databases["webhook_plan_multidb_other"] = api.DatabaseConfig{
		Type:         "mysql",
		AllowedRepos: []string{"octocat/hello-world"},
		AllowedDirs:  []string{"schema2"},
	}

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}
	prFiles := []*gh.CommitFile{
		{Filename: new("schema/schemabot.yaml"), Status: new("modified")},
		{Filename: new("schema2/schemabot.yaml"), Status: new("modified")},
	}
	result := setupFakeGitHubForPlanWithPRFiles(t, mux, schemaFiles, schemabotConfig, dbName, prFiles)
	registerSecondSchemabotConfig(t, mux, "schema2/schemabot.yaml", "database: webhook_plan_multidb_other\ntype: mysql\n")

	h := newE2EHandler(t, svc, client)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Multiple Databases Detected")
		assert.Contains(t, body, dbName)
		assert.Contains(t, body, "webhook_plan_multidb_other")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for multiple databases rejection comment")
	}

	plans, err := svc.Storage().Plans().GetByPR(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	for _, plan := range plans {
		assert.NotEqual(t, dbName, plan.Database, "ambiguous unscoped plan should not be stored")
	}
}

// TestE2EPlanUnscopedPlansOwnedDatabaseWhenPRTouchesUnownedConfig verifies the
// fan-out contract for unscoped commands on a PR spanning several databases:
// discovered configs are narrowed to the ones this deployment manages before
// deciding whether the command is ambiguous. A PR touching one config this
// deployment owns and one it does not must plan the owned database rather than
// reject with "Multiple Databases Detected" — the unowned config is another
// deployment's to handle.
func TestE2EPlanUnscopedPlansOwnedDatabaseWhenPRTouchesUnownedConfig(t *testing.T) {
	dbName := "webhook_plan_fanout_owned"
	svc := setupE2EService(t, dbName)
	dbConfig := svc.Config().Databases[dbName]
	dbConfig.AllowedRepos = []string{"octocat/hello-world"}
	dbConfig.AllowedDirs = []string{"schema"}
	svc.Config().Databases[dbName] = dbConfig

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}
	prFiles := []*gh.CommitFile{
		{Filename: new("schema/schemabot.yaml"), Status: new("modified")},
		{Filename: new("unowned/schemabot.yaml"), Status: new("modified")},
	}
	result := setupFakeGitHubForPlanWithPRFiles(t, mux, schemaFiles, schemabotConfig, dbName, prFiles)
	registerSecondSchemabotConfig(t, mux, "unowned/schemabot.yaml", "database: webhook_plan_fanout_unowned\ntype: mysql\n")

	h := newE2EHandler(t, svc, client)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "## Schema Change Plan")
		assert.Contains(t, body, "CREATE TABLE")
		assert.Contains(t, body, dbName)
		assert.NotContains(t, body, "webhook_plan_fanout_unowned")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for plan comment for the owned database")
	}
}

// TestE2EPlanUnscopedPlansRegisteredDatabaseWhenPRTouchesUnregisteredConfig
// verifies the fan-out contract on repos partitioned by database registry
// rather than allowed_dirs: with no directory allowlist configured, every
// discovered config passes the directory filter, so multiplicity must be
// decided over the configs whose database this deployment has registered. A PR
// touching one registered database's config and one unregistered database's
// config must plan the registered database instead of rejecting with
// "Multiple Databases Detected" — the unregistered config belongs to whichever
// deployment has that database in its registry.
func TestE2EPlanUnscopedPlansRegisteredDatabaseWhenPRTouchesUnregisteredConfig(t *testing.T) {
	dbName := "webhook_plan_registry_owned"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}
	prFiles := []*gh.CommitFile{
		{Filename: new("schema/schemabot.yaml"), Status: new("modified")},
		{Filename: new("other/schemabot.yaml"), Status: new("modified")},
	}
	result := setupFakeGitHubForPlanWithPRFiles(t, mux, schemaFiles, schemabotConfig, dbName, prFiles)
	registerSecondSchemabotConfig(t, mux, "other/schemabot.yaml", "database: webhook_plan_registry_unregistered\ntype: mysql\n")

	h := newE2EHandler(t, svc, client)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "## Schema Change Plan")
		assert.Contains(t, body, "CREATE TABLE")
		assert.Contains(t, body, dbName)
		assert.NotContains(t, body, "Multiple Databases Detected")
		assert.NotContains(t, body, "webhook_plan_registry_unregistered")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for plan comment for the registered database")
	}
}

// registerSecondSchemabotConfig serves an additional schemabot.yaml at the
// given repo path so a test PR can span more than one discovered config.
func registerSecondSchemabotConfig(t *testing.T, mux *http.ServeMux, repoPath, content string) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/"+repoPath, func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.RepositoryContent{
			Name:     new(path.Base(repoPath)),
			Path:     new(repoPath),
			Content:  new(base64.StdEncoding.EncodeToString([]byte(content))),
			Encoding: new("base64"),
		}))
	})
}

// TestE2EPlanSourcePolicyBlocksUnauthorizedRepo verifies the trusted GitHub
// discovery path enforces server-side source policy before a plan is stored.
// The manual command path should also write a failing aggregate check so the
// Checks UI matches the failure comment.
func TestE2EPlanSourcePolicyBlocksUnauthorizedRepo(t *testing.T) {
	dbName := "webhook_source_policy_block"
	svc := setupE2EService(t, dbName)
	dbConfig := svc.Config().Databases[dbName]
	dbConfig.AllowedRepos = []string{"octocat/orders"}
	dbConfig.AllowedDirs = []string{"schema"}
	svc.Config().Databases[dbName] = dbConfig

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	h := newE2EHandler(t, svc, client)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "plan failed")

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "## ❌ Plan Failed")
		assert.Contains(t, body, "source policy")
		assert.Contains(t, body, "repo &#34;octocat/hello-world&#34; is not authorized")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for source policy failure comment")
	}

	plans, err := svc.Storage().Plans().GetByPR(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	for _, plan := range plans {
		assert.NotEqual(t, dbName, plan.Database, "source-policy-blocked plan should not be stored")
	}

	check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	assert.Nil(t, check, "source-policy-blocked plan should not store a per-database check")

	var aggregateCheck checkRunCapture
	select {
	case aggregateCheck = <-result.checkRuns:
	case <-time.After(webhookIntegrationCheckRunDeadline):
		t.Fatal("timed out waiting for source policy aggregate check run")
	}
	assert.Equal(t, aggregateCheckName, aggregateCheck.Name)
	assert.Equal(t, checkStatusCompleted, aggregateCheck.Status)
	assert.Equal(t, checkConclusionFailure, aggregateCheck.Conclusion)
	require.NotNil(t, aggregateCheck.Output)
	assert.Contains(t, aggregateCheck.Output.Summary, "source policy")

	aggregate, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, aggregateSentinel, aggregateSentinel, aggregateSentinel)
	require.NoError(t, err)
	require.NotNil(t, aggregate, "source-policy-blocked plan should store a failing aggregate check")
	assert.Equal(t, "abc123", aggregate.HeadSHA)
	assert.Equal(t, checkStatusCompleted, aggregate.Status)
	assert.Equal(t, checkConclusionFailure, aggregate.Conclusion)
}

func TestE2EPlanNoChanges(t *testing.T) {
	dbName := "webhook_plan_nochanges"
	svc := setupE2EService(t, dbName)

	// Create the table in the target DB first so the plan finds no changes
	ctx := t.Context()
	appDSN := strings.Replace(e2eTargetDSN, "/target_test", "/"+dbName, 1) + "&multiStatements=true"
	db, err := sql.Open("block-mysql", appDSN)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)
	_ = db.Close()

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "plan generated successfully")

	// Verify plan comment — should say no changes
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "No schema changes detected")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for plan comment")
	}

	// Verify check run — should be success
	select {
	case cr := <-result.checkRuns:
		assert.Equal(t, "completed", cr.Status)
		assert.Equal(t, "success", cr.Conclusion)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for check run")
	}

	// Verify plan was persisted to SchemaBot storage
	plans, err := svc.Storage().Plans().GetByPR(ctx, "octocat/hello-world", 1)
	require.NoError(t, err)
	require.NotEmpty(t, plans, "expected at least one plan record")
	var noChangesPlan *storage.Plan
	for _, p := range plans {
		if p.Database == dbName {
			noChangesPlan = p
			break
		}
	}
	require.NotNil(t, noChangesPlan, "expected a plan record for database %s", dbName)
	assert.Equal(t, dbName, noChangesPlan.Database)
	assert.Equal(t, "staging", noChangesPlan.Environment)

	// Verify check record was persisted — no changes, so conclusion is "success"
	check, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check, "expected a check record")
	assert.False(t, check.HasChanges, "check should indicate no changes")
	assert.Equal(t, "completed", check.Status)
	assert.Equal(t, "success", check.Conclusion)
}

// TestE2EPlanConfigNotFound verifies that a plan command on a PR that changes
// schema files with no schemabot.yaml anywhere in the repository fails with
// the config-not-found error rather than planning unmanaged schema.
func TestE2EPlanConfigNotFound(t *testing.T) {
	dbName := "webhook_plan_noconfig"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// The PR changes a schema file, but no schemabot.yaml config exists.
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}
	result := setupFakeGitHubForPlan(t, mux, schemaFiles, "", dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "schema request error handled")

	// Verify error comment about no config
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "No SchemaBot Configuration Found")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for error comment")
	}
}

// TestE2EPlanCleansStalePlanOnlyChecksBeforeConvergingAggregates verifies the
// check-refresh path of a plan command on a PR with no managed schema changes
// when an earlier commit left behind a stale plan-only blocking check (for
// example the PR reverted its schema change after a plan ran). The stale
// per-database check must be cleaned up to passing on the current head — the
// aggregate would otherwise stay blocked and contradict the refresh comment.
func TestE2EPlanCleansStalePlanOnlyChecksBeforeConvergingAggregates(t *testing.T) {
	dbName := "webhook_plan_stale_converge"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	require.NoError(t, svc.Storage().Checks().Upsert(ctx, &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "oldsha111",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: dbName,
		CheckRunID:   100,
		HasChanges:   true,
		Status:       checkStatusCompleted,
		Conclusion:   checkConclusionActionRequired,
	}))

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	result := setupFakeGitHubForPlan(t, mux, map[string]string{}, "", dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}
	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "no managed schema changes handled")

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "No Managed Schema Changes")
		assert.Contains(t, body, "refreshed as passing")
		assert.Contains(t, body, "abc123")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for check refresh comment")
	}

	check, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check, "expected stored per-database check to survive cleanup")
	assert.Equal(t, "abc123", check.HeadSHA)
	assert.Equal(t, checkStatusCompleted, check.Status)
	assert.Equal(t, checkConclusionSuccess, check.Conclusion)
	assert.False(t, check.HasChanges)
}

// TestE2EPlanScopedToOneEnvironmentBlocksOnOtherEnvironmentApply verifies that
// a plan command scoped with -e cannot convert a PR's checks to passing while
// an apply in a different environment still owns the PR's state: the check
// refresh spans every environment's aggregate, so apply-owned state anywhere
// must block it with the reconciliation comment instead.
func TestE2EPlanScopedToOneEnvironmentBlocksOnOtherEnvironmentApply(t *testing.T) {
	dbName := "webhook_plan_cross_env_apply"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	apply := &storage.Apply{
		ApplyIdentifier: "apply-cross-env",
		Database:        dbName,
		DatabaseType:    "mysql",
		Environment:     "production",
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		State:           state.Apply.Running,
		Engine:          "spirit",
	}
	applyID, err := svc.Storage().Applies().Create(ctx, apply)
	require.NoError(t, err)

	require.NoError(t, svc.Storage().Checks().Upsert(ctx, &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "oldsha111",
		Environment:  "production",
		DatabaseType: "mysql",
		DatabaseName: dbName,
		CheckRunID:   100,
		ApplyID:      applyID,
		HasChanges:   true,
		Status:       checkStatusInProgress,
	}))

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	result := setupFakeGitHubForPlan(t, mux, map[string]string{}, "", dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}
	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Schema Change Reconciliation Required")
		assert.Contains(t, body, "production")
		assert.NotContains(t, body, "refreshed as passing")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for reconciliation comment")
	}
}

func TestE2EMultiEnvPlan(t *testing.T) {
	dbName := "webhook_multi_env"
	svc := setupE2EServiceMultiEnv(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	// "schemabot plan" without -e → multi-env plan
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "multi-env plan started")

	// Multi-env plan runs as a background goroutine — wait for the single combined comment
	select {
	case body := <-result.comments:
		// Should be a combined comment (not separate per env)
		assert.Contains(t, body, "## Schema Change Plan")
		assert.Contains(t, body, "CREATE TABLE")
		assert.Contains(t, body, dbName)

		// Both envs have identical changes (empty target DBs), so should be deduplicated
		assert.Contains(t, body, "Staging & Production",
			"identical plans should have combined environment header")
		assert.NotContains(t, body, "### Staging\n",
			"should not have separate Staging section when plans are identical")

		// Footer should suggest staging first
		assert.Contains(t, body, "schemabot apply -e staging")
		assert.Contains(t, body, "schemabot apply -e production")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for multi-env plan comment")
	}

	// Should get the aggregate check run
	select {
	case cr := <-result.checkRuns:
		assert.Equal(t, aggregateCheckName, cr.Name)
		assert.Equal(t, "completed", cr.Status)
		assert.Equal(t, "action_required", cr.Conclusion)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for aggregate check run")
	}
}

func TestE2EMultiEnvPlanDifferentChanges(t *testing.T) {
	dbName := "webhook_multi_env_diff"
	svc := setupE2EServiceMultiEnv(t, dbName)

	// Pre-create the table in staging so staging has no changes, but production still does
	ctx := t.Context()
	appDSNStaging := strings.Replace(e2eTargetDSN, "/target_test", "/"+dbName+"_staging", 1) + "&multiStatements=true"
	db, err := sql.Open("block-mysql", appDSNStaging)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)
	_ = db.Close()

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "multi-env plan started")

	// Wait for the combined comment
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "## Schema Change Plan")

		// Plans differ: staging has no changes, production has changes
		// Should NOT be deduplicated — should show separate sections
		assert.Contains(t, body, "### Staging")
		assert.Contains(t, body, "### Production")
		assert.Contains(t, body, "No schema changes detected")
		assert.Contains(t, body, "CREATE TABLE")

		// Footer should only suggest production
		assert.Contains(t, body, "schemabot apply -e production")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for multi-env plan comment")
	}
}

// TestE2EPlanUsesServerSideTarget verifies that the webhook plan handler routes
// using the database target policy from server config.
func TestE2EPlanUsesServerSideTarget(t *testing.T) {
	dbName := "webhook_server_target"
	ctx := t.Context()

	// Create the app database on the target
	targetDB, err := sql.Open("block-mysql", e2eTargetDSN+"&multiStatements=true")
	require.NoError(t, err)
	_, err = targetDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+dbName+"`")
	require.NoError(t, err)
	_ = targetDB.Close()

	t.Cleanup(func() {
		db, err := sql.Open("block-mysql", e2eTargetDSN+"&multiStatements=true")
		if err == nil {
			_, _ = db.ExecContext(t.Context(), "DROP DATABASE IF EXISTS `"+dbName+"`")
			_ = db.Close()
		}
	})

	appDSN := strings.Replace(e2eTargetDSN, "/target_test", "/"+dbName, 1)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	schemabotDB, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	t.Cleanup(func() { _ = schemabotDB.Close() })

	st := mysqlstore.New(schemabotDB)

	// Clean up stale data
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM checks WHERE database_name = ?", dbName)
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM checks WHERE repository = 'octocat/hello-world' AND pull_request = 1")
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM plans WHERE database_name = ?", dbName)

	localClient, err := tern.NewLocalClient(tern.LocalConfig{
		Database:  dbName,
		Type:      "mysql",
		TargetDSN: appDSN,
	}, st, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = localClient.Close() })

	// The tern client is registered under "team-a/staging", so plan must use
	// the deployment stored in databases.<db>.environments.staging.
	serverConfig := &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			dbName: {
				Type: "mysql",
				Environments: map[string]api.EnvironmentConfig{
					"staging": {Target: "team-a-target", Deployment: "team-a"},
				},
			},
		},
		TernDeployments: api.TernConfig{
			"team-a": api.TernEndpoints{
				"staging": "localhost:9999", // address not dialed; pre-injected client is used instead
			},
		},
		Repos: map[string]api.RepoConfig{
			"octocat/hello-world": {},
		},
	}

	svc := api.New(st, serverConfig, map[string]tern.Client{
		"team-a/staging": localClient,
	}, logger)
	t.Cleanup(func() { _ = svc.Close() })

	// Set up fake GitHub API
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}
	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "plan generated successfully")

	// Verify plan comment was posted with the expected DDL
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "## Schema Change Plan")
		assert.Contains(t, body, "CREATE TABLE")
		assert.Contains(t, body, dbName)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for plan comment")
	}
}

// --- Container helpers (matches e2e/setup_test.go patterns) ---

// TestE2EPlanExcludesIgnoredNamespaces verifies that namespace directories
// listed in schemabot.yaml ignore_namespaces never reach the plan: a schema
// root carrying a local-test-only namespace alongside the real one produces a
// plan (comment, stored record, check) covering only the real namespace, so
// SchemaBot never tries to reconcile test fixtures against the live database.
func TestE2EPlanExcludesIgnoredNamespaces(t *testing.T) {
	dbName := "webhook_plan_ignore_ns"
	// ignore_namespaces requires the namespace-free MySQL DSN shape (a
	// database-scoped DSN diffs the whole database as one unit and refuses the
	// exclusion), so this scenario configures the target the supported way.
	svc := setupE2EServiceOpts(t, dbName, e2eServiceOpts{targetDSN: namespaceFreeTargetDSN(t)})
	dbConfig := svc.Config().Databases[dbName]
	dbConfig.AllowedRepos = []string{"octocat/hello-world"}
	dbConfig.AllowedDirs = []string{"schema"}
	svc.Config().Databases[dbName] = dbConfig

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\nignore_namespaces:\n  - local_fixtures\n", dbName)
	schemaFiles := map[string]string{
		dbName + "/users.sql":        "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
		"local_fixtures/widgets.sql": "CREATE TABLE `widgets` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "plan generated successfully")

	// The plan comment covers the real namespace only, and discloses the
	// exclusion so a reviewer can tell the namespace was withheld by config
	// rather than unchanged.
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "## Schema Change Plan")
		assert.Contains(t, body, "users")
		assert.NotContains(t, body, "widgets")
		assert.Contains(t, body, "ℹ️ Namespaces excluded from this plan by `ignore_namespaces`: `local_fixtures`")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for plan comment")
	}

	// The stored plan record carries no trace of the ignored namespace.
	ctx := t.Context()
	plans, err := svc.Storage().Plans().GetByPR(ctx, "octocat/hello-world", 1)
	require.NoError(t, err)
	var plan *storage.Plan
	for _, p := range plans {
		if p.Database == dbName {
			plan = p
			break
		}
	}
	require.NotNil(t, plan, "expected a plan record for database %s", dbName)
	require.NotNil(t, plan.Namespaces)
	assert.Contains(t, plan.Namespaces, dbName)
	assert.NotContains(t, plan.Namespaces, "local_fixtures")
}

// TestE2EPlanIgnoringEveryNamespaceFails verifies the safety boundary when
// ignore_namespaces excludes every namespace in the schema root: the plan
// must fail with an error naming the exclusion rather than planning an empty
// desired state, which on a live database would read as "drop everything".
func TestE2EPlanIgnoringEveryNamespaceFails(t *testing.T) {
	dbName := "webhook_plan_ignore_all"
	svc := setupE2EService(t, dbName)
	dbConfig := svc.Config().Databases[dbName]
	dbConfig.AllowedRepos = []string{"octocat/hello-world"}
	dbConfig.AllowedDirs = []string{"schema"}
	svc.Config().Databases[dbName] = dbConfig

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\nignore_namespaces:\n  - %s\n  - local_fixtures\n", dbName, dbName)
	schemaFiles := map[string]string{
		dbName + "/users.sql":        "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
		"local_fixtures/widgets.sql": "CREATE TABLE `widgets` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "schema request error handled")

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "after excluding ignored namespaces")
		assert.NotContains(t, body, "No schema changes detected")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for error comment")
	}
}
