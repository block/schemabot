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
	"strings"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
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
		assert.Contains(t, body, "repo \"octocat/hello-world\" is not authorized")
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
	db, err := sql.Open("mysql", appDSN)
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

func TestE2EPlanConfigNotFound(t *testing.T) {
	dbName := "webhook_plan_noconfig"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// No schemabot.yaml config — empty schema files, no config
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
	assert.Contains(t, rr.Body.String(), "schema request error handled")

	// Verify error comment about no config
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "No SchemaBot Configuration Found")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for error comment")
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
	db, err := sql.Open("mysql", appDSNStaging)
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

func TestE2EAutoPlan(t *testing.T) {
	dbName := "webhook_autoplan"
	svc := setupE2EService(t, dbName)
	svc.Config().Tenant = "alpha"

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

	// Send a pull_request "opened" webhook instead of an issue_comment
	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "opened",
		headSHA: "abc123",
		headRef: "feature-branch",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "auto-plan started")

	// Auto-plan runs in a background goroutine — wait for the plan comment
	select {
	case body := <-result.comments:
		firstLine, _, _ := strings.Cut(body, "\n")
		assert.Equal(t, "## Schema Change Plan — Staging", firstLine)
		assert.Contains(t, body, "**Tenant**: `alpha`")
		assert.Contains(t, body, "CREATE TABLE")
		assert.Contains(t, body, dbName)
		assert.Contains(t, body, "schemabot apply -e staging --tenant alpha")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for auto-plan comment")
	}

	// Verify check run was created
	select {
	case cr := <-result.checkRuns:
		assert.Contains(t, cr.Name, "SchemaBot")
		assert.Equal(t, "completed", cr.Status)
		assert.Equal(t, "action_required", cr.Conclusion)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for check run")
	}
}

// A push that only changes non-schema inputs still refreshes SchemaBot checks
// for the new PR commit without adding another plan comment to the PR timeline.
func TestE2EAutoPlanSynchronizeApplicationOnlyChangeSkipsComment(t *testing.T) {
	dbName := "webhook_autoplan_app_only_sync"
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

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)
	registerCompareFiles(t, mux, "oldsha", "newsha", []*gh.CommitFile{{
		Filename: new("app/service.go"),
		Status:   new("modified"),
	}})

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:    "synchronize",
		beforeSHA: "oldsha",
		headSHA:   "newsha",
		headRef:   "feature-branch",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "auto-plan started")

	select {
	case cr := <-result.checkRuns:
		assert.Contains(t, cr.Name, "SchemaBot")
		assert.Equal(t, "completed", cr.Status)
		assert.Equal(t, "action_required", cr.Conclusion)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for check run")
	}

	select {
	case body := <-result.comments:
		t.Fatalf("expected no comment for application-only synchronize, got: %s", body)
	case <-time.After(3 * time.Second):
	}
}

// TestE2EAutoPlanSourcePolicyBlocksWithFailingAggregate verifies auto-plan
// source-policy failures create a failing aggregate Check Run for branch
// protection instead of only posting a PR comment.
func TestE2EAutoPlanSourcePolicyBlocksWithFailingAggregate(t *testing.T) {
	dbName := "webhook_autoplan_source_policy_block"
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

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "opened",
		headSHA: "abc123",
		headRef: "feature-branch",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "auto-plan started")

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "failed to plan")
		assert.Contains(t, body, "source policy")
		assert.Contains(t, body, "repo \"octocat/hello-world\" is not authorized")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for source policy auto-plan comment")
	}

	var aggregateCheck checkRunCapture
	select {
	case aggregateCheck = <-result.checkRuns:
	case <-time.After(webhookIntegrationCheckRunDeadline):
		t.Fatal("timed out waiting for source policy aggregate check run")
	}
	assert.Equal(t, aggregateCheckName, aggregateCheck.Name)
	assert.Equal(t, "completed", aggregateCheck.Status)
	assert.Equal(t, "failure", aggregateCheck.Conclusion)
	require.NotNil(t, aggregateCheck.Output)
	assert.Contains(t, aggregateCheck.Output.Summary, "source policy")

	check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, aggregateSentinel, aggregateSentinel, aggregateSentinel)
	require.NoError(t, err)
	require.NotNil(t, check, "source-policy-blocked auto-plan should store a failing aggregate check")
	assert.Equal(t, "abc123", check.HeadSHA)
	assert.Equal(t, "completed", check.Status)
	assert.Equal(t, "failure", check.Conclusion)

	plans, err := svc.Storage().Plans().GetByPR(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	for _, plan := range plans {
		assert.NotEqual(t, dbName, plan.Database, "source-policy-blocked auto-plan should not store a plan")
	}
}

// TestE2EReopenedPRAutoPlansCurrentHead verifies that reopening a PR follows
// the same auto-plan path as a new PR and records checks on the current commit.
func TestE2EReopenedPRAutoPlansCurrentHead(t *testing.T) {
	dbName := "webhook_reopened_autoplan"
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

	// Fake GitHub returns schema files and PR metadata for the reopened commit.
	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)
	h := newE2EHandler(t, svc, client)

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "reopened",
		headSHA: "abc123",
		headRef: "feature-branch",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "auto-plan started")

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Schema Change Plan")
		assert.Contains(t, body, "CREATE TABLE")
		assert.Contains(t, body, dbName)
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for reopened auto-plan comment")
	}

	select {
	case cr := <-result.checkRuns:
		assert.Equal(t, aggregateCheckName, cr.Name)
		assert.Equal(t, "abc123", cr.HeadSHA)
		assert.Equal(t, checkStatusCompleted, cr.Status)
		assert.Equal(t, checkConclusionActionRequired, cr.Conclusion)
	case <-time.After(webhookIntegrationCheckRunDeadline):
		t.Fatal("timed out waiting for reopened auto-plan check run")
	}

	// The stored check state must be tied to the reopened commit SHA, not any
	// stale SHA from before the PR was closed.
	check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check)
	assert.Equal(t, "abc123", check.HeadSHA)
	assert.Equal(t, checkConclusionActionRequired, check.Conclusion)
}

func TestE2EAutoPlanWithLintViolations(t *testing.T) {
	dbName := "webhook_autoplan_lint"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	// Use a FLOAT column (triggers has_float linter at warning severity, not unsafe).
	schemaFiles := map[string]string{
		"bad_table.sql": "CREATE TABLE `bad_table` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `amount` float NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	// Send a pull_request "opened" webhook to trigger auto-plan
	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "opened",
		headSHA: "abc123",
		headRef: "feature-branch",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "auto-plan started")

	// Wait for the plan comment — should include lint violations from LintSchema
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Schema Change Plan")
		assert.Contains(t, body, "CREATE TABLE")
		assert.Contains(t, body, "Lint Warnings", "plan comment should include lint violations section")
		assert.Contains(t, body, "bad_table", "lint warning should reference the table name")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for auto-plan comment with lint violations")
	}

	// Verify check run was created
	select {
	case cr := <-result.checkRuns:
		assert.Contains(t, cr.Name, "SchemaBot")
		assert.Equal(t, "completed", cr.Status)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for check run")
	}
}

func TestE2EAutoPlanNoChangesSkipsComment(t *testing.T) {
	dbName := "webhook_autoplan_nochange"
	svc := setupE2EService(t, dbName)

	// Pre-create the table so there are no changes
	ctx := t.Context()
	appDSN := strings.Replace(e2eTargetDSN, "/target_test", "/"+dbName, 1) + "&multiStatements=true"
	db, err := sql.Open("mysql", appDSN)
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

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "synchronize",
		headSHA: "abc123",
		headRef: "feature-branch",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "auto-plan started")

	// Check run should still be created (for PR status)
	select {
	case cr := <-result.checkRuns:
		assert.Equal(t, "completed", cr.Status)
		assert.Equal(t, "success", cr.Conclusion)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for check run")
	}

	// No comment should be posted — give it a moment to confirm nothing arrives
	select {
	case body := <-result.comments:
		t.Fatalf("expected no comment for auto-plan with no changes, but got: %s", body)
	case <-time.After(3 * time.Second):
		// expected: no comment posted
	}
}

func TestE2EAutoPlanNoSchemaFiles(t *testing.T) {
	dbName := "webhook_autoplan_noschema"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// PR info
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{
				Ref: new("feature-branch"),
				SHA: new("abc123"),
			},
			Base: &gh.PullRequestBranch{
				Ref: new("main"),
				SHA: new("def456"),
			},
			User: &gh.User{Login: new("testuser")},
		})
	})

	// PR changed files — only non-schema files
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, r *http.Request) {
		files := []*gh.CommitFile{
			{Filename: new("README.md"), Status: new("modified")},
			{Filename: new("main.go"), Status: new("modified")},
		}
		_ = json.NewEncoder(w).Encode(files)
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "opened",
		headSHA: "abc123",
		headRef: "feature-branch",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "no schema files in PR")
}

// TestE2EGitHubUnavailableDuringConfigDiscoveryPublishesFailingAggregates
// verifies that SchemaBot fails closed when it can verify the PR commit but
// cannot inspect changed files because GitHub returns an availability error.
func TestE2EGitHubUnavailableDuringConfigDiscoveryPublishesFailingAggregates(t *testing.T) {
	svc := setupE2EServiceWithAllowedEnvs(t, []string{"staging", "production"})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// PR metadata is available, so SchemaBot knows the current commit SHA and
	// can safely publish failing aggregate checks against that SHA.
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{
				Ref: new("feature-branch"),
				SHA: new("abc123"),
			},
			Base: &gh.PullRequestBranch{
				Ref: new("main"),
				SHA: new("def456"),
			},
			User: &gh.User{Login: new("testuser")},
		})
	})

	// Changed-file discovery fails after the PR commit is known. This is a
	// fail-closed condition for every configured environment.
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	checkRuns := make(chan checkRunCapture, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		checkRuns <- body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	h := newE2EHandler(t, svc, client)

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "opened",
		headSHA: "abc123",
		headRef: "feature-branch",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "config discovery failed")

	seen := map[string]bool{}
	for i := range 2 {
		select {
		case cr := <-checkRuns:
			seen[cr.Name] = true
			assert.Equal(t, checkStatusCompleted, cr.Status)
			assert.Equal(t, checkConclusionFailure, cr.Conclusion)
			assert.Equal(t, "abc123", cr.HeadSHA)
		case <-time.After(webhookIntegrationCheckRunDeadline):
			t.Fatalf("timed out waiting for failing aggregate check run %d/2, seen: %v", i+1, seen)
		}
	}
	assert.True(t, seen["SchemaBot (staging)"])
	assert.True(t, seen["SchemaBot (production)"])

	// Each aggregate stores a machine-readable GitHub-unavailable blocking
	// reason so operators can distinguish this from a schema/config error.
	for _, env := range []string{"staging", "production"} {
		check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, env, aggregateSentinel, aggregateSentinel)
		require.NoError(t, err)
		require.NotNil(t, check)
		assert.Equal(t, githubConfigDiscoveryUnavailableBlock.blockingReason, check.BlockingReason)
		assert.Contains(t, check.ErrorMessage, githubConfigDiscoveryUnavailableBlock.message)
	}
}

// TestE2EGitHubUnavailableDuringAutoPlanDoesNotPublishCheckRun verifies that
// SchemaBot does not create or store a check run when it cannot verify the
// current PR commit SHA at all.
func TestE2EGitHubUnavailableDuringAutoPlanDoesNotPublishCheckRun(t *testing.T) {
	svc := setupE2EServiceWithAllowedEnvs(t, []string{"staging"})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// The initial PR lookup fails, so SchemaBot does not know which commit SHA
	// a check run should target.
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	checkRuns := make(chan checkRunCapture, 1)
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		checkRuns <- body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	h := newE2EHandler(t, svc, client)

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "opened",
		headSHA: "abc123",
		headRef: "feature-branch",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "config discovery failed")

	// Publishing a check run without a verified head SHA could mark the wrong
	// commit, so no GitHub or stored aggregate check should be created.
	select {
	case cr := <-checkRuns:
		t.Fatalf("GitHub outage should not publish a check run, got: %+v", cr)
	case <-time.After(250 * time.Millisecond):
	}

	aggregate, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", aggregateSentinel, aggregateSentinel)
	require.NoError(t, err)
	assert.Nil(t, aggregate)
}

// TestE2EAutoPlanWithOnlySchemaBotConfigChangeClearsRollbackCheck verifies the
// self-serve reconciliation path for a PR whose live database already matches
// the current schema files. A no-op schemabot.yaml edit should make config
// discovery deterministic, auto-plan no changes, and clear the
// rollback-created blocking check state without operator intervention.
func TestE2EAutoPlanWithOnlySchemaBotConfigChangeClearsRollbackCheck(t *testing.T) {
	dbName := "webhook_noop_config_reconcile"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	schemaSQL := "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
	cfg, err := mysql.ParseDSN(e2eTargetDSN)
	require.NoError(t, err)
	cfg.DBName = dbName
	db, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(ctx))
	_, err = db.ExecContext(ctx, strings.TrimSuffix(schemaSQL, ";"))
	require.NoError(t, err)

	require.NoError(t, svc.Storage().Checks().Upsert(ctx, &storage.Check{
		Repository:     "octocat/hello-world",
		PullRequest:    1,
		HeadSHA:        "abc123",
		Environment:    "staging",
		DatabaseType:   "mysql",
		DatabaseName:   dbName,
		CheckRunID:     100,
		HasChanges:     true,
		Status:         checkStatusCompleted,
		Conclusion:     checkConclusionActionRequired,
		BlockingReason: rollbackCompletedBlock.blockingReason,
		ErrorMessage:   rollbackCompletedBlock.message,
	}))

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	setupFakeGitHubForPlanWithPRFiles(t, mux, map[string]string{"users.sql": schemaSQL}, schemabotConfig, dbName, []*gh.CommitFile{
		{
			Filename: new("schema/schemabot.yaml"),
			Status:   new("modified"),
		},
	})

	h := newE2EHandler(t, svc, client)
	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "synchronize"}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "auto-plan started")

	var check *storage.Check
	require.Eventually(t, func() bool {
		check, err = svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
		return err == nil && check != nil && check.Status == checkStatusCompleted && check.Conclusion == checkConclusionSuccess
	}, webhookIntegrationPollDeadline, 500*time.Millisecond, "no-op config auto-plan should clear rollback-created blocking check")
	assert.Equal(t, "abc123", check.HeadSHA)
	assert.False(t, check.HasChanges)
	assert.Zero(t, check.ApplyID)
	assert.Empty(t, check.BlockingReason)
	assert.Empty(t, check.ErrorMessage)
}

// TestE2EPlanUsesServerSideTarget verifies that the webhook plan handler routes
// using the database target policy from server config.
func TestE2EPlanUsesServerSideTarget(t *testing.T) {
	dbName := "webhook_server_target"
	ctx := t.Context()

	// Create the app database on the target
	targetDB, err := sql.Open("mysql", e2eTargetDSN+"&multiStatements=true")
	require.NoError(t, err)
	_, err = targetDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+dbName+"`")
	require.NoError(t, err)
	_ = targetDB.Close()

	t.Cleanup(func() {
		db, err := sql.Open("mysql", e2eTargetDSN+"&multiStatements=true")
		if err == nil {
			_, _ = db.ExecContext(t.Context(), "DROP DATABASE IF EXISTS `"+dbName+"`")
			_ = db.Close()
		}
	})

	appDSN := strings.Replace(e2eTargetDSN, "/target_test", "/"+dbName, 1)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
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

// TestE2EMultiAppAutoPlan simulates a monorepo with multiple apps, each with their
// own schema directory and database. Verifies that a PR touching one app only creates
// checks for that app's database, not for others.
func TestE2EMultiAppAutoPlan(t *testing.T) {
	// Create two databases simulating two apps in a monorepo
	paymentsSvc := setupE2EService(t, "payments")
	ordersSvc := setupE2EService(t, "orders")
	_ = ordersSvc // orders is configured but not touched by this PR

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// Simulate a monorepo with two apps, each with their own schema dir:
	//   payments-service/mysql/schema/schemabot.yaml  → database: payments
	//   orders-service/mysql/schema/schemabot.yaml    → database: orders
	// The PR only changes payments — orders should NOT be planned.
	paymentsConfig := "database: payments\ntype: mysql\n"
	ordersConfig := "database: orders\ntype: mysql\n"
	transactionsSQL := "CREATE TABLE `transactions` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `amount_cents` bigint NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
	auditLogSQL := "CREATE TABLE `audit_log` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `action` varchar(50) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"

	result := &planFlowResult{
		comments:  make(chan string, 10),
		reactions: make(chan string, 10),
		checkRuns: make(chan checkRunCapture, 10),
	}

	// PR info
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{Ref: new("feature-branch"), SHA: new("abc123")},
			Base: &gh.PullRequestBranch{Ref: new("main"), SHA: new("def456")},
			User: &gh.User{Login: new("testuser")},
		})
	})

	// PR changed files in payments-service only (two namespaces: payments + payments_audit)
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]*gh.CommitFile{
			{Filename: new("payments-service/mysql/schema/payments/transactions.sql"), Status: new("added")},
			{Filename: new("payments-service/mysql/schema/payments_audit/audit_log.sql"), Status: new("added")},
		})
	})

	// Git tree contains BOTH apps' schema files (full repo tree)
	treeEntries := []*gh.TreeEntry{
		// payments app (two namespaces)
		{Path: new("payments-service/mysql/schema/schemabot.yaml"), Mode: new("100644"), Type: new("blob"), SHA: new("configsha_payments"), Size: new(len(paymentsConfig))},
		{Path: new("payments-service/mysql/schema/payments/transactions.sql"), Mode: new("100644"), Type: new("blob"), SHA: new("blobsha_transactions"), Size: new(len(transactionsSQL))},
		{Path: new("payments-service/mysql/schema/payments_audit/audit_log.sql"), Mode: new("100644"), Type: new("blob"), SHA: new("blobsha_audit"), Size: new(len(auditLogSQL))},
		// orders app (not in changed files)
		{Path: new("orders-service/mysql/schema/schemabot.yaml"), Mode: new("100644"), Type: new("blob"), SHA: new("configsha_orders"), Size: new(len(ordersConfig))},
	}

	blobContents := map[string]string{
		"configsha_payments":   paymentsConfig,
		"blobsha_transactions": transactionsSQL,
		"blobsha_audit":        auditLogSQL,
		"configsha_orders":     ordersConfig,
	}

	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/abc123", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.Tree{SHA: new("abc123"), Entries: treeEntries, Truncated: new(false)})
	})

	mux.HandleFunc("GET /repos/octocat/hello-world/git/blobs/", func(w http.ResponseWriter, r *http.Request) {
		sha := r.URL.Path[len("/repos/octocat/hello-world/git/blobs/"):]
		if _, ok := blobContents[sha]; !ok {
			http.NotFound(w, r)
			return
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(blobContents[sha]))
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"sha":"%s","content":"%s","encoding":"base64","size":%d}`, sha, encoded, len(blobContents[sha]))
	})

	mux.HandleFunc("GET /repos/octocat/hello-world/contents/", func(w http.ResponseWriter, r *http.Request) {
		filePath := r.URL.Path[len("/repos/octocat/hello-world/contents/"):]
		if filePath == "payments-service/mysql/schema/schemabot.yaml" {
			_ = json.NewEncoder(w).Encode(gh.RepositoryContent{
				Name:     new("schemabot.yaml"),
				Path:     new("payments-service/mysql/schema/schemabot.yaml"),
				Content:  new(base64.StdEncoding.EncodeToString([]byte(paymentsConfig))),
				Encoding: new("base64"),
			})
			return
		}
		http.NotFound(w, r)
	})

	// Capture comments, reactions, check runs
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		result.comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		result.checkRuns <- body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})
	mux.HandleFunc("PATCH /repos/octocat/hello-world/check-runs/", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		result.checkRuns <- body
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	// Wire up BOTH databases in the service
	h := newE2EHandler(t, paymentsSvc, client)

	// Send PR opened webhook (triggers auto-plan)
	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "opened"}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// Should get a plan comment for payments only, showing both namespaces
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "payments", "plan comment should be for payments database")
		assert.NotContains(t, body, "orders", "should NOT plan orders database")
		assert.Contains(t, body, "CREATE TABLE")
		assert.Contains(t, body, "transactions", "should include payments namespace table")
		assert.Contains(t, body, "audit_log", "should include payments_audit namespace table")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for plan comment")
	}

	// Only the aggregate check run should be created.
	hasAggregate := false
	deadline := time.After(5 * time.Second)
	for {
		select {
		case cr := <-result.checkRuns:
			if cr.Name == aggregateCheckName {
				hasAggregate = true
				goto checksDone
			}
		case <-deadline:
			goto checksDone
		}
	}
checksDone:
	assert.True(t, hasAggregate, "expected aggregate check run")
}

// TestE2EAutoPlanFailsWhenConfiguredEnvironmentsAreNotAllowed verifies that
// SchemaBot fails closed when schema files changed, but the server-configured
// database environments do not overlap this service's allowed environments.
// This is a configuration mismatch, not "no work".
func TestE2EAutoPlanFailsWhenConfiguredEnvironmentsAreNotAllowed(t *testing.T) {
	dbName := "webhook_no_owned_envs"
	svc := setupE2EServiceWithAllowedEnvs(t, []string{"sandbox"})
	configureE2EServiceEnvironments(t, svc, dbName, "staging", "production")

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// The database is configured for staging and production, but this test service
	// processes only sandbox. SchemaBot cannot safely plan this schema change
	// because none of the configured environments are allowed.
	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)
	h := newE2EHandler(t, svc, client)

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "opened",
		headSHA: "abc123",
		headRef: "feature-branch",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "auto-plan started")

	// The webhook still accepts the PR event asynchronously, but auto-plan posts
	// a failing aggregate because this service cannot process any environment
	// configured for the database.
	select {
	case cr := <-result.checkRuns:
		assert.Equal(t, "SchemaBot (sandbox)", cr.Name)
		assert.Equal(t, "abc123", cr.HeadSHA)
		assert.Equal(t, checkStatusCompleted, cr.Status)
		assert.Equal(t, checkConclusionFailure, cr.Conclusion)
		require.NotNil(t, cr.Output)
		assert.Equal(t, noAllowedConfiguredEnvironmentsBlock.message, cr.Output.Summary)
	case <-time.After(webhookIntegrationCheckRunDeadline):
		t.Fatal("timed out waiting for failing aggregate for allowed environment")
	}

	require.Eventually(t, func() bool {
		aggregate, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "sandbox", aggregateSentinel, aggregateSentinel)
		return err == nil && aggregate != nil &&
			aggregate.Conclusion == checkConclusionFailure &&
			aggregate.BlockingReason == noAllowedConfiguredEnvironmentsBlock.blockingReason &&
			aggregate.ErrorMessage == noAllowedConfiguredEnvironmentsBlock.message
	}, 5*time.Second, 100*time.Millisecond, "failing aggregate should be stored for allowed environment")

	// There should be no plan comment because no environment reached planning.
	select {
	case body := <-result.comments:
		t.Fatalf("expected no plan comment when no configured environments are allowed, got: %s", body)
	case <-time.After(500 * time.Millisecond):
	}
}
