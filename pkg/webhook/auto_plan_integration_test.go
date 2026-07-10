//go:build integration

// Automatic plan-on-push webhook integration tests (PR opened/reopened/synchronize),
// as opposed to the explicit /schemabot plan command tests in plan_integration_test.go.

package webhook

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/spirit/pkg/utils"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

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
	filesFetched := make(chan struct{})
	var filesFetchedOnce sync.Once
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, r *http.Request) {
		filesFetchedOnce.Do(func() { close(filesFetched) })
		files := []*gh.CommitFile{
			{Filename: new("README.md"), Status: new("modified")},
			{Filename: new("main.go"), Status: new("modified")},
		}
		_ = json.NewEncoder(w).Encode(files)
	})
	checkRuns := make(chan checkRunCapture, 1)
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		checkRuns <- body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})
	comments := make(chan string, 1)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))

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
	assert.Contains(t, rr.Body.String(), "auto-plan started")
	select {
	case <-filesFetched:
	case <-time.After(webhookIntegrationCheckRunDeadline):
		t.Fatal("timed out waiting for no-schema auto-plan discovery")
	}
	select {
	case cr := <-checkRuns:
		assert.Equal(t, aggregateCheckName, cr.Name)
		assert.Equal(t, checkStatusCompleted, cr.Status)
		assert.Equal(t, checkConclusionSuccess, cr.Conclusion)
		assert.Equal(t, "abc123", cr.HeadSHA)
	case <-time.After(webhookIntegrationCheckRunDeadline):
		t.Fatal("timed out waiting for no-schema passing aggregate check run")
	}
	var check *storage.Check
	var checkErr error
	require.Eventually(t, func() bool {
		check, checkErr = svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, aggregateSentinel, aggregateSentinel, aggregateSentinel)
		return checkErr == nil && check != nil
	}, webhookIntegrationCheckRunDeadline, 100*time.Millisecond, "no-schema auto-plan should store a passing aggregate check")
	require.NoError(t, checkErr)
	require.NotNil(t, check)
	assert.Equal(t, "abc123", check.HeadSHA)
	assert.Equal(t, checkStatusCompleted, check.Status)
	assert.Equal(t, checkConclusionSuccess, check.Conclusion)
	select {
	case body := <-comments:
		t.Fatalf("expected no plan comment for no-schema auto-plan, got: %s", body)
	case <-time.After(250 * time.Millisecond):
	}
}

// TestE2EAutoPlanBootstrapDoesNotBlockWebhookAck verifies that pull-request
// auto-plan acknowledges the webhook before GitHub client construction runs.
// Client construction can perform remote GitHub calls, so the response path must
// not depend on it being fast or available.
func TestE2EAutoPlanBootstrapDoesNotBlockWebhookAck(t *testing.T) {
	svc := setupE2EServiceWithAllowedEnvs(t, []string{"staging"})
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	bootstrapStarted := make(chan struct{})
	bootstrapRelease := make(chan struct{})
	factory := &fakeClientFactory{
		forInstallationStarted: bootstrapStarted,
		forInstallationRelease: bootstrapRelease,
		forInstallationErr:     errors.New("github unavailable"),
	}

	h := NewHandler(svc, factory, nil, logger)
	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "opened",
		headSHA: "abc123",
		headRef: "feature-branch",
	}, nil)

	rr := httptest.NewRecorder()
	served := make(chan struct{})
	go func() {
		h.ServeHTTP(rr, req)
		close(served)
	}()

	select {
	case <-served:
	case <-time.After(time.Second):
		require.FailNow(t, "webhook response waited for auto-plan bootstrap")
	}
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "auto-plan started")

	select {
	case <-bootstrapStarted:
	case <-time.After(webhookIntegrationCheckRunDeadline):
		require.FailNow(t, "timed out waiting for async auto-plan bootstrap")
	}
	close(bootstrapRelease)
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
	assert.Contains(t, rr.Body.String(), "auto-plan started")

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
		var check *storage.Check
		var checkErr error
		require.Eventually(t, func() bool {
			check, checkErr = svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, env, aggregateSentinel, aggregateSentinel)
			return checkErr == nil && check != nil
		}, webhookIntegrationCheckRunDeadline, 100*time.Millisecond, "stored aggregate check should be visible for %s", env)
		require.NoError(t, checkErr)
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
	prFetchAttempted := make(chan struct{})
	var prFetchAttemptedOnce sync.Once
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		prFetchAttemptedOnce.Do(func() { close(prFetchAttempted) })
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
	assert.Contains(t, rr.Body.String(), "auto-plan started")
	select {
	case <-prFetchAttempted:
	case <-time.After(webhookIntegrationCheckRunDeadline):
		t.Fatal("timed out waiting for auto-plan to verify PR head")
	}

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

// A PR that changes schema files under a directory the server config manages
// (databases.<db>.allowed_dirs) but contains no schemabot.yaml must fail closed:
// dropping or omitting the config cannot silently land DDL ungated. SchemaBot
// publishes a blocking aggregate instead of a passing one. To offboard a
// directory an operator removes it from allowed_dirs in the server config.
func TestE2EAutoPlanManagedDirMissingConfigBlocks(t *testing.T) {
	dbName := "webhook_autoplan_managed_dir_missing_config"
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

	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}
	// Empty schemabotConfig: the schema directory is server-managed via
	// allowed_dirs, but no schemabot.yaml resolves for it (as if removed).
	result := setupFakeGitHubForPlan(t, mux, schemaFiles, "", dbName)

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

	var aggregateCheck checkRunCapture
	select {
	case aggregateCheck = <-result.checkRuns:
	case <-time.After(webhookIntegrationCheckRunDeadline):
		t.Fatal("timed out waiting for managed-dir-missing-config aggregate check run")
	}
	assert.Equal(t, aggregateCheckName, aggregateCheck.Name)
	assert.Equal(t, "completed", aggregateCheck.Status)
	assert.Equal(t, "failure", aggregateCheck.Conclusion)
	require.NotNil(t, aggregateCheck.Output)
	assert.Contains(t, aggregateCheck.Output.Summary, "schemabot.yaml")
	assert.Contains(t, aggregateCheck.Output.Summary, "allowed_dirs")

	var check *storage.Check
	var checkErr error
	require.Eventually(t, func() bool {
		check, checkErr = svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, aggregateSentinel, aggregateSentinel, aggregateSentinel)
		return checkErr == nil && check != nil
	}, webhookIntegrationCheckRunDeadline, 100*time.Millisecond, "managed-dir-missing-config auto-plan must store a failing aggregate check")
	require.NoError(t, checkErr)
	require.NotNil(t, check)
	assert.Equal(t, "abc123", check.HeadSHA)
	assert.Equal(t, "completed", check.Status)
	assert.Equal(t, "failure", check.Conclusion)
	assert.Equal(t, "managed_dir_missing_config", check.BlockingReason)

	plans, err := svc.Storage().Plans().GetByPR(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	for _, plan := range plans {
		assert.NotEqual(t, dbName, plan.Database, "a blocked auto-plan must not store a plan for the managed database")
	}
}

// Moving a managed schema directory in one PR — the schemabot.yaml and its
// schema files relocate together — must not be mistaken for an unmanaged schema
// change. The destination files stay covered by the moved config, so auto-plan
// proceeds normally instead of failing closed. Both the old and new directories
// are in allowed_dirs, as for an operator-gated move.
func TestE2EAutoPlanSchemaDirMoveNotBlocked(t *testing.T) {
	dbName := "webhook_autoplan_schema_dir_move"
	svc := setupE2EService(t, dbName)
	dbConfig := svc.Config().Databases[dbName]
	dbConfig.AllowedRepos = []string{"octocat/hello-world"}
	dbConfig.AllowedDirs = []string{"schema", "old/schema"}
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
	// The PR relocates the config and schema file out of old/schema; the moved
	// config lands at schema/schemabot.yaml (served by the helper) and covers the
	// new schema/<db>/users.sql.
	prFiles := []*gh.CommitFile{
		{Filename: new("old/schema/schemabot.yaml"), Status: new("removed")},
		{Filename: new("old/schema/users.sql"), Status: new("removed")},
		{Filename: new("schema/schemabot.yaml"), Status: new("added")},
		{Filename: new("schema/" + dbName + "/users.sql"), Status: new("added")},
	}
	result := setupFakeGitHubForPlanWithPRFiles(t, mux, schemaFiles, schemabotConfig, dbName, prFiles)

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

	// A normal plan-with-changes check, not a fail-closed failure, proves the
	// move was not mistaken for an unmanaged schema change.
	select {
	case cr := <-result.checkRuns:
		assert.Contains(t, cr.Name, "SchemaBot")
		assert.Equal(t, "action_required", cr.Conclusion)
	case <-time.After(webhookIntegrationCheckRunDeadline):
		t.Fatal("timed out waiting for check run")
	}

	check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, aggregateSentinel, aggregateSentinel, aggregateSentinel)
	require.NoError(t, err)
	if check != nil {
		assert.NotEqual(t, "managed_dir_missing_config", check.BlockingReason, "a clean move must not be blocked as a missing-config change")
	}
}
