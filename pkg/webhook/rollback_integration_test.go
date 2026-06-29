//go:build integration

// Rollback command webhook integration tests.

package webhook

import (
	"database/sql"
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

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// TestE2ERollbackPlanViaWebhook tests the full rollback flow:
// 1. Plan + apply a schema change via the service (simulating a prior apply)
// 2. Run "schemabot rollback <apply-id> -e staging" via webhook
// 3. Verify the rollback plan comment is posted with reverse DDL
func TestE2ERollbackPlanViaWebhook(t *testing.T) {
	dbName := "webhook_rollback"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	// Step 1: Create an initial table in the target DB (the "before" state)
	appDSN := strings.Replace(e2eTargetDSN, "/target_test", "/"+dbName, 1) + "&multiStatements=true"
	db, err := sql.Open("mysql", appDSN)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)
	_ = db.Close()

	// Step 2: Plan + apply adding an index (this stores original files for rollback)
	schemaWithIndex := "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `idx_name` (`name`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
	prNumber := int32(1)
	planReq := api.PlanRequest{
		Database:    dbName,
		Environment: "staging",
		Type:        "mysql",
		Repository:  "octocat/hello-world",
		PullRequest: &prNumber,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			dbName: {Files: map[string]string{"users.sql": schemaWithIndex}},
		},
	}
	planResp, err := svc.ExecutePlan(ctx, planReq)
	require.NoError(t, err)
	require.NotEmpty(t, planResp.Changes, "expected DDL changes")

	applyReq := api.ApplyRequest{
		PlanID:      planResp.PlanID,
		Environment: "staging",
		Options:     map[string]string{"allow_unsafe": "true"},
	}
	applyResp, applyID, err := svc.ExecuteApply(ctx, applyReq)
	require.NoError(t, err)
	require.True(t, applyResp.Accepted)
	require.Greater(t, applyID, int64(0))

	// Wait for apply to complete
	require.Eventually(t, func() bool {
		apply, err := svc.Storage().Applies().Get(ctx, applyID)
		if err != nil || apply == nil {
			return false
		}
		return state.IsState(apply.State, state.Apply.Completed)
	}, 30*time.Second, 500*time.Millisecond, "apply should complete")

	// Step 3: Set up fake GitHub and webhook handler
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	// Schema files still have the index (current desired state)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{
		"users.sql": schemaWithIndex,
	}, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	// Get the apply identifier for the rollback command
	storedApply, err := svc.Storage().Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, storedApply)

	require.NoError(t, svc.Storage().Checks().Upsert(ctx, &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: dbName,
		CheckRunID:   42,
		ApplyID:      applyID,
		HasChanges:   false,
		Status:       checkStatusCompleted,
		Conclusion:   checkConclusionSuccess,
	}))

	// Step 4: Send rollback command with the apply ID
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: fmt.Sprintf("schemabot rollback %s -e staging", storedApply.ApplyIdentifier),
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "rollback started")

	// Step 5: Verify rollback plan comment was posted
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "## Schema Rollback Plan")
		assert.Contains(t, body, "DROP INDEX", "rollback should drop the index we added")
		assert.Contains(t, body, "schemabot rollback-confirm -e staging")
		assert.Contains(t, body, "schemabot unlock")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for rollback plan comment")
	}

	// Step 6: Verify lock was acquired
	lock, err := svc.Storage().Locks().Get(ctx, dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock, "lock should be held after rollback command")
	assert.Equal(t, "octocat/hello-world#1", lock.Owner)
	assert.True(t, strings.HasPrefix(lock.PendingPlanID, rollbackPendingPlanPrefix), "rollback lock should pin a tagged rollback plan")

	check, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check)
	assert.Equal(t, checkStatusCompleted, check.Status)
	assert.Equal(t, checkConclusionSuccess, check.Conclusion)
	assert.Equal(t, applyID, check.ApplyID)

	select {
	case cr := <-result.checkRuns:
		t.Fatalf("rollback planning should not update check runs before confirmation, got: %+v", cr)
	case <-time.After(500 * time.Millisecond):
	}
}

// TestE2ERollbackApplyNotFound tests rollback with a nonexistent apply ID.
func TestE2ERollbackApplyNotFound(t *testing.T) {
	dbName := "webhook_rollback_none"
	svc := setupE2EService(t, dbName)

	h, comments, _ := newTestHandler(t)
	h.service = svc

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback apply_deadbeef0000 -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-comments:
		assert.Contains(t, body, "Apply Not Found")
		assert.Contains(t, body, "apply_deadbeef0000")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for comment")
	}
}

// TestE2ERollbackConfirmNoLock tests rollback-confirm when no lock is held.
func TestE2ERollbackConfirmNoLock(t *testing.T) {
	dbName := "webhook_rbconfirm_nolock"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	_, err := svc.Storage().Applies().Create(t.Context(), &storage.Apply{
		ApplyIdentifier: "apply_aabbccdd0012",
		Database:        dbName,
		DatabaseType:    "mysql",
		Environment:     "staging",
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		State:           "completed",
		Engine:          "spirit",
	})
	require.NoError(t, err)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback-confirm -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// Should post a "no lock found" comment
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "No Lock Found")
		assert.Contains(t, body, "schemabot rollback <apply-id> -e staging")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for no-lock comment")
	}
}

// TestE2ERollbackConfirmExecutesAndPostsComments verifies the full rollback-confirm
// flow: rollback plan → rollback-confirm → apply executes → summary comment posted
// on the correct PR. This catches regressions where watchApplyProgress loses the
// repo/PR/installationID context and fails to post comments.
func TestE2ERollbackConfirmExecutesAndPostsComments(t *testing.T) {
	dbName := "webhook_rbconfirm_exec"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	// Step 1: Create initial table
	cfg, err := mysql.ParseDSN(e2eTargetDSN)
	require.NoError(t, err)
	cfg.DBName = dbName
	cfg.MultiStatements = true
	appDSN := cfg.FormatDSN()
	db, err := sql.Open("mysql", appDSN)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)
	_ = db.Close()

	// Step 2: Plan + apply adding an index (captures original files for rollback)
	schemaWithIndex := "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `idx_name` (`name`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
	prNumber := int32(1)
	planResp, err := svc.ExecutePlan(ctx, api.PlanRequest{
		Database:    dbName,
		Environment: "staging",
		Type:        "mysql",
		Repository:  "octocat/hello-world",
		PullRequest: &prNumber,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			dbName: {Files: map[string]string{"users.sql": schemaWithIndex}},
		},
	})
	require.NoError(t, err)

	applyResp, applyID, err := svc.ExecuteApply(ctx, api.ApplyRequest{
		PlanID:      planResp.PlanID,
		Environment: "staging",
		Options:     map[string]string{"allow_unsafe": "true"},
	})
	require.NoError(t, err)
	require.True(t, applyResp.Accepted)

	require.Eventually(t, func() bool {
		a, err := svc.Storage().Applies().Get(ctx, applyID)
		return err == nil && a != nil && a.State == "completed"
	}, 30*time.Second, 500*time.Millisecond, "initial apply should complete")

	// Step 3: Run rollback to generate plan and acquire lock
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{
		"users.sql": schemaWithIndex,
	}, schemabotConfig, dbName)

	h := newE2EHandler(t, svc, client)

	storedApply, err := svc.Storage().Applies().Get(ctx, applyID)
	require.NoError(t, err)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: fmt.Sprintf("schemabot rollback %s -e staging", storedApply.ApplyIdentifier),
		isPR:    true,
	}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// Drain the rollback plan comment
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Rollback Plan")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for rollback plan comment")
	}

	// Step 4: Run rollback-confirm — this triggers the apply + watchApplyProgress
	req = buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback-confirm -e staging",
		isPR:    true,
	}, nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// Step 5: Verify that the summary comment arrives on the PR.
	// This is the critical assertion — if repo/PR/installationID are wrong,
	// the comment goes to the wrong URL and never reaches the channel.
	gotSummary := false
	deadline := time.After(webhookIntegrationPollDeadline)
	for !gotSummary {
		select {
		case body := <-result.comments:
			if strings.Contains(body, "Schema Change") && (strings.Contains(body, "Applied") || strings.Contains(body, "Complete") || strings.Contains(body, "Failed")) {
				gotSummary = true
				assert.Contains(t, body, "DROP INDEX", "rollback should drop the index")
			}
		case <-deadline:
			t.Fatal("timed out waiting for rollback summary comment — " +
				"watchApplyProgress may have lost repo/PR/installationID context")
		}
	}
}

// TestE2ERollbackConfirmUpdatesCheckToActionRequired verifies that after a
// rollback-confirm completes, the check run transitions to action_required
// (not success) since the PR's schema changes have been undone.
func TestE2ERollbackConfirmUpdatesCheckToActionRequired(t *testing.T) {
	dbName := "webhook_rb_check"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	// Step 1: Create initial table
	cfg, err := mysql.ParseDSN(e2eTargetDSN)
	require.NoError(t, err)
	cfg.DBName = dbName
	cfg.MultiStatements = true
	db, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)
	_ = db.Close()

	// Step 2: Plan + apply adding an index
	schemaWithIndex := "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `idx_name` (`name`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
	prNumber := int32(1)
	planResp, err := svc.ExecutePlan(ctx, api.PlanRequest{
		Database:    dbName,
		Environment: "staging",
		Type:        "mysql",
		Repository:  "octocat/hello-world",
		PullRequest: &prNumber,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			dbName: {Files: map[string]string{"users.sql": schemaWithIndex}},
		},
	})
	require.NoError(t, err)

	applyResp, applyID, err := svc.ExecuteApply(ctx, api.ApplyRequest{
		PlanID:      planResp.PlanID,
		Environment: "staging",
		Options:     map[string]string{"allow_unsafe": "true"},
	})
	require.NoError(t, err)
	require.True(t, applyResp.Accepted)

	require.Eventually(t, func() bool {
		a, err := svc.Storage().Applies().Get(ctx, applyID)
		return err == nil && a != nil && a.State == "completed"
	}, 30*time.Second, 500*time.Millisecond, "initial apply should complete")

	// Step 3: Seed a check record (simulates what plan/apply creates)
	err = svc.Storage().Checks().Upsert(ctx, &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: dbName,
		CheckRunID:   42,
		ApplyID:      applyID,
		HasChanges:   false,
		Status:       checkStatusCompleted,
		Conclusion:   checkConclusionSuccess,
	})
	require.NoError(t, err)

	// Step 4: Set up fake GitHub and run rollback + rollback-confirm
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{
		"users.sql": schemaWithIndex,
	}, schemabotConfig, dbName)

	h := newE2EHandler(t, svc, client)

	storedApply, err := svc.Storage().Applies().Get(ctx, applyID)
	require.NoError(t, err)

	// Run rollback
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: fmt.Sprintf("schemabot rollback %s -e staging", storedApply.ApplyIdentifier),
		isPR:    true,
	}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case <-result.comments:
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for rollback plan comment")
	}

	// Run rollback-confirm
	req = buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback-confirm -e staging",
		isPR:    true,
	}, nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// Wait for the rollback apply to complete and check to be updated
	require.Eventually(t, func() bool {
		check, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
		if err != nil {
			return false
		}
		return isRollbackActionRequiredWithoutApplyOwnership(check)
	}, webhookIntegrationPollDeadline, 500*time.Millisecond,
		"check should transition to action_required without active apply ownership after rollback")

	deadline := time.After(webhookIntegrationPollDeadline)
	for {
		select {
		case cr := <-result.checkRuns:
			if cr.Name == aggregateCheckName &&
				cr.Status == checkStatusCompleted &&
				cr.Conclusion == checkConclusionActionRequired {
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for rollback aggregate to become action_required")
		}
	}
}

func isRollbackActionRequiredWithoutApplyOwnership(check *storage.Check) bool {
	if check == nil {
		return false
	}
	return check.Status == checkStatusCompleted &&
		check.Conclusion == checkConclusionActionRequired &&
		check.ApplyID == 0 &&
		check.BlockingReason == rollbackCompletedBlock.blockingReason &&
		check.ErrorMessage == rollbackCompletedBlock.message
}

// TestE2ERollbackIgnoredByNonOwningInstance verifies that in a multi-instance
// setup, an instance that doesn't own the apply's environment silently ignores
// rollback commands instead of reacting or posting "Apply Not Found".
func TestE2ERollbackIgnoredByNonOwningInstance(t *testing.T) {
	dbName := "webhook_rb_multienv"
	svc := setupE2EServiceWithAllowedEnvs(t, []string{"production"})
	ctx := t.Context()

	// Seed a completed apply for staging (owned by the other instance)
	_, err := svc.Storage().Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply-aabbccdd0011",
		Database:        dbName,
		DatabaseType:    "mysql",
		Environment:     "staging",
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		State:           "completed",
		Engine:          "spirit",
	})
	require.NoError(t, err)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	comments := make(chan string, 10)
	reactions := make(chan string, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Content string `json:"content"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		reactions <- body.Content
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	commands := []string{
		"schemabot rollback apply-aabbccdd0011 -e staging",
		"schemabot rollback-confirm apply-aabbccdd0011 -e staging",
		"schemabot rollback-confirm -e staging",
	}
	for _, command := range commands {
		req := buildWebhookRequest(t, webhookPayloadOpts{
			comment: command,
			isPR:    true,
		}, nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		require.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "environment handled by another instance")
	}

	// The production instance should NOT post any comment (it silently ignores
	// the staging commands). Wait long enough for any async handler to fire.
	select {
	case body := <-comments:
		t.Fatalf("production instance should not post a comment for staging rollback command, got: %s", body)
	case reaction := <-reactions:
		t.Fatalf("production instance should not react to staging rollback command, got: %s", reaction)
	case <-time.After(2 * time.Second):
		// Expected: no comment or reaction posted.
	}
}
