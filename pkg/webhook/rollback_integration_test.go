//go:build integration

// Rollback command webhook integration tests.

package webhook

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	mysql "github.com/block/mysql"
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
	db, err := sql.Open("block-mysql", appDSN)
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

// rollbackTestSchemaWithIndex is the PR's desired users table: the seeded
// table plus an index, so rolling the PR back drops the index again.
const rollbackTestSchemaWithIndex = "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `idx_name` (`name`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"

// rollbackTestSchemaWithoutIndex is the seeded users table with no index, so
// an apply of it after rollbackTestSchemaWithIndex drops the index and rolling
// that apply back adds it again.
const rollbackTestSchemaWithoutIndex = "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"

// seedCompletedIndexApply creates the users table on the target and runs PR
// octocat/hello-world#1's apply adding an index to it through to completion,
// capturing the original files a rollback needs. It returns the completed
// apply's ID.
func seedCompletedIndexApply(t *testing.T, svc *api.Service, dbName string) int64 {
	t.Helper()
	ctx := t.Context()
	createRollbackTestUsersTable(t, dbName)

	prNumber := int32(1)
	planResp, err := svc.ExecutePlan(ctx, api.PlanRequest{
		Database:    dbName,
		Environment: "staging",
		Type:        "mysql",
		Repository:  "octocat/hello-world",
		PullRequest: &prNumber,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			dbName: {Files: map[string]string{"users.sql": rollbackTestSchemaWithIndex}},
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
	return applyID
}

// TestE2ERollbackConfirmExecutesAndPostsComments verifies the full rollback-confirm
// flow: rollback plan → rollback-confirm → apply executes → summary comment posted
// on the correct PR. The rollback drops the index the PR added, which is an
// unsafe change: the plan comment names it, a rollback-confirm without
// --allow-unsafe is refused with the exact command to re-issue while the lock
// keeps pinning the plan, and the re-issued command runs the rollback with the
// operator's consent recorded on the rollback apply. This also catches
// regressions where watchApplyProgress loses the repo/PR/installationID context
// and fails to post comments.
func TestE2ERollbackConfirmExecutesAndPostsComments(t *testing.T) {
	dbName := "webhook_rbconfirm_exec"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	// Steps 1-2: Create the initial table, then plan + apply adding an index
	// (captures original files for rollback).
	schemaWithIndex := rollbackTestSchemaWithIndex
	applyID := seedCompletedIndexApply(t, svc, dbName)

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

	// The rollback plan comment names the index drop as an unsafe change and
	// says confirming it takes --allow-unsafe.
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Rollback Plan")
		assert.Contains(t, body, "1 unsafe change detected")
		assert.Contains(t, body, "`idx_name`")
		assert.Contains(t, body, "To confirm this rollback, add `--allow-unsafe` to confirm 1 unsafe change (`users`)")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for rollback plan comment")
	}
	pinnedLock, err := svc.Storage().Locks().Get(ctx, dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, pinnedLock)

	// Step 4: rollback-confirm without --allow-unsafe is refused, names the
	// index drop and the command to re-issue, creates no rollback apply, and
	// leaves the lock pinning the same rollback plan.
	req = buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback-confirm -e staging",
		isPR:    true,
	}, nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	refusal := awaitCommentContaining(t, result, "Rollback rejected")
	assert.Contains(t, refusal, "**⛔ Rollback rejected**: 1 unsafe change detected")
	assert.Contains(t, refusal, "`idx_name`")
	assert.Contains(t, refusal, "DROP INDEX")
	assert.Contains(t, refusal, "```\nschemabot rollback-confirm -e staging --allow-unsafe\n```")
	applies, err := svc.Storage().Applies().GetByDatabase(ctx, dbName, "mysql", "staging")
	require.NoError(t, err)
	for _, a := range applies {
		assert.False(t, a.IsRollback(), "a refused rollback-confirm must not create a rollback apply (found %s)", a.ApplyIdentifier)
	}
	lockAfterRefusal, err := svc.Storage().Locks().Get(ctx, dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lockAfterRefusal, "a refused rollback-confirm must keep the lock")
	assert.Equal(t, pinnedLock.PendingPlanID, lockAfterRefusal.PendingPlanID, "a refused rollback-confirm must keep the rollback plan pinned")

	// Step 5: the re-issued rollback-confirm with --allow-unsafe triggers the
	// apply + watchApplyProgress.
	req = buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback-confirm -e staging --allow-unsafe",
		isPR:    true,
	}, nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// Step 6: Verify that the summary comment arrives on the PR with rollback
	// vocabulary — a completed rollback announces "Rollback Complete", never a
	// green-check applied schema change. This is also the critical delivery
	// assertion — if repo/PR/installationID are wrong, the comment goes to the
	// wrong URL and never reaches the channel.
	gotSummary := false
	deadline := time.After(webhookIntegrationPollDeadline)
	for !gotSummary {
		select {
		case body := <-result.comments:
			if strings.Contains(body, "Rollback Complete") {
				gotSummary = true
				assert.Contains(t, body, "⏪", "the rollback summary carries the rewind emoji, not the green check")
				assert.Contains(t, body, "Rolled back successfully")
				assert.Contains(t, body, "DROP INDEX", "rollback should drop the index")
				assert.NotContains(t, body, "Schema Change Applied")
			}
		case <-deadline:
			t.Fatal("timed out waiting for the rollback summary comment — " +
				"watchApplyProgress may have lost repo/PR/installationID context")
		}
	}

	// Step 7: The rollback apply is attributed to the user who confirmed it,
	// in the same caller format as any other PR command, so history and
	// progress views show who acted rather than the lock owner, and it carries
	// the --allow-unsafe consent the operator gave.
	rollbackApply := requireSingleRollbackApply(t, svc, dbName)
	assert.Equal(t, "github:testuser@octocat/hello-world#1", rollbackApply.Caller)
	assert.True(t, rollbackApply.GetOptions().AllowUnsafe, "the rollback apply must carry the operator's --allow-unsafe consent")
}

// requireSingleRollbackApply returns the one rollback apply stored for the
// database in staging.
func requireSingleRollbackApply(t *testing.T, svc *api.Service, dbName string) *storage.Apply {
	t.Helper()
	applies, err := svc.Storage().Applies().GetByDatabase(t.Context(), dbName, "mysql", "staging")
	require.NoError(t, err)
	var rollbackApply *storage.Apply
	for _, a := range applies {
		if a.IsRollback() {
			require.Nil(t, rollbackApply, "expected exactly one rollback apply row")
			rollbackApply = a
		}
	}
	require.NotNil(t, rollbackApply, "rollback apply row should exist")
	return rollbackApply
}

// A PR's apply dropped the index on users, so rolling it back adds the index
// again: a rollback with no unsafe changes. The rollback plan comment carries
// no unsafe warning, rollback-confirm without --allow-unsafe runs it, and the
// rollback apply carries no unsafe consent the operator never gave.
func TestE2ERollbackConfirmSafeRollbackRunsWithoutAllowUnsafe(t *testing.T) {
	dbName := "webhook_rbconfirm_safe"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	// The PR first adds the index, then drops it again; the drop is the apply
	// rolled back.
	seedCompletedIndexApply(t, svc, dbName)
	prNumber := int32(1)
	planResp, err := svc.ExecutePlan(ctx, api.PlanRequest{
		Database:    dbName,
		Environment: "staging",
		Type:        "mysql",
		Repository:  "octocat/hello-world",
		PullRequest: &prNumber,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			dbName: {Files: map[string]string{"users.sql": rollbackTestSchemaWithoutIndex}},
		},
	})
	require.NoError(t, err)
	_, dropApplyID, err := svc.ExecuteApply(ctx, api.ApplyRequest{
		PlanID:      planResp.PlanID,
		Environment: "staging",
		Options:     map[string]string{"allow_unsafe": "true"},
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		a, err := svc.Storage().Applies().Get(ctx, dropApplyID)
		return err == nil && a != nil && state.IsState(a.State, state.Apply.Completed)
	}, webhookIntegrationPollDeadline, 500*time.Millisecond, "the index drop apply should complete")
	dropApply, err := svc.Storage().Applies().Get(ctx, dropApplyID)
	require.NoError(t, err)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")
	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{
		"users.sql": rollbackTestSchemaWithoutIndex,
	}, schemabotConfig, dbName)
	h := newE2EHandler(t, svc, client)
	sendComment := func(comment string) {
		t.Helper()
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{comment: comment, isPR: true}, nil))
		require.Equal(t, http.StatusOK, rr.Code)
	}

	sendComment(fmt.Sprintf("schemabot rollback %s -e staging", dropApply.ApplyIdentifier))
	plan := awaitCommentContaining(t, result, "Rollback Plan")
	assert.Contains(t, plan, "ADD INDEX")
	assert.NotContains(t, plan, "unsafe")
	assert.Contains(t, plan, "To confirm this rollback, comment:\n```\nschemabot rollback-confirm -e staging\n```")

	sendComment("schemabot rollback-confirm -e staging")
	summary := awaitCommentContaining(t, result, "Rollback Complete")
	assert.Contains(t, summary, "ADD INDEX")

	rollbackApply := requireSingleRollbackApply(t, svc, dbName)
	assert.False(t, rollbackApply.GetOptions().AllowUnsafe, "a rollback with no unsafe changes must not send allow_unsafe")
	assert.True(t, rollbackApply.GetOptions().Rollback)
}

// A PR's apply added an index, so rolling it back drops it: an unsafe change
// the rollback plan comment names. When GitHub rejects that comment, the pin
// rollback-confirm would act on does not survive. --allow-unsafe on a later
// confirm would otherwise consent to a drop the operator was never shown. A
// lock the rollback command acquired is released; a lock the PR already held
// keeps its hold with no pending rollback.
func TestE2ERollbackPinIsWithdrawnWhenThePlanCommentCannotBePosted(t *testing.T) {
	tests := []struct {
		name          string
		dbName        string
		prHeldTheLock bool
	}{
		{name: "a lock the rollback command acquired is released", dbName: "webhook_rb_undisclosed_new"},
		{name: "a lock the PR already held keeps its hold without the pin", dbName: "webhook_rb_undisclosed_held", prHeldTheLock: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := setupE2EService(t, tt.dbName)
			ctx := t.Context()
			applyID := seedCompletedIndexApply(t, svc, tt.dbName)
			storedApply, err := svc.Storage().Applies().Get(ctx, applyID)
			require.NoError(t, err)
			if tt.prHeldTheLock {
				require.NoError(t, svc.Storage().Locks().Acquire(ctx, &storage.Lock{
					DatabaseName: tt.dbName, DatabaseType: "mysql", Owner: "octocat/hello-world#1",
					Repository: "octocat/hello-world", PullRequest: 1,
				}))
			}

			mux := http.NewServeMux()
			server := httptest.NewServer(mux)
			t.Cleanup(server.Close)
			client := gh.NewClient(nil)
			client.BaseURL, _ = url.Parse(server.URL + "/")
			result := setupFakeGitHubForPlan(t, mux, map[string]string{
				"users.sql": rollbackTestSchemaWithIndex,
			}, fmt.Sprintf("database: %s\ntype: mysql\n", tt.dbName), tt.dbName)
			result.FailCommentPost.Store(true)
			h := newE2EHandler(t, svc, client)

			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{
				comment: fmt.Sprintf("schemabot rollback %s -e staging", storedApply.ApplyIdentifier),
				isPR:    true,
			}, nil))
			require.Equal(t, http.StatusOK, rr.Code)

			// The plan comment was attempted, naming the drop, and GitHub rejected it.
			plan := awaitCommentContaining(t, result, "Rollback Plan")
			assert.Contains(t, plan, "DROP INDEX")

			require.Eventually(t, func() bool {
				lock, err := svc.Storage().Locks().Get(ctx, tt.dbName, "mysql")
				if err != nil {
					return false
				}
				if !tt.prHeldTheLock {
					return lock == nil
				}
				return lock != nil && lock.Owner == "octocat/hello-world#1" && lock.PendingPlanID == ""
			}, webhookIntegrationPollDeadline, 100*time.Millisecond,
				"a rollback pin whose plan comment never landed must not be left for rollback-confirm to act on")
		})
	}
}

// lockRepinningStorage wraps the service's storage so a test can change the
// database lock in the window between rollback-confirm resolving its pinned
// plan and the rollback apply row being written.
type lockRepinningStorage struct {
	storage.Storage
	applies *lockRepinningApplyStore
}

func (s *lockRepinningStorage) Applies() storage.ApplyStore { return s.applies }

// lockRepinningApplyStore runs an armed hook once, immediately before the next
// apply row is created, then delegates to the real store.
type lockRepinningApplyStore struct {
	storage.ApplyStore

	mu           sync.Mutex
	beforeCreate func(ctx context.Context) error
}

func (s *lockRepinningApplyStore) arm(hook func(ctx context.Context) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.beforeCreate = hook
}

func (s *lockRepinningApplyStore) CreateWithGroupedOperations(ctx context.Context, apply *storage.Apply, groups []*storage.ApplyOperationWithTasks) (int64, error) {
	s.mu.Lock()
	hook := s.beforeCreate
	s.beforeCreate = nil
	s.mu.Unlock()
	if hook != nil {
		if err := hook(ctx); err != nil {
			return 0, fmt.Errorf("before-create hook for apply %s: %w", apply.ApplyIdentifier, err)
		}
	}
	return s.ApplyStore.CreateWithGroupedOperations(ctx, apply, groups)
}

// rollbackConfirmRaceFixture is a PR whose index apply has completed and whose
// `schemabot rollback` has posted its plan and pinned the lock to it, with a
// hook that changes the lock in the window between rollback-confirm resolving
// that pin and the rollback apply row being written.
type rollbackConfirmRaceFixture struct {
	svc           *api.Service
	h             *Handler
	result        *planFlowResult
	applies       *lockRepinningApplyStore
	lockStore     storage.LockStore
	confirmedLock *storage.Lock
}

func setupRollbackConfirmRace(t *testing.T, dbName string) *rollbackConfirmRaceFixture {
	t.Helper()
	f := &rollbackConfirmRaceFixture{}
	f.svc = setupE2EServiceWithStorage(t, dbName, func(st storage.Storage) storage.Storage {
		f.applies = &lockRepinningApplyStore{ApplyStore: st.Applies()}
		f.lockStore = st.Locks()
		return &lockRepinningStorage{Storage: st, applies: f.applies}
	})
	ctx := t.Context()

	cfg, err := mysql.ParseDSN(e2eTargetDSN)
	require.NoError(t, err)
	cfg.DBName = dbName
	cfg.MultiStatements = true
	db, err := sql.Open("block-mysql", cfg.FormatDSN())
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	schemaWithIndex := "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `idx_name` (`name`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
	prNumber := int32(1)
	planResp, err := f.svc.ExecutePlan(ctx, api.PlanRequest{
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

	applyResp, applyID, err := f.svc.ExecuteApply(ctx, api.ApplyRequest{
		PlanID:      planResp.PlanID,
		Environment: "staging",
		Options:     map[string]string{"allow_unsafe": "true"},
	})
	require.NoError(t, err)
	require.True(t, applyResp.Accepted)

	require.Eventually(t, func() bool {
		a, err := f.svc.Storage().Applies().Get(ctx, applyID)
		return err == nil && a != nil && state.IsState(a.State, state.Apply.Completed)
	}, webhookIntegrationPollDeadline, 500*time.Millisecond, "initial apply should complete")

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	f.result = setupFakeGitHubForPlan(t, mux, map[string]string{
		"users.sql": schemaWithIndex,
	}, schemabotConfig, dbName)

	f.h = newE2EHandler(t, f.svc, client)

	storedApply, err := f.svc.Storage().Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, storedApply)

	f.sendComment(t, fmt.Sprintf("schemabot rollback %s -e staging", storedApply.ApplyIdentifier))
	select {
	case body := <-f.result.comments:
		require.Contains(t, body, "Rollback Plan")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for rollback plan comment")
	}

	f.confirmedLock, err = f.lockStore.Get(ctx, dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, f.confirmedLock, "the rollback command should hold the lock")
	require.True(t, strings.HasPrefix(f.confirmedLock.PendingPlanID, rollbackPendingPlanPrefix),
		"the rollback command should pin its plan on the lock")
	return f
}

func (f *rollbackConfirmRaceFixture) sendComment(t *testing.T, comment string) {
	t.Helper()
	rr := httptest.NewRecorder()
	f.h.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{comment: comment, isPR: true}, nil))
	require.Equal(t, http.StatusOK, rr.Code)
}

// TestE2ERollbackConfirmRejectedWhenLockIntentChangesBeforeExecute covers a
// rollback-confirm racing another lock command for the same PR. The operator
// reviews rollback plan A and confirms it; after the confirm resolves plan A
// from the lock but before the rollback apply is stored, the lock changes
// under it: a second rollback command re-pins it to plan B, or an unlock
// releases it. The rollback runs with unsafe changes allowed, so it must only
// run under the lock intent the operator confirmed: the confirm is rejected
// with guidance to re-run the rollback, no rollback apply is created, and the
// lock is left exactly as the other command left it.
func TestE2ERollbackConfirmRejectedWhenLockIntentChangesBeforeExecute(t *testing.T) {
	newerPin := rollbackPendingPlanPrefix + "plan_newer_rollback"
	tests := []struct {
		name       string
		dbName     string
		changeLock func(ctx context.Context, f *rollbackConfirmRaceFixture) error
		assertLock func(t *testing.T, f *rollbackConfirmRaceFixture, lock *storage.Lock)
	}{
		{
			name:   "a newer rollback re-pins the lock",
			dbName: "webhook_rbconfirm_repin",
			changeLock: func(ctx context.Context, f *rollbackConfirmRaceFixture) error {
				return f.lockStore.Acquire(ctx, &storage.Lock{
					DatabaseName:  f.confirmedLock.DatabaseName,
					DatabaseType:  f.confirmedLock.DatabaseType,
					Owner:         f.confirmedLock.Owner,
					Repository:    f.confirmedLock.Repository,
					PullRequest:   f.confirmedLock.PullRequest,
					PendingPlanID: newerPin,
				})
			},
			assertLock: func(t *testing.T, f *rollbackConfirmRaceFixture, lock *storage.Lock) {
				require.NotNil(t, lock, "the rejected confirm must leave the newer lock in place")
				assert.Equal(t, f.confirmedLock.Owner, lock.Owner)
				assert.Equal(t, newerPin, lock.PendingPlanID, "the rejected confirm must not touch the newer pin")
			},
		},
		{
			name:   "an unlock releases the lock",
			dbName: "webhook_rbconfirm_unlock",
			changeLock: func(ctx context.Context, f *rollbackConfirmRaceFixture) error {
				return f.lockStore.Release(ctx, f.confirmedLock.DatabaseName, f.confirmedLock.DatabaseType, f.confirmedLock.Owner)
			},
			assertLock: func(t *testing.T, _ *rollbackConfirmRaceFixture, lock *storage.Lock) {
				assert.Nil(t, lock, "the rejected confirm must not re-acquire the released lock")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := setupRollbackConfirmRace(t, tt.dbName)
			ctx := t.Context()

			f.applies.arm(func(ctx context.Context) error { return tt.changeLock(ctx, f) })
			f.sendComment(t, "schemabot rollback-confirm -e staging --allow-unsafe")

			select {
			case body := <-f.result.comments:
				assert.Contains(t, body, msgRollbackLockIntentChanged)
				assert.NotContains(t, body, storage.ErrLockIntentChanged.Error(), "the raw storage error must not reach the PR comment")
			case <-time.After(webhookIntegrationPollDeadline):
				t.Fatal("timed out waiting for the rollback-confirm rejection comment")
			}

			allApplies, err := f.svc.Storage().Applies().GetByDatabase(ctx, tt.dbName, "mysql", "staging")
			require.NoError(t, err)
			for _, a := range allApplies {
				assert.False(t, a.IsRollback(), "no rollback apply may be created once the lock stops pinning the confirmed plan (found %s)", a.ApplyIdentifier)
			}

			lock, err := f.lockStore.Get(ctx, tt.dbName, "mysql")
			require.NoError(t, err)
			tt.assertLock(t, f, lock)
		})
	}
}

// An operator runs `schemabot rollback` on a PR and, before confirming it, a
// same-PR `schemabot apply -e staging` arrives. The apply is refused with a
// reply naming rollback-confirm and unlock, and the lock stays pinned to the
// rollback plan so rollback-confirm still has it to execute. Once the rollback
// has run, its lock is stale and the same apply command takes the lock over
// for a fresh plan.
func TestE2EApplyKeepsPendingRollbackLock(t *testing.T) {
	dbName := "webhook_apply_rollback_pin"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()
	applyID := seedCompletedIndexApply(t, svc, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{
		"users.sql": rollbackTestSchemaWithIndex,
	}, schemabotConfig, dbName)
	h := newE2EHandler(t, svc, client)

	sendComment := func(comment string) {
		t.Helper()
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{comment: comment, isPR: true}, nil))
		require.Equal(t, http.StatusOK, rr.Code)
	}

	storedApply, err := svc.Storage().Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, storedApply)
	sendComment(fmt.Sprintf("schemabot rollback %s -e staging", storedApply.ApplyIdentifier))
	awaitCommentContaining(t, result, "Rollback Plan")

	rollbackLock, err := svc.Storage().Locks().Get(ctx, dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, rollbackLock)
	require.True(t, strings.HasPrefix(rollbackLock.PendingPlanID, rollbackPendingPlanPrefix), "rollback should pin its plan on the lock")

	// Same-PR apply while the rollback awaits confirmation: refused, lock kept.
	sendComment("schemabot apply -e staging")
	refusal := awaitCommentContaining(t, result, "belongs to a rollback plan that has not been confirmed")
	assert.Contains(t, refusal, "`schemabot rollback-confirm -e staging`")
	assert.Contains(t, refusal, "`schemabot unlock`")
	assert.Contains(t, refusal, dbName)

	lock, err := svc.Storage().Locks().Get(ctx, dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock, "a refused apply must not release the rollback's lock")
	assert.Equal(t, rollbackLock.PendingPlanID, lock.PendingPlanID, "a refused apply must leave the lock pinned to the rollback plan")

	// The rollback runs, which leaves its lock behind as stale.
	sendComment("schemabot rollback-confirm -e staging --allow-unsafe")
	awaitCommentContaining(t, result, "Rollback Complete")

	// The same apply command now re-plans and re-pins the lock to its own plan.
	sendComment("schemabot apply -e staging")
	require.Eventually(t, func() bool {
		lock, err := svc.Storage().Locks().Get(ctx, dbName, "mysql")
		return err == nil && lock != nil && lock.PendingPlanID != "" &&
			!strings.HasPrefix(lock.PendingPlanID, rollbackPendingPlanPrefix)
	}, webhookIntegrationPollDeadline, 100*time.Millisecond, "an apply after the rollback ran should take the stale lock over for its own plan")
}

// createRollbackTestUsersTable creates the users table without its index on
// the target database, so a plan against rollbackTestSchemaWithIndex has an
// index to add.
func createRollbackTestUsersTable(t *testing.T, dbName string) {
	t.Helper()
	cfg, err := mysql.ParseDSN(e2eTargetDSN)
	require.NoError(t, err)
	cfg.DBName = dbName
	db, err := sql.Open("block-mysql", cfg.FormatDSN())
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)
	require.NoError(t, db.Close())
}

// rollbackPinMoment names the step of an apply's lock handling at which a
// concurrent same-PR rollback pins its plan on the lock.
type rollbackPinMoment int

const (
	// pinDuringStaleRelease pins the rollback as the apply releases the stale
	// lock it inspected.
	pinDuringStaleRelease rollbackPinMoment = iota
	// pinDuringPlanning pins the rollback while the apply plans, before it
	// acquires the lock for its own plan.
	pinDuringPlanning
)

// concurrentRollbackStorage wraps the service's storage so a test can slip a
// same-PR rollback's lock pin in between the steps of an apply's lock handling,
// the interleaving a `schemabot rollback` racing a `schemabot apply` on the
// same PR produces.
type concurrentRollbackStorage struct {
	storage.Storage
	locks *concurrentRollbackLocks
}

func (s *concurrentRollbackStorage) Locks() storage.LockStore { return s.locks }

type concurrentRollbackLocks struct {
	storage.LockStore
	pin    *storage.Lock
	moment rollbackPinMoment

	once   sync.Once
	pinErr error
}

func (l *concurrentRollbackLocks) pinRollback(ctx context.Context) {
	l.once.Do(func() {
		l.pinErr = l.Acquire(ctx, l.pin)
	})
}

func (l *concurrentRollbackLocks) ReleaseIfPendingPlanID(ctx context.Context, database, dbType, owner, pendingPlanID string) (bool, error) {
	if l.moment == pinDuringStaleRelease {
		l.pinRollback(ctx)
	}
	return l.LockStore.ReleaseIfPendingPlanID(ctx, database, dbType, owner, pendingPlanID)
}

func (l *concurrentRollbackLocks) AcquireIfPendingPlanID(ctx context.Context, lock *storage.Lock, observedPendingPlanID string) error {
	if l.moment == pinDuringPlanning {
		l.pinRollback(ctx)
	}
	return l.LockStore.AcquireIfPendingPlanID(ctx, lock, observedPendingPlanID)
}

// A same-PR `schemabot rollback` pins its plan on the lock while a same-PR
// `schemabot apply -e staging` is running, at either point the apply touches
// the lock: as it releases a stale apply lock it inspected, or as it acquires
// the lock for the plan it just built. In both the apply is refused with a
// reply saying the lock changed under it, and the rollback's pin stays in
// place so rollback-confirm still has it to execute.
func TestE2EApplyKeepsConcurrentRollbackPin(t *testing.T) {
	tests := []struct {
		name      string
		dbName    string
		moment    rollbackPinMoment
		staleLock bool
		// wantPlans is how many plans the apply builds before it is refused:
		// none when the refusal comes at the stale release, one when it comes
		// at the acquire after planning.
		wantPlans int
	}{
		{
			name:      "rollback pins during the stale release",
			dbName:    "webhook_apply_rb_race_release",
			moment:    pinDuringStaleRelease,
			staleLock: true,
			wantPlans: 0,
		},
		{
			name:      "rollback pins during planning",
			dbName:    "webhook_apply_rb_race_plan",
			moment:    pinDuringPlanning,
			wantPlans: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			locks := &concurrentRollbackLocks{
				moment: tt.moment,
				pin: &storage.Lock{
					DatabaseName: tt.dbName, DatabaseType: "mysql", Owner: "octocat/hello-world#1",
					Repository: "octocat/hello-world", PullRequest: 1,
					PendingPlanID: rollbackPendingPlanPrefix + "plan_concurrent",
				},
			}
			svc := setupE2EServiceWithStorage(t, tt.dbName, func(st storage.Storage) storage.Storage {
				locks.LockStore = st.Locks()
				return &concurrentRollbackStorage{Storage: st, locks: locks}
			})
			ctx := t.Context()
			createRollbackTestUsersTable(t, tt.dbName)

			if tt.staleLock {
				// An apply lock this PR left behind, pinned to a plan that has run.
				require.NoError(t, svc.Storage().Locks().Acquire(ctx, &storage.Lock{
					DatabaseName: tt.dbName, DatabaseType: "mysql", Owner: "octocat/hello-world#1",
					Repository: "octocat/hello-world", PullRequest: 1, PendingPlanID: "plan_stale",
				}))
			}

			mux := http.NewServeMux()
			server := httptest.NewServer(mux)
			t.Cleanup(server.Close)
			client := gh.NewClient(nil)
			client.BaseURL, _ = url.Parse(server.URL + "/")
			result := setupFakeGitHubForPlan(t, mux, map[string]string{
				"users.sql": rollbackTestSchemaWithIndex,
			}, fmt.Sprintf("database: %s\ntype: mysql\n", tt.dbName), tt.dbName)
			h := newE2EHandler(t, svc, client)

			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{comment: "schemabot apply -e staging", isPR: true}, nil))
			require.Equal(t, http.StatusOK, rr.Code)

			refusal := awaitCommentContaining(t, result, "changed the lock on")
			assert.Contains(t, refusal, "`"+tt.dbName+"`")
			assert.Contains(t, refusal, "Retry the apply")
			require.NoError(t, locks.pinErr, "the concurrent rollback must have pinned the lock")

			lock, err := svc.Storage().Locks().Get(ctx, tt.dbName, "mysql")
			require.NoError(t, err)
			require.NotNil(t, lock, "the refused apply must not release the rollback's lock")
			assert.Equal(t, locks.pin.PendingPlanID, lock.PendingPlanID, "the concurrent rollback's pin must survive the apply")
			requireNoApplies(t, svc, tt.dbName)

			plans, err := svc.Storage().Plans().GetByPR(ctx, "octocat/hello-world", 1)
			require.NoError(t, err)
			plans = slices.DeleteFunc(plans, func(p *storage.Plan) bool { return p.Database != tt.dbName })
			assert.Len(t, plans, tt.wantPlans, "the apply should be refused at the step the rollback pinned during")
		})
	}
}

// failingPlanGetStorage wraps the service's storage so a test can make loading
// one plan fail, as a storage outage during that read would.
type failingPlanGetStorage struct {
	storage.Storage
	plans *failingPlanGetStore
}

func (s *failingPlanGetStorage) Plans() storage.PlanStore { return s.plans }

type failingPlanGetStore struct {
	storage.PlanStore

	mu     sync.Mutex
	failID string
}

func (s *failingPlanGetStore) failFor(planIdentifier string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failID = planIdentifier
}

func (s *failingPlanGetStore) Get(ctx context.Context, planIdentifier string) (*storage.Plan, error) {
	s.mu.Lock()
	failID := s.failID
	s.mu.Unlock()
	if failID != "" && planIdentifier == failID {
		return nil, fmt.Errorf("load plan %s: storage unavailable", planIdentifier)
	}
	return s.PlanStore.Get(ctx, planIdentifier)
}

// An operator runs `schemabot rollback` on a PR and, before confirming it,
// a same-PR `schemabot apply -e staging` arrives while storage cannot load the
// rollback plan the lock pins. The apply cannot tell whether the rollback has
// run, so it is rejected with a retry prompt and leaves the lock pinned to the
// rollback rather than treating a read it never completed as proof either way.
func TestE2EApplyRejectedWhenPendingRollbackPlanUnreadable(t *testing.T) {
	dbName := "webhook_apply_rb_unreadable"
	plans := &failingPlanGetStore{}
	svc := setupE2EServiceWithStorage(t, dbName, func(st storage.Storage) storage.Storage {
		plans.PlanStore = st.Plans()
		return &failingPlanGetStorage{Storage: st, plans: plans}
	})
	ctx := t.Context()
	applyID := seedCompletedIndexApply(t, svc, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")
	result := setupFakeGitHubForPlan(t, mux, map[string]string{
		"users.sql": rollbackTestSchemaWithIndex,
	}, fmt.Sprintf("database: %s\ntype: mysql\n", dbName), dbName)
	h := newE2EHandler(t, svc, client)

	sendComment := func(comment string) {
		t.Helper()
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{comment: comment, isPR: true}, nil))
		require.Equal(t, http.StatusOK, rr.Code)
	}

	storedApply, err := svc.Storage().Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, storedApply)
	sendComment(fmt.Sprintf("schemabot rollback %s -e staging", storedApply.ApplyIdentifier))
	awaitCommentContaining(t, result, "Rollback Plan")

	rollbackLock, err := svc.Storage().Locks().Get(ctx, dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, rollbackLock)
	rollbackPlanID, ok := rollbackPlanIDFromLock(rollbackLock)
	require.True(t, ok, "rollback should pin its plan on the lock")
	plans.failFor(rollbackPlanID)

	sendComment("schemabot apply -e staging")
	refusal := awaitCommentContaining(t, result, "pending rollback. The apply was rejected")
	assert.Contains(t, refusal, "SchemaBot could not check this PR")
	assert.Contains(t, refusal, "retry the command")

	lock, err := svc.Storage().Locks().Get(ctx, dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock, "a rejected apply must not release the rollback's lock")
	assert.Equal(t, rollbackLock.PendingPlanID, lock.PendingPlanID, "a rejected apply must leave the lock pinned to the rollback plan")
}

// checkWriteRecorder wraps the service's storage so a test can observe every
// stored check state write for one database's check row. Any concurrent
// aggregate refresh republishes the stored row to GitHub, so even a transient
// stored success is an externally visible passing check — asserting on the
// final row alone cannot prove the invariant that a state was never written.
type checkWriteRecorder struct {
	storage.Storage
	checks *recordingCheckStore
}

func (s *checkWriteRecorder) Checks() storage.CheckStore { return s.checks }

// checkWrite is one observable stored check state write: the store operation
// that performed it and the status/conclusion it made visible.
type checkWrite struct {
	op         string
	status     string
	conclusion string
}

// recordingCheckStore records every conclusion-writing stored check state
// operation that lands for one database's check row. Operations that clear or
// delete check state without writing a status/conclusion pass through
// unrecorded; tests assert on the final stored row to cover those. Recording
// starts when StartRecording is called so a test can scope the observation
// window to the flow under test.
type recordingCheckStore struct {
	storage.CheckStore
	databaseName string

	mu      sync.Mutex
	enabled bool
	writes  []checkWrite
}

func (s *recordingCheckStore) StartRecording() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.enabled = true
}

func (s *recordingCheckStore) Writes() []checkWrite {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.writes)
}

func (s *recordingCheckStore) record(op, databaseName, status, conclusion string) {
	if databaseName != s.databaseName {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.enabled {
		return
	}
	s.writes = append(s.writes, checkWrite{op: op, status: status, conclusion: conclusion})
}

func (s *recordingCheckStore) Upsert(ctx context.Context, check *storage.Check) error {
	err := s.CheckStore.Upsert(ctx, check)
	if err == nil {
		s.record("upsert", check.DatabaseName, check.Status, check.Conclusion)
	}
	return err
}

func (s *recordingCheckStore) UpsertPlanResult(ctx context.Context, check *storage.Check, drift storage.PlanDriftState) (bool, error) {
	stored, err := s.CheckStore.UpsertPlanResult(ctx, check, drift)
	if err == nil && stored {
		s.record("upsert_plan_result", check.DatabaseName, check.Status, check.Conclusion)
	}
	return stored, err
}

func (s *recordingCheckStore) RecoverApplyOwnedCheckWithNoOpPlan(ctx context.Context, check *storage.Check) (bool, error) {
	updated, err := s.CheckStore.RecoverApplyOwnedCheckWithNoOpPlan(ctx, check)
	if err == nil && updated {
		s.record("recover_apply_owned_check", check.DatabaseName, check.Status, check.Conclusion)
	}
	return updated, err
}

func (s *recordingCheckStore) MarkStalePlanSuccessful(ctx context.Context, check *storage.Check) (bool, error) {
	updated, err := s.CheckStore.MarkStalePlanSuccessful(ctx, check)
	if err == nil && updated {
		s.record("mark_stale_plan_successful", check.DatabaseName, check.Status, check.Conclusion)
	}
	return updated, err
}

func (s *recordingCheckStore) CompleteForApply(ctx context.Context, check *storage.Check, apply *storage.Apply) (bool, error) {
	updated, err := s.CheckStore.CompleteForApply(ctx, check, apply)
	if err == nil && updated {
		s.record("complete_for_apply", check.DatabaseName, check.Status, check.Conclusion)
	}
	return updated, err
}

func (s *recordingCheckStore) MarkActionRequiredForApply(ctx context.Context, check *storage.Check, apply *storage.Apply) (bool, error) {
	updated, err := s.CheckStore.MarkActionRequiredForApply(ctx, check, apply)
	if err == nil && updated {
		s.record("mark_action_required_for_apply", check.DatabaseName, check.Status, check.Conclusion)
	}
	return updated, err
}

// TestE2ERollbackConfirmUpdatesCheckToActionRequired verifies that after a
// rollback-confirm completes, the check run transitions to action_required
// (not success) since the PR's schema changes have been undone. The stored
// check state must move to action_required directly: a completed rollback
// means the PR's change is reverted on the target, so no write in the terminal
// path may make the stored state read as a passing check, even transiently —
// a concurrent aggregate refresh (another database's plan, a check_run event,
// another pod) reading that window would publish a passing required check for
// a PR whose schema change is gone.
func TestE2ERollbackConfirmUpdatesCheckToActionRequired(t *testing.T) {
	dbName := "webhook_rb_check"
	recorder := &recordingCheckStore{databaseName: dbName}
	svc := setupE2EServiceWithStorage(t, dbName, func(st storage.Storage) storage.Storage {
		recorder.CheckStore = st.Checks()
		return &checkWriteRecorder{Storage: st, checks: recorder}
	})
	ctx := t.Context()

	// Step 1: Create initial table
	cfg, err := mysql.ParseDSN(e2eTargetDSN)
	require.NoError(t, err)
	cfg.DBName = dbName
	cfg.MultiStatements = true
	db, err := sql.Open("block-mysql", cfg.FormatDSN())
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

	// Run rollback-confirm, recording every stored check state write for this
	// database from here on so the terminal transition can be proven direct.
	recorder.StartRecording()
	req = buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback-confirm -e staging --allow-unsafe",
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

	check, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check)
	assert.Equal(t, checkStatusCompleted, check.Status)
	assert.Equal(t, checkConclusionActionRequired, check.Conclusion)
	assert.Equal(t, rollbackCompletedBlock.blockingReason, check.BlockingReason)
	assert.Equal(t, rollbackCompletedBlock.message, check.ErrorMessage)

	// The completed rollback's terminal transition must be in_progress →
	// action_required with nothing in between: a stored success, however brief,
	// is a passing required check to any concurrent aggregate reader. Prove
	// both halves — the check was marked in_progress while the rollback ran,
	// and the only completed conclusion ever stored is action_required.
	writes := recorder.Writes()
	require.NotEmpty(t, writes, "recorder should observe the rollback's stored check state writes")
	assert.True(t, slices.ContainsFunc(writes, func(w checkWrite) bool {
		return w.status == checkStatusInProgress
	}), "stored check state must be marked in_progress while the rollback executes")
	for _, w := range writes {
		assert.NotEqual(t, checkConclusionSuccess, w.conclusion,
			"stored check state was written as success during rollback finalization (op %s, status %s)", w.op, w.status)
		if w.status == checkStatusCompleted {
			assert.Equal(t, checkConclusionActionRequired, w.conclusion,
				"every completed stored check state write during rollback finalization must be action_required (op %s)", w.op)
		}
	}
	// The recorder logs each write after its commit, on the writer's own
	// goroutine, so recorded order across goroutines is not commit order — the
	// claim upsert can be recorded after the terminal write it preceded. Assert
	// on the last completed-status write, which only the terminal path produces.
	terminalIdx := -1
	for i, w := range writes {
		if w.status == checkStatusCompleted {
			terminalIdx = i
		}
	}
	require.GreaterOrEqual(t, terminalIdx, 0, "recorder should observe the rollback's terminal stored check state write")
	terminal := writes[terminalIdx]
	assert.Equal(t, "mark_action_required_for_apply", terminal.op)
	assert.Equal(t, checkConclusionActionRequired, terminal.conclusion)

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
