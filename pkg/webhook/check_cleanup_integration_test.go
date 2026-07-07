//go:build integration

// Stale check cleanup, reconciliation, and PR-close webhook integration tests.

package webhook

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// TestE2EPRCloseCleanup verifies PR-close cleanup against real storage. While
// the PR has a running apply, closing it retains the database lock (so no other
// PR can start a concurrent apply on the same database) and retains stored
// check state (so a close-and-reopen cannot turn in-flight apply state into a
// passing check). Once the apply reaches a terminal state, closing the PR
// releases the lock and deletes the stored check state.
func TestE2EPRCloseCleanup(t *testing.T) {
	dbName := "webhook_pr_close"
	svc := setupE2EService(t, dbName)

	// Seed a lock and check record for this PR
	err := svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: "mysql",
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		Owner:        "octocat/hello-world#1",
	})
	require.NoError(t, err)

	err = svc.Storage().Checks().Upsert(t.Context(), &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: dbName,
		CheckRunID:   42,
		HasChanges:   true,
		Status:       "completed",
		Conclusion:   "action_required",
	})
	require.NoError(t, err)

	applyID, err := svc.Storage().Applies().Create(t.Context(), &storage.Apply{
		ApplyIdentifier: "apply-close-running",
		Database:        dbName,
		DatabaseType:    "mysql",
		Environment:     "staging",
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		State:           state.Apply.Running,
		Engine:          "spirit",
	})
	require.NoError(t, err)

	h := NewHandler(svc, nil, nil, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))

	// Close the PR while the apply is running. The handler is invoked directly
	// (not through the async webhook goroutine) so the retention assertions
	// below observe a finished cleanup pass.
	h.handlePRClosed("octocat/hello-world", 1, 12345, false)

	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock, "lock must be retained while the apply is running")
	assert.Equal(t, "octocat/hello-world#1", lock.Owner)

	check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check, "stored check state must be retained while the apply is running")
	assert.Equal(t, "action_required", check.Conclusion)

	// Finish the apply, then close the PR again through the webhook path.
	apply, err := svc.Storage().Applies().Get(t.Context(), applyID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	apply.State = state.Apply.Completed
	require.NoError(t, svc.Storage().Applies().Update(t.Context(), apply))

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "closed"}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "PR close cleanup started")

	// Poll until lock is released (cleanup runs async)
	require.Eventually(t, func() bool {
		lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
		return err == nil && lock == nil
	}, 5*time.Second, 100*time.Millisecond, "lock should be released on PR close once the apply is terminal")

	// Poll until check is deleted
	require.Eventually(t, func() bool {
		check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", dbName)
		return err == nil && check == nil
	}, 5*time.Second, 100*time.Millisecond, "check should be deleted on PR close once the apply is terminal")
}

// TestE2EStaleCheckCleanup verifies that checks for databases no longer in the PR
// are marked as success on synchronize.
func TestE2EStaleCheckCleanup(t *testing.T) {
	dbName := "webhook_stale_check"
	svc := setupE2EService(t, dbName)

	// Seed a check for a database that WON'T be in the next auto-plan
	err := svc.Storage().Checks().Upsert(t.Context(), &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "old-sha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "removed_database",
		CheckRunID:   42,
		HasChanges:   true,
		Status:       "completed",
		Conclusion:   "action_required",
	})
	require.NoError(t, err)

	// Also seed a check for the database that WILL be in the auto-plan
	err = svc.Storage().Checks().Upsert(t.Context(), &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "old-sha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: dbName,
		CheckRunID:   43,
		HasChanges:   true,
		Status:       "completed",
		Conclusion:   "action_required",
	})
	require.NoError(t, err)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	h := newE2EHandler(t, svc, client)

	// Send synchronize event (simulates new commits pushed)
	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "synchronize"}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	// The stale check (removed_database) should be updated to success
	require.Eventually(t, func() bool {
		check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", "removed_database")
		if err != nil || check == nil {
			return false
		}
		return check.Conclusion == "success"
	}, 10*time.Second, 200*time.Millisecond, "stale check should be updated to success")

	// The active check (dbName) should still exist (may be updated by auto-plan)
	check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check, "active check should still exist")
}

// TestE2EReconcileStaleInProgressCheck verifies that when a check is stuck at
// "in_progress" from a crashed apply, the next plan or apply command reconciles
// it to the apply's terminal state. The reconciled result belongs to the
// commit the apply ran against, so on the PR's newer HEAD the aggregate stays
// open (blocking) rather than passing on the old result; the plan that
// triggered reconciliation stores the current commit's own results and drives
// the aggregate to its real conclusion.
func TestE2EReconcileStaleInProgressCheck(t *testing.T) {
	dbName := "webhook_stale_inprogress"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	// Seed a completed apply (simulates an apply that finished but the goroutine died)
	apply := &storage.Apply{
		ApplyIdentifier: "apply-stale-test",
		Database:        dbName,
		DatabaseType:    "mysql",
		Environment:     "staging",
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		State:           state.Apply.Completed,
		Engine:          "spirit",
	}
	applyID, err := svc.Storage().Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	// Seed a check stuck at "in_progress" (the goroutine died before updating it)
	err = svc.Storage().Checks().Upsert(ctx, &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "oldsha999",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: dbName,
		CheckRunID:   42,
		ApplyID:      applyID,
		HasChanges:   true,
		Status:       checkStatusInProgress,
		Conclusion:   "",
	})
	require.NoError(t, err)

	// Seed an aggregate also stuck at in_progress
	err = svc.Storage().Checks().Upsert(ctx, &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "oldsha999",
		Environment:  aggregateSentinel,
		DatabaseType: aggregateSentinel,
		DatabaseName: aggregateSentinel,
		CheckRunID:   100,
		HasChanges:   true,
		Status:       checkStatusInProgress,
		Conclusion:   "",
	})
	require.NoError(t, err)

	// Set up fake GitHub
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	result := setupFakeGitHubForPlan(t, mux, nil, "", dbName)
	h := newE2EHandler(t, svc, client)
	installClient := ghclient.NewInstallationClient(client, h.logger)

	require.NoError(t, h.reconcileStaleChecks(ctx, installClient, "octocat/hello-world", 1))

	check, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check)
	assert.Equal(t, checkStatusCompleted, check.Status)
	assert.Equal(t, checkConclusionSuccess, check.Conclusion)
	assert.Equal(t, applyID, check.ApplyID)

	// The reconciled success belongs to the previous commit, so the aggregate
	// published on the current HEAD holds it as a blocking placeholder instead
	// of passing on a result the current commit never produced.
	select {
	case checkRun := <-result.checkRuns:
		assert.Equal(t, aggregateCheckName, checkRun.Name)
		assert.Equal(t, "abc123", checkRun.HeadSHA)
		assert.Equal(t, checkStatusInProgress, checkRun.Status)
		assert.Empty(t, checkRun.Conclusion)
		require.NotNil(t, checkRun.Output)
		assert.Equal(t, awaitingCurrentCommitTitle, checkRun.Output.Title)
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for aggregate check run")
	}

	aggregate, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, aggregateSentinel, aggregateSentinel, aggregateSentinel)
	require.NoError(t, err)
	require.NotNil(t, aggregate)
	assert.Equal(t, "abc123", aggregate.HeadSHA)
	assert.Equal(t, checkStatusInProgress, aggregate.Status)
	assert.Empty(t, aggregate.Conclusion)
}

// TestE2EReconcileStaleInProgressCheckFailure verifies startup/webhook
// reconciliation for a check that is still in_progress even though its apply
// already failed. Reconciliation records the failure on the stored
// per-database state; because that result belongs to the commit the apply ran
// against, the aggregate on the PR's newer HEAD keeps blocking as a
// placeholder until the current commit's own results land.
func TestE2EReconcileStaleInProgressCheckFailure(t *testing.T) {
	dbName := "webhook_stale_inprogress_failed"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	// Seed the terminal apply. This models a driver that reached a failed apply
	// state, then crashed before it updated GitHub check state.
	apply := &storage.Apply{
		ApplyIdentifier: "apply-stale-failed",
		Database:        dbName,
		DatabaseType:    "mysql",
		Environment:     "staging",
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		State:           state.Apply.Failed,
		Engine:          "spirit",
	}
	applyID, err := svc.Storage().Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	// The stored per-database and aggregate check state still say in_progress.
	// The per-database row points at the failed apply that reconciliation should
	// use as the source of truth.
	require.NoError(t, svc.Storage().Checks().Upsert(ctx, &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "oldsha999",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: dbName,
		CheckRunID:   42,
		ApplyID:      applyID,
		HasChanges:   true,
		Status:       checkStatusInProgress,
		Conclusion:   "",
	}))
	require.NoError(t, svc.Storage().Checks().Upsert(ctx, &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "oldsha999",
		Environment:  aggregateSentinel,
		DatabaseType: aggregateSentinel,
		DatabaseName: aggregateSentinel,
		CheckRunID:   100,
		HasChanges:   true,
		Status:       checkStatusInProgress,
		Conclusion:   "",
	}))

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// Fake GitHub provides current PR metadata so reconciliation can safely
	// update the aggregate check on the current commit SHA.
	result := setupFakeGitHubForPlan(t, mux, nil, "", dbName)
	h := newE2EHandler(t, svc, client)
	installClient := ghclient.NewInstallationClient(client, h.logger)

	require.NoError(t, h.reconcileStaleChecks(ctx, installClient, "octocat/hello-world", 1))

	// Reconciliation should copy the failed apply result into the stored
	// per-database check while preserving ownership by the original apply.
	check, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check)
	assert.Equal(t, checkStatusCompleted, check.Status)
	assert.Equal(t, checkConclusionFailure, check.Conclusion)
	assert.Equal(t, applyID, check.ApplyID)

	// The reconciled failure belongs to the previous commit; the aggregate on
	// the current HEAD holds it as a blocking placeholder. Merge stays blocked
	// either way, and the current commit's own plan results decide the real
	// conclusion.
	select {
	case checkRun := <-result.checkRuns:
		assert.Equal(t, aggregateCheckName, checkRun.Name)
		assert.Equal(t, "abc123", checkRun.HeadSHA)
		assert.Equal(t, checkStatusInProgress, checkRun.Status)
		assert.Empty(t, checkRun.Conclusion)
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for blocking aggregate check run")
	}
}

// TestE2EPRCloseReopenRetainsStartedApplyBlock verifies that closing and
// reopening a PR cannot bypass a block that requires operator reconciliation.
// Scenario: an apply for the PR completes, then a later commit removes the
// schema change from the PR, leaving the stored check state terminal,
// action_required, and still owned by the apply — the live database no longer
// matches the PR contents. Closing the PR deletes the PR's plan-only check
// state but retains the blocked apply-owned row. Reopening the PR (whose net
// diff no longer touches schema files) folds the retained row back into the
// aggregate on the current head SHA, which reports action_required instead of
// passing.
func TestE2EPRCloseReopenRetainsStartedApplyBlock(t *testing.T) {
	dbName := "webhook_close_reopen_block"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	// The apply completed before the schema change was removed from the PR.
	applyID, err := svc.Storage().Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply-close-reopen",
		Database:        dbName,
		DatabaseType:    "mysql",
		Environment:     "staging",
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		State:           state.Apply.Completed,
		Engine:          "spirit",
	})
	require.NoError(t, err)

	// Stored check state after stale cleanup blocked the removed schema change:
	// terminal, action_required, still owned by the apply.
	require.NoError(t, svc.Storage().Checks().Upsert(ctx, &storage.Check{
		Repository:     "octocat/hello-world",
		PullRequest:    1,
		HeadSHA:        "oldsha111",
		Environment:    "staging",
		DatabaseType:   "mysql",
		DatabaseName:   dbName,
		CheckRunID:     42,
		ApplyID:        applyID,
		HasChanges:     true,
		Status:         checkStatusCompleted,
		Conclusion:     checkConclusionActionRequired,
		BlockingReason: schemaRemovedAfterApplyBlock.blockingReason,
		ErrorMessage:   schemaRemovedAfterApplyBlock.message,
	}))

	// Plan-only check state for another database on the same PR: PR close
	// deletes it because no apply ever started for it.
	require.NoError(t, svc.Storage().Checks().Upsert(ctx, &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "oldsha111",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "webhook_close_reopen_planonly",
		CheckRunID:   43,
		HasChanges:   true,
		Status:       checkStatusCompleted,
		Conclusion:   checkConclusionActionRequired,
	}))

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// Fake GitHub serves a PR whose current diff has no schema files, matching
	// a PR whose later commit reverted the applied schema change.
	result := setupFakeGitHubForPlan(t, mux, nil, "", dbName)
	h := newE2EHandler(t, svc, client)

	// Close the PR. Close cleanup runs async, so wait for the plan-only row to
	// be deleted before asserting on the retained row.
	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "closed"}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	require.Eventually(t, func() bool {
		check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", "webhook_close_reopen_planonly")
		return err == nil && check == nil
	}, webhookIntegrationPollDeadline, 100*time.Millisecond, "plan-only check state should be deleted on PR close")

	// The blocked apply-owned row survives the close with its block intact.
	check, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check, "blocked apply-owned check state must survive PR close")
	assert.Equal(t, checkStatusCompleted, check.Status)
	assert.Equal(t, checkConclusionActionRequired, check.Conclusion)
	assert.Equal(t, applyID, check.ApplyID)
	assert.Equal(t, schemaRemovedAfterApplyBlock.blockingReason, check.BlockingReason)

	// Reopen the PR. Auto-plan finds no schema files, and stale-check cleanup
	// re-blocks the retained row on the current head SHA.
	req = buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "reopened"}, nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	require.Eventually(t, func() bool {
		check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", dbName)
		return err == nil && check != nil && check.HeadSHA == "abc123"
	}, webhookIntegrationPollDeadline, 100*time.Millisecond, "retained check state should be re-blocked on the reopened PR's head SHA")

	check, err = svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check)
	assert.Equal(t, checkStatusCompleted, check.Status)
	assert.Equal(t, checkConclusionActionRequired, check.Conclusion)
	assert.Equal(t, applyID, check.ApplyID)
	assert.Equal(t, schemaRemovedAfterApplyBlock.blockingReason, check.BlockingReason)

	// The aggregate on the reopened head SHA folds the retained block in and
	// stays action_required — never success.
	deadline := time.After(webhookIntegrationPollDeadline)
	for {
		var blocked bool
		select {
		case checkRun := <-result.checkRuns:
			require.NotEqual(t, checkConclusionSuccess, checkRun.Conclusion,
				"close and reopen must not produce a passing check run while the block stands")
			blocked = checkRun.Name == aggregateCheckName &&
				checkRun.Conclusion == checkConclusionActionRequired &&
				checkRun.HeadSHA == "abc123"
		case <-deadline:
			t.Fatal("timed out waiting for blocking aggregate check run after reopen")
		}
		if blocked {
			break
		}
	}

	aggregate, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, aggregateSentinel, aggregateSentinel, aggregateSentinel)
	require.NoError(t, err)
	require.NotNil(t, aggregate)
	assert.Equal(t, "abc123", aggregate.HeadSHA)
	assert.Equal(t, checkStatusCompleted, aggregate.Status)
	assert.Equal(t, checkConclusionActionRequired, aggregate.Conclusion)
}

// TestE2EUnmergedCloseRetainsSuccessfulApplyOwnedCheck verifies that closing a
// PR without merging cannot bypass a started apply's block via a stale success
// conclusion. Scenario: an apply for the PR completes and its stored check
// state concludes success, then a later commit removes the applied schema
// change and the PR is closed — without merging — before stale cleanup runs,
// so the row still reads success at close time. The unmerged close retains the
// apply-owned success row: the stored success only proves the database matched
// the PR when the row was written, and the unmerged branch means the change
// never landed. Reopening the PR (whose diff no longer touches schema files)
// runs stale cleanup, which converts the retained row to action_required and
// folds it into an aggregate that blocks merge instead of passing.
func TestE2EUnmergedCloseRetainsSuccessfulApplyOwnedCheck(t *testing.T) {
	dbName := "webhook_unmerged_close_success"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	// The apply completed before the schema change was removed from the PR.
	applyID, err := svc.Storage().Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply-unmerged-close",
		Database:        dbName,
		DatabaseType:    "mysql",
		Environment:     "staging",
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		State:           state.Apply.Completed,
		Engine:          "spirit",
	})
	require.NoError(t, err)

	// Stored check state as the apply left it: terminal, success, still owned
	// by the apply. Stale cleanup for the commit that removed the schema change
	// has not run, so nothing has converted the row yet.
	require.NoError(t, svc.Storage().Checks().Upsert(ctx, &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "oldsha222",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: dbName,
		CheckRunID:   42,
		ApplyID:      applyID,
		HasChanges:   true,
		Status:       checkStatusCompleted,
		Conclusion:   checkConclusionSuccess,
	}))

	// Plan-only check state for another database on the same PR. The unmerged
	// close deletes it, which doubles as the signal that the async close
	// cleanup pass finished before the retention assertions below run.
	require.NoError(t, svc.Storage().Checks().Upsert(ctx, &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "oldsha222",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "webhook_unmerged_close_planonly",
		CheckRunID:   43,
		HasChanges:   true,
		Status:       checkStatusCompleted,
		Conclusion:   checkConclusionActionRequired,
	}))

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// Fake GitHub serves a PR whose current diff has no schema files, matching
	// a PR whose later commit reverted the applied schema change.
	result := setupFakeGitHubForPlan(t, mux, nil, "", dbName)
	h := newE2EHandler(t, svc, client)

	// Close the PR without merging. Close cleanup runs async, so wait for the
	// plan-only row to be deleted — the same cleanup statement decides the
	// retained row's fate — before asserting on the retained row.
	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "closed", merged: false}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	require.Eventually(t, func() bool {
		check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", "webhook_unmerged_close_planonly")
		return err == nil && check == nil
	}, webhookIntegrationPollDeadline, 100*time.Millisecond, "plan-only check state should be deleted on unmerged PR close")

	// The apply-owned success row survives the unmerged close untouched.
	check, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check, "apply-owned success row must survive an unmerged close")
	assert.Equal(t, checkStatusCompleted, check.Status)
	assert.Equal(t, checkConclusionSuccess, check.Conclusion)
	assert.Equal(t, applyID, check.ApplyID)

	// Reopen the PR. Auto-plan finds no schema files; stale-check cleanup
	// converts the retained row on the current head SHA before the aggregate
	// for that commit is published.
	req = buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "reopened"}, nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	require.Eventually(t, func() bool {
		check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", dbName)
		return err == nil && check != nil && check.Conclusion == checkConclusionActionRequired
	}, webhookIntegrationPollDeadline, 100*time.Millisecond, "retained success row should be converted to action_required on reopen")

	check, err = svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check)
	assert.Equal(t, "abc123", check.HeadSHA)
	assert.Equal(t, checkStatusCompleted, check.Status)
	assert.Equal(t, checkConclusionActionRequired, check.Conclusion)
	assert.Equal(t, applyID, check.ApplyID)
	assert.Equal(t, schemaRemovedAfterApplyBlock.blockingReason, check.BlockingReason)

	// The aggregate on the reopened head SHA folds the converted block in and
	// reports action_required — never success, even though the "no managed
	// schema changes" path also runs for this reopen: the retained apply-owned
	// row blocks the passing aggregate until it is converted.
	deadline := time.After(webhookIntegrationPollDeadline)
	for {
		var blocked bool
		select {
		case checkRun := <-result.checkRuns:
			require.NotEqual(t, checkConclusionSuccess, checkRun.Conclusion,
				"unmerged close and reopen must not produce a passing check run while the block stands")
			blocked = checkRun.Name == aggregateCheckName &&
				checkRun.Conclusion == checkConclusionActionRequired &&
				checkRun.HeadSHA == "abc123"
		case <-deadline:
			t.Fatal("timed out waiting for blocking aggregate check run after reopen")
		}
		if blocked {
			break
		}
	}

	aggregate, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, aggregateSentinel, aggregateSentinel, aggregateSentinel)
	require.NoError(t, err)
	require.NotNil(t, aggregate)
	assert.Equal(t, "abc123", aggregate.HeadSHA)
	assert.Equal(t, checkStatusCompleted, aggregate.Status)
	assert.Equal(t, checkConclusionActionRequired, aggregate.Conclusion)
}
