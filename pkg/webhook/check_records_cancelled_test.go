//go:build integration

package webhook

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

const (
	cancelledCheckRepo = "octocat/hello-world"
	cancelledCheckPR   = 1
	cancelledCheckEnv  = "staging"
)

func createCancelledCheckApply(t *testing.T, svc *api.Service, database string, rollback bool) *storage.Apply {
	t.Helper()
	apply := &storage.Apply{
		ApplyIdentifier: "apply-cancelled-check",
		Database:        database,
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     cancelledCheckEnv,
		Repository:      cancelledCheckRepo,
		PullRequest:     cancelledCheckPR,
		State:           state.Apply.Cancelled,
		Engine:          storage.EngineSpirit,
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{Rollback: rollback}),
	}
	applyID, err := svc.Storage().Applies().Create(t.Context(), apply)
	require.NoError(t, err)
	apply.ID = applyID
	return apply
}

func createCancelledCheckTask(t *testing.T, svc *api.Service, apply *storage.Apply, identifier, taskState string) {
	t.Helper()
	now := time.Now()
	_, err := svc.Storage().Tasks().Create(t.Context(), &storage.Task{
		TaskIdentifier: identifier,
		ApplyID:        apply.ID,
		PlanID:         apply.PlanID,
		Database:       apply.Database,
		DatabaseType:   apply.DatabaseType,
		Engine:         apply.Engine,
		Repository:     apply.Repository,
		PullRequest:    apply.PullRequest,
		Environment:    apply.Environment,
		State:          taskState,
		TableName:      "users",
		DDL:            "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
		DDLAction:      "ALTER",
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	require.NoError(t, err)
}

func storeCancelledOwnedCheck(t *testing.T, svc *api.Service, apply *storage.Apply, status, conclusion string) {
	t.Helper()
	require.NoError(t, svc.Storage().Checks().Upsert(t.Context(), &storage.Check{
		Repository:   cancelledCheckRepo,
		PullRequest:  cancelledCheckPR,
		HeadSHA:      "oldsha999",
		Environment:  apply.Environment,
		DatabaseType: apply.DatabaseType,
		DatabaseName: apply.Database,
		CheckRunID:   42,
		ApplyID:      apply.ID,
		HasChanges:   true,
		Status:       status,
		Conclusion:   conclusion,
	}))
}

func loadCancelledCheck(t *testing.T, svc *api.Service, database string) *storage.Check {
	t.Helper()
	check, err := svc.Storage().Checks().Get(t.Context(), cancelledCheckRepo, cancelledCheckPR, cancelledCheckEnv, storage.DatabaseTypeMySQL, database)
	require.NoError(t, err)
	require.NotNil(t, check)
	return check
}

// A cancelled forward apply never changed the target environment, so when no
// earlier apply did either, terminal handling releases check ownership while
// keeping the check blocked until a fresh plan establishes the desired state.
func TestUpdateCheckRecordForApplyResult_CancelledForwardApplyReleasesOwnership(t *testing.T) {
	database := "webhook_cancelled_release"
	svc := setupE2EService(t, database)
	apply := createCancelledCheckApply(t, svc, database, false)
	storeCancelledOwnedCheck(t, svc, apply, checkStatusInProgress, "")
	h := NewHandler(svc, nil, nil, testLogger())

	updated, err := h.updateCheckRecordForApplyResult(t.Context(), cancelledCheckRepo, cancelledCheckPR, apply)
	require.NoError(t, err)
	assert.True(t, updated)

	check := loadCancelledCheck(t, svc, database)
	assert.Zero(t, check.ApplyID)
	assert.Equal(t, checkStatusCompleted, check.Status)
	assert.Equal(t, checkConclusionActionRequired, check.Conclusion)
	assert.True(t, check.HasChanges)
	assert.Equal(t, applyCancelledBlock.blockingReason, check.BlockingReason)
	assert.Equal(t, applyCancelledBlock.message, check.ErrorMessage)
}

// When one task completed before its sibling was cancelled, the cancelled
// apply may have changed the target. Terminal handling must retain ownership
// and leave an actionable failure that does not reconcile repeatedly.
func TestUpdateCheckRecordForApplyResult_CancelledForwardApplyWithCompletedTaskRetainsOwnership(t *testing.T) {
	database := "webhook_cancelled_partial_completion"
	svc := setupE2EService(t, database)
	apply := createCancelledCheckApply(t, svc, database, false)
	createCancelledCheckTask(t, svc, apply, "task-cancelled-completed", state.Task.Completed)
	createCancelledCheckTask(t, svc, apply, "task-cancelled-sibling", state.Task.Cancelled)
	storeCancelledOwnedCheck(t, svc, apply, checkStatusInProgress, "")
	h := NewHandler(svc, nil, nil, testLogger())

	updated, err := h.updateCheckRecordForApplyResult(t.Context(), cancelledCheckRepo, cancelledCheckPR, apply)
	require.NoError(t, err)
	assert.True(t, updated)

	check := loadCancelledCheck(t, svc, database)
	assert.Equal(t, apply.ID, check.ApplyID)
	assert.Equal(t, checkStatusCompleted, check.Status)
	assert.Equal(t, checkConclusionFailure, check.Conclusion)
	assert.True(t, check.HasChanges)
	assert.Equal(t, applyCancelledAfterTaskCompletedBlock.blockingReason, check.BlockingReason)
	assert.Equal(t, applyCancelledAfterTaskCompletedBlock.message, check.ErrorMessage)
	assert.False(t, checkNeedsTerminalReconcile(check, apply))
}

// A cancelled apply's terminal state can be authoritative while its stored
// check is completed, failed, and still owned. Stale reconciliation must
// re-drive that shape once, release ownership, and leave a second pass with
// nothing to repair.
func TestReconcileStaleChecks_CancelledOwnedFailureReleasesOnce(t *testing.T) {
	database := "webhook_cancelled_wedge"
	svc := setupE2EService(t, database)
	apply := createCancelledCheckApply(t, svc, database, false)
	storeCancelledOwnedCheck(t, svc, apply, checkStatusCompleted, checkConclusionFailure)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	client := gh.NewClient(nil)
	baseURL, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client.BaseURL = baseURL
	setupFakeGitHubForPlan(t, mux, nil, "", database)
	installClient := ghclient.NewInstallationClient(client, testLogger())
	h := NewHandler(svc, &fakeClientFactory{client: installClient}, nil, testLogger())

	require.NoError(t, h.reconcileStaleChecks(t.Context(), installClient, cancelledCheckRepo, cancelledCheckPR))
	check := loadCancelledCheck(t, svc, database)
	assert.Zero(t, check.ApplyID)
	assert.Equal(t, checkStatusCompleted, check.Status)
	assert.Equal(t, checkConclusionActionRequired, check.Conclusion)
	assert.Equal(t, applyCancelledBlock.blockingReason, check.BlockingReason)
	assert.False(t, checkNeedsTerminalReconcile(check, apply))

	require.NoError(t, h.reconcileStaleChecks(t.Context(), installClient, cancelledCheckRepo, cancelledCheckPR))
	check = loadCancelledCheck(t, svc, database)
	assert.Zero(t, check.ApplyID)
	assert.Equal(t, applyCancelledBlock.blockingReason, check.BlockingReason)
}

// Cancelling a rollback leaves the original schema change live, so terminal
// handling must retain ownership and the existing failure behavior rather than
// allowing a later no-change plan to unblock the PR.
func TestUpdateCheckRecordForApplyResult_CancelledRollbackRetainsOwnership(t *testing.T) {
	database := "webhook_cancelled_rollback"
	svc := setupE2EService(t, database)
	apply := createCancelledCheckApply(t, svc, database, true)
	storeCancelledOwnedCheck(t, svc, apply, checkStatusInProgress, "")
	h := NewHandler(svc, nil, nil, testLogger())

	updated, err := h.updateCheckRecordForApplyResult(t.Context(), cancelledCheckRepo, cancelledCheckPR, apply)
	require.NoError(t, err)
	assert.True(t, updated)

	check := loadCancelledCheck(t, svc, database)
	assert.Equal(t, apply.ID, check.ApplyID)
	assert.Equal(t, checkStatusCompleted, check.Status)
	assert.Equal(t, checkConclusionFailure, check.Conclusion)
	assert.True(t, check.HasChanges)
}

// A completed task under an earlier failed forward apply proves that part of
// the schema change reached the target. A later clean cancellation must retain
// ownership, and a plan with no managed files must keep the gate blocked.
func TestE2EPlanCancelledApplyWithEarlierFailedApplyCompletedTaskStaysBlocked(t *testing.T) {
	database := "webhook_cancelled_prior_complete"
	svc := setupE2EService(t, database)
	priorID, err := svc.Storage().Applies().Create(t.Context(), &storage.Apply{
		ApplyIdentifier: "apply-prior-failed",
		Database:        database,
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     cancelledCheckEnv,
		Repository:      cancelledCheckRepo,
		PullRequest:     cancelledCheckPR,
		State:           state.Apply.Failed,
		Engine:          storage.EngineSpirit,
	})
	require.NoError(t, err)
	priorApply, err := svc.Storage().Applies().Get(t.Context(), priorID)
	require.NoError(t, err)
	require.NotNil(t, priorApply)
	createCancelledCheckTask(t, svc, priorApply, "task-prior-completed", state.Task.Completed)
	apply := createCancelledCheckApply(t, svc, database, false)
	require.Greater(t, apply.ID, priorID)
	storeCancelledOwnedCheck(t, svc, apply, checkStatusInProgress, "")

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	client := gh.NewClient(nil)
	baseURL, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client.BaseURL = baseURL
	result := setupFakeGitHubForPlan(t, mux, map[string]string{}, "", database)
	installClient := ghclient.NewInstallationClient(client, testLogger())
	h := NewHandler(svc, &fakeClientFactory{client: installClient}, nil, testLogger())

	req := buildWebhookRequest(t, webhookPayloadOpts{comment: "schemabot plan -e staging", isPR: true}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Schema Change Reconciliation Required")
		assert.NotContains(t, body, "refreshed as passing")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for reconciliation comment")
	}

	check := loadCancelledCheck(t, svc, database)
	assert.Equal(t, apply.ID, check.ApplyID)
	assert.Equal(t, checkStatusCompleted, check.Status)
	assert.Equal(t, checkConclusionFailure, check.Conclusion)
	assert.Equal(t, applyCancelledAfterTaskCompletedBlock.blockingReason, check.BlockingReason)
	assert.False(t, checkNeedsTerminalReconcile(check, apply))
}

func TestCompletedForwardTaskBeforeCancellationIgnoresRollbackAndUnrelatedHistory(t *testing.T) {
	cancelled := &storage.Apply{ID: 4, Database: "target", DatabaseType: storage.DatabaseTypeMySQL, Environment: cancelledCheckEnv}
	rollback := &storage.Apply{
		ID: 2, Database: "target", DatabaseType: storage.DatabaseTypeMySQL, Environment: cancelledCheckEnv,
		Options: storage.MarshalApplyOptions(storage.ApplyOptions{Rollback: true}),
	}
	unrelated := &storage.Apply{ID: 3, Database: "other", DatabaseType: storage.DatabaseTypeMySQL, Environment: cancelledCheckEnv}
	tasks := []*storage.Task{
		{ApplyID: rollback.ID, State: state.Task.Completed},
		{ApplyID: unrelated.ID, State: state.Task.Completed},
	}

	assert.Nil(t, completedForwardTaskBeforeCancellation([]*storage.Apply{rollback, unrelated, cancelled}, tasks, cancelled))
}

// When a cancelled apply is the only apply for a schema change removed from
// the PR, a plan command must heal the owned failure, find no started apply
// records at the gate, and converge the current-head aggregate to passing.
func TestE2EPlanCancelledApplyWithNoManagedFilesConvergesPassing(t *testing.T) {
	database := "webhook_cancelled_no_files"
	svc := setupE2EService(t, database)
	apply := createCancelledCheckApply(t, svc, database, false)
	storeCancelledOwnedCheck(t, svc, apply, checkStatusCompleted, checkConclusionFailure)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	client := gh.NewClient(nil)
	baseURL, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client.BaseURL = baseURL
	result := setupFakeGitHubForPlan(t, mux, map[string]string{}, "", database)
	installClient := ghclient.NewInstallationClient(client, testLogger())
	h := NewHandler(svc, &fakeClientFactory{client: installClient}, nil, testLogger())

	req := buildWebhookRequest(t, webhookPayloadOpts{comment: "schemabot plan -e staging", isPR: true}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "no managed schema changes handled")

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "No Managed Schema Changes")
		assert.Contains(t, body, "refreshed as passing")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for passing convergence comment")
	}

	check := loadCancelledCheck(t, svc, database)
	assert.Zero(t, check.ApplyID)
	assert.Equal(t, checkStatusCompleted, check.Status)
	assert.Equal(t, checkConclusionSuccess, check.Conclusion)
	assert.False(t, check.HasChanges)
	aggregate := waitForStoredAggregate(t, svc, cancelledCheckRepo, cancelledCheckPR, checkStatusCompleted, checkConclusionSuccess)
	assert.Equal(t, "abc123", aggregate.HeadSHA)
}
