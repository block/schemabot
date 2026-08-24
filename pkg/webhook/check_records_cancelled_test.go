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

// An earlier completed forward apply means the target environment may contain
// a schema change even though the newest apply was cancelled. Ownership must
// remain intact, and a plan with no managed files must keep the gate blocked.
func TestE2EPlanCancelledApplyWithEarlierCompletionStaysBlocked(t *testing.T) {
	database := "webhook_cancelled_prior_complete"
	svc := setupE2EService(t, database)
	priorID, err := svc.Storage().Applies().Create(t.Context(), &storage.Apply{
		ApplyIdentifier: "apply-prior-completed",
		Database:        database,
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     cancelledCheckEnv,
		Repository:      cancelledCheckRepo,
		PullRequest:     cancelledCheckPR,
		State:           state.Apply.Completed,
		Engine:          storage.EngineSpirit,
	})
	require.NoError(t, err)
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
	assert.Equal(t, checkStatusInProgress, check.Status)
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
