//go:build integration

package webhook

import (
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// Retargeting preserves reviewed plans and outstanding confirmations. Rejected
// commands never replace the plan or release the lock; the same intent is still
// available when the PR returns to its enforced target.
func TestExcludedPRPreservesConfirmation(t *testing.T) {
	database := "webhook_target_confirmation"
	svc := setupE2EService(t, database)
	client, mux := setupGitHubServer(t)
	var base atomic.Value
	base.Store("parent")
	serveMutablePRTarget(t, mux, &base)
	serveUnrequiredTarget(t, mux)
	comments := make(chan string, 4)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))
	h := NewHandler(svc, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}, nil, testLogger())
	plan := &storage.Plan{PlanIdentifier: "plan-preserved-target", CreatedAt: time.Now().UTC(), Database: database, DatabaseType: "mysql", Repository: "octocat/hello-world", PullRequest: 1, HeadSHA: "same-head", Environment: "staging"}
	_, err := svc.Storage().Plans().Create(t.Context(), plan)
	require.NoError(t, err)
	require.NoError(t, svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{DatabaseName: database, DatabaseType: "mysql", Owner: "octocat/hello-world#1", Repository: "octocat/hello-world", PullRequest: 1, PendingPlanID: plan.PlanIdentifier}))
	for _, confirm := range []bool{false, true} {
		var retry bool
		if confirm {
			retry, err = h.applyConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", database, 12345, "alice", CommandResult{})
		} else {
			retry, err = h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "staging", database, 12345, "alice", CommandResult{})
		}
		require.NoError(t, err)
		assert.False(t, retry)
	}
	lock, err := svc.Storage().Locks().Get(t.Context(), database, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, plan.PlanIdentifier, lock.PendingPlanID)
	assert.Equal(t, "octocat/hello-world#1", lock.Owner)
	preserved, err := svc.Storage().Plans().Get(t.Context(), plan.PlanIdentifier)
	require.NoError(t, err)
	require.NotNil(t, preserved)
	assert.Equal(t, "same-head", preserved.HeadSHA)
	applies, err := svc.Storage().Applies().GetByPR(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	assert.Empty(t, applies, "excluded commands durably accept no applies")
}

// An accepted apply records its terminal outcome during exclusion, while its
// callback leaves the GitHub Check Run unchanged. Retargeting does not cancel
// pending accepted work or discard an outcome needed by later reconciliation.
func TestExcludedPRRecordsAcceptedApplyOutcomeWithoutPublishing(t *testing.T) {
	database := "webhook_target_outcome"
	svc := setupE2EService(t, database)
	client, mux := setupGitHubServer(t)
	var base atomic.Value
	base.Store("parent")
	serveMutablePRTarget(t, mux, &base)
	serveUnrequiredTarget(t, mux)
	var writes atomic.Int32
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		writes.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})
	mux.HandleFunc("PATCH /repos/octocat/hello-world/check-runs/123", func(w http.ResponseWriter, _ *http.Request) {
		writes.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})
	h := NewHandler(svc, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}, nil, testLogger())
	apply := &storage.Apply{ApplyIdentifier: "apply-preserved-target", Database: database, DatabaseType: "mysql", Environment: "staging", Repository: "octocat/hello-world", PullRequest: 1, InstallationID: 12345, State: state.Apply.Pending, Engine: storage.EngineSpirit}
	id, err := svc.Storage().Applies().Create(t.Context(), apply)
	require.NoError(t, err)
	apply.ID = id
	require.NoError(t, svc.Storage().Checks().Upsert(t.Context(), &storage.Check{Repository: apply.Repository, PullRequest: 1, HeadSHA: "same-head", Environment: "staging", DatabaseType: "mysql", DatabaseName: database, CheckRunID: 123, ApplyID: id, Status: checkStatusInProgress, HasChanges: true}))
	message, err := h.runAutoPlanForPR(t.Context(), ghclient.NewInstallationClient(client, testLogger()), apply.Repository, 1, "same-head", 12345, "pull_request", "edited", "", "")
	require.NoError(t, err)
	assert.Equal(t, "skipped: PR target is excluded", message)
	accepted, err := svc.Storage().Applies().Get(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, state.Apply.Pending, accepted.State, "accepted work stays queued")
	apply.State = state.Apply.Completed
	require.NoError(t, svc.Storage().Applies().Update(t.Context(), apply))
	h.refreshChecksForTerminalApply(t.Context(), apply, "target test")
	result, err := svc.Storage().Checks().Get(t.Context(), apply.Repository, 1, "staging", "mysql", database)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, checkStatusCompleted, result.Status)
	assert.Equal(t, checkConclusionSuccess, result.Conclusion, "terminal truth is recorded independently of GitHub publication")
	assert.Equal(t, int32(0), writes.Load())
}
