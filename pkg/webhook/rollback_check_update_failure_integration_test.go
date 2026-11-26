//go:build integration

// Rollback-confirm comment when the required status check cannot be updated.

package webhook

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// checkStartFailingStorage serves a check store that can refuse the stored
// check state write which marks a check in progress.
type checkStartFailingStorage struct {
	storage.Storage
	checks *checkStartFailingStore
}

func (s *checkStartFailingStorage) Checks() storage.CheckStore { return s.checks }

// checkStartFailingStore, once armed, fails the write that marks a check in
// progress with err, or reports the stored check as holding blockingReason.
type checkStartFailingStore struct {
	storage.CheckStore
	armed          atomic.Bool
	err            error
	blockingReason string
}

func (s *checkStartFailingStore) Get(ctx context.Context, repo string, pr int, environment, dbType, database string) (*storage.Check, error) {
	check, err := s.CheckStore.Get(ctx, repo, pr, environment, dbType, database)
	if err != nil || !s.armed.Load() || s.blockingReason == "" {
		return check, err
	}
	return &storage.Check{
		Repository:     repo,
		PullRequest:    pr,
		Environment:    environment,
		DatabaseType:   dbType,
		DatabaseName:   database,
		Status:         checkStatusCompleted,
		Conclusion:     "failure",
		BlockingReason: s.blockingReason,
	}, nil
}

func (s *checkStartFailingStore) Upsert(ctx context.Context, check *storage.Check) error {
	if s.armed.Load() && s.err != nil && check.Status == checkStatusInProgress {
		return s.err
	}
	return s.CheckStore.Upsert(ctx, check)
}

// rollbackConfirmWithCheckStartFailure posts a rollback plan for a completed
// apply, arms failing, then confirms the rollback and returns the comment that
// reports the check update.
func rollbackConfirmWithCheckStartFailure(t *testing.T, dbName string, failing *checkStartFailingStore, commentText string) string {
	t.Helper()
	svc := setupE2EServiceWithStorage(t, dbName, func(st storage.Storage) storage.Storage {
		failing.CheckStore = st.Checks()
		return &checkStartFailingStorage{Storage: st, checks: failing}
	})

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

	storedApply, err := svc.Storage().Applies().Get(t.Context(), applyID)
	require.NoError(t, err)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: fmt.Sprintf("schemabot rollback %s -e staging", storedApply.ApplyIdentifier),
		isPR:    true,
	}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	awaitCommentContaining(t, result, "Rollback Plan")

	failing.armed.Store(true)
	req = buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback-confirm -e staging --allow-unsafe",
		isPR:    true,
	}, nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	body := awaitCommentContaining(t, result, commentText)
	requireSingleRollbackApply(t, svc, dbName)
	return body
}

// A rollback-confirm whose rollback was accepted but whose required status check
// could not be marked in progress tells the PR that the rollback was accepted
// and unaffected, that the check does not show it yet, and how to refresh it.
// The storage failure, which names an internal address, stays in the server log.
func TestE2ERollbackConfirmCheckUpdateFailureKeepsTheCauseOutOfThePR(t *testing.T) {
	dbName := "webhook_rb_check_fail"
	failing := &checkStartFailingStore{err: errors.New(rollbackInternalFailureText)}

	body := rollbackConfirmWithCheckStartFailure(t, dbName, failing, "could not update the required status check")

	assert.Contains(t, body, fmt.Sprintf("The rollback for `%s` in `staging` was accepted", dbName))
	assert.Contains(t, body, "so the check does not show it yet")
	assert.Contains(t, body, "The rollback itself is not affected.")
	assert.Contains(t, body, "Once the rollback finishes, run plan again to refresh the check")
	assert.Contains(t, body, "the SchemaBot server logs have the cause")
	assertNoInternalFailureText(t, body)
}

// A rollback-confirm whose check still holds the deployment drift found at
// review time leaves the check as it was, and the comment names the drift and
// the step that clears it rather than a generic check update failure.
func TestE2ERollbackConfirmCheckUpdateNamesTheDriftBlock(t *testing.T) {
	dbName := "webhook_rb_check_drift"
	failing := &checkStartFailingStore{blockingReason: storage.ReviewTimeDeploymentDriftBlockingReason}

	body := rollbackConfirmWithCheckStartFailure(t, dbName, failing, "still shows the deployment drift found at review time")

	assert.Contains(t, body, fmt.Sprintf("The rollback for `%s` in `staging` was accepted", dbName))
	assert.Contains(t, body, "Reconcile the deployments the check summary names, then run plan again.")
	assert.NotContains(t, body, "could not update the required status check")
}
