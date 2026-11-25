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

type checkStartFailingStore struct {
	storage.CheckStore
	armed atomic.Bool
	err   error
}

func (s *checkStartFailingStore) Upsert(ctx context.Context, check *storage.Check) error {
	if s.armed.Load() && check.Status == checkStatusInProgress {
		return s.err
	}
	return s.CheckStore.Upsert(ctx, check)
}

// A rollback-confirm whose rollback was accepted but whose required status check
// could not be marked in progress tells the PR that the rollback was accepted
// and unaffected, and that the check does not show it yet. The storage failure,
// which names an internal address, stays in the server log.
func TestE2ERollbackConfirmCheckUpdateFailureKeepsTheCauseOutOfThePR(t *testing.T) {
	dbName := "webhook_rb_check_fail"
	failing := &checkStartFailingStore{err: errors.New(rollbackInternalFailureText)}
	svc := setupE2EServiceWithStorage(t, dbName, func(st storage.Storage) storage.Storage {
		failing.CheckStore = st.Checks()
		return &checkStartFailingStorage{Storage: st, checks: failing}
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

	storedApply, err := svc.Storage().Applies().Get(ctx, applyID)
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

	body := awaitCommentContaining(t, result, "could not update the required status check")
	assert.Contains(t, body, fmt.Sprintf("The rollback for `%s` in `staging` was accepted", dbName))
	assert.Contains(t, body, "so the check does not show it yet")
	assert.Contains(t, body, "The rollback itself is not affected.")
	assert.Contains(t, body, "When the rollback completes, SchemaBot updates the check")
	assert.Contains(t, body, "the SchemaBot server logs have the cause")
	assertNoInternalFailureText(t, body)

	requireSingleRollbackApply(t, svc, dbName)
}
