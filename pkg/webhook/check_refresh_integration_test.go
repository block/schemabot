//go:build integration

// Check refresh guardrail integration tests. When an apply reaches terminal
// success on a (environment, database type, database) target, every other open
// PR with stored check state against that target planned against a schema that
// no longer exists. These tests exercise the durable refresh request lifecycle
// against the real webhook harness, starting with recording at the operator
// drive tail.

package webhook

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// clearCheckRefreshRequests empties the shared check_refresh_requests table.
// The table is cross-test shared state: apply drive tails in earlier tests
// record requests the processor never drains there, and this test's drain
// would otherwise claim them before its own.
func clearCheckRefreshRequests(t *testing.T) {
	t.Helper()
	db, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	_, err = db.ExecContext(t.Context(), "DELETE FROM check_refresh_requests")
	require.NoError(t, err)
}

// TestE2ECheckRefreshRecordedOnApplyTerminalSuccess drives a real apply
// through the webhook command path to terminal success and verifies the
// operator drive tail durably records a check refresh request for the apply's
// target before the apply is considered done — the request other pods' sibling
// PR checks are refreshed from.
func TestE2ECheckRefreshRecordedOnApplyTerminalSuccess(t *testing.T) {
	clearCheckRefreshRequests(t)
	dbName := "webhook_checkrefresh_drivetail"
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

	h := newE2EHandler(t, svc, client)

	// The drive tail must invoke the registered recorded-notifier so a
	// co-located processor drains the request immediately instead of waiting
	// for its next poll tick. Installed after the handler so this probe is
	// the active registration.
	kicked := make(chan struct{}, 1)
	svc.OnCheckRefreshRecorded = func() {
		select {
		case kicked <- struct{}{}:
		default:
		}
	}

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "apply started")

	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "## Schema Change Apply")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for apply plan comment")
	}

	// The operator drives the apply to terminal success in the background.
	var apply *storage.Apply
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		applies, err := svc.Storage().Applies().GetByPR(t.Context(), "octocat/hello-world", 1)
		if !assert.NoError(collect, err) {
			return
		}
		for _, a := range applies {
			if a.Database == dbName && state.IsState(a.State, state.Apply.Completed) {
				apply = a
				return
			}
		}
		assert.Fail(collect, "no completed apply for the target database yet")
	}, webhookIntegrationPollDeadline, 100*time.Millisecond)

	// The drive tail records the refresh request as part of the terminal
	// transition, so it must be visible as soon as the apply is completed.
	var refreshReq *storage.CheckRefreshRequest
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		req, err := svc.Storage().CheckRefreshRequests().GetByApplyID(t.Context(), apply.ID)
		if !assert.NoError(collect, err) || !assert.NotNil(collect, req) {
			return
		}
		refreshReq = req
	}, webhookIntegrationPollDeadline, 100*time.Millisecond)

	assert.Equal(t, apply.ApplyIdentifier, refreshReq.ApplyIdentifier)
	assert.Equal(t, "staging", refreshReq.Environment)
	assert.Equal(t, "mysql", refreshReq.DatabaseType)
	assert.Equal(t, dbName, refreshReq.DatabaseName)
	assert.Equal(t, "octocat/hello-world", refreshReq.Repository)
	assert.Equal(t, 1, refreshReq.PullRequest)
	assert.Equal(t, apply.Caller, refreshReq.RequestedBy)
	assert.Equal(t, storage.CheckRefreshPending, refreshReq.State)

	select {
	case <-kicked:
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the drive tail to invoke the check refresh recorded-notifier")
	}
}
