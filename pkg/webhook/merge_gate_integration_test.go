//go:build integration

// Check refresh guardrail integration tests. When an apply reaches terminal
// success on a (environment, database type, database) target, every other open
// PR with stored check state against that target planned against a schema that
// no longer exists. These tests exercise the durable refresh request lifecycle
// end to end against the real webhook harness: recording at the operator drive
// tail, the backstop sweep, the sibling PR fan-out with attribution, the
// fail-closed flip when a re-plan fails, the in-flight apply guard,
// same-target request coalescing, and the recorded-request kick that drains
// without waiting for a poll tick.

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

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

const checkRefreshTestLeaseOwner = "check-refresh-test-driver"

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

// recordRefreshRequest records a pending refresh request directly, standing in
// for the operator drive tail so the processor side can be exercised in
// isolation.
func recordRefreshRequest(t *testing.T, svc *api.Service, req *storage.CheckRefreshRequest) *storage.CheckRefreshRequest {
	t.Helper()
	recorded, err := svc.Storage().CheckRefreshRequests().Record(t.Context(), req)
	require.NoError(t, err)
	require.True(t, recorded)
	return req
}

// seedRefreshTargetCheck stores per-database plan check state for a PR against
// the given target, as an earlier plan would have recorded it.
func seedRefreshTargetCheck(t *testing.T, svc *api.Service, pr int, env, dbName, status, conclusion, changeSummary string) *storage.Check {
	t.Helper()
	check := &storage.Check{
		Repository:    "octocat/hello-world",
		PullRequest:   pr,
		HeadSHA:       "abc123",
		Environment:   env,
		DatabaseType:  "mysql",
		DatabaseName:  dbName,
		HasChanges:    true,
		Status:        status,
		Conclusion:    conclusion,
		ChangeSummary: changeSummary,
	}
	require.NoError(t, svc.Storage().Checks().Upsert(t.Context(), check))
	return check
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
		req, err := svc.Storage().CheckRefreshRequests().GetByApplyAndKind(t.Context(), apply.ID, storage.CheckRefreshKindSettle)
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

// TestE2ECheckRefreshSweepBackfillsMissedApply verifies the outbox backstop: a
// completed apply with no refresh request (a pod crash between the terminal
// write and the drive-tail recording) is found by the processor's sweep and
// its request backfilled with full attribution, so the fan-out is never lost.
func TestE2ECheckRefreshSweepBackfillsMissedApply(t *testing.T) {
	clearCheckRefreshRequests(t)
	dbName := "webhook_checkrefresh_sweep"
	// The sweep reads and writes storage only, so the lighter storage-backed
	// service is enough — no target database, tern client, or operator.
	svc := setupE2EServiceWithConfig(t, &api.ServerConfig{})
	ctx := t.Context()

	lock := &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: "mysql",
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		Owner:        "octocat/hello-world#1",
	}
	require.NoError(t, svc.Storage().Locks().Acquire(ctx, lock))
	lock, err := svc.Storage().Locks().Get(ctx, dbName, "mysql")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply_checkrefresh_sweep_%d", time.Now().UnixNano()),
		LockID:          lock.ID,
		PlanID:          1,
		Database:        dbName,
		DatabaseType:    "mysql",
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		Environment:     "staging",
		Caller:          "cli:sweeper@host",
		InstallationID:  12345,
		Engine:          "spirit",
		State:           state.Apply.Completed,
	}
	applyID, err := svc.Storage().Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID
	completedAt := time.Now()
	apply.CompletedAt = &completedAt
	require.NoError(t, svc.Storage().Applies().Update(ctx, apply))

	h := newE2EHandler(t, svc, gh.NewClient(nil))
	h.sweepCheckRefreshRequests(ctx)

	refreshReq, err := svc.Storage().CheckRefreshRequests().GetByApplyAndKind(ctx, applyID, storage.CheckRefreshKindSettle)
	require.NoError(t, err)
	require.NotNil(t, refreshReq, "the sweep must backfill a refresh request for a completed apply that has none")
	assert.Equal(t, apply.ApplyIdentifier, refreshReq.ApplyIdentifier)
	assert.Equal(t, "staging", refreshReq.Environment)
	assert.Equal(t, dbName, refreshReq.DatabaseName)
	assert.Equal(t, "cli:sweeper@host", refreshReq.RequestedBy)
	assert.Equal(t, storage.CheckRefreshPending, refreshReq.State)

	// Recording is idempotent per apply: a second sweep pass over the same
	// window must not duplicate or reset the request.
	h.sweepCheckRefreshRequests(ctx)
	again, err := svc.Storage().CheckRefreshRequests().GetByApplyAndKind(ctx, applyID, storage.CheckRefreshKindSettle)
	require.NoError(t, err)
	require.NotNil(t, again)
	assert.Equal(t, refreshReq.ID, again.ID)
}

// TestE2ECheckRefreshReplansSiblingPRAndSkipsOriginator verifies the fan-out:
// after an apply on another PR changes a target's live schema, a sibling PR
// with stored plan check state on that target is re-planned against the live
// schema at its current head, and the refreshed check's change summary carries
// the attribution (which apply, by whom) so a reviewer knows why an unchanged
// PR was re-planned. The originating PR's own check state is left to its apply
// lifecycle and never re-planned.
func TestE2ECheckRefreshReplansSiblingPRAndSkipsOriginator(t *testing.T) {
	clearCheckRefreshRequests(t)
	dbName := "webhook_checkrefresh_fanout"
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
	setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	// The sibling PR (#1) holds plan check state from before the schema
	// change. The originating PR (#2) has check state on the same target; the
	// fake GitHub serves no fixtures for it, so any attempt to re-plan it
	// would fail the drain loudly instead of passing silently.
	seedRefreshTargetCheck(t, svc, 1, "staging", dbName,
		checkStatusCompleted, checkConclusionActionRequired, "1 table created")
	originator := seedRefreshTargetCheck(t, svc, 2, "staging", dbName,
		checkStatusCompleted, checkConclusionActionRequired, "originator summary")

	h := newE2EHandler(t, svc, client)

	applyIdentifier := fmt.Sprintf("apply_checkrefresh_fanout_%d", time.Now().UnixNano())
	refreshReq := recordRefreshRequest(t, svc, &storage.CheckRefreshRequest{
		ApplyID:         91000001,
		Kind:            storage.CheckRefreshKindSettle,
		ApplyIdentifier: applyIdentifier,
		Environment:     "staging",
		DatabaseType:    "mysql",
		DatabaseName:    dbName,
		Repository:      "octocat/hello-world",
		PullRequest:     2,
		RequestedBy:     "cli:tester@host",
	})

	h.drainCheckRefreshRequests(t.Context(), checkRefreshTestLeaseOwner)

	// The sibling PR's stored check state was re-planned against the live
	// schema at its current head, with the attribution note appended.
	refreshed, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, refreshed)
	assert.Equal(t, "abc123", refreshed.HeadSHA)
	assert.True(t, refreshed.HasChanges)
	assert.Contains(t, refreshed.ChangeSummary, "re-planned: schema for "+dbName+" in staging changed")
	assert.Contains(t, refreshed.ChangeSummary, applyIdentifier)
	assert.Contains(t, refreshed.ChangeSummary, "cli:tester@host")
	assert.Empty(t, refreshed.BlockingReason)

	// The originating PR's stored check state is untouched.
	originatorAfter, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 2, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, originatorAfter)
	assert.Equal(t, originator.ChangeSummary, originatorAfter.ChangeSummary)
	assert.Equal(t, originator.Status, originatorAfter.Status)
	assert.Equal(t, originator.Conclusion, originatorAfter.Conclusion)
	assert.Empty(t, originatorAfter.BlockingReason)

	// The request itself is terminal-successful.
	finished, err := svc.Storage().CheckRefreshRequests().GetByApplyAndKind(t.Context(), refreshReq.ApplyID, storage.CheckRefreshKindSettle)
	require.NoError(t, err)
	require.NotNil(t, finished)
	assert.Equal(t, storage.CheckRefreshCompleted, finished.State)
}

// TestE2ECheckRefreshReplanFailureFailsCheckClosed verifies the fail-closed
// guarantee: when a sibling PR's re-plan fails (here: its schema files no
// longer parse), its stored check state is durably flipped to a blocking
// conclusion with a fixed sanitized message — a plan computed against a schema
// that no longer exists must not keep passing — and the request still
// completes because the block is a durable outcome, not a retry.
func TestE2ECheckRefreshReplanFailureFailsCheckClosed(t *testing.T) {
	clearCheckRefreshRequests(t)
	dbName := "webhook_checkrefresh_failclosed"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (broken",
	}
	setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	seedRefreshTargetCheck(t, svc, 1, "staging", dbName,
		checkStatusCompleted, checkConclusionSuccess, "no changes")

	h := newE2EHandler(t, svc, client)

	applyIdentifier := fmt.Sprintf("apply_checkrefresh_failclosed_%d", time.Now().UnixNano())
	refreshReq := recordRefreshRequest(t, svc, &storage.CheckRefreshRequest{
		ApplyID:         91000002,
		Kind:            storage.CheckRefreshKindSettle,
		ApplyIdentifier: applyIdentifier,
		Environment:     "staging",
		DatabaseType:    "mysql",
		DatabaseName:    dbName,
		RequestedBy:     "cli:tester@host",
	})

	h.drainCheckRefreshRequests(t.Context(), checkRefreshTestLeaseOwner)

	blocked, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, blocked)
	assert.Equal(t, checkStatusCompleted, blocked.Status)
	assert.Equal(t, checkConclusionActionRequired, blocked.Conclusion)
	assert.True(t, blocked.HasChanges)
	assert.Equal(t, schemaChangedReplanFailedBlock.blockingReason, blocked.BlockingReason)
	assert.Equal(t, schemaChangedReplanFailedBlock.message, blocked.ErrorMessage)
	assert.Contains(t, blocked.ChangeSummary, "re-plan failed — see server logs")
	assert.Contains(t, blocked.ChangeSummary, applyIdentifier)

	finished, err := svc.Storage().CheckRefreshRequests().GetByApplyAndKind(t.Context(), refreshReq.ApplyID, storage.CheckRefreshKindSettle)
	require.NoError(t, err)
	require.NotNil(t, finished)
	assert.Equal(t, storage.CheckRefreshCompleted, finished.State)
}

// TestE2ECheckRefreshLeavesInFlightApplyCheckUntouched verifies that a started
// apply remains authoritative: a sibling PR whose stored check state is owned
// by an in-flight apply (status in_progress with an apply id) is never
// re-planned or flipped — the apply's own terminal update refreshes it — and
// the request completes without touching GitHub for that PR.
func TestE2ECheckRefreshLeavesInFlightApplyCheckUntouched(t *testing.T) {
	clearCheckRefreshRequests(t)
	dbName := "webhook_checkrefresh_inflight"
	// The in-flight guard fires on stored check state alone, so the lighter
	// storage-backed service is enough — no target database or operator.
	svc := setupE2EServiceWithConfig(t, &api.ServerConfig{})

	// No GitHub fixtures at all: the in-flight guard fires before any GitHub
	// call, so a fetch attempt for this PR fails the drain loudly.
	client := gh.NewClient(nil)
	server := httptest.NewServer(http.NewServeMux())
	t.Cleanup(server.Close)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	inFlight := seedRefreshTargetCheck(t, svc, 1, "staging", dbName,
		checkStatusInProgress, "", "apply in flight")
	inFlight.ApplyID = 424242
	require.NoError(t, svc.Storage().Checks().Upsert(t.Context(), inFlight))

	h := newE2EHandler(t, svc, client)

	refreshReq := recordRefreshRequest(t, svc, &storage.CheckRefreshRequest{
		ApplyID:         91000003,
		Kind:            storage.CheckRefreshKindSettle,
		ApplyIdentifier: fmt.Sprintf("apply_checkrefresh_inflight_%d", time.Now().UnixNano()),
		Environment:     "staging",
		DatabaseType:    "mysql",
		DatabaseName:    dbName,
		RequestedBy:     "cli:tester@host",
	})

	h.drainCheckRefreshRequests(t.Context(), checkRefreshTestLeaseOwner)

	untouched, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, untouched)
	assert.Equal(t, checkStatusInProgress, untouched.Status)
	assert.Equal(t, int64(424242), untouched.ApplyID)
	assert.Equal(t, "apply in flight", untouched.ChangeSummary)
	assert.Empty(t, untouched.BlockingReason)

	finished, err := svc.Storage().CheckRefreshRequests().GetByApplyAndKind(t.Context(), refreshReq.ApplyID, storage.CheckRefreshKindSettle)
	require.NoError(t, err)
	require.NotNil(t, finished)
	assert.Equal(t, storage.CheckRefreshCompleted, finished.State)
}

// TestE2ECheckRefreshCoalescesPendingSiblingRequests verifies coalescing: two
// applies completing on the same target need only one fan-out, because a
// re-plan against the live schema covers every schema change recorded before
// it started. The drain drives the older request and completes the younger one
// without ever claiming it.
func TestE2ECheckRefreshCoalescesPendingSiblingRequests(t *testing.T) {
	clearCheckRefreshRequests(t)
	dbName := "webhook_checkrefresh_coalesce"
	// Coalescing is pure request-lifecycle behavior in storage, so the lighter
	// storage-backed service is enough — no target database or operator.
	svc := setupE2EServiceWithConfig(t, &api.ServerConfig{})

	client := gh.NewClient(nil)
	server := httptest.NewServer(http.NewServeMux())
	t.Cleanup(server.Close)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	h := newE2EHandler(t, svc, client)

	first := recordRefreshRequest(t, svc, &storage.CheckRefreshRequest{
		ApplyID:         91000004,
		Kind:            storage.CheckRefreshKindSettle,
		ApplyIdentifier: fmt.Sprintf("apply_checkrefresh_coalesce_a_%d", time.Now().UnixNano()),
		Environment:     "staging",
		DatabaseType:    "mysql",
		DatabaseName:    dbName,
		RequestedBy:     "cli:tester@host",
	})
	second := recordRefreshRequest(t, svc, &storage.CheckRefreshRequest{
		ApplyID:         91000005,
		Kind:            storage.CheckRefreshKindSettle,
		ApplyIdentifier: fmt.Sprintf("apply_checkrefresh_coalesce_b_%d", time.Now().UnixNano()),
		Environment:     "staging",
		DatabaseType:    "mysql",
		DatabaseName:    dbName,
		RequestedBy:     "cli:tester@host",
	})

	h.drainCheckRefreshRequests(t.Context(), checkRefreshTestLeaseOwner)

	driven, err := svc.Storage().CheckRefreshRequests().GetByApplyAndKind(t.Context(), first.ApplyID, storage.CheckRefreshKindSettle)
	require.NoError(t, err)
	require.NotNil(t, driven)
	assert.Equal(t, storage.CheckRefreshCompleted, driven.State)
	assert.Equal(t, 1, driven.Attempts, "the older request runs the fan-out")

	coalesced, err := svc.Storage().CheckRefreshRequests().GetByApplyAndKind(t.Context(), second.ApplyID, storage.CheckRefreshKindSettle)
	require.NoError(t, err)
	require.NotNil(t, coalesced)
	assert.Equal(t, storage.CheckRefreshCompleted, coalesced.State)
	assert.Equal(t, 0, coalesced.Attempts, "the younger request is coalesced, never claimed")
}

// TestE2ECheckRefreshKickDrainsWithoutTick verifies the recorded-request kick:
// a request recorded while the processor sleeps between polls is drained as
// soon as the drive tail's notifier fires, so sibling PR checks re-plan
// without waiting out the poll interval. The poll interval is set far beyond
// the test deadline, so only the kick can explain the drain.
func TestE2ECheckRefreshKickDrainsWithoutTick(t *testing.T) {
	clearCheckRefreshRequests(t)
	dbName := "webhook_checkrefresh_kick"
	// The request lifecycle is storage-only when no sibling checks exist, so
	// the lighter storage-backed service is enough — no target database or
	// operator.
	svc := setupE2EServiceWithConfig(t, &api.ServerConfig{})

	client := gh.NewClient(nil)
	server := httptest.NewServer(http.NewServeMux())
	t.Cleanup(server.Close)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	h := newE2EHandler(t, svc, client)
	require.NotNil(t, svc.OnCheckRefreshRecorded,
		"the handler registers the drive-tail kick on the service at construction")

	// A sentinel recorded before start is drained by the driver's startup
	// pass; its completion means the driver is parked on the (hour-long)
	// ticker, so nothing but a kick can drain the next request.
	sentinel := recordRefreshRequest(t, svc, &storage.CheckRefreshRequest{
		ApplyID:         91000006,
		Kind:            storage.CheckRefreshKindSettle,
		ApplyIdentifier: fmt.Sprintf("apply_checkrefresh_kick_sentinel_%d", time.Now().UnixNano()),
		Environment:     "staging",
		DatabaseType:    "mysql",
		DatabaseName:    dbName,
		RequestedBy:     "cli:tester@host",
	})

	h.checkRefreshPollInterval = time.Hour
	h.StartCheckRefreshProcessor(t.Context())
	t.Cleanup(h.StopCheckRefreshProcessor)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		got, err := svc.Storage().CheckRefreshRequests().GetByApplyAndKind(t.Context(), sentinel.ApplyID, storage.CheckRefreshKindSettle)
		if !assert.NoError(collect, err) || !assert.NotNil(collect, got) {
			return
		}
		assert.Equal(collect, storage.CheckRefreshCompleted, got.State)
	}, webhookIntegrationPollDeadline, 100*time.Millisecond,
		"the startup pass drains the sentinel request")

	kicked := recordRefreshRequest(t, svc, &storage.CheckRefreshRequest{
		ApplyID:         91000007,
		Kind:            storage.CheckRefreshKindSettle,
		ApplyIdentifier: fmt.Sprintf("apply_checkrefresh_kick_%d", time.Now().UnixNano()),
		Environment:     "staging",
		DatabaseType:    "mysql",
		DatabaseName:    dbName,
		RequestedBy:     "cli:tester@host",
	})
	// Wake the driver through the same registration the drive tail uses.
	svc.OnCheckRefreshRecorded()

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		got, err := svc.Storage().CheckRefreshRequests().GetByApplyAndKind(t.Context(), kicked.ApplyID, storage.CheckRefreshKindSettle)
		if !assert.NoError(collect, err) || !assert.NotNil(collect, got) {
			return
		}
		assert.Equal(collect, storage.CheckRefreshCompleted, got.State)
	}, webhookIntegrationPollDeadline, 100*time.Millisecond,
		"the kick drains the request without a poll tick")
}
