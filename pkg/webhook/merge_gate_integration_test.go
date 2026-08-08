//go:build integration

// Merge gate guardrail integration tests. Before an apply's engine work
// starts on a (environment, database type, database) target, a preflight
// request holds every sibling PR's stored check on that target
// action-required — with a PR comment explaining the hold — so a merge cannot
// land on a verdict the apply is about to invalidate. Once the apply settles
// terminally, a settle request re-plans those siblings against the live
// schema, refreshing stale verdicts and releasing the holds. These tests
// exercise the durable request lifecycle end to end against the real webhook
// harness: the operator gate and drive tail recording both kinds, the
// backstop and release sweeps, the hold and re-plan fan-outs with
// attribution, the fail-closed flip when a re-plan fails, the in-flight apply
// guard, same-target request coalescing, settle deferral behind an active
// preflighted apply, and the recorded-request kick that drains without
// waiting for a poll tick.

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
	"strings"
	"sync"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

const mergeGateTestLeaseOwner = "merge-gate-test-driver"

// clearMergeGateRequests empties the shared merge_gate_requests table.
// The table is cross-test shared state: apply drive tails in earlier tests
// record requests the processor never drains there, and this test's drain
// would otherwise claim them before its own.
func clearMergeGateRequests(t *testing.T) {
	t.Helper()
	db, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	_, err = db.ExecContext(t.Context(), "DELETE FROM merge_gate_requests")
	require.NoError(t, err)
}

// recordRefreshRequest records a pending merge gate request directly, standing in
// for the operator drive tail so the processor side can be exercised in
// isolation.
func recordRefreshRequest(t *testing.T, svc *api.Service, req *storage.MergeGateRequest) *storage.MergeGateRequest {
	t.Helper()
	recorded, err := svc.Storage().MergeGateRequests().Record(t.Context(), req)
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

// TestE2EMergeGateRecordedOnApplyTerminalSuccess drives a real apply
// through the webhook command path to terminal success and verifies both
// durable requests around it: the operator gate records a preflight the
// processor must complete before the apply's engine work starts, and the
// drive tail records a settle for the apply's target before the apply is
// considered done — the request other pods' sibling PR checks are refreshed
// from.
func TestE2EMergeGateRecordedOnApplyTerminalSuccess(t *testing.T) {
	clearMergeGateRequests(t)
	dbName := "webhook_mergegate_drivetail"
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
	// for its next poll tick. The probe chains the handler's own kick: the
	// preflight gate blocks the apply until the processor completes the
	// preflight fan-out, so the wake-up must still reach the processor.
	kick := svc.OnMergeGateRecorded
	require.NotNil(t, kick, "the handler registers the processor kick on the service at construction")
	kicked := make(chan struct{}, 1)
	svc.OnMergeGateRecorded = func() {
		select {
		case kicked <- struct{}{}:
		default:
		}
		kick()
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

	// The gate's preflight is a hard precondition of the drive: a completed
	// apply proves its preflight fan-out finished before the engine started.
	preflight, err := svc.Storage().MergeGateRequests().GetByApplyAndKind(t.Context(), apply.ID, storage.MergeGateKindPreflight)
	require.NoError(t, err)
	require.NotNil(t, preflight, "the gate records a preflight request before the apply starts")
	assert.Equal(t, storage.MergeGateCompleted, preflight.State,
		"an apply cannot reach terminal state before its preflight fan-out completed")

	// The drive tail records the settle request as part of the terminal
	// transition, so it must be visible as soon as the apply is completed.
	var gateReq *storage.MergeGateRequest
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		req, err := svc.Storage().MergeGateRequests().GetByApplyAndKind(t.Context(), apply.ID, storage.MergeGateKindSettle)
		if !assert.NoError(collect, err) || !assert.NotNil(collect, req) {
			return
		}
		gateReq = req
	}, webhookIntegrationPollDeadline, 100*time.Millisecond)

	assert.Equal(t, apply.ApplyIdentifier, gateReq.ApplyIdentifier)
	assert.Equal(t, "staging", gateReq.Environment)
	assert.Equal(t, "mysql", gateReq.DatabaseType)
	assert.Equal(t, dbName, gateReq.DatabaseName)
	assert.Equal(t, "octocat/hello-world", gateReq.Repository)
	assert.Equal(t, "1", gateReq.ChangeKey)
	assert.Equal(t, apply.Caller, gateReq.RequestedBy)

	select {
	case <-kicked:
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the drive tail to invoke the merge gate recorded-notifier")
	}
}

// TestE2EMergeGateSweepBackfillsMissedApply verifies the outbox backstop: a
// completed apply with no merge gate request (a pod crash between the terminal
// write and the drive-tail recording) is found by the processor's sweep and
// its request backfilled with full attribution, so the fan-out is never lost.
func TestE2EMergeGateSweepBackfillsMissedApply(t *testing.T) {
	clearMergeGateRequests(t)
	dbName := "webhook_mergegate_sweep"
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
		ApplyIdentifier: fmt.Sprintf("apply_mergegate_sweep_%d", time.Now().UnixNano()),
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

	h := newE2EHandlerWithoutMergeGateProcessor(t, svc, gh.NewClient(nil))
	h.sweepMergeGateRequests(ctx)

	gateReq, err := svc.Storage().MergeGateRequests().GetByApplyAndKind(ctx, applyID, storage.MergeGateKindSettle)
	require.NoError(t, err)
	require.NotNil(t, gateReq, "the sweep must backfill a merge gate request for a completed apply that has none")
	assert.Equal(t, apply.ApplyIdentifier, gateReq.ApplyIdentifier)
	assert.Equal(t, "staging", gateReq.Environment)
	assert.Equal(t, dbName, gateReq.DatabaseName)
	assert.Equal(t, "cli:sweeper@host", gateReq.RequestedBy)
	assert.Equal(t, storage.MergeGatePending, gateReq.State)

	// Recording is idempotent per apply: a second sweep pass over the same
	// window must not duplicate or reset the request.
	h.sweepMergeGateRequests(ctx)
	again, err := svc.Storage().MergeGateRequests().GetByApplyAndKind(ctx, applyID, storage.MergeGateKindSettle)
	require.NoError(t, err)
	require.NotNil(t, again)
	assert.Equal(t, gateReq.ID, again.ID)
}

// TestE2EMergeGateReplansSiblingPRAndSkipsOriginator verifies the fan-out:
// after an apply on another PR changes a target's live schema, a sibling PR
// with stored plan check state on that target is re-planned against the live
// schema at its current head, and the refreshed check's change summary carries
// the attribution (which apply, by whom) so a reviewer knows why an unchanged
// PR was re-planned. The originating PR's own check state is left to its apply
// lifecycle and never re-planned.
func TestE2EMergeGateReplansSiblingPRAndSkipsOriginator(t *testing.T) {
	clearMergeGateRequests(t)
	dbName := "webhook_mergegate_fanout"
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

	h := newE2EHandlerWithoutMergeGateProcessor(t, svc, client)

	applyIdentifier := fmt.Sprintf("apply_mergegate_fanout_%d", time.Now().UnixNano())
	gateReq := recordRefreshRequest(t, svc, &storage.MergeGateRequest{
		ApplyID:         91000001,
		Kind:            storage.MergeGateKindSettle,
		ApplyIdentifier: applyIdentifier,
		Environment:     "staging",
		DatabaseType:    "mysql",
		DatabaseName:    dbName,
		Repository:      "octocat/hello-world",
		ChangeKey:       "2",
		RequestedBy:     "cli:tester@host",
	})

	h.drainMergeGateRequests(t.Context(), mergeGateTestLeaseOwner)

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
	finished, err := svc.Storage().MergeGateRequests().GetByApplyAndKind(t.Context(), gateReq.ApplyID, storage.MergeGateKindSettle)
	require.NoError(t, err)
	require.NotNil(t, finished)
	assert.Equal(t, storage.MergeGateCompleted, finished.State)
}

// TestE2EMergeGateReplanFailureFailsCheckClosed verifies the fail-closed
// guarantee: when a sibling PR's re-plan fails (here: its schema files no
// longer parse), its stored check state is durably flipped to a blocking
// conclusion with a fixed sanitized message — a plan computed against a schema
// that no longer exists must not keep passing — and the request still
// completes because the block is a durable outcome, not a retry.
func TestE2EMergeGateReplanFailureFailsCheckClosed(t *testing.T) {
	clearMergeGateRequests(t)
	dbName := "webhook_mergegate_failclosed"
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

	h := newE2EHandlerWithoutMergeGateProcessor(t, svc, client)

	applyIdentifier := fmt.Sprintf("apply_mergegate_failclosed_%d", time.Now().UnixNano())
	gateReq := recordRefreshRequest(t, svc, &storage.MergeGateRequest{
		ApplyID:         91000002,
		Kind:            storage.MergeGateKindSettle,
		ApplyIdentifier: applyIdentifier,
		Environment:     "staging",
		DatabaseType:    "mysql",
		DatabaseName:    dbName,
		RequestedBy:     "cli:tester@host",
	})

	h.drainMergeGateRequests(t.Context(), mergeGateTestLeaseOwner)

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

	finished, err := svc.Storage().MergeGateRequests().GetByApplyAndKind(t.Context(), gateReq.ApplyID, storage.MergeGateKindSettle)
	require.NoError(t, err)
	require.NotNil(t, finished)
	assert.Equal(t, storage.MergeGateCompleted, finished.State)
}

// TestE2EMergeGateLeavesInFlightApplyCheckUntouched verifies that a started
// apply remains authoritative: a sibling PR whose stored check state is owned
// by an in-flight apply (status in_progress with an apply id) is never
// re-planned or flipped — the apply's own terminal update refreshes it — and
// the request completes without touching GitHub for that PR.
func TestE2EMergeGateLeavesInFlightApplyCheckUntouched(t *testing.T) {
	clearMergeGateRequests(t)
	dbName := "webhook_mergegate_inflight"
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

	h := newE2EHandlerWithoutMergeGateProcessor(t, svc, client)

	gateReq := recordRefreshRequest(t, svc, &storage.MergeGateRequest{
		ApplyID:         91000003,
		Kind:            storage.MergeGateKindSettle,
		ApplyIdentifier: fmt.Sprintf("apply_mergegate_inflight_%d", time.Now().UnixNano()),
		Environment:     "staging",
		DatabaseType:    "mysql",
		DatabaseName:    dbName,
		RequestedBy:     "cli:tester@host",
	})

	h.drainMergeGateRequests(t.Context(), mergeGateTestLeaseOwner)

	untouched, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, untouched)
	assert.Equal(t, checkStatusInProgress, untouched.Status)
	assert.Equal(t, int64(424242), untouched.ApplyID)
	assert.Equal(t, "apply in flight", untouched.ChangeSummary)
	assert.Empty(t, untouched.BlockingReason)

	finished, err := svc.Storage().MergeGateRequests().GetByApplyAndKind(t.Context(), gateReq.ApplyID, storage.MergeGateKindSettle)
	require.NoError(t, err)
	require.NotNil(t, finished)
	assert.Equal(t, storage.MergeGateCompleted, finished.State)
}

// TestE2EMergeGateCoalescesPendingSiblingRequests verifies coalescing: two
// applies completing on the same target need only one fan-out, because a
// re-plan against the live schema covers every schema change recorded before
// it started. The drain drives the older request and completes the younger one
// without ever claiming it.
func TestE2EMergeGateCoalescesPendingSiblingRequests(t *testing.T) {
	clearMergeGateRequests(t)
	dbName := "webhook_mergegate_coalesce"
	// Coalescing is pure request-lifecycle behavior in storage, so the lighter
	// storage-backed service is enough — no target database or operator.
	svc := setupE2EServiceWithConfig(t, &api.ServerConfig{})

	client := gh.NewClient(nil)
	server := httptest.NewServer(http.NewServeMux())
	t.Cleanup(server.Close)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	h := newE2EHandlerWithoutMergeGateProcessor(t, svc, client)

	first := recordRefreshRequest(t, svc, &storage.MergeGateRequest{
		ApplyID:         91000004,
		Kind:            storage.MergeGateKindSettle,
		ApplyIdentifier: fmt.Sprintf("apply_mergegate_coalesce_a_%d", time.Now().UnixNano()),
		Environment:     "staging",
		DatabaseType:    "mysql",
		DatabaseName:    dbName,
		RequestedBy:     "cli:tester@host",
	})
	second := recordRefreshRequest(t, svc, &storage.MergeGateRequest{
		ApplyID:         91000005,
		Kind:            storage.MergeGateKindSettle,
		ApplyIdentifier: fmt.Sprintf("apply_mergegate_coalesce_b_%d", time.Now().UnixNano()),
		Environment:     "staging",
		DatabaseType:    "mysql",
		DatabaseName:    dbName,
		RequestedBy:     "cli:tester@host",
	})

	h.drainMergeGateRequests(t.Context(), mergeGateTestLeaseOwner)

	driven, err := svc.Storage().MergeGateRequests().GetByApplyAndKind(t.Context(), first.ApplyID, storage.MergeGateKindSettle)
	require.NoError(t, err)
	require.NotNil(t, driven)
	assert.Equal(t, storage.MergeGateCompleted, driven.State)
	assert.Equal(t, 1, driven.Attempts, "the older request runs the fan-out")

	coalesced, err := svc.Storage().MergeGateRequests().GetByApplyAndKind(t.Context(), second.ApplyID, storage.MergeGateKindSettle)
	require.NoError(t, err)
	require.NotNil(t, coalesced)
	assert.Equal(t, storage.MergeGateCompleted, coalesced.State)
	assert.Equal(t, 0, coalesced.Attempts, "the younger request is coalesced, never claimed")
}

// TestE2EMergeGateKickDrainsWithoutTick verifies the recorded-request kick:
// a request recorded while the processor sleeps between polls is drained as
// soon as the drive tail's notifier fires, so sibling PR checks re-plan
// without waiting out the poll interval. The poll interval is set far beyond
// the test deadline, so only the kick can explain the drain.
func TestE2EMergeGateKickDrainsWithoutTick(t *testing.T) {
	clearMergeGateRequests(t)
	dbName := "webhook_mergegate_kick"
	// The request lifecycle is storage-only when no sibling checks exist, so
	// the lighter storage-backed service is enough — no target database or
	// operator.
	svc := setupE2EServiceWithConfig(t, &api.ServerConfig{})

	client := gh.NewClient(nil)
	server := httptest.NewServer(http.NewServeMux())
	t.Cleanup(server.Close)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	h := newE2EHandlerWithoutMergeGateProcessor(t, svc, client)

	// A sentinel recorded before start is drained by the driver's startup
	// pass; its completion means the driver is parked on the (hour-long)
	// ticker, so nothing but a kick can drain the next request.
	sentinel := recordRefreshRequest(t, svc, &storage.MergeGateRequest{
		ApplyID:         91000006,
		Kind:            storage.MergeGateKindSettle,
		ApplyIdentifier: fmt.Sprintf("apply_mergegate_kick_sentinel_%d", time.Now().UnixNano()),
		Environment:     "staging",
		DatabaseType:    "mysql",
		DatabaseName:    dbName,
		RequestedBy:     "cli:tester@host",
	})

	h.mergeGatePollInterval = time.Hour
	h.StartMergeGateProcessor(t.Context())
	t.Cleanup(h.StopMergeGateProcessor)
	require.NotNil(t, svc.OnMergeGateRecorded,
		"starting the processor registers the drive-tail kick on the service")

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		got, err := svc.Storage().MergeGateRequests().GetByApplyAndKind(t.Context(), sentinel.ApplyID, storage.MergeGateKindSettle)
		if !assert.NoError(collect, err) || !assert.NotNil(collect, got) {
			return
		}
		assert.Equal(collect, storage.MergeGateCompleted, got.State)
	}, webhookIntegrationPollDeadline, 100*time.Millisecond,
		"the startup pass drains the sentinel request")

	kicked := recordRefreshRequest(t, svc, &storage.MergeGateRequest{
		ApplyID:         91000007,
		Kind:            storage.MergeGateKindSettle,
		ApplyIdentifier: fmt.Sprintf("apply_mergegate_kick_%d", time.Now().UnixNano()),
		Environment:     "staging",
		DatabaseType:    "mysql",
		DatabaseName:    dbName,
		RequestedBy:     "cli:tester@host",
	})
	// Wake the driver through the same registration the drive tail uses.
	svc.OnMergeGateRecorded()

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		got, err := svc.Storage().MergeGateRequests().GetByApplyAndKind(t.Context(), kicked.ApplyID, storage.MergeGateKindSettle)
		if !assert.NoError(collect, err) || !assert.NotNil(collect, got) {
			return
		}
		assert.Equal(collect, storage.MergeGateCompleted, got.State)
	}, webhookIntegrationPollDeadline, 100*time.Millisecond,
		"the kick drains the request without a poll tick")
}

// seedApplyWithLock creates a lock and an apply in the given state against
// the target, standing in for an apply the operator drove there.
func seedApplyWithLock(t *testing.T, svc *api.Service, dbName, applyState string, pr int) *storage.Apply {
	t.Helper()
	ctx := t.Context()
	lock := &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: "mysql",
		Repository:   "octocat/hello-world",
		PullRequest:  pr,
		Owner:        fmt.Sprintf("octocat/hello-world#%d", pr),
	}
	require.NoError(t, svc.Storage().Locks().Acquire(ctx, lock))
	lock, err := svc.Storage().Locks().Get(ctx, dbName, "mysql")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply_mergegate_%s_%d", applyState, time.Now().UnixNano()),
		LockID:          lock.ID,
		PlanID:          1,
		Database:        dbName,
		DatabaseType:    "mysql",
		Repository:      "octocat/hello-world",
		PullRequest:     pr,
		Environment:     "staging",
		Caller:          "cli:preflighter@host",
		InstallationID:  12345,
		Engine:          "spirit",
		State:           applyState,
	}
	applyID, err := svc.Storage().Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID
	return apply
}

// TestE2ECheckPreflightHoldsSiblingChecksAndComments verifies the preflight
// fan-out that runs before an apply's engine work starts: a sibling PR whose
// stored check on the target is green — the only kind of check a merge can
// land on — is flipped to action required with the apply-in-flight blocking
// reason, and one comment explaining the hold is posted on the PR. A retried
// fan-out converges without flipping the row again or posting a duplicate
// comment: the flip skips already-held rows and the comment is deduplicated
// by its hidden marker.
func TestE2ECheckPreflightHoldsSiblingChecksAndComments(t *testing.T) {
	clearMergeGateRequests(t)
	dbName := "webhook_mergegate_preflight"
	// The hold flips stored state and talks to GitHub; no re-plan runs, so the
	// lighter storage-backed service is enough — no target database.
	svc := setupE2EServiceWithConfig(t, &api.ServerConfig{})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}
	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	// Serve the PR's issue comments for the marker search that makes the hold
	// comment idempotent; posted comments are fed back via servedComments.
	var commentsMu sync.Mutex
	var servedComments []string
	mux.HandleFunc("GET /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, _ *http.Request) {
		commentsMu.Lock()
		defer commentsMu.Unlock()
		comments := make([]*gh.IssueComment, 0, len(servedComments))
		for i, body := range servedComments {
			comments = append(comments, &gh.IssueComment{ID: new(int64(i + 1)), Body: new(body)})
		}
		_ = json.NewEncoder(w).Encode(comments)
	})

	// The sibling PR's green check is the merge-vulnerable state the hold
	// exists for.
	seedRefreshTargetCheck(t, svc, 1, "staging", dbName,
		checkStatusCompleted, checkConclusionSuccess, "no changes")

	h := newE2EHandlerWithoutMergeGateProcessor(t, svc, client)

	applyIdentifier := fmt.Sprintf("apply_mergegate_preflight_%d", time.Now().UnixNano())
	preflight := recordRefreshRequest(t, svc, &storage.MergeGateRequest{
		ApplyID:         91000008,
		Kind:            storage.MergeGateKindPreflight,
		ApplyIdentifier: applyIdentifier,
		Environment:     "staging",
		DatabaseType:    "mysql",
		DatabaseName:    dbName,
		Repository:      "octocat/hello-world",
		ChangeKey:       "2",
		RequestedBy:     "cli:preflighter@host",
	})

	h.drainMergeGateRequests(t.Context(), mergeGateTestLeaseOwner)

	held, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, held)
	assert.Equal(t, checkStatusCompleted, held.Status)
	assert.Equal(t, checkConclusionActionRequired, held.Conclusion)
	assert.Equal(t, applyInFlightBlock.blockingReason, held.BlockingReason)
	assert.Equal(t, applyInFlightBlock.message, held.ErrorMessage)
	assert.Contains(t, held.ChangeSummary, "held: apply "+applyIdentifier)
	assert.Contains(t, held.ChangeSummary, "cli:preflighter@host")

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Schema Check On Hold")
		assert.Contains(t, body, applyIdentifier)
		assert.Contains(t, body, "cli:preflighter@host")
		assert.Contains(t, body, checkHoldCommentMarker(preflight))
		commentsMu.Lock()
		servedComments = append(servedComments, body)
		commentsMu.Unlock()
	default:
		t.Fatal("the preflight fan-out must post a comment explaining the hold")
	}

	finished, err := svc.Storage().MergeGateRequests().GetByApplyAndKind(t.Context(), preflight.ApplyID, storage.MergeGateKindPreflight)
	require.NoError(t, err)
	require.NotNil(t, finished)
	assert.Equal(t, storage.MergeGateCompleted, finished.State)

	// A retried fan-out (for example after a lease handover) converges: the
	// already-held row is not re-flipped and the marker search suppresses a
	// second comment.
	db, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	_, err = db.ExecContext(t.Context(), `
		UPDATE merge_gate_requests
		SET state = 'pending', attempts = 0, lease_owner = NULL, lease_token = NULL,
			lease_expires_at = NULL, completed_at = NULL
		WHERE id = ?`, finished.ID)
	require.NoError(t, err)

	h.drainMergeGateRequests(t.Context(), mergeGateTestLeaseOwner)

	refinished, err := svc.Storage().MergeGateRequests().GetByApplyAndKind(t.Context(), preflight.ApplyID, storage.MergeGateKindPreflight)
	require.NoError(t, err)
	assert.Equal(t, storage.MergeGateCompleted, refinished.State)
	select {
	case body := <-result.comments:
		t.Fatalf("a retried preflight fan-out must not post a duplicate hold comment, got: %s", body)
	default:
	}
	stillHeld, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	assert.Equal(t, applyInFlightBlock.blockingReason, stillHeld.BlockingReason)
}

// TestE2ECheckPreflightFlipsQueuedSiblingMergeGroupCheck verifies the render
// phase against a sibling PR that is already sitting in the merge queue: the
// admission check recorded on its merge-group commit passed before this apply
// existed, and the queue watches only that commit, so flipping the PR head
// alone cannot stop the queued merge. The preflight must post an
// action-required aggregate check on the merge-group commit — which makes
// GitHub remove the PR from the queue mid-test — and post the ejection
// guidance comment telling the author to queue again once the checks settle.
// A retried fan-out converges: the merge-group check updates in place and the
// marker suppresses a duplicate ejection comment.
func TestE2ECheckPreflightFlipsQueuedSiblingMergeGroupCheck(t *testing.T) {
	clearMergeGateRequests(t)
	dbName := "webhook_mergegate_queue_flip"
	svc := setupE2EServiceWithConfig(t, &api.ServerConfig{})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}
	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	// The sibling PR is queued and the queue has built its merge group: the
	// merge-queue-entry query reports this synthetic merge-group commit.
	mergeGroupSHA := "mergegroupflip001"
	result.MergeQueueHeadSHA.Store(&mergeGroupSHA)

	// Serve the PR's issue comments for the marker searches that make the hold
	// and ejection comments idempotent; posted comments are fed back in.
	var commentsMu sync.Mutex
	var servedComments []string
	mux.HandleFunc("GET /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, _ *http.Request) {
		commentsMu.Lock()
		defer commentsMu.Unlock()
		comments := make([]*gh.IssueComment, 0, len(servedComments))
		for i, body := range servedComments {
			comments = append(comments, &gh.IssueComment{ID: new(int64(i + 1)), Body: new(body)})
		}
		_ = json.NewEncoder(w).Encode(comments)
	})

	// The queued sibling's green stored check is what the stale admission
	// verdict was folded from.
	seedRefreshTargetCheck(t, svc, 1, "staging", dbName,
		checkStatusCompleted, checkConclusionSuccess, "no changes")

	h := newE2EHandlerWithoutMergeGateProcessor(t, svc, client)

	applyIdentifier := fmt.Sprintf("apply_mergegate_queue_flip_%d", time.Now().UnixNano())
	preflight := recordRefreshRequest(t, svc, &storage.MergeGateRequest{
		ApplyID:         91000010,
		Kind:            storage.MergeGateKindPreflight,
		ApplyIdentifier: applyIdentifier,
		Environment:     "staging",
		DatabaseType:    "mysql",
		DatabaseName:    dbName,
		Repository:      "octocat/hello-world",
		ChangeKey:       "2",
		RequestedBy:     "cli:preflighter@host",
	})

	h.drainMergeGateRequests(t.Context(), mergeGateTestLeaseOwner)

	// The queue watches the merge-group commit: the flip must land there,
	// action-required, under the same aggregate check name the admission pass
	// used.
	var flips []checkRunCapture
collect:
	for {
		select {
		case run := <-result.checkRuns:
			if run.HeadSHA == mergeGroupSHA {
				flips = append(flips, run)
			}
		default:
			break collect
		}
	}
	require.Len(t, flips, 1, "the preflight must post exactly one admission flip on the merge-group commit")
	assert.Equal(t, checkStatusCompleted, flips[0].Status)
	assert.Equal(t, checkConclusionActionRequired, flips[0].Conclusion)
	require.NotNil(t, flips[0].Output)
	assert.Equal(t, mergeGroupBlockedTitle, flips[0].Output.Title)

	// Two comments, in order: the hold explanation, then the ejection guidance
	// pointing the author back at the queue.
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Schema Check On Hold")
		assert.Contains(t, body, checkHoldCommentMarker(preflight))
		commentsMu.Lock()
		servedComments = append(servedComments, body)
		commentsMu.Unlock()
	default:
		t.Fatal("the preflight fan-out must post the hold comment")
	}
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Removed From Merge Queue")
		assert.Contains(t, body, "`"+dbName+"` in `staging`")
		assert.Contains(t, body, "add it to the merge queue again")
		assert.Contains(t, body, mergeQueueEjectedCommentMarker(mergeGroupSHA))
		commentsMu.Lock()
		servedComments = append(servedComments, body)
		commentsMu.Unlock()
	default:
		t.Fatal("the preflight fan-out must post the merge-queue ejection guidance comment")
	}

	finished, err := svc.Storage().MergeGateRequests().GetByApplyAndKind(t.Context(), preflight.ApplyID, storage.MergeGateKindPreflight)
	require.NoError(t, err)
	require.NotNil(t, finished)
	assert.Equal(t, storage.MergeGateCompleted, finished.State)

	// A retried fan-out (for example after a lease handover) converges: the
	// merge-group check updates in place rather than duplicating, and the
	// marker search suppresses a second ejection comment.
	db, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	_, err = db.ExecContext(t.Context(), `
		UPDATE merge_gate_requests
		SET state = 'pending', attempts = 0, lease_owner = NULL, lease_token = NULL,
			lease_expires_at = NULL, completed_at = NULL
		WHERE id = ?`, finished.ID)
	require.NoError(t, err)

	h.drainMergeGateRequests(t.Context(), mergeGateTestLeaseOwner)

	refinished, err := svc.Storage().MergeGateRequests().GetByApplyAndKind(t.Context(), preflight.ApplyID, storage.MergeGateKindPreflight)
	require.NoError(t, err)
	assert.Equal(t, storage.MergeGateCompleted, refinished.State)
	select {
	case body := <-result.comments:
		t.Fatalf("a retried preflight fan-out must not post duplicate comments, got: %s", body)
	default:
	}
}

// TestE2ECheckPreflightStoredHoldsLandDuringGitHubOutage exercises the
// preflight fan-out against a fully unavailable GitHub API. The hold phase is
// storage-only, so the sibling PR's green stored check still flips
// action-required and holds_recorded_at — the signal the operator gate starts
// the apply on — still lands; the render phase fails and keeps the request
// retryable for when GitHub recovers. A GitHub outage must never leave a
// sibling's green stored check standing, and must never block the apply on
// the rendering of its own holds.
func TestE2ECheckPreflightStoredHoldsLandDuringGitHubOutage(t *testing.T) {
	clearMergeGateRequests(t)
	dbName := "webhook_mergegate_outage"
	svc := setupE2EServiceWithConfig(t, &api.ServerConfig{})

	// Every GitHub call fails: the API is down for the whole fan-out.
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "github unavailable", http.StatusServiceUnavailable)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// The sibling PR's green check is the merge-vulnerable state the hold
	// exists for.
	seedRefreshTargetCheck(t, svc, 1, "staging", dbName,
		checkStatusCompleted, checkConclusionSuccess, "no changes")

	h := newE2EHandler(t, svc, client)

	applyIdentifier := fmt.Sprintf("apply_mergegate_outage_%d", time.Now().UnixNano())
	preflight := recordRefreshRequest(t, svc, &storage.MergeGateRequest{
		ApplyID:         91000012,
		Kind:            storage.MergeGateKindPreflight,
		ApplyIdentifier: applyIdentifier,
		Environment:     "staging",
		DatabaseType:    "mysql",
		DatabaseName:    dbName,
		Repository:      "octocat/hello-world",
		ChangeKey:       "2",
		RequestedBy:     "cli:preflighter@host",
	})

	h.drainMergeGateRequests(t.Context(), mergeGateTestLeaseOwner)

	// The stored hold landed despite the outage.
	held, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, held)
	assert.Equal(t, checkStatusCompleted, held.Status)
	assert.Equal(t, checkConclusionActionRequired, held.Conclusion)
	assert.Equal(t, applyInFlightBlock.blockingReason, held.BlockingReason)
	assert.Contains(t, held.ChangeSummary, "held: apply "+applyIdentifier)

	// The stamp the operator gate waits on is set, while the request itself
	// stays retryable so the render lands once GitHub recovers.
	req, err := svc.Storage().MergeGateRequests().GetByApplyAndKind(t.Context(), preflight.ApplyID, storage.MergeGateKindPreflight)
	require.NoError(t, err)
	require.NotNil(t, req)
	assert.NotNil(t, req.HoldsRecordedAt, "the hold phase must stamp holds_recorded_at even when GitHub is down")
	assert.Equal(t, storage.MergeGateFailed, req.State, "the render failure keeps the request in its retry lifecycle")
	assert.NotNil(t, req.RetryAfter, "the render failure is retryable, not terminal")
	assert.Contains(t, req.LastError, "verify PR state for check preflight")
}

// TestE2ECheckPreflightGateStartsApplyDuringGitHubOutage drives a real apply
// end to end while every GitHub call fails, proving the two halves of the
// outage contract together: the preflight's storage-only hold phase still
// lands (the sibling PR's green stored check flips action-required, so a
// merge cannot ride the outage past a verdict the apply invalidates), and
// the operator gate starts the apply on the stored holds without waiting
// for the unreachable code-host rendering — a GitHub outage must never
// block an apply, least of all one mitigating an incident.
func TestE2ECheckPreflightGateStartsApplyDuringGitHubOutage(t *testing.T) {
	clearMergeGateRequests(t)
	dbName := "webhook_mergegate_outage_apply"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	// Every GitHub call fails: the API is down for the whole apply lifecycle.
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "github unavailable", http.StatusServiceUnavailable)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// The sibling PR's green check is the merge-vulnerable state the hold
	// exists for.
	seedRefreshTargetCheck(t, svc, 1, "staging", dbName,
		checkStatusCompleted, checkConclusionSuccess, "no changes")

	// The processor runs as the server runs it: the gate's kick must reach it
	// so the hold phase drains without a poll tick.
	newE2EHandler(t, svc, client)

	// The apply enters through the direct service API — the path a CLI apply
	// takes during an incident, which never touches GitHub for discovery.
	schema := "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
	planResp, err := svc.ExecutePlan(ctx, api.PlanRequest{
		Database:    dbName,
		Environment: "staging",
		Type:        "mysql",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			dbName: {Files: map[string]string{"users.sql": schema}},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, planResp.Changes, "expected DDL changes")

	applyResp, applyID, err := svc.ExecuteApply(ctx, api.ApplyRequest{
		PlanID:      planResp.PlanID,
		Environment: "staging",
		Caller:      "cli:sev-operator@host",
	})
	require.NoError(t, err)
	require.True(t, applyResp.Accepted)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	// The operator gate records the preflight, the processor's hold phase
	// flips the sibling stored check, and the apply drives to terminal
	// success — all without a single successful GitHub call.
	var apply *storage.Apply
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		a, err := svc.Storage().Applies().Get(t.Context(), applyID)
		if !assert.NoError(collect, err) || !assert.NotNil(collect, a) {
			return
		}
		assert.True(collect, state.IsState(a.State, state.Apply.Completed),
			"apply state %s is not completed yet", a.State)
		apply = a
	}, webhookIntegrationPollDeadline, 100*time.Millisecond)

	// The stamp the gate started the apply on is durable, while the render
	// could not have completed against a dead GitHub — the request stays in
	// its retry lifecycle for when GitHub recovers.
	preflight, err := svc.Storage().MergeGateRequests().GetByApplyAndKind(ctx, applyID, storage.MergeGateKindPreflight)
	require.NoError(t, err)
	require.NotNil(t, preflight, "the gate records a preflight request before the apply starts")
	assert.NotNil(t, preflight.HoldsRecordedAt, "the apply started, so the stored holds must have been recorded")
	assert.NotEqual(t, storage.MergeGateCompleted, preflight.State,
		"the code-host rendering cannot complete while GitHub is down")

	// The sibling's stored check never went back to green: the hold landed
	// before the apply started, and once the apply settles the re-plan fails
	// closed against the dead GitHub. Either way the merge stays blocked.
	held, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, held)
	assert.Equal(t, checkConclusionActionRequired, held.Conclusion)
	assert.Contains(t, held.ChangeSummary, apply.ApplyIdentifier,
		"the blocked check attributes its state to the apply that caused it")

	// The drive tail still records the settle, so the siblings are re-planned
	// once GitHub recovers.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		settle, err := svc.Storage().MergeGateRequests().GetByApplyAndKind(t.Context(), applyID, storage.MergeGateKindSettle)
		if !assert.NoError(collect, err) {
			return
		}
		assert.NotNil(collect, settle, "the drive tail records a settle for the completed apply")
	}, webhookIntegrationPollDeadline, 100*time.Millisecond)
}

// TestE2ECheckReleaseSweepSettlesFailedPreflightedApply verifies holds always
// release: an apply that held sibling PR checks and then failed — its drive
// tail may never run, for example when it is cancelled while queued — is
// found by the release sweep, which backfills a settle; the settle's fan-out
// re-plans the held sibling against the live schema, replacing the hold with
// a live verdict.
func TestE2ECheckReleaseSweepSettlesFailedPreflightedApply(t *testing.T) {
	clearMergeGateRequests(t)
	dbName := "webhook_mergegate_release"
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

	apply := seedApplyWithLock(t, svc, dbName, state.Apply.Failed, 2)
	recordRefreshRequest(t, svc, &storage.MergeGateRequest{
		ApplyID:         apply.ID,
		Kind:            storage.MergeGateKindPreflight,
		ApplyIdentifier: apply.ApplyIdentifier,
		Environment:     "staging",
		DatabaseType:    "mysql",
		DatabaseName:    dbName,
		Repository:      "octocat/hello-world",
		ChangeKey:       "2",
		RequestedBy:     apply.Caller,
	})

	// The sibling PR's check is held, as the apply's preflight left it.
	heldCheck := seedRefreshTargetCheck(t, svc, 1, "staging", dbName,
		checkStatusCompleted, checkConclusionActionRequired, "held: apply in flight")
	heldCheck.BlockingReason = applyInFlightBlock.blockingReason
	require.NoError(t, svc.Storage().Checks().Upsert(t.Context(), heldCheck))

	h := newE2EHandlerWithoutMergeGateProcessor(t, svc, client)

	h.sweepPreflightedAppliesMissingSettle(t.Context())
	settle, err := svc.Storage().MergeGateRequests().GetByApplyAndKind(t.Context(), apply.ID, storage.MergeGateKindSettle)
	require.NoError(t, err)
	require.NotNil(t, settle, "the release sweep must backfill a settle for a preflighted terminal apply")
	assert.Equal(t, apply.ApplyIdentifier, settle.ApplyIdentifier)
	assert.Equal(t, apply.Caller, settle.RequestedBy)

	// Recording is idempotent per apply and kind: a second sweep pass must not
	// duplicate the settle.
	h.sweepPreflightedAppliesMissingSettle(t.Context())
	again, err := svc.Storage().MergeGateRequests().GetByApplyAndKind(t.Context(), apply.ID, storage.MergeGateKindSettle)
	require.NoError(t, err)
	require.NotNil(t, again)
	assert.Equal(t, settle.ID, again.ID)

	h.drainMergeGateRequests(t.Context(), mergeGateTestLeaseOwner)

	released, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, released)
	assert.Empty(t, released.BlockingReason, "the settle re-plan replaces the hold with a live verdict")
	assert.Contains(t, released.ChangeSummary, "re-planned:")

	finished, err := svc.Storage().MergeGateRequests().GetByApplyAndKind(t.Context(), apply.ID, storage.MergeGateKindSettle)
	require.NoError(t, err)
	assert.Equal(t, storage.MergeGateCompleted, finished.State)
}

// TestE2ECheckSettleDefersToActivePreflightedApply verifies hold ordering
// when applies overlap on a target: an earlier apply's settle must not
// re-plan sibling checks while a later preflighted apply is still running
// there — the re-plan would compute verdicts against a schema that apply is
// about to change, overwriting its holds. The settle completes without
// touching the checks; the active apply's own settle re-plans them when it
// finishes.
func TestE2ECheckSettleDefersToActivePreflightedApply(t *testing.T) {
	clearMergeGateRequests(t)
	dbName := "webhook_mergegate_defer"
	// The deferral decision is storage-only, so the lighter storage-backed
	// service is enough — no target database and no GitHub fixtures: any
	// GitHub call would fail the drain loudly.
	svc := setupE2EServiceWithConfig(t, &api.ServerConfig{})

	client := gh.NewClient(nil)
	server := httptest.NewServer(http.NewServeMux())
	t.Cleanup(server.Close)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// A later apply on the target is mid-drive with its preflight already
	// completed, so its holds are live.
	active := seedApplyWithLock(t, svc, dbName, state.Apply.Running, 2)
	activePreflight := recordRefreshRequest(t, svc, &storage.MergeGateRequest{
		ApplyID:         active.ID,
		Kind:            storage.MergeGateKindPreflight,
		ApplyIdentifier: active.ApplyIdentifier,
		Environment:     "staging",
		DatabaseType:    "mysql",
		DatabaseName:    dbName,
		Repository:      "octocat/hello-world",
		ChangeKey:       "2",
		RequestedBy:     active.Caller,
	})
	claimed, err := svc.Storage().MergeGateRequests().ClaimNext(t.Context(), mergeGateTestLeaseOwner, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, activePreflight.ID, claimed.ID)
	require.NoError(t, svc.Storage().MergeGateRequests().MarkCompleted(t.Context(), claimed.ID, claimed.LeaseToken))

	heldCheck := seedRefreshTargetCheck(t, svc, 1, "staging", dbName,
		checkStatusCompleted, checkConclusionActionRequired, "held: apply in flight")
	heldCheck.BlockingReason = applyInFlightBlock.blockingReason
	require.NoError(t, svc.Storage().Checks().Upsert(t.Context(), heldCheck))

	h := newE2EHandlerWithoutMergeGateProcessor(t, svc, client)

	// An earlier apply on the same target settles while the later one runs.
	settle := recordRefreshRequest(t, svc, &storage.MergeGateRequest{
		ApplyID:         91000009,
		Kind:            storage.MergeGateKindSettle,
		ApplyIdentifier: fmt.Sprintf("apply_mergegate_defer_%d", time.Now().UnixNano()),
		Environment:     "staging",
		DatabaseType:    "mysql",
		DatabaseName:    dbName,
		RequestedBy:     "cli:tester@host",
	})

	h.drainMergeGateRequests(t.Context(), mergeGateTestLeaseOwner)

	finished, err := svc.Storage().MergeGateRequests().GetByApplyAndKind(t.Context(), settle.ApplyID, storage.MergeGateKindSettle)
	require.NoError(t, err)
	require.NotNil(t, finished)
	assert.Equal(t, storage.MergeGateCompleted, finished.State,
		"the deferred settle completes; the active apply's own settle covers the target")

	stillHeld, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, stillHeld)
	assert.Equal(t, checkConclusionActionRequired, stillHeld.Conclusion)
	assert.Equal(t, applyInFlightBlock.blockingReason, stillHeld.BlockingReason,
		"the active apply's holds must survive an earlier apply's settle")
}

// TestE2ECheckPlanBornHeldDuringActivePreflightedApply verifies plan-time
// merge gate awareness: while a preflighted apply is changing a target, a
// sibling change that plans against that target must not mint a fresh
// passing check — the preflight fan-out already held the checks that existed
// when the apply started, and a new plan (a pushed commit or a manual plan
// command) would otherwise sidestep those holds. A plan whose verdict would
// pass is stored born held with the apply-in-flight blocking reason instead,
// and the apply's settle fan-out re-plans it like any other held check. The
// plan comment still posts normally: the hold changes the stored verdict,
// not the plan UX.
func TestE2ECheckPlanBornHeldDuringActivePreflightedApply(t *testing.T) {
	clearMergeGateRequests(t)
	dbName := "webhook_mergegate_bornheld"
	svc := setupE2EService(t, dbName)

	// The target already matches the PR schema, so the plan finds no changes
	// and its verdict would pass.
	ctx := t.Context()
	appDSN := strings.Replace(e2eTargetDSN, "/target_test", "/"+dbName, 1) + "&multiStatements=true"
	db, err := sql.Open("mysql", appDSN)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)
	_ = db.Close()

	// Another change's apply is mid-flight on the target with a recorded
	// preflight, meaning its holds are (or are about to be) in force.
	apply := seedApplyWithLock(t, svc, dbName, state.Apply.Running, 2)
	recordRefreshRequest(t, svc, &storage.MergeGateRequest{
		ApplyID:         apply.ID,
		Kind:            storage.MergeGateKindPreflight,
		ApplyIdentifier: apply.ApplyIdentifier,
		Environment:     "staging",
		DatabaseType:    "mysql",
		DatabaseName:    dbName,
		Repository:      "octocat/hello-world",
		ChangeKey:       "2",
		RequestedBy:     apply.Caller,
	})

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

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}
	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// The plan comment posts as usual.
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "No schema changes detected")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for plan comment")
	}

	// The stored check is born held, not passing.
	check, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check, "expected a stored check record for the plan")
	assert.False(t, check.HasChanges)
	assert.Equal(t, checkStatusCompleted, check.Status)
	assert.Equal(t, checkConclusionActionRequired, check.Conclusion)
	assert.Equal(t, applyInFlightBlock.blockingReason, check.BlockingReason)
	assert.Contains(t, check.ChangeSummary, "held: an apply in flight is changing "+dbName+" in staging")
}
