//go:build integration

package mysqlstore

import (
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// recordTestRefreshRequest records a pending settle refresh request for a
// synthetic completed apply and returns it. Each request needs its own
// applies row because (apply_id, kind) is the idempotency key; the anchor
// apply gets its own lock database (locks are unique per database) while the
// request carries the target under test, so several requests can share one
// target.
func recordTestRefreshRequest(t *testing.T, store *Storage, name, env, dbType, dbName, repo string, pr int) *storage.CheckRefreshRequest {
	t.Helper()
	lock := createTestLockWithPR(t, store, name+"_lock_db", dbType, env, repo, pr)
	apply := createTestApplyWithStateAndEnv(t, store, lock, name, 0, state.Apply.Completed, env)
	return recordTestRefreshRequestForApply(t, store, apply, storage.CheckRefreshKindSettle, env, dbType, dbName, repo, pr)
}

func recordTestRefreshRequestForApply(t *testing.T, store *Storage, apply *storage.Apply, kind, env, dbType, dbName, repo string, pr int) *storage.CheckRefreshRequest {
	t.Helper()
	req := &storage.CheckRefreshRequest{
		ApplyID:         apply.ID,
		Kind:            kind,
		ApplyIdentifier: apply.ApplyIdentifier,
		Environment:     env,
		DatabaseType:    dbType,
		DatabaseName:    dbName,
		Repository:      repo,
		PullRequest:     pr,
		RequestedBy:     "cli:user@host",
	}
	recorded, err := store.CheckRefreshRequests().Record(t.Context(), req)
	require.NoError(t, err)
	require.True(t, recorded)
	require.NotZero(t, req.ID)
	return req
}

// Scenario: the drive tail and the backstop sweep may both record a refresh
// request for the same apply. The unique key on the apply and kind makes
// recording idempotent per kind, so a duplicate recording reports
// recorded=false instead of duplicating the fan-out — while the same apply's
// preflight and settle coexist as separate rows.
func TestCheckRefreshStore_RecordIsIdempotentPerApplyAndKind(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	req := recordTestRefreshRequest(t, store, "apply_refresh_1", "staging", storage.DatabaseTypeMySQL, "testdb", "org/repo", 11)

	again := &storage.CheckRefreshRequest{
		ApplyID:         req.ApplyID,
		Kind:            storage.CheckRefreshKindSettle,
		ApplyIdentifier: req.ApplyIdentifier,
		Environment:     req.Environment,
		DatabaseType:    req.DatabaseType,
		DatabaseName:    req.DatabaseName,
	}
	recorded, err := store.CheckRefreshRequests().Record(ctx, again)
	require.NoError(t, err)
	assert.False(t, recorded)

	got, err := store.CheckRefreshRequests().GetByApplyAndKind(ctx, req.ApplyID, storage.CheckRefreshKindSettle)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, storage.CheckRefreshPending, got.State)
	assert.Equal(t, storage.CheckRefreshKindSettle, got.Kind)
	assert.Equal(t, "apply_refresh_1", got.ApplyIdentifier)
	assert.Equal(t, "testdb", got.DatabaseName)
	assert.Equal(t, storage.DatabaseTypeMySQL, got.DatabaseType)
	assert.Equal(t, "staging", got.Environment)
	assert.Equal(t, "org/repo", got.Repository)
	assert.Equal(t, 11, got.PullRequest)
	assert.Equal(t, "cli:user@host", got.RequestedBy)

	// The same apply's preflight is a separate row, not a duplicate.
	preflight := &storage.CheckRefreshRequest{
		ApplyID:         req.ApplyID,
		Kind:            storage.CheckRefreshKindPreflight,
		ApplyIdentifier: req.ApplyIdentifier,
		Environment:     req.Environment,
		DatabaseType:    req.DatabaseType,
		DatabaseName:    req.DatabaseName,
	}
	recorded, err = store.CheckRefreshRequests().Record(ctx, preflight)
	require.NoError(t, err)
	assert.True(t, recorded)

	gotPre, err := store.CheckRefreshRequests().GetByApplyAndKind(ctx, req.ApplyID, storage.CheckRefreshKindPreflight)
	require.NoError(t, err)
	require.NotNil(t, gotPre)
	assert.Equal(t, storage.CheckRefreshKindPreflight, gotPre.Kind)
	assert.NotEqual(t, got.ID, gotPre.ID)
}

func TestCheckRefreshStore_RecordRejectsIncompleteRequests(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	for name, req := range map[string]*storage.CheckRefreshRequest{
		"missing apply row id":    {Kind: storage.CheckRefreshKindSettle, ApplyIdentifier: "a", Environment: "staging", DatabaseType: "mysql", DatabaseName: "db"},
		"missing apply id":        {Kind: storage.CheckRefreshKindSettle, ApplyID: 1, Environment: "staging", DatabaseType: "mysql", DatabaseName: "db"},
		"missing target database": {Kind: storage.CheckRefreshKindSettle, ApplyID: 1, ApplyIdentifier: "a", Environment: "staging", DatabaseType: "mysql"},
		"missing kind":            {ApplyID: 1, ApplyIdentifier: "a", Environment: "staging", DatabaseType: "mysql", DatabaseName: "db"},
		"unknown kind":            {Kind: "bogus", ApplyID: 1, ApplyIdentifier: "a", Environment: "staging", DatabaseType: "mysql", DatabaseName: "db"},
	} {
		_, err := store.CheckRefreshRequests().Record(ctx, req)
		assert.Error(t, err, name)
	}
}

// Scenario: the processor claims the oldest pending request, rotating a fresh
// lease and incrementing attempts. A second claimant must not receive the same
// request while the lease is live.
func TestCheckRefreshStore_ClaimNextLeasesOldestPending(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	first := recordTestRefreshRequest(t, store, "apply_claim_1", "staging", storage.DatabaseTypeMySQL, "db_claim_1", "org/repo", 21)
	recordTestRefreshRequest(t, store, "apply_claim_2", "staging", storage.DatabaseTypeMySQL, "db_claim_2", "org/repo", 22)

	claimed, err := store.CheckRefreshRequests().ClaimNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, first.ID, claimed.ID)
	assert.Equal(t, storage.CheckRefreshProcessing, claimed.State)
	assert.Equal(t, "driver-a", claimed.LeaseOwner)
	assert.NotEmpty(t, claimed.LeaseToken)
	assert.Equal(t, 1, claimed.Attempts)
	require.NotNil(t, claimed.LeaseExpiresAt)

	second, err := store.CheckRefreshRequests().ClaimNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, second)
	assert.NotEqual(t, claimed.ID, second.ID, "a live lease must not be reclaimed")

	third, err := store.CheckRefreshRequests().ClaimNext(ctx, "driver-c", time.Minute)
	require.NoError(t, err)
	assert.Nil(t, third, "no pending request remains once both are leased")
}

// Scenario: a driver crashes mid-fan-out. Once its lease expires the request
// is claimable again — but only while under the attempt cap, so a poison
// request cannot be reclaimed forever.
func TestCheckRefreshStore_ClaimNextReclaimsExpiredLeaseUnderAttemptCap(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	req := recordTestRefreshRequest(t, store, "apply_expired_1", "staging", storage.DatabaseTypeMySQL, "db_expired", "org/repo", 31)

	claimed, err := store.CheckRefreshRequests().ClaimNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	_, err = testDB.ExecContext(ctx, `UPDATE check_refresh_requests SET lease_expires_at = NOW(6) - INTERVAL 1 SECOND WHERE id = ?`, req.ID)
	require.NoError(t, err)

	reclaimed, err := store.CheckRefreshRequests().ClaimNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, reclaimed)
	assert.Equal(t, req.ID, reclaimed.ID)
	assert.Equal(t, "driver-b", reclaimed.LeaseOwner)
	assert.NotEqual(t, claimed.LeaseToken, reclaimed.LeaseToken, "reclaim must rotate the lease token")
	assert.Equal(t, 2, reclaimed.Attempts)

	_, err = testDB.ExecContext(ctx, `UPDATE check_refresh_requests SET lease_expires_at = NOW(6) - INTERVAL 1 SECOND, attempts = ? WHERE id = ?`,
		storage.MaxCheckRefreshAttempts, req.ID)
	require.NoError(t, err)

	capped, err := store.CheckRefreshRequests().ClaimNext(ctx, "driver-c", time.Minute)
	require.NoError(t, err)
	assert.Nil(t, capped, "an expired lease at the attempt cap must not be reclaimed")
}

// Scenario: a failed fan-out is retried after its retry window elapses, and a
// terminal failure (nil retryAfter) is never handed out again.
func TestCheckRefreshStore_MarkFailedRetryableAndTerminal(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	req := recordTestRefreshRequest(t, store, "apply_failed_1", "staging", storage.DatabaseTypeMySQL, "db_failed", "org/repo", 41)

	claimed, err := store.CheckRefreshRequests().ClaimNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	past := time.Now().Add(-time.Second)
	require.NoError(t, store.CheckRefreshRequests().MarkFailed(ctx, claimed.ID, claimed.LeaseToken, "plan engine unavailable", &past))

	got, err := store.CheckRefreshRequests().GetByApplyAndKind(ctx, req.ApplyID, storage.CheckRefreshKindSettle)
	require.NoError(t, err)
	assert.Equal(t, storage.CheckRefreshFailed, got.State)
	assert.Equal(t, "plan engine unavailable", got.LastError)
	require.NotNil(t, got.RetryAfter)
	assert.Nil(t, got.CompletedAt, "a retryable failure is not terminal")

	reclaimed, err := store.CheckRefreshRequests().ClaimNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, reclaimed)
	assert.Equal(t, claimed.ID, reclaimed.ID)

	require.NoError(t, store.CheckRefreshRequests().MarkFailed(ctx, reclaimed.ID, reclaimed.LeaseToken, "still failing", nil))
	got, err = store.CheckRefreshRequests().GetByApplyAndKind(ctx, req.ApplyID, storage.CheckRefreshKindSettle)
	require.NoError(t, err)
	assert.Equal(t, storage.CheckRefreshFailed, got.State)
	assert.Nil(t, got.RetryAfter)
	assert.NotNil(t, got.CompletedAt, "a terminal failure stamps completed_at")

	none, err := store.CheckRefreshRequests().ClaimNext(ctx, "driver-c", time.Minute)
	require.NoError(t, err)
	assert.Nil(t, none, "a terminal failure must never be reclaimed")
}

// Scenario: completion is lease-guarded and idempotent — a retry with the
// retained token is a no-op, while a stale token (the row was reclaimed)
// reports lease loss instead of overwriting the new owner's run.
func TestCheckRefreshStore_MarkCompletedLeaseSemantics(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	recordTestRefreshRequest(t, store, "apply_complete_1", "staging", storage.DatabaseTypeMySQL, "db_complete", "org/repo", 51)

	claimed, err := store.CheckRefreshRequests().ClaimNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	require.NoError(t, store.CheckRefreshRequests().MarkCompleted(ctx, claimed.ID, claimed.LeaseToken))
	require.NoError(t, store.CheckRefreshRequests().MarkCompleted(ctx, claimed.ID, claimed.LeaseToken), "same-token retry is a no-op")

	err = store.CheckRefreshRequests().MarkCompleted(ctx, claimed.ID, "stale-token")
	assert.ErrorIs(t, err, storage.ErrCheckRefreshLeaseLost)

	err = store.CheckRefreshRequests().MarkCompleted(ctx, claimed.ID+9999, "any")
	assert.ErrorIs(t, err, storage.ErrCheckRefreshNotFound)
}

// Scenario: the heartbeat extends a live lease so a long fan-out is not
// reclaimed mid-flight, and reports lease loss once another driver owns the
// row.
func TestCheckRefreshStore_HeartbeatExtendsLease(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	recordTestRefreshRequest(t, store, "apply_heartbeat_1", "staging", storage.DatabaseTypeMySQL, "db_heartbeat", "org/repo", 61)

	claimed, err := store.CheckRefreshRequests().ClaimNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	require.NoError(t, store.CheckRefreshRequests().Heartbeat(ctx, claimed.ID, claimed.LeaseToken, time.Hour))

	var expiresIn float64
	require.NoError(t, testDB.QueryRowContext(ctx,
		`SELECT TIMESTAMPDIFF(SECOND, NOW(6), lease_expires_at) FROM check_refresh_requests WHERE id = ?`,
		claimed.ID).Scan(&expiresIn))
	assert.Greater(t, expiresIn, float64(30*60), "heartbeat must extend the lease well past the original minute")

	err = store.CheckRefreshRequests().Heartbeat(ctx, claimed.ID, "stale-token", time.Minute)
	assert.ErrorIs(t, err, storage.ErrCheckRefreshLeaseLost)
}

// Scenario: several applies complete on the same target while a fan-out is
// queued. One fan-out re-plans against the live schema, so it covers every
// request recorded before it started: the driver lists the pending siblings
// and completes them without their own fan-outs.
func TestCheckRefreshStore_PendingForTargetAndCoalesce(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	first := recordTestRefreshRequest(t, store, "apply_coalesce_1", "staging", storage.DatabaseTypeMySQL, "db_coalesce", "org/repo", 71)
	sibling := recordTestRefreshRequest(t, store, "apply_coalesce_2", "staging", storage.DatabaseTypeMySQL, "db_coalesce", "org/repo", 72)
	recordTestRefreshRequest(t, store, "apply_other_target", "production", storage.DatabaseTypeMySQL, "db_coalesce", "org/repo", 73)

	pending, err := store.CheckRefreshRequests().PendingForTarget(ctx, "staging", storage.DatabaseTypeMySQL, "db_coalesce", storage.CheckRefreshKindSettle, first.ID)
	require.NoError(t, err)
	require.Len(t, pending, 1, "same target only, excluding the claimed request")
	assert.Equal(t, sibling.ID, pending[0].ID)

	coalesced, err := store.CheckRefreshRequests().CompletePendingCoalesced(ctx, sibling.ID)
	require.NoError(t, err)
	assert.True(t, coalesced)

	got, err := store.CheckRefreshRequests().GetByApplyAndKind(ctx, sibling.ApplyID, storage.CheckRefreshKindSettle)
	require.NoError(t, err)
	assert.Equal(t, storage.CheckRefreshCompleted, got.State)
	assert.NotNil(t, got.CompletedAt)

	// A request that is no longer pending (here: already completed) is left to
	// its own lifecycle.
	coalesced, err = store.CheckRefreshRequests().CompletePendingCoalesced(ctx, sibling.ID)
	require.NoError(t, err)
	assert.False(t, coalesced)
}

// Scenario: a pod crashes between an apply's terminal write and the drive
// tail's refresh recording. The applies table is the outbox: the sweep finds
// completed applies in the lookback window with no refresh request, and only
// those — recorded, non-completed, and out-of-window applies stay out.
func TestCheckRefreshStore_FindCompletedAppliesMissingRequest(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	completeApply := func(name, dbName string, pr int, completedAt time.Time) *storage.Apply {
		lock := createTestLockWithPR(t, store, dbName, storage.DatabaseTypeMySQL, "staging", "org/repo", pr)
		apply := createTestApplyWithStateAndEnv(t, store, lock, name, 0, state.Apply.Completed, "staging")
		apply.CompletedAt = &completedAt
		require.NoError(t, store.Applies().Update(ctx, apply))
		return apply
	}

	missing := completeApply("apply_sweep_missing", "db_sweep_1", 81, time.Now())
	recorded := completeApply("apply_sweep_recorded", "db_sweep_2", 82, time.Now())
	_, err := store.CheckRefreshRequests().Record(ctx, &storage.CheckRefreshRequest{
		ApplyID:         recorded.ID,
		Kind:            storage.CheckRefreshKindSettle,
		ApplyIdentifier: recorded.ApplyIdentifier,
		Environment:     recorded.Environment,
		DatabaseType:    recorded.DatabaseType,
		DatabaseName:    recorded.Database,
	})
	require.NoError(t, err)
	completeApply("apply_sweep_old", "db_sweep_3", 83, time.Now().Add(-2*time.Hour))
	failedLock := createTestLockWithPR(t, store, "db_sweep_4", storage.DatabaseTypeMySQL, "staging", "org/repo", 84)
	createTestApplyWithStateAndEnv(t, store, failedLock, "apply_sweep_failed", 0, state.Apply.Failed, "staging")

	applies, err := store.CheckRefreshRequests().FindCompletedAppliesMissingRequest(ctx, time.Hour)
	require.NoError(t, err)
	require.Len(t, applies, 1)
	assert.Equal(t, missing.ApplyIdentifier, applies[0].ApplyIdentifier)
	assert.Equal(t, "db_sweep_1", applies[0].Database)
}

// Scenario: a driver is hard-killed on the request's final attempt, leaving it
// wedged in processing with an expired lease that ClaimNext will never hand
// out. The stuck sweep terminalizes exactly those rows; a wedged row still
// under the attempt cap stays reclaimable instead.
func TestCheckRefreshStore_TerminateStuckProcessing(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	stuck := recordTestRefreshRequest(t, store, "apply_stuck_1", "staging", storage.DatabaseTypeMySQL, "db_stuck_1", "org/repo", 91)
	reclaimable := recordTestRefreshRequest(t, store, "apply_stuck_2", "staging", storage.DatabaseTypeMySQL, "db_stuck_2", "org/repo", 92)

	_, err := testDB.ExecContext(ctx, `
		UPDATE check_refresh_requests
		SET state = ?, lease_owner = 'dead', lease_token = 'dead-token',
			lease_expires_at = NOW(6) - INTERVAL 1 SECOND, attempts = ?
		WHERE id = ?
	`, storage.CheckRefreshProcessing, storage.MaxCheckRefreshAttempts, stuck.ID)
	require.NoError(t, err)
	_, err = testDB.ExecContext(ctx, `
		UPDATE check_refresh_requests
		SET state = ?, lease_owner = 'dead', lease_token = 'dead-token',
			lease_expires_at = NOW(6) - INTERVAL 1 SECOND, attempts = 1
		WHERE id = ?
	`, storage.CheckRefreshProcessing, reclaimable.ID)
	require.NoError(t, err)

	terminated, err := store.CheckRefreshRequests().TerminateStuckProcessing(ctx, "attempt cap with expired lease")
	require.NoError(t, err)
	assert.Equal(t, int64(1), terminated)

	got, err := store.CheckRefreshRequests().GetByApplyAndKind(ctx, stuck.ApplyID, storage.CheckRefreshKindSettle)
	require.NoError(t, err)
	assert.Equal(t, storage.CheckRefreshFailed, got.State)
	assert.Nil(t, got.RetryAfter, "terminated rows must not be retryable")
	assert.Equal(t, "attempt cap with expired lease", got.LastError)

	still, err := store.CheckRefreshRequests().GetByApplyAndKind(ctx, reclaimable.ApplyID, storage.CheckRefreshKindSettle)
	require.NoError(t, err)
	assert.Equal(t, storage.CheckRefreshProcessing, still.State, "an under-cap wedged row stays reclaimable")
}

// Scenario: the refresh fan-out must find every PR planning against a target
// across repositories — a CLI apply carries no repository, so the reverse
// index cannot be scoped to one.
func TestCheckStore_GetByTargetSpansRepositories(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	for _, c := range []struct {
		repo string
		pr   int
		db   string
	}{
		{"org/repo-a", 1, "db_target"},
		{"org/repo-b", 2, "db_target"},
		{"org/repo-a", 3, "db_other"},
	} {
		require.NoError(t, store.Checks().Upsert(ctx, &storage.Check{
			Repository:   c.repo,
			PullRequest:  c.pr,
			HeadSHA:      "sha",
			Environment:  "staging",
			DatabaseType: storage.DatabaseTypeMySQL,
			DatabaseName: c.db,
			Status:       "completed",
			Conclusion:   "success",
		}))
	}

	checks, err := store.Checks().GetByTarget(ctx, "staging", storage.DatabaseTypeMySQL, "db_target")
	require.NoError(t, err)
	require.Len(t, checks, 2)
	assert.Equal(t, "org/repo-a", checks[0].Repository)
	assert.Equal(t, 1, checks[0].PullRequest)
	assert.Equal(t, "org/repo-b", checks[1].Repository)
	assert.Equal(t, 2, checks[1].PullRequest)
}

// Scenario: after a refresh re-plan fails, the stored check is failed closed —
// but only while the row still holds the head SHA the refresher read (a racing
// synchronize that stored a newer head wins) and never while an in-progress
// apply owns the row (the started apply's lifecycle stays authoritative).
func TestCheckStore_MarkBlockedForFailedRefresh(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	seed := func(pr int, headSHA string) *storage.Check {
		check := &storage.Check{
			Repository:   "org/repo",
			PullRequest:  pr,
			HeadSHA:      headSHA,
			Environment:  "staging",
			DatabaseType: storage.DatabaseTypeMySQL,
			DatabaseName: "db_block",
			Status:       "completed",
			Conclusion:   "success",
		}
		require.NoError(t, store.Checks().Upsert(ctx, check))
		return check
	}

	flip := func(check *storage.Check) (bool, error) {
		blocked := *check
		blocked.Status = "completed"
		blocked.Conclusion = "action_required"
		blocked.HasChanges = true
		blocked.BlockingReason = "schema_changed_replan_failed"
		blocked.ErrorMessage = "re-plan failed"
		blocked.ChangeSummary = "schema changed; re-plan failed"
		return store.Checks().MarkBlockedForFailedRefresh(ctx, &blocked)
	}

	// Matching head SHA: the flip lands and the row blocks.
	current := seed(101, "head-current")
	flipped, err := flip(current)
	require.NoError(t, err)
	assert.True(t, flipped)
	got, err := store.Checks().Get(ctx, "org/repo", 101, "staging", storage.DatabaseTypeMySQL, "db_block")
	require.NoError(t, err)
	assert.Equal(t, "action_required", got.Conclusion)
	assert.Equal(t, "schema_changed_replan_failed", got.BlockingReason)
	assert.Equal(t, "re-plan failed", got.ErrorMessage)
	assert.Equal(t, "schema changed; re-plan failed", got.ChangeSummary)
	assert.True(t, got.HasChanges)

	// Stale head SHA: a racing synchronize stored a newer head; its result wins.
	racing := seed(102, "head-old")
	_, err = testDB.ExecContext(ctx, `UPDATE checks SET head_sha = 'head-new' WHERE repository = 'org/repo' AND pull_request = 102`)
	require.NoError(t, err)
	flipped, err = flip(racing)
	require.NoError(t, err)
	assert.False(t, flipped)
	got, err = store.Checks().Get(ctx, "org/repo", 102, "staging", storage.DatabaseTypeMySQL, "db_block")
	require.NoError(t, err)
	assert.Equal(t, "success", got.Conclusion, "the newer head's stored result is preserved")

	// In-progress apply-owned row: the started apply stays authoritative.
	owned := seed(103, "head-owned")
	_, err = testDB.ExecContext(ctx, `UPDATE checks SET status = 'in_progress', apply_id = 424242 WHERE repository = 'org/repo' AND pull_request = 103`)
	require.NoError(t, err)
	owned.Status = "in_progress"
	flipped, err = flip(owned)
	require.NoError(t, err)
	assert.False(t, flipped)
	got, err = store.Checks().Get(ctx, "org/repo", 103, "staging", storage.DatabaseTypeMySQL, "db_block")
	require.NoError(t, err)
	assert.Equal(t, "in_progress", got.Status, "the in-flight apply-owned row is untouched")
	assert.Equal(t, int64(424242), got.ApplyID)
}

// Scenario: an apply's preflight exhausted its retry budget during a GitHub
// outage and failed terminally. The gate re-arms exactly that row back to
// pending with a fresh attempt budget so the next drive attempt can succeed;
// rows in any other state are left to their own lifecycle.
func TestCheckRefreshStore_ReopenForRetry(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	req := recordTestRefreshRequest(t, store, "apply_reopen_1", "staging", storage.DatabaseTypeMySQL, "db_reopen", "org/repo", 111)

	// A pending row is not reopenable.
	reopened, err := store.CheckRefreshRequests().ReopenForRetry(ctx, req.ID)
	require.NoError(t, err)
	assert.False(t, reopened, "a pending row must not be reopened")

	claimed, err := store.CheckRefreshRequests().ClaimNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.NoError(t, store.CheckRefreshRequests().MarkFailed(ctx, claimed.ID, claimed.LeaseToken, "github unavailable", nil))

	reopened, err = store.CheckRefreshRequests().ReopenForRetry(ctx, req.ID)
	require.NoError(t, err)
	assert.True(t, reopened)

	got, err := store.CheckRefreshRequests().GetByApplyAndKind(ctx, req.ApplyID, storage.CheckRefreshKindSettle)
	require.NoError(t, err)
	assert.Equal(t, storage.CheckRefreshPending, got.State)
	assert.Equal(t, 0, got.Attempts)
	assert.Nil(t, got.RetryAfter)
	assert.Nil(t, got.CompletedAt)

	reclaimed, err := store.CheckRefreshRequests().ClaimNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, reclaimed, "a reopened row is claimable again")
	assert.Equal(t, req.ID, reclaimed.ID)
}

// Scenario: an apply that recorded a preflight held sibling PR checks, then
// settled terminally without a settle request — for example it was cancelled
// while queued, so the drive tail never ran. The release sweep must find
// exactly those applies so a settle fan-out can release the holds; applies
// whose settle exists, applies without a preflight, and still-running applies
// stay out.
func TestCheckRefreshStore_FindTerminalAppliesWithPreflightMissingSettle(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	makeApply := func(name, dbName string, pr int, applyState string) *storage.Apply {
		lock := createTestLockWithPR(t, store, dbName, storage.DatabaseTypeMySQL, "staging", "org/repo", pr)
		return createTestApplyWithStateAndEnv(t, store, lock, name, 0, applyState, "staging")
	}

	// Failed apply with a preflight and no settle: the sweep must find it.
	missing := makeApply("apply_release_missing", "db_release_1", 121, state.Apply.Failed)
	recordTestRefreshRequestForApply(t, store, missing, storage.CheckRefreshKindPreflight, "staging", storage.DatabaseTypeMySQL, "db_release_1", "org/repo", 121)

	// Terminal apply whose settle was already recorded: covered.
	settled := makeApply("apply_release_settled", "db_release_2", 122, state.Apply.Completed)
	recordTestRefreshRequestForApply(t, store, settled, storage.CheckRefreshKindPreflight, "staging", storage.DatabaseTypeMySQL, "db_release_2", "org/repo", 122)
	recordTestRefreshRequestForApply(t, store, settled, storage.CheckRefreshKindSettle, "staging", storage.DatabaseTypeMySQL, "db_release_2", "org/repo", 122)

	// Terminal apply that never recorded a preflight: it held nothing.
	makeApply("apply_release_no_preflight", "db_release_3", 123, state.Apply.Failed)

	// Still-running apply with a preflight: its own settle comes later.
	running := makeApply("apply_release_running", "db_release_4", 124, state.Apply.Running)
	recordTestRefreshRequestForApply(t, store, running, storage.CheckRefreshKindPreflight, "staging", storage.DatabaseTypeMySQL, "db_release_4", "org/repo", 124)

	applies, err := store.CheckRefreshRequests().FindTerminalAppliesWithPreflightMissingSettle(ctx, time.Hour)
	require.NoError(t, err)
	require.Len(t, applies, 1)
	assert.Equal(t, missing.ApplyIdentifier, applies[0].ApplyIdentifier)
	assert.Equal(t, "db_release_1", applies[0].Database)

	// Outside the lookback window the apply is no longer swept.
	_, err = testDB.ExecContext(ctx, `UPDATE applies SET updated_at = NOW() - INTERVAL 2 HOUR WHERE id = ?`, missing.ID)
	require.NoError(t, err)
	applies, err = store.CheckRefreshRequests().FindTerminalAppliesWithPreflightMissingSettle(ctx, time.Hour)
	require.NoError(t, err)
	assert.Empty(t, applies)
}

// Scenario: an earlier apply's settle fan-out is about to re-plan sibling PR
// checks while a newer preflighted apply is still running on the same target.
// Re-planning now would overwrite the newer apply's holds with pre-cutover
// verdicts, so the settle defers whenever a non-terminal preflighted apply
// exists on the target — and only then: queued applies that never recorded a
// preflight have invalidated nothing.
func TestCheckRefreshStore_HasActivePreflightedApplyOnTarget(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Running apply on the target without a preflight: no deferral.
	lockNoPre := createTestLockWithPR(t, store, "db_active_1", storage.DatabaseTypeMySQL, "staging", "org/repo", 131)
	createTestApplyWithStateAndEnv(t, store, lockNoPre, "apply_active_no_pre", 0, state.Apply.Running, "staging")
	active, err := store.CheckRefreshRequests().HasActivePreflightedApplyOnTarget(ctx, "staging", storage.DatabaseTypeMySQL, "db_active_target")
	require.NoError(t, err)
	assert.False(t, active, "a running apply without a preflight has held nothing")

	// Running apply with a preflight on the target: defer.
	lockPre := createTestLockWithPR(t, store, "db_active_2", storage.DatabaseTypeMySQL, "staging", "org/repo", 132)
	preflighted := createTestApplyWithStateAndEnv(t, store, lockPre, "apply_active_pre", 0, state.Apply.Running, "staging")
	recordTestRefreshRequestForApply(t, store, preflighted, storage.CheckRefreshKindPreflight, "staging", storage.DatabaseTypeMySQL, "db_active_target", "org/repo", 132)

	active, err = store.CheckRefreshRequests().HasActivePreflightedApplyOnTarget(ctx, "staging", storage.DatabaseTypeMySQL, "db_active_target")
	require.NoError(t, err)
	assert.True(t, active)

	// A different target is unaffected.
	active, err = store.CheckRefreshRequests().HasActivePreflightedApplyOnTarget(ctx, "production", storage.DatabaseTypeMySQL, "db_active_target")
	require.NoError(t, err)
	assert.False(t, active)

	// Once the preflighted apply settles terminally, the deferral lifts.
	preflighted.State = state.Apply.Failed
	require.NoError(t, store.Applies().Update(ctx, preflighted))
	active, err = store.CheckRefreshRequests().HasActivePreflightedApplyOnTarget(ctx, "staging", storage.DatabaseTypeMySQL, "db_active_target")
	require.NoError(t, err)
	assert.False(t, active)
}

// Scenario: the preflight fan-out holds a sibling PR's green check
// action-required before an apply starts. The flip is conditional the same way
// a failed-refresh block is (head SHA, no in-flight apply-owned rows), and
// additionally skips rows already holding for an apply so a retried fan-out
// reports the hold as already in place instead of newly flipped.
func TestCheckStore_MarkBlockedForApplyInFlight(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	check := &storage.Check{
		Repository:   "org/repo",
		PullRequest:  141,
		HeadSHA:      "head-hold",
		Environment:  "staging",
		DatabaseType: storage.DatabaseTypeMySQL,
		DatabaseName: "db_hold",
		Status:       "completed",
		Conclusion:   "success",
	}
	require.NoError(t, store.Checks().Upsert(ctx, check))

	hold := *check
	hold.Status = "completed"
	hold.Conclusion = "action_required"
	hold.BlockingReason = "apply_in_flight_on_target"
	hold.ChangeSummary = "held: apply apply_hold_1 is changing db_hold in staging"

	flipped, err := store.Checks().MarkBlockedForApplyInFlight(ctx, &hold)
	require.NoError(t, err)
	assert.True(t, flipped)
	got, err := store.Checks().Get(ctx, "org/repo", 141, "staging", storage.DatabaseTypeMySQL, "db_hold")
	require.NoError(t, err)
	assert.Equal(t, "action_required", got.Conclusion)
	assert.Equal(t, "apply_in_flight_on_target", got.BlockingReason)

	// A retried fan-out sees the hold already in place and does not re-flip.
	flipped, err = store.Checks().MarkBlockedForApplyInFlight(ctx, &hold)
	require.NoError(t, err)
	assert.False(t, flipped, "an already-held row is not flipped again")

	// A racing synchronize that stored a newer head wins over the hold.
	stale := &storage.Check{
		Repository:   "org/repo",
		PullRequest:  142,
		HeadSHA:      "head-old",
		Environment:  "staging",
		DatabaseType: storage.DatabaseTypeMySQL,
		DatabaseName: "db_hold",
		Status:       "completed",
		Conclusion:   "success",
	}
	require.NoError(t, store.Checks().Upsert(ctx, stale))
	_, err = testDB.ExecContext(ctx, `UPDATE checks SET head_sha = 'head-new' WHERE repository = 'org/repo' AND pull_request = 142`)
	require.NoError(t, err)
	staleHold := *stale
	staleHold.Conclusion = "action_required"
	staleHold.BlockingReason = "apply_in_flight_on_target"
	flipped, err = store.Checks().MarkBlockedForApplyInFlight(ctx, &staleHold)
	require.NoError(t, err)
	assert.False(t, flipped)
	got, err = store.Checks().Get(ctx, "org/repo", 142, "staging", storage.DatabaseTypeMySQL, "db_hold")
	require.NoError(t, err)
	assert.Equal(t, "success", got.Conclusion, "the newer head's stored result is preserved")

	// An in-progress apply-owned row stays authoritative.
	owned := &storage.Check{
		Repository:   "org/repo",
		PullRequest:  143,
		HeadSHA:      "head-owned",
		Environment:  "staging",
		DatabaseType: storage.DatabaseTypeMySQL,
		DatabaseName: "db_hold",
		Status:       "completed",
		Conclusion:   "success",
	}
	require.NoError(t, store.Checks().Upsert(ctx, owned))
	_, err = testDB.ExecContext(ctx, `UPDATE checks SET status = 'in_progress', apply_id = 424243 WHERE repository = 'org/repo' AND pull_request = 143`)
	require.NoError(t, err)
	ownedHold := *owned
	ownedHold.Status = "in_progress"
	ownedHold.Conclusion = "action_required"
	ownedHold.BlockingReason = "apply_in_flight_on_target"
	flipped, err = store.Checks().MarkBlockedForApplyInFlight(ctx, &ownedHold)
	require.NoError(t, err)
	assert.False(t, flipped)
	got, err = store.Checks().Get(ctx, "org/repo", 143, "staging", storage.DatabaseTypeMySQL, "db_hold")
	require.NoError(t, err)
	assert.Equal(t, "in_progress", got.Status, "the in-flight apply-owned row is untouched")
	assert.Equal(t, int64(424243), got.ApplyID)
}
