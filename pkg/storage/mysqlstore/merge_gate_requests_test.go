//go:build integration

package mysqlstore

import (
	"strconv"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// recordTestMergeGateRequest records a pending merge gate request for a synthetic
// completed apply and returns it. Each request needs its own applies row
// because apply_id is the idempotency key; the anchor apply gets its own lock
// database (locks are unique per database) while the request carries the
// target under test, so several requests can share one target.
func recordTestMergeGateRequest(t *testing.T, store *Storage, name, env, dbType, dbName, repo string, pr int) *storage.MergeGateRequest {
	t.Helper()
	lock := createTestLockWithPR(t, store, name+"_lock_db", dbType, env, repo, pr)
	apply := createTestApplyWithStateAndEnv(t, store, lock, name, 0, state.Apply.Completed, env)
	req := &storage.MergeGateRequest{
		ApplyID:         apply.ID,
		ApplyIdentifier: apply.ApplyIdentifier,
		Environment:     env,
		DatabaseType:    dbType,
		DatabaseName:    dbName,
		Repository:      repo,
		ChangeKey:       strconv.Itoa(pr),
		RequestedBy:     "cli:user@host",
	}
	recorded, err := store.MergeGateRequests().Record(t.Context(), req)
	require.NoError(t, err)
	require.True(t, recorded)
	require.NotZero(t, req.ID)
	return req
}

// Scenario: the drive tail and the backstop sweep may both record a merge
// gate request for the same apply. The unique key on the apply makes recording
// idempotent, so the second recording reports recorded=false instead of
// duplicating the fan-out.
func TestMergeGateStore_RecordIsIdempotentPerApply(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	req := recordTestMergeGateRequest(t, store, "apply_refresh_1", "staging", storage.DatabaseTypeMySQL, "testdb", "org/repo", 11)

	again := &storage.MergeGateRequest{
		ApplyID:         req.ApplyID,
		ApplyIdentifier: req.ApplyIdentifier,
		Environment:     req.Environment,
		DatabaseType:    req.DatabaseType,
		DatabaseName:    req.DatabaseName,
	}
	recorded, err := store.MergeGateRequests().Record(ctx, again)
	require.NoError(t, err)
	assert.False(t, recorded)

	got, err := store.MergeGateRequests().GetByApplyID(ctx, req.ApplyID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, storage.MergeGatePending, got.State)
	assert.Equal(t, "apply_refresh_1", got.ApplyIdentifier)
	assert.Equal(t, "testdb", got.DatabaseName)
	assert.Equal(t, storage.DatabaseTypeMySQL, got.DatabaseType)
	assert.Equal(t, "staging", got.Environment)
	assert.Equal(t, "org/repo", got.Repository)
	assert.Equal(t, storage.ProviderGitHub, got.Provider)
	assert.Equal(t, "11", got.ChangeKey)
	assert.Equal(t, "cli:user@host", got.RequestedBy)
}

func TestMergeGateStore_RecordRejectsIncompleteRequests(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	for name, req := range map[string]*storage.MergeGateRequest{
		"missing apply row id":    {ApplyIdentifier: "a", Environment: "staging", DatabaseType: "mysql", DatabaseName: "db"},
		"missing apply id":        {ApplyID: 1, Environment: "staging", DatabaseType: "mysql", DatabaseName: "db"},
		"missing target database": {ApplyID: 1, ApplyIdentifier: "a", Environment: "staging", DatabaseType: "mysql"},
	} {
		_, err := store.MergeGateRequests().Record(ctx, req)
		assert.Error(t, err, name)
	}
}

// Scenario: the processor claims the oldest pending request, rotating a fresh
// lease and incrementing attempts. A second claimant must not receive the same
// request while the lease is live.
func TestMergeGateStore_ClaimNextLeasesOldestPending(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	first := recordTestMergeGateRequest(t, store, "apply_claim_1", "staging", storage.DatabaseTypeMySQL, "db_claim_1", "org/repo", 21)
	recordTestMergeGateRequest(t, store, "apply_claim_2", "staging", storage.DatabaseTypeMySQL, "db_claim_2", "org/repo", 22)

	claimed, err := store.MergeGateRequests().ClaimNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, first.ID, claimed.ID)
	assert.Equal(t, storage.MergeGateProcessing, claimed.State)
	assert.Equal(t, "driver-a", claimed.LeaseOwner)
	assert.NotEmpty(t, claimed.LeaseToken)
	assert.Equal(t, 1, claimed.Attempts)
	require.NotNil(t, claimed.LeaseExpiresAt)

	second, err := store.MergeGateRequests().ClaimNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, second)
	assert.NotEqual(t, claimed.ID, second.ID, "a live lease must not be reclaimed")

	third, err := store.MergeGateRequests().ClaimNext(ctx, "driver-c", time.Minute)
	require.NoError(t, err)
	assert.Nil(t, third, "no pending request remains once both are leased")
}

// Scenario: a driver crashes mid-fan-out. Once its lease expires the request
// is claimable again — but only while under the attempt cap, so a poison
// request cannot be reclaimed forever.
func TestMergeGateStore_ClaimNextReclaimsExpiredLeaseUnderAttemptCap(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	req := recordTestMergeGateRequest(t, store, "apply_expired_1", "staging", storage.DatabaseTypeMySQL, "db_expired", "org/repo", 31)

	claimed, err := store.MergeGateRequests().ClaimNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	_, err = testDB.ExecContext(ctx, `UPDATE merge_gate_requests SET lease_expires_at = NOW(6) - INTERVAL 1 SECOND WHERE id = ?`, req.ID)
	require.NoError(t, err)

	reclaimed, err := store.MergeGateRequests().ClaimNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, reclaimed)
	assert.Equal(t, req.ID, reclaimed.ID)
	assert.Equal(t, "driver-b", reclaimed.LeaseOwner)
	assert.NotEqual(t, claimed.LeaseToken, reclaimed.LeaseToken, "reclaim must rotate the lease token")
	assert.Equal(t, 2, reclaimed.Attempts)

	_, err = testDB.ExecContext(ctx, `UPDATE merge_gate_requests SET lease_expires_at = NOW(6) - INTERVAL 1 SECOND, attempts = ? WHERE id = ?`,
		storage.MaxMergeGateAttempts, req.ID)
	require.NoError(t, err)

	capped, err := store.MergeGateRequests().ClaimNext(ctx, "driver-c", time.Minute)
	require.NoError(t, err)
	assert.Nil(t, capped, "an expired lease at the attempt cap must not be reclaimed")
}

// Scenario: a failed fan-out is retried after its retry window elapses, and a
// terminal failure (nil retryAfter) is never handed out again.
func TestMergeGateStore_MarkFailedRetryableAndTerminal(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	req := recordTestMergeGateRequest(t, store, "apply_failed_1", "staging", storage.DatabaseTypeMySQL, "db_failed", "org/repo", 41)

	claimed, err := store.MergeGateRequests().ClaimNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	// The claim predicate compares retry_after against the database clock, so
	// place it far enough in the past to be immune to client/server skew.
	past := time.Now().Add(-time.Minute)
	require.NoError(t, store.MergeGateRequests().MarkFailed(ctx, claimed.ID, claimed.LeaseToken, "plan engine unavailable", &past))

	got, err := store.MergeGateRequests().GetByApplyID(ctx, req.ApplyID)
	require.NoError(t, err)
	assert.Equal(t, storage.MergeGateFailed, got.State)
	assert.Equal(t, "plan engine unavailable", got.LastError)
	require.NotNil(t, got.RetryAfter)
	assert.Nil(t, got.CompletedAt, "a retryable failure is not terminal")

	reclaimed, err := store.MergeGateRequests().ClaimNext(ctx, "driver-b", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, reclaimed)
	assert.Equal(t, claimed.ID, reclaimed.ID)

	require.NoError(t, store.MergeGateRequests().MarkFailed(ctx, reclaimed.ID, reclaimed.LeaseToken, "still failing", nil))
	got, err = store.MergeGateRequests().GetByApplyID(ctx, req.ApplyID)
	require.NoError(t, err)
	assert.Equal(t, storage.MergeGateFailed, got.State)
	assert.Nil(t, got.RetryAfter)
	assert.NotNil(t, got.CompletedAt, "a terminal failure stamps completed_at")

	none, err := store.MergeGateRequests().ClaimNext(ctx, "driver-c", time.Minute)
	require.NoError(t, err)
	assert.Nil(t, none, "a terminal failure must never be reclaimed")
}

// Scenario: completion is lease-guarded and idempotent — a retry with the
// retained token is a no-op, while a stale token (the row was reclaimed)
// reports lease loss instead of overwriting the new owner's run.
func TestMergeGateStore_MarkCompletedLeaseSemantics(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	recordTestMergeGateRequest(t, store, "apply_complete_1", "staging", storage.DatabaseTypeMySQL, "db_complete", "org/repo", 51)

	claimed, err := store.MergeGateRequests().ClaimNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	require.NoError(t, store.MergeGateRequests().MarkCompleted(ctx, claimed.ID, claimed.LeaseToken))
	require.NoError(t, store.MergeGateRequests().MarkCompleted(ctx, claimed.ID, claimed.LeaseToken), "same-token retry is a no-op")

	err = store.MergeGateRequests().MarkCompleted(ctx, claimed.ID, "stale-token")
	assert.ErrorIs(t, err, storage.ErrMergeGateLeaseLost)

	err = store.MergeGateRequests().MarkCompleted(ctx, claimed.ID+9999, "any")
	assert.ErrorIs(t, err, storage.ErrMergeGateNotFound)
}

// Scenario: the heartbeat extends a live lease so a long fan-out is not
// reclaimed mid-flight, and reports lease loss once another driver owns the
// row.
func TestMergeGateStore_HeartbeatExtendsLease(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	recordTestMergeGateRequest(t, store, "apply_heartbeat_1", "staging", storage.DatabaseTypeMySQL, "db_heartbeat", "org/repo", 61)

	claimed, err := store.MergeGateRequests().ClaimNext(ctx, "driver-a", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	require.NoError(t, store.MergeGateRequests().Heartbeat(ctx, claimed.ID, claimed.LeaseToken, time.Hour))

	var expiresIn float64
	require.NoError(t, testDB.QueryRowContext(ctx,
		`SELECT TIMESTAMPDIFF(SECOND, NOW(6), lease_expires_at) FROM merge_gate_requests WHERE id = ?`,
		claimed.ID).Scan(&expiresIn))
	assert.Greater(t, expiresIn, float64(30*60), "heartbeat must extend the lease well past the original minute")

	err = store.MergeGateRequests().Heartbeat(ctx, claimed.ID, "stale-token", time.Minute)
	assert.ErrorIs(t, err, storage.ErrMergeGateLeaseLost)
}

// Scenario: several applies complete on the same target while a fan-out is
// queued. One fan-out re-plans against the live schema, so it covers every
// request recorded before it started: the driver lists the pending siblings
// and completes them without their own fan-outs.
func TestMergeGateStore_PendingForTargetAndCoalesce(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	first := recordTestMergeGateRequest(t, store, "apply_coalesce_1", "staging", storage.DatabaseTypeMySQL, "db_coalesce", "org/repo", 71)
	sibling := recordTestMergeGateRequest(t, store, "apply_coalesce_2", "staging", storage.DatabaseTypeMySQL, "db_coalesce", "org/repo", 72)
	recordTestMergeGateRequest(t, store, "apply_other_target", "production", storage.DatabaseTypeMySQL, "db_coalesce", "org/repo", 73)

	pending, err := store.MergeGateRequests().PendingForTarget(ctx, "staging", storage.DatabaseTypeMySQL, "db_coalesce", first.ID)
	require.NoError(t, err)
	require.Len(t, pending, 1, "same target only, excluding the claimed request")
	assert.Equal(t, sibling.ID, pending[0].ID)

	coalesced, err := store.MergeGateRequests().CompletePendingCoalesced(ctx, sibling.ID)
	require.NoError(t, err)
	assert.True(t, coalesced)

	got, err := store.MergeGateRequests().GetByApplyID(ctx, sibling.ApplyID)
	require.NoError(t, err)
	assert.Equal(t, storage.MergeGateCompleted, got.State)
	assert.NotNil(t, got.CompletedAt)

	// A request that is no longer pending (here: already completed) is left to
	// its own lifecycle.
	coalesced, err = store.MergeGateRequests().CompletePendingCoalesced(ctx, sibling.ID)
	require.NoError(t, err)
	assert.False(t, coalesced)
}

// Scenario: a pod crashes between an apply's terminal write and the drive
// tail's merge gate recording. The applies table is the outbox: the sweep finds
// completed applies in the lookback window with no merge gate request, and only
// those — recorded, non-completed, and out-of-window applies stay out.
func TestMergeGateStore_FindCompletedAppliesMissingRequest(t *testing.T) {
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
	_, err := store.MergeGateRequests().Record(ctx, &storage.MergeGateRequest{
		ApplyID:         recorded.ID,
		ApplyIdentifier: recorded.ApplyIdentifier,
		Environment:     recorded.Environment,
		DatabaseType:    recorded.DatabaseType,
		DatabaseName:    recorded.Database,
	})
	require.NoError(t, err)
	completeApply("apply_sweep_old", "db_sweep_3", 83, time.Now().Add(-2*time.Hour))
	failedLock := createTestLockWithPR(t, store, "db_sweep_4", storage.DatabaseTypeMySQL, "staging", "org/repo", 84)
	createTestApplyWithStateAndEnv(t, store, failedLock, "apply_sweep_failed", 0, state.Apply.Failed, "staging")

	applies, err := store.MergeGateRequests().FindCompletedAppliesMissingRequest(ctx, time.Hour)
	require.NoError(t, err)
	require.Len(t, applies, 1)
	assert.Equal(t, missing.ApplyIdentifier, applies[0].ApplyIdentifier)
	assert.Equal(t, "db_sweep_1", applies[0].Database)
}

// Scenario: a driver is hard-killed on the request's final attempt, leaving it
// wedged in processing with an expired lease that ClaimNext will never hand
// out. The stuck sweep terminalizes exactly those rows; a wedged row still
// under the attempt cap stays reclaimable instead.
func TestMergeGateStore_TerminateStuckProcessing(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	stuck := recordTestMergeGateRequest(t, store, "apply_stuck_1", "staging", storage.DatabaseTypeMySQL, "db_stuck_1", "org/repo", 91)
	reclaimable := recordTestMergeGateRequest(t, store, "apply_stuck_2", "staging", storage.DatabaseTypeMySQL, "db_stuck_2", "org/repo", 92)

	_, err := testDB.ExecContext(ctx, `
		UPDATE merge_gate_requests
		SET state = ?, lease_owner = 'dead', lease_token = 'dead-token',
			lease_expires_at = NOW(6) - INTERVAL 1 SECOND, attempts = ?
		WHERE id = ?
	`, storage.MergeGateProcessing, storage.MaxMergeGateAttempts, stuck.ID)
	require.NoError(t, err)
	_, err = testDB.ExecContext(ctx, `
		UPDATE merge_gate_requests
		SET state = ?, lease_owner = 'dead', lease_token = 'dead-token',
			lease_expires_at = NOW(6) - INTERVAL 1 SECOND, attempts = 1
		WHERE id = ?
	`, storage.MergeGateProcessing, reclaimable.ID)
	require.NoError(t, err)

	terminated, err := store.MergeGateRequests().TerminateStuckProcessing(ctx, "attempt cap with expired lease")
	require.NoError(t, err)
	assert.Equal(t, int64(1), terminated)

	got, err := store.MergeGateRequests().GetByApplyID(ctx, stuck.ApplyID)
	require.NoError(t, err)
	assert.Equal(t, storage.MergeGateFailed, got.State)
	assert.Nil(t, got.RetryAfter, "terminated rows must not be retryable")
	assert.Equal(t, "attempt cap with expired lease", got.LastError)

	still, err := store.MergeGateRequests().GetByApplyID(ctx, reclaimable.ApplyID)
	require.NoError(t, err)
	assert.Equal(t, storage.MergeGateProcessing, still.State, "an under-cap wedged row stays reclaimable")
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

// Scenario: after a settle re-plan fails, the stored check is failed closed —
// but only while the row still holds the head SHA the processor read (a racing
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
