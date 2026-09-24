package storagetest

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// TestMergeGateRequests runs the behavioral parity suite for
// storage.MergeGateRequestStore: per-apply recording idempotence and required
// fields, FIFO claim ordering with lease rotation, claim exclusivity under
// concurrent drivers, expired-lease reclaim under the attempt cap, retryable
// versus terminal failure including terminalization at the cap, attempt-
// refunding release, lease-guarded completion and heartbeat, same-target
// coalescing, the outbox sweep over completed applies with no request, and the
// stuck-processing sweep.
func TestMergeGateRequests(t *testing.T, h Harness) {
	// record records a pending request for a synthetic completed apply. Each
	// request needs its own applies row because apply_id is the idempotency
	// key; the anchor apply gets its own lock database (locks are unique per
	// database) while the request carries the target under test, so several
	// requests can share one target.
	record := func(t *testing.T, store storage.Storage, name, env, dbName string, pr int) *storage.MergeGateRequest {
		t.Helper()
		lock := CreateLockWithPR(t, store, name+"_lock_db", storage.DatabaseTypeMySQL, "org/repo", pr)
		apply := CreateApplyWithStateAndEnv(t, store, lock, name, 0, state.Apply.Completed, env)
		req := &storage.MergeGateRequest{
			ApplyID:         apply.ID,
			ApplyIdentifier: apply.ApplyIdentifier,
			Environment:     env,
			DatabaseType:    storage.DatabaseTypeMySQL,
			DatabaseName:    dbName,
			Repository:      "org/repo",
			ChangeKey:       strconv.Itoa(pr),
			RequestedBy:     "cli:user@host",
		}
		recorded, err := store.MergeGateRequests().Record(t.Context(), req)
		require.NoError(t, err)
		require.True(t, recorded)
		require.NotZero(t, req.ID)
		return req
	}

	// claimExpecting claims the next request and asserts it is the one the
	// test expects, so later assertions never run against the wrong claim.
	claimExpecting := func(t *testing.T, store storage.Storage, want int64) *storage.MergeGateRequest {
		t.Helper()
		claimed, err := store.MergeGateRequests().ClaimNext(t.Context(), "driver-a", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claimed)
		require.Equal(t, want, claimed.ID)
		return claimed
	}

	// driveToCapExhausted records a request and drives it to the attempt cap
	// through the interface alone: every attempt but the last is claimed and
	// failed retryably with an elapsed retry window, and the final attempt is
	// claimed with finalLease. A finalLease short enough to expire leaves the
	// row in the stuck-processing shape a hard-killed driver produces. The
	// request must be the only claimable row while this runs, or the loop
	// claims a bystander.
	driveToCapExhausted := func(t *testing.T, store storage.Storage, name, dbName string, pr int, finalLease time.Duration) *storage.MergeGateRequest {
		t.Helper()
		ctx := t.Context()
		req := record(t, store, name, "staging", dbName, pr)
		past := time.Now().UTC().Add(-time.Hour)
		for range storage.MaxMergeGateAttempts - 1 {
			claimed := claimExpecting(t, store, req.ID)
			require.NoError(t, store.MergeGateRequests().MarkFailed(ctx, claimed.ID, claimed.LeaseToken, "attempt failed", &past))
		}
		claimed, err := store.MergeGateRequests().ClaimNext(ctx, "driver-a", finalLease)
		require.NoError(t, err)
		require.NotNil(t, claimed)
		require.Equal(t, req.ID, claimed.ID)
		require.Equal(t, storage.MaxMergeGateAttempts, claimed.Attempts)
		return req
	}

	// The drive tail and the backstop sweep may both record a request for the
	// same apply. The unique key on the apply makes recording idempotent, so
	// the second recording reports recorded=false instead of duplicating the
	// fan-out.
	t.Run("RecordIsIdempotentPerApply", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		req := record(t, store, "apply_refresh_1", "staging", "testdb", 11)

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
	})

	// A request whose target or originating apply is not fully identified is
	// rejected at the write boundary: a fan-out cannot be aimed at an
	// incomplete target, and an unattributable row could never be swept.
	t.Run("RecordRejectsIncompleteRequests", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		for name, req := range map[string]*storage.MergeGateRequest{
			"missing apply row id":    {ApplyIdentifier: "a", Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "db"},
			"missing apply id":        {ApplyID: 1, Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL, DatabaseName: "db"},
			"missing target database": {ApplyID: 1, ApplyIdentifier: "a", Environment: "staging", DatabaseType: storage.DatabaseTypeMySQL},
		} {
			_, err := store.MergeGateRequests().Record(ctx, req)
			assert.Error(t, err, name)
		}
	})

	// The identity keys a fan-out is aimed at fold to their canonical
	// spelling on the way in, so a request recorded from a mixed-case
	// environment or database still matches the target lookups. Byte-comparing
	// dialects would otherwise find no siblings and silently fan out to
	// nothing.
	t.Run("CanonicalIdentityKey", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLockWithPR(t, store, "db_canonical_lock", storage.DatabaseTypeMySQL, "MixedCase/Sample-Repo", 5)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "apply_canonical", 0, state.Apply.Completed, "staging")
		req := &storage.MergeGateRequest{
			ApplyID:         apply.ID,
			ApplyIdentifier: apply.ApplyIdentifier,
			Environment:     "Staging",
			DatabaseType:    "MySQL",
			DatabaseName:    "DB_Canonical",
			Repository:      "MixedCase/Sample-Repo",
		}
		recorded, err := store.MergeGateRequests().Record(ctx, req)
		require.NoError(t, err)
		require.True(t, recorded)

		got, err := store.MergeGateRequests().GetByApplyID(ctx, apply.ID)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, "staging", got.Environment)
		assert.Equal(t, "mysql", got.DatabaseType)
		assert.Equal(t, "db_canonical", got.DatabaseName)
		assert.Equal(t, "mixedcase/sample-repo", got.Repository)

		// The target lookup folds its arguments too, so a caller holding the
		// mixed-case spelling still finds the row.
		pending, err := store.MergeGateRequests().PendingForTarget(ctx, "Staging", "MySQL", "DB_Canonical", 0)
		require.NoError(t, err)
		require.Len(t, pending, 1)
		assert.Equal(t, got.ID, pending[0].ID)
	})

	// The processor claims the oldest pending request, rotating a fresh lease
	// and incrementing attempts. A second claimant must not receive the same
	// request while the lease is live.
	t.Run("ClaimNextLeasesOldestPending", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		first := record(t, store, "apply_claim_1", "staging", "db_claim_1", 21)
		record(t, store, "apply_claim_2", "staging", "db_claim_2", 22)

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
	})

	// A driver crashes mid-fan-out. Once its lease expires the request is
	// claimable again — but only while under the attempt cap, so a poison
	// request cannot be reclaimed forever.
	t.Run("ClaimNextReclaimsExpiredLeaseUnderAttemptCap", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		req := record(t, store, "apply_expired_1", "staging", "db_expired", 31)

		claimed, err := store.MergeGateRequests().ClaimNext(ctx, "driver-a", time.Millisecond)
		require.NoError(t, err)
		require.NotNil(t, claimed)

		var reclaimed *storage.MergeGateRequest
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			reclaimed, err = store.MergeGateRequests().ClaimNext(ctx, "driver-b", time.Minute)
			if !assert.NoError(collect, err) {
				return
			}
			assert.NotNil(collect, reclaimed)
		}, pollDeadline, pollInterval)
		assert.Equal(t, req.ID, reclaimed.ID)
		assert.Equal(t, "driver-b", reclaimed.LeaseOwner)
		assert.NotEqual(t, claimed.LeaseToken, reclaimed.LeaseToken, "reclaim must rotate the lease token")
		assert.Equal(t, 2, reclaimed.Attempts)
	})

	// At the attempt cap an expired lease is no longer reclaimable, so the
	// request stops consuming driver attempts and the stuck sweep is the only
	// path that can move it.
	t.Run("ClaimNextIgnoresExpiredLeaseAtAttemptCap", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		req := driveToCapExhausted(t, store, "apply_capped_1", "db_capped", 32, time.Millisecond)

		// Wait for the lease to lapse before asserting: under a live lease a
		// nil claim would pass for the wrong reason.
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			got, err := store.MergeGateRequests().GetByApplyID(ctx, req.ApplyID)
			if !assert.NoError(collect, err) {
				return
			}
			if !assert.NotNil(collect, got.LeaseExpiresAt) {
				return
			}
			assert.True(collect, got.LeaseExpiresAt.Before(time.Now().UTC()))
		}, pollDeadline, pollInterval)

		capped, err := store.MergeGateRequests().ClaimNext(ctx, "driver-c", time.Minute)
		require.NoError(t, err)
		assert.Nil(t, capped, "an expired lease at the attempt cap must not be reclaimed")
	})

	// A failed fan-out is retried after its retry window elapses, and a
	// terminal failure (nil retryAfter) is never handed out again.
	t.Run("MarkFailedRetryableAndTerminal", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		req := record(t, store, "apply_failed_1", "staging", "db_failed", 41)

		claimed := claimExpecting(t, store, req.ID)

		// The claim predicate compares retry_after against the database clock,
		// so place it far enough in the past to be immune to client/server skew.
		past := time.Now().UTC().Add(-time.Minute)
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
	})

	// A retryable failure recorded on the final attempt is stored terminally.
	// The claim predicate only reclaims a failed row under the attempt cap, so
	// a row left retryable at the cap would be one nothing ever claims again
	// and nothing ever resolves — queued forever by its stored state, dead in
	// practice.
	t.Run("MarkFailedTerminalizesAtAttemptCap", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		req := driveToCapExhausted(t, store, "apply_cap_fail", "db_cap_fail", 46, time.Hour)
		claimed, err := store.MergeGateRequests().GetByApplyID(ctx, req.ApplyID)
		require.NoError(t, err)
		require.Equal(t, storage.MaxMergeGateAttempts, claimed.Attempts)

		past := time.Now().UTC().Add(-time.Hour)
		require.NoError(t, store.MergeGateRequests().MarkFailed(ctx, claimed.ID, claimed.LeaseToken, "final attempt failed", &past))

		got, err := store.MergeGateRequests().GetByApplyID(ctx, req.ApplyID)
		require.NoError(t, err)
		assert.Equal(t, storage.MergeGateFailed, got.State)
		assert.Equal(t, "final attempt failed", got.LastError)
		assert.Nil(t, got.RetryAfter, "a failure at the attempt cap is not retryable")
		assert.NotNil(t, got.CompletedAt, "a failure at the attempt cap is terminal")

		none, err := store.MergeGateRequests().ClaimNext(ctx, "driver-b", time.Minute)
		require.NoError(t, err)
		assert.Nil(t, none)
	})

	// A driver standing down without deciding the request's outcome hands it
	// back and refunds the attempt: a rolling deploy must not spend a request's
	// attempt budget, or a fan-out that would have succeeded reaches the cap on
	// process lifecycle events alone.
	t.Run("ReleaseRefundsAttempt", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		req := record(t, store, "apply_release_1", "staging", "db_release", 47)
		claimed := claimExpecting(t, store, req.ID)
		require.Equal(t, 1, claimed.Attempts)

		require.NoError(t, store.MergeGateRequests().Release(ctx, claimed.ID, claimed.LeaseToken))

		released, err := store.MergeGateRequests().GetByApplyID(ctx, req.ApplyID)
		require.NoError(t, err)
		assert.Equal(t, storage.MergeGatePending, released.State)
		assert.Equal(t, 0, released.Attempts, "the released attempt is refunded")
		assert.Empty(t, released.LeaseOwner)
		assert.Empty(t, released.LeaseToken)
		assert.Nil(t, released.LeaseExpiresAt)

		reclaimed := claimExpecting(t, store, req.ID)
		assert.Equal(t, 1, reclaimed.Attempts, "the reclaim starts from the refunded count")

		err = store.MergeGateRequests().Release(ctx, claimed.ID, "stale-token")
		assert.ErrorIs(t, err, storage.ErrMergeGateLeaseLost)

		require.NoError(t, store.MergeGateRequests().MarkCompleted(ctx, reclaimed.ID, reclaimed.LeaseToken))
		err = store.MergeGateRequests().Release(ctx, reclaimed.ID, reclaimed.LeaseToken)
		assert.ErrorIs(t, err, storage.ErrMergeGateLeaseLost, "a finished request cannot be handed back")

		err = store.MergeGateRequests().Release(ctx, reclaimed.ID+9999, "any")
		assert.ErrorIs(t, err, storage.ErrMergeGateNotFound)
	})

	// Several drivers poll the same queue at once. The claim is exclusive: no
	// request is ever handed to two drivers, because two drivers fanning out
	// the same target would each re-plan every PR gated on it and race to write
	// the resulting check state.
	//
	// Exclusivity is the property under test, not exhaustiveness. Concurrent
	// claimants skip each other's locked rows, so a single round of polls can
	// leave a claimable request behind; the processor polls in a loop, and the
	// serial drain below stands in for the next pass. What must never happen
	// is the same request coming back twice.
	t.Run("ClaimNextIsExclusiveUnderConcurrency", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		const requests = 4
		for i := range requests {
			record(t, store, "apply_contend_"+strconv.Itoa(i), "staging", "db_contend", 480+i)
		}

		var mu sync.Mutex
		claimedBy := make(map[int64]string)
		var claimErrs []error

		// Every claim, concurrent or serial, goes through here so a request
		// handed out twice fails the test wherever the duplicate came from.
		claim := func(owner string) (bool, error) {
			claimed, err := store.MergeGateRequests().ClaimNext(ctx, owner, time.Minute)
			if err != nil {
				return false, err
			}
			if claimed == nil {
				return false, nil
			}
			mu.Lock()
			defer mu.Unlock()
			if previous, taken := claimedBy[claimed.ID]; taken {
				return false, fmt.Errorf("request %d claimed by both %s and %s", claimed.ID, previous, owner)
			}
			claimedBy[claimed.ID] = owner
			return true, nil
		}

		// More drivers than requests, so the losers exercise the empty-queue
		// path in the same round.
		const drivers = requests + 2
		var wg sync.WaitGroup
		for i := range drivers {
			owner := "driver-" + strconv.Itoa(i)
			wg.Go(func() {
				if _, err := claim(owner); err != nil {
					mu.Lock()
					claimErrs = append(claimErrs, err)
					mu.Unlock()
				}
			})
		}
		wg.Wait()
		require.Empty(t, claimErrs)
		require.NotEmpty(t, claimedBy, "contending drivers must not all come away empty")

		// Drain what the contended round skipped. Each request appears exactly
		// once across both phases.
		for range requests {
			claimed, err := claim("driver-drain")
			require.NoError(t, err)
			if !claimed {
				break
			}
		}
		assert.Len(t, claimedBy, requests, "every request is claimed exactly once")
	})

	// Completion is lease-guarded and idempotent — a retry with the retained
	// token is a no-op, while a stale token (the row was reclaimed) reports
	// lease loss instead of overwriting the new owner's run.
	t.Run("MarkCompletedLeaseSemantics", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		req := record(t, store, "apply_complete_1", "staging", "db_complete", 51)
		claimed := claimExpecting(t, store, req.ID)

		require.NoError(t, store.MergeGateRequests().MarkCompleted(ctx, claimed.ID, claimed.LeaseToken))
		require.NoError(t, store.MergeGateRequests().MarkCompleted(ctx, claimed.ID, claimed.LeaseToken), "same-token retry is a no-op")

		err := store.MergeGateRequests().MarkCompleted(ctx, claimed.ID, "stale-token")
		assert.ErrorIs(t, err, storage.ErrMergeGateLeaseLost)

		err = store.MergeGateRequests().MarkCompleted(ctx, claimed.ID+9999, "any")
		assert.ErrorIs(t, err, storage.ErrMergeGateNotFound)
	})

	// The heartbeat extends a live lease so a long fan-out is not reclaimed
	// mid-flight, and reports lease loss once another driver owns the row.
	t.Run("HeartbeatExtendsLease", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		req := record(t, store, "apply_heartbeat_1", "staging", "db_heartbeat", 61)
		claimed := claimExpecting(t, store, req.ID)
		require.NotNil(t, claimed.LeaseExpiresAt)

		require.NoError(t, store.MergeGateRequests().Heartbeat(ctx, claimed.ID, claimed.LeaseToken, time.Hour))

		extended, err := store.MergeGateRequests().GetByApplyID(ctx, req.ApplyID)
		require.NoError(t, err)
		require.NotNil(t, extended.LeaseExpiresAt)
		assert.Greater(t, extended.LeaseExpiresAt.Sub(*claimed.LeaseExpiresAt), 30*time.Minute,
			"heartbeat must extend the lease well past the original minute")

		err = store.MergeGateRequests().Heartbeat(ctx, claimed.ID, "stale-token", time.Minute)
		assert.ErrorIs(t, err, storage.ErrMergeGateLeaseLost)
	})

	// A terminal write keeps the lease token so its own retry stays idempotent,
	// which makes a token match alone a poor liveness signal. A heartbeat on a
	// finished request reports lease loss, so a driver whose fan-out already
	// resolved cannot read a successful heartbeat as permission to keep going.
	t.Run("HeartbeatRejectsFinishedRequest", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		req := record(t, store, "apply_heartbeat_2", "staging", "db_heartbeat_done", 62)
		claimed := claimExpecting(t, store, req.ID)

		require.NoError(t, store.MergeGateRequests().MarkCompleted(ctx, claimed.ID, claimed.LeaseToken))

		err := store.MergeGateRequests().Heartbeat(ctx, claimed.ID, claimed.LeaseToken, time.Hour)
		assert.ErrorIs(t, err, storage.ErrMergeGateLeaseLost)

		err = store.MergeGateRequests().Heartbeat(ctx, claimed.ID+9999, "any", time.Minute)
		assert.ErrorIs(t, err, storage.ErrMergeGateNotFound)
	})

	// Several applies complete on the same target while a fan-out is queued.
	// One fan-out re-plans against the live schema, so it covers every request
	// recorded before it started: the driver lists the pending siblings and
	// completes them without their own fan-outs.
	t.Run("PendingForTargetAndCoalesce", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		first := record(t, store, "apply_coalesce_1", "staging", "db_coalesce", 71)
		sibling := record(t, store, "apply_coalesce_2", "staging", "db_coalesce", 72)
		record(t, store, "apply_other_target", "production", "db_coalesce", 73)

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

		// A request that is no longer pending (here: already completed) is
		// left to its own lifecycle.
		coalesced, err = store.MergeGateRequests().CompletePendingCoalesced(ctx, sibling.ID)
		require.NoError(t, err)
		assert.False(t, coalesced)
	})

	// A pod crashes between an apply's terminal write and the drive tail's
	// recording. The applies table is the outbox: the sweep finds completed
	// applies in the lookback window with no request, and only those —
	// recorded, non-completed, and out-of-window applies stay out.
	t.Run("FindCompletedAppliesMissingRequest", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		completeApply := func(name, dbName string, pr int, completedAt time.Time) *storage.Apply {
			lock := CreateLockWithPR(t, store, dbName, storage.DatabaseTypeMySQL, "org/repo", pr)
			apply := CreateApplyWithStateAndEnv(t, store, lock, name, 0, state.Apply.Completed, "staging")
			apply.CompletedAt = &completedAt
			require.NoError(t, store.Applies().Update(ctx, apply))
			return apply
		}

		missing := completeApply("apply_sweep_missing", "db_sweep_1", 81, time.Now().UTC())
		recorded := completeApply("apply_sweep_recorded", "db_sweep_2", 82, time.Now().UTC())
		_, err := store.MergeGateRequests().Record(ctx, &storage.MergeGateRequest{
			ApplyID:         recorded.ID,
			ApplyIdentifier: recorded.ApplyIdentifier,
			Environment:     recorded.Environment,
			DatabaseType:    recorded.DatabaseType,
			DatabaseName:    recorded.Database,
		})
		require.NoError(t, err)
		completeApply("apply_sweep_old", "db_sweep_3", 83, time.Now().UTC().Add(-2*time.Hour))
		failedLock := CreateLockWithPR(t, store, "db_sweep_4", storage.DatabaseTypeMySQL, "org/repo", 84)
		CreateApplyWithStateAndEnv(t, store, failedLock, "apply_sweep_failed", 0, state.Apply.Failed, "staging")

		applies, err := store.MergeGateRequests().FindCompletedAppliesMissingRequest(ctx, time.Hour)
		require.NoError(t, err)
		require.Len(t, applies, 1)
		assert.Equal(t, missing.ApplyIdentifier, applies[0].ApplyIdentifier)
		assert.Equal(t, "db_sweep_1", applies[0].Database)
	})

	// A driver is hard-killed on the request's final attempt, leaving it
	// wedged in processing with an expired lease that ClaimNext will never
	// hand out. The stuck sweep terminalizes exactly those rows; a wedged row
	// still under the attempt cap stays reclaimable instead.
	t.Run("TerminateStuckProcessing", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		// Build each row's state sequentially so the FIFO claim inside the
		// cap-drive helper always lands on its own request: a row at the cap
		// or holding a live lease is invisible to later claims.
		stuck := driveToCapExhausted(t, store, "apply_stuck_1", "db_stuck_1", 91, time.Millisecond)
		driveToCapExhausted(t, store, "apply_stuck_2", "db_stuck_2", 92, time.Minute)

		reclaimable := record(t, store, "apply_stuck_3", "staging", "db_stuck_3", 93)
		underCap, err := store.MergeGateRequests().ClaimNext(ctx, "driver-a", time.Millisecond)
		require.NoError(t, err)
		require.NotNil(t, underCap)
		require.Equal(t, reclaimable.ID, underCap.ID)

		var terminated int64
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			terminated, err = store.MergeGateRequests().TerminateStuckProcessing(ctx, "attempt cap with expired lease")
			if !assert.NoError(collect, err) {
				return
			}
			assert.Equal(collect, int64(1), terminated)
		}, pollDeadline, pollInterval)

		got, err := store.MergeGateRequests().GetByApplyID(ctx, stuck.ApplyID)
		require.NoError(t, err)
		assert.Equal(t, storage.MergeGateFailed, got.State)
		assert.Nil(t, got.RetryAfter, "terminated rows must not be retryable")
		assert.Equal(t, "attempt cap with expired lease", got.LastError)

		still, err := store.MergeGateRequests().GetByApplyID(ctx, reclaimable.ApplyID)
		require.NoError(t, err)
		assert.Equal(t, storage.MergeGateProcessing, still.State, "an under-cap wedged row stays reclaimable")
	})

	t.Run("Record_DBError", func(t *testing.T) {
		_, err := h.NewUnreachableStorage(t).MergeGateRequests().Record(t.Context(), &storage.MergeGateRequest{
			ApplyID:         1,
			ApplyIdentifier: "apply",
			Environment:     "staging",
			DatabaseType:    storage.DatabaseTypeMySQL,
			DatabaseName:    "db",
		})
		require.Error(t, err)
	})

	t.Run("GetByApplyID_DBError", func(t *testing.T) {
		_, err := h.NewUnreachableStorage(t).MergeGateRequests().GetByApplyID(t.Context(), 1)
		require.Error(t, err)
	})

	t.Run("ClaimNext_DBError", func(t *testing.T) {
		_, err := h.NewUnreachableStorage(t).MergeGateRequests().ClaimNext(t.Context(), "driver-a", time.Minute)
		require.Error(t, err)
	})

	t.Run("PendingForTarget_DBError", func(t *testing.T) {
		_, err := h.NewUnreachableStorage(t).MergeGateRequests().PendingForTarget(t.Context(), "staging", storage.DatabaseTypeMySQL, "db", 0)
		require.Error(t, err)
	})

	t.Run("Heartbeat_DBError", func(t *testing.T) {
		require.Error(t, h.NewUnreachableStorage(t).MergeGateRequests().Heartbeat(t.Context(), 1, "token", time.Minute))
	})

	t.Run("Release_DBError", func(t *testing.T) {
		require.Error(t, h.NewUnreachableStorage(t).MergeGateRequests().Release(t.Context(), 1, "token"))
	})

	t.Run("MarkCompleted_DBError", func(t *testing.T) {
		require.Error(t, h.NewUnreachableStorage(t).MergeGateRequests().MarkCompleted(t.Context(), 1, "token"))
	})

	t.Run("CompletePendingCoalesced_DBError", func(t *testing.T) {
		_, err := h.NewUnreachableStorage(t).MergeGateRequests().CompletePendingCoalesced(t.Context(), 1)
		require.Error(t, err)
	})

	t.Run("MarkFailed_DBError", func(t *testing.T) {
		require.Error(t, h.NewUnreachableStorage(t).MergeGateRequests().MarkFailed(t.Context(), 1, "token", "boom", nil))
	})

	t.Run("FindCompletedAppliesMissingRequest_DBError", func(t *testing.T) {
		_, err := h.NewUnreachableStorage(t).MergeGateRequests().FindCompletedAppliesMissingRequest(t.Context(), time.Hour)
		require.Error(t, err)
	})

	t.Run("TerminateStuckProcessing_DBError", func(t *testing.T) {
		_, err := h.NewUnreachableStorage(t).MergeGateRequests().TerminateStuckProcessing(t.Context(), "sweep")
		require.Error(t, err)
	})
}
