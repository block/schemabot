package storagetest

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// TestControlRequests runs the behavioral parity suite for
// storage.ControlRequestStore: the durable pending-request lifecycle
// (request/complete/fail), convergence of concurrent requests on one row, the
// release latch, the settled-request audit read, remote-failure mirroring, and
// the apply-lease guard on pending resolution.
func TestControlRequests(t *testing.T, h Harness) {
	// createControlRequestApply creates a stopped apply — the state an
	// operator's start request targets — and returns its ID.
	createControlRequestApply := func(t *testing.T, store storage.Storage, dbName, applyIdentifier string) int64 {
		t.Helper()
		lock := CreateLock(t, store, dbName, storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, applyIdentifier, 801, state.Apply.Stopped, "staging")
		return apply.ID
	}

	// RequestPending_ReturnsExistingPending verifies a repeat request while
	// one is already pending returns the existing durable row — with its
	// original metadata — and reports alreadyPending, so a retried command
	// cannot queue duplicate work or change the payload in flight.
	t.Run("RequestPending_ReturnsExistingPending", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		applyID := createControlRequestApply(t, store, "control_pending_db", "apply_control_pending")
		first, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
			ApplyID:     applyID,
			Operation:   storage.ControlOperationStart,
			Status:      storage.ControlRequestPending,
			RequestedBy: "operator",
			Metadata:    []byte(`{"started_count":1}`),
		})
		require.NoError(t, err)
		require.False(t, alreadyPending)

		second, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
			ApplyID:     applyID,
			Operation:   storage.ControlOperationStart,
			Status:      storage.ControlRequestPending,
			RequestedBy: "operator",
			Metadata:    []byte(`{"started_count":2}`),
		})
		require.NoError(t, err)
		require.True(t, alreadyPending)

		assert.Equal(t, first.ID, second.ID)
		assert.JSONEq(t, string(first.Metadata), string(second.Metadata))
	})

	// RequestPending_ConcurrentFirstRequests verifies concurrent operator
	// requests for the same apply operation converge on one durable pending
	// row — exactly one caller creates it, every other caller observes the
	// same row as already pending — so retries and double-clicks do not
	// create extra work.
	t.Run("RequestPending_ConcurrentFirstRequests", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		applyID := createControlRequestApply(t, store, "control_concurrent_db", "apply_control_concurrent")
		const requestCount = 8
		type requestResult struct {
			req            *storage.ApplyControlRequest
			alreadyPending bool
			err            error
		}
		start := make(chan struct{})
		results := make(chan requestResult, requestCount)
		var wg sync.WaitGroup
		for range requestCount {
			wg.Go(func() {
				<-start
				req, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
					ApplyID:     applyID,
					Operation:   storage.ControlOperationStart,
					Status:      storage.ControlRequestPending,
					RequestedBy: "operator",
					Metadata:    []byte(`{"started_count":1}`),
				})
				results <- requestResult{req: req, alreadyPending: alreadyPending, err: err}
			})
		}
		close(start)
		wg.Wait()
		close(results)

		var requestID int64
		var createdCount int
		var alreadyPendingCount int
		for result := range results {
			require.NoError(t, result.err)
			require.NotNil(t, result.req)
			assert.Equal(t, storage.ControlRequestPending, result.req.Status)
			if requestID == 0 {
				requestID = result.req.ID
			}
			assert.Equal(t, requestID, result.req.ID)
			if result.alreadyPending {
				alreadyPendingCount++
			} else {
				createdCount++
			}
		}
		assert.Equal(t, 1, createdCount)
		assert.Equal(t, requestCount-1, alreadyPendingCount)
	})

	// RequestPending_ResetsCompletedRequest verifies a fresh request after
	// the previous one settled reuses the durable row: it returns to pending
	// with the new requester and metadata and a cleared completion timestamp,
	// so a completed adjustment never pins the payload of a later one.
	t.Run("RequestPending_ResetsCompletedRequest", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		applyID := createControlRequestApply(t, store, "control_reset_db", "apply_control_reset")
		first, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
			ApplyID:     applyID,
			Operation:   storage.ControlOperationStart,
			Status:      storage.ControlRequestPending,
			RequestedBy: "operator-a",
			Metadata:    []byte(`{"started_count":1}`),
		})
		require.NoError(t, err)
		require.False(t, alreadyPending)

		require.NoError(t, store.ControlRequests().CompletePending(ctx, applyID, storage.ControlOperationStart))

		second, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
			ApplyID:     applyID,
			Operation:   storage.ControlOperationStart,
			Status:      storage.ControlRequestPending,
			RequestedBy: "operator-b",
			Metadata:    []byte(`{"started_count":2}`),
		})
		require.NoError(t, err)
		require.False(t, alreadyPending)

		assert.Equal(t, first.ID, second.ID)
		assert.Equal(t, storage.ControlRequestPending, second.Status)
		assert.Equal(t, "operator-b", second.RequestedBy)
		assert.Nil(t, second.CompletedAt)
		assert.JSONEq(t, `{"started_count":2}`, string(second.Metadata))
	})

	// CompletePending verifies the happy-path resolution: a pending request
	// is visible through GetPending until completed, after which GetPending
	// hides it and GetByOperation shows the settled row with its completion
	// timestamp.
	t.Run("CompletePending", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		applyID := createControlRequestApply(t, store, "control_complete_db", "apply_control_complete")
		created, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
			ApplyID:   applyID,
			Operation: storage.ControlOperationStart,
			Status:    storage.ControlRequestPending,
			Metadata:  []byte(`{}`),
		})
		require.NoError(t, err)
		require.False(t, alreadyPending)

		pending, err := store.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationStart)
		require.NoError(t, err)
		require.NotNil(t, pending)
		assert.Equal(t, created.ID, pending.ID)

		require.NoError(t, store.ControlRequests().CompletePending(ctx, applyID, storage.ControlOperationStart))

		pending, err = store.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationStart)
		require.NoError(t, err)
		assert.Nil(t, pending)

		completed, err := store.ControlRequests().GetByOperation(ctx, applyID, storage.ControlOperationStart)
		require.NoError(t, err)
		require.NotNil(t, completed)
		assert.Equal(t, created.ID, completed.ID)
		assert.Equal(t, storage.ControlRequestCompleted, completed.Status)
		assert.NotNil(t, completed.CompletedAt)
	})

	// FailPending verifies the failure resolution: the pending row settles as
	// failed with the error message and completion timestamp recorded, and
	// GetPending no longer returns it.
	t.Run("FailPending", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		applyID := createControlRequestApply(t, store, "control_fail_db", "apply_control_fail")
		created, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
			ApplyID:   applyID,
			Operation: storage.ControlOperationStart,
			Status:    storage.ControlRequestPending,
			Metadata:  []byte(`{}`),
		})
		require.NoError(t, err)
		require.False(t, alreadyPending)

		require.NoError(t, store.ControlRequests().FailPending(ctx, applyID, storage.ControlOperationStart, "remote start failed"))

		pending, err := store.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationStart)
		require.NoError(t, err)
		assert.Nil(t, pending)

		failed, err := store.ControlRequests().GetByOperation(ctx, applyID, storage.ControlOperationStart)
		require.NoError(t, err)
		require.NotNil(t, failed)
		assert.Equal(t, created.ID, failed.ID)
		assert.Equal(t, storage.ControlRequestFailed, failed.Status)
		assert.Equal(t, "remote start failed", failed.ErrorMessage)
		assert.NotNil(t, failed.CompletedAt)
	})

	// LeaseGuardsPendingResolution verifies resolving a pending request is
	// apply-lease-guarded: a caller holding a stale lease fails closed with
	// ErrApplyLeaseLost and the request stays pending, while the current
	// lease holder resolves it. The lease is established through the driver
	// claim path, so the guard is exercised exactly as a drive exercises it.
	t.Run("LeaseGuardsPendingResolution", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "control_lease_db", storage.DatabaseTypeMySQL)
		apply := CreateClaimedApply(t, store, lock, "apply_control_lease", 802, "current-driver")

		created, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
			ApplyID:   apply.ID,
			Operation: storage.ControlOperationStart,
			Status:    storage.ControlRequestPending,
			Metadata:  []byte(`{}`),
		})
		require.NoError(t, err)
		require.False(t, alreadyPending)

		staleCtx := storage.WithApplyLease(ctx, storage.ApplyLease{
			ApplyID: apply.ID, Owner: "old-driver", Token: "stale-token",
		})
		require.ErrorIs(t, store.ControlRequests().CompletePending(staleCtx, apply.ID, storage.ControlOperationStart), storage.ErrApplyLeaseLost)
		require.ErrorIs(t, store.ControlRequests().FailPending(staleCtx, apply.ID, storage.ControlOperationStart, "stale failure"), storage.ErrApplyLeaseLost)

		pending, err := store.ControlRequests().GetPending(ctx, apply.ID, storage.ControlOperationStart)
		require.NoError(t, err)
		require.NotNil(t, pending, "a stale lease must not resolve the pending request")
		assert.Equal(t, created.ID, pending.ID)

		ownedCtx := storage.WithApplyLease(ctx, storage.ApplyLease{
			ApplyID: apply.ID, Owner: apply.LeaseOwner, Token: apply.LeaseToken,
		})
		require.NoError(t, store.ControlRequests().CompletePending(ownedCtx, apply.ID, storage.ControlOperationStart))

		completed, err := store.ControlRequests().GetByOperation(ctx, apply.ID, storage.ControlOperationStart)
		require.NoError(t, err)
		require.NotNil(t, completed)
		assert.Equal(t, storage.ControlRequestCompleted, completed.Status)
	})

	// RequestPending_ReleaseLatchIdempotent verifies a release is a one-way
	// latch: requesting it twice (an operator double-click or a retried
	// release call) converges on the single durable row rather than creating
	// extra control work.
	t.Run("RequestPending_ReleaseLatchIdempotent", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		applyID := createControlRequestApply(t, store, "control_release_db", "apply_control_release")
		first, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
			ApplyID:     applyID,
			Operation:   storage.ControlOperationRelease,
			Status:      storage.ControlRequestPending,
			RequestedBy: "operator-a",
			Metadata:    []byte(`{}`),
		})
		require.NoError(t, err)
		require.False(t, alreadyPending)

		second, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
			ApplyID:     applyID,
			Operation:   storage.ControlOperationRelease,
			Status:      storage.ControlRequestPending,
			RequestedBy: "operator-b",
			Metadata:    []byte(`{}`),
		})
		require.NoError(t, err)
		require.True(t, alreadyPending)
		assert.Equal(t, first.ID, second.ID)
	})

	// GetByOperation_ReturnsRegardlessOfStatus verifies GetByOperation reads
	// the latch regardless of status, so callers can observe a completed (or
	// failed) release that GetPending hides. The release latch holds a paused
	// rollout open while pending or completed, matching
	// ApplyControlRequest.ReleasesPausedRollout.
	t.Run("GetByOperation_ReturnsRegardlessOfStatus", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		applyID := createControlRequestApply(t, store, "control_get_by_op_db", "apply_control_get_by_op")

		none, err := store.ControlRequests().GetByOperation(ctx, applyID, storage.ControlOperationRelease)
		require.NoError(t, err)
		assert.Nil(t, none)

		created, _, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
			ApplyID:   applyID,
			Operation: storage.ControlOperationRelease,
			Status:    storage.ControlRequestPending,
			Metadata:  []byte(`{}`),
		})
		require.NoError(t, err)

		pending, err := store.ControlRequests().GetByOperation(ctx, applyID, storage.ControlOperationRelease)
		require.NoError(t, err)
		require.NotNil(t, pending)
		assert.Equal(t, created.ID, pending.ID)
		assert.Equal(t, storage.ControlRequestPending, pending.Status)
		assert.True(t, pending.ReleasesPausedRollout())

		require.NoError(t, store.ControlRequests().CompletePending(ctx, applyID, storage.ControlOperationRelease))

		gone, err := store.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationRelease)
		require.NoError(t, err)
		assert.Nil(t, gone)

		completed, err := store.ControlRequests().GetByOperation(ctx, applyID, storage.ControlOperationRelease)
		require.NoError(t, err)
		require.NotNil(t, completed)
		assert.Equal(t, created.ID, completed.ID)
		assert.Equal(t, storage.ControlRequestCompleted, completed.Status)
		assert.True(t, completed.ReleasesPausedRollout())
	})

	// GetByOperation_FailedReleaseDoesNotLatch verifies a failed release does
	// not latch the rollout open: GetByOperation still returns the row for
	// audit, but ReleasesPausedRollout reports false so the rollout stays
	// paused until a fresh release succeeds.
	t.Run("GetByOperation_FailedReleaseDoesNotLatch", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		applyID := createControlRequestApply(t, store, "control_failed_release_db", "apply_control_failed_release")
		_, _, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
			ApplyID:   applyID,
			Operation: storage.ControlOperationRelease,
			Status:    storage.ControlRequestPending,
			Metadata:  []byte(`{}`),
		})
		require.NoError(t, err)
		require.NoError(t, store.ControlRequests().FailPending(ctx, applyID, storage.ControlOperationRelease, "remote release failed"))

		failed, err := store.ControlRequests().GetByOperation(ctx, applyID, storage.ControlOperationRelease)
		require.NoError(t, err)
		require.NotNil(t, failed)
		assert.Equal(t, storage.ControlRequestFailed, failed.Status)
		assert.Equal(t, "remote release failed", failed.ErrorMessage)
		assert.False(t, failed.ReleasesPausedRollout())
	})

	// ListSettled verifies the audit read over resolved requests: an apply
	// with no settled requests returns an empty result, only completed and
	// failed rows are included — a pending request is live work, not audit
	// history — and rows come back ordered by operation.
	t.Run("ListSettled", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		applyID := createControlRequestApply(t, store, "control_settled_db", "apply_control_settled")

		settled, err := store.ControlRequests().ListSettled(ctx, applyID)
		require.NoError(t, err)
		assert.Empty(t, settled)

		// A completed stop, a failed start, and a still-pending release.
		_, _, err = store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
			ApplyID: applyID, Operation: storage.ControlOperationStop,
			Status: storage.ControlRequestPending, Metadata: []byte(`{}`),
		})
		require.NoError(t, err)
		require.NoError(t, store.ControlRequests().CompletePending(ctx, applyID, storage.ControlOperationStop))

		_, _, err = store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
			ApplyID: applyID, Operation: storage.ControlOperationStart,
			Status: storage.ControlRequestPending, Metadata: []byte(`{}`),
		})
		require.NoError(t, err)
		require.NoError(t, store.ControlRequests().FailPending(ctx, applyID, storage.ControlOperationStart, "remote start failed"))

		_, _, err = store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
			ApplyID: applyID, Operation: storage.ControlOperationRelease,
			Status: storage.ControlRequestPending, Metadata: []byte(`{}`),
		})
		require.NoError(t, err)

		settled, err = store.ControlRequests().ListSettled(ctx, applyID)
		require.NoError(t, err)
		require.Len(t, settled, 2, "a pending request is not settled")
		assert.Equal(t, storage.ControlOperationStart, settled[0].Operation)
		assert.Equal(t, storage.ControlRequestFailed, settled[0].Status)
		assert.Equal(t, "remote start failed", settled[0].ErrorMessage)
		assert.Equal(t, storage.ControlOperationStop, settled[1].Operation)
		assert.Equal(t, storage.ControlRequestCompleted, settled[1].Status)
	})

	// RecordRemoteFailure_CreatesFailedRowWhenMissing verifies mirroring a
	// rejection this plane never queued: the mirror creates a failed row
	// carrying the reported error and the mirrored-rejection marker, so the
	// operator notice survives even though no local request lifecycle owns
	// the row.
	t.Run("RecordRemoteFailure_CreatesFailedRowWhenMissing", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		applyID := createControlRequestApply(t, store, "control_mirror_create_db", "apply_control_mirror_create")
		changed, err := store.ControlRequests().RecordRemoteFailure(ctx, &storage.ApplyControlRequest{
			ApplyID:      applyID,
			Operation:    storage.ControlOperationCutover,
			RequestedBy:  storage.ForwardingControlRequestCaller,
			ErrorMessage: "remote cutover rejected",
		})
		require.NoError(t, err)
		assert.True(t, changed, "a first-seen rejection must be recorded")

		row, err := store.ControlRequests().GetByOperation(ctx, applyID, storage.ControlOperationCutover)
		require.NoError(t, err)
		require.NotNil(t, row)
		assert.Equal(t, storage.ControlRequestFailed, row.Status)
		assert.Equal(t, "remote cutover rejected", row.ErrorMessage)
		assert.Equal(t, storage.ForwardingControlRequestCaller, row.RequestedBy)
		assert.True(t, row.IsMirroredRemoteRejection(), "a mirror-created row carries the mirrored-rejection marker")
		assert.NotNil(t, row.CompletedAt)

		pending, err := store.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationCutover)
		require.NoError(t, err)
		assert.Nil(t, pending, "a mirrored rejection is settled, never pending")
	})

	// RecordRemoteFailure_IgnoresPendingRow verifies a rejection report never
	// overwrites a pending row: a pending request is a live command this
	// plane has not handed over yet, so the report in hand describes a
	// superseded attempt and overwriting it would drop the operator's
	// command.
	t.Run("RecordRemoteFailure_IgnoresPendingRow", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		applyID := createControlRequestApply(t, store, "control_mirror_pending_db", "apply_control_mirror_pending")
		created, _, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
			ApplyID:     applyID,
			Operation:   storage.ControlOperationStart,
			Status:      storage.ControlRequestPending,
			RequestedBy: "operator-a",
			Metadata:    []byte(`{}`),
		})
		require.NoError(t, err)

		changed, err := store.ControlRequests().RecordRemoteFailure(ctx, &storage.ApplyControlRequest{
			ApplyID:      applyID,
			Operation:    storage.ControlOperationStart,
			RequestedBy:  storage.ForwardingControlRequestCaller,
			ErrorMessage: "remote start rejected",
		})
		require.NoError(t, err)
		assert.False(t, changed, "a rejection of a superseded attempt must not touch the live request")

		pending, err := store.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationStart)
		require.NoError(t, err)
		require.NotNil(t, pending, "the pending request must survive the stale rejection")
		assert.Equal(t, created.ID, pending.ID)
		assert.Equal(t, "operator-a", pending.RequestedBy)
		assert.Empty(t, pending.ErrorMessage)
	})

	// RecordRemoteFailure_OvertakesCompletedRow verifies a rejection lands on
	// the row a real rejection targets: this plane completes a request the
	// moment the serving plane accepts it, so a later rejection report flips
	// the completed row to failed. The requester who issued the command is
	// preserved over the forwarding path's identity, since the notice exists
	// to tell an operator which of their commands did not take effect.
	t.Run("RecordRemoteFailure_OvertakesCompletedRow", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		applyID := createControlRequestApply(t, store, "control_mirror_completed_db", "apply_control_mirror_completed")
		_, _, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
			ApplyID:     applyID,
			Operation:   storage.ControlOperationStart,
			Status:      storage.ControlRequestPending,
			RequestedBy: "operator-a",
			Metadata:    []byte(`{}`),
		})
		require.NoError(t, err)
		require.NoError(t, store.ControlRequests().CompletePending(ctx, applyID, storage.ControlOperationStart))

		changed, err := store.ControlRequests().RecordRemoteFailure(ctx, &storage.ApplyControlRequest{
			ApplyID:      applyID,
			Operation:    storage.ControlOperationStart,
			RequestedBy:  storage.ForwardingControlRequestCaller,
			ErrorMessage: "remote start rejected",
		})
		require.NoError(t, err)
		assert.True(t, changed)

		row, err := store.ControlRequests().GetByOperation(ctx, applyID, storage.ControlOperationStart)
		require.NoError(t, err)
		require.NotNil(t, row)
		assert.Equal(t, storage.ControlRequestFailed, row.Status)
		assert.Equal(t, "remote start rejected", row.ErrorMessage)
		assert.Equal(t, "operator-a", row.RequestedBy, "the operator who issued the command is preserved over the forwarding path")
	})

	// RecordRemoteFailure_UnchangedReportIsIdempotent verifies the same
	// rejection reported on every poll is recorded exactly once: an unchanged
	// row means the failure is already known, while a report carrying a
	// different error is a distinct rejection and is re-recorded.
	t.Run("RecordRemoteFailure_UnchangedReportIsIdempotent", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		applyID := createControlRequestApply(t, store, "control_mirror_idempotent_db", "apply_control_mirror_idempotent")
		report := &storage.ApplyControlRequest{
			ApplyID:      applyID,
			Operation:    storage.ControlOperationCutover,
			RequestedBy:  storage.ForwardingControlRequestCaller,
			ErrorMessage: "remote cutover rejected",
		}
		changed, err := store.ControlRequests().RecordRemoteFailure(ctx, report)
		require.NoError(t, err)
		require.True(t, changed)

		changed, err = store.ControlRequests().RecordRemoteFailure(ctx, report)
		require.NoError(t, err)
		assert.False(t, changed, "an already-recorded rejection must not report changed again")

		changed, err = store.ControlRequests().RecordRemoteFailure(ctx, &storage.ApplyControlRequest{
			ApplyID:      applyID,
			Operation:    storage.ControlOperationCutover,
			RequestedBy:  storage.ForwardingControlRequestCaller,
			ErrorMessage: "remote cutover rejected for a new reason",
		})
		require.NoError(t, err)
		assert.True(t, changed, "a rejection with a different error is a distinct failure")

		row, err := store.ControlRequests().GetByOperation(ctx, applyID, storage.ControlOperationCutover)
		require.NoError(t, err)
		require.NotNil(t, row)
		assert.Equal(t, "remote cutover rejected for a new reason", row.ErrorMessage)
	})

	// ClearRemoteFailure_ClearsMirroredRejection verifies the mirror retires
	// its own rejection once the operation later succeeds: the mirror-created
	// failed row settles as completed with its error cleared, and a second
	// clear reports nothing left to do.
	t.Run("ClearRemoteFailure_ClearsMirroredRejection", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		applyID := createControlRequestApply(t, store, "control_mirror_clear_db", "apply_control_mirror_clear")
		changed, err := store.ControlRequests().RecordRemoteFailure(ctx, &storage.ApplyControlRequest{
			ApplyID:      applyID,
			Operation:    storage.ControlOperationCutover,
			RequestedBy:  storage.ForwardingControlRequestCaller,
			ErrorMessage: "remote cutover rejected",
		})
		require.NoError(t, err)
		require.True(t, changed)

		cleared, err := store.ControlRequests().ClearRemoteFailure(ctx, applyID, storage.ControlOperationCutover)
		require.NoError(t, err)
		assert.True(t, cleared)

		row, err := store.ControlRequests().GetByOperation(ctx, applyID, storage.ControlOperationCutover)
		require.NoError(t, err)
		require.NotNil(t, row)
		assert.Equal(t, storage.ControlRequestCompleted, row.Status)
		assert.Empty(t, row.ErrorMessage)

		cleared, err = store.ControlRequests().ClearRemoteFailure(ctx, applyID, storage.ControlOperationCutover)
		require.NoError(t, err)
		assert.False(t, cleared, "an already-cleared rejection has nothing left to clear")
	})

	// ClearRemoteFailure_IgnoresRowsItDidNotCreate verifies only a row the
	// mirror itself created is the mirror's to clear: a missing row, a
	// pending request, and a failure this plane recorded through its own
	// request lifecycle are all left untouched, since clearing them could
	// erase a failure recorded for this plane's own reason.
	t.Run("ClearRemoteFailure_IgnoresRowsItDidNotCreate", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		applyID := createControlRequestApply(t, store, "control_mirror_ignore_db", "apply_control_mirror_ignore")

		cleared, err := store.ControlRequests().ClearRemoteFailure(ctx, applyID, storage.ControlOperationStart)
		require.NoError(t, err)
		assert.False(t, cleared, "a missing row has nothing to clear")

		_, _, err = store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
			ApplyID:     applyID,
			Operation:   storage.ControlOperationStart,
			Status:      storage.ControlRequestPending,
			RequestedBy: "operator-a",
			Metadata:    []byte(`{}`),
		})
		require.NoError(t, err)

		cleared, err = store.ControlRequests().ClearRemoteFailure(ctx, applyID, storage.ControlOperationStart)
		require.NoError(t, err)
		assert.False(t, cleared, "a pending request is not the mirror's to clear")

		pending, err := store.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationStart)
		require.NoError(t, err)
		require.NotNil(t, pending)

		require.NoError(t, store.ControlRequests().FailPending(ctx, applyID, storage.ControlOperationStart, "local start failed"))

		cleared, err = store.ControlRequests().ClearRemoteFailure(ctx, applyID, storage.ControlOperationStart)
		require.NoError(t, err)
		assert.False(t, cleared, "a locally recorded failure is not the mirror's to clear")

		row, err := store.ControlRequests().GetByOperation(ctx, applyID, storage.ControlOperationStart)
		require.NoError(t, err)
		require.NotNil(t, row)
		assert.Equal(t, storage.ControlRequestFailed, row.Status)
		assert.Equal(t, "local start failed", row.ErrorMessage)
	})

	t.Run("RequestPending_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, _, err := store.ControlRequests().RequestPending(t.Context(), &storage.ApplyControlRequest{
			ApplyID:   1,
			Operation: storage.ControlOperationStart,
			Status:    storage.ControlRequestPending,
			Metadata:  []byte(`{}`),
		})
		require.Error(t, err)
	})

	t.Run("GetPending_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.ControlRequests().GetPending(t.Context(), 1, storage.ControlOperationStart)
		require.Error(t, err)
	})

	t.Run("GetByOperation_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.ControlRequests().GetByOperation(t.Context(), 1, storage.ControlOperationStart)
		require.Error(t, err)
	})

	t.Run("CompletePending_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		require.Error(t, store.ControlRequests().CompletePending(t.Context(), 1, storage.ControlOperationStart))
	})

	t.Run("FailPending_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		require.Error(t, store.ControlRequests().FailPending(t.Context(), 1, storage.ControlOperationStart, "failure"))
	})

	t.Run("ListSettled_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.ControlRequests().ListSettled(t.Context(), 1)
		require.Error(t, err)
	})

	t.Run("RecordRemoteFailure_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.ControlRequests().RecordRemoteFailure(t.Context(), &storage.ApplyControlRequest{
			ApplyID:      1,
			Operation:    storage.ControlOperationStart,
			ErrorMessage: "failure",
		})
		require.Error(t, err)
	})

	t.Run("ClearRemoteFailure_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.ControlRequests().ClearRemoteFailure(t.Context(), 1, storage.ControlOperationStart)
		require.Error(t, err)
	})
}
