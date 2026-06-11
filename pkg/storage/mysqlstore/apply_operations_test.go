//go:build integration

package mysqlstore

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func TestApplyOperationStore_InsertAndGet(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_md_insert", 1)

	ad := &storage.ApplyOperation{
		ApplyID:    apply.ID,
		Deployment: "region-a",
		Target:     "payments",
	}

	id, err := store.ApplyOperations().Insert(ctx, ad)
	require.NoError(t, err)
	require.NotZero(t, id)
	assert.Equal(t, id, ad.ID)
	assert.Equal(t, state.ApplyOperation.Pending, ad.State, "default state should be pending")

	got, err := store.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, apply.ID, got.ApplyID)
	assert.Equal(t, "region-a", got.Deployment)
	assert.Equal(t, "payments", got.Target)
	assert.Equal(t, state.ApplyOperation.Pending, got.State)
	assert.Empty(t, got.ErrorMessage)
	assert.Nil(t, got.StartedAt)
	assert.Nil(t, got.CompletedAt)
	assert.Empty(t, got.EngineResumeContext)
	assert.Empty(t, got.EngineResumeMetadata)
	assert.NotZero(t, got.CreatedAt)
	assert.NotZero(t, got.UpdatedAt)
}

func TestApplyOperationStore_Get_NotFound(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	got, err := store.ApplyOperations().Get(ctx, 999999)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestApplyOperationStore_EngineResumeState(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeVitess, "staging")
	apply := createTestApply(t, store, lock, "apply_op_engine_resume_state", 1)
	operationID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID:    apply.ID,
		Deployment: "region-a",
		Target:     "payments",
	})
	require.NoError(t, err)

	_, err = store.ApplyOperations().GetEngineResumeState(ctx, operationID)
	require.ErrorIs(t, err, storage.ErrEngineResumeStateNotFound)

	require.ErrorContains(t, store.ApplyOperations().SaveEngineResumeState(ctx, operationID, nil), "resume state is nil")
	require.ErrorContains(t, store.ApplyOperations().SaveEngineResumeState(ctx, operationID, &storage.EngineResumeState{
		ApplyOperationID: operationID + 1,
		MigrationContext: "ctx-wrong-operation",
		Metadata:         `{"branch_name":"wrong-branch"}`,
	}), "resume state belongs to apply_operation")

	initial := &storage.EngineResumeState{
		ApplyOperationID: operationID,
		MigrationContext: "ctx-123",
		Metadata:         `{"branch_name":"branch-123","deploy_request_id":123}`,
	}
	require.NoError(t, store.ApplyOperations().SaveEngineResumeState(ctx, operationID, initial))

	retrieved, err := store.ApplyOperations().GetEngineResumeState(ctx, operationID)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, operationID, retrieved.ApplyOperationID)
	assert.Equal(t, "ctx-123", retrieved.MigrationContext)
	assert.JSONEq(t, initial.Metadata, retrieved.Metadata)

	operation, err := store.ApplyOperations().Get(ctx, operationID)
	require.NoError(t, err)
	require.NotNil(t, operation)
	assert.Equal(t, "ctx-123", operation.EngineResumeContext)
	assert.JSONEq(t, initial.Metadata, operation.EngineResumeMetadata)

	updated := &storage.EngineResumeState{
		ApplyOperationID: operationID,
		MigrationContext: "ctx-456",
		Metadata:         `{"branch_name":"branch-456","deploy_request_id":456}`,
	}
	require.NoError(t, store.ApplyOperations().SaveEngineResumeState(ctx, operationID, updated))

	retrieved, err = store.ApplyOperations().GetEngineResumeState(ctx, operationID)
	require.NoError(t, err)
	assert.Equal(t, "ctx-456", retrieved.MigrationContext)
	assert.JSONEq(t, updated.Metadata, retrieved.Metadata)
}

func TestApplyOperationStore_EngineResumeStateMissingOperation(t *testing.T) {
	clearTables(t)
	store := New(testDB)

	resumeState, err := store.ApplyOperations().GetEngineResumeState(t.Context(), 99999)
	require.ErrorIs(t, err, storage.ErrApplyOperationNotFound)
	assert.Nil(t, resumeState)

	err = store.ApplyOperations().SaveEngineResumeState(t.Context(), 99999, &storage.EngineResumeState{
		ApplyOperationID: 99999,
		MigrationContext: "ctx-missing",
		Metadata:         `{"branch_name":"missing"}`,
	})
	require.ErrorIs(t, err, storage.ErrApplyOperationNotFound)
}

func TestApplyOperationStore_GetByApplyAndDeployment(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_md_getby", 1)

	_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", Target: "payments",
	})
	require.NoError(t, err)

	got, err := store.ApplyOperations().GetByApplyAndDeployment(ctx, apply.ID, "region-a")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "region-a", got.Deployment)

	// Unknown deployment for this apply → nil, no error.
	got, err = store.ApplyOperations().GetByApplyAndDeployment(ctx, apply.ID, "region-zzz")
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestApplyOperationStore_UniqueConstraint(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_md_unique", 1)

	_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a",
	})
	require.NoError(t, err)

	// Duplicate (apply_id, deployment) → typed error.
	_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a",
	})
	require.ErrorIs(t, err, storage.ErrApplyOperationExists)

	// Different deployment for same apply → ok.
	_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-b",
	})
	require.NoError(t, err)

	// Same deployment under a *different* apply → ok.
	apply2 := createTestApplyWithStateAndEnv(t, store, lock, "apply_md_unique_other", 2, state.Apply.Completed, "staging")
	_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply2.ID, Deployment: "region-a",
	})
	require.NoError(t, err)
}

func TestApplyOperationStore_ListByApply(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_md_list", 1)

	// Empty list → empty slice, no error.
	got, err := store.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Empty(t, got)

	// Insert three children in deployment order.
	for _, dep := range []string{"region-a", "region-b", "region-c"} {
		_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: dep, Target: "payments",
		})
		require.NoError(t, err)
	}

	got, err = store.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, got, 3)

	// ListByApply is ordered by id (insertion order).
	deps := make([]string, len(got))
	for i, ad := range got {
		deps[i] = ad.Deployment
	}
	assert.Equal(t, []string{"region-a", "region-b", "region-c"}, deps)
}

func TestApplyOperationStore_ListByApply_Isolation(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply1 := createTestApply(t, store, lock, "apply_md_iso_1", 1)
	apply2 := createTestApplyWithStateAndEnv(t, store, lock, "apply_md_iso_2", 2, state.Apply.Completed, "staging")

	_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply1.ID, Deployment: "region-a",
	})
	require.NoError(t, err)
	_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply2.ID, Deployment: "region-a",
	})
	require.NoError(t, err)

	got, err := store.ApplyOperations().ListByApply(ctx, apply1.ID)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, apply1.ID, got[0].ApplyID)
}

func TestApplyOperationStore_StateTransitions(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_md_transitions", 1)

	id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a",
	})
	require.NoError(t, err)

	// pending → running: MarkStarted stamps started_at.
	require.NoError(t, store.ApplyOperations().MarkStarted(ctx, id))
	got, err := store.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, state.ApplyOperation.Running, got.State)
	require.NotNil(t, got.StartedAt, "MarkStarted must stamp started_at")
	assert.Nil(t, got.CompletedAt)

	// MarkStarted is idempotent w.r.t. started_at (uses COALESCE).
	originalStartedAt := *got.StartedAt
	require.NoError(t, store.ApplyOperations().MarkStarted(ctx, id))
	got, err = store.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, originalStartedAt, *got.StartedAt, "started_at must not move on re-MarkStarted")

	// running → completed: MarkCompleted stamps completed_at.
	require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, id))
	got, err = store.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, state.ApplyOperation.Completed, got.State)
	require.NotNil(t, got.CompletedAt, "MarkCompleted must stamp completed_at")
	assert.True(t, state.IsApplyOperationTerminal(got.State))
}

func TestApplyOperationStore_MarkFailed(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_md_failed", 1)

	id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a",
	})
	require.NoError(t, err)

	require.NoError(t, store.ApplyOperations().MarkFailed(ctx, id, "tern endpoint unreachable"))
	got, err := store.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, state.ApplyOperation.Failed, got.State)
	assert.Equal(t, "tern endpoint unreachable", got.ErrorMessage)
	require.NotNil(t, got.CompletedAt)
	assert.True(t, state.IsApplyOperationTerminal(got.State))
}

// TestApplyOperationStore_MarkTerminal verifies that non-resumable terminal
// states (cancelled, reverted) stamp completed_at, while the resumable stopped
// state mirrors via UpdateState and leaves completed_at nil — matching the
// apply-level convention since stopped work may still resume.
func TestApplyOperationStore_MarkTerminal(t *testing.T) {
	ctx := t.Context()
	store := New(testDB)

	for _, terminalState := range []string{state.ApplyOperation.Cancelled, state.ApplyOperation.Reverted} {
		t.Run(terminalState+"_stamps_completed_at", func(t *testing.T) {
			clearTables(t)
			lock := createTestLock(t, store, "testdb", "mysql", "staging")
			apply := createTestApply(t, store, lock, "apply_md_term_"+terminalState, 1)
			id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
				ApplyID: apply.ID, Deployment: "region-a",
			})
			require.NoError(t, err)

			require.NoError(t, store.ApplyOperations().MarkTerminal(ctx, id, terminalState))
			got, err := store.ApplyOperations().Get(ctx, id)
			require.NoError(t, err)
			assert.Equal(t, terminalState, got.State)
			require.NotNil(t, got.CompletedAt, "MarkTerminal must stamp completed_at for %s", terminalState)
			assert.True(t, state.IsApplyOperationTerminal(got.State))
		})
	}

	t.Run("stopped_keeps_completed_at_nil", func(t *testing.T) {
		clearTables(t)
		lock := createTestLock(t, store, "testdb", "mysql", "staging")
		apply := createTestApply(t, store, lock, "apply_md_term_stopped", 1)
		id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: "region-a",
		})
		require.NoError(t, err)

		require.NoError(t, store.ApplyOperations().UpdateState(ctx, id, state.ApplyOperation.Stopped))
		got, err := store.ApplyOperations().Get(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, state.ApplyOperation.Stopped, got.State)
		assert.Nil(t, got.CompletedAt, "stopped is resumable and must keep completed_at nil")
		assert.True(t, state.IsApplyOperationTerminal(got.State))
	})

	t.Run("idempotent_preserves_completed_at", func(t *testing.T) {
		clearTables(t)
		lock := createTestLock(t, store, "testdb", "mysql", "staging")
		apply := createTestApply(t, store, lock, "apply_md_term_idem", 1)
		id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: "region-a",
		})
		require.NoError(t, err)

		require.NoError(t, store.ApplyOperations().MarkTerminal(ctx, id, state.ApplyOperation.Cancelled))
		first, err := store.ApplyOperations().Get(ctx, id)
		require.NoError(t, err)
		require.NotNil(t, first.CompletedAt)

		require.NoError(t, store.ApplyOperations().MarkTerminal(ctx, id, state.ApplyOperation.Cancelled))
		second, err := store.ApplyOperations().Get(ctx, id)
		require.NoError(t, err)
		require.NotNil(t, second.CompletedAt)
		assert.Equal(t, first.CompletedAt.UnixNano(), second.CompletedAt.UnixNano(),
			"COALESCE should preserve original completed_at across repeat MarkTerminal calls")
	})

	t.Run("missing_row_returns_not_found", func(t *testing.T) {
		clearTables(t)
		err := store.ApplyOperations().MarkTerminal(ctx, 999999, state.ApplyOperation.Cancelled)
		require.ErrorIs(t, err, storage.ErrApplyOperationNotFound)
	})
}

func TestApplyOperationStore_UpdateState(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_md_update", 1)

	id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a",
	})
	require.NoError(t, err)

	require.NoError(t, store.ApplyOperations().UpdateState(ctx, id, state.ApplyOperation.Running))
	got, err := store.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, state.ApplyOperation.Running, got.State)

	// Updating a nonexistent row returns ErrApplyOperationNotFound.
	err = store.ApplyOperations().UpdateState(ctx, 999999, state.ApplyOperation.Completed)
	require.ErrorIs(t, err, storage.ErrApplyOperationNotFound)
}

// TestApplyOperationStore_UpdateState_Idempotent guards against the regression
// where re-applying the same state to an existing row would surface
// ErrApplyOperationNotFound because MySQL's default RowsAffected counts
// changed (not matched) rows.
func TestApplyOperationStore_UpdateState_Idempotent(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_md_update_idem", 1)

	id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a",
	})
	require.NoError(t, err)

	require.NoError(t, store.ApplyOperations().UpdateState(ctx, id, state.ApplyOperation.Running))
	// Repeat with the same state — must be a no-op, not a not-found error.
	require.NoError(t, store.ApplyOperations().UpdateState(ctx, id, state.ApplyOperation.Running))
}

func TestApplyOperationStore_MarkStarted_NotFound(t *testing.T) {
	clearTables(t)
	store := New(testDB)
	err := store.ApplyOperations().MarkStarted(t.Context(), 999999)
	require.ErrorIs(t, err, storage.ErrApplyOperationNotFound)
}

// TestApplyOperationStore_MarkStarted_Idempotent guards against the regression
// where re-issuing MarkStarted on an already-started row would surface
// ErrApplyOperationNotFound. The query uses COALESCE on started_at, so the
// repeat is a true no-op on row contents.
func TestApplyOperationStore_MarkStarted_Idempotent(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_md_started_idem", 1)

	id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a",
	})
	require.NoError(t, err)

	require.NoError(t, store.ApplyOperations().MarkStarted(ctx, id))
	first, err := store.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, first.StartedAt)

	// Repeat call must not error and must preserve the original started_at.
	require.NoError(t, store.ApplyOperations().MarkStarted(ctx, id))
	second, err := store.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, second.StartedAt)
	assert.Equal(t, first.StartedAt.UnixNano(), second.StartedAt.UnixNano(),
		"COALESCE should preserve original started_at across repeat MarkStarted calls")
}

func TestApplyOperationStore_MarkCompleted_NotFound(t *testing.T) {
	clearTables(t)
	store := New(testDB)
	err := store.ApplyOperations().MarkCompleted(t.Context(), 999999)
	require.ErrorIs(t, err, storage.ErrApplyOperationNotFound)
}

// TestApplyOperationStore_MarkCompleted_Idempotent guards against the
// regression where re-issuing MarkCompleted on an already-completed row
// within the same DATETIME second would leave every column unchanged and
// surface ErrApplyOperationNotFound. COALESCE on completed_at + the
// existence-aware checkUpdatedOrExists helper together make the repeat
// a true no-op.
func TestApplyOperationStore_MarkCompleted_Idempotent(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_completed_idem", 1)

	id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a",
	})
	require.NoError(t, err)

	require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, id))
	first, err := store.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, first.CompletedAt)

	// Repeat call must not error and must preserve the original completed_at.
	require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, id))
	second, err := store.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, second.CompletedAt)
	assert.Equal(t, first.CompletedAt.UnixNano(), second.CompletedAt.UnixNano(),
		"COALESCE should preserve original completed_at across repeat MarkCompleted calls")
}

func TestApplyOperationStore_MarkFailed_NotFound(t *testing.T) {
	clearTables(t)
	store := New(testDB)
	err := store.ApplyOperations().MarkFailed(t.Context(), 999999, "boom")
	require.ErrorIs(t, err, storage.ErrApplyOperationNotFound)
}

// TestApplyOperationStore_MarkFailed_Idempotent guards against the same
// no-op-update-within-same-second regression as MarkCompleted_Idempotent,
// for the failure path.
func TestApplyOperationStore_MarkFailed_Idempotent(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_failed_idem", 1)

	id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a",
	})
	require.NoError(t, err)

	require.NoError(t, store.ApplyOperations().MarkFailed(ctx, id, "boom"))
	first, err := store.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, first.CompletedAt)

	// Repeat call with the same error message must not error and must
	// preserve the original completed_at.
	require.NoError(t, store.ApplyOperations().MarkFailed(ctx, id, "boom"))
	second, err := store.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, second.CompletedAt)
	assert.Equal(t, first.CompletedAt.UnixNano(), second.CompletedAt.UnixNano(),
		"COALESCE should preserve original completed_at across repeat MarkFailed calls")
}

// TestApplyOperationStore_FindNextApplyOperation_ClaimsPending verifies that
// a freshly inserted pending child row is claimable: returned to the caller,
// transitioned to running, and stamped with started_at + heartbeat in one
// transaction. A second immediate claim returns nil because no other row
// needs work.
func TestApplyOperationStore_FindNextApplyOperation_ClaimsPending(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_claim_pending", 1)

	id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", Target: "payments",
	})
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, id, claimed.ID)
	assert.Equal(t, "region-a", claimed.Deployment)

	persisted, err := store.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.ApplyOperation.Running, persisted.State, "pending claim must transition to running")
	require.NotNil(t, persisted.StartedAt, "pending claim must stamp started_at")

	// No other claimable rows → second call returns nil cleanly.
	again, err := store.ApplyOperations().FindNextApplyOperation(ctx)
	require.NoError(t, err)
	assert.Nil(t, again)
}

// TestApplyOperationStore_FindNextApplyOperation_SkipsFreshRunning verifies
// that a running row whose heartbeat is fresh is *not* re-claimed: the active
// worker still owns it.
func TestApplyOperationStore_FindNextApplyOperation_SkipsFreshRunning(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_skip_fresh", 1)

	id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a",
	})
	require.NoError(t, err)
	require.NoError(t, store.ApplyOperations().MarkStarted(ctx, id))

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx)
	require.NoError(t, err)
	assert.Nil(t, claimed, "fresh running rows must not be re-claimed")
}

// TestApplyOperationStore_FindNextApplyOperation_ClaimsStaleRunning verifies
// the recovery path: an active row whose heartbeat is older than the staleness
// window is re-claimed without changing its state, and the heartbeat is
// refreshed.
func TestApplyOperationStore_FindNextApplyOperation_ClaimsStaleRunning(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_claim_stale", 1)

	id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a",
	})
	require.NoError(t, err)
	require.NoError(t, store.ApplyOperations().MarkStarted(ctx, id))

	// Backdate the heartbeat past the staleness window.
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_operations SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?
	`, id)
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, id, claimed.ID)
	assert.Equal(t, state.ApplyOperation.Running, claimed.State, "stale running row must keep its state on re-claim")

	persisted, err := store.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.ApplyOperation.Running, persisted.State)
	// Heartbeat refreshed inside the claim transaction.
	assert.WithinDuration(t, time.Now(), persisted.UpdatedAt, 5*time.Second)
}

// TestApplyOperationStore_FindNextApplyOperation_SkipsTerminal verifies that
// every terminal state listed in state.IsApplyOperationTerminal is excluded
// from the claim. ApplyOperation shares the full Apply state vocabulary, so
// the contract has to hold for completed, failed, stopped, cancelled, and
// reverted — not just the two that show up most often.
func TestApplyOperationStore_FindNextApplyOperation_SkipsTerminal(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_skip_terminal", 1)

	terminalStates := []string{
		state.ApplyOperation.Completed,
		state.ApplyOperation.Failed,
		state.ApplyOperation.Stopped,
		state.ApplyOperation.Cancelled,
		state.ApplyOperation.Reverted,
	}

	var ids []int64
	for i, terminalState := range terminalStates {
		require.True(t, state.IsApplyOperationTerminal(terminalState),
			"terminalStates[%d]=%q is not actually terminal — fix the test fixture", i, terminalState)
		id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID:    apply.ID,
			Deployment: fmt.Sprintf("region-%d", i),
			State:      terminalState,
		})
		require.NoError(t, err)
		ids = append(ids, id)
	}

	// Backdate every row so staleness can't be the reason they're skipped;
	// the only thing keeping them off the claim list must be their state.
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	_, err := testDB.ExecContext(ctx, fmt.Sprintf(`
		UPDATE apply_operations SET updated_at = NOW() - INTERVAL 1 HOUR WHERE id IN (%s)
	`, placeholders(len(ids))), args...)
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx)
	require.NoError(t, err)
	assert.Nil(t, claimed, "terminal rows must never be re-claimed (full vocabulary)")
}

// TestApplyOperationStore_FindNextApplyOperation_ClaimsStoppedWithPendingStart
// verifies stop/start parity with ApplyStore.FindNextApply: a stopped operation
// whose parent apply has a pending start request is reclaimable so the operator
// can resume it. Without this, a stopped operation would strand the apply's
// start request under the operation-claim path. The re-claim keeps the row's
// stopped state (the resume drive transitions it) and refreshes the heartbeat.
func TestApplyOperationStore_FindNextApplyOperation_ClaimsStoppedWithPendingStart(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_op_stopped_start", 1, state.Apply.Stopped, "staging")

	id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.Stopped,
	})
	require.NoError(t, err)

	_, _, err = store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator",
	})
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimed, "stopped operation with a pending start request must be reclaimable")
	assert.Equal(t, id, claimed.ID)
	assert.Equal(t, state.ApplyOperation.Stopped, claimed.State, "re-claim must keep the stopped state for the resume drive to transition")

	persisted, err := store.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.ApplyOperation.Stopped, persisted.State)
	assert.WithinDuration(t, time.Now(), persisted.UpdatedAt, 5*time.Second, "heartbeat must be refreshed on re-claim")
}

// TestApplyOperationStore_FindNextApplyOperation_SkipsStoppedWithCompletedStart
// verifies the claim predicate filters on a *pending* start request: once the
// start request is no longer pending, the stopped operation is terminal again
// and must not be re-claimed.
func TestApplyOperationStore_FindNextApplyOperation_SkipsStoppedWithCompletedStart(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_op_stopped_done", 1, state.Apply.Stopped, "staging")

	_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.Stopped,
	})
	require.NoError(t, err)

	_, _, err = store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator",
	})
	require.NoError(t, err)
	// Move the start request out of pending; the stopped row is terminal again.
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_control_requests SET status = ? WHERE apply_id = ? AND operation = ?
	`, storage.ControlRequestCompleted, apply.ID, storage.ControlOperationStart)
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx)
	require.NoError(t, err)
	assert.Nil(t, claimed, "stopped operation must not be reclaimed once its start request is no longer pending")
}

// TestApplyOperationStore_FindNextApplyOperation_ClaimsFailedRetryableWithinBudget
// verifies recovery parity with ApplyStore.FindNextApply: a failed_retryable
// operation is reclaimable while its parent apply still has recovery budget
// (attempt < max) and the failure is recent. The re-claim keeps the row's
// failed_retryable state (the resume drive transitions it) and refreshes the
// heartbeat.
func TestApplyOperationStore_FindNextApplyOperation_ClaimsFailedRetryableWithinBudget(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_op_retryable_ok", 1, state.Apply.FailedRetryable, "staging")
	// Budget remaining and a recent failure: the parent is reclaimable.
	_, err := testDB.ExecContext(ctx, `
		UPDATE applies SET attempt = ?, updated_at = NOW() WHERE id = ?
	`, maxRecoveryAttempts-1, apply.ID)
	require.NoError(t, err)

	id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.FailedRetryable,
	})
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimed, "failed_retryable operation within budget must be reclaimable")
	assert.Equal(t, id, claimed.ID)
	assert.Equal(t, state.ApplyOperation.FailedRetryable, claimed.State, "re-claim must keep failed_retryable for the resume drive to transition")

	persisted, err := store.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.ApplyOperation.FailedRetryable, persisted.State)
	assert.WithinDuration(t, time.Now(), persisted.UpdatedAt, 5*time.Second, "heartbeat must be refreshed on re-claim")
}

// TestApplyOperationStore_FindNextApplyOperation_SkipsFailedRetryableBudgetExhausted
// verifies the recovery budget is enforced: once the parent apply's attempt
// count reaches the limit, the failed_retryable operation is no longer
// reclaimable and stays quiet rather than being re-claimed forever.
func TestApplyOperationStore_FindNextApplyOperation_SkipsFailedRetryableBudgetExhausted(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_op_retryable_spent", 1, state.Apply.FailedRetryable, "staging")
	_, err := testDB.ExecContext(ctx, `
		UPDATE applies SET attempt = ?, updated_at = NOW() WHERE id = ?
	`, maxRecoveryAttempts, apply.ID)
	require.NoError(t, err)

	_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.FailedRetryable,
	})
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx)
	require.NoError(t, err)
	assert.Nil(t, claimed, "failed_retryable operation must not be reclaimed once the recovery budget is spent")
}

// TestApplyOperationStore_FindNextApplyOperation_SkipsFailedRetryableStale
// verifies the freshness window is enforced: an old failed_retryable failure is
// not reclaimed, matching ApplyStore.FindNextApply's recovery-freshness gate.
func TestApplyOperationStore_FindNextApplyOperation_SkipsFailedRetryableStale(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_op_retryable_stale", 1, state.Apply.FailedRetryable, "staging")
	// Budget remains, but the failure is older than the freshness window.
	_, err := testDB.ExecContext(ctx, `
		UPDATE applies SET attempt = 1, updated_at = NOW() - INTERVAL ? DAY WHERE id = ?
	`, retryableRecoveryFreshnessDays+1, apply.ID)
	require.NoError(t, err)

	_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.FailedRetryable,
	})
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx)
	require.NoError(t, err)
	assert.Nil(t, claimed, "failed_retryable operation must not be reclaimed once the failure is stale")
}

// TestApplyOperationStore_FindNextApplyOperation_SkipsFailedRetryableParentActivelyRetrying
// verifies the failed_retryable claim is gated on the parent apply's state, not
// just its retry budget. Once a worker claims the parent for retry it
// transitions the parent to running (an active state) and refreshes its
// heartbeat, while the child row intentionally stays failed_retryable. A peer
// must not keep re-claiming that child every poll while the retry is healthy.
func TestApplyOperationStore_FindNextApplyOperation_SkipsFailedRetryableParentActivelyRetrying(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_op_retryable_active", 1, state.Apply.Running, "staging")
	// Parent already claimed for retry: running, budget remaining, fresh heartbeat.
	_, err := testDB.ExecContext(ctx, `
		UPDATE applies SET state = ?, attempt = ?, updated_at = NOW() WHERE id = ?
	`, state.Apply.Running, maxRecoveryAttempts-1, apply.ID)
	require.NoError(t, err)

	_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.FailedRetryable,
	})
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx)
	require.NoError(t, err)
	assert.Nil(t, claimed, "a failed_retryable child must not be re-claimed while its parent is actively (and freshly) retrying")
}

// TestApplyOperationStore_FindNextApplyOperation_ClaimsFailedRetryableParentActiveStale
// verifies crash recovery: if the worker driving a retry dies, the parent apply
// is left active (running) with a stale heartbeat while the child row still says
// failed_retryable. Another worker must be able to reclaim that child to recover
// the in-flight retry, mirroring ApplyStore.FindNextApply's stale-active clause.
func TestApplyOperationStore_FindNextApplyOperation_ClaimsFailedRetryableParentActiveStale(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_op_retryable_crash", 1, state.Apply.Running, "staging")
	// Parent claimed then crashed: running, budget remaining, heartbeat stale.
	_, err := testDB.ExecContext(ctx, `
		UPDATE applies SET state = ?, attempt = ?, updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?
	`, state.Apply.Running, maxRecoveryAttempts-1, apply.ID)
	require.NoError(t, err)

	id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.FailedRetryable,
	})
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimed, "a failed_retryable child must be reclaimable when its parent retry crashed (active + stale)")
	assert.Equal(t, id, claimed.ID)
	assert.Equal(t, state.ApplyOperation.FailedRetryable, claimed.State, "re-claim must keep failed_retryable for the resume drive to transition")
}

// TestApplyOperationStore_FindNextApplyOperation_ClaimsFailedRetryableParentActiveStaleBudgetExhausted
// verifies the crash-recovery branch is not budget-gated: the retry attempt was
// already admitted and counted when the parent was claimed, so a crashed retry
// must still be recoverable even after the attempt count reaches the limit —
// matching the apply-level stale-active clause, which carries no budget check.
func TestApplyOperationStore_FindNextApplyOperation_ClaimsFailedRetryableParentActiveStaleBudgetExhausted(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_op_retryable_crash_spent", 1, state.Apply.Running, "staging")
	// Last retry admitted (attempt at max) then the worker crashed: active + stale.
	_, err := testDB.ExecContext(ctx, `
		UPDATE applies SET state = ?, attempt = ?, updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?
	`, state.Apply.Running, maxRecoveryAttempts, apply.ID)
	require.NoError(t, err)

	id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.FailedRetryable,
	})
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimed, "a crashed retry must be recoverable even with the budget spent; the attempt was already counted")
	assert.Equal(t, id, claimed.ID)
}

// TestApplyOperationStore_FindNextApplyOperation_ConcurrentClaims verifies
// the SKIP LOCKED contract on a contended row: N workers race to claim a
// single pending child row, and exactly one wins. Mirrors the apply-level
// TestApplyStore_FindNextApplyConcurrentPendingClaims.
func TestApplyOperationStore_FindNextApplyOperation_ConcurrentClaims(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_concurrent", 1)

	id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a",
	})
	require.NoError(t, err)

	const workers = 16
	stores := make([]*Storage, workers)
	for i := range workers {
		db, openErr := sql.Open("mysql", testDSNChangedRows)
		require.NoError(t, openErr)
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		t.Cleanup(func() {
			require.NoError(t, db.Close())
		})
		stores[i] = New(db)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var claimed []*storage.ApplyOperation
	var claimErrors []error

	for i := range workers {
		workerStore := stores[i]
		wg.Go(func() {
			<-start
			got, claimErr := workerStore.ApplyOperations().FindNextApplyOperation(ctx)

			mu.Lock()
			defer mu.Unlock()
			if claimErr != nil {
				claimErrors = append(claimErrors, claimErr)
				return
			}
			if got != nil {
				claimed = append(claimed, got)
			}
		})
	}

	close(start)
	wg.Wait()

	require.Empty(t, claimErrors)
	require.Len(t, claimed, 1, "only one worker should claim a single pending apply_operation")
	assert.Equal(t, "region-a", claimed[0].Deployment)
	assert.Equal(t, state.ApplyOperation.Pending, claimed[0].State, "caller sees the pre-claim state")

	persisted, err := store.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.ApplyOperation.Running, persisted.State)
}

// TestApplyOperationStore_FindNextApplyOperation_OrdersSiblings verifies that
// a multi-deployment apply is claimed one deployment at a time, in insertion
// (deployment_order) order: a later sibling stays unclaimable until every
// earlier sibling has completed. This is the sequential rollout an operator
// expects — region B never starts before region A finishes.
func TestApplyOperationStore_FindNextApplyOperation_OrdersSiblings(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_ordered", 1)

	deployments := []string{"region-a", "region-b", "region-c"}
	ids := make([]int64, len(deployments))
	for i, deployment := range deployments {
		id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: deployment,
		})
		require.NoError(t, err)
		ids[i] = id
	}

	// Each claim returns the next deployment in order; the rollout only
	// advances once the prior deployment is marked completed.
	for i, deployment := range deployments {
		claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx)
		require.NoError(t, err)
		require.NotNil(t, claimed, "deployment %q should be claimable once earlier siblings completed", deployment)
		assert.Equal(t, ids[i], claimed.ID)
		assert.Equal(t, deployment, claimed.Deployment)

		// A second claim before completing the current row yields nothing:
		// the claimed row is running (not completed), so it gates its siblings.
		blocked, err := store.ApplyOperations().FindNextApplyOperation(ctx)
		require.NoError(t, err)
		assert.Nil(t, blocked, "later siblings must wait while %q is still running", deployment)

		require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, claimed.ID))
	}

	// All deployments completed → nothing left to claim.
	done, err := store.ApplyOperations().FindNextApplyOperation(ctx)
	require.NoError(t, err)
	assert.Nil(t, done)
}

// TestApplyOperationStore_FindNextApplyOperation_HaltsOnFailedSibling verifies
// halt-on-first-failure: when an earlier deployment has failed, no later
// sibling of the same apply is claimed. The rollout stops until an operator
// intervenes rather than racing ahead past a failed region.
func TestApplyOperationStore_FindNextApplyOperation_HaltsOnFailedSibling(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_halt", 1)

	// region-a failed; region-b and region-c are pending behind it.
	failedID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.Failed,
	})
	require.NoError(t, err)
	for _, deployment := range []string{"region-b", "region-c"} {
		_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: deployment,
		})
		require.NoError(t, err)
	}

	// Backdate the failed row so staleness can't be the reason it's skipped —
	// the only thing holding the rollout is the earlier non-completed sibling.
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_operations SET updated_at = NOW() - INTERVAL 1 HOUR WHERE id = ?
	`, failedID)
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx)
	require.NoError(t, err)
	assert.Nil(t, claimed, "later siblings must not be claimed once an earlier deployment failed")
}

// TestApplyOperationStore_FindNextApplyOperation_PendingStartRequestDoesNotBypassSiblingGate
// verifies that a parent apply's pending start request does not let a later
// pending deployment jump the deployment-order gate. Start requests resume
// eligible work (e.g. a stopped operation); they must never reorder a rollout
// by claiming a pending sibling while an earlier sibling is still non-completed.
func TestApplyOperationStore_FindNextApplyOperation_PendingStartRequestDoesNotBypassSiblingGate(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_op_pending_start", 1, state.Apply.Running, "staging")

	// region-a is running (fresh, so not stale-claimable); region-b is pending
	// behind it. The earlier non-completed sibling gates the later pending one.
	runningID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.Running,
	})
	require.NoError(t, err)
	_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-b", State: state.ApplyOperation.Pending,
	})
	require.NoError(t, err)

	// A pending start request on the parent apply must not unblock region-b.
	_, _, err = store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator",
	})
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx)
	require.NoError(t, err)
	assert.Nil(t, claimed, "a pending start request must not let a later pending sibling bypass the deployment-order gate")

	// Sanity: once the earlier sibling completes, the gate opens normally —
	// confirming the nil above was the order gate, not an unrelated skip.
	require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, runningID))
	next, err := store.ApplyOperations().FindNextApplyOperation(ctx)
	require.NoError(t, err)
	require.NotNil(t, next, "region-b must be claimable once region-a completes")
	assert.Equal(t, "region-b", next.Deployment)
}

// TestApplyOperationStore_FindNextApplyOperation_IsolatesApplies verifies the
// sibling gate is scoped per apply: a blocked deployment in one apply does not
// hold back the first deployment of an unrelated apply.
func TestApplyOperationStore_FindNextApplyOperation_IsolatesApplies(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Each apply needs its own lock — only one apply may be active per target.
	lockA := createTestLock(t, store, "testdb_a", "mysql", "staging")
	lockB := createTestLock(t, store, "testdb_b", "mysql", "staging")

	// Apply A: region-a running fresh, region-b pending behind it (blocked).
	applyA := createTestApply(t, store, lockA, "apply_op_iso_a", 1)
	runningID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: applyA.ID, Deployment: "region-a",
	})
	require.NoError(t, err)
	require.NoError(t, store.ApplyOperations().MarkStarted(ctx, runningID))
	_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: applyA.ID, Deployment: "region-b",
	})
	require.NoError(t, err)

	// Apply B: its own first deployment is pending and must be claimable
	// independently of apply A's in-flight rollout.
	applyB := createTestApply(t, store, lockB, "apply_op_iso_b", 1)
	bID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: applyB.ID, Deployment: "region-a",
	})
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimed, "an unrelated apply's first deployment must still be claimable")
	assert.Equal(t, bID, claimed.ID, "the only claimable row is apply B's first deployment")
}

// TestApplyOperationStore_FindNextApplyOperation_DBError covers the error
// surface when the underlying connection is gone.
func TestApplyOperationStore_FindNextApplyOperation_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.ApplyOperations().FindNextApplyOperation(t.Context())
	require.Error(t, err)
}

// TestApplyOperationStore_Heartbeat verifies that Heartbeat moves updated_at
// forward for an existing row and is a silent no-op for an unknown id.
func TestApplyOperationStore_Heartbeat(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_heartbeat", 1)

	id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a",
	})
	require.NoError(t, err)

	// Backdate updated_at so the heartbeat refresh is observable.
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_operations SET updated_at = NOW() - INTERVAL 5 MINUTE WHERE id = ?
	`, id)
	require.NoError(t, err)

	before, err := store.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)

	require.NoError(t, store.ApplyOperations().Heartbeat(ctx, id))

	after, err := store.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)
	assert.True(t, after.UpdatedAt.After(before.UpdatedAt), "Heartbeat must move updated_at forward")
	assert.WithinDuration(t, time.Now(), after.UpdatedAt, 5*time.Second)

	// Silent no-op on unknown id (matches ApplyStore.Heartbeat semantics).
	require.NoError(t, store.ApplyOperations().Heartbeat(ctx, 999999))
}

func TestApplyOperationStore_LeaseGuardsWrites(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_lease", 1)
	otherApply := createTestApplyWithStateAndEnv(t, store, lock, "apply_op_lease_other", 2, state.Apply.Completed, "staging")
	_, err := testDB.ExecContext(ctx, `
		UPDATE applies
		SET lease_owner = ?, lease_token = ?, lease_acquired_at = NOW()
		WHERE id = ?
	`, "current-worker", "current-token", apply.ID)
	require.NoError(t, err)

	staleCtx := storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: apply.ID, Owner: "old-worker", Token: "stale-token"})
	currentCtx := storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: apply.ID, Owner: "current-worker", Token: "current-token"})

	updateID := createApplyOperationForLeaseTest(t, store, apply.ID, "region-update")
	require.ErrorIs(t, store.ApplyOperations().UpdateState(staleCtx, updateID, state.ApplyOperation.Running), storage.ErrApplyLeaseLost)
	assertApplyOperationState(t, store, updateID, state.ApplyOperation.Pending)
	require.NoError(t, store.ApplyOperations().UpdateState(currentCtx, updateID, state.ApplyOperation.Running))
	assertApplyOperationState(t, store, updateID, state.ApplyOperation.Running)

	startedID := createApplyOperationForLeaseTest(t, store, apply.ID, "region-started")
	require.ErrorIs(t, store.ApplyOperations().MarkStarted(staleCtx, startedID), storage.ErrApplyLeaseLost)
	assertApplyOperationState(t, store, startedID, state.ApplyOperation.Pending)
	require.NoError(t, store.ApplyOperations().MarkStarted(currentCtx, startedID))
	assertApplyOperationState(t, store, startedID, state.ApplyOperation.Running)

	completedID := createApplyOperationForLeaseTest(t, store, apply.ID, "region-completed")
	require.ErrorIs(t, store.ApplyOperations().MarkCompleted(staleCtx, completedID), storage.ErrApplyLeaseLost)
	assertApplyOperationState(t, store, completedID, state.ApplyOperation.Pending)
	require.NoError(t, store.ApplyOperations().MarkCompleted(currentCtx, completedID))
	assertApplyOperationState(t, store, completedID, state.ApplyOperation.Completed)

	failedID := createApplyOperationForLeaseTest(t, store, apply.ID, "region-failed")
	require.ErrorIs(t, store.ApplyOperations().MarkFailed(staleCtx, failedID, "stale failure"), storage.ErrApplyLeaseLost)
	failed, err := store.ApplyOperations().Get(ctx, failedID)
	require.NoError(t, err)
	require.NotNil(t, failed)
	assert.Equal(t, state.ApplyOperation.Pending, failed.State)
	assert.Empty(t, failed.ErrorMessage)
	require.NoError(t, store.ApplyOperations().MarkFailed(currentCtx, failedID, "current failure"))
	failed, err = store.ApplyOperations().Get(ctx, failedID)
	require.NoError(t, err)
	require.NotNil(t, failed)
	assert.Equal(t, state.ApplyOperation.Failed, failed.State)
	assert.Equal(t, "current failure", failed.ErrorMessage)

	heartbeatID := createApplyOperationForLeaseTest(t, store, apply.ID, "region-heartbeat")
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_operations SET updated_at = NOW() - INTERVAL 5 MINUTE WHERE id = ?
	`, heartbeatID)
	require.NoError(t, err)
	beforeHeartbeat, err := store.ApplyOperations().Get(ctx, heartbeatID)
	require.NoError(t, err)
	require.ErrorIs(t, store.ApplyOperations().Heartbeat(staleCtx, heartbeatID), storage.ErrApplyLeaseLost)
	afterStaleHeartbeat, err := store.ApplyOperations().Get(ctx, heartbeatID)
	require.NoError(t, err)
	assert.Equal(t, beforeHeartbeat.UpdatedAt, afterStaleHeartbeat.UpdatedAt)
	require.NoError(t, store.ApplyOperations().Heartbeat(currentCtx, heartbeatID))
	afterCurrentHeartbeat, err := store.ApplyOperations().Get(ctx, heartbeatID)
	require.NoError(t, err)
	assert.True(t, afterCurrentHeartbeat.UpdatedAt.After(beforeHeartbeat.UpdatedAt))

	resumeID := createApplyOperationForLeaseTest(t, store, apply.ID, "region-resume-state")
	require.ErrorIs(t, store.ApplyOperations().SaveEngineResumeState(staleCtx, resumeID, &storage.EngineResumeState{
		ApplyOperationID: resumeID,
		MigrationContext: "stale-context",
		Metadata:         `{"deploy_request_id":123}`,
	}), storage.ErrApplyLeaseLost)
	resumeAfterStale, err := store.ApplyOperations().Get(ctx, resumeID)
	require.NoError(t, err)
	require.NotNil(t, resumeAfterStale)
	assert.Empty(t, resumeAfterStale.EngineResumeContext)
	assert.Empty(t, resumeAfterStale.EngineResumeMetadata)
	require.NoError(t, store.ApplyOperations().SaveEngineResumeState(currentCtx, resumeID, &storage.EngineResumeState{
		ApplyOperationID: resumeID,
		MigrationContext: "current-context",
		Metadata:         `{"deploy_request_id":456}`,
	}))
	resumeAfterCurrent, err := store.ApplyOperations().Get(ctx, resumeID)
	require.NoError(t, err)
	require.NotNil(t, resumeAfterCurrent)
	assert.Equal(t, "current-context", resumeAfterCurrent.EngineResumeContext)
	assert.JSONEq(t, `{"deploy_request_id":456}`, resumeAfterCurrent.EngineResumeMetadata)

	otherID := createApplyOperationForLeaseTest(t, store, otherApply.ID, "region-other")
	require.ErrorIs(t, store.ApplyOperations().UpdateState(currentCtx, otherID, state.ApplyOperation.Running), storage.ErrApplyLeaseLost)
	assertApplyOperationState(t, store, otherID, state.ApplyOperation.Pending)

	require.ErrorIs(t, store.ApplyOperations().DeleteByApply(staleCtx, apply.ID), storage.ErrApplyLeaseLost)
	remaining, err := store.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.NotEmpty(t, remaining)
	require.NoError(t, store.ApplyOperations().DeleteByApply(currentCtx, apply.ID))
	remaining, err = store.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Empty(t, remaining)
}

func createApplyOperationForLeaseTest(t *testing.T, store *Storage, applyID int64, deployment string) int64 {
	t.Helper()
	id, err := store.ApplyOperations().Insert(t.Context(), &storage.ApplyOperation{
		ApplyID:    applyID,
		Deployment: deployment,
	})
	require.NoError(t, err)
	return id
}

func assertApplyOperationState(t *testing.T, store *Storage, id int64, expected string) {
	t.Helper()
	got, err := store.ApplyOperations().Get(t.Context(), id)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, expected, got.State)
}

func TestApplyOperationStore_Heartbeat_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	err = store.ApplyOperations().Heartbeat(t.Context(), 1)
	require.Error(t, err)
}

func TestApplyOperationStore_DeleteByApply(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply1 := createTestApply(t, store, lock, "apply_md_del_1", 1)
	apply2 := createTestApplyWithStateAndEnv(t, store, lock, "apply_md_del_2", 2, state.Apply.Completed, "staging")

	for _, dep := range []string{"region-a", "region-b"} {
		_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{ApplyID: apply1.ID, Deployment: dep})
		require.NoError(t, err)
	}
	_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{ApplyID: apply2.ID, Deployment: "region-a"})
	require.NoError(t, err)

	require.NoError(t, store.ApplyOperations().DeleteByApply(ctx, apply1.ID))

	got, err := store.ApplyOperations().ListByApply(ctx, apply1.ID)
	require.NoError(t, err)
	require.Empty(t, got)

	got, err = store.ApplyOperations().ListByApply(ctx, apply2.ID)
	require.NoError(t, err)
	require.Len(t, got, 1)

	// DeleteByApply on an unknown apply is a no-op.
	require.NoError(t, store.ApplyOperations().DeleteByApply(ctx, 999999))
}

// DB error tests — mirror the pattern used by apply_comments_test.go.

func TestApplyOperationStore_Insert_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.ApplyOperations().Insert(t.Context(), &storage.ApplyOperation{
		ApplyID: 1, Deployment: "region-a",
	})
	require.Error(t, err)
}

func TestApplyOperationStore_Get_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.ApplyOperations().Get(t.Context(), 1)
	require.Error(t, err)
}

func TestApplyOperationStore_GetByApplyAndDeployment_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.ApplyOperations().GetByApplyAndDeployment(t.Context(), 1, "region-a")
	require.Error(t, err)
}

func TestApplyOperationStore_ListByApply_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.ApplyOperations().ListByApply(t.Context(), 1)
	require.Error(t, err)
}

func TestApplyOperationStore_UpdateState_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	err = store.ApplyOperations().UpdateState(t.Context(), 1, state.ApplyOperation.Running)
	require.Error(t, err)
}

func TestApplyOperationStore_MarkStarted_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	err = store.ApplyOperations().MarkStarted(t.Context(), 1)
	require.Error(t, err)
}

func TestApplyOperationStore_MarkCompleted_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	err = store.ApplyOperations().MarkCompleted(t.Context(), 1)
	require.Error(t, err)
}

func TestApplyOperationStore_MarkFailed_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	err = store.ApplyOperations().MarkFailed(t.Context(), 1, "boom")
	require.Error(t, err)
}

func TestApplyOperationStore_DeleteByApply_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	err = store.ApplyOperations().DeleteByApply(t.Context(), 1)
	require.Error(t, err)
}
