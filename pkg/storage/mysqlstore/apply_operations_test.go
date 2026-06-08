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
