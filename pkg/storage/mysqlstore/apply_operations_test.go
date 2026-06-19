//go:build integration

package mysqlstore

import (
	"context"
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
	assert.Equal(t, storage.CutoverPolicyRolling, got.CutoverPolicy, "an unset cutover_policy defaults to rolling")
	assert.Empty(t, got.ErrorMessage)
	assert.Nil(t, got.StartedAt)
	assert.Nil(t, got.CompletedAt)
	assert.Empty(t, got.EngineResumeContext)
	assert.Empty(t, got.EngineResumeMetadata)
	assert.NotZero(t, got.CreatedAt)
	assert.NotZero(t, got.UpdatedAt)
}

// TestApplyOperationStore_CutoverPolicyRoundTrip verifies that an explicit
// cutover_policy is persisted and read back unchanged, and that an empty policy
// falls back to rolling on insert — matching the column's NOT NULL default so a
// caller that omits the policy never silently degrades the rollout.
func TestApplyOperationStore_CutoverPolicyRoundTrip(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_cutover_policy", 1)

	barrierID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID:       apply.ID,
		Deployment:    "region-a",
		Target:        "payments",
		CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)

	defaultedOp := &storage.ApplyOperation{
		ApplyID:    apply.ID,
		Deployment: "region-b",
		Target:     "payments",
	}
	defaultedID, err := store.ApplyOperations().Insert(ctx, defaultedOp)
	require.NoError(t, err)
	assert.Equal(t, storage.CutoverPolicyRolling, defaultedOp.CutoverPolicy, "Insert normalizes an empty policy to rolling on the passed struct")

	barrier, err := store.ApplyOperations().Get(ctx, barrierID)
	require.NoError(t, err)
	require.NotNil(t, barrier)
	assert.Equal(t, storage.CutoverPolicyBarrier, barrier.CutoverPolicy, "an explicit barrier policy round-trips unchanged")

	defaulted, err := store.ApplyOperations().Get(ctx, defaultedID)
	require.NoError(t, err)
	require.NotNil(t, defaulted)
	assert.Equal(t, storage.CutoverPolicyRolling, defaulted.CutoverPolicy, "an omitted policy is stored as rolling")
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

	// ListByApply is ordered by (created_at, id); rows inserted in the same
	// second tie-break on id, preserving insertion (deployment) order.
	deps := make([]string, len(got))
	for i, ad := range got {
		deps[i] = ad.Deployment
	}
	assert.Equal(t, []string{"region-a", "region-b", "region-c"}, deps)
}

// TestApplyOperationStore_ListByApply_OrderedByCreatedAt proves ListByApply
// honours created_at before id, matching the deployment order the claim gate
// enforces. A row inserted later but stamped with an earlier created_at must
// sort ahead of its lower-id siblings, so the aggregate projection and the
// first-failed-deployment message agree with the order the rollout runs in.
func TestApplyOperationStore_ListByApply_OrderedByCreatedAt(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_md_list_order", 1)

	ids := make(map[string]int64, 3)
	for _, dep := range []string{"region-a", "region-b", "region-c"} {
		id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: dep, Target: "payments",
		})
		require.NoError(t, err)
		ids[dep] = id
	}

	// Backdate the last-inserted (highest-id) row so its created_at precedes its
	// siblings. Ordering by id alone would keep it last; ordering by created_at
	// must move it to the front.
	_, err := testDB.ExecContext(ctx,
		"UPDATE apply_operations SET created_at = '2000-01-01 00:00:00' WHERE id = ?",
		ids["region-c"])
	require.NoError(t, err)

	got, err := store.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, got, 3)

	deps := make([]string, len(got))
	for i, ad := range got {
		deps[i] = ad.Deployment
	}
	assert.Equal(t, []string{"region-c", "region-a", "region-b"}, deps,
		"created_at must take precedence over id in ListByApply ordering")
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

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, id, claimed.ID)
	assert.Equal(t, "region-a", claimed.Deployment)
	assert.Equal(t, "test-operator", claimed.LeaseOwner, "claim must record the lease owner")
	assert.NotEmpty(t, claimed.LeaseToken, "claim must rotate a lease token onto the row")
	require.NotNil(t, claimed.LeaseAcquiredAt, "claim must stamp lease_acquired_at")
	opLease := claimed.Lease()
	assert.True(t, opLease.Valid(), "claimed operation must expose a valid lease")
	assert.Equal(t, claimed.ID, opLease.OperationID)
	assert.Equal(t, apply.ID, opLease.ApplyID)

	persisted, err := store.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.ApplyOperation.Running, persisted.State, "pending claim must transition to running")
	require.NotNil(t, persisted.StartedAt, "pending claim must stamp started_at")
	assert.Equal(t, claimed.LeaseToken, persisted.LeaseToken, "rotated lease token must be persisted")
	assert.Equal(t, "test-operator", persisted.LeaseOwner)

	// No other claimable rows → second call returns nil cleanly.
	again, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
	require.NoError(t, err)
	assert.Nil(t, again)
}

// TestApplyOperationStore_FindNextApplyOperation_SkipsFreshRunning verifies
// that a running row whose heartbeat is fresh is *not* re-claimed: the active
// driver still owns it.
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

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
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

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "recovery-operator")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, id, claimed.ID)
	assert.Equal(t, state.ApplyOperation.Running, claimed.State, "stale running row must keep its state on re-claim")
	assert.Equal(t, "recovery-operator", claimed.LeaseOwner, "re-claim must rotate the lease to the new owner")
	assert.NotEmpty(t, claimed.LeaseToken, "re-claim must rotate a lease token")

	persisted, err := store.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.ApplyOperation.Running, persisted.State)
	assert.Equal(t, "recovery-operator", persisted.LeaseOwner, "rotated lease owner must be persisted")
	assert.Equal(t, claimed.LeaseToken, persisted.LeaseToken)
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

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
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

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
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

// TestApplyOperationStore_FindNextApplyOperation_ClaimsWaitingForDeployWithPendingStart
// verifies that a deferred deploy start request returns to the operator loop.
// The claim keeps the operation in waiting_for_deploy so resume can trigger the
// deferred deploy with the persisted engine context.
func TestApplyOperationStore_FindNextApplyOperation_ClaimsWaitingForDeployWithPendingStart(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeVitess, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_op_waiting_deploy_start", 1, state.Apply.WaitingForDeploy, "staging")

	id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.WaitingForDeploy,
	})
	require.NoError(t, err)
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_operations
		SET lease_owner = 'waiting-owner', lease_token = 'waiting-token', lease_acquired_at = NOW() - INTERVAL 2 MINUTE, updated_at = NOW()
		WHERE id = ?
	`, id)
	require.NoError(t, err)

	_, _, err = store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator",
	})
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
	require.NoError(t, err)
	require.NotNil(t, claimed, "waiting-for-deploy operation with a pending start request must be reclaimable")
	assert.Equal(t, id, claimed.ID)
	assert.Equal(t, state.ApplyOperation.WaitingForDeploy, claimed.State)
	assert.Equal(t, "test-operator", claimed.LeaseOwner)
	assert.NotEmpty(t, claimed.LeaseToken)

	persisted, err := store.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.ApplyOperation.WaitingForDeploy, persisted.State)
	assert.Equal(t, "test-operator", persisted.LeaseOwner)
	assert.Equal(t, claimed.LeaseToken, persisted.LeaseToken)
	assert.WithinDuration(t, time.Now(), persisted.UpdatedAt, 5*time.Second, "heartbeat must be refreshed on re-claim")

	claimedAgain, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
	require.NoError(t, err)
	assert.Nil(t, claimedAgain, "fresh processing lease should prevent another driver from taking the same deferred deploy start request")

	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_operations SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?
	`, id)
	require.NoError(t, err)

	reclaimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "recovery-operator")
	require.NoError(t, err)
	require.NotNil(t, reclaimed, "stale deferred deploy operation owner should be reclaimable")
	assert.Equal(t, id, reclaimed.ID)
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

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
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

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
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

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
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

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
	require.NoError(t, err)
	assert.Nil(t, claimed, "failed_retryable operation must not be reclaimed once the failure is stale")
}

// TestApplyOperationStore_FindNextApplyOperation_SkipsFailedRetryableParentActivelyRetrying
// verifies the failed_retryable claim is gated on the parent apply's state, not
// just its retry budget. Once a driver claims the parent for retry it
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

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
	require.NoError(t, err)
	assert.Nil(t, claimed, "a failed_retryable child must not be re-claimed while its parent is actively (and freshly) retrying")
}

// TestApplyOperationStore_FindNextApplyOperation_ClaimsFailedRetryableParentActiveStale
// verifies crash recovery: if the driver driving a retry dies, the parent apply
// is left active (running) with a stale heartbeat while the child row still says
// failed_retryable. Another driver must be able to reclaim that child to recover
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

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
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
	// Last retry admitted (attempt at max) then the driver crashed: active + stale.
	_, err := testDB.ExecContext(ctx, `
		UPDATE applies SET state = ?, attempt = ?, updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?
	`, state.Apply.Running, maxRecoveryAttempts, apply.ID)
	require.NoError(t, err)

	id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.FailedRetryable,
	})
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
	require.NoError(t, err)
	require.NotNil(t, claimed, "a crashed retry must be recoverable even with the budget spent; the attempt was already counted")
	assert.Equal(t, id, claimed.ID)
}

// TestApplyOperationStore_FindNextApplyOperation_RecoversStaleSetupPhase
// verifies crash recovery during PlanetScale engine setup under the
// operation-claim model. While a driver drives setup the operation row is
// running and the parent apply moves through setup-phase states
// (applying_branch_changes here). If that driver dies, both rows are left active
// with a stale heartbeat. A peer must be able to lease the stale operation and
// then acquire the parent apply lease via ClaimApplyByID — both halves of the
// recovery path — so setup resumes instead of stranding the apply forever.
func TestApplyOperationStore_FindNextApplyOperation_RecoversStaleSetupPhase(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeVitess, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_op_setup_crash", 1, state.Apply.ApplyingBranchChanges, "staging")
	require.Equal(t, storage.EnginePlanetScale, apply.Engine, "setup-phase states only occur for the PlanetScale engine")
	_, err := testDB.ExecContext(ctx, `
		UPDATE applies SET state = ?, updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?
	`, state.Apply.ApplyingBranchChanges, apply.ID)
	require.NoError(t, err)

	id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.Running,
	})
	require.NoError(t, err)
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_operations SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?
	`, id)
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "operator-a")
	require.NoError(t, err)
	require.NotNil(t, claimed, "a stale running operation must be reclaimable when its parent apply crashed mid-setup")
	assert.Equal(t, id, claimed.ID)
	assert.Equal(t, state.ApplyOperation.Running, claimed.State, "re-claim keeps the running state for the resume drive")

	parent, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "operator-a")
	require.NoError(t, err)
	require.NotNil(t, parent, "the stale setup-phase parent apply must be claimable so the operation can be driven")
	assert.Equal(t, state.Apply.ApplyingBranchChanges, parent.State)
	assert.Equal(t, storage.EnginePlanetScale, parent.Engine, "the reclaimed parent apply keeps its PlanetScale engine")
	assert.Equal(t, "operator-a", parent.LeaseOwner)
	assert.NotEmpty(t, parent.LeaseToken)
}

// TestApplyOperationStore_FindNextApplyOperation_ConcurrentClaims verifies
// the SKIP LOCKED contract on a contended row: N drivers race to claim a
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

	const drivers = 16
	stores := make([]*Storage, drivers)
	for i := range drivers {
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

	for i := range drivers {
		driverStore := stores[i]
		driverOwner := fmt.Sprintf("operator-%d", i)
		wg.Go(func() {
			<-start
			got, claimErr := driverStore.ApplyOperations().FindNextApplyOperation(ctx, driverOwner)

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
	require.Len(t, claimed, 1, "only one driver should claim a single pending apply_operation")
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
		claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
		require.NoError(t, err)
		require.NotNil(t, claimed, "deployment %q should be claimable once earlier siblings completed", deployment)
		assert.Equal(t, ids[i], claimed.ID)
		assert.Equal(t, deployment, claimed.Deployment)

		// A second claim before completing the current row yields nothing:
		// the claimed row is running (not completed), so it gates its siblings.
		blocked, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
		require.NoError(t, err)
		assert.Nil(t, blocked, "later siblings must wait while %q is still running", deployment)

		require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, claimed.ID))
	}

	// All deployments completed → nothing left to claim.
	done, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
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

	// region-a failed; region-b and region-c are pending behind it, all under
	// the halt policy.
	failedID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.Failed, OnFailure: storage.OnFailureHalt,
	})
	require.NoError(t, err)
	for _, deployment := range []string{"region-b", "region-c"} {
		_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: deployment, OnFailure: storage.OnFailureHalt,
		})
		require.NoError(t, err)
	}

	// The resolved policy is persisted as the enum string on each row.
	failedRow, err := store.ApplyOperations().Get(ctx, failedID)
	require.NoError(t, err)
	assert.Equal(t, storage.OnFailureHalt, failedRow.OnFailure, "the resolved policy is persisted as the on_failure enum")

	// Backdate the failed row so staleness can't be the reason it's skipped —
	// the only thing holding the rollout is the earlier non-completed sibling.
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_operations SET updated_at = NOW() - INTERVAL 1 HOUR WHERE id = ?
	`, failedID)
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
	require.NoError(t, err)
	assert.Nil(t, claimed, "later siblings must not be claimed once an earlier deployment failed")
}

// TestApplyOperationStore_FindNextApplyOperation_OnFailureContinueClaimsPastFailedSibling
// verifies that when an apply's on_failure policy is "continue", a
// terminal-failed earlier deployment no longer blocks later siblings: the
// rollout continues and the next ordered deployment is claimed instead of
// stalling at the first failure. Only terminal `failed` is exempted — the
// policy controls rollout continuation, not the apply's verdict.
func TestApplyOperationStore_FindNextApplyOperation_OnFailureContinueClaimsPastFailedSibling(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_continue", 1)

	// region-a failed; region-b and region-c are pending behind it. With
	// on_failure "continue" on every row, the failed sibling is treated as
	// settled and the rollout proceeds to region-b.
	failed, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.Failed, OnFailure: storage.OnFailureContinue,
	})
	require.NoError(t, err)
	for _, deployment := range []string{"region-b", "region-c"} {
		_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: deployment, OnFailure: storage.OnFailureContinue,
		})
		require.NoError(t, err)
	}

	// The resolved policy is persisted as the enum string on each row.
	failedRow, err := store.ApplyOperations().Get(ctx, failed)
	require.NoError(t, err)
	assert.Equal(t, storage.OnFailureContinue, failedRow.OnFailure, "the resolved policy is persisted as the on_failure enum")

	// Backdate the failed row so staleness can't be the reason it's skipped.
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_operations SET updated_at = NOW() - INTERVAL 1 HOUR WHERE id = ?
	`, failed)
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
	require.NoError(t, err)
	require.NotNil(t, claimed, "on_failure continue must let the rollout proceed past a failed sibling")
	assert.Equal(t, "region-b", claimed.Deployment, "the next ordered deployment after the failed one is claimed")
	assert.Equal(t, "test-operator", claimed.LeaseOwner, "claim must record the lease owner")

	// The pending → running transition is persisted on the row, even though the
	// returned struct reflects the pre-claim state read by the SELECT.
	stored, err := store.ApplyOperations().Get(ctx, claimed.ID)
	require.NoError(t, err)
	assert.Equal(t, state.ApplyOperation.Running, stored.State, "the claimed pending row is transitioned to running")
}

// TestApplyOperationStore_FindNextApplyOperation_OnFailureContinueStillBlocksOnRunningSibling
// verifies that on_failure "continue" only exempts terminal `failed` — an
// earlier sibling that is still in-flight (running) continues to block later
// deployments, because reordering around work that has not settled would race
// the rollout.
func TestApplyOperationStore_FindNextApplyOperation_OnFailureContinueStillBlocksOnRunningSibling(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_continue_running", 1)

	// region-a is running (fresh, so not stale-claimable); region-b is pending
	// behind it. Even with on_failure "continue", an in-flight sibling gates
	// the later pending one — only terminal failure is exempted.
	runningID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.Running, OnFailure: storage.OnFailureContinue,
	})
	require.NoError(t, err)
	_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-b", OnFailure: storage.OnFailureContinue,
	})
	require.NoError(t, err)

	// Keep region-a fresh so it is not stale-reclaimable; the only thing that
	// could surface region-b is the (absent) failure exemption.
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_operations SET updated_at = NOW() WHERE id = ?
	`, runningID)
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
	require.NoError(t, err)
	assert.Nil(t, claimed, "a running earlier sibling still blocks even with on_failure continue")
}

// TestApplyOperationStore_FindNextApplyOperation_UnrecognizedOnFailureBlocksFailedSibling
// pins the fail-closed property of the claim predicate: only the exact value
// "continue" exempts a terminal-failed earlier sibling. Any other stored value
// — here "pause", which config validation rejects today but a future change or
// a hand-written row could let reach storage — must keep the failed sibling
// blocking so the rollout never races ahead on an unrecognized policy.
func TestApplyOperationStore_FindNextApplyOperation_UnrecognizedOnFailureBlocksFailedSibling(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_unrecognized", 1)

	// region-a failed; region-b is pending behind it. The failed row carries an
	// unrecognized on_failure value, so the exemption (which keys on exact
	// "continue") does not apply and the failed sibling keeps blocking.
	failedID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.Failed, OnFailure: storage.OnFailurePause,
	})
	require.NoError(t, err)
	_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-b", OnFailure: storage.OnFailurePause,
	})
	require.NoError(t, err)

	// The unrecognized value is persisted verbatim — it is not normalized to a
	// known enum on the way into storage.
	failedRow, err := store.ApplyOperations().Get(ctx, failedID)
	require.NoError(t, err)
	assert.Equal(t, storage.OnFailurePause, failedRow.OnFailure, "an unrecognized on_failure value is stored as-is")

	// Backdate the failed row so staleness can't be the reason it's skipped —
	// the only thing holding the rollout is the earlier failed sibling.
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_operations SET updated_at = NOW() - INTERVAL 1 HOUR WHERE id = ?
	`, failedID)
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
	require.NoError(t, err)
	assert.Nil(t, claimed, "an unrecognized on_failure value must fail closed and keep the failed sibling blocking")
}

// TestApplyOperationStore_FindNextApplyOperation_BarrierClaimsPastSiblingAtCutoverBarrier
// verifies the barrier cutover policy: a later deployment may start its copy
// phase once an earlier sibling has reached the cutover barrier
// (waiting_for_cutover), rather than waiting for it to fully complete. This is
// the parallel-copy relaxation the barrier policy enables.
func TestApplyOperationStore_FindNextApplyOperation_BarrierClaimsPastSiblingAtCutoverBarrier(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_barrier_claims", 1)

	// region-a has finished copying and is parked at the cutover barrier;
	// region-b is pending behind it. Both rows carry the barrier policy.
	_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a",
		State: state.ApplyOperation.WaitingForCutover, CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)
	regionBID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-b", CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)

	// region-a stays fresh so the stale-reclaim clause can't surface it — the
	// only row that should be claimable is region-b, via the barrier relaxation.
	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
	require.NoError(t, err)
	require.NotNil(t, claimed, "barrier must let a later deployment copy once an earlier sibling reaches the cutover barrier")
	assert.Equal(t, regionBID, claimed.ID)
	assert.Equal(t, "region-b", claimed.Deployment)
}

// TestApplyOperationStore_FindNextApplyOperation_BarrierClaimsPastSiblingInRevertWindow
// verifies that revert_window — a PlanetScale post-cutover success state where
// the schema change has already been applied — is treated as past the cutover
// barrier under the barrier policy, so an earlier sibling sitting in its revert
// window does not block a later deployment from starting its copy phase.
func TestApplyOperationStore_FindNextApplyOperation_BarrierClaimsPastSiblingInRevertWindow(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_barrier_revert_window", 1)

	// region-a has cut over and is holding its post-cutover revert window;
	// region-b is pending behind it. Both rows carry the barrier policy.
	_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a",
		State: state.ApplyOperation.RevertWindow, CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)
	regionBID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-b", CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)

	// region-a stays fresh so the stale-reclaim clause can't surface it — the
	// only row that should be claimable is region-b, via the barrier relaxation.
	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
	require.NoError(t, err)
	require.NotNil(t, claimed, "barrier must let a later deployment copy once an earlier sibling reaches its post-cutover revert window")
	assert.Equal(t, regionBID, claimed.ID)
	assert.Equal(t, "region-b", claimed.Deployment)
}

// TestApplyOperationStore_FindNextApplyOperation_RollingBlocksOnSiblingAtCutoverBarrier
// verifies that the default rolling policy keeps the fully serial gate: an
// earlier sibling parked at the cutover barrier (waiting_for_cutover) still
// blocks a later pending deployment, which is only released once the earlier
// sibling completes.
func TestApplyOperationStore_FindNextApplyOperation_RollingBlocksOnSiblingAtCutoverBarrier(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_rolling_blocks", 1)

	// Default (rolling) policy: leave CutoverPolicy unset so it resolves to rolling.
	_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.WaitingForCutover,
	})
	require.NoError(t, err)
	_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-b",
	})
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
	require.NoError(t, err)
	assert.Nil(t, claimed, "rolling must keep a later deployment blocked until the earlier sibling completes")
}

// TestApplyOperationStore_FindNextApplyOperation_BarrierStillBlocksOnRunningSibling
// verifies that barrier only relaxes the gate for siblings that have reached
// the cutover barrier — an earlier sibling still copying (running) continues to
// block later deployments, because nothing past the barrier has settled yet.
func TestApplyOperationStore_FindNextApplyOperation_BarrierStillBlocksOnRunningSibling(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_barrier_running", 1)

	// region-a is still copying (running, fresh so not stale-reclaimable);
	// region-b is pending behind it. Both carry the barrier policy.
	_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a",
		State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)
	_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-b", CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
	require.NoError(t, err)
	assert.Nil(t, claimed, "barrier must still block a later deployment while an earlier sibling is still copying")
}

// TestApplyOperationStore_FindNextApplyOperation_BarrierHaltsOnFailedSibling
// verifies that barrier does not weaken halt-on-first-failure: a terminal-failed
// earlier sibling still blocks later deployments under barrier, so a failed
// region halts the rollout rather than letting later regions race ahead.
func TestApplyOperationStore_FindNextApplyOperation_BarrierHaltsOnFailedSibling(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_barrier_failed", 1)

	failedID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a",
		State: state.ApplyOperation.Failed, CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)
	_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-b", CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)

	// Backdate the failed row so staleness can't be mistaken for the reason it
	// is skipped; a failed earlier sibling must block regardless.
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_operations SET updated_at = NOW() - INTERVAL 1 HOUR WHERE id = ?
	`, failedID)
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
	require.NoError(t, err)
	assert.Nil(t, claimed, "barrier must still halt the rollout on a failed earlier deployment")
}

// TestApplyOperationStore_FindNextApplyOperation_SkipsStaleMultiOpBarrierParked
// verifies the copy claim does NOT re-lease a multi-deployment operation parked
// at the cutover barrier, even once its heartbeat goes stale: that row is
// reserved for the deployment-ordered cutover claim. Without the exemption the
// copy claim would re-drive a parked deployment and loop.
func TestApplyOperationStore_FindNextApplyOperation_SkipsStaleMultiOpBarrierParked(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_barrier_parked_skip", 1)

	parkedID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a",
		State: state.ApplyOperation.WaitingForCutover, CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)
	// A completed sibling makes this a multi-operation apply without leaving any
	// other claimable (pending) row, so the parked row is the only candidate.
	_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-b",
		State: state.ApplyOperation.Completed, CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)

	// Backdate the parked row past the staleness window so only the new
	// barrier-park exemption — not freshness — keeps it from being re-claimed.
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_operations SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?
	`, parkedID)
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
	require.NoError(t, err)
	assert.Nil(t, claimed, "the copy claim must not re-lease a parked multi-op barrier deployment")
}

// TestApplyOperationStore_FindNextApplyOperation_ClaimsStaleSingleOpBarrierParked
// verifies the exemption is scoped to multi-deployment applies: a single-op
// barrier apply (no sibling) parked at the cutover barrier is still re-claimed
// when stale, preserving manual --defer-cutover crash-recovery behaviour.
func TestApplyOperationStore_FindNextApplyOperation_ClaimsStaleSingleOpBarrierParked(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_barrier_parked_single", 1)

	parkedID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a",
		State: state.ApplyOperation.WaitingForCutover, CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)

	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_operations SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?
	`, parkedID)
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
	require.NoError(t, err)
	require.NotNil(t, claimed, "a single-op barrier apply has no sibling, so a stale parked row is still recoverable")
	assert.Equal(t, parkedID, claimed.ID)
}

// TestApplyOperationStore_FindNextApplyOperation_ClaimsStaleMultiOpRollingParked
// verifies the exemption is scoped to the barrier policy: a multi-op rolling
// deployment that is stale at waiting_for_cutover is still re-claimed by the
// copy claim, because rolling drives copy and cutover inline.
func TestApplyOperationStore_FindNextApplyOperation_ClaimsStaleMultiOpRollingParked(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_rolling_parked", 1)

	// Default (rolling) policy: leave CutoverPolicy unset so it resolves to rolling.
	parkedID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.WaitingForCutover,
	})
	require.NoError(t, err)
	_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-b", State: state.ApplyOperation.Completed,
	})
	require.NoError(t, err)

	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_operations SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?
	`, parkedID)
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
	require.NoError(t, err)
	require.NotNil(t, claimed, "rolling drives cutover inline, so a stale parked row is still recoverable")
	assert.Equal(t, parkedID, claimed.ID)
}

// --- FindNextApplyOperationCutover (OC-1): the cutover-claim predicate ---
//
// These tests pin the cutover gate, the counterpart to the copy gate above.
// It claims a row parked at waiting_for_cutover and transitions it to
// cutting_over, but only when its turn comes in deployment order. Unlike the
// copy gate's barrier relaxation, the cutover "done" set is completed-only, so
// the high-risk atomic swaps never overlap and run strictly in order.

// The cutover claim only drives the population OC-2 parks-and-releases for it:
// barrier policy, a multi-operation apply, and a parent apply not started with
// manual --defer-cutover (see shouldReleaseAtCutoverBarrier). These helpers seed
// that eligible shape so the behavioral tests below fail for the reason under
// test, not the eligibility gate.

// setTestApplyOptions overwrites an apply's stored options, e.g. to set
// defer_cutover=true for the manual-defer exclusion.
func setTestApplyOptions(t *testing.T, applyID int64, opts storage.ApplyOptions) {
	t.Helper()
	_, err := testDB.ExecContext(t.Context(), `
		UPDATE applies SET options = ? WHERE id = ?
	`, string(storage.MarshalApplyOptions(opts)), applyID)
	require.NoError(t, err)
}

// insertCompletedBarrierSibling adds a terminal (completed) barrier sibling so a
// single-candidate cutover test satisfies the multi-operation eligibility gate.
// A completed sibling is never itself claimable and, being terminal, never
// blocks an earlier-deployment ordering check.
func insertCompletedBarrierSibling(t *testing.T, store *Storage, applyID int64, deployment string) {
	t.Helper()
	_, err := store.ApplyOperations().Insert(t.Context(), &storage.ApplyOperation{
		ApplyID: applyID, Deployment: deployment,
		State: state.ApplyOperation.Completed, CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)
}

// TestApplyOperationStore_FindNextApplyOperationCutover_ClaimsEarliestParked
// verifies the happy path: a waiting_for_cutover row with no earlier sibling is
// claimed and transitioned to cutting_over, with a fresh lease rotated onto it.
func TestApplyOperationStore_FindNextApplyOperationCutover_ClaimsEarliestParked(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_cutover_claim", 1)

	id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", Target: "payments",
		State: state.ApplyOperation.WaitingForCutover, CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)
	insertCompletedBarrierSibling(t, store, apply.ID, "region-z")

	claimed, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "cutover-operator")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, id, claimed.ID)
	assert.Equal(t, state.ApplyOperation.WaitingForCutover, claimed.State, "returned row carries its pre-claim state")
	assert.Equal(t, "cutover-operator", claimed.LeaseOwner, "claim must record the lease owner")
	assert.NotEmpty(t, claimed.LeaseToken, "claim must rotate a lease token onto the row")
	require.NotNil(t, claimed.LeaseAcquiredAt, "claim must stamp lease_acquired_at")

	persisted, err := store.ApplyOperations().Get(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.ApplyOperation.CuttingOver, persisted.State, "cutover claim must transition waiting_for_cutover → cutting_over")
	assert.Equal(t, claimed.LeaseToken, persisted.LeaseToken, "rotated lease token must be persisted")
	assert.Equal(t, "cutover-operator", persisted.LeaseOwner)

	// Now in cutting_over and fresh → not re-claimable.
	again, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "cutover-operator")
	require.NoError(t, err)
	assert.Nil(t, again)
}

// TestApplyOperationStore_FindNextApplyOperationCutover_OrdersSiblings verifies
// the cutover gate runs strictly in deployment order with a completed-only
// "done" set: a later sibling is claimable only once every earlier sibling has
// reached completed, not merely reached the cutover barrier.
func TestApplyOperationStore_FindNextApplyOperationCutover_OrdersSiblings(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_cutover_ordered", 1)

	deployments := []string{"region-a", "region-b", "region-c"}
	ids := make([]int64, len(deployments))
	for i, deployment := range deployments {
		id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: deployment, State: state.ApplyOperation.WaitingForCutover,
			CutoverPolicy: storage.CutoverPolicyBarrier,
		})
		require.NoError(t, err)
		ids[i] = id
	}

	for i, deployment := range deployments {
		claimed, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "cutover-operator")
		require.NoError(t, err)
		require.NotNil(t, claimed, "cutover %q should be claimable once earlier siblings completed", deployment)
		assert.Equal(t, ids[i], claimed.ID)
		assert.Equal(t, deployment, claimed.Deployment)

		// The claimed row is now cutting_over (not completed), so it gates its
		// later siblings: a second claim before completion yields nothing.
		blocked, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "cutover-operator")
		require.NoError(t, err)
		assert.Nil(t, blocked, "later cutovers must wait while %q is still cutting over", deployment)

		require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, claimed.ID))
	}

	done, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "cutover-operator")
	require.NoError(t, err)
	assert.Nil(t, done)
}

// TestApplyOperationStore_FindNextApplyOperationCutover_BlocksOnSiblingInRevertWindow
// pins the difference from the copy gate's barrier relaxation: an earlier
// sibling that has reached its cutover barrier but is not yet completed
// (revert_window) still blocks a later cutover. The cutover "done" set is
// completed-only, so swaps never overlap.
func TestApplyOperationStore_FindNextApplyOperationCutover_BlocksOnSiblingInRevertWindow(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_cutover_revert", 1)

	// region-a is in revert_window (fresh, so not stale-recoverable); region-b
	// is parked behind it. Under the copy gate this sibling would relax the
	// barrier — under the cutover gate it keeps blocking.
	earlierID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.RevertWindow,
		CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)
	_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-b", State: state.ApplyOperation.WaitingForCutover,
		CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "cutover-operator")
	require.NoError(t, err)
	assert.Nil(t, claimed, "a later cutover must wait while an earlier sibling is in revert_window, not yet completed")

	// Sanity: once the earlier sibling completes, region-b becomes claimable —
	// confirming the nil above was the completed-only gate, not an unrelated skip.
	require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, earlierID))
	next, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "cutover-operator")
	require.NoError(t, err)
	require.NotNil(t, next, "region-b must be claimable once region-a completes")
	assert.Equal(t, "region-b", next.Deployment)
}

// TestApplyOperationStore_FindNextApplyOperationCutover_HaltsOnFailedSibling
// verifies the default fail-closed behaviour: a terminal-failed earlier sibling
// blocks a later cutover.
func TestApplyOperationStore_FindNextApplyOperationCutover_HaltsOnFailedSibling(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_cutover_halt", 1)

	failedID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a",
		State: state.ApplyOperation.Failed, OnFailure: storage.OnFailureHalt,
		CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)
	_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-b",
		State: state.ApplyOperation.WaitingForCutover, OnFailure: storage.OnFailureHalt,
		CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)

	// Backdate the failed row so staleness can't be mistaken for the reason it
	// is skipped; a failed earlier sibling must block on its state alone.
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_operations SET updated_at = NOW() - INTERVAL 1 HOUR WHERE id = ?
	`, failedID)
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "cutover-operator")
	require.NoError(t, err)
	assert.Nil(t, claimed, "a later cutover must not run once an earlier deployment failed")
}

// TestApplyOperationStore_FindNextApplyOperationCutover_OnFailureContinueClaimsPastFailedSibling
// verifies the on_failure "continue" exemption: a terminal-failed earlier
// sibling no longer blocks the cutover, matching the copy gate.
func TestApplyOperationStore_FindNextApplyOperationCutover_OnFailureContinueClaimsPastFailedSibling(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_cutover_continue", 1)

	failedID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a",
		State: state.ApplyOperation.Failed, OnFailure: storage.OnFailureContinue,
		CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)
	_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-b",
		State: state.ApplyOperation.WaitingForCutover, OnFailure: storage.OnFailureContinue,
		CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)

	// Backdate the failed row so staleness can't be the reason it is skipped.
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_operations SET updated_at = NOW() - INTERVAL 1 HOUR WHERE id = ?
	`, failedID)
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "cutover-operator")
	require.NoError(t, err)
	require.NotNil(t, claimed, "on_failure continue must let the cutover proceed past a failed sibling")
	assert.Equal(t, "region-b", claimed.Deployment)

	persisted, err := store.ApplyOperations().Get(ctx, claimed.ID)
	require.NoError(t, err)
	assert.Equal(t, state.ApplyOperation.CuttingOver, persisted.State, "the claimed parked row is transitioned to cutting_over")
}

// TestApplyOperationStore_FindNextApplyOperationCutover_PendingStopBlocks
// verifies that a pending stop control request halts remaining cutovers even
// when the deployment-order gate is otherwise open — mirroring the copy gate's
// pending-stop guard so `stop` arrests the rollout at the cutover barrier too.
func TestApplyOperationStore_FindNextApplyOperationCutover_PendingStopBlocks(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_cutover_stop", 1)

	id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.WaitingForCutover,
		CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)
	insertCompletedBarrierSibling(t, store, apply.ID, "region-z")

	_, _, err = store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator",
	})
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "cutover-operator")
	require.NoError(t, err)
	assert.Nil(t, claimed, "a pending stop request must halt the cutover")

	// Sanity: clearing the stop reopens the gate, confirming the nil was the
	// stop guard and not an unrelated skip.
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_control_requests SET status = ? WHERE apply_id = ? AND operation = ?
	`, storage.ControlRequestCompleted, apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)

	next, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "cutover-operator")
	require.NoError(t, err)
	require.NotNil(t, next, "the cutover must be claimable once the stop is cleared")
	assert.Equal(t, id, next.ID)
}

// TestApplyOperationStore_FindNextApplyOperationCutover_RecoversStaleInFlight
// verifies the recovery path: a row already mid-cutover (cutting_over or
// revert_window) whose heartbeat has gone stale is re-leased without changing
// its state — its driver died and another driver resumes it. This path carries
// no ordering gate, because the row already started its swap.
func TestApplyOperationStore_FindNextApplyOperationCutover_RecoversStaleInFlight(t *testing.T) {
	for _, inFlight := range []string{state.ApplyOperation.CuttingOver, state.ApplyOperation.RevertWindow} {
		t.Run(inFlight, func(t *testing.T) {
			clearTables(t)
			ctx := t.Context()
			store := New(testDB)

			lock := createTestLock(t, store, "testdb", "mysql", "staging")
			apply := createTestApply(t, store, lock, "apply_op_cutover_recover_"+inFlight, 1)

			id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
				ApplyID: apply.ID, Deployment: "region-a", State: inFlight,
				CutoverPolicy: storage.CutoverPolicyBarrier,
			})
			require.NoError(t, err)
			insertCompletedBarrierSibling(t, store, apply.ID, "region-z")

			// Backdate the heartbeat past the staleness window.
			_, err = testDB.ExecContext(ctx, `
				UPDATE apply_operations SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?
			`, id)
			require.NoError(t, err)

			claimed, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "recovery-operator")
			require.NoError(t, err)
			require.NotNil(t, claimed)
			assert.Equal(t, id, claimed.ID)
			assert.Equal(t, inFlight, claimed.State, "a recovered in-flight cutover keeps its state")
			assert.Equal(t, "recovery-operator", claimed.LeaseOwner, "recovery must rotate the lease to the new owner")
			assert.NotEmpty(t, claimed.LeaseToken)

			persisted, err := store.ApplyOperations().Get(ctx, id)
			require.NoError(t, err)
			assert.Equal(t, inFlight, persisted.State, "state is unchanged on recovery")
			assert.Equal(t, "recovery-operator", persisted.LeaseOwner)
			assert.WithinDuration(t, time.Now(), persisted.UpdatedAt, 5*time.Second, "heartbeat is refreshed on recovery")
		})
	}
}

// TestApplyOperationStore_FindNextApplyOperationCutover_SkipsFreshInFlight
// verifies that a row mid-cutover whose heartbeat is fresh is not re-claimed:
// the active driver still owns it.
func TestApplyOperationStore_FindNextApplyOperationCutover_SkipsFreshInFlight(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_cutover_fresh", 1)

	_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.CuttingOver,
		CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)
	insertCompletedBarrierSibling(t, store, apply.ID, "region-z")

	claimed, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "cutover-operator")
	require.NoError(t, err)
	assert.Nil(t, claimed, "a fresh in-flight cutover must not be re-claimed")
}

// TestApplyOperationStore_FindNextApplyOperationCutover_IgnoresCopyPhaseRows
// verifies the cutover gate only starts rows parked at waiting_for_cutover: a
// pending or running row (the copy gate's territory) is never claimed for
// cutover, keeping the two phases on separate claim primitives.
func TestApplyOperationStore_FindNextApplyOperationCutover_IgnoresCopyPhaseRows(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_cutover_copyphase", 1)

	for i, copyState := range []string{state.ApplyOperation.Pending, state.ApplyOperation.Running} {
		_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: fmt.Sprintf("region-%d", i), State: copyState,
			CutoverPolicy: storage.CutoverPolicyBarrier,
		})
		require.NoError(t, err)
	}

	claimed, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "cutover-operator")
	require.NoError(t, err)
	assert.Nil(t, claimed, "the cutover gate must not claim copy-phase (pending/running) rows")
}

// TestApplyOperationStore_FindNextApplyOperationCutover_RequiresOwner verifies
// the owner is required to claim a cutover.
func TestApplyOperationStore_FindNextApplyOperationCutover_RequiresOwner(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	_, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "")
	require.Error(t, err)
	assert.ErrorIs(t, err, storage.ErrApplyLeaseLost)
}

// TestApplyOperationStore_FindNextApplyOperationCutover_SkipsRollingPolicy
// verifies the eligibility gate's policy term: a multi-op apply parked at the
// cutover barrier under the rolling policy is never deployment-ordered at the
// cutover phase, so the cutover worker must not claim it.
func TestApplyOperationStore_FindNextApplyOperationCutover_SkipsRollingPolicy(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_cutover_rolling", 1)

	for _, deployment := range []string{"region-a", "region-b"} {
		_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: deployment, State: state.ApplyOperation.WaitingForCutover,
			CutoverPolicy: storage.CutoverPolicyRolling,
		})
		require.NoError(t, err)
	}

	claimed, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "cutover-operator")
	require.NoError(t, err)
	assert.Nil(t, claimed, "the cutover worker must not claim rolling-policy operations")
}

// TestApplyOperationStore_FindNextApplyOperationCutover_SkipsSingleOpBarrier
// verifies the eligibility gate's multi-op term: a single-operation barrier
// apply has no siblings to order, so it never auto-defers — its cutover stays on
// the copy/manual path and the cutover worker must not claim it.
func TestApplyOperationStore_FindNextApplyOperationCutover_SkipsSingleOpBarrier(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_cutover_singleop", 1)

	_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.WaitingForCutover,
		CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "cutover-operator")
	require.NoError(t, err)
	assert.Nil(t, claimed, "the cutover worker must not claim a single-operation barrier apply")
}

// TestApplyOperationStore_FindNextApplyOperationCutover_SkipsManualDeferCutover
// verifies the eligibility gate's manual-defer term: when the parent apply was
// started with --defer-cutover, the operator holds the claim and polls for a
// manual cutover, so the cutover worker must not steal the parked row.
func TestApplyOperationStore_FindNextApplyOperationCutover_SkipsManualDeferCutover(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_cutover_manualdefer", 1)
	setTestApplyOptions(t, apply.ID, storage.ApplyOptions{DeferCutover: true})

	_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.WaitingForCutover,
		CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)
	insertCompletedBarrierSibling(t, store, apply.ID, "region-z")

	claimed, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "cutover-operator")
	require.NoError(t, err)
	assert.Nil(t, claimed, "the cutover worker must not claim a manually deferred apply")
}

// TestApplyOperationStore_FindNextApplyOperationCutover_SkipsManualDeferRecovery
// verifies the eligibility gate also binds the stale-recovery branch: a stale
// in-flight cutover whose parent apply is manually deferred reached cutting_over
// via the manual path, so the cutover worker must not recover it.
func TestApplyOperationStore_FindNextApplyOperationCutover_SkipsManualDeferRecovery(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_cutover_manualdefer_recover", 1)
	setTestApplyOptions(t, apply.ID, storage.ApplyOptions{DeferCutover: true})

	id, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.CuttingOver,
		CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	require.NoError(t, err)
	insertCompletedBarrierSibling(t, store, apply.ID, "region-z")

	// Backdate the heartbeat past the staleness window so only the eligibility
	// gate — not freshness — can be the reason it is skipped.
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_operations SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?
	`, id)
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperationCutover(ctx, "recovery-operator")
	require.NoError(t, err)
	assert.Nil(t, claimed, "the cutover worker must not recover a manually deferred in-flight cutover")
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

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
	require.NoError(t, err)
	assert.Nil(t, claimed, "a pending start request must not let a later pending sibling bypass the deployment-order gate")

	// Sanity: once the earlier sibling completes, the gate opens normally —
	// confirming the nil above was the order gate, not an unrelated skip.
	require.NoError(t, store.ApplyOperations().MarkCompleted(ctx, runningID))
	next, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
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

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
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
	_, err = store.ApplyOperations().FindNextApplyOperation(t.Context(), "test-operator")
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
	`, "current-driver", "current-token", apply.ID)
	require.NoError(t, err)

	staleCtx := storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: apply.ID, Owner: "old-driver", Token: "stale-token"})
	currentCtx := storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: apply.ID, Owner: "current-driver", Token: "current-token"})

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

// An operation lease guards writes on the operation's own token, independent of
// the parent apply lease. A write under a stale operation token must fail closed
// and leave the row untouched, and an operation lease must be enforced even when
// the parent apply lease in context is current.
func TestApplyOperationStore_OperationLeaseGuardsWrites(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_op_oplease", 1)

	// The parent apply holds a current lease the whole time, so any write that
	// succeeds proves the operation token (not the apply token) is enforced.
	_, err := testDB.ExecContext(ctx, `
		UPDATE applies
		SET lease_owner = ?, lease_token = ?, lease_acquired_at = NOW()
		WHERE id = ?
	`, "current-driver", "apply-token", apply.ID)
	require.NoError(t, err)

	opCtx := func(id int64, token string) context.Context {
		return storage.WithOperationLease(ctx, storage.OperationLease{
			ApplyID:     apply.ID,
			OperationID: id,
			Owner:       "driver",
			Token:       token,
		})
	}

	updateID := createApplyOperationForLeaseTest(t, store, apply.ID, "op-update")
	stampOperationLease(t, updateID, "driver", "op-token")
	require.ErrorIs(t, store.ApplyOperations().UpdateState(opCtx(updateID, "stale-op-token"), updateID, state.ApplyOperation.Running), storage.ErrApplyLeaseLost)
	assertApplyOperationState(t, store, updateID, state.ApplyOperation.Pending)
	require.NoError(t, store.ApplyOperations().UpdateState(opCtx(updateID, "op-token"), updateID, state.ApplyOperation.Running))
	assertApplyOperationState(t, store, updateID, state.ApplyOperation.Running)

	startedID := createApplyOperationForLeaseTest(t, store, apply.ID, "op-started")
	stampOperationLease(t, startedID, "driver", "op-token")
	require.ErrorIs(t, store.ApplyOperations().MarkStarted(opCtx(startedID, "stale-op-token"), startedID), storage.ErrApplyLeaseLost)
	assertApplyOperationState(t, store, startedID, state.ApplyOperation.Pending)
	require.NoError(t, store.ApplyOperations().MarkStarted(opCtx(startedID, "op-token"), startedID))
	assertApplyOperationState(t, store, startedID, state.ApplyOperation.Running)

	completedID := createApplyOperationForLeaseTest(t, store, apply.ID, "op-completed")
	stampOperationLease(t, completedID, "driver", "op-token")
	require.ErrorIs(t, store.ApplyOperations().MarkCompleted(opCtx(completedID, "stale-op-token"), completedID), storage.ErrApplyLeaseLost)
	assertApplyOperationState(t, store, completedID, state.ApplyOperation.Pending)
	require.NoError(t, store.ApplyOperations().MarkCompleted(opCtx(completedID, "op-token"), completedID))
	assertApplyOperationState(t, store, completedID, state.ApplyOperation.Completed)

	failedID := createApplyOperationForLeaseTest(t, store, apply.ID, "op-failed")
	stampOperationLease(t, failedID, "driver", "op-token")
	require.ErrorIs(t, store.ApplyOperations().MarkFailed(opCtx(failedID, "stale-op-token"), failedID, "stale failure"), storage.ErrApplyLeaseLost)
	failed, err := store.ApplyOperations().Get(ctx, failedID)
	require.NoError(t, err)
	require.NotNil(t, failed)
	assert.Equal(t, state.ApplyOperation.Pending, failed.State)
	assert.Empty(t, failed.ErrorMessage)
	require.NoError(t, store.ApplyOperations().MarkFailed(opCtx(failedID, "op-token"), failedID, "current failure"))
	failed, err = store.ApplyOperations().Get(ctx, failedID)
	require.NoError(t, err)
	require.NotNil(t, failed)
	assert.Equal(t, state.ApplyOperation.Failed, failed.State)
	assert.Equal(t, "current failure", failed.ErrorMessage)

	heartbeatID := createApplyOperationForLeaseTest(t, store, apply.ID, "op-heartbeat")
	stampOperationLease(t, heartbeatID, "driver", "op-token")
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_operations SET updated_at = NOW() - INTERVAL 5 MINUTE WHERE id = ?
	`, heartbeatID)
	require.NoError(t, err)
	beforeHeartbeat, err := store.ApplyOperations().Get(ctx, heartbeatID)
	require.NoError(t, err)
	require.ErrorIs(t, store.ApplyOperations().Heartbeat(opCtx(heartbeatID, "stale-op-token"), heartbeatID), storage.ErrApplyLeaseLost)
	afterStaleHeartbeat, err := store.ApplyOperations().Get(ctx, heartbeatID)
	require.NoError(t, err)
	assert.Equal(t, beforeHeartbeat.UpdatedAt, afterStaleHeartbeat.UpdatedAt)
	require.NoError(t, store.ApplyOperations().Heartbeat(opCtx(heartbeatID, "op-token"), heartbeatID))
	afterCurrentHeartbeat, err := store.ApplyOperations().Get(ctx, heartbeatID)
	require.NoError(t, err)
	assert.True(t, afterCurrentHeartbeat.UpdatedAt.After(beforeHeartbeat.UpdatedAt))

	resumeID := createApplyOperationForLeaseTest(t, store, apply.ID, "op-resume-state")
	stampOperationLease(t, resumeID, "driver", "op-token")
	require.ErrorIs(t, store.ApplyOperations().SaveEngineResumeState(opCtx(resumeID, "stale-op-token"), resumeID, &storage.EngineResumeState{
		ApplyOperationID: resumeID,
		MigrationContext: "stale-context",
		Metadata:         `{"deploy_request_id":123}`,
	}), storage.ErrApplyLeaseLost)
	resumeAfterStale, err := store.ApplyOperations().Get(ctx, resumeID)
	require.NoError(t, err)
	require.NotNil(t, resumeAfterStale)
	assert.Empty(t, resumeAfterStale.EngineResumeContext)
	require.NoError(t, store.ApplyOperations().SaveEngineResumeState(opCtx(resumeID, "op-token"), resumeID, &storage.EngineResumeState{
		ApplyOperationID: resumeID,
		MigrationContext: "current-context",
		Metadata:         `{"deploy_request_id":456}`,
	}))
	resumeAfterCurrent, err := store.ApplyOperations().Get(ctx, resumeID)
	require.NoError(t, err)
	require.NotNil(t, resumeAfterCurrent)
	assert.Equal(t, "current-context", resumeAfterCurrent.EngineResumeContext)

	// Operation lease takes precedence: even with a current apply lease also on
	// the context, a stale operation token must fail closed.
	precedenceID := createApplyOperationForLeaseTest(t, store, apply.ID, "op-precedence")
	stampOperationLease(t, precedenceID, "driver", "op-token")
	bothCtx := storage.WithApplyLease(opCtx(precedenceID, "stale-op-token"), storage.ApplyLease{
		ApplyID: apply.ID, Owner: "current-driver", Token: "apply-token",
	})
	require.ErrorIs(t, store.ApplyOperations().UpdateState(bothCtx, precedenceID, state.ApplyOperation.Running), storage.ErrApplyLeaseLost)
	assertApplyOperationState(t, store, precedenceID, state.ApplyOperation.Pending)
}

func stampOperationLease(t *testing.T, id int64, owner, token string) {
	t.Helper()
	_, err := testDB.ExecContext(t.Context(), `
		UPDATE apply_operations
		SET lease_owner = ?, lease_token = ?, lease_acquired_at = NOW()
		WHERE id = ?
	`, owner, token, id)
	require.NoError(t, err)
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

// TestApplyOperationStore_FindNextApplyOperation_PendingStopGateHaltsSibling
// verifies that a pending stop control request halts remaining siblings under
// on_failure "continue": a pending sibling that would otherwise be claimable
// (the earlier failed sibling is continue-exempted) is not started while the
// stop is pending, so `stop` actually stops the rollout instead of letting the
// next deployment kick off.
func TestApplyOperationStore_FindNextApplyOperation_PendingStopGateHaltsSibling(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_op_stop_gate", 1, state.Apply.Running, "staging")

	// region-a failed (continue), region-b pending behind it. Without a stop
	// this is exactly the OnFailureContinueClaimsPastFailedSibling case, where
	// region-b would be claimed.
	failed, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.Failed, OnFailure: storage.OnFailureContinue,
	})
	require.NoError(t, err)
	_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-b", OnFailure: storage.OnFailureContinue,
	})
	require.NoError(t, err)
	// Backdate the failed row so staleness can't be the reason region-b is held.
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_operations SET updated_at = NOW() - INTERVAL 1 HOUR WHERE id = ?
	`, failed)
	require.NoError(t, err)

	// A pending stop request now exists for the apply.
	_, _, err = store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator",
	})
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
	require.NoError(t, err)
	assert.Nil(t, claimed, "a pending stop must halt the next sibling even under on_failure continue")
}

// TestApplyOperationStore_FindNextApplyOperation_PendingStopDoesNotBlockStaleRecovery
// verifies the stop gate covers only pending (not-yet-started) rows: a row that
// already started and went stale is recovering work it began, so it is still
// re-leasable even while a stop is pending. The in-flight drive observes the
// stop through the engine.
func TestApplyOperationStore_FindNextApplyOperation_PendingStopDoesNotBlockStaleRecovery(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_op_stop_stale", 1, state.Apply.Running, "staging")

	runningID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.Running, OnFailure: storage.OnFailureContinue,
	})
	require.NoError(t, err)
	// Make the running row stale so it is re-leasable for crash recovery.
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_operations SET updated_at = NOW() - INTERVAL 1 HOUR WHERE id = ?
	`, runningID)
	require.NoError(t, err)

	_, _, err = store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator",
	})
	require.NoError(t, err)

	claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "test-operator")
	require.NoError(t, err)
	require.NotNil(t, claimed, "a stale active row is recovering started work and is not gated by a pending stop")
	assert.Equal(t, runningID, claimed.ID)
	assert.Equal(t, state.ApplyOperation.Running, claimed.State, "re-lease keeps the running state for the resume drive")
}

// TestApplyOperationStore_MarkPendingStoppedByApply verifies the operator stop
// reconciliation primitive: it terminalizes only the still-pending operations of
// an apply to stopped, leaving running and already-terminal rows untouched, and
// keeps completed_at nil because stopped is resumable.
func TestApplyOperationStore_MarkPendingStoppedByApply(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_op_mark_stopped", 1, state.Apply.Running, "staging")

	failedID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.Failed, OnFailure: storage.OnFailureContinue,
	})
	require.NoError(t, err)
	runningID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-b", State: state.ApplyOperation.Running, OnFailure: storage.OnFailureContinue,
	})
	require.NoError(t, err)
	pendingID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-c", OnFailure: storage.OnFailureContinue,
	})
	require.NoError(t, err)

	stopped, err := store.ApplyOperations().MarkPendingStoppedByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(1), stopped, "only the single pending row is stopped")

	pending, err := store.ApplyOperations().Get(ctx, pendingID)
	require.NoError(t, err)
	assert.Equal(t, state.ApplyOperation.Stopped, pending.State, "pending row is terminalized to stopped")
	assert.Nil(t, pending.CompletedAt, "stopped is resumable, so completed_at stays nil")

	failed, err := store.ApplyOperations().Get(ctx, failedID)
	require.NoError(t, err)
	assert.Equal(t, state.ApplyOperation.Failed, failed.State, "an already-terminal row keeps its recorded result")

	running, err := store.ApplyOperations().Get(ctx, runningID)
	require.NoError(t, err)
	assert.Equal(t, state.ApplyOperation.Running, running.State, "an in-flight row is left for its own driver")

	// A second call is a no-op once nothing is pending.
	again, err := store.ApplyOperations().MarkPendingStoppedByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(0), again, "no pending rows remain to stop")
}

// Under fan-out multi-operation mode the driving driver holds only an operation
// lease, not the parent apply lease. MarkPendingStoppedByApply must still let it
// stop the apply's pending siblings, authorized by the owning operation row's
// own lease token, and must fail closed when that token is stale or names a
// different apply so a displaced driver cannot stop siblings it no longer owns.
func TestApplyOperationStore_MarkPendingStoppedByApply_OperationLease(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_op_mark_stopped_oplease", 1, state.Apply.Running, "staging")

	// The owning operation the driver holds a lease on. It is running (driving),
	// so MarkPendingStoppedByApply never touches it.
	ownerID := createApplyOperationForLeaseTest(t, store, apply.ID, "region-owner")
	require.NoError(t, store.ApplyOperations().UpdateState(ctx, ownerID, state.ApplyOperation.Running))
	stampOperationLease(t, ownerID, "driver", "op-token")

	pendingID := createApplyOperationForLeaseTest(t, store, apply.ID, "region-pending")

	opCtx := func(applyID, opID int64, token string) context.Context {
		return storage.WithOperationLease(ctx, storage.OperationLease{
			ApplyID:     applyID,
			OperationID: opID,
			Owner:       "driver",
			Token:       token,
		})
	}

	// A stale operation token while a pending sibling remains fails closed and
	// leaves the sibling pending.
	_, err := store.ApplyOperations().MarkPendingStoppedByApply(opCtx(apply.ID, ownerID, "stale-op-token"), apply.ID)
	require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
	assertApplyOperationState(t, store, pendingID, state.ApplyOperation.Pending)

	// An operation lease naming a different apply does not authorize the stop.
	_, err = store.ApplyOperations().MarkPendingStoppedByApply(opCtx(apply.ID+1, ownerID, "op-token"), apply.ID)
	require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
	assertApplyOperationState(t, store, pendingID, state.ApplyOperation.Pending)

	// The current operation token stops the pending sibling and leaves the
	// running owner untouched.
	stopped, err := store.ApplyOperations().MarkPendingStoppedByApply(opCtx(apply.ID, ownerID, "op-token"), apply.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(1), stopped)
	assertApplyOperationState(t, store, pendingID, state.ApplyOperation.Stopped)
	assertApplyOperationState(t, store, ownerID, state.ApplyOperation.Running)

	// A repeat under the current token is a clean no-op (lease still owned).
	again, err := store.ApplyOperations().MarkPendingStoppedByApply(opCtx(apply.ID, ownerID, "op-token"), apply.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(0), again)
}
