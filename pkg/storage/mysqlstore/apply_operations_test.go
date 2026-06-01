//go:build integration

package mysqlstore

import (
	"database/sql"
	"testing"

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
