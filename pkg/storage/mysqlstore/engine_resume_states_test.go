//go:build integration

package mysqlstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

func TestEngineResumeStateStore_SaveAndGet(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeVitess, "staging")
	apply := createTestApply(t, store, lock, "apply_engine_resume_state", 1)

	initial := &storage.EngineResumeState{
		ApplyID:          apply.ID,
		Engine:           storage.EnginePlanetScale,
		MigrationContext: "ctx-123",
		Metadata:         `{"branch_name":"branch-123","deploy_request_id":123}`,
	}
	require.NoError(t, store.EngineResumeStates().Save(ctx, initial))

	retrieved, err := store.EngineResumeStates().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, apply.ID, retrieved.ApplyID)
	assert.Equal(t, storage.EnginePlanetScale, retrieved.Engine)
	assert.Equal(t, "ctx-123", retrieved.MigrationContext)
	assert.JSONEq(t, initial.Metadata, retrieved.Metadata)
	assert.NotZero(t, retrieved.CreatedAt)
	assert.NotZero(t, retrieved.UpdatedAt)

	updated := &storage.EngineResumeState{
		ApplyID:          apply.ID,
		Engine:           storage.EnginePlanetScale,
		MigrationContext: "ctx-456",
		Metadata:         `{"branch_name":"branch-456","deploy_request_id":456}`,
	}
	require.NoError(t, store.EngineResumeStates().Save(ctx, updated))

	retrieved, err = store.EngineResumeStates().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	assert.Equal(t, "ctx-456", retrieved.MigrationContext)
	assert.JSONEq(t, updated.Metadata, retrieved.Metadata)
}

func TestEngineResumeStateStore_GetMissing(t *testing.T) {
	clearTables(t)
	store := New(testDB)

	resumeState, err := store.EngineResumeStates().GetByApplyID(t.Context(), 99999)

	require.ErrorIs(t, err, storage.ErrEngineResumeStateNotFound)
	assert.Nil(t, resumeState)
}
