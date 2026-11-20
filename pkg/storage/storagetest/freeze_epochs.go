package storagetest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// testFreezeEpochs covers the freeze epoch: Create bumps it, and an admission
// write whose guard holds a stale epoch is refused and records nothing.
func testFreezeEpochs(t *testing.T, h Harness) {
	t.Run("CreateBumpsTheEnvironmentsEpoch", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		now := time.Now().UTC()

		epoch, err := store.Freezes().Epoch(ctx, "Production")
		require.NoError(t, err)
		assert.Zero(t, epoch, "an environment never frozen is at epoch 0")

		createFreeze(t, store, onDemandFreeze("production", now))
		createFreeze(t, store, onDemandFreeze("PRODUCTION", now))
		epoch, err = store.Freezes().Epoch(ctx, "production")
		require.NoError(t, err)
		assert.Equal(t, int64(2), epoch, "each freeze bumps the epoch, whatever the environment's case")

		_, err = store.Freezes().Lift(ctx, createFreeze(t, store, onDemandFreeze("staging", now)).Identifier, "bob", "")
		require.NoError(t, err)
		epoch, err = store.Freezes().Epoch(ctx, "staging")
		require.NoError(t, err)
		assert.Equal(t, int64(1), epoch, "a lift does not bump the epoch")
		epoch, err = store.Freezes().Epoch(ctx, "production")
		require.NoError(t, err)
		assert.Equal(t, int64(2), epoch, "another environment's freeze leaves the epoch alone")
	})

	t.Run("CreateApplyRefusesAStaleGuard", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "epoch_db", storage.DatabaseTypeMySQL)
		guarded := guardAtCurrentEpoch(t, store, "staging")
		createFreeze(t, store, onDemandFreeze("staging", time.Now().UTC()))

		_, err := store.Applies().Create(guarded, epochTestApply(lock, "apply-epoch-stale", state.Apply.Pending))
		require.ErrorIs(t, err, storage.ErrFreezeEpochChanged)
		stored, err := store.Applies().GetByApplyIdentifier(ctx, "apply-epoch-stale")
		require.NoError(t, err)
		assert.Nil(t, stored, "a refused apply is never stored")

		_, err = store.Applies().Create(guardAtCurrentEpoch(t, store, "Staging"), epochTestApply(lock, "apply-epoch-current", state.Apply.Pending))
		require.NoError(t, err, "a guard read after the freeze admits the apply")
		other := CreateLock(t, store, "epoch_other_db", storage.DatabaseTypeMySQL)
		_, err = store.Applies().Create(ctx, epochTestApply(other, "apply-epoch-unguarded", state.Apply.Pending))
		require.NoError(t, err, "a write without a guard ran no change gate and is not checked")
	})

	t.Run("CreateWithGroupedOperationsRefusesAStaleGuard", func(t *testing.T) {
		// The API creates every apply through CreateWithGroupedOperations.
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "epoch_grouped_db", storage.DatabaseTypeMySQL)
		guarded := guardAtCurrentEpoch(t, store, "staging")
		createFreeze(t, store, onDemandFreeze("staging", time.Now().UTC()))

		refused := epochTestApply(lock, "apply-epoch-grouped", state.Apply.Pending)
		_, err := store.Applies().CreateWithGroupedOperations(guarded, refused, epochTestGroups(refused))
		require.ErrorIs(t, err, storage.ErrFreezeEpochChanged)
		stored, err := store.Applies().GetByApplyIdentifier(ctx, "apply-epoch-grouped")
		require.NoError(t, err)
		assert.Nil(t, stored, "a refused apply is never stored")
		tasks, err := store.Tasks().GetByDatabase(ctx, lock.DatabaseName)
		require.NoError(t, err)
		assert.Empty(t, tasks, "a refused apply stores no tasks")

		admitted := epochTestApply(lock, "apply-epoch-grouped-current", state.Apply.Pending)
		id, err := store.Applies().CreateWithGroupedOperations(guardAtCurrentEpoch(t, store, "staging"), admitted, epochTestGroups(admitted))
		require.NoError(t, err, "a guard read after the freeze admits the apply")
		operations, err := store.ApplyOperations().ListByApply(ctx, id)
		require.NoError(t, err)
		assert.Len(t, operations, 1)
		tasks, err = store.Tasks().GetByDatabase(ctx, lock.DatabaseName)
		require.NoError(t, err)
		assert.Len(t, tasks, 1, "only the admitted apply's task is stored")
	})

	t.Run("RequestPendingRefusesAStaleGuardForStartAndCutover", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "epoch_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "apply-epoch-control", 9701, state.Apply.WaitingForCutover, "staging")
		guarded := guardAtCurrentEpoch(t, store, "staging")
		createFreeze(t, store, onDemandFreeze("staging", time.Now().UTC()))

		for _, operation := range []storage.ControlOperation{storage.ControlOperationStart, storage.ControlOperationCutover} {
			_, _, err := store.ControlRequests().RequestPending(guarded, epochTestRequest(apply, operation))
			require.ErrorIs(t, err, storage.ErrFreezeEpochChanged, "operation %s", operation)
			pending, err := store.ControlRequests().GetPending(ctx, apply.ID, operation)
			require.NoError(t, err)
			assert.Nil(t, pending, "a refused %s is never recorded", operation)
		}

		_, _, err := store.ControlRequests().RequestPending(guarded, epochTestRequest(apply, storage.ControlOperationStop))
		require.NoError(t, err, "a stop admits nothing, so it is not checked")
		_, _, err = store.ControlRequests().RequestPending(guardAtCurrentEpoch(t, store, "staging"), epochTestRequest(apply, storage.ControlOperationCutover))
		require.NoError(t, err, "a guard read after the freeze records the cutover")
	})

	t.Run("RequestPendingRefusesAGuardWithNoEpochRow", func(t *testing.T) {
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "epoch_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "apply-epoch-norow", 9702, state.Apply.WaitingForCutover, "staging")
		guarded := storage.WithFreezeEpochGuard(t.Context(), storage.FreezeEpochGuard{Environment: "never-read", Epoch: 0})

		_, _, err := store.ControlRequests().RequestPending(guarded, epochTestRequest(apply, storage.ControlOperationCutover))
		require.ErrorIs(t, err, storage.ErrFreezeEpochChanged, "a guard not read through Epoch has no row to lock")
	})
}

// guardAtCurrentEpoch returns a context carrying environment's current
// freeze epoch, as a passed change gate does.
func guardAtCurrentEpoch(t *testing.T, store storage.Storage, environment string) context.Context {
	t.Helper()
	epoch, err := store.Freezes().Epoch(t.Context(), environment)
	require.NoError(t, err)
	return storage.WithFreezeEpochGuard(t.Context(), storage.FreezeEpochGuard{Environment: environment, Epoch: epoch})
}

func epochTestApply(lock *storage.Lock, applyID, applyState string) *storage.Apply {
	return &storage.Apply{
		ApplyIdentifier: applyID,
		LockID:          lock.ID,
		PlanID:          9700,
		Database:        lock.DatabaseName,
		DatabaseType:    lock.DatabaseType,
		Repository:      lock.Repository,
		PullRequest:     lock.PullRequest,
		Environment:     "staging",
		Engine:          storage.EngineForType(lock.DatabaseType),
		State:           applyState,
	}
}

// epochTestGroups returns one operation with one task for apply.
func epochTestGroups(apply *storage.Apply) []*storage.ApplyOperationWithTasks {
	return []*storage.ApplyOperationWithTasks{{
		Operation: &storage.ApplyOperation{Deployment: "default", OperationKey: "default", State: state.ApplyOperation.Pending},
		Tasks:     []*storage.Task{newTask(apply, "task_"+apply.ApplyIdentifier, "users", time.Now())},
	}}
}

func epochTestRequest(apply *storage.Apply, operation storage.ControlOperation) *storage.ApplyControlRequest {
	return &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   operation,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:bob",
		Metadata:    []byte(`{}`),
	}
}
