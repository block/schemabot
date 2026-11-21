package storagetest

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOutboxEvents verifies that completing writes persist notification intent,
// losing writers leave none, and publication removes intent from the pending set.
func TestOutboxEvents(t *testing.T, h Harness) {
	for _, derived := range []bool{false, true} {
		name := "Direct"
		if derived {
			name = "Derived"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			store := h.NewStorage(t)
			apply := createOutboxApply(t, store, state.Apply.Running)
			completedAt := time.Now().UTC().Truncate(time.Second)
			if derived {
				swapped, err := store.Applies().UpdateDerivedState(ctx, apply.ID, state.Apply.Pending, state.Apply.Completed, "", nil, &completedAt)
				require.NoError(t, err)
				require.False(t, swapped)
				events, err := store.OutboxEvents().ListUnpublished(ctx, 0, 10)
				require.NoError(t, err)
				require.Empty(t, events)
				swapped, err = store.Applies().UpdateDerivedState(ctx, apply.ID, state.Apply.Running, state.Apply.Completed, "", nil, &completedAt)
				require.NoError(t, err)
				require.True(t, swapped)
			} else {
				apply.State = state.Apply.Completed
				apply.CompletedAt = &completedAt
				require.NoError(t, store.Applies().Update(ctx, apply))
			}
			events, err := store.OutboxEvents().ListUnpublished(ctx, 0, 10)
			require.NoError(t, err)
			require.Len(t, events, 1)
			assert.Equal(t, apply.ID, events[0].ApplyID)
			assert.Equal(t, "apply_completed", events[0].EventType)
			var message storage.ApplyCompletedEvent
			require.NoError(t, json.Unmarshal(events[0].Payload, &message))
			assert.Equal(t, apply.ApplyIdentifier, message.ID)
			assert.Equal(t, "outbox_db", message.Database)
			assert.Equal(t, storage.DatabaseTypeMySQL, message.DatabaseType)
			assert.Equal(t, "staging", message.Environment)
			assert.Equal(t, "org/repo", message.Repository)
			assert.Equal(t, 42, message.PullRequest)
			assert.WithinDuration(t, completedAt, message.CompletedAt, time.Second)
			oldest, err := store.OutboxEvents().OldestUnpublishedAt(ctx)
			require.NoError(t, err)
			require.NotNil(t, oldest)
			assert.WithinDuration(t, events[0].CreatedAt, *oldest, time.Second)

			// A same-outcome refresh preserves the committed event and its payload.
			apply.State = state.Apply.Completed
			later := completedAt.Add(time.Minute)
			apply.CompletedAt = &later
			require.NoError(t, store.Applies().Update(ctx, apply))
			swapped, err := store.Applies().UpdateDerivedState(ctx, apply.ID, state.Apply.Completed, state.Apply.Completed, "", nil, &later)
			require.NoError(t, err)
			require.True(t, swapped)
			again, err := store.OutboxEvents().ListUnpublished(ctx, 0, 10)
			require.NoError(t, err)
			require.Len(t, again, 1)
			assert.JSONEq(t, string(events[0].Payload), string(again[0].Payload))
			require.NoError(t, store.OutboxEvents().MarkPublished(ctx, events[0].ID))
			require.NoError(t, store.OutboxEvents().MarkPublished(ctx, events[0].ID))
			again, err = store.OutboxEvents().ListUnpublished(ctx, 0, 10)
			require.NoError(t, err)
			assert.Empty(t, again)
			oldest, err = store.OutboxEvents().OldestUnpublishedAt(ctx)
			require.NoError(t, err)
			assert.Nil(t, oldest)
		})
	}

	t.Run("HistoricalCompletedRefresh", func(t *testing.T) {
		store := h.NewStorage(t)
		apply := createOutboxApply(t, store, state.Apply.Completed)
		apply.ErrorMessage = "same outcome"
		require.NoError(t, store.Applies().Update(t.Context(), apply))
		events, err := store.OutboxEvents().ListUnpublished(t.Context(), 0, 10)
		require.NoError(t, err)
		assert.Empty(t, events)
	})

	t.Run("StaleOperationLease", func(t *testing.T) {
		store := h.NewStorage(t)
		apply := createOutboxApply(t, store, state.Apply.Running)
		opID, err := store.ApplyOperations().Insert(t.Context(), &storage.ApplyOperation{ApplyID: apply.ID, Deployment: "region-a", OperationKey: "schema"})
		require.NoError(t, err)
		ctx := storage.WithOperationLease(t.Context(), storage.OperationLease{ApplyID: apply.ID, OperationID: opID, Token: "stale-token", Owner: "old-driver"})
		_, err = store.Applies().UpdateDerivedState(ctx, apply.ID, state.Apply.Running, state.Apply.Completed, "", nil, nil)
		require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
		events, err := store.OutboxEvents().ListUnpublished(t.Context(), 0, 10)
		require.NoError(t, err)
		assert.Empty(t, events)
	})

	t.Run("StaleApplyLease", func(t *testing.T) {
		store := h.NewStorage(t)
		apply := createOutboxApply(t, store, state.Apply.Running)
		apply.State = state.Apply.Completed
		ctx := storage.WithApplyLease(t.Context(), storage.ApplyLease{ApplyID: apply.ID, Token: "stale-token", Owner: "old-driver"})
		require.ErrorIs(t, store.Applies().Update(ctx, apply), storage.ErrApplyLeaseLost)
		_, err := store.Applies().UpdateDerivedState(ctx, apply.ID, state.Apply.Running, state.Apply.Completed, "", nil, nil)
		require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
		events, err := store.OutboxEvents().ListUnpublished(t.Context(), 0, 10)
		require.NoError(t, err)
		assert.Empty(t, events)
	})
}

func createOutboxApply(t *testing.T, store storage.Storage, applyState string) *storage.Apply {
	t.Helper()
	lock := CreateLock(t, store, "outbox_db", storage.DatabaseTypeMySQL)
	apply := &storage.Apply{
		ApplyIdentifier: "apply_outbox", LockID: lock.ID,
		Database: "outbox_db", DatabaseType: storage.DatabaseTypeMySQL,
		Environment: "staging", Repository: "org/repo", PullRequest: 42,
		State: applyState,
	}
	id, err := store.Applies().Create(t.Context(), apply)
	require.NoError(t, err)
	apply.ID = id
	return apply
}
