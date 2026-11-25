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

// TestOutboxEvents verifies that completion writes persist notification intent,
// losing writers leave none, and publication removes intent from the pending set.
func TestOutboxEvents(t *testing.T, h Harness) {
	outcome := state.Apply.Completed
	for _, derived := range []bool{false, true} {
		name := "Direct"
		if derived {
			name = "Derived"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			store := h.NewStorage(t)
			highWater, err := store.OutboxEvents().MaxID(ctx)
			require.NoError(t, err)
			require.Zero(t, highWater)
			apply := createOutboxApply(t, store, state.Apply.Running)
			completedAt := time.Now().UTC().Truncate(time.Second)
			if derived {
				swapped, err := store.Applies().UpdateDerivedState(ctx, apply.ID, state.Apply.Pending, outcome, "", nil, &completedAt)
				require.NoError(t, err)
				require.False(t, swapped)
				events, err := store.OutboxEvents().ListUnpublished(ctx, 0, 10)
				require.NoError(t, err)
				require.Empty(t, events)
				swapped, err = store.Applies().UpdateDerivedState(ctx, apply.ID, state.Apply.Running, outcome, "", nil, &completedAt)
				require.NoError(t, err)
				require.True(t, swapped)
			} else {
				apply.State = outcome
				apply.CompletedAt = &completedAt
				require.NoError(t, store.Applies().Update(ctx, apply))
			}
			events, err := store.OutboxEvents().ListUnpublished(ctx, 0, 10)
			require.NoError(t, err)
			require.Len(t, events, 1)
			highWater, err = store.OutboxEvents().MaxID(ctx)
			require.NoError(t, err)
			assert.Equal(t, events[0].ID, highWater)
			assert.Equal(t, apply.ID, events[0].ApplyID)
			assert.Equal(t, "apply.finished", events[0].EventType)
			var message storage.ApplyFinishedEvent
			require.NoError(t, json.Unmarshal(events[0].Payload, &message))
			assert.Equal(t, apply.ApplyIdentifier, message.ID)
			assert.Len(t, message.EventID, 36)
			assert.Equal(t, "apply.finished", message.Type)
			assert.Equal(t, outcome, message.Outcome)
			assert.Equal(t, "outbox_db", message.App)
			assert.WithinDuration(t, time.Now(), time.Unix(0, message.OccurredAtNanos), 5*time.Second)
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
			apply.State = outcome
			later := completedAt.Add(time.Minute)
			apply.CompletedAt = &later
			require.NoError(t, store.Applies().Update(ctx, apply))
			swapped, err := store.Applies().UpdateDerivedState(ctx, apply.ID, outcome, outcome, "", nil, &later)
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

	t.Run("Quarantine", func(t *testing.T) {
		store := h.NewStorage(t)
		apply := createOutboxApply(t, store, state.Apply.Running)
		apply.State = state.Apply.Completed
		require.NoError(t, store.Applies().Update(t.Context(), apply))
		events, err := store.OutboxEvents().ListUnpublished(t.Context(), 0, 10)
		require.NoError(t, err)
		require.Len(t, events, 1)
		event := events[0]
		require.NoError(t, store.OutboxEvents().Quarantine(t.Context(), event.ID))
		require.NoError(t, store.OutboxEvents().Quarantine(t.Context(), event.ID))
		events, err = store.OutboxEvents().ListUnpublished(t.Context(), 0, 10)
		require.NoError(t, err)
		assert.Empty(t, events)
		oldest, err := store.OutboxEvents().OldestUnpublishedAt(t.Context())
		require.NoError(t, err)
		require.NotNil(t, oldest)
		assert.WithinDuration(t, event.CreatedAt, *oldest, time.Second)
		stored, err := store.Applies().Get(t.Context(), apply.ID)
		require.NoError(t, err)
		assert.Equal(t, state.Apply.Completed, stored.State)
		logs, err := store.ApplyLogs().GetByApply(t.Context(), apply.ID)
		require.NoError(t, err)
		require.Len(t, logs, 1)
		assert.Equal(t, storage.LogLevelError, logs[0].Level)
		assert.Equal(t, storage.LogEventError, logs[0].EventType)
		assert.Equal(t, "Completion notification quarantined after a panic. Delivery is held; no release operation is available.", logs[0].Message)
		var metadata struct {
			OutboxID int64 `json:"outbox_id"`
		}
		require.NoError(t, json.Unmarshal(logs[0].Metadata, &metadata))
		assert.Equal(t, event.ID, metadata.OutboxID)
	})

	t.Run("PublishedQuarantine", func(t *testing.T) {
		store := h.NewStorage(t)
		apply := createOutboxApply(t, store, state.Apply.Running)
		apply.State = state.Apply.Completed
		require.NoError(t, store.Applies().Update(t.Context(), apply))
		events, err := store.OutboxEvents().ListUnpublished(t.Context(), 0, 10)
		require.NoError(t, err)
		require.Len(t, events, 1)
		require.NoError(t, store.OutboxEvents().MarkPublished(t.Context(), events[0].ID))
		require.NoError(t, store.OutboxEvents().Quarantine(t.Context(), events[0].ID))
		logs, err := store.ApplyLogs().GetByApply(t.Context(), apply.ID)
		require.NoError(t, err)
		assert.Empty(t, logs, "a prefetched copy cannot quarantine an already published notification")
		oldest, err := store.OutboxEvents().OldestUnpublishedAt(t.Context())
		require.NoError(t, err)
		assert.Nil(t, oldest)
	})

	t.Run("Exclusions", func(t *testing.T) {
		t.Run("HistoricalOutcomeRefresh", func(t *testing.T) {
			store := h.NewStorage(t)
			apply := createOutboxApply(t, store, outcome)
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
			_, err = store.Applies().UpdateDerivedState(ctx, apply.ID, state.Apply.Running, outcome, "", nil, nil)
			require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
			events, err := store.OutboxEvents().ListUnpublished(t.Context(), 0, 10)
			require.NoError(t, err)
			assert.Empty(t, events)
		})

		t.Run("StaleApplyLease", func(t *testing.T) {
			store := h.NewStorage(t)
			apply := createOutboxApply(t, store, state.Apply.Running)
			apply.State = outcome
			ctx := storage.WithApplyLease(t.Context(), storage.ApplyLease{ApplyID: apply.ID, Token: "stale-token", Owner: "old-driver"})
			require.ErrorIs(t, store.Applies().Update(ctx, apply), storage.ErrApplyLeaseLost)
			_, err := store.Applies().UpdateDerivedState(ctx, apply.ID, state.Apply.Running, outcome, "", nil, nil)
			require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
			events, err := store.OutboxEvents().ListUnpublished(t.Context(), 0, 10)
			require.NoError(t, err)
			assert.Empty(t, events)
		})
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
