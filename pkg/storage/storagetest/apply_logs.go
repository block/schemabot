package storagetest

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// TestApplyLogs runs the behavioral parity suite for storage.ApplyLogStore.
// The ordering assertions exercise the interface's normative contract:
// created_at ascending with ties broken by ascending id, so rapid same-
// timestamp appends always read back in insertion order.
func TestApplyLogs(t *testing.T, h Harness) {
	// GetByApply verifies the full audit read: every entry for the apply, in
	// insertion order per the ordering contract, and an unknown apply
	// returns no entries.
	t.Run("GetByApply", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "all_logs_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "apply_all_logs", 600, state.Apply.Running, "staging")

		const seeded = 4
		for i := 1; i <= seeded; i++ {
			require.NoError(t, store.ApplyLogs().Append(ctx, &storage.ApplyLog{
				ApplyID:   apply.ID,
				Level:     storage.LogLevelInfo,
				EventType: storage.LogEventInfo,
				Source:    storage.LogSourceSchemaBot,
				Message:   fmt.Sprintf("entry %d", i),
			}))
		}

		all, err := store.ApplyLogs().GetByApply(ctx, apply.ID)
		require.NoError(t, err)
		require.Len(t, all, seeded)
		for i, entry := range all {
			assert.Equal(t, fmt.Sprintf("entry %d", i+1), entry.Message)
		}

		none, err := store.ApplyLogs().GetByApply(ctx, apply.ID+1000)
		require.NoError(t, err)
		assert.Empty(t, none)
	})

	// GetRecentByApply verifies the bounded tail read used by the
	// failed-summary comment: only the newest limit entries are returned, in
	// insertion order per the ordering contract.
	t.Run("GetRecentByApply", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "recent_logs_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "apply_recent_logs", 601, state.Apply.Running, "staging")

		const seeded = 5
		for i := 1; i <= seeded; i++ {
			require.NoError(t, store.ApplyLogs().Append(ctx, &storage.ApplyLog{
				ApplyID:   apply.ID,
				Level:     storage.LogLevelInfo,
				EventType: storage.LogEventInfo,
				Source:    storage.LogSourceSchemaBot,
				Message:   fmt.Sprintf("entry %d", i),
			}))
		}

		recent, err := store.ApplyLogs().GetRecentByApply(ctx, apply.ID, 3)
		require.NoError(t, err)
		require.Len(t, recent, 3)
		assert.Equal(t, "entry 3", recent[0].Message)
		assert.Equal(t, "entry 4", recent[1].Message)
		assert.Equal(t, "entry 5", recent[2].Message)

		all, err := store.ApplyLogs().GetRecentByApply(ctx, apply.ID, seeded*2)
		require.NoError(t, err)
		require.Len(t, all, seeded)
		for i, entry := range all {
			assert.Equal(t, fmt.Sprintf("entry %d", i+1), entry.Message)
		}

		none, err := store.ApplyLogs().GetRecentByApply(ctx, apply.ID+1000, 3)
		require.NoError(t, err)
		assert.Empty(t, none)
	})

	// List_ReturnsNewestWindow verifies the bounded read behind the logs API:
	// when an apply has more entries than the requested limit, the window is
	// the newest limit entries in chronological order — an operator tailing a
	// busy apply sees the latest activity, not a stale head. Insertion order
	// within the window holds per the ordering contract.
	t.Run("List_ReturnsNewestWindow", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "list_logs_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "apply_list_logs", 602, state.Apply.Running, "staging")

		const seeded = 6
		for i := 1; i <= seeded; i++ {
			level := storage.LogLevelInfo
			if i%2 == 0 {
				level = storage.LogLevelWarn
			}
			require.NoError(t, store.ApplyLogs().Append(ctx, &storage.ApplyLog{
				ApplyID:   apply.ID,
				Level:     level,
				EventType: storage.LogEventInfo,
				Source:    storage.LogSourceSchemaBot,
				Message:   fmt.Sprintf("entry %d", i),
			}))
		}

		window, err := store.ApplyLogs().List(ctx, storage.ApplyLogFilter{ApplyID: apply.ID, Limit: 3})
		require.NoError(t, err)
		require.Len(t, window, 3)
		assert.Equal(t, "entry 4", window[0].Message)
		assert.Equal(t, "entry 5", window[1].Message)
		assert.Equal(t, "entry 6", window[2].Message)

		all, err := store.ApplyLogs().List(ctx, storage.ApplyLogFilter{ApplyID: apply.ID, Limit: seeded * 2})
		require.NoError(t, err)
		require.Len(t, all, seeded)
		for i, entry := range all {
			assert.Equal(t, fmt.Sprintf("entry %d", i+1), entry.Message)
		}

		warns, err := store.ApplyLogs().List(ctx, storage.ApplyLogFilter{ApplyID: apply.ID, Level: storage.LogLevelWarn, Limit: 2})
		require.NoError(t, err)
		require.Len(t, warns, 2)
		assert.Equal(t, "entry 4", warns[0].Message)
		assert.Equal(t, "entry 6", warns[1].Message)
	})

	t.Run("Append_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		err := store.ApplyLogs().Append(t.Context(), &storage.ApplyLog{
			ApplyID:   1,
			Level:     storage.LogLevelInfo,
			EventType: storage.LogEventInfo,
			Source:    storage.LogSourceSchemaBot,
			Message:   "entry",
		})
		require.Error(t, err)
	})

	t.Run("GetByApply_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.ApplyLogs().GetByApply(t.Context(), 1)
		require.Error(t, err)
	})

	t.Run("GetRecentByApply_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.ApplyLogs().GetRecentByApply(t.Context(), 1, 3)
		require.Error(t, err)
	})

	t.Run("List_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.ApplyLogs().List(t.Context(), storage.ApplyLogFilter{ApplyID: 1, Limit: 3})
		require.Error(t, err)
	})
}
