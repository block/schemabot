//go:build integration

package mysqlstore

import (
	"fmt"
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestApplyLogStore_GetRecentByApply verifies the bounded tail read used by
// the failed-summary comment: only the newest limit entries are returned, in
// chronological order, even when entries share a created_at second (id breaks
// the tie so insertion order is preserved).
func TestApplyLogStore_GetRecentByApply(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "recent_logs_db", storage.DatabaseTypeMySQL, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_recent_logs", 601, state.Apply.Running, "staging")

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
}

// TestApplyLogStore_List_ReturnsNewestWindow verifies the bounded read behind
// the logs API: when an apply has more entries than the requested limit, the
// window is the newest limit entries in chronological order — an operator
// tailing a busy apply sees the latest activity, not a stale head. Ties on
// created_at (second precision) are broken by id so insertion order holds.
func TestApplyLogStore_List_ReturnsNewestWindow(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "list_logs_db", storage.DatabaseTypeMySQL, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_list_logs", 602, state.Apply.Running, "staging")

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
}
