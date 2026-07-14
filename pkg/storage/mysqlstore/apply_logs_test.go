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
