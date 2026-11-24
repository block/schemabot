//go:build integration

package sqlstore

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Event metadata captures the configured application label and completion time.
func TestMySQLApplyEventMetadata(t *testing.T) {
	testApplyEventMetadata(t, func(t *testing.T, opts ...storage.Option) *Storage {
		t.Helper()
		clearTables(t)
		return NewMySQL(testDB, opts...)
	})
}

// PostgreSQL records the same immutable completion payload.
func TestPostgresApplyEventMetadata(t *testing.T) {
	_, db := testutil.StartPostgres(t, "apply_outcome_outbox")
	applyPostgresTestSchema(t, db)
	testApplyEventMetadata(t, func(t *testing.T, opts ...storage.Option) *Storage {
		t.Helper()
		clearPostgresTables(t, db)
		return NewPostgres(db, opts...)
	})
}

func testApplyEventMetadata(t *testing.T, newStore func(*testing.T, ...storage.Option) *Storage) {
	t.Helper()
	store := newStore(t, storage.WithChangeEventApp(func(database string) string {
		assert.Equal(t, "outcome_db", database)
		return "checkout"
	}))
	lock := createTestLock(t, store, "outcome_db", storage.DatabaseTypeMySQL)
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_example", 1, state.Apply.Running, "production")
	before := time.Now()
	swapped, err := store.Applies().UpdateDerivedState(t.Context(), apply.ID, state.Apply.Running, state.Apply.Completed, "", nil, nil)
	require.NoError(t, err)
	require.True(t, swapped)
	events, err := store.OutboxEvents().ListUnpublished(t.Context(), 0, 10)
	require.NoError(t, err)
	require.Len(t, events, 1)
	var event storage.ApplyFinishedEvent
	require.NoError(t, json.Unmarshal(events[0].Payload, &event))
	assert.Equal(t, "checkout", event.App)
	assert.Equal(t, state.Apply.Completed, event.Outcome)
	assert.GreaterOrEqual(t, event.OccurredAtNanos, before.UnixNano())
	assert.LessOrEqual(t, event.OccurredAtNanos, time.Now().UnixNano())
}
