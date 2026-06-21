//go:build integration

package mysqlstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// shard_progress is the per-shard display read-model: the reconciler upserts one
// row per (apply_operation, namespace, table, shard) and the renderer reads them
// back per operation. These tests prove a fresh row round-trips, re-upserting the
// same shard updates in place (no duplicate rows), reads are ordered, and one
// operation's shards never leak into another's.

func TestShardProgressStore_UpsertAndGet(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)
	const opID = int64(1001)

	sp := &storage.ShardProgress{
		ApplyOperationID: opID,
		Namespace:        "resolute",
		TableName:        "users",
		Shard:            "-80",
		State:            "running",
		ProgressPercent:  44,
		RowsCopied:       220000,
		RowsTotal:        500000,
		ETASeconds:       720,
		CutoverAttempts:  1,
		ReadyToComplete:  false,
	}
	require.NoError(t, store.ShardProgress().Upsert(ctx, sp))

	got, err := store.ShardProgress().GetByApplyOperationID(ctx, opID)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, opID, got[0].ApplyOperationID)
	assert.Equal(t, "resolute", got[0].Namespace)
	assert.Equal(t, "users", got[0].TableName)
	assert.Equal(t, "-80", got[0].Shard)
	assert.Equal(t, "running", got[0].State)
	assert.Equal(t, 44, got[0].ProgressPercent)
	assert.Equal(t, int64(220000), got[0].RowsCopied)
	assert.Equal(t, int64(500000), got[0].RowsTotal)
	assert.Equal(t, int64(720), got[0].ETASeconds)
	assert.Equal(t, 1, got[0].CutoverAttempts)
	assert.False(t, got[0].ReadyToComplete)
}

func TestShardProgressStore_UpsertIsIdempotent(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)
	const opID = int64(1001)

	sp := &storage.ShardProgress{
		ApplyOperationID: opID, Namespace: "resolute", TableName: "users", Shard: "-80",
		State: "running", ProgressPercent: 10, RowsCopied: 50000, RowsTotal: 500000,
	}
	require.NoError(t, store.ShardProgress().Upsert(ctx, sp))

	// Re-upsert the same shard key with advanced progress: updates in place.
	sp.State = "waiting_for_cutover"
	sp.ProgressPercent = 100
	sp.RowsCopied = 500000
	sp.ReadyToComplete = true
	require.NoError(t, store.ShardProgress().Upsert(ctx, sp))

	got, err := store.ShardProgress().GetByApplyOperationID(ctx, opID)
	require.NoError(t, err)
	require.Len(t, got, 1, "re-upserting the same shard must update in place, not insert a duplicate")
	assert.Equal(t, "waiting_for_cutover", got[0].State)
	assert.Equal(t, 100, got[0].ProgressPercent)
	assert.Equal(t, int64(500000), got[0].RowsCopied)
	assert.True(t, got[0].ReadyToComplete)
}

func TestShardProgressStore_OrderedAndIsolatedByOperation(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)
	const opA = int64(1001)
	const opB = int64(1002)

	// Insert opA's shards out of order; reads come back ordered (namespace, table, shard).
	for _, sp := range []*storage.ShardProgress{
		{ApplyOperationID: opA, Namespace: "resolute", TableName: "users", Shard: "80-", State: "running"},
		{ApplyOperationID: opA, Namespace: "resolute", TableName: "users", Shard: "-80", State: "running"},
		{ApplyOperationID: opA, Namespace: "reporting", TableName: "events", Shard: "-", State: "running"},
	} {
		require.NoError(t, store.ShardProgress().Upsert(ctx, sp))
	}
	// A shard under a different operation must not leak into opA's results.
	require.NoError(t, store.ShardProgress().Upsert(ctx, &storage.ShardProgress{
		ApplyOperationID: opB, Namespace: "resolute", TableName: "users", Shard: "-80", State: "running",
	}))

	got, err := store.ShardProgress().GetByApplyOperationID(ctx, opA)
	require.NoError(t, err)
	require.Len(t, got, 3, "only opA's shards")
	assert.Equal(t, []string{"reporting", "resolute", "resolute"},
		[]string{got[0].Namespace, got[1].Namespace, got[2].Namespace}, "ordered by namespace")
	assert.Equal(t, []string{"-", "-80", "80-"},
		[]string{got[0].Shard, got[1].Shard, got[2].Shard}, "then by table_name, shard")

	gotB, err := store.ShardProgress().GetByApplyOperationID(ctx, opB)
	require.NoError(t, err)
	assert.Len(t, gotB, 1, "opB's shards are isolated from opA")

	gotEmpty, err := store.ShardProgress().GetByApplyOperationID(ctx, int64(9999))
	require.NoError(t, err)
	assert.Empty(t, gotEmpty, "an operation with no shards returns no rows and no error")
}
