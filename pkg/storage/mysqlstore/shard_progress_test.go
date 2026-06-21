//go:build integration

package mysqlstore

import (
	"context"
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

	// Insert opA's shards out of order — across two namespaces, and two tables
	// within the `resolute` namespace — so the read exercises the full
	// (namespace, table_name, shard) ordering, including table_name within a namespace.
	for _, sp := range []*storage.ShardProgress{
		{ApplyOperationID: opA, Namespace: "resolute", TableName: "users", Shard: "80-", State: "running"},
		{ApplyOperationID: opA, Namespace: "resolute", TableName: "users", Shard: "-80", State: "running"},
		{ApplyOperationID: opA, Namespace: "resolute", TableName: "orders", Shard: "-80", State: "running"},
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
	require.Len(t, got, 4, "only opA's shards")
	gotKeys := make([][3]string, len(got))
	for i, sp := range got {
		gotKeys[i] = [3]string{sp.Namespace, sp.TableName, sp.Shard}
	}
	assert.Equal(t, [][3]string{
		{"reporting", "events", "-"},  // namespace first
		{"resolute", "orders", "-80"}, // then table_name within the namespace (orders < users)
		{"resolute", "users", "-80"},  // then shard within the table
		{"resolute", "users", "80-"},
	}, gotKeys, "ordered by (namespace, table_name, shard)")

	gotB, err := store.ShardProgress().GetByApplyOperationID(ctx, opB)
	require.NoError(t, err)
	assert.Len(t, gotB, 1, "opB's shards are isolated from opA")

	gotEmpty, err := store.ShardProgress().GetByApplyOperationID(ctx, int64(9999))
	require.NoError(t, err)
	assert.Empty(t, gotEmpty, "an operation with no shards returns no rows and no error")
}

// When the caller holds an operation lease, Upsert is scoped to it so a displaced
// driver that lost the lease fails closed instead of overwriting the read-model
// with stale progress — the same guard tasks.Update enforces.
func TestShardProgressStore_OperationLeaseGuardsUpsert(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_sp_oplease", 1)
	opID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", Target: "payments",
	})
	require.NoError(t, err)
	stampOperationLease(t, opID, "driver", "op-token")

	opCtx := func(token string) context.Context {
		return storage.WithOperationLease(ctx, storage.OperationLease{
			ApplyID: apply.ID, OperationID: opID, Owner: "driver", Token: token,
		})
	}

	sp := &storage.ShardProgress{
		ApplyOperationID: opID, Namespace: "resolute", TableName: "users", Shard: "-80",
		State: "running", ProgressPercent: 30,
	}

	// A displaced driver (stale operation token) fails closed and writes nothing.
	require.ErrorIs(t, store.ShardProgress().Upsert(opCtx("stale-op-token"), sp), storage.ErrApplyLeaseLost)
	got, err := store.ShardProgress().GetByApplyOperationID(ctx, opID)
	require.NoError(t, err)
	assert.Empty(t, got, "a lost lease must not write shard progress")

	// The lease holder writes successfully.
	require.NoError(t, store.ShardProgress().Upsert(opCtx("op-token"), sp))
	got, err = store.ShardProgress().GetByApplyOperationID(ctx, opID)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "running", got[0].State)
	assert.Equal(t, 30, got[0].ProgressPercent)

	// A later stale write must not overwrite the lease holder's row.
	sp.State = "failed"
	require.ErrorIs(t, store.ShardProgress().Upsert(opCtx("stale-op-token"), sp), storage.ErrApplyLeaseLost)
	got, err = store.ShardProgress().GetByApplyOperationID(ctx, opID)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "running", got[0].State, "stale driver must not overwrite the read-model")
}
