//go:build integration

package tern

import (
	"database/sql"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// The operator's lease-held drive persists a table's per-shard breakdown as
// per-shard tasks, so the renderer can show per-shard state from storage. A
// read-path caller without the operation lease must not write — it only renders.
func TestWriteShardProgressPersistsPerShardTasksUnderLease(t *testing.T) {
	dsn := sharedDSN
	setupStorageSchema(t, dsn)
	t.Cleanup(func() { cleanupTasks(t, dsn) })

	ctx := t.Context()
	stor := createStorage(t, dsn)
	logger := slog.New(slog.DiscardHandler)

	now := time.Now()
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-shard-%d", now.UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeVitess,
		Deployment:     "region-a",
		Environment:    localClientTestEnvironment,
		CreatedAt:      now,
		Namespaces: map[string]*storage.NamespacePlanData{
			"resolute": {Tables: []storage.TableChange{
				{Namespace: "resolute", Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN email VARCHAR(255)", Operation: "alter"},
			}},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)

	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-shard-%d", now.UnixNano()),
		PlanID:          planID,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeVitess,
		Deployment:      "region-a",
		Engine:          storage.EnginePlanetScale,
		State:           state.Apply.Running,
		Environment:     localClientTestEnvironment,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := stor.Applies().Create(ctx, apply)
	require.NoError(t, err)

	opID, err := stor.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID:    applyID,
		Deployment: "region-a",
		Target:     "resolute",
		State:      state.ApplyOperation.Running,
	})
	require.NoError(t, err)

	// Stamp the operation lease the operator drive holds.
	leaseDB, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(leaseDB)
	require.NoError(t, leaseDB.PingContext(ctx))
	_, err = leaseDB.ExecContext(ctx, `
		UPDATE apply_operations SET lease_owner = ?, lease_token = ?, lease_acquired_at = NOW() WHERE id = ?
	`, "driver", "op-token", opID)
	require.NoError(t, err)

	client := &LocalClient{storage: stor, logger: logger}

	perTable := &storage.Task{
		TaskIdentifier:   fmt.Sprintf("task-users-%d", now.UnixNano()),
		ApplyID:          applyID,
		ApplyOperationID: &opID,
		PlanID:           planID,
		Database:         "testdb",
		DatabaseType:     storage.DatabaseTypeVitess,
		Engine:           storage.EnginePlanetScale,
		Environment:      localClientTestEnvironment,
		State:            state.Task.Running,
		Namespace:        "resolute",
		TableName:        "users",
		DDL:              "ALTER TABLE `users` ADD COLUMN email VARCHAR(255)",
		DDLAction:        "alter",
	}
	tp := &engine.TableProgress{
		Namespace: "resolute",
		Table:     "users",
		Shards: []engine.ShardProgress{
			{Shard: "-80", State: "running", Progress: 44, RowsCopied: 220000, RowsTotal: 500000, ETASeconds: 720, CutoverAttempts: 1},
			{Shard: "80-", State: "running", Progress: 60, RowsCopied: 300000, RowsTotal: 500000, ETASeconds: 480},
		},
	}

	leaseCtx := storage.WithOperationLease(ctx, storage.OperationLease{
		ApplyID: applyID, OperationID: opID, Owner: "driver", Token: "op-token",
	})

	// Under the lease, both shards are persisted as per-shard tasks.
	client.writeShardProgress(leaseCtx, perTable, tp, now)

	got, err := stor.Tasks().GetShardProgressByApplyOperationID(ctx, opID)
	require.NoError(t, err)
	require.Len(t, got, 2, "both shards persisted as per-shard tasks")
	// The per-table loader must not surface shard rows, so the operator drive
	// never re-processes them as per-table tasks on the next reload.
	perTableRows, err := stor.Tasks().GetByApplyOperationID(ctx, opID)
	require.NoError(t, err)
	assert.Empty(t, perTableRows, "per-shard rows must not leak into the per-table loader")
	byShard := map[string]*storage.Task{}
	for _, task := range got {
		assert.Equal(t, "resolute", task.Namespace)
		assert.Equal(t, "users", task.TableName)
		byShard[task.Shard] = task
	}
	require.Contains(t, byShard, "-80")
	require.Contains(t, byShard, "80-")
	assert.Equal(t, 44, byShard["-80"].ProgressPercent)
	assert.Equal(t, int64(220000), byShard["-80"].RowsCopied)
	assert.Equal(t, int64(500000), byShard["-80"].RowsTotal)
	assert.Equal(t, 720, byShard["-80"].ETASeconds)
	assert.Equal(t, 1, byShard["-80"].CutoverAttempts)
	assert.Equal(t, 60, byShard["80-"].ProgressPercent)

	// A later drive pass updates in place — no duplicate rows.
	tp.Shards[0].Progress = 100
	tp.Shards[0].RowsCopied = 500000
	client.writeShardProgress(leaseCtx, perTable, tp, time.Now())
	got, err = stor.Tasks().GetShardProgressByApplyOperationID(ctx, opID)
	require.NoError(t, err)
	require.Len(t, got, 2, "re-running the drive updates shards in place, no duplicates")
	for _, task := range got {
		if task.Shard == "-80" {
			assert.Equal(t, 100, task.ProgressPercent, "shard -80 advanced to 100%")
		}
	}

	// A read-path caller (no operation lease) must not write.
	cleanupTasks(t, dsn)
	client.writeShardProgress(ctx, perTable, tp, time.Now())
	got, err = stor.Tasks().GetShardProgressByApplyOperationID(ctx, opID)
	require.NoError(t, err)
	assert.Empty(t, got, "without an operation lease the drive write-through is a no-op")
}
