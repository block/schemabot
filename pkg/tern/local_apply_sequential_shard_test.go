package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/storage"
)

// A sharded task must reach the engine with exactly one target shard, or a
// sharded engine (Strata) rejects it with "expected exactly one target shard,
// got 0". The shard is also placed on the SchemaChange.
func TestSequentialEngineApplyRequest_PropagatesShard(t *testing.T) {
	task := &storage.Task{
		TaskIdentifier: "task-1", Database: "cdb_resolute", Namespace: "cdb_resolute_sharded",
		TableName: "mutes", Shard: "-40", DDL: "ALTER TABLE `mutes` ADD INDEX `created_at` (`created_at`)",
	}

	req := sequentialEngineApplyRequest(task, nil, nil)

	require.Equal(t, []string{"-40"}, req.TargetShards, "the task's shard must reach the engine as a single target shard")
	require.Len(t, req.Changes, 1)
	assert.Equal(t, "-40", req.Changes[0].Shard.Name, "the shard is also set on the SchemaChange")
	require.Len(t, req.Changes[0].TableChanges, 1)
	assert.Equal(t, "mutes", req.Changes[0].TableChanges[0].Table)
	assert.Equal(t, task.DDL, req.Changes[0].TableChanges[0].DDL)
}

// A non-sharded task leaves the shard fields unset, so the non-sharded engine
// path is unchanged.
func TestSequentialEngineApplyRequest_NonShardedUnchanged(t *testing.T) {
	task := &storage.Task{
		TaskIdentifier: "task-1", Database: "testdb", Namespace: "testdb",
		TableName: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
	}

	req := sequentialEngineApplyRequest(task, nil, nil)

	assert.Empty(t, req.TargetShards, "a non-sharded task targets no shard")
	require.Len(t, req.Changes, 1)
	assert.Equal(t, engine.Shard{}, req.Changes[0].Shard, "no shard on the SchemaChange")
}
