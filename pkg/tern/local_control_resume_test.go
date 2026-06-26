package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// A sharded re-plan repeats the same table across shards, each with its own DDL.
// replanShardTableDDL must key by (shard, table) so one shard's remaining diff
// never reconciles another shard's task. Keying by table name alone would
// collapse these two entries into one and conflate per-shard tasks.
func TestReplanShardTableDDLKeysPerShard(t *testing.T) {
	ddlA := "ALTER TABLE `mutes` ADD INDEX (`created_at`)"
	ddlB := "ALTER TABLE `mutes` ADD INDEX (`updated_at`)" // 80- has drifted differently
	result := &engine.PlanResult{
		Changes: []engine.SchemaChange{
			{Shard: engine.Shard{Name: "-80"}, TableChanges: []engine.TableChange{{Table: "mutes", DDL: ddlA}}},
			{Shard: engine.Shard{Name: "80-"}, TableChanges: []engine.TableChange{{Table: "mutes", DDL: ddlB}}},
		},
	}

	got := replanShardTableDDL(result)

	require.Len(t, got, 2, "same table on two shards must produce two distinct keys")
	assert.Equal(t, ddlA, got[shardTableKey{shard: "-80", table: "mutes"}])
	assert.Equal(t, ddlB, got[shardTableKey{shard: "80-", table: "mutes"}])
}

// For a non-sharded engine the shard name is empty, so keying degrades to
// table-only and matches the pre-sharding lookup.
func TestReplanShardTableDDLNonShardedDegradesToTable(t *testing.T) {
	ddl := "ALTER TABLE `mutes` ADD INDEX (`created_at`)"
	result := &engine.PlanResult{
		Changes: []engine.SchemaChange{
			{TableChanges: []engine.TableChange{{Table: "mutes", DDL: ddl}}},
		},
	}

	got := replanShardTableDDL(result)

	require.Len(t, got, 1)
	assert.Equal(t, ddl, got[shardTableKey{table: "mutes"}])
}
