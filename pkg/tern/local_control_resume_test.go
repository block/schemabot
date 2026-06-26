package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// A sharded re-plan repeats the same table across shards (and across keyspaces),
// each with its own DDL. replanShardTableDDL must key by (namespace, shard,
// table) so one shard's — or one keyspace's — remaining diff never reconciles
// another's task. Keying by less than the full tuple would collapse these
// entries and conflate tasks.
func TestReplanShardTableDDLKeysPerNamespaceAndShard(t *testing.T) {
	ddlA := "ALTER TABLE `mutes` ADD INDEX (`created_at`)"
	ddlB := "ALTER TABLE `mutes` ADD INDEX (`updated_at`)" // 80- has drifted differently
	ddlC := "ALTER TABLE `mutes` ADD INDEX (`deleted_at`)" // a second keyspace, same shard+table
	result := &engine.PlanResult{
		Changes: []engine.SchemaChange{
			{Namespace: "ks1", Shard: engine.Shard{Name: "-80"}, TableChanges: []engine.TableChange{{Table: "mutes", DDL: ddlA}}},
			{Namespace: "ks1", Shard: engine.Shard{Name: "80-"}, TableChanges: []engine.TableChange{{Table: "mutes", DDL: ddlB}}},
			{Namespace: "ks2", Shard: engine.Shard{Name: "-80"}, TableChanges: []engine.TableChange{{Table: "mutes", DDL: ddlC}}},
		},
	}

	got := replanShardTableDDL(result)

	require.Len(t, got, 3, "same table across shards and keyspaces must produce three distinct keys")
	assert.Equal(t, ddlA, got[shardTableKey{namespace: "ks1", shard: "-80", table: "mutes"}])
	assert.Equal(t, ddlB, got[shardTableKey{namespace: "ks1", shard: "80-", table: "mutes"}])
	assert.Equal(t, ddlC, got[shardTableKey{namespace: "ks2", shard: "-80", table: "mutes"}], "the same shard+table in another keyspace is not conflated")
}

// For a non-sharded engine the shard name is empty, so keying degrades to
// (namespace, table) and matches the pre-sharding lookup.
func TestReplanShardTableDDLNonShardedDegradesToTable(t *testing.T) {
	ddl := "ALTER TABLE `mutes` ADD INDEX (`created_at`)"
	result := &engine.PlanResult{
		Changes: []engine.SchemaChange{
			{Namespace: "commerce", TableChanges: []engine.TableChange{{Table: "mutes", DDL: ddl}}},
		},
	}

	got := replanShardTableDDL(result)

	require.Len(t, got, 1)
	assert.Equal(t, ddl, got[shardTableKey{namespace: "commerce", table: "mutes"}])
}
