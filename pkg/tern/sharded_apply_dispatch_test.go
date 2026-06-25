package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// A gRPC data plane must advertise sharded-apply fan-out so the control plane
// fans a sharded plan out into one claimable apply_operation per shard and
// dispatches each scoped to its shard. The capability is unconditional: apply-
// create only fans out when the stored plan carries changing shards, so a
// non-sharded plan is unaffected.
func TestGRPCClientAdvertisesShardedApplyFanout(t *testing.T) {
	var client Client = &GRPCClient{}
	capable, ok := client.(ShardedApplyFanoutSupport)
	require.True(t, ok, "GRPCClient must implement ShardedApplyFanoutSupport")
	assert.True(t, capable.SupportsShardedApplyFanout())
}

// A dispatched apply carries the control plane's authoritative, already-scoped
// DDL changes for one apply_operation; the data plane must execute exactly
// those, not re-expand the whole stored plan. This is what lets a per-(shard,
// table) operation drive only its own table change.
func TestApplyRequestDDLChangesHonorsDispatchedScope(t *testing.T) {
	c := &LocalClient{}
	plan := &storage.Plan{
		Namespaces: map[string]*storage.NamespacePlanData{
			"cdb_resolute_sharded": {Tables: []storage.TableChange{
				{Namespace: "cdb_resolute_sharded", Table: "mutes", DDL: "ALTER TABLE `mutes` ADD INDEX (`created_at`)", Operation: "alter"},
				{Namespace: "cdb_resolute_sharded", Table: "logs", DDL: "ALTER TABLE `logs` ADD INDEX (`created_at`)", Operation: "alter"},
			}},
		},
	}

	req := &ternv1.ApplyRequest{
		DdlChanges: []*ternv1.TableChange{{
			Namespace:  "cdb_resolute_sharded",
			TableName:  "mutes",
			Ddl:        "ALTER TABLE `mutes` ADD INDEX (`created_at`)",
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
		}},
	}

	got := c.applyRequestDDLChanges(req, plan)
	require.Len(t, got, 1, "only the dispatched operation's table change is executed, not the whole keyspace")
	assert.Equal(t, "mutes", got[0].Table)
	assert.Equal(t, "cdb_resolute_sharded", got[0].Namespace)
	assert.Equal(t, "alter", got[0].Operation, "proto change type round-trips to the plan's DDL action")
	assert.Contains(t, got[0].DDL, "ADD INDEX")
}

// A local apply (no dispatched changes) falls back to the whole stored plan,
// preserving the pre-existing single-deployment behavior.
func TestApplyRequestDDLChangesFallsBackToPlan(t *testing.T) {
	c := &LocalClient{}
	plan := &storage.Plan{
		Namespaces: map[string]*storage.NamespacePlanData{
			"cdb_resolute_sharded": {Tables: []storage.TableChange{
				{Namespace: "cdb_resolute_sharded", Table: "mutes", DDL: "ALTER TABLE `mutes` ADD INDEX (`created_at`)", Operation: "alter"},
			}},
		},
	}

	got := c.applyRequestDDLChanges(&ternv1.ApplyRequest{}, plan)
	require.Len(t, got, 1)
	assert.Equal(t, "mutes", got[0].Table)
}
