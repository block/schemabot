package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/storage"
)

const pershardNamespace = "cdb_resolute_sharded"

func pershardTargets() []routing.ExecutionTarget {
	return []routing.ExecutionTarget{{DatabaseType: storage.DatabaseTypeStrata, Deployment: "cdb-resolute", Target: "cdb-resolute"}}
}

func pershardTestTime() time.Time { return time.Unix(1700000000, 0).UTC() }

// operationKeys returns each group's operation key paired with its tasks' DDLs,
// so a test can assert exactly which (shard, table) operations were built and
// what DDL each carries.
func operationDDLByKey(groups []*storage.ApplyOperationWithTasks) map[string][]string {
	out := make(map[string][]string, len(groups))
	for _, g := range groups {
		ddls := make([]string, 0, len(g.Tasks))
		for _, task := range g.Tasks {
			ddls = append(ddls, task.DDL)
		}
		out[g.Operation.OperationKey] = ddls
	}
	return out
}

// Each shard is driven from its own changes, so a keyspace whose shards diverge
// fans out into the per-shard operations that shard actually needs — not a
// uniform cross-product.
func TestBuildShardedApplyOperationGroupsUsesPerShardDDL(t *testing.T) {
	mutesDDL := "ALTER TABLE `mutes` ADD INDEX (`created_at`)"
	logsDDL := "ALTER TABLE `logs` ADD INDEX (`created_at`)"

	plan := &storage.Plan{
		Database: "cdb-resolute",
		Shards: []storage.ShardPlan{
			{Namespace: pershardNamespace, Shard: "-80", Changes: []storage.TableChange{
				{Namespace: pershardNamespace, Table: "mutes", DDL: mutesDDL, Operation: "alter"},
			}},
			// 80- has drifted further: it needs both tables.
			{Namespace: pershardNamespace, Shard: "80-", Changes: []storage.TableChange{
				{Namespace: pershardNamespace, Table: "mutes", DDL: mutesDDL, Operation: "alter"},
				{Namespace: pershardNamespace, Table: "logs", DDL: logsDDL, Operation: "alter"},
			}},
		},
	}

	groups, err := buildShardedApplyOperationGroups(plan, pershardTargets(), "production", storage.ApplyOptions{}, "", "", pershardTestTime())
	require.NoError(t, err)

	got := operationDDLByKey(groups)
	assert.Equal(t, map[string][]string{
		pershardNamespace + "/-80/mutes": {mutesDDL},
		pershardNamespace + "/80-/mutes": {mutesDDL},
		pershardNamespace + "/80-/logs":  {logsDDL},
	}, got, "-80 needs only mutes; 80- needs mutes and logs")

	for _, g := range groups {
		require.Len(t, g.Tasks, 1)
		assert.NotEmpty(t, g.Tasks[0].Shard, "every task is shard-tagged")
	}
}

// A shard with no changes is not a changing shard, so it produces no operations
// (membership is implied by changes, not a separate flag).
func TestBuildShardedApplyOperationGroupsSkipsShardsWithoutChanges(t *testing.T) {
	mutesDDL := "ALTER TABLE `mutes` ADD INDEX (`created_at`)"
	plan := &storage.Plan{
		Database: "cdb-resolute",
		Shards: []storage.ShardPlan{
			{Namespace: pershardNamespace, Shard: "-80", Changes: []storage.TableChange{
				{Namespace: pershardNamespace, Table: "mutes", DDL: mutesDDL, Operation: "alter"},
			}},
			{Namespace: pershardNamespace, Shard: "80-"}, // unchanged: no changes
		},
	}

	groups, err := buildShardedApplyOperationGroups(plan, pershardTargets(), "production", storage.ApplyOptions{}, "", "", pershardTestTime())
	require.NoError(t, err)

	assert.Equal(t, map[string][]string{
		pershardNamespace + "/-80/mutes": {mutesDDL},
	}, operationDDLByKey(groups), "only the shard with changes fans out; the unchanged shard stays out")
}

// Per-shard change data is remote/untrusted (it crosses the gRPC boundary), so a
// malformed change fails closed rather than persisting corrupt plan_data.
func TestProtoShardPlansToStorageFailsClosedOnMalformedChange(t *testing.T) {
	shardWith := func(ch *ternv1.TableChange) []*ternv1.ShardPlan {
		return []*ternv1.ShardPlan{{Namespace: pershardNamespace, Shard: "-80", Changes: []*ternv1.TableChange{ch}}}
	}
	cases := []struct {
		name string
		ch   *ternv1.TableChange
		want string
	}{
		{"nil change", nil, "is null"},
		{"empty table", &ternv1.TableChange{Ddl: "ALTER TABLE `mutes` ADD INDEX (`x`)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER}, "empty table or DDL"},
		{"empty ddl", &ternv1.TableChange{TableName: "mutes", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER}, "empty table or DDL"},
		{"unsupported type", &ternv1.TableChange{TableName: "mutes", Ddl: "x", ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA}, "unsupported change type"},
		{"namespace mismatch", &ternv1.TableChange{TableName: "mutes", Ddl: "x", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "other"}, "disagrees with shard namespace"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := protoShardPlansToStorage(shardWith(tc.ch))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// A malformed per-shard change in the stored plan fails closed before building an
// operation key or engine request from empty/mismatched fields.
func TestBuildShardedApplyOperationGroupsFailsClosedOnMalformedChange(t *testing.T) {
	plan := &storage.Plan{
		Database: "cdb-resolute",
		Shards: []storage.ShardPlan{{
			Namespace: pershardNamespace, Shard: "-80",
			Changes: []storage.TableChange{{Namespace: pershardNamespace, Table: "", DDL: "ALTER TABLE `mutes` ADD INDEX (`x`)", Operation: "alter"}},
		}},
	}
	_, err := buildShardedApplyOperationGroups(plan, pershardTargets(), "production", storage.ApplyOptions{}, "", "", pershardTestTime())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty table")
}

// Per-shard changes survive the proto boundary so the control plane rebuilds the
// fan-out from each shard's own DDL.
func TestProtoShardPlansToStorageCarriesPerShardChanges(t *testing.T) {
	got, err := protoShardPlansToStorage([]*ternv1.ShardPlan{{
		Namespace: pershardNamespace,
		Shard:     "-80",
		Changes: []*ternv1.TableChange{{
			Namespace:  pershardNamespace,
			TableName:  "mutes",
			Ddl:        "ALTER TABLE `mutes` ADD INDEX (`created_at`)",
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
		}},
	}})

	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Len(t, got[0].Changes, 1)
	assert.Equal(t, "mutes", got[0].Changes[0].Table)
	assert.Equal(t, pershardNamespace, got[0].Changes[0].Namespace)
	assert.Equal(t, "ALTER TABLE `mutes` ADD INDEX (`created_at`)", got[0].Changes[0].DDL)
	assert.Equal(t, "alter", got[0].Changes[0].Operation)
}
