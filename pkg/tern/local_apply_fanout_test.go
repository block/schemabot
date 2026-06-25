package tern

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

const fanoutNamespace = "cdb_resolute_sharded"

func fanoutTestTime() time.Time { return time.Unix(1700000000, 0).UTC() }

func shardedPlan(shards ...string) *storage.Plan {
	plan := &storage.Plan{
		ID:           7,
		Database:     "cdb-resolute",
		DatabaseType: storage.DatabaseTypeStrata,
		Target:       "cdb-resolute",
	}
	for _, shard := range shards {
		plan.Shards = append(plan.Shards, storage.ShardPlan{
			Shard:       shard,
			Namespace:   fanoutNamespace,
			NeedsChange: true,
		})
	}
	return plan
}

func mutesIndexChange() []storage.TableChange {
	return []storage.TableChange{{
		Namespace: fanoutNamespace,
		Table:     "mutes",
		DDL:       "ALTER TABLE `mutes` ADD INDEX (`created_at`)",
		Operation: "alter",
	}}
}

func fanoutTestClient() *LocalClient {
	return &LocalClient{
		config: LocalConfig{Database: "cdb-resolute", Type: storage.DatabaseTypeStrata},
		logger: slog.Default(),
	}
}

// A sharded plan fans out into one pending work operation per (namespace, shard,
// table), each carrying a single shard-tagged task, with shards in sorted order.
// This is the shape the operator claims to hand the engine exactly one target
// shard per operation.
func TestBuildShardedOperationGroupsFansOutPerShard(t *testing.T) {
	c := fanoutTestClient()
	apply := &storage.Apply{Deployment: "cdb-resolute", Environment: "production", Engine: storage.EngineStrata}
	plan := shardedPlan("80-c0", "-40", "c0-", "40-80") // intentionally unsorted

	groups := c.buildShardedOperationGroups(apply, plan, mutesIndexChange(), nil, fanoutTestTime())
	require.Len(t, groups, 4, "one work operation per changing shard")

	wantShards := []string{"-40", "40-80", "80-c0", "c0-"}
	for i, group := range groups {
		shard := wantShards[i]
		require.NotNil(t, group.Operation)
		assert.Equal(t, storage.ApplyOperationKindWork, group.Operation.OperationKind)
		assert.Equal(t, state.ApplyOperation.Pending, group.Operation.State)
		assert.Equal(t, "cdb-resolute", group.Operation.Deployment)
		assert.Equal(t, "cdb-resolute", group.Operation.Target)
		assert.Equal(t, fanoutNamespace+"/"+shard+"/mutes", group.Operation.OperationKey,
			"operation key must encode namespace/shard/table")

		require.Len(t, group.Tasks, 1, "each shard operation carries exactly one task")
		task := group.Tasks[0]
		assert.Equal(t, shard, task.Shard, "task must be tagged with its shard")
		assert.Equal(t, "mutes", task.TableName)
		assert.Equal(t, fanoutNamespace, task.Namespace)
		assert.Equal(t, state.Task.Pending, task.State)
		assert.Equal(t, storage.EngineStrata, task.Engine)
	}
}

// taskTargetShards over a single shard operation's tasks yields exactly one
// shard — the invariant the strata engine's work path enforces.
func TestShardedOperationGroupsYieldSingleTargetShard(t *testing.T) {
	c := fanoutTestClient()
	apply := &storage.Apply{Deployment: "cdb-resolute", Engine: storage.EngineStrata}
	groups := c.buildShardedOperationGroups(apply, shardedPlan("-80", "80-"), mutesIndexChange(), nil, fanoutTestTime())
	require.Len(t, groups, 2)
	for _, group := range groups {
		assert.Len(t, taskTargetShards(group.Tasks), 1,
			"each operation must resolve to exactly one target shard")
	}
}

// A VSchema-changing namespace also gets a task-less group_finalizer that trails
// its shard work.
func TestBuildShardedOperationGroupsAppendsVSchemaFinalizer(t *testing.T) {
	c := fanoutTestClient()
	apply := &storage.Apply{Deployment: "cdb-resolute", Engine: storage.EngineStrata}
	plan := shardedPlan("-80", "80-")
	plan.Namespaces = map[string]*storage.NamespacePlanData{
		fanoutNamespace: {Artifacts: map[string]string{vSchemaArtifactName: "{}"}},
	}

	groups := c.buildShardedOperationGroups(apply, plan, mutesIndexChange(), nil, fanoutTestTime())
	require.Len(t, groups, 3, "two shard work operations plus one finalizer")

	finalizer := groups[len(groups)-1]
	assert.Equal(t, storage.ApplyOperationKindGroupFinalizer, finalizer.Operation.OperationKind)
	assert.Equal(t, fanoutNamespace+"/"+finalizerOperationKeySegment, finalizer.Operation.OperationKey)
	assert.Empty(t, finalizer.Tasks, "a group_finalizer carries no tasks")
}

// When the plan has no per-shard membership for the changing tables, the sharded
// builder declines (returns nil) so the caller falls back to a single operation.
func TestBuildShardedOperationGroupsDeclinesWithoutShards(t *testing.T) {
	c := fanoutTestClient()
	apply := &storage.Apply{Deployment: "cdb-resolute", Engine: storage.EngineStrata}

	assert.Nil(t, c.buildShardedOperationGroups(apply, &storage.Plan{ID: 1}, mutesIndexChange(), nil, fanoutTestTime()),
		"no shards at all → decline")

	// A changing namespace whose changing table's namespace has no shard rows
	// must decline rather than silently dropping the table's work.
	otherNamespaceChange := []storage.TableChange{{Namespace: "other", Table: "t", DDL: "ALTER TABLE `t` ADD COLUMN c int"}}
	assert.Nil(t, c.buildShardedOperationGroups(apply, shardedPlan("-80"), otherNamespaceChange, nil, fanoutTestTime()),
		"changing table namespace with no shard membership → decline")
}

// The non-sharded fallback (no changing shards) produces a single whole-deployment
// work operation carrying every task with no shard, preserving the pre-fan-out
// shape. A custom engine that does not report externally-authoritative progress
// makes SupportsShardedApplyFanout report true, so this exercises the fallback
// inside buildApplyOperationGroups rather than the engine gate.
func TestBuildApplyOperationGroupsFallsBackToSingleOperation(t *testing.T) {
	c := fanoutTestClient()
	c.customEngine = namedPlanEngine{name: storage.EngineStrata}
	require.True(t, c.SupportsShardedApplyFanout())

	apply := &storage.Apply{Deployment: "cdb-resolute", Engine: storage.EngineStrata}
	ddlChanges := []storage.TableChange{
		{Namespace: fanoutNamespace, Table: "mutes", DDL: "ALTER TABLE `mutes` ADD INDEX (`created_at`)", Operation: "alter"},
		{Namespace: fanoutNamespace, Table: "logs", DDL: "ALTER TABLE `logs` ADD INDEX (`created_at`)", Operation: "alter"},
	}

	// No shards on the plan → no per-shard fan-out → single operation.
	groups := c.buildApplyOperationGroups(apply, &storage.Plan{ID: 1, Target: "cdb-resolute"}, ddlChanges, nil, fanoutTestTime())
	require.Len(t, groups, 1)
	assert.Equal(t, storage.ApplyOperationKindWork, groups[0].Operation.OperationKind)
	assert.Empty(t, groups[0].Operation.OperationKey, "single operation uses the legacy empty key")
	require.Len(t, groups[0].Tasks, 2, "all tasks ride the one operation")
	for _, task := range groups[0].Tasks {
		assert.Empty(t, task.Shard, "non-sharded tasks carry no shard")
	}
}
