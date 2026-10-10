package api

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/tern"
)

// ordersMemberPlan is one member's plan against the ns_0 namespace: an index
// build on orders at the given size, a metadata-only column add on refunds,
// and a new audit_log table.
func ordersMemberPlan(ordersBytes int64) tern.ChangeSet {
	refundsBytes := int64(9_000_000_000)
	return tern.ChangeSet{Changes: []*ternv1.SchemaChange{{
		Namespace: "ns_0",
		TableChanges: []*ternv1.TableChange{
			{TableName: "orders", Namespace: "ns_0", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Ddl: "ALTER TABLE `orders` ADD INDEX `idx_created_at` (`created_at`)", EstimatedBytes: &ordersBytes},
			{TableName: "refunds", Namespace: "ns_0", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Ddl: "ALTER TABLE `refunds` ADD COLUMN `reason` varchar(255)", EstimatedBytes: &refundsBytes},
			{TableName: "audit_log", Namespace: "ns_0", ChangeType: ternv1.ChangeType_CHANGE_TYPE_CREATE, Ddl: "CREATE TABLE `audit_log` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"},
		},
	}}}
}

// A rollout's plan response lists each planned member's own size for every
// table whose cost grows with its size, read from that member's plan rather
// than its group's: two members that share a group still report their own
// sizes. A metadata-only change and a created table get no size, and a member
// that could not be planned has none to report.
func TestPlanRolloutResponse_ListsEachMembersOwnTableSizes(t *testing.T) {
	rollup := PlanRollup{
		Planning: PlanIndependent,
		Entries: []DeploymentRollupEntry{
			{DatabaseType: "mysql", Deployment: "prod", Target: "orders-001", Class: DeploymentMatch, PlanFingerprint: "fp", ChangeSet: ordersMemberPlan(310_000_000_000)},
			{DatabaseType: "mysql", Deployment: "prod", Target: "orders-002", Class: DeploymentPlanned, PlanFingerprint: "fp", ChangeSet: ordersMemberPlan(41_000_000_000)},
			{DatabaseType: "mysql", Deployment: "prod", Target: "orders-003", Class: DeploymentErrored, Err: errors.New("dial orders-003: connection refused")},
		},
	}

	resp := planRolloutResponse(rollup)

	require.Len(t, resp.Groups, 1, "both planned members run the same work")
	assert.Equal(t, []string{"prod/orders-001", "prod/orders-002"}, resp.Groups[0].Members)
	large, small := int64(310_000_000_000), int64(41_000_000_000)
	assert.Equal(t, []*apitypes.PlanMemberTableSizeResponse{
		{Member: "prod/orders-001", Namespace: "ns_0", Table: "orders", EstimatedBytes: &large},
		{Member: "prod/orders-002", Namespace: "ns_0", Table: "orders", EstimatedBytes: &small},
	}, resp.TableSizes)
}

// A mirrored member whose plan diverged from the primary's is listed for
// attention, not grouped, so its sizes stay out of the rollout's table sizes
// even though it keeps a plan of its own: the sizes describe only the members
// an apply would run as planned.
func TestPlanRolloutResponse_LeavesDivergedMemberOutOfTableSizes(t *testing.T) {
	rollup := PlanRollup{
		Planning: PlanMirrored,
		Entries: []DeploymentRollupEntry{
			{DatabaseType: "mysql", Deployment: "prod", Target: "orders-001", Class: DeploymentMatch, PlanFingerprint: "fp", ChangeSet: ordersMemberPlan(310_000_000_000)},
			{DatabaseType: "mysql", Deployment: "prod", Target: "orders-002", Class: DeploymentDiverged, ChangeSet: ordersMemberPlan(41_000_000_000)},
		},
	}

	resp := planRolloutResponse(rollup)

	require.Len(t, resp.Attention, 1)
	assert.Equal(t, "prod/orders-002", resp.Attention[0].Member)
	assert.Equal(t, apitypes.PlanMemberDiverged, resp.Attention[0].Reason)
	primary := int64(310_000_000_000)
	assert.Equal(t, []*apitypes.PlanMemberTableSizeResponse{
		{Member: "prod/orders-001", Namespace: "ns_0", Table: "orders", EstimatedBytes: &primary},
	}, resp.TableSizes)
}

// Shards of one namespace usually carry the same statement, so each table's
// shard DDL keeps one copy of every distinct statement, in the order the
// shards first carry them, and a shard's change to another table stays under
// that table.
func TestDistinctShardDDL_KeepsEachStatementOnce(t *testing.T) {
	addColumn := "ALTER TABLE `orders` ADD COLUMN `note` varchar(255)"
	addIndex := "ALTER TABLE `orders` ADD INDEX `idx_note` (`note`)"
	shard := func(name, ddl string) *ternv1.ShardPlan {
		return &ternv1.ShardPlan{Namespace: "commerce", Shard: name, Changes: []*ternv1.TableChange{{TableName: "orders", Ddl: ddl}}}
	}
	shards := []*ternv1.ShardPlan{
		shard("-40", addColumn),
		shard("40-80", addIndex),
		shard("80-c0", addColumn),
		shard("c0-", addIndex),
		{Namespace: "commerce", Shard: "c0-", Changes: []*ternv1.TableChange{{TableName: "refunds", Ddl: addColumn}}},
	}

	assert.Equal(t, map[memberTableRef][]string{
		{"commerce", "orders"}:  {addColumn, addIndex},
		{"commerce", "refunds"}: {addColumn},
	}, distinctShardDDL(shards))
}

// A sharded table is sized when any shard's statement grows with the table,
// even if the namespace view carries a metadata-only one, and is left out
// when every shard runs the same metadata-only statement.
func TestMemberSizedTables_JudgesEveryShardsStatement(t *testing.T) {
	addColumn := "ALTER TABLE `orders` ADD COLUMN `note` varchar(255)"
	addIndex := "ALTER TABLE `orders` ADD INDEX `idx_note` (`note`)"
	bytes := int64(120_000_000_000)
	entry := func(shardDDL ...string) DeploymentRollupEntry {
		var shards []*ternv1.ShardPlan
		for _, d := range shardDDL {
			shards = append(shards, &ternv1.ShardPlan{Namespace: "commerce", Changes: []*ternv1.TableChange{{TableName: "orders", Ddl: d}}})
		}
		return DeploymentRollupEntry{DatabaseType: "vitess", Deployment: "prod", Target: "commerce-001", ChangeSet: tern.ChangeSet{
			Changes: []*ternv1.SchemaChange{{Namespace: "commerce", TableChanges: []*ternv1.TableChange{
				{TableName: "orders", Namespace: "commerce", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Ddl: addColumn, EstimatedBytes: &bytes},
			}}},
			Shards: shards,
		}}
	}

	sized := MemberSizedTables(entry(addColumn, addIndex))
	require.Len(t, sized, 1)
	assert.Equal(t, "commerce", sized[0].Namespace)
	assert.Equal(t, "orders", sized[0].Change.GetTableName())
	assert.Equal(t, &bytes, sized[0].Change.EstimatedBytes)

	assert.Empty(t, MemberSizedTables(entry(addColumn, addColumn, addColumn)))
}
