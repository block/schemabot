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
		{Target: "prod/orders-001", Namespace: "ns_0", Table: "orders", EstimatedBytes: &large},
		{Target: "prod/orders-002", Namespace: "ns_0", Table: "orders", EstimatedBytes: &small},
	}, resp.TableSizes)
}
