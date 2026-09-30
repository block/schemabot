package commands

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

// A plan of a twelve-target rollout says how big each table it rebuilds is
// across the rollout before the plan groups: a table every target changes
// gives its total, the largest target by name, and the smallest, and a table
// one target changes names that target. The operator reads the biggest copy
// the apply will run without adding up twelve plans.
func TestWritePlanBody_TwelveTargetRolloutShowsTableSizes(t *testing.T) {
	var sizes []*apitypes.PlanMemberTableSizeResponse
	var members []string
	for i := 1; i <= 12; i++ {
		target := fmt.Sprintf("orders-%03d", i)
		members = append(members, target)
		bytes := int64(104_900_000_000)
		switch target {
		case "orders-007":
			bytes = 310_000_000_000
		case "orders-001":
			bytes = 41_000_000_000
		}
		sizes = append(sizes, &apitypes.PlanMemberTableSizeResponse{Member: target, Namespace: "ns_0", Table: "orders", EstimatedBytes: &bytes})
	}
	refunds := int64(2_500_000_000)
	sizes = append(sizes, &apitypes.PlanMemberTableSizeResponse{Member: "orders-003", Namespace: "ns_0", Table: "refunds", EstimatedBytes: &refunds})

	plan := &apitypes.PlanResponse{
		Database: "orders",
		Engine:   "spirit",
		Changes:  addColumnTo("region"),
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     12,
			Independent: true,
			Groups: []*apitypes.PlanMemberGroupResponse{
				{Members: members, Primary: true, Changes: addColumnTo("region")},
			},
			TableSizes: sizes,
		},
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))

	assert.Contains(t, out, "Table sizes (12 targets):\n")
	assert.Contains(t, out, "  ns_0.orders   ~1.4 TB total · largest ~310 GB on orders-007 · smallest ~41 GB\n")
	assert.Contains(t, out, "  ns_0.refunds  ~2.5 GB on orders-003\n")
	sizesAt := strings.Index(out, "Table sizes")
	groupAt := strings.Index(out, "▸ all 12 targets")
	require.NotEqual(t, -1, groupAt, "the group heading is written")
	assert.Less(t, sizesAt, groupAt, "table sizes lead the plan groups")
}

// A table a target reports no estimate for is counted, since the total then
// understates the table, and a rollout where no target reported any estimate
// prints no size section at all.
func TestWritePlanBody_RolloutTableSizesCountMissingEstimates(t *testing.T) {
	large, small := int64(310_000_000_000), int64(41_000_000_000)
	plan := &apitypes.PlanResponse{
		Database: "orders",
		Engine:   "spirit",
		Changes:  addColumnTo("region"),
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     3,
			Independent: true,
			Groups: []*apitypes.PlanMemberGroupResponse{
				{Members: []string{"orders-001", "orders-002", "orders-003"}, Primary: true, Changes: addColumnTo("region")},
			},
			TableSizes: []*apitypes.PlanMemberTableSizeResponse{
				{Member: "orders-001", Namespace: "ns_0", Table: "orders", EstimatedBytes: &small},
				{Member: "orders-002", Namespace: "ns_0", Table: "orders", EstimatedBytes: &large},
				{Member: "orders-003", Namespace: "ns_0", Table: "orders"},
			},
		},
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	assert.Contains(t, out, "  ns_0.orders  ~351 GB total across 2 of 3 targets · largest ~310 GB on orders-002 · smallest ~41 GB · 1 has no estimate\n")

	for _, s := range plan.Rollout.TableSizes {
		s.EstimatedBytes = nil
	}
	out = stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	assert.NotContains(t, out, "Table sizes")
}
