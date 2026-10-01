package commands

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/presentation"
)

// A plan of a twelve-target rollout says how big each table it rebuilds is
// across the rollout, in the PR comment's words and in its place: after every
// group's DDL, directly above the one plan summary. A table every target
// changes gives its total, the largest target by name, and the smallest, and a
// table one target changes names that target. The operator reads the biggest
// copy the apply will run without adding up twelve plans.
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

	out := stripAnsi(captureStdout(func() { writePlanBody(sizedRolloutPlan(members, sizes), false) }))

	assert.Contains(t, out, "\n📊 Table sizes:\n"+
		"  • orders: ~1.4 TB across 12 targets · largest ~310 GB on orders-007 · smallest ~41 GB\n"+
		"  • refunds: ~2.5 GB on orders-003\n"+
		"\n📋 Plan: ")
	ddlAt := strings.Index(out, "ADD COLUMN `region`")
	sizesAt := strings.Index(out, "📊 Table sizes:")
	require.NotEqual(t, -1, ddlAt, "the group's DDL is written")
	assert.Less(t, ddlAt, sizesAt, "table sizes follow the plan groups")
}

// sizedRolloutPlan is a plan of one group of members that all add a column,
// carrying the given per-member table sizes.
func sizedRolloutPlan(members []string, sizes []*apitypes.PlanMemberTableSizeResponse) *apitypes.PlanResponse {
	return &apitypes.PlanResponse{
		Database: "orders",
		Engine:   "spirit",
		Changes:  addColumnTo("region"),
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     len(members),
			Independent: true,
			Groups: []*apitypes.PlanMemberGroupResponse{
				{Members: members, Primary: true, Changes: addColumnTo("region")},
			},
			TableSizes: sizes,
		},
	}
}

// A table a target reports no estimate for names that target, since the total
// then understates the table, and a rollout where no target reported any
// estimate prints no size section at all.
func TestWritePlanBody_RolloutTableSizesNameMissingEstimates(t *testing.T) {
	large, small := int64(310_000_000_000), int64(41_000_000_000)
	plan := sizedRolloutPlan([]string{"orders-001", "orders-002", "orders-003"}, []*apitypes.PlanMemberTableSizeResponse{
		{Member: "orders-001", Namespace: "ns_0", Table: "orders", EstimatedBytes: &small},
		{Member: "orders-002", Namespace: "ns_0", Table: "orders", EstimatedBytes: &large},
		{Member: "orders-003", Namespace: "ns_0", Table: "orders"},
	})

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	assert.Contains(t, out, "  • orders: ~351 GB across 2 of 3 targets · largest ~310 GB on orders-002 · smallest ~41 GB · size estimate unavailable on orders-003\n")

	for _, s := range plan.Rollout.TableSizes {
		s.EstimatedBytes = nil
	}
	out = stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	assert.NotContains(t, out, "Table sizes")
}

// Sizes spanning several namespaces name each table with its namespace, so a
// table name two namespaces share stays unambiguous.
func TestWritePlanBody_RolloutTableSizesQualifyAcrossNamespaces(t *testing.T) {
	a, b := int64(2_500_000_000), int64(41_000_000_000)
	plan := sizedRolloutPlan([]string{"orders-001"}, []*apitypes.PlanMemberTableSizeResponse{
		{Member: "orders-001", Namespace: "ns_0", Table: "orders", EstimatedBytes: &a},
		{Member: "orders-001", Namespace: "ns_1", Table: "orders", EstimatedBytes: &b},
	})

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	assert.Contains(t, out, "📊 Table sizes:\n"+
		"  • ns_0.orders: ~2.5 GB on orders-001\n"+
		"  • ns_1.orders: ~41 GB on orders-001\n")
}

// Up to five tables are listed in rollout order, as the PR comment lists them
// in the open. Past five the largest are listed first, as the comment's
// collapsed section lists them, and past its cap the rest are counted in a
// closing line.
func TestWritePlanBody_RolloutTableSizesListLargestFirstPastFive(t *testing.T) {
	sizesFor := func(tables int) []*apitypes.PlanMemberTableSizeResponse {
		var sizes []*apitypes.PlanMemberTableSizeResponse
		for i := range tables {
			bytes := int64(i+1) * 1_000_000_000
			sizes = append(sizes, &apitypes.PlanMemberTableSizeResponse{Member: "orders-001", Namespace: "ns_0", Table: fmt.Sprintf("t%02d", i+1), EstimatedBytes: &bytes})
		}
		return sizes
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(sizedRolloutPlan([]string{"orders-001"}, sizesFor(5)), false) }))
	assert.Contains(t, out, "📊 Table sizes:\n  • t01: ~1 GB on orders-001\n", "five tables keep rollout order")

	out = stripAnsi(captureStdout(func() { writePlanBody(sizedRolloutPlan([]string{"orders-001"}, sizesFor(6)), false) }))
	assert.Contains(t, out, "📊 Table sizes:\n  • t06: ~6 GB on orders-001\n  • t05: ~5 GB on orders-001\n", "six tables list the largest first")

	out = stripAnsi(captureStdout(func() {
		writePlanBody(sizedRolloutPlan([]string{"orders-001"}, sizesFor(presentation.TableSizesShown+2)), false)
	}))
	assert.Equal(t, presentation.TableSizesShown, strings.Count(out, "  • t"), "the section lists only the cap")
	assert.Contains(t, out, "  • t03: ~3 GB on orders-001\n  …and 2 more tables\n\n📋 Plan: ")
	assert.NotContains(t, out, "  • t02:")
}

// A table no member reported an estimate for ranks last, so past the cap it is
// among the tables the closing line counts, and the line says how many of them
// have no estimate, as the PR comment's does.
func TestWritePlanBody_RolloutTableSizesCountTablesWithoutEstimatePastCap(t *testing.T) {
	var sizes []*apitypes.PlanMemberTableSizeResponse
	sizes = append(sizes, &apitypes.PlanMemberTableSizeResponse{Member: "orders-001", Namespace: "ns_0", Table: "ledger"})
	for i := range presentation.TableSizesShown + 1 {
		bytes := int64(i+1) * 1_000_000_000
		sizes = append(sizes, &apitypes.PlanMemberTableSizeResponse{Member: "orders-001", Namespace: "ns_0", Table: fmt.Sprintf("t%02d", i+1), EstimatedBytes: &bytes})
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(sizedRolloutPlan([]string{"orders-001"}, sizes), false) }))
	assert.Contains(t, out, "  • t02: ~2 GB on orders-001\n  …and 2 more tables (1 without a size estimate)\n\n📋 Plan: ")
	assert.NotContains(t, out, "  • ledger:")
}
