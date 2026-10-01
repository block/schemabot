package commands

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/cliname"
)

// addColumnTo is one ALTER adding column to the orders table.
func addColumnTo(column string) []*apitypes.SchemaChangeResponse {
	return []*apitypes.SchemaChangeResponse{{
		Namespace: "orders",
		TableChanges: []*apitypes.TableChangeResponse{{
			TableName:  "orders",
			Namespace:  "orders",
			DDL:        "ALTER TABLE `orders` ADD COLUMN `" + column + "` varchar(32)",
			ChangeType: "alter",
		}},
	}}
}

// paymentsTargets names targets prod/payments-<from>..prod/payments-<to>, the
// way the API names the targets of one deployment.
func paymentsTargets(from, to int) []string {
	var names []string
	for i := from; i <= to; i++ {
		names = append(names, fmt.Sprintf("prod/payments-%03d", i))
	}
	return names
}

// A rollout of three targets, two of which still need the column and one
// already at the desired schema, is planned one group at a time: each
// heading names its targets, the group with work leads with its DDL, and the
// settled target says it has nothing to run. The primary's plan alone would
// have hidden that prod/payments-003 is done.
func TestWritePlanBody_ThreeTargetRolloutGroupsTargetsByPlan(t *testing.T) {
	plan := &apitypes.PlanResponse{
		Database: "orders",
		Engine:   "spirit",
		Changes:  addColumnTo("region"),
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     3,
			Independent: true,
			Groups: []*apitypes.PlanMemberGroupResponse{
				{Members: []string{"prod/payments-003"}, Changes: []*apitypes.SchemaChangeResponse{}},
				{Members: []string{"prod/payments-001", "prod/payments-002"}, Primary: true, Changes: addColumnTo("region")},
			},
		},
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	assert.Contains(t, out, "▸ targets prod/payments-001, prod/payments-002", "%s", out)
	assert.Contains(t, out, "▸ target prod/payments-003", "%s", out)
	assertBefore(t, out, "▸ targets prod/payments-001, prod/payments-002", "ADD COLUMN `region`")
	assertBefore(t, out, "ADD COLUMN `region`", "▸ target prod/payments-003")
	assertBefore(t, out, "▸ target prod/payments-003", "No schema changes detected")
	assert.Equal(t, 1, strings.Count(out, "ADD COLUMN `region`"), "each distinct plan's DDL is shown once")
}

// A rollout of 64 targets that all run one plan reads as one group: the
// heading states its coverage rather than naming 64 targets inline, the names
// fold onto a few wrapped lines capped with a count of the rest, and the DDL
// is shown once.
func TestWritePlanBody_SixtyFourTargetRolloutFoldsItsMembers(t *testing.T) {
	members := paymentsTargets(1, 64)
	plan := &apitypes.PlanResponse{
		Database: "orders",
		Engine:   "spirit",
		Changes:  addColumnTo("region"),
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     64,
			Independent: true,
			Groups:      []*apitypes.PlanMemberGroupResponse{{Members: members, Primary: true, Changes: addColumnTo("region")}},
		},
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	assert.NotContains(t, out, "diverge", "one plan across the rollout is not divergence")
	assert.Contains(t, out, "▸ all 64 targets", "%s", out)
	assert.Contains(t, out, "prod/payments-001, prod/payments-002,", "%s", out)
	assert.Contains(t, out, "prod/payments-024, and 40 more", "%s", out)
	assert.NotContains(t, out, "prod/payments-025", "names past the cap are counted, not listed")
	assert.Equal(t, 1, strings.Count(out, "ADD COLUMN `region`"), "the DDL is shown once for the whole group")
	for line := range strings.SplitSeq(out, "\n") {
		assert.LessOrEqual(t, utf8.RuneCountInString(line), 76, "a folded name line fits the terminal, indent included: %q", line)
	}
}

// A group of a few targets whose names do not fit on the heading's line reads
// like a wide group: the heading states its coverage and the names fold
// beneath it, so no line runs past the terminal width.
func TestWritePlanBody_LongTargetNamesFoldUnderTheHeading(t *testing.T) {
	members := []string{"prod/payments-ledger-primary-001", "prod/payments-ledger-primary-002", "prod/payments-ledger-primary-003"}
	plan := &apitypes.PlanResponse{
		Database: "orders",
		Engine:   "spirit",
		Changes:  addColumnTo("region"),
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     3,
			Independent: true,
			Groups:      []*apitypes.PlanMemberGroupResponse{{Members: members, Primary: true, Changes: addColumnTo("region")}},
		},
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	assert.Contains(t, out, "▸ all 3 targets\n  prod/payments-ledger-primary-001, prod/payments-ledger-primary-002,\n  prod/payments-ledger-primary-003\n", "%s", out)
	for line := range strings.SplitSeq(out, "\n") {
		assert.LessOrEqual(t, utf8.RuneCountInString(line), 76, "every line fits the terminal: %q", line)
	}
}

// A rollout whose every member needs attention has no group to show. It lists
// those members and stops: the primary's own changes are not shown as the
// rollout's plan, and the output never says the rollout has no changes, since
// what the unplanned members would run is unknown.
func TestWritePlanBody_RolloutWithNoGroupsShowsOnlyItsAttention(t *testing.T) {
	plan := &apitypes.PlanResponse{
		Database: "orders",
		Engine:   "spirit",
		Changes:  addColumnTo("region"),
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     2,
			Independent: true,
			Attention: []*apitypes.PlanMemberAttentionResponse{
				{Member: "prod/payments-001", Reason: apitypes.PlanMemberUnplanned, Detail: "plan failed; see server logs"},
				{Member: "prod/payments-002", Reason: apitypes.PlanMemberUnplanned, Detail: "plan failed; see server logs"},
			},
		},
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	assert.Contains(t, out, "2 targets need attention before an apply can run on them", "%s", out)
	assert.NotContains(t, out, "ADD COLUMN `region`", "the primary's changes are not the rollout's plan")
	assert.NotContains(t, out, "No schema changes detected", "unknown work is never reported as none")
	assert.NotContains(t, out, "📋 Plan:")
}

// A 64-target rollout in which 61 targets still need a column and 3 already
// have it reads like one plan: each group shows only its heading and its DDL,
// the settled group says so in one line, and a single summary at the bottom
// counts what the rollout runs, worded as the PR comment words it. No group closes
// on a summary or a "✓ No schema changes detected." of its own, which would
// read as the end of the plan part-way through it.
func TestWritePlanBody_DivergingRolloutEndsOnOneSummary(t *testing.T) {
	plan := &apitypes.PlanResponse{
		Database: "orders",
		Engine:   "spirit",
		Changes:  addColumnTo("region"),
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     64,
			Independent: true,
			Groups: []*apitypes.PlanMemberGroupResponse{
				{Members: paymentsTargets(62, 64), Changes: []*apitypes.SchemaChangeResponse{}},
				{Members: paymentsTargets(1, 61), Primary: true, Changes: addColumnTo("region")},
			},
		},
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	want := strings.Join([]string{
		"▸ 61 of 64 targets",
		"  prod/payments-001, prod/payments-002, prod/payments-003,",
		"  prod/payments-004, prod/payments-005, prod/payments-006,",
		"  prod/payments-007, prod/payments-008, prod/payments-009,",
		"  prod/payments-010, prod/payments-011, prod/payments-012,",
		"  prod/payments-013, prod/payments-014, prod/payments-015,",
		"  prod/payments-016, prod/payments-017, prod/payments-018,",
		"  prod/payments-019, prod/payments-020, prod/payments-021,",
		"  prod/payments-022, prod/payments-023, prod/payments-024, and 37 more",
		"",
		"     ~ orders",
		"       ALTER TABLE `orders` ADD COLUMN `region` varchar(32);",
		"",
		"▸ targets prod/payments-062, prod/payments-063, prod/payments-064",
		"",
		"  No schema changes detected",
		"",
		"📋 Plan: 1 table to alter",
		"",
		"",
	}, "\n")
	assert.Equal(t, want, out)
	assert.Equal(t, 1, strings.Count(out, "📋 Plan:"), "a rollout plan has one summary")
	assert.NotContains(t, out, "✓ No schema changes detected.", "a settled group beside groups with work does not close the plan")
}

// A statement several groups run is one change of the rollout, and a table
// several groups alter is one table to alter. Two groups that alter orders
// differently but build the same index alter one table and create one index.
func TestWritePlanBody_RolloutSummaryCountsEachChangeOnce(t *testing.T) {
	withIndex := func(column string) []*apitypes.SchemaChangeResponse {
		changes := addColumnTo(column)
		changes[0].TableChanges = append(changes[0].TableChanges, &apitypes.TableChangeResponse{
			TableName:  "orders",
			Namespace:  "orders",
			DDL:        "CREATE INDEX `idx_created_at` ON `orders` (`created_at`)",
			ChangeType: "create_index",
		})
		return changes
	}
	plan := &apitypes.PlanResponse{
		Database: "orders",
		Engine:   "spirit",
		Changes:  withIndex("region"),
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     64,
			Independent: true,
			Groups: []*apitypes.PlanMemberGroupResponse{
				{Members: paymentsTargets(1, 40), Primary: true, Changes: withIndex("region")},
				{Members: paymentsTargets(41, 64), Changes: withIndex("zone")},
			},
		},
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	assert.Equal(t, 1, strings.Count(out, "📋 Plan:"), "%s", out)
	assert.True(t, strings.HasSuffix(out, "📋 Plan: 1 table to alter, 1 index to create\n\n"), "the one summary closes the plan:\n%s", out)
}

// The PR comment's distinct-plans rollout: testapp_1 and testapp_2 add an
// email column to users, and testapp_3 adds the column and an index on it.
// users is the one table either plan alters, so the CLI closes on the line the
// comment does, "1 table to alter", with no count of targets beside it.
func TestWritePlanBody_RolloutSummaryCountsATableEveryGroupAltersOnce(t *testing.T) {
	alter := func(ddl ...string) []*apitypes.SchemaChangeResponse {
		sc := &apitypes.SchemaChangeResponse{Namespace: "testapp"}
		for _, d := range ddl {
			sc.TableChanges = append(sc.TableChanges, &apitypes.TableChangeResponse{TableName: "users", Namespace: "testapp", DDL: d, ChangeType: "alter"})
		}
		return []*apitypes.SchemaChangeResponse{sc}
	}
	const addEmail = "ALTER TABLE `users` ADD COLUMN `email` varchar(255) NULL"
	const indexEmail = "ALTER TABLE `users` ADD INDEX `idx_email`(`email`)"
	plan := &apitypes.PlanResponse{
		Database: "testapp",
		Engine:   "spirit",
		Changes:  alter(addEmail),
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     3,
			Independent: true,
			Groups: []*apitypes.PlanMemberGroupResponse{
				{Members: []string{"primary/testapp_1", "primary/testapp_2"}, Primary: true, Changes: alter(addEmail)},
				{Members: []string{"primary/testapp_3"}, Changes: alter(addEmail, indexEmail)},
			},
		},
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	assert.Contains(t, out, "▸ targets primary/testapp_1, primary/testapp_2\n", "%s", out)
	assert.Contains(t, out, "▸ target primary/testapp_3\n", "%s", out)
	assert.True(t, strings.HasSuffix(out, "\n📋 Plan: 1 table to alter\n\n"), "the one summary closes the plan as the comment's does:\n%s", out)
}

// A sharded rollout's plans open on a header per namespace, which brings its
// own blank line. The group heading leaves that line to the header, so one
// blank line separates the heading from "── ns_0 ──", as one does everywhere
// else in the plan.
func TestWritePlanBody_RolloutGroupHeadingAndNamespaceHeaderAreOneBlankLineApart(t *testing.T) {
	inNamespaces := func(namespaces ...string) []*apitypes.SchemaChangeResponse {
		var changes []*apitypes.SchemaChangeResponse
		for _, ns := range namespaces {
			changes = append(changes, &apitypes.SchemaChangeResponse{
				Namespace: ns,
				TableChanges: []*apitypes.TableChangeResponse{{
					TableName: "refunds", Namespace: ns, DDL: "ALTER TABLE `refunds` ADD COLUMN `region` varchar(32)", ChangeType: "alter",
				}},
			})
		}
		return changes
	}
	plan := &apitypes.PlanResponse{
		Database: "payments",
		Engine:   "spirit",
		Changes:  inNamespaces("ns_0", "ns_1"),
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     3,
			Independent: true,
			Groups: []*apitypes.PlanMemberGroupResponse{
				{Members: paymentsTargets(1, 2), Primary: true, Changes: inNamespaces("ns_0", "ns_1")},
				{Members: paymentsTargets(3, 3), Changes: []*apitypes.SchemaChangeResponse{}},
			},
		},
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	assert.Contains(t, out, "▸ targets prod/payments-001, prod/payments-002\n\n  ── ns_0 ──\n", "%s", out)
	assert.NotContains(t, out, "prod/payments-002\n\n\n", "no second blank line under the heading:\n%s", out)
	assert.Contains(t, out, "▸ target prod/payments-003\n\n  No schema changes detected\n", "%s", out)
}

// A rollout already at the desired schema everywhere closes as a single
// plan's does, on the one "✓ No schema changes detected." line.
func TestWritePlanBody_ConvergedRolloutEndsOnNoChanges(t *testing.T) {
	plan := &apitypes.PlanResponse{
		Database: "orders",
		Engine:   "spirit",
		Changes:  []*apitypes.SchemaChangeResponse{},
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     3,
			Independent: true,
			Groups:      []*apitypes.PlanMemberGroupResponse{{Members: paymentsTargets(1, 3), Primary: true, Changes: []*apitypes.SchemaChangeResponse{}}},
		},
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	assert.Equal(t, "▸ targets prod/payments-001, prod/payments-002, prod/payments-003\n\n✓ No schema changes detected.\n\n", out)
}

// A rollout whose planned targets are all at the desired schema is not
// settled while another target could not be planned: that target's work is
// unknown, never none. So the plan names it first, says under the planned
// group's heading that it has nothing to run, and does not close on the "✓ No
// schema changes detected." verdict that would read as the whole rollout done.
func TestWritePlanBody_ConvergedGroupsBesideAnUnplannedTargetGetNoVerdict(t *testing.T) {
	plan := &apitypes.PlanResponse{
		Database: "orders",
		Engine:   "spirit",
		Changes:  []*apitypes.SchemaChangeResponse{},
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     3,
			Independent: true,
			Groups:      []*apitypes.PlanMemberGroupResponse{{Members: paymentsTargets(1, 2), Primary: true, Changes: []*apitypes.SchemaChangeResponse{}}},
			Attention: []*apitypes.PlanMemberAttentionResponse{
				{Member: "prod/payments-003", Reason: apitypes.PlanMemberUnplanned, Detail: "could not be planned; see server logs for the cause, then plan again"},
			},
		},
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	assertBefore(t, out, "• prod/payments-003 — could not be planned", "▸ targets prod/payments-001, prod/payments-002")
	assert.Contains(t, out, "▸ targets prod/payments-001, prod/payments-002\n\n  No schema changes detected\n", "%s", out)
	assert.NotContains(t, out, "✓", "no rollout-wide verdict while a target's work is unknown:\n%s", out)
}

// A schema-per-namespace plan can run the same ALTER directly in two
// namespaces for the same reason: refunds is empty in both ns_0 and ns_1.
// Those are two write-blocking statements on two tables, so the notice names
// each, qualified by its namespace. The shard rows repeating a namespace's
// change are that one change, named once.
func TestWritePlanBody_DirectExecutionNoticeNamesEachNamespace(t *testing.T) {
	const reason = "the table has ~0 rows"
	directIn := func(namespace string) *apitypes.TableChangeResponse {
		return &apitypes.TableChangeResponse{
			TableName: "refunds", Namespace: namespace, ChangeType: "alter",
			DDL:           "ALTER TABLE `refunds` ADD COLUMN `region` varchar(32)",
			ExecutionMode: "direct", ModeReason: reason,
		}
	}
	plan := &apitypes.PlanResponse{
		Database: "payments",
		Engine:   "spirit",
		Changes: []*apitypes.SchemaChangeResponse{
			{Namespace: "ns_0", TableChanges: []*apitypes.TableChangeResponse{directIn("ns_0")}},
			{Namespace: "ns_1", TableChanges: []*apitypes.TableChangeResponse{directIn("ns_1")}},
		},
		Shards: []*apitypes.ShardPlanResponse{
			{Namespace: "ns_0", Shard: "-80", Changes: []*apitypes.TableChangeResponse{directIn("")}},
			{Namespace: "ns_0", Shard: "80-", Changes: []*apitypes.TableChangeResponse{directIn("")}},
			{Namespace: "ns_1", Shard: "-80", Changes: []*apitypes.TableChangeResponse{directIn("")}},
		},
	}

	notices := directChangeNotices(plan)
	require.Len(t, notices, 2, "one notice per namespace, however many shards run it: %+v", notices)
	assert.Equal(t, "ns_0.refunds", notices[0].Table)
	assert.Equal(t, "ns_1.refunds", notices[1].Table)
	for _, n := range notices {
		assert.Equal(t, reason, n.Reason)
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	assert.Contains(t, out, "1. ns_0.refunds: "+reason+"\n  2. ns_1.refunds: "+reason+"\n", "%s", out)
}

// Staging and production can share a primary plan while production's other
// target runs something staging's do not: here prod/payments-002 adds zone.
// A rollout's plan names its members, so the two never collapse into one
// "Staging & Production" section, which would render staging's groups only
// and never show production's zone column.
func TestOutputMultiEnvPlanResult_RolloutsWithTheSamePrimaryRenderTheirOwnSections(t *testing.T) {
	staging := &apitypes.PlanResponse{
		Database: "orders",
		Engine:   "spirit",
		Changes:  addColumnTo("region"),
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     2,
			Independent: true,
			Groups: []*apitypes.PlanMemberGroupResponse{
				{Members: []string{"staging/payments-001", "staging/payments-002"}, Primary: true, Changes: addColumnTo("region")},
			},
		},
	}
	production := &apitypes.PlanResponse{
		Database: "orders",
		Engine:   "spirit",
		Changes:  addColumnTo("region"),
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     2,
			Independent: true,
			Groups: []*apitypes.PlanMemberGroupResponse{
				{Members: []string{"prod/payments-001"}, Primary: true, Changes: addColumnTo("region")},
				{Members: []string{"prod/payments-002"}, Changes: addColumnTo("zone")},
			},
		},
	}
	require.Equal(t, planFingerprint(staging), planFingerprint(production), "the primaries alone read as the same plan")

	out := stripAnsi(captureStdout(func() {
		outputMultiEnvPlanResult(map[string]*apitypes.PlanResponse{"staging": staging, "production": production}, "orders", "orders")
	}))
	assert.NotContains(t, out, "Staging & Production", "%s", out)
	assertBefore(t, out, "▸ targets staging/payments-001, staging/payments-002", "▸ target prod/payments-002")
	assertBefore(t, out, "▸ target prod/payments-002", "ADD COLUMN `zone`")
}

// A 64-target rollout that splits 40/24 names each group by its share of the
// rollout, so a subset never reads like the whole.
func TestWritePlanBody_SixtyFourTargetRolloutSplitStatesCoverage(t *testing.T) {
	plan := &apitypes.PlanResponse{
		Database: "orders",
		Engine:   "spirit",
		Changes:  addColumnTo("region"),
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     64,
			Independent: true,
			Groups: []*apitypes.PlanMemberGroupResponse{
				{Members: paymentsTargets(1, 40), Primary: true, Changes: addColumnTo("region")},
				{Members: paymentsTargets(41, 64), Changes: addColumnTo("zone")},
			},
		},
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	assertBefore(t, out, "▸ 40 of 64 targets", "ADD COLUMN `region`")
	assertBefore(t, out, "ADD COLUMN `region`", "▸ 24 of 64 targets")
	assertBefore(t, out, "▸ 24 of 64 targets", "ADD COLUMN `zone`")
	assert.Contains(t, out, "prod/payments-041, prod/payments-042,")
}

// Two targets can run the same ALTER differently: the direct execution policy
// judges each target's own table, so a small one runs it as native DDL that
// blocks writes while a large one runs it through Spirit. Each group discloses
// its own verdict, so the write-blocking statement is named under the target
// that runs it and under no other.
func TestWritePlanBody_RolloutDisclosesDirectExecutionUnderTheTargetThatRunsIt(t *testing.T) {
	direct := addColumnTo("region")
	direct[0].TableChanges[0].ExecutionMode = "direct"
	direct[0].TableChanges[0].ModeReason = "table is 12 MiB, within the direct execution bound"
	plan := &apitypes.PlanResponse{
		Database: "orders",
		Engine:   "spirit",
		Changes:  addColumnTo("region"),
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     2,
			Independent: true,
			Groups: []*apitypes.PlanMemberGroupResponse{
				{Members: []string{"prod/payments-001"}, Primary: true, Changes: addColumnTo("region")},
				{Members: []string{"prod/payments-002"}, Changes: direct},
			},
		},
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	const notice = "Direct execution: runs as native MySQL DDL, not through Spirit, and blocks writes to the table while it runs:"
	assert.Equal(t, 1, strings.Count(out, notice), "only the target that runs it natively discloses it:\n%s", out)
	assertBefore(t, out, "▸ target prod/payments-002", notice)
	assert.Contains(t, out, "1. orders: table is 12 MiB, within the direct execution bound", "%s", out)
	first, _, found := strings.Cut(out, "▸ target prod/payments-002")
	require.True(t, found)
	assert.NotContains(t, first, notice, "the target running it through Spirit carries no disclosure")
}

// A member the server could not plan is named ahead of the plans, since no
// plan below covers it and an apply will be refused until it is planned.
func TestWritePlanBody_RolloutNamesUnplannedMembersFirst(t *testing.T) {
	plan := &apitypes.PlanResponse{
		Database: "orders",
		Engine:   "spirit",
		Changes:  addColumnTo("region"),
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     3,
			Independent: true,
			Groups:      []*apitypes.PlanMemberGroupResponse{{Members: []string{"prod/payments-001", "prod/payments-002"}, Primary: true, Changes: addColumnTo("region")}},
			Attention: []*apitypes.PlanMemberAttentionResponse{
				{Member: "prod/payments-003", Reason: apitypes.PlanMemberUnplanned, Detail: "could not be planned; see server logs for the cause, then plan again"},
			},
		},
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	assertBefore(t, out, "1 target needs attention before an apply can run on it:", "▸ targets prod/payments-001, prod/payments-002")
	assert.Contains(t, out, "• prod/payments-003 — could not be planned; see server logs for the cause, then plan again")
}

// An apply is gated on every member's plan: a rollout whose primary is
// already at the desired schema still has work when another member does, and
// an unsafe change on any member is an unsafe change of the apply.
func TestPlanResponse_RolloutGatesCoverEveryMember(t *testing.T) {
	drop := []*apitypes.SchemaChangeResponse{{
		Namespace: "orders",
		TableChanges: []*apitypes.TableChangeResponse{{
			TableName: "legacy", Namespace: "orders", DDL: "DROP TABLE `legacy`", ChangeType: "drop", IsUnsafe: true, UnsafeReason: "drops a table",
		}},
	}}
	plan := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{},
		Rollout: &apitypes.PlanRolloutResponse{
			Members: 3,
			Groups: []*apitypes.PlanMemberGroupResponse{
				{Members: []string{"prod/payments-001"}, Primary: true, Changes: []*apitypes.SchemaChangeResponse{}},
				{Members: []string{"prod/payments-002"}, Changes: drop},
				{Members: []string{"prod/payments-003"}, Changes: drop},
			},
		},
	}
	require.False(t, plan.HasChanges(), "the primary alone has nothing to run")
	assert.True(t, plan.RolloutHasChanges())
	unsafe := plan.RolloutUnsafeChanges()
	require.Len(t, unsafe, 1, "one unsafe change run by two groups is one change")
	assert.Equal(t, "legacy", unsafe[0].Table)
}

// A sharded rollout's groups can drop the same table in different namespaces:
// one group drops orders_1.legacy and the other orders_2.legacy. Those are two
// drops, so the apply's unsafe changes list both and an operator consents to
// both. The same namespace's drop, run by a second group, is still one change.
func TestPlanResponse_RolloutUnsafeChangesKeepTheSameTableInEachNamespace(t *testing.T) {
	dropIn := func(namespace string) []*apitypes.SchemaChangeResponse {
		return []*apitypes.SchemaChangeResponse{{
			Namespace: namespace,
			TableChanges: []*apitypes.TableChangeResponse{{
				TableName: "legacy", Namespace: namespace, DDL: "DROP TABLE `legacy`", ChangeType: "drop", IsUnsafe: true, UnsafeReason: "drops a table",
			}},
		}}
	}
	plan := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{},
		Rollout: &apitypes.PlanRolloutResponse{
			Members: 4,
			Groups: []*apitypes.PlanMemberGroupResponse{
				{Members: []string{"prod/payments-001"}, Primary: true, Changes: []*apitypes.SchemaChangeResponse{}},
				{Members: []string{"prod/payments-002"}, Changes: dropIn("orders_1")},
				{Members: []string{"prod/payments-003"}, Changes: dropIn("orders_2")},
				{Members: []string{"prod/payments-004"}, Changes: dropIn("orders_2")},
			},
		},
	}
	unsafe := plan.RolloutUnsafeChanges()
	require.Len(t, unsafe, 2, "the drop in each namespace is its own change, and a namespace's drop run twice is one")
	for _, c := range unsafe {
		assert.Equal(t, "legacy", c.Table)
		assert.Equal(t, "DROP TABLE `legacy`", c.DDL)
	}
}

func assertBefore(t *testing.T, out, first, second string) {
	t.Helper()
	i, j := strings.Index(out, first), strings.Index(out, second)
	require.GreaterOrEqualf(t, i, 0, "missing %q in:\n%s", first, out)
	require.GreaterOrEqualf(t, j, 0, "missing %q in:\n%s", second, out)
	assert.Lessf(t, i, j, "%q should come before %q in:\n%s", first, second, out)
}

// rolloutPlanServer serves plan as the response to every plan request and an
// environment with no active schema change to every status request, records
// the path of every request the CLI makes, and answers anything else with a
// server error.
func rolloutPlanServer(t *testing.T, plan *apitypes.PlanResponse) (*httptest.Server, *[]string) {
	t.Helper()
	body, err := json.Marshal(plan)
	require.NoError(t, err)
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		switch r.URL.Path {
		case "/api/plan":
			writeTestJSON(t, w, body)
		case "/api/status":
			writeTestJSON(t, w, []byte(`{}`))
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	t.Cleanup(server.Close)
	return server, &paths
}

// writeTestJSON answers a test server request with body as JSON.
func writeTestJSON(t *testing.T, w http.ResponseWriter, body []byte) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	_, err := w.Write(body)
	assert.NoError(t, err)
}

// An apply runs on every member of the rollout, so while one member could not
// be planned the CLI names it and refuses before asking the server to apply.
func TestApplyCmd_RefusesWhileARolloutMemberNeedsAttention(t *testing.T) {
	server, paths := rolloutPlanServer(t, &apitypes.PlanResponse{
		PlanID:  "plan-orders-1",
		Engine:  "mysql",
		Changes: addColumnTo("region"),
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     3,
			Independent: true,
			Groups:      []*apitypes.PlanMemberGroupResponse{{Members: paymentsTargets(1, 2), Primary: true, Changes: addColumnTo("region")}},
			Attention: []*apitypes.PlanMemberAttentionResponse{
				{Member: "prod/payments-003", Reason: apitypes.PlanMemberUnplanned, Detail: "could not be planned; see server logs for the cause, then plan again"},
			},
		},
	})

	cmd := ApplyCmd{SchemaDir: writeTestSchemaDir(t), Environment: "production", NoLock: true, AutoApprove: true}
	var runErr error
	out := stripAnsi(captureStdout(func() { runErr = cmd.Run(&Globals{Endpoint: server.URL}) }))

	require.Error(t, runErr)
	assert.Contains(t, runErr.Error(), "1 of 3 rollout members cannot be applied as planned")
	assert.Contains(t, out, "• prod/payments-003 — could not be planned")
	assert.NotContains(t, *paths, "/api/apply", "no apply is requested while a member has no plan")
}

// payments-001, the rollout primary, is already at the desired schema, and
// payments-002 and 003 still need a column. The apply is not a no-op, and
// what it shows before asking for consent is every target's plan: the
// column the other two targets add, and the targets that add it.
func TestApplyCmd_PromptFollowsEveryTargetsPlan(t *testing.T) {
	server, paths := rolloutPlanServer(t, &apitypes.PlanResponse{
		PlanID: "plan-orders-1",
		Engine: "mysql",
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     3,
			Independent: true,
			Groups: []*apitypes.PlanMemberGroupResponse{
				{Members: paymentsTargets(1, 1), Primary: true, Changes: []*apitypes.SchemaChangeResponse{}},
				{Members: paymentsTargets(2, 3), Changes: addColumnTo("region")},
			},
		},
	})

	cmd := ApplyCmd{SchemaDir: writeTestSchemaDir(t), Environment: "production", NoLock: true}
	out := stripAnsi(captureStdout(func() { _ = cmd.Run(&Globals{Endpoint: server.URL}) }))

	assert.NotContains(t, out, "No changes. Your schema is up-to-date.", "a converged primary does not make the rollout a no-op:\n%s", out)
	prompt := strings.Index(out, "Do you want to apply these changes?")
	require.GreaterOrEqual(t, prompt, 0, "the apply asks for consent:\n%s", out)
	shown := out[:prompt]
	assert.Contains(t, shown, "ADD COLUMN `region`", "the other targets' change is shown before the prompt:\n%s", out)
	assert.Contains(t, shown, "prod/payments-002", "%s", out)
	assert.Contains(t, shown, "prod/payments-003", "%s", out)
	assert.NotContains(t, *paths, "/api/apply", "nothing is applied without a yes")
}

// A rollout whose primary is already at the desired schema is applied from the
// primary's empty plan, and apply creation refuses another target's unsafe
// change there whatever the flags. The server lists those targets on the
// rollout, so even with --allow-unsafe the CLI refuses before asking the
// server to apply, and names the narrowed apply that runs each target under
// its own plan and its own consent.
func TestApplyCmd_UnsafeChangeBesideAConvergedPrimaryNamesTheNarrowedApply(t *testing.T) {
	drop := []*apitypes.SchemaChangeResponse{{
		Namespace: "orders",
		TableChanges: []*apitypes.TableChangeResponse{{
			TableName: "legacy", Namespace: "orders", DDL: "DROP TABLE `legacy`", ChangeType: "drop", IsUnsafe: true, UnsafeReason: "drops a table",
		}},
	}}
	const undisclosed = `carries an unsafe change for table "legacy" that the reviewed plan does not carry`
	server, paths := rolloutPlanServer(t, &apitypes.PlanResponse{
		PlanID: "plan-orders-1",
		Engine: "mysql",
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     3,
			Independent: true,
			Groups: []*apitypes.PlanMemberGroupResponse{
				{Members: paymentsTargets(1, 1), Primary: true, Changes: []*apitypes.SchemaChangeResponse{}},
				{Members: paymentsTargets(2, 3), Changes: drop},
			},
			Refused: []*apitypes.PlanMemberRefusalResponse{
				{Member: "prod/payments-002", Target: "payments-002", Reason: apitypes.PlanMemberNeedsTarget, Detail: undisclosed, AllowUnsafe: true},
				{Member: "prod/payments-003", Target: "payments-003", Reason: apitypes.PlanMemberNeedsTarget, Detail: undisclosed, AllowUnsafe: true},
			},
		},
	})

	schemaDir := writeTestSchemaDir(t)
	cmd := ApplyCmd{SchemaDir: schemaDir, Environment: "production", NoLock: true, AutoApprove: true, AllowUnsafe: true}
	var runErr error
	out := stripAnsi(captureStdout(func() { runErr = cmd.Run(&Globals{Endpoint: server.URL}) }))

	require.ErrorIs(t, runErr, ErrSilent)
	assert.NotContains(t, *paths, "/api/apply", "no apply is requested that apply creation would refuse")
	assert.Contains(t, out, "Apply blocked: an apply of the whole rollout cannot run the plan of 2 targets", "%s", out)
	assert.Contains(t, out, "• prod/payments-002 — "+undisclosed)
	for _, target := range []string{"payments-002", "payments-003"} {
		assert.Contains(t, out, "apply -s "+schemaDir+" -e production --target "+target+" --allow-unsafe", "%s", out)
	}
}

// The primary has work, and two other targets carry changes a rollout-wide
// apply from the CLI cannot run whatever the flags: us drops a table the
// primary's plan does not, and payments-003 runs its ALTER as direct-execution
// DDL, which only the pull request comment discloses under it. The CLI refuses
// before it locks or prompts, even under --allow-unsafe, and names the
// narrowed apply for each with the selector the server returned: us is a
// deployment with the one target us-main, which is what --target accepts. The
// opt-in is suggested only for the target whose own plan is unsafe. A target
// whose change its engine refuses gets no rerun, since no apply runs it: it is
// told to change the schema files, and the narrowed applies are offered for
// the other targets only, without the rollout rerun its refusal would block.
func TestApplyCmd_RefusesTargetsARolloutWideApplyCannotRunBesidePrimaryWork(t *testing.T) {
	server, paths := rolloutPlanServer(t, &apitypes.PlanResponse{
		PlanID:  "plan-orders-1",
		Engine:  "mysql",
		Changes: addColumnTo("region"),
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     4,
			Independent: true,
			Groups: []*apitypes.PlanMemberGroupResponse{
				{Members: []string{"prod/payments-001", "prod/payments-002", "prod/payments-003", "us"}, Primary: true, Changes: addColumnTo("region")},
			},
			Refused: []*apitypes.PlanMemberRefusalResponse{
				{Member: "prod/payments-002", Target: "payments-002", Reason: apitypes.PlanMemberBlocked, Detail: "carries changes its target's engine refuses"},
				{Member: "prod/payments-003", Target: "payments-003", Reason: apitypes.PlanMemberNeedsTarget, Detail: `runs table "users" as direct-execution DDL`},
				{Member: "us", Target: "us-main", Reason: apitypes.PlanMemberNeedsTarget, Detail: `carries an unsafe change for table "legacy" that the reviewed plan does not carry`, AllowUnsafe: true},
			},
		},
	})

	schemaDir := writeTestSchemaDir(t)
	cmd := ApplyCmd{SchemaDir: schemaDir, Environment: "production", AutoApprove: true, AllowUnsafe: true}
	var runErr error
	out := stripAnsi(captureStdout(func() { runErr = cmd.Run(&Globals{Endpoint: server.URL}) }))

	require.ErrorIs(t, runErr, ErrSilent)
	assert.Equal(t, []string{"/api/status", "/api/plan"}, *paths, "no lock is checked or taken and no apply is requested:\n%s", out)
	assert.Contains(t, out, "Apply blocked: an apply of the whole rollout cannot run the plan of 3 targets", "%s", out)
	assert.Contains(t, out, "• prod/payments-002 — carries changes its target's engine refuses")
	assert.Contains(t, out, "apply -s "+schemaDir+" -e production --target payments-003\n", "a direct change needs no unsafe opt-in:\n%s", out)
	assert.Contains(t, out, "apply -s "+schemaDir+" -e production --target us-main --allow-unsafe", "%s", out)
	assert.NotContains(t, out, "--target us ", "a deployment's display name is not a selector")
	assert.NotContains(t, out, "--target payments-002", "no apply runs a change its engine refuses")
	assert.NotContains(t, out, "retry with", "no opt-in the server would refuse is suggested")
	assert.Contains(t, out, "No apply can run the changes of the 1 target whose engine refuses them; change the schema files so each target's engine accepts them.", "%s", out)
	assert.Contains(t, out, "Apply each other target on its own, under its own plan and its own consent:", "%s", out)
	assert.NotContains(t, out, "then apply the rollout again", "the rollout stays refused while a target's engine refuses its change:\n%s", out)
}

// A rollout whose only refused target carries a change its engine refuses
// names no narrowed apply, only the schema-file remedy, and offers no rollout
// rerun.
func TestApplyCmd_RefusesARolloutWhoseOnlyRefusalIsBlocked(t *testing.T) {
	server, paths := rolloutPlanServer(t, &apitypes.PlanResponse{
		PlanID:  "plan-orders-1",
		Engine:  "mysql",
		Changes: addColumnTo("region"),
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     2,
			Independent: true,
			Groups:      []*apitypes.PlanMemberGroupResponse{{Members: paymentsTargets(1, 2), Primary: true, Changes: addColumnTo("region")}},
			Refused: []*apitypes.PlanMemberRefusalResponse{
				{Member: "prod/payments-002", Target: "payments-002", Reason: apitypes.PlanMemberBlocked, Detail: "carries changes its target's engine refuses"},
			},
		},
	})

	cmd := ApplyCmd{SchemaDir: writeTestSchemaDir(t), Environment: "production", NoLock: true, AutoApprove: true}
	var runErr error
	out := stripAnsi(captureStdout(func() { runErr = cmd.Run(&Globals{Endpoint: server.URL}) }))

	require.ErrorIs(t, runErr, ErrSilent)
	assert.NotContains(t, *paths, "/api/apply")
	assert.Contains(t, out, "No apply can run these changes; change the schema files so each target's engine accepts them.", "%s", out)
	assert.NotContains(t, out, "--target", "%s", out)
	assert.NotContains(t, out, "Apply each", "%s", out)
}

// A plan can carry a sharded namespace's DDL on its shard rows alone. That is
// work an apply runs, on the primary's plan and on any group's.
func TestPlanResponse_ShardRowsAloneAreWork(t *testing.T) {
	shardOnly := []*apitypes.ShardPlanResponse{{
		Namespace: "orders_sharded",
		Shard:     "-80",
		Changes: []*apitypes.TableChangeResponse{{
			TableName: "orders", Namespace: "orders_sharded", DDL: "ALTER TABLE `orders` ADD COLUMN `region` varchar(32)", ChangeType: "alter",
		}},
	}}
	settled := []*apitypes.ShardPlanResponse{{Namespace: "orders_sharded", Shard: "80-"}}

	assert.True(t, (&apitypes.PlanResponse{Shards: shardOnly}).HasChanges(), "the primary's shard rows are work")
	assert.False(t, (&apitypes.PlanResponse{Shards: settled}).HasChanges(), "a shard row with no changes is a settled shard")

	plan := &apitypes.PlanResponse{
		Rollout: &apitypes.PlanRolloutResponse{
			Members: 2,
			Groups: []*apitypes.PlanMemberGroupResponse{
				{Members: []string{"prod/payments-001"}, Primary: true, Changes: []*apitypes.SchemaChangeResponse{}, Shards: settled},
				{Members: []string{"prod/payments-002"}, Changes: []*apitypes.SchemaChangeResponse{}, Shards: shardOnly},
			},
		},
	}
	assert.True(t, plan.RolloutHasChanges(), "another group's shard rows are work of the rollout")
}

// With --output json the member list is not printed, so the refusal's error
// names each member that needs attention and what it needs.
func TestApplyCmd_JSONOutputNamesTheMembersThatNeedAttention(t *testing.T) {
	server, paths := rolloutPlanServer(t, &apitypes.PlanResponse{
		PlanID:  "plan-orders-1",
		Engine:  "mysql",
		Changes: addColumnTo("region"),
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     3,
			Independent: true,
			Groups:      []*apitypes.PlanMemberGroupResponse{{Members: paymentsTargets(1, 2), Primary: true, Changes: addColumnTo("region")}},
			Attention: []*apitypes.PlanMemberAttentionResponse{
				{Member: "prod/payments-003", Reason: apitypes.PlanMemberUnplanned, Detail: "could not be planned; see server logs for the cause, then plan again"},
			},
		},
	})

	cmd := ApplyCmd{SchemaDir: writeTestSchemaDir(t), Environment: "production", NoLock: true, AutoApprove: true, Output: OutputFormatJSON}
	var runErr error
	out := stripAnsi(captureStdout(func() { runErr = cmd.Run(&Globals{Endpoint: server.URL}) }))

	require.Error(t, runErr)
	assert.Equal(t, "1 of 3 rollout members cannot be applied as planned; resolve each one, then apply again: prod/payments-003 (could not be planned; see server logs for the cause, then plan again)", runErr.Error())
	assert.NotContains(t, out, "• prod/payments-003", "the member list is not printed in JSON mode:\n%s", out)
	assert.NotContains(t, *paths, "/api/apply")
}

// With --output json the refusal's member list is not printed either, so the
// error names each refused target, why, and what runs it: the narrowed apply
// for a target whose own plan needs it, and no apply for one whose engine
// refuses its change.
func TestApplyCmd_JSONOutputNamesTheRefusedTargets(t *testing.T) {
	server, paths := rolloutPlanServer(t, &apitypes.PlanResponse{
		PlanID:  "plan-orders-1",
		Engine:  "mysql",
		Changes: addColumnTo("region"),
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     3,
			Independent: true,
			Groups:      []*apitypes.PlanMemberGroupResponse{{Members: paymentsTargets(1, 3), Primary: true, Changes: addColumnTo("region")}},
			Refused: []*apitypes.PlanMemberRefusalResponse{
				{Member: "prod/payments-002", Target: "payments-002", Reason: apitypes.PlanMemberBlocked, Detail: "carries changes its target's engine refuses"},
				{Member: "prod/payments-003", Target: "payments-003", Reason: apitypes.PlanMemberNeedsTarget, Detail: `carries an unsafe change for table "legacy" that the reviewed plan does not carry`, AllowUnsafe: true},
			},
		},
	})

	schemaDir := writeTestSchemaDir(t)
	cmd := ApplyCmd{SchemaDir: schemaDir, Environment: "production", NoLock: true, AutoApprove: true, Output: OutputFormatJSON}
	var runErr error
	out := stripAnsi(captureStdout(func() { runErr = cmd.Run(&Globals{Endpoint: server.URL}) }))

	require.Error(t, runErr)
	assert.Equal(t, "an apply of the whole rollout cannot run the plan of 2 of 3 rollout members: "+
		"prod/payments-002 (carries changes its target's engine refuses; no apply runs it, so change the schema files); "+
		`prod/payments-003 (carries an unsafe change for table "legacy" that the reviewed plan does not carry; run it with: `+cliname.Name()+" apply -s "+schemaDir+" -e production --target payments-003 --allow-unsafe)",
		runErr.Error())
	assert.NotContains(t, out, "Apply blocked", "the human refusal is not printed in JSON mode:\n%s", out)
	assert.Equal(t, []string{"/api/status", "/api/plan"}, *paths, "no lock is checked or taken and no apply is requested")
}

// With --output json an unsafe plan's warning is not printed, so the error
// names each unsafe change and the command that permits them, starting with
// the binary name, and a narrowed apply's command keeps its target.
func TestApplyCmd_JSONOutputNamesTheUnsafeChanges(t *testing.T) {
	drop := []*apitypes.SchemaChangeResponse{{
		Namespace: "orders",
		TableChanges: []*apitypes.TableChangeResponse{{
			TableName: "legacy", Namespace: "orders", DDL: "DROP TABLE `legacy`", ChangeType: "drop", IsUnsafe: true, UnsafeReason: "drops a table",
		}},
	}}
	schemaDir := writeTestSchemaDir(t)

	for _, tc := range []struct {
		name, target, narrowedTo, rerun string
	}{
		{name: "rollout-wide", rerun: "apply -s " + schemaDir + " -e production --allow-unsafe"},
		{name: "narrowed", target: "payments-002", narrowedTo: "prod/payments-002", rerun: "apply -s " + schemaDir + " -e production --target payments-002 --allow-unsafe"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, paths := rolloutPlanServer(t, &apitypes.PlanResponse{PlanID: "plan-orders-1", Engine: "mysql", Changes: drop, NarrowedTo: tc.narrowedTo})
			cmd := ApplyCmd{SchemaDir: schemaDir, Environment: "production", Target: tc.target, NoLock: true, AutoApprove: true, Output: OutputFormatJSON}
			var runErr error
			out := stripAnsi(captureStdout(func() { runErr = cmd.Run(&Globals{Endpoint: server.URL}) }))

			require.Error(t, runErr)
			assert.NotErrorIs(t, runErr, ErrSilent, "nothing else reports the refusal in JSON mode")
			assert.Equal(t, "apply blocked: 1 unsafe change(s) detected (legacy: drops a table); to proceed with these destructive changes, re-run with: "+cliname.Name()+" "+tc.rerun, runErr.Error())
			assert.NotContains(t, out, "Apply blocked", "the human warning is not printed in JSON mode:\n%s", out)
			assert.NotContains(t, *paths, "/api/apply")
		})
	}
}

// A suggested command is pasted into a shell, so a schema directory or
// selector that the shell would split or interpret is quoted.
func TestApplyCmd_SuggestedCommandsQuoteTheirArguments(t *testing.T) {
	drop := []*apitypes.SchemaChangeResponse{{
		Namespace: "orders",
		TableChanges: []*apitypes.TableChangeResponse{{
			TableName: "legacy", Namespace: "orders", DDL: "DROP TABLE `legacy`", ChangeType: "drop", IsUnsafe: true, UnsafeReason: "drops a table",
		}},
	}}
	schemaDir := filepath.Join(t.TempDir(), "my schema")
	require.NoError(t, os.CopyFS(schemaDir, os.DirFS(writeTestSchemaDir(t))))

	t.Run("narrowed rerun", func(t *testing.T) {
		server, _ := rolloutPlanServer(t, &apitypes.PlanResponse{
			PlanID: "plan-orders-1",
			Engine: "mysql",
			Rollout: &apitypes.PlanRolloutResponse{
				Members:     2,
				Independent: true,
				Groups: []*apitypes.PlanMemberGroupResponse{
					{Members: paymentsTargets(1, 1), Primary: true, Changes: []*apitypes.SchemaChangeResponse{}},
					{Members: paymentsTargets(2, 2), Changes: drop},
				},
				Refused: []*apitypes.PlanMemberRefusalResponse{
					{Member: "prod/payments-002", Target: "payments 002", Reason: apitypes.PlanMemberNeedsTarget, Detail: "drops a table", AllowUnsafe: true},
				},
			},
		})
		cmd := ApplyCmd{SchemaDir: schemaDir, Environment: "production", NoLock: true, AutoApprove: true}
		out := stripAnsi(captureStdout(func() { _ = cmd.Run(&Globals{Endpoint: server.URL}) }))
		assert.Contains(t, out, "apply -s '"+schemaDir+"' -e production --target 'payments 002' --allow-unsafe", "%s", out)
	})

	t.Run("unsafe retry", func(t *testing.T) {
		server, _ := rolloutPlanServer(t, &apitypes.PlanResponse{PlanID: "plan-orders-1", Engine: "mysql", Changes: drop})
		cmd := ApplyCmd{SchemaDir: schemaDir, Environment: "production", NoLock: true, AutoApprove: true}
		out := stripAnsi(captureStdout(func() { _ = cmd.Run(&Globals{Endpoint: server.URL}) }))
		assert.Contains(t, out, "apply -s '"+schemaDir+"' -e production --allow-unsafe", "%s", out)
	})
}
