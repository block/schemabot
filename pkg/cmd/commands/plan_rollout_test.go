package commands

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
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
// already at the desired schema, is planned one group at a time: the groups
// are introduced as divergence, each heading names its targets, the group
// with work leads with its DDL, and the settled target says it has nothing to
// run. The primary's plan alone would have hidden that prod/payments-003 is done.
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
	assert.Contains(t, out, "Targets diverge — what applies where:", "%s", out)
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
	assert.Contains(t, out, "prod/payments-024,\n  and 40 more", "%s", out)
	assert.NotContains(t, out, "prod/payments-025", "names past the cap are counted, not listed")
	assert.Equal(t, 1, strings.Count(out, "ADD COLUMN `region`"), "the DDL is shown once for the whole group")
	for line := range strings.SplitSeq(out, "\n") {
		assert.LessOrEqual(t, len(line), 80, "a folded name line fits the terminal: %q", line)
	}
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
	assert.Contains(t, out, "Targets diverge — what applies where:")
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

func assertBefore(t *testing.T, out, first, second string) {
	t.Helper()
	i, j := strings.Index(out, first), strings.Index(out, second)
	require.GreaterOrEqualf(t, i, 0, "missing %q in:\n%s", first, out)
	require.GreaterOrEqualf(t, j, 0, "missing %q in:\n%s", second, out)
	assert.Lessf(t, i, j, "%q should come before %q in:\n%s", first, second, out)
}

// rolloutPlanServer serves plan as the response to every plan request and
// records the path of every request the CLI makes, answering anything else
// with a server error.
func rolloutPlanServer(t *testing.T, plan *apitypes.PlanResponse) (*httptest.Server, *[]string) {
	t.Helper()
	body, err := json.Marshal(plan)
	require.NoError(t, err)
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if r.URL.Path != "/api/plan" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, writeErr := w.Write(body)
		require.NoError(t, writeErr)
	}))
	t.Cleanup(server.Close)
	return server, &paths
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

// A primary already at the desired schema does not make the rollout a no-op:
// the apply proceeds when another member still has work.
func TestApplyCmd_ProceedsWhenOnlyAnotherMemberHasWork(t *testing.T) {
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

	cmd := ApplyCmd{SchemaDir: writeTestSchemaDir(t), Environment: "production", NoLock: true, AutoApprove: true}
	out := stripAnsi(captureStdout(func() { _ = cmd.Run(&Globals{Endpoint: server.URL}) }))

	assert.NotContains(t, out, "No changes. Your schema is up-to-date.", "%s", out)
	assert.Contains(t, *paths, "/api/apply", "the apply is requested for the members with work:\n%s", out)
}
