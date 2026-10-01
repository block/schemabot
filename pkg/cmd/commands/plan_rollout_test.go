package commands

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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
// whose change its engine refuses is named without a rerun, since no apply
// runs it.
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
}
