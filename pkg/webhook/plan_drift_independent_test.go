package webhook

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/glyph"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// These tests cross the api→webhook boundary on purpose. The api package pins
// what a member is classified as, and the templates package pins how a
// classification renders, but neither can see the pairing: a classification the
// renderer does not know about still classifies correctly and still renders as
// something else. The rollup, the preview and the comment are driven end to end
// here so the contract a member was planned under is the one the reviewer reads.

// independentMemberDiff builds one rollout member's diff carrying its own DDL,
// so members can be given genuinely different plans the way targets that hold
// their own schemas have them.
func independentMemberDiff(target, ddl string, blocked bool) api.DeploymentPlanDiff {
	change := &ternv1.SchemaChange{
		Namespace: "orders",
		TableChanges: []*ternv1.TableChange{{
			TableName:  "orders",
			Ddl:        ddl,
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
			Namespace:  "orders",
		}},
	}
	if blocked {
		change.TableChanges[0].ExecutionMode = engine.ExecutionModeBlocked
	}
	return api.DeploymentPlanDiff{
		DatabaseType: "mysql",
		Deployment:   "commerce",
		Target:       target,
		Changes:      []*ternv1.SchemaChange{change},
	}
}

func driftMembers(diffs []api.DeploymentPlanDiff) []routing.ExecutionTarget {
	members := make([]routing.ExecutionTarget, len(diffs))
	for i, d := range diffs {
		members[i] = routing.ExecutionTarget{Deployment: d.Deployment, Target: d.Target}
	}
	return members
}

// renderDriftComment rolls members up under the given contract and renders the
// plan comment the reviewer would see.
func renderDriftComment(t *testing.T, diffs []api.DeploymentPlanDiff, planning api.MemberPlanning) (api.PlanRollup, string) {
	t.Helper()
	rollup, err := api.RollupDeploymentDiffs(diffs, driftMembers(diffs), planning)
	require.NoError(t, err)
	return rollup, templates.RenderPlanComment(templates.PlanCommentData{
		Database:        "orders",
		Environment:     "production",
		IsMySQL:         true,
		DeploymentDrift: deploymentDriftPreview(rollup),
	})
}

// Three targets that hold their own schemas produce three different plans, and
// the rollup is clean because each was plannable — not because they agree. The
// comment must say what was established. Claiming they share one plan would
// present the reviewer with the opposite of what the rollup checked, and it is
// the claim the mirrored contract makes about the identical input.
func TestReviewDriftComment_IndependentCleanDoesNotClaimAgreement(t *testing.T) {
	diffs := []api.DeploymentPlanDiff{
		independentMemberDiff("orders-001", "ALTER TABLE `orders` ADD COLUMN `email` varchar(255)", false),
		independentMemberDiff("orders-002", "ALTER TABLE `orders` ADD COLUMN `phone` varchar(32)", false),
		independentMemberDiff("orders-003", "ALTER TABLE `orders` ADD COLUMN `region` varchar(8)", false),
	}

	rollup, out := renderDriftComment(t, diffs, api.PlanIndependent)
	assert.True(t, rollup.Clean, "every member was plannable")
	assert.Equal(t, api.PlanIndependent, rollup.Planning, "the rollup must carry the contract it classified under")
	for _, e := range rollup.Entries {
		assert.Equal(t, api.DeploymentPlanned, e.Class, "target %q", e.Target)
	}

	for _, target := range []string{"orders-001", "orders-002", "orders-003"} {
		assert.Contains(t, out, "#### `commerce/"+target+"`\n\n```sql\n",
			"each target's plan renders under it alone, not as one shared plan")
	}
	assert.NotContains(t, out, "Same plan on all",
		"members that were never compared must not be described as agreeing")

	// The same input under the mirrored contract is drift, and blocks. The two
	// renderings differing is the whole point of carrying the contract through.
	mirrored, mirroredOut := renderDriftComment(t, diffs, api.PlanMirrored)
	assert.False(t, mirrored.Clean)
	assert.Contains(t, mirroredOut, "Deployment drift detected")
}

// A target whose plan the engine will refuse is the one thing a clean
// independent rollup still has to surface. Members hold different change sets,
// so the primary's blocked count says nothing about the others — this is the
// only place a non-primary member's refused DDL reaches the reviewer, and it is
// disclosed under the DDL it refuses.
func TestReviewDriftComment_IndependentSurfacesBlockedMember(t *testing.T) {
	diffs := []api.DeploymentPlanDiff{
		independentMemberDiff("orders-001", "ALTER TABLE `orders` ADD COLUMN `email` varchar(255)", false),
		independentMemberDiff("orders-002", "ALTER TABLE `orders` DROP COLUMN `legacy`", true),
	}

	rollup, out := renderDriftComment(t, diffs, api.PlanIndependent)
	require.True(t, rollup.Clean, "a blocked change does not make the rollup unclean")
	assert.Equal(t, 0, rollup.Entries[0].Blocked)
	assert.Equal(t, 1, rollup.Entries[1].Blocked, "the blocked change must be counted on the member that carries it")

	// Both members are addressed by one deployment, so the deployment name alone
	// would leave the reviewer unable to tell which target holds the refused
	// change.
	assert.Contains(t, out, "#### `commerce/orders-002`\n\n```sql\nALTER TABLE `orders` DROP COLUMN `legacy`;\n```\n\n"+glyph.Refused+" **Cannot apply**: 1 change the engine refuses to execute\n- `orders`\n",
		"the refused change is disclosed under the target and DDL that carry it")
	assert.Equal(t, 1, strings.Count(out, "**Cannot apply**"), "the target that refuses nothing carries no disclosure")
	assert.NotContains(t, out, "planned against its own schema", "every target is already named under the plan it runs")
}

// Targets that would run the same DDL share a group, but whether the engine
// refuses that DDL can depend on the target. The disclosure names only the
// targets that refuse it, so the rest of the group is not reported as failing.
func TestReviewDriftComment_BlockedChangeNamesOnlyTheTargetsThatRefuseIt(t *testing.T) {
	const drop = "ALTER TABLE `orders` DROP COLUMN `legacy`"
	diffs := []api.DeploymentPlanDiff{
		independentMemberDiff("orders-001", drop, false),
		independentMemberDiff("orders-002", drop, true),
		independentMemberDiff("orders-003", drop, false),
	}

	rollup, out := renderDriftComment(t, diffs, api.PlanIndependent)
	require.True(t, rollup.Clean)
	assert.Contains(t, out, "`commerce/orders-001`, `commerce/orders-002`, `commerce/orders-003`\n\n```sql\n", "one DDL, one group")
	assert.Contains(t, out, "- `orders` on target `commerce/orders-002`\n", "only the refusing target is named")

	// When every target in the group refuses the change, the heading already
	// names them.
	for i := range diffs {
		diffs[i].Changes[0].TableChanges[0].ExecutionMode = engine.ExecutionModeBlocked
	}
	_, allOut := renderDriftComment(t, diffs, api.PlanIndependent)
	assert.Contains(t, allOut, "**Cannot apply**: 1 change the engine refuses to execute\n- `orders`\n")
}

// A member that could not be planned blocks, and the wording has to name that.
// Targets that hold their own schemas are never expected to agree, so calling
// their failure drift would send an operator to reconcile targets that are
// supposed to differ.
func TestReviewDriftComment_IndependentErroredSaysCouldNotPlan(t *testing.T) {
	diffs := []api.DeploymentPlanDiff{
		independentMemberDiff("orders-001", "ALTER TABLE `orders` ADD COLUMN `email` varchar(255)", false),
		independentMemberDiff("orders-002", "ALTER TABLE `orders` ADD COLUMN `phone` varchar(32)", false),
	}
	diffs[1].Err = errors.New("dial tcp 10.0.0.7:3306: connect: connection refused")

	rollup, out := renderDriftComment(t, diffs, api.PlanIndependent)
	require.False(t, rollup.Clean)

	assert.Contains(t, out, "Some targets could not be planned")
	assert.Contains(t, out, "could not plan")
	assert.NotContains(t, out, "Deployment drift detected")
	assert.NotContains(t, out, "10.0.0.7",
		"the raw producer error must not reach the PR comment")

	summary := summarizeReviewDrift(rollup)
	assert.Contains(t, summary, "could not plan: commerce")
	assert.NotContains(t, summary, "drift blocks apply")
}

// Each target group names the stored plan its members run, so a comment that
// cuts a group's DDL can point at the plan that holds all of it. A group takes
// its first member's stored plan, and the comment names it under the group's
// first member. The primary's group points at the reviewed plan whatever its
// entry carries, since the primary runs the reviewed plan: the entry here holds
// an identifier of its own so the comment is seen to ignore it.
func TestDeploymentPlanGroups_CarryEachGroupsStoredPlan(t *testing.T) {
	wide := func(column string) string {
		columns := make([]string, 3000)
		for i := range columns {
			columns[i] = fmt.Sprintf("ADD COLUMN `%s_%d` varchar(255)", column, i)
		}
		return "ALTER TABLE `orders` " + strings.Join(columns, ", ")
	}
	diffs := []api.DeploymentPlanDiff{
		independentMemberDiff("orders-001", wide("email"), false),
		independentMemberDiff("orders-002", wide("phone"), false),
		independentMemberDiff("orders-003", wide("phone"), false),
	}
	rollup, err := api.RollupDeploymentDiffs(diffs, driftMembers(diffs), api.PlanIndependent)
	require.NoError(t, err)
	rollup.Entries[0].PlanIdentifier = "plan_orders_001"
	rollup.Entries[1].PlanIdentifier = "plan_orders_002"
	rollup.Entries[2].PlanIdentifier = "plan_orders_003"

	groups := deploymentPlanGroups(rollup)
	require.Len(t, groups, 2)
	assert.True(t, groups[0].Primary)
	assert.Equal(t, []string{"commerce/orders-002", "commerce/orders-003"}, groups[1].Members)
	assert.Equal(t, "plan_orders_002", groups[1].PlanID)

	body := templates.RenderPlanComment(templates.PlanCommentData{
		Database:        "orders",
		Environment:     "production",
		IsMySQL:         true,
		PlanID:          "plan_reviewed",
		DeploymentDrift: deploymentDriftPreview(rollup),
	})
	primary, rest, found := strings.Cut(body, "`commerce/orders-002`, `commerce/orders-003`")
	require.True(t, found, "the second group renders under its own heading")
	assert.True(t, strings.Contains(primary, "the full plan for this target is available from the CLI with `schemabot list-plans -e production plan_reviewed`."),
		"the primary's cut DDL names the reviewed plan")
	assert.False(t, strings.Contains(body, "plan_orders_001"), "the primary's own entry identifier is never named")
	assert.True(t, strings.Contains(rest, "the full plan for `commerce/orders-002` is available from the CLI with `schemabot list-plans -e production plan_orders_002` (every target in this group runs the same DDL)."),
		"the group's cut DDL names its first member's plan and whose it is")
	assert.False(t, strings.Contains(body, "plan_orders_003"), "a group names only its first member's plan")
}

// A target's own plan can route a statement to direct execution when the
// reviewed target's does not, because the verdict belongs to the target that
// runs it. That write-blocking DDL is disclosed under the target that carries
// it, the way the reviewed plan's own direct changes are, and once rather than
// again plan-wide. The verdict is matched the way apply admission matches it,
// ignoring case, so a verdict spelled in another case that runs directly is
// disclosed too.
func TestReviewDriftComment_IndependentDisclosesDirectMember(t *testing.T) {
	for _, mode := range []string{engine.ExecutionModeDirect, strings.ToUpper(engine.ExecutionModeDirect)} {
		t.Run(mode, func(t *testing.T) {
			diffs := []api.DeploymentPlanDiff{
				independentMemberDiff("orders-001", "ALTER TABLE `orders` ADD COLUMN `email` varchar(255)", false),
				independentMemberDiff("orders-002", "ALTER TABLE `orders` ADD COLUMN `phone` varchar(32)", false),
			}
			direct := diffs[1].Changes[0].TableChanges[0]
			direct.ExecutionMode = mode
			direct.ModeReason = "table is 12 MiB, within the direct execution bound"

			rollup, out := renderDriftComment(t, diffs, api.PlanIndependent)
			require.True(t, rollup.Clean)
			assert.Contains(t, out, "#### `commerce/orders-002`\n\n```sql\nALTER TABLE `orders` ADD COLUMN `phone` varchar(32);\n```\n\n⚙️ **Direct execution**: 1 change will run as native MySQL DDL, not through Spirit\n- `orders`: table is 12 MiB, within the direct execution bound\n",
				"the direct change is disclosed under the target and DDL that carry it")
			assert.Equal(t, 1, strings.Count(out, "**Direct execution**"), "the target that runs nothing directly carries no disclosure")
		})
	}
}

// A target can carry the same verdict on one table in more than one namespace:
// orders-001 runs `users` directly in both `ns_0` and `ns_1`, while orders-002
// runs the same statements through Spirit. The disclosure names orders-001
// once, and still names it, so orders-002 is not reported as running
// write-blocking DDL, or as refusing a change, that it does not.
func TestReviewDriftComment_VerdictInTwoNamespacesNamesItsTargetOnce(t *testing.T) {
	const alter = "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"
	for _, tc := range []struct {
		mode, reason, line string
	}{
		{engine.ExecutionModeDirect, "table is 12 MiB, within the direct execution bound", "- `users` on target `commerce/orders-001`: table is 12 MiB, within the direct execution bound\n"},
		{engine.ExecutionModeBlocked, "", "- `users` on target `commerce/orders-001`\n"},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			memberDiff := func(target, mode string) api.DeploymentPlanDiff {
				diff := api.DeploymentPlanDiff{DatabaseType: "mysql", Deployment: "commerce", Target: target}
				for _, ns := range []string{"ns_0", "ns_1"} {
					diff.Changes = append(diff.Changes, &ternv1.SchemaChange{
						Namespace: ns,
						TableChanges: []*ternv1.TableChange{{
							TableName: "users", Ddl: alter, ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
							Namespace: ns, ExecutionMode: mode, ModeReason: tc.reason,
						}},
					})
				}
				return diff
			}
			diffs := []api.DeploymentPlanDiff{memberDiff("orders-001", tc.mode), memberDiff("orders-002", "")}

			rollup, err := api.RollupDeploymentDiffs(diffs, driftMembers(diffs), api.PlanIndependent)
			require.NoError(t, err)
			groups := deploymentPlanGroups(rollup)
			require.Len(t, groups, 1, "one DDL, one group")
			verdicts := groups[0].BlockedChanges
			if tc.mode == engine.ExecutionModeDirect {
				verdicts = nil
				for _, dc := range groups[0].DirectChanges {
					verdicts = append(verdicts, templates.BlockedChangeData(dc))
				}
			}
			require.Len(t, verdicts, 1, "one table, one reason")
			assert.Equal(t, []string{"commerce/orders-001"}, verdicts[0].Targets, "the target is named once, and the one that runs through Spirit is not named")
			assert.Equal(t, 2, verdicts[0].TotalTargets)

			out := templates.RenderPlanComment(templates.PlanCommentData{
				Database: "orders", Environment: "production", IsMySQL: true, DeploymentDrift: deploymentDriftPreview(rollup),
			})
			assert.Contains(t, out, tc.line, "the disclosure names only the target that carries the verdict")
		})
	}
}
