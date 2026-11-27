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
	"github.com/block/schemabot/pkg/ui"
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
		assert.Contains(t, out, "### Target `commerce/"+target+"`\n\n```sql\n",
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
	assert.Contains(t, out, "### Target `commerce/orders-002`\n\n```sql\nALTER TABLE `orders` DROP COLUMN `legacy`;\n```\n\n"+glyph.Refused+" **Cannot apply**: 1 change the engine refuses to execute\n- `orders`\n",
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
	assert.Contains(t, out, "rolling out to all 3 targets", "one DDL, one group, every target counted")
	assert.NotContains(t, out, "### ", "one plan renders with no target heading")
	assert.Contains(t, out, "- `orders` on target `commerce/orders-002`\n", "only the refusing target is named")

	// When every target with the change refuses it, the plan summary already
	// counts them, so the refusal names none.
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
// first member. The primary's group points at the primary plan whatever its
// entry carries, since the primary runs the primary plan: the entry here holds
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
	rest, primary, found := strings.Cut(body, "### Target `commerce/orders-001`")
	require.True(t, found, "the primary renders under its own heading")
	assert.Contains(t, rest, "### 2 of 3 targets\n\n- `commerce/orders-002`\n- `commerce/orders-003`", "the larger group leads")
	assert.True(t, strings.Contains(primary, "the full plan for this target is available from the CLI with `schemabot list-plans -e production plan_reviewed`."),
		"the primary's cut DDL names the primary target's plan")
	assert.False(t, strings.Contains(body, "plan_orders_001"), "the primary's own entry identifier is never named")
	assert.True(t, strings.Contains(rest, "the full plan for `commerce/orders-002` is available from the CLI with `schemabot list-plans -e production plan_orders_002` (every target in this group runs the same DDL)."),
		"the group's cut DDL names its first member's plan and whose it is")
	assert.False(t, strings.Contains(body, "plan_orders_003"), "a group names only its first member's plan")
}

// A target's own plan can route a statement to direct execution when the
// primary target's does not, because the verdict belongs to the target that
// runs it. That write-blocking DDL is disclosed under the target that carries
// it, the way the primary plan's own direct changes are, and once rather than
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
			assert.Contains(t, out, "### Target `commerce/orders-002`\n\n```sql\nALTER TABLE `orders` ADD COLUMN `phone` varchar(32);\n```\n\n⚙️ **Direct execution**: 1 change will run as native MySQL DDL, not through Spirit\n- `orders`: table is 12 MiB, within the direct execution bound\n",
				"the direct change is disclosed under the target and DDL that carry it")
			assert.Equal(t, 1, strings.Count(out, "**Direct execution**"), "the target that runs nothing directly carries no disclosure")
		})
	}
}

// A target can carry the same verdict on one table in more than one namespace:
// orders-001 runs `users` directly, or has it refused, in both `ns_0` and
// `ns_1`, while orders-002 runs the same statements through Spirit. Each entry
// names orders-001 once, and orders-002 is not reported as running
// write-blocking DDL, or as refusing a change, that it does not. Either verdict
// names each namespace's table, since each is its own relation, as the CLI and
// the primary plan's comment do.
func TestReviewDriftComment_VerdictInTwoNamespacesNamesItsTargetOnce(t *testing.T) {
	const alter = "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"
	for _, tc := range []struct {
		mode, reason string
		tables       []string
		lines        []string
	}{
		{engine.ExecutionModeDirect, "table is 12 MiB, within the direct execution bound", []string{"ns_0.users", "ns_1.users"},
			[]string{
				"- `ns_0.users` on target `commerce/orders-001`: table is 12 MiB, within the direct execution bound\n",
				"- `ns_1.users` on target `commerce/orders-001`: table is 12 MiB, within the direct execution bound\n",
			}},
		{engine.ExecutionModeBlocked, "", []string{"ns_0.users", "ns_1.users"},
			[]string{"- `ns_0.users` on target `commerce/orders-001`\n", "- `ns_1.users` on target `commerce/orders-001`\n"}},
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
			require.Len(t, verdicts, len(tc.tables))
			for i, v := range verdicts {
				assert.Equal(t, tc.tables[i], v.Table)
				assert.Equal(t, []string{"commerce/orders-001"}, v.Targets, "the target is named once, and the one that runs through Spirit is not named")
				assert.Equal(t, 2, v.TotalTargets)
			}

			out := templates.RenderPlanComment(templates.PlanCommentData{
				Database: "orders", Environment: "production", IsMySQL: true, DeploymentDrift: deploymentDriftPreview(rollup),
			})
			for _, line := range tc.lines {
				assert.Contains(t, out, line, "the disclosure names only the target that carries the verdict")
			}
		})
	}
}

// Lint reads each target's live schema beside the changes it would run, so two
// targets running the same DDL can raise different findings, and a target with
// different DDL raises its own. Every target's findings render in one section
// after the target plans, each once however many targets raised it, naming
// the targets that did when not every target with work did. Error-severity
// findings are unsafe changes, disclosed and gated as such, so they stay out
// of the advisory list.
func TestReviewDriftComment_LintNamesTheTargetsThatRaisedIt(t *testing.T) {
	primary := independentMemberDiff("orders-001", "ALTER TABLE `orders` ADD COLUMN `email` varchar(255)", false)
	primary.LintViolations = []*ternv1.LintViolation{memberLint("orders", "column `email` should not be nullable", "warning")}
	second := independentMemberDiff("orders-002", "ALTER TABLE `orders` ADD COLUMN `phone` varchar(32)", false)
	second.LintViolations = []*ternv1.LintViolation{
		memberLint("orders", "table has no index on `phone`", "warning"),
		memberLint("orders", "column `phone` is unsafe", "error"),
	}
	third := independentMemberDiff("orders-003", "ALTER TABLE `orders` ADD COLUMN `phone` varchar(32)", false)
	third.LintViolations = []*ternv1.LintViolation{
		memberLint("orders", "table has no index on `phone`", "warning"),
		memberLint("orders", "table `orders` has no primary key", "warning"),
	}
	body := renderLintComment(t, primary, second, third)

	assert.Contains(t, body, "💡 **Lint Warnings**: 3 advisory findings\n"+
		"1. `orders` on target `commerce/orders-001`: column `email` should not be nullable\n"+
		"2. `orders` on targets `commerce/orders-002`, `commerce/orders-003`: table has no index on `phone`\n"+
		"3. `orders` on target `commerce/orders-003`: table `orders` has no primary key\n",
		"each finding renders once, naming the targets that raised it")
	assert.NotContains(t, body, "is unsafe", "an error-severity finding is an unsafe change, not advisory lint")
	assertLintFollowsTargetPlans(t, body)
}

// A finding every target with work raises is about the schema they are all
// brought to, so it names no targets, as a single-target plan's would. A
// target already at the schema has nothing to lint and does not make the
// finding read as partial. A finding one target raises from its own live
// state, such as an auto-increment counter near its type's capacity, names
// that target.
func TestReviewDriftComment_LintEveryTargetRaisesNamesNoTargets(t *testing.T) {
	const shared = "table `orders` has no primary key"
	const counter = "AUTO_INCREMENT value 1932735283 is above 85% of the capacity (2147483647) of the auto-inc column's `int` data type"
	primary := independentMemberDiff("orders-001", "ALTER TABLE `orders` ADD COLUMN `email` varchar(255)", false)
	primary.LintViolations = []*ternv1.LintViolation{memberLint("orders", shared, "warning")}
	second := independentMemberDiff("orders-002", "ALTER TABLE `orders` ADD COLUMN `phone` varchar(32)", false)
	second.LintViolations = []*ternv1.LintViolation{memberLint("orders", shared, "warning")}
	third := independentMemberDiff("orders-003", "ALTER TABLE `orders` ADD COLUMN `phone` varchar(32)", false)
	third.LintViolations = []*ternv1.LintViolation{memberLint("orders", shared, "warning"), memberLint("orders", counter, "warning")}
	converged := api.DeploymentPlanDiff{DatabaseType: "mysql", Deployment: "commerce", Target: "orders-004"}

	body := renderLintComment(t, primary, second, third, converged)

	assert.Contains(t, body, "💡 **Lint Warnings**: 2 advisory findings\n"+
		"1. `orders`: "+shared+"\n"+
		"2. `orders` on target `commerce/orders-003`: "+counter+"\n",
		"the shared finding names no target; the counter finding names the one target that raised it")
	assertLintFollowsTargetPlans(t, body)
}

// Findings sort by table so a table's findings sit together, whichever target
// group raised them first. A finding raised by more targets than read inline
// states its coverage on the line and collapses the names beneath it.
func TestReviewDriftComment_LintSortsByTableAndCollapsesWideTargetLists(t *testing.T) {
	const drift = "Column \"score\" in table \"riders\" uses \"float\" data type"
	const shared = "table `docks` has no primary key"
	var diffs []api.DeploymentPlanDiff
	for n := 1; n <= 10; n++ {
		d := independentMemberDiff(fmt.Sprintf("orders-%03d", n), "ALTER TABLE `orders` ADD COLUMN `email` varchar(255)", false)
		if n < 10 {
			d.LintViolations = append(d.LintViolations, memberLint("riders", drift, "warning"))
		}
		d.LintViolations = append(d.LintViolations, memberLint("docks", shared, "warning"))
		diffs = append(diffs, d)
	}

	body := renderLintComment(t, diffs...)

	assert.Contains(t, body, "💡 **Lint Warnings**: 2 advisory findings\n"+
		"1. `docks`: "+shared+"\n"+
		"2. `riders` on 9 of 10 targets: "+ui.CodeQuoteIdentifiers(drift)+"\n"+
		"   <details>\n   <summary>Target names</summary>\n\n"+
		"   ```\n"+
		"   commerce/orders-001  commerce/orders-002  commerce/orders-003\n"+
		"   commerce/orders-004  commerce/orders-005  commerce/orders-006\n"+
		"   commerce/orders-007  commerce/orders-008  commerce/orders-009\n"+
		"   ```\n\n   </details>\n",
		"findings sort by table, and a wide target list collapses under its coverage as a grid")
}

// Several findings raised by the same targets name those targets once, above
// the findings, rather than repeating the list on every line. This is the
// shape of a rollout where some targets already hold tables the others are
// about to create: every finding the existing tables raise comes from the
// same few targets. A finding every target raised still names none, and a
// finding alone in its set of targets keeps them on its own line, ahead of
// the shared sets so it never reads as one of a heading's findings.
func TestReviewDriftComment_LintNamesSharedTargetsOnce(t *testing.T) {
	const shared = "table `docks` has no primary key"
	const created = "Column `created_at` uses `TIMESTAMP`"
	const updated = "Column `updated_at` uses `TIMESTAMP`"
	const counter = "AUTO_INCREMENT value is above 85% of the column's capacity"
	var diffs []api.DeploymentPlanDiff
	for n := 1; n <= 4; n++ {
		d := independentMemberDiff(fmt.Sprintf("orders-%03d", n), "ALTER TABLE `orders` ADD COLUMN `email` varchar(255)", false)
		d.LintViolations = append(d.LintViolations, memberLint("docks", shared, "warning"))
		if n <= 2 {
			d.LintViolations = append(d.LintViolations, memberLint("bikes", created, "warning"), memberLint("bikes", updated, "warning"))
		}
		if n == 4 {
			d.LintViolations = append(d.LintViolations, memberLint("trips", counter, "warning"))
		}
		diffs = append(diffs, d)
	}

	body := renderLintComment(t, diffs...)

	assert.Contains(t, body, "💡 **Lint Warnings**: 4 advisory findings\n"+
		"1. `docks`: "+shared+"\n"+
		"2. `trips` on target `commerce/orders-004`: "+counter+"\n"+
		"\n"+
		"On targets `commerce/orders-001`, `commerce/orders-002`:\n\n"+
		"3. `bikes`: "+created+"\n"+
		"4. `bikes`: "+updated+"\n\n",
		"findings that name their own targets come first, then each set of shared targets is named once above its findings")
	assert.Equal(t, 1, strings.Count(body, "`commerce/orders-001`"), "the targets the findings share are named once")
	assertLintFollowsTargetPlans(t, body)
}

// assertLintFollowsTargetPlans asserts the plan carries one lint section, after
// every target group and before the plan summary.
func assertLintFollowsTargetPlans(t *testing.T, body string) {
	t.Helper()
	assert.Equal(t, 1, strings.Count(body, "**Lint Warnings**"), "every target's findings render in one section")
	lint := strings.Index(body, "💡 **Lint Warnings**")
	assert.Greater(t, lint, strings.LastIndex(body, "### "), "the lint section follows every target group")
	assert.Less(t, lint, strings.Index(body, "📋 **Plan**"), "the lint section precedes the plan summary")
}

func memberLint(table, message, severity string) *ternv1.LintViolation {
	return &ternv1.LintViolation{Table: table, Message: message, Severity: severity, Linter: "test"}
}

// renderLintComment rolls independent members up and renders the plan comment,
// with the primary's own findings as the plan response carries them.
func renderLintComment(t *testing.T, diffs ...api.DeploymentPlanDiff) string {
	t.Helper()
	rollup, err := api.RollupDeploymentDiffs(diffs, driftMembers(diffs), api.PlanIndependent)
	require.NoError(t, err)
	var primaryLint []templates.LintViolationData
	for _, v := range diffs[0].LintViolations {
		primaryLint = append(primaryLint, templates.LintViolationData{Table: v.GetTable(), Message: v.GetMessage()})
	}
	return templates.RenderPlanComment(templates.PlanCommentData{
		Database:        "orders",
		Environment:     "production",
		IsMySQL:         true,
		DatabaseType:    "mysql",
		LintViolations:  primaryLint,
		DeploymentDrift: deploymentDriftPreview(rollup),
	})
}

// An independently planned target refuses four statements on `events` for one
// reason. The target group's disclosure names the table once with its four
// steps, the way the CLI and the primary plan's comment count them, rather
// than as one change.
func TestReviewDriftComment_RefusedStatementsKeepTheirStepCount(t *testing.T) {
	const reason = "the table is 1.4 GiB with its indexes"
	memberDiff := func(target string) api.DeploymentPlanDiff {
		diff := api.DeploymentPlanDiff{DatabaseType: "postgres", Deployment: "commerce", Target: target}
		var changes []*ternv1.TableChange
		for i := range 4 {
			changes = append(changes, &ternv1.TableChange{
				TableName: "events", Ddl: fmt.Sprintf("ALTER TABLE public.events VALIDATE CONSTRAINT c%d", i),
				ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "public",
				ExecutionMode: engine.ExecutionModeBlocked, ModeReason: reason,
			})
		}
		diff.Changes = []*ternv1.SchemaChange{{Namespace: "public", TableChanges: changes}}
		return diff
	}
	diffs := []api.DeploymentPlanDiff{memberDiff("orders-001"), memberDiff("orders-002")}

	rollup, err := api.RollupDeploymentDiffs(diffs, driftMembers(diffs), api.PlanIndependent)
	require.NoError(t, err)
	groups := deploymentPlanGroups(rollup)
	require.Len(t, groups, 1)
	require.Len(t, groups[0].BlockedChanges, 1, "one table, one reason")
	assert.Equal(t, "events", groups[0].BlockedChanges[0].Table)
	assert.Equal(t, 4, groups[0].BlockedChanges[0].Steps)

	out := templates.RenderPlanComment(templates.PlanCommentData{
		Database: "orders", Environment: "production", DeploymentDrift: deploymentDriftPreview(rollup),
	})
	assert.Contains(t, out, "`events` (4 steps)")
}

// Two targets run the same work on `public.events` and `archive.audit`, and
// their engines refuse different parts of it. A refusal both targets carry is
// listed once, whether or not each target's own list qualified the table name,
// and the group qualifies every name once its refusals span both namespaces.
// A target that refuses fewer of the table's statements carries a different
// verdict, listed under it with its own step count.
func TestDeploymentPlanGroups_MatchRefusalsAcrossTargetsOnNamespaceAndSteps(t *testing.T) {
	const reason = "the table is 1.4 GiB with its indexes"
	memberDiff := func(target string, blockedEvents int, blockAudit bool) api.DeploymentPlanDiff {
		mode := func(blocked bool) string {
			if blocked {
				return engine.ExecutionModeBlocked
			}
			return ""
		}
		var events []*ternv1.TableChange
		for i := range 2 {
			events = append(events, &ternv1.TableChange{
				TableName: "events", Ddl: fmt.Sprintf("ALTER TABLE public.events VALIDATE CONSTRAINT c%d", i),
				ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "public",
				ExecutionMode: mode(i < blockedEvents), ModeReason: reason,
			})
		}
		audit := &ternv1.TableChange{
			TableName: "audit", Ddl: "ALTER TABLE archive.audit VALIDATE CONSTRAINT c0",
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER, Namespace: "archive",
			ExecutionMode: mode(blockAudit), ModeReason: reason,
		}
		diff := api.DeploymentPlanDiff{DatabaseType: "postgres", Deployment: "commerce", Target: target}
		diff.Changes = []*ternv1.SchemaChange{
			{Namespace: "public", TableChanges: events},
			{Namespace: "archive", TableChanges: []*ternv1.TableChange{audit}},
		}
		return diff
	}

	for _, tc := range []struct {
		name        string
		second      api.DeploymentPlanDiff
		wantBlocked []templates.BlockedChangeData
	}{
		{
			name:   "same refusal, qualified on one target only",
			second: memberDiff("orders-002", 2, false),
			wantBlocked: []templates.BlockedChangeData{
				{Namespace: "public", Table: "public.events", Reason: reason, Steps: 2},
				{Namespace: "archive", Table: "archive.audit", Reason: reason, Steps: 1, Targets: []string{"commerce/orders-001"}, TotalTargets: 2},
			},
		},
		{
			name:   "fewer statements refused",
			second: memberDiff("orders-002", 1, false),
			wantBlocked: []templates.BlockedChangeData{
				{Namespace: "public", Table: "public.events", Reason: reason, Steps: 2, Targets: []string{"commerce/orders-001"}, TotalTargets: 2},
				{Namespace: "archive", Table: "archive.audit", Reason: reason, Steps: 1, Targets: []string{"commerce/orders-001"}, TotalTargets: 2},
				{Namespace: "public", Table: "public.events", Reason: reason, Steps: 1, Targets: []string{"commerce/orders-002"}, TotalTargets: 2},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			diffs := []api.DeploymentPlanDiff{memberDiff("orders-001", 2, true), tc.second}
			rollup, err := api.RollupDeploymentDiffs(diffs, driftMembers(diffs), api.PlanIndependent)
			require.NoError(t, err)
			groups := deploymentPlanGroups(rollup)
			require.Len(t, groups, 1, "both targets run the same work")
			assert.Equal(t, tc.wantBlocked, groups[0].BlockedChanges)
		})
	}
}
