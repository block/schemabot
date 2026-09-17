package templates

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// A clean multi-deployment rollup renders one uniform line confirming every
// deployment would plan the reviewed change, so a reviewer sees the change is
// safe to apply everywhere at a glance.
func TestRenderPlanComment_DriftCleanShowsUniformLine(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
		DeploymentDrift: &DeploymentDriftData{
			Computed: true,
			Clean:    true,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "eu", Primary: true, Class: "match"},
				{Deployment: "au", Class: "match"},
				{Deployment: "us", Class: "match"},
			},
		},
	}

	out := RenderPlanComment(data)
	assert.Contains(t, out, "Same plan on all 3 deployments")
	assert.Contains(t, out, "`eu`, `au`, `us`")
}

// A diverged deployment is named with a compact change summary, and an errored
// deployment is called out as unverifiable, so a reviewer knows exactly which
// deployment to reconcile before approving.
func TestRenderPlanComment_DriftNotCleanListsDeployments(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
		DeploymentDrift: &DeploymentDriftData{
			Computed: true,
			Clean:    false,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "eu", Primary: true, Class: "match"},
				{Deployment: "au", Class: "diverged", Detail: "1 unexpected change(s) vs the reviewed plan"},
				{Deployment: "us", Class: "errored", Detail: "diff failed; see server logs"},
			},
		},
	}

	out := RenderPlanComment(data)
	assert.Contains(t, out, "Deployment drift detected")
	assert.Contains(t, out, "`eu` (primary)")
	assert.Contains(t, out, "`au`")
	assert.Contains(t, out, "diverged")
	assert.Contains(t, out, "1 unexpected change(s) vs the reviewed plan")
	assert.Contains(t, out, "`us`")
	assert.Contains(t, out, "could not verify")
	assert.Contains(t, out, "diff failed; see server logs")
}

// Drift on a non-primary deployment must surface even when the reviewed primary
// plan is a clean no-op: the change is a no-op only on the primary, so hiding
// the drift behind the no-changes short-circuit would let a diverged deployment
// pass review unnoticed.
func TestRenderPlanComment_DriftShownWhenPlanIsNoOp(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true,
		Changes: nil, // no-op primary plan
		DeploymentDrift: &DeploymentDriftData{
			Computed: true,
			Clean:    false,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "eu", Primary: true, Class: "match"},
				{Deployment: "au", Class: "diverged", Detail: "2 missing change(s) vs the reviewed plan"},
			},
		},
	}

	out := RenderPlanComment(data)
	assert.Contains(t, out, "Deployment drift detected")
	assert.Contains(t, out, "`au`")
	assert.Contains(t, out, "2 missing change(s) vs the reviewed plan")
}

// When the rollup itself could not be computed, the preview says drift is
// unverified and the check is failing closed, so a reviewer understands why the
// gate is blocking rather than seeing a silent pass.
func TestRenderPlanComment_DriftNotComputedWarns(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
		DeploymentDrift: &DeploymentDriftData{Computed: false},
	}

	out := RenderPlanComment(data)
	assert.Contains(t, out, "Could not verify deployment drift")
	assert.Contains(t, out, "failing closed")
}

// A nil rollup (single-deployment database, or drift not evaluated) renders no
// drift section at all.
func TestRenderPlanComment_NoDriftSectionWhenNil(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
	}

	out := RenderPlanComment(data)
	assert.NotContains(t, out, "Same plan on all")
	assert.NotContains(t, out, "Deployment drift detected")
	assert.NotContains(t, out, "Could not verify deployment drift")
}

// The multi-env no-changes short-circuit must not hide drift: when every
// environment's primary plan is a no-op but a deployment diverged, the comment
// still renders the per-environment drift breakdown instead of the simple
// "no schema changes" message.
func TestRenderMultiEnvPlanComment_NoChangesShortCircuitDoesNotHideDrift(t *testing.T) {
	prod := &PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true,
		Changes: nil,
		DeploymentDrift: &DeploymentDriftData{
			Computed: true,
			Clean:    false,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "eu", Primary: true, Class: "match"},
				{Deployment: "au", Class: "diverged", Detail: "1 unexpected change(s) vs the reviewed plan"},
			},
		},
	}
	data := MultiEnvPlanCommentData{
		Database:     "testapp",
		Environments: []string{"production"},
		Plans:        map[string]*PlanCommentData{"production": prod},
	}

	out := RenderMultiEnvPlanComment(data)
	// The top-level "no changes for any environment" short-circuit must not fire,
	// because it would return before rendering any drift.
	assert.NotContains(t, out, "No schema changes detected** for any environment")
	assert.Contains(t, out, "Deployment drift detected")
	assert.Contains(t, out, "`au`")
}

// Environments are only deduplicated when both their plans and their drift
// rollups render identically. Two environments with the same no-op plan but
// different drift must not collapse into one, or a diverged deployment in one
// environment would be hidden.
func TestPlansIdentical_DifferentDriftPreventsDedup(t *testing.T) {
	clean := &PlanCommentData{
		Database: "testapp", Changes: nil,
		DeploymentDrift: &DeploymentDriftData{
			Computed: true, Clean: true,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "eu", Primary: true, Class: "match"},
				{Deployment: "au", Class: "match"},
			},
		},
	}
	drifted := &PlanCommentData{
		Database: "testapp", Changes: nil,
		DeploymentDrift: &DeploymentDriftData{
			Computed: true, Clean: false,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "eu", Primary: true, Class: "match"},
				{Deployment: "au", Class: "diverged", Detail: "1 unexpected change(s) vs the reviewed plan"},
			},
		},
	}

	assert.False(t, plansIdentical(clean, drifted), "differing drift must block dedup")
	assert.True(t, plansIdentical(clean, clean), "identical plans and drift dedup")
}

// The uniform clean drift line is emitted before the change list, so the
// deployment safety signal stays at the top of the PR comment.
func TestRenderPlanComment_DriftBeforeChangeList(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
		DeploymentDrift: &DeploymentDriftData{
			Computed: true, Clean: true,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "eu", Primary: true, Class: "match"},
				{Deployment: "au", Class: "match"},
			},
		},
	}

	out := RenderPlanComment(data)
	driftIdx := strings.Index(out, "Same plan on all")
	changeIdx := strings.Index(out, "ADD COLUMN")
	assert.Positive(t, driftIdx)
	assert.Positive(t, changeIdx)
	assert.Less(t, driftIdx, changeIdx, "drift line appears before the change list")
}

// AnyEnvHasDriftToShow drives the auto-plan comment-skip decision: it is true
// only when an environment has drift that must be explained (diverged or
// unverifiable), so a red check from drift is never left without a comment. A
// clean or nil rollup is not "drift to show".
func TestAnyEnvHasDriftToShow(t *testing.T) {
	drift := func(computed, clean bool) *DeploymentDriftData {
		return &DeploymentDriftData{Computed: computed, Clean: clean}
	}
	cases := []struct {
		name  string
		plans map[string]*PlanCommentData
		want  bool
	}{
		{"no drift data", map[string]*PlanCommentData{"prod": {}}, false},
		{"clean rollup", map[string]*PlanCommentData{"prod": {DeploymentDrift: drift(true, true)}}, false},
		{"diverged rollup", map[string]*PlanCommentData{"prod": {DeploymentDrift: drift(true, false)}}, true},
		{"uncomputed rollup", map[string]*PlanCommentData{"prod": {DeploymentDrift: drift(false, false)}}, true},
		{"nil plan", map[string]*PlanCommentData{"prod": nil}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := MultiEnvPlanCommentData{Environments: []string{"prod"}, Plans: tc.plans}
			assert.Equal(t, tc.want, AnyEnvHasDriftToShow(data))
		})
	}
}

// When one deployment addresses several targets, the deployment name alone
// labels two different members identically. The plan comment names every member
// of that deployment by its routing pair, while a sibling deployment that
// addresses a single target keeps its plain name.
func TestRenderPlanComment_DriftNamesMultiTargetMembers(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
		DeploymentDrift: &DeploymentDriftData{
			Computed:    true,
			Clean:       false,
			Independent: true,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "primary", Target: "testapp-001", Primary: true, Class: "planned"},
				{Deployment: "primary", Target: "testapp-002", Class: "errored", Detail: "diff failed; see server logs"},
				{Deployment: "eu-west", Target: "orders-eu", Class: "planned"},
			},
		},
	}

	out := RenderPlanComment(data)
	assert.Contains(t, out, "Some targets could not be planned")
	assert.Contains(t, out, "`primary/testapp-001` (primary)")
	assert.Contains(t, out, "`primary/testapp-002`")
	assert.Contains(t, out, "`eu-west`")
	// An independent target has no plan of its own to compare, so its failure
	// is reported as unplanned rather than unverified.
	assert.Contains(t, out, "could not plan")
	assert.NotContains(t, out, "could not verify")
}

// The uniform clean line names members the same way the per-member breakdown
// does, so a reviewer sees one vocabulary for the rollout across both renderings.
func TestRenderPlanComment_DriftCleanNamesMultiTargetMembers(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
		DeploymentDrift: &DeploymentDriftData{
			Computed:    true,
			Clean:       true,
			Independent: true,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "primary", Target: "testapp-001", Primary: true, Class: "planned"},
				{Deployment: "primary", Target: "testapp-002", Class: "planned"},
				{Deployment: "eu-west", Target: "orders-eu", Class: "planned"},
			},
			Plans: []DeploymentPlanGroup{{
				Members: []string{"primary/testapp-001", "primary/testapp-002", "eu-west"},
				Primary: true,
				Changes: planGroupChanges(1),
			}},
		},
	}

	out := RenderPlanComment(data)
	assert.Contains(t, out, "Planned separately for all 3 targets")
	assert.Contains(t, out, "`primary/testapp-001`, `primary/testapp-002`, `eu-west`")
	assert.True(t, strings.Contains(out, "every target needs the same change"))
}

// Targets are free to hold different schemas, so what an operator needs to know
// is how much they agree this round. The comment says how many distinct plans
// the apply would run and how many targets are already there, which the contract
// alone cannot tell them.
func TestRenderPlanComment_PlanGroupsDescribeThisRound(t *testing.T) {
	render := func(plans []DeploymentPlanGroup) string {
		members := make([]DeploymentDriftEntry, 0, 5)
		for _, g := range plans {
			for range g.Members {
				members = append(members, DeploymentDriftEntry{Deployment: "primary", Class: "planned"})
			}
		}
		members[0].Primary = true
		return RenderPlanComment(PlanCommentData{
			Database: "testapp", Environment: "production", IsMySQL: true,
			Changes: []KeyspaceChangeData{{
				Keyspace:   "testapp",
				Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
			}},
			DeploymentDrift: &DeploymentDriftData{
				Computed: true, Clean: true, Independent: true,
				Deployments: members,
				Plans:       plans,
			},
		})
	}
	group := func(statements int, members ...string) DeploymentPlanGroup {
		return DeploymentPlanGroup{Members: members, Changes: planGroupChanges(statements)}
	}

	cases := []struct {
		name   string
		plans  []DeploymentPlanGroup
		expect string
	}{
		{
			name:   "every target needs the same change",
			plans:  []DeploymentPlanGroup{group(1, "a", "b", "c")},
			expect: "every target needs the same change.",
		},
		{
			name:   "some targets are already there",
			plans:  []DeploymentPlanGroup{group(1, "a", "c", "d"), group(0, "b", "e")},
			expect: "3 need this change, 2 are already at this schema.",
		},
		{
			name:   "a single target still needs it",
			plans:  []DeploymentPlanGroup{group(1, "a"), group(0, "b")},
			expect: "1 needs this change, 1 is already at this schema.",
		},
		{
			name:   "targets need different changes",
			plans:  []DeploymentPlanGroup{group(1, "a", "b", "c"), group(2, "d", "e")},
			expect: "2 distinct plans. Each target applies its own.",
		},
		{
			name:   "different changes with some already there",
			plans:  []DeploymentPlanGroup{group(1, "a", "b"), group(2, "c"), group(0, "d", "e")},
			expect: "2 distinct plans across the 3 targets that change; 2 are already at this schema.",
		},
		{
			name:   "the whole fleet is already there",
			plans:  []DeploymentPlanGroup{group(0, "a", "b", "c")},
			expect: "every target is already at this schema.",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Contains(t, render(tc.plans), tc.expect)
		})
	}
}

// A plan that only rewrites the vschema runs no DDL, and is still work. It is
// described as a change the targets need rather than as a schema they already
// hold, which would tell an operator the apply does nothing.
func TestRenderPlanComment_VSchemaOnlyPlanIsNotAlreadyApplied(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "production",
		Changes: []KeyspaceChangeData{{Keyspace: "testapp", VSchemaChanged: true}},
		DeploymentDrift: &DeploymentDriftData{
			Computed: true, Clean: true, Independent: true,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "primary", Target: "testapp_1", Primary: true, Class: "planned"},
				{Deployment: "primary", Target: "testapp_2", Class: "planned"},
			},
			Plans: []DeploymentPlanGroup{
				{
					Members: []string{"primary/testapp_1"},
					Primary: true,
					Changes: []KeyspaceChangeData{{Keyspace: "testapp", VSchemaChanged: true}},
				},
				{Members: []string{"primary/testapp_2"}},
			},
		},
	}

	out := RenderPlanComment(data)
	assert.Contains(t, out, "1 needs this change, 1 is already at this schema.")
}

// A rollup that reaches the comment ungrouped states the contract and nothing
// more. Claiming the targets agree — or that they do not — would be a claim
// about plans nobody compared.
func TestRenderPlanComment_UngroupedIndependentRollupStatesTheContract(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
		DeploymentDrift: &DeploymentDriftData{
			Computed: true, Clean: true, Independent: true,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "primary", Target: "testapp_1", Primary: true, Class: "planned"},
				{Deployment: "primary", Target: "testapp_2", Class: "planned"},
			},
		},
	}

	out := RenderPlanComment(data)
	assert.Contains(t, out, "each target holds its own schema, so their plans are not expected to match.")
	assert.NotContains(t, out, "distinct plans")
}

// A member name reaches the comment from server config, so the rollup renders
// it as a code span it cannot break out of: a name carrying a backtick or a
// line break stays one readable name on one line instead of closing its span
// and writing markdown into a comment operators act on.
func TestRenderPlanComment_DriftContainsHostileMemberNames(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
		DeploymentDrift: &DeploymentDriftData{
			Computed: true,
			Clean:    false,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "eu", Primary: true, Class: "match"},
				{Deployment: "us`\n## Injected", Class: "diverged", Detail: "1 unexpected change(s) vs the reviewed plan"},
			},
		},
	}

	out := RenderPlanComment(data)
	assert.NotContains(t, out, "\n## Injected", "a name must not start a heading of its own")
	assert.Contains(t, out, "`` us` ## Injected ``")
}

// renderGroupedPlan renders a plan comment for an independent rollout whose
// members run the given groups. reviewed is the primary's own plan, which is the
// one the comment would render on its own if the members did not disagree.
func renderGroupedPlan(reviewed []KeyspaceChangeData, plans []DeploymentPlanGroup) string {
	var members []DeploymentDriftEntry
	for _, g := range plans {
		for range g.Members {
			members = append(members, DeploymentDriftEntry{Deployment: "primary", Class: "planned"})
		}
	}
	members[0].Primary = true
	return RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "production", DatabaseType: "mysql", IsMySQL: true,
		Changes: reviewed,
		DeploymentDrift: &DeploymentDriftData{
			Computed: true, Clean: true, Independent: true,
			Deployments: members,
			Plans:       plans,
		},
	})
}

// A rollout whose targets all run the reviewed plan is described by the reviewed
// plan itself. Attributing the one block to a member list would only repeat the
// line above it, so the comment renders exactly as it does without a rollout.
func TestRenderPlanComment_OnePlanRendersAsTheReviewedPlan(t *testing.T) {
	out := renderGroupedPlan(planGroupChanges(1), []DeploymentPlanGroup{
		{Members: []string{"primary/a", "primary/b"}, Primary: true, Changes: planGroupChanges(1)},
	})

	assert.Contains(t, out, "ALTER TABLE `t0` ADD COLUMN `c` int")
	assert.NotContains(t, out, "<details open>")
	assert.NotContains(t, out, "Applying runs each target's own plan")
	// The reviewed plan's own summary, not the rollout's.
	assert.Contains(t, out, "📋 **Plan**: **1** table to alter")
}

// Targets that are free to differ usually do, so each distinct plan is rendered
// under the members that would run it. The reviewed plan is the one an operator
// has already read, so its block is the one left open.
func TestRenderPlanComment_DistinctPlansRenderUnderTheirMembers(t *testing.T) {
	out := renderGroupedPlan(planGroupChanges(1), []DeploymentPlanGroup{
		{Members: []string{"primary/a", "primary/b"}, Primary: true, Changes: planGroupChanges(1)},
		{Members: []string{"eu/c"}, Changes: planGroupChanges(2)},
	})

	assert.Contains(t, out, "<details open>\n<summary><b>`primary/a` (primary), `primary/b` — 1 DDL statement</b></summary>")
	assert.Contains(t, out, "<details>\n<summary><b>`eu/c` — 2 DDL statements</b></summary>")
	// Every group's DDL is on the comment, not the reviewed one's alone.
	assert.Contains(t, out, "ALTER TABLE `t1` ADD COLUMN `c` int")
	assert.Contains(t, out, "⚠️ Applying runs each target's own plan, including the ones collapsed above.")
	assert.Contains(t, out, "📋 **Plan**: 2 distinct plans on 3 targets")
}

// A target already holding the desired schema has no plan to collapse, so it is
// named rather than hidden: a converging fleet is what the operator is watching
// for, and the remaining group's plan stays open.
func TestRenderPlanComment_ConvergedGroupIsNamedNotCollapsed(t *testing.T) {
	out := renderGroupedPlan(planGroupChanges(1), []DeploymentPlanGroup{
		{Members: []string{"primary/a"}, Primary: true, Changes: planGroupChanges(1)},
		{Members: []string{"primary/b", "eu/c"}, Changes: nil},
	})

	assert.Contains(t, out, "**`primary/a` (primary)** — 1 DDL statement")
	assert.Contains(t, out, "**`primary/b`, `eu/c`** — already at this schema, nothing to apply.")
	assert.NotContains(t, out, "<details open>")
	assert.NotContains(t, out, "Applying runs each target's own plan")
	assert.Contains(t, out, "📋 **Plan**: 1 DDL statement on 1 of 3 targets")
}

// The reviewed plan is the primary's, so a primary that is already at the
// desired schema says nothing about its siblings. The comment reports the work
// the apply would do on them rather than reporting the round as a no-op.
func TestRenderPlanComment_ConvergedPrimaryStillShowsSiblingWork(t *testing.T) {
	out := renderGroupedPlan(nil, []DeploymentPlanGroup{
		{Members: []string{"primary/a"}, Primary: true, Changes: nil},
		{Members: []string{"eu/c"}, Changes: planGroupChanges(2)},
	})

	assert.NotContains(t, out, "No schema changes detected")
	assert.Contains(t, out, "**`primary/a` (primary)** — already at this schema, nothing to apply.")
	assert.Contains(t, out, "ALTER TABLE `t1` ADD COLUMN `c` int")
	assert.Contains(t, out, "📋 **Plan**: 2 DDL statements on 1 of 2 targets")
}

// The summary line stands in for the reviewed plan's own, so it counts the
// rollout: how much of the fleet still needs the one plan, or how many plans
// there are when the targets disagree.
func TestRenderPlanComment_PlanSummaryCountsTheRollout(t *testing.T) {
	cases := []struct {
		name   string
		plans  []DeploymentPlanGroup
		expect string
	}{
		{
			name: "part of the fleet is already there",
			plans: []DeploymentPlanGroup{
				{Members: []string{"a", "b"}, Primary: true, Changes: planGroupChanges(1)},
				{Members: []string{"c"}, Changes: nil},
			},
			expect: "📋 **Plan**: 1 DDL statement on 2 of 3 targets",
		},
		{
			name: "the targets disagree",
			plans: []DeploymentPlanGroup{
				{Members: []string{"a"}, Primary: true, Changes: planGroupChanges(1)},
				{Members: []string{"b"}, Changes: planGroupChanges(3)},
				{Members: []string{"c"}, Changes: nil},
			},
			expect: "📋 **Plan**: 2 distinct plans on 3 targets",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Contains(t, renderGroupedPlan(planGroupChanges(1), tc.plans), tc.expect)
		})
	}
}

// A fleet can run to hundreds of targets. Naming every one buries the plan the
// names are a heading for, so a list summarizes past a fixed count while the
// headline still states how many targets the apply covers.
func TestRenderPlanComment_LargeFleetNamesClamp(t *testing.T) {
	members := make([]DeploymentDriftEntry, 144)
	for i := range members {
		members[i] = DeploymentDriftEntry{Deployment: "primary", Target: fmt.Sprintf("my_db_%d", i+1), Class: "planned"}
	}
	members[0].Primary = true

	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "production", DatabaseType: "mysql", IsMySQL: true,
		Changes: planGroupChanges(1),
		DeploymentDrift: &DeploymentDriftData{
			Computed: true, Clean: true, Independent: true,
			Deployments: members,
		},
	})

	assert.Contains(t, out, "**Planned separately for all 144 targets**")
	assert.Contains(t, out, "(`primary/my_db_1`, `primary/my_db_2`, `primary/my_db_3` and 141 more)")
	assert.NotContains(t, out, "my_db_4")
}

// The reviewed member is first in rollout order, so a clamped group heading
// still names the plan an operator has already read.
func TestRenderPlanComment_ClampedGroupHeadingKeepsThePrimary(t *testing.T) {
	big := make([]string, 0, 10)
	for i := range 10 {
		big = append(big, fmt.Sprintf("primary/my_db_%d", i+1))
	}

	out := renderGroupedPlan(planGroupChanges(1), []DeploymentPlanGroup{
		{Members: big, Primary: true, Changes: planGroupChanges(1)},
		{Members: []string{"eu/my_db_11"}, Changes: planGroupChanges(2)},
	})

	assert.Contains(t, out, "`primary/my_db_1` (primary), `primary/my_db_2`, `primary/my_db_3` and 7 more — 1 DDL statement")
	assert.NotContains(t, out, "my_db_4")
}

// The clamp keeps a list readable without costing a reader a name it would have
// been just as short to state.
func TestClampNameList(t *testing.T) {
	names := []string{"a", "b", "c", "d", "e", "f"}
	assert.Equal(t, "a, b, c", clampNameList(names[:3]))
	assert.Equal(t, "a, b, c, d", clampNameList(names[:4]), "one name over the limit is named, not summarized")
	assert.Equal(t, "a, b, c and 2 more", clampNameList(names[:5]))
	assert.Equal(t, "a, b, c and 3 more", clampNameList(names))
}

// A blocked rollup is the only place the comment says which member blocked and
// why, so its list is never summarized however large the fleet is.
func TestRenderPlanComment_BlockedMembersAreAllNamed(t *testing.T) {
	members := make([]DeploymentDriftEntry, 6)
	for i := range members {
		members[i] = DeploymentDriftEntry{Deployment: "primary", Target: fmt.Sprintf("my_db_%d", i+1), Class: "match"}
	}
	members[0].Primary = true
	members[5].Class = "errored"

	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "production", DatabaseType: "mysql", IsMySQL: true,
		Changes: planGroupChanges(1),
		DeploymentDrift: &DeploymentDriftData{
			Computed: true, Clean: false,
			Deployments: members,
		},
	})

	for i := range members {
		assert.Contains(t, out, fmt.Sprintf("`primary/my_db_%d`", i+1))
	}
	assert.NotContains(t, out, "and 3 more")
}

// A vschema rewrite carries no DDL and is still work, so a group's label counts
// it alongside statements rather than describing the plan by its DDL alone.
func TestPlanGroupWorkLabel(t *testing.T) {
	assert.Equal(t, "1 DDL statement", planGroupWorkLabel(1, 0))
	assert.Equal(t, "2 DDL statements", planGroupWorkLabel(2, 0))
	assert.Equal(t, "1 vschema update", planGroupWorkLabel(0, 1))
	assert.Equal(t, "2 vschema updates", planGroupWorkLabel(0, 2))
	assert.Equal(t, "2 DDL statements and 1 vschema update", planGroupWorkLabel(2, 1))
}

// planGroupChanges builds a group plan running the given number of statements.
// A group running none is already at the desired schema.
func planGroupChanges(statements int) []KeyspaceChangeData {
	if statements == 0 {
		return nil
	}
	ks := KeyspaceChangeData{Keyspace: "testapp"}
	for i := range statements {
		ks.Statements = append(ks.Statements, fmt.Sprintf("ALTER TABLE `t%d` ADD COLUMN `c` int", i))
	}
	return []KeyspaceChangeData{ks}
}
