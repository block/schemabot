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
