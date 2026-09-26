package templates

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	assert.NotContains(t, out, "matches the reviewed plan")
}

// A clean rollup where one deployment will refuse a change at apply still
// confirms the plan is uniform, then names each deployment with its blocked
// count so a reviewer knows which deployment admission will refuse.
func TestRenderPlanComment_DriftCleanNamesBlockedDeployments(t *testing.T) {
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
				{Deployment: "au", Class: "match", Blocked: 1},
				{Deployment: "us", Class: "match"},
			},
		},
	}

	out := RenderPlanComment(data)
	assert.Contains(t, out, "Same plan on all 3 deployments")
	assert.Contains(t, out, "`eu` (primary) ✅ matches the reviewed plan\n")
	assert.Contains(t, out, "`au` ✅ matches the reviewed plan · blocked: 1\n")
	assert.Contains(t, out, "`us` ✅ matches the reviewed plan\n")
	assert.NotContains(t, out, "`eu` (primary) ✅ matches the reviewed plan · blocked:")
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

// A deployment that both diverged and will refuse changes shows both facts, with
// the refusal count before the drift detail so the sanitized detail stays the
// trailing clause. The same ordering holds on the could-not-verify line.
func TestRenderPlanComment_DriftNotCleanShowsBlockedCounts(t *testing.T) {
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
				{Deployment: "au", Class: "diverged", Blocked: 2, Detail: "1 unexpected change(s) vs the reviewed plan"},
				{Deployment: "us", Class: "errored", Blocked: 3, Detail: "diff failed; see server logs"},
			},
		},
	}

	out := RenderPlanComment(data)
	assert.Contains(t, out, "`au` ⚠️ diverged · blocked: 2 — 1 unexpected change(s) vs the reviewed plan\n")
	assert.Contains(t, out, "`us` ❌ could not verify · blocked: 3 — diff failed; see server logs\n")
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
// when an environment has drift that must be explained (diverged or
// unverifiable) or is a rollout still converging, so a PR is never left with no
// comment where the fleet does not hold the reviewed schema. A clean rollup
// whose targets are all there, and a nil rollup, are not "drift to show".
func TestAnyEnvHasDriftToShow(t *testing.T) {
	drift := func(computed, clean bool) *DeploymentDriftData {
		return &DeploymentDriftData{Computed: computed, Clean: clean}
	}
	converging := func() *DeploymentDriftData {
		d := drift(true, true)
		d.Independent = true
		d.Plans = []DeploymentPlanGroup{
			{Members: []string{"primary/testapp_1"}, Primary: true},
			{Members: []string{"primary/testapp_2"}, Changes: convergingAlter()},
		}
		return d
	}
	converged := func() *DeploymentDriftData {
		d := drift(true, true)
		d.Independent = true
		d.Plans = []DeploymentPlanGroup{
			{Members: []string{"primary/testapp_1", "primary/testapp_2"}, Primary: true},
		}
		return d
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
		{"converging rollout", map[string]*PlanCommentData{"prod": {DeploymentDrift: converging()}}, true},
		{"fully converged rollout", map[string]*PlanCommentData{"prod": {DeploymentDrift: converged()}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := MultiEnvPlanCommentData{Environments: []string{"prod"}, Plans: tc.plans}
			assert.Equal(t, tc.want, AnyEnvHasDriftToShow(data))
		})
	}
}

func convergingAlter() []KeyspaceChangeData {
	return []KeyspaceChangeData{{
		Keyspace:   "testapp",
		Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
	}}
}

// A rollout partway through converging reaches the reviewer on the multi
// environment comment too. Both environments' reviewed targets are already at
// the desired schema while a target in production is not, so the comment cannot
// collapse to the one green line that says no environment changes: that line is
// what a reviewer merges on, and here it would be read as the whole fleet
// holding this schema.
func TestRenderMultiEnvPlanComment_ConvergingRolloutIsNotAllClear(t *testing.T) {
	convergingDrift := func(others ...string) *DeploymentDriftData {
		entries := []DeploymentDriftEntry{
			{Deployment: "primary", Target: "testapp_1", Primary: true, Class: "planned"},
		}
		for _, o := range others {
			entries = append(entries, DeploymentDriftEntry{Deployment: "primary", Target: o, Class: "planned"})
		}
		return &DeploymentDriftData{
			Computed: true, Clean: true, Independent: true,
			Deployments: entries,
			Plans: []DeploymentPlanGroup{
				{Members: []string{"primary/testapp_1"}, Primary: true},
				{Members: []string{"primary/" + others[0]}, Changes: convergingAlter()},
			},
		}
	}
	convergedDrift := &DeploymentDriftData{
		Computed: true, Clean: true, Independent: true,
		Deployments: []DeploymentDriftEntry{
			{Deployment: "primary", Target: "testapp_1", Primary: true, Class: "planned"},
			{Deployment: "primary", Target: "testapp_2", Class: "planned"},
		},
		Plans: []DeploymentPlanGroup{
			{Members: []string{"primary/testapp_1", "primary/testapp_2"}, Primary: true},
		},
	}

	data := MultiEnvPlanCommentData{
		Database: "testapp", DatabaseType: "mysql", IsMySQL: true,
		Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{
			"staging":    {Database: "testapp", Environment: "staging", IsMySQL: true, DeploymentDrift: convergedDrift},
			"production": {Database: "testapp", Environment: "production", IsMySQL: true, DeploymentDrift: convergingDrift("testapp_2")},
		},
	}

	out := RenderMultiEnvPlanComment(data)
	assert.NotContains(t, out, "**No schema changes detected** for any environment.")
	assert.Contains(t, out, "**target `primary/testapp_2`**")
	assert.Contains(t, out, "ADD COLUMN `email`")
	// Staging is genuinely converged, so its own section keeps the green line,
	// and production, which still has work, does not.
	assert.Equal(t, 1, strings.Count(out, "✅ **No schema changes detected**"))

	// With every environment's rollout converged, the all-clear is correct and
	// still renders.
	data.Plans["production"] = &PlanCommentData{Database: "testapp", Environment: "production", IsMySQL: true, DeploymentDrift: convergedDrift}
	assert.Contains(t, RenderMultiEnvPlanComment(data), "**No schema changes detected** for any environment.")
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

// A rollout whose targets all need the same change names every target on the
// one group heading, the way a uniform sharded keyspace names its shards, and
// in the same vocabulary the per-member breakdown uses.
func TestRenderPlanComment_OneTargetPlanNamesEveryTarget(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true, DatabaseType: "mysql",
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
	assert.Contains(t, out, "\n**targets `primary/testapp-001`, `primary/testapp-002`, `eu-west`**\n\n```sql\nALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	assert.NotContains(t, out, "Targets diverge", "one plan is not divergence")
	assert.Contains(t, out, "📋 **Plan**: **1** table to alter")
}

// Targets whose plans differ render the way shards whose plans differ do: the
// divergence is introduced once, each group is headed by the targets it covers,
// and a group already at the desired schema says so in place of DDL.
func TestRenderPlanComment_DivergentTargetsReadLikeDivergentShards(t *testing.T) {
	render := func(plans ...DeploymentPlanGroup) string {
		var members []DeploymentDriftEntry
		for _, g := range plans {
			for range g.Members {
				members = append(members, DeploymentDriftEntry{Deployment: "primary", Class: "planned"})
			}
		}
		members[0].Primary = true
		plans[0].Primary = true
		return RenderPlanComment(PlanCommentData{
			Database: "testapp", Environment: "production", IsMySQL: true, DatabaseType: "mysql",
			Changes: plans[0].Changes,
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

	t.Run("some targets are already there", func(t *testing.T) {
		out := render(group(1, "a", "c"), group(0, "b"))
		assert.Contains(t, out, "Targets diverge — what applies where:\n\n**targets `a`, `c`**\n\n```sql\n")
		assert.Contains(t, out, "**target `b`**\n\n_Already applied — no change._")
		assert.Contains(t, out, "📋 **Plan**: **1** table to alter")
	})
	t.Run("targets need different changes", func(t *testing.T) {
		out := render(group(1, "a"), group(2, "b", "c"))
		assert.Contains(t, out, "Targets diverge — what applies where:\n\n**target `a`**\n\n```sql\n")
		assert.Contains(t, out, "**targets `b`, `c`**\n\n```sql\n")
		assert.NotContains(t, out, "_Already applied")
		assert.Equal(t, 1, strings.Count(out, "📋 **Plan**: "), "the plans are summarized once, together")
	})
	t.Run("a wide group collapses its names", func(t *testing.T) {
		out := render(group(1, "a", "b", "c", "d", "e", "f", "g", "h", "i"), group(0, "j"))
		assert.Contains(t, out, "<details>\n<summary><b>9 of 10 targets</b></summary>\n\n`a`, `b`, `c`, `d`, `e`, `f`, `g`, `h`, `i`\n\n</details>")
		assert.Contains(t, out, "**target `j`**\n\n_Already applied — no change._")
	})
}

// A plan that only rewrites the vschema runs no DDL, and is still work. It is
// shown as the targets' plan and counted in the summary rather than as a schema
// they already hold, which would tell an operator the apply does nothing.
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
	assert.Equal(t, 1, strings.Count(out, "_Already applied — no change._"), "only the target with nothing to run is already applied")
	assert.Contains(t, out, "**target `primary/testapp_2`**\n\n_Already applied — no change._")
	assert.Contains(t, out, "📋 **Plan**: **1** vschema update")
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

// A rollout whose reviewed target is already at the desired schema, while other
// targets are not, must not read as a no-op. The reviewed plan is empty, but a
// reviewer who reads "no schema changes detected" merges believing the fleet
// holds this schema, so the comment shows the plan the other targets still run
// and the reviewed target as already applied. It offers no apply command: a PR
// apply runs from the reviewed plan, and an empty one does not run the others.
func TestRenderPlanComment_ConvergedPrimaryDoesNotReadAsNoOp(t *testing.T) {
	alter := []KeyspaceChangeData{{
		Keyspace:   "testapp",
		Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
	}}
	data := PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true, DatabaseType: "mysql",
		DeploymentDrift: &DeploymentDriftData{
			Computed: true, Clean: true, Independent: true,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "primary", Target: "testapp_1", Primary: true, Class: "planned"},
				{Deployment: "primary", Target: "testapp_2", Class: "planned"},
				{Deployment: "primary", Target: "testapp_3", Class: "planned"},
			},
			Plans: []DeploymentPlanGroup{
				{Members: []string{"primary/testapp_1"}, Primary: true},
				{Members: []string{"primary/testapp_2", "primary/testapp_3"}, Changes: alter},
			},
		},
	}

	out := RenderPlanComment(data)
	assert.NotContains(t, out, "✅ **No schema changes detected**")
	assert.Contains(t, out, "**target `primary/testapp_1`**\n\n_Already applied — no change._")
	assert.Contains(t, out, "**targets `primary/testapp_2`, `primary/testapp_3`**\n\n```sql\nALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	assert.Contains(t, out, "📋 **Plan**: **1** table to alter")
	assert.NotContains(t, out, "schemabot apply", "an empty reviewed plan cannot apply the other targets' plans")
}

// A rollout where every target is already at the desired schema is a no-op, and
// says so once: the comment's no-changes line speaks for every target, so no
// rollout line repeats it.
func TestRenderPlanComment_FullyConvergedRolloutIsStillANoOp(t *testing.T) {
	data := PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true,
		DeploymentDrift: &DeploymentDriftData{
			Computed: true, Clean: true, Independent: true,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "primary", Target: "testapp_1", Primary: true, Class: "planned"},
				{Deployment: "primary", Target: "testapp_2", Class: "planned"},
			},
			Plans: []DeploymentPlanGroup{
				{Members: []string{"primary/testapp_1", "primary/testapp_2"}, Primary: true},
			},
		},
	}

	out := RenderPlanComment(data)
	assert.Contains(t, out, "✅ **No schema changes detected**")
	assert.Equal(t, 1, strings.Count(out, "✅"), "the no-op is said once")
}

// A clean rollout's line is a statement about the rollout, not a verdict, so it
// carries no mark. When nothing is left to apply on any member, the comment's
// no-changes line already says so for all of them, and the rollout line is left
// out rather than repeating it.
func TestRenderPlanComment_NothingToApplyIsSaidOnce(t *testing.T) {
	email := []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"}}}
	mirrored := []DeploymentDriftEntry{
		{Deployment: "eu", Primary: true, Class: "match"},
		{Deployment: "au", Class: "match"},
	}
	independent := []DeploymentDriftEntry{
		{Deployment: "primary", Target: "testapp_1", Primary: true, Class: "planned"},
		{Deployment: "primary", Target: "testapp_2", Class: "planned"},
	}
	render := func(reviewed []KeyspaceChangeData, drift DeploymentDriftData) string {
		return RenderPlanComment(PlanCommentData{
			Database: "testapp", Environment: "production", IsMySQL: true,
			Changes:         reviewed,
			DeploymentDrift: &drift,
		})
	}

	t.Run("mirrored deployments share a plan with work", func(t *testing.T) {
		out := render(email, DeploymentDriftData{Computed: true, Clean: true, Deployments: mirrored})
		assert.Contains(t, out, "\n**Same plan on all 2 deployments** (`eu`, `au`).")
	})
	t.Run("mirrored deployments are all at the schema", func(t *testing.T) {
		out := render(nil, DeploymentDriftData{Computed: true, Clean: true, Deployments: mirrored})
		assert.Contains(t, out, "✅ **No schema changes detected**")
		assert.Equal(t, 1, strings.Count(out, "✅"), "the no-op is said once")
	})
	t.Run("independent targets whose plans were not grouped", func(t *testing.T) {
		out := render(email, DeploymentDriftData{Computed: true, Clean: true, Independent: true, Deployments: independent})
		assert.Contains(t, out, "\n**Planned separately for all 2 targets**")
	})
}

const (
	targetPlanEmail = "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"
	targetPlanIndex = "ALTER TABLE `users` ADD INDEX `idx_email` (`email`)"
)

// targetPlanRollout is a rollout of four independent targets in which the
// reviewed target and one other need the email column, a third needs it with an
// index as well, and the fourth already has both. The reviewed target's group
// carries a stand-in plan, so a test can tell whether the reviewed plan itself
// or the group's own copy of it was rendered.
func targetPlanRollout(reviewed []KeyspaceChangeData) *DeploymentDriftData {
	return &DeploymentDriftData{
		Computed: true, Clean: true, Independent: true,
		Deployments: []DeploymentDriftEntry{
			{Deployment: "primary", Target: "testapp_1", Primary: true, Class: "planned"},
			{Deployment: "primary", Target: "testapp_2", Class: "planned"},
			{Deployment: "primary", Target: "testapp_3", Class: "planned"},
			{Deployment: "primary", Target: "testapp_4", Class: "planned"},
		},
		Plans: []DeploymentPlanGroup{
			{Members: []string{"primary/testapp_1", "primary/testapp_2"}, Primary: true, Changes: reviewed},
			{Members: []string{"primary/testapp_4"}, Changes: []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{targetPlanEmail, targetPlanIndex}}}},
			{Members: []string{"primary/testapp_3"}},
		},
	}
}

// Independent targets each apply their own plan, so the comment shows every
// plan the apply would run, each under the targets that run it. The reviewed
// target's group shows the reviewed plan itself, and the plans are summarized
// once below them, counting each table once however many targets change it.
func TestRenderPlanComment_EachTargetPlanRendersUnderItsTargets(t *testing.T) {
	standIn := []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{"ALTER TABLE `stand_in` ADD COLUMN `x` int"}}}
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "production", IsMySQL: true, DatabaseType: "mysql",
		Changes:         []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{targetPlanEmail}}},
		DeploymentDrift: targetPlanRollout(standIn),
	})

	positions := map[string]int{}
	for _, want := range []string{
		"Targets diverge — what applies where:",
		"**targets `primary/testapp_1`, `primary/testapp_2`**",
		"ADD COLUMN `email`",
		"**target `primary/testapp_4`**",
		"ADD INDEX `idx_email`",
		"**target `primary/testapp_3`**\n\n_Already applied — no change._",
		"📋 **Plan**: **1** table to alter",
	} {
		positions[want] = strings.Index(out, want)
		require.GreaterOrEqual(t, positions[want], 0, "%q missing from:\n%s", want, out)
	}
	assert.Less(t, positions["**targets `primary/testapp_1`, `primary/testapp_2`**"], positions["ADD COLUMN `email`"])
	assert.Less(t, positions["ADD COLUMN `email`"], positions["**target `primary/testapp_4`**"], "each plan's DDL sits under its own targets")
	assert.Less(t, positions["**target `primary/testapp_4`**"], positions["ADD INDEX `idx_email`"])
	assert.Less(t, positions["ADD INDEX `idx_email`"], positions["📋 **Plan**: **1** table to alter"])

	assert.NotContains(t, out, "stand_in", "the reviewed target's group renders the reviewed plan")
	assert.Equal(t, 2, strings.Count(out, "ADD COLUMN `email`"), "the reviewed plan renders once, under its targets, and not again below them")
	assert.Equal(t, 1, strings.Count(out, "📋 **Plan**: "), "the plans are summarized once, together")
}

// Each environment's section renders its rollout's plans the same way, folding
// a plan with more than one change into a details block as the section does for
// its own plan.
func TestRenderMultiEnvPlanComment_EachTargetPlanRendersUnderItsTargets(t *testing.T) {
	reviewed := []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{targetPlanEmail}}}
	out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "testapp", DatabaseType: "mysql", IsMySQL: true,
		Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{
			"staging":    {Database: "testapp", Environment: "staging", IsMySQL: true, DatabaseType: "mysql", Changes: reviewed},
			"production": {Database: "testapp", Environment: "production", IsMySQL: true, DatabaseType: "mysql", Changes: reviewed, DeploymentDrift: targetPlanRollout(reviewed)},
		},
	})

	_, production, found := strings.Cut(out, "Production")
	require.True(t, found, "the production section is missing from:\n%s", out)
	assert.Contains(t, production, "Targets diverge — what applies where:\n\n**targets `primary/testapp_1`, `primary/testapp_2`**")
	otherHeader := strings.Index(production, "**target `primary/testapp_4`**")
	details := strings.Index(production, "<details>\n<summary>Show SQL (2 statements)</summary>")
	assert.GreaterOrEqual(t, otherHeader, 0)
	assert.Greater(t, details, otherHeader, "the two-statement plan folds under its own targets")
	assert.Less(t, details, strings.Index(production, "ADD INDEX `idx_email`"))
	assert.Contains(t, production, "**target `primary/testapp_3`**\n\n_Already applied — no change._")
	assert.Equal(t, 1, strings.Count(production, "📋 **Plan**: "), "the plans are summarized once, together")
}

// The comment's DDL budget is shared across every block it renders, so a
// rollout that renders each target's plan counts every one of them.
func TestCountCommentDDLBlocks_CountsEveryTargetPlan(t *testing.T) {
	reviewed := []KeyspaceChangeData{{Keyspace: "testapp", Statements: []string{targetPlanEmail}}}
	data := PlanCommentData{Changes: reviewed, DeploymentDrift: targetPlanRollout(reviewed)}
	assert.Equal(t, 2, countCommentDDLBlocks(data))

	data.DeploymentDrift = nil
	assert.Equal(t, 1, countCommentDDLBlocks(data))
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
