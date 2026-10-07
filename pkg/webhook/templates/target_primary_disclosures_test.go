package templates

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// primaryDropsLegacyPlan is a rollout in which orders_a, the target planned
// first, drops orders.legacy over an unfinished copy of orders, while orders_b
// already has the schema.
func primaryDropsLegacyPlan(environment string) PlanCommentData {
	changes := []KeyspaceChangeData{{Keyspace: "orders", Statements: []string{"DROP TABLE `legacy`"}}}
	return PlanCommentData{
		Database: "orders", Environment: environment, IsMySQL: true,
		Changes:          changes,
		HasUnsafeChanges: true,
		UnsafeChanges:    []UnsafeChangeData{{Table: "legacy", Reason: "drops the table and all of its rows", ChangeType: "drop"}},
		DiscardedCopies: []ExistingCopyData{{
			Namespace: "orders", Tables: []string{"orders"}, Reason: engine.DiscardStatementDiffers, Age: "3h 12m",
			Statement: "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)",
		}},
		DeploymentDrift: &DeploymentDriftData{
			Computed: true, Clean: true, Independent: true,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "shop", Target: "orders_b", Class: "match"},
				{Deployment: "shop", Target: "orders_a", Primary: true, Class: "planned"},
			},
			Plans: []DeploymentPlanGroup{
				{Members: []string{"shop/orders_b"}},
				{Members: []string{"shop/orders_a"}, Primary: true, Changes: changes},
			},
		},
	}
}

// sectionOf returns the part of body from heading up to the next heading of
// the same level, or up to the rule above the comment's footer when it is the
// last section, failing the test when the heading is missing.
func sectionOf(t *testing.T, body, heading string) string {
	t.Helper()
	_, rest, found := strings.Cut(body, heading)
	require.True(t, found, "missing %q in:\n%s", heading, body)
	level := heading[:strings.Index(heading, " ")+1]
	if next := strings.Index(rest, "\n"+level); next >= 0 {
		rest = rest[:next]
	}
	if footer := strings.Index(rest, "\n---\n"); footer >= 0 {
		rest = rest[:footer]
	}
	return rest
}

// withDistinctPlan adds orders_c, which runs a plan of its own, so each plan
// renders under a heading naming the targets that run it.
func withDistinctPlan(data PlanCommentData) PlanCommentData {
	drift := *data.DeploymentDrift
	drift.Deployments = append(slices.Clone(drift.Deployments), DeploymentDriftEntry{Deployment: "shop", Target: "orders_c", Class: "planned"})
	drift.Plans = append(slices.Clone(drift.Plans), DeploymentPlanGroup{
		Members: []string{"shop/orders_c"},
		Changes: []KeyspaceChangeData{{Keyspace: "orders", Statements: []string{"ALTER TABLE `orders` ADD COLUMN `note` varchar(64)"}}},
	})
	data.DeploymentDrift = &drift
	return data
}

// When every target with work runs the same plan, it renders once with no
// target heading. The unsafe changes it discloses are every such target's,
// so they name none. The existing copies are read from one target alone, so
// they name it: a reader would otherwise take them for every target's. The
// converged orders_b gets no section, only the count and names under the plan
// summary.
func TestRenderPlanComment_RolloutDisclosuresNameOnlyTheTargetCopiesWereReadFrom(t *testing.T) {
	body := RenderPlanComment(primaryDropsLegacyPlan("production"))

	assert.NotContains(t, body, "### ", "one plan renders with no target heading")
	assert.Contains(t, body, "On target `shop/orders_a`:\n\n⚠️ **Applying destroys work in progress**: 1 unfinished copy on the target")
	assert.Equal(t, 1, strings.Count(body, "unsafe change detected"), "the warning is said once")
	assert.Equal(t, 1, strings.Count(body, "destroys work in progress"), "the copy is said once")
	assert.NotContains(t, body, groupNoChanges, "the converged target has no section")
	assert.Contains(t, body, "rolling out to 1 of 2 targets (1 already has it)")
	assert.Contains(t, body, "Needs it: `shop/orders_a` · Already has it: `shop/orders_b`")
}

// When targets run different plans, each plan's heading names the targets
// that run it, so the Targets block under the plan summary does not list them
// again: it lists only the targets already at the schema, and is left out
// when every target has work.
func TestRenderPlanComment_HeadedRolloutListsOnlyTargetsThatAlreadyHaveIt(t *testing.T) {
	body := RenderPlanComment(withDistinctPlan(primaryDropsLegacyPlan("production")))

	assert.Contains(t, body, "### Target `shop/orders_c`")
	assert.Contains(t, body, "<details>\n<summary>Targets</summary>\n\nAlready has it: `shop/orders_b`\n\n</details>\n")
	assert.NotContains(t, body, "Needs it:")

	data := withDistinctPlan(primaryDropsLegacyPlan("production"))
	drift := *data.DeploymentDrift
	drift.Plans = slices.DeleteFunc(slices.Clone(drift.Plans), DeploymentPlanGroup.Empty)
	drift.Deployments = slices.DeleteFunc(slices.Clone(drift.Deployments), func(d DeploymentDriftEntry) bool { return d.Target == "orders_b" })
	data.DeploymentDrift = &drift
	body = RenderPlanComment(data)

	assert.Contains(t, body, "### Target `shop/orders_c`")
	assert.NotContains(t, body, "<summary>Targets</summary>", "every target with work is named by its plan's heading")
}

// A target that shares its plan with other targets is still named on its
// copies, since they were read from it and not from the other targets.
func TestRenderPlanComment_RolloutCopiesNameTheirTargetInASharedPlan(t *testing.T) {
	data := primaryDropsLegacyPlan("production")
	data.DeploymentDrift.Deployments = append(data.DeploymentDrift.Deployments,
		DeploymentDriftEntry{Deployment: "shop", Target: "orders_c", Class: "planned"})
	data.DeploymentDrift.Plans[1].Members = []string{"shop/orders_a", "shop/orders_c"}
	body := RenderPlanComment(data)

	assert.NotContains(t, body, "### ", "one plan renders with no target heading")
	assert.Contains(t, body, "On target `shop/orders_a`:\n\n⚠️ **Applying destroys work in progress**")
	assert.Contains(t, body, "rolling out to 2 of 3 targets (1 already has it)")
}

// When targets run different plans, the disclosures read from the target
// planned first sit under its heading, which names it, so the copies do not
// name it again. Disclosed after the last plan instead, they would read as
// orders_c's.
func TestRenderPlanComment_RolloutDisclosuresSitUnderTheirTargetsHeading(t *testing.T) {
	body := RenderPlanComment(withDistinctPlan(primaryDropsLegacyPlan("production")))

	first := sectionOf(t, body, "### Target `shop/orders_a`")
	assert.Contains(t, first, "unsafe change detected")
	assert.Contains(t, first, "**Applying destroys work in progress**: 1 unfinished copy on the target")
	assert.NotContains(t, first, "On target", "a lone target's heading already names it")

	other := sectionOf(t, body, "### Target `shop/orders_c`")
	assert.NotContains(t, other, "unsafe change")
	assert.NotContains(t, other, "work in progress")
	assert.NotContains(t, body, "Target `shop/orders_b`", "the converged target has no section")
	assert.Equal(t, 1, strings.Count(body, "unsafe change detected"), "the warning is not repeated plan-wide")
}

// An environment's section in a multi-environment comment renders its one
// plan the same way: no target heading, with the copies naming their target.
func TestMultiEnvPlanComment_RolloutDisclosuresNameTheirTarget(t *testing.T) {
	staging, production := primaryDropsLegacyPlan("staging"), primaryDropsLegacyPlan("production")
	production.DiscardedCopies = nil
	body := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "orders", DatabaseType: "mysql", IsMySQL: true,
		Environments: []string{"staging", "production"},
		Plans:        map[string]*PlanCommentData{"staging": &staging, "production": &production},
	})

	_, stagingSection, found := strings.Cut(body, "### Staging")
	require.True(t, found, body)
	stagingSection, productionSection, _ := strings.Cut(stagingSection, "### Production")
	assert.NotContains(t, stagingSection, "#### Target", "one plan renders with no target heading")
	assert.Contains(t, stagingSection, "unsafe change detected")
	assert.Contains(t, stagingSection, "On target `shop/orders_a`:")
	assert.NotContains(t, productionSection, "work in progress")
}

// The unsafe refusal lists every unsafe change itself, so the plan above it
// does not repeat them, and it discloses no existing copies, as a single
// target's refusal does not.
func TestRenderUnsafeChangesBlocked_RolloutPlanRepeatsNothing(t *testing.T) {
	body := RenderUnsafeChangesBlocked(primaryDropsLegacyPlan("production"))

	assert.Contains(t, body, "Apply rejected**: 1 unsafe change detected\n1. `legacy`")
	assert.Equal(t, 1, strings.Count(body, "drops the table and all of its rows"), "the refusal lists the drop once")
	assert.NotContains(t, body, "### ", "one plan renders with no target heading")
	assert.NotContains(t, body, "work in progress")
}

// An unsafe finding on a table another pull request changed still names that
// pull request on the finding, and the attribution does not also get a
// section of its own.
func TestRenderPlanComment_RolloutUnsafeFindingCarriesAttribution(t *testing.T) {
	data := primaryDropsLegacyPlan("production")
	data.Repository = "acme/orders"
	data.AttributedChanges = []AttributedChangeData{{Table: "legacy", Repository: "acme/orders", PullRequest: 4790}}
	body := RenderPlanComment(data)

	assert.Equal(t, 1, strings.Count(body, "`legacy`: drops the table and all of its rows (changed by open PR [#4790](https://github.com/acme/orders/pull/4790))"))
	assert.NotContains(t, body, "Check before applying")
}

// When the primary target creates a table that another target already has and
// changes unsafely, the attribution is about the other target's change, not
// the creation. A creation destroys nothing, so its finding carries no note,
// and the attribution keeps a section of its own rather than folding onto it.
func TestRenderPlanComment_PrimaryTargetCreationNeverCarriesAnotherTargetsAttribution(t *testing.T) {
	data := primaryDropsLegacyPlan("production")
	data.Repository = "acme/orders"
	create := []KeyspaceChangeData{{Keyspace: "orders", Statements: []string{"CREATE TABLE `stations` (`id` bigint NOT NULL, `seen_at` timestamp NULL, PRIMARY KEY (`id`))"}}}
	data.Changes = create
	data.DiscardedCopies = nil
	data.UnsafeChanges = []UnsafeChangeData{{Table: "stations", Reason: "has_timestamp: column seen_at uses TIMESTAMP", ChangeType: "create"}}
	data.DeploymentDrift.Deployments[0].Class = "planned"
	data.DeploymentDrift.Plans = []DeploymentPlanGroup{
		{Members: []string{"primary/orders_a"}, Primary: true, Changes: create},
		{
			Members:       []string{"primary/orders_b"},
			Changes:       []KeyspaceChangeData{{Keyspace: "orders", Statements: []string{"ALTER TABLE `stations` DROP COLUMN `legacy_ref`"}}},
			UnsafeChanges: []UnsafeChangeData{{Table: "stations", Reason: "DROP COLUMN discards the column's data", ChangeType: "alter"}},
		},
	}
	data.AttributedChanges = []AttributedChangeData{{Table: "stations", Repository: "acme/orders", PullRequest: 4790}}
	body := RenderPlanComment(data)

	primary := sectionOf(t, body, "### Target `primary/orders_a`")
	assert.Contains(t, primary, "`stations`: has_timestamp: column seen_at uses TIMESTAMP\n")
	assert.NotContains(t, body, "(changed by open PR", "no finding carries the attribution")
	assert.Contains(t, body, "**Check before applying**: 1 destructive change SchemaBot cannot attribute to this PR")
}
