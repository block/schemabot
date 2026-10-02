package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// primaryDropsLegacyPlan is a rollout whose primary target orders_a drops
// orders.legacy over an unfinished copy of orders, while orders_b already has
// the schema. Targets with work lead, so orders_b's group renders last.
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
				{Deployment: "primary", Target: "orders_b", Class: "match"},
				{Deployment: "primary", Target: "orders_a", Primary: true, Class: "planned"},
			},
			Plans: []DeploymentPlanGroup{
				{Members: []string{"primary/orders_b"}},
				{Members: []string{"primary/orders_a"}, Primary: true, Changes: changes},
			},
		},
	}
}

// sectionOf returns the part of body from heading up to the next heading of
// the same level, failing the test when the heading is missing.
func sectionOf(t *testing.T, body, heading string) string {
	t.Helper()
	_, rest, found := strings.Cut(body, heading)
	require.True(t, found, "missing %q in:\n%s", heading, body)
	level := heading[:strings.Index(heading, " ")+1]
	if next := strings.Index(rest, "\n"+level); next >= 0 {
		rest = rest[:next]
	}
	return rest
}

// The unsafe changes and the existing copies a plan comment discloses are read
// from the primary target alone, so when every target's plan renders they sit
// under the primary target's heading. Disclosed after the last group instead,
// they would read as the converged orders_b's, which drops nothing and holds no
// copy.
func TestRenderPlanComment_PrimaryTargetDisclosuresSitUnderItsHeading(t *testing.T) {
	body := RenderPlanComment(primaryDropsLegacyPlan("production"))

	primary := sectionOf(t, body, "### Target `primary/orders_a`")
	assert.Contains(t, primary, "unsafe change detected")
	assert.Contains(t, primary, "**Applying destroys work in progress**: 1 unfinished copy on the target")
	assert.NotContains(t, primary, "On the primary target", "a lone target's heading already names it")

	converged := sectionOf(t, body, "### Target `primary/orders_b`")
	assert.Contains(t, converged, groupNoChanges)
	assert.NotContains(t, converged, "unsafe change")
	assert.NotContains(t, converged, "work in progress")
	assert.Equal(t, 1, strings.Count(body, "unsafe change detected"), "the warning is not repeated plan-wide")
	assert.Equal(t, 1, strings.Count(body, "destroys work in progress"), "the copy is not repeated plan-wide")
}

// A primary target that shares its group with other targets is named on its
// copies, since they were read from it and not from the group's other
// targets.
func TestRenderPlanComment_PrimaryTargetCopiesNameItInASharedGroup(t *testing.T) {
	data := primaryDropsLegacyPlan("production")
	data.DeploymentDrift.Deployments = append(data.DeploymentDrift.Deployments,
		DeploymentDriftEntry{Deployment: "primary", Target: "orders_c", Class: "planned"})
	data.DeploymentDrift.Plans[1].Members = []string{"primary/orders_a", "primary/orders_c"}
	body := RenderPlanComment(data)

	group := sectionOf(t, body, "### 2 of 3 targets")
	assert.Contains(t, group, "On the primary target `primary/orders_a`:\n\n⚠️ **Applying destroys work in progress**")
	assert.Contains(t, group, "unsafe change detected")
	assert.NotContains(t, sectionOf(t, body, "### Target `primary/orders_b`"), "work in progress")
}

// An environment's section in a multi-environment comment discloses its
// primary target's unsafe changes and copies under that target's heading too.
func TestMultiEnvPlanComment_PrimaryTargetDisclosuresSitUnderItsHeading(t *testing.T) {
	staging, production := primaryDropsLegacyPlan("staging"), primaryDropsLegacyPlan("production")
	production.DiscardedCopies = nil
	body := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "orders", DatabaseType: "mysql", IsMySQL: true,
		Environments: []string{"staging", "production"},
		Plans:        map[string]*PlanCommentData{"staging": &staging, "production": &production},
	})

	_, stagingSection, found := strings.Cut(body, "### Staging")
	require.True(t, found, body)
	stagingSection, _, _ = strings.Cut(stagingSection, "### Production")
	primary := sectionOf(t, stagingSection, "#### Target `primary/orders_a`")
	assert.Contains(t, primary, "unsafe change detected")
	assert.Contains(t, primary, "destroys work in progress")
	assert.NotContains(t, sectionOf(t, stagingSection, "#### Target `primary/orders_b`"), "unsafe change")
}
