package webhook

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/engine"
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

	assert.Contains(t, out, "Planned separately for all 3 targets")
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
// only place a non-primary member's refused DDL reaches the reviewer.
func TestReviewDriftComment_IndependentSurfacesBlockedMember(t *testing.T) {
	diffs := []api.DeploymentPlanDiff{
		independentMemberDiff("orders-001", "ALTER TABLE `orders` ADD COLUMN `email` varchar(255)", false),
		independentMemberDiff("orders-002", "ALTER TABLE `orders` DROP COLUMN `legacy`", true),
	}

	rollup, out := renderDriftComment(t, diffs, api.PlanIndependent)
	require.True(t, rollup.Clean, "a blocked change does not make the rollup unclean")
	assert.Equal(t, 0, rollup.Entries[0].Blocked)
	assert.Equal(t, 1, rollup.Entries[1].Blocked, "the blocked change must be counted on the member that carries it")

	assert.Contains(t, out, "Planned separately for all 2 targets")
	assert.Contains(t, out, "`commerce` ✅ planned against its own schema · blocked: 1")
	assert.NotContains(t, out, "could not verify",
		"a planned member is not an unverifiable one")
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
