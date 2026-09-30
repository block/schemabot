package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// A PR apply whose reviewed target is already converged runs the other targets'
// plans only when the rollout passed its contract, some target has work, and
// the comment renders every target's plan. Anything else refuses, since the
// apply would otherwise run statements its comment never showed.
func TestRolloutRunsMemberWork(t *testing.T) {
	pending := reviewDriftOutcome{state: driftClean, work: memberWork{pending: 2, members: 3, names: []string{"payments-002", "payments-003"}}}
	targetPlans := func() *templates.DeploymentDriftData {
		return &templates.DeploymentDriftData{
			Computed: true, Clean: true, Independent: true,
			Plans: []templates.DeploymentPlanGroup{
				{Members: []string{"primary/payments-001"}, Primary: true},
				{Members: []string{"primary/payments-002", "primary/payments-003"}, Changes: []templates.KeyspaceChangeData{{
					Keyspace:   "payments",
					Statements: []string{"ALTER TABLE `orders` ADD COLUMN `region` varchar(16)"},
				}}},
			},
		}
	}

	assert.True(t, rolloutRunsMemberWork(pending, targetPlans()))

	blocked := pending
	blocked.state = driftBlocked
	assert.False(t, rolloutRunsMemberWork(blocked, targetPlans()), "a blocked rollout never runs")

	converged := reviewDriftOutcome{state: driftClean, work: memberWork{members: 3}}
	assert.False(t, rolloutRunsMemberWork(converged, targetPlans()), "a rollout with no work left has nothing to run")

	assert.False(t, rolloutRunsMemberWork(pending, nil), "no preview means the comment showed no target's plan")

	mirrored := targetPlans()
	mirrored.Independent = false
	assert.False(t, rolloutRunsMemberWork(pending, mirrored), "mirrored targets render the reviewed plan alone")

	diverged := targetPlans()
	diverged.Clean = false
	assert.False(t, rolloutRunsMemberWork(pending, diverged), "a diverged rollout renders no target plans")
}

// Confirming an apply covers the statements its comment showed. A member planned
// again at confirm whose statements differ from the confirmed round's, by any
// field an operator reads, is not covered.
func TestSameMemberWork(t *testing.T) {
	plan := func(mutate func(*storage.TableChange)) *storage.Plan {
		change := storage.TableChange{
			Namespace: "payments",
			Table:     "orders",
			Operation: "alter",
			DDL:       "ALTER TABLE `orders` ADD COLUMN `region` varchar(16)",
		}
		if mutate != nil {
			mutate(&change)
		}
		return &storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{
			"payments": {Tables: []storage.TableChange{change}},
		}}
	}

	assert.True(t, sameMemberWork(plan(nil), plan(nil)))
	assert.False(t, sameMemberWork(plan(nil), plan(func(c *storage.TableChange) {
		c.DDL = "ALTER TABLE `orders` ADD COLUMN `region` varchar(32)"
	})), "a different statement")
	assert.False(t, sameMemberWork(plan(nil), plan(func(c *storage.TableChange) {
		c.Table = "refunds"
	})), "a different table")
	assert.False(t, sameMemberWork(plan(nil), plan(func(c *storage.TableChange) {
		c.ExecutionMode = "direct"
	})), "a different execution mode")
	assert.False(t, sameMemberWork(plan(nil), &storage.Plan{}), "work the confirmed round did not plan")

	sharded := plan(nil)
	sharded.Shards = []storage.ShardPlan{{Shard: "-80", Namespace: "payments", Changes: sharded.FlatDDLChanges()}}
	assert.False(t, sameMemberWork(plan(nil), sharded), "a shard's own changes")

	finalized := plan(nil)
	finalized.Namespaces["payments"].Finalize = true
	assert.False(t, sameMemberWork(plan(nil), finalized), "a finalizer")
}

// A rollout member whose apply would discard an unfinished copy is named on the
// work the PR apply reads, so the refusal tells the operator which target holds
// the copy at stake.
func TestMemberWorkOfNamesTheCopyAtStake(t *testing.T) {
	work := tern.ChangeSet{Changes: []*ternv1.SchemaChange{{
		Namespace: "payments",
		TableChanges: []*ternv1.TableChange{{
			Namespace:  "payments",
			TableName:  "orders",
			Ddl:        "ALTER TABLE `orders` ADD COLUMN `region` varchar(16)",
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
		}},
	}}}
	rollup := api.PlanRollup{Clean: true, Planning: api.PlanIndependent, Entries: []api.DeploymentRollupEntry{
		{Deployment: "primary", Target: "payments-001", Class: api.DeploymentPlanned},
		{
			Deployment: "primary", Target: "payments-002", Class: api.DeploymentPlanned, ChangeSet: work,
			ExistingCopiesReported: true,
			ExistingCopies:         []*ternv1.ExistingCopy{{Namespace: "payments", Disposition: "discard", Tables: []string{"orders"}}},
		},
	}}

	got := memberWorkOf(&rollup)
	assert.Equal(t, `target primary/payments-002: applying its plan discards the unfinished copy of orders in namespace "payments"`, got.copyAtStake)
	assert.Equal(t, []string{"primary/payments-002"}, got.names)

	rollup.Entries[1].ExistingCopies = nil
	assert.Empty(t, memberWorkOf(&rollup).copyAtStake, "a member that reported a clean target puts nothing at stake")
}

// A PostgreSQL targets rollout whose reviewed target already has the schema
// while another target still needs a column. The engine does not read a target
// for unfinished copies, so the member's plan comes back without that
// disclosure, and a PR apply refuses its work whatever its flags. The plan
// comment still shows that target's plan, says why it cannot be applied from
// the PR, and offers no apply command, in the single- and the
// multi-environment comment alike.
func TestPlanCommentOffersNoApplyWhenAMembersCopiesWereNotRead(t *testing.T) {
	alter := &ternv1.SchemaChange{Namespace: "public", TableChanges: []*ternv1.TableChange{{
		Namespace:  "public",
		TableName:  "orders",
		Ddl:        "ALTER TABLE orders ADD COLUMN region varchar(16)",
		ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
	}}}
	member := func(target string, changes ...*ternv1.SchemaChange) api.DeploymentPlanDiff {
		return api.DeploymentPlanDiff{DatabaseType: "postgres", Deployment: "primary", Target: target, Changes: changes}
	}
	diffs := []api.DeploymentPlanDiff{member("payments-001"), member("payments-002", alter)}
	rollup, err := api.RollupDeploymentDiffs(diffs, []routing.ExecutionTarget{
		{Deployment: "primary", Target: "payments-001"},
		{Deployment: "primary", Target: "payments-002"},
	}, api.PlanIndependent)
	require.NoError(t, err)
	require.True(t, rollup.Clean)

	outcome := reviewDriftOutcome{state: driftClean, work: memberWorkOf(&rollup)}
	data := templates.PlanCommentData{
		Database: "payments", DatabaseType: "postgres", Environment: "production",
		DeploymentDrift: deploymentDriftPreview(rollup),
	}
	h := &Handler{logger: testLogger()}
	h.annotateMemberApplyRefusal(t.Context(), &data, &apitypes.PlanResponse{PlanID: "plan-reviewed", Database: "payments"}, "production", outcome, "octocat/payments", 7)
	assert.Equal(t, "target primary/payments-002: its data plane did not report whether applying its plan discards an unfinished copy", data.MemberApplyRefusal)

	single := templates.RenderPlanComment(data)
	assert.Contains(t, single, "ALTER TABLE orders ADD COLUMN region varchar(16)", "the other target's plan is still shown")
	assert.Contains(t, single, "**This PR cannot apply the other targets' plans**: the reviewed target already has this schema, but target primary/payments-002: its data plane did not report")
	assert.NotContains(t, single, "schemabot apply", "an apply that is refused whatever its flags is never offered")

	staging := &templates.PlanCommentData{Database: "payments", DatabaseType: "postgres", Environment: "staging"}
	multi := templates.RenderMultiEnvPlanComment(templates.MultiEnvPlanCommentData{
		Database: "payments", DatabaseType: "postgres",
		Environments: []string{"staging", "production"},
		Plans:        map[string]*templates.PlanCommentData{"staging": staging, "production": &data},
	})
	assert.Contains(t, multi, "**This PR cannot apply the other targets' plans**")
	assert.NotContains(t, multi, "schemabot apply")
	assert.NotContains(t, multi, "No changes to apply", "a target still needs the change")
}
