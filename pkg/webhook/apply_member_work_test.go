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

// A PR apply runs the other targets' plans only when the rollout passed its
// contract, a target other than the reviewed one has work, and the comment
// renders every target's plan. Anything else refuses, since the apply would
// otherwise run statements its comment never showed.
func TestRolloutRunsMemberWork(t *testing.T) {
	pending := reviewDriftOutcome{state: driftClean, work: memberWork{pending: 2, members: 3, others: 2, names: []string{"payments-002", "payments-003"}}}
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

	reviewedOnly := reviewDriftOutcome{state: driftClean, work: memberWork{pending: 1, members: 3, names: []string{"payments-001"}}}
	assert.False(t, rolloutRunsMemberWork(reviewedOnly, targetPlans()), "work on the reviewed target alone runs the reviewed plan")

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

	vschema := func(document, diff string) *storage.Plan {
		p := plan(nil)
		p.Namespaces["payments"].Artifacts = map[string]string{storage.VSchemaArtifactName: document}
		p.Namespaces["payments"].Metadata = map[string]string{
			storage.PlanMetadataVSchemaChanged: "true",
			storage.PlanMetadataVSchemaDiff:    diff,
		}
		return p
	}
	const hashed = `{"vindexes":{"hash":{"type":"hash"}}}`
	assert.True(t, sameMemberWork(vschema(hashed, "+ vindex hash"), vschema(hashed, "+ vindex hash")))
	assert.False(t, sameMemberWork(vschema(hashed, "+ vindex hash"), vschema(`{"vindexes":{"xxhash":{"type":"xxhash"}}}`, "+ vindex hash")),
		"a different VSchema document in a namespace that changes its VSchema either way")
	assert.False(t, sameMemberWork(vschema(hashed, "+ vindex hash"), vschema(hashed, "- vindex lookup\n+ vindex hash")),
		"the same document with a different recorded effect")
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

// A confirmation covers the statements its comment showed on every target,
// the reviewed one included. The reviewed target's re-plan at confirm must run
// what the confirmed plan showed, unless it now runs nothing; each other target
// with work must run what the confirmed round planned for it.
func TestRoundCoversWork(t *testing.T) {
	plan := func(ddl string) *storage.Plan {
		return &storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{
			"payments": {Tables: []storage.TableChange{{
				Namespace: "payments", Table: "orders", Operation: "alter", DDL: ddl,
			}}},
		}}
	}
	const region = "ALTER TABLE `orders` ADD COLUMN `region` varchar(16)"
	const wider = "ALTER TABLE `orders` ADD COLUMN `region` varchar(32)"
	members := func(ddl string) map[string]*storage.Plan {
		return map[string]*storage.Plan{"eu/payments-002": plan(ddl)}
	}

	covered, reason := roundCoversWork(plan(region), plan(region), members(region), members(region))
	assert.True(t, covered, reason)

	covered, reason = roundCoversWork(plan(region), &storage.Plan{}, members(region), members(region))
	assert.True(t, covered, "a reviewed target that converged since runs nothing: %s", reason)

	covered, reason = roundCoversWork(plan(region), plan(wider), members(region), members(region))
	assert.False(t, covered)
	assert.Equal(t, "the reviewed target would run statements the confirmed plan did not show", reason)

	covered, reason = roundCoversWork(&storage.Plan{}, plan(region), members(region), members(region))
	assert.False(t, covered, "work on a reviewed target the confirmed plan showed as converged")
	assert.Equal(t, "the reviewed target would run statements the confirmed plan did not show", reason)

	covered, reason = roundCoversWork(plan(region), plan(region), members(region), members(wider))
	assert.False(t, covered)
	assert.Equal(t, "target eu/payments-002 would run statements the confirmed round did not plan", reason)

	// A target whose statement is unchanged but now runs as direct-execution
	// DDL would run write-blocking native DDL the confirmed comment disclosed as
	// running through the schema change engine.
	nowDirect := members(region)
	nowDirect["eu/payments-002"].Namespaces["payments"].Tables[0].ExecutionMode = "direct"
	covered, reason = roundCoversWork(plan(region), plan(region), members(region), nowDirect)
	assert.False(t, covered, "a target that turned direct since the confirmed round is refused")
	assert.Equal(t, "target eu/payments-002 would run statements the confirmed round did not plan", reason)

	covered, reason = roundCoversWork(plan(region), plan(region), map[string]*storage.Plan{}, members(region))
	assert.False(t, covered)
	assert.Equal(t, "target eu/payments-002 has work the confirmed round did not plan", reason)
}

// When the reviewed target has work too, the refusal counts every target that
// needs the change, the reviewed one included, and never calls that list the
// targets other than the reviewed one.
func TestPendingRolloutMessageNamesTheTargetsThatNeedTheChange(t *testing.T) {
	outcome := reviewDriftOutcome{state: driftClean, work: memberWork{pending: 2, members: 2, others: 1, names: []string{"eu", "us"}}}

	assert.Equal(t,
		"2 of 2 targets need this change: eu, us. The plans of the targets other than the reviewed one were not on the comment this apply acts on, so nothing was applied. Run apply again for this environment to review and confirm each target's own plan.",
		pendingRolloutMessage(outcome, false))
}

// An apply-confirm refused because a plan changed after the confirmation tells
// the operator which target's plan changed. When it is the reviewed target's own
// re-plan, the message says so rather than blaming the other targets' plans.
func TestUnconfirmedWorkMessageNamesTheTargetWhosePlanChanged(t *testing.T) {
	plan := func(ddl string) *storage.Plan {
		return &storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{
			"payments": {Tables: []storage.TableChange{{Namespace: "payments", Table: "orders", Operation: "alter", DDL: ddl}}},
		}}
	}
	confirmed := plan("ALTER TABLE `orders` MODIFY COLUMN `region` varchar(255)")
	unchanged := map[string]*storage.Plan{"us/payments-002": plan("ALTER TABLE `orders` ADD COLUMN `region` varchar(32)")}

	covered, reason := roundCoversWork(confirmed, plan("ALTER TABLE `orders` MODIFY COLUMN `region` varchar(32)"), unchanged, unchanged)
	require.False(t, covered)
	message := unconfirmedWorkMessage(memberWork{pending: 2, members: 2, names: []string{"eu/payments-001", "us/payments-002"}}, reason)
	assert.Equal(t,
		"2 of 2 targets need this change: eu/payments-001, us/payments-002. This confirmation no longer covers what the apply would run: the reviewed target would run statements the confirmed plan did not show, so nothing was applied. Run apply again for this environment to review and confirm each target's own plan.",
		message)
	assert.NotContains(t, message, "other than the reviewed one")
}
