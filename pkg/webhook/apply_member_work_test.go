package webhook

import (
	"errors"
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
// contract, a target other than the primary has work, and the comment
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
	assert.False(t, rolloutRunsMemberWork(reviewedOnly, targetPlans()), "work on the primary target alone runs the primary target's plan")

	assert.False(t, rolloutRunsMemberWork(pending, nil), "no preview means the comment showed no target's plan")

	mirrored := targetPlans()
	mirrored.Independent = false
	assert.False(t, rolloutRunsMemberWork(pending, mirrored), "mirrored targets render the primary target's plan alone")

	diverged := targetPlans()
	diverged.Clean = false
	assert.False(t, rolloutRunsMemberWork(pending, diverged), "a diverged rollout renders no target plans")
}

// Deferred cutover is meaningful whenever a runnable target has engine work,
// even if the primary is all-direct or converged. All-direct rollouts omit the
// flag from the confirmation footer, and unknown targets never establish that
// verdict or make the rollout runnable.
func TestRolloutAllChangesDirect(t *testing.T) {
	direct := independentMemberDiff("eu", "ALTER TABLE `orders` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `user_id`)", false)
	direct.Changes[0].TableChanges[0].ExecutionMode = "direct"
	engine := independentMemberDiff("us", "ALTER TABLE `orders` ADD INDEX `idx_user_id` (`user_id`)", false)
	otherDirect := direct
	otherDirect.Target = "us"
	converged := direct
	converged.Changes = nil
	unknown := engine
	unknown.Err = errors.New("target diff unavailable")
	sharded := engine
	sharded.Shards = []*ternv1.ShardPlan{{Namespace: "orders", Shard: "-80", Changes: engine.Changes[0].TableChanges}}
	finalizer := converged
	finalizer.Target = "us"
	finalizer.Changes = []*ternv1.SchemaChange{{Namespace: "orders", Metadata: map[string]string{apitypes.NeedsFinalizerMetadataKey: "true"}}}
	vschema := converged
	vschema.Target = "us"
	vschema.Changes = []*ternv1.SchemaChange{{Namespace: "orders", Metadata: map[string]string{apitypes.VSchemaChangedMetadataKey: "true"}}}

	for _, tc := range []struct {
		name  string
		diffs []api.DeploymentPlanDiff
		want  bool
		clean bool
	}{
		{name: "primary direct secondary engine", diffs: []api.DeploymentPlanDiff{direct, engine}, clean: true},
		{name: "all direct", diffs: []api.DeploymentPlanDiff{direct, otherDirect}, want: true, clean: true},
		{name: "converged primary secondary direct", diffs: []api.DeploymentPlanDiff{converged, otherDirect}, want: true, clean: true},
		{name: "converged primary secondary engine", diffs: []api.DeploymentPlanDiff{converged, engine}, clean: true},
		{name: "converged rollout", diffs: []api.DeploymentPlanDiff{converged}, clean: true},
		{name: "single direct", diffs: []api.DeploymentPlanDiff{direct}, want: true, clean: true},
		{name: "unknown secondary", diffs: []api.DeploymentPlanDiff{direct, unknown}},
		{name: "engine work on a shard", diffs: []api.DeploymentPlanDiff{direct, sharded}, clean: true},
		{name: "member finalizer", diffs: []api.DeploymentPlanDiff{direct, finalizer}, clean: true},
		{name: "member vschema", diffs: []api.DeploymentPlanDiff{direct, vschema}, clean: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rollup, err := api.RollupDeploymentDiffs(tc.diffs, driftMembers(tc.diffs), api.PlanIndependent)
			require.NoError(t, err)
			require.Equal(t, tc.clean, rollup.Clean)
			allDirect := rolloutAllChangesDirect(rollup)
			assert.Equal(t, tc.want, allDirect)
			preview := deploymentDriftPreview(rollup)
			outcome := reviewDriftOutcome{state: driftClean, work: memberWorkOf(&rollup)}
			if !rollup.Clean {
				outcome.state = driftBlocked
				assert.False(t, rolloutRunsMemberWork(outcome, preview), "an unknown member fails closed")
				return
			}
			if outcome.work.pending == 0 {
				return
			}
			var statements []string
			for _, change := range rollup.Entries[0].ChangeSet.AuthoritativeTableChanges() {
				statements = append(statements, change.GetDdl())
			}
			body := templates.RenderPlanComment(templates.PlanCommentData{
				Database: "orders", Environment: "production", DatabaseType: "mysql", IsMySQL: true,
				IsLocked: true, PendingManualConfirmation: true, DeferCutover: true,
				AllChangesDirect: allDirect, DeploymentDrift: preview,
				Changes: []templates.KeyspaceChangeData{{Keyspace: "orders", Statements: statements}},
			})
			if tc.want {
				assert.Contains(t, body, "schemabot apply-confirm -e production\n")
			} else {
				assert.Contains(t, body, "schemabot apply-confirm -e production --defer-cutover\n")
			}
		})
	}
	assert.False(t, rolloutAllChangesDirect(api.PlanRollup{Clean: true}), "an empty round has no direct work")
}

// Confirming an apply covers the work its comment showed. A member planned
// again at confirm whose work differs from the confirmed round's, by any field
// an operator reads, is not covered, and the difference names the part of the
// work that changed so the refusal never points the operator at statements
// that did not.
func TestMemberWorkDifference(t *testing.T) {
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

	assert.Equal(t, workUnchanged, memberWorkDifference(plan(nil), plan(nil)))
	assert.Equal(t, workStatements, memberWorkDifference(plan(nil), plan(func(c *storage.TableChange) {
		c.DDL = "ALTER TABLE `orders` ADD COLUMN `region` varchar(32)"
	})), "a different statement")
	assert.Equal(t, workStatements, memberWorkDifference(plan(nil), plan(func(c *storage.TableChange) {
		c.Table = "refunds"
	})), "a different table")
	assert.Equal(t, workExecutionMode, memberWorkDifference(plan(nil), plan(func(c *storage.TableChange) {
		c.ExecutionMode = "direct"
	})), "the same statement, now run as direct execution")
	assert.Equal(t, workExecutionMode, memberWorkDifference(plan(func(c *storage.TableChange) {
		c.ExecutionMode = "direct"
	}), plan(func(c *storage.TableChange) {
		c.ExecutionMode = "blocked"
	})), "the same statement, now blocked")
	assert.Equal(t, workStatements, memberWorkDifference(plan(nil), &storage.Plan{}), "work the confirmed round did not plan")
	assert.Equal(t, workUnsafe, memberWorkDifference(plan(nil), plan(func(c *storage.TableChange) {
		c.IsUnsafe = true
		c.UnsafeReason = "drop_index: index idx_email is visible"
	})), "the same statement, now unsafe on this target's schema")
	assert.Equal(t, workUnsafe, memberWorkDifference(plan(func(c *storage.TableChange) {
		c.IsUnsafe = true
		c.UnsafeReason = "has_timestamp: column created_at uses TIMESTAMP"
	}), plan(func(c *storage.TableChange) {
		c.IsUnsafe = true
		c.UnsafeReason = "drop_index: index idx_email is visible"
	})), "the same statement, unsafe for another reason")

	sharded := plan(nil)
	sharded.Shards = []storage.ShardPlan{{Shard: "-80", Namespace: "payments", Changes: sharded.FlatDDLChanges()}}
	assert.Equal(t, workStatements, memberWorkDifference(plan(nil), sharded), "a shard's own changes")
	directShard := plan(nil)
	directShard.Shards = []storage.ShardPlan{{Shard: "-80", Namespace: "payments", Changes: directShard.FlatDDLChanges()}}
	directShard.Shards[0].Changes[0].ExecutionMode = "direct"
	assert.Equal(t, workExecutionMode, memberWorkDifference(sharded, directShard), "a shard's change that now runs as direct execution")
	unsafeShard := plan(nil)
	unsafeShard.Shards = []storage.ShardPlan{{Shard: "-80", Namespace: "payments", Changes: unsafeShard.FlatDDLChanges()}}
	unsafeShard.Shards[0].Changes[0].IsUnsafe = true
	assert.Equal(t, workUnsafe, memberWorkDifference(sharded, unsafeShard), "a shard's change that is now unsafe")

	finalized := plan(nil)
	finalized.Namespaces["payments"].Finalize = true
	assert.Equal(t, workFinalizer, memberWorkDifference(plan(nil), finalized), "a finalizer")

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
	assert.Equal(t, workUnchanged, memberWorkDifference(vschema(hashed, "+ vindex hash"), vschema(hashed, "+ vindex hash")))
	assert.Equal(t, workVSchema, memberWorkDifference(vschema(hashed, "+ vindex hash"), vschema(`{"vindexes":{"xxhash":{"type":"xxhash"}}}`, "+ vindex hash")),
		"a different VSchema document in a namespace that changes its VSchema either way")
	assert.Equal(t, workVSchema, memberWorkDifference(vschema(hashed, "+ vindex hash"), vschema(hashed, "- vindex lookup\n+ vindex hash")),
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

// A PostgreSQL targets rollout whose primary target already has the schema
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
	assert.Contains(t, single, "**This PR cannot apply every target's plan**: target primary/payments-002: its data plane did not report")
	assert.NotContains(t, single, "schemabot apply", "an apply that is refused whatever its flags is never offered")

	staging := &templates.PlanCommentData{Database: "payments", DatabaseType: "postgres", Environment: "staging"}
	multi := templates.RenderMultiEnvPlanComment(templates.MultiEnvPlanCommentData{
		Database: "payments", DatabaseType: "postgres",
		Environments: []string{"staging", "production"},
		Plans:        map[string]*templates.PlanCommentData{"staging": staging, "production": &data},
	})
	assert.Contains(t, multi, "**This PR cannot apply every target's plan**")
	assert.NotContains(t, multi, "schemabot apply")
	assert.NotContains(t, multi, "No changes to apply", "a target still needs the change")
}

// A confirmation covers the statements its comment showed on every target,
// the primary included. The primary target's re-plan at confirm must run
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
	assert.True(t, covered, "a primary target that converged since runs nothing: %s", reason)

	covered, reason = roundCoversWork(plan(region), plan(wider), members(region), members(region))
	assert.False(t, covered)
	assert.Equal(t, "the re-plan of the primary target differs from the confirmed plan in its statements", reason)

	covered, reason = roundCoversWork(&storage.Plan{}, plan(region), members(region), members(region))
	assert.False(t, covered, "work on a primary target the confirmed plan showed as converged")
	assert.Equal(t, "the re-plan of the primary target differs from the confirmed plan in its statements", reason)

	reviewedDirect := plan(region)
	reviewedDirect.Namespaces["payments"].Tables[0].ExecutionMode = "direct"
	covered, reason = roundCoversWork(plan(region), reviewedDirect, members(region), members(region))
	assert.False(t, covered, "a reviewed-target statement that turned direct since the confirmed round is refused")
	assert.Equal(t, "the re-plan of the primary target differs from the confirmed plan in how its statements run", reason)

	covered, reason = roundCoversWork(plan(region), plan(region), members(region), members(wider))
	assert.False(t, covered)
	assert.Equal(t, "the plan of target eu/payments-002 differs from what the confirmed round planned, in its statements", reason)

	// A target whose statement is unchanged but now runs as direct-execution
	// DDL would run write-blocking native DDL the confirmed comment disclosed as
	// running through the schema change engine.
	nowDirect := members(region)
	nowDirect["eu/payments-002"].Namespaces["payments"].Tables[0].ExecutionMode = "direct"
	covered, reason = roundCoversWork(plan(region), plan(region), members(region), nowDirect)
	assert.False(t, covered, "a target that turned direct since the confirmed round is refused")
	assert.Equal(t, "the plan of target eu/payments-002 differs from what the confirmed round planned, in how its statements run", reason)

	covered, reason = roundCoversWork(plan(region), plan(region), map[string]*storage.Plan{}, members(region))
	assert.False(t, covered)
	assert.Equal(t, "target eu/payments-002 has work the confirmed round did not plan", reason)
}

// When the primary target has work too, the refusal counts every target that
// needs the change, the primary included, and never calls that list the
// targets other than the primary.
func TestPendingRolloutMessageNamesTheTargetsThatNeedTheChange(t *testing.T) {
	outcome := reviewDriftOutcome{state: driftClean, work: memberWork{pending: 2, members: 2, others: 1, names: []string{"eu", "us"}}}

	assert.Equal(t,
		"2 of 2 targets need this change: eu, us. The plans of the targets other than the primary were not on the comment this apply acts on, so nothing was applied. Run apply again for this environment to review and confirm each target's own plan.",
		pendingRolloutMessage(outcome, false))
}

// An apply-confirm refused because a plan changed after the confirmation tells
// the operator which target's plan changed. When it is the primary target's own
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
		"2 of 2 targets need this change: eu/payments-001, us/payments-002. This confirmation no longer covers what the apply would run: the re-plan of the primary target differs from the confirmed plan in its statements, so nothing was applied. Run apply again for this environment to review and confirm each target's own plan.",
		message)
	assert.NotContains(t, message, "other than the primary")
}
