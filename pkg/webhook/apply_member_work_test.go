package webhook

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
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
	unsafeFor := func(reason string) *storage.Plan {
		return plan(func(c *storage.TableChange) {
			c.IsUnsafe = true
			c.UnsafeReason = reason
		})
	}
	const createdAt = "Column `created_at` uses `TIMESTAMP`"
	const updatedAt = "Column `updated_at` uses `TIMESTAMP`"
	assert.Equal(t, workUnchanged, memberWorkDifference(unsafeFor(createdAt+"; "+updatedAt), unsafeFor(updatedAt+"; "+createdAt)),
		"the same findings reported in another order")
	assert.Equal(t, workUnsafe, memberWorkDifference(unsafeFor(createdAt+"; "+updatedAt), unsafeFor(createdAt)),
		"a finding the confirmed round showed is gone")
	assert.Equal(t, workUnsafe, memberWorkDifference(unsafeFor(createdAt), unsafeFor(createdAt+"; "+updatedAt)),
		"a finding the confirmed round did not show")
	const sameOnTwoColumns = "Column uses `TIMESTAMP`"
	assert.Equal(t, workUnsafe, memberWorkDifference(unsafeFor(sameOnTwoColumns), unsafeFor(sameOnTwoColumns+"; "+sameOnTwoColumns)),
		"the same message raised on a second column is a second finding")

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

// A primary member's identity is part of the confirmation even when its DDL
// matches another member's reviewed DDL. A converged primary still fixes the
// round's identity while another target has work; an unchanged identity allows
// the primary to converge without discarding consent for that other work.
func TestRoundCoversWorkRequiresTheReviewedPrimaryMember(t *testing.T) {
	pinned := storedPlan(1, "plan_reviewed", addEmail)
	pinned.Deployment, pinned.Target = "primary", "eu"
	current := storedPlan(2, "plan_current", addEmail)
	current.Deployment, current.Target = "primary", "us"
	members := map[string]*storage.Plan{"primary/ap": storedPlan(3, "plan_ap", addEmail)}

	covered, reason := roundCoversWork(pinned, current, members, members)
	assert.False(t, covered, "the same DDL on another primary member needs fresh review")
	assert.Equal(t, "the primary target is not the one the confirmed plan reviewed", reason)

	current.Namespaces = nil
	covered, reason = roundCoversWork(pinned, current, members, members)
	assert.False(t, covered, "a changed primary with no work still requires a fresh rollout confirmation")
	assert.Equal(t, primaryTargetDifferenceReason(workTarget), reason)

	current.Target = "eu"
	covered, reason = roundCoversWork(pinned, current, members, members)
	assert.True(t, covered, "the reviewed primary converged without changing identity: %s", reason)
}

type primaryConfirmationPlanStore struct {
	rollbackConfirmTestPlanStore
}

func (s *primaryConfirmationPlanStore) List(_ context.Context, opts storage.ListPlansOptions) ([]*storage.Plan, error) {
	var members []*storage.Plan
	for _, plan := range s.plans {
		if plan.PrimaryPlanIdentifier == opts.PrimaryPlanIdentifier {
			members = append(members, plan)
		}
	}
	return members, nil
}

// Both stored comparison paths bind consent to the deployment and target, not
// just identical statements. Shrinking a rollout retains that binding, while
// unchanged routing still permits the reviewed work and the single-target gates.
func TestConfirmationRequiresTheReviewedPrimaryMember(t *testing.T) {
	for _, tc := range []struct {
		name             string
		deployment       string
		target           string
		rolloutAtConfirm bool
		confirmedRollout bool
		changed          bool
	}{
		{name: "target changed", deployment: "eu", target: "other", rolloutAtConfirm: true, confirmedRollout: true, changed: true},
		{name: "deployment changed", deployment: "us", target: "payments", rolloutAtConfirm: true, confirmedRollout: true, changed: true},
		{name: "rollout shrank to another member", deployment: "us", target: "payments", confirmedRollout: true, changed: true},
		{name: "unchanged primary after shrink", deployment: "eu", target: "payments", confirmedRollout: true},
		{name: "unchanged rollout primary", deployment: "eu", target: "payments", rolloutAtConfirm: true, confirmedRollout: true},
		{name: "single target changed", deployment: "eu", target: "other", changed: true},
		{name: "unchanged single target", deployment: "eu", target: "payments"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pinned := storedPlan(1, "plan_reviewed", addEmail)
			pinned.Deployment, pinned.Target = "eu", "payments"
			current := storedPlan(2, "plan_current", addEmail)
			current.Deployment, current.Target = tc.deployment, tc.target
			store := &primaryConfirmationPlanStore{rollbackConfirmTestPlanStore: rollbackConfirmTestPlanStore{
				plans: map[string]*storage.Plan{pinned.PlanIdentifier: pinned, current.PlanIdentifier: current},
			}}
			if tc.confirmedRollout {
				member := storedPlan(3, "plan_member", addEmail)
				member.Deployment, member.Target = "ap", "payments"
				member.PrimaryPlanIdentifier = pinned.PlanIdentifier
				store.plans[member.PlanIdentifier] = member
			}
			var logs bytes.Buffer
			h := &Handler{
				service: api.New(&rollbackConfirmTestStorage{plans: store}, &api.ServerConfig{}, nil, testLogger()),
				logger:  slog.New(slog.NewTextHandler(&logs, nil)),
			}

			covered, reason, err := h.confirmationCoversMemberWork(t.Context(), pinned.PlanIdentifier, current.PlanIdentifier, "production")
			require.NoError(t, err)
			assert.Equal(t, !tc.changed, covered)
			wantDifference := workUnchanged
			if tc.changed {
				wantDifference = workTarget
				assert.Equal(t, "the primary target is not the one the confirmed plan reviewed", reason)
			} else {
				assert.Empty(t, reason)
			}
			difference, err := h.confirmationCoversPrimaryTarget(t.Context(), pinned.PlanIdentifier, current.PlanIdentifier, "production", tc.rolloutAtConfirm)
			require.NoError(t, err)
			assert.Equal(t, wantDifference, difference)
			if tc.changed {
				assert.Contains(t, logs.String(), "confirmed_deployment=eu confirmed_target=payments")
				assert.Contains(t, logs.String(), "current_deployment="+tc.deployment+" current_target="+tc.target)
				assert.Contains(t, logs.String(), "pending_plan_id=plan_reviewed plan_id=plan_current")
			}
		})
	}
}

type failingMemberListPlanStore struct {
	rollbackConfirmTestPlanStore
}

func (s *failingMemberListPlanStore) List(context.Context, storage.ListPlansOptions) ([]*storage.Plan, error) {
	return nil, errors.New("member plan listing unavailable")
}

// The primary member's identity is settled by the two plans alone, so a
// confirmation whose primary moved is refused even when the member plans cannot
// be read: the pending confirmation is released rather than preserved behind a
// read failure that can tell the operator nothing more.
func TestChangedPrimaryIsRefusedBeforeMemberPlansAreRead(t *testing.T) {
	pinned := storedPlan(1, "plan_reviewed", addEmail)
	pinned.Deployment, pinned.Target = "eu", "payments"
	current := storedPlan(2, "plan_current", addEmail)
	current.Deployment, current.Target = "us", "payments"
	store := &failingMemberListPlanStore{rollbackConfirmTestPlanStore: rollbackConfirmTestPlanStore{
		plans: map[string]*storage.Plan{pinned.PlanIdentifier: pinned, current.PlanIdentifier: current},
	}}
	var logs bytes.Buffer
	h := &Handler{
		service: api.New(&rollbackConfirmTestStorage{plans: store}, &api.ServerConfig{}, nil, testLogger()),
		logger:  slog.New(slog.NewTextHandler(&logs, nil)),
	}

	covered, reason, err := h.confirmationCoversMemberWork(t.Context(), pinned.PlanIdentifier, current.PlanIdentifier, "production")
	require.NoError(t, err)
	assert.False(t, covered)
	assert.Equal(t, "the primary target is not the one the confirmed plan reviewed", reason)
	assert.Contains(t, logs.String(), "confirmed_deployment=eu confirmed_target=payments")
	assert.Contains(t, logs.String(), "current_deployment=us current_target=payments")

	current.Deployment = "eu"
	_, _, err = h.confirmationCoversMemberWork(t.Context(), pinned.PlanIdentifier, current.PlanIdentifier, "production")
	require.ErrorContains(t, err, "member plan listing unavailable", "an unchanged primary still needs the member plans to decide")
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

// A changed primary gets target-change guidance rather than claiming its
// identical statements changed. The preview uses that same curated reason.
func TestUnconfirmedWorkMessageForAChangedPrimary(t *testing.T) {
	message := unconfirmedWorkMessage(memberWork{}, primaryTargetDifferenceReason(workTarget))
	assert.Equal(t,
		"This confirmation no longer covers what the apply would run: the primary target is not the one the confirmed plan reviewed, so nothing was applied. Run apply again for this environment to review and confirm each target's own plan.", message)
	preview := PreviewConfirmationPrimaryTargetChanged()
	assert.Contains(t, preview, "the primary target is not the one the confirmed plan reviewed")
	assert.Contains(t, preview, "Run apply again for this environment")
	assert.NotContains(t, preview, "in its statements")
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

// An apply that does not run the other targets' plans runs the primary plan
// alone, so the primary plan alone decides whether --defer-cutover has anything
// to defer, and no other target's plan is read. The handler has no storage, so
// reading one would fail the test.
func TestDeferCutoverHasNothingToDefer_PrimaryPlanDecidesWhenNoOtherTargetRuns(t *testing.T) {
	h := &Handler{}
	direct := &apitypes.PlanResponse{PlanID: "plan-1", Changes: []*apitypes.SchemaChangeResponse{{
		Namespace: "orders", TableChanges: []*apitypes.TableChangeResponse{{TableName: "orders", ExecutionMode: "direct"}},
	}}}
	engine := &apitypes.PlanResponse{PlanID: "plan-2", Changes: []*apitypes.SchemaChangeResponse{{
		Namespace: "orders", TableChanges: []*apitypes.TableChangeResponse{{TableName: "orders"}},
	}}}

	nothingToDefer, err := h.deferCutoverHasNothingToDefer(t.Context(), direct, "staging", false)
	require.NoError(t, err)
	assert.True(t, nothingToDefer, "a primary plan of direct changes has no cutover to defer")

	nothingToDefer, err = h.deferCutoverHasNothingToDefer(t.Context(), engine, "staging", false)
	require.NoError(t, err)
	assert.False(t, nothingToDefer, "a primary plan with an engine-driven change has a cutover to defer")
}

// --defer-cutover has a cutover to defer when any target the apply runs
// carries an engine-driven change, whichever target that is. A target already
// at the desired schema runs nothing, so it decides nothing either way.
func TestRolloutAllChangesDirect(t *testing.T) {
	directPrimary := &apitypes.PlanResponse{Changes: []*apitypes.SchemaChangeResponse{{
		Namespace: "orders", TableChanges: []*apitypes.TableChangeResponse{{TableName: "orders", ExecutionMode: "direct"}},
	}}}
	enginePrimary := &apitypes.PlanResponse{Changes: []*apitypes.SchemaChangeResponse{{
		Namespace: "orders", TableChanges: []*apitypes.TableChangeResponse{{TableName: "orders"}},
	}}}
	convergedPrimary := &apitypes.PlanResponse{}
	plan := func(mode string) *storage.Plan {
		return &storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{
			"orders": {Tables: []storage.TableChange{{Table: "orders", Operation: "alter", ExecutionMode: mode}}},
		}}
	}
	converged := &storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{"orders": {}}}

	tests := []struct {
		name    string
		primary *apitypes.PlanResponse
		members map[string]*storage.Plan
		want    bool
	}{
		{name: "the primary target alone runs direct changes", primary: directPrimary, want: true},
		{name: "the primary target is converged and another target runs only direct changes",
			primary: convergedPrimary, members: map[string]*storage.Plan{"us": plan("direct"), "eu": converged}, want: true},
		{name: "the primary target runs direct changes and another target runs an engine change",
			primary: directPrimary, members: map[string]*storage.Plan{"us": plan("")}, want: false},
		{name: "another target runs direct changes and the primary target runs an engine change",
			primary: enginePrimary, members: map[string]*storage.Plan{"us": plan("direct")}, want: false},
		{name: "every target runs only direct changes",
			primary: directPrimary, members: map[string]*storage.Plan{"us": plan("direct")}, want: true},
		{name: "no target has work", primary: convergedPrimary, members: map[string]*storage.Plan{"us": converged}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, rolloutAllChangesDirect(tt.primary, tt.members))
		})
	}
}

// An apply that could not verify the other targets' plans tells the operator
// how to recover from the state the rejection left: an automatic apply
// released its lock, so the apply command starts over, while apply-confirm
// kept the pending confirmation, so confirming again is the retry.
func TestUnverifiedMemberWorkMessage(t *testing.T) {
	assert.Equal(t,
		"SchemaBot could not verify the other targets' plans, so nothing was applied. Run apply again, and see server logs if it persists.",
		unverifiedMemberWorkMessage("the other targets' plans", "staging", true))
	assert.Equal(t,
		"SchemaBot could not verify the other targets' plans, so nothing was applied. The pending confirmation is preserved; re-run `schemabot apply-confirm -e staging` with the same flags, and see server logs if it persists.",
		unverifiedMemberWorkMessage("the other targets' plans", "staging", false))
}
