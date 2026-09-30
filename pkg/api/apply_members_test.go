package api

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// listingPlanStore serves a fixed set of stored plans to List, newest first, so
// a test can control exactly which member plans apply creation can find.
type listingPlanStore struct {
	mockPlanLookupStore
	plans   []*storage.Plan
	listErr error
}

func (s *listingPlanStore) List(_ context.Context, opts storage.ListPlansOptions) ([]*storage.Plan, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	var matched []*storage.Plan
	for _, plan := range s.plans {
		if plan.PrimaryPlanIdentifier == opts.PrimaryPlanIdentifier {
			matched = append(matched, plan)
		}
	}
	return matched, nil
}

func memberResolutionService(t *testing.T, env EnvironmentConfig, plans storage.PlanStore) *Service {
	t.Helper()
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"testapp": {
				Type:         storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{"production": env},
			},
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorageWithPlanLookup{plans: plans}, cfg, map[string]tern.Client{}, logger)
}

func multiTargetEnv() EnvironmentConfig {
	return EnvironmentConfig{Deployment: "eu", Targets: targetNames("testapp-001", "testapp-002")}
}

func mirroredEnv() EnvironmentConfig {
	return EnvironmentConfig{
		Deployments:     map[string]DeploymentTarget{"eu": {Target: "testapp"}, "us": {Target: "testapp"}},
		DeploymentOrder: []string{"eu", "us"},
	}
}

func primaryPlanRow(target string) *storage.Plan {
	return &storage.Plan{
		ID:             10,
		PlanIdentifier: "plan-primary",
		Database:       "testapp",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "eu",
		Target:         target,
		Repository:     "org/repo",
		PullRequest:    7,
		HeadSHA:        "abc123",
	}
}

// memberPlanRow is a plan stored for a non-primary member, stamped with the
// reviewed plan it was produced alongside.
func memberPlanRow(identifier, target, primaryPlanIdentifier string) *storage.Plan {
	plan := primaryPlanRow(target)
	plan.ID = 0
	plan.PlanIdentifier = identifier
	plan.PrimaryPlanIdentifier = primaryPlanIdentifier
	return plan
}

func targetsFor(t *testing.T, svc *Service) []routing.ExecutionTarget {
	t.Helper()
	targets, err := svc.config.ResolveDatabaseTargets("testapp", "production")
	require.NoError(t, err)
	return targets
}

// Members that are expected to hold the same schema store no plans of their
// own, because every one of them runs the plan the operator reviewed.
func TestResolveApplyMembers_MirroredMembersShareTheApplyPlan(t *testing.T) {
	plans := &listingPlanStore{}
	svc := memberResolutionService(t, mirroredEnv(), plans)
	plan := primaryPlanRow("testapp")

	members, err := svc.resolveApplyMembers(t.Context(), plan, "production", targetsFor(t, svc))
	require.NoError(t, err)
	require.Len(t, members, 2)
	for _, member := range members {
		assert.Same(t, plan, member.Plan, "member %s must run the reviewed plan", member.MemberID())
	}
}

// The trusted control-plane enqueue path creates applies on servers that hold
// no database config, and apply creation falls back to the plan's own stored
// target there. That single member is the plan's own primary, so it runs the
// apply's plan without consulting config for a contract that could not change
// the outcome.
func TestResolveApplyMembers_SingleMemberNeedsNoDatabaseConfig(t *testing.T) {
	plans := &listingPlanStore{listErr: errors.New("a single member needs no member-plan lookup")}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithPlanLookup{plans: plans}, &ServerConfig{}, map[string]tern.Client{}, logger)
	plan := primaryPlanRow("testapp-001")

	members, err := svc.resolveApplyMembers(t.Context(), plan, "production", []routing.ExecutionTarget{
		{DatabaseType: plan.DatabaseType, Deployment: plan.Deployment, Target: plan.Target},
	})
	require.NoError(t, err)
	require.Len(t, members, 1)
	assert.Equal(t, "eu/testapp-001", members[0].MemberID())
	assert.Same(t, plan, members[0].Plan)
}

// A lone rollout member runs the apply's plan without a lookup, which is only
// sound when that member is the target the plan was produced for. A route that
// resolves to some other target is refused rather than running the primary's
// DDL against a target nothing planned.
func TestResolveApplyMembers_SingleMemberMustBeThePlansOwnTarget(t *testing.T) {
	plans := &listingPlanStore{listErr: errors.New("a single member needs no member-plan lookup")}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithPlanLookup{plans: plans}, &ServerConfig{}, map[string]tern.Client{}, logger)
	plan := primaryPlanRow("testapp-001")

	_, err := svc.resolveApplyMembers(t.Context(), plan, "production", []routing.ExecutionTarget{
		{DatabaseType: plan.DatabaseType, Deployment: plan.Deployment, Target: "testapp-002"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "eu/testapp-002", "the refusal names the target that was resolved")
	assert.Contains(t, err.Error(), "eu/testapp-001", "and the target the plan was produced for")
}

// Each target of a multi-target environment runs the plan stored for that
// target in the review round the apply's own plan is the reviewed plan of.
func TestResolveApplyMembers_IndependentMembersRunTheirOwnPlans(t *testing.T) {
	plan := primaryPlanRow("testapp-001")
	secondPlan := memberPlanRow("plan-second", "testapp-002", "plan-primary")
	plans := &listingPlanStore{plans: []*storage.Plan{secondPlan}}
	svc := memberResolutionService(t, multiTargetEnv(), plans)

	members, err := svc.resolveApplyMembers(t.Context(), plan, "production", targetsFor(t, svc))
	require.NoError(t, err)
	require.Len(t, members, 2)
	assert.Equal(t, "eu/testapp-001", members[0].MemberID())
	assert.Same(t, plan, members[0].Plan, "the primary runs the plan the apply was created from")
	assert.Equal(t, "eu/testapp-002", members[1].MemberID())
	assert.Same(t, secondPlan, members[1].Plan)
}

// A plan stored in another review round of the same pull request — an earlier
// push, or a re-plan of this very commit — would run DDL the operator never
// approved, so the member counts as unplanned and apply creation fails.
func TestResolveApplyMembers_MemberPlanFromAnotherRoundIsNotUsed(t *testing.T) {
	plan := primaryPlanRow("testapp-001")
	stale := memberPlanRow("plan-stale", "testapp-002", "plan-earlier-round")
	plans := &listingPlanStore{plans: []*storage.Plan{stale}}
	svc := memberResolutionService(t, multiTargetEnv(), plans)

	_, err := svc.resolveApplyMembers(t.Context(), plan, "production", targetsFor(t, svc))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no stored plan for rollout member eu/testapp-002")
}

// A member with no plan at all cannot be applied: the apply's plan describes a
// different target's schema, so there is no safe substitute.
func TestResolveApplyMembers_MissingMemberPlanFailsClosed(t *testing.T) {
	plan := primaryPlanRow("testapp-001")
	plans := &listingPlanStore{}
	svc := memberResolutionService(t, multiTargetEnv(), plans)

	_, err := svc.resolveApplyMembers(t.Context(), plan, "production", targetsFor(t, svc))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no stored plan for rollout member eu/testapp-002")
}

// Member plans are only written by a pull request review, so a CLI apply against
// a multi-target environment has none and fails closed rather than running the
// primary's DDL against every target.
func TestResolveApplyMembers_PlanWithoutPullRequestReviewFailsClosed(t *testing.T) {
	plan := primaryPlanRow("testapp-001")
	plan.HeadSHA = ""
	plans := &listingPlanStore{}
	svc := memberResolutionService(t, multiTargetEnv(), plans)

	_, err := svc.resolveApplyMembers(t.Context(), plan, "production", targetsFor(t, svc))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "was not produced by a pull request review")
}

// An environment respelled as mirrored after its review still applies what was
// reviewed: the round stored a plan per member, and those plans are what the
// operator approved. Current config cannot reinterpret a finished review.
func TestResolveApplyMembers_ReviewedRoundOutranksCurrentConfig(t *testing.T) {
	plan := primaryPlanRow("testapp")
	secondPlan := memberPlanRow("plan-second", "testapp", "plan-primary")
	secondPlan.Deployment = "us"
	plans := &listingPlanStore{plans: []*storage.Plan{secondPlan}}
	svc := memberResolutionService(t, mirroredEnv(), plans)

	members, err := svc.resolveApplyMembers(t.Context(), plan, "production", targetsFor(t, svc))
	require.NoError(t, err)
	require.Len(t, members, 2)
	assert.Same(t, plan, members[0].Plan, "the primary runs the plan the apply was created from")
	assert.Same(t, secondPlan, members[1].Plan, "a member planned on its own keeps its reviewed plan")
}

// A storage failure while loading member plans is not an absence of members: it
// blocks apply creation rather than falling back to the apply's plan.
func TestResolveApplyMembers_MemberPlanLookupFailureBlocks(t *testing.T) {
	plan := primaryPlanRow("testapp-001")
	plans := &listingPlanStore{listErr: errors.New("storage unavailable")}
	svc := memberResolutionService(t, multiTargetEnv(), plans)

	_, err := svc.resolveApplyMembers(t.Context(), plan, "production", targetsFor(t, svc))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "storage unavailable")
}

// An operation names a plan of its own exactly when it runs a different plan
// than its apply. Members that run the apply's plan leave it unset, which is
// what "runs the apply's plan" already means downstream.
func TestNewPendingApplyOperation_StampsPlanIDOnlyForOwnPlan(t *testing.T) {
	applyPlan := primaryPlanRow("testapp-001")
	ownPlan := &storage.Plan{ID: 11, Deployment: "eu", Target: "testapp-002"}
	now := pershardTestTime()

	shared := newPendingApplyOperation(
		applyMember{Target: routing.ExecutionTarget{Deployment: "eu", Target: "testapp-001"}, Plan: applyPlan},
		applyPlan, "", "", "", now)
	assert.Zero(t, shared.PlanID, "a member running the apply's plan names no plan of its own")
	assert.Equal(t, "testapp-001", shared.Target)

	own := newPendingApplyOperation(
		applyMember{Target: routing.ExecutionTarget{Deployment: "eu", Target: "testapp-002"}, Plan: ownPlan},
		applyPlan, "", "", "", now)
	assert.Equal(t, int64(11), own.PlanID)
	assert.Equal(t, "testapp-002", own.Target)
	assert.Equal(t, "eu", own.Deployment)
}

// Two targets of one deployment are two distinct members, so each gets its own
// operation for the same table rather than one member's work being folded into
// the other's.
func TestBuildApplyOperationGroups_TargetsOfOneDeploymentGetOwnOperations(t *testing.T) {
	applyPlan := primaryPlanRow("testapp-001")
	secondPlan := &storage.Plan{ID: 11, Deployment: "eu", Target: "testapp-002"}
	members := []applyMember{
		{Target: routing.ExecutionTarget{Deployment: "eu", Target: "testapp-001"}, Plan: applyPlan},
		{Target: routing.ExecutionTarget{Deployment: "eu", Target: "testapp-002"}, Plan: secondPlan},
	}
	taskChanges := []storage.TableChange{{Namespace: "testapp", Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", Operation: "alter"}}

	groups, sharded, err := buildApplyOperationGroups(applyPlan, taskChanges, members, "production", storage.ApplyOptions{}, "", "", pershardTestTime())
	require.NoError(t, err)
	assert.False(t, sharded)
	require.Len(t, groups, 2)
	assert.Equal(t, "testapp-001", groups[0].Operation.Target)
	assert.Zero(t, groups[0].Operation.PlanID)
	assert.Equal(t, "testapp-002", groups[1].Operation.Target)
	assert.Equal(t, int64(11), groups[1].Operation.PlanID)
}

// An operation key is unique per deployment, so it only has to name a target
// where the deployment addresses more than one. A deployment with a single
// target leaves the key alone, keeping it the shape every reader already parses.
func TestMemberOperationKeys_QualifiesOnlyMultiTargetDeployments(t *testing.T) {
	members := []applyMember{
		{Target: routing.ExecutionTarget{Deployment: "eu", Target: "testapp-001"}},
		{Target: routing.ExecutionTarget{Deployment: "eu", Target: "testapp-002"}},
		{Target: routing.ExecutionTarget{Deployment: "us", Target: "testapp"}},
	}
	keys := newMemberOperationKeys(members)

	qualified, err := keys.qualify(members[0], "testapp/-80/mutes")
	require.NoError(t, err)
	assert.Equal(t, "testapp-001/testapp/-80/mutes", qualified)

	whole, err := keys.qualify(members[0], "")
	require.NoError(t, err)
	assert.Equal(t, "testapp-001", whole, "a member with no scoped work is named by its target alone")

	sole, err := keys.qualify(members[2], "testapp/-80/mutes")
	require.NoError(t, err)
	assert.Equal(t, "testapp/-80/mutes", sole, "a deployment with one target needs no target component")
}

// Config refuses the delimiter in a target's name, but an apply can be created
// from a plan stored under a config that no longer applies. A target that would
// make its members' operation keys ambiguous to split fails apply creation
// rather than producing keys no reader can take apart.
func TestMemberOperationKeys_RefusesTargetCarryingTheDelimiter(t *testing.T) {
	members := []applyMember{
		{Target: routing.ExecutionTarget{Deployment: "eu", Target: "testapp/001"}},
		{Target: routing.ExecutionTarget{Deployment: "eu", Target: "testapp-002"}},
	}
	keys := newMemberOperationKeys(members)

	_, err := keys.qualify(members[0], "testapp/users")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "target")
}

// A member whose own plan found nothing to change is already converged. Its
// operation is recorded as completed so the apply covers every member it
// addressed, rather than leaving a pending operation no driver can ever finish.
func TestBuildApplyOperationGroups_ConvergedMemberIsCompletedOnCreation(t *testing.T) {
	usersDDL := "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"
	applyPlan := primaryPlanRow("testapp-001")
	applyPlan.Namespaces = map[string]*storage.NamespacePlanData{
		"testapp": {Tables: []storage.TableChange{{Namespace: "testapp", Table: "users", DDL: usersDDL, Operation: "alter"}}},
	}
	convergedPlan := &storage.Plan{ID: 11, Deployment: "eu", Target: "testapp-002"}
	members := []applyMember{
		{Target: routing.ExecutionTarget{Deployment: "eu", Target: "testapp-001"}, Plan: applyPlan},
		{Target: routing.ExecutionTarget{Deployment: "eu", Target: "testapp-002"}, Plan: convergedPlan},
	}
	taskChanges := applyTaskChanges(applyPlan)

	groups, _, err := buildApplyOperationGroups(applyPlan, taskChanges, members, "production", storage.ApplyOptions{}, "", "", pershardTestTime())
	require.NoError(t, err)
	require.Len(t, groups, 2)

	assert.Equal(t, state.ApplyOperation.Pending, groups[0].Operation.State)
	require.Len(t, groups[0].Tasks, 1)

	converged := groups[1].Operation
	assert.Empty(t, groups[1].Tasks, "a converged member has no work to drive")
	assert.Equal(t, state.ApplyOperation.Completed, converged.State)
	assert.Nil(t, converged.StartedAt, "a converged member never started: nothing ran on its target")
	require.NotNil(t, converged.CompletedAt)
	assert.Equal(t, pershardTestTime(), *converged.CompletedAt)
}

// A sharded plan describes the same (namespace, shard, table) work on every
// member, and the operation key is unique per deployment. Two targets of one
// deployment therefore lead their keys with the target's name, so each gets its
// own operation rather than one target's shard work being folded into the other's.
func TestBuildShardedApplyOperationGroups_TargetsOfOneDeploymentDoNotShareOperations(t *testing.T) {
	mutesDDL := "ALTER TABLE `mutes` ADD INDEX (`created_at`)"
	applyPlan := &storage.Plan{
		ID:       10,
		Database: "testapp",
		Shards: []storage.ShardPlan{
			{Namespace: pershardNamespace, Shard: "-80", Changes: []storage.TableChange{
				{Namespace: pershardNamespace, Table: "mutes", DDL: mutesDDL, Operation: "alter"},
			}},
		},
	}
	secondPlan := &storage.Plan{ID: 11, Database: "testapp", Shards: applyPlan.Shards}
	members := []applyMember{
		{Target: routing.ExecutionTarget{Deployment: "eu", Target: "testapp-001"}, Plan: applyPlan},
		{Target: routing.ExecutionTarget{Deployment: "eu", Target: "testapp-002"}, Plan: secondPlan},
	}

	groups, err := buildShardedApplyOperationGroups(applyPlan, members, newMemberOperationKeys(members), "production", storage.ApplyOptions{}, "", "", pershardTestTime())
	require.NoError(t, err)
	require.Len(t, groups, 2, "each target needs its own operation for the shard's table")

	byTarget := map[string]*storage.ApplyOperationWithTasks{}
	for _, g := range groups {
		byTarget[g.Operation.Target] = g
	}
	require.Contains(t, byTarget, "testapp-001")
	require.Contains(t, byTarget, "testapp-002")
	for target, group := range byTarget {
		assert.Equal(t, target+"/"+pershardNamespace+"/-80/mutes", group.Operation.OperationKey)
		assert.Equal(t, "eu", group.Operation.Deployment)
		require.Len(t, group.Tasks, 1, "target %s must carry its shard's work exactly once", target)
		assert.Equal(t, mutesDDL, group.Tasks[0].DDL)
		assert.Equal(t, "-80", group.Tasks[0].Shard)
	}
	assert.Zero(t, byTarget["testapp-001"].Operation.PlanID)
	assert.Equal(t, int64(11), byTarget["testapp-002"].Operation.PlanID)
}

// multiTargetApplyService wires the stores apply creation reaches before it
// builds operations, so a test can drive createStoredApply for an environment
// whose members are planned independently.
func multiTargetApplyService(t *testing.T, plans storage.PlanStore) *Service {
	t.Helper()
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"testapp": {
				Type:         storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{"production": multiTargetEnv()},
			},
		},
	}
	applies := &capturingApplyStore{}
	tasks := &capturingTaskStore{}
	applies.taskStore = tasks
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorageWithApplyStores{
		plans:     plans,
		applies:   applies,
		tasks:     tasks,
		locks:     &emptyLockStore{},
		applyLogs: &noopApplyLogStore{},
		controls:  &memoryControlRequestStore{},
	}, cfg, map[string]tern.Client{}, logger)
}

// memberPlanWithChange is a member's own stored plan carrying one table change,
// which is the DDL an apply would build that member's tasks from.
func memberPlanWithChange(change storage.TableChange) *storage.Plan {
	plan := memberPlanRow("plan-second", "testapp-002", "plan-primary")
	plan.Namespaces = map[string]*storage.NamespacePlanData{
		"testapp": {Tables: []storage.TableChange{change}},
	}
	return plan
}

// Apply admission clears the plan the apply was created from, which is the
// primary's. A member planned against its own live schema runs its own plan
// instead, so a statement the engine refuses reaches its target through that
// member rather than through the plan the operator reviewed — admission has to
// clear every member's plan, and name the one that carries the change.
func TestCreateStoredApply_BlockedMemberPlanIsRefused(t *testing.T) {
	member := memberPlanWithChange(storage.TableChange{
		Namespace:     "testapp",
		Table:         "orders",
		Operation:     "alter",
		DDL:           "ALTER TABLE `orders` ADD COLUMN `region` varchar(16)",
		ExecutionMode: "blocked",
		ModeReason:    "the engine refuses this statement",
	})
	svc := multiTargetApplyService(t, &listingPlanStore{plans: []*storage.Plan{member}})

	_, _, err := svc.createStoredApply(t.Context(), primaryPlanRow("testapp-001"), ApplyRequest{Environment: "production"}, nil, "apply-blocked-member")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "rollout member eu/testapp-002")
	assert.Contains(t, err.Error(), "blocked change")
	assert.Contains(t, err.Error(), "orders")
	refused, ok := errors.AsType[*MemberPlanRefusedError](err)
	require.True(t, ok, "the refusal is typed so a caller can name the target without rendering the error")
	assert.Equal(t, MemberPlanBlocked, refused.Refusal)
	assert.Equal(t, "eu/testapp-002", refused.Target, "the target is named the way the plan comment names it")
	assert.Equal(t, "orders", refused.Table)
}

// The unsafe opt-in is the operator's, given against the disclosure on the
// comment it confirms, and that disclosure lists the reviewed plan's unsafe
// changes. A member whose own plan drops the same table the reviewed plan drops
// runs it under the opt-in, and needs the opt-in exactly as the reviewed plan
// does.
func TestCreateStoredApply_DisclosedUnsafeMemberChangeNeedsTheOptIn(t *testing.T) {
	drop := storage.TableChange{
		Namespace: "testapp",
		Table:     "legacy_orders",
		Operation: "drop",
		DDL:       "DROP TABLE `legacy_orders`",
	}
	svc := multiTargetApplyService(t, &listingPlanStore{plans: []*storage.Plan{memberPlanWithChange(drop)}})
	reviewed := primaryPlanRow("testapp-001")
	reviewed.Namespaces = map[string]*storage.NamespacePlanData{"testapp": {Tables: []storage.TableChange{drop}}}

	_, _, err := svc.createStoredApply(t.Context(), reviewed, ApplyRequest{Environment: "production"}, nil, "apply-unsafe-member")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "legacy_orders")
	assert.Contains(t, err.Error(), "allow_unsafe")
	refused, ok := errors.AsType[*MemberPlanRefusedError](err)
	require.True(t, ok, "the refusal is typed so a caller can name the target without rendering the error")
	assert.Equal(t, MemberPlanUnsafe, refused.Refusal)
	assert.Equal(t, "legacy_orders", refused.Table)

	_, _, err = svc.createStoredApply(t.Context(), reviewed, ApplyRequest{Environment: "production"},
		map[string]string{"allow_unsafe": "true"}, "apply-unsafe-member-opted-in")
	require.NoError(t, err, "the opt-in covers a member's unsafe change the disclosure named")
}

// A member planned against a schema of its own can carry an unsafe change the
// reviewed plan does not: us has drifted and drops a column eu never had. The
// comment's unsafe disclosure names only the reviewed plan's changes, so the
// operator's opt-in was never given for us's drop, and apply creation refuses
// it even under the opt-in, rather than give retry advice that could never
// succeed.
func TestCreateStoredApply_UndisclosedUnsafeMemberChangeIsRefusedUnderTheOptIn(t *testing.T) {
	reviewedDrop := storage.TableChange{
		Namespace: "testapp",
		Table:     "users",
		Operation: "alter",
		DDL:       "ALTER TABLE `users` DROP COLUMN `nickname`",
		IsUnsafe:  true,
	}
	memberDrop := reviewedDrop
	memberDrop.DDL = "ALTER TABLE `users` DROP COLUMN `nickname`, DROP COLUMN `legacy_id`"
	svc := multiTargetApplyService(t, &listingPlanStore{plans: []*storage.Plan{memberPlanWithChange(memberDrop)}})
	reviewed := primaryPlanRow("testapp-001")
	reviewed.Namespaces = map[string]*storage.NamespacePlanData{"testapp": {Tables: []storage.TableChange{reviewedDrop}}}

	_, _, err := svc.createStoredApply(t.Context(), reviewed, ApplyRequest{Environment: "production"},
		map[string]string{"allow_unsafe": "true"}, "apply-undisclosed-unsafe")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rollout member eu/testapp-002")
	assert.Contains(t, err.Error(), "carries an unsafe change for table \"users\" whose statement differs from the one the reviewed plan discloses for that table",
		"the reviewed plan drops a column from users too, so the refusal says the statements differ")
	assert.Contains(t, err.Error(), "the disclosure on reviewed plan plan-primary never named it")
	assert.NotContains(t, err.Error(), "retry with allow_unsafe", "no opt-in covers a change the disclosure never named")
	applies, ok := svc.storage.Applies().(*capturingApplyStore)
	require.True(t, ok)
	assert.Nil(t, applies.apply, "nothing is stored for a refused apply")
}

// The preflight a PR apply runs before it pauses asks of each member what apply
// creation asks, so a confirmation is never pinned for work creation refuses.
func TestMemberWorkTheReviewedPlanCannotRun(t *testing.T) {
	alter := storage.TableChange{Namespace: "testapp", Table: "users", Operation: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"}
	drop := storage.TableChange{Namespace: "testapp", Table: "legacy_orders", Operation: "drop", DDL: "DROP TABLE `legacy_orders`"}
	planWith := func(changes ...storage.TableChange) *storage.Plan {
		plan := primaryPlanRow("testapp-001")
		plan.Namespaces = map[string]*storage.NamespacePlanData{"testapp": {Tables: changes}}
		return plan
	}
	direct := alter
	direct.ExecutionMode = "direct"
	blocked := alter
	blocked.ExecutionMode = "blocked"
	shardOnly := memberPlanRow("plan-second", "testapp-002", "plan-primary")
	shardOnly.Shards = []storage.ShardPlan{{Namespace: "testapp", Shard: "-80", Changes: []storage.TableChange{alter}}}

	for _, tc := range []struct {
		name     string
		reviewed *storage.Plan
		member   *storage.Plan
		want     string
	}{
		{name: "same work", reviewed: planWith(alter), member: planWith(alter), want: ""},
		{name: "disclosed unsafe change", reviewed: planWith(alter, drop), member: planWith(alter, drop), want: ""},
		{name: "undisclosed unsafe change", reviewed: planWith(alter), member: planWith(alter, drop), want: `carries an unsafe change for table "legacy_orders" that the reviewed plan does not carry`},
		{name: "direct execution", reviewed: planWith(alter), member: planWith(direct), want: `runs table "users" as direct-execution DDL`},
		{name: "direct execution the reviewed plan runs too", reviewed: planWith(direct), member: planWith(direct), want: `runs table "users" as direct-execution DDL`},
		{name: "blocked change", reviewed: planWith(alter), member: planWith(blocked), want: "carries changes its target's engine refuses"},
		{name: "work outside the shape", reviewed: planWith(alter), member: shardOnly, want: "has per-shard changes in namespaces [testapp]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := MemberWorkTheReviewedPlanCannotRun(tc.reviewed, tc.member)
			if tc.want == "" {
				assert.Empty(t, got)
				return
			}
			assert.Contains(t, got, tc.want)
		})
	}
}

// A converged member is settled at creation because the rollout still has work
// elsewhere, so the primary's own target being the converged one is no different
// from a sibling's: the members that do carry DDL must still be driven.
func TestBuildApplyOperationGroups_ConvergedPrimaryIsCompletedWhenASiblingHasWork(t *testing.T) {
	applyPlan := primaryPlanRow("testapp-001")
	siblingPlan := &storage.Plan{ID: 11, Deployment: "eu", Target: "testapp-002"}
	siblingPlan.Namespaces = map[string]*storage.NamespacePlanData{
		"testapp": {Tables: []storage.TableChange{{
			Namespace: "testapp", Table: "users",
			DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", Operation: "alter",
		}}},
	}
	members := []applyMember{
		{Target: routing.ExecutionTarget{Deployment: "eu", Target: "testapp-001"}, Plan: applyPlan},
		{Target: routing.ExecutionTarget{Deployment: "eu", Target: "testapp-002"}, Plan: siblingPlan},
	}

	groups, _, err := buildApplyOperationGroups(applyPlan, applyTaskChanges(applyPlan), members,
		"production", storage.ApplyOptions{}, "", "", pershardTestTime())
	require.NoError(t, err)
	require.Len(t, groups, 2)

	assert.Empty(t, groups[0].Tasks, "the primary's own target already holds the change")
	assert.Equal(t, state.ApplyOperation.Completed, groups[0].Operation.State)

	assert.Equal(t, state.ApplyOperation.Pending, groups[1].Operation.State)
	require.Len(t, groups[1].Tasks, 1, "the sibling's own DDL is still driven")
}

// An apply whose every member is converged has nothing for any driver to claim.
// Settling its operations would admit an apply that is terminal before a driver
// ever sees it, holding the database lock with nothing left to resolve it, so
// the operations stay pending and storage refuses the apply outright.
func TestBuildApplyOperationGroups_ApplyWithNoWorkStaysPending(t *testing.T) {
	for _, tc := range []struct {
		name    string
		targets []string
	}{
		{name: "single member", targets: []string{"testapp-001"}},
		{name: "mirrored members", targets: []string{"testapp-001", "testapp-002"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			applyPlan := primaryPlanRow("testapp-001")
			members := make([]applyMember, 0, len(tc.targets))
			for _, target := range tc.targets {
				members = append(members, applyMember{
					Target: routing.ExecutionTarget{Deployment: "eu", Target: target},
					Plan:   applyPlan,
				})
			}

			groups, _, err := buildApplyOperationGroups(applyPlan, applyTaskChanges(applyPlan), members,
				"production", storage.ApplyOptions{}, "", "", pershardTestTime())
			require.NoError(t, err)
			require.Len(t, groups, len(tc.targets))

			for i, group := range groups {
				assert.Empty(t, group.Tasks, "group %d", i)
				assert.Equal(t, state.ApplyOperation.Pending, group.Operation.State,
					"group %d must stay pending so storage refuses an apply with no drivable work", i)
				assert.Nil(t, group.Operation.CompletedAt, "group %d", i)
			}
		})
	}
}

// An apply created from a reviewed plan with no work runs each other member's
// own plan. The members that already hold the schema are settled at creation
// and the one that needs the column is driven from its own DDL.
func TestCreateStoredApply_EmptyReviewedPlanRunsTheOtherMembersPlans(t *testing.T) {
	member := memberPlanWithChange(storage.TableChange{
		Namespace: "testapp",
		Table:     "users",
		Operation: "alter",
		DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
	})
	svc := multiTargetApplyService(t, &listingPlanStore{plans: []*storage.Plan{member}})

	_, _, err := svc.createStoredApply(t.Context(), primaryPlanRow("testapp-001"), ApplyRequest{Environment: "production"}, nil, "apply-converged-primary")
	require.NoError(t, err)

	applies, ok := svc.storage.Applies().(*capturingApplyStore)
	require.True(t, ok)
	require.Len(t, applies.operations, 2)
	byTarget := map[string]*storage.ApplyOperation{}
	for _, op := range applies.operations {
		byTarget[op.Target] = op
	}
	assert.Equal(t, state.ApplyOperation.Completed, byTarget["testapp-001"].State, "the reviewed target already holds the schema")
	assert.Equal(t, state.ApplyOperation.Pending, byTarget["testapp-002"].State)

	tasks := applies.taskStore.tasks
	require.Len(t, tasks, 1, "only the member that needs the column gets work")
	assert.Equal(t, "users", tasks[0].TableName)
	assert.Contains(t, tasks[0].DDL, "ADD COLUMN `email`")
}

// An apply created from a reviewed plan with no work gives every member one work
// operation, so member work that needs another shape is refused rather than
// settled as done. So are direct-execution DDL and unsafe changes, even under
// the opt-in, whose consent is given against a disclosure the empty reviewed
// plan does not carry.
func TestCreateStoredApply_EmptyReviewedPlanRefusesMemberWorkItCannotCarry(t *testing.T) {
	alter := storage.TableChange{
		Namespace: "testapp",
		Table:     "users",
		Operation: "alter",
		DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
	}
	for _, tc := range []struct {
		name   string
		member func() *storage.Plan
		want   string
	}{
		{
			name: "per-shard changes",
			member: func() *storage.Plan {
				plan := memberPlanRow("plan-second", "testapp-002", "plan-primary")
				plan.Shards = []storage.ShardPlan{{Shard: "-80", Namespace: "testapp", Changes: []storage.TableChange{alter}}}
				return plan
			},
			want: "carries per-shard changes",
		},
		{
			name: "finalizer",
			member: func() *storage.Plan {
				plan := memberPlanRow("plan-second", "testapp-002", "plan-primary")
				plan.Namespaces = map[string]*storage.NamespacePlanData{"testapp": {Finalize: true}}
				return plan
			},
			want: "finalizes namespaces [testapp]",
		},
		{
			name: "direct execution",
			member: func() *storage.Plan {
				direct := alter
				direct.ExecutionMode = "direct"
				return memberPlanWithChange(direct)
			},
			want: "runs table \"users\" as direct-execution DDL",
		},
		{
			name: "unsafe change under the opt-in",
			member: func() *storage.Plan {
				return memberPlanWithChange(storage.TableChange{
					Namespace: "testapp",
					Table:     "users",
					Operation: "drop",
					DDL:       "DROP TABLE `users`",
				})
			},
			want: "carries an unsafe change for table \"users\"",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := multiTargetApplyService(t, &listingPlanStore{plans: []*storage.Plan{tc.member()}})

			_, _, err := svc.createStoredApply(t.Context(), primaryPlanRow("testapp-001"), ApplyRequest{Environment: "production"}, map[string]string{"allow_unsafe": "true"}, "apply-converged-primary")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "rollout member eu/testapp-002")
			assert.Contains(t, err.Error(), tc.want)
			applies, ok := svc.storage.Applies().(*capturingApplyStore)
			require.True(t, ok)
			assert.Nil(t, applies.apply, "nothing is stored for a refused apply")
		})
	}
}

// The apply's shape, per-shard fan-out, finalizer, or one work operation per
// member, is chosen from the apply's own plan, and every member is built into
// that shape. A member planned on its own can carry work that shape has no place
// for, and building it anyway would leave the member with no operation for that
// work: settled as done, or never created, while its target never got the
// change. Apply creation refuses instead, naming the member.
func TestBuildApplyOperationGroups_MemberWorkTheApplyShapeCannotCarryIsRefused(t *testing.T) {
	alter := storage.TableChange{
		Namespace: "testapp",
		Table:     "users",
		Operation: "alter",
		DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
	}
	flatPlan := func(p *storage.Plan) *storage.Plan {
		p.Namespaces = map[string]*storage.NamespacePlanData{"testapp": {Tables: []storage.TableChange{alter}}}
		return p
	}
	shardedPlan := func(p *storage.Plan) *storage.Plan {
		p = flatPlan(p)
		p.Shards = []storage.ShardPlan{{Shard: "-80", Namespace: "testapp", Changes: []storage.TableChange{alter}}}
		return p
	}
	vschemaOnlyPlan := func(p *storage.Plan) *storage.Plan {
		p.Namespaces = map[string]*storage.NamespacePlanData{"testapp": {
			Artifacts: map[string]string{storage.VSchemaArtifactName: `{"tables":{"users":{}}}`},
		}}
		return p
	}
	shardChangesOnlyPlan := func(p *storage.Plan) *storage.Plan {
		p.Shards = []storage.ShardPlan{{Shard: "-80", Namespace: "testapp", Changes: []storage.TableChange{alter}}}
		return p
	}
	for _, tc := range []struct {
		name     string
		reviewed func(*storage.Plan) *storage.Plan
		member   func(*storage.Plan) *storage.Plan
	}{
		{name: "table changes reviewed, member changes only its VSchema", reviewed: flatPlan, member: vschemaOnlyPlan},
		{name: "table changes reviewed, member changes only shards", reviewed: flatPlan, member: shardChangesOnlyPlan},
		{name: "per-shard changes reviewed, member changes only tables", reviewed: shardedPlan, member: flatPlan},
		{name: "VSchema change reviewed, member changes tables", reviewed: vschemaOnlyPlan, member: flatPlan},
		{name: "table changes reviewed, member changes tables in one namespace and only shards in another", reviewed: flatPlan, member: func(p *storage.Plan) *storage.Plan {
			p = flatPlan(p)
			p.Shards = []storage.ShardPlan{{Shard: "-80", Namespace: "ledger", Changes: []storage.TableChange{{
				Namespace: "ledger", Table: "entries", Operation: "alter", DDL: "ALTER TABLE `entries` ADD COLUMN `memo` text",
			}}}}
			return p
		}},
		{name: "VSchema change reviewed, member finalizes and changes only shards", reviewed: vschemaOnlyPlan, member: func(p *storage.Plan) *storage.Plan {
			p = vschemaOnlyPlan(p)
			p.Shards = []storage.ShardPlan{{Shard: "-80", Namespace: "testapp", Changes: []storage.TableChange{alter}}}
			return p
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			applyPlan := tc.reviewed(primaryPlanRow("testapp-001"))
			memberPlan := tc.member(memberPlanRow("plan-second", "testapp-002", "plan-primary"))
			members := []applyMember{
				{Target: routing.ExecutionTarget{Deployment: "eu", Target: "testapp-001"}, Plan: applyPlan},
				{Target: routing.ExecutionTarget{Deployment: "eu", Target: "testapp-002"}, Plan: memberPlan},
			}

			groups, _, err := buildApplyOperationGroups(applyPlan, applyTaskChanges(applyPlan), members,
				"production", storage.ApplyOptions{}, "", "", pershardTestTime())
			require.Error(t, err, "built %d groups", len(groups))
			assert.Contains(t, err.Error(), "rollout member eu/testapp-002")
			assert.Contains(t, err.Error(), "plan-second")
			assert.Nil(t, groups)
		})
	}
}

// The reviewed plan chooses the apply's shape, and outside the per-shard shape
// its own per-shard changes are only carried by table statements for the same
// namespace. A reviewed plan whose shards change with no such statement would
// build an operation with nothing to run on the reviewed target, so apply
// creation refuses it, naming the reviewed target.
func TestBuildApplyOperationGroups_ReviewedShardWorkTheApplyShapeCannotCarryIsRefused(t *testing.T) {
	alter := storage.TableChange{
		Namespace: "testapp",
		Table:     "users",
		Operation: "alter",
		DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
	}
	applyPlan := primaryPlanRow("testapp-001")
	applyPlan.Shards = []storage.ShardPlan{{Shard: "-80", Namespace: "testapp", Changes: []storage.TableChange{alter}}}
	sibling := memberPlanRow("plan-second", "testapp-002", "plan-primary")
	sibling.Namespaces = map[string]*storage.NamespacePlanData{"testapp": {Tables: []storage.TableChange{alter}}}
	members := []applyMember{
		{Target: routing.ExecutionTarget{Deployment: "eu", Target: "testapp-001"}, Plan: applyPlan},
		{Target: routing.ExecutionTarget{Deployment: "eu", Target: "testapp-002"}, Plan: sibling},
	}

	groups, _, err := buildApplyOperationGroups(applyPlan, applyTaskChanges(applyPlan), members,
		"production", storage.ApplyOptions{}, "", "", pershardTestTime())
	require.Error(t, err, "built %d groups", len(groups))
	assert.Contains(t, err.Error(), "rollout member eu/testapp-001")
	assert.Contains(t, err.Error(), "per-shard changes in namespaces [testapp]")
	assert.Nil(t, groups)
}

// A finalizer-only apply gives every member a finalizer. A member planned on
// its own that already holds the change has no namespace to finalize, so its
// finalizer is recorded as settled at creation rather than left pending for a
// driver that could not run it, while the reviewed target's finalizer waits to
// be driven.
func TestBuildApplyOperationGroups_ConvergedMemberFinalizerIsCompletedOnCreation(t *testing.T) {
	applyPlan := primaryPlanRow("testapp-001")
	applyPlan.Namespaces = map[string]*storage.NamespacePlanData{"testapp": {
		Artifacts: map[string]string{storage.VSchemaArtifactName: `{"tables":{"users":{}}}`},
	}}
	converged := memberPlanRow("plan-second", "testapp-002", "plan-primary")
	converged.ID = 11
	members := []applyMember{
		{Target: routing.ExecutionTarget{Deployment: "eu", Target: "testapp-001"}, Plan: applyPlan},
		{Target: routing.ExecutionTarget{Deployment: "eu", Target: "testapp-002"}, Plan: converged},
	}

	now := pershardTestTime()
	groups, sharded, err := buildApplyOperationGroups(applyPlan, applyTaskChanges(applyPlan), members,
		"production", storage.ApplyOptions{}, "", "", now)
	require.NoError(t, err)
	assert.False(t, sharded)
	require.Len(t, groups, 2)
	byTarget := map[string]*storage.ApplyOperation{}
	for _, group := range groups {
		assert.Equal(t, storage.ApplyOperationKindGroupFinalizer, group.Operation.OperationKind)
		byTarget[group.Operation.Target] = group.Operation
	}
	assert.Equal(t, state.ApplyOperation.Pending, byTarget["testapp-001"].State, "the reviewed target's finalizer is driven")
	assert.Nil(t, byTarget["testapp-001"].CompletedAt)
	assert.Equal(t, state.ApplyOperation.Completed, byTarget["testapp-002"].State, "the converged member has nothing to finalize")
	require.NotNil(t, byTarget["testapp-002"].CompletedAt)
	assert.Equal(t, now, *byTarget["testapp-002"].CompletedAt)
	assert.Nil(t, byTarget["testapp-002"].StartedAt, "nothing ran on the converged member")
}

// The operator consents to direct-execution DDL against the locked comment's
// disclosure, which names only the reviewed plan's statements. A member planned
// on its own that runs direct-execution DDL is refused even when the reviewed
// target has work of its own, including the same statement run directly.
func TestCreateStoredApply_MemberDirectExecutionIsRefusedWhenTheReviewedPlanHasWork(t *testing.T) {
	alter := storage.TableChange{
		Namespace: "testapp",
		Table:     "users",
		Operation: "alter",
		DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
	}
	direct := alter
	direct.ExecutionMode = "direct"
	// The disclosure names the reviewed target's table at the size measured
	// there, so the reviewed plan running the identical statement directly
	// consents to nothing on the other target's copy of the table.
	for name, reviewedChange := range map[string]storage.TableChange{
		"reviewed plan runs it online":   alter,
		"reviewed plan runs it directly": direct,
	} {
		t.Run(name, func(t *testing.T) {
			svc := multiTargetApplyService(t, &listingPlanStore{plans: []*storage.Plan{memberPlanWithChange(direct)}})
			reviewed := primaryPlanRow("testapp-001")
			reviewed.Namespaces = map[string]*storage.NamespacePlanData{"testapp": {Tables: []storage.TableChange{reviewedChange}}}

			_, _, err := svc.createStoredApply(t.Context(), reviewed, ApplyRequest{Environment: "production"}, nil, "apply-member-direct")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "rollout member eu/testapp-002")
			assert.Contains(t, err.Error(), "runs table \"users\" as direct-execution DDL")
			applies, ok := svc.storage.Applies().(*capturingApplyStore)
			require.True(t, ok)
			assert.Nil(t, applies.apply, "nothing is stored for a refused apply")
		})
	}
}

// Two PostgreSQL targets map the namespace to differently named physical
// schemas, so each plan qualifies the same drop with its own schema. The comment
// groups them as one change, and the reviewed plan's unsafe disclosure names it,
// so the member's copy runs under the opt-in. A drop of another column on the
// same table is a different statement, and the refusal says the statements
// differ.
func TestUndisclosedMemberUnsafeChange_SchemaQualifierIsNotADifference(t *testing.T) {
	planWith := func(ddl string) *storage.Plan {
		plan := primaryPlanRow("testapp-001")
		plan.DatabaseType = storage.DatabaseTypePostgres
		plan.Namespaces = map[string]*storage.NamespacePlanData{"app": {Tables: []storage.TableChange{
			{Namespace: "app", Table: "users", Operation: "alter", DDL: ddl, IsUnsafe: true},
		}}}
		return plan
	}
	reviewed := planWith(`ALTER TABLE "app_eu".users DROP COLUMN legacy`)

	assert.Empty(t, UndisclosedMemberUnsafeChange(reviewed, planWith(`ALTER TABLE "app_us".users DROP COLUMN legacy`)),
		"the same drop rendered against another physical schema is the change the disclosure named")
	assert.Equal(t, `carries an unsafe change for table "users" whose statement differs from the one the reviewed plan discloses for that table`,
		UndisclosedMemberUnsafeChange(reviewed, planWith(`ALTER TABLE "app_us".users DROP COLUMN nickname`)))
}

// A sharded apply builds every member into per-shard operations. A member
// planned on its own that already holds the change has no changing shard, so it
// is recorded as settled at creation rather than left out of the apply, which
// would then address fewer targets than the rollout has.
func TestBuildShardedApplyOperationGroups_ConvergedMemberIsCompletedOnCreation(t *testing.T) {
	mutes := storage.TableChange{Namespace: pershardNamespace, Table: "mutes", DDL: "ALTER TABLE `mutes` ADD INDEX (`created_at`)", Operation: "alter"}
	applyPlan := primaryPlanRow("testapp-001")
	applyPlan.Namespaces = map[string]*storage.NamespacePlanData{pershardNamespace: {Tables: []storage.TableChange{mutes}}}
	applyPlan.Shards = []storage.ShardPlan{{Namespace: pershardNamespace, Shard: "-80", Changes: []storage.TableChange{mutes}}}
	converged := memberPlanRow("plan-second", "testapp-002", "plan-primary")
	converged.ID = 11
	converged.Deployment = "us"
	members := []applyMember{
		{Target: routing.ExecutionTarget{Deployment: "eu", Target: "testapp-001"}, Plan: applyPlan},
		{Target: routing.ExecutionTarget{Deployment: "us", Target: "testapp-002"}, Plan: converged},
	}

	now := pershardTestTime()
	groups, sharded, err := buildApplyOperationGroups(applyPlan, applyTaskChanges(applyPlan), members,
		"production", storage.ApplyOptions{}, "", "", now)
	require.NoError(t, err)
	assert.True(t, sharded)
	byDeployment := map[string][]*storage.ApplyOperationWithTasks{}
	for _, group := range groups {
		byDeployment[group.Operation.Deployment] = append(byDeployment[group.Operation.Deployment], group)
	}
	require.Len(t, byDeployment["eu"], 1)
	assert.Equal(t, pershardNamespace+"/-80/mutes", byDeployment["eu"][0].Operation.OperationKey)
	assert.Equal(t, state.ApplyOperation.Pending, byDeployment["eu"][0].Operation.State, "the reviewed target's shard work is driven")
	require.Len(t, byDeployment["us"], 1, "the converged member stays in the apply")
	settled := byDeployment["us"][0]
	assert.Empty(t, settled.Tasks, "the converged member has nothing to run")
	assert.Equal(t, storage.ApplyOperationKindWork, settled.Operation.OperationKind)
	assert.Equal(t, state.ApplyOperation.Completed, settled.Operation.State)
	require.NotNil(t, settled.Operation.CompletedAt)
	assert.Equal(t, now, *settled.Operation.CompletedAt)
	assert.Nil(t, settled.Operation.StartedAt, "nothing ran on the converged member")
	assert.Equal(t, int64(11), settled.Operation.PlanID)
}
