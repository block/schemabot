package api

import (
	"context"
	"errors"
	"fmt"
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
	return EnvironmentConfig{Deployment: "eu", Targets: []string{"testapp-001", "testapp-002"}}
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
	require.NotNil(t, converged.StartedAt)
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
}

// The unsafe opt-in is the operator's, and it is asked for per apply rather than
// per member. A member whose own plan drops a table therefore needs the same
// opt-in the primary's would, or the drop runs on the strength of an approval
// that was never given for it.
func TestCreateStoredApply_UnsafeMemberPlanNeedsTheOptIn(t *testing.T) {
	member := memberPlanWithChange(storage.TableChange{
		Namespace: "testapp",
		Table:     "legacy_orders",
		Operation: "drop",
		DDL:       "DROP TABLE `legacy_orders`",
	})
	svc := multiTargetApplyService(t, &listingPlanStore{plans: []*storage.Plan{member}})
	primary := primaryPlanRow("testapp-001")

	_, _, err := svc.createStoredApply(t.Context(), primary, ApplyRequest{Environment: "production"}, nil, "apply-unsafe-member")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rollout member eu/testapp-002")
	assert.Contains(t, err.Error(), "legacy_orders")
	assert.Contains(t, err.Error(), "allow_unsafe")

	_, _, err = svc.createStoredApply(t.Context(), primary, ApplyRequest{Environment: "production"},
		map[string]string{"allow_unsafe": "true"}, "apply-unsafe-member-opted-in")
	assert.NotContains(t, fmt.Sprint(err), "allow_unsafe",
		"the opt-in the operator gave covers every member's plan")
}
