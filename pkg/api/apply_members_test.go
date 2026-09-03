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

func (s *listingPlanStore) List(context.Context, storage.ListPlansOptions) ([]*storage.Plan, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.plans, nil
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

func targetsFor(t *testing.T, svc *Service) []routing.ExecutionTarget {
	t.Helper()
	targets, err := svc.config.ResolveDatabaseTargets("testapp", "production")
	require.NoError(t, err)
	return targets
}

// Members that are expected to hold the same schema all run the plan the
// operator reviewed, so every member is paired with the apply's own plan and no
// member-plan lookup is needed.
func TestResolveApplyMembers_MirroredMembersShareTheApplyPlan(t *testing.T) {
	plans := &listingPlanStore{listErr: errors.New("List must not be called for mirrored members")}
	svc := memberResolutionService(t, mirroredEnv(), plans)
	plan := primaryPlanRow("testapp")

	members, err := svc.resolveApplyMembers(t.Context(), plan, "production", targetsFor(t, svc))
	require.NoError(t, err)
	require.Len(t, members, 2)
	for _, member := range members {
		assert.Same(t, plan, member.Plan, "member %s must run the reviewed plan", member.MemberID())
	}
}

// Each target of a multi-target environment runs the plan stored for that
// target in the same review round, matched on the head SHA the apply's plan was
// created for.
func TestResolveApplyMembers_IndependentMembersRunTheirOwnPlans(t *testing.T) {
	plan := primaryPlanRow("testapp-001")
	secondPlan := &storage.Plan{ID: 11, PlanIdentifier: "plan-second", Deployment: "eu", Target: "testapp-002", HeadSHA: "abc123"}
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

// A plan stored for an earlier push is a different review round. Matching it to
// this apply would run DDL the operator never reviewed on this commit, so the
// member counts as unplanned and apply creation fails.
func TestResolveApplyMembers_MemberPlanFromAnotherRoundIsNotUsed(t *testing.T) {
	plan := primaryPlanRow("testapp-001")
	stale := &storage.Plan{ID: 9, PlanIdentifier: "plan-stale", Deployment: "eu", Target: "testapp-002", HeadSHA: "older"}
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

// Member plans are only written by a pull request review, so a plan with no head
// SHA has no round to match against and cannot drive a multi-target apply.
func TestResolveApplyMembers_PlanWithoutHeadSHAFailsClosed(t *testing.T) {
	plan := primaryPlanRow("testapp-001")
	plan.HeadSHA = ""
	plans := &listingPlanStore{}
	svc := memberResolutionService(t, multiTargetEnv(), plans)

	_, err := svc.resolveApplyMembers(t.Context(), plan, "production", targetsFor(t, svc))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no head SHA to match member plans against")
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

// A sharded plan produces the same operation key for the same (namespace, shard,
// table) on every member, so operations are grouped by member and key together.
// Two targets of one deployment each get their own operation for that key rather
// than one target's shard work being folded into the other's.
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

	groups, err := buildShardedApplyOperationGroups(applyPlan, members, "production", storage.ApplyOptions{}, "", "", pershardTestTime())
	require.NoError(t, err)
	require.Len(t, groups, 2, "each target needs its own operation for the shard's table")

	byTarget := map[string]*storage.ApplyOperationWithTasks{}
	for _, g := range groups {
		byTarget[g.Operation.Target] = g
	}
	require.Contains(t, byTarget, "testapp-001")
	require.Contains(t, byTarget, "testapp-002")
	for target, group := range byTarget {
		assert.Equal(t, pershardNamespace+"/-80/mutes", group.Operation.OperationKey)
		assert.Equal(t, "eu", group.Operation.Deployment)
		require.Len(t, group.Tasks, 1, "target %s must carry its shard's work exactly once", target)
		assert.Equal(t, mutesDDL, group.Tasks[0].DDL)
		assert.Equal(t, "-80", group.Tasks[0].Shard)
	}
	assert.Zero(t, byTarget["testapp-001"].Operation.PlanID)
	assert.Equal(t, int64(11), byTarget["testapp-002"].Operation.PlanID)
}
