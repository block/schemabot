package api

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// reviewedPlanStore holds the reviewed plan of testapp/production the way its
// planner stored it for the eu target named, so the rollout can pair each
// member with the plan an apply of it would run.
func reviewedPlanStore(t *testing.T, reviewed *ternv1.PlanResponse, target string) *recordingPlanStore {
	t.Helper()
	namespaces, err := protoChangesToNamespaces(reviewed.Changes, nil)
	require.NoError(t, err)
	return &recordingPlanStore{mockPlanLookupStore: mockPlanLookupStore{plan: &storage.Plan{
		PlanIdentifier: reviewed.PlanId,
		Database:       "testapp",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Environment:    "production",
		Deployment:     "eu",
		Target:         target,
		Namespaces:     namespaces,
	}}}
}

// A plan requested through the API for an environment whose targets each hold
// their own schema says what an apply would run on every target, one group per
// distinct plan with the primary's first, and stores the plan each non-primary
// target runs so an apply created from the response has one for every member.
func TestPlanRollout_IndependentTargetsGroupByPlan(t *testing.T) {
	reviewed := reviewedUsersPlan("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	plans := reviewedPlanStore(t, reviewed, "testapp-001")
	svc := multiTargetService(t, &mockTernClient{planDiffResp: alterUsersDiff("ALTER TABLE `users` ADD COLUMN `phone` varchar(32)")}, plans)

	rollout, err := svc.planRollout(t.Context(), planDiffReq(t), reviewed,
		&apitypes.PlanResponse{Deployment: "eu", Target: "testapp-001"})
	require.NoError(t, err)
	require.NotNil(t, rollout)

	assert.Equal(t, 2, rollout.Members)
	assert.True(t, rollout.Independent)
	assert.Empty(t, rollout.Attention)
	assert.Empty(t, rollout.Refused, "a rollout-wide apply runs each target's own safe ALTER")
	require.Len(t, rollout.Groups, 2, "the two targets plan different DDL")

	primary := rollout.Groups[0]
	assert.True(t, primary.Primary)
	assert.Equal(t, []string{"eu/testapp-001"}, primary.Members)
	require.Len(t, primary.Changes, 1)
	require.Len(t, primary.Changes[0].TableChanges, 1)
	assert.Equal(t, "users", primary.Changes[0].TableChanges[0].TableName)
	assert.Contains(t, primary.Changes[0].TableChanges[0].DDL, "ADD COLUMN `email`")

	other := rollout.Groups[1]
	assert.False(t, other.Primary)
	assert.Equal(t, []string{"eu/testapp-002"}, other.Members)
	require.Len(t, other.Changes, 1)
	require.Len(t, other.Changes[0].TableChanges, 1)
	assert.Contains(t, other.Changes[0].TableChanges[0].DDL, "ADD COLUMN `phone`")

	require.Len(t, plans.created, 1, "the non-primary target's plan is stored for the apply")
	assert.Equal(t, "testapp-002", plans.created[0].Target)
	assert.Equal(t, reviewed.PlanId, plans.created[0].PrimaryPlanIdentifier)
}

// Targets that would run the same work share one group.
func TestPlanRollout_MatchingTargetsShareAGroup(t *testing.T) {
	ddl := "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"
	svc := multiTargetService(t, &mockTernClient{planDiffResp: alterUsersDiff(ddl)}, reviewedPlanStore(t, reviewedUsersPlan(ddl), "testapp-001"))

	rollout, err := svc.planRollout(t.Context(), planDiffReq(t), reviewedUsersPlan(ddl),
		&apitypes.PlanResponse{Deployment: "eu", Target: "testapp-001"})
	require.NoError(t, err)
	require.NotNil(t, rollout)
	require.Len(t, rollout.Groups, 1)
	assert.True(t, rollout.Groups[0].Primary)
	assert.Equal(t, []string{"eu/testapp-001", "eu/testapp-002"}, rollout.Groups[0].Members)
}

// Targets that plan the same DDL but run it under different execution verdicts
// are separate groups: the direct execution policy judges each target's own
// table, and a group's changes carry one verdict for every member it names, so
// a target whose change runs as write-blocking native DDL is never shown under
// a target whose change runs through the engine.
func TestPlanRollout_SameDDLUnderDifferentExecutionModesSplitsGroups(t *testing.T) {
	ddl := "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"
	direct := alterUsersDiff(ddl)
	direct.Changes[0].TableChanges[0].ExecutionMode = "direct"
	direct.Changes[0].TableChanges[0].ModeReason = "table is 12 MiB, within the direct execution bound"
	svc := multiTargetService(t, &mockTernClient{planDiffResp: direct}, reviewedPlanStore(t, reviewedUsersPlan(ddl), "testapp-001"))

	rollout, err := svc.planRollout(t.Context(), planDiffReq(t), reviewedUsersPlan(ddl),
		&apitypes.PlanResponse{Deployment: "eu", Target: "testapp-001"})
	require.NoError(t, err)
	require.NotNil(t, rollout)
	require.Len(t, rollout.Groups, 2, "the same DDL under different verdicts is two groups")

	assert.Equal(t, []string{"eu/testapp-001"}, rollout.Groups[0].Members)
	require.Len(t, rollout.Groups[0].Changes, 1)
	require.Len(t, rollout.Groups[0].Changes[0].TableChanges, 1)
	assert.Empty(t, rollout.Groups[0].Changes[0].TableChanges[0].ExecutionMode)

	assert.Equal(t, []string{"eu/testapp-002"}, rollout.Groups[1].Members)
	require.Len(t, rollout.Groups[1].Changes, 1)
	require.Len(t, rollout.Groups[1].Changes[0].TableChanges, 1)
	tc := rollout.Groups[1].Changes[0].TableChanges[0]
	assert.Equal(t, "direct", tc.ExecutionMode)
	assert.Equal(t, "table is 12 MiB, within the direct execution bound", tc.ModeReason)

	// Only a pull request comment discloses testapp-002's direct change under
	// it, so a rollout-wide apply through the API refuses it, and the rollout
	// names the target selector that applies testapp-002 on its own.
	require.Len(t, rollout.Refused, 1)
	assert.Equal(t, &apitypes.PlanMemberRefusalResponse{
		Member: "eu/testapp-002",
		Target: "testapp-002",
		Reason: apitypes.PlanMemberNeedsTarget,
		Detail: `runs table "users" as direct-execution DDL, which a rollout-wide apply runs only from the pull request comment that discloses it under this target`,
	}, rollout.Refused[0])
}

// A target that could not be planned joins no group: it is listed for
// attention with a fixed detail, because the raw error can carry hostnames and
// dial failures that do not belong in a response.
func TestPlanRollout_UnplannedTargetNeedsAttention(t *testing.T) {
	svc := multiTargetService(t, &mockTernClient{planDiffErr: errors.New("dial tcp 10.0.0.7:3306: connection refused")}, &recordingPlanStore{})

	rollout, err := svc.planRollout(t.Context(), planDiffReq(t), reviewedUsersPlan("ALTER TABLE `users` ADD COLUMN `email` varchar(255)"),
		&apitypes.PlanResponse{Deployment: "eu", Target: "testapp-001"})
	require.NoError(t, err)
	require.NotNil(t, rollout)
	require.Len(t, rollout.Groups, 1)
	assert.Equal(t, []string{"eu/testapp-001"}, rollout.Groups[0].Members)
	require.Len(t, rollout.Attention, 1)
	assert.Equal(t, "eu/testapp-002", rollout.Attention[0].Member)
	assert.Equal(t, apitypes.PlanMemberUnplanned, rollout.Attention[0].Reason)
	assert.Equal(t, unplannedMemberDetail, rollout.Attention[0].Detail)
	assert.NotContains(t, rollout.Attention[0].Detail, "10.0.0.7")
}

// Deployments that mirror the primary run its plan, so one whose live schema
// would plan something else is listed for attention rather than grouped: the
// primary's plan does not describe it.
func TestPlanRollout_DivergedMirroredDeploymentNeedsAttention(t *testing.T) {
	svc := mirroredService(t, &mockTernClient{},
		&mockTernClient{planDiffResp: alterUsersDiff("ALTER TABLE `users` ADD COLUMN `phone` varchar(32)")}, &recordingPlanStore{})

	rollout, err := svc.planRollout(t.Context(), planDiffReq(t), reviewedUsersPlan("ALTER TABLE `users` ADD COLUMN `email` varchar(255)"),
		&apitypes.PlanResponse{Deployment: "eu", Target: "testapp"})
	require.NoError(t, err)
	require.NotNil(t, rollout)
	assert.False(t, rollout.Independent)
	require.Len(t, rollout.Groups, 1)
	assert.Equal(t, []string{"eu"}, rollout.Groups[0].Members)
	require.Len(t, rollout.Attention, 1)
	assert.Equal(t, "us", rollout.Attention[0].Member)
	assert.Equal(t, apitypes.PlanMemberDiverged, rollout.Attention[0].Reason)
}

// A primary plan that reported errors already fails the plan, so no member is
// planned alongside it.
func TestPlanRollout_SkipsAPrimaryPlanWithErrors(t *testing.T) {
	client := &mockTernClient{planDiffResp: alterUsersDiff("ALTER TABLE `users` ADD COLUMN `phone` varchar(32)")}
	svc := multiTargetService(t, client, &recordingPlanStore{})
	reviewed := reviewedUsersPlan("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	reviewed.Errors = []string{"users.sql: syntax error"}

	rollout, err := svc.planRollout(t.Context(), planDiffReq(t), reviewed,
		&apitypes.PlanResponse{Deployment: "eu", Target: "testapp-001"})
	require.NoError(t, err)
	assert.Nil(t, rollout)
	assert.Nil(t, client.planDiffReq, "no member is diffed beside a failed primary plan")
}
