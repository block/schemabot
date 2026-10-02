package api

import (
	"bytes"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// reviewedPlanStore holds the primary plan of testapp/production the way its
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

// Targets that run the same DDL directly are still separate groups when the
// reasons for their verdicts differ: the reason carries the target's own table
// size, and a group's changes carry one reason for every member it names. So
// testapp-001's 12 MiB table and testapp-002's 40 MiB one are each shown with
// their own size rather than both under the primary's.
func TestPlanRollout_SameDirectDDLUnderDifferentReasonsSplitsGroups(t *testing.T) {
	ddl := "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"
	const primaryReason = "table is 12 MiB, within the direct execution bound"
	const memberReason = "table is 40 MiB, within the direct execution bound"
	reviewed := reviewedUsersPlan(ddl)
	reviewed.Changes[0].TableChanges[0].ExecutionMode = "direct"
	reviewed.Changes[0].TableChanges[0].ModeReason = primaryReason
	member := alterUsersDiff(ddl)
	member.Changes[0].TableChanges[0].ExecutionMode = "direct"
	member.Changes[0].TableChanges[0].ModeReason = memberReason
	svc := multiTargetService(t, &mockTernClient{planDiffResp: member}, reviewedPlanStore(t, reviewed, "testapp-001"))

	rollout, err := svc.planRollout(t.Context(), planDiffReq(t), reviewed,
		&apitypes.PlanResponse{Deployment: "eu", Target: "testapp-001"})
	require.NoError(t, err)
	require.NotNil(t, rollout)
	require.Len(t, rollout.Groups, 2, "the same direct DDL under different reasons is two groups")

	for i, want := range []struct {
		member string
		reason string
	}{{"eu/testapp-001", primaryReason}, {"eu/testapp-002", memberReason}} {
		group := rollout.Groups[i]
		assert.Equal(t, []string{want.member}, group.Members)
		require.Len(t, group.Changes, 1)
		require.Len(t, group.Changes[0].TableChanges, 1)
		assert.Equal(t, "direct", group.Changes[0].TableChanges[0].ExecutionMode)
		assert.Equal(t, want.reason, group.Changes[0].TableChanges[0].ModeReason, "%s is shown with its own reason", want.member)
	}
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

// recordLogs points the service's logger at a buffer, so a test can read what
// an operator would see in the server logs.
func recordLogs(svc *Service) *bytes.Buffer {
	var logs bytes.Buffer
	svc.logger = slog.New(slog.NewTextHandler(&logs, nil))
	return &logs
}

// After an apply narrowed to the primary eu lands, eu is at the desired schema
// while us, which mirrors it, still lacks the email column. us diverges from
// the primary, and a rollout-wide apply is refused while it does, so planning
// again alone never changes the answer: its detail names the narrowed apply
// that brings it in line, with the selector --target accepts for it. The
// server log names what us would run that the primary does not, and says
// that the refusal is the CLI's, since the server pairs a mirrored member
// with the primary's plan.
func TestPlanRollout_MirroredMemberBehindAConvergedPrimaryNamesTheNarrowedApply(t *testing.T) {
	svc := mirroredService(t, &mockTernClient{},
		&mockTernClient{planDiffResp: alterUsersDiff("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")}, &recordingPlanStore{})
	logs := recordLogs(svc)
	converged := &ternv1.PlanResponse{PlanId: "plan_eu", Engine: ternv1.Engine_ENGINE_SPIRIT}

	rollout, err := svc.planRollout(t.Context(), planDiffReq(t), converged,
		&apitypes.PlanResponse{Deployment: "eu", Target: "testapp"})
	require.NoError(t, err)
	require.NotNil(t, rollout)
	require.Len(t, rollout.Attention, 1)
	assert.Equal(t, "us", rollout.Attention[0].Member)
	assert.Equal(t, apitypes.PlanMemberDiverged, rollout.Attention[0].Reason)
	assert.Contains(t, rollout.Attention[0].Detail, "apply each target on its own with --target until they match (this one is --target us/testapp)")

	out := logs.String()
	assert.Contains(t, out, `msg="rollout member diverged from the primary it mirrors; the plan lists it as needing attention, and the CLI refuses a rollout-wide apply until it matches"`)
	assert.Contains(t, out, "deployment=us")
	assert.Contains(t, out, "testapp.users")
}

// What becomes of a rollout-wide apply beside a member that could not be
// planned depends on how the environment plans its members, and the warning
// says which: a target planned against its own schema has no stored plan, so
// apply creation refuses the apply; a mirrored deployment would be paired with
// the primary's plan, so the CLI refuses it on the attention entry.
func TestPlanRollout_UnplannedMemberLogSaysWhoRefusesTheApply(t *testing.T) {
	dialErr := errors.New("dial tcp 10.0.0.7:3306: connection refused")
	reviewed := reviewedUsersPlan("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")

	independent := multiTargetService(t, &mockTernClient{planDiffErr: dialErr}, &recordingPlanStore{})
	independentLogs := recordLogs(independent)
	_, err := independent.planRollout(t.Context(), planDiffReq(t), reviewed, &apitypes.PlanResponse{Deployment: "eu", Target: "testapp-001"})
	require.NoError(t, err)
	assert.Contains(t, independentLogs.String(), `msg="rollout member could not be planned; apply creation refuses a rollout-wide apply of this plan, which stores no plan for it"`)

	mirrored := mirroredService(t, &mockTernClient{}, &mockTernClient{planDiffErr: dialErr}, &recordingPlanStore{})
	mirroredLogs := recordLogs(mirrored)
	_, err = mirrored.planRollout(t.Context(), planDiffReq(t), reviewed, &apitypes.PlanResponse{Deployment: "eu", Target: "testapp"})
	require.NoError(t, err)
	assert.Contains(t, mirroredLogs.String(), `msg="rollout member could not be planned; the plan lists it as needing attention, and the CLI refuses a rollout-wide apply until it is planned"`)
}

// The primary plan that reported errors already fails the plan, so no other
// member is planned beside it. The rollout still says so, listing each other
// member as not planned, so an operator reading the primary's errors can tell
// the other targets were never looked at rather than found fine.
func TestPlanRollout_ListsTheOtherMembersAsNotPlannedBesideAPrimaryPlanWithErrors(t *testing.T) {
	client := &mockTernClient{planDiffResp: alterUsersDiff("ALTER TABLE `users` ADD COLUMN `phone` varchar(32)")}
	svc := multiTargetService(t, client, &recordingPlanStore{})
	reviewed := reviewedUsersPlan("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	reviewed.Errors = []string{"users.sql: syntax error"}

	rollout, err := svc.planRollout(t.Context(), planDiffReq(t), reviewed,
		&apitypes.PlanResponse{Deployment: "eu", Target: "testapp-001"})
	require.NoError(t, err)
	assert.Nil(t, client.planDiffReq, "no member is diffed beside a failed primary plan")
	require.NotNil(t, rollout, "the rollout says the other members were not planned")
	assert.Equal(t, 2, rollout.Members)
	assert.True(t, rollout.Independent)
	assert.Empty(t, rollout.Groups, "no member ran a plan to group")
	assert.Empty(t, rollout.Refused)
	assert.Equal(t, []*apitypes.PlanMemberAttentionResponse{{
		Member: "eu/testapp-002", Reason: apitypes.PlanMemberUnplanned,
		Detail: "not planned, because the primary's plan reported errors; fix them, then plan again",
	}}, rollout.Attention)
}

// Once every member is planned, the plan reads its stored rows back to list
// what a rollout-wide apply refuses. A failure there fails the plan rather
// than returning an empty refusal list, and its error says that planning
// succeeded and the read-back is what failed.
func TestPlanRollout_RefusalCheckFailureFailsThePlanAndSaysWhatFailed(t *testing.T) {
	svc := multiTargetService(t, &mockTernClient{planDiffResp: alterUsersDiff("ALTER TABLE `users` ADD COLUMN `phone` varchar(32)")}, &recordingPlanStore{})

	_, err := svc.planRollout(t.Context(), planDiffReq(t), reviewedUsersPlan("ALTER TABLE `users` ADD COLUMN `email` varchar(255)"),
		&apitypes.PlanResponse{Deployment: "eu", Target: "testapp-001"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "every rollout member of plan plan_eu was planned, but the members a rollout-wide apply of it refuses could not be listed")
	assert.Contains(t, err.Error(), "plan plan_eu was stored, but no row carries its identifier")
}
