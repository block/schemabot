package api

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
)

const workAddEmail = "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"

func independentRollup(t *testing.T, diffs ...DeploymentPlanDiff) PlanRollup {
	t.Helper()
	rollup, err := RollupDeploymentDiffs(diffs, rollupMembers(diffs), PlanIndependent)
	require.NoError(t, err)
	return rollup
}

// A member with work is counted whether or not the primary has any, and the
// members that have nothing to run are not.
func TestPlanRollup_MembersWithWork(t *testing.T) {
	rollup := independentRollup(t,
		rollupDeployment("eu"),
		rollupDeployment("us", rollupAlterUsers(workAddEmail)),
		rollupDeployment("au"),
	)
	require.True(t, rollup.Clean)
	assert.Equal(t, []routing.ExecutionTarget{{Deployment: "us", Target: "us"}}, rollup.MembersWithWork())

	assert.Empty(t, independentRollup(t, rollupDeployment("eu"), rollupDeployment("us")).MembersWithWork())
}

// An errored member has no plan to read work from, so it is not counted, and
// the rollup carrying it is not Clean for the caller to refuse.
func TestPlanRollup_MembersWithWorkSkipsErroredMembers(t *testing.T) {
	failed := rollupDeployment("us", rollupAlterUsers(workAddEmail))
	failed.Err = errors.New("dial target")
	rollup := independentRollup(t, rollupDeployment("eu"), failed)

	assert.False(t, rollup.Clean)
	assert.Empty(t, rollup.MembersWithWork())
}

// A member after the primary with work puts a copy at stake when its diff
// discards one, or when its data plane did not report copies at all. An adopted
// copy, and members with nothing to run, put nothing at stake.
func TestPlanRollup_MemberCopyAtStake(t *testing.T) {
	reported := func(d DeploymentPlanDiff, copies ...*ternv1.ExistingCopy) DeploymentPlanDiff {
		d.ExistingCopiesReported = true
		d.ExistingCopies = copies
		return d
	}
	adopt := &ternv1.ExistingCopy{Namespace: "testapp", Disposition: "adopt", Tables: []string{"users"}}
	discard := &ternv1.ExistingCopy{Namespace: "testapp", Disposition: "discard", Tables: []string{"users"}}

	at, reason := independentRollup(t,
		rollupDeployment("eu", rollupAlterUsers(workAddEmail)),
		reported(rollupDeployment("us", rollupAlterUsers(workAddEmail)), adopt),
		rollupDeployment("au"),
	).MemberCopyAtStake()
	assert.Equal(t, -1, at, "the primary's copies are on the primary target's plan, an adopt continues the work, and au has nothing to run")
	assert.Empty(t, reason)

	at, reason = independentRollup(t,
		rollupDeployment("eu"),
		reported(rollupDeployment("us", rollupAlterUsers(workAddEmail)), adopt, discard),
	).MemberCopyAtStake()
	assert.Equal(t, 1, at)
	assert.Equal(t, `applying its plan discards the unfinished copy of users in namespace "testapp"`, reason)

	at, reason = independentRollup(t,
		rollupDeployment("eu"),
		rollupDeployment("us", rollupAlterUsers(workAddEmail)),
	).MemberCopyAtStake()
	assert.Equal(t, 1, at, "a member whose data plane did not report copies cannot be shown to have none")
	assert.Contains(t, reason, "did not report")
}

// A member's copy disclosure travels from its data plane's PlanDiff response,
// through the review rollup, to the member the PR apply refuses on: a member
// reporting a copy its plan discards is the one at stake, a member reporting a
// clean target is not, and a member whose data plane read nothing (a PostgreSQL
// or PlanetScale target, or an older data plane) is at stake as an unknown.
func TestRollupReviewTimeDrift_MemberCopyDisclosureReachesTheRollup(t *testing.T) {
	discard := &ternv1.ExistingCopy{Namespace: "testapp", Disposition: "discard", Reason: "statement_differs", Tables: []string{"users"}}
	cases := []struct {
		name       string
		copies     []*ternv1.ExistingCopy
		reported   bool
		wantAt     int
		wantReason string
	}{
		{name: "reported copy the plan discards", copies: []*ternv1.ExistingCopy{discard}, reported: true, wantAt: 1,
			wantReason: `applying its plan discards the unfinished copy of users in namespace "testapp"`},
		{name: "reported clean target", reported: true, wantAt: -1},
		{name: "data plane did not read the target", wantAt: 1,
			wantReason: "its data plane did not report whether applying its plan discards an unfinished copy"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			diff := alterUsersDiff("ALTER TABLE `users` ADD COLUMN `phone` varchar(32)")
			diff.ExistingCopies = tc.copies
			diff.ExistingCopiesReported = tc.reported
			svc := multiTargetService(t, &mockTernClient{planDiffResp: diff}, &recordingPlanStore{})

			rollup, err := svc.RollupReviewTimeDrift(t.Context(), planDiffReq(t), &ternv1.PlanResponse{PlanId: "plan_eu", Engine: ternv1.Engine_ENGINE_SPIRIT}, multiTargetMember("testapp-001"))
			require.NoError(t, err)
			require.True(t, rollup.Clean)

			at, reason := rollup.MemberCopyAtStake()
			assert.Equal(t, tc.wantAt, at)
			assert.Equal(t, tc.wantReason, reason)
		})
	}
}
