package api

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
