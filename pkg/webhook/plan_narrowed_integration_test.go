//go:build integration

package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

// An operator plans one member of a two-member rollout with --target, against
// planners that share the service's storage and store the plan row themselves,
// with no narrowing. The plan is returned narrowed to that member, its stored
// row records the narrowing, and an apply of the whole rollout from it is
// refused before any apply is created.
func TestE2ENarrowedPlanOnSharedStorageIsHeldToItsMember(t *testing.T) {
	dbName := "webhook_narrowed_shared_storage"
	svc := setupE2EReviewDriftService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersBaseSchema},
		{name: "us", liveSchema: usersBaseSchema},
	})
	target := dbName + "-us-target"
	member := "us/" + target
	pr := int32(1)

	planResp, err := svc.ExecutePlan(t.Context(), api.PlanRequest{
		Database:    dbName,
		Environment: driftEnv,
		Type:        "mysql",
		Repository:  "octocat/hello-world",
		PullRequest: &pr,
		Target:      target,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			dbName: {Files: map[string]string{"users.sql": usersWithEmailSchema}},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, planResp.Changes, "the member lacks the email column, so the plan has work")
	assert.Equal(t, member, planResp.NarrowedTo)

	stored, err := svc.Storage().Plans().Get(t.Context(), planResp.PlanID)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, "us", stored.Deployment)
	assert.Equal(t, target, stored.Target)
	assert.Equal(t, member, stored.NarrowedTo)

	_, _, err = svc.ExecuteApply(t.Context(), api.ApplyRequest{PlanID: planResp.PlanID, Environment: driftEnv})
	var mismatch *api.PlanMemberMismatchError
	require.ErrorAs(t, err, &mismatch)
	assert.True(t, mismatch.PlanNarrowed)
	assert.Contains(t, err.Error(), "apply it with target "+member)
	requireNoApplies(t, svc, dbName)
}
