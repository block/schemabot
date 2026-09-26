//go:build integration

package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// TestOperator_StaleOperationUnderFailedParentReleasesTheRolloutTargets covers
// the release path for a rollout kept live by an operation under a finished
// parent. A rollout recorded failed on region-a while region-b was still
// running, and region-b's driver then died, leaving its operation running with
// a stale heartbeat. That operation keeps the rollout's targets reserved, so a
// new apply on either deployment is refused. The next poll re-leases the stale
// operation whatever its parent's state, settles it from the failed parent,
// and both deployments are free again.
func TestOperator_StaleOperationUnderFailedParentReleasesTheRolloutTargets(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)

	seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
		applyIdentifier: "stale-under-failed-parent",
		parentState:     state.Apply.Failed,
		cutoverPolicy:   storage.CutoverPolicyRolling,
		onFailure:       storage.OnFailureHalt,
		deployments:     []string{"region-a", "region-b"},
		perOpState: map[string]string{
			"region-a": state.ApplyOperation.Failed,
			"region-b": state.ApplyOperation.Running,
		},
		perTaskState: map[string]string{
			"region-a": state.Task.Failed,
			"region-b": state.Task.Running,
		},
	})
	staleID := seed.opID("region-b")
	_, err := db.ExecContext(ctx, `
		UPDATE apply_operations
		SET lease_owner = 'crashed-driver', lease_token = 'crashed-token', updated_at = NOW() - INTERVAL 10 MINUTE
		WHERE id = ?`, staleID)
	require.NoError(t, err, "age region-b's heartbeat past the staleness window")

	createOn := func(deployment, identifier string) error {
		_, err := stor.Applies().Create(ctx, &storage.Apply{
			ApplyIdentifier: identifier,
			Database:        "payments",
			DatabaseType:    storage.DatabaseTypeMySQL,
			Repository:      "octocat/hello-world",
			PullRequest:     2,
			Environment:     "staging",
			Deployment:      deployment,
			Engine:          storage.EngineForType(storage.DatabaseTypeMySQL),
			State:           state.Apply.Pending,
			Options:         storage.MarshalApplyOptions(storage.ApplyOptions{}),
		})
		return err
	}
	require.ErrorIs(t, createOn("region-b", "region-b-while-stale"), storage.ErrActiveApplyExists,
		"the stale running operation still holds region-b")
	require.ErrorIs(t, createOn("region-a", "region-a-while-stale"), storage.ErrActiveApplyExists,
		"the rollout it keeps live still holds region-a")

	svc := newMatrixService(t, stor, map[string]tern.Client{})
	svc.recoverApplyOperation(ctx, 1, "recovering-driver")

	op, err := stor.ApplyOperations().Get(ctx, staleID)
	require.NoError(t, err)
	require.NotNil(t, op)
	assert.Equal(t, state.ApplyOperation.Failed, op.State, "the re-leased operation settles from its failed parent")
	parent, err := stor.Applies().Get(ctx, seed.applyID)
	require.NoError(t, err)
	require.NotNil(t, parent)
	assert.Equal(t, state.Apply.Failed, parent.State, "settling the operation does not reopen the failed parent")

	require.NoError(t, createOn("region-b", "region-b-after-release"), "region-b is free once its operation settles")
	require.NoError(t, createOn("region-a", "region-a-after-release"), "region-a is free once no operation of the rollout is in progress")
}
