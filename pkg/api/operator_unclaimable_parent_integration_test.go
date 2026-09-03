//go:build integration

package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// TestOperator_UnclaimableParentReleasesOperationLease covers the driver's
// behavior when it claims an operation but a peer holds a fresh lease on the
// parent apply, so the parent claim the drive also needs is refused. The
// driver must release the operation lease it just acquired so any driver can
// retry the operation on the very next poll — retaining the fresh lease would
// stall the operation for the full lease staleness window over a refusal that
// clears as soon as the peer's lease does.
func TestOperator_UnclaimableParentReleasesOperationLease(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)

	seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
		applyIdentifier: "unclaimable-parent-release",
		parentState:     state.Apply.Pending,
		cutoverPolicy:   storage.CutoverPolicyRolling,
		onFailure:       storage.OnFailureHalt,
		deployments:     []string{"region-a"},
		opState:         state.ApplyOperation.Pending,
		taskState:       state.Task.Pending,
	})

	// A peer driver claims the parent apply first, holding a fresh lease on it.
	peerApply, err := stor.Applies().ClaimApplyByID(ctx, seed.applyID, "peer-driver")
	require.NoError(t, err)
	require.NotNil(t, peerApply, "the peer must hold the parent apply lease for the scenario")

	svc := newMatrixService(t, stor, map[string]tern.Client{})
	svc.recoverApplyOperation(ctx, 1, "blocked-driver")

	opID := seed.opID("region-a")
	op, err := stor.ApplyOperations().Get(ctx, opID)
	require.NoError(t, err)
	require.NotNil(t, op)
	assert.Empty(t, op.LeaseOwner, "the blocked driver must release the operation lease it cannot use")
	assert.Empty(t, op.LeaseToken, "the released operation must carry no lease token")
	assert.Equal(t, state.ApplyOperation.Running, op.State,
		"the claimed operation keeps its state; the release only surrenders the lease")
	assert.True(t, op.UpdatedAt.Before(time.Now().Add(-storage.ApplyLeaseStaleAfter/2)),
		"the released operation's heartbeat must be backdated past the staleness window")

	reclaimed, err := stor.ApplyOperations().FindNextApplyOperation(ctx, "retry-driver")
	require.NoError(t, err)
	require.NotNil(t, reclaimed, "the released operation must be re-claimable on the next poll")
	assert.Equal(t, opID, reclaimed.ID)
}
