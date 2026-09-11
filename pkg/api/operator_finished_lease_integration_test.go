//go:build integration

package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
)

// A drive that ends leaves a lease that reads exactly like one a driver just
// took, and retryable expiry gates on that lease alone. The driver therefore has
// to hand the lease back as it returns, or every ended operation defers its
// apply's expiry for a full staleness window.
func TestOperator_FinishedDriveHandsItsOperationLeaseBack(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)

	// Both ways a drive ends. A completed deployment is the ordinary one and the
	// one a lease-only gate would otherwise stall on every successful rollout;
	// failed_retryable is the one whose leftover lease is indistinguishable from
	// a live retry's.
	for _, ended := range []struct {
		name      string
		taskState string
		opState   string
	}{
		{"completed", state.Task.Completed, state.ApplyOperation.Completed},
		{"failed", state.Task.Failed, state.ApplyOperation.Failed},
	} {
		t.Run(ended.name, func(t *testing.T) {
			resetMatrixTables(t, ctx, db)
			seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
				applyIdentifier: "finished-drive-handback-" + ended.name,
				parentState:     state.Apply.Pending,
				cutoverPolicy:   storage.CutoverPolicyRolling,
				onFailure:       storage.OnFailureHalt,
				deployments:     []string{"region-a"},
				opState:         state.ApplyOperation.Pending,
				taskState:       state.Task.Pending,
			})

			svc := newMatrixService(t, stor, matrixClients(stor, &driveRecorder{}, map[string]matrixOutcome{
				"region-a": {taskState: ended.taskState, errMsg: "drive ended"},
			}))
			svc.recoverApplyOperation(ctx, 1, "handback-driver")

			op, err := stor.ApplyOperations().Get(ctx, seed.opID("region-a"))
			require.NoError(t, err)
			require.NotNil(t, op)
			require.Equal(t, ended.opState, op.State, "the drive must reach the ended state under test")
			assert.Empty(t, op.LeaseOwner, "a drive that ended must not leave its lease on the row")
			assert.Empty(t, op.LeaseToken, "a released operation carries no lease token")
		})
	}
}

// Shutdown owns the handback from the moment the claim drain opens, because it
// is the only path that waits for the engine to come down first. A drive that
// returns during the drain returns because StopOperator cancelled it, and its
// engine is still copying: clearing the lease there would advertise the
// operation as undriven while this process still holds the target's lock, and
// the lease-only expiry gate would let a peer settle the tree mid-change.
func TestOperator_ClaimDrainKeepsTheOperationLeaseForShutdown(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	stor := mysqlstore.New(openMatrixStorage(t))

	seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
		applyIdentifier: "drain-retains-lease",
		parentState:     state.Apply.Pending,
		cutoverPolicy:   storage.CutoverPolicyRolling,
		onFailure:       storage.OnFailureHalt,
		deployments:     []string{"region-a"},
		opState:         state.ApplyOperation.Pending,
		taskState:       state.Task.Pending,
	})

	// The drive runs to completion, so this is the strongest form of the case:
	// even an operation that plainly ended keeps its lease while the drain is
	// open, because the drain — not the drive — knows whether the engine is down.
	svc := newMatrixService(t, stor, matrixClients(stor, &driveRecorder{}, map[string]matrixOutcome{
		"region-a": {taskState: state.Task.Completed},
	}))
	svc.beginClaimDrain()
	svc.recoverApplyOperation(ctx, 1, "draining-driver")

	op, err := stor.ApplyOperations().Get(ctx, seed.opID("region-a"))
	require.NoError(t, err)
	require.NotNil(t, op)
	assert.NotEmpty(t, op.LeaseToken,
		"a drive returning during the drain must leave its lease for shutdown to hand back after the halt")

	// The claim is left registered rather than dropped, so the shutdown handback
	// has something to collect and can refuse a deployment that did not halt.
	_, heldOperations := svc.drainHeldClaims()
	require.Len(t, heldOperations, 1, "the drive's claim must still be registered for shutdown to drain")
	assert.Equal(t, op.LeaseToken, heldOperations[0].lease.Token,
		"the registered claim must be the lease still on the row")
}
