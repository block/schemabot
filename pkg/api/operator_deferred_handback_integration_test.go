//go:build integration

package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// handBackMatrixClient stands in for a drive that delivers a deferred apply's
// pending stop or cancel and stops there: under a delivery-only context it
// returns the hand-back without touching the operation, and otherwise drives
// like matrixTernClient.
type handBackMatrixClient struct {
	*matrixTernClient
	deliveryOnly []bool
}

func (c *handBackMatrixClient) ResumeApplyOperation(ctx context.Context, apply *storage.Apply, applyOperationID int64) error {
	deliveryOnly := tern.ControlDeliveryOnlyForDeferredCutover(ctx)
	c.deliveryOnly = append(c.deliveryOnly, deliveryOnly)
	if deliveryOnly {
		return tern.ErrDeferredCutoverDriveHandedBack
	}
	return c.matrixTernClient.ResumeApplyOperation(ctx, apply, applyOperationID)
}

// The reserved driver claims an apply deferring its cutover only to deliver a
// pending stop or cancel. When delivering it does not end the drive, the
// reserved driver hands every lease it took straight back, so the next poll of
// a driver that runs deferred work picks the operation up and drives it, rather
// than the reserved driver holding it at the cutover or the row sitting leased
// for a staleness window.
func TestOperator_ReservedDriverHandsADeferredDriveBackAfterDelivery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)

	// One deployment drives under the parent lease as well as the operation
	// lease; two drive under the operation lease alone.
	for _, tc := range []struct {
		name        string
		deployments []string
	}{
		{"single operation", []string{"region-a"}},
		{"multi operation", []string{"region-a", "region-b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetMatrixTables(t, ctx, db)
			seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
				applyIdentifier: "reserved-handback-" + tc.deployments[len(tc.deployments)-1],
				parentState:     state.Apply.Pending,
				cutoverPolicy:   storage.CutoverPolicyRolling,
				onFailure:       storage.OnFailureHalt,
				deployments:     tc.deployments,
				opState:         state.ApplyOperation.Pending,
				taskState:       state.Task.Pending,
			})

			outcomes := make(map[string]matrixOutcome, len(tc.deployments))
			for _, dep := range tc.deployments {
				outcomes[dep] = matrixOutcome{taskState: state.Task.Completed}
			}
			clients := matrixClients(stor, &driveRecorder{}, outcomes)
			regionA := &handBackMatrixClient{matrixTernClient: clients["region-a/staging"].(*matrixTernClient)}
			clients["region-a/staging"] = regionA
			svc := newMatrixService(t, stor, clients)
			svc.operatorDrivers.Store(2)

			svc.recoverApplyOperation(ctx, reservedDriverID, "reserved-driver")

			require.Equal(t, []bool{true}, regionA.deliveryOnly, "the reserved driver's drive runs delivery-only")
			op, err := stor.ApplyOperations().Get(ctx, seed.opID("region-a"))
			require.NoError(t, err)
			require.NotNil(t, op)
			assert.False(t, state.IsApplyOperationTerminal(op.State), "a handed-back drive leaves the operation to be resumed, got %s", op.State)
			assert.Empty(t, op.LeaseToken, "a handed-back drive must not leave its operation lease on the row")
			assert.Empty(t, getApply(t, ctx, stor, seed.applyID).LeaseToken, "a handed-back drive must not leave a parent lease on the apply")

			svc.recoverApplyOperation(ctx, reservedDriverID+1, "pool-driver")

			require.Equal(t, []bool{true, false}, regionA.deliveryOnly, "the next pool driver claims the handed-back operation at once")
			assert.Equal(t, state.ApplyOperation.Completed, opState(t, ctx, stor, seed.opID("region-a")))
		})
	}
}
