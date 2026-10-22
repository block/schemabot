//go:build integration

package api

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// displacingTernClient stands in for a drive that a peer displaces partway
// through. While the drive runs, the operation's heartbeat goes stale and a
// peer leases the operation, is refused the parent apply this drive still
// holds, and releases the operation again. The drive then returns, as a drive
// does once its next operation-scoped write is refused.
type displacingTernClient struct {
	*mockTernClient
	t    *testing.T
	db   *sql.DB
	stor storage.Storage
}

func (c *displacingTernClient) ResumeApplyOperation(ctx context.Context, apply *storage.Apply, applyOperationID int64) error {
	stalenessBackdate(c.t, ctx, c.db, apply.ID)
	peerOp, err := c.stor.ApplyOperations().FindNextApplyOperation(ctx, "peer-driver")
	if err != nil {
		return fmt.Errorf("peer claim of operation %d: %w", applyOperationID, err)
	}
	if peerOp == nil || peerOp.ID != applyOperationID {
		return fmt.Errorf("peer did not lease the stale operation %d: %v", applyOperationID, peerOp)
	}
	refused, err := c.stor.Applies().ClaimApplyByID(ctx, apply.ID, "peer-driver")
	if err != nil {
		return fmt.Errorf("peer claim of parent apply %d: %w", apply.ID, err)
	}
	if refused != nil {
		return fmt.Errorf("peer claimed parent apply %d that the drive still holds", apply.ID)
	}
	released, err := c.stor.ApplyOperations().ReleaseClaim(ctx, peerOp.Lease())
	if err != nil {
		return fmt.Errorf("peer release of operation %d: %w", applyOperationID, err)
	}
	if !released {
		return fmt.Errorf("peer could not release operation %d", applyOperationID)
	}
	return nil
}

// A single-operation drive holds two leases: the operation's, renewed by the
// operation heartbeat, and the parent apply's, renewed by the apply heartbeat
// and by the drive's own state writes. When only the operation goes stale, a
// peer can take the operation but not the parent, so it hands the operation
// back, and the drive it displaced exits at its next operation-scoped write.
// That drive must hand back the parent lease too. Left in place, it refuses
// every claim for a full staleness window while nothing drives the apply.
//
// While shutdown's claim drain is open the parent lease is left alone, because
// shutdown hands claims back only after it has halted the engines.
func TestOperator_DisplacedDriveHandsBackTheParentApply(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)

	for _, tc := range []struct {
		name     string
		draining bool
	}{
		{name: "hands back the parent"},
		{name: "leaves the parent for shutdown during the claim drain", draining: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetMatrixTables(t, ctx, db)
			seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
				applyIdentifier: "displaced-drive-parent-handback",
				parentState:     state.Apply.Pending,
				cutoverPolicy:   storage.CutoverPolicyRolling,
				onFailure:       storage.OnFailureHalt,
				deployments:     []string{"region-a"},
				opState:         state.ApplyOperation.Pending,
				taskState:       state.Task.Pending,
			})

			svc := newMatrixService(t, stor, map[string]tern.Client{
				"region-a/staging": &displacingTernClient{mockTernClient: &mockTernClient{}, t: t, db: db, stor: stor},
			})
			if tc.draining {
				svc.beginClaimDrain()
			}
			svc.recoverApplyOperation(ctx, 1, "displaced-driver")

			apply, err := stor.Applies().Get(ctx, seed.applyID)
			require.NoError(t, err)
			require.NotNil(t, apply)
			assert.Equal(t, state.Apply.Running, apply.State, "the displaced drive must leave the parent's state as it found it")
			if tc.draining {
				assert.Equal(t, "displaced-driver", apply.LeaseOwner,
					"a drive returning during the drain must leave its parent lease for shutdown to hand back after the halt")
				return
			}
			assert.Empty(t, apply.LeaseToken, "the displaced drive must hand back the parent apply lease")

			op, err := stor.ApplyOperations().FindNextApplyOperation(ctx, "peer-driver")
			require.NoError(t, err)
			require.NotNil(t, op, "the released operation must be offered on the next poll")
			reclaimed, err := stor.Applies().ClaimApplyByID(ctx, seed.applyID, "peer-driver")
			require.NoError(t, err)
			require.NotNil(t, reclaimed, "the peer must claim the parent apply on its next poll, not a staleness window later")
			assert.Equal(t, "peer-driver", reclaimed.LeaseOwner)
		})
	}
}
