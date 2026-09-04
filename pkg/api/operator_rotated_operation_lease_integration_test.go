//go:build integration

package api

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// TestOperator_RotatedOperationLeaseHandsBackTheParentApply covers the window
// between a driver's two claims. Leasing the operation and claiming the parent
// apply are separate transactions, so a peer can rotate the operation lease onto
// itself in between. Driving on would run engine work under a capability this
// driver no longer holds, and the refusal would surface as whichever
// operation-scoped write happened to be attempted first — a re-plan, a task
// transition — rather than as the claim race it is. The driver must hand the
// parent apply back instead, leaving the peer that owns the operation able to
// claim the parent and drive.
func TestOperator_RotatedOperationLeaseHandsBackTheParentApply(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)

	// A crashed retry: the parent apply is active with a stale heartbeat while
	// its operation row still says failed_retryable.
	seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
		applyIdentifier: "operation-lease-rotated",
		parentState:     state.Apply.Running,
		cutoverPolicy:   storage.CutoverPolicyRolling,
		onFailure:       storage.OnFailureHalt,
		deployments:     []string{"region-a"},
		opState:         state.ApplyOperation.FailedRetryable,
		taskState:       state.Task.FailedRetryable,
	})
	_, err := db.ExecContext(ctx,
		"UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?", seed.applyID)
	require.NoError(t, err, "backdate the parent apply heartbeat")
	stalenessBackdate(t, ctx, db, seed.applyID)

	op, err := stor.ApplyOperations().FindNextApplyOperation(ctx, "superseded-driver")
	require.NoError(t, err)
	require.NotNil(t, op, "the crashed retry must be recoverable for the scenario")
	staleLease := op.Lease()

	// A peer takes the operation lease before this driver reaches its parent
	// claim, leaving the driver holding a superseded token.
	released, err := stor.ApplyOperations().ReleaseClaim(ctx, staleLease)
	require.NoError(t, err)
	require.True(t, released)
	peerOp, err := stor.ApplyOperations().FindNextApplyOperation(ctx, "peer-driver")
	require.NoError(t, err)
	require.NotNil(t, peerOp, "the peer must hold the operation lease for the scenario")
	require.NotEqual(t, staleLease.Token, peerOp.LeaseToken)

	svc := newMatrixService(t, stor, map[string]tern.Client{})
	svc.recoverSingleApplyOperation(ctx, 1, "superseded-driver", op, staleLease)

	persistedOp, err := stor.ApplyOperations().Get(ctx, op.ID)
	require.NoError(t, err)
	require.NotNil(t, persistedOp)
	assert.Equal(t, "peer-driver", persistedOp.LeaseOwner, "the peer keeps the operation lease it rotated on")
	assert.Equal(t, peerOp.LeaseToken, persistedOp.LeaseToken, "the superseded driver must not rotate the lease back to itself")
	assert.Equal(t, state.ApplyOperation.FailedRetryable, persistedOp.State,
		"the superseded driver must not transition an operation it no longer leases")

	apply, err := stor.Applies().Get(ctx, seed.applyID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	assert.Empty(t, apply.LeaseOwner, "the parent apply lease must be handed back when the operation lease is gone")
	assert.Empty(t, apply.LeaseToken, "the released parent apply must carry no lease token")
	assert.Equal(t, state.Apply.Running, apply.State, "handing the parent back must not terminalize it")

	reclaimed, err := stor.Applies().ClaimApplyByID(ctx, seed.applyID, "peer-driver")
	require.NoError(t, err)
	require.NotNil(t, reclaimed, "the peer that owns the operation must be able to claim the parent apply")
	assert.Equal(t, "peer-driver", reclaimed.LeaseOwner)
}

// TestOperator_UnprovenOperationLeaseIsHandedBackForTheNextPoll covers the same
// pre-drive re-check when storage cannot answer it. Nothing showed the lease
// gone, so this driver still holds it — and the claim query gates a recovering
// operation on its own heartbeat, which this driver's claim just refreshed.
// Handing only the parent back would leave the operation unclaimable for a full
// staleness window over one failed read, so the driver releases both and the row
// is offered again on the next poll.
func TestOperator_UnprovenOperationLeaseIsHandedBackForTheNextPoll(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)

	seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
		applyIdentifier: "operation-lease-unproven",
		parentState:     state.Apply.Running,
		cutoverPolicy:   storage.CutoverPolicyRolling,
		onFailure:       storage.OnFailureHalt,
		deployments:     []string{"region-a"},
		opState:         state.ApplyOperation.FailedRetryable,
		taskState:       state.Task.FailedRetryable,
	})
	_, err := db.ExecContext(ctx,
		"UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?", seed.applyID)
	require.NoError(t, err, "backdate the parent apply heartbeat")
	stalenessBackdate(t, ctx, db, seed.applyID)

	op, err := stor.ApplyOperations().FindNextApplyOperation(ctx, "blip-driver")
	require.NoError(t, err)
	require.NotNil(t, op, "the crashed retry must be recoverable for the scenario")
	opLease := op.Lease()

	svc := newMatrixService(t, unreadableOperationStorage{Storage: stor}, map[string]tern.Client{})
	svc.recoverSingleApplyOperation(ctx, 1, "blip-driver", op, opLease)

	persistedOp, err := stor.ApplyOperations().Get(ctx, op.ID)
	require.NoError(t, err)
	require.NotNil(t, persistedOp)
	assert.Empty(t, persistedOp.LeaseOwner, "a driver that will not drive must not keep the operation lease")
	assert.Empty(t, persistedOp.LeaseToken, "the released operation must carry no lease token")
	assert.Equal(t, state.ApplyOperation.FailedRetryable, persistedOp.State,
		"handing the operation back must not transition it")

	apply, err := stor.Applies().Get(ctx, seed.applyID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	assert.Empty(t, apply.LeaseOwner, "the parent apply lease must be handed back alongside the operation lease")
	assert.Equal(t, state.Apply.Running, apply.State, "handing the parent back must not terminalize it")

	next, err := stor.ApplyOperations().FindNextApplyOperation(ctx, "peer-driver")
	require.NoError(t, err)
	require.NotNil(t, next, "the operation must be offered on the next poll, not held for a staleness window")
	assert.Equal(t, op.ID, next.ID)
	assert.Equal(t, "peer-driver", next.LeaseOwner)
}

// unreadableOperationStorage fails exactly the operation read the pre-drive
// lease re-check makes, the way a storage blip would, and delegates everything
// else to real storage so the release path is exercised against the database.
type unreadableOperationStorage struct {
	storage.Storage
}

func (s unreadableOperationStorage) ApplyOperations() storage.ApplyOperationStore {
	return unreadableOperationStore{ApplyOperationStore: s.Storage.ApplyOperations()}
}

type unreadableOperationStore struct {
	storage.ApplyOperationStore
}

func (s unreadableOperationStore) Get(context.Context, int64) (*storage.ApplyOperation, error) {
	return nil, errors.New("storage unavailable")
}
