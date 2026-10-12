//go:build integration

package api

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// unansweredStartClient completes the operation's tasks and stores the parent
// apply completed, then reports that it could not answer the pending start —
// the drive that finds no remaining work, records the outcome, and loses the
// storage write answering the operator's start.
type unansweredStartClient struct {
	*matrixTernClient
}

func (c *unansweredStartClient) ResumeApplyOperation(ctx context.Context, apply *storage.Apply, applyOperationID int64) error {
	if err := c.matrixTernClient.ResumeApplyOperation(ctx, apply, applyOperationID); err != nil {
		return err
	}
	completed := *apply
	now := time.Now()
	completed.State = state.Apply.Completed
	completed.CompletedAt = &now
	if err := c.stor.Applies().Update(ctx, &completed); err != nil {
		return fmt.Errorf("unanswered start fake: store apply %s completed: %w", apply.ApplyIdentifier, err)
	}
	return fmt.Errorf("%w: complete pending start for apply %s: control request store unavailable",
		tern.ErrPostTerminalReconcile, apply.ApplyIdentifier)
}

// An operator started an apply whose resumed drive found nothing left to do.
// The drive stored the apply completed, then storage refused the write
// answering the operator's start. The completed outcome stands and nothing
// re-claims a completed apply, so the operator's post-drive settlement is the
// last chance to answer the start: the start reads as completed, and the
// operation row settles from its completed task instead of staying claimable.
func TestOperator_StartUnansweredAfterTheStoredOutcomeIsSettledByTheOperator(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)

	seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
		applyIdentifier: "start-unanswered-after-outcome",
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
	_, _, err = stor.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     seed.applyID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "matrix-test",
	})
	require.NoError(t, err, "request pending start")

	op, err := stor.ApplyOperations().FindNextApplyOperation(ctx, "recovering-driver")
	require.NoError(t, err)
	require.NotNil(t, op, "the operation must be claimable for the scenario")

	svc := newMatrixService(t, stor, map[string]tern.Client{
		"region-a/staging": &unansweredStartClient{matrixTernClient: &matrixTernClient{
			mockTernClient: &mockTernClient{},
			stor:           stor,
			deployment:     "region-a",
			rec:            &driveRecorder{},
			outcome:        matrixOutcome{taskState: state.Task.Completed},
		}},
	})
	svc.recoverSingleApplyOperation(ctx, 1, "recovering-driver", op, op.Lease())

	start, err := stor.ControlRequests().GetByOperation(ctx, seed.applyID, storage.ControlOperationStart)
	require.NoError(t, err)
	require.NotNil(t, start)
	assert.Equal(t, storage.ControlRequestCompleted, start.Status,
		"the operator answers the start the drive could not, against the stored completed outcome")

	persistedOp, err := stor.ApplyOperations().Get(ctx, op.ID)
	require.NoError(t, err)
	require.NotNil(t, persistedOp)
	assert.Equal(t, state.ApplyOperation.Completed, persistedOp.State,
		"the operation row settles from its completed task rather than staying claimable")

	apply, err := stor.Applies().Get(ctx, seed.applyID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	assert.Equal(t, state.Apply.Completed, apply.State, "the stored outcome stands")
}
