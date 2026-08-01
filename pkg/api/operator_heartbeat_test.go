package api

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// TestOperationHeartbeatFailureStopsDrive verifies how an operation drive
// reacts to a failed operation-row heartbeat write: a definitively lost
// operation lease stops the driver immediately, a transient storage error
// inside the lease staleness window keeps the run going so the heartbeat is
// retried on the next tick, and failures that have persisted for the full
// window stop the driver because a peer can already have reclaimed the stale
// operation row.
func TestOperationHeartbeatFailureStopsDrive(t *testing.T) {
	svc := newTestService()
	op := &storage.ApplyOperation{
		ID:            7,
		ApplyID:       41,
		Deployment:    "default",
		OperationKind: storage.ApplyOperationKindWork,
		State:         state.Apply.Running,
	}
	apply := &storage.Apply{
		ID:              41,
		ApplyIdentifier: "apply-operation-heartbeat",
		Database:        "widgets",
		Deployment:      "default",
		Environment:     "staging",
		State:           state.Apply.Running,
	}

	t.Run("lost operation lease stops the driver immediately", func(t *testing.T) {
		hbErr := fmt.Errorf("heartbeat apply_operation %d: %w", op.ID, storage.ErrApplyLeaseLost)
		assert.True(t, svc.operationHeartbeatFailureStopsDrive(t.Context(), 1, op, apply, hbErr, time.Now()))
	})

	t.Run("transient failure inside the window keeps driving", func(t *testing.T) {
		hbErr := errors.New("connection refused")
		assert.False(t, svc.operationHeartbeatFailureStopsDrive(t.Context(), 1, op, apply, hbErr, time.Now()))
	})

	t.Run("failures spanning the staleness window stop the driver", func(t *testing.T) {
		hbErr := errors.New("connection refused")
		lastSuccess := time.Now().Add(-storage.ApplyLeaseStaleAfter)
		assert.True(t, svc.operationHeartbeatFailureStopsDrive(t.Context(), 1, op, apply, hbErr, lastSuccess))
	})
}
