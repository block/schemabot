package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
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

// erroringTaskStore fails GetByApplyOperationID so a test can prove a liveness
// read failure never stops a drive.
type erroringTaskStore struct {
	storage.TaskStore
	err error
}

func (s *erroringTaskStore) GetByApplyOperationID(context.Context, int64) ([]*storage.Task, error) {
	return nil, s.err
}

// TestOperationDriveStalled verifies the drive liveness check that runs on
// every successful operation heartbeat: a drive whose poll loop keeps mirroring
// task rows is alive, while a drive that has mirrored nothing for the full
// stall window is presumed wedged and must be cancelled — its fresh heartbeat
// would otherwise hold the operation lease forever and no peer driver could
// reclaim the work. Group finalizers mirror no tasks and are exempt, young
// drives get the full window before their first mirror write, and a failed
// liveness read keeps the drive going.
func TestOperationDriveStalled(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	newStallService := func(tasks storage.TaskStore) *Service {
		return New(&mockStorageWithTasksAndOperations{tasks: tasks}, testServerConfig(), nil, logger)
	}
	op := &storage.ApplyOperation{
		ID:            7,
		ApplyID:       41,
		Deployment:    "default",
		OperationKind: storage.ApplyOperationKindWork,
		State:         state.Apply.Running,
	}
	apply := &storage.Apply{
		ID:              41,
		ApplyIdentifier: "apply-drive-stall",
		Database:        "widgets",
		Deployment:      "default",
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	stalledStart := time.Now().Add(-2 * ApplyDriveStallAfter)

	t.Run("a drive younger than the stall window is never stalled", func(t *testing.T) {
		svc := newStallService(&stubTaskStore{})
		assert.False(t, svc.operationDriveStalled(t.Context(), 1, op, apply, time.Now()))
	})

	t.Run("fresh task mirror writes keep the drive alive", func(t *testing.T) {
		svc := newStallService(&stubTaskStore{tasks: []*storage.Task{
			{State: state.Task.Running, UpdatedAt: time.Now().Add(-time.Second)},
		}})
		assert.False(t, svc.operationDriveStalled(t.Context(), 1, op, apply, stalledStart))
	})

	t.Run("a drive silent for the full stall window is stalled", func(t *testing.T) {
		svc := newStallService(&stubTaskStore{tasks: []*storage.Task{
			{State: state.Task.Running, UpdatedAt: stalledStart},
		}})
		assert.True(t, svc.operationDriveStalled(t.Context(), 1, op, apply, stalledStart))
	})

	t.Run("a drive that never mirrored a task is judged from drive start", func(t *testing.T) {
		svc := newStallService(&stubTaskStore{})
		assert.True(t, svc.operationDriveStalled(t.Context(), 1, op, apply, stalledStart))
	})

	t.Run("group finalizers are exempt", func(t *testing.T) {
		finalizer := &storage.ApplyOperation{
			ID:            8,
			ApplyID:       41,
			Deployment:    "default",
			OperationKind: storage.ApplyOperationKindGroupFinalizer,
			State:         state.Apply.Running,
		}
		svc := newStallService(&stubTaskStore{})
		assert.False(t, svc.operationDriveStalled(t.Context(), 1, finalizer, apply, stalledStart))
	})

	t.Run("a liveness read failure keeps the drive going", func(t *testing.T) {
		svc := newStallService(&erroringTaskStore{err: errors.New("connection refused")})
		assert.False(t, svc.operationDriveStalled(t.Context(), 1, op, apply, stalledStart))
	})
}
