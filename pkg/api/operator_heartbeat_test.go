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
// reclaim the work. Operations that carry no task rows by design — group
// finalizers and VSchema-only work — mirror nothing and are exempt, young
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
	stalledStart := time.Now().Add(-2 * storage.ApplyDriveStallAfter)

	t.Run("a drive younger than the stall window is never stalled", func(t *testing.T) {
		// The stale task row is a previous claim's leftover: a young drive gets
		// the full window before its first mirror write even when the rows it
		// inherited stopped advancing long ago.
		svc := newStallService(&stubTaskStore{tasks: []*storage.Task{
			{State: state.Task.Running, UpdatedAt: stalledStart},
		}})
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

	t.Run("a task-less work operation mirrors nothing by design and is exempt", func(t *testing.T) {
		svc := newStallService(&stubTaskStore{})
		assert.False(t, svc.operationDriveStalled(t.Context(), 1, op, apply, stalledStart))
	})

	t.Run("group finalizers are exempt", func(t *testing.T) {
		finalizer := &storage.ApplyOperation{
			ID:            8,
			ApplyID:       41,
			Deployment:    "default",
			OperationKind: storage.ApplyOperationKindGroupFinalizer,
			State:         state.Apply.Running,
		}
		// The stale task row would stall a work operation; only the operation
		// kind exempts the finalizer.
		svc := newStallService(&stubTaskStore{tasks: []*storage.Task{
			{State: state.Task.Running, UpdatedAt: stalledStart},
		}})
		assert.False(t, svc.operationDriveStalled(t.Context(), 1, finalizer, apply, stalledStart))
	})

	t.Run("a liveness read failure keeps the drive going", func(t *testing.T) {
		svc := newStallService(&erroringTaskStore{err: errors.New("connection refused")})
		assert.False(t, svc.operationDriveStalled(t.Context(), 1, op, apply, stalledStart))
	})
}

// resumePromotionOperationStore answers MarkResumedRunning with a fixed result
// and counts the calls.
type resumePromotionOperationStore struct {
	storage.ApplyOperationStore
	promoted bool
	err      error
	marks    int
}

func (s *resumePromotionOperationStore) MarkResumedRunning(context.Context, int64) (bool, error) {
	s.marks++
	return s.promoted, s.err
}

// parentReloadStore fails every parent reload and counts the attempts.
type parentReloadStore struct {
	storage.ApplyStore
	gets int
}

func (s *parentReloadStore) Get(context.Context, int64) (*storage.Apply, error) {
	s.gets++
	return nil, errors.New("connection refused")
}

type resumePromotionStorage struct {
	mockStorage
	tasks    storage.TaskStore
	applyOps storage.ApplyOperationStore
	applies  storage.ApplyStore
}

func (m *resumePromotionStorage) Tasks() storage.TaskStore                     { return m.tasks }
func (m *resumePromotionStorage) ApplyOperations() storage.ApplyOperationStore { return m.applyOps }
func (m *resumePromotionStorage) Applies() storage.ApplyStore                  { return m.applies }

// TestAdvanceResumePromotion verifies what the operation heartbeat does for an
// operation a start resumed. It waits while no task is in flight. A lost lease
// stops the drive like any other displaced driver. A drive that holds the
// parent apply lease marks the row running but leaves the parent to its
// engine. A parent projection that fails is retried on the next tick without
// marking the row again.
func TestAdvanceResumePromotion(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	op := &storage.ApplyOperation{
		ID:            7,
		ApplyID:       41,
		Deployment:    "default",
		OperationKind: storage.ApplyOperationKindWork,
		State:         state.ApplyOperation.Resuming,
	}
	apply := &storage.Apply{
		ID:              41,
		ApplyIdentifier: "apply-resume-promotion",
		Database:        "widgets",
		Deployment:      "default",
		Environment:     "staging",
		State:           state.Apply.Resuming,
	}
	inFlight := &stubTaskStore{tasks: []*storage.Task{{State: state.Task.Running}}}
	newPromotionService := func(tasks storage.TaskStore, ops *resumePromotionOperationStore, applies *parentReloadStore) *Service {
		return New(&resumePromotionStorage{tasks: tasks, applyOps: ops, applies: applies}, testServerConfig(), nil, logger)
	}

	t.Run("waits while no task is in flight", func(t *testing.T) {
		ops := &resumePromotionOperationStore{promoted: true}
		svc := newPromotionService(&stubTaskStore{tasks: []*storage.Task{{State: state.Task.Stopped}}}, ops, &parentReloadStore{})
		resume := newResumePromotion(op)
		assert.Equal(t, resumeContinue, svc.advanceResumePromotion(t.Context(), 1, op, apply, &resume))
		assert.Zero(t, ops.marks)
		assert.True(t, resume.awaitingRunning)
	})

	t.Run("a lost lease stops the drive", func(t *testing.T) {
		ops := &resumePromotionOperationStore{err: fmt.Errorf("mark resumed: %w", storage.ErrApplyLeaseLost)}
		applies := &parentReloadStore{}
		svc := newPromotionService(inFlight, ops, applies)
		resume := newResumePromotion(op)
		assert.Equal(t, resumeLeaseLost, svc.advanceResumePromotion(t.Context(), 1, op, apply, &resume))
		assert.Zero(t, applies.gets, "a displaced driver does not project the parent")
	})

	t.Run("under the apply lease the parent is left to the engine", func(t *testing.T) {
		ops := &resumePromotionOperationStore{promoted: true}
		applies := &parentReloadStore{}
		svc := newPromotionService(inFlight, ops, applies)
		resume := newResumePromotion(op)
		ctx := storage.WithApplyLease(t.Context(), storage.ApplyLease{ApplyID: apply.ID, Owner: "driver", Token: "token"})
		assert.Equal(t, resumeContinue, svc.advanceResumePromotion(ctx, 1, op, apply, &resume))
		assert.Equal(t, 1, ops.marks)
		assert.Zero(t, applies.gets)
		assert.Equal(t, resumePromotion{}, resume, "nothing is left to do")
	})

	t.Run("a failed projection is retried without marking the row again", func(t *testing.T) {
		ops := &resumePromotionOperationStore{promoted: true}
		applies := &parentReloadStore{}
		svc := newPromotionService(inFlight, ops, applies)
		resume := newResumePromotion(op)
		assert.Equal(t, resumeContinue, svc.advanceResumePromotion(t.Context(), 1, op, apply, &resume))
		assert.Equal(t, resumeContinue, svc.advanceResumePromotion(t.Context(), 1, op, apply, &resume))
		assert.Equal(t, 1, ops.marks)
		assert.Equal(t, 2, applies.gets)
		assert.Equal(t, resumePromotion{parentStale: true}, resume)
	})
}
