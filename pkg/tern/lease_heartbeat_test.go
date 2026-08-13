package tern

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func leaseHeartbeatTestApply() *storage.Apply {
	return &storage.Apply{
		ID:              41,
		ApplyIdentifier: "apply-lease-heartbeat",
		Database:        "widgets",
		Deployment:      "default",
		Environment:     "staging",
		State:           state.Apply.Running,
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// failingHeartbeatApplyStore is an ApplyStore whose Heartbeat always returns
// the configured error, counting calls so tests can observe retry behavior.
type failingHeartbeatApplyStore struct {
	storage.ApplyStore
	mu    sync.Mutex
	err   error
	calls int
}

func (s *failingHeartbeatApplyStore) Heartbeat(context.Context, int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return s.err
}

func (s *failingHeartbeatApplyStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// recordingHeartbeatOperationStore records the operation IDs heartbeated so a
// test can assert which row a drive kept alive.
type recordingHeartbeatOperationStore struct {
	storage.ApplyOperationStore
	mu           sync.Mutex
	heartbeatIDs []int64
}

func (s *recordingHeartbeatOperationStore) Heartbeat(_ context.Context, id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.heartbeatIDs = append(s.heartbeatIDs, id)
	return nil
}

func (s *recordingHeartbeatOperationStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.heartbeatIDs)
}

func (s *recordingHeartbeatOperationStore) lastID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.heartbeatIDs) == 0 {
		return 0
	}
	return s.heartbeatIDs[len(s.heartbeatIDs)-1]
}

// TestStartApplyHeartbeatFollowsTheDriveScope covers which row a local drive
// keeps alive. An operation-scoped drive holds only its operation lease — the
// parent applies row belongs to the parent lease and the rollout projection —
// so it must heartbeat its operation row and leave the parent untouched. Any
// other drive carries the parent apply lease and heartbeats the parent. Sending
// the heartbeat to the wrong row is not merely misattributed: storage refuses
// the write, and the refusal reads as a lost lease that cancels the drive.
func TestStartApplyHeartbeatFollowsTheDriveScope(t *testing.T) {
	apply := leaseHeartbeatTestApply()

	t.Run("an operation-scoped drive heartbeats its operation row", func(t *testing.T) {
		applies := &failingHeartbeatApplyStore{err: storage.ErrApplyLeaseLost}
		operations := &recordingHeartbeatOperationStore{}
		client := &LocalClient{
			storage:           &exactProgressStorage{applies: applies, applyOperations: operations},
			logger:            discardLogger(),
			heartbeatInterval: 5 * time.Millisecond,
		}

		driveCtx, cancelDrive := context.WithCancel(t.Context())
		defer cancelDrive()
		opLeaseCtx := storage.WithOperationLease(driveCtx, storage.OperationLease{
			ApplyID: apply.ID, OperationID: 77, Owner: "driver-1", Token: "op-token",
		})
		stop := client.startApplyHeartbeat(opLeaseCtx, apply, cancelDrive)
		defer stop()

		require.Eventually(t, func() bool {
			return operations.callCount() >= 2
		}, time.Second, 10*time.Millisecond, "the drive must heartbeat its operation row")
		assert.Equal(t, int64(77), operations.lastID(), "the claimed operation row is the one kept alive")
		assert.Zero(t, applies.callCount(), "the parent applies row must not be heartbeated by an operation-scoped drive")
		assert.NoError(t, driveCtx.Err(), "heartbeating the right row must not cancel the drive")
	})

	t.Run("a drive holding both leases heartbeats the parent apply", func(t *testing.T) {
		applies := &failingHeartbeatApplyStore{}
		operations := &recordingHeartbeatOperationStore{}
		client := &LocalClient{
			storage:           &exactProgressStorage{applies: applies, applyOperations: operations},
			logger:            discardLogger(),
			heartbeatInterval: 5 * time.Millisecond,
		}

		dualLeaseCtx := storage.WithOperationLease(
			storage.WithApplyLease(t.Context(), storage.ApplyLease{ApplyID: apply.ID, Owner: "driver-1", Token: "apply-token"}),
			storage.OperationLease{ApplyID: apply.ID, OperationID: 77, Owner: "driver-1", Token: "op-token"},
		)
		stop := client.startApplyHeartbeat(dualLeaseCtx, apply)
		defer stop()

		require.Eventually(t, func() bool {
			return applies.callCount() >= 2
		}, time.Second, 10*time.Millisecond, "a drive that still holds the parent lease heartbeats the parent")
		assert.Zero(t, operations.callCount(), "the single-operation drive's liveness is reported on the parent row")
	})
}

// TestApplyHeartbeatFailureStopsDrive verifies how a local drive reacts to a
// failed apply heartbeat write: a definitively lost lease stops the drive
// immediately, a transient storage error inside the lease staleness window
// keeps the drive running so the heartbeat is retried on the next tick, and
// failures that have persisted for the full window stop the drive because a
// peer operator can already have reclaimed the stale row.
func TestApplyHeartbeatFailureStopsDrive(t *testing.T) {
	client := &LocalClient{logger: discardLogger()}
	apply := leaseHeartbeatTestApply()

	t.Run("lost lease stops the drive immediately", func(t *testing.T) {
		hbErr := fmt.Errorf("heartbeat apply %d: %w", apply.ID, storage.ErrApplyLeaseLost)
		assert.True(t, client.applyHeartbeatFailureStopsDrive(t.Context(), apply, hbErr, time.Now(), false))
	})

	t.Run("transient failure inside the window keeps driving", func(t *testing.T) {
		hbErr := errors.New("connection refused")
		assert.False(t, client.applyHeartbeatFailureStopsDrive(t.Context(), apply, hbErr, time.Now(), false))
	})

	t.Run("failures spanning the staleness window stop the drive", func(t *testing.T) {
		hbErr := errors.New("connection refused")
		lastSuccess := time.Now().Add(-storage.ApplyLeaseStaleAfter)
		assert.True(t, client.applyHeartbeatFailureStopsDrive(t.Context(), apply, hbErr, lastSuccess, false))
	})

	t.Run("an operation-scoped drive classifies its own lease the same way", func(t *testing.T) {
		hbErr := fmt.Errorf("apply_operation %d is no longer owned by its operation lease: %w", 9, storage.ErrApplyLeaseLost)
		assert.True(t, client.applyHeartbeatFailureStopsDrive(t.Context(), apply, hbErr, time.Now(), true))
	})
}

// TestDriveEndingHeartbeatFailure verifies how a gRPC drive reacts to a failed
// lease heartbeat write: a definitively lost lease ends the drive immediately,
// a transient storage error inside the lease staleness window keeps the drive
// polling so the heartbeat is retried on the next tick, and failures that have
// persisted for the full window end the drive because a peer driver can
// already have reclaimed the stale row.
func TestDriveEndingHeartbeatFailure(t *testing.T) {
	apply := leaseHeartbeatTestApply()

	t.Run("lost lease ends the drive immediately", func(t *testing.T) {
		hbErr := fmt.Errorf("heartbeat apply %d: %w", apply.ID, storage.ErrApplyLeaseLost)
		stopErr := driveEndingHeartbeatFailure(slog.Default(), apply, hbErr, time.Now())
		require.Error(t, stopErr)
		assert.ErrorIs(t, stopErr, storage.ErrApplyLeaseLost)
	})

	t.Run("transient failure inside the window keeps driving", func(t *testing.T) {
		hbErr := errors.New("connection refused")
		assert.NoError(t, driveEndingHeartbeatFailure(slog.Default(), apply, hbErr, time.Now()))
	})

	t.Run("failures spanning the staleness window end the drive", func(t *testing.T) {
		hbErr := errors.New("connection refused")
		lastSuccess := time.Now().Add(-storage.ApplyLeaseStaleAfter)
		stopErr := driveEndingHeartbeatFailure(slog.Default(), apply, hbErr, lastSuccess)
		require.Error(t, stopErr)
		assert.ErrorIs(t, stopErr, hbErr)
		assert.ErrorIs(t, stopErr, ErrApplyLeasePresumedLost,
			"the presumed-lost sentinel must survive wrapping so the operator can classify the displacement")
	})
}

// TestStartApplyHeartbeatStopsDriveOnLostLease verifies the heartbeat
// goroutine's stop wiring: when storage reports the apply lease definitively
// lost, the heartbeat cancels the drive context so the displaced driver stops
// executing engine work and writing apply state.
func TestStartApplyHeartbeatStopsDriveOnLostLease(t *testing.T) {
	store := &failingHeartbeatApplyStore{err: storage.ErrApplyLeaseLost}
	client := &LocalClient{
		storage:           &exactProgressStorage{applies: store},
		logger:            discardLogger(),
		heartbeatInterval: 5 * time.Millisecond,
	}

	driveCtx, cancelDrive := context.WithCancel(t.Context())
	defer cancelDrive()
	stop := client.startApplyHeartbeat(t.Context(), leaseHeartbeatTestApply(), cancelDrive)
	defer stop()

	require.Eventually(t, func() bool {
		return driveCtx.Err() != nil
	}, time.Second, 10*time.Millisecond, "lost lease must cancel the drive context")
}

// TestStartApplyHeartbeatRetriesTransientFailures verifies that a heartbeat
// write failing with a transient storage error does not stop the drive: the
// heartbeat keeps ticking and retrying while the failures stay inside the
// lease staleness window.
func TestStartApplyHeartbeatRetriesTransientFailures(t *testing.T) {
	store := &failingHeartbeatApplyStore{err: errors.New("connection refused")}
	client := &LocalClient{
		storage:           &exactProgressStorage{applies: store},
		logger:            discardLogger(),
		heartbeatInterval: 5 * time.Millisecond,
	}

	driveCtx, cancelDrive := context.WithCancel(t.Context())
	defer cancelDrive()
	stop := client.startApplyHeartbeat(t.Context(), leaseHeartbeatTestApply(), cancelDrive)
	defer stop()

	require.Eventually(t, func() bool {
		return store.callCount() >= 3
	}, time.Second, 10*time.Millisecond, "heartbeat must keep retrying transient failures")
	assert.NoError(t, driveCtx.Err(), "transient heartbeat failures inside the staleness window must not stop the drive")
}
