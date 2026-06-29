package api

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// releaseTestControlStore is a minimal ControlRequestStore for release handler
// tests: GetPending returns the configured pending stop/cancel request,
// RequestPending records new release requests (or reports an existing release
// latch as already pending), and GetByOperation is unused here.
type releaseTestControlStore struct {
	storage.ControlRequestStore
	pendingStop     *storage.ApplyControlRequest
	pendingCancel   *storage.ApplyControlRequest
	existingRelease *storage.ApplyControlRequest
	requested       []*storage.ApplyControlRequest
}

func (s *releaseTestControlStore) GetPending(_ context.Context, _ int64, op storage.ControlOperation) (*storage.ApplyControlRequest, error) {
	switch op {
	case storage.ControlOperationStop:
		return s.pendingStop, nil
	case storage.ControlOperationCancel:
		return s.pendingCancel, nil
	}
	return nil, nil
}

func (s *releaseTestControlStore) RequestPending(_ context.Context, req *storage.ApplyControlRequest) (*storage.ApplyControlRequest, bool, error) {
	if s.existingRelease != nil {
		return s.existingRelease, true, nil
	}
	s.requested = append(s.requested, req)
	return req, false, nil
}

type releaseTestStorage struct {
	mockStorage
	applyOps storage.ApplyOperationStore
	control  storage.ControlRequestStore
}

func (m *releaseTestStorage) ApplyOperations() storage.ApplyOperationStore { return m.applyOps }
func (m *releaseTestStorage) ControlRequests() storage.ControlRequestStore { return m.control }
func (m *releaseTestStorage) ApplyLogs() storage.ApplyLogStore             { return &noopApplyLogStore{} }

func newReleaseTestService(ops []*storage.ApplyOperation, control storage.ControlRequestStore) *Service {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&releaseTestStorage{
		applyOps: &listingApplyOperationStore{ops: ops},
		control:  control,
	}, testServerConfig(), nil, logger)
}

func pauseRolloutOps() []*storage.ApplyOperation {
	return []*storage.ApplyOperation{
		{ID: 1, State: state.ApplyOperation.Failed, OnFailure: storage.OnFailurePause},
		{ID: 2, State: state.ApplyOperation.Pending, OnFailure: storage.OnFailurePause},
	}
}

// TestExecuteReleaseForApply verifies the release handler records a release latch
// only for a rollout currently paused after an on_failure=pause failure, and
// rejects every other state with an operator-actionable conflict.
func TestExecuteReleaseForApply(t *testing.T) {
	pausedApply := func() *storage.Apply {
		return &storage.Apply{ID: 3, ApplyIdentifier: "apply-pause", State: state.Apply.Paused, Environment: "staging"}
	}

	t.Run("records the release latch for a paused rollout", func(t *testing.T) {
		control := &releaseTestControlStore{}
		svc := newReleaseTestService(pauseRolloutOps(), control)

		resp, httpStatus, err := svc.executeReleaseForApply(t.Context(), pausedApply(), "alice")
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
		assert.Empty(t, resp.Status)
		assert.Equal(t, http.StatusOK, httpStatus)
		require.Len(t, control.requested, 1, "a paused rollout records exactly one release request")
		assert.Equal(t, storage.ControlOperationRelease, control.requested[0].Operation)
		assert.Equal(t, "alice", control.requested[0].RequestedBy)
	})

	t.Run("reports already_requested when a release is already latched", func(t *testing.T) {
		control := &releaseTestControlStore{existingRelease: &storage.ApplyControlRequest{
			Operation: storage.ControlOperationRelease, Status: storage.ControlRequestPending,
		}}
		svc := newReleaseTestService(pauseRolloutOps(), control)

		resp, httpStatus, err := svc.executeReleaseForApply(t.Context(), pausedApply(), "alice")
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
		assert.Equal(t, releaseResponseStatusAlreadyRequested, resp.Status)
		assert.Equal(t, http.StatusAccepted, httpStatus)
		assert.Empty(t, control.requested, "an already-latched release records no new request")
	})

	t.Run("rejects an apply that is not paused", func(t *testing.T) {
		control := &releaseTestControlStore{}
		svc := newReleaseTestService(pauseRolloutOps(), control)
		apply := pausedApply()
		apply.State = state.Apply.Running

		_, _, err := svc.executeReleaseForApply(t.Context(), apply, "alice")
		require.Error(t, err)
		assert.Equal(t, http.StatusConflict, ControlOperationHTTPStatus(err))
		assert.Empty(t, control.requested)
	})

	t.Run("rejects an apply with no pause deployment", func(t *testing.T) {
		control := &releaseTestControlStore{}
		ops := []*storage.ApplyOperation{
			{ID: 1, State: state.ApplyOperation.Failed, OnFailure: storage.OnFailureHalt},
		}
		svc := newReleaseTestService(ops, control)

		_, _, err := svc.executeReleaseForApply(t.Context(), pausedApply(), "alice")
		require.Error(t, err)
		assert.Equal(t, http.StatusConflict, ControlOperationHTTPStatus(err))
		assert.Empty(t, control.requested)
	})

	t.Run("rejects a terminal apply", func(t *testing.T) {
		control := &releaseTestControlStore{}
		svc := newReleaseTestService(pauseRolloutOps(), control)
		apply := pausedApply()
		apply.State = state.Apply.Failed

		_, _, err := svc.executeReleaseForApply(t.Context(), apply, "alice")
		require.Error(t, err)
		assert.Equal(t, http.StatusConflict, ControlOperationHTTPStatus(err))
		assert.Empty(t, control.requested)
	})

	t.Run("rejects when a stop is pending", func(t *testing.T) {
		control := &releaseTestControlStore{pendingStop: &storage.ApplyControlRequest{
			Operation: storage.ControlOperationStop, Status: storage.ControlRequestPending,
		}}
		svc := newReleaseTestService(pauseRolloutOps(), control)

		_, _, err := svc.executeReleaseForApply(t.Context(), pausedApply(), "alice")
		require.Error(t, err)
		assert.Equal(t, http.StatusConflict, ControlOperationHTTPStatus(err))
		assert.Empty(t, control.requested, "a pending stop blocks the release latch")
	})
}
