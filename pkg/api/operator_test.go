package api

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func TestOperatorWorkersConfig(t *testing.T) {
	t.Run("default workers", func(t *testing.T) {
		config := &ServerConfig{}
		assert.Equal(t, 0, config.OperatorWorkers)
		assert.Equal(t, 4, DefaultOperatorWorkers)
	})

	t.Run("configured workers", func(t *testing.T) {
		config := &ServerConfig{OperatorWorkers: 3}
		assert.Equal(t, 3, config.OperatorWorkers)
	})

	t.Run("deprecated scheduler_workers folds into operator_workers", func(t *testing.T) {
		config := &ServerConfig{SchedulerWorkers: 2}
		require.NoError(t, config.resolveDeprecatedOperatorWorkers())
		assert.Equal(t, 2, config.OperatorWorkers)
		assert.Equal(t, 0, config.SchedulerWorkers)
	})

	t.Run("setting both keys is rejected", func(t *testing.T) {
		config := &ServerConfig{OperatorWorkers: 4, SchedulerWorkers: 2}
		assert.Error(t, config.resolveDeprecatedOperatorWorkers())
	})
}

// recordingApplyOperationStore records the state-mutating call made by
// markOperationFromApplyState. It embeds the interface so only the methods the
// test exercises need implementations; any other call panics, which keeps the
// test honest about the code path it covers.
type recordingApplyOperationStore struct {
	storage.ApplyOperationStore
	updateStateID    int64
	updateStateValue string
	updateStateErr   error
}

func (r *recordingApplyOperationStore) UpdateState(_ context.Context, id int64, newState string) error {
	r.updateStateID = id
	r.updateStateValue = newState
	return r.updateStateErr
}

type mockStorageWithApplyOperations struct {
	mockStorage
	applyOps storage.ApplyOperationStore
}

func (m *mockStorageWithApplyOperations) ApplyOperations() storage.ApplyOperationStore {
	return m.applyOps
}

func newOperatorTestService(opStore storage.ApplyOperationStore) *Service {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorageWithApplyOperations{applyOps: opStore}, testServerConfig(), nil, logger)
}

// TestMarkOperationFromApplyState_MirrorsFailedRetryable verifies that a parent
// apply in failed_retryable mirrors that state (not a terminal one) onto the
// operation row via UpdateState, leaving it reclaimable for retry. Returning
// marked=true lets the caller re-derive the parent state from its children.
func TestMarkOperationFromApplyState_MirrorsFailedRetryable(t *testing.T) {
	opStore := &recordingApplyOperationStore{}
	svc := newOperatorTestService(opStore)

	op := &storage.ApplyOperation{ID: 7, Deployment: "region-a"}
	apply := &storage.Apply{
		ID:              3,
		ApplyIdentifier: "apply-retryable",
		State:           state.Apply.FailedRetryable,
		Environment:     "staging",
	}

	marked, err := svc.markOperationFromApplyState(t.Context(), 1, op, apply)
	require.NoError(t, err)
	assert.True(t, marked, "failed_retryable parent must mark the operation so derived apply state is recomputed")
	assert.Equal(t, int64(7), opStore.updateStateID, "the claimed operation row must be the one updated")
	assert.Equal(t, state.Apply.FailedRetryable, opStore.updateStateValue,
		"failed_retryable must be mirrored down, not a terminal state")
}
