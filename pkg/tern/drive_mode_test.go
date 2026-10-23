package tern

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

type driveModeTaskStore struct {
	storage.TaskStore
	tasks map[int64][]*storage.Task
	err   error
}

func (s *driveModeTaskStore) GetByApplyOperationID(_ context.Context, applyOperationID int64) ([]*storage.Task, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.tasks[applyOperationID], nil
}

// The operator drives a claimed operation by DecideApplyDriveMode, and the stop
// path reads the same decision to tell whether a stop supersedes a pending
// start. Every mode resolves here, so the two can only change together.
func TestDecideApplyDriveMode(t *testing.T) {
	eu := &storage.ApplyOperation{ID: 1, OperationKey: "eu", Deployment: "eu"}
	us := &storage.ApplyOperation{ID: 2, OperationKey: "us", Deployment: "us"}
	withTasks := &driveModeTaskStore{tasks: map[int64][]*storage.Task{1: {{ID: 10}}, 2: {{ID: 20}}}}
	taskless := &driveModeTaskStore{tasks: map[int64][]*storage.Task{}}

	tests := []struct {
		name           string
		apply          *storage.Apply
		ops            []*storage.ApplyOperation
		tasks          *driveModeTaskStore
		want           ApplyDriveMode
		operationLease bool
	}{
		{name: "no operation attached", apply: &storage.Apply{}, tasks: withTasks, want: ApplyDriveUnattached, operationLease: false},
		{name: "lone operation with tasks", apply: &storage.Apply{}, ops: []*storage.ApplyOperation{eu}, tasks: withTasks, want: ApplyDriveParentLease, operationLease: false},
		{name: "several operations", apply: &storage.Apply{}, ops: []*storage.ApplyOperation{eu, us}, tasks: withTasks, want: ApplyDriveOperationSet, operationLease: true},
		{name: "lone operation the manifest expects siblings of", apply: &storage.Apply{ExpectedOperationKeys: []string{"eu", "us"}}, ops: []*storage.ApplyOperation{eu}, tasks: withTasks, want: ApplyDriveAwaitingSiblings, operationLease: true},
		{name: "lone operation the manifest expects alone", apply: &storage.Apply{ExpectedOperationKeys: []string{"eu"}}, ops: []*storage.ApplyOperation{eu}, tasks: withTasks, want: ApplyDriveParentLease, operationLease: false},
		{name: "lone task-less operation", apply: &storage.Apply{}, ops: []*storage.ApplyOperation{eu}, tasks: taskless, want: ApplyDriveTasklessOperation, operationLease: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &controlTestStorage{tasks: tt.tasks}
			mode, err := DecideApplyDriveMode(t.Context(), store, tt.apply, tt.ops)
			require.NoError(t, err)
			assert.Equal(t, tt.want, mode)
			assert.Equal(t, tt.operationLease, mode.UnderOperationLease())

			underLease, err := ApplyDrivesUnderOperationLease(t.Context(), store, tt.apply, tt.ops)
			require.NoError(t, err)
			assert.Equal(t, tt.operationLease, underLease, "the stop path reads the operator's decision")
		})
	}

	t.Run("a task lookup failure is an error, not a drive mode", func(t *testing.T) {
		store := &controlTestStorage{tasks: &driveModeTaskStore{err: errors.New("connection reset")}}
		_, err := DecideApplyDriveMode(t.Context(), store, &storage.Apply{}, []*storage.ApplyOperation{eu})
		require.ErrorContains(t, err, "connection reset")
		assert.ErrorContains(t, err, `apply_operation 1 (deployment "eu")`)
	})
}
