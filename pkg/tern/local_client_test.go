package tern

import (
	"context"
	"log/slog"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

type exactProgressApplyStore struct {
	storage.ApplyStore
	apply *storage.Apply
	err   error
}

func (s *exactProgressApplyStore) GetByApplyIdentifier(context.Context, string) (*storage.Apply, error) {
	return s.apply, s.err
}

type exactProgressTaskStore struct {
	storage.TaskStore
	tasks []*storage.Task
	err   error
}

func (s *exactProgressTaskStore) GetByApplyID(context.Context, int64) ([]*storage.Task, error) {
	return s.tasks, s.err
}

type exactProgressStorage struct {
	storage.Storage
	applies storage.ApplyStore
	tasks   storage.TaskStore
}

func (s *exactProgressStorage) Applies() storage.ApplyStore { return s.applies }
func (s *exactProgressStorage) Tasks() storage.TaskStore    { return s.tasks }

func TestLocalClient_Apply_RequiresEnvironmentField(t *testing.T) {
	client, err := NewLocalClient(LocalConfig{
		Database: "testdb",
		Type:     storage.DatabaseTypeMySQL,
	}, nil, slog.Default())
	require.NoError(t, err)

	_, err = client.Apply(t.Context(), &ternv1.ApplyRequest{
		PlanId:  "plan-123",
		Options: map[string]string{"environment": "development"},
	})
	require.ErrorContains(t, err, "environment is required")
}

func TestLocalClient_ProgressByApplyIDReturnsNotFoundForMissingApplyData(t *testing.T) {
	testCases := []struct {
		name      string
		apply     *storage.Apply
		tasks     []*storage.Task
		wantError error
	}{
		{
			name:      "missing apply",
			wantError: storage.ErrApplyNotFound,
		},
		{
			name:      "missing tasks",
			apply:     &storage.Apply{ID: 42, ApplyIdentifier: "apply-missing-tasks"},
			wantError: storage.ErrTaskNotFound,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := &LocalClient{
				storage: &exactProgressStorage{
					applies: &exactProgressApplyStore{apply: tc.apply},
					tasks:   &exactProgressTaskStore{tasks: tc.tasks},
				},
				logger: slog.Default(),
			}

			_, err := client.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: "apply-missing"})
			require.ErrorIs(t, err, tc.wantError)
		})
	}
}

func TestTaskStateWithNoBackwardProgressPolicyCoversTaskStates(t *testing.T) {
	taskValue := reflect.ValueOf(state.Task)
	taskType := taskValue.Type()

	for i := range taskValue.NumField() {
		taskName := taskType.Field(i).Name
		taskState := taskValue.Field(i).String()
		_, hasProgressRank := activeTaskProgressRank(taskState)
		hasPolicy := state.IsTerminalTaskState(taskState) ||
			blocksActiveEngineProgress(taskState) ||
			hasProgressRank

		assert.Truef(t, hasPolicy,
			"task state %s=%q must be terminal, scheduler/control-owned, or ranked as active progress",
			taskName, taskState)
	}
}

func TestProgressTableStatusNormalizesEngineStateAndKeepsStoredStateAhead(t *testing.T) {
	tests := []struct {
		name             string
		storedTaskState  string
		engineTableState string
		expected         string
	}{
		{
			name:             "running engine state stays canonical",
			storedTaskState:  state.Task.Pending,
			engineTableState: "copyRows",
			expected:         state.Task.Running,
		},
		{
			name:             "unknown engine state defaults to running",
			storedTaskState:  state.Task.Pending,
			engineTableState: "something_unknown",
			expected:         state.Task.Running,
		},
		{
			name:             "terminal stored state wins over stale engine state",
			storedTaskState:  state.Task.Completed,
			engineTableState: state.Task.Running,
			expected:         state.Task.Completed,
		},
		{
			name:             "stored cutover wait does not move backward to running",
			storedTaskState:  state.Task.WaitingForCutover,
			engineTableState: state.Task.Running,
			expected:         state.Task.WaitingForCutover,
		},
		{
			name:             "stored running does not move backward to pending",
			storedTaskState:  state.Task.Running,
			engineTableState: "queued",
			expected:         state.Task.Running,
		},
		{
			name:             "terminal engine state can advance active stored state",
			storedTaskState:  state.Task.Running,
			engineTableState: "complete",
			expected:         state.Task.Completed,
		},
		{
			name:             "stopped engine state can advance active stored state",
			storedTaskState:  state.Task.Running,
			engineTableState: state.Task.Stopped,
			expected:         state.Task.Stopped,
		},
		{
			name:             "deploy wait can advance to running after deploy starts",
			storedTaskState:  state.Task.WaitingForDeploy,
			engineTableState: state.Task.Running,
			expected:         state.Task.Running,
		},
		{
			name:             "scheduler retryable state is preserved against active engine state",
			storedTaskState:  state.Task.FailedRetryable,
			engineTableState: state.Task.Running,
			expected:         state.Task.FailedRetryable,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, progressTableStatus(tc.storedTaskState, tc.engineTableState))
		})
	}
}
