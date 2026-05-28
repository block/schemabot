package tern

import (
	"context"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
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

func (s *exactProgressTaskStore) Update(context.Context, *storage.Task) error {
	return s.err
}

type exactProgressStorage struct {
	storage.Storage
	applies         storage.ApplyStore
	tasks           storage.TaskStore
	logs            storage.ApplyLogStore
	controlRequests storage.ControlRequestStore
}

func (s *exactProgressStorage) Applies() storage.ApplyStore { return s.applies }
func (s *exactProgressStorage) Tasks() storage.TaskStore    { return s.tasks }
func (s *exactProgressStorage) ApplyLogs() storage.ApplyLogStore {
	if s.logs != nil {
		return s.logs
	}
	return &mockApplyLogStore{}
}
func (s *exactProgressStorage) ControlRequests() storage.ControlRequestStore {
	return s.controlRequests
}

type testControlRequestStore struct {
	storage.ControlRequestStore
	requests []*storage.ApplyControlRequest
}

func (s *testControlRequestStore) RequestPending(_ context.Context, req *storage.ApplyControlRequest) (*storage.ApplyControlRequest, bool, error) {
	for _, existing := range s.requests {
		if existing.ApplyID == req.ApplyID && existing.Operation == req.Operation {
			if existing.Status == storage.ControlRequestPending {
				return cloneTestControlRequest(existing), true, nil
			}
			existing.Status = storage.ControlRequestPending
			existing.RequestedBy = req.RequestedBy
			existing.ErrorMessage = ""
			existing.Metadata = append(existing.Metadata[:0], req.Metadata...)
			existing.CompletedAt = nil
			return cloneTestControlRequest(existing), false, nil
		}
	}
	stored := cloneTestControlRequest(req)
	stored.ID = int64(len(s.requests) + 1)
	s.requests = append(s.requests, stored)
	return cloneTestControlRequest(stored), false, nil
}

func (s *testControlRequestStore) GetPending(_ context.Context, applyID int64, operation storage.ControlOperation) (*storage.ApplyControlRequest, error) {
	for i := len(s.requests) - 1; i >= 0; i-- {
		req := s.requests[i]
		if req.ApplyID == applyID && req.Operation == operation && req.Status == storage.ControlRequestPending {
			return cloneTestControlRequest(req), nil
		}
	}
	return nil, nil
}

func (s *testControlRequestStore) CompletePending(_ context.Context, applyID int64, operation storage.ControlOperation) error {
	for _, req := range s.requests {
		if req.ApplyID == applyID && req.Operation == operation && req.Status == storage.ControlRequestPending {
			req.Status = storage.ControlRequestCompleted
		}
	}
	return nil
}

func cloneTestControlRequest(req *storage.ApplyControlRequest) *storage.ApplyControlRequest {
	if req == nil {
		return nil
	}
	clone := *req
	if req.Metadata != nil {
		clone.Metadata = append([]byte(nil), req.Metadata...)
	}
	return &clone
}

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

func TestPrepareStoppedTasksForResumeQueuesOnlyStoppedTasks(t *testing.T) {
	completedAt := time.Date(2026, 5, 26, 20, 0, 0, 0, time.UTC)
	for _, applyState := range []string{state.Apply.Pending, state.Apply.Running} {
		t.Run(applyState, func(t *testing.T) {
			stoppedTask := &storage.Task{
				TaskIdentifier: "task-stopped",
				State:          state.Task.Stopped,
				CompletedAt:    &completedAt,
			}
			completedTask := &storage.Task{
				TaskIdentifier: "task-completed",
				State:          state.Task.Completed,
				CompletedAt:    &completedAt,
			}
			client := &LocalClient{
				storage: &exactProgressStorage{
					tasks: &exactProgressTaskStore{tasks: []*storage.Task{stoppedTask, completedTask}},
					controlRequests: &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
						ApplyID:   123,
						Operation: storage.ControlOperationStart,
						Status:    storage.ControlRequestPending,
					}}},
				},
				logger: slog.Default(),
			}
			apply := &storage.Apply{ID: 123, State: applyState}

			controlReq, err := pendingStartControlRequest(t.Context(), client.storage, apply)
			require.NoError(t, err)
			client.prepareStoppedTasksForResume(t.Context(), apply, []*storage.Task{stoppedTask, completedTask}, controlReq != nil)

			assert.Equal(t, state.Task.Pending, stoppedTask.State)
			assert.Nil(t, stoppedTask.CompletedAt)
			assert.Equal(t, state.Task.Completed, completedTask.State)
			assert.Equal(t, &completedAt, completedTask.CompletedAt)
		})
	}
}

func TestPrepareStoppedTasksForResumeIgnoresApplyWithoutStartRequest(t *testing.T) {
	completedAt := time.Date(2026, 5, 26, 20, 0, 0, 0, time.UTC)
	task := &storage.Task{
		TaskIdentifier: "task-stopped",
		State:          state.Task.Stopped,
		CompletedAt:    &completedAt,
	}
	client := &LocalClient{
		storage: &exactProgressStorage{
			tasks:           &exactProgressTaskStore{tasks: []*storage.Task{task}},
			controlRequests: &testControlRequestStore{},
		},
		logger: slog.Default(),
	}

	client.prepareStoppedTasksForResume(t.Context(), &storage.Apply{ID: 123, State: state.Apply.Running}, []*storage.Task{task}, false)

	assert.Equal(t, state.Task.Stopped, task.State)
	assert.Equal(t, &completedAt, task.CompletedAt)
}

func TestSyncStoredTaskProgressFromEngineTablePersistsRows(t *testing.T) {
	now := time.Date(2026, 5, 25, 17, 45, 0, 0, time.UTC)
	startedAt := now.Add(-time.Minute)
	task := &storage.Task{
		TaskIdentifier: "task-progress",
		State:          state.Task.Running,
	}

	changed := syncStoredTaskProgressFromEngineTable(task, &engine.TableProgress{
		Table:      "users",
		State:      state.Task.Running,
		Progress:   42,
		RowsCopied: 420,
		RowsTotal:  1000,
		ETASeconds: 12,
		StartedAt:  &startedAt,
	}, now)

	require.True(t, changed)
	assert.Equal(t, int64(420), task.RowsCopied)
	assert.Equal(t, int64(1000), task.RowsTotal)
	assert.Equal(t, 42, task.ProgressPercent)
	assert.Equal(t, 12, task.ETASeconds)
	assert.Equal(t, &startedAt, task.StartedAt)
	assert.Equal(t, now, task.UpdatedAt)
}

func TestSyncStoredTaskProgressFromEngineTablePreservesRowsWhenEngineOmitsTotals(t *testing.T) {
	now := time.Date(2026, 5, 25, 17, 45, 0, 0, time.UTC)
	task := &storage.Task{
		TaskIdentifier:  "task-progress",
		State:           state.Task.Running,
		RowsCopied:      950,
		RowsTotal:       1000,
		ProgressPercent: 95,
		ETASeconds:      3,
	}

	changed := syncStoredTaskProgressFromEngineTable(task, &engine.TableProgress{
		Table:    "users",
		State:    state.Task.Running,
		Progress: 0,
	}, now)

	require.False(t, changed)
	assert.Equal(t, int64(950), task.RowsCopied)
	assert.Equal(t, int64(1000), task.RowsTotal)
	assert.Equal(t, 95, task.ProgressPercent)
	assert.Equal(t, 3, task.ETASeconds)
	assert.True(t, task.UpdatedAt.IsZero())
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
