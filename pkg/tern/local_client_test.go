package tern

import (
	"context"
	"log/slog"
	"reflect"
	"slices"
	"testing"
	"time"

	spirittable "github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func TestPulledSchemaFileContentValidatesDDL(t *testing.T) {
	content, err := pulledSchemaFileContent("orders", spirittable.TableSchema{
		Name:   "users",
		Schema: "CREATE TABLE `users` (`id` bigint NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	})

	require.NoError(t, err)
	assert.Equal(t, "CREATE TABLE `users` (`id` bigint NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci\n", content)

	_, err = pulledSchemaFileContent("orders", spirittable.TableSchema{
		Name:   "broken_users",
		Schema: "CREATE TABLE",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse pulled schema for database orders table broken_users")
}

type exactProgressApplyStore struct {
	storage.ApplyStore
	apply *storage.Apply
	err   error
}

func (s *exactProgressApplyStore) Get(context.Context, int64) (*storage.Apply, error) {
	return s.apply, s.err
}

func (s *exactProgressApplyStore) GetByApplyIdentifier(context.Context, string) (*storage.Apply, error) {
	return s.apply, s.err
}

func (s *exactProgressApplyStore) Update(_ context.Context, apply *storage.Apply) error {
	if s.err != nil {
		return s.err
	}
	s.apply = apply
	return nil
}

type exactProgressTaskStore struct {
	storage.TaskStore
	tasks []*storage.Task
	err   error
}

func (s *exactProgressTaskStore) GetByApplyID(context.Context, int64) ([]*storage.Task, error) {
	return s.tasks, s.err
}

func (s *exactProgressTaskStore) Get(_ context.Context, taskIdentifier string) (*storage.Task, error) {
	if s.err != nil {
		return nil, s.err
	}
	for _, task := range s.tasks {
		if task.TaskIdentifier == taskIdentifier {
			return task, nil
		}
	}
	return nil, nil
}

func (s *exactProgressTaskStore) GetByDatabase(context.Context, string) ([]*storage.Task, error) {
	return s.tasks, s.err
}

func (s *exactProgressTaskStore) Update(context.Context, *storage.Task) error {
	return s.err
}

type fakeControlEngine struct {
	engine.Engine
	stopCount      int
	cutoverCount   int
	cutoverResult  *engine.ControlResult
	cutoverErr     error
	progressReq    *engine.ProgressRequest
	progressResult *engine.ProgressResult
	progressErr    error
}

func (e *fakeControlEngine) Name() string { return "fake" }

func (e *fakeControlEngine) Plan(context.Context, *engine.PlanRequest) (*engine.PlanResult, error) {
	return &engine.PlanResult{}, nil
}

func (e *fakeControlEngine) Apply(context.Context, *engine.ApplyRequest) (*engine.ApplyResult, error) {
	return &engine.ApplyResult{Accepted: true}, nil
}

func (e *fakeControlEngine) Progress(_ context.Context, req *engine.ProgressRequest) (*engine.ProgressResult, error) {
	e.progressReq = req
	if e.progressResult != nil || e.progressErr != nil {
		return e.progressResult, e.progressErr
	}
	return &engine.ProgressResult{State: engine.StateRunning}, nil
}

func (e *fakeControlEngine) Stop(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	e.stopCount++
	return &engine.ControlResult{Accepted: true}, nil
}

func (e *fakeControlEngine) Start(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	return &engine.ControlResult{Accepted: true}, nil
}

func (e *fakeControlEngine) Cutover(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	e.cutoverCount++
	if e.cutoverResult != nil || e.cutoverErr != nil {
		return e.cutoverResult, e.cutoverErr
	}
	return &engine.ControlResult{Accepted: true}, nil
}

func (e *fakeControlEngine) Revert(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	return &engine.ControlResult{Accepted: true}, nil
}

func (e *fakeControlEngine) SkipRevert(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	return &engine.ControlResult{Accepted: true}, nil
}

func (e *fakeControlEngine) Volume(context.Context, *engine.VolumeRequest) (*engine.VolumeResult, error) {
	return &engine.VolumeResult{Accepted: true}, nil
}

type exactProgressStorage struct {
	storage.Storage
	applies         storage.ApplyStore
	tasks           storage.TaskStore
	logs            storage.ApplyLogStore
	controlRequests storage.ControlRequestStore
	vitessApplyData storage.VitessApplyDataStore
	applyOperations storage.ApplyOperationStore
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
func (s *exactProgressStorage) VitessApplyData() storage.VitessApplyDataStore {
	return s.vitessApplyData
}
func (s *exactProgressStorage) ApplyOperations() storage.ApplyOperationStore {
	return s.applyOperations
}

type exactProgressVitessApplyDataStore struct {
	storage.VitessApplyDataStore
	data *storage.VitessApplyData
	err  error
}

func (s *exactProgressVitessApplyDataStore) GetByApplyID(context.Context, int64) (*storage.VitessApplyData, error) {
	return s.data, s.err
}

type exactProgressApplyOperationStore struct {
	storage.ApplyOperationStore
	data  *storage.EngineResumeState
	err   error
	saved *storage.EngineResumeState
}

func (s *exactProgressApplyOperationStore) SaveEngineResumeState(_ context.Context, operationID int64, resumeState *storage.EngineResumeState) error {
	if s.err != nil {
		return s.err
	}
	clone := *resumeState
	clone.ApplyOperationID = operationID
	s.saved = &clone
	s.data = &clone
	return nil
}

func (s *exactProgressApplyOperationStore) GetEngineResumeState(context.Context, int64) (*storage.EngineResumeState, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.data == nil {
		return nil, storage.ErrEngineResumeStateNotFound
	}
	return s.data, nil
}

func TestApplyCancelHandleDoesNotCancelNewerOwner(t *testing.T) {
	client := &LocalClient{}
	oldCtx, oldCancel := context.WithCancel(t.Context())
	oldGeneration := client.setApplyCancel(oldCancel)
	oldHandle := client.currentApplyCancel()

	newCtx, newCancel := context.WithCancel(t.Context())
	defer newCancel()
	newGeneration := client.setApplyCancel(newCancel)

	client.cancelApplyHandle(oldHandle)
	assert.Equal(t, oldGeneration, oldHandle.generation)
	require.Eventually(t, func() bool {
		return oldCtx.Err() != nil
	}, time.Second, 10*time.Millisecond)
	assert.NoError(t, newCtx.Err())

	client.clearApplyCancel(oldGeneration)
	currentHandle := client.currentApplyCancel()
	assert.Equal(t, newGeneration, currentHandle.generation)
	assert.NotNil(t, currentHandle.cancel)

	client.clearApplyCancel(newGeneration)
	assert.Nil(t, client.currentApplyCancel().cancel)
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
	for _, v := range slices.Backward(s.requests) {
		req := v
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

func (s *testControlRequestStore) FailPending(_ context.Context, applyID int64, operation storage.ControlOperation, errorMessage string) error {
	for _, req := range s.requests {
		if req.ApplyID == applyID && req.Operation == operation && req.Status == storage.ControlRequestPending {
			req.Status = storage.ControlRequestFailed
			req.ErrorMessage = errorMessage
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

func TestLocalClient_ProgressRequiresApplyID(t *testing.T) {
	client := &LocalClient{logger: slog.Default()}

	_, err := client.Progress(t.Context(), &ternv1.ProgressRequest{
		Environment: "staging",
	})
	require.ErrorContains(t, err, "apply_id is required")
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

			_, err := client.Progress(t.Context(), &ternv1.ProgressRequest{
				ApplyId:     "apply-missing",
				Environment: "staging",
			})
			require.ErrorIs(t, err, tc.wantError)
		})
	}
}

func TestLocalClient_VitessProgressRequiresEngineResumeState(t *testing.T) {
	operationID := int64(99)
	apply := &storage.Apply{ID: 42, ApplyIdentifier: "apply-vitess-progress", DatabaseType: storage.DatabaseTypeVitess, Engine: storage.EnginePlanetScale}
	task := &storage.Task{
		ID:               7,
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TaskIdentifier:   "task-vitess-progress",
		Database:         "testdb",
		DatabaseType:     storage.DatabaseTypeVitess,
		Engine:           storage.EnginePlanetScale,
		Namespace:        "commerce",
		TableName:        "users",
		State:            state.Task.Running,
	}
	eng := &fakeControlEngine{}
	client := &LocalClient{
		config: LocalConfig{
			Database: "testdb",
			Type:     storage.DatabaseTypeVitess,
		},
		storage: &exactProgressStorage{
			applies:         &exactProgressApplyStore{apply: apply},
			tasks:           &exactProgressTaskStore{tasks: []*storage.Task{task}},
			vitessApplyData: &exactProgressVitessApplyDataStore{},
			applyOperations: &exactProgressApplyOperationStore{},
		},
		planetscaleEngine: eng,
		logger:            slog.Default(),
	}

	_, err := client.Progress(t.Context(), &ternv1.ProgressRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: "staging",
	})

	require.ErrorIs(t, err, storage.ErrEngineResumeStateNotFound)
	assert.Nil(t, eng.progressReq, "progress should not call the engine without resume state")
}

func TestLocalClient_VitessProgressPassesAndPersistsEngineResumeState(t *testing.T) {
	operationID := int64(99)
	apply := &storage.Apply{ID: 42, ApplyIdentifier: "apply-vitess-progress", DatabaseType: storage.DatabaseTypeVitess, Engine: storage.EnginePlanetScale}
	task := &storage.Task{
		ID:               7,
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TaskIdentifier:   "task-vitess-progress",
		Database:         "testdb",
		DatabaseType:     storage.DatabaseTypeVitess,
		Engine:           storage.EnginePlanetScale,
		Namespace:        "commerce",
		TableName:        "users",
		State:            state.Task.Running,
		DDLAction:        "alter",
	}
	storedResumeState := &storage.EngineResumeState{
		ApplyOperationID: operationID,
		MigrationContext: "ctx-123",
		Metadata:         `{"branch_name":"branch-123","deploy_request_id":321,"deploy_request_url":"https://example.test/deploys/321"}`,
	}
	updatedMetadata := `{"branch_name":"branch-123","deploy_request_id":321,"deploy_request_url":"https://example.test/deploys/321","is_instant":true}`
	applyOperations := &exactProgressApplyOperationStore{data: storedResumeState}
	eng := &fakeControlEngine{progressResult: &engine.ProgressResult{
		State: engine.StateRunning,
		ResumeState: &engine.ResumeState{
			MigrationContext: "ctx-123",
			Metadata:         updatedMetadata,
		},
		Tables: []engine.TableProgress{{
			Namespace: "commerce",
			Table:     "users",
			State:     state.Task.Running,
			Progress:  42,
			IsInstant: true,
		}},
	}}
	client := &LocalClient{
		config: LocalConfig{
			Database: "testdb",
			Type:     storage.DatabaseTypeVitess,
		},
		storage: &exactProgressStorage{
			applies: &exactProgressApplyStore{apply: apply},
			tasks:   &exactProgressTaskStore{tasks: []*storage.Task{task}},
			vitessApplyData: &exactProgressVitessApplyDataStore{data: &storage.VitessApplyData{
				ApplyID:   apply.ID,
				IsInstant: true,
			}},
			applyOperations: applyOperations,
		},
		planetscaleEngine: eng,
		logger:            slog.Default(),
	}

	progress, err := client.Progress(t.Context(), &ternv1.ProgressRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: "staging",
	})

	require.NoError(t, err)
	require.NotNil(t, eng.progressReq)
	require.NotNil(t, eng.progressReq.ResumeState)
	assert.Equal(t, storedResumeState.MigrationContext, eng.progressReq.ResumeState.MigrationContext)
	assert.JSONEq(t, storedResumeState.Metadata, eng.progressReq.ResumeState.Metadata)
	require.NotNil(t, applyOperations.saved)
	assert.Equal(t, operationID, applyOperations.saved.ApplyOperationID)
	assert.JSONEq(t, updatedMetadata, applyOperations.saved.Metadata)
	require.Len(t, progress.Tables, 1)
	assert.Equal(t, int32(42), progress.Tables[0].PercentComplete)
	assert.True(t, progress.Tables[0].IsInstant)
}

func TestLocalClient_ProcessPendingStopControlRequest(t *testing.T) {
	apply := &storage.Apply{
		ID:              123,
		ApplyIdentifier: "apply-stop-local",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:             456,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-stop-local",
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		TableName:      "users",
		State:          state.Task.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	fakeEngine := &fakeControlEngine{}
	logs := &mockApplyLogStore{}
	client := &LocalClient{
		config: LocalConfig{
			Database: "testdb",
			Type:     storage.DatabaseTypeMySQL,
		},
		storage: &exactProgressStorage{
			applies:         &exactProgressApplyStore{apply: apply},
			tasks:           &exactProgressTaskStore{tasks: []*storage.Task{task}},
			logs:            logs,
			controlRequests: controlRequests,
		},
		spiritEngine: fakeEngine,
		logger:       slog.Default(),
	}

	handled, err := client.processPendingStopControlRequest(t.Context(), apply)
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, 1, fakeEngine.stopCount)
	assert.Equal(t, state.Task.Stopped, task.State)
	assert.Equal(t, state.Apply.Stopped, apply.State)
	controlReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	assert.Nil(t, controlReq)
	assert.True(t, hasLogMessageContaining(logs.logs, "Stop requested: 1 tasks stopped, 0 skipped (caller: cli:alice)"))
}

// A revert-window apply has already cut over, so a durable stop request against
// it is a permanent rejection. Processing it must resolve the request terminally
// (failed) with the operator-facing reason instead of bubbling a retryable error
// that keeps the request pending and spins the operator-owned retry loop forever.
// The apply stays in the revert window for the operator to revert or skip-revert.
func TestLocalClient_ProcessPendingStopControlRequestRejectsRevertWindow(t *testing.T) {
	apply := &storage.Apply{
		ID:              321,
		ApplyIdentifier: "apply-revert-window-stop",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeVitess,
		Environment:     "staging",
		State:           state.Apply.RevertWindow,
	}
	task := &storage.Task{
		ID:             654,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-revert-window-stop",
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeVitess,
		TableName:      "users",
		State:          state.Task.RevertWindow,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	fakeEngine := &fakeControlEngine{}
	logs := &mockApplyLogStore{}
	client := &LocalClient{
		config: LocalConfig{
			Database: "testdb",
			Type:     storage.DatabaseTypeVitess,
		},
		storage: &exactProgressStorage{
			applies:         &exactProgressApplyStore{apply: apply},
			tasks:           &exactProgressTaskStore{tasks: []*storage.Task{task}},
			logs:            logs,
			controlRequests: controlRequests,
		},
		planetscaleEngine: fakeEngine,
		logger:            slog.Default(),
	}

	handled, err := client.processPendingStopControlRequest(t.Context(), apply)

	require.NoError(t, err, "a permanent rejection must not bubble a retryable error")
	assert.True(t, handled, "the durable request is resolved, so the owner must not retry")
	assert.Equal(t, 0, fakeEngine.stopCount, "stop must not touch the engine for a revert-window apply")
	assert.Equal(t, state.Apply.RevertWindow, apply.State, "revert-window apply must not be recorded as cancelled or stopped")
	assert.Equal(t, state.Task.RevertWindow, task.State, "revert-window task must be preserved")

	pending, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	assert.Nil(t, pending, "the stop request must not be left pending")

	require.Len(t, controlRequests.requests, 1)
	resolved := controlRequests.requests[0]
	assert.Equal(t, storage.ControlRequestFailed, resolved.Status, "the stop request must be terminally failed, not retried")
	assert.Equal(t, "schema change apply-revert-window-stop is in the revert window and has already been applied: use revert to undo it or skip-revert to finalize it", resolved.ErrorMessage)
	assert.True(t, hasLogMessageContaining(logs.logs, "Pending stop request rejected: schema change apply-revert-window-stop is in the revert window"))
}

func TestLocalClient_ProcessPendingCutoverControlRequest(t *testing.T) {
	apply := &storage.Apply{
		ID:              125,
		ApplyIdentifier: "apply-cutover-local",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.WaitingForCutover,
	}
	task := &storage.Task{
		ID:             458,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-cutover-local",
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		TableName:      "users",
		State:          state.Task.WaitingForCutover,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCutover,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	fakeEngine := &fakeControlEngine{}
	logs := &mockApplyLogStore{}
	client := &LocalClient{
		config: LocalConfig{
			Database: "testdb",
			Type:     storage.DatabaseTypeMySQL,
		},
		storage: &exactProgressStorage{
			applies:         &exactProgressApplyStore{apply: apply},
			tasks:           &exactProgressTaskStore{tasks: []*storage.Task{task}},
			logs:            logs,
			controlRequests: controlRequests,
		},
		spiritEngine: fakeEngine,
		logger:       slog.Default(),
	}

	err := client.processPendingCutoverControlRequest(t.Context(), apply)
	require.NoError(t, err)
	assert.Equal(t, 1, fakeEngine.cutoverCount)
	controlReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCutover)
	require.NoError(t, err)
	assert.Nil(t, controlReq)
	assert.True(t, hasLogMessageContaining(logs.logs, "Cutover triggered (caller: cli:alice)"))
}

func TestLocalClient_ProcessPendingCutoverControlRequestUsesCompletedTaskForCutoverReadyApply(t *testing.T) {
	apply := &storage.Apply{
		ID:              127,
		ApplyIdentifier: "apply-cutover-completed-task-local",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.WaitingForCutover,
	}
	task := &storage.Task{
		ID:             460,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-cutover-completed-task-local",
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		TableName:      "users",
		State:          state.Task.Completed,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCutover,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	fakeEngine := &fakeControlEngine{}
	client := &LocalClient{
		config: LocalConfig{
			Database: "testdb",
			Type:     storage.DatabaseTypeMySQL,
		},
		storage: &exactProgressStorage{
			applies:         &exactProgressApplyStore{apply: apply},
			tasks:           &exactProgressTaskStore{tasks: []*storage.Task{task}},
			logs:            &mockApplyLogStore{},
			controlRequests: controlRequests,
		},
		spiritEngine: fakeEngine,
		logger:       slog.Default(),
	}

	err := client.processPendingCutoverControlRequest(t.Context(), apply)
	require.NoError(t, err)
	assert.Equal(t, 1, fakeEngine.cutoverCount)
	controlReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCutover)
	require.NoError(t, err)
	assert.Nil(t, controlReq)
}

func TestLocalClient_ProcessPendingCutoverControlRequestWaitsWhenNotReady(t *testing.T) {
	apply := &storage.Apply{
		ID:              128,
		ApplyIdentifier: "apply-cutover-wait-local",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:             461,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-cutover-wait-local",
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		TableName:      "users",
		State:          state.Task.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCutover,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	fakeEngine := &fakeControlEngine{}
	client := &LocalClient{
		config: LocalConfig{
			Database: "testdb",
			Type:     storage.DatabaseTypeMySQL,
		},
		storage: &exactProgressStorage{
			applies:         &exactProgressApplyStore{apply: apply},
			tasks:           &exactProgressTaskStore{tasks: []*storage.Task{task}},
			logs:            &mockApplyLogStore{},
			controlRequests: controlRequests,
		},
		spiritEngine: fakeEngine,
		logger:       slog.Default(),
	}

	err := client.processPendingCutoverControlRequest(t.Context(), apply)
	require.NoError(t, err)
	assert.Equal(t, 0, fakeEngine.cutoverCount)
	pending, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCutover)
	require.NoError(t, err)
	require.NotNil(t, pending)
	assert.Equal(t, storage.ControlRequestPending, pending.Status)
}

func TestLocalClient_ProcessPendingCutoverControlRequestWaitsWhileRecovering(t *testing.T) {
	apply := &storage.Apply{
		ID:              129,
		ApplyIdentifier: "apply-cutover-recovering-local",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Recovering,
	}
	task := &storage.Task{
		ID:             462,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-cutover-recovering-local",
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		TableName:      "users",
		State:          state.Task.Recovering,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCutover,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	fakeEngine := &fakeControlEngine{}
	client := &LocalClient{
		config: LocalConfig{
			Database: "testdb",
			Type:     storage.DatabaseTypeMySQL,
		},
		storage: &exactProgressStorage{
			applies:         &exactProgressApplyStore{apply: apply},
			tasks:           &exactProgressTaskStore{tasks: []*storage.Task{task}},
			logs:            &mockApplyLogStore{},
			controlRequests: controlRequests,
		},
		spiritEngine: fakeEngine,
		logger:       slog.Default(),
	}

	err := client.processPendingCutoverControlRequest(t.Context(), apply)
	require.NoError(t, err)
	assert.Equal(t, 0, fakeEngine.cutoverCount)
	pending, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCutover)
	require.NoError(t, err)
	require.NotNil(t, pending)
	assert.Equal(t, storage.ControlRequestPending, pending.Status)
}

func TestLocalClient_ProcessPendingCutoverControlRequestFailsRejectedRequest(t *testing.T) {
	apply := &storage.Apply{
		ID:              126,
		ApplyIdentifier: "apply-cutover-rejected-local",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.WaitingForCutover,
	}
	task := &storage.Task{
		ID:             459,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-cutover-rejected-local",
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		TableName:      "users",
		State:          state.Task.WaitingForCutover,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCutover,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	client := &LocalClient{
		config: LocalConfig{
			Database: "testdb",
			Type:     storage.DatabaseTypeMySQL,
		},
		storage: &exactProgressStorage{
			applies:         &exactProgressApplyStore{apply: apply},
			tasks:           &exactProgressTaskStore{tasks: []*storage.Task{task}},
			logs:            &mockApplyLogStore{},
			controlRequests: controlRequests,
		},
		spiritEngine: &fakeControlEngine{cutoverResult: &engine.ControlResult{Accepted: false, Message: "not ready for cutover"}},
		logger:       slog.Default(),
	}

	err := client.processPendingCutoverControlRequest(t.Context(), apply)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not ready for cutover")
	pending, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCutover)
	require.NoError(t, err)
	assert.Nil(t, pending)
	require.Len(t, controlRequests.requests, 1)
	assert.Equal(t, storage.ControlRequestFailed, controlRequests.requests[0].Status)
	assert.Equal(t, "not ready for cutover", controlRequests.requests[0].ErrorMessage)
}

func TestLocalClient_StopPreservesTerminalCancelledApply(t *testing.T) {
	apply := &storage.Apply{
		ID:              124,
		ApplyIdentifier: "apply-cancelled-local",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeVitess,
		Environment:     "staging",
		State:           state.Apply.Cancelled,
	}
	task := &storage.Task{
		ID:             457,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-cancelled-local",
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeVitess,
		TableName:      "users",
		State:          state.Task.Cancelled,
	}
	fakeEngine := &fakeControlEngine{}
	client := &LocalClient{
		config: LocalConfig{
			Database: "testdb",
			Type:     storage.DatabaseTypeVitess,
		},
		storage: &exactProgressStorage{
			applies: &exactProgressApplyStore{apply: apply},
			tasks:   &exactProgressTaskStore{tasks: []*storage.Task{task}},
		},
		planetscaleEngine: fakeEngine,
		logger:            slog.Default(),
	}

	resp, err := client.stop(t.Context(), &ternv1.StopRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: apply.Environment,
	}, "cli:second")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(0), resp.StoppedCount)
	assert.Equal(t, int64(1), resp.SkippedCount)
	assert.Equal(t, 0, fakeEngine.stopCount)
	assert.Equal(t, state.Apply.Cancelled, apply.State)
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
			isRecoveryState(taskState) ||
			hasProgressRank

		assert.Truef(t, hasPolicy,
			"task state %s=%q must be terminal, operator/control-owned, recovery-owned, or ranked as active progress",
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
			name:             "Vitess defer deploy can pause after deploy request validation",
			storedTaskState:  state.Task.Running,
			engineTableState: state.Task.WaitingForDeploy,
			expected:         state.Task.WaitingForDeploy,
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
			name:             "operator retryable state is preserved against active engine state",
			storedTaskState:  state.Task.FailedRetryable,
			engineTableState: state.Task.Running,
			expected:         state.Task.FailedRetryable,
		},
		{
			name:             "recovery preserves cutover-ready storage while engine reports row copy",
			storedTaskState:  state.Task.Recovering,
			engineTableState: state.Task.Running,
			expected:         state.Task.Recovering,
		},
		{
			name:             "recovery exits when engine proves cutover readiness",
			storedTaskState:  state.Task.Recovering,
			engineTableState: state.Task.WaitingForCutover,
			expected:         state.Task.WaitingForCutover,
		},
		{
			name:             "recovery ignores cutting over because cutover readiness was not re-established",
			storedTaskState:  state.Task.Recovering,
			engineTableState: state.Task.CuttingOver,
			expected:         state.Task.Recovering,
		},
		{
			name:             "recovery stays visible until engine proves cutover readiness",
			storedTaskState:  state.Task.Recovering,
			engineTableState: state.Task.Pending,
			expected:         state.Task.Recovering,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, progressTableStatus(tc.storedTaskState, tc.engineTableState))
		})
	}
}

func TestRecoveringProtoConversion(t *testing.T) {
	assert.Equal(t, ternv1.State_STATE_RECOVERING, storageStateToProto(state.Apply.Recovering))
	assert.Equal(t, ternv1.State_STATE_RECOVERING, storageStateToProto(state.Task.Recovering))
	assert.Equal(t, ternv1.State_STATE_RECOVERING, storageStateToProto("recovering_cutover"))
	assert.Equal(t, state.Apply.Recovering, ProtoStateToStorage(ternv1.State_STATE_RECOVERING))
}

// dsnLogAttrs describes a target DSN for logging using only the network address
// and database name. The DSN password — even one containing reserved characters
// like '/', '@', and ':' — must never appear in the emitted attributes, and the
// host and database must be reported so operators can trace the target.
func TestDSNLogAttrs(t *testing.T) {
	const password = "p:a/ss@w:rd/with@special"
	dsn := "appuser:" + password + "@tcp(db.example.internal:3306)/orders?parseTime=true"

	attrs := dsnLogAttrs(dsn)

	for _, a := range attrs {
		if s, ok := a.(string); ok {
			assert.NotContains(t, s, password, "log attribute must not contain the DSN password")
		}
	}

	values := attrValues(t, attrs)
	assert.Equal(t, "db.example.internal:3306", values["target_addr"])
	assert.Equal(t, "orders", values["target_db"])
	assert.NotContains(t, values, "target_dsn_prefix", "raw DSN prefix must not be logged")
}

// dsnLogAttrs records that parsing failed without echoing any part of an
// unparseable DSN, so a malformed credential-bearing string never reaches logs.
func TestDSNLogAttrsUnparseable(t *testing.T) {
	const password = "secretpw"
	// Missing the slash before the database name makes ParseDSN fail.
	dsn := "appuser:" + password + "@tcp(db.example.internal:3306)"

	attrs := dsnLogAttrs(dsn)

	for _, a := range attrs {
		if s, ok := a.(string); ok {
			assert.NotContains(t, s, password, "failed-parse log attribute must not contain the DSN password")
		}
	}

	values := attrValues(t, attrs)
	assert.Equal(t, false, values["target_dsn_parsed"])
	assert.NotContains(t, values, "target_addr")
}

// attrValues converts a flat slog key/value attribute slice into a map keyed by
// attribute name for assertions.
func attrValues(t *testing.T, attrs []any) map[string]any {
	t.Helper()
	require.Equal(t, 0, len(attrs)%2, "attributes must form key/value pairs")
	values := make(map[string]any, len(attrs)/2)
	for i := 0; i+1 < len(attrs); i += 2 {
		key, ok := attrs[i].(string)
		require.True(t, ok, "attribute key must be a string")
		values[key] = attrs[i+1]
	}
	return values
}

// listApplyOperationStore is a fake ApplyOperationStore that returns a fixed set
// of operation rows from ListByApply so the aggregate-state projection can be
// exercised without a database.
type listApplyOperationStore struct {
	storage.ApplyOperationStore
	ops []*storage.ApplyOperation
	err error
}

func (s *listApplyOperationStore) ListByApply(context.Context, int64) ([]*storage.ApplyOperation, error) {
	return s.ops, s.err
}

// deriveAggregateApplyState projects applies.state over all of an apply's
// operation rows. With one operation per apply the projection equals the current
// deployment's own derived state, so behaviour is unchanged; with a still-active
// sibling the apply stays non-terminal so one deployment's drive cannot clobber
// the rollout-level aggregate.
func TestDeriveAggregateApplyState(t *testing.T) {
	const currentOpID = int64(1)

	taskWith := func(taskState string) *storage.Task {
		id := currentOpID
		return &storage.Task{State: taskState, ApplyOperationID: &id}
	}

	t.Run("one operation per apply matches current deployment derivation", func(t *testing.T) {
		taskStateSets := [][]string{
			{state.Task.Completed},
			{state.Task.Completed, state.Task.Completed},
			{state.Task.Running, state.Task.Pending},
			{state.Task.Failed, state.Task.Completed},
			{state.Task.WaitingForCutover, state.Task.Completed},
			{state.Task.Pending},
		}
		for _, taskStateSet := range taskStateSets {
			tasks := make([]*storage.Task, len(taskStateSet))
			for i, ts := range taskStateSet {
				tasks[i] = taskWith(ts)
			}
			client := &LocalClient{
				storage: &exactProgressStorage{
					applyOperations: &listApplyOperationStore{
						ops: []*storage.ApplyOperation{
							{ID: currentOpID, State: state.ApplyOperation.Pending},
						},
					},
				},
				logger: slog.Default(),
			}
			apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-one-op"}

			got, ok := client.deriveAggregateApplyState(t.Context(), apply, tasks)
			want := state.DeriveApplyState(taskStates(tasks))
			assert.True(t, ok, "task states %v: current op row present, projection must be determined", taskStateSet)
			assert.Equal(t, want, got, "task states %v", taskStateSet)
		}
	})

	t.Run("pending sibling keeps the apply non-terminal", func(t *testing.T) {
		tasks := []*storage.Task{taskWith(state.Task.Completed)}
		client := &LocalClient{
			storage: &exactProgressStorage{
				applyOperations: &listApplyOperationStore{
					ops: []*storage.ApplyOperation{
						{ID: currentOpID, State: state.ApplyOperation.Running},
						{ID: 2, State: state.ApplyOperation.Pending},
					},
				},
			},
			logger: slog.Default(),
		}
		apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-multi-op"}

		got, ok := client.deriveAggregateApplyState(t.Context(), apply, tasks)
		assert.True(t, ok, "current op row present, projection must be determined")
		assert.False(t, state.IsTerminalApplyState(got), "derived state %q must be non-terminal", got)
		assert.False(t, state.IsState(got, state.Apply.Completed), "derived state must not clobber the pending sibling to completed")
	})

	// Under on_failure "continue" a terminally failed deployment does not
	// terminalize the apply while a sibling is still pending: the apply is held
	// running so the remaining deployment gets its turn, then settles to failed
	// once every sibling is terminal.
	t.Run("continue policy holds the apply active past a failed deployment", func(t *testing.T) {
		tasks := []*storage.Task{taskWith(state.Task.Failed)}
		client := &LocalClient{
			storage: &exactProgressStorage{
				applyOperations: &listApplyOperationStore{
					ops: []*storage.ApplyOperation{
						{ID: currentOpID, State: state.ApplyOperation.Failed, OnFailure: storage.OnFailureContinue},
						{ID: 2, State: state.ApplyOperation.Pending, OnFailure: storage.OnFailureContinue},
					},
				},
			},
			logger: slog.Default(),
		}
		apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-continue"}

		got, ok := client.deriveAggregateApplyState(t.Context(), apply, tasks)
		assert.True(t, ok, "current op row present, projection must be determined")
		assert.Equal(t, state.Apply.Running, got, "continue policy must hold the apply active until the pending sibling settles")
	})

	// Default policy (on_failure unset) fails closed: a terminally failed
	// deployment terminalizes the apply even with a pending sibling.
	t.Run("default policy terminalizes the apply on a failed deployment", func(t *testing.T) {
		tasks := []*storage.Task{taskWith(state.Task.Failed)}
		client := &LocalClient{
			storage: &exactProgressStorage{
				applyOperations: &listApplyOperationStore{
					ops: []*storage.ApplyOperation{
						{ID: currentOpID, State: state.ApplyOperation.Failed},
						{ID: 2, State: state.ApplyOperation.Pending},
					},
				},
			},
			logger: slog.Default(),
		}
		apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-halt"}

		got, ok := client.deriveAggregateApplyState(t.Context(), apply, tasks)
		assert.True(t, ok, "current op row present, projection must be determined")
		assert.Equal(t, state.Apply.Failed, got, "default policy must terminalize the apply on a failed deployment")
	})

	// No operation model in use: the tasks carry no apply_operation_id, or the
	// operation store is not configured. There are no siblings, so the per-task
	// derivation is authoritative and may terminalize — this preserves
	// single-writer/legacy behaviour for applies that predate apply_operation_id.
	taskNoOp := func(taskState string) *storage.Task {
		return &storage.Task{State: taskState}
	}
	noOperationModel := map[string]struct {
		tasks []*storage.Task
		store storage.ApplyOperationStore
	}{
		"tasks carry no apply_operation_id": {
			tasks: []*storage.Task{taskNoOp(state.Task.Completed)},
			store: &listApplyOperationStore{ops: []*storage.ApplyOperation{{ID: currentOpID, State: state.ApplyOperation.Pending}}},
		},
		"operation store not configured": {
			tasks: []*storage.Task{taskWith(state.Task.Completed)},
			store: nil,
		},
	}

	for name, tc := range noOperationModel {
		t.Run("no operation model ("+name+") terminalizes from tasks", func(t *testing.T) {
			client := &LocalClient{
				storage: &exactProgressStorage{applyOperations: tc.store},
				logger:  slog.Default(),
			}
			apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-no-op-model"}

			want := state.DeriveApplyState(taskStates(tc.tasks))
			require.True(t, state.IsTerminalApplyState(want), "precondition: per-task derivation is terminal")
			got, ok := client.deriveAggregateApplyState(t.Context(), apply, tc.tasks)
			assert.True(t, ok, "no siblings exist, so a terminal per-task derivation is authoritative")
			assert.Equal(t, want, got)
		})
	}

	// Operation model in use (tasks carry an apply_operation_id) but the sibling
	// rows cannot be read consistently. The sibling states are unknown, so a
	// terminal current-deployment derivation must not become the aggregate: a
	// transient read failure on the last-finishing deployment would otherwise
	// mark the whole apply terminal while siblings are still in flight. The
	// projection is reported undetermined (ok=false) so the caller keeps the
	// stored value for the next poll to reconcile.
	unreadableSiblings := map[string]storage.ApplyOperationStore{
		"operations cannot be read": &listApplyOperationStore{err: assert.AnError},
		"no operation rows found":   &listApplyOperationStore{ops: nil},
		"current operation row missing": &listApplyOperationStore{
			ops: []*storage.ApplyOperation{
				{ID: 2, State: state.ApplyOperation.Pending},
				{ID: 3, State: state.ApplyOperation.Pending},
			},
		},
	}

	for name, store := range unreadableSiblings {
		t.Run("unreadable siblings ("+name+") refuse to terminalize a terminal current deployment", func(t *testing.T) {
			tasks := []*storage.Task{taskWith(state.Task.Completed)}
			client := &LocalClient{
				storage: &exactProgressStorage{applyOperations: store},
				logger:  slog.Default(),
			}
			apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-unreadable-terminal"}

			require.True(t, state.IsTerminalApplyState(state.DeriveApplyState(taskStates(tasks))), "precondition: current deployment derives terminal")
			_, ok := client.deriveAggregateApplyState(t.Context(), apply, tasks)
			assert.False(t, ok, "must not overwrite stored apply state from a terminal single-op derivation")
		})

		t.Run("unreadable siblings ("+name+") fall back to a non-terminal current deployment", func(t *testing.T) {
			tasks := []*storage.Task{taskWith(state.Task.Running), taskWith(state.Task.Pending)}
			client := &LocalClient{
				storage: &exactProgressStorage{applyOperations: store},
				logger:  slog.Default(),
			}
			apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-unreadable-active"}

			want := state.DeriveApplyState(taskStates(tasks))
			require.False(t, state.IsTerminalApplyState(want), "precondition: current deployment derives non-terminal")
			got, ok := client.deriveAggregateApplyState(t.Context(), apply, tasks)
			assert.True(t, ok, "non-terminal single-op derivation is a safe fallback")
			assert.Equal(t, want, got)
		})
	}
}

// capturedLog records a single emitted log record's level, message, and
// attributes for assertions.
type capturedLog struct {
	level slog.Level
	msg   string
	attrs map[string]any
}

// captureHandler is a minimal slog.Handler that records every emitted record so
// tests can assert on level, message, and attributes.
type captureHandler struct {
	records *[]capturedLog
}

func (h captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h captureHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := make(map[string]any, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()
		return true
	})
	*h.records = append(*h.records, capturedLog{level: r.Level, msg: r.Message, attrs: attrs})
	return nil
}

func (h captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h captureHandler) WithGroup(string) slog.Handler      { return h }

// A canonical, positive revert_window_duration in metadata is honored, while an
// empty value falls back to the engine default. A malformed or non-positive
// value — which the server never writes but an embedder populating metadata
// directly can — fails observably: it warns with the offending value and the
// engine default rather than silently defaulting.
func TestLocalClient_RevertWindowDuration(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		want      time.Duration
		wantWarn  bool
		warnValue string
	}{
		{name: "honors a valid positive duration", value: "45m", want: 45 * time.Minute},
		{name: "empty uses engine default", value: "", want: defaultRevertWindowDuration},
		{name: "unparseable warns and uses default", value: "not-a-duration", want: defaultRevertWindowDuration, wantWarn: true, warnValue: "not-a-duration"},
		{name: "zero warns and uses default", value: "0s", want: defaultRevertWindowDuration, wantWarn: true, warnValue: "0s"},
		{name: "negative warns and uses default", value: "-5m", want: defaultRevertWindowDuration, wantWarn: true, warnValue: "-5m"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var records []capturedLog
			client := &LocalClient{
				config: LocalConfig{
					Database: "orders",
					Metadata: map[string]string{"revert_window_duration": tc.value},
				},
				logger: slog.New(captureHandler{records: &records}),
			}

			got := client.revertWindowDuration()
			assert.Equal(t, tc.want, got)

			var warnings []capturedLog
			for _, r := range records {
				if r.level == slog.LevelWarn {
					warnings = append(warnings, r)
				}
			}

			if !tc.wantWarn {
				assert.Empty(t, warnings, "no warning expected")
				return
			}

			require.Len(t, warnings, 1, "exactly one warning expected")
			w := warnings[0]
			assert.Equal(t, tc.warnValue, w.attrs["value"])
			assert.Equal(t, "orders", w.attrs["database"])
			assert.Equal(t, defaultRevertWindowDuration, w.attrs["default"])
		})
	}
}
