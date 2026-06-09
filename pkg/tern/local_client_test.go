package tern

import (
	"context"
	"log/slog"
	"reflect"
	"slices"
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
