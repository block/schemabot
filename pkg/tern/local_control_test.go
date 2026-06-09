package tern

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/planetscale"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

type controlTestApplyStore struct {
	storage.ApplyStore
	apply *storage.Apply
}

func (s *controlTestApplyStore) Get(_ context.Context, id int64) (*storage.Apply, error) {
	if s.apply == nil || s.apply.ID != id {
		return nil, nil
	}
	return s.apply, nil
}

func (s *controlTestApplyStore) GetByApplyIdentifier(_ context.Context, applyIdentifier string) (*storage.Apply, error) {
	if s.apply == nil || s.apply.ApplyIdentifier != applyIdentifier {
		return nil, nil
	}
	return s.apply, nil
}

func (s *controlTestApplyStore) Update(_ context.Context, apply *storage.Apply) error {
	s.apply = apply
	return nil
}

type controlTestTaskStore struct {
	storage.TaskStore
	tasks []*storage.Task
}

func (s *controlTestTaskStore) GetByApplyID(_ context.Context, _ int64) ([]*storage.Task, error) {
	return s.tasks, nil
}

func (s *controlTestTaskStore) GetByDatabase(_ context.Context, _ string) ([]*storage.Task, error) {
	return s.tasks, nil
}

func (s *controlTestTaskStore) Update(_ context.Context, task *storage.Task) error {
	for i, storedTask := range s.tasks {
		if storedTask.ID == task.ID || storedTask.TaskIdentifier == task.TaskIdentifier {
			s.tasks[i] = task
			return nil
		}
	}
	return storage.ErrTaskNotFound
}

type controlTestApplyLogStore struct {
	storage.ApplyLogStore
}

func (s *controlTestApplyLogStore) Append(context.Context, *storage.ApplyLog) error {
	return nil
}

type controlTestApplyOperationStore struct {
	storage.ApplyOperationStore
	data *storage.EngineResumeState
	err  error
}

func (s *controlTestApplyOperationStore) GetEngineResumeState(context.Context, int64) (*storage.EngineResumeState, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.data == nil {
		return nil, storage.ErrEngineResumeStateNotFound
	}
	return s.data, nil
}

type controlTestStorage struct {
	storage.Storage
	applies         storage.ApplyStore
	tasks           storage.TaskStore
	applyLogs       storage.ApplyLogStore
	applyOperations storage.ApplyOperationStore
	controlRequests storage.ControlRequestStore
}

func (s *controlTestStorage) Applies() storage.ApplyStore {
	return s.applies
}

func (s *controlTestStorage) Tasks() storage.TaskStore {
	return s.tasks
}

func (s *controlTestStorage) ApplyLogs() storage.ApplyLogStore {
	return s.applyLogs
}

func (s *controlTestStorage) VitessApplyData() storage.VitessApplyDataStore {
	return nil
}

func (s *controlTestStorage) ApplyOperations() storage.ApplyOperationStore {
	return s.applyOperations
}

func (s *controlTestStorage) ControlRequests() storage.ControlRequestStore {
	if s.controlRequests != nil {
		return s.controlRequests
	}
	return &testControlRequestStore{}
}

type controlCaptureEngine struct {
	engine.Engine
	cutoverReq *engine.ControlRequest
	stopReq    *engine.ControlRequest
	stopErr    error
}

func (e *controlCaptureEngine) Name() string {
	return "control-capture"
}

func (e *controlCaptureEngine) Cutover(_ context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	e.cutoverReq = req
	return &engine.ControlResult{Accepted: true}, nil
}

func (e *controlCaptureEngine) Stop(_ context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	e.stopReq = req
	if e.stopErr != nil {
		return nil, e.stopErr
	}
	return &engine.ControlResult{Accepted: true}, nil
}

func (e *controlCaptureEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	return &engine.ProgressResult{}, nil
}

func newMySQLControlTestClient(apply *storage.Apply, tasks []*storage.Task, eng *controlCaptureEngine) *LocalClient {
	return &LocalClient{
		config: LocalConfig{
			Database: "testdb",
			Type:     storage.DatabaseTypeMySQL,
		},
		storage: &controlTestStorage{
			applies:   &controlTestApplyStore{apply: apply},
			tasks:     &controlTestTaskStore{tasks: tasks},
			applyLogs: &controlTestApplyLogStore{},
		},
		spiritEngine: eng,
		logger:       slog.Default(),
	}
}

func newVitessControlTestClient(apply *storage.Apply, tasks []*storage.Task, resumeState *storage.EngineResumeState, eng engine.Engine) *LocalClient {
	return &LocalClient{
		config: LocalConfig{
			Database: "testdb",
			Type:     storage.DatabaseTypeVitess,
		},
		storage: &controlTestStorage{
			applies:         &controlTestApplyStore{apply: apply},
			tasks:           &controlTestTaskStore{tasks: tasks},
			applyLogs:       &controlTestApplyLogStore{},
			applyOperations: &controlTestApplyOperationStore{data: resumeState},
		},
		planetscaleEngine: eng,
		logger:            slog.Default(),
	}
}

func engineResumeStateFromPlanetScaleData(t *testing.T, operationID int64, data planetscale.ResumeData) *storage.EngineResumeState {
	t.Helper()
	engineState, err := planetscale.BuildResumeState(data)
	require.NoError(t, err)
	return &storage.EngineResumeState{
		ApplyOperationID: operationID,
		MigrationContext: engineState.MigrationContext,
		Metadata:         engineState.Metadata,
	}
}

func maybeEngineResumeStateFromPlanetScaleData(operationID int64, data planetscale.ResumeData) *storage.EngineResumeState {
	engineState, err := planetscale.BuildResumeState(data)
	if err != nil {
		return nil
	}
	return &storage.EngineResumeState{
		ApplyOperationID: operationID,
		MigrationContext: engineState.MigrationContext,
		Metadata:         engineState.Metadata,
	}
}

// Local MySQL stop is resumable: stopped task rows and the stored apply row
// should move to stopped together so a later operator-owned start can claim
// the apply without waiting for stale heartbeat recovery.
func TestLocalClient_StopMarksMySQLApplyStopped(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-mysql-stop",
		State:           state.Apply.Running,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
	}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-mysql-stop",
		Database:       "testdb",
		State:          state.Task.Running,
	}
	eng := &controlCaptureEngine{}
	client := newMySQLControlTestClient(apply, []*storage.Task{task}, eng)

	resp, err := client.Stop(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier})

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(1), resp.StoppedCount)
	assert.Equal(t, state.Task.Stopped, task.State)
	assert.Equal(t, state.Apply.Stopped, apply.State)
	assert.Nil(t, apply.CompletedAt)
	require.NotNil(t, eng.stopReq, "stop should call the engine")
}

// PlanetScale cutover must include the opaque resume state recorded for the
// apply. If that state is missing, LocalClient returns an error before
// invoking the engine so the storage invariant violation is visible.
func TestLocalClient_CutoverRequiresEngineResumeState(t *testing.T) {
	operationID := int64(99)
	apply := &storage.Apply{ID: 42, ApplyIdentifier: "apply-vitess-control"}
	task := &storage.Task{
		ID:               7,
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TaskIdentifier:   "task-vitess-control",
		State:            state.Task.WaitingForCutover,
	}
	eng := &controlCaptureEngine{}
	client := newVitessControlTestClient(apply, []*storage.Task{task}, nil, eng)

	_, err := client.Cutover(t.Context(), &ternv1.CutoverRequest{ApplyId: apply.ApplyIdentifier})

	require.ErrorIs(t, err, storage.ErrEngineResumeStateNotFound)
	assert.Nil(t, eng.cutoverReq, "cutover should not call the engine without resume state")
}

// PlanetScale can persist opaque resume state before a deploy request is
// created. Control operations must wait until the state has the full deploy
// request metadata needed to address the server-side deploy request.
func TestLocalClient_CutoverRequiresCompleteEngineResumeState(t *testing.T) {
	testCases := []struct {
		name        string
		resumeData  planetscale.ResumeData
		missingPart string
	}{
		{
			name: "branch setup before deploy request",
			resumeData: planetscale.ResumeData{
				BranchName:       "branch-123",
				MigrationContext: "ctx-123",
			},
			missingPart: "deploy_request_id",
		},
		{
			name: "missing branch",
			resumeData: planetscale.ResumeData{
				DeployRequestID:  321,
				MigrationContext: "ctx-123",
				DeployRequestURL: "https://example.test/deploys/321",
			},
			missingPart: "branch_name",
		},
		{
			name: "missing deploy request URL",
			resumeData: planetscale.ResumeData{
				BranchName:       "branch-123",
				DeployRequestID:  321,
				MigrationContext: "ctx-123",
			},
			missingPart: "deploy_request_url",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			operationID := int64(99)
			apply := &storage.Apply{ID: 42, ApplyIdentifier: "apply-vitess-control"}
			task := &storage.Task{
				ID:               7,
				ApplyID:          apply.ID,
				ApplyOperationID: &operationID,
				TaskIdentifier:   "task-vitess-control",
				State:            state.Task.WaitingForCutover,
			}
			eng := planetscale.New(slog.Default())
			resumeState := maybeEngineResumeStateFromPlanetScaleData(operationID, tc.resumeData)
			client := newVitessControlTestClient(apply, []*storage.Task{task}, resumeState, eng)

			_, err := client.Cutover(t.Context(), &ternv1.CutoverRequest{ApplyId: apply.ApplyIdentifier})

			require.ErrorContains(t, err, "cutover control resume state is incomplete")
			require.ErrorContains(t, err, tc.missingPart)
		})
	}
}

// PlanetScale cutover uses the stored resume state to address the correct
// server-side deploy request. LocalClient should pass that metadata through to
// the engine without requiring a live progress poll first.
func TestLocalClient_CutoverPassesEngineResumeState(t *testing.T) {
	operationID := int64(99)
	apply := &storage.Apply{ID: 42, ApplyIdentifier: "apply-vitess-control"}
	task := &storage.Task{
		ID:               7,
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TaskIdentifier:   "task-vitess-control",
		State:            state.Task.WaitingForCutover,
	}
	resumeData := planetscale.ResumeData{
		BranchName:       "branch-123",
		DeployRequestID:  321,
		MigrationContext: "ctx-123",
		DeployRequestURL: "https://example.test/deploys/321",
		IsInstant:        true,
		DeferredDeploy:   true,
	}
	resumeState := engineResumeStateFromPlanetScaleData(t, operationID, resumeData)
	eng := &controlCaptureEngine{}
	client := newVitessControlTestClient(apply, []*storage.Task{task}, resumeState, eng)

	resp, err := client.Cutover(t.Context(), &ternv1.CutoverRequest{ApplyId: apply.ApplyIdentifier})

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	require.NotNil(t, eng.cutoverReq, "cutover should call the engine")
	require.NotNil(t, eng.cutoverReq.ResumeState)
	assert.Equal(t, "testdb", eng.cutoverReq.Database)
	assert.Equal(t, resumeData.MigrationContext, eng.cutoverReq.ResumeState.MigrationContext)

	var metadata struct {
		BranchName       string `json:"branch_name"`
		DeployRequestID  uint64 `json:"deploy_request_id"`
		DeployRequestURL string `json:"deploy_request_url"`
		IsInstant        bool   `json:"is_instant"`
		DeferredDeploy   bool   `json:"deferred_deploy"`
	}
	require.NoError(t, json.Unmarshal([]byte(eng.cutoverReq.ResumeState.Metadata), &metadata))
	assert.Equal(t, resumeData.BranchName, metadata.BranchName)
	assert.Equal(t, resumeData.DeployRequestID, metadata.DeployRequestID)
	assert.Equal(t, resumeData.DeployRequestURL, metadata.DeployRequestURL)
	assert.Equal(t, resumeData.IsInstant, metadata.IsInstant)
	assert.Equal(t, resumeData.DeferredDeploy, metadata.DeferredDeploy)
}

// PlanetScale stop maps to permanent deploy-request cancellation. If the engine
// cannot cancel the deploy request, LocalClient should return the error instead
// of marking the apply cancelled locally.
func TestLocalClient_StopReturnsVitessEngineStopError(t *testing.T) {
	operationID := int64(99)
	apply := &storage.Apply{ID: 42, ApplyIdentifier: "apply-vitess-control"}
	task := &storage.Task{
		ID:               7,
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TaskIdentifier:   "task-vitess-control",
		State:            state.Task.Running,
	}
	resumeState := engineResumeStateFromPlanetScaleData(t, operationID, planetscale.ResumeData{
		BranchName:       "branch-123",
		DeployRequestID:  321,
		MigrationContext: "ctx-123",
		DeployRequestURL: "https://example.test/deploys/321",
	})
	stopErr := errors.New("cancel deploy request failed")
	eng := &controlCaptureEngine{stopErr: stopErr}
	client := newVitessControlTestClient(apply, []*storage.Task{task}, resumeState, eng)

	_, err := client.Stop(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier})

	require.ErrorIs(t, err, stopErr)
	require.NotNil(t, eng.stopReq, "stop should call the engine with deploy metadata")
	assert.Equal(t, state.Task.Running, task.State)
}
