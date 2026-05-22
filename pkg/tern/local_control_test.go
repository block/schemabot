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
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

type controlTestApplyStore struct {
	storage.ApplyStore
	apply *storage.Apply
}

func (s *controlTestApplyStore) GetByApplyIdentifier(_ context.Context, applyIdentifier string) (*storage.Apply, error) {
	if s.apply == nil || s.apply.ApplyIdentifier != applyIdentifier {
		return nil, nil
	}
	return s.apply, nil
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

type controlTestApplyLogStore struct {
	storage.ApplyLogStore
}

func (s *controlTestApplyLogStore) Append(context.Context, *storage.ApplyLog) error {
	return nil
}

type controlTestVitessApplyDataStore struct {
	storage.VitessApplyDataStore
	data *storage.VitessApplyData
	err  error
}

func (s *controlTestVitessApplyDataStore) GetByApplyID(context.Context, int64) (*storage.VitessApplyData, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.data, nil
}

type controlTestStorage struct {
	storage.Storage
	applies         storage.ApplyStore
	tasks           storage.TaskStore
	applyLogs       storage.ApplyLogStore
	vitessApplyData storage.VitessApplyDataStore
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
	return s.vitessApplyData
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

func newVitessControlTestClient(apply *storage.Apply, tasks []*storage.Task, vitessData *storage.VitessApplyData, vitessDataErr error, eng *controlCaptureEngine) *LocalClient {
	return &LocalClient{
		config: LocalConfig{
			Database: "testdb",
			Type:     storage.DatabaseTypeVitess,
		},
		storage: &controlTestStorage{
			applies:         &controlTestApplyStore{apply: apply},
			tasks:           &controlTestTaskStore{tasks: tasks},
			applyLogs:       &controlTestApplyLogStore{},
			vitessApplyData: &controlTestVitessApplyDataStore{data: vitessData, err: vitessDataErr},
		},
		planetscaleEngine: eng,
		logger:            slog.Default(),
	}
}

// PlanetScale cutover must include the durable deploy metadata recorded for the
// apply. If that metadata is missing, LocalClient returns an error before
// invoking the engine so the storage invariant violation is visible.
func TestLocalClient_CutoverRequiresVitessApplyData(t *testing.T) {
	apply := &storage.Apply{ID: 42, ApplyIdentifier: "apply-vitess-control"}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-vitess-control",
		State:          state.Task.WaitingForCutover,
	}
	eng := &controlCaptureEngine{}
	client := newVitessControlTestClient(apply, []*storage.Task{task}, nil, storage.ErrVitessApplyDataNotFound, eng)

	_, err := client.Cutover(t.Context(), &ternv1.CutoverRequest{ApplyId: apply.ApplyIdentifier})

	require.ErrorIs(t, err, storage.ErrVitessApplyDataNotFound)
	assert.Nil(t, eng.cutoverReq, "cutover should not call the engine without deploy metadata")
}

// PlanetScale stores an initial VitessApplyData row before a deploy request is
// created. Control operations must wait until the row has the full deploy
// request metadata needed to address the server-side deploy request.
func TestLocalClient_CutoverRequiresCompleteVitessApplyData(t *testing.T) {
	testCases := []struct {
		name        string
		vitessData  *storage.VitessApplyData
		missingPart string
	}{
		{
			name: "branch setup before deploy request",
			vitessData: &storage.VitessApplyData{
				ApplyID:          42,
				BranchName:       "branch-123",
				MigrationContext: "ctx-123",
			},
			missingPart: "deploy_request_id",
		},
		{
			name: "missing branch",
			vitessData: &storage.VitessApplyData{
				ApplyID:          42,
				DeployRequestID:  321,
				MigrationContext: "ctx-123",
				DeployRequestURL: "https://example.test/deploys/321",
			},
			missingPart: "branch_name",
		},
		{
			name: "missing deploy request URL",
			vitessData: &storage.VitessApplyData{
				ApplyID:          42,
				BranchName:       "branch-123",
				DeployRequestID:  321,
				MigrationContext: "ctx-123",
			},
			missingPart: "deploy_request_url",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			apply := &storage.Apply{ID: 42, ApplyIdentifier: "apply-vitess-control"}
			task := &storage.Task{
				ID:             7,
				ApplyID:        apply.ID,
				TaskIdentifier: "task-vitess-control",
				State:          state.Task.WaitingForCutover,
			}
			eng := &controlCaptureEngine{}
			client := newVitessControlTestClient(apply, []*storage.Task{task}, tc.vitessData, nil, eng)

			_, err := client.Cutover(t.Context(), &ternv1.CutoverRequest{ApplyId: apply.ApplyIdentifier})

			require.ErrorContains(t, err, "deploy request metadata is incomplete")
			require.ErrorContains(t, err, tc.missingPart)
			assert.Nil(t, eng.cutoverReq, "cutover should not call the engine without deploy request metadata")
		})
	}
}

// PlanetScale cutover uses the stored deploy metadata to address the correct
// server-side deploy request. LocalClient should pass that metadata through to
// the engine without requiring a live progress poll first.
func TestLocalClient_CutoverPassesVitessApplyData(t *testing.T) {
	apply := &storage.Apply{ID: 42, ApplyIdentifier: "apply-vitess-control"}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-vitess-control",
		State:          state.Task.WaitingForCutover,
	}
	vitessData := &storage.VitessApplyData{
		ApplyID:          apply.ID,
		BranchName:       "branch-123",
		DeployRequestID:  321,
		MigrationContext: "ctx-123",
		DeployRequestURL: "https://example.test/deploys/321",
		IsInstant:        true,
		DeferredDeploy:   true,
	}
	eng := &controlCaptureEngine{}
	client := newVitessControlTestClient(apply, []*storage.Task{task}, vitessData, nil, eng)

	resp, err := client.Cutover(t.Context(), &ternv1.CutoverRequest{ApplyId: apply.ApplyIdentifier})

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	require.NotNil(t, eng.cutoverReq, "cutover should call the engine")
	require.NotNil(t, eng.cutoverReq.ResumeState)
	assert.Equal(t, "testdb", eng.cutoverReq.Database)
	assert.Equal(t, vitessData.MigrationContext, eng.cutoverReq.ResumeState.MigrationContext)

	var metadata struct {
		BranchName       string `json:"branch_name"`
		DeployRequestID  uint64 `json:"deploy_request_id"`
		DeployRequestURL string `json:"deploy_request_url"`
		IsInstant        bool   `json:"is_instant"`
		DeferredDeploy   bool   `json:"deferred_deploy"`
	}
	require.NoError(t, json.Unmarshal([]byte(eng.cutoverReq.ResumeState.Metadata), &metadata))
	assert.Equal(t, vitessData.BranchName, metadata.BranchName)
	assert.Equal(t, vitessData.DeployRequestID, metadata.DeployRequestID)
	assert.Equal(t, vitessData.DeployRequestURL, metadata.DeployRequestURL)
	assert.Equal(t, vitessData.IsInstant, metadata.IsInstant)
	assert.Equal(t, vitessData.DeferredDeploy, metadata.DeferredDeploy)
}

// PlanetScale stop maps to permanent deploy-request cancellation. If the engine
// cannot cancel the deploy request, LocalClient should return the error instead
// of marking the apply cancelled locally.
func TestLocalClient_StopReturnsVitessEngineStopError(t *testing.T) {
	apply := &storage.Apply{ID: 42, ApplyIdentifier: "apply-vitess-control"}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-vitess-control",
		State:          state.Task.Running,
	}
	vitessData := &storage.VitessApplyData{
		ApplyID:          apply.ID,
		BranchName:       "branch-123",
		DeployRequestID:  321,
		MigrationContext: "ctx-123",
		DeployRequestURL: "https://example.test/deploys/321",
	}
	stopErr := errors.New("cancel deploy request failed")
	eng := &controlCaptureEngine{stopErr: stopErr}
	client := newVitessControlTestClient(apply, []*storage.Task{task}, vitessData, nil, eng)

	_, err := client.Stop(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier})

	require.ErrorIs(t, err, stopErr)
	require.NotNil(t, eng.stopReq, "stop should call the engine with deploy metadata")
	assert.Equal(t, state.Task.Running, task.State)
}
