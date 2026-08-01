package tern

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

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
	cutoverReq  *engine.ControlRequest
	stopReq     *engine.ControlRequest
	cancelReq   *engine.ControlRequest
	progressReq *engine.ProgressRequest
	stopErr     error
	cancelErr   error
	// onStop runs when Stop is invoked, before it returns. Used to observe
	// storage state at the moment of the engine stop (e.g. to assert the engine
	// is stopped before tasks are marked stopped/cancelled).
	onStop func()
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
	if e.onStop != nil {
		e.onStop()
	}
	if e.stopErr != nil {
		return nil, e.stopErr
	}
	return &engine.ControlResult{Accepted: true}, nil
}

func (e *controlCaptureEngine) Cancel(_ context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	e.cancelReq = req
	if e.onStop != nil {
		e.onStop()
	}
	if e.cancelErr != nil {
		return nil, e.cancelErr
	}
	return &engine.ControlResult{Accepted: true}, nil
}

func (e *controlCaptureEngine) Progress(_ context.Context, req *engine.ProgressRequest) (*engine.ProgressResult, error) {
	e.progressReq = req
	return &engine.ProgressResult{}, nil
}

func newMySQLControlTestClient(apply *storage.Apply, tasks []*storage.Task, eng *controlCaptureEngine) *LocalClient {
	return &LocalClient{
		config: LocalConfig{
			Database:  "testdb",
			Type:      storage.DatabaseTypeMySQL,
			TargetDSN: "root@tcp(localhost:3306)/",
		},
		storage: &controlTestStorage{
			applies:         &controlTestApplyStore{apply: apply},
			tasks:           &controlTestTaskStore{tasks: tasks},
			applyLogs:       &controlTestApplyLogStore{},
			controlRequests: &testControlRequestStore{},
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
			controlRequests: &testControlRequestStore{},
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
		Namespace:      "testdb",
		State:          state.Task.Running,
	}
	eng := &controlCaptureEngine{}
	client := newMySQLControlTestClient(apply, []*storage.Task{task}, eng)

	resp, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(1), resp.StoppedCount)
	assert.Equal(t, state.Task.Stopped, task.State)
	assert.Equal(t, state.Apply.Stopped, apply.State)
	assert.Nil(t, apply.CompletedAt)
	require.NotNil(t, eng.stopReq, "stop should call the engine")
}

// External Stop records durable intent in shared storage. The apply owner
// observes the pending request and performs the local engine stop, so any
// replica can accept the request without mutating stopped state itself.
func TestLocalClient_StopQueuesStopRequestForApplyOwner(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-mysql-stop-queued",
		State:           state.Apply.Running,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
	}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-mysql-stop-queued",
		Database:       "testdb",
		Namespace:      "testdb",
		State:          state.Task.Running,
	}
	eng := &controlCaptureEngine{}
	client := newMySQLControlTestClient(apply, []*storage.Task{task}, eng)
	var wakeApplyID, wakeDatabase, wakeEnvironment string
	client.config.WakeOperator = func(applyIdentifier, database, environment string) {
		wakeApplyID = applyIdentifier
		wakeDatabase = database
		wakeEnvironment = environment
	}

	resp, err := client.Stop(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier})

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	assert.Nil(t, eng.stopReq, "external stop should not call the local engine directly")
	assert.Equal(t, state.Task.Running, task.State)
	assert.Equal(t, state.Apply.Running, apply.State)
	controlReq, err := client.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	require.NotNil(t, controlReq)
	assert.Equal(t, "tern-grpc", controlReq.RequestedBy)
	assert.Equal(t, apply.ApplyIdentifier, wakeApplyID)
	assert.Equal(t, apply.Database, wakeDatabase)
	assert.Equal(t, apply.Environment, wakeEnvironment)
}

// External Cancel records durable intent in shared storage instead of cancelling
// inline. A Cancel RPC can land on any replica, but only the lease owner driving
// the apply holds the in-process engine state — so a non-owner replica records a
// pending cancel request for the owning driver to claim, without touching the
// engine or mutating cancelled state itself. This keeps cancel from depending on
// the request happening to hit the owner pod.
func TestLocalClient_CancelQueuesCancelRequestForApplyOwner(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-vitess-cancel-queued",
		State:           state.Apply.Running,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeVitess,
		Environment:     "staging",
	}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-vitess-cancel-queued",
		Database:       "testdb",
		Namespace:      "testdb",
		State:          state.Task.Running,
	}
	eng := &controlCaptureEngine{}
	client := newVitessControlTestClient(apply, []*storage.Task{task}, nil, eng)
	var wakeApplyID, wakeDatabase, wakeEnvironment string
	client.config.WakeOperator = func(applyIdentifier, database, environment string) {
		wakeApplyID = applyIdentifier
		wakeDatabase = database
		wakeEnvironment = environment
	}

	resp, err := client.Cancel(t.Context(), &ternv1.CancelRequest{ApplyId: apply.ApplyIdentifier})

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	assert.Nil(t, eng.cancelReq, "external cancel must not cancel the local engine directly")
	assert.Equal(t, state.Task.Running, task.State, "external cancel must not mutate task state inline")
	assert.Equal(t, state.Apply.Running, apply.State, "external cancel must not mutate apply state inline")
	controlReq, err := client.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	require.NotNil(t, controlReq)
	assert.Equal(t, "tern-grpc", controlReq.RequestedBy)
	assert.Equal(t, apply.ApplyIdentifier, wakeApplyID)
	assert.Equal(t, apply.Database, wakeDatabase)
	assert.Equal(t, apply.Environment, wakeEnvironment)
}

// Apply-owner stop is only authoritative after the local Spirit runner stops.
// If the engine cannot stop, storage must remain active so user-facing status
// does not diverge from a runner that is still copying rows.
func TestLocalClient_StopOwnedApplyReturnsMySQLEngineStopError(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-mysql-stop-non-owner",
		State:           state.Apply.Running,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
	}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-mysql-stop-non-owner",
		Database:       "testdb",
		Namespace:      "testdb",
		State:          state.Task.Running,
	}
	stopErr := errors.New("no active schema change to stop")
	eng := &controlCaptureEngine{stopErr: stopErr}
	client := newMySQLControlTestClient(apply, []*storage.Task{task}, eng)

	_, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.ErrorIs(t, err, stopErr)
	require.NotNil(t, eng.stopReq, "stop should call the local engine before mutating storage")
	assert.Equal(t, state.Task.Running, task.State)
	assert.Equal(t, state.Apply.Running, apply.State)
	assert.Nil(t, apply.CompletedAt)
}

// Sequential MySQL applies can contain tasks from multiple namespaces, but only
// one Spirit operation is live at a time. Stop and the post-stop progress
// snapshot must use the namespace for the task that had live engine work, not a
// different targeted task that happened to appear first in storage order.
func TestLocalClient_StopSnapshotsProgressWithStoppedTaskNamespace(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-mysql-multi-namespace-stop",
		State:           state.Apply.Running,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
	}
	pendingTask := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-pending",
		Database:       "testdb",
		Namespace:      "pending_schema",
		State:          state.Task.Pending,
	}
	liveTask := &storage.Task{
		ID:             8,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-live",
		Database:       "testdb",
		Namespace:      "live_schema",
		State:          state.Task.Running,
	}
	eng := &controlCaptureEngine{}
	client := newMySQLControlTestClient(apply, []*storage.Task{pendingTask, liveTask}, eng)

	resp, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	require.NotNil(t, eng.stopReq, "stop should call the engine for the live task")
	require.NotNil(t, eng.stopReq.Credentials)
	assert.Equal(t, "root@tcp(localhost:3306)/live_schema", eng.stopReq.Credentials.DSN)
	require.NotNil(t, eng.progressReq, "stop should snapshot progress with the stopped task credentials")
	require.NotNil(t, eng.progressReq.Credentials)
	assert.Equal(t, "root@tcp(localhost:3306)/live_schema", eng.progressReq.Credentials.DSN)
}

// A stop request can race with a driver that finalized all task rows but
// exited before finalizing the apply row. When every targeted task is already
// terminal, stop derives the apply's final state from its tasks: an apply
// whose tasks failed must finish as failed, never as completed, so the
// failure stays visible to operators instead of being masked as a success.
// When the derived state is failed, the failure reason from the task rows is
// propagated to the apply record so operators can triage without digging into
// individual tasks; a reason already on the apply is kept.
func TestLocalClient_StopAllTasksTerminalDerivesApplyState(t *testing.T) {
	testCases := []struct {
		name              string
		taskStates        []string
		taskErrors        []string
		applyErrorMessage string
		wantApplyState    string
		wantErrorMessage  string
	}{
		{
			name:           "all tasks completed",
			taskStates:     []string{state.Task.Completed, state.Task.Completed},
			wantApplyState: state.Apply.Completed,
		},
		{
			name:             "all tasks failed",
			taskStates:       []string{state.Task.Failed, state.Task.Failed},
			taskErrors:       []string{"", "row copy failed: lock wait timeout"},
			wantApplyState:   state.Apply.Failed,
			wantErrorMessage: "table t2 failed: row copy failed: lock wait timeout",
		},
		{
			name:             "failed task among completed tasks",
			taskStates:       []string{state.Task.Completed, state.Task.Failed},
			taskErrors:       []string{"", "cutover failed: deadlock detected"},
			wantApplyState:   state.Apply.Failed,
			wantErrorMessage: "table t2 failed: cutover failed: deadlock detected",
		},
		{
			name:              "existing apply error message is kept",
			taskStates:        []string{state.Task.Failed, state.Task.Failed},
			taskErrors:        []string{"row copy failed: lock wait timeout", ""},
			applyErrorMessage: "operator recorded failure reason",
			wantApplyState:    state.Apply.Failed,
			wantErrorMessage:  "operator recorded failure reason",
		},
		{
			name:           "all tasks cancelled",
			taskStates:     []string{state.Task.Cancelled, state.Task.Cancelled},
			wantApplyState: state.Apply.Cancelled,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			apply := &storage.Apply{
				ID:              42,
				ApplyIdentifier: "apply-mysql-stop-terminal",
				State:           state.Apply.Running,
				Database:        "testdb",
				DatabaseType:    storage.DatabaseTypeMySQL,
				Environment:     "staging",
				ErrorMessage:    tc.applyErrorMessage,
			}
			tasks := make([]*storage.Task, 0, len(tc.taskStates))
			for i, taskState := range tc.taskStates {
				task := &storage.Task{
					ID:             int64(i + 1),
					ApplyID:        apply.ID,
					TaskIdentifier: fmt.Sprintf("task-mysql-stop-terminal-%d", i+1),
					TableName:      fmt.Sprintf("t%d", i+1),
					Database:       "testdb",
					Namespace:      "testdb",
					State:          taskState,
				}
				if i < len(tc.taskErrors) {
					task.ErrorMessage = tc.taskErrors[i]
				}
				tasks = append(tasks, task)
			}
			client := newMySQLControlTestClient(apply, tasks, &controlCaptureEngine{})

			resp, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

			require.NoError(t, err)
			assert.True(t, resp.Accepted)
			assert.Equal(t, int64(0), resp.StoppedCount)
			assert.Equal(t, int64(len(tasks)), resp.SkippedCount)
			assert.Equal(t, "Schema change already "+tc.wantApplyState, resp.ErrorMessage)
			assert.Equal(t, tc.wantApplyState, apply.State)
			assert.Equal(t, tc.wantErrorMessage, apply.ErrorMessage)
			require.NotNil(t, apply.CompletedAt)
		})
	}
}

// When the apply owner performs a cutover, the request must include the opaque
// resume state recorded for the apply. If that state is missing, the owner
// returns an error before invoking the engine so the storage invariant
// violation is visible.
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

	_, err := client.cutover(t.Context(), &ternv1.CutoverRequest{ApplyId: apply.ApplyIdentifier}, "")

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

			_, err := client.cutover(t.Context(), &ternv1.CutoverRequest{ApplyId: apply.ApplyIdentifier}, "")

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

	resp, err := client.cutover(t.Context(), &ternv1.CutoverRequest{ApplyId: apply.ApplyIdentifier}, "")

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

// PlanetScale stop is unsupported because deploy-request cancellation is
// permanent and belongs behind the cancel command.
func TestLocalClient_StopRejectsVitessApply(t *testing.T) {
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
	eng := &controlCaptureEngine{}
	client := newVitessControlTestClient(apply, []*storage.Task{task}, resumeState, eng)

	_, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "stop not supported")
	assert.Nil(t, eng.stopReq, "unsupported stop must not call the engine")
	assert.Equal(t, state.Task.Running, task.State)
}

// A recovering Spirit task still has a live runner copying rows under a detached
// context, so stop must kill it via the engine before storage records the stop.
// Otherwise storage reports stopped while the runner keeps working and a later
// resume blocks behind the abandoned runner.
func TestLocalClient_StopRecoveringMySQLStopsEngineBeforeStorage(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-mysql-recovering",
		State:           state.Apply.Recovering,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
	}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-mysql-recovering",
		Database:       "testdb",
		Namespace:      "testdb",
		State:          state.Task.Recovering,
	}
	eng := &controlCaptureEngine{}
	stateAtEngineStop := ""
	eng.onStop = func() { stateAtEngineStop = task.State }
	client := newMySQLControlTestClient(apply, []*storage.Task{task}, eng)

	resp, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(1), resp.StoppedCount)
	require.NotNil(t, eng.stopReq, "stop should call the engine for a recovering task")
	assert.Equal(t, state.Task.Recovering, stateAtEngineStop, "engine must be stopped before the task is marked stopped")
	assert.Equal(t, state.Task.Stopped, task.State)
	assert.Equal(t, state.Apply.Stopped, apply.State)
}

// A PlanetScale waiting-for-deploy task has a created, startable deploy request.
// Cancel must cancel that deploy request via the engine before recording the
// cancellation, otherwise the deploy stays startable outside SchemaBot while
// SchemaBot reports it cancelled.
func TestLocalClient_CancelWaitingForDeployCancelsDeployRequest(t *testing.T) {
	operationID := int64(99)
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-vitess-waiting-deploy",
		State:           state.Apply.WaitingForDeploy,
	}
	task := &storage.Task{
		ID:               7,
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TaskIdentifier:   "task-vitess-waiting-deploy",
		State:            state.Task.WaitingForDeploy,
	}
	resumeData := planetscale.ResumeData{
		BranchName:       "branch-123",
		DeployRequestID:  321,
		MigrationContext: "ctx-123",
		DeployRequestURL: "https://example.test/deploys/321",
		DeferredDeploy:   true,
	}
	resumeState := engineResumeStateFromPlanetScaleData(t, operationID, resumeData)
	eng := &controlCaptureEngine{}
	stateAtEngineCancel := ""
	eng.onStop = func() { stateAtEngineCancel = task.State }
	client := newVitessControlTestClient(apply, []*storage.Task{task}, resumeState, eng)

	resp, err := client.cancelOwnedApply(t.Context(), &ternv1.CancelRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	require.NotNil(t, eng.cancelReq, "cancel must cancel the deploy request for a waiting-for-deploy task")
	require.NotNil(t, eng.cancelReq.ResumeState)
	assert.Equal(t, resumeData.MigrationContext, eng.cancelReq.ResumeState.MigrationContext)
	assert.Equal(t, state.Task.WaitingForDeploy, stateAtEngineCancel, "deploy request must be cancelled before the task is marked cancelled")
	assert.Equal(t, state.Task.Cancelled, task.State)
	assert.Equal(t, state.Apply.Cancelled, apply.State)
}

// A PlanetScale failed_retryable task paused after a transient failure (e.g.
// repeated progress-poll errors) still has its created, startable deploy request
// and persisted resume state. Cancel must cancel that deploy request via the
// engine before recording the cancellation — otherwise the deploy stays
// startable outside SchemaBot while SchemaBot reports it cancelled.
func TestLocalClient_CancelFailedRetryableCancelsDeployRequest(t *testing.T) {
	operationID := int64(99)
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-vitess-failed-retryable",
		State:           state.Apply.FailedRetryable,
	}
	task := &storage.Task{
		ID:               7,
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TaskIdentifier:   "task-vitess-failed-retryable",
		State:            state.Task.FailedRetryable,
	}
	resumeData := planetscale.ResumeData{
		BranchName:       "branch-123",
		DeployRequestID:  321,
		MigrationContext: "ctx-123",
		DeployRequestURL: "https://example.test/deploys/321",
		DeferredDeploy:   true,
	}
	resumeState := engineResumeStateFromPlanetScaleData(t, operationID, resumeData)
	eng := &controlCaptureEngine{}
	stateAtEngineCancel := ""
	eng.onStop = func() { stateAtEngineCancel = task.State }
	client := newVitessControlTestClient(apply, []*storage.Task{task}, resumeState, eng)

	resp, err := client.cancelOwnedApply(t.Context(), &ternv1.CancelRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	require.NotNil(t, eng.cancelReq, "cancel must cancel the deploy request for a failed_retryable task")
	require.NotNil(t, eng.cancelReq.ResumeState)
	assert.Equal(t, resumeData.MigrationContext, eng.cancelReq.ResumeState.MigrationContext)
	assert.Equal(t, state.Task.FailedRetryable, stateAtEngineCancel, "deploy request must be cancelled before the task is marked cancelled")
	assert.Equal(t, state.Task.Cancelled, task.State)
	assert.Equal(t, state.Apply.Cancelled, apply.State)
}

// Cancel must preserve an apply's recorded final outcome. Completed, failed,
// and reverted describe engine work that already happened on the target, so a
// late cancel must not rewrite them — a fully-applied change recorded as
// cancelled would mislead the merge gate and operators. Cancelled is
// re-writable (repeated cancel is idempotent) and stopped is terminal but
// still cancellable, so neither is preserved.
func TestCancelPreservesApplyOutcome(t *testing.T) {
	assert.True(t, cancelPreservesApplyOutcome(state.Apply.Completed))
	assert.True(t, cancelPreservesApplyOutcome(state.Apply.Failed))
	assert.True(t, cancelPreservesApplyOutcome(state.Apply.Reverted))
	assert.False(t, cancelPreservesApplyOutcome(state.Apply.Cancelled))
	assert.False(t, cancelPreservesApplyOutcome(state.Apply.Stopped))
	assert.False(t, cancelPreservesApplyOutcome(state.Apply.Running))
	assert.False(t, cancelPreservesApplyOutcome(state.Apply.FailedRetryable))
}

// A cancel that arrives after the apply already reached a final outcome must
// not rewrite the stored apply record. The schema change already ran (or
// failed) on the target, so flipping the record to cancelled would tell the
// merge gate and operators that a deployed change never happened. The cancel
// is acknowledged with nothing left to cancel, the engine is not touched, and
// the apply keeps its recorded state and completion time.
func TestLocalClient_CancelAfterFinalOutcomePreservesApplyState(t *testing.T) {
	testCases := []struct {
		name       string
		applyState string
		taskState  string
	}{
		{name: "completed apply stays completed", applyState: state.Apply.Completed, taskState: state.Task.Completed},
		{name: "failed apply stays failed", applyState: state.Apply.Failed, taskState: state.Task.Failed},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			completedAt := time.Now().Add(-time.Hour)
			apply := &storage.Apply{
				ID:              42,
				ApplyIdentifier: "apply-vitess-cancel-final",
				State:           tc.applyState,
				Database:        "testdb",
				DatabaseType:    storage.DatabaseTypeVitess,
				Environment:     "staging",
				CompletedAt:     &completedAt,
			}
			task := &storage.Task{
				ID:             7,
				ApplyID:        apply.ID,
				TaskIdentifier: "task-vitess-cancel-final",
				Database:       "testdb",
				Namespace:      "testdb",
				State:          tc.taskState,
			}
			eng := &controlCaptureEngine{}
			client := newVitessControlTestClient(apply, []*storage.Task{task}, nil, eng)

			resp, err := client.cancelOwnedApply(t.Context(), &ternv1.CancelRequest{ApplyId: apply.ApplyIdentifier}, "")

			require.NoError(t, err)
			assert.True(t, resp.Accepted)
			assert.Equal(t, int64(0), resp.CancelledCount)
			assert.Equal(t, int64(1), resp.SkippedCount)
			assert.Nil(t, eng.cancelReq, "cancel of a finished apply must not touch the engine")
			assert.Equal(t, tc.applyState, apply.State, "cancel must not rewrite the recorded final outcome")
			assert.Equal(t, tc.taskState, task.State)
			require.NotNil(t, apply.CompletedAt)
			assert.Equal(t, completedAt, *apply.CompletedAt, "cancel must not restamp the completion time")
		})
	}
}

// Stopped is terminal but explicitly cancellable: an operator can cancel a
// stopped apply to abandon its checkpoint permanently. Cancel must still move
// the stopped apply and its stopped tasks to cancelled.
func TestLocalClient_CancelStoppedApplyMarksCancelled(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-mysql-cancel-stopped",
		State:           state.Apply.Stopped,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
	}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-mysql-cancel-stopped",
		Database:       "testdb",
		Namespace:      "testdb",
		State:          state.Task.Stopped,
	}
	client := newMySQLControlTestClient(apply, []*storage.Task{task}, &controlCaptureEngine{})

	resp, err := client.cancelOwnedApply(t.Context(), &ternv1.CancelRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(1), resp.CancelledCount)
	assert.Equal(t, state.Task.Cancelled, task.State)
	assert.Equal(t, state.Apply.Cancelled, apply.State)
	require.NotNil(t, apply.CompletedAt)
}

// A task in the revert window has already cut over: the new schema is live.
// Stop must reject rather than record it as cancelled, so an operator chooses
// explicitly between reverting (undo) and skip-revert (finalize). The engine is
// not touched and the task state is preserved. The rejection names the
// apply-level identifier the operator supplied, not the per-table task id.
func TestLocalClient_StopRejectsRevertWindow(t *testing.T) {
	operationID := int64(99)
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-vitess-revert-window",
		State:           state.Apply.RevertWindow,
	}
	task := &storage.Task{
		ID:               7,
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TaskIdentifier:   "task-vitess-revert-window",
		State:            state.Task.RevertWindow,
	}
	resumeState := engineResumeStateFromPlanetScaleData(t, operationID, planetscale.ResumeData{
		BranchName:       "branch-123",
		DeployRequestID:  321,
		MigrationContext: "ctx-123",
		DeployRequestURL: "https://example.test/deploys/321",
	})
	eng := &controlCaptureEngine{}
	client := newVitessControlTestClient(apply, []*storage.Task{task}, resumeState, eng)

	_, err := client.Stop(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier})

	require.Error(t, err)
	assert.ErrorContains(t, err, "stop not supported")
	assert.NotContains(t, err.Error(), task.TaskIdentifier, "rejection must name the apply identifier, not the per-table task id")
	assert.Nil(t, eng.stopReq, "stop must not touch the engine for a revert-window task")
	assert.Equal(t, state.Task.RevertWindow, task.State, "revert-window task must not be marked cancelled by stop")
	assert.Equal(t, state.Apply.RevertWindow, apply.State, "revert-window apply must not be marked cancelled by stop")
}

// A task whose revert is in flight has the engine unwinding an already-applied
// change. Cancel must be rejected without touching the engine: cancelling the
// deploy request mid-revert would interrupt the unwind at an arbitrary point,
// and recording the task cancelled would settle storage while the provider
// keeps reverting. The revert owns the terminal outcome.
func TestLocalClient_CancelRejectsRevertingTask(t *testing.T) {
	operationID := int64(99)
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-vitess-reverting",
		State:           state.Apply.Reverting,
	}
	task := &storage.Task{
		ID:               7,
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TaskIdentifier:   "task-vitess-reverting",
		State:            state.Task.Reverting,
	}
	resumeState := engineResumeStateFromPlanetScaleData(t, operationID, planetscale.ResumeData{
		BranchName:       "branch-123",
		DeployRequestID:  321,
		MigrationContext: "ctx-123",
		DeployRequestURL: "https://example.test/deploys/321",
	})
	eng := &controlCaptureEngine{}
	client := newVitessControlTestClient(apply, []*storage.Task{task}, resumeState, eng)

	_, err := client.cancelOwnedApply(t.Context(), &ternv1.CancelRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.Error(t, err)
	assert.ErrorContains(t, err, "is being reverted")
	assert.ErrorContains(t, err, apply.ApplyIdentifier)
	assert.NotContains(t, err.Error(), task.TaskIdentifier, "rejection must name the apply identifier, not the per-table task id")
	assert.Nil(t, eng.cancelReq, "cancel must not touch the engine for a reverting task")
	assert.Equal(t, state.Task.Reverting, task.State, "reverting task must not be marked cancelled")
	assert.Equal(t, state.Apply.Reverting, apply.State, "reverting apply must not be marked cancelled")
}

// Skip-revert finalization has no task marker — the tasks already completed at
// cutover and the apply alone dwells in skipping_revert while the provider
// finalizes. Cancel must be rejected on the apply's own state: recording the
// apply cancelled would settle storage while the provider finishes the
// skip-revert, and the finalization owns the terminal outcome.
func TestLocalClient_CancelRejectsSkippingRevertApply(t *testing.T) {
	operationID := int64(99)
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-vitess-skipping-revert",
		State:           state.Apply.SkippingRevert,
	}
	task := &storage.Task{
		ID:               7,
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TaskIdentifier:   "task-vitess-skipping-revert",
		State:            state.Task.Completed,
	}
	eng := &controlCaptureEngine{}
	client := newVitessControlTestClient(apply, []*storage.Task{task}, nil, eng)

	_, err := client.cancelOwnedApply(t.Context(), &ternv1.CancelRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.Error(t, err)
	assert.ErrorContains(t, err, "finalizing skip-revert")
	assert.ErrorContains(t, err, apply.ApplyIdentifier)
	assert.Nil(t, eng.cancelReq, "cancel must not touch the engine while skip-revert finalizes")
	assert.Equal(t, state.Apply.SkippingRevert, apply.State, "skipping_revert apply must not be marked cancelled")
}

// A durable cancel request that finds the apply in a revert phase is resolved
// as permanently failed — not retried and not executed. Failing the stored
// request stops the operator-owned retry loop, and the apply keeps its
// revert-phase state so the in-flight revert or finalization drives the
// terminal outcome.
func TestLocalClient_PendingCancelFailsClosedForRevertPhase(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-vitess-pending-cancel-reverting",
		State:           state.Apply.Reverting,
	}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-vitess-pending-cancel-reverting",
		State:          state.Task.Reverting,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:   apply.ID,
		Operation: storage.ControlOperationCancel,
		Status:    storage.ControlRequestPending,
	}}}
	eng := &controlCaptureEngine{}
	client := &LocalClient{
		config: LocalConfig{Database: "testdb", Type: storage.DatabaseTypeVitess},
		storage: &controlTestStorage{
			applies:         &controlTestApplyStore{apply: apply},
			tasks:           &controlTestTaskStore{tasks: []*storage.Task{task}},
			applyLogs:       &controlTestApplyLogStore{},
			controlRequests: controlRequests,
		},
		planetscaleEngine: eng,
		logger:            slog.Default(),
	}

	handled, err := client.processPendingCancelControlRequest(t.Context(), apply)

	require.NoError(t, err)
	assert.True(t, handled)
	assert.Nil(t, eng.cancelReq, "cancel must not touch the engine for a revert-phase apply")
	assert.Equal(t, state.Apply.Reverting, apply.State, "revert-phase apply must keep its state")
	pending, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.Nil(t, pending, "the durable cancel request must be resolved, not left pending")
	resolved, err := controlRequests.GetByOperation(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	require.NotNil(t, resolved)
	assert.Equal(t, storage.ControlRequestFailed, resolved.Status, "a revert-phase cancel is a permanent rejection")
}

// Cancelled must never overwrite a revert-phase apply state, whichever path
// asks for it: the engine's revert or skip-revert owns the terminal outcome,
// and settling storage under it would report an unwinding change as resolved.
func TestMarkApplyCancelledRefusesRevertPhase(t *testing.T) {
	for _, applyState := range []string{state.Apply.RevertWindow, state.Apply.Reverting, state.Apply.SkippingRevert} {
		t.Run(applyState, func(t *testing.T) {
			apply := &storage.Apply{
				ID:              42,
				ApplyIdentifier: "apply-revert-phase-guard",
				State:           applyState,
			}
			client := newVitessControlTestClient(apply, nil, nil, &controlCaptureEngine{})

			err := client.markApplyCancelled(t.Context(), apply.ID)

			require.Error(t, err)
			assert.ErrorContains(t, err, apply.ApplyIdentifier)
			assert.Equal(t, applyState, apply.State, "revert-phase apply state must be preserved")
			assert.Nil(t, apply.CompletedAt)
		})
	}
}

// suppressParentApplyWrites engages only for an operation-lease-only drive (a
// multi-operation fan-out): the parent applies row is owned by the operator's
// projection CAS, so the drive must not write it. A drive carrying the parent
// apply lease (single-operation or whole-apply) writes the parent directly.
func TestSuppressParentApplyWrites(t *testing.T) {
	applyLease := storage.ApplyLease{ApplyID: 1, Owner: "d", Token: "t"}
	opLease := storage.OperationLease{ApplyID: 1, OperationID: 2, Owner: "d", Token: "t"}

	t.Run("operation lease only suppresses", func(t *testing.T) {
		ctx := storage.WithOperationLease(t.Context(), opLease)
		assert.True(t, suppressParentApplyWrites(ctx))
	})
	t.Run("apply lease writes the parent directly", func(t *testing.T) {
		ctx := storage.WithApplyLease(t.Context(), applyLease)
		assert.False(t, suppressParentApplyWrites(ctx))
	})
	t.Run("dual lease writes the parent directly", func(t *testing.T) {
		ctx := storage.WithOperationLease(storage.WithApplyLease(t.Context(), applyLease), opLease)
		assert.False(t, suppressParentApplyWrites(ctx))
	})
	t.Run("no lease does not suppress", func(t *testing.T) {
		assert.False(t, suppressParentApplyWrites(t.Context()))
	})
	t.Run("invalid operation lease does not suppress", func(t *testing.T) {
		ctx := storage.WithOperationLease(t.Context(), storage.OperationLease{})
		assert.False(t, suppressParentApplyWrites(ctx))
	})
}

// controlTestFailingTaskStore serves tasks like controlTestTaskStore but
// refuses every task write, simulating storage unavailability at the moment a
// task state transition must be persisted.
type controlTestFailingTaskStore struct {
	controlTestTaskStore
	updateErr error
}

func (s *controlTestFailingTaskStore) Update(context.Context, *storage.Task) error {
	return s.updateErr
}

// applyAcceptingEngine accepts any apply request, standing in for an engine
// that has already started the schema change when a later storage write fails.
type applyAcceptingEngine struct {
	controlCaptureEngine
}

func (e *applyAcceptingEngine) Apply(context.Context, *engine.ApplyRequest) (*engine.ApplyResult, error) {
	return &engine.ApplyResult{Accepted: true}, nil
}

func newFailingTaskWriteClient(apply *storage.Apply, tasks []*storage.Task, eng engine.Engine, updateErr error) *LocalClient {
	client := &LocalClient{
		config: LocalConfig{
			Database:  "testdb",
			Type:      apply.DatabaseType,
			TargetDSN: "root@tcp(localhost:3306)/",
		},
		storage: &controlTestStorage{
			applies: &controlTestApplyStore{apply: apply},
			tasks: &controlTestFailingTaskStore{
				controlTestTaskStore: controlTestTaskStore{tasks: tasks},
				updateErr:            updateErr,
			},
			applyLogs:       &controlTestApplyLogStore{},
			controlRequests: &testControlRequestStore{},
		},
		logger: slog.Default(),
	}
	if apply.DatabaseType == storage.DatabaseTypeVitess {
		client.planetscaleEngine = eng
	} else {
		client.spiritEngine = eng
	}
	return client
}

// Stop reports success to the operator only once storage reflects it. When the
// stopped task state cannot be persisted, the stop must fail so the operator
// retries, rather than report a stop that left task rows active in storage.
func TestLocalClient_StopFailsClosedWhenTaskWriteFails(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-mysql-stop-task-write",
		State:           state.Apply.Running,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
	}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-mysql-stop-task-write",
		Database:       "testdb",
		Namespace:      "testdb",
		State:          state.Task.Running,
	}
	updateErr := errors.New("storage write refused")
	client := newFailingTaskWriteClient(apply, []*storage.Task{task}, &controlCaptureEngine{}, updateErr)

	_, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.ErrorIs(t, err, updateErr)
	assert.ErrorContains(t, err, "persist stopped task state during stop")
	assert.Equal(t, state.Apply.Running, apply.State, "stored apply must stay running when the stop could not be persisted")
	assert.Nil(t, apply.CompletedAt)
}

// Cancel reports success to the operator only once storage reflects it. When
// the cancelled task state cannot be persisted, the cancel must fail so the
// operator retries, rather than report a cancel that left task rows active in
// storage.
func TestLocalClient_CancelFailsClosedWhenTaskWriteFails(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-mysql-cancel-task-write",
		State:           state.Apply.Stopped,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
	}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-mysql-cancel-task-write",
		Database:       "testdb",
		Namespace:      "testdb",
		State:          state.Task.Stopped,
	}
	updateErr := errors.New("storage write refused")
	client := newFailingTaskWriteClient(apply, []*storage.Task{task}, &controlCaptureEngine{}, updateErr)

	_, err := client.cancelOwnedApply(t.Context(), &ternv1.CancelRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.ErrorIs(t, err, updateErr)
	assert.ErrorContains(t, err, "persist cancelled task state during cancel")
	assert.Equal(t, state.Apply.Stopped, apply.State, "stored apply must stay stopped when the cancel could not be persisted")
}

// A sequential drive must not keep polling engine work against task rows that
// storage never accepted. When the engine accepts a task but the running-state
// write fails, the current apply owner exits (taskAbort) so operator recovery
// re-drives from the authoritative stored state instead of in-memory state a
// crash would lose.
func TestLocalClient_RunEngineTaskAbortsWhenTaskWriteFails(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-mysql-run-task-write",
		State:           state.Apply.Running,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
	}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-mysql-run-task-write",
		Database:       "testdb",
		Namespace:      "testdb",
		TableName:      "t1",
		DDL:            "ALTER TABLE `t1` ADD COLUMN `c` INT",
		State:          state.Task.Pending,
	}
	updateErr := errors.New("storage write refused")
	client := newFailingTaskWriteClient(apply, []*storage.Task{task}, &applyAcceptingEngine{}, updateErr)

	action := client.runEngineTask(t.Context(), apply, task, nil, nil)

	assert.Equal(t, taskAbort, action, "the drive must exit for operator retry when the running task state cannot be persisted")
}

// A failed task-state write must leave the in-memory task reflecting the
// stored state. Callers log and branch on task.State after a failed write
// (conflict checks, finalization loops), so a struct claiming a transition
// storage never accepted would mislead both the code and the operator reading
// its logs.
func TestLocalClient_TransitionTaskStateRestoresTaskOnWriteFailure(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-mysql-transition-task-write",
		State:           state.Apply.Running,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
	}
	updatedAt := time.Now().Add(-time.Minute)
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-mysql-transition-task-write",
		Database:       "testdb",
		Namespace:      "testdb",
		State:          state.Task.Running,
		UpdatedAt:      updatedAt,
	}
	updateErr := errors.New("storage write refused")
	client := newFailingTaskWriteClient(apply, []*storage.Task{task}, &controlCaptureEngine{}, updateErr)

	err := client.transitionTaskState(t.Context(), task, 0, state.Task.Stopped, "")

	require.ErrorIs(t, err, updateErr)
	assert.Equal(t, state.Task.Running, task.State, "in-memory task state must match the stored state after a refused write")
	assert.Equal(t, updatedAt, task.UpdatedAt, "in-memory update time must match the stored row after a refused write")
}

// Once the engine has accepted a grouped apply, its schema change is in
// flight, so a failed running task-state write must not record a failed
// outcome — the engine may still complete the change on the target. The
// current apply owner exits and the apply stays non-terminal so recovery
// re-drives it from stored state.
func TestLocalClient_GroupedApplyExitsWithoutTerminalizingWhenTaskWriteFails(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-mysql-grouped-task-write",
		State:           state.Apply.Pending,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
	}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-mysql-grouped-task-write",
		Database:       "testdb",
		Namespace:      "testdb",
		TableName:      "t1",
		DDL:            "ALTER TABLE `t1` ADD COLUMN `c` INT",
		State:          state.Task.Pending,
	}
	plan := &storage.Plan{
		PlanIdentifier: "plan-mysql-grouped-task-write",
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Namespaces:     map[string]*storage.NamespacePlanData{"testdb": {}},
	}
	updateErr := errors.New("storage write refused")
	client := newFailingTaskWriteClient(apply, []*storage.Task{task}, &applyAcceptingEngine{}, updateErr)
	client.heartbeatInterval = time.Hour

	client.executeGroupedApply(t.Context(), apply, []*storage.Task{task}, plan, nil, false)

	assert.Equal(t, state.Apply.Running, apply.State, "apply must stay non-terminal so recovery re-drives the in-flight engine work")
	assert.Nil(t, apply.CompletedAt, "an apply that could not persist task state has no final outcome to stamp")
}

// Consuming a pending stop control request binds the apply's identity to the
// consumption logger, so the settle line carries apply_id, repo, and pr — the
// attrs an operator filters on to correlate a stop with its PR — without the
// call site hand-listing them.
func TestProcessPendingStopControlRequest_LogsCarryApplyIdentity(t *testing.T) {
	apply := &storage.Apply{
		ID: 9, ApplyIdentifier: "apply-stop-identity",
		Database: "cdb_resolute", DatabaseType: storage.DatabaseTypeStrata,
		Environment: "staging", State: state.Apply.Completed,
		Repository: "org/repo", PullRequest: 123,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID: apply.ID, Operation: storage.ControlOperationStop,
		Status: storage.ControlRequestPending, RequestedBy: "operator",
	}}}
	var records []capturedLog
	client := &LocalClient{
		storage: &mockStorage{
			applies:         &mockApplyStore{apply: apply},
			controlRequests: controlRequests,
		},
		logger: slog.New(captureHandler{records: &records}),
	}

	handled, err := client.processPendingStopControlRequest(t.Context(), apply)
	require.NoError(t, err)
	assert.True(t, handled)

	line := requireCapturedLog(t, records, "completing pending stop request for resolved apply")
	assert.Equal(t, "apply-stop-identity", line.attrs["apply_id"])
	assert.Equal(t, "cdb_resolute", line.attrs["database"])
	assert.Equal(t, storage.DatabaseTypeStrata, line.attrs["database_type"])
	assert.Equal(t, "staging", line.attrs["environment"])
	assert.Equal(t, "org/repo", line.attrs["repo"])
	assert.Equal(t, int64(123), line.attrs["pr"])
	assert.Equal(t, "operator", line.attrs["requested_by"])
	assert.Equal(t, state.Apply.Completed, line.attrs["state"],
		"the settle line must snapshot the state current at consumption time")

	settled, err := controlRequests.GetByOperation(t.Context(), apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, storage.ControlRequestCompleted, settled.Status)
}
