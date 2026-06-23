package tern

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	progressReq *engine.ProgressRequest
	stopErr     error
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

	_, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.ErrorIs(t, err, stopErr)
	require.NotNil(t, eng.stopReq, "stop should call the engine with deploy metadata")
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
// Stop must cancel that deploy request via the engine before recording the
// cancellation, otherwise the deploy stays startable from the PlanetScale UI
// while SchemaBot reports it cancelled.
func TestLocalClient_StopWaitingForDeployCancelsDeployRequest(t *testing.T) {
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
	stateAtEngineStop := ""
	eng.onStop = func() { stateAtEngineStop = task.State }
	client := newVitessControlTestClient(apply, []*storage.Task{task}, resumeState, eng)

	resp, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	require.NotNil(t, eng.stopReq, "stop must cancel the deploy request for a waiting-for-deploy task")
	require.NotNil(t, eng.stopReq.ResumeState)
	assert.Equal(t, resumeData.MigrationContext, eng.stopReq.ResumeState.MigrationContext)
	assert.Equal(t, state.Task.WaitingForDeploy, stateAtEngineStop, "deploy request must be cancelled before the task is marked cancelled")
	assert.Equal(t, state.Task.Cancelled, task.State)
	assert.Equal(t, state.Apply.Cancelled, apply.State)
}

// A PlanetScale failed_retryable task paused after a transient failure (e.g.
// repeated progress-poll errors) still has its created, startable deploy request
// and persisted resume state. Stop is a terminal operator action, so it must
// cancel that deploy request via the engine before recording the cancellation —
// otherwise the deploy stays startable from the PlanetScale UI while SchemaBot
// reports it cancelled.
func TestLocalClient_StopFailedRetryableCancelsDeployRequest(t *testing.T) {
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
	stateAtEngineStop := ""
	eng.onStop = func() { stateAtEngineStop = task.State }
	client := newVitessControlTestClient(apply, []*storage.Task{task}, resumeState, eng)

	resp, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	require.NotNil(t, eng.stopReq, "stop must cancel the deploy request for a failed_retryable task")
	require.NotNil(t, eng.stopReq.ResumeState)
	assert.Equal(t, resumeData.MigrationContext, eng.stopReq.ResumeState.MigrationContext)
	assert.Equal(t, state.Task.FailedRetryable, stateAtEngineStop, "deploy request must be cancelled before the task is marked cancelled")
	assert.Equal(t, state.Task.Cancelled, task.State)
	assert.Equal(t, state.Apply.Cancelled, apply.State)
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
	assert.ErrorContains(t, err, "schema change apply-vitess-revert-window is in the revert window and has already been applied")
	assert.ErrorContains(t, err, "use revert to undo it or skip-revert to finalize it")
	assert.NotContains(t, err.Error(), task.TaskIdentifier, "rejection must name the apply identifier, not the per-table task id")
	assert.Nil(t, eng.stopReq, "stop must not touch the engine for a revert-window task")
	assert.Equal(t, state.Task.RevertWindow, task.State, "revert-window task must not be marked cancelled by stop")
	assert.Equal(t, state.Apply.RevertWindow, apply.State, "revert-window apply must not be marked cancelled by stop")
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
