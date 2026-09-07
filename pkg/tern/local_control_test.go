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
	"github.com/block/schemabot/pkg/engine/postgres"
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
	data       *storage.EngineResumeState
	err        error
	operations []*storage.ApplyOperation
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

func (s *controlTestApplyOperationStore) ListByApply(context.Context, int64) ([]*storage.ApplyOperation, error) {
	return s.operations, nil
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
	// progressResult is returned from Progress when set; a zero ProgressResult
	// is returned otherwise.
	progressResult *engine.ProgressResult
	progressErr    error
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
	if e.progressErr != nil {
		return nil, e.progressErr
	}
	if e.progressResult != nil {
		return e.progressResult, nil
	}
	return &engine.ProgressResult{}, nil
}

// externallyAuthoritativeCaptureEngine mimics an engine whose Progress relays
// the provider's record of the change rather than in-process state, so the
// live-work probe holds it to provider-terminal answers.
type externallyAuthoritativeCaptureEngine struct {
	controlCaptureEngine
}

func (e *externallyAuthoritativeCaptureEngine) ProgressIsExternallyAuthoritative() bool {
	return true
}

// controlValidatingCaptureEngine mimics an engine that addresses remote work:
// its control operations cannot proceed without persisted resume state, so its
// validator gate rejects a control request that carries none.
type controlValidatingCaptureEngine struct {
	controlCaptureEngine
}

func (e *controlValidatingCaptureEngine) ValidateControlResumeState(_ engine.ControlOperation, resumeState *engine.ResumeState) error {
	if resumeState == nil || resumeState.Metadata == "" {
		return errors.New("no active schema change")
	}
	return nil
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
// An engine that addresses remote work (a deploy request) cannot deliver a
// control operation without persisted resume state. Its validator gate must
// reject the operation before the engine is invoked when no resume state was
// ever persisted for the task's apply operation.
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
	eng := &controlValidatingCaptureEngine{}
	client := newVitessControlTestClient(apply, []*storage.Task{task}, nil, eng)

	_, err := client.cutover(t.Context(), &ternv1.CutoverRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.ErrorContains(t, err, "no active schema change")
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

// newCustomTypeControlTestClient builds a client for a database type without a
// built-in engine (registered through an engine factory in production), wired
// to the given capture engine and optional persisted resume state.
func newCustomTypeControlTestClient(apply *storage.Apply, tasks []*storage.Task, resumeState *storage.EngineResumeState, eng engine.Engine) *LocalClient {
	return &LocalClient{
		config: LocalConfig{
			Database: "testdb",
			Type:     storage.DatabaseTypeStrata,
		},
		storage: &controlTestStorage{
			applies:         &controlTestApplyStore{apply: apply},
			tasks:           &controlTestTaskStore{tasks: tasks},
			applyLogs:       &controlTestApplyLogStore{},
			applyOperations: &controlTestApplyOperationStore{data: resumeState},
			controlRequests: &testControlRequestStore{},
		},
		customEngine: eng,
		logger:       slog.Default(),
	}
}

// A database type without a built-in engine can address remote work through
// persisted resume state, exactly like Vitess does. The control request built
// for its cancel must carry the stored resume state so the engine can reach
// the work it is asked to terminate.
func TestLocalClient_CancelCarriesStoredResumeStateForCustomEngineType(t *testing.T) {
	operationID := int64(99)
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-custom-cancel",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:               7,
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TaskIdentifier:   "task-custom-cancel",
		State:            state.Task.Running,
	}
	resumeState := &storage.EngineResumeState{
		ApplyOperationID: operationID,
		MigrationContext: "ctx-custom-cancel",
		Metadata:         `{"change_id":"321"}`,
	}
	eng := &controlValidatingCaptureEngine{}
	client := newCustomTypeControlTestClient(apply, []*storage.Task{task}, resumeState, eng)

	resp, err := client.cancelOwnedApply(t.Context(), &ternv1.CancelRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	require.NotNil(t, eng.cancelReq, "cancel must reach the engine for a task with live engine work")
	require.NotNil(t, eng.cancelReq.ResumeState, "the cancel request must carry the stored resume state")
	assert.Equal(t, "ctx-custom-cancel", eng.cancelReq.ResumeState.MigrationContext)
	assert.JSONEq(t, `{"change_id":"321"}`, eng.cancelReq.ResumeState.Metadata)
	assert.Equal(t, state.Task.Cancelled, task.State)
	assert.Equal(t, state.Apply.Cancelled, apply.State)
}

// A cancel that reaches an engine with no live work for the task has nothing
// left to stop: the engine cancel error must not surface, and the durable
// cancel completes by terminalizing the task and apply. Surfacing the error
// would abort the drive and re-run the same failing cancel on every claim.
func TestLocalClient_CancelCompletesWhenEngineHasNoLiveWork(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-mysql-cancel-no-live-work",
		State:           state.Apply.Running,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
	}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-mysql-cancel-no-live-work",
		Database:       "testdb",
		Namespace:      "testdb",
		State:          state.Task.Running,
	}
	eng := &controlCaptureEngine{
		cancelErr:      engine.NewPermanentError("no active schema change to cancel"),
		progressResult: &engine.ProgressResult{State: engine.StatePending, Message: "No active schema change"},
	}
	client := newMySQLControlTestClient(apply, []*storage.Task{task}, eng)

	resp, err := client.cancelOwnedApply(t.Context(), &ternv1.CancelRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(1), resp.CancelledCount)
	require.NotNil(t, eng.cancelReq, "cancel must attempt the engine before deciding it has no live work")
	require.NotNil(t, eng.progressReq, "the live-work probe must ask the engine before completing the cancel")
	assert.Equal(t, state.Task.Cancelled, task.State)
	assert.Equal(t, state.Apply.Cancelled, apply.State)
	assert.NotNil(t, apply.CompletedAt)
}

// A cancel the engine refuses while it still has live work must surface: the
// destructive direction for cancel is leaving the work running past a recorded
// cancel, so storage must stay active until the engine work is actually down.
func TestLocalClient_CancelSurfacesErrorWhenEngineHasLiveWork(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-mysql-cancel-live-work",
		State:           state.Apply.Running,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
	}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-mysql-cancel-live-work",
		Database:       "testdb",
		Namespace:      "testdb",
		State:          state.Task.Running,
	}
	cancelErr := errors.New("cancel refused")
	eng := &controlCaptureEngine{
		cancelErr:      cancelErr,
		progressResult: &engine.ProgressResult{State: engine.StateRunning},
	}
	client := newMySQLControlTestClient(apply, []*storage.Task{task}, eng)

	_, err := client.cancelOwnedApply(t.Context(), &ternv1.CancelRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.ErrorIs(t, err, cancelErr)
	require.NotNil(t, eng.progressReq, "the live-work probe must run before the cancel error surfaces")
	assert.Equal(t, state.Task.Running, task.State, "a refused cancel over live work must not terminalize the task")
	assert.Equal(t, state.Apply.Running, apply.State)
}

// When the live-work probe itself fails, the engine's state is unknown:
// completing the cancel could record running work as cancelled. The original
// cancel error surfaces so the drive retries with engine state intact.
func TestLocalClient_CancelSurfacesErrorWhenLiveWorkProbeFails(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-mysql-cancel-probe-error",
		State:           state.Apply.Running,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
	}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-mysql-cancel-probe-error",
		Database:       "testdb",
		Namespace:      "testdb",
		State:          state.Task.Running,
	}
	cancelErr := errors.New("cancel refused")
	eng := &controlCaptureEngine{
		cancelErr:   cancelErr,
		progressErr: errors.New("progress unavailable"),
	}
	client := newMySQLControlTestClient(apply, []*storage.Task{task}, eng)

	_, err := client.cancelOwnedApply(t.Context(), &ternv1.CancelRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.ErrorIs(t, err, cancelErr, "the original cancel error must surface, not the probe error")
	assert.Equal(t, state.Task.Running, task.State, "an uncertain probe must never terminalize the task")
	assert.Equal(t, state.Apply.Running, apply.State)
}

// An externally authoritative engine reporting pending has an open change the
// provider can still run — for PlanetScale, a deploy request awaiting
// validation or deploy. A failed cancel must surface rather than settle, or
// the change could land on the target after storage recorded it cancelled.
func TestLocalClient_CancelSurfacesErrorWhenProviderChangeStillOpen(t *testing.T) {
	operationID := int64(99)
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-vitess-cancel-open-change",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:               7,
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TaskIdentifier:   "task-vitess-cancel-open-change",
		State:            state.Task.Running,
	}
	resumeState := &storage.EngineResumeState{
		ApplyOperationID: operationID,
		MigrationContext: "ctx-vitess-cancel",
		Metadata:         `{"deploy_request_id":321}`,
	}
	cancelErr := errors.New("cancel rejected: deploy request state pending")
	eng := &externallyAuthoritativeCaptureEngine{controlCaptureEngine: controlCaptureEngine{
		cancelErr:      cancelErr,
		progressResult: &engine.ProgressResult{State: engine.StatePending},
	}}
	client := newVitessControlTestClient(apply, []*storage.Task{task}, resumeState, eng)

	_, err := client.cancelOwnedApply(t.Context(), &ternv1.CancelRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.ErrorIs(t, err, cancelErr)
	assert.Equal(t, state.Task.Running, task.State, "an open provider change must never be recorded cancelled")
	assert.Equal(t, state.Apply.Running, apply.State)
}

// When the provider's own record says the change is closed as cancelled, a
// failed cancel call has nothing left to kill: the durable cancel completes by
// terminalizing the task and apply instead of re-running a guaranteed-failing
// cancel on every claim.
func TestLocalClient_CancelCompletesWhenProviderClosedTheChange(t *testing.T) {
	operationID := int64(99)
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-vitess-cancel-closed-change",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:               7,
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TaskIdentifier:   "task-vitess-cancel-closed-change",
		State:            state.Task.Running,
	}
	resumeState := &storage.EngineResumeState{
		ApplyOperationID: operationID,
		MigrationContext: "ctx-vitess-cancel",
		Metadata:         `{"deploy_request_id":321}`,
	}
	eng := &externallyAuthoritativeCaptureEngine{controlCaptureEngine: controlCaptureEngine{
		cancelErr:      errors.New("cancel rejected: deploy request already cancelled"),
		progressResult: &engine.ProgressResult{State: engine.StateCancelled},
	}}
	client := newVitessControlTestClient(apply, []*storage.Task{task}, resumeState, eng)

	resp, err := client.cancelOwnedApply(t.Context(), &ternv1.CancelRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	assert.Equal(t, state.Task.Cancelled, task.State)
	assert.Equal(t, state.Apply.Cancelled, apply.State)
}

// An instance-local engine that still tracks the change after a failed cancel
// — even in a terminal state — holds engine-owned work such as target cleanup
// that only a retried cancel finishes. The cancel error must surface so the
// next claim retries instead of settling over the unfinished cleanup.
func TestLocalClient_CancelSurfacesErrorWhenEngineStillTracksCancelCleanup(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-mysql-cancel-cleanup-pending",
		State:           state.Apply.Running,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
	}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-mysql-cancel-cleanup-pending",
		Database:       "testdb",
		Namespace:      "testdb",
		State:          state.Task.Running,
	}
	cancelErr := errors.New("drop cancelled schema change artifacts: connection refused")
	eng := &controlCaptureEngine{
		cancelErr:      cancelErr,
		progressResult: &engine.ProgressResult{State: engine.StateCancelled},
	}
	client := newMySQLControlTestClient(apply, []*storage.Task{task}, eng)

	_, err := client.cancelOwnedApply(t.Context(), &ternv1.CancelRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.ErrorIs(t, err, cancelErr)
	assert.Equal(t, state.Task.Running, task.State, "tracked cleanup work must keep the durable cancel pending for retry")
	assert.Equal(t, state.Apply.Running, apply.State)
}

// An engine that addresses remote work fails closed when no resume state was
// ever persisted for the task's apply operation: the real PlanetScale engine's
// validator gate must reject the cancel before the engine is invoked, so a
// deploy request is never guessed at.
func TestLocalClient_CancelRequiresEngineResumeState(t *testing.T) {
	operationID := int64(99)
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-vitess-cancel-no-resume-state",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:               7,
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TaskIdentifier:   "task-vitess-cancel-no-resume-state",
		State:            state.Task.Running,
	}
	eng := planetscale.New(slog.Default())
	client := newVitessControlTestClient(apply, []*storage.Task{task}, nil, eng)

	_, err := client.cancelOwnedApply(t.Context(), &ternv1.CancelRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.ErrorContains(t, err, "validate cancel resume state for task task-vitess-cancel-no-resume-state",
		"the validator gate must reject before the engine is invoked")
	require.ErrorContains(t, err, "no active schema change")
	assert.Equal(t, state.Task.Running, task.State, "a rejected cancel must not terminalize the task")
	assert.Equal(t, state.Apply.Running, apply.State)
}

// The live-work predicate reads the engine's own report: only an affirmative
// nothing-left-to-cancel answer clears the probe, and every uncertain or
// active shape counts as live work so a cancel never completes over work that
// can still run. The bar depends on who owns the engine's truth: an
// instance-local engine clears only on its idle sentinel, while an externally
// authoritative engine clears only on a provider answer that closed the
// change.
func TestEngineProgressShowsLiveWork(t *testing.T) {
	local := &controlCaptureEngine{}
	authoritative := &externallyAuthoritativeCaptureEngine{}

	assert.True(t, engineProgressShowsLiveWork(local, nil), "a missing probe result is uncertainty, which counts as live work")
	assert.True(t, engineProgressShowsLiveWork(authoritative, nil), "a missing probe result is uncertainty, which counts as live work")

	t.Run("instance-local engine", func(t *testing.T) {
		assert.False(t, engineProgressShowsLiveWork(local, &engine.ProgressResult{State: engine.StatePending}),
			"pending is the idle sentinel: the engine tracks nothing for the target")

		liveWork := []engine.State{
			engine.StateRunning,
			engine.StateWaitingForDeploy,
			engine.StateWaitingForCutover,
			engine.StateCuttingOver,
			engine.StateRevertWindow,
			engine.StateReverting,
			engine.StateCompleted,
			engine.StateReverted,
			engine.StateStopped,
			engine.StateCancelled,
			engine.StateFailed,
			engine.State(""),
		}
		for _, s := range liveWork {
			assert.True(t, engineProgressShowsLiveWork(local, &engine.ProgressResult{State: s}),
				"a tracked state %q still ends with engine-owned work a retried cancel must finish", s)
		}
	})

	t.Run("externally authoritative engine", func(t *testing.T) {
		assert.False(t, engineProgressShowsLiveWork(authoritative, &engine.ProgressResult{State: engine.StateCancelled}),
			"a provider-cancelled change is closed")
		assert.False(t, engineProgressShowsLiveWork(authoritative, &engine.ProgressResult{State: engine.StateFailed}),
			"a provider failure with no retry path is closed")
		assert.True(t, engineProgressShowsLiveWork(authoritative, &engine.ProgressResult{State: engine.StateFailed, Retryable: true}),
			"a retryable provider failure remains runnable")

		liveWork := []engine.State{
			engine.StatePending,
			engine.StateRunning,
			engine.StateWaitingForDeploy,
			engine.StateWaitingForCutover,
			engine.StateCuttingOver,
			engine.StateRevertWindow,
			engine.StateReverting,
			engine.StateCompleted,
			engine.StateReverted,
			engine.StateStopped,
			engine.State(""),
		}
		for _, s := range liveWork {
			assert.True(t, engineProgressShowsLiveWork(authoritative, &engine.ProgressResult{State: s}),
				"state %q is an open or unwinding provider change and must keep the cancel error surfacing", s)
		}
	})
}

// A stop that reaches an engine with no live work for the task has nothing
// left to pause — after a driver crash and restart, the engine tracks nothing
// in memory while the persisted checkpoint already holds the resume point. The
// engine stop error must not surface: the durable stop completes by recording
// the tasks and apply stopped (resumable, no completion time), so a later
// start resumes from the checkpoint. Surfacing the error would abort the drive
// and re-run the same failing stop on every claim.
func TestLocalClient_StopCompletesWhenEngineHasNoLiveWork(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-mysql-stop-no-live-work",
		State:           state.Apply.Running,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
	}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-mysql-stop-no-live-work",
		Database:       "testdb",
		Namespace:      "testdb",
		State:          state.Task.Running,
	}
	eng := &controlCaptureEngine{
		stopErr:        engine.NewPermanentError("no active schema change to stop"),
		progressResult: &engine.ProgressResult{State: engine.StatePending, Message: "No active schema change"},
	}
	client := newMySQLControlTestClient(apply, []*storage.Task{task}, eng)

	resp, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(1), resp.StoppedCount)
	require.NotNil(t, eng.stopReq, "stop must attempt the engine before deciding it has no live work")
	require.NotNil(t, eng.progressReq, "the live-work probe must ask the engine before completing the stop")
	assert.Equal(t, state.Task.Stopped, task.State)
	assert.Equal(t, state.Apply.Stopped, apply.State)
	assert.Nil(t, apply.CompletedAt, "a stopped apply is resumable and must not carry a completion time")
}

// A stop the engine refuses while it still has live work must surface: a
// stopped record over an engine still copying rows would let the change keep
// running unwatched, so storage must stay active until the engine work is
// actually paused.
func TestLocalClient_StopSurfacesErrorWhenEngineHasLiveWork(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-mysql-stop-live-work",
		State:           state.Apply.Running,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
	}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-mysql-stop-live-work",
		Database:       "testdb",
		Namespace:      "testdb",
		State:          state.Task.Running,
	}
	stopErr := errors.New("stop refused")
	eng := &controlCaptureEngine{
		stopErr:        stopErr,
		progressResult: &engine.ProgressResult{State: engine.StateRunning},
	}
	client := newMySQLControlTestClient(apply, []*storage.Task{task}, eng)

	_, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.ErrorIs(t, err, stopErr)
	require.NotNil(t, eng.progressReq, "the live-work probe must run before the stop error surfaces")
	assert.Equal(t, state.Task.Running, task.State, "a refused stop over live work must not record the task stopped")
	assert.Equal(t, state.Apply.Running, apply.State)
}

// When the live-work probe itself fails, the engine's state is unknown:
// completing the stop could record running work as stopped. The original stop
// error surfaces so the drive retries with engine state intact.
func TestLocalClient_StopSurfacesErrorWhenLiveWorkProbeFails(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-mysql-stop-probe-error",
		State:           state.Apply.Running,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
	}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-mysql-stop-probe-error",
		Database:       "testdb",
		Namespace:      "testdb",
		State:          state.Task.Running,
	}
	stopErr := errors.New("stop refused")
	eng := &controlCaptureEngine{
		stopErr:     stopErr,
		progressErr: errors.New("progress unavailable"),
	}
	client := newMySQLControlTestClient(apply, []*storage.Task{task}, eng)

	_, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.ErrorIs(t, err, stopErr, "the original stop error must surface, not the probe error")
	assert.Equal(t, state.Task.Running, task.State, "an uncertain probe must never record the task stopped")
	assert.Equal(t, state.Apply.Running, apply.State)
}

// An externally authoritative engine reporting pending has an open change the
// provider can still run. A failed stop must surface rather than settle, or
// the change could land on the target after storage recorded it stopped.
func TestLocalClient_StopSurfacesErrorWhenProviderChangeStillOpen(t *testing.T) {
	operationID := int64(99)
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-strata-stop-open-change",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:               7,
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TaskIdentifier:   "task-strata-stop-open-change",
		State:            state.Task.Running,
	}
	resumeState := &storage.EngineResumeState{
		ApplyOperationID: operationID,
		MigrationContext: "ctx-strata-stop",
		Metadata:         `{"change_id":"321"}`,
	}
	stopErr := errors.New("stop rejected: provider change state pending")
	eng := &externallyAuthoritativeCaptureEngine{controlCaptureEngine: controlCaptureEngine{
		stopErr:        stopErr,
		progressResult: &engine.ProgressResult{State: engine.StatePending},
	}}
	client := newCustomTypeControlTestClient(apply, []*storage.Task{task}, resumeState, eng)

	_, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.ErrorIs(t, err, stopErr)
	assert.Equal(t, state.Task.Running, task.State, "an open provider change must never be recorded stopped")
	assert.Equal(t, state.Apply.Running, apply.State)
}

// When the provider's own record says the change is closed, a failed stop call
// has nothing left to pause: the durable stop completes by recording the tasks
// and apply stopped instead of re-running a guaranteed-failing stop on every
// claim. Stopped stays the resumable operator verb — a later start re-plans
// against the target rather than resuming the closed provider change.
func TestLocalClient_StopCompletesWhenProviderClosedTheChange(t *testing.T) {
	operationID := int64(99)
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-strata-stop-closed-change",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:               7,
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TaskIdentifier:   "task-strata-stop-closed-change",
		State:            state.Task.Running,
	}
	resumeState := &storage.EngineResumeState{
		ApplyOperationID: operationID,
		MigrationContext: "ctx-strata-stop",
		Metadata:         `{"change_id":"321"}`,
	}
	eng := &externallyAuthoritativeCaptureEngine{controlCaptureEngine: controlCaptureEngine{
		stopErr:        errors.New("stop rejected: provider change already cancelled"),
		progressResult: &engine.ProgressResult{State: engine.StateCancelled},
	}}
	client := newCustomTypeControlTestClient(apply, []*storage.Task{task}, resumeState, eng)

	resp, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(1), resp.StoppedCount)
	require.NotNil(t, eng.progressReq, "the live-work probe must ask the engine before completing the stop")
	require.NotNil(t, eng.progressReq.ResumeState, "the probe must carry the persisted resume state to address the provider change")
	assert.Equal(t, "ctx-strata-stop", eng.progressReq.ResumeState.MigrationContext)
	assert.Equal(t, state.Task.Stopped, task.State)
	assert.Equal(t, state.Apply.Stopped, apply.State)
	assert.Nil(t, apply.CompletedAt, "a stopped apply is resumable and must not carry a completion time")
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

// newPostgresControlTestClient wires the real PostgreSQL engine so durable
// control tests exercise the engine's own typed declines end to end, not a
// fake standing in for them.
func newPostgresControlTestClient(apply *storage.Apply, tasks []*storage.Task, controlRequests *testControlRequestStore) *LocalClient {
	return &LocalClient{
		config: LocalConfig{
			Database:  "testdb",
			Type:      storage.DatabaseTypePostgres,
			TargetDSN: "postgres://localhost:5432/testdb",
		},
		storage: &controlTestStorage{
			applies:         &controlTestApplyStore{apply: apply},
			tasks:           &controlTestTaskStore{tasks: tasks},
			applyLogs:       &controlTestApplyLogStore{},
			applyOperations: &controlTestApplyOperationStore{},
			controlRequests: controlRequests,
		},
		postgresEngine: postgres.New(),
		logger:         slog.Default(),
	}
}

// A durable stop request against a running PostgreSQL apply is resolved as
// permanently failed: the engine declines stop as unsupported for its database
// type, and no retry can ever succeed — leaving the request pending would
// re-run the same rejection on every drive claim while the DDL keeps
// executing. The apply and its task keep their states so the running change
// settles through its own apply path.
func TestLocalClient_PendingStopResolvesPostgresUnsupportedDecline(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-postgres-pending-stop",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypePostgres,
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-postgres-pending-stop",
		State:          state.Task.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator",
	}}}
	client := newPostgresControlTestClient(apply, []*storage.Task{task}, controlRequests)

	handled, err := client.processPendingStopControlRequest(t.Context(), apply)

	require.NoError(t, err)
	assert.False(t, handled, "a declined stop must not read as an operator stop, or the drive loop would mark the running apply stopped")
	assert.Equal(t, state.Apply.Running, apply.State, "the running apply must be left untouched to settle on its own")
	assert.Equal(t, state.Task.Running, task.State, "the running task must not be marked stopped by a declined stop")
	pending, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	assert.Nil(t, pending, "the durable stop request must be resolved, not left pending")
	resolved, err := controlRequests.GetByOperation(t.Context(), apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	require.NotNil(t, resolved)
	assert.Equal(t, storage.ControlRequestFailed, resolved.Status, "an unsupported-operation decline is a permanent rejection")
	assert.NotEmpty(t, resolved.ErrorMessage, "the failed request must carry the engine's reason for the operator")

	handled, err = client.processPendingStopControlRequest(t.Context(), apply)
	require.NoError(t, err)
	assert.False(t, handled, "a resolved decline must not be re-consumed on the next drive claim")
}

// A durable cancel request against a running PostgreSQL apply is resolved as
// permanently failed for the same reason as stop: the engine's decline is
// deterministic, so failing the stored request terminally is the only way to
// end the operator-owned retry loop without misrepresenting the healthy
// running change.
func TestLocalClient_PendingCancelResolvesPostgresUnsupportedDecline(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-postgres-pending-cancel",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypePostgres,
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-postgres-pending-cancel",
		State:          state.Task.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator",
	}}}
	client := newPostgresControlTestClient(apply, []*storage.Task{task}, controlRequests)

	handled, err := client.processPendingCancelControlRequest(t.Context(), apply)

	require.NoError(t, err)
	assert.False(t, handled, "a declined cancel must not read as an operator cancel, or the drive loop would mark the running apply stopped")
	assert.Equal(t, state.Apply.Running, apply.State, "the running apply must be left untouched to settle on its own")
	assert.Equal(t, state.Task.Running, task.State, "the running task must not be marked cancelled by a declined cancel")
	pending, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.Nil(t, pending, "the durable cancel request must be resolved, not left pending")
	resolved, err := controlRequests.GetByOperation(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	require.NotNil(t, resolved)
	assert.Equal(t, storage.ControlRequestFailed, resolved.Status, "an unsupported-operation decline is a permanent rejection")
	assert.NotEmpty(t, resolved.ErrorMessage, "the failed request must carry the engine's reason for the operator")

	handled, err = client.processPendingCancelControlRequest(t.Context(), apply)
	require.NoError(t, err)
	assert.False(t, handled, "a resolved decline must not be re-consumed on the next drive claim")
}

// revertPhaseDeclineFixture stages an apply in its revert window with one
// pending request for the given revert-phase operation, and returns the client
// that drives it alongside the stored request the drive would consume.
func revertPhaseDeclineFixture(t *testing.T, operation storage.ControlOperation) (*LocalClient, *storage.Apply, *testControlRequestStore, *storage.ApplyControlRequest) {
	t.Helper()
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-postgres-" + string(operation),
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypePostgres,
		State:           state.Apply.RevertWindow,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   operation,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator",
	}}}
	client := newPostgresControlTestClient(apply, nil, controlRequests)
	pending, err := controlRequests.GetPending(t.Context(), apply.ID, operation)
	require.NoError(t, err)
	require.NotNil(t, pending)
	return client, apply, controlRequests, pending
}

// A durable revert request the engine declines for its whole database type is
// resolved as permanently failed. The decline is deterministic, so leaving the
// request pending would re-run the same rejection on every drive claim for as
// long as the revert window lasts. The window itself is untouched: the refusal
// declines the operator's command rather than carrying it out, so the schema
// change finishes the way it would have without the command, and the operator
// reads the engine's reason on the failed request.
func TestLocalClient_DeclinedRevertRequestResolvesTerminally(t *testing.T) {
	client, apply, controlRequests, pending := revertPhaseDeclineFixture(t, storage.ControlOperationRevert)

	_, revertErr := client.getEngine().Revert(t.Context(), nil)
	require.Error(t, revertErr, "the PostgreSQL engine declines revert for its whole database type")

	client.resolveOrRetryRevertPhaseRequest(t.Context(), client.logger, apply,
		storage.ControlOperationRevert, storage.LogEventRevertTriggered, pending, revertErr)

	stillPending, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationRevert)
	require.NoError(t, err)
	assert.Nil(t, stillPending, "the durable revert request must be resolved, not left for the next drive claim to retry")
	resolved, err := controlRequests.GetByOperation(t.Context(), apply.ID, storage.ControlOperationRevert)
	require.NoError(t, err)
	require.NotNil(t, resolved)
	assert.Equal(t, storage.ControlRequestFailed, resolved.Status, "an unsupported-operation decline is a permanent rejection")
	assert.Contains(t, resolved.ErrorMessage, "no revert window",
		"the failed request must carry the engine's reason, not a generic failure")
	assert.Equal(t, state.Apply.RevertWindow, apply.State, "a declined revert must leave the revert window running")
}

// A durable skip-revert request is resolved the same way and for the same
// reason: an engine with no revert window to close declines identically every
// time it is asked, so the request is failed with that reason instead of
// re-attempted until the window expires on its own.
func TestLocalClient_DeclinedSkipRevertRequestResolvesTerminally(t *testing.T) {
	client, apply, controlRequests, pending := revertPhaseDeclineFixture(t, storage.ControlOperationSkipRevert)

	_, skipErr := client.getEngine().SkipRevert(t.Context(), nil)
	require.Error(t, skipErr, "the PostgreSQL engine declines skip-revert for its whole database type")

	client.resolveOrRetryRevertPhaseRequest(t.Context(), client.logger, apply,
		storage.ControlOperationSkipRevert, storage.LogEventSkipRevertTriggered, pending, skipErr)

	stillPending, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationSkipRevert)
	require.NoError(t, err)
	assert.Nil(t, stillPending, "the durable skip-revert request must be resolved, not left for the next drive claim to retry")
	resolved, err := controlRequests.GetByOperation(t.Context(), apply.ID, storage.ControlOperationSkipRevert)
	require.NoError(t, err)
	require.NotNil(t, resolved)
	assert.Equal(t, storage.ControlRequestFailed, resolved.Status, "an unsupported-operation decline is a permanent rejection")
	assert.Contains(t, resolved.ErrorMessage, "no revert window",
		"the failed request must carry the engine's reason, not a generic failure")
	assert.Equal(t, state.Apply.RevertWindow, apply.State, "a declined skip-revert must leave the revert window running")
}

// A revert that fails for a reason a later attempt could survive stays pending.
// Only a decline the engine issues for its whole database type is permanent, so
// terminal resolution must not swallow the ordinary failures the operator-owned
// retry loop exists to ride out.
func TestLocalClient_RetryableRevertRequestFailureStaysPending(t *testing.T) {
	client, apply, controlRequests, pending := revertPhaseDeclineFixture(t, storage.ControlOperationRevert)

	client.resolveOrRetryRevertPhaseRequest(t.Context(), client.logger, apply,
		storage.ControlOperationRevert, storage.LogEventRevertTriggered, pending,
		errors.New("data plane is unreachable"))

	stillPending, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationRevert)
	require.NoError(t, err)
	require.NotNil(t, stillPending, "a retryable failure must leave the request for the next drive claim")
	assert.Equal(t, storage.ControlRequestPending, stillPending.Status, "a retryable failure is not a rejection")
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

// leaseLostTaskStore refuses every task write the way a lease-guarded UPDATE
// does once the operation lease has moved to a peer driver — the apply lease
// the control operation holds is a different lease, so the apply-level write
// that follows would still land.
type leaseLostTaskStore struct {
	*controlTestTaskStore
}

func (s *leaseLostTaskStore) Update(context.Context, *storage.Task) error {
	return storage.ErrApplyLeaseLost
}

// A stop holds the apply lease, but each task write is guarded by the operation
// lease, and driver churn can hand that one to a peer mid-stop. When the task
// writes are refused the stop must fail rather than settle the apply on its
// own: an apply recorded as stopped over task rows that still read as active
// work detaches the apply from its tasks, and the tasks left behind hold the
// database with neither start nor a later apply able to act on them. An
// operator re-issuing the stop is the recoverable outcome.
func TestLocalClient_StopFailsWhenTaskWritesAreRefused(t *testing.T) {
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-stop-refused",
		State:           state.Apply.Running,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
	}
	task := &storage.Task{
		ID:             7,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-stop-refused",
		Database:       "testdb",
		Namespace:      "testdb",
		State:          state.Task.Running,
	}
	client := newMySQLControlTestClient(apply, []*storage.Task{task}, &controlCaptureEngine{})
	stor := client.storage.(*controlTestStorage)
	stor.tasks = &leaseLostTaskStore{controlTestTaskStore: stor.tasks.(*controlTestTaskStore)}

	resp, err := client.stopOwnedApply(t.Context(), &ternv1.StopRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.Error(t, err, "a stop whose task writes were all refused must not report success")
	require.ErrorIs(t, err, storage.ErrApplyLeaseLost, "the refusal's cause must survive for triage")
	assert.Contains(t, err.Error(), "testdb")
	assert.Nil(t, resp)
	assert.Equal(t, state.Apply.Running, apply.State, "the apply must not be settled over task rows that never moved")
	assert.Equal(t, state.Task.Running, task.State)
}

// A cancel splits the same two leases as a stop, and an apply recorded as
// cancelled over task rows that never moved detaches it from its tasks the same
// way.
func TestLocalClient_CancelFailsWhenTaskWritesAreRefused(t *testing.T) {
	apply := &storage.Apply{
		ID:              43,
		ApplyIdentifier: "apply-cancel-refused",
		State:           state.Apply.Running,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
	}
	task := &storage.Task{
		ID:             8,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-cancel-refused",
		Database:       "testdb",
		Namespace:      "testdb",
		State:          state.Task.Running,
	}
	client := newMySQLControlTestClient(apply, []*storage.Task{task}, &controlCaptureEngine{})
	stor := client.storage.(*controlTestStorage)
	stor.tasks = &leaseLostTaskStore{controlTestTaskStore: stor.tasks.(*controlTestTaskStore)}

	resp, err := client.cancelOwnedApply(t.Context(), &ternv1.CancelRequest{ApplyId: apply.ApplyIdentifier}, "")

	require.Error(t, err, "a cancel whose task writes were all refused must not report success")
	require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
	assert.Nil(t, resp)
	assert.Equal(t, state.Apply.Running, apply.State)
	assert.Equal(t, state.Task.Running, task.State)
}

// A control operation reports what it persisted: a task write that lands is
// counted, one that is refused is not, because the counts become the
// operator-facing "N tasks stopped" event and response. Every task is still
// attempted, so a retry has only the refused ones left to write.
func TestLocalClient_ControlOperationCountsOnlyLandedTaskWrites(t *testing.T) {
	apply := &storage.Apply{
		ID:              44,
		ApplyIdentifier: "apply-partial-stop",
		State:           state.Apply.Running,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
	}
	landing := &storage.Task{
		ID: 9, ApplyID: apply.ID, TaskIdentifier: "task-lands",
		Database: "testdb", Namespace: "testdb", State: state.Task.Running,
	}
	refused := &storage.Task{
		ID: 10, ApplyID: apply.ID, TaskIdentifier: "task-refused",
		Database: "testdb", Namespace: "testdb", State: state.Task.Running,
	}
	alreadyDone := &storage.Task{
		ID: 11, ApplyID: apply.ID, TaskIdentifier: "task-done",
		Database: "testdb", Namespace: "testdb", State: state.Task.Completed,
	}
	client := newMySQLControlTestClient(apply, nil, &controlCaptureEngine{})
	stor := client.storage.(*controlTestStorage)
	stor.tasks = &selectivelyFailingTaskStore{
		controlTestTaskStore: &controlTestTaskStore{tasks: []*storage.Task{landing, refused, alreadyDone}},
		failIdentifier:       refused.TaskIdentifier,
	}

	marked, skipped, applyID, err := client.markTasksWithState(t.Context(),
		[]*storage.Task{landing, refused, alreadyDone}, apply.ID, StatementIndex[engine.TableProgress]{}, state.Task.Stopped)

	require.Error(t, err, "a refused task write must be reported, not absorbed into the count")
	assert.Contains(t, err.Error(), "failed to settle 1 of 2 tasks",
		"the count in the message is the failures, and it must read as such")
	assert.Equal(t, int64(1), marked, "only the landed write is counted")
	assert.Equal(t, int64(1), skipped, "the already-terminal task is skipped, not marked")
	assert.Equal(t, apply.ID, applyID)
	assert.Equal(t, state.Task.Stopped, landing.State, "every task is still attempted")
	assert.Equal(t, state.Task.Completed, alreadyDone.State)
}

// selectivelyFailingTaskStore refuses the write for one named task and serves
// the rest normally, standing in for a lease that moves partway through a
// control operation's task writes.
type selectivelyFailingTaskStore struct {
	*controlTestTaskStore
	failIdentifier string
}

func (s *selectivelyFailingTaskStore) Update(ctx context.Context, task *storage.Task) error {
	if task.TaskIdentifier == s.failIdentifier {
		return storage.ErrApplyLeaseLost
	}
	return s.controlTestTaskStore.Update(ctx, task)
}

// restingStartApply builds a stopped apply and its resting task, last written
// `age` ago, for exercising how start resolves resumable work.
func restingStartApply(taskState string, age time.Duration) (*storage.Apply, *storage.Task) {
	apply := &storage.Apply{
		ID:              90,
		ApplyIdentifier: "apply-resume-target",
		State:           state.Apply.Stopped,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
	}
	task := &storage.Task{
		ID:             91,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-resume-target",
		Database:       "testdb",
		Namespace:      "testdb",
		TableName:      "users",
		State:          taskState,
		UpdatedAt:      time.Now().Add(-age),
	}
	return apply, task
}

// The window that keeps an unqualified start from reaching back forever is a
// way of choosing among candidates, not a rule about what may be resumed. An
// operator who names the apply has already chosen, so the change resumes however
// long it has been resting — refusing it here reads as the change not existing.
func TestLocalClient_StartResumesANamedApplyOlderThanTheDiscoveryWindow(t *testing.T) {
	apply, task := restingStartApply(state.Task.Stopped, stoppedApplyDiscoveryWindow+48*time.Hour)
	client := newMySQLControlTestClient(apply, []*storage.Task{task}, &controlCaptureEngine{})

	resolved, startedCount, skippedCount, err := client.resolveStartRequest(t.Context(),
		&ternv1.StartRequest{ApplyId: apply.ApplyIdentifier})

	require.NoError(t, err, "a named apply resumes however long it has been resting")
	require.NotNil(t, resolved)
	assert.Equal(t, apply.ApplyIdentifier, resolved.ApplyIdentifier)
	assert.Equal(t, int64(1), startedCount)
	assert.Equal(t, int64(0), skippedCount)
}

// An unqualified start picks among candidates, so it does keep the window — but
// it must say that is why it passed over the change, and point at the command
// that resumes it anyway. Reporting it as "nothing to resume" sends the operator
// looking for a change that is sitting right there.
func TestLocalClient_StartOutsideTheDiscoveryWindowNamesTheWayForward(t *testing.T) {
	apply, task := restingStartApply(state.Task.Stopped, stoppedApplyDiscoveryWindow+48*time.Hour)
	client := newMySQLControlTestClient(apply, []*storage.Task{task}, &controlCaptureEngine{})

	_, _, _, err := client.resolveStartRequest(t.Context(), &ternv1.StartRequest{})

	require.Error(t, err, "an unqualified start passes over a change older than the window")
	assert.Contains(t, err.Error(), apply.ApplyIdentifier,
		"the refusal names the change it passed over, not just that nothing was found")
	assert.Contains(t, err.Error(), "re-issue the start naming",
		"the refusal points at the command that resumes the change anyway")
}

// Task states arrive in proto form for terminal states too, so a finished
// table must be counted as skipped rather than reported as a state start
// cannot act on — the latter tells the operator something is wedged when the
// change is simply done.
func TestLocalClient_StartSkipsProtoFormattedTerminalTasks(t *testing.T) {
	apply, task := restingStartApply("STATE_COMPLETED", time.Minute)
	client := newMySQLControlTestClient(apply, []*storage.Task{task}, &controlCaptureEngine{})

	_, _, _, err := client.resolveStartRequest(t.Context(),
		&ternv1.StartRequest{ApplyId: apply.ApplyIdentifier})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "already reached a terminal state",
		"a proto-formatted completed state is still a finished table")
	assert.NotContains(t, err.Error(), "state start cannot act on")
}

// A task that is neither terminal nor resting is not something a start can act
// on, and saying only "nothing to resume" hides which state it is actually in.
// The refusal names the states so an operator can tell a task awaiting its
// driver from one stranded under a terminal apply.
func TestLocalClient_StartNamesTheStatesItCannotResume(t *testing.T) {
	apply, task := restingStartApply(state.Task.FailedRetryable, time.Minute)
	client := newMySQLControlTestClient(apply, []*storage.Task{task}, &controlCaptureEngine{})

	_, _, _, err := client.resolveStartRequest(t.Context(),
		&ternv1.StartRequest{ApplyId: apply.ApplyIdentifier})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "state start cannot act on")
	assert.Contains(t, err.Error(), state.Task.FailedRetryable, "the refusal names the state it found")
	assert.Contains(t, err.Error(), apply.ApplyIdentifier)
}

// Task states arrive in proto form ("STATE_STOPPED") as well as canonical
// lowercase, so start must compare them normalized. A raw comparison silently
// finds no resumable work and refuses a change that is sitting ready.
func TestLocalClient_StartResolvesProtoFormattedTaskStates(t *testing.T) {
	apply, task := restingStartApply("STATE_STOPPED", time.Minute)
	client := newMySQLControlTestClient(apply, []*storage.Task{task}, &controlCaptureEngine{})

	resolved, startedCount, _, err := client.resolveStartRequest(t.Context(),
		&ternv1.StartRequest{ApplyId: apply.ApplyIdentifier})

	require.NoError(t, err, "a proto-formatted stopped state is still stopped")
	require.NotNil(t, resolved)
	assert.Equal(t, int64(1), startedCount)
}

// Tables that finished before the stop are skipped, not resumed, and the count
// is what tells the operator how much of the change is already done.
func TestLocalClient_StartSkipsTerminalTablesOfTheSameApply(t *testing.T) {
	apply, resting := restingStartApply(state.Task.Stopped, time.Minute)
	done := &storage.Task{
		ID: 92, ApplyID: apply.ID, TaskIdentifier: "task-already-done",
		Database: "testdb", Namespace: "testdb", TableName: "orders",
		State: state.Task.Completed, UpdatedAt: time.Now(),
	}
	otherApply := &storage.Task{
		ID: 93, ApplyID: 999, TaskIdentifier: "task-other-apply",
		Database: "testdb", Namespace: "testdb", TableName: "shipments",
		State: state.Task.Stopped, UpdatedAt: time.Now(),
	}
	client := newMySQLControlTestClient(apply, []*storage.Task{resting, done, otherApply}, &controlCaptureEngine{})

	_, startedCount, skippedCount, err := client.resolveStartRequest(t.Context(),
		&ternv1.StartRequest{ApplyId: apply.ApplyIdentifier})

	require.NoError(t, err)
	assert.Equal(t, int64(1), startedCount, "only the resting table is resumed")
	assert.Equal(t, int64(1), skippedCount, "the finished table is reported as skipped, not silently dropped")
}

// Every task of the apply already finished, so there is nothing to resume — but
// that is a completed change, not a missing one, and the refusal must say which.
func TestLocalClient_StartDistinguishesAFinishedApplyFromAMissingOne(t *testing.T) {
	apply, task := restingStartApply(state.Task.Completed, time.Minute)
	client := newMySQLControlTestClient(apply, []*storage.Task{task}, &controlCaptureEngine{})

	_, _, _, err := client.resolveStartRequest(t.Context(),
		&ternv1.StartRequest{ApplyId: apply.ApplyIdentifier})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "already reached a terminal state")
	assert.NotContains(t, err.Error(), "state start cannot act on")
}

// An engine that declines an operation for its whole database type has given a
// deterministic answer, and the RPC surface has to express it as a refusal. An
// error is the one shape that cannot cross the boundary: the gRPC server maps
// every error to a generic internal status, so the caller cannot tell a
// permanent decline from a transient failure and retries a request that can
// never succeed.
func TestLocalClientRevertWindowDeclinesAreRefusalsNotErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		decline error
		call    func(*LocalClient) (bool, string, error)
		reason  string
	}{
		{
			name:    "revert",
			decline: engine.NewUnsupportedOperationError("revert is not supported for these schema changes"),
			reason:  "revert is not supported",
			call: func(c *LocalClient) (bool, string, error) {
				resp, err := c.Revert(t.Context(), &ternv1.RevertRequest{Environment: "staging"})
				if resp == nil {
					return false, "", err
				}
				return resp.Accepted, resp.ErrorMessage, err
			},
		},
		{
			name:    "skip-revert",
			decline: engine.NewUnsupportedOperationError("skip-revert is not supported for these schema changes"),
			reason:  "skip-revert is not supported",
			call: func(c *LocalClient) (bool, string, error) {
				resp, err := c.SkipRevert(t.Context(), &ternv1.SkipRevertRequest{Environment: "staging"})
				if resp == nil {
					return false, "", err
				}
				return resp.Accepted, resp.ErrorMessage, err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newDecliningControlLocalClient(tc.decline)
			accepted, message, err := tc.call(client)
			require.NoError(t, err, "a deterministic decline must not surface as an error the caller would retry")
			assert.False(t, accepted, "the operation was declined")
			assert.Contains(t, message, tc.reason, "the refusal carries the engine's reason to the caller")
		})
	}
}

// newDecliningControlLocalClient builds a client whose engine declines the
// revert-window controls for its whole database type.
func newDecliningControlLocalClient(decline error) *LocalClient {
	apply := &storage.Apply{
		ID: 1, ApplyIdentifier: "apply-decline", Database: "testdb",
		Environment: "staging", State: state.Apply.RevertWindow,
	}
	task := &storage.Task{
		ID: 1, ApplyID: apply.ID, TaskIdentifier: "task-decline",
		Database: "testdb", Namespace: "testdb", TableName: "users", State: state.Task.RevertWindow,
	}
	return &LocalClient{
		config: LocalConfig{
			Database:  "testdb",
			Type:      storage.DatabaseTypeMySQL,
			TargetDSN: "root@tcp(localhost:3306)/",
		},
		storage: &controlTestStorage{
			applies:         &controlTestApplyStore{apply: apply},
			tasks:           &controlTestTaskStore{tasks: []*storage.Task{task}},
			applyLogs:       &controlTestApplyLogStore{},
			controlRequests: &testControlRequestStore{},
		},
		spiritEngine: &fakeControlEngine{revertErr: decline, skipRevertErr: decline},
		logger:       slog.Default(),
	}
}
