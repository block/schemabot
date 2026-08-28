package tern

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdoptableEngineTerminalStates(t *testing.T) {
	tests := []struct {
		name           string
		engineState    engine.State
		retryable      bool
		wantApplyState string
		wantTaskState  string
		wantOK         bool
	}{
		{
			name:           "completed adopts completed",
			engineState:    engine.StateCompleted,
			wantApplyState: state.Apply.Completed,
			wantTaskState:  state.Task.Completed,
			wantOK:         true,
		},
		{
			name:           "non-retryable failed adopts failed",
			engineState:    engine.StateFailed,
			wantApplyState: state.Apply.Failed,
			wantTaskState:  state.Task.Failed,
			wantOK:         true,
		},
		{
			name:        "retryable failed is not adoptable so the kill command is honored",
			engineState: engine.StateFailed,
			retryable:   true,
			wantOK:      false,
		},
		{
			name:           "cancelled adopts cancelled",
			engineState:    engine.StateCancelled,
			wantApplyState: state.Apply.Cancelled,
			wantTaskState:  state.Task.Cancelled,
			wantOK:         true,
		},
		{
			name:           "reverted adopts reverted",
			engineState:    engine.StateReverted,
			wantApplyState: state.Apply.Reverted,
			wantTaskState:  state.Task.Reverted,
			wantOK:         true,
		},
		{
			name:        "stopped is resumable so a pending cancel still has work to kill",
			engineState: engine.StateStopped,
			wantOK:      false,
		},
		{
			name:        "running is live work",
			engineState: engine.StateRunning,
			wantOK:      false,
		},
		{
			name:        "revert window is owned by the revert and skip-revert paths",
			engineState: engine.StateRevertWindow,
			wantOK:      false,
		},
		{
			name:        "pending is not terminal",
			engineState: engine.StatePending,
			wantOK:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			applyState, taskState, ok := adoptableEngineTerminalStates(tt.engineState, tt.retryable)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantApplyState, applyState)
			assert.Equal(t, tt.wantTaskState, taskState)
		})
	}
}

// reconcileFixture wires a LocalClient over the in-memory control fakes with
// one apply, one task, and any pending control requests, so each reconcile
// test only states what differs.
type reconcileFixture struct {
	client          *LocalClient
	apply           *storage.Apply
	task            *storage.Task
	engine          *fakeControlEngine
	storage         *exactProgressStorage
	controlRequests *testControlRequestStore
	logs            *mockApplyLogStore
}

func newReconcileFixture(fakeEngine *fakeControlEngine, requests ...*storage.ApplyControlRequest) *reconcileFixture {
	apply := &storage.Apply{
		ID:              321,
		ApplyIdentifier: "apply-reconcile",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.ValidatingDeployRequest,
	}
	task := &storage.Task{
		ID:             654,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-reconcile",
		Database:       "testdb",
		Namespace:      "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		TableName:      "users",
		State:          state.Task.WaitingForDeploy,
	}
	controlRequests := &testControlRequestStore{requests: requests}
	logs := &mockApplyLogStore{}
	store := &exactProgressStorage{
		applies:         &exactProgressApplyStore{apply: apply},
		tasks:           &exactProgressTaskStore{tasks: []*storage.Task{task}},
		logs:            logs,
		controlRequests: controlRequests,
	}
	client := &LocalClient{
		config: LocalConfig{
			Database: "testdb",
			Type:     storage.DatabaseTypeMySQL,
		},
		storage:      store,
		spiritEngine: fakeEngine,
		logger:       slog.Default(),
	}
	return &reconcileFixture{
		client:          client,
		apply:           apply,
		task:            task,
		engine:          fakeEngine,
		storage:         store,
		controlRequests: controlRequests,
		logs:            logs,
	}
}

func pendingReconcileRequest(applyID int64, op storage.ControlOperation) *storage.ApplyControlRequest {
	return &storage.ApplyControlRequest{
		ApplyID:     applyID,
		Operation:   op,
		Status:      storage.ControlRequestPending,
		RequestedBy: "github:alice",
	}
}

// The core adoption path: the engine's backend already drove the change to
// completed before the drive consumed the pending cancel. The drive must adopt
// the engine's outcome — settling the task and apply to completed with full
// progress — and moot the pending cancel without ever invoking the engine's
// Cancel, so the operator never re-runs a command the backend would reject
// forever.
func TestReconcileEngineTerminalTruth_AdoptsCompletedAndMootsCancel(t *testing.T) {
	fx := newReconcileFixture(
		&fakeControlEngine{
			externallyAuthoritative: true,
			progressResult:          &engine.ProgressResult{State: engine.StateCompleted},
		},
		pendingReconcileRequest(321, storage.ControlOperationCancel),
	)

	handled, err := fx.client.reconcileEngineTerminalTruthBeforeCommands(t.Context(), fx.apply, []*storage.Task{fx.task})
	require.NoError(t, err)
	assert.True(t, handled)
	assert.NotNil(t, fx.engine.progressReq, "the drive must read the engine's authoritative state")
	assert.Equal(t, 0, fx.engine.cancelCount, "the doomed cancel must never reach the engine")
	assert.Equal(t, 0, fx.engine.stopCount)
	assert.Equal(t, state.Task.Completed, fx.task.State)
	assert.Equal(t, 100, fx.task.ProgressPercent, "a completed task reports full progress")
	require.NotNil(t, fx.task.CompletedAt)
	assert.Equal(t, state.Apply.Completed, fx.apply.State)
	require.NotNil(t, fx.apply.CompletedAt)
	controlReq, err := fx.controlRequests.GetPending(t.Context(), fx.apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.Nil(t, controlReq, "the durable cancel request must complete so the operator stops re-claiming the apply")
	assert.True(t, hasLogMessageContaining(fx.logs.logs,
		"Schema change reached completed on the engine before the pending stop/cancel was consumed; apply recorded as completed (1 tasks settled, 0 already terminal) (caller: github:alice)"))
}

// A pending stop is mooted the same way when the engine's backend reports the
// change failed non-retryably: the stored state adopts failed (with the
// engine's error message) instead of recording a dead change as stopped.
func TestReconcileEngineTerminalTruth_AdoptsFailedAndMootsStop(t *testing.T) {
	fx := newReconcileFixture(
		&fakeControlEngine{
			externallyAuthoritative: true,
			progressResult: &engine.ProgressResult{
				State:        engine.StateFailed,
				Retryable:    false,
				ErrorMessage: "deploy request errored during cutover",
			},
		},
		pendingReconcileRequest(321, storage.ControlOperationStop),
	)

	handled, err := fx.client.reconcileEngineTerminalTruthBeforeCommands(t.Context(), fx.apply, []*storage.Task{fx.task})
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, 0, fx.engine.stopCount, "the doomed stop must never reach the engine")
	assert.Equal(t, state.Task.Failed, fx.task.State)
	assert.Equal(t, "deploy request errored during cutover", fx.task.ErrorMessage)
	require.NotNil(t, fx.task.CompletedAt)
	assert.Equal(t, state.Apply.Failed, fx.apply.State)
	assert.Equal(t, "deploy request errored during cutover", fx.apply.ErrorMessage)
	controlReq, err := fx.controlRequests.GetPending(t.Context(), fx.apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	assert.Nil(t, controlReq)
}

// When the engine reports the change still live, the reconcile must step aside
// so the pending command is consumed exactly as before.
func TestReconcileEngineTerminalTruth_NonTerminalEngineFallsThroughToCommand(t *testing.T) {
	fx := newReconcileFixture(
		&fakeControlEngine{
			externallyAuthoritative: true,
			progressResult:          &engine.ProgressResult{State: engine.StateRunning},
		},
		pendingReconcileRequest(321, storage.ControlOperationCancel),
	)

	handled, err := fx.client.reconcileEngineTerminalTruthBeforeCommands(t.Context(), fx.apply, []*storage.Task{fx.task})
	require.NoError(t, err)
	assert.False(t, handled)
	assert.NotNil(t, fx.engine.progressReq)
	assert.Equal(t, state.Task.WaitingForDeploy, fx.task.State, "the reconcile must not touch a live task")
	controlReq, err := fx.controlRequests.GetPending(t.Context(), fx.apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	require.NotNil(t, controlReq, "the pending cancel must remain for the command path to consume")
	assert.Equal(t, storage.ControlRequestPending, controlReq.Status)
}

// A failed progress read must fail toward consuming the command, never toward
// skipping it: an unreadable backend must not delay a kill command that may
// still be able to act.
func TestReconcileEngineTerminalTruth_ProgressErrorFallsThroughToCommand(t *testing.T) {
	fx := newReconcileFixture(
		&fakeControlEngine{
			externallyAuthoritative: true,
			progressErr:             errors.New("planetscale api unavailable"),
		},
		pendingReconcileRequest(321, storage.ControlOperationCancel),
	)

	handled, err := fx.client.reconcileEngineTerminalTruthBeforeCommands(t.Context(), fx.apply, []*storage.Task{fx.task})
	require.NoError(t, err)
	assert.False(t, handled)
	controlReq, err := fx.controlRequests.GetPending(t.Context(), fx.apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	require.NotNil(t, controlReq)
	assert.Equal(t, storage.ControlRequestPending, controlReq.Status)
}

// Persisted engine resume state is how a change is addressed, on every database
// type — the progress read that decides whether an already-landed outcome may be
// adopted must carry it, or it answers for a different change (or for none) and
// the drive kills work the engine had already finished.
func TestReconcileEngineTerminalTruth_ProgressReadCarriesPersistedResumeState(t *testing.T) {
	fx := newReconcileFixture(
		&fakeControlEngine{
			externallyAuthoritative: true,
			progressResult:          &engine.ProgressResult{State: engine.StateCompleted},
		},
		pendingReconcileRequest(321, storage.ControlOperationCancel),
	)
	operationID := int64(77)
	fx.task.ApplyOperationID = &operationID
	fx.storage.applyOperations = &exactProgressApplyOperationStore{
		data: &storage.EngineResumeState{
			ApplyOperationID: operationID,
			MigrationContext: "task-reconcile",
			Metadata:         `{"deploy_request_id":42}`,
		},
	}

	handled, err := fx.client.reconcileEngineTerminalTruthBeforeCommands(t.Context(), fx.apply, []*storage.Task{fx.task})
	require.NoError(t, err)
	assert.True(t, handled)
	require.NotNil(t, fx.engine.progressReq)
	require.NotNil(t, fx.engine.progressReq.ResumeState, "the progress read must address the change through its persisted resume state")
	assert.Equal(t, "task-reconcile", fx.engine.progressReq.ResumeState.MigrationContext)
	assert.Equal(t, `{"deploy_request_id":42}`, fx.engine.progressReq.ResumeState.Metadata)
	assert.Equal(t, state.Apply.Completed, fx.apply.State)
	assert.Equal(t, state.Task.Completed, fx.task.State)
}

// An instance-local engine's progress says nothing about work a previous
// process drove, so the reconcile must not even read it — those engines
// reconcile completed work through the resume re-plan.
func TestReconcileEngineTerminalTruth_InstanceLocalEngineIsNotConsulted(t *testing.T) {
	fx := newReconcileFixture(
		&fakeControlEngine{
			externallyAuthoritative: false,
			progressResult:          &engine.ProgressResult{State: engine.StateCompleted},
		},
		pendingReconcileRequest(321, storage.ControlOperationCancel),
	)

	handled, err := fx.client.reconcileEngineTerminalTruthBeforeCommands(t.Context(), fx.apply, []*storage.Task{fx.task})
	require.NoError(t, err)
	assert.False(t, handled)
	assert.Nil(t, fx.engine.progressReq, "an instance-local engine's progress must not be read as backend truth")
	assert.Equal(t, state.Task.WaitingForDeploy, fx.task.State)
}

// With no pending kill command there is nothing to protect, so the reconcile
// is a no-op — the drive's own progress polling reconciles engine state.
func TestReconcileEngineTerminalTruth_NoPendingCommandIsNoOp(t *testing.T) {
	fx := newReconcileFixture(&fakeControlEngine{
		externallyAuthoritative: true,
		progressResult:          &engine.ProgressResult{State: engine.StateCompleted},
	})

	handled, err := fx.client.reconcileEngineTerminalTruthBeforeCommands(t.Context(), fx.apply, []*storage.Task{fx.task})
	require.NoError(t, err)
	assert.False(t, handled)
	assert.Nil(t, fx.engine.progressReq)
	assert.Equal(t, state.Apply.ValidatingDeployRequest, fx.apply.State)
}

// A revert-phase apply's outcome is owned by the revert and skip-revert paths,
// and the command gates permanently reject stop/cancel for it — the reconcile
// must not race the in-flight revert phase by adopting a terminal state.
func TestReconcileEngineTerminalTruth_RevertPhaseApplyIsNotReconciled(t *testing.T) {
	fx := newReconcileFixture(
		&fakeControlEngine{
			externallyAuthoritative: true,
			progressResult:          &engine.ProgressResult{State: engine.StateReverted},
		},
		pendingReconcileRequest(321, storage.ControlOperationCancel),
	)
	fx.apply.State = state.Apply.Reverting

	handled, err := fx.client.reconcileEngineTerminalTruthBeforeCommands(t.Context(), fx.apply, []*storage.Task{fx.task})
	require.NoError(t, err)
	assert.False(t, handled)
	assert.Nil(t, fx.engine.progressReq)
	assert.Equal(t, state.Apply.Reverting, fx.apply.State)
}

// With no task addressing live engine work there is no backend change whose
// terminal truth could contradict the command; the command path owns the
// terminal and task-less shapes.
func TestReconcileEngineTerminalTruth_NoLiveEngineWorkIsNotReconciled(t *testing.T) {
	fx := newReconcileFixture(
		&fakeControlEngine{
			externallyAuthoritative: true,
			progressResult:          &engine.ProgressResult{State: engine.StateCompleted},
		},
		pendingReconcileRequest(321, storage.ControlOperationCancel),
	)
	fx.task.State = state.Task.Pending

	handled, err := fx.client.reconcileEngineTerminalTruthBeforeCommands(t.Context(), fx.apply, []*storage.Task{fx.task})
	require.NoError(t, err)
	assert.False(t, handled)
	assert.Nil(t, fx.engine.progressReq)
	assert.Equal(t, state.Task.Pending, fx.task.State)
}

// A stopped engine state is resumable — a pending cancel still has live
// backend work to kill — so the reconcile must fall through to the command.
func TestReconcileEngineTerminalTruth_StoppedEngineStateFallsThroughToCommand(t *testing.T) {
	fx := newReconcileFixture(
		&fakeControlEngine{
			externallyAuthoritative: true,
			progressResult:          &engine.ProgressResult{State: engine.StateStopped},
		},
		pendingReconcileRequest(321, storage.ControlOperationCancel),
	)

	handled, err := fx.client.reconcileEngineTerminalTruthBeforeCommands(t.Context(), fx.apply, []*storage.Task{fx.task})
	require.NoError(t, err)
	assert.False(t, handled)
	controlReq, err := fx.controlRequests.GetPending(t.Context(), fx.apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	require.NotNil(t, controlReq)
}

// When both a stop and a cancel are pending, adopting the engine's terminal
// truth moots both durable requests in one sweep.
func TestReconcileEngineTerminalTruth_MootsBothPendingCommands(t *testing.T) {
	fx := newReconcileFixture(
		&fakeControlEngine{
			externallyAuthoritative: true,
			progressResult:          &engine.ProgressResult{State: engine.StateCompleted},
		},
		pendingReconcileRequest(321, storage.ControlOperationStop),
		pendingReconcileRequest(321, storage.ControlOperationCancel),
	)

	handled, err := fx.client.reconcileEngineTerminalTruthBeforeCommands(t.Context(), fx.apply, []*storage.Task{fx.task})
	require.NoError(t, err)
	assert.True(t, handled)
	for _, op := range []storage.ControlOperation{storage.ControlOperationStop, storage.ControlOperationCancel} {
		controlReq, err := fx.controlRequests.GetPending(t.Context(), fx.apply.ID, op)
		require.NoError(t, err)
		assert.Nil(t, controlReq, "pending %s must be mooted by the adopted terminal state", op)
	}
}
