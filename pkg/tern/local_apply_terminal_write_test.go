package tern

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// derivedStateRefusingApplyStore serves reads from the wrapped snapshot store
// but fails the rollout projection's state write, modelling a storage outage at
// the moment a grouped drive records the apply's settled state.
type derivedStateRefusingApplyStore struct {
	*snapshotApplyStore
	err error
}

func (s *derivedStateRefusingApplyStore) UpdateDerivedState(ctx context.Context, id int64, expectedState, newState, errorMessage string, startedAt, completedAt *time.Time) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	return s.snapshotApplyStore.UpdateDerivedState(ctx, id, expectedState, newState, errorMessage, startedAt, completedAt)
}

// pendingControlRequestStore returns a control request store holding one
// pending request of the given operation against applyID.
func pendingControlRequestStore(applyID int64, operation storage.ControlOperation) *testControlRequestStore {
	return &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ID:          1,
		ApplyID:     applyID,
		Operation:   operation,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator",
	}}}
}

// A sequential apply's last table has finished while an operator's stop is
// still pending, and the write recording the apply completed fails. Storage
// still reports the apply running, so the drive must not answer for an outcome
// it never recorded: the stop stays pending for the claim that finalizes the
// apply next, and the observer posts no terminal summary. Once the write lands,
// the same finalization settles the stop and notifies the observer exactly once.
func TestFinalizeSequentialApply_OutcomeSideEffectsWaitForTheStoredOutcome(t *testing.T) {
	cases := []struct {
		name              string
		updateErr         error
		wantStoredState   string
		wantStopStatus    storage.ControlRequestStatus
		wantTerminalCalls int
	}{
		{
			name:              "outcome write fails",
			updateErr:         errors.New("storage unavailable"),
			wantStoredState:   state.Apply.Running,
			wantStopStatus:    storage.ControlRequestPending,
			wantTerminalCalls: 0,
		},
		{
			name:              "outcome write lands",
			wantStoredState:   state.Apply.Completed,
			wantStopStatus:    storage.ControlRequestCompleted,
			wantTerminalCalls: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stored := failureLogTestApply(state.Apply.Running, 0)
			done := &storage.Task{ID: 1, TaskIdentifier: "task-1", ApplyID: stored.ID, TableName: "orders", State: state.Task.Completed}
			applies := &mockApplyStore{apply: stored, updateErr: tc.updateErr}
			controlRequests := pendingControlRequestStore(stored.ID, storage.ControlOperationStop)
			client := &LocalClient{
				config: LocalConfig{Database: "orders", Type: storage.DatabaseTypeMySQL},
				storage: &mockStorage{
					applies:         applies,
					tasks:           &mockTaskStore{tasks: []*storage.Task{done}},
					logs:            &mockApplyLogStore{},
					controlRequests: controlRequests,
				},
				logger: slog.Default(),
			}
			observer := &terminalRecordingObserver{}
			client.SetObserver(stored.ID, observer)

			drive := *stored
			err := client.finalizeSequentialApply(t.Context(), &drive, []*storage.Task{done}, nil, false)

			if tc.updateErr != nil {
				require.ErrorIs(t, err, tc.updateErr)
				assert.Contains(t, err.Error(), "apply-7", "the error names the apply the drive could not finalize")
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantStoredState, applies.apply.State)
			require.Len(t, controlRequests.requests, 1)
			assert.Equal(t, tc.wantStopStatus, controlRequests.requests[0].Status)
			assert.Len(t, observer.terminal, tc.wantTerminalCalls)
		})
	}
}

// A previous drive settled every task of an apply but exited before it could
// record the apply's outcome, so the apply was re-claimed still running with an
// operator's revert pending. The re-claim finds no remaining work and records
// the apply completed; the revert that outcome moots settles before the
// terminal summary posts, rather than staying pending on an apply nothing will
// claim again.
func TestResumeApplyWithTasks_NoRemainingWorkSettlesMootedRequests(t *testing.T) {
	logs := &mockApplyLogStore{}
	taskStore := &exactProgressTaskStore{}
	c, eng, apply, applies, tasks := newLandedSiblingResume(t, taskStore, logs)
	plan := &storage.Plan{ID: 5}
	apply.PlanID = plan.ID
	applies.stored = *apply
	store := c.storage.(*exactProgressStorage)
	store.plans = &fakePlanStore{getByIDFn: func(int64) (*storage.Plan, error) { return plan, nil }}
	controlRequests := pendingControlRequestStore(apply.ID, storage.ControlOperationRevert)
	store.controlRequests = controlRequests
	for _, task := range tasks {
		task.State = state.Task.Completed
	}
	taskStore.tasks = tasks
	observer := &terminalRecordingObserver{}
	c.SetObserver(apply.ID, observer)

	require.NoError(t, c.resumeApplyWithTasks(t.Context(), apply, nil, tasks, nil, false, false))

	assert.Equal(t, state.Apply.Completed, applies.stored.State)
	require.Len(t, controlRequests.requests, 1)
	assert.Equal(t, storage.ControlRequestCompleted, controlRequests.requests[0].Status,
		"the completed outcome moots the pending revert")
	require.Len(t, observer.terminal, 1)
	assert.Equal(t, state.Apply.Completed, observer.terminal[0].State)
	assert.Empty(t, eng.applied, "an apply with no remaining work hands nothing to the engine")
}

// A grouped apply's engine reports the schema change completed while an
// operator's revert is still pending, and the rollout projection's write of the
// completed state fails. Storage still reports the apply running, so the drive
// exits without answering for the outcome: the revert stays pending for the
// claim that finalizes the apply next, and the observer posts no terminal
// summary. Once the write lands, the same tick settles the revert and notifies
// the observer exactly once.
func TestPollForCompletionAtomic_OutcomeSideEffectsWaitForTheStoredOutcome(t *testing.T) {
	cases := []struct {
		name              string
		writeErr          error
		wantStoredState   string
		wantRevertStatus  storage.ControlRequestStatus
		wantTerminalCalls int
	}{
		{
			name:              "outcome write fails",
			writeErr:          errors.New("storage unavailable"),
			wantStoredState:   state.Apply.Running,
			wantRevertStatus:  storage.ControlRequestPending,
			wantTerminalCalls: 0,
		},
		{
			name:              "outcome write lands",
			wantStoredState:   state.Apply.Completed,
			wantRevertStatus:  storage.ControlRequestCompleted,
			wantTerminalCalls: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eng := &lostWorkEngine{phaseSequenceEngine: phaseSequenceEngine{results: []*engine.ProgressResult{
				{State: engine.StateCompleted},
			}}}
			client, apply, tasks, _ := lostWorkAtomicPollFixture(eng, lostWorkTrustBudgetAmple)
			store := client.storage.(*exactProgressStorage)
			snapshot, ok := store.applies.(*snapshotApplyStore)
			require.True(t, ok)
			store.applies = &derivedStateRefusingApplyStore{snapshotApplyStore: snapshot, err: tc.writeErr}
			controlRequests := pendingControlRequestStore(apply.ID, storage.ControlOperationRevert)
			store.controlRequests = controlRequests
			observer := &terminalRecordingObserver{}
			client.SetObserver(apply.ID, observer)

			pollErr := client.pollForCompletionAtomic(t.Context(), apply, tasks, nil, nil, map[string]string{}, false)
			if tc.writeErr != nil {
				require.ErrorIs(t, pollErr, tc.writeErr)
			} else {
				require.NoError(t, pollErr)
			}

			assert.Equal(t, tc.wantStoredState, snapshot.stored.State)
			require.Len(t, controlRequests.requests, 1)
			assert.Equal(t, tc.wantRevertStatus, controlRequests.requests[0].Status)
			assert.Len(t, observer.terminal, tc.wantTerminalCalls)
		})
	}
}

// driveCancellingApplyStore serves reads from the wrapped snapshot store and
// cancels the drive when the rollout projection's state write is attempted,
// failing the write the way a storage driver does once its context is gone.
// It models an operator's stop landing while a grouped drive is recording the
// apply's settled state.
type driveCancellingApplyStore struct {
	*snapshotApplyStore
	cancel context.CancelFunc
}

func (s *driveCancellingApplyStore) UpdateDerivedState(ctx context.Context, _ int64, _, _, _ string, _, _ *time.Time) (bool, error) {
	s.cancel()
	return false, ctx.Err()
}

// stopCancellingEngine reports a grouped apply still running and, on its first
// progress poll, cancels the drive the way an operator's stop does: through the
// client's registered cancel handle, leaving every other context alive.
type stopCancellingEngine struct {
	*fakeControlEngine
	client *LocalClient
}

func (e *stopCancellingEngine) Progress(ctx context.Context, req *engine.ProgressRequest) (*engine.ProgressResult, error) {
	e.client.cancelApplyHandle(e.client.currentApplyCancel())
	return e.fakeControlEngine.Progress(ctx, req)
}

// A grouped drive's context ends — an operator's stop cancels it, or its lease
// is lost — before or while the drive records an outcome. The stored apply is
// left as it stands for the next claim, and the poll hands back with no error:
// a cancelled drive is not an outcome the apply failed to record, so reporting
// it as one would have the resume pause or fail a schema change that is still
// healthy. The pending revert stays pending and no terminal summary posts.
func TestPollForCompletionAtomic_CancelledDriveHandsBackWithoutAnOutcome(t *testing.T) {
	cases := []struct {
		name   string
		engine *lostWorkEngine
		// prepare arranges the cancellation and returns the drive context.
		prepare func(t *testing.T, store *exactProgressStorage, snapshot *snapshotApplyStore) context.Context
	}{
		{
			name:   "cancelled before the next poll",
			engine: &lostWorkEngine{phaseSequenceEngine: phaseSequenceEngine{results: []*engine.ProgressResult{{State: engine.StateRunning}}}},
			prepare: func(t *testing.T, _ *exactProgressStorage, _ *snapshotApplyStore) context.Context {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				return ctx
			},
		},
		{
			name:   "cancelled while recording the settled state",
			engine: &lostWorkEngine{phaseSequenceEngine: phaseSequenceEngine{results: []*engine.ProgressResult{{State: engine.StateCompleted}}}},
			prepare: func(t *testing.T, store *exactProgressStorage, snapshot *snapshotApplyStore) context.Context {
				ctx, cancel := context.WithCancel(t.Context())
				store.applies = &driveCancellingApplyStore{snapshotApplyStore: snapshot, cancel: cancel}
				return ctx
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, apply, tasks, _ := lostWorkAtomicPollFixture(tc.engine, lostWorkTrustBudgetAmple)
			store := client.storage.(*exactProgressStorage)
			snapshot, ok := store.applies.(*snapshotApplyStore)
			require.True(t, ok)
			controlRequests := pendingControlRequestStore(apply.ID, storage.ControlOperationRevert)
			store.controlRequests = controlRequests
			observer := &terminalRecordingObserver{}
			client.SetObserver(apply.ID, observer)
			ctx := tc.prepare(t, store, snapshot)

			require.NoError(t, client.pollForCompletionAtomic(ctx, apply, tasks, nil, nil, map[string]string{}, false))

			assert.Equal(t, state.Apply.Running, snapshot.stored.State, "the stored apply is left for the next claim")
			require.Len(t, controlRequests.requests, 1)
			assert.Equal(t, storage.ControlRequestPending, controlRequests.requests[0].Status)
			assert.Empty(t, observer.terminal)
		})
	}
}

// An operator stops a grouped apply while a blocking resume is polling it. The
// stop cancels only the drive, so the resume returns to its caller with no
// error and the stored apply untouched: the stop handler settles the apply on
// its own request context, and an error here would read as the engine failing
// the schema change and race that settlement with a retryable or failed
// verdict of the drive's own.
func TestLaunchAtomicResume_StopCancellingTheDriveIsNotAnEngineFailure(t *testing.T) {
	operationStore := &exactProgressApplyOperationStore{
		data: &storage.EngineResumeState{
			ApplyOperationID: 7,
			MigrationContext: "ctx-reattach",
			Metadata:         `{"branch_name":"branch-1","deploy_request_id":5}`,
		},
	}
	applyResult := &engine.ApplyResult{
		Accepted: true,
		ResumeState: &engine.ResumeState{
			MigrationContext: "ctx-reattach",
			Metadata:         `{"branch_name":"branch-1","deploy_request_id":5}`,
		},
	}
	client, apply, tasks, plan, applyStore := reattachResumeFixture(operationStore, applyResult)
	client.planetscaleEngine = &stopCancellingEngine{fakeControlEngine: client.planetscaleEngine.(*fakeControlEngine), client: client}
	client.heartbeatInterval = time.Hour
	client.taskPollIntervalOverride = time.Millisecond
	client.storage.(*exactProgressStorage).logs = &mockApplyLogStore{}
	observer := &terminalRecordingObserver{}
	client.SetObserver(apply.ID, observer)

	driveCtx, cancelDrive := context.WithCancel(t.Context())
	defer cancelDrive()
	generation := client.setApplyCancel(cancelDrive)
	defer client.clearApplyCancel(generation)

	err := client.launchAtomicResume(driveCtx, apply, tasks, plan, apply.GetOptions().Map(), "Recovering from checkpoint", true, false, false)

	require.NoError(t, err)
	require.ErrorIs(t, driveCtx.Err(), context.Canceled, "the stop cancelled the drive")
	assert.Equal(t, state.Apply.Recovering, applyStore.apply.State, "the stored apply is left for the stop handler to settle")
	assert.Nil(t, applyStore.apply.CompletedAt)
	assert.Empty(t, observer.terminal)
}

// A sequential drive is cancelled — by an operator's stop or a lost lease —
// as it finalizes the apply, and the outcome write fails under the cancelled
// context. The failure describes the cancellation, not the outcome: the drive
// hands the apply back with no error, the stop stays pending for the stop
// handler or the next claim to settle, and no terminal summary posts.
func TestFinalizeSequentialApply_CancelledDriveHandsBackWithoutAnOutcome(t *testing.T) {
	stored := failureLogTestApply(state.Apply.Running, 0)
	done := &storage.Task{ID: 1, TaskIdentifier: "task-1", ApplyID: stored.ID, TableName: "orders", State: state.Task.Completed}
	applies := &mockApplyStore{apply: stored, updateErr: context.Canceled}
	controlRequests := pendingControlRequestStore(stored.ID, storage.ControlOperationStop)
	client := &LocalClient{
		config: LocalConfig{Database: "orders", Type: storage.DatabaseTypeMySQL},
		storage: &mockStorage{
			applies:         applies,
			tasks:           &mockTaskStore{tasks: []*storage.Task{done}},
			logs:            &mockApplyLogStore{},
			controlRequests: controlRequests,
		},
		logger: slog.Default(),
	}
	observer := &terminalRecordingObserver{}
	client.SetObserver(stored.ID, observer)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	drive := *stored
	require.NoError(t, client.finalizeSequentialApply(ctx, &drive, []*storage.Task{done}, nil, false))

	assert.Equal(t, state.Apply.Running, applies.apply.State)
	require.Len(t, controlRequests.requests, 1)
	assert.Equal(t, storage.ControlRequestPending, controlRequests.requests[0].Status)
	assert.Empty(t, observer.terminal)
}
