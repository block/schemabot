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

			client.pollForCompletionAtomic(t.Context(), apply, tasks, nil, nil, map[string]string{}, false)

			assert.Equal(t, tc.wantStoredState, snapshot.stored.State)
			require.Len(t, controlRequests.requests, 1)
			assert.Equal(t, tc.wantRevertStatus, controlRequests.requests[0].Status)
			assert.Len(t, observer.terminal, tc.wantTerminalCalls)
		})
	}
}
