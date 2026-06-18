package api

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

func TestOperatorWorkersConfig(t *testing.T) {
	t.Run("default workers", func(t *testing.T) {
		config := &ServerConfig{}
		assert.Equal(t, 0, config.OperatorWorkers)
		assert.Equal(t, 4, DefaultOperatorWorkers)
	})

	t.Run("configured workers", func(t *testing.T) {
		config := &ServerConfig{OperatorWorkers: 3}
		assert.Equal(t, 3, config.OperatorWorkers)
	})

	t.Run("deprecated scheduler_workers folds into operator_workers", func(t *testing.T) {
		config := &ServerConfig{SchedulerWorkers: 2}
		require.NoError(t, config.resolveDeprecatedOperatorWorkers())
		assert.Equal(t, 2, config.OperatorWorkers)
		assert.Equal(t, 0, config.SchedulerWorkers)
	})

	t.Run("setting both keys is rejected", func(t *testing.T) {
		config := &ServerConfig{OperatorWorkers: 4, SchedulerWorkers: 2}
		assert.Error(t, config.resolveDeprecatedOperatorWorkers())
	})
}

// recordingApplyOperationStore records the state-mutating call made by
// markOperationFromApplyState. It embeds the interface so only the methods the
// test exercises need implementations; any other call panics, which keeps the
// test honest about the code path it covers.
type recordingApplyOperationStore struct {
	storage.ApplyOperationStore
	updateStateID    int64
	updateStateValue string
	updateStateErr   error
}

func (r *recordingApplyOperationStore) UpdateState(_ context.Context, id int64, newState string) error {
	r.updateStateID = id
	r.updateStateValue = newState
	return r.updateStateErr
}

type mockStorageWithApplyOperations struct {
	mockStorage
	applyOps storage.ApplyOperationStore
}

func (m *mockStorageWithApplyOperations) ApplyOperations() storage.ApplyOperationStore {
	return m.applyOps
}

func newOperatorTestService(opStore storage.ApplyOperationStore) *Service {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorageWithApplyOperations{applyOps: opStore}, testServerConfig(), nil, logger)
}

type noopProgressObserver struct{}

func (noopProgressObserver) OnProgress(*storage.Apply, []*storage.Task) {}

func (noopProgressObserver) OnTerminal(*storage.Apply, []*storage.Task) {}

func TestResumeClaimedApplyRoutesRecoveredObserver(t *testing.T) {
	deploymentClient := &mockTernClient{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{
		plans:   &staticPlanStore{},
		applies: &staticApplyStore{},
	}, &ServerConfig{}, map[string]tern.Client{
		"east/staging": deploymentClient,
	}, logger)
	observer := noopProgressObserver{}
	svc.OnApplyRecovered = func(apply *storage.Apply) {
		svc.SetApplyObserver(apply.Database, apply.Deployment, apply.Environment, apply.ID, observer)
	}
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-42",
		Database:        "appdb",
		Deployment:      "east",
		Environment:     "staging",
		State:           state.Apply.Pending,
	}

	resumed, err := svc.resumeClaimedApply(t.Context(), 1, apply, 0, "")

	require.NoError(t, err)
	assert.True(t, resumed)
	assert.Same(t, apply, deploymentClient.resumeApply)
	assert.Equal(t, int64(42), deploymentClient.observerApplyID)
	assert.Equal(t, observer, deploymentClient.observer)
}

// A multi-operation drive must not register the per-driver progress/terminal
// observer: the aggregate terminal summary is published once by the projection
// CAS winner, not per deployment. resumeClaimedApplyWithOptions suppresses the
// OnApplyRecovered hook so no observer is set for the drive.
func TestResumeClaimedApplyWithOptions_SuppressesRecoveredObserverForMultiOp(t *testing.T) {
	deploymentClient := &mockTernClient{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{
		plans:   &staticPlanStore{},
		applies: &staticApplyStore{},
	}, &ServerConfig{}, map[string]tern.Client{
		"east/staging": deploymentClient,
	}, logger)
	observerSet := false
	svc.OnApplyRecovered = func(apply *storage.Apply) {
		observerSet = true
		svc.SetApplyObserver(apply.Database, apply.Deployment, apply.Environment, apply.ID, noopProgressObserver{})
	}
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-42",
		Database:        "appdb",
		Deployment:      "east",
		Environment:     "staging",
		State:           state.Apply.Pending,
	}

	resumed, err := svc.resumeClaimedApplyWithOptions(t.Context(), 1, apply, 0, "east",
		resumeClaimedApplyOptions{suppressRecoveredObserver: true})

	require.NoError(t, err)
	assert.True(t, resumed)
	assert.Same(t, apply, deploymentClient.resumeApply)
	assert.False(t, observerSet, "a multi-op drive must not fire the per-driver observer hook")
	assert.Zero(t, deploymentClient.observerApplyID, "no observer must be registered for a multi-op drive")
	assert.Nil(t, deploymentClient.observer)
}

// TestMarkOperationFromApplyState_MirrorsFailedRetryable verifies that a parent
// apply in failed_retryable mirrors that state (not a terminal one) onto the
// operation row via UpdateState, leaving it reclaimable for retry. Returning
// marked=true lets the caller re-derive the parent state from its children.
func TestMarkOperationFromApplyState_MirrorsFailedRetryable(t *testing.T) {
	opStore := &recordingApplyOperationStore{}
	svc := newOperatorTestService(opStore)

	op := &storage.ApplyOperation{ID: 7, Deployment: "region-a"}
	apply := &storage.Apply{
		ID:              3,
		ApplyIdentifier: "apply-retryable",
		State:           state.Apply.FailedRetryable,
		Environment:     "staging",
	}

	marked, err := svc.markOperationFromApplyState(t.Context(), 1, op, apply)
	require.NoError(t, err)
	assert.True(t, marked, "failed_retryable parent must mark the operation so derived apply state is recomputed")
	assert.Equal(t, int64(7), opStore.updateStateID, "the claimed operation row must be the one updated")
	assert.Equal(t, state.Apply.FailedRetryable, opStore.updateStateValue,
		"failed_retryable must be mirrored down, not a terminal state")
}

// listingApplyOperationStore returns a fixed set of operation rows from
// ListByApply so the derived-apply-state projection can be exercised against a
// multi-deployment sibling set.
type listingApplyOperationStore struct {
	storage.ApplyOperationStore
	ops []*storage.ApplyOperation
}

func (s *listingApplyOperationStore) ListByApply(context.Context, int64) ([]*storage.ApplyOperation, error) {
	return s.ops, nil
}

// recordingApplyStore captures the projection persisted by UpdateDerivedState so
// the test can assert the derived state and completed_at stamping. swapped is
// returned to model whether the compare-and-swap matched the expected state.
type recordingApplyStore struct {
	storage.ApplyStore
	updated       *storage.Apply
	expectedState string
	swapped       bool
}

func (s *recordingApplyStore) UpdateDerivedState(_ context.Context, applyID int64, expectedState, newState, errorMessage string, startedAt, completedAt *time.Time) (bool, error) {
	s.expectedState = expectedState
	if !s.swapped {
		return false, nil
	}
	s.updated = &storage.Apply{
		ID:           applyID,
		State:        newState,
		ErrorMessage: errorMessage,
		StartedAt:    startedAt,
		CompletedAt:  completedAt,
	}
	return true, nil
}

func newOperatorStateTestService(opStore storage.ApplyOperationStore, applyStore storage.ApplyStore) *Service {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorageWithApplyStoresAndOperations{applyOps: opStore, applies: applyStore}, testServerConfig(), nil, logger)
}

type mockStorageWithApplyStoresAndOperations struct {
	mockStorage
	applyOps storage.ApplyOperationStore
	applies  storage.ApplyStore
}

func (m *mockStorageWithApplyStoresAndOperations) ApplyOperations() storage.ApplyOperationStore {
	return m.applyOps
}
func (m *mockStorageWithApplyStoresAndOperations) Applies() storage.ApplyStore { return m.applies }

// TestUpdateApplyStateFromOperations_ContinuePolicy verifies that the operator's
// apply-state writer projects the rollout policy over the sibling set: under
// on_failure "continue" a terminally failed deployment holds the apply running
// while a sibling is still pending, while the default policy fails closed and
// terminalizes the apply.
func TestUpdateApplyStateFromOperations_ContinuePolicy(t *testing.T) {
	cases := []struct {
		name      string
		ops       []*storage.ApplyOperation
		wantState string
		wantDone  bool
	}{
		{
			name: "continue holds the apply running_degraded past a failed deployment",
			ops: []*storage.ApplyOperation{
				{ID: 1, State: state.ApplyOperation.Failed, OnFailure: storage.OnFailureContinue},
				{ID: 2, State: state.ApplyOperation.Pending, OnFailure: storage.OnFailureContinue},
			},
			wantState: state.Apply.RunningDegraded,
			wantDone:  false,
		},
		{
			name: "default policy terminalizes the apply on a failed deployment",
			ops: []*storage.ApplyOperation{
				{ID: 1, State: state.ApplyOperation.Failed},
				{ID: 2, State: state.ApplyOperation.Pending},
			},
			wantState: state.Apply.Failed,
			wantDone:  true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			applyStore := &recordingApplyStore{swapped: true}
			svc := newOperatorStateTestService(&listingApplyOperationStore{ops: tc.ops}, applyStore)

			apply := &storage.Apply{
				ID:              3,
				ApplyIdentifier: "apply-projection",
				State:           state.Apply.Pending,
				Environment:     "staging",
			}

			_, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply, allowLeaseScopedFailedReopen)
			require.NoError(t, err)
			require.NotNil(t, applyStore.updated, "derived state differs from current, so the apply must be persisted")
			assert.Equal(t, state.Apply.Pending, applyStore.expectedState, "the write must compare-and-swap on the state read before deriving")
			assert.Equal(t, tc.wantState, applyStore.updated.State)
			if tc.wantDone {
				assert.NotNil(t, applyStore.updated.CompletedAt, "terminal derived state stamps completed_at")
			} else {
				assert.Nil(t, applyStore.updated.CompletedAt, "non-terminal derived state leaves completed_at nil")
			}
		})
	}
}

// TestUpdateApplyStateFromOperations_ReopenFailedGuard verifies the terminal
// guard's reopen exception. Under on_failure "continue" a sibling failure can
// terminalize the parent apply to failed before the rollout settles; once a
// live sibling still derives the projection running_degraded, a lease-holding
// caller may reopen the parent failed → running_degraded so the remaining
// siblings run to completion. The exception is deliberately narrow: only a
// failed parent over a genuinely failed child base may reopen, only to
// running_degraded, and only when the caller holds the apply lease. Every other
// terminal-to-non-terminal transition
// — including reviving a failed parent from an unscoped reconciliation path, and
// any genuinely terminal verdict (completed/cancelled/reverted) — stays an error.
func TestUpdateApplyStateFromOperations_ReopenFailedGuard(t *testing.T) {
	cases := []struct {
		name       string
		parent     string
		ops        []*storage.ApplyOperation
		reopen     failedApplyReopenPolicy
		wantErr    bool
		wantState  string
		wantUpdate bool
	}{
		{
			name:   "lease-scoped reopen holds the failed apply running_degraded for a live sibling",
			parent: state.Apply.Failed,
			ops: []*storage.ApplyOperation{
				{ID: 1, State: state.ApplyOperation.Failed, OnFailure: storage.OnFailureContinue},
				{ID: 2, State: state.ApplyOperation.Running, OnFailure: storage.OnFailureContinue},
			},
			reopen:     allowLeaseScopedFailedReopen,
			wantState:  state.Apply.RunningDegraded,
			wantUpdate: true,
		},
		{
			name:   "unscoped reconciliation refuses to revive a failed apply",
			parent: state.Apply.Failed,
			ops: []*storage.ApplyOperation{
				{ID: 1, State: state.ApplyOperation.Failed, OnFailure: storage.OnFailureContinue},
				{ID: 2, State: state.ApplyOperation.Running, OnFailure: storage.OnFailureContinue},
			},
			reopen:  rejectFailedApplyReopen,
			wantErr: true,
		},
		{
			name:   "completed apply is never revived even with the lease",
			parent: state.Apply.Completed,
			ops: []*storage.ApplyOperation{
				{ID: 1, State: state.ApplyOperation.Running, OnFailure: storage.OnFailureContinue},
				{ID: 2, State: state.ApplyOperation.Completed, OnFailure: storage.OnFailureContinue},
			},
			reopen:  allowLeaseScopedFailedReopen,
			wantErr: true,
		},
		{
			name:   "stale failed apply over a non-failed child base is not reopened",
			parent: state.Apply.Failed,
			ops: []*storage.ApplyOperation{
				{ID: 1, State: state.ApplyOperation.Running, OnFailure: storage.OnFailureContinue},
				{ID: 2, State: state.ApplyOperation.Completed, OnFailure: storage.OnFailureContinue},
			},
			reopen:  allowLeaseScopedFailedReopen,
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			applyStore := &recordingApplyStore{swapped: true}
			svc := newOperatorStateTestService(&listingApplyOperationStore{ops: tc.ops}, applyStore)

			// A terminal parent always carries a stamped completed_at; seed one
			// so the reopen-clears-completed_at assertion is meaningful.
			completedAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
			apply := &storage.Apply{
				ID:              7,
				ApplyIdentifier: "apply-reopen",
				State:           tc.parent,
				Environment:     "staging",
				CompletedAt:     &completedAt,
			}

			_, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply, tc.reopen)
			if tc.wantErr {
				require.Error(t, err)
				assert.Nil(t, applyStore.updated, "a rejected transition must not persist the apply")
				return
			}
			require.NoError(t, err)
			if !tc.wantUpdate {
				assert.Nil(t, applyStore.updated)
				return
			}
			require.NotNil(t, applyStore.updated, "a reopened apply must be persisted")
			assert.Equal(t, tc.wantState, applyStore.updated.State)
			assert.Nil(t, applyStore.updated.CompletedAt, "a reopened running apply clears completed_at")
		})
	}
}

// stubTaskStore returns a fixed task set for GetByApplyOperationID so an
// operation's own drive result can be derived from its tasks.
type stubTaskStore struct {
	storage.TaskStore
	tasks []*storage.Task
}

func (s *stubTaskStore) GetByApplyOperationID(context.Context, int64) ([]*storage.Task, error) {
	return s.tasks, nil
}

// markFailedRecordingApplyOperationStore records MarkFailed so a test can assert
// the operation row was persisted failed with its own task's message.
type markFailedRecordingApplyOperationStore struct {
	storage.ApplyOperationStore
	called    bool
	failedID  int64
	failedMsg string
}

func (s *markFailedRecordingApplyOperationStore) MarkFailed(_ context.Context, id int64, errMsg string) error {
	s.called = true
	s.failedID = id
	s.failedMsg = errMsg
	return nil
}

type mockStorageWithTasksAndOperations struct {
	mockStorage
	tasks    storage.TaskStore
	applyOps storage.ApplyOperationStore
}

func (m *mockStorageWithTasksAndOperations) Tasks() storage.TaskStore { return m.tasks }

func (m *mockStorageWithTasksAndOperations) ApplyOperations() storage.ApplyOperationStore {
	return m.applyOps
}

// TestMarkOperationFromOwnResult_PersistsFailedIndependentOfParent verifies that
// the drive path records a failed deployment from the operation's OWN tasks
// regardless of the parent apply's state. Under the on_failure "continue"
// projection the parent is held running while sibling deployments are still in
// flight; deriving the operation from its own failing task still marks the row
// failed (with that task's message) rather than leaving it claimable to be
// re-driven, so the deployment-order gate and the parent re-derivation observe
// the real outcome.
func TestMarkOperationFromOwnResult_PersistsFailedIndependentOfParent(t *testing.T) {
	opStore := &markFailedRecordingApplyOperationStore{}
	taskStore := &stubTaskStore{tasks: []*storage.Task{
		{State: state.Task.Completed},
		{State: state.Task.Failed, ErrorMessage: "spirit: cutover failed"},
	}}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithTasksAndOperations{tasks: taskStore, applyOps: opStore}, testServerConfig(), nil, logger)

	op := &storage.ApplyOperation{ID: 9, Deployment: "region-b", OnFailure: storage.OnFailureContinue}

	marked, err := svc.markOperationFromOwnResult(t.Context(), 1, op)
	require.NoError(t, err)
	assert.True(t, marked, "a failed operation must be durably recorded so it is not re-claimed")
	assert.True(t, opStore.called, "the operation row must be marked failed from its own tasks")
	assert.Equal(t, int64(9), opStore.failedID, "the claimed operation row must be the one marked failed")
	assert.Equal(t, "spirit: cutover failed", opStore.failedMsg,
		"the failure message must come from the operation's own failing task")
}

// TestMarkOperationFromOwnResult_LeavesNonTerminalClaimable verifies that an
// operation whose own tasks are still running is left claimable (marked=false,
// no terminal write) so a later poll re-leases and resumes it, rather than being
// prematurely terminalized from a still-in-flight drive.
func TestMarkOperationFromOwnResult_LeavesNonTerminalClaimable(t *testing.T) {
	opStore := &markFailedRecordingApplyOperationStore{}
	taskStore := &stubTaskStore{tasks: []*storage.Task{
		{State: state.Task.Running},
		{State: state.Task.Completed},
	}}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithTasksAndOperations{tasks: taskStore, applyOps: opStore}, testServerConfig(), nil, logger)

	op := &storage.ApplyOperation{ID: 11, Deployment: "region-c", OnFailure: storage.OnFailureContinue}

	marked, err := svc.markOperationFromOwnResult(t.Context(), 1, op)
	require.NoError(t, err)
	assert.False(t, marked, "a still-running operation must be left claimable for a later poll")
	assert.False(t, opStore.called, "no terminal write should occur for a non-terminal operation")
}

// updateStateRecordingApplyOperationStore records UpdateState so a test can
// assert a parked operation is persisted at waiting_for_cutover (completed_at nil).
type updateStateRecordingApplyOperationStore struct {
	storage.ApplyOperationStore
	called       bool
	updatedID    int64
	updatedState string
}

func (s *updateStateRecordingApplyOperationStore) UpdateState(_ context.Context, id int64, newState string) error {
	s.called = true
	s.updatedID = id
	s.updatedState = newState
	return nil
}

// TestMarkOperationFromOwnResult_PersistsWaitingForCutover verifies that an
// operation whose own tasks have parked at the cutover barrier is durably
// recorded at waiting_for_cutover via UpdateState (not a terminal write), so the
// row survives the copy drive's release and the deployment-ordered cutover claim
// can pick it up later.
func TestMarkOperationFromOwnResult_PersistsWaitingForCutover(t *testing.T) {
	opStore := &updateStateRecordingApplyOperationStore{}
	taskStore := &stubTaskStore{tasks: []*storage.Task{
		{State: state.Task.WaitingForCutover},
		{State: state.Task.WaitingForCutover},
	}}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithTasksAndOperations{tasks: taskStore, applyOps: opStore}, testServerConfig(), nil, logger)

	op := &storage.ApplyOperation{ID: 13, Deployment: "region-d", CutoverPolicy: storage.CutoverPolicyBarrier}

	marked, err := svc.markOperationFromOwnResult(t.Context(), 1, op)
	require.NoError(t, err)
	assert.True(t, marked, "a parked operation must be durably recorded so the copy claim does not re-drive it")
	assert.True(t, opStore.called, "the operation row must be updated to waiting_for_cutover from its own tasks")
	assert.Equal(t, int64(13), opStore.updatedID)
	assert.True(t, state.IsState(opStore.updatedState, state.Apply.WaitingForCutover),
		"the parked operation must be persisted at waiting_for_cutover, got %q", opStore.updatedState)
}

// TestUpdateApplyStateFromOperations_StampsAggregateFailureMessage verifies that
// when the rollout settles to failed the parent apply's ErrorMessage is surfaced
// from the first failed operation, not from the last-driven (here, successful)
// sibling. The failing deployment ran first and the apply carries no prior
// message; the derived failed verdict must be accompanied by that deployment's
// reason so an operator sees why the apply failed.
func TestUpdateApplyStateFromOperations_StampsAggregateFailureMessage(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "region-a", State: state.ApplyOperation.Failed, ErrorMessage: "spirit: cutover failed"},
		{ID: 2, Deployment: "region-b", State: state.ApplyOperation.Completed},
	}
	applyStore := &recordingApplyStore{swapped: true}
	svc := newOperatorStateTestService(&listingApplyOperationStore{ops: ops}, applyStore)

	apply := &storage.Apply{
		ID:              3,
		ApplyIdentifier: "apply-projection",
		State:           state.Apply.Running,
		Environment:     "staging",
	}

	_, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply, allowLeaseScopedFailedReopen)
	require.NoError(t, err)
	require.NotNil(t, applyStore.updated, "derived failed state differs from running, so the apply must be persisted")
	assert.Equal(t, state.Apply.Failed, applyStore.updated.State)
	assert.Equal(t, "deployment region-a failed: spirit: cutover failed", applyStore.updated.ErrorMessage,
		"the failure reason must come from the failed operation, not the successful last sibling")
}

// TestUpdateApplyStateFromOperations_FirstFailedDeploymentWins verifies that
// when more than one deployment fails the surfaced reason comes from the first
// failed operation in deployment order — the order ListByApply returns rows in,
// matching the order the claim gate drives them. The rollout's failure verdict
// is the first failure, so a later failed sibling's message must not override it.
func TestUpdateApplyStateFromOperations_FirstFailedDeploymentWins(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "region-a", State: state.ApplyOperation.Failed, ErrorMessage: "spirit: region-a cutover failed"},
		{ID: 2, Deployment: "region-b", State: state.ApplyOperation.Failed, ErrorMessage: "spirit: region-b cutover failed"},
	}
	applyStore := &recordingApplyStore{swapped: true}
	svc := newOperatorStateTestService(&listingApplyOperationStore{ops: ops}, applyStore)

	apply := &storage.Apply{
		ID:              3,
		ApplyIdentifier: "apply-projection",
		State:           state.Apply.Running,
		Environment:     "staging",
	}

	_, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply, allowLeaseScopedFailedReopen)
	require.NoError(t, err)
	require.NotNil(t, applyStore.updated)
	assert.Equal(t, state.Apply.Failed, applyStore.updated.State)
	assert.Equal(t, "deployment region-a failed: spirit: region-a cutover failed", applyStore.updated.ErrorMessage,
		"the reason must come from the first failed deployment in order, not a later failed sibling")
}

// TestUpdateApplyStateFromOperations_KeepsExistingMessageWhenNoOperationCarriesOne
// verifies that a derived failed verdict preserves the apply's existing message
// when no failed operation row carries one, rather than blanking the reason.
func TestUpdateApplyStateFromOperations_KeepsExistingMessageWhenNoOperationCarriesOne(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "region-a", State: state.ApplyOperation.Failed},
		{ID: 2, Deployment: "region-b", State: state.ApplyOperation.Completed},
	}
	applyStore := &recordingApplyStore{swapped: true}
	svc := newOperatorStateTestService(&listingApplyOperationStore{ops: ops}, applyStore)

	apply := &storage.Apply{
		ID:              3,
		ApplyIdentifier: "apply-projection",
		State:           state.Apply.Running,
		Environment:     "staging",
		ErrorMessage:    "prior reason",
	}

	_, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply, allowLeaseScopedFailedReopen)
	require.NoError(t, err)
	require.NotNil(t, applyStore.updated)
	assert.Equal(t, state.Apply.Failed, applyStore.updated.State)
	assert.Equal(t, "prior reason", applyStore.updated.ErrorMessage,
		"with no operation-level message the existing apply reason must be preserved")
}

// TestUpdateApplyStateFromOperations_ReturnsProjectionResult verifies the
// structured projection result the writer returns: whether the compare-and-swap
// advanced the apply row, the previous and derived states, and whether the swap
// terminalized a previously non-terminal apply. Callers in the multi-deployment
// fan-out work key the single-publisher terminal summary off this result, so the
// fields must distinguish a winning terminal swap from a non-terminal swap, a
// no-op match, and a lost race.
func TestUpdateApplyStateFromOperations_ReturnsProjectionResult(t *testing.T) {
	startedAt := time.Now().Add(-time.Minute)
	cases := []struct {
		name     string
		ops      []*storage.ApplyOperation
		apply    *storage.Apply
		casMatch bool
		want     applyProjectionResult
	}{
		{
			name: "winning swap to terminal",
			ops: []*storage.ApplyOperation{
				{ID: 1, Deployment: "region-a", State: state.ApplyOperation.Failed, ErrorMessage: "boom"},
				{ID: 2, Deployment: "region-b", State: state.ApplyOperation.Completed},
			},
			apply:    &storage.Apply{ID: 3, ApplyIdentifier: "apply-a", State: state.Apply.Running, StartedAt: &startedAt},
			casMatch: true,
			want:     applyProjectionResult{Swapped: true, PreviousState: state.Apply.Running, DerivedState: state.Apply.Failed, BecameTerminal: true, OperationCount: 2},
		},
		{
			name: "winning non-terminal swap",
			ops: []*storage.ApplyOperation{
				{ID: 1, Deployment: "region-a", State: state.ApplyOperation.Running},
				{ID: 2, Deployment: "region-b", State: state.ApplyOperation.Pending},
			},
			apply:    &storage.Apply{ID: 3, ApplyIdentifier: "apply-b", State: state.Apply.Pending},
			casMatch: true,
			want:     applyProjectionResult{Swapped: true, PreviousState: state.Apply.Pending, DerivedState: state.Apply.Running, BecameTerminal: false, OperationCount: 2},
		},
		{
			name: "no-op match",
			ops: []*storage.ApplyOperation{
				{ID: 1, Deployment: "region-a", State: state.ApplyOperation.Running},
				{ID: 2, Deployment: "region-b", State: state.ApplyOperation.Running},
			},
			apply:    &storage.Apply{ID: 3, ApplyIdentifier: "apply-c", State: state.Apply.Running, StartedAt: &startedAt},
			casMatch: true,
			want:     applyProjectionResult{Swapped: false, PreviousState: state.Apply.Running, DerivedState: state.Apply.Running, BecameTerminal: false, OperationCount: 2},
		},
		{
			name: "lost race",
			ops: []*storage.ApplyOperation{
				{ID: 1, Deployment: "region-a", State: state.ApplyOperation.Failed, ErrorMessage: "boom"},
				{ID: 2, Deployment: "region-b", State: state.ApplyOperation.Completed},
			},
			apply:    &storage.Apply{ID: 3, ApplyIdentifier: "apply-d", State: state.Apply.Running, StartedAt: &startedAt},
			casMatch: false,
			want:     applyProjectionResult{Swapped: false, PreviousState: state.Apply.Running, DerivedState: state.Apply.Failed, BecameTerminal: false, OperationCount: 2},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			applyStore := &recordingApplyStore{swapped: tc.casMatch}
			svc := newOperatorStateTestService(&listingApplyOperationStore{ops: tc.ops}, applyStore)

			got, err := svc.updateApplyStateFromOperations(t.Context(), 1, tc.apply, allowLeaseScopedFailedReopen)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// fakeControlRequestStore is a minimal ControlRequestStore for stop
// reconciliation tests: GetPending returns the configured pending stop request,
// and CompletePending records the operations it was asked to complete.
type fakeControlRequestStore struct {
	storage.ControlRequestStore
	pendingStop *storage.ApplyControlRequest
	completed   []storage.ControlOperation
}

func (s *fakeControlRequestStore) GetPending(_ context.Context, _ int64, op storage.ControlOperation) (*storage.ApplyControlRequest, error) {
	if op == storage.ControlOperationStop {
		return s.pendingStop, nil
	}
	return nil, nil
}

func (s *fakeControlRequestStore) CompletePending(_ context.Context, _ int64, op storage.ControlOperation) error {
	s.completed = append(s.completed, op)
	return nil
}

// markPendingStoppedRecordingStore records MarkPendingStoppedByApply so a test
// can assert the operator stop reconciliation terminalized the pending siblings.
type markPendingStoppedRecordingStore struct {
	storage.ApplyOperationStore
	called     bool
	stoppedFor int64
	count      int64
}

func (s *markPendingStoppedRecordingStore) MarkPendingStoppedByApply(_ context.Context, applyID int64) (int64, error) {
	s.called = true
	s.stoppedFor = applyID
	return s.count, nil
}

// getApplyStore returns a fixed apply from Get so completePendingStopIfApplyResolved
// can be driven against a chosen terminal/non-terminal state.
type getApplyStore struct {
	storage.ApplyStore
	apply *storage.Apply
}

func (s *getApplyStore) Get(_ context.Context, _ int64) (*storage.Apply, error) {
	return s.apply, nil
}

type mockStorageWithControlAndOps struct {
	mockStorage
	applies  storage.ApplyStore
	applyOps storage.ApplyOperationStore
	control  storage.ControlRequestStore
}

func (m *mockStorageWithControlAndOps) Applies() storage.ApplyStore { return m.applies }
func (m *mockStorageWithControlAndOps) ApplyOperations() storage.ApplyOperationStore {
	return m.applyOps
}
func (m *mockStorageWithControlAndOps) ControlRequests() storage.ControlRequestStore {
	return m.control
}

func newStopReconcileTestService(applies storage.ApplyStore, ops storage.ApplyOperationStore, control storage.ControlRequestStore) *Service {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorageWithControlAndOps{applies: applies, applyOps: ops, control: control}, testServerConfig(), nil, logger)
}

// TestStopPendingOperationsForPendingStop verifies the operator terminalizes
// pending siblings only when a stop is actually pending for the apply.
func TestStopPendingOperationsForPendingStop(t *testing.T) {
	apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-stop", Environment: "staging"}

	t.Run("stops pending siblings when a stop is pending", func(t *testing.T) {
		ops := &markPendingStoppedRecordingStore{count: 2}
		control := &fakeControlRequestStore{pendingStop: &storage.ApplyControlRequest{
			ApplyID: apply.ID, Operation: storage.ControlOperationStop, Status: storage.ControlRequestPending,
		}}
		svc := newStopReconcileTestService(&getApplyStore{}, ops, control)

		require.NoError(t, svc.stopPendingOperationsForPendingStop(t.Context(), 1, apply))
		assert.True(t, ops.called, "a pending stop must terminalize pending siblings")
		assert.Equal(t, apply.ID, ops.stoppedFor)
	})

	t.Run("no-op when no stop is pending", func(t *testing.T) {
		ops := &markPendingStoppedRecordingStore{}
		control := &fakeControlRequestStore{pendingStop: nil}
		svc := newStopReconcileTestService(&getApplyStore{}, ops, control)

		require.NoError(t, svc.stopPendingOperationsForPendingStop(t.Context(), 1, apply))
		assert.False(t, ops.called, "without a pending stop, no siblings are stopped")
	})
}

// TestCompletePendingStopIfApplyResolved verifies the operator completes a
// pending stop request only once the apply has settled terminally.
func TestCompletePendingStopIfApplyResolved(t *testing.T) {
	pendingStop := func() *storage.ApplyControlRequest {
		return &storage.ApplyControlRequest{ApplyID: 9, Operation: storage.ControlOperationStop, Status: storage.ControlRequestPending}
	}

	t.Run("completes the stop when the apply is terminal", func(t *testing.T) {
		applies := &getApplyStore{apply: &storage.Apply{ID: 9, ApplyIdentifier: "apply-done", State: state.Apply.Failed}}
		control := &fakeControlRequestStore{pendingStop: pendingStop()}
		svc := newStopReconcileTestService(applies, &markPendingStoppedRecordingStore{}, control)

		require.NoError(t, svc.completePendingStopIfApplyResolved(t.Context(), 1, 9))
		require.Len(t, control.completed, 1, "a terminal apply with a pending stop completes the request")
		assert.Equal(t, storage.ControlOperationStop, control.completed[0])
	})

	t.Run("leaves the stop pending while the apply is non-terminal", func(t *testing.T) {
		applies := &getApplyStore{apply: &storage.Apply{ID: 9, ApplyIdentifier: "apply-running", State: state.Apply.Running}}
		control := &fakeControlRequestStore{pendingStop: pendingStop()}
		svc := newStopReconcileTestService(applies, &markPendingStoppedRecordingStore{}, control)

		require.NoError(t, svc.completePendingStopIfApplyResolved(t.Context(), 1, 9))
		assert.Empty(t, control.completed, "a non-terminal apply must not complete the stop request")
	})

	t.Run("no-op when no stop is pending", func(t *testing.T) {
		applies := &getApplyStore{apply: &storage.Apply{ID: 9, ApplyIdentifier: "apply-done", State: state.Apply.Failed}}
		control := &fakeControlRequestStore{pendingStop: nil}
		svc := newStopReconcileTestService(applies, &markPendingStoppedRecordingStore{}, control)

		require.NoError(t, svc.completePendingStopIfApplyResolved(t.Context(), 1, 9))
		assert.Empty(t, control.completed, "no pending stop means nothing to complete")
	})
}

// TestUpdateApplyStateFromOperations_StaleWriteSkipped verifies that when the
// compare-and-swap misses — another drive advanced the apply between the
// operator's read and write — the operator skips quietly rather than erroring or
// reviving a stale projection.
func TestUpdateApplyStateFromOperations_StaleWriteSkipped(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, State: state.ApplyOperation.Failed},
		{ID: 2, State: state.ApplyOperation.Pending},
	}
	applyStore := &recordingApplyStore{swapped: false}
	svc := newOperatorStateTestService(&listingApplyOperationStore{ops: ops}, applyStore)

	apply := &storage.Apply{
		ID:              3,
		ApplyIdentifier: "apply-projection",
		State:           state.Apply.Pending,
		Environment:     "staging",
	}

	_, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply, allowLeaseScopedFailedReopen)
	require.NoError(t, err)
	assert.Equal(t, state.Apply.Pending, applyStore.expectedState, "the write must compare-and-swap on the state read before deriving")
	assert.Nil(t, applyStore.updated, "a CAS miss must not record a persisted projection")
}
