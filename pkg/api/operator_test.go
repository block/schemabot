package api

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/panicsafe"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

func TestDriversConfig(t *testing.T) {
	t.Run("default drivers", func(t *testing.T) {
		config := &ServerConfig{}
		assert.Equal(t, 0, config.Drivers)
		assert.Equal(t, 4, DefaultDrivers)
	})

	t.Run("configured drivers", func(t *testing.T) {
		config := &ServerConfig{Drivers: 3}
		assert.Equal(t, 3, config.Drivers)
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

// releaseLatchControlStore returns a fixed release latch from GetByOperation so
// the operator's apply-state writer can be driven with or without a release. It
// records the queried applyID so a test can assert the latch is read for the
// apply being projected, not some other apply.
type releaseLatchControlStore struct {
	storage.ControlRequestStore
	release        *storage.ApplyControlRequest
	queriedApply   int64
	queriedRelease bool
}

func (s *releaseLatchControlStore) GetByOperation(_ context.Context, applyID int64, op storage.ControlOperation) (*storage.ApplyControlRequest, error) {
	if op == storage.ControlOperationRelease {
		s.queriedApply = applyID
		s.queriedRelease = true
		return s.release, nil
	}
	return nil, nil
}

// TestUpdateApplyStateFromOperations_PausePolicy verifies the operator projects
// the release latch over an on_failure "pause" rollout: an unreleased pause
// holds the apply paused while a later sibling is still pending, and an operator
// release latches the rollout open so the same failure projects running_degraded
// like continue.
func TestUpdateApplyStateFromOperations_PausePolicy(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, State: state.ApplyOperation.Failed, OnFailure: storage.OnFailurePause},
		{ID: 2, State: state.ApplyOperation.Pending, OnFailure: storage.OnFailurePause},
	}
	cases := []struct {
		name      string
		release   *storage.ApplyControlRequest
		wantState string
	}{
		{
			name:      "unreleased pause holds the apply paused",
			release:   nil,
			wantState: state.Apply.Paused,
		},
		{
			name:      "released pause projects running_degraded like continue",
			release:   &storage.ApplyControlRequest{Operation: storage.ControlOperationRelease, Status: storage.ControlRequestCompleted},
			wantState: state.Apply.RunningDegraded,
		},
		{
			name:      "failed release does not latch and stays paused",
			release:   &storage.ApplyControlRequest{Operation: storage.ControlOperationRelease, Status: storage.ControlRequestFailed},
			wantState: state.Apply.Paused,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			applyStore := &recordingApplyStore{swapped: true}
			control := &releaseLatchControlStore{release: tc.release}
			logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
			svc := New(&mockStorageWithControlAndOps{
				applies:  applyStore,
				applyOps: &listingApplyOperationStore{ops: ops},
				control:  control,
			}, testServerConfig(), nil, logger)

			apply := &storage.Apply{
				ID:              3,
				ApplyIdentifier: "apply-pause",
				State:           state.Apply.Pending,
				Environment:     "staging",
			}

			_, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply, allowLeaseScopedFailedReopen)
			require.NoError(t, err)
			require.NotNil(t, applyStore.updated)
			assert.Equal(t, tc.wantState, applyStore.updated.State)
			require.True(t, control.queriedRelease, "a pause rollout must read the release latch")
			assert.Equal(t, apply.ID, control.queriedApply, "the release latch must be read for the apply being projected")
		})
	}
}

// TestUpdateApplyStateFromOperations_NoPauseSkipsReleaseLatch verifies the
// operator does not read the release latch when no operation uses on_failure
// pause: a non-pause rollout's projection cannot depend on a release, so it pays
// neither the read nor its failure mode.
func TestUpdateApplyStateFromOperations_NoPauseSkipsReleaseLatch(t *testing.T) {
	applyStore := &recordingApplyStore{swapped: true}
	control := &releaseLatchControlStore{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithControlAndOps{
		applies: applyStore,
		applyOps: &listingApplyOperationStore{ops: []*storage.ApplyOperation{
			{ID: 1, State: state.ApplyOperation.Failed, OnFailure: storage.OnFailureContinue},
			{ID: 2, State: state.ApplyOperation.Pending, OnFailure: storage.OnFailureContinue},
		}},
		control: control,
	}, testServerConfig(), nil, logger)

	apply := &storage.Apply{
		ID:              3,
		ApplyIdentifier: "apply-continue",
		State:           state.Apply.Pending,
		Environment:     "staging",
	}

	_, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply, allowLeaseScopedFailedReopen)
	require.NoError(t, err)
	assert.False(t, control.queriedRelease, "a rollout with no pause operation must not read the release latch")
}

func TestUpdateApplyStateFromOperations_FinalizerPendingIsNonTerminal(t *testing.T) {
	applyStore := &recordingApplyStore{swapped: true}
	svc := newOperatorStateTestService(&listingApplyOperationStore{ops: []*storage.ApplyOperation{
		{ID: 1, State: state.ApplyOperation.Completed, OperationKind: storage.ApplyOperationKindWork},
		{ID: 2, State: state.ApplyOperation.Completed, OperationKind: storage.ApplyOperationKindWork},
		{ID: 3, State: state.ApplyOperation.Pending, OperationKind: storage.ApplyOperationKindGroupFinalizer},
	}}, applyStore)

	apply := &storage.Apply{
		ID:              3,
		ApplyIdentifier: "apply-finalizer-pending",
		State:           state.Apply.Running,
		Environment:     "staging",
	}

	_, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply, allowLeaseScopedFailedReopen)
	require.NoError(t, err)
	require.NotNil(t, applyStore.updated, "the pending finalizer must keep the aggregate non-terminal")
	assert.Equal(t, state.Apply.Pending, applyStore.updated.State)
	assert.Nil(t, applyStore.updated.CompletedAt)
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

// casApplyStore models the applies row as a compare-and-swap against a single
// durable state. Get returns the durable state so a reload observes writes made
// by an earlier UpdateDerivedState, and UpdateDerivedState only swaps when the
// caller's expected state matches the durable state — exactly like the SQL CAS.
type casApplyStore struct {
	storage.ApplyStore
	mu       sync.Mutex
	template storage.Apply
	state    string
}

func (s *casApplyStore) Get(context.Context, int64) (*storage.Apply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.template
	a.State = s.state
	return &a, nil
}

func (s *casApplyStore) UpdateDerivedState(_ context.Context, _ int64, expectedState, newState, _ string, _, _ *time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != expectedState {
		return false, nil
	}
	s.state = newState
	return true, nil
}

func (s *casApplyStore) currentState() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// recoverOperationStore backs the single claimed operation through the recover
// flow: Get/ListByApply return the live row and MarkFailed transitions it.
type recoverOperationStore struct {
	storage.ApplyOperationStore
	mu sync.Mutex
	op *storage.ApplyOperation
}

func (s *recoverOperationStore) Get(context.Context, int64) (*storage.ApplyOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op := *s.op
	return &op, nil
}

func (s *recoverOperationStore) ListByApply(context.Context, int64) ([]*storage.ApplyOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op := *s.op
	return []*storage.ApplyOperation{&op}, nil
}

func (s *recoverOperationStore) MarkFailed(_ context.Context, _ int64, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.op.State = state.ApplyOperation.Failed
	s.op.ErrorMessage = errMsg
	return nil
}

func (s *recoverOperationStore) Heartbeat(context.Context, int64) error { return nil }

// recoverTestStorage wires the stores the recover flow touches, including the
// plan lookup the routing tern client requires to build.
type recoverTestStorage struct {
	mockStorage
	applies storage.ApplyStore
	ops     storage.ApplyOperationStore
}

func (s *recoverTestStorage) Applies() storage.ApplyStore                  { return s.applies }
func (s *recoverTestStorage) ApplyOperations() storage.ApplyOperationStore { return s.ops }
func (s *recoverTestStorage) Plans() storage.PlanStore                     { return &staticPlanStore{} }

// When a multi-deployment operation has no tasks, the recover flow fails it
// closed. By the time it fails, the pre-drive projection has already moved the
// durable parent apply from pending to running, so the failure projection must
// compare-and-swap against the reloaded running state — not the stale pending
// apply it started the drive with — or the parent is stranded running.
func TestRecoverMultiApplyOperation_FailsTaskLessOperationAgainstReloadedParent(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	applyStore := &casApplyStore{
		template: storage.Apply{
			ID:              7,
			ApplyIdentifier: "apply-multi-op-recover",
			Database:        "testdb",
			DatabaseType:    storage.DatabaseTypeMySQL,
			Environment:     "staging",
		},
		state: state.Apply.Pending,
	}
	opStore := &recoverOperationStore{op: &storage.ApplyOperation{
		ID:         42,
		ApplyID:    7,
		Deployment: "west",
		State:      state.ApplyOperation.Running,
	}}
	deploymentClient := &mockTernClient{resumeErr: tern.ErrNoTasksForApplyOperation}

	svc := New(
		&recoverTestStorage{applies: applyStore, ops: opStore},
		testServerConfig(),
		map[string]tern.Client{"west/staging": deploymentClient},
		logger,
	)

	svc.recoverMultiApplyOperation(t.Context(), 1, &storage.ApplyOperation{
		ID:         42,
		ApplyID:    7,
		Deployment: "west",
		State:      state.ApplyOperation.Running,
	}, storage.OperationLease{})

	assert.Equal(t, state.Apply.Failed, applyStore.currentState(),
		"the parent apply must be failed after the task-less operation is terminalized against the reloaded running state")
	assert.Equal(t, state.ApplyOperation.Failed, opStore.op.State,
		"the task-less operation row must be marked failed")
}

// cutoverOpStore backs the cutover claim path: FindNextApplyOperationCutover
// hands back the barrier-parked operation whose turn it is, ListByApply reports a
// genuine multi-operation set (so the operation-lease-only drive is valid), and
// MarkFailed terminalizes the claimed row.
type cutoverOpStore struct {
	storage.ApplyOperationStore
	mu      sync.Mutex
	op      *storage.ApplyOperation
	sibling *storage.ApplyOperation
	claimed bool
}

func (s *cutoverOpStore) FindNextApplyOperationCutover(context.Context, string) (*storage.ApplyOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimed = true
	op := *s.op
	return &op, nil
}

func (s *cutoverOpStore) Get(context.Context, int64) (*storage.ApplyOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op := *s.op
	return &op, nil
}

func (s *cutoverOpStore) ListByApply(context.Context, int64) ([]*storage.ApplyOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op := *s.op
	sibling := *s.sibling
	return []*storage.ApplyOperation{&op, &sibling}, nil
}

func (s *cutoverOpStore) MarkFailed(_ context.Context, _ int64, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.op.State = state.ApplyOperation.Failed
	s.op.ErrorMessage = errMsg
	return nil
}

func (s *cutoverOpStore) Heartbeat(context.Context, int64) error { return nil }

// The cutover claim path drives a barrier-parked operation through its swap via
// ResumeApplyOperationCutover, not the copy-phase ResumeApplyOperation, and only
// when the claimed operation belongs to a genuine multi-operation apply so the
// operation-lease-only drive (with parent-write suppression) is valid.
func TestRecoverApplyOperationCutover_RoutesThroughCutoverDrive(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	applyStore := &casApplyStore{
		template: storage.Apply{
			ID:              7,
			ApplyIdentifier: "apply-cutover",
			Database:        "testdb",
			DatabaseType:    storage.DatabaseTypeMySQL,
			Environment:     "staging",
		},
		state: state.Apply.Running,
	}
	opStore := &cutoverOpStore{
		op: &storage.ApplyOperation{
			ID:         42,
			ApplyID:    7,
			Deployment: "west",
			State:      state.ApplyOperation.CuttingOver,
			LeaseOwner: "driver-1",
			LeaseToken: "token-1",
		},
		sibling: &storage.ApplyOperation{
			ID:         41,
			ApplyID:    7,
			Deployment: "east",
			State:      state.ApplyOperation.Completed,
		},
	}
	// Fail closed on no tasks so the drive short-circuits to the task-less
	// terminalization; the routing assertion only needs the cutover entrypoint to
	// have been called.
	deploymentClient := &mockTernClient{resumeErr: tern.ErrNoTasksForApplyOperation}

	svc := New(
		&recoverTestStorage{applies: applyStore, ops: opStore},
		testServerConfig(),
		map[string]tern.Client{"west/staging": deploymentClient},
		logger,
	)

	consumed := svc.recoverApplyOperationCutover(t.Context(), 1, "driver-1")

	assert.True(t, consumed, "claiming a parked cutover must consume the tick")
	assert.True(t, opStore.claimed, "the cutover claim predicate must be queried")
	deploymentClient.resumeMu.Lock()
	cutoverID := deploymentClient.resumeCutoverOperationID
	copyID := deploymentClient.resumeOperationID
	deploymentClient.resumeMu.Unlock()
	assert.Equal(t, int64(42), cutoverID, "the operation must be driven through the cutover entrypoint")
	assert.Equal(t, int64(0), copyID, "the cutover claim must not route through the copy-phase entrypoint")
	assert.Equal(t, state.ApplyOperation.Failed, opStore.op.State,
		"the task-less cutover operation must be terminalized failed")
}

// A claimed cutover operation that is not part of a multi-operation apply must
// not be driven: the operation-lease-only path (and its parent-write
// suppression) is only correct for a genuine fan-out, so a single-operation set
// fails closed without calling any resume entrypoint.
func TestRecoverApplyOperationCutover_RejectsSingleOperationSet(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	applyStore := &casApplyStore{
		template: storage.Apply{ID: 7, ApplyIdentifier: "apply-cutover-single", Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging"},
		state:    state.Apply.Running,
	}
	opStore := &recoverOperationStore{op: &storage.ApplyOperation{
		ID:         42,
		ApplyID:    7,
		Deployment: "west",
		State:      state.ApplyOperation.CuttingOver,
		LeaseOwner: "driver-1",
		LeaseToken: "token-1",
	}}
	deploymentClient := &mockTernClient{}

	svc := New(
		&recoverTestStorage{applies: applyStore, ops: &singleCutoverOpStore{recoverOperationStore: opStore}},
		testServerConfig(),
		map[string]tern.Client{"west/staging": deploymentClient},
		logger,
	)

	consumed := svc.recoverApplyOperationCutover(t.Context(), 1, "driver-1")

	assert.True(t, consumed, "claiming any operation consumes the tick even when it is rejected")
	deploymentClient.resumeMu.Lock()
	cutoverID := deploymentClient.resumeCutoverOperationID
	copyID := deploymentClient.resumeOperationID
	deploymentClient.resumeMu.Unlock()
	assert.Equal(t, int64(0), cutoverID, "a single-operation set must not be driven through cutover")
	assert.Equal(t, int64(0), copyID, "a single-operation set must not be driven through copy")
}

// singleCutoverOpStore exposes a recoverOperationStore (single-operation
// ListByApply) through the cutover claim predicate.
type singleCutoverOpStore struct {
	*recoverOperationStore
}

func (s *singleCutoverOpStore) FindNextApplyOperationCutover(context.Context, string) (*storage.ApplyOperation, error) {
	op := *s.op
	return &op, nil
}

// expiryErrorApplyStore fails the retryable-apply expiry maintenance pass and
// records whether the claim path (FindNextApply) was still reached afterwards.
type expiryErrorApplyStore struct {
	storage.ApplyStore
	findNextCalled bool
}

func (s *expiryErrorApplyStore) ExpireRetryable(context.Context) ([]*storage.RetryableApplyExpiration, error) {
	return nil, errors.New("storage unavailable")
}

func (s *expiryErrorApplyStore) FindNextApply(context.Context, string) (*storage.Apply, error) {
	s.findNextCalled = true
	return nil, nil
}

// Retryable-apply expiry is best-effort maintenance: a storage failure there
// must not stop a driver from claiming new pending work in the same tick, or a
// transient expiry error would starve every queued apply behind it.
func TestRecoverApplies_ExpiryErrorDoesNotBlockClaim(t *testing.T) {
	applyStore := &expiryErrorApplyStore{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	claimOperations := false
	cfg := testServerConfig()
	cfg.OperatorClaimOperations = &claimOperations
	svc := New(&mockStorageWithApplyStores{applies: applyStore}, cfg, nil, logger)

	svc.recoverApplies(t.Context(), 1)

	assert.True(t, applyStore.findNextCalled,
		"FindNextApply must run even when ExpireRetryable fails")
}

// panickingResumeClient simulates an engine whose resume path hits a code or
// data fault (for example malformed stored metadata) and panics mid-drive.
type panickingResumeClient struct {
	mockTernClient
	panicValue string
}

func (c *panickingResumeClient) ResumeApply(context.Context, *storage.Apply) error {
	panic(c.panicValue)
}

func (c *panickingResumeClient) ResumeApplyOperation(context.Context, *storage.Apply, int64) error {
	panic(c.panicValue)
}

// recordingTaskStore serves a fixed task set and records state writes, so tests
// can assert what the panic containment persisted.
type recordingTaskStore struct {
	storage.TaskStore
	tasks   []*storage.Task
	updated []*storage.Task
}

func (s *recordingTaskStore) GetByApplyID(_ context.Context, applyID int64) ([]*storage.Task, error) {
	tasks := make([]*storage.Task, 0, len(s.tasks))
	for _, task := range s.tasks {
		if task.ApplyID == applyID {
			tasks = append(tasks, task)
		}
	}
	return tasks, nil
}

func (s *recordingTaskStore) GetByApplyOperationID(_ context.Context, applyOperationID int64) ([]*storage.Task, error) {
	tasks := make([]*storage.Task, 0, len(s.tasks))
	for _, task := range s.tasks {
		if task.ApplyOperationID != nil && *task.ApplyOperationID == applyOperationID {
			tasks = append(tasks, task)
		}
	}
	return tasks, nil
}

func (s *recordingTaskStore) Update(_ context.Context, task *storage.Task) error {
	s.updated = append(s.updated, task)
	return nil
}

// panicContainmentApplyStore claims a single apply, rotating a lease onto it,
// and records the terminal write the panic containment path performs. Once the
// apply is terminal it is no longer claimable, matching the durable claim
// predicate.
type panicContainmentApplyStore struct {
	storage.ApplyStore
	apply        *storage.Apply
	claims       int
	updateCalled bool
}

func (s *panicContainmentApplyStore) ExpireRetryable(context.Context) ([]*storage.RetryableApplyExpiration, error) {
	return nil, nil
}

func (s *panicContainmentApplyStore) FindNextApply(_ context.Context, owner string) (*storage.Apply, error) {
	s.claims++
	if s.apply == nil || state.IsTerminalApplyState(s.apply.State) {
		return nil, nil
	}
	s.apply.LeaseOwner = owner
	s.apply.LeaseToken = "lease-token"
	return s.apply, nil
}

func (s *panicContainmentApplyStore) Get(context.Context, int64) (*storage.Apply, error) {
	if s.apply == nil {
		return nil, nil
	}
	fresh := *s.apply
	return &fresh, nil
}

func (s *panicContainmentApplyStore) Update(_ context.Context, apply *storage.Apply) error {
	s.updateCalled = true
	s.apply = apply
	return nil
}

// staticOperationLookupStore serves one operation row for routing lookups.
type staticOperationLookupStore struct {
	storage.ApplyOperationStore
	op *storage.ApplyOperation
}

func (s *staticOperationLookupStore) Get(context.Context, int64) (*storage.ApplyOperation, error) {
	return s.op, nil
}

// A panic inside the engine drive must be contained to the claimed apply: the
// resume call returns an error instead of crashing the driver, the apply is
// marked failed (permanent) so recovery does not re-claim the poisoned row and
// panic again, and the apply's tasks are failed so dependent state can settle.
// The containment write persists the reloaded row, so a field a peer wrote
// between the claim and the panic survives instead of being clobbered by the
// claim-time snapshot.
func TestResumeClaimedApply_DrivePanicFailsApply(t *testing.T) {
	client := &panickingResumeClient{panicValue: "corrupt engine metadata"}
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-42",
		Database:        "appdb",
		Deployment:      "east",
		Environment:     "staging",
		State:           state.Apply.Running,
		LeaseOwner:      "driver-0",
		LeaseToken:      "lease-token",
	}
	applyStore := &panicContainmentApplyStore{apply: apply}
	taskStore := &recordingTaskStore{tasks: []*storage.Task{
		{ID: 9, ApplyID: 42, State: state.Task.Running},
	}}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{
		plans:   &staticPlanStore{},
		applies: applyStore,
		tasks:   taskStore,
	}, &ServerConfig{}, map[string]tern.Client{
		"east/staging": client,
	}, logger)

	ctx := storage.WithApplyLease(t.Context(), apply.Lease())
	// The drive holds the claim-time snapshot while a peer writes the stored
	// row (a skip-revert dispatch) before the panic is contained.
	claimed := *apply
	peerWrite := time.Now()
	apply.RevertSkippedAt = &peerWrite

	var resumed bool
	var err error
	require.NotPanics(t, func() {
		resumed, err = svc.resumeClaimedApply(ctx, 0, &claimed, 0, "")
	})

	require.Error(t, err)
	var drivePanic *panicsafe.Error
	require.ErrorAs(t, err, &drivePanic, "a contained drive panic must surface as a panicsafe error")
	assert.Equal(t, "corrupt engine metadata", drivePanic.Value)
	assert.False(t, resumed)

	require.True(t, applyStore.updateCalled, "the apply row must be written to its failed state")
	written := applyStore.apply
	assert.True(t, state.IsState(written.State, state.Apply.Failed),
		"the apply must be failed (permanent), not failed_retryable, so it is not re-claimed and re-panicked")
	assert.Contains(t, written.ErrorMessage, "corrupt engine metadata")
	require.NotNil(t, written.CompletedAt)
	require.NotNil(t, written.RevertSkippedAt,
		"a peer update stored between the claim and the panic must survive the containment write")

	require.Len(t, taskStore.updated, 1, "the in-flight task must be settled")
	assert.True(t, state.IsState(taskStore.updated[0].State, state.Task.Failed))
	assert.Contains(t, taskStore.updated[0].ErrorMessage, "corrupt engine metadata")
}

// A drive panic contained while holding only an operation lease fails the
// operation's tasks but leaves the parent applies row untouched: under the
// multi-operation fan-out the parent state is owned by the rollout projection,
// which settles it from the failed operation row.
func TestResumeClaimedApply_DrivePanicUnderOperationLeaseLeavesParentToProjection(t *testing.T) {
	client := &panickingResumeClient{panicValue: "corrupt operation metadata"}
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-42",
		Database:        "appdb",
		Deployment:      "east",
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	operationID := int64(7)
	applyStore := &panicContainmentApplyStore{apply: apply}
	taskStore := &recordingTaskStore{tasks: []*storage.Task{
		{ID: 9, ApplyID: 42, ApplyOperationID: &operationID, State: state.Task.Running},
	}}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{
		plans:   &staticPlanStore{},
		applies: applyStore,
		tasks:   taskStore,
		operations: &staticOperationLookupStore{op: &storage.ApplyOperation{
			ID:         operationID,
			ApplyID:    42,
			Deployment: "east",
			State:      state.ApplyOperation.Running,
		}},
	}, &ServerConfig{}, map[string]tern.Client{
		"east/staging": client,
	}, logger)

	ctx := storage.WithOperationLease(t.Context(), storage.OperationLease{
		ApplyID:     42,
		OperationID: operationID,
		Owner:       "driver-0",
		Token:       "op-token",
	})
	var resumed bool
	var err error
	require.NotPanics(t, func() {
		resumed, err = svc.resumeClaimedApply(ctx, 0, apply, operationID, "east")
	})

	var drivePanic *panicsafe.Error
	require.ErrorAs(t, err, &drivePanic)
	assert.False(t, resumed)

	assert.False(t, applyStore.updateCalled,
		"an operation-lease-only drive must not write the parent applies row; the rollout projection owns it")
	assert.True(t, state.IsState(apply.State, state.Apply.Running))

	require.Len(t, taskStore.updated, 1, "the operation's task must be failed so the operation row can settle")
	assert.True(t, state.IsState(taskStore.updated[0].State, state.Task.Failed))
	assert.Contains(t, taskStore.updated[0].ErrorMessage, "corrupt operation metadata")
}

// One poisoned apply must degrade only itself: the drive tick that claims it
// contains the panic and marks the apply failed, and the driver keeps claiming
// fresh work on subsequent ticks instead of crashing the process.
func TestDriveTick_ContainsDrivePanicAndKeepsClaiming(t *testing.T) {
	client := &panickingResumeClient{panicValue: "corrupt engine metadata"}
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-42",
		Database:        "appdb",
		Deployment:      "east",
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	applyStore := &panicContainmentApplyStore{apply: apply}
	taskStore := &recordingTaskStore{tasks: []*storage.Task{
		{ID: 9, ApplyID: 42, State: state.Task.Running},
	}}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	claimOperations := false
	cfg := testServerConfig()
	cfg.OperatorClaimOperations = &claimOperations
	svc := New(&mockStorageWithApplyStores{
		plans:   &staticPlanStore{},
		applies: applyStore,
		tasks:   taskStore,
	}, cfg, map[string]tern.Client{
		"east/staging": client,
	}, logger)

	require.NotPanics(t, func() { svc.driveTick(t.Context(), 0) })
	assert.Equal(t, 1, applyStore.claims)
	assert.True(t, state.IsState(applyStore.apply.State, state.Apply.Failed),
		"the poisoned apply must be failed so it is not re-claimed")

	require.NotPanics(t, func() { svc.driveTick(t.Context(), 0) })
	assert.Equal(t, 2, applyStore.claims,
		"the driver must keep claiming after containing the panic, and the failed apply must not be claimable")
}

// panickingClaimApplyStore simulates a panic in the claim machinery itself
// (outside the engine drive), counting claim attempts.
type panickingClaimApplyStore struct {
	storage.ApplyStore
	claims int
}

func (s *panickingClaimApplyStore) ExpireRetryable(context.Context) ([]*storage.RetryableApplyExpiration, error) {
	return nil, nil
}

func (s *panickingClaimApplyStore) FindNextApply(context.Context, string) (*storage.Apply, error) {
	s.claims++
	panic("claim scan hit a corrupt row")
}

// A panic in the claim machinery itself (outside the engine drive) must not
// kill the driver goroutine: the tick boundary contains it and the driver
// polls again on the next tick.
func TestDriveTick_ContainsClaimPanic(t *testing.T) {
	applyStore := &panickingClaimApplyStore{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	claimOperations := false
	cfg := testServerConfig()
	cfg.OperatorClaimOperations = &claimOperations
	svc := New(&mockStorageWithApplyStores{applies: applyStore}, cfg, nil, logger)

	require.NotPanics(t, func() { svc.driveTick(t.Context(), 0) })
	require.NotPanics(t, func() { svc.driveTick(t.Context(), 0) })
	assert.Equal(t, 2, applyStore.claims, "the driver must keep polling after each contained panic")
}
