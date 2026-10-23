package tern

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// activeAppliesDelta sums the active-apply gauge across every series the test
// recorded, which is the net change the code under test made to it.
func activeAppliesDelta(t *testing.T, reader *metric.ManualReader) int64 {
	t.Helper()
	var total int64
	for _, p := range collectCounterPoints(t, reader, "schemabot.active_applies") {
		total += p.Value
	}
	return total
}

// An apply is counted on the active-apply gauge from acceptance until it
// settles, and a stopped apply is still in progress: it waits on an operator's
// start or cancel. So stopping a running apply leaves the count alone, and the
// cancel that settles a stopped apply is what releases it.
//
// Each transition is judged from the state storage holds when the command
// lands, which is what keeps a retryable pause balanced. A drive delivers the
// command after claiming the apply, and that claim moves a paused apply to
// running while the operator counts it again, so the command finds it running
// (the first two cases). The pause cases pin the transition on its own: a pause
// was already released, so cancelling it releases nothing more and stopping it
// counts it again.
func TestControlTransitionsMoveTheActiveAppliesGauge(t *testing.T) {
	tests := []struct {
		name      string
		fromState string
		mark      func(c *LocalClient, t *testing.T, applyID int64) error
		wantDelta int64
	}{
		{name: "stopping a running apply keeps it counted", fromState: state.Apply.Running, mark: markStopped, wantDelta: 0},
		{name: "cancelling a running apply releases it", fromState: state.Apply.Running, mark: markCancelled, wantDelta: -1},
		{name: "cancelling a stopped apply releases it", fromState: state.Apply.Stopped, mark: markCancelled, wantDelta: -1},
		{name: "cancelling a retryable pause releases nothing", fromState: state.Apply.FailedRetryable, mark: markCancelled, wantDelta: 0},
		{name: "stopping a retryable pause counts it again", fromState: state.Apply.FailedRetryable, mark: markStopped, wantDelta: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := newTernMetricsReader(t)
			apply := failureLogTestApply(tt.fromState, 0)
			client, _ := newFailureLogTestClient(apply, nil)

			require.NoError(t, tt.mark(client, t, apply.ID))

			assert.Equal(t, tt.wantDelta, activeAppliesDelta(t, reader))
		})
	}
}

func markStopped(c *LocalClient, t *testing.T, applyID int64) error {
	return c.markApplyStopped(t.Context(), applyID)
}

func markCancelled(c *LocalClient, t *testing.T, applyID int64) error {
	return c.markApplyCancelled(t.Context(), applyID)
}

// A sequential drive that records its own outcome releases the apply once it
// settles, and keeps a stopped outcome counted, so the start that later resumes
// it and the outcome that ends it leave the gauge balanced.
func TestFinalizeSequentialApplyReleasesTheGaugeOnlyWhenTheApplySettles(t *testing.T) {
	tests := []struct {
		name          string
		stoppedByUser bool
		wantDelta     int64
	}{
		{name: "a stopped outcome stays counted", stoppedByUser: true, wantDelta: 0},
		{name: "a completed outcome releases the apply", wantDelta: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := newTernMetricsReader(t)
			apply := failureLogTestApply(state.Apply.Running, 0)
			task := &storage.Task{ID: 1, TaskIdentifier: "task-1", ApplyID: apply.ID, TableName: "orders", State: state.Task.Completed}
			if tt.stoppedByUser {
				task.State = state.Task.Stopped
			}
			client, _ := newFailureLogTestClient(apply, []*storage.Task{task})

			require.NoError(t, client.finalizeSequentialApply(t.Context(), apply, []*storage.Task{task}, nil, tt.stoppedByUser))

			assert.Equal(t, tt.wantDelta, activeAppliesDelta(t, reader))
		})
	}
}

// A gRPC drive adopts the data plane's terminal state. A stopped one is not
// settled, so it stays counted; the outcome the resumed apply later reaches
// releases it, once.
func TestPersistTerminalStateFromRemoteKeepsAStoppedApplyCounted(t *testing.T) {
	tests := []struct {
		name        string
		remoteState string
		wantDelta   int64
	}{
		{name: "a stopped data plane apply stays counted", remoteState: state.Apply.Stopped, wantDelta: 0},
		{name: "a completed data plane apply releases it", remoteState: state.Apply.Completed, wantDelta: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := newTernMetricsReader(t)
			stored := failureLogTestApply(state.Apply.Running, 0)
			client := &GRPCClient{
				logger: slog.Default(),
				storage: &mockStorage{
					applies:         &mockApplyStore{apply: stored},
					logs:            &mockApplyLogStore{},
					controlRequests: &testControlRequestStore{},
				},
			}
			remote := *stored
			remote.State = tt.remoteState

			require.NoError(t, client.persistTerminalStateFromRemote(t.Context(), stored, &remote, time.Now(), applyTaskScope{}))

			assert.Equal(t, tt.wantDelta, activeAppliesDelta(t, reader))
		})
	}
}

// A grouped drive records the apply's settled state from the engine's progress.
// A stopped engine run leaves the apply stopped and counted; a completed one
// releases it.
func TestGroupedProgressTickReleasesTheGaugeOnlyWhenTheApplySettles(t *testing.T) {
	tests := []struct {
		name        string
		engineState engine.State
		taskState   string
		wantState   string
		wantDelta   int64
	}{
		{name: "a stopped engine run stays counted", engineState: engine.StateStopped, taskState: state.Task.Stopped, wantState: state.Apply.Stopped, wantDelta: 0},
		{name: "a completed engine run releases the apply", engineState: engine.StateCompleted, taskState: state.Task.Completed, wantState: state.Apply.Completed, wantDelta: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := newTernMetricsReader(t)
			opID := int64(1)
			apply := &storage.Apply{ID: 42, ApplyIdentifier: "apply-grouped-gauge", Database: "testdb", State: state.Apply.Running}
			tasks := []*storage.Task{{
				TaskIdentifier: "task-users", ApplyID: apply.ID, ApplyOperationID: &opID,
				State: state.Task.Running, TableName: "users", Namespace: "testdb",
			}}
			applies := &snapshotApplyStore{stored: *apply}
			client := &LocalClient{
				storage: &exactProgressStorage{
					applies:         applies,
					tasks:           &exactProgressTaskStore{tasks: tasks},
					logs:            &mockApplyLogStore{},
					controlRequests: &testControlRequestStore{},
					applyOperations: &listApplyOperationStore{ops: []*storage.ApplyOperation{{ID: opID, State: state.ApplyOperation.Running}}},
				},
				logger: slog.Default(),
			}
			eng := &fakeControlEngine{progressResult: &engine.ProgressResult{
				State:  tt.engineState,
				Tables: []engine.TableProgress{{Namespace: "testdb", Table: "users", State: tt.taskState, Progress: 100}},
			}}

			done := client.handleAtomicProgressTick(t.Context(), eng, apply, tasks, &engine.Credentials{}, nil,
				&atomicPollState{lastProgressLog: time.Now()}, apply.GetOptions().Map(), false)

			require.True(t, done)
			require.Equal(t, tt.wantState, applies.stored.State)
			assert.Equal(t, tt.wantDelta, activeAppliesDelta(t, reader))
		})
	}
}

// A retry claim counts a paused apply again, so each pause must release it. A
// drive pauses the apply for retry and the write commits, but its reply is
// lost. When storage holds the pause under this drive's lease, the drive
// releases the gauge exactly once, logs the retry, and sends the progress
// update, so the gauge is back where it was before the claim. When the pause
// is stored under another lease, the write is not this drive's: the drive
// returns the write error and the gauge keeps the claim's count.
func TestPauseWithALostReplyReleasesTheGaugeOnce(t *testing.T) {
	const failure = "engine lost its connection to the target"
	tests := []struct {
		name         string
		storedToken  string
		wantErr      bool
		wantDelta    int64
		wantProgress int
	}{
		{name: "pause stored under this drive's lease", storedToken: "tok-a", wantDelta: 0, wantProgress: 1},
		{name: "pause stored under another lease", storedToken: "tok-b", wantErr: true, wantDelta: 1, wantProgress: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := newTernMetricsReader(t)
			stored := failureLogTestApply(state.Apply.Running, 1)
			stored.LeaseToken = tt.storedToken
			writeErr := errors.New("connection reset by peer")
			applies := &lostReplyApplyStore{snapshotApplyStore: &snapshotApplyStore{stored: *stored}, err: writeErr}
			task := &storage.Task{ID: 1, TaskIdentifier: "task-1", ApplyID: stored.ID, TableName: "orders", State: state.Task.Running}
			logs := &mockApplyLogStore{}
			client := &LocalClient{
				config: LocalConfig{Database: "orders", Type: storage.DatabaseTypeMySQL},
				storage: &exactProgressStorage{
					applies:         applies,
					tasks:           &exactProgressTaskStore{tasks: []*storage.Task{task}},
					logs:            logs,
					controlRequests: &testControlRequestStore{},
				},
				logger: slog.Default(),
			}
			observer := &progressRecordingObserver{}
			client.SetObserver(stored.ID, observer)
			ctx := storage.WithApplyLease(t.Context(), storage.ApplyLease{ApplyID: stored.ID, Owner: "driver-a", Token: "tok-a"})
			// The retry claim that admitted this drive counted the apply.
			metrics.AdjustActiveApplies(ctx, 1, stored.Database, stored.Deployment, stored.Environment)

			drive := *stored
			drive.LeaseToken = tt.storedToken
			err := client.markApplyRetryableWithTasks(ctx, &drive, []*storage.Task{task}, failure)

			assert.Equal(t, state.Apply.FailedRetryable, applies.stored.State, "the pause committed either way")
			if tt.wantErr {
				require.ErrorIs(t, err, writeErr)
				assert.Equal(t, state.Apply.Running, drive.State)
				assert.Empty(t, logs.logs)
			} else {
				require.NoError(t, err)
				assert.Equal(t, state.Apply.FailedRetryable, drive.State, "the drive adopts the pause storage holds")
				assert.True(t, hasLogMessageContaining(logs.logs, "Apply paused for operator retry"), "the retry is recorded in the apply log")
			}
			assert.Len(t, observer.progress, tt.wantProgress)
			assert.Equal(t, tt.wantDelta, activeAppliesDelta(t, reader))
		})
	}
}

// An operator starts a stopped grouped apply and its resume finds nothing left
// to do, so it records the apply completed. The stopped apply was still
// counted, so that completion releases it.
func TestGroupedResumeWithNoRemainingWorkReleasesTheStoppedApply(t *testing.T) {
	reader := newTernMetricsReader(t)
	eng := &replanSequenceEngine{plans: []*engine.PlanResult{usersEmailChange(), {}}}
	r := newGroupedStartResume(t, eng, func(requests *testControlRequestStore) storage.ControlRequestStore { return requests })

	require.NoError(t, r.resume(t))

	require.Equal(t, state.Apply.Completed, r.applies.stored.State)
	assert.Equal(t, int64(-1), activeAppliesDelta(t, reader))
}

// A cancel that arrives after every task already settled derives the apply's
// state from its tasks, and that settled state releases the apply.
func TestCancelOverSettledTasksReleasesTheGauge(t *testing.T) {
	reader := newTernMetricsReader(t)
	apply := &storage.Apply{
		ID: 42, ApplyIdentifier: "apply-cancel-all-terminal", State: state.Apply.Running,
		Database: "testdb", DatabaseType: storage.DatabaseTypeVitess, Environment: "staging",
	}
	task := &storage.Task{
		ID: 7, ApplyID: apply.ID, TaskIdentifier: "task-done", Database: "testdb",
		Namespace: "testdb", TableName: "users", State: state.Task.Completed,
	}
	client := newVitessControlTestClient(apply, []*storage.Task{task}, nil, &controlCaptureEngine{})

	_, err := client.settleCancelAllTasksTerminal(t.Context(), apply.ID, 1, "operator")

	require.NoError(t, err)
	require.Equal(t, state.Apply.Completed, apply.State)
	assert.Equal(t, int64(-1), activeAppliesDelta(t, reader))
}

// A drive releases the active-apply gauge only for an outcome that leaves the
// counted states, and only when the apply it claimed had not settled already:
// a settled claim was released when it settled, and a stopped outcome stays
// counted until the apply settles.
func TestReleaseActiveApplyForDriveOutcomeSkipsCountedOutcomesAndSettledClaims(t *testing.T) {
	tests := []struct {
		name         string
		claimedState string
		outcome      string
		wantDelta    int64
	}{
		{name: "a settled outcome of a running claim releases", claimedState: state.Apply.Running, outcome: state.Apply.Completed, wantDelta: -1},
		{name: "a settled outcome of a stopped claim releases", claimedState: state.Apply.Stopped, outcome: state.Apply.Cancelled, wantDelta: -1},
		{name: "an outcome that is still counted releases nothing", claimedState: state.Apply.Running, outcome: state.Apply.Stopped, wantDelta: 0},
		{name: "a claim that had already settled releases nothing", claimedState: state.Apply.Completed, outcome: state.Apply.Cancelled, wantDelta: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := newTernMetricsReader(t)
			apply := failureLogTestApply(tt.outcome, 0)

			releaseActiveApplyForDriveOutcome(t.Context(), apply, tt.claimedState)

			assert.Equal(t, tt.wantDelta, activeAppliesDelta(t, reader))
		})
	}
}
