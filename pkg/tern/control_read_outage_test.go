package tern

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// outageControlRequestStore fails pending-request reads while its outage
// lasts, standing in for a storage connection that drops and comes back.
type outageControlRequestStore struct {
	*testControlRequestStore
	mu sync.Mutex
	// failReads is how many more reads fail; a negative value fails every read.
	failReads int
	reads     int
}

func (s *outageControlRequestStore) ListPending(ctx context.Context, applyID int64) ([]*storage.ApplyControlRequest, error) {
	s.mu.Lock()
	s.reads++
	failing := s.failReads != 0
	if s.failReads > 0 {
		s.failReads--
	}
	s.mu.Unlock()
	if failing {
		return nil, errors.New("terminating connection due to administrator command")
	}
	return s.testControlRequestStore.ListPending(ctx, applyID)
}

func (s *outageControlRequestStore) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

// A run of failed reads hands the apply back only once it has lasted the
// whole window, measured from the first failure, and a successful read starts
// the measurement over.
func TestControlReadOutageHandsBackAfterTheWindow(t *testing.T) {
	var outage controlReadOutage
	start := time.Unix(1_700_000_000, 0)
	window := time.Minute

	assert.False(t, outage.recordFailure(start, window), "the first failure opens the run")
	assert.False(t, outage.recordFailure(start.Add(59*time.Second), window), "a run shorter than the window keeps the drive")
	assert.Equal(t, 2, outage.failures)
	assert.Equal(t, 59*time.Second, outage.failingFor(start.Add(59*time.Second)))
	assert.True(t, outage.recordFailure(start.Add(window), window), "a run as long as the window hands the apply back")

	outage.recordSuccess()
	assert.Zero(t, outage.failures)
	assert.Zero(t, outage.failingFor(start.Add(2*window)))
	assert.False(t, outage.recordFailure(start.Add(2*window), window), "a run after a success is measured from its own first failure")
}

// The window a drive rides out matches the lease staleness window, the same
// one its heartbeat and a peer's claim measure against (OW-3).
func TestControlReadOutageWindowIsTheLeaseStalenessWindow(t *testing.T) {
	assert.Equal(t, storage.ApplyLeaseStaleAfter, (&LocalClient{}).controlReadOutageWindow())
	assert.Equal(t, time.Second, (&LocalClient{controlReadOutageWindowOverride: time.Second}).controlReadOutageWindow())
}

func controlReadOutageSequentialFixture(controlRequests storage.ControlRequestStore, eng engine.Engine) (*LocalClient, *storage.Apply, *storage.Task) {
	task := &storage.Task{
		ID: 1, ApplyID: 1, TaskIdentifier: "task-control-read-outage",
		Database: "orders", TableName: "line_items", State: state.Task.Running,
	}
	apply := &storage.Apply{
		ID: 1, ApplyIdentifier: "apply-control-read-outage", Database: "orders",
		Environment: "staging", State: state.Apply.Running,
	}
	client := &LocalClient{
		customEngine:             eng,
		taskPollIntervalOverride: 5 * time.Millisecond,
		progressPollJitter:       func(int64) int64 { return 0 },
		storage: &exactProgressStorage{
			applies:         &exactProgressApplyStore{apply: apply},
			tasks:           &exactProgressTaskStore{tasks: []*storage.Task{task}},
			controlRequests: controlRequests,
			logs:            &mockApplyLogStore{},
		},
		logger: slog.Default(),
	}
	return client, apply, task
}

// A sequential drive is building an index when storage drops its connection
// for a few polls. The pending control requests cannot be read, but nothing
// was lost: a request waits in storage until a drive reads it. The drive keeps
// the build running, reads again at the next poll, and finishes the task once
// storage answers, instead of exiting and halting the build.
func TestPollTaskToCompletionRidesOutAControlRequestReadOutage(t *testing.T) {
	controlRequests := &outageControlRequestStore{testControlRequestStore: &testControlRequestStore{}, failReads: 3}
	eng := &phaseSequenceEngine{results: []*engine.ProgressResult{
		{State: engine.StateCompleted, Tables: []engine.TableProgress{{Namespace: "orders", Table: "line_items", State: state.Task.Completed, Progress: 100}}},
	}}
	client, apply, task := controlReadOutageSequentialFixture(controlRequests, eng)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	action := client.pollTaskToCompletion(ctx, apply, task, nil, nil)

	assert.Equal(t, taskContinue, action, "the drive finishes the task once storage answers")
	assert.Equal(t, state.Task.Completed, task.State)
	assert.Equal(t, 4, controlRequests.readCount(), "three failed reads, then the read that succeeds")
	assert.Equal(t, 1, eng.calls, "no progress check runs on a poll whose control requests were unreadable")
}

// When the pending control requests stay unreadable for the whole lease
// staleness window, the drive can no longer promise to deliver an operator's
// command, so it hands the apply back for another driver to claim rather than
// polling on without ever seeing one.
func TestPollTaskToCompletionHandsBackAfterAControlRequestReadOutageOutlastsTheWindow(t *testing.T) {
	controlRequests := &outageControlRequestStore{testControlRequestStore: &testControlRequestStore{}, failReads: -1}
	eng := &phaseSequenceEngine{results: []*engine.ProgressResult{
		{State: engine.StateRunning, Tables: []engine.TableProgress{{Namespace: "orders", Table: "line_items", State: state.Task.Running, Progress: 40}}},
	}}
	client, apply, task := controlReadOutageSequentialFixture(controlRequests, eng)
	client.controlReadOutageWindowOverride = 50 * time.Millisecond

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	action := client.pollTaskToCompletion(ctx, apply, task, nil, nil)

	assert.Equal(t, taskHandover, action, "the apply is handed back, not abandoned")
	require.NoError(t, ctx.Err(), "the drive gave up on the window, not on the test's deadline")
	assert.Greater(t, controlRequests.readCount(), 1, "the drive read again before giving up")
	assert.Zero(t, eng.calls, "no progress check runs while the control requests are unreadable")
	assert.Equal(t, state.Task.Running, task.State, "the task is left for the next driver")
}

// A grouped drive skips the rest of a tick whose pending control requests it
// could not read, keeps polling while the run of failed reads is inside the
// lease staleness window, and stops polling once the run has lasted the whole
// window. A successful read ends the run.
func TestHandleAtomicProgressTickRidesOutAControlRequestReadOutage(t *testing.T) {
	f := newRevertWindowTickFixture(storage.ApplyOptions{})
	f.ps.stateEnteredAt = time.Now()
	outage := &outageControlRequestStore{testControlRequestStore: f.controlRequests, failReads: -1}
	f.client.storage.(*exactProgressStorage).controlRequests = outage
	f.client.progressPollJitter = func(int64) int64 { return 0 }
	f.client.controlReadOutageWindowOverride = time.Hour

	assert.False(t, f.tick(t), "a failed read inside the window keeps the drive polling")
	assert.Equal(t, 1, f.ps.controlReadOutage.failures)
	assert.Zero(t, f.eng.progressCalls, "the tick stops before the progress check")
	assert.False(t, f.tick(t))
	assert.Equal(t, 2, f.ps.controlReadOutage.failures)
	assert.Equal(t, 2*f.client.taskPollInterval(), f.ps.nextPollWait, "the next poll backs off")

	outage.mu.Lock()
	outage.failReads = 0
	outage.mu.Unlock()
	assert.False(t, f.tick(t), "an open revert window keeps the drive polling")
	assert.Zero(t, f.ps.controlReadOutage.failures, "a successful read ends the run")
	assert.Equal(t, 1, f.eng.progressCalls, "the tick reaches the progress check once the read succeeds")

	outage.mu.Lock()
	outage.failReads = -1
	outage.mu.Unlock()
	f.client.controlReadOutageWindowOverride = time.Nanosecond
	stateBeforeHandBack := f.apply.State
	assert.False(t, f.tick(t), "the first failure of a new run keeps the drive")
	assert.True(t, f.tick(t), "a run that has lasted the window stops the drive")
	require.NoError(t, f.ps.terminalErr, "the apply is handed back, not settled")
	assert.Equal(t, stateBeforeHandBack, f.apply.State, "the hand-back writes no state")
}
