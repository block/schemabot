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

	assert.Equal(t, window, outage.remaining(start, window), "no open run leaves the whole window")
	assert.False(t, outage.recordFailure(start, start.Add(time.Second), window), "the first failure opens the run")
	assert.Equal(t, time.Second, outage.failingFor(start.Add(time.Second)), "the run is measured from when its first read started")
	assert.False(t, outage.recordFailure(start.Add(58*time.Second), start.Add(59*time.Second), window), "a run shorter than the window keeps the drive")
	assert.Equal(t, 2, outage.failures)
	assert.Equal(t, time.Second, outage.remaining(start.Add(59*time.Second), window))
	assert.Equal(t, time.Second, outage.nextPollWait(10*time.Second, start.Add(59*time.Second), window), "the next read is not scheduled past the window")
	assert.Equal(t, 500*time.Millisecond, outage.nextPollWait(500*time.Millisecond, start.Add(59*time.Second), window))
	assert.True(t, outage.recordFailure(start.Add(59*time.Second), start.Add(window), window), "a run as long as the window hands the apply back")
	assert.Zero(t, outage.remaining(start.Add(2*window), window))

	outage.recordSuccess()
	assert.Zero(t, outage.failures)
	assert.Zero(t, outage.failingFor(start.Add(2*window)))
	assert.False(t, outage.recordFailure(start.Add(2*window), start.Add(2*window), window), "a run after a success is measured from its own first failure")
}

// A first read that hangs for the whole window is cut off at the window and
// counts in full, so a drive whose heartbeat still succeeds cannot sit on a
// hung read past the window.
func TestControlReadOutageMeasuresAHungFirstReadFromItsStart(t *testing.T) {
	var outage controlReadOutage
	start := time.Unix(1_700_000_000, 0)
	assert.True(t, outage.recordFailure(start, start.Add(time.Minute), time.Minute))
}

// A read that hangs is bounded by the outage window rather than by the
// drive's context, so the drive hands the apply back once the window passes
// even though nothing cancels the drive.
func TestPollTaskToCompletionHandsBackWhenAControlRequestReadHangsPastTheWindow(t *testing.T) {
	controlRequests := &hangingControlRequestStore{ControlRequestStore: &testControlRequestStore{}}
	eng := &phaseSequenceEngine{results: []*engine.ProgressResult{
		{State: engine.StateRunning, Tables: []engine.TableProgress{{Namespace: "orders", Table: "line_items", State: state.Task.Running, Progress: 40}}},
	}}
	client, apply, task := controlReadOutageSequentialFixture(controlRequests, eng)
	client.controlReadOutageWindowOverride = 50 * time.Millisecond

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	start := time.Now()
	action := client.pollTaskToCompletion(ctx, apply, task, nil, nil)

	assert.Equal(t, taskHandover, action)
	require.NoError(t, ctx.Err(), "the window ended the read, not the test's deadline")
	assert.Less(t, time.Since(start), 5*time.Second, "the hung read was cut off at the window")
	assert.Positive(t, controlRequests.reads.Load())
	assert.Zero(t, eng.calls)
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
	stateBeforeHandBack := f.apply.State
	assert.False(t, f.tick(t), "the first failure of a new run keeps the drive")
	f.ps.controlReadOutage.since = time.Now().Add(-time.Hour)
	assert.True(t, f.tick(t), "a run that has lasted the window stops the drive")
	require.NoError(t, f.ps.terminalErr, "the apply is handed back, not settled")
	assert.Equal(t, stateBeforeHandBack, f.apply.State, "the hand-back writes no state")
}

// alternatingControlRequestStore lets every other read succeed, so a grouped
// tick reads its pending requests before the progress check and then fails
// the read after the progress sync, on every tick.
type alternatingControlRequestStore struct {
	*testControlRequestStore
	mu    sync.Mutex
	reads int
}

func (s *alternatingControlRequestStore) ListPending(ctx context.Context, applyID int64) ([]*storage.ApplyControlRequest, error) {
	s.mu.Lock()
	s.reads++
	failing := s.reads%2 == 0
	s.mu.Unlock()
	if failing {
		return nil, errors.New("terminating connection due to administrator command")
	}
	return s.testControlRequestStore.ListPending(ctx, applyID)
}

// A grouped tick acts on revert handling and settlement only after the read
// that follows the progress sync. When that read fails on every tick while
// the one before the progress check succeeds, the run of failures still
// grows and reaches the window, so a queued request is never left unread
// while the lease stays fresh.
func TestHandleAtomicProgressTickHandsBackWhenTheReadAfterTheProgressSyncKeepsFailing(t *testing.T) {
	f := newRevertWindowTickFixture(storage.ApplyOptions{})
	f.ps.stateEnteredAt = time.Now()
	f.client.storage.(*exactProgressStorage).controlRequests = &alternatingControlRequestStore{testControlRequestStore: f.controlRequests}
	f.client.progressPollJitter = func(int64) int64 { return 0 }
	f.client.controlReadOutageWindowOverride = time.Hour

	assert.False(t, f.tick(t))
	assert.Equal(t, 1, f.ps.controlReadOutage.failures)
	assert.False(t, f.tick(t))
	assert.Equal(t, 2, f.ps.controlReadOutage.failures, "the successful read before the progress check does not end the run")

	f.ps.controlReadOutage.since = time.Now().Add(-time.Hour)
	assert.True(t, f.tick(t), "the run reaches the window and the drive hands the apply back")
	require.NoError(t, f.ps.terminalErr)
}

// cancellingControlRequestStore ends the drive's context during the pending
// read, standing in for a drive cancelled by shutdown or a lost lease while
// storage is failing.
type cancellingControlRequestStore struct {
	*testControlRequestStore
	cancel context.CancelFunc
	reads  int
}

func (s *cancellingControlRequestStore) ListPending(ctx context.Context, applyID int64) ([]*storage.ApplyControlRequest, error) {
	s.reads++
	s.cancel()
	<-ctx.Done()
	return nil, ctx.Err()
}

// A sequential drive cancelled while it reads its pending control requests
// hands the apply back at once rather than counting the failed read toward
// an outage it would then ride out.
func TestPollTaskToCompletionHandsBackWhenTheDriveIsCancelledDuringAControlRead(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	controlRequests := &cancellingControlRequestStore{testControlRequestStore: &testControlRequestStore{}, cancel: cancel}
	eng := &phaseSequenceEngine{results: []*engine.ProgressResult{
		{State: engine.StateRunning, Tables: []engine.TableProgress{{Namespace: "orders", Table: "line_items", State: state.Task.Running, Progress: 40}}},
	}}
	client, apply, task := controlReadOutageSequentialFixture(controlRequests, eng)
	client.controlReadOutageWindowOverride = time.Hour

	action := client.pollTaskToCompletion(ctx, apply, task, nil, nil)

	assert.Equal(t, taskHandover, action)
	assert.Equal(t, 1, controlRequests.reads, "the drive does not read again once it is cancelled")
	assert.Zero(t, eng.calls)
	assert.Equal(t, state.Task.Running, task.State, "the task is left for the next driver")
}

// A grouped drive cancelled while it reads its pending control requests stops
// polling without counting the read as a failure of storage.
func TestHandleAtomicProgressTickStopsWhenTheDriveIsCancelledDuringAControlRead(t *testing.T) {
	f := newRevertWindowTickFixture(storage.ApplyOptions{})
	f.ps.stateEnteredAt = time.Now()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	f.client.storage.(*exactProgressStorage).controlRequests = &cancellingControlRequestStore{testControlRequestStore: f.controlRequests, cancel: cancel}
	f.client.controlReadOutageWindowOverride = time.Hour

	assert.True(t, f.client.handleAtomicProgressTick(ctx, f.eng, f.apply, f.tasks, &engine.Credentials{}, nil, f.ps, f.apply.GetOptions().Map(), false))
	assert.Zero(t, f.ps.controlReadOutage.failures, "a cancelled read is not an outage")
	assert.Zero(t, f.eng.progressCalls)
	require.NoError(t, f.ps.terminalErr)
}

// The read a drive held at a deferred cutover makes is cut off at what is
// left of the outage window and counts toward the run, so a read that hangs
// while the heartbeat succeeds cannot keep the drive held past the window.
func TestParkedPollReadHeldIsBoundedByTheOutageWindow(t *testing.T) {
	apply := &storage.Apply{ID: 9, ApplyIdentifier: "apply-held"}
	store := &mockStorage{controlRequests: &hangingControlRequestStore{ControlRequestStore: &testControlRequestStore{}}}
	p := newParkedPoll()
	now := time.Now()
	require.True(t, p.runFullPoll(t.Context(), store, apply, now, slog.Default()))
	p.hold()

	var outage controlReadOutage
	start := time.Now()
	assert.True(t, p.runFullPollWithin(t.Context(), store, apply, now.Add(time.Second), slog.Default(), &outage, 50*time.Millisecond),
		"a read that timed out runs the full poll")
	assert.Less(t, time.Since(start), 5*time.Second, "the hung read was cut off at the window")
	assert.Equal(t, 1, outage.failures, "the timed-out read counts toward the run")
	assert.True(t, outage.recordFailure(start, time.Now(), 50*time.Millisecond), "the full poll's own read finds the window spent")
}
