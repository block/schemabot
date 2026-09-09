package postgres

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/pg-sprite/pkg/progress"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

type fakeBuildTracker struct {
	snapshot  progress.Snapshot
	cancelErr error
	cancelled atomic.Bool
}

func (f *fakeBuildTracker) Progress(context.Context) (progress.Snapshot, error) {
	return f.snapshot, nil
}

func (f *fakeBuildTracker) CancelBuild(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.cancelled.Store(true)
	return f.cancelErr
}

// fakeDrive stands in for the background apply goroutine behind one tracked
// apply: it records whether the engine cancelled the drive's context and lets
// the test publish the terminal result the drive would have settled on.
type fakeDrive struct {
	eng       *Engine
	key       string
	ctx       context.Context
	done      chan struct{}
	cancelled chan struct{}
}

const cancelTestKey = "apply-1"

// runningDrive tracks one running apply of the given shape whose executor is
// the fake tracker, and returns the drive the test settles.
func runningDrive(t *testing.T, tracker buildTracker, concurrentIndex bool) (*Engine, *fakeDrive) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	drive := &fakeDrive{key: cancelTestKey, ctx: ctx, done: make(chan struct{}), cancelled: make(chan struct{})}
	eng := &Engine{progress: map[string]*trackedApply{
		cancelTestKey: {
			result: &engine.ProgressResult{State: engine.StateRunning}, tracker: tracker,
			logger: slog.New(slog.DiscardHandler), concurrentIndex: concurrentIndex,
			cancelApply: func() {
				cancel()
				select {
				case <-drive.cancelled:
				default:
					close(drive.cancelled)
				}
			},
			done: drive.done,
		},
	}}
	drive.eng = eng
	return eng, drive
}

// settle publishes the drive's terminal result and closes its done channel.
func (d *fakeDrive) settle(state engine.State, detail string) {
	d.eng.mu.Lock()
	d.eng.progress[d.key].result = &engine.ProgressResult{State: state, ErrorMessage: detail}
	d.eng.mu.Unlock()
	close(d.done)
}

// settleWhenCancelled settles the drive as the executor would once the engine
// cancels the drive's context.
func (d *fakeDrive) settleWhenCancelled(state engine.State) {
	go func() {
		<-d.ctx.Done()
		d.settle(state, "")
	}()
}

// settleOnSignal settles the drive as the executor would once the tracker
// signalled the build backend.
func (d *fakeDrive) settleOnSignal(t *testing.T, tracker *fakeBuildTracker, state engine.State) {
	go func() {
		assert.Eventually(t, tracker.cancelled.Load, backgroundApplyDeadline, time.Millisecond)
		d.settle(state, "")
	}()
}

func (d *fakeDrive) wasCancelled() bool {
	select {
	case <-d.cancelled:
		return true
	default:
		return false
	}
}

func cancelRequest() *engine.ControlRequest {
	return &engine.ControlRequest{ResumeState: &engine.ResumeState{MigrationContext: cancelTestKey}}
}

func requestedCancel(t *testing.T, eng *Engine) bool {
	t.Helper()
	eng.mu.Lock()
	defer eng.mu.Unlock()
	return eng.progress[cancelTestKey].cancelRequested
}

// A cancel that reaches the running build signals its backend, leaves the
// drive's own context alone, and reports once the drive publishes the
// cancelled outcome.
func TestCancelSignalsTheBuildAndReportsTheDriveOutcome(t *testing.T) {
	tracker := &fakeBuildTracker{}
	eng, drive := runningDrive(t, tracker, true)
	drive.settleOnSignal(t, tracker, engine.StateCancelled)

	result, err := eng.Cancel(t.Context(), cancelRequest())
	require.NoError(t, err)
	assert.True(t, result.Accepted)
	assert.True(t, tracker.cancelled.Load())
	assert.False(t, drive.wasCancelled(), "a signalled build needs no context cancellation")
	assert.True(t, requestedCancel(t, eng))
}

// When the tracker cannot put a signal on a running statement — no build
// tracked yet, a build that already returned, a backend this role cannot
// observe, or a reserved session that failed — the engine cancels the drive's
// own context instead of declining, and still answers from the drive's outcome.
func TestCancelFallsBackToTheApplyContextWhenTheBuildCannotBeSignalled(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "no active build", err: progress.ErrNoActiveBuild},
		{name: "build not running", err: progress.ErrBuildNotRunning},
		{name: "build unobservable", err: progress.ErrBuildUnobservable},
		{name: "reserved session failed", err: errors.New("connection lost")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tracker := &fakeBuildTracker{cancelErr: tc.err}
			eng, drive := runningDrive(t, tracker, true)
			drive.settleWhenCancelled(engine.StateCancelled)

			result, err := eng.Cancel(t.Context(), cancelRequest())
			require.NoError(t, err)
			assert.True(t, result.Accepted)
			assert.True(t, drive.wasCancelled())
			assert.True(t, requestedCancel(t, eng), "the flag stays set: a signal that failed late may still have landed")
		})
	}
}

// A cancel whose signal arrives after the build has finished is answered by
// what landed: a completed change is reported as completed, never recorded
// cancelled on the strength of a signal that reached nothing.
func TestCancelReportsACompletionThatBeatTheSignal(t *testing.T) {
	tracker := &fakeBuildTracker{cancelErr: progress.ErrNoActiveBuild}
	eng, drive := runningDrive(t, tracker, true)
	drive.settleWhenCancelled(engine.StateCompleted)

	result, err := eng.Cancel(t.Context(), cancelRequest())

	assert.Nil(t, result)
	require.Error(t, err)
	assert.True(t, engine.IsAlreadyCompleted(err))
	assert.False(t, engine.IsUnsupportedOperation(err))
}

// A cancel over a change that has already failed has nothing left to stop: it
// is accepted, and the failure the change settled on travels with the answer.
func TestCancelIsAcceptedOverAFailedChange(t *testing.T) {
	tracker := &fakeBuildTracker{}
	eng, drive := runningDrive(t, tracker, true)
	drive.settleOnSignal(t, tracker, engine.StateFailed)

	result, err := eng.Cancel(t.Context(), cancelRequest())
	require.NoError(t, err)
	assert.True(t, result.Accepted)
	assert.Contains(t, result.Message, "already failed")
}

// A cancel that arrives after the drive published answers from that result
// without signalling anything.
func TestCancelAnswersFromATerminalResult(t *testing.T) {
	tests := []struct {
		name          string
		state         engine.State
		wantAccepted  bool
		wantCompleted bool
	}{
		{name: "cancelled", state: engine.StateCancelled, wantAccepted: true},
		{name: "failed", state: engine.StateFailed, wantAccepted: true},
		{name: "completed", state: engine.StateCompleted, wantCompleted: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tracker := &fakeBuildTracker{}
			eng, drive := runningDrive(t, tracker, true)
			drive.settle(tc.state, "")

			result, err := eng.Cancel(t.Context(), cancelRequest())

			assert.False(t, tracker.cancelled.Load())
			assert.False(t, drive.wasCancelled())
			if tc.wantCompleted {
				require.Error(t, err)
				assert.True(t, engine.IsAlreadyCompleted(err))
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantAccepted, result.Accepted)
		})
	}
}

// Plain DDL is never cancellable on PostgreSQL: its statements commit or fail
// on their own, so the decline is typed for the durable request to resolve
// terminally, and nothing is signalled or recorded on the apply.
func TestCancelRefusesPlainDDL(t *testing.T) {
	tracker := &fakeBuildTracker{}
	eng, drive := runningDrive(t, tracker, false)

	result, err := eng.Cancel(t.Context(), cancelRequest())

	assert.Nil(t, result)
	require.Error(t, err)
	assert.True(t, engine.IsUnsupportedOperation(err))
	assert.False(t, tracker.cancelled.Load())
	assert.False(t, drive.wasCancelled())
	assert.False(t, requestedCancel(t, eng))
}

// A cancel for an apply this engine does not track is permanent: the engine
// tracks its applies in this process, so retrying cannot make one appear.
func TestCancelWithNoTrackedApplyIsPermanent(t *testing.T) {
	eng := New()

	result, err := eng.Cancel(t.Context(), cancelRequest())

	assert.Nil(t, result)
	require.Error(t, err)
	assert.False(t, engine.IsRetryable(err))
	assert.False(t, engine.IsUnsupportedOperation(err))
}

// A caller that has already given up sends nothing: the apply is untouched
// and the cancel flag does not outlive the attempt.
func TestCancelSendsNothingWhenTheCallerHasGivenUp(t *testing.T) {
	tracker := &fakeBuildTracker{}
	eng, drive := runningDrive(t, tracker, true)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	result, err := eng.Cancel(ctx, cancelRequest())

	assert.Nil(t, result)
	require.ErrorIs(t, err, context.Canceled)
	assert.False(t, tracker.cancelled.Load())
	assert.False(t, drive.wasCancelled())
	assert.False(t, requestedCancel(t, eng))
}

// A signalled build that does not settle within the caller's deadline is
// reported as still running, not as cancelled; the flag stays set so the
// drive's eventual exit still reads as this cancel.
func TestCancelReportsABuildThatHasNotSettled(t *testing.T) {
	tracker := &fakeBuildTracker{}
	eng, drive := runningDrive(t, tracker, true)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	result, err := eng.Cancel(ctx, cancelRequest())

	assert.Nil(t, result)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Contains(t, err.Error(), "has not settled")
	assert.True(t, tracker.cancelled.Load())
	assert.False(t, drive.wasCancelled())
	assert.True(t, requestedCancel(t, eng))
}
