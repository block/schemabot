package postgres

import (
	"context"
	"errors"
	"fmt"
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
	// cancelBuild, when set, replaces the default CancelBuild so a test can
	// script what one signal attempt does and when it returns.
	cancelBuild func(ctx context.Context) error
}

func (f *fakeBuildTracker) Progress(context.Context) (progress.Snapshot, error) {
	return f.snapshot, nil
}

func (f *fakeBuildTracker) CancelBuild(ctx context.Context) error {
	if f.cancelBuild != nil {
		return f.cancelBuild(ctx)
	}
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

// requestedCancel reports what the drive would read when classifying a
// cancellation it sees.
func requestedCancel(t *testing.T, eng *Engine) bool {
	t.Helper()
	return eng.cancelRequested(cancelTestKey)
}

// settleCancelledBuild settles the drive as the real drive does when its build
// comes back cancelled: as the operator's cancel only when the engine reads a
// cancel as requested, and otherwise as a failure the next drive retries.
func (d *fakeDrive) settleCancelledBuild() {
	if d.eng.cancelRequested(d.key) {
		d.settle(engine.StateCancelled, "")
		return
	}
	d.settle(engine.StateFailed, "concurrent index build cancelled from outside SchemaBot")
}

func (d *fakeDrive) publishedState() engine.State {
	d.eng.mu.Lock()
	defer d.eng.mu.Unlock()
	return d.eng.progress[d.key].result.State
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

// An operator cancels a concurrent index build and the signal reaches the
// build backend, but the caller's deadline expires before the reply comes
// back. The caller hears an error, yet the build returns cancelled by that
// signal, and the drive records the operator's cancel rather than a failure
// for the next drive to retry.
func TestCancelStaysRequestedWhenTheCallerGivesUpAfterSignalling(t *testing.T) {
	ctx, giveUp := context.WithCancel(t.Context())
	defer giveUp()
	tracker := &fakeBuildTracker{}
	tracker.cancelBuild = func(context.Context) error {
		tracker.cancelled.Store(true)
		giveUp()
		return fmt.Errorf("cancel concurrent index build backend 4242: read reply: %w", context.DeadlineExceeded)
	}
	eng, drive := runningDrive(t, tracker, true)

	result, err := eng.Cancel(ctx, cancelRequest())

	assert.Nil(t, result)
	require.ErrorIs(t, err, context.Canceled)
	assert.False(t, drive.wasCancelled(), "a caller that gave up does not fall back to the drive's context")
	assert.True(t, requestedCancel(t, eng), "a signal that may have landed keeps the cancel requested")
	drive.settleCancelledBuild()
	assert.Equal(t, engine.StateCancelled, drive.publishedState())
}

// A caller that gives up after the tracker positively reports that nothing was
// signalled leaves no cancel recorded, so a backend cancellation from outside
// SchemaBot is still a failure the next drive retries.
func TestCancelIsNotRequestedWhenTheCallerGivesUpWithNothingSignalled(t *testing.T) {
	ctx, giveUp := context.WithCancel(t.Context())
	defer giveUp()
	tracker := &fakeBuildTracker{}
	tracker.cancelBuild = func(context.Context) error {
		giveUp()
		return fmt.Errorf("cancel concurrent index build backend 4242: %w", progress.ErrBuildNotRunning)
	}
	eng, drive := runningDrive(t, tracker, true)

	result, err := eng.Cancel(ctx, cancelRequest())

	assert.Nil(t, result)
	require.ErrorIs(t, err, progress.ErrBuildNotRunning)
	assert.False(t, drive.wasCancelled())
	assert.False(t, requestedCancel(t, eng))
}

// Two cancels for the same build overlap. The first signals the build
// backend; a retry whose caller gives up finds nothing it could signal. The
// build comes back cancelled by the first signal — whether or not the first
// call has heard back yet — and both the drive and the first caller report the
// operator's cancel: the retry that sent nothing does not withdraw it.
func TestCancelThatSendsNothingDoesNotWithdrawAnOverlappingCancel(t *testing.T) {
	tests := []struct {
		name string
		// buildReturnsInFlight has the build come back cancelled while the
		// first call is still waiting for its signal's reply.
		buildReturnsInFlight bool
	}{
		{name: "first signal still awaiting its reply", buildReturnsInFlight: true},
		{name: "first signal acknowledged and waiting to settle", buildReturnsInFlight: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			firstEntered, retryReturned := make(chan struct{}), make(chan struct{})
			retryCtx, giveUp := context.WithCancel(t.Context())
			defer giveUp()
			tracker := &fakeBuildTracker{}
			eng, drive := runningDrive(t, tracker, true)
			var calls atomic.Int32
			tracker.cancelBuild = func(context.Context) error {
				if calls.Add(1) > 1 {
					giveUp()
					return fmt.Errorf("cancel concurrent index build backend 4242: %w", progress.ErrBuildNotRunning)
				}
				close(firstEntered)
				if tc.buildReturnsInFlight {
					<-retryReturned
					drive.settleCancelledBuild()
				}
				return nil
			}

			type outcome struct {
				result *engine.ControlResult
				err    error
			}
			first := make(chan outcome, 1)
			go func() {
				result, err := eng.Cancel(t.Context(), cancelRequest())
				first <- outcome{result: result, err: err}
			}()
			select {
			case <-firstEntered:
			case <-time.After(backgroundApplyDeadline):
				require.FailNow(t, "the first cancel never reached the tracker")
			}

			_, err := eng.Cancel(retryCtx, cancelRequest())
			require.ErrorIs(t, err, progress.ErrBuildNotRunning)
			close(retryReturned)
			if !tc.buildReturnsInFlight {
				drive.settleCancelledBuild()
			}

			var got outcome
			select {
			case got = <-first:
			case <-time.After(backgroundApplyDeadline):
				require.FailNow(t, "the first cancel never settled")
			}
			require.NoError(t, got.err)
			assert.True(t, got.result.Accepted)
			assert.Equal(t, "Concurrent index build cancelled", got.result.Message)
			assert.Equal(t, engine.StateCancelled, drive.publishedState())
			assert.True(t, requestedCancel(t, eng))
		})
	}
}
