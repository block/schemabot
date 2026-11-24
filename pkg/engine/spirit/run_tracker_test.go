package spirit

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// runTrackerTestDeadline bounds a test's wait for a run to return.
const runTrackerTestDeadline = 10 * time.Second

// startTestRun starts run as rm's tracked run the way Apply and Start do:
// claimed under the engine lock.
func startTestRun(eng *Engine, rm *runningSchemaChange, run func()) {
	eng.mu.Lock()
	defer eng.mu.Unlock()
	rm.run.start(run)
}

// currentRunExit returns the handle on rm's current run, read under the
// engine lock.
func currentRunExit(eng *Engine, rm *runningSchemaChange) runExit {
	eng.mu.Lock()
	defer eng.mu.Unlock()
	return rm.run.exit()
}

// testRunAlive reports whether rm's current run has not returned yet.
func testRunAlive(eng *Engine, rm *runningSchemaChange) bool {
	return !currentRunExit(eng, rm).returned()
}

// waitForTestRun waits for rm's current run to return, failing the test if it
// has not returned within runTrackerTestDeadline.
func waitForTestRun(t *testing.T, eng *Engine, rm *runningSchemaChange) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), runTrackerTestDeadline)
	defer cancel()
	require.NoError(t, currentRunExit(eng, rm).wait(ctx), "the run did not return")
}

// stopWithExpiringWait sends a stop whose caller gives up almost at once, so
// the stop's wait ends at its caller's context while the runner is still
// alive, and checks that the stop says so.
func stopWithExpiringWait(t *testing.T, eng *Engine) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	_, err := eng.Stop(ctx, &engine.ControlRequest{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Contains(t, err.Error(), "the runner has not returned")
}

// A stop whose wait ends at its caller's context leaves the change stopped
// with its runner alive. Once that runner returns, a start resumes the change
// in a new run, possibly while the stop that gave up is still unwinding its
// wait. Each run is tracked on its own, so the new run never shares anything
// with that wait: the start is accepted and its run returns cleanly, every
// time, under the race detector as well.
func TestStartAfterAStopWhoseWaitExpiredResumesInARunOfItsOwn(t *testing.T) {
	for range 20 {
		eng := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
		rm := registerRunningSchemaChange(eng)
		rm.cancelFunc = func() {}
		release := make(chan struct{})
		startTestRun(eng, rm, func() { <-release })

		stopWithExpiringWait(t, eng)

		close(release)
		waitForTestRun(t, eng, rm)

		result, err := eng.Start(t.Context(), &engine.ControlRequest{})
		require.NoError(t, err, "the start is accepted once the stopped runner has returned")
		require.NotNil(t, result)
		assert.Equal(t, "Resumed from checkpoint", result.Message)
		// The resumed run fails at the connection because this fixture runs no
		// target; wait it out so it does not outlive the iteration.
		waitForTestRun(t, eng, rm)
	}
}

// After a stop whose wait ended at its caller's context, the change is marked
// stopped while the runner it interrupted is still returning, and that runner
// can still change the target on its way out. Progress reports the change as
// running with a message saying it is stopping, so no caller reads the stop as
// settled; once the runner has returned, progress reports it stopped.
func TestProgressReportsAStopWaitingForItsRunnerAsRunningUntilTheRunnerReturns(t *testing.T) {
	eng := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	rm := registerRunningSchemaChange(eng)
	rm.cancelFunc = func() {}
	release := make(chan struct{})
	var releaseRunner sync.Once
	t.Cleanup(func() { releaseRunner.Do(func() { close(release) }) })
	startTestRun(eng, rm, func() { <-release })

	stopWithExpiringWait(t, eng)

	stopping := pollProgress(t, eng)
	assert.Equal(t, engine.StateRunning, stopping.State, "a stop whose runner is still alive is not settled")
	assert.False(t, stopping.State.IsTerminal())
	assert.Equal(t, stoppingMessage, stopping.Message)

	releaseRunner.Do(func() { close(release) })
	waitForTestRun(t, eng, rm)

	stopped := pollProgress(t, eng)
	assert.Equal(t, engine.StateStopped, stopped.State, "the stop is settled once the runner has returned")
	assert.Equal(t, "Schema change stopped", stopped.Message)
}

// Two starts can reach a stopped change at once, as when a start is resent
// while the first is still being served. Exactly one of them resumes the
// change in a new run; the other finds it already running, so the change is
// never run twice over on the same tables.
func TestConcurrentStartsOnAStoppedChangeResumeItOnce(t *testing.T) {
	host, _ := silentTarget(t)
	for range 50 {
		eng := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
		rm := &runningSchemaChange{
			database:       "testdb",
			tables:         []string{"users"},
			tableNamespace: map[string]string{},
			originalDDLs:   []string{connectPhaseAlter},
			state:          engine.StateStopped,
			host:           host,
			username:       "root",
		}
		eng.installRunningSchemaChange(rm)

		type startOutcome struct {
			result *engine.ControlResult
			err    error
		}
		outcomes := make(chan startOutcome, 2)
		ready := make(chan struct{})
		var starts sync.WaitGroup
		for range 2 {
			starts.Go(func() {
				<-ready
				result, err := eng.Start(t.Context(), &engine.ControlRequest{})
				outcomes <- startOutcome{result: result, err: err}
			})
		}
		close(ready)
		starts.Wait()
		close(outcomes)

		messages := map[string]int{}
		for outcome := range outcomes {
			require.NoError(t, outcome.err)
			require.NotNil(t, outcome.result)
			assert.True(t, outcome.result.Accepted)
			messages[outcome.result.Message]++
		}
		assert.Equal(t, map[string]int{"Resumed from checkpoint": 1, "Already running": 1}, messages,
			"exactly one start resumes the change")

		// The resumed run is connecting to a target that never answers; cancel
		// it and wait it out so it does not outlive the iteration.
		eng.mu.Lock()
		cancelRun := rm.cancelFunc
		eng.mu.Unlock()
		require.NotNil(t, cancelRun)
		cancelRun()
		waitForTestRun(t, eng, rm)
	}
}

// A drain waits for the change's run to return and then releases the change.
// A start can resume the change in a new run in between. The drain then waits
// for the resumed run within its own deadline, and keeps tracking the change
// while that run works on the target, so an ownership halt still reaches it
// and the caller does not proceed as if the target had drained. Once the
// resumed run returns, the drain releases the change.
func TestDrainWaitsForARunResumedBetweenItsWaitAndItsRelease(t *testing.T) {
	eng := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	rm := registerRunningSchemaChange(eng)
	startTestRun(eng, rm, func() {})
	waitForTestRun(t, eng, rm)

	release := make(chan struct{})
	var releaseRun sync.Once
	t.Cleanup(func() { releaseRun.Do(func() { close(release) }) })
	var resumeOnce sync.Once
	eng.mu.Lock()
	eng.drainRaceWindow = func() {
		resumeOnce.Do(func() { startTestRun(eng, rm, func() { <-release }) })
	}
	eng.mu.Unlock()

	shortCtx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err := eng.DrainContext(shortCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "the drain waits for the resumed run")
	assert.Contains(t, err.Error(), "still running")

	eng.mu.Lock()
	tracked := eng.runningSchemaChange
	eng.mu.Unlock()
	assert.Same(t, rm, tracked, "the change stays tracked while the resumed run is working")
	assert.True(t, testRunAlive(eng, rm))

	releaseRun.Do(func() { close(release) })
	ctx, cancelDrain := context.WithTimeout(t.Context(), runTrackerTestDeadline)
	defer cancelDrain()
	require.NoError(t, eng.DrainContext(ctx))

	eng.mu.Lock()
	tracked = eng.runningSchemaChange
	eng.mu.Unlock()
	assert.Nil(t, tracked, "the change is released once the resumed run has returned")
}
