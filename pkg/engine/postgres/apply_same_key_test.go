package postgres

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/block/pg-sprite/pkg/progress"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

const (
	sameKeyFirstDDL  = "ALTER TABLE public.users ADD COLUMN email text"
	sameKeySecondDDL = "ALTER TABLE public.users ADD COLUMN phone text"
)

// waitSignalHandler reports on a channel each time an apply logs that it is
// waiting for the previous drive under its identity, so a test knows the apply
// has reached the wait without sleeping.
type waitSignalHandler struct {
	slog.Handler
	waiting chan struct{}
}

func (h waitSignalHandler) Handle(ctx context.Context, record slog.Record) error {
	if strings.Contains(record.Message, "waiting for the previous drive") {
		select {
		case h.waiting <- struct{}{}:
		default:
		}
	}
	return h.Handler.Handle(ctx, record)
}

func newWaitSignalLogger() (*slog.Logger, chan struct{}) {
	waiting := make(chan struct{}, 1)
	return slog.New(waitSignalHandler{Handler: slog.NewTextHandler(io.Discard, nil), waiting: waiting}), waiting
}

func awaitWaitSignal(t *testing.T, waiting <-chan struct{}) {
	t.Helper()
	select {
	case <-waiting:
	case <-time.After(backgroundApplyDeadline):
		require.Fail(t, "the second apply never reached the wait for the running drive")
	}
}

type applyOutcome struct {
	result *engine.ApplyResult
	err    error
}

// applyAsync runs Apply on its own goroutine and reports its outcome on the
// returned channel.
func applyAsync(ctx context.Context, eng *Engine, req *engine.ApplyRequest) <-chan applyOutcome {
	outcome := make(chan applyOutcome, 1)
	go func() {
		result, err := eng.Apply(ctx, req)
		outcome <- applyOutcome{result: result, err: err}
	}()
	return outcome
}

func awaitApplyOutcome(t *testing.T, outcome <-chan applyOutcome) applyOutcome {
	t.Helper()
	select {
	case got := <-outcome:
		return got
	case <-time.After(backgroundApplyDeadline):
		require.Fail(t, "Apply did not return")
		return applyOutcome{}
	}
}

func requireNoExecutorStart(t *testing.T, scripted *scriptedExecutor, message string) {
	t.Helper()
	select {
	case <-scripted.envelopes:
		require.Fail(t, message)
	default:
	}
}

func sameKeyApplyRequest(resume *engine.ResumeState, ddl string, logger *slog.Logger) *engine.ApplyRequest {
	return &engine.ApplyRequest{
		Database: "app",
		Changes: []engine.SchemaChange{{
			Namespace:    "public",
			TableChanges: []engine.TableChange{{Table: "users", DDL: ddl}},
		}},
		Credentials: &engine.Credentials{DSN: "postgres://schemabot:secret@db.invalid/app?sslmode=disable"},
		ResumeState: resume,
		Logger:      logger,
	}
}

// engineWithExecutors returns an engine whose executor for each statement is
// the scripted one registered under that statement. Every executor is released
// and the engine drained when the test ends, in that order, so no parked drive
// outlives the test.
func engineWithExecutors(t *testing.T, byDDL map[string]*scriptedExecutor) *Engine {
	t.Helper()
	eng := New()
	eng.execute = func(ctx context.Context, conn targetConn, change nativeApply, limit int64, tracker *progress.Tracker, logger *slog.Logger) error {
		scripted, ok := byDDL[change.sql]
		if !ok {
			return errors.New("no scripted executor for " + change.sql)
		}
		return scripted.execute(ctx, conn, change, limit, tracker, logger)
	}
	t.Cleanup(eng.Drain)
	t.Cleanup(func() {
		for _, scripted := range byDDL {
			scripted.release()
		}
	})
	return eng
}

func finishingExecutor() *scriptedExecutor {
	return newScriptedExecutor(func(tracker *progress.Tracker) error {
		tracker.Finish(nil)
		return nil
	})
}

// Two applies under one identity never run together. An operator retries a
// schema change while its first drive is still parked in the executor: the
// second apply must not claim the identity or start its executor until the
// first drive has exited, and the first drive's terminal result must not land
// in the second apply's progress entry.
func TestApplyWaitsForTheRunningDriveUnderTheSameKey(t *testing.T) {
	first, second := finishingExecutor(), finishingExecutor()
	eng := engineWithExecutors(t, map[string]*scriptedExecutor{sameKeyFirstDDL: first, sameKeySecondDDL: second})
	const key = "task-a"
	resume := &engine.ResumeState{MigrationContext: key}

	accepted, err := eng.Apply(t.Context(), sameKeyApplyRequest(resume, sameKeyFirstDDL, slog.New(slog.NewTextHandler(io.Discard, nil))))
	require.NoError(t, err)
	require.True(t, accepted.Accepted)
	firstTracker := first.tracker(t)

	logger, waiting := newWaitSignalLogger()
	outcome := applyAsync(t.Context(), eng, sameKeyApplyRequest(resume, sameKeySecondDDL, logger))
	awaitWaitSignal(t, waiting)

	select {
	case <-outcome:
		require.Fail(t, "the second apply returned while the first drive was still running")
	default:
	}
	requireNoExecutorStart(t, second, "the second apply started its executor while the first drive was still running")
	eng.mu.Lock()
	tracked := eng.progress[key]
	eng.mu.Unlock()
	require.NotNil(t, tracked)
	require.Same(t, firstTracker, tracked.tracker, "the waiting apply must leave the running drive's entry alone")

	first.release()
	got := awaitApplyOutcome(t, outcome)
	require.NoError(t, got.err)
	require.True(t, got.result.Accepted)
	secondTracker := second.tracker(t)

	eng.mu.Lock()
	tracked = eng.progress[key]
	eng.mu.Unlock()
	require.NotNil(t, tracked)
	require.Same(t, secondTracker, tracked.tracker, "after the first drive exits the second apply owns the entry")
	progressNow := pollProgress(t, eng, key)
	assert.Equal(t, engine.StateRunning, progressNow.State,
		"the first drive's terminal result must not make the second apply look finished")

	second.release()
	require.Eventually(t, func() bool {
		return pollProgress(t, eng, key).State.IsTerminal()
	}, backgroundApplyDeadline, 10*time.Millisecond)
	assert.Equal(t, engine.StateCompleted, pollProgress(t, eng, key).State)
}

// A second apply whose caller gives up while it waits for the running drive is
// refused and leaves no trace: the running drive's entry is untouched, the
// second executor never starts, and the engine has no extra drive to wait for.
func TestApplyWaitEndsWithTheCallersContext(t *testing.T) {
	first, second := finishingExecutor(), finishingExecutor()
	eng := engineWithExecutors(t, map[string]*scriptedExecutor{sameKeyFirstDDL: first, sameKeySecondDDL: second})
	const key = "task-a"
	resume := &engine.ResumeState{MigrationContext: key}

	_, err := eng.Apply(t.Context(), sameKeyApplyRequest(resume, sameKeyFirstDDL, slog.New(slog.NewTextHandler(io.Discard, nil))))
	require.NoError(t, err)
	firstTracker := first.tracker(t)
	eng.mu.Lock()
	before := eng.progress[key]
	eng.mu.Unlock()
	require.NotNil(t, before)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	logger, waiting := newWaitSignalLogger()
	outcome := applyAsync(ctx, eng, sameKeyApplyRequest(resume, sameKeySecondDDL, logger))
	awaitWaitSignal(t, waiting)
	cancel()

	got := awaitApplyOutcome(t, outcome)
	require.Error(t, got.err)
	assert.Nil(t, got.result)
	assert.ErrorIs(t, got.err, context.Canceled)
	assert.ErrorContains(t, got.err, `wait for the previous PostgreSQL schema change "task-a" to exit`)

	eng.mu.Lock()
	after := eng.progress[key]
	eng.mu.Unlock()
	require.Same(t, before, after, "a refused apply must not replace the running drive's entry")
	require.Same(t, firstTracker, after.tracker)
	assert.Equal(t, engine.StateRunning, pollProgress(t, eng, key).State)
	requireNoExecutorStart(t, second, "a refused apply must not start its executor")

	first.release()
	drainCtx, cancelDrain := context.WithTimeout(t.Context(), backgroundApplyDeadline)
	defer cancelDrain()
	require.NoError(t, eng.DrainContext(drainCtx),
		"the refused apply must leave no drive behind for the engine to wait on")
	requireNoExecutorStart(t, second, "a refused apply must not start its executor")
	assert.Equal(t, engine.StatePending, pollProgress(t, eng, key).State, "the drain clears the first drive's entry")
}

// Applies under different identities still run side by side: waiting is per
// key, never engine-wide.
func TestApplyDoesNotWaitForADriveUnderAnotherKey(t *testing.T) {
	first, second := finishingExecutor(), finishingExecutor()
	eng := engineWithExecutors(t, map[string]*scriptedExecutor{sameKeyFirstDDL: first, sameKeySecondDDL: second})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	_, err := eng.Apply(t.Context(), sameKeyApplyRequest(&engine.ResumeState{MigrationContext: "task-a"}, sameKeyFirstDDL, logger))
	require.NoError(t, err)
	first.tracker(t)

	ctx, cancel := context.WithTimeout(t.Context(), backgroundApplyDeadline)
	defer cancel()
	_, err = eng.Apply(ctx, sameKeyApplyRequest(&engine.ResumeState{MigrationContext: "task-b"}, sameKeySecondDDL, logger))
	require.NoError(t, err, "an apply under another identity must not wait for the running drive")
	second.tracker(t)

	assert.Equal(t, engine.StateRunning, pollProgress(t, eng, "task-a").State)
	assert.Equal(t, engine.StateRunning, pollProgress(t, eng, "task-b").State)
}

// An apply that names no identity is refused: Progress and Cancel could never
// find it, so a schema change accepted that way could not be polled or
// cancelled.
func TestApplyRefusesARequestWithoutAnIdentity(t *testing.T) {
	tests := []struct {
		name   string
		resume *engine.ResumeState
	}{
		{name: "no resume state", resume: nil},
		{name: "empty identity", resume: &engine.ResumeState{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scripted := finishingExecutor()
			eng := engineWithExecutors(t, map[string]*scriptedExecutor{sameKeyFirstDDL: scripted})

			result, err := eng.Apply(t.Context(), sameKeyApplyRequest(tt.resume, sameKeyFirstDDL, slog.New(slog.NewTextHandler(io.Discard, nil))))
			require.Error(t, err)
			assert.Nil(t, result)
			assert.ErrorContains(t, err, "PostgreSQL apply request names no apply")
			requireNoExecutorStart(t, scripted, "a refused apply must not start its executor")
			eng.mu.Lock()
			tracked := len(eng.progress)
			eng.mu.Unlock()
			assert.Zero(t, tracked, "a refused apply must not be tracked")
		})
	}
}

// waitHookHandler runs onWait each time an apply logs that it is waiting for
// the previous drive, before the apply reaches its wait.
type waitHookHandler struct {
	slog.Handler
	onWait func()
}

func (h waitHookHandler) Handle(ctx context.Context, record slog.Record) error {
	if strings.Contains(record.Message, "waiting for the previous drive") {
		h.onWait()
	}
	return h.Handler.Handle(ctx, record)
}

// A drive being brought down cancels its context while the engine halts the
// older drive it was waiting on, so both are ready when the waiting apply
// wakes. Whichever it wakes on, the caller has ended, and no drive is started
// for it. The cancellation and the older drive's exit both land before the
// wait, in every round, so a waiter that skips the check after waking is
// accepted in most rounds.
func TestApplyWhoseWaitEndsAsTheRunningDriveExitsStartsNoDrive(t *testing.T) {
	const rounds = 40
	const key = "task-a"
	started := 0
	for range rounds {
		first, second := finishingExecutor(), finishingExecutor()
		eng := engineWithExecutors(t, map[string]*scriptedExecutor{sameKeyFirstDDL: first, sameKeySecondDDL: second})
		resume := &engine.ResumeState{MigrationContext: key}
		_, err := eng.Apply(t.Context(), sameKeyApplyRequest(resume, sameKeyFirstDDL, slog.New(slog.NewTextHandler(io.Discard, nil))))
		require.NoError(t, err)
		first.tracker(t)

		ctx, cancel := context.WithCancel(t.Context())
		logger := slog.New(waitHookHandler{Handler: slog.NewTextHandler(io.Discard, nil), onWait: func() {
			cancel()
			first.release()
			eng.mu.Lock()
			done := eng.progress[key].done
			eng.mu.Unlock()
			<-done
		}})
		got, err := eng.Apply(ctx, sameKeyApplyRequest(resume, sameKeySecondDDL, logger))
		if err == nil && got != nil && got.Accepted {
			started++
		} else {
			require.ErrorIs(t, err, context.Canceled)
		}
		cancel()
	}
	assert.Zero(t, started, "drives started for a caller whose wait had ended")
}

const sameKeyThirdDDL = "ALTER TABLE public.users ADD COLUMN nickname text"

// Two applies waiting on one running drive wake together when it exits. One
// claims the identity, and the other waits again for that new drive rather
// than starting a second drive beside it.
func TestApplyWaitersClaimTheIdentityOneAtATime(t *testing.T) {
	first, second, third := finishingExecutor(), finishingExecutor(), finishingExecutor()
	eng := engineWithExecutors(t, map[string]*scriptedExecutor{sameKeyFirstDDL: first, sameKeySecondDDL: second, sameKeyThirdDDL: third})
	resume := &engine.ResumeState{MigrationContext: "task-a"}
	_, err := eng.Apply(t.Context(), sameKeyApplyRequest(resume, sameKeyFirstDDL, slog.New(slog.NewTextHandler(io.Discard, nil))))
	require.NoError(t, err)
	first.tracker(t)

	loggerB, waitingB := newWaitSignalLogger()
	loggerC, waitingC := newWaitSignalLogger()
	outB := applyAsync(t.Context(), eng, sameKeyApplyRequest(resume, sameKeySecondDDL, loggerB))
	outC := applyAsync(t.Context(), eng, sameKeyApplyRequest(resume, sameKeyThirdDDL, loggerC))
	awaitWaitSignal(t, waitingB)
	awaitWaitSignal(t, waitingC)

	first.release()
	var winner, loser *scriptedExecutor
	var loserWaiting <-chan struct{}
	var loserOutcome <-chan applyOutcome
	select {
	case got := <-outB:
		require.NoError(t, got.err)
		winner, loser, loserWaiting, loserOutcome = second, third, waitingC, outC
	case got := <-outC:
		require.NoError(t, got.err)
		winner, loser, loserWaiting, loserOutcome = third, second, waitingB, outB
	case <-time.After(backgroundApplyDeadline):
		require.Fail(t, "neither waiting apply was accepted")
	}
	winner.tracker(t)
	awaitWaitSignal(t, loserWaiting)
	requireNoExecutorStart(t, loser, "the second waiter started a drive beside the first")
	select {
	case <-loserOutcome:
		require.Fail(t, "the second waiter returned while the new drive was running")
	default:
	}

	winner.release()
	got := awaitApplyOutcome(t, loserOutcome)
	require.NoError(t, got.err)
	loser.tracker(t)
}
