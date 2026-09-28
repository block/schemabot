package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/block/pg-sprite/pkg/executor"
	"github.com/block/pg-sprite/pkg/progress"

	"github.com/block/schemabot/pkg/engine"
)

// cancelSettleTimeout bounds how long Cancel waits for a signalled drive to
// publish its outcome. A cancelled build returns as soon as the server aborts
// its statement; what follows is the executor's detached catalog verdict,
// which runs under its own statement timeout, the removal of the invalid
// index the build left behind, which runs detached under
// cancelledIndexCleanupBound, and the terminal publish. The settle wait
// must hold all three, which is why the cleanup bound is kept well inside
// it. A drive that has not settled by then is reported as still running
// rather than as cancelled, and the next cancel attempt waits for it again.
const cancelSettleTimeout = 2 * time.Minute

// Cancel ends this apply's concurrent index build and answers with the
// outcome the drive settles on. The build backend is signalled through the
// apply's pg-sprite tracker; when that signal cannot reach a running
// statement — the build has not started, has already returned, or its
// backend is not observable or signallable by this role — the drive's own
// context is cancelled instead, which ends whatever statement it is on. The
// durable control request remains the source of operator intent; the
// tracked apply's cancelRequested only tells the drive that the cancellation
// it sees is this one.
//
// Cancel does not report before the drive has published: a signal that races
// the build's completion may find the index already valid, and that apply is
// complete, not cancelled. Only the drive's terminal result decides, so a
// cancel that beats the outcome is answered from it and a cancel that arrives
// after it is answered the same way.
func (e *Engine) Cancel(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	key := ""
	if req != nil {
		key = progressIdentity(req.ResumeState)
	}

	e.mu.Lock()
	tracked := e.progress[key]
	if tracked == nil {
		e.mu.Unlock()
		// The engine tracks its applies in this process, so with nothing
		// tracked there is no change this instance could ever cancel;
		// retrying cannot make one appear.
		return nil, engine.NewPermanentError("no active PostgreSQL schema change to cancel for apply %q", key)
	}
	if tracked.result.State.IsTerminal() {
		e.mu.Unlock()
		return e.settledCancel(key, tracked)
	}
	if !tracked.concurrentIndex {
		e.mu.Unlock()
		return nil, engine.NewUnsupportedOperationError("cancel is supported for PostgreSQL concurrent index builds only: this schema change runs as plain DDL that commits or fails on its own")
	}
	tracker, logger, cancelApply, done := tracked.tracker, tracked.logger, tracked.cancelApply, tracked.done
	if err := ctx.Err(); err != nil {
		e.mu.Unlock()
		// A caller that has already given up sends nothing and records
		// nothing on the apply.
		logger.Info("PostgreSQL cancel not attempted: the caller gave up before the build was signalled",
			"task_id", key, "reason", err)
		return nil, fmt.Errorf("cancel PostgreSQL concurrent index build for apply %q: caller gave up before signalling: %w", key, err)
	}
	// The intent is visible to the drive before the signal goes out, since the
	// build can return cancelled before CancelBuild does.
	tracked.cancelsInFlight++
	e.mu.Unlock()

	if err := tracker.CancelBuild(ctx); err != nil {
		if ctx.Err() != nil {
			// The caller gave up while the cancel was being attempted. Only
			// the typed outcomes positively say nothing was signalled; any
			// other error may have come back after the signal landed.
			mayHaveSignalled := !cancelSignalNotSent(err)
			e.settleCancelAttempt(tracked, mayHaveSignalled)
			if mayHaveSignalled {
				logger.Warn("PostgreSQL cancel caller gave up after the build backend may have been signalled; the drive will still read a cancellation as the operator's",
					"task_id", key, "reason", err, "caller_error", ctx.Err())
			} else {
				logger.Info("PostgreSQL cancel caller gave up and the build backend was not signalled; the apply is unchanged",
					"task_id", key, "reason", err, "caller_error", ctx.Err())
			}
			return nil, fmt.Errorf("cancel PostgreSQL concurrent index build for apply %q: %w (caller gave up: %w)", key, err, ctx.Err())
		}
		// Whatever kept the signal from a running statement — no build
		// tracked yet, a build that already returned, a backend this role
		// cannot observe or signal, a reserved session that failed — the
		// drive's own context still ends the statement it is on, and a build
		// that has already returned settles on its own.
		e.settleCancelAttempt(tracked, true)
		logger.Info("PostgreSQL cancel could not signal the build backend; cancelling the apply's own context instead",
			"task_id", key, "reason", err)
		cancelApply()
	} else {
		e.settleCancelAttempt(tracked, true)
	}

	settle, cancel := context.WithTimeout(ctx, cancelSettleTimeout)
	defer cancel()
	select {
	case <-done:
		return e.settledCancel(key, tracked)
	case <-settle.Done():
		return nil, fmt.Errorf("cancel PostgreSQL concurrent index build for apply %q: the build was signalled but has not settled: %w", key, settle.Err())
	}
}

// settledCancel answers a cancel from the terminal result the drive published.
// A completed change is reported as such so the caller reconciles to the
// outcome that actually landed; a cancelled or failed change has no work left
// to stop, so the cancel is accepted over it, carrying the failure detail so
// the operator sees what the change settled on.
func (e *Engine) settledCancel(key string, tracked *trackedApply) (*engine.ControlResult, error) {
	e.mu.Lock()
	state, detail, logger := tracked.result.State, tracked.result.ErrorMessage, tracked.logger
	e.mu.Unlock()
	switch state {
	case engine.StateCompleted:
		return nil, engine.NewAlreadyCompletedError("cancel rejected: the PostgreSQL schema change for apply %q completed before the cancel took effect", key)
	case engine.StateCancelled:
		return &engine.ControlResult{Accepted: true, Message: "Concurrent index build cancelled"}, nil
	case engine.StateFailed:
		logger.Info("PostgreSQL cancel accepted over a schema change that had already failed",
			"task_id", key, "detail", detail)
		return &engine.ControlResult{Accepted: true, Message: "Schema change had already failed before the cancel: " + detail}, nil
	default:
		return nil, fmt.Errorf("cancel PostgreSQL schema change for apply %q: the drive settled in state %q, which cancel has no answer for", key, state)
	}
}

// settleCancelAttempt ends one Cancel call's in-flight claim on the apply.
// A call whose signal may have been sent, or that went on to cancel the
// drive's context, leaves the cancel recorded for good; a call that sent
// nothing leaves the record as the other calls made it, so an earlier or
// concurrent Cancel whose signal did go out is still read as the operator's.
func (e *Engine) settleCancelAttempt(tracked *trackedApply, mayHaveActed bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if mayHaveActed {
		tracked.cancelRequested = true
	}
	tracked.cancelsInFlight--
}

// cancelSignalNotSent reports whether a CancelBuild error positively says no
// signal was sent to the build backend: no build was tracked, the server
// showed no statement running on its backend, or the backend's state was
// hidden so none was sent blind.
func cancelSignalNotSent(err error) bool {
	return errors.Is(err, progress.ErrNoActiveBuild) ||
		errors.Is(err, progress.ErrBuildNotRunning) ||
		errors.Is(err, progress.ErrBuildUnobservable)
}

// cancelRequested reports whether a Cancel may have acted on the apply, or is
// acting on it now, so the drive can classify the cancellation it sees as the
// operator's.
func (e *Engine) cancelRequested(key string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	tracked := e.progress[key]
	return tracked != nil && (tracked.cancelRequested || tracked.cancelsInFlight > 0)
}

// isCancellation reports whether an executor error is a cancelled statement in
// any of the forms a cancellation takes: the server's query_canceled surfaced
// by pg-sprite as an external cancellation, the drive's own context ending
// while a statement ran, or that context's error surfaced directly by a step
// that had no statement in flight.
func isCancellation(err error) bool {
	return errors.Is(err, executor.ErrCancelledExternally) ||
		errors.Is(err, executor.ErrCancelledByCaller) ||
		errors.Is(err, context.Canceled)
}
