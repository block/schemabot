package tern

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// logApplyFailure records a permanent failure in the apply's own log stream. It
// is the only surface an operator reads from the CLI or the PR summary, so a
// failure that lands only in the server logs reads there as an apply that went
// terminal for no stated reason. Call it only after the state change is stored,
// so the log never claims an outcome storage did not take.
func (c *LocalClient) logApplyFailure(ctx context.Context, apply *storage.Apply, previousState, errMsg string) {
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelError, storage.LogEventError, storage.LogSourceSchemaBot,
		fmt.Sprintf("Apply failed: %s", errMsg), previousState, state.Apply.Failed)
}

// logApplyPausedForRetry records a retryable pause with the budget it has left,
// so the apply log shows recovery counting down its attempts rather than only
// the silence between them. The remaining count is derived from the apply's own
// attempt counter, which a recovery claim advances — counting drives instead
// would run past the budget it is measured against.
func (c *LocalClient) logApplyPausedForRetry(ctx context.Context, apply *storage.Apply, previousState, errMsg string) {
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventError, storage.LogSourceSchemaBot,
		fmt.Sprintf("Apply paused for operator retry (%d of %d recovery attempts remaining): %s",
			storage.MaxRecoveryAttempts-apply.Attempt, storage.MaxRecoveryAttempts, errMsg),
		previousState, state.Apply.FailedRetryable)
}

// postTerminalReconcileError is a failure in the work an apply owes after its
// terminal outcome is durably stored, such as answering the start that
// admitted the claim. The outcome it follows is already recorded, so the error
// describes a side effect left undone, never the schema change: a caller must
// not record a failure or a retryable pause over the stored outcome, and
// returns the error so the drive reports what it left undone.
type postTerminalReconcileError struct {
	storedState string
	err         error
}

func (e *postTerminalReconcileError) Error() string {
	return fmt.Sprintf("reconcile after the stored %s outcome: %v", e.storedState, e.err)
}

func (e *postTerminalReconcileError) Unwrap() error { return e.err }

// Is matches ErrPostTerminalReconcile, so a caller outside this package can
// recognise the failure without depending on its concrete type.
func (e *postTerminalReconcileError) Is(target error) bool { return target == ErrPostTerminalReconcile }

// driveCancelled reports whether the drive's context has been cancelled, and
// records in the server log that the apply is being handed back when it has.
//
// A cancelled drive is a process shutting down or a claim being released. From
// that moment every engine call and storage write fails with the same
// cancellation, so a failure observed under it describes the driver rather than
// the schema change: the change itself is untouched and often still healthy on
// the provider. Recording that failure would fail or pause a change that is
// fine, and because the write recording it runs on the same cancelled context,
// whether the false outcome lands at all is a race with the successor's claim.
// That race is not the protection — this guard is. A settlement write detached
// from the drive's context so it survives cancellation (context.WithoutCancel)
// would record the false outcome reliably instead of rarely, so every
// settlement write stays behind this guard whatever context it runs on.
// The apply is left exactly as it stands, for the driver that reclaims it to
// resume from its stored state.
//
// An operator-initiated stop or cancel does not rely on this path: the control
// handler settles the tasks and the apply itself on its own request context,
// and cancels the drive only so it stops iterating.
func (c *LocalClient) driveCancelled(ctx context.Context, apply *storage.Apply, during string) bool {
	if ctx.Err() == nil {
		return false
	}
	c.logger.With(apply.IdentityLogAttrs()...).Info(
		"drive cancelled "+during+"; handing the apply back for another driver to claim",
		append(apply.MutableLogAttrs(), "error", ctx.Err())...)
	return true
}

// taskVerdictMessage returns the error message a failure verdict records on a
// task: the task's own error when the engine reported one, otherwise the
// apply-wide reason.
func taskVerdictMessage(task *storage.Task, errMsg string) string {
	if task.ErrorMessage != "" {
		return task.ErrorMessage
	}
	return errMsg
}

// refusedTaskVerdict decides what an apply failure helper returns when storage
// refuses a task's verdict write. The apply's outcome — and, in a
// multi-operation drive, the operation row the operator derives — is read from
// the task rows, so the helper stops before recording anything on top of a
// verdict storage never took: the apply stays active, and the claim that picks
// it up next reads every task as stored and settles it. A write refused because
// the drive was cancelled hands the apply back with nil, like the apply write.
//
// Verdicts already stored for earlier tasks stay stored. A whole-apply drive's
// next claim reads one of them as the apply's outcome and settles the refused
// table from it. An operation drive gets no next claim: the operator derives
// the operation row from the task rows as they stand, so the row settles over a
// table that still reads running until the stranded-task reaper closes it.
func (c *LocalClient) refusedTaskVerdict(ctx context.Context, apply *storage.Apply, task *storage.Task, verdict, errMsg string, err error) error {
	if c.driveCancelled(ctx, apply, "while recording a task verdict") {
		return nil
	}
	attrs := append(task.LogAttrs(), "apply_id", apply.ApplyIdentifier, "verdict", verdict, "failure", errMsg, "error", err)
	if lease, ok := storage.OperationLeaseFromContext(ctx); ok {
		attrs = append(attrs, "apply_operation_id", lease.OperationID)
	}
	switch {
	case errors.Is(err, storage.ErrApplyLeaseLost):
		c.logger.Warn("task verdict was refused because the drive's lease was lost; this driver exits and leaves the apply to the lease holder", attrs...)
	case errors.Is(err, storage.ErrValueRejected):
		// The same write is refused every time, so the apply stays active
		// through every claim that retries it rather than settling.
		c.logger.Error("task verdict was refused outright because storage cannot hold a value it carries; this driver exits with the apply still active, and every later claim is refused the same write until the stored value changes", attrs...)
	default:
		c.logger.Error("failed to record a task verdict; current driver will exit with the apply still active and no terminal side effects run, for the next claim to settle", attrs...)
	}
	return fmt.Errorf("record %s task verdicts for apply %s (database %s): %w",
		verdict, apply.ApplyIdentifier, apply.Database, err)
}

// failApplyWithTasks marks all tasks and the apply as failed with the given error.
// If the apply is already in a terminal state (e.g., cancelled by Stop()), the
// stored state is not overwritten; the settled state is adopted into the
// in-memory apply so callers that notify observers afterwards report the
// concurrent verdict instead of the stale pre-failure state.
//
// Each task's failed verdict is stored before anything is recorded on the
// apply. A refused verdict write stops the helper there and returns the error
// with the apply untouched (see refusedTaskVerdict).
//
// The in-memory apply reads terminal on return only when storage holds a
// terminal state for it. A failed write that returns an error is re-read
// (failureWriteLanded): when storage holds the failure anyway, the write
// landed and the helper carries on as if it had returned nil. When the failed
// write does not land, the in-memory apply is put back to its pre-failure
// state, the active-apply gauge is left alone, and the write error is
// returned: the stored apply is still active, so nothing that answers for its
// outcome may run, and the claim that picks it up next settles it. A drive
// cancelled before or while recording the failure hands the apply back with
// nil and the apply untouched.
//
// It records the failure only. The side effects a terminal apply owes —
// settling the control requests it moots and posting the terminal summary —
// are the caller's; failApplyAndNotify runs both for drives that owe nothing
// else.
func (c *LocalClient) failApplyWithTasks(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, errMsg string) error {
	if c.driveCancelled(ctx, apply, "before recording that the apply failed") {
		return nil
	}
	for _, task := range tasks {
		if state.IsTerminalTaskState(task.State) {
			continue
		}
		if err := c.markTaskFailed(ctx, task, taskVerdictMessage(task, errMsg)); err != nil {
			return c.refusedTaskVerdict(ctx, apply, task, "failed", errMsg, err)
		}
	}

	now := time.Now()
	logger := c.logger.With(apply.IdentityLogAttrs()...)
	// A multi-operation drive owns only its operation: the failed tasks above
	// carry the outcome, the operator derives the operation row from them and
	// projects the parent, so the parent failed write, apply-level metric, and
	// failure log are the operator's to make.
	if suppressParentApplyWrites(ctx) {
		logger.Info("operation drive failed its tasks; operator derives the operation row and projects the parent",
			"error_message", errMsg)
		return nil
	}
	// Re-read the apply from storage — Stop() may have already set a terminal
	// state (e.g., cancelled) between when the engine error occurred and now.
	fresh, err := c.storage.Applies().Get(ctx, apply.ID)
	if err == nil && fresh != nil && state.IsTerminalApplyState(fresh.State) {
		logger.Debug("apply already in terminal state, not overwriting",
			"state", fresh.State)
		*apply = *fresh
		return nil
	}

	previous := *apply
	apply.State = state.Apply.Failed
	apply.ErrorMessage = errMsg
	apply.CompletedAt = &now
	apply.UpdatedAt = now
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		*apply = previous
		if c.driveCancelled(ctx, apply, "while recording that the apply failed") {
			return nil
		}
		landed, stored := c.failureWriteLanded(ctx, apply, errMsg, err)
		if !landed {
			logger.Error("failed to record the apply failed; current apply owner will exit for operator retry with the apply still active, its pending control requests pending, and no terminal summary posted",
				append(apply.MutableLogAttrs(), "failure", errMsg, "error", err)...)
			return fmt.Errorf("record apply %s (database %s) failed from stored state %s: %w",
				apply.ApplyIdentifier, apply.Database, previous.State, err)
		}
		logger.Warn("the write recording the apply failed returned an error, but storage holds the failure; continuing with the stored outcome",
			append(stored.MutableLogAttrs(), "error", err)...)
		*apply = *stored
	}
	c.logApplyFailure(ctx, apply, previous.State, errMsg)
	metrics.AdjustActiveApplies(ctx, -1, apply.Database, apply.Deployment, apply.Environment)
	return nil
}

// failureWriteLanded re-reads the apply after the write recording it failed
// returned an error, and reports whether storage holds that failure anyway: the
// write committed and only its reply was lost, as when the connection drops
// before the server's acknowledgement arrives. The write is this drive's only
// when storage holds the apply failed with this drive's message and still
// under this drive's apply lease: a successor that reclaimed the apply can
// record the same failure with the same message, and a displaced driver must
// exit rather than take that outcome, or its side effects, as its own (OW-2).
// So a write refused for a lost lease is never recovered, and neither is one
// made by a drive holding no apply lease, since nothing then proves whose write
// storage holds. Anything else, including a re-read that fails, is a write that
// did not land, and the drive goes on to treat it that way.
func (c *LocalClient) failureWriteLanded(ctx context.Context, apply *storage.Apply, errMsg string, writeErr error) (bool, *storage.Apply) {
	if errors.Is(writeErr, storage.ErrApplyLeaseLost) {
		return false, nil
	}
	lease, held := storage.ApplyLeaseFromContext(ctx)
	if !held || lease.ApplyID != apply.ID {
		c.logger.Debug("the drive holds no apply lease to prove the failure write was its own; treating the failure as not recorded",
			apply.LogAttrs()...)
		return false, nil
	}
	stored, err := c.storage.Applies().Get(ctx, apply.ID)
	if err != nil {
		c.logger.Warn("could not re-read the apply after the write recording it failed returned an error; treating the failure as not recorded",
			append(apply.LogAttrs(), "error", err)...)
		return false, nil
	}
	if stored == nil {
		c.logger.Warn("the apply was not found when re-read after the write recording it failed returned an error; treating the failure as not recorded",
			apply.LogAttrs()...)
		return false, nil
	}
	if !state.IsState(stored.State, state.Apply.Failed) {
		c.logger.Debug("storage does not hold the apply failed after the write recording it returned an error; the write did not land",
			stored.LogAttrs()...)
		return false, nil
	}
	if !failureStoredUnderLease(stored, errMsg, lease) {
		// Storage holds a failure, but not provably this drive's: another
		// drive recorded its own, or a successor recorded the same one under
		// its own lease. The token value itself is never logged.
		c.logger.Warn("storage holds the apply failed, but not provably by this drive's write; treating the failure as not recorded and leaving the stored outcome to the drive that owns it",
			append(stored.LogAttrs(),
				"message_matched", stored.ErrorMessage == errMsg,
				"lease_token_matched", stored.LeaseToken == lease.Token)...)
		return false, nil
	}
	return true, stored
}

// failureStoredUnderLease reports whether the stored apply is failed with
// errMsg and still carries the lease token the drive wrote under.
func failureStoredUnderLease(stored *storage.Apply, errMsg string, lease storage.ApplyLease) bool {
	return state.IsState(stored.State, state.Apply.Failed) &&
		stored.ErrorMessage == errMsg &&
		stored.LeaseToken == lease.Token
}

// failApplyAndNotify fails the apply and, once storage holds a terminal state
// for it, runs the side effects that state owes: the start that admitted this
// claim is answered with the failure, the pending control requests the outcome
// moots are settled, then the observer posts the terminal summary. Nothing
// later re-claims a failed apply to do any of these, so a drive that fails an
// apply and owes nothing more than that goes through here rather than calling
// failApplyWithTasks directly. A settlement failure is logged and the summary
// still posts (see settleRequestsForStoredOutcome); the operator's post-drive
// settlement retries the requests left pending.
//
// A multi-operation drive owns only its operation, so there the operator's
// projection settles the parent and posts the summary; failApplyWithTasks
// already logged that handoff. A drive cancelled before the failure was
// recorded leaves the apply non-terminal for another driver, so it owes
// nothing. A failure write that did not land is returned with nothing settled.
func (c *LocalClient) failApplyAndNotify(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, errMsg string) error {
	if err := c.failApplyWithTasks(ctx, apply, tasks, errMsg); err != nil {
		return err
	}
	if suppressParentApplyWrites(ctx) {
		return nil
	}
	if !state.IsTerminalApplyState(apply.State) {
		c.logger.Debug("apply left non-terminal by a cancelled drive; no terminal side effects are owed",
			apply.LogAttrs()...)
		return nil
	}
	logger := c.logger.With(apply.IdentityLogAttrs()...)
	if state.IsState(apply.State, state.Apply.Failed) {
		c.failPendingStartForFailedApply(ctx, logger, apply, errMsg)
	}
	c.settleRequestsForStoredOutcome(ctx, logger, apply)
	c.notifyTerminalObserver(apply, tasks)
	return nil
}

// failPendingStartForFailedApply answers the pending start of an apply whose
// stored state is failed. The start admitted the claim that failed, and no
// later claim of a failed apply will consume it, so it fails with the apply's
// reason: the operator reads that their command did not take effect. An apply
// adopted into another terminal state keeps its pending start — a stopped
// apply's start stays deliverable to the claim that resumes it.
//
// A read or write that does not land is logged rather than returned (RC-5):
// the request stays pending and nothing re-claims the apply to settle it,
// which the log says, while the caller still posts the summary the stored
// failure owes.
func (c *LocalClient) failPendingStartForFailedApply(ctx context.Context, logger *slog.Logger, apply *storage.Apply, errMsg string) {
	startReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationStart)
	if err != nil {
		logger.Warn("failed to read the pending start request for the stored failure; a pending start stays pending with nothing left to claim the failed apply, and the terminal summary still posts",
			append(apply.MutableLogAttrs(), "error", err)...)
		return
	}
	if startReq == nil {
		logger.Debug("no pending start request to answer for the stored failure")
		return
	}
	if err := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart, errMsg); err != nil {
		logger.Warn("failed to fail the pending start request for the stored failure; it stays pending with nothing left to claim the failed apply, and the terminal summary still posts",
			append(apply.MutableLogAttrs(), "error", err)...)
	}
}

// markApplyRetryableWithTasks pauses an apply after a retryable engine failure.
// Non-terminal tasks move to failed_retryable so operator recovery can decide
// which work to re-dispatch on the next attempt.
//
// Like failApplyWithTasks, a refused task verdict write stops the helper
// before the apply is touched, and the pause's side effects — the retry log, the
// active-apply gauge, and the observer's progress update — run only once the
// pause is stored. When the write returns an error, the in-memory apply is
// put back to its pre-pause state and the write error is returned.
//
// Unlike failApplyWithTasks, a pause write that returns an error is not
// re-read to find out whether it landed anyway. A pause is not an outcome: it
// answers no control request and owes no terminal summary, and a stored pause
// leaves the apply claimable, so the next claim re-drives it and logs and
// posts whatever it arrives at. What a pause that committed with its reply
// lost leaves undone is bounded to the retry log entry, the observer's
// progress update for this attempt, and this drive's active-apply gauge
// release.
func (c *LocalClient) markApplyRetryableWithTasks(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, errMsg string) error {
	if c.driveCancelled(ctx, apply, "before pausing the apply for retry") {
		return nil
	}
	for _, task := range tasks {
		if state.IsTerminalTaskState(task.State) {
			continue
		}
		if err := c.markTaskRetryable(ctx, task, taskVerdictMessage(task, errMsg)); err != nil {
			return c.refusedTaskVerdict(ctx, apply, task, "failed_retryable", errMsg, err)
		}
	}

	logger := c.logger.With(apply.IdentityLogAttrs()...)
	// A multi-operation drive owns only its operation: the failed_retryable
	// tasks above carry the outcome, the operator derives the operation row from
	// them and projects the parent, so the parent retryable write, apply-level
	// metric, retry log, and observer are the operator's to make.
	if suppressParentApplyWrites(ctx) {
		logger.Info("operation drive paused its tasks for retry; operator derives the operation row and projects the parent",
			"error_message", errMsg)
		return nil
	}
	// Re-read the apply from storage; Stop() may have already moved it to a
	// terminal state between the engine error and this update.
	fresh, err := c.storage.Applies().Get(ctx, apply.ID)
	if err == nil && fresh != nil && state.IsTerminalApplyState(fresh.State) {
		logger.Debug("apply already in terminal state, not marking retryable",
			"state", fresh.State)
		*apply = *fresh
		return nil
	}

	previous := *apply
	apply.State = state.Apply.FailedRetryable
	apply.ErrorMessage = errMsg
	apply.CompletedAt = nil
	apply.UpdatedAt = time.Now()
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		*apply = previous
		if c.driveCancelled(ctx, apply, "while pausing the apply for retry") {
			return nil
		}
		logger.Error("failed to record the apply paused for retry; current apply owner will exit for operator retry with the apply still active",
			append(apply.MutableLogAttrs(), "failure", errMsg, "error", err)...)
		return fmt.Errorf("record apply %s (database %s) paused for retry from stored state %s: %w",
			apply.ApplyIdentifier, apply.Database, previous.State, err)
	}
	c.logApplyPausedForRetry(ctx, apply, previous.State, errMsg)
	metrics.AdjustActiveApplies(ctx, -1, apply.Database, apply.Deployment, apply.Environment)
	if obs := c.getObserver(apply.ID); obs != nil {
		obs.OnProgress(apply, tasks)
	}
	return nil
}
