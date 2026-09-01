package tern

import (
	"context"
	"fmt"
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

// failApplyWithTasks marks all tasks and the apply as failed with the given error.
// If the apply is already in a terminal state (e.g., cancelled by Stop()), the
// stored state is not overwritten; the settled state is adopted into the
// in-memory apply so callers that notify observers afterwards report the
// concurrent verdict instead of the stale pre-failure state.
func (c *LocalClient) failApplyWithTasks(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, errMsg string) {
	if c.driveCancelled(ctx, apply, "before recording that the apply failed") {
		return
	}
	now := time.Now()
	for _, task := range tasks {
		if state.IsTerminalTaskState(task.State) {
			continue
		}
		if task.ErrorMessage == "" {
			task.ErrorMessage = errMsg
		}
		task.CompletedAt = &now
		c.transitionTaskState(ctx, task, 0, state.Task.Failed, "")
	}

	logger := c.logger.With(apply.IdentityLogAttrs()...)
	// A multi-operation drive owns only its operation: the failed tasks above
	// carry the outcome, the operator derives the operation row from them and
	// projects the parent, so the parent failed write, apply-level metric, and
	// failure log are the operator's to make.
	if suppressParentApplyWrites(ctx) {
		logger.Info("operation drive failed its tasks; operator derives the operation row and projects the parent",
			"error_message", errMsg)
		return
	}
	// Re-read the apply from storage — Stop() may have already set a terminal
	// state (e.g., cancelled) between when the engine error occurred and now.
	fresh, err := c.storage.Applies().Get(ctx, apply.ID)
	if err == nil && fresh != nil && state.IsTerminalApplyState(fresh.State) {
		logger.Debug("apply already in terminal state, not overwriting",
			"state", fresh.State)
		*apply = *fresh
		return
	}

	previousState := apply.State
	apply.State = state.Apply.Failed
	apply.ErrorMessage = errMsg
	apply.CompletedAt = &now
	apply.UpdatedAt = now
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		logger.Error("failed to update apply state", append(apply.MutableLogAttrs(), "error", err)...)
	} else {
		c.logApplyFailure(ctx, apply, previousState, errMsg)
	}
	metrics.AdjustActiveApplies(ctx, -1, apply.Database, apply.Deployment, apply.Environment)
}

// markApplyRetryableWithTasks pauses an apply after a retryable engine failure.
// Non-terminal tasks move to failed_retryable so operator recovery can decide
// which work to re-dispatch on the next attempt.
func (c *LocalClient) markApplyRetryableWithTasks(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, errMsg string) {
	if c.driveCancelled(ctx, apply, "before pausing the apply for retry") {
		return
	}
	for _, task := range tasks {
		if state.IsTerminalTaskState(task.State) {
			continue
		}
		if task.ErrorMessage == "" {
			task.ErrorMessage = errMsg
		}
		task.CompletedAt = nil
		c.transitionTaskState(ctx, task, 0, state.Task.FailedRetryable, "")
	}

	logger := c.logger.With(apply.IdentityLogAttrs()...)
	// A multi-operation drive owns only its operation: the failed_retryable
	// tasks above carry the outcome, the operator derives the operation row from
	// them and projects the parent, so the parent retryable write, apply-level
	// metric, retry log, and observer are the operator's to make.
	if suppressParentApplyWrites(ctx) {
		logger.Info("operation drive paused its tasks for retry; operator derives the operation row and projects the parent",
			"error_message", errMsg)
		return
	}
	// Re-read the apply from storage; Stop() may have already moved it to a
	// terminal state between the engine error and this update.
	fresh, err := c.storage.Applies().Get(ctx, apply.ID)
	if err == nil && fresh != nil && state.IsTerminalApplyState(fresh.State) {
		logger.Debug("apply already in terminal state, not marking retryable",
			"state", fresh.State)
		*apply = *fresh
		return
	}

	previousState := apply.State
	apply.State = state.Apply.FailedRetryable
	apply.ErrorMessage = errMsg
	apply.CompletedAt = nil
	apply.UpdatedAt = time.Now()
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		logger.Error("failed to update apply state", append(apply.MutableLogAttrs(), "error", err)...)
	} else {
		c.logApplyPausedForRetry(ctx, apply, previousState, errMsg)
	}
	metrics.AdjustActiveApplies(ctx, -1, apply.Database, apply.Deployment, apply.Environment)
	if obs := c.getObserver(apply.ID); obs != nil {
		obs.OnProgress(apply, tasks)
	}
}
