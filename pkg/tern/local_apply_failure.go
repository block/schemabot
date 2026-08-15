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

// failApplyWithTasks marks all tasks and the apply as failed with the given error.
// If the apply is already in a terminal state (e.g., cancelled by Stop()), the
// stored state is not overwritten; the settled state is adopted into the
// in-memory apply so callers that notify observers afterwards report the
// concurrent verdict instead of the stale pre-failure state.
func (c *LocalClient) failApplyWithTasks(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, errMsg string) {
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
