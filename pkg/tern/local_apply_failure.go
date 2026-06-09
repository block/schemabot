package tern

import (
	"context"
	"time"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// failApplyWithTasks marks all tasks and the apply as failed with the given error.
// If the apply is already in a terminal state (e.g., cancelled by Stop()), the
// apply state is not overwritten.
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

	// Re-read the apply from storage — Stop() may have already set a terminal
	// state (e.g., cancelled) between when the engine error occurred and now.
	fresh, err := c.storage.Applies().Get(ctx, apply.ID)
	if err == nil && fresh != nil && state.IsTerminalApplyState(fresh.State) {
		c.logger.Debug("apply already in terminal state, not overwriting",
			"apply_id", apply.ApplyIdentifier, "state", fresh.State)
		return
	}

	apply.State = state.Apply.Failed
	apply.ErrorMessage = errMsg
	apply.CompletedAt = &now
	apply.UpdatedAt = now
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		c.logger.Error("failed to update apply state", "apply_id", apply.ApplyIdentifier, "state", state.Apply.Failed, "error", err)
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

	// Re-read the apply from storage; Stop() may have already moved it to a
	// terminal state between the engine error and this update.
	fresh, err := c.storage.Applies().Get(ctx, apply.ID)
	if err == nil && fresh != nil && state.IsTerminalApplyState(fresh.State) {
		c.logger.Debug("apply already in terminal state, not marking retryable",
			"apply_id", apply.ApplyIdentifier, "state", fresh.State)
		return
	}

	apply.State = state.Apply.FailedRetryable
	apply.ErrorMessage = errMsg
	apply.CompletedAt = nil
	apply.UpdatedAt = time.Now()
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		c.logger.Error("failed to update apply state", "apply_id", apply.ApplyIdentifier, "state", state.Apply.FailedRetryable, "error", err)
	}
	metrics.AdjustActiveApplies(ctx, -1, apply.Database, apply.Deployment, apply.Environment)
	if obs := c.getObserver(apply.ID); obs != nil {
		obs.OnProgress(apply, tasks)
	}
}
