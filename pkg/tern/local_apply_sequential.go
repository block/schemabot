package tern

import (
	"context"
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// executeApplySequential runs each DDL as a separate Spirit call (independent mode).
// Each table copies and cuts over independently.
func (c *LocalClient) executeApplySequential(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, plan *storage.Plan, options map[string]string) {
	defer c.startApplyHeartbeat(ctx, apply)()
	seqStart := time.Now()
	creds := c.credentials()
	defer c.setupSpiritLogging(ctx, apply, tasks)()

	c.logger.Info("executeApplySequential starting",
		"apply_id", apply.ApplyIdentifier,
		"task_count", len(tasks),
		"plan_ddl_count", len(plan.FlatDDLChanges()),
		"elapsed_ms", time.Since(seqStart).Milliseconds(),
	)

	now := time.Now()
	apply.State = state.Apply.Running
	apply.StartedAt = &now
	apply.UpdatedAt = now
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		c.logger.Error("failed to update apply state", "apply_id", apply.ApplyIdentifier, "state", state.Apply.Running, "error", err)
	}

	var failedTask *storage.Task
	var stoppedByUser bool

	for i, task := range tasks {
		if handled, err := c.processPendingStopControlRequest(ctx, apply); err != nil {
			c.logger.Warn("pending stop request processing failed; current apply owner will exit for scheduler retry",
				"apply_id", apply.ApplyIdentifier, "error", err)
			return
		} else if handled {
			stoppedByUser = true
			break
		}

		action := c.checkTaskReady(ctx, task)
		if action == taskStopped {
			stoppedByUser = true
			break
		}
		if action == taskSkip {
			continue
		}

		c.logger.Info("executeApplySequential: starting task",
			"iteration", i+1, "total_tasks", len(tasks),
			"task_id", task.TaskIdentifier, "table", task.TableName,
			"elapsed_ms", time.Since(seqStart).Milliseconds(),
		)

		action = c.runEngineTask(ctx, apply, task, plan, options, creds)

		// Notify observer after each task completes
		if obs := c.getObserver(apply.ID); obs != nil {
			obs.OnProgress(apply, tasks)
		}

		if action == taskFailed {
			failedTask = task
			break
		}
		if action == taskAbort {
			return
		}
		if action == taskStopped {
			stoppedByUser = true
			break
		}
	}

	// Update apply state based on task outcomes
	c.logger.Info("executeApplySequential loop finished",
		"apply_id", apply.ApplyIdentifier,
		"tasks_processed", len(tasks),
		"failed_task", failedTask != nil,
		"stopped_by_user", stoppedByUser,
	)
	c.finalizeSequentialApply(ctx, apply, tasks, failedTask, stoppedByUser)
	c.logger.Info("sequential apply finished", "apply_id", apply.ApplyIdentifier, "state", apply.State)
}

// taskAction indicates the outcome of a single task execution step.
type taskAction int

const (
	taskContinue taskAction = iota // Task completed successfully, proceed to next
	taskFailed                     // Task failed, stop processing
	taskStopped                    // Task/apply was stopped by user, stop processing
	taskSkip                       // Task should be skipped (error fetching state)
	taskAbort                      // Current owner should exit without changing final state
)

// checkTaskReady verifies a task is ready to execute by checking context cancellation
// and re-fetching the task's current state from storage.
func (c *LocalClient) checkTaskReady(ctx context.Context, task *storage.Task) taskAction {
	if ctx.Err() != nil {
		c.logger.Info("apply context cancelled, stopping sequential loop",
			"task_id", task.TaskIdentifier, "table", task.TableName)
		return taskStopped
	}
	freshTask, err := c.storage.Tasks().Get(ctx, task.TaskIdentifier)
	if err != nil {
		c.logger.Error("failed to fetch task state", "task_id", task.TaskIdentifier, "error", err)
		return taskSkip
	}
	if freshTask == nil {
		c.logger.Error("task not found", "task_id", task.TaskIdentifier)
		return taskSkip
	}
	if freshTask.State == state.Task.Stopped {
		c.logger.Info("task was stopped by user, skipping", "task_id", task.TaskIdentifier, "table", task.TableName)
		return taskStopped
	}
	if state.IsTerminalTaskState(freshTask.State) {
		c.logger.Info("task already in terminal state, skipping",
			"task_id", task.TaskIdentifier, "table", task.TableName, "state", freshTask.State)
		return taskSkip
	}
	return taskContinue
}

// runEngineTask calls the engine for a single DDL, marks the task running, and polls to completion.
// Returns the outcome: taskContinue (completed), taskFailed, or taskStopped.
func (c *LocalClient) runEngineTask(ctx context.Context, apply *storage.Apply, task *storage.Task, plan *storage.Plan, options map[string]string, creds *engine.Credentials) taskAction {
	if handled, err := c.processPendingStopControlRequest(ctx, apply); err != nil {
		c.logger.Warn("pending stop request processing failed before sequential engine apply; current apply owner will exit for scheduler retry",
			"apply_id", apply.ApplyIdentifier, "task_id", task.TaskIdentifier, "error", err)
		return taskAbort
	} else if handled {
		return taskStopped
	}

	// Sequential mode: one DDL per engine call. Use the task identifier as
	// MigrationContext so each table's schema change is tracked independently.
	result, err := c.getEngine().Apply(ctx, &engine.ApplyRequest{
		Database: task.Database,
		Changes: []engine.SchemaChange{{
			Namespace:    task.Namespace,
			TableChanges: []engine.TableChange{{Table: task.TableName, DDL: task.DDL}},
		}},
		Options:     options,
		ResumeState: &engine.ResumeState{MigrationContext: task.TaskIdentifier},
		Credentials: creds,
	})

	if err != nil {
		if c.shouldRetryEngineError(err) {
			c.markTaskRetryable(ctx, task, err.Error())
		} else {
			c.markTaskFailed(ctx, task, err.Error())
		}
		c.logger.Error("task failed", "error", err, "task_id", task.TaskIdentifier, "table", task.TableName)
		return taskFailed
	}
	if !result.Accepted {
		c.markTaskFailed(ctx, task, result.Message)
		c.logger.Error("task rejected", "message", result.Message, "task_id", task.TaskIdentifier, "table", task.TableName)
		return taskFailed
	}

	// Mark task running
	now := time.Now()
	task.StartedAt = &now
	c.transitionTaskState(ctx, task, 0, state.Task.Running, "")
	c.logger.Info("task running", "task_id", task.TaskIdentifier, "table", task.TableName)

	// Poll to completion
	pollAction := c.pollTaskToCompletion(ctx, apply, task, creds)
	if pollAction == taskAbort || pollAction == taskStopped {
		return pollAction
	}

	switch task.State {
	case state.Task.Failed, state.Task.FailedRetryable:
		return taskFailed
	case state.Task.Stopped:
		return taskStopped
	default:
		return taskContinue
	}
}

// Timeouts for idle states where user action is expected.
const (
	// waitingForManualActionTimeout is how long to wait for a manual trigger
	// (deploy or cutover) before auto-cancelling the apply.
	waitingForManualActionTimeout = 14 * 24 * time.Hour

	// defaultRevertWindowDuration is the default revert window period.
	// 30 minutes matches PlanetScale's default.
	defaultRevertWindowDuration = 30 * time.Minute
)

// atomicPollState tracks mutable state across polling ticks in atomic mode.
type atomicPollState struct {
	lastTaskState   string
	lastLoggedState string
	lastProgressLog time.Time

	// stateEnteredAt tracks when the current waiting state was entered,
	// used for timeout enforcement on deferred cutover and revert window.
	stateEnteredAt time.Time

	// revertSkipped is set after SkipRevert is called to prevent repeated calls.
	revertSkipped bool

	// consecutiveErrors tracks progress poll failures to fail fast when the
	// engine is unreachable (e.g., branch deleted mid-apply).
	consecutiveErrors int
}

// startApplyHeartbeat starts a background goroutine that heartbeats the apply
// every 10 seconds, preventing the scheduler from treating it as crashed.
// Returns a cancel function that stops the heartbeat. Must be deferred by the caller.
func (c *LocalClient) startApplyHeartbeat(ctx context.Context, apply *storage.Apply) context.CancelFunc {
	hbCtx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(c.heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				if err := c.storage.Applies().Heartbeat(hbCtx, apply.ID); err != nil {
					c.logger.Warn("heartbeat failed", "apply_id", apply.ApplyIdentifier, "error", err)
				}
			}
		}
	}()
	return cancel
}

// pollForCompletionAtomic polls the engine for progress in atomic mode (all tasks share state).

// pollTaskToCompletion polls a single task to completion (sequential mode).
func (c *LocalClient) pollTaskToCompletion(ctx context.Context, apply *storage.Apply, task *storage.Task, creds *engine.Credentials) taskAction {
	eng := c.getEngine()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return taskStopped
		case <-ticker.C:
			if handled, err := c.processPendingStopControlRequest(ctx, apply); err != nil {
				c.logger.Warn("pending stop request processing failed; current apply owner will exit for scheduler retry",
					"apply_id", apply.ApplyIdentifier, "task_id", task.TaskIdentifier, "error", err)
				return taskAbort
			} else if handled {
				task.State = state.Task.Stopped
				return taskStopped
			}

			// Re-fetch task state from storage to detect external changes (e.g., Stop).
			// This also guards against a race where a new apply starts and the engine's
			// runningMigration no longer corresponds to this task.
			freshTask, fetchErr := c.storage.Tasks().Get(ctx, task.TaskIdentifier)
			if fetchErr == nil && freshTask != nil && state.IsTerminalTaskState(freshTask.State) {
				// Task was already marked terminal externally — stop polling
				task.State = freshTask.State
				return taskContinue
			}

			result, err := eng.Progress(ctx, &engine.ProgressRequest{
				Database:    task.Database,
				Credentials: creds,
			})
			if err != nil {
				c.logger.Warn("progress check failed", "error", err, "task_id", task.TaskIdentifier)
				continue
			}

			now := time.Now()
			prevState := task.State
			task.State = taskStateFromProgressResult(result)
			task.UpdatedAt = now
			retryableFailure := state.IsState(task.State, state.Task.FailedRetryable)

			// Update progress fields from engine result
			if len(result.Tables) > 0 {
				// For single-DDL task, use the first table's progress
				tp := result.Tables[0]
				task.RowsCopied = tp.RowsCopied
				task.RowsTotal = tp.RowsTotal
				task.ProgressPercent = tp.Progress
				task.ETASeconds = int(tp.ETASeconds)
				task.IsInstant = tp.IsInstant
			}

			if result.State.IsTerminal() {
				if retryableFailure {
					task.CompletedAt = nil
				} else {
					task.CompletedAt = &now
				}
				if result.State == engine.StateCompleted {
					task.ProgressPercent = 100
				}
				if result.State == engine.StateFailed {
					if msg := progressFailureMessage(result); msg != "" {
						task.ErrorMessage = msg
					}
				}
				logMsg := ""
				if task.ApplyID > 0 {
					logMsg = fmt.Sprintf("Task %s finished: engine_state=%s message=%q rows=%d/%d",
						task.TaskIdentifier, result.State, result.Message, task.RowsCopied, task.RowsTotal)
				}
				c.transitionTaskState(ctx, task, task.ApplyID, task.State, logMsg)
				c.logger.Info("task finished",
					"task_id", task.TaskIdentifier,
					"table", task.TableName,
					"engine_state", result.State,
					"engine_message", result.Message,
					"prev_storage_state", prevState,
					"rows_copied", task.RowsCopied,
					"rows_total", task.RowsTotal,
				)
				return taskContinue
			}

			c.transitionTaskState(ctx, task, 0, task.State, "")

			// Notify observer with full apply + tasks context
			if obs := c.getObserver(task.ApplyID); obs != nil {
				if apply, err := c.storage.Applies().Get(ctx, task.ApplyID); err == nil && apply != nil {
					if allTasks, err := c.storage.Tasks().GetByApplyID(ctx, task.ApplyID); err == nil {
						obs.OnProgress(apply, allTasks)
					}
				}
			}
		}
	}
}

// markTaskFailed sets a task to FAILED state with the given error message and persists it.
func (c *LocalClient) markTaskFailed(ctx context.Context, task *storage.Task, errMsg string) {
	now := time.Now()
	task.ErrorMessage = errMsg
	task.CompletedAt = &now
	c.transitionTaskState(ctx, task, 0, state.Task.Failed, "")
}

// markTaskRetryable records a task failure that scheduler recovery may retry.
func (c *LocalClient) markTaskRetryable(ctx context.Context, task *storage.Task, errMsg string) {
	task.ErrorMessage = errMsg
	task.CompletedAt = nil
	c.transitionTaskState(ctx, task, 0, state.Task.FailedRetryable, "")
}

func (c *LocalClient) shouldRetryEngineError(err error) bool {
	return c.config.Type == storage.DatabaseTypeMySQL && engine.IsRetryable(err)
}

// failApplyWithTasks marks all tasks and the apply as failed with the given error.
// If the apply is already in a terminal state (e.g., cancelled by Stop()), the
// apply state is not overwritten.

// finalizeSequentialApply updates the apply state based on sequential task outcomes.
// Permanent failures cancel remaining pending tasks; retryable failures leave
// pending tasks queued for scheduler recovery.
func (c *LocalClient) finalizeSequentialApply(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, failedTask *storage.Task, stoppedByUser bool) {
	now := time.Now()
	if freshApply, err := c.storage.Applies().Get(ctx, apply.ID); err != nil {
		c.logger.Error("failed to reload apply before sequential finalization", "apply_id", apply.ApplyIdentifier, "error", err)
		return
	} else if freshApply != nil && state.IsTerminalApplyState(freshApply.State) {
		c.logger.Info("apply already terminal in storage, not overwriting during sequential finalization",
			"apply_id", apply.ApplyIdentifier,
			"stored_state", freshApply.State)
		*apply = *freshApply
		if err := completePendingStopControlRequests(ctx, c.storage, apply); err != nil {
			c.logger.Warn("failed to complete pending stop request for terminal sequential apply",
				"apply_id", apply.ApplyIdentifier, "error", err)
		}
		return
	}
	switch {
	case failedTask != nil && failedTask.State == state.Task.FailedRetryable:
		apply.State = state.Apply.FailedRetryable
		apply.ErrorMessage = fmt.Sprintf("table %s failed: %s", failedTask.TableName, failedTask.ErrorMessage)
		apply.CompletedAt = nil
	case failedTask != nil:
		apply.State = state.Apply.Failed
		apply.ErrorMessage = fmt.Sprintf("table %s failed: %s", failedTask.TableName, failedTask.ErrorMessage)
		apply.CompletedAt = &now
		for _, task := range tasks {
			if task.State == state.Task.Pending {
				c.transitionTaskState(ctx, task, 0, state.Task.Cancelled, "")
			}
		}
	case stoppedByUser:
		apply.State = state.Apply.Stopped
	default:
		apply.State = state.Apply.Completed
		apply.CompletedAt = &now
	}
	apply.UpdatedAt = now
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		c.logger.Error("failed to update apply state", "apply_id", apply.ApplyIdentifier, "state", apply.State, "error", err)
	}
	if state.IsTerminalApplyState(apply.State) {
		if err := completePendingStopControlRequests(ctx, c.storage, apply); err != nil {
			c.logger.Warn("failed to complete pending stop request after sequential finalization",
				"apply_id", apply.ApplyIdentifier, "error", err)
			return
		}
	}
	metrics.AdjustActiveApplies(ctx, -1, apply.Database, apply.Environment)

	if apply.State == state.Apply.FailedRetryable {
		if obs := c.getObserver(apply.ID); obs != nil {
			obs.OnProgress(apply, tasks)
		}
		return
	}

	// Notify observer of terminal state, then clean up
	if obs := c.getObserver(apply.ID); obs != nil {
		obs.OnTerminal(apply, tasks)
		c.clearObserver(apply.ID)
	}
}
