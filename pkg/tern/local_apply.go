package tern

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/spirit"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// checkActiveTaskConflict verifies there's no active schema change for this database.
// Uses retry loop and engine verification to handle stale storage state.
func (c *LocalClient) checkActiveTaskConflict(ctx context.Context, plan *storage.Plan) error {
	for attempt := range 10 {
		existingTasks, err := c.storage.Tasks().GetByDatabase(ctx, plan.Database)
		if err != nil {
			return fmt.Errorf("check existing tasks: %w", err)
		}

		c.logger.Debug("conflict check: found tasks", "count", len(existingTasks), "database", plan.Database, "attempt", attempt)

		blockingTaskID := c.findBlockingTask(ctx, existingTasks, plan)
		if blockingTaskID == "" {
			return nil
		}

		// Retry: 10 attempts with 100ms sleep gives 1 second total wait.
		// Handles the race where storage is updated but Spirit hasn't fully finished.
		if attempt < 9 {
			c.logger.Debug("found potentially stale active task, retrying", "task_id", blockingTaskID, "attempt", attempt)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		return fmt.Errorf("schema change already in progress for this database")
	}
	return nil
}

// findBlockingTask checks if any non-terminal task for this database is truly active.
// Returns the blocking task's identifier, or "" if no conflict exists.
// As a side effect, resolves stale tasks by checking engine state.
func (c *LocalClient) findBlockingTask(ctx context.Context, tasks []*storage.Task, plan *storage.Plan) string {
	for _, t := range tasks {
		c.logger.Debug("conflict check: checking task", "task_id", t.TaskIdentifier, "state", t.State, "is_terminal", state.IsTerminalTaskState(t.State))
		if t.DatabaseType != plan.DatabaseType || state.IsTerminalTaskState(t.State) {
			continue
		}

		// Storage says non-terminal — verify with engine before blocking.
		if c.tryResolveStaleTask(ctx, t, plan.Database) {
			continue // Task was stale; engine confirmed it's done.
		}

		c.logger.Debug("conflict check: task is active", "task_id", t.TaskIdentifier)
		return t.TaskIdentifier
	}
	return ""
}

// tryResolveStaleTask checks the engine to see if a non-terminal task is actually done.
// If the engine reports a terminal state, or reports no active work for a task that
// storage believes is in-flight, the task is updated in storage and no longer blocks.
// Resting tasks (Stopped, FailedRetryable) are left untouched.
// Returns true if the task was resolved (no longer blocking).
func (c *LocalClient) tryResolveStaleTask(ctx context.Context, t *storage.Task, database string) bool {
	eng := c.getEngine()
	if eng == nil {
		c.logger.Error("tryResolveStaleTask: engine is nil", "database", database)
		return false
	}

	result, err := eng.Progress(ctx, &engine.ProgressRequest{
		Database:    database,
		Credentials: c.credentials(),
	})
	c.logger.Debug("conflict check: engine progress", "task_id", t.TaskIdentifier, "engine_state", result.State, "message", result.Message, "err", err)
	if err != nil {
		return false
	}

	// Engine says terminal — update storage and unblock.
	// IMPORTANT: Only trust terminal states, NOT "No active schema change".
	// "No active schema change" just means Spirit has no runningMigration,
	// which could mean completed, never started, or crashed.
	if result.State.IsTerminal() {
		c.logger.Info("conflict check: engine reports terminal state",
			"task_id", t.TaskIdentifier, "engine_state", result.State,
			"engine_message", result.Message, "storage_state", t.State)
		now := time.Now()
		t.CompletedAt = &now
		c.transitionTaskState(ctx, t, 0, engineStateToStorage(result.State), "")
		return true
	}

	// The engine has no active work. For in-flight states this means the task was
	// abandoned (e.g. a server crash) and must be failed so it stops blocking.
	// Resting states (Stopped, FailedRetryable) also have no active engine work,
	// but that is expected — Spirit keeps the checkpoint until an operator resumes
	// or retries. Failing them here would destroy resumable work and void the
	// operator retry budget, so leave them untouched and let the conflict/lock
	// logic decide whether the new apply proceeds.
	if result.Message == "No active schema change" {
		if !state.IsInFlightTaskState(t.State) {
			c.logger.Debug("conflict check: leaving resting task untouched (no active engine work expected)",
				"task_id", t.TaskIdentifier, "storage_state", t.State)
			return false
		}
		c.logger.Info("conflict check: cleaning up stale task (no active schema change in engine)",
			"task_id", t.TaskIdentifier, "storage_state", t.State, "started_at", t.StartedAt)
		now := time.Now()
		t.ErrorMessage = "Task abandoned: engine has no active schema change (server may have crashed)"
		t.CompletedAt = &now
		c.transitionTaskState(ctx, t, 0, state.Task.Failed, "")
		return true
	}

	return false
}

// logApplyEvent appends a log entry for an apply operation.
func (c *LocalClient) logApplyEvent(ctx context.Context, applyID int64, taskID *int64, level, eventType, source, message string, oldState, newState string) {
	log := &storage.ApplyLog{
		ApplyID:   applyID,
		TaskID:    taskID,
		Level:     level,
		EventType: eventType,
		Source:    source,
		Message:   message,
		OldState:  oldState,
		NewState:  newState,
		CreatedAt: time.Now(),
	}
	if err := c.storage.ApplyLogs().Append(ctx, log); err != nil {
		c.logger.Warn("failed to log apply event", "error", err, "event", eventType, "message", message)
	}
}

// setupSpiritLogging wires up Spirit's log callback to route engine logs to the apply_logs table.
// Builds a table-name-to-task lookup so each log line is attributed to the correct task.
// Returns a cleanup function that must be deferred.
func (c *LocalClient) setupSpiritLogging(ctx context.Context, apply *storage.Apply, tasks []*storage.Task) func() {
	spiritEng, ok := c.spiritEngine.(*spirit.Engine)
	if !ok {
		return func() {}
	}

	taskByTable := make(map[string]*storage.Task)
	var firstTask *storage.Task
	for _, task := range tasks {
		taskByTable[task.TableName] = task
		if firstTask == nil {
			firstTask = task
		}
	}

	spiritEng.SetLogCallback(func(level slog.Level, tableName, msg string) {
		logLevel := storage.LogLevelInfo
		if level >= slog.LevelWarn {
			logLevel = storage.LogLevelWarn
		}
		if level >= slog.LevelError {
			logLevel = storage.LogLevelError
		}
		task := taskByTable[tableName]
		if task == nil {
			task = firstTask
		}
		var taskID *int64
		if task != nil {
			id := task.ID
			taskID = &id
		}
		c.logApplyEvent(ctx, apply.ID, taskID, logLevel, storage.LogEventInfo, storage.LogSourceSpirit, msg, "", "")
	})
	return func() { spiritEng.SetLogCallback(nil) }
}

// transitionTaskState updates a task's state, persists it, and optionally logs a state transition.
// Fields like CompletedAt, StartedAt, ErrorMessage, or progress must be set on the task BEFORE calling this.
func (c *LocalClient) transitionTaskState(ctx context.Context, task *storage.Task, applyID int64, newState string, logMsg string) {
	oldState := task.State
	task.State = newState
	task.UpdatedAt = time.Now()
	if err := c.storage.Tasks().Update(ctx, task); err != nil {
		c.logger.Error("failed to update task state", "task_id", task.TaskIdentifier, "state", newState, "error", err)
	}
	if logMsg != "" && applyID > 0 {
		taskID := task.ID
		c.logApplyEvent(ctx, applyID, &taskID, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			logMsg, oldState, newState)
	}
}

// markTasksRunning sets DDL tasks to running state with a start timestamp.
func (c *LocalClient) markTasksRunning(ctx context.Context, tasks []*storage.Task) {
	now := time.Now()
	for _, task := range tasks {
		task.State = state.Task.Running
		task.StartedAt = &now
		task.UpdatedAt = now
		if err := c.storage.Tasks().Update(ctx, task); err != nil {
			c.logger.Error("failed to update task state", "task_id", task.TaskIdentifier, "state", state.Task.Running, "error", err)
		}
	}
}

// runWithRecovery wraps an apply function with panic recovery so a single panic
// doesn't crash the entire process. On panic, all tasks and the apply are marked failed.
func (c *LocalClient) runWithRecovery(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			errMsg := fmt.Sprintf("panic in apply goroutine: %v", r)
			c.logger.Error(errMsg, "apply_id", apply.ApplyIdentifier)
			c.failApplyWithTasks(ctx, apply, tasks, errMsg)
		}
	}()
	fn()
}

// groupedApplyMode classifies the grouped-apply strategy for a drive. It reads
// DeferCutover from the effective options map (which may carry an automatic
// barrier-park decision, see effectiveCopyDriveOptions) rather than from
// apply.GetOptions(), so an operation-scoped copy drive that must park at the
// cutover barrier takes the atomic-cutover path.
func groupedApplyMode(apply *storage.Apply, options map[string]string) string {
	opts := storage.ApplyOptionsFromMap(options)
	switch {
	case apply.DatabaseType == storage.DatabaseTypeMySQL && opts.DeferCutover:
		return "spirit_atomic_cutover"
	case apply.DatabaseType == storage.DatabaseTypeVitess:
		return "vitess_deploy_request"
	default:
		return "grouped_engine_apply"
	}
}

func groupedApplyModeDescription(apply *storage.Apply, options map[string]string) string {
	switch groupedApplyMode(apply, options) {
	case "spirit_atomic_cutover":
		return "Spirit atomic cutover"
	case "vitess_deploy_request":
		return "Vitess deploy request"
	default:
		return "grouped engine apply"
	}
}

func (c *LocalClient) usesGroupedApply(apply *storage.Apply, options map[string]string) bool {
	if apply.DatabaseType == storage.DatabaseTypeVitess {
		return true
	}
	return apply.DatabaseType == storage.DatabaseTypeMySQL && storage.ApplyOptionsFromMap(options).DeferCutover
}

func (c *LocalClient) setApplyCancel(cancel context.CancelFunc) uint64 {
	c.cancelMu.Lock()
	c.cancelApplyGeneration++
	generation := c.cancelApplyGeneration
	c.cancelApply = cancel
	c.cancelMu.Unlock()
	return generation
}

func (c *LocalClient) clearApplyCancel(generation uint64) {
	c.cancelMu.Lock()
	if c.cancelApplyGeneration == generation {
		c.cancelApply = nil
	}
	c.cancelMu.Unlock()
}

func (c *LocalClient) currentApplyCancel() applyCancelHandle {
	c.cancelMu.Lock()
	defer c.cancelMu.Unlock()
	return applyCancelHandle{generation: c.cancelApplyGeneration, cancel: c.cancelApply}
}

func (c *LocalClient) cancelApplyHandle(handle applyCancelHandle) {
	if handle.cancel != nil {
		handle.cancel()
	}
	c.cancelMu.Lock()
	if c.cancelApplyGeneration == handle.generation {
		c.cancelApply = nil
	}
	c.cancelMu.Unlock()
}

func (c *LocalClient) startApplyExecution(ctx context.Context, cancelGeneration uint64, cancel context.CancelFunc, apply *storage.Apply, tasks []*storage.Task, plan *storage.Plan, options map[string]string, releaseAtCutoverBarrier bool) {
	go func() {
		defer c.clearApplyCancel(cancelGeneration)
		defer cancel()
		c.runApplyExecution(ctx, apply, tasks, plan, options, releaseAtCutoverBarrier)
	}()
}

func (c *LocalClient) runApplyExecution(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, plan *storage.Plan, options map[string]string, releaseAtCutoverBarrier bool) {
	if c.usesGroupedApply(apply, options) {
		c.runWithRecovery(ctx, apply, tasks, func() {
			c.executeGroupedApply(ctx, apply, tasks, plan, options, releaseAtCutoverBarrier)
		})
		return
	}

	c.runWithRecovery(ctx, apply, tasks, func() {
		c.executeApplySequential(ctx, apply, tasks, plan, options)
	})
}

// executeGroupedApply runs all DDLs in one engine operation. For Spirit with
// defer_cutover, this is atomic cutover; for Vitess, this is one deploy request.

// deriveOverallState determines the overall state from a list of tasks.
// Priority order:
// 1. RUNNING/WAITING_FOR_CUTOVER/CUTTING_OVER - active work in progress
// 2. FAILED - at least one task failed (CANCELLED tasks also indicate failure)
// 3. FAILED_RETRYABLE - operator recovery may retry failed task work
// 4. PENDING - more work queued
// 5. STOPPED - apply was stopped (even if some tasks completed)
// 6. COMPLETED - all tasks completed successfully
func deriveOverallState(tasks []*storage.Task) string {
	if len(tasks) == 0 {
		return state.Task.Pending
	}

	var hasRunning, hasPending, hasStopped, hasFailed, hasRetryableFailed, hasCancelled, hasCompleted, hasRevertWindow bool
	var runningState string

	for _, t := range tasks {
		switch t.State {
		case state.Task.Running:
			hasRunning = true
			runningState = state.Task.Running
		case state.Task.WaitingForCutover:
			hasRunning = true
			runningState = state.Task.WaitingForCutover
		case state.Task.CuttingOver:
			hasRunning = true
			runningState = state.Task.CuttingOver
		case state.Task.Pending:
			hasPending = true
		case state.Task.Stopped:
			hasStopped = true
		case state.Task.Failed:
			hasFailed = true
		case state.Task.FailedRetryable:
			hasRetryableFailed = true
		case state.Task.Cancelled:
			hasCancelled = true
		case state.Task.Completed:
			hasCompleted = true
		case state.Task.RevertWindow:
			hasRevertWindow = true
		}
	}

	// Priority order
	if hasRunning {
		return runningState
	}
	if hasFailed || hasCancelled {
		// Cancelled implies a prior task failed (sequential mode), so overall is failed.
		// For Vitess cancellation (user-initiated), the apply state is set directly.
		return state.Task.Failed
	}
	if hasRetryableFailed {
		return state.Task.FailedRetryable
	}
	if hasPending {
		return state.Task.Pending
	}
	if hasStopped {
		return state.Task.Stopped
	}
	if hasRevertWindow {
		return state.Task.RevertWindow
	}
	if hasCompleted {
		return state.Task.Completed
	}

	// Fallback to first task's state
	return tasks[0].State
}

// deriveApplyPhase returns the apply state transition from an engine event.
// Returns empty string if the event is informational (no state transition).
func deriveApplyPhase(event engine.ApplyEvent) string {
	return event.NewState
}

// applyEventStateTransition updates an apply's state based on an engine event.
// Skips the write if the state hasn't changed. On DB write failure, rolls back
// the in-memory state so the next event with the same NewState retries.
// Returns the new state if a transition occurred, or empty string if skipped.
func applyEventStateTransition(apply *storage.Apply, event engine.ApplyEvent, updateFn func(*storage.Apply) error, logger *slog.Logger) string {
	oldState := apply.State
	newState := deriveApplyPhase(event)
	if newState == "" || newState == oldState {
		return ""
	}
	apply.State = newState
	apply.UpdatedAt = time.Now()
	if err := updateFn(apply); err != nil {
		logger.Error("failed to update apply phase", "apply_id", apply.ApplyIdentifier, "state", newState, "error", err)
		apply.State = oldState
		return ""
	}
	return newState
}
