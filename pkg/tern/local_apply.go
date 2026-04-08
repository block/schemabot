package tern

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
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
// If the engine reports terminal or "no active schema change", the task is updated in storage.
// Returns true if the task was resolved (no longer blocking).
func (c *LocalClient) tryResolveStaleTask(ctx context.Context, t *storage.Task, database string) bool {
	eng := c.getEngine()
	if eng == nil {
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

	// Spirit has no active schema change but task isn't terminal — task is stale.
	// Crashed or failed without updating storage.
	if result.Message == "No active schema change" {
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
	_ = c.storage.Tasks().Update(ctx, task)
	if logMsg != "" && applyID > 0 {
		taskID := task.ID
		c.logApplyEvent(ctx, applyID, &taskID, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			logMsg, oldState, newState)
	}
}

// markTasksRunning sets all tasks to running state with a start timestamp.
func (c *LocalClient) markTasksRunning(ctx context.Context, tasks []*storage.Task) {
	now := time.Now()
	for _, task := range tasks {
		task.State = state.Task.Running
		task.StartedAt = &now
		task.UpdatedAt = now
		_ = c.storage.Tasks().Update(ctx, task)
	}
}

// executeApplyAtomic runs all DDLs in one Spirit call for atomic cutover (--defer-cutover).
// All tables copy together and cut over atomically.
func (c *LocalClient) executeApplyAtomic(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, plan *storage.Plan, options map[string]string) {
	defer c.startApplyHeartbeat(ctx, apply)()
	creds := c.credentials()

	// Extract all DDLs and table names from tasks
	ddl := make([]string, len(tasks))
	tableNames := make([]string, len(tasks))
	for i, t := range tasks {
		ddl[i] = t.DDL
		tableNames[i] = t.TableName
	}

	// Log atomic mode start
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
		fmt.Sprintf("Starting atomic mode (defer-cutover) with %d tables: %v", len(tasks), tableNames), "", "")

	eng := c.getEngine()
	defer c.setupSpiritLogging(ctx, apply, tasks)()

	// Call engine to apply all DDLs together
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
		"Calling engine.Apply for all tables", "", "")

	// Build per-table changes for the engine
	var tableChanges []engine.TableChange
	for _, t := range tasks {
		tableChanges = append(tableChanges, engine.TableChange{
			Table: t.TableName,
			DDL:   t.DDL,
		})
	}

	// Atomic mode: all DDLs in one engine call. Use the apply identifier as
	// MigrationContext so all migrations share one context for progress tracking.
	result, err := eng.Apply(ctx, &engine.ApplyRequest{
		Database: apply.Database,
		Changes: []engine.SchemaChange{{
			Namespace:    apply.Database,
			TableChanges: tableChanges,
		}},
		Options:     options,
		ResumeState: &engine.ResumeState{MigrationContext: apply.ApplyIdentifier},
		Credentials: creds,
	})

	if err != nil {
		c.failApplyWithTasks(ctx, apply, tasks, err.Error())
		c.logger.Error("atomic apply failed", "error", err, "apply_id", apply.ApplyIdentifier)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelError, storage.LogEventError, storage.LogSourceSchemaBot,
			fmt.Sprintf("Engine apply failed: %v", err), state.Apply.Pending, state.Apply.Failed)
		return
	}

	if !result.Accepted {
		c.failApplyWithTasks(ctx, apply, tasks, result.Message)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelError, storage.LogEventError, storage.LogSourceSchemaBot,
			fmt.Sprintf("Engine apply not accepted: %s", result.Message), state.Apply.Pending, state.Apply.Failed)
		return
	}

	// All tasks start running together
	c.markTasksRunning(ctx, tasks)
	now := time.Now()
	apply.State = state.Apply.Running
	apply.StartedAt = &now
	apply.UpdatedAt = now
	_ = c.storage.Applies().Update(ctx, apply)
	c.logger.Info("atomic apply started", "apply_id", apply.ApplyIdentifier, "task_count", len(tasks))
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
		fmt.Sprintf("All %d tables started copying in parallel", len(tasks)), state.Apply.Pending, state.Apply.Running)

	// Poll for completion - all tasks share the same state
	c.pollForCompletionAtomic(ctx, apply, tasks, creds)
}

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
	_ = c.storage.Applies().Update(ctx, apply)

	defer c.startApplyHeartbeat(ctx, apply)()

	var failedTask *storage.Task
	var stoppedByUser bool

	for i, task := range tasks {
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

		action = c.runEngineTask(ctx, task, plan, options, creds)
		if action == taskFailed {
			failedTask = task
			break
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
func (c *LocalClient) runEngineTask(ctx context.Context, task *storage.Task, plan *storage.Plan, options map[string]string, creds *engine.Credentials) taskAction {
	// Sequential mode: one DDL per engine call. Use the task identifier as
	// MigrationContext so each table's schema change is tracked independently.
	result, err := c.getEngine().Apply(ctx, &engine.ApplyRequest{
		Database: task.Database,
		Changes: []engine.SchemaChange{{
			Namespace:    task.Database,
			TableChanges: []engine.TableChange{{Table: task.TableName, DDL: task.DDL}},
		}},
		Options:     options,
		ResumeState: &engine.ResumeState{MigrationContext: task.TaskIdentifier},
		Credentials: creds,
	})

	if err != nil {
		c.markTaskFailed(ctx, task, err.Error())
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
	c.pollTaskToCompletion(ctx, task, creds)

	switch task.State {
	case state.Task.Failed:
		return taskFailed
	case state.Task.Stopped:
		return taskStopped
	default:
		return taskContinue
	}
}

// atomicPollState tracks mutable state across polling ticks in atomic mode.
type atomicPollState struct {
	lastTaskState   string
	lastLoggedState string
	lastProgressLog time.Time
}

// startApplyHeartbeat starts a background goroutine that heartbeats the apply
// every 10 seconds, preventing the recovery worker from treating it as crashed.
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
func (c *LocalClient) pollForCompletionAtomic(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, creds *engine.Credentials) {
	eng := c.getEngine()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	ps := &atomicPollState{lastProgressLog: time.Now()}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if done := c.handleAtomicProgressTick(ctx, eng, apply, tasks, creds, ps); done {
				return
			}
		}
	}
}

// handleAtomicProgressTick processes a single progress poll tick in atomic mode.
// Returns true when the apply has reached a terminal state.
func (c *LocalClient) handleAtomicProgressTick(ctx context.Context, eng engine.Engine, apply *storage.Apply, tasks []*storage.Task, creds *engine.Credentials, ps *atomicPollState) bool {
	result, err := eng.Progress(ctx, &engine.ProgressRequest{
		Database:    apply.Database,
		Credentials: creds,
	})
	if err != nil {
		c.logger.Warn("progress check failed", "error", err, "apply_id", apply.ApplyIdentifier)
		return false
	}

	now := time.Now()
	newState := engineStateToStorage(result.State)

	// Log state transitions
	if newState != ps.lastTaskState {
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			fmt.Sprintf("State changed to %s", newState), ps.lastTaskState, newState)
		ps.lastTaskState = newState
	}

	// Log progress every 10 seconds
	c.logAtomicProgress(ctx, apply, result, ps, now)

	// Update all tasks with engine progress
	c.syncAtomicTaskProgress(ctx, tasks, result, newState, now)

	// Auto-trigger cutover if waiting and not in defer mode
	if result.State == engine.StateWaitingForCutover && !apply.GetOptions().DeferCutover {
		c.logger.Info("auto-triggering cutover (not in defer mode)", "apply_id", apply.ApplyIdentifier)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventCutoverTriggered, storage.LogSourceSchemaBot,
			"Auto-triggering cutover (defer_cutover not set)", "", "")
		if _, err := eng.Cutover(ctx, &engine.ControlRequest{Database: apply.Database, Credentials: creds}); err != nil {
			c.logger.Error("auto-cutover failed", "error", err, "apply_id", apply.ApplyIdentifier)
		}
	}

	// Update apply state
	apply.State = taskStateToApplyState(newState)
	apply.UpdatedAt = now

	if result.State.IsTerminal() {
		apply.CompletedAt = &now
		_ = c.storage.Applies().Update(ctx, apply)
		c.logger.Info("atomic apply completed", "apply_id", apply.ApplyIdentifier, "state", result.State, "task_count", len(tasks))
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			fmt.Sprintf("Apply completed with state: %s", result.State), ps.lastTaskState, apply.State)
		return true
	}

	_ = c.storage.Applies().Update(ctx, apply)
	return false
}

// logAtomicProgress logs per-table progress to apply_logs every 10 seconds.
func (c *LocalClient) logAtomicProgress(ctx context.Context, apply *storage.Apply, result *engine.ProgressResult, ps *atomicPollState, now time.Time) {
	if time.Since(ps.lastProgressLog) <= 10*time.Second || len(result.Tables) == 0 {
		return
	}
	var parts []string
	for _, t := range result.Tables {
		if t.RowsTotal > 0 {
			pct := float64(t.RowsCopied) / float64(t.RowsTotal) * 100
			parts = append(parts, fmt.Sprintf("%s: %.1f%%", t.Table, pct))
		} else {
			parts = append(parts, fmt.Sprintf("%s: %s", t.Table, t.State))
		}
	}
	if len(parts) > 0 && result.Message != ps.lastLoggedState {
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventProgress, storage.LogSourceSchemaBot,
			fmt.Sprintf("Progress: %s (%s)", strings.Join(parts, ", "), result.Message), "", "")
		ps.lastLoggedState = result.Message
	}
	ps.lastProgressLog = now
}

// syncAtomicTaskProgress updates all tasks with engine state and per-table progress.
func (c *LocalClient) syncAtomicTaskProgress(ctx context.Context, tasks []*storage.Task, result *engine.ProgressResult, newState string, now time.Time) {
	tableProgress := make(map[string]engine.TableProgress, len(result.Tables))
	for _, tp := range result.Tables {
		tableProgress[tp.Table] = tp
	}

	for _, task := range tasks {
		if result.State.IsTerminal() {
			task.CompletedAt = &now
		}
		if tp, ok := tableProgress[task.TableName]; ok {
			task.RowsCopied = tp.RowsCopied
			task.RowsTotal = tp.RowsTotal
			task.ProgressPercent = tp.Progress
			task.ETASeconds = int(tp.ETASeconds)
		}
		c.transitionTaskState(ctx, task, 0, newState, "")
	}
}

// pollTaskToCompletion polls a single task to completion (sequential mode).
func (c *LocalClient) pollTaskToCompletion(ctx context.Context, task *storage.Task, creds *engine.Credentials) {
	eng := c.getEngine()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Re-fetch task state from storage to detect external changes (e.g., Stop).
			// This also guards against a race where a new apply starts and the engine's
			// runningMigration no longer corresponds to this task.
			freshTask, fetchErr := c.storage.Tasks().Get(ctx, task.TaskIdentifier)
			if fetchErr == nil && freshTask != nil && state.IsTerminalTaskState(freshTask.State) {
				// Task was already marked terminal externally — stop polling
				task.State = freshTask.State
				return
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
			task.State = engineStateToStorage(result.State)
			task.UpdatedAt = now

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
				task.CompletedAt = &now
				if result.State == engine.StateFailed {
					if result.ErrorMessage != "" {
						task.ErrorMessage = result.ErrorMessage
					} else if result.Message != "" {
						task.ErrorMessage = result.Message
					}
				}
				logMsg := ""
				if task.ApplyID > 0 {
					logMsg = fmt.Sprintf("Task %s completed: engine_state=%s message=%q rows=%d/%d",
						task.TaskIdentifier, result.State, result.Message, task.RowsCopied, task.RowsTotal)
				}
				c.transitionTaskState(ctx, task, task.ApplyID, engineStateToStorage(result.State), logMsg)
				c.logger.Info("task completed",
					"task_id", task.TaskIdentifier,
					"table", task.TableName,
					"engine_state", result.State,
					"engine_message", result.Message,
					"prev_storage_state", prevState,
					"rows_copied", task.RowsCopied,
					"rows_total", task.RowsTotal,
				)
				return
			}

			c.transitionTaskState(ctx, task, 0, engineStateToStorage(result.State), "")
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

// failApplyWithTasks marks all tasks and the apply as failed with the given error.
func (c *LocalClient) failApplyWithTasks(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, errMsg string) {
	now := time.Now()
	for _, task := range tasks {
		task.ErrorMessage = errMsg
		task.CompletedAt = &now
		c.transitionTaskState(ctx, task, 0, state.Task.Failed, "")
	}
	apply.State = state.Apply.Failed
	apply.ErrorMessage = errMsg
	apply.CompletedAt = &now
	apply.UpdatedAt = now
	_ = c.storage.Applies().Update(ctx, apply)
}

// finalizeSequentialApply updates the apply state based on sequential task outcomes.
// On failure, remaining PENDING tasks are cancelled.
func (c *LocalClient) finalizeSequentialApply(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, failedTask *storage.Task, stoppedByUser bool) {
	now := time.Now()
	switch {
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
	_ = c.storage.Applies().Update(ctx, apply)
}

// deriveOverallState determines the overall state from a list of tasks.
// Priority order:
// 1. RUNNING/WAITING_FOR_CUTOVER/CUTTING_OVER - active work in progress
// 2. PENDING - more work queued
// 3. STOPPED - apply was stopped (even if some tasks completed)
// 4. FAILED - at least one task failed (CANCELLED tasks also indicate failure)
// 5. COMPLETED - all tasks completed successfully
func deriveOverallState(tasks []*storage.Task) string {
	if len(tasks) == 0 {
		return state.Task.Pending
	}

	var hasRunning, hasPending, hasStopped, hasFailed, hasCancelled, hasCompleted bool
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
		case state.Task.Cancelled:
			hasCancelled = true
		case state.Task.Completed:
			hasCompleted = true
		}
	}

	// Priority order
	if hasRunning {
		return runningState
	}
	if hasPending {
		return state.Task.Pending
	}
	if hasStopped {
		return state.Task.Stopped
	}
	if hasFailed || hasCancelled {
		// CANCELLED implies a prior task failed, so overall state is FAILED
		return state.Task.Failed
	}
	if hasCompleted {
		return state.Task.Completed
	}

	// Fallback to first task's state
	return tasks[0].State
}
