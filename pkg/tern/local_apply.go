package tern

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/spirit"
	"github.com/block/schemabot/pkg/metrics"
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
// VSchema tasks are skipped — their state is driven by the deploy request
// lifecycle (deriveVSchemaTaskState), not by apply start.
func (c *LocalClient) markTasksRunning(ctx context.Context, tasks []*storage.Task) {
	now := time.Now()
	for _, task := range tasks {
		if task.DDLAction == "vschema_update" {
			continue
		}
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

func groupedApplyMode(apply *storage.Apply) string {
	opts := apply.GetOptions()
	switch {
	case apply.DatabaseType == storage.DatabaseTypeMySQL && opts.DeferCutover:
		return "spirit_atomic_cutover"
	case apply.DatabaseType == storage.DatabaseTypeVitess:
		return "vitess_deploy_request"
	default:
		return "grouped_engine_apply"
	}
}

func groupedApplyModeDescription(apply *storage.Apply) string {
	switch groupedApplyMode(apply) {
	case "spirit_atomic_cutover":
		return "Spirit atomic cutover"
	case "vitess_deploy_request":
		return "Vitess deploy request"
	default:
		return "grouped engine apply"
	}
}

func (c *LocalClient) usesGroupedApply(apply *storage.Apply) bool {
	if apply.DatabaseType == storage.DatabaseTypeVitess {
		return true
	}
	return apply.DatabaseType == storage.DatabaseTypeMySQL && apply.GetOptions().DeferCutover
}

func (c *LocalClient) setApplyCancel(cancel context.CancelFunc) {
	c.cancelMu.Lock()
	c.cancelApply = cancel
	c.cancelMu.Unlock()
}

func (c *LocalClient) clearApplyCancel() {
	c.cancelMu.Lock()
	c.cancelApply = nil
	c.cancelMu.Unlock()
}

func (c *LocalClient) startApplyExecution(ctx context.Context, cancel context.CancelFunc, apply *storage.Apply, tasks []*storage.Task, plan *storage.Plan, options map[string]string) {
	go func() {
		defer c.clearApplyCancel()
		defer cancel()
		c.runApplyExecution(ctx, apply, tasks, plan, options)
	}()
}

func (c *LocalClient) runApplyExecution(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, plan *storage.Plan, options map[string]string) {
	if c.usesGroupedApply(apply) {
		c.runWithRecovery(ctx, apply, tasks, func() {
			c.executeGroupedApply(ctx, apply, tasks, plan, options)
		})
		return
	}

	c.runWithRecovery(ctx, apply, tasks, func() {
		c.executeApplySequential(ctx, apply, tasks, plan, options)
	})
}

// executeGroupedApply runs all DDLs in one engine operation. For Spirit with
// defer_cutover, this is atomic cutover; for Vitess, this is one deploy request.
func (c *LocalClient) executeGroupedApply(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, plan *storage.Plan, options map[string]string) {
	defer c.startApplyHeartbeat(ctx, apply)()
	creds := c.credentials()
	mode := groupedApplyMode(apply)
	modeDescription := groupedApplyModeDescription(apply)

	// Extract all DDLs and table names from tasks
	ddl := make([]string, len(tasks))
	tableNames := make([]string, len(tasks))
	for i, t := range tasks {
		ddl[i] = t.DDL
		tableNames[i] = t.TableName
	}

	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
		fmt.Sprintf("Starting %s with %d tables: %v", modeDescription, len(tasks), tableNames), "", "")

	eng := c.getEngine()
	defer c.setupSpiritLogging(ctx, apply, tasks)()

	// Call engine to apply all DDLs together
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
		"Calling engine.Apply for all tables", "", "")

	// Build per-namespace changes from the plan. For Vitess databases, each
	// namespace is a keyspace (e.g., "testapp", "testapp_sharded"). For MySQL,
	c.logger.Info("building changes from plan", "namespaces", len(plan.Namespaces), "plan_id", plan.PlanIdentifier)
	if len(plan.Namespaces) == 0 {
		c.failApplyWithTasks(ctx, apply, tasks, "plan has no namespace data")
		return
	}
	if c.config.Type == storage.DatabaseTypeMySQL && len(plan.Namespaces) > 1 {
		var names []string
		for ns := range plan.Namespaces {
			names = append(names, ns)
		}
		c.failApplyWithTasks(ctx, apply, tasks,
			fmt.Sprintf("MySQL applies support one namespace per apply, but plan has %d: %v", len(plan.Namespaces), names))
		return
	}
	changes := planNamespacesToChanges(plan.Namespaces)

	// For Vitess: initialize the VitessApplyData row before the engine starts.
	// State transitions (preparing_branch, applying_branch_changes, etc.) are
	// handled by the engine via ApplyEvent.NewState in the OnEvent callback.
	if c.config.Type == storage.DatabaseTypeVitess {
		if err := c.storage.VitessApplyData().Save(ctx, &storage.VitessApplyData{
			ApplyID:          apply.ID,
			MigrationContext: apply.ApplyIdentifier,
		}); err != nil {
			c.logger.Error("failed to save vitess apply data", "apply_id", apply.ID, "error", err)
		}
	}

	// Mark the apply as started before calling the engine. The engine may run
	// for a long time (branch creation, DDL application, deploy request) and
	// started_at should reflect when work actually began, not when it finished.
	now := time.Now()
	apply.StartedAt = &now
	apply.UpdatedAt = now
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		c.logger.Error("failed to set started_at", "apply_id", apply.ApplyIdentifier, "error", err)
	}

	// Grouped mode: all DDLs in one engine call. Use the apply identifier as
	// MigrationContext so all table work shares one context for progress tracking.
	result, err := eng.Apply(ctx, &engine.ApplyRequest{
		Database:    apply.Database,
		PlanID:      plan.PlanIdentifier,
		Changes:     changes,
		SchemaFiles: plan.SchemaFiles,
		Options:     options,
		ResumeState: &engine.ResumeState{MigrationContext: apply.ApplyIdentifier},
		Credentials: creds,
		OnEvent: func(event engine.ApplyEvent) {
			oldState := apply.State
			newState := deriveApplyPhase(event)
			c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
				event.Message, oldState, newState)
			applyEventStateTransition(apply, event, func(a *storage.Apply) error {
				return c.storage.Applies().Update(ctx, a)
			}, c.logger)
		},
		OnStateChange: func(rs *engine.ResumeState) {
			if rs == nil {
				c.logger.Debug("OnStateChange: nil resume state", "apply_id", apply.ApplyIdentifier)
				return
			}
			meta, err := decodePSMetadataForStorage(rs.Metadata)
			if err != nil {
				c.logger.Warn("OnStateChange: failed to decode metadata", "apply_id", apply.ApplyIdentifier, "error", err)
				return
			}
			if meta == nil {
				c.logger.Warn("OnStateChange: no PS metadata in resume state", "apply_id", apply.ApplyIdentifier)
				return
			}
			if saveErr := c.storage.VitessApplyData().Save(ctx, &storage.VitessApplyData{
				ApplyID:          apply.ID,
				BranchName:       meta.BranchName,
				DeployRequestID:  meta.DeployRequestID,
				MigrationContext: rs.MigrationContext,
				DeployRequestURL: meta.DeployRequestURL,
				IsInstant:        meta.IsInstant,
				DeferredDeploy:   meta.DeferredDeploy,
			}); saveErr != nil {
				c.logger.Warn("OnStateChange: failed to persist resume state", "apply_id", apply.ApplyIdentifier, "error", saveErr)
			}
		},
	})

	if err != nil {
		newState := state.Apply.Failed
		if c.shouldRetryEngineError(err) {
			c.markApplyRetryableWithTasks(ctx, apply, tasks, err.Error())
			newState = state.Apply.FailedRetryable
		} else {
			c.failApplyWithTasks(ctx, apply, tasks, err.Error())
		}
		if newState == state.Apply.FailedRetryable {
			c.logger.Warn("apply paused for scheduler retry", "mode", mode, "error", err, "apply_id", apply.ApplyIdentifier)
		} else {
			c.logger.Error("apply failed", "mode", mode, "error", err, "apply_id", apply.ApplyIdentifier)
		}
		logLevel := storage.LogLevelError
		if newState == state.Apply.FailedRetryable {
			logLevel = storage.LogLevelWarn
		}
		c.logApplyEvent(ctx, apply.ID, nil, logLevel, storage.LogEventError, storage.LogSourceSchemaBot,
			fmt.Sprintf("Engine apply failed: %v", err), state.Apply.Pending, newState)
		return
	}

	if !result.Accepted {
		c.failApplyWithTasks(ctx, apply, tasks, result.Message)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelError, storage.LogEventError, storage.LogSourceSchemaBot,
			fmt.Sprintf("Engine apply not accepted: %s", result.Message), state.Apply.Pending, state.Apply.Failed)
		return
	}

	// Save vitess_apply_data and set IsInstant on tasks BEFORE marking running.
	// The progress handler reads both vitess_apply_data.is_instant and task.is_instant
	// to determine the instant label — both must be committed before the first poll.
	var resumeState *engine.ResumeState
	if result.ResumeState != nil {
		resumeState = result.ResumeState
		if meta, err := decodePSMetadataForStorage(resumeState.Metadata); meta != nil && err == nil {
			c.logger.Info("saving VitessApplyData from apply result",
				"apply_id", apply.ApplyIdentifier,
				"is_instant", meta.IsInstant,
				"deploy_request_id", meta.DeployRequestID,
				"raw_metadata", resumeState.Metadata[:min(len(resumeState.Metadata), 200)],
			)
			if saveErr := c.storage.VitessApplyData().Save(ctx, &storage.VitessApplyData{
				ApplyID:          apply.ID,
				BranchName:       meta.BranchName,
				DeployRequestID:  meta.DeployRequestID,
				MigrationContext: resumeState.MigrationContext,
				DeployRequestURL: meta.DeployRequestURL,
				IsInstant:        meta.IsInstant,
				DeferredDeploy:   meta.DeferredDeploy,
			}); saveErr != nil {
				c.logger.Warn("failed to save vitess apply data", "apply_id", apply.ApplyIdentifier, "error", saveErr)
			}
		}
	}

	if result.ResumeState != nil {
		if meta, err := decodePSMetadataForStorage(result.ResumeState.Metadata); meta != nil && err == nil && meta.IsInstant {
			for _, task := range tasks {
				task.IsInstant = true
			}
		}
	}
	c.markTasksRunning(ctx, tasks)
	if c.config.Type == storage.DatabaseTypeVitess {
		apply.State = state.Apply.ValidatingDeployRequest
	} else {
		apply.State = state.Apply.Running
	}
	apply.UpdatedAt = time.Now()
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		c.logger.Error("failed to update apply state", "apply_id", apply.ApplyIdentifier, "state", state.Apply.Running, "error", err)
	}
	c.logger.Info("apply started", "mode", mode, "apply_id", apply.ApplyIdentifier, "task_count", len(tasks))
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
		fmt.Sprintf("All %d tables started copying in parallel", len(tasks)), state.Apply.Pending, apply.State)

	// Poll for completion - all tasks share the same state
	c.pollForCompletionAtomic(ctx, apply, tasks, creds, resumeState)
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
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		c.logger.Error("failed to update apply state", "apply_id", apply.ApplyIdentifier, "state", state.Apply.Running, "error", err)
	}

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

		// Notify observer after each task completes
		if obs := c.getObserver(apply.ID); obs != nil {
			obs.OnProgress(apply, tasks)
		}

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
	c.pollTaskToCompletion(ctx, task, creds)

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
func (c *LocalClient) pollForCompletionAtomic(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, creds *engine.Credentials, resumeState *engine.ResumeState) {
	eng := c.getEngine()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	ps := &atomicPollState{lastProgressLog: time.Now()}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if done := c.handleAtomicProgressTick(ctx, eng, apply, tasks, creds, resumeState, ps); done {
				return
			}
		}
	}
}

// handleAtomicProgressTick processes a single progress poll tick in atomic mode.
// Returns true when the apply has reached a terminal state.
func (c *LocalClient) handleAtomicProgressTick(ctx context.Context, eng engine.Engine, apply *storage.Apply, tasks []*storage.Task, creds *engine.Credentials, resumeState *engine.ResumeState, ps *atomicPollState) bool {
	result, err := eng.Progress(ctx, &engine.ProgressRequest{
		Database:    apply.Database,
		Credentials: creds,
		ResumeState: resumeState,
	})
	if err != nil {
		// Permanent errors (e.g., deploy request not found) fail immediately.
		var permanent *engine.PermanentError
		if errors.As(err, &permanent) {
			c.logger.Error("progress check failed with permanent error",
				"error", err, "apply_id", apply.ApplyIdentifier)
			c.failApplyWithTasks(ctx, apply, tasks, fmt.Sprintf("progress polling failed: %v", err))
			return true
		}
		ps.consecutiveErrors++
		c.logger.Warn("progress check failed",
			"error", err, "apply_id", apply.ApplyIdentifier, "consecutive_errors", ps.consecutiveErrors)
		if ps.consecutiveErrors >= 10 {
			if c.shouldRetryEngineError(err) {
				c.logger.Warn("progress polling failed repeatedly, pausing apply for scheduler retry",
					"apply_id", apply.ApplyIdentifier, "consecutive_errors", ps.consecutiveErrors)
				c.markApplyRetryableWithTasks(ctx, apply, tasks, fmt.Sprintf("progress polling failed after %d consecutive errors: %v", ps.consecutiveErrors, err))
				return true
			}
			c.logger.Error("progress polling failed repeatedly, failing apply",
				"apply_id", apply.ApplyIdentifier, "consecutive_errors", ps.consecutiveErrors)
			c.failApplyWithTasks(ctx, apply, tasks, fmt.Sprintf("progress polling failed after %d consecutive errors: %v", ps.consecutiveErrors, err))
			return true
		}
		return false
	}
	ps.consecutiveErrors = 0

	// Update resumeState if the engine returned a newer one (e.g., with
	// updated metadata like deploy request URL or migration context).
	if result.ResumeState != nil && resumeState != nil {
		*resumeState = *result.ResumeState
	}

	now := time.Now()
	newState := taskStateFromProgressResult(result)

	// Log state transitions and track when waiting states are entered (for timeouts)
	if newState != ps.lastTaskState {
		msg := fmt.Sprintf("State changed to %s", newState)
		if result.Message != "" {
			msg = fmt.Sprintf("State changed to %s (%s)", newState, result.Message)
		}
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			msg, ps.lastTaskState, newState)
		ps.lastTaskState = newState
		ps.stateEnteredAt = now
	}

	// Log progress every 10 seconds
	c.logAtomicProgress(ctx, apply, result, ps, now)

	// Update all tasks with engine progress
	c.syncAtomicTaskProgress(ctx, tasks, result, newState, now)

	opts := apply.GetOptions()
	controlReq := &engine.ControlRequest{
		Database:    apply.Database,
		Credentials: creds,
		ResumeState: resumeState,
	}

	// Auto-trigger deploy if waiting and not in defer-deploy mode
	if result.State == engine.StateWaitingForDeploy && !opts.DeferDeploy {
		c.logger.Info("auto-triggering deploy (not in defer-deploy mode)", "apply_id", apply.ApplyIdentifier)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventDeployTriggered, storage.LogSourceSchemaBot,
			"Auto-triggering deploy (defer_deploy not set)", "", "")
		if _, err := eng.Start(ctx, controlReq); err != nil {
			c.logger.Error("auto-deploy failed", "error", err, "apply_id", apply.ApplyIdentifier)
		}
	}

	// Auto-trigger cutover if waiting and not in defer mode
	if result.State == engine.StateWaitingForCutover && !opts.DeferCutover {
		c.logger.Info("auto-triggering cutover (not in defer mode)", "apply_id", apply.ApplyIdentifier)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventCutoverTriggered, storage.LogSourceSchemaBot,
			"Auto-triggering cutover (defer_cutover not set)", "", "")
		if _, err := eng.Cutover(ctx, controlReq); err != nil {
			c.logger.Error("auto-cutover failed", "error", err, "apply_id", apply.ApplyIdentifier)
		}
	}

	// Timeout: cancel the apply if waiting for manual deploy too long.
	if result.State == engine.StateWaitingForDeploy && opts.DeferDeploy &&
		!ps.stateEnteredAt.IsZero() && time.Since(ps.stateEnteredAt) > waitingForManualActionTimeout {
		c.logger.Info("waiting-for-deploy timed out, cancelling apply",
			"apply_id", apply.ApplyIdentifier, "timeout", waitingForManualActionTimeout)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			fmt.Sprintf("Waiting for deploy timed out after %s, cancelling", waitingForManualActionTimeout), "", "")
		if _, err := eng.Stop(ctx, controlReq); err != nil {
			c.logger.Error("timeout stop failed", "error", err, "apply_id", apply.ApplyIdentifier)
		}
	}

	// Timeout: cancel the apply if waiting for manual cutover too long.
	if result.State == engine.StateWaitingForCutover && opts.DeferCutover &&
		!ps.stateEnteredAt.IsZero() && time.Since(ps.stateEnteredAt) > waitingForManualActionTimeout {
		c.logger.Info("waiting-for-cutover timed out, cancelling apply",
			"apply_id", apply.ApplyIdentifier, "timeout", waitingForManualActionTimeout)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			fmt.Sprintf("Waiting for cutover timed out after %s, cancelling", waitingForManualActionTimeout), "", "")
		if _, err := eng.Stop(ctx, controlReq); err != nil {
			c.logger.Error("timeout stop failed", "error", err, "apply_id", apply.ApplyIdentifier)
		}
	}

	// If --skip-revert was set, auto-skip the revert window immediately.
	if result.State == engine.StateRevertWindow && opts.SkipRevert && !ps.revertSkipped {
		c.logger.Info("auto-skipping revert window (--skip-revert)",
			"apply_id", apply.ApplyIdentifier,
		)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			"Auto-skipping revert window (--skip-revert)", "", "")
		_, err := eng.SkipRevert(ctx, controlReq)
		if err != nil {
			c.logger.Error("auto-skip revert failed", "error", err, "apply_id", apply.ApplyIdentifier)
		} else {
			c.logger.Info("skip-revert triggered", "apply_id", apply.ApplyIdentifier, "reason", "--skip-revert")
			c.markRevertSkipped(ctx, apply)
		}
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventSkipRevertTriggered, storage.LogSourceSchemaBot,
			"Skip-revert triggered (--skip-revert)", state.Apply.RevertWindow, "")
		ps.revertSkipped = true
	}

	// Revert window enabled (default): auto-skip based on deployed_at + configured duration.
	// Falls back to stateEnteredAt if deployed_at is unavailable.
	if result.State == engine.StateRevertWindow && !opts.SkipRevert && !ps.revertSkipped {
		revertDeadline := c.revertWindowDeadline(result.ResumeState, ps.stateEnteredAt)
		if !revertDeadline.IsZero() && now.After(revertDeadline) {
			c.logger.Info("revert window expired, skipping", "apply_id", apply.ApplyIdentifier, "deadline", revertDeadline)
			c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
				"Revert window expired, finalizing", "", "")
			if _, err := eng.SkipRevert(ctx, controlReq); err != nil {
				c.logger.Error("revert window timeout skip failed", "error", err, "apply_id", apply.ApplyIdentifier)
			} else {
				c.markRevertSkipped(ctx, apply)
				c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventSkipRevertTriggered, storage.LogSourceSchemaBot,
					"Revert window expired, skip-revert triggered", state.Apply.RevertWindow, "")
			}
			ps.revertSkipped = true
		}
	}

	// Update apply state
	apply.State = taskStateToApplyState(newState)
	apply.UpdatedAt = now

	if result.State.IsTerminal() {
		retryableFailure := state.IsState(newState, state.Task.FailedRetryable)
		if retryableFailure {
			apply.CompletedAt = nil
		} else {
			apply.CompletedAt = &now
		}
		// Propagate error message from failed tasks to the apply record
		if result.State == engine.StateFailed {
			if msg := progressFailureMessage(result); msg != "" {
				apply.ErrorMessage = msg
			} else {
				for _, task := range tasks {
					if (task.State == state.Task.Failed || task.State == state.Task.FailedRetryable) && task.ErrorMessage != "" {
						apply.ErrorMessage = fmt.Sprintf("table %s failed: %s", task.TableName, task.ErrorMessage)
						break
					}
				}
			}
		}
		if err := c.storage.Applies().Update(ctx, apply); err != nil {
			c.logger.Error("failed to update apply state", "apply_id", apply.ApplyIdentifier, "state", apply.State, "error", err)
		}
		metrics.AdjustActiveApplies(ctx, -1, apply.Database, apply.Environment)
		switch {
		case retryableFailure:
			c.logger.Warn("apply paused for scheduler retry",
				"mode", groupedApplyMode(apply), "apply_id", apply.ApplyIdentifier, "error", apply.ErrorMessage, "task_count", len(tasks))
		case result.State == engine.StateFailed:
			c.logger.Error("apply failed",
				"mode", groupedApplyMode(apply), "apply_id", apply.ApplyIdentifier, "error", apply.ErrorMessage, "task_count", len(tasks))
		default:
			c.logger.Info("apply completed",
				"mode", groupedApplyMode(apply), "apply_id", apply.ApplyIdentifier, "state", result.State, "task_count", len(tasks))
		}
		eventMessage := fmt.Sprintf("Apply completed with state: %s", result.State)
		if retryableFailure {
			eventMessage = "Apply paused for scheduler retry after retryable engine failure"
		}
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			eventMessage, ps.lastTaskState, apply.State)

		if retryableFailure {
			if obs := c.getObserver(apply.ID); obs != nil {
				obs.OnProgress(apply, tasks)
			}
			return true
		}

		// Notify observer of terminal state, then clean up
		if obs := c.getObserver(apply.ID); obs != nil {
			obs.OnTerminal(apply, tasks)
			c.clearObserver(apply.ID)
		}
		return true
	}

	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		c.logger.Error("failed to update apply state", "apply_id", apply.ApplyIdentifier, "state", apply.State, "error", err)
	}

	// Notify observer of progress update
	if obs := c.getObserver(apply.ID); obs != nil {
		obs.OnProgress(apply, tasks)
	}
	return false
}

// markRevertSkipped sets RevertSkippedAt on the VitessApplyData record so
// progress consumers know finalization is in progress.
func (c *LocalClient) markRevertSkipped(ctx context.Context, apply *storage.Apply) {
	now := time.Now()
	if vad, err := c.storage.VitessApplyData().GetByApplyID(ctx, apply.ID); err == nil {
		vad.RevertSkippedAt = &now
		if saveErr := c.storage.VitessApplyData().Save(ctx, vad); saveErr != nil {
			c.logger.Warn("failed to save revert_skipped_at", "apply_id", apply.ApplyIdentifier, "error", saveErr)
		}
	}
}

// revertWindowDuration returns the configured revert window duration,
// falling back to PlanetScale's default of 30 minutes.
func (c *LocalClient) revertWindowDuration() time.Duration {
	if s := c.config.Metadata["revert_window_duration"]; s != "" {
		if d, err := time.ParseDuration(s); err == nil && d > 0 {
			return d
		}
	}
	return defaultRevertWindowDuration
}

// revertWindowDeadline computes when the revert window expires.
// Uses deployed_at from engine metadata (accurate to PlanetScale's clock) plus
// the configured revert period. Falls back to stateEnteredAt if metadata is unavailable.
func (c *LocalClient) revertWindowDeadline(resumeState *engine.ResumeState, stateEnteredAt time.Time) time.Time {
	duration := c.revertWindowDuration()
	if resumeState != nil && resumeState.Metadata != "" {
		if meta, err := decodePSMetadataForStorage(resumeState.Metadata); err == nil && meta != nil && meta.DeployedAt != nil {
			return meta.DeployedAt.Add(duration)
		}
	}
	if !stateEnteredAt.IsZero() {
		return stateEnteredAt.Add(duration)
	}
	return time.Time{}
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
	tableProgress := indexEngineTableProgress(result.Tables)
	retryableFailure := state.IsState(newState, state.Task.FailedRetryable)
	instantFromMetadata := false
	if result.ResumeState != nil && result.ResumeState.Metadata != "" {
		if meta, err := decodePSMetadataForStorage(result.ResumeState.Metadata); err == nil && meta != nil {
			instantFromMetadata = meta.IsInstant
		}
	}

	for _, task := range tasks {
		if retryableFailure && state.IsTerminalTaskState(task.State) {
			continue
		}
		// VSchema tasks follow deploy-request-level state, not per-migration state.
		// They have no SHOW VITESS_MIGRATIONS rows. Their state transitions are:
		// pending → running (during in_progress_vschema) → completed/failed.
		if task.DDLAction == "vschema_update" {
			vsState := c.deriveVSchemaTaskState(task, result, newState, now)
			if vsState != task.State {
				msg := fmt.Sprintf("VSchema %s → %s", task.State, vsState)
				c.transitionTaskState(ctx, task, task.ApplyID, vsState, msg)
			}
			continue
		}

		if tp, ok := engineProgressForTask(tableProgress, task); ok {
			task.RowsCopied = tp.RowsCopied
			task.RowsTotal = tp.RowsTotal
			task.ProgressPercent = tp.Progress
			task.ETASeconds = int(tp.ETASeconds)
			task.IsInstant = tp.IsInstant
			if tp.StartedAt != nil && task.StartedAt == nil {
				task.StartedAt = tp.StartedAt
			}
			if tp.CompletedAt != nil && !retryableFailure && task.CompletedAt == nil {
				task.CompletedAt = tp.CompletedAt
			}
		} else if instantFromMetadata {
			task.IsInstant = true
			if result.State.IsTerminal() && !retryableFailure {
				task.ProgressPercent = 100
			}
		}
		if task.StartedAt == nil && newState != state.Task.Pending {
			task.StartedAt = &now
		}
		if result.State.IsTerminal() && !retryableFailure && task.CompletedAt == nil {
			task.CompletedAt = &now
		}
		if result.State == engine.StateFailed && task.ErrorMessage == "" {
			if msg := progressFailureMessage(result); msg != "" {
				task.ErrorMessage = msg
			}
		}
		if result.State == engine.StateCompleted {
			task.ProgressPercent = 100
		}
		c.transitionTaskState(ctx, task, 0, newState, "")
	}
}

// deriveVSchemaTaskState determines the state for a VSchema task based on
// the engine progress result. VSchema tasks have no per-migration rows in
// SHOW VITESS_MIGRATIONS — their state tracks the deploy request's VSchema
// application phase (in_progress_vschema).
func (c *LocalClient) deriveVSchemaTaskState(task *storage.Task, result *engine.ProgressResult, taskState string, now time.Time) string {
	if state.IsTerminalTaskState(task.State) {
		return task.State
	}

	switch {
	case state.IsState(taskState, state.Task.FailedRetryable):
		if task.ErrorMessage == "" {
			task.ErrorMessage = progressFailureMessage(result)
		}
		return state.Task.FailedRetryable
	case result.Message == engine.MessageApplyingVSchema:
		if task.StartedAt == nil {
			task.StartedAt = &now
		}
		return state.Task.Running
	case result.State == engine.StateFailed:
		if task.CompletedAt == nil {
			task.CompletedAt = &now
		}
		return state.Task.Failed
	case state.IsState(taskState, state.Task.RevertWindow):
		if task.CompletedAt == nil {
			task.CompletedAt = &now
		}
		return state.Task.RevertWindow
	case result.State.IsTerminal(), state.IsState(taskState, state.Task.Completed):
		if task.CompletedAt == nil {
			task.CompletedAt = &now
		}
		return state.Task.Completed
	default:
		return task.State
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
				return
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
	metrics.AdjustActiveApplies(ctx, -1, apply.Database, apply.Environment)
}

// markApplyRetryableWithTasks pauses an apply after a retryable engine failure.
// Non-terminal tasks move to failed_retryable so scheduler recovery can decide
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
	metrics.AdjustActiveApplies(ctx, -1, apply.Database, apply.Environment)
	if obs := c.getObserver(apply.ID); obs != nil {
		obs.OnProgress(apply, tasks)
	}
}

// finalizeSequentialApply updates the apply state based on sequential task outcomes.
// Permanent failures cancel remaining pending tasks; retryable failures leave
// pending tasks queued for scheduler recovery.
func (c *LocalClient) finalizeSequentialApply(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, failedTask *storage.Task, stoppedByUser bool) {
	now := time.Now()
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

// deriveOverallState determines the overall state from a list of tasks.
// Priority order:
// 1. RUNNING/WAITING_FOR_CUTOVER/CUTTING_OVER - active work in progress
// 2. FAILED - at least one task failed (CANCELLED tasks also indicate failure)
// 3. FAILED_RETRYABLE - scheduler recovery may retry failed task work
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

// planNamespacesToChanges converts stored plan namespace data to engine schema
// changes for the Apply call. VSchema metadata is only set when the plan
// stored a VSchema diff (i.e., the Plan detected a real change).
func planNamespacesToChanges(namespaces map[string]*storage.NamespacePlanData) []engine.SchemaChange {
	var changes []engine.SchemaChange
	for namespace, nsData := range namespaces {
		var tableChanges []engine.TableChange
		for _, tc := range nsData.Tables {
			tableChanges = append(tableChanges, engine.TableChange{
				Table:     tc.Table,
				DDL:       tc.DDL,
				Operation: ddl.OpToStatementType(tc.Operation),
			})
		}
		metadata := make(map[string]string)
		if len(nsData.VSchema) > 0 {
			metadata["vschema_changed"] = "true"
		}
		changes = append(changes, engine.SchemaChange{
			Namespace:    namespace,
			TableChanges: tableChanges,
			Metadata:     metadata,
		})
	}
	return changes
}
