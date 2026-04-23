package tern

import (
	"context"
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// Start resumes a stopped schema change.
// On resume, we re-plan against the current DB state to find which DDLs are still
// needed. Tables that completed before the stop are detected as no-ops by the
// diff and their tasks are marked completed. Only the remaining DDLs are sent to
// the engine, which auto-detects Spirit checkpoints for partially-copied tables.
func (c *LocalClient) Start(ctx context.Context, req *ternv1.StartRequest) (*ternv1.StartResponse, error) {
	tasks, err := c.storage.Tasks().GetByDatabase(ctx, c.config.Database)
	if err != nil {
		return nil, fmt.Errorf("get tasks failed: %w", err)
	}

	// Find the target apply: either from the request's apply_id or the most recent stopped apply.
	// We scope to a single apply to avoid cross-contamination: a poller race can
	// erroneously mark tasks from earlier applies as STOPPED (see pollTaskToCompletion).
	var apply *storage.Apply
	maxAge := 7 * 24 * time.Hour

	c.logger.Info("Start: looking for stopped tasks",
		"database", c.config.Database,
		"apply_id", req.ApplyId,
		"task_count", len(tasks),
	)

	if req.ApplyId != "" {
		// Use the explicitly requested apply
		a, err := c.storage.Applies().GetByApplyIdentifier(ctx, req.ApplyId)
		if err != nil || a == nil {
			return nil, fmt.Errorf("apply %s not found", req.ApplyId)
		}
		apply = a
		c.logger.Info("Start: found apply", "apply_internal_id", apply.ID, "apply_identifier", apply.ApplyIdentifier, "state", apply.State)
	} else {
		// First pass: find the most recent stopped apply
		for _, task := range tasks {
			if task.State != state.Task.Stopped {
				continue
			}
			if time.Since(task.UpdatedAt) > maxAge {
				continue
			}
			if task.ApplyID > 0 {
				a, _ := c.storage.Applies().Get(ctx, task.ApplyID)
				if a != nil && (apply == nil || a.UpdatedAt.After(apply.UpdatedAt)) {
					apply = a
				}
			}
		}
	}

	if apply == nil {
		return nil, fmt.Errorf("no stopped schema change to resume")
	}

	// Second pass: collect stopped tasks ONLY from the target apply
	var stoppedTasks []*storage.Task
	for _, task := range tasks {
		c.logger.Info("Start: checking task",
			"task_id", task.TaskIdentifier,
			"table", task.TableName,
			"state", task.State,
			"apply_id", task.ApplyID,
			"target_apply_id", apply.ID,
		)
		if task.State != state.Task.Stopped {
			continue
		}
		if task.ApplyID != apply.ID {
			continue
		}
		if time.Since(task.UpdatedAt) > maxAge {
			c.logger.Info("skipping old stopped task", "task_id", task.TaskIdentifier, "updated_at", task.UpdatedAt)
			continue
		}
		stoppedTasks = append(stoppedTasks, task)
	}

	if len(stoppedTasks) == 0 {
		return nil, fmt.Errorf("no stopped schema change to resume (found %d tasks for database, apply has ID %d)", len(tasks), apply.ID)
	}

	// Re-plan: diff current DB state against the plan's desired schema to find
	// which DDLs are still needed. Tables that already completed will not appear
	// in the re-plan result.
	plan, err := c.storage.Plans().GetByID(ctx, apply.PlanID)
	if err != nil || plan == nil {
		return nil, fmt.Errorf("plan not found for apply %s", apply.ApplyIdentifier)
	}

	rp, err := c.replanAndFilterTasks(ctx, apply, stoppedTasks, plan)
	if err != nil {
		return nil, err
	}

	resumeTasks := rp.ActiveTasks
	completedCount := rp.CompletedCount
	now := time.Now()

	if len(resumeTasks) == 0 {
		// All tasks were already done — mark apply completed
		apply.State = state.Apply.Completed
		apply.CompletedAt = &now
		apply.UpdatedAt = now
		if err := c.storage.Applies().Update(ctx, apply); err != nil {
			c.logger.Error("failed to update apply state", "apply_id", apply.ApplyIdentifier, "state", state.Apply.Completed, "error", err)
		}
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			"All tasks already completed on resume (re-plan shows no changes)", apply.State, state.Apply.Completed)

		return &ternv1.StartResponse{
			Accepted:     true,
			StartedCount: 0,
			SkippedCount: completedCount,
		}, nil
	}

	options := buildApplyOptions(apply)
	oldApplyState := apply.State

	if apply.GetOptions().DeferCutover {
		logMsg := fmt.Sprintf("Resume requested: %d tasks resumed, %d already completed", len(resumeTasks), completedCount)
		if err := c.launchAtomicResume(ctx, apply, resumeTasks, plan, options, logMsg); err != nil {
			return nil, err
		}
	} else {
		// Sequential mode: process each task one at a time in a background goroutine.
		// Mark tasks as PENDING synchronously so the progress API shows a non-stopped
		// state immediately — the watcher exits on STOPPED and would miss the resume.
		for _, task := range resumeTasks {
			c.transitionTaskState(ctx, task, 0, state.Task.Pending, "")
		}

		apply.State = state.Apply.Running
		apply.UpdatedAt = now
		if err := c.storage.Applies().Update(ctx, apply); err != nil {
			c.logger.Error("failed to update apply state", "apply_id", apply.ApplyIdentifier, "state", state.Apply.Running, "error", err)
		}

		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStartRequested, storage.LogSourceSchemaBot,
			fmt.Sprintf("Resume requested (sequential): %d tasks to resume, %d already completed", len(resumeTasks), completedCount), oldApplyState, state.Apply.Running)

		resumeCtx, cancelResume := context.WithCancel(context.Background())
		c.cancelApply = cancelResume
		go c.resumeApplySequential(resumeCtx, apply, resumeTasks, plan, options)
	}

	return &ternv1.StartResponse{
		Accepted:     true,
		StartedCount: int64(len(resumeTasks)),
		SkippedCount: completedCount,
	}, nil
}

// resumeApplySequential processes resumed tasks one at a time in sequence.
// This preserves the sequential behavior of the original apply when --defer-cutover
// was NOT used. Each task gets its own eng.Apply + pollTaskToCompletion cycle.
func (c *LocalClient) resumeApplySequential(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, plan *storage.Plan, options map[string]string) {
	defer c.startApplyHeartbeat(ctx, apply)()
	creds := c.credentials()
	eng := c.getEngine()

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

		c.logger.Info("resumeApplySequential: starting task",
			"iteration", i+1, "total_tasks", len(tasks),
			"task_id", task.TaskIdentifier, "table", task.TableName,
		)

		// Wait for any in-flight engine work to finish before checking schema.
		// Without this, the previous task's cutover might complete between our
		// schema check and the new eng.Apply() call, causing "Duplicate key name".
		if drainer, ok := eng.(engine.Drainer); ok {
			drainer.Drain()
		}

		// Verify this table still needs changes before applying. There's a race
		// between re-plan (which reads schema) and Spirit's cutover (which renames
		// the shadow table). If Spirit completed the cutover after the re-plan read
		// the schema, the table already has the desired changes.
		needsChange, err := c.tableStillNeedsChange(ctx, apply, plan, task.TableName)
		if err != nil {
			c.logger.Warn("could not verify table schema state, proceeding with apply",
				"task_id", task.TaskIdentifier, "table", task.TableName, "error", err)
		} else if !needsChange {
			c.logger.Info("table already has desired schema, skipping",
				"task_id", task.TaskIdentifier, "table", task.TableName)
			now := time.Now()
			task.ProgressPercent = 100
			task.CompletedAt = &now
			c.transitionTaskState(ctx, task, apply.ID, state.Task.Completed,
				fmt.Sprintf("Task %s already completed (cutover raced with re-plan)", task.TaskIdentifier))
			continue
		}

		action = c.runEngineTask(ctx, task, plan, options, creds)

		taskID := task.ID
		c.logApplyEvent(ctx, apply.ID, &taskID, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			fmt.Sprintf("Task %s resumed (sequential %d/%d)", task.TaskIdentifier, i+1, len(tasks)),
			state.Task.Stopped, state.Task.Running)

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
	c.finalizeSequentialApply(ctx, apply, tasks, failedTask, stoppedByUser)
	c.logger.Info("sequential resume finished", "apply_id", apply.ApplyIdentifier, "state", apply.State)
}

// tableStillNeedsChange does a quick re-plan to check if a specific table
// still needs schema changes. Returns false if the table already has the
// desired schema (e.g., Spirit's cutover completed during the stop sequence).
func (c *LocalClient) tableStillNeedsChange(ctx context.Context, apply *storage.Apply, plan *storage.Plan, tableName string) (bool, error) {
	creds := c.credentials()
	eng := c.getEngine()
	if eng == nil {
		return false, fmt.Errorf("no engine available")
	}

	result, err := eng.Plan(ctx, &engine.PlanRequest{
		Database:     apply.Database,
		DatabaseType: c.config.Type,
		SchemaFiles:  plan.SchemaFiles,
		Credentials:  creds,
	})
	if err != nil {
		return false, fmt.Errorf("re-plan check failed: %w", err)
	}

	for _, tc := range result.FlatTableChanges() {
		if tc.Table == tableName {
			return true, nil
		}
	}
	return false, nil
}

// replanResult holds the result of replanAndFilterTasks.
type replanResult struct {
	// ActiveTasks are tasks that still need changes (DDLs updated from re-plan).
	ActiveTasks []*storage.Task
	// CompletedCount is the number of tasks marked completed (no longer in diff).
	CompletedCount int64
}

// replanAndFilterTasks re-plans against the current DB state to determine which
// tasks still need changes. Tasks whose tables no longer appear in the diff are
// marked completed. Remaining tasks get their DDL updated from the re-plan result.
// Used by both Start() and ResumeApply() to handle tables that completed before
// stop or crash.
func (c *LocalClient) replanAndFilterTasks(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, plan *storage.Plan) (*replanResult, error) {
	creds := c.credentials()
	eng := c.getEngine()
	if eng == nil {
		return nil, fmt.Errorf("no engine available")
	}

	replanOut, err := eng.Plan(ctx, &engine.PlanRequest{
		Database:     apply.Database,
		DatabaseType: c.config.Type,
		SchemaFiles:  plan.SchemaFiles,
		Credentials:  creds,
	})
	if err != nil {
		return nil, fmt.Errorf("re-plan failed: %w", err)
	}

	// Build set of tables that still need changes
	needsChange := make(map[string]bool, len(replanOut.FlatTableChanges()))
	replanDDL := make(map[string]string, len(replanOut.FlatTableChanges()))
	for _, tc := range replanOut.FlatTableChanges() {
		needsChange[tc.Table] = true
		replanDDL[tc.Table] = tc.DDL
	}

	// Partition tasks: already-done vs still-needed
	now := time.Now()
	var activeTasks []*storage.Task
	var completedCount int64
	for _, task := range tasks {
		if task.State == state.Task.Completed {
			continue
		}
		if !needsChange[task.TableName] {
			// Table no longer in diff — it already completed
			task.ProgressPercent = 100
			task.CompletedAt = &now
			c.transitionTaskState(ctx, task, apply.ID, state.Task.Completed,
				fmt.Sprintf("Task %s already completed (re-plan shows no remaining changes)", task.TaskIdentifier))
			completedCount++
		} else {
			if ddl, ok := replanDDL[task.TableName]; ok {
				task.DDL = ddl
			}
			activeTasks = append(activeTasks, task)
		}
	}

	return &replanResult{ActiveTasks: activeTasks, CompletedCount: completedCount}, nil
}

// buildApplyOptions converts apply options to the string map used by the engine.
func buildApplyOptions(apply *storage.Apply) map[string]string {
	opts := apply.GetOptions()
	options := make(map[string]string)
	if opts.DeferCutover {
		options["defer_cutover"] = "true"
	}
	if opts.AllowUnsafe {
		options["allow_unsafe"] = "true"
	}
	return options
}

// launchAtomicResume sends all DDLs to the engine in one call, marks tasks and
// apply as RUNNING, logs the provided message, and starts pollForCompletionAtomic
// in a background goroutine. Used by both Start() and ResumeApply().
func (c *LocalClient) launchAtomicResume(ctx context.Context, apply *storage.Apply,
	tasks []*storage.Task, plan *storage.Plan, options map[string]string, logMessage string) error {

	var ddls []string
	for _, t := range tasks {
		ddls = append(ddls, t.DDL)
	}

	creds := c.credentials()
	eng := c.getEngine()

	// Build table changes from the DDL list
	var tableChanges []engine.TableChange
	for _, ddl := range ddls {
		tableChanges = append(tableChanges, engine.TableChange{DDL: ddl})
	}

	// Resume atomic apply after restart. Use apply identifier as MigrationContext
	// so Spirit can find existing checkpoint tables from the original run.
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
		return fmt.Errorf("engine apply failed: %w", err)
	}
	if !result.Accepted {
		return fmt.Errorf("engine did not accept apply: %s", result.Message)
	}

	now := time.Now()
	oldApplyState := apply.State

	for _, task := range tasks {
		c.transitionTaskState(ctx, task, 0, state.Task.Running, "")
	}

	apply.State = state.Apply.Running
	apply.UpdatedAt = now
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		c.logger.Error("failed to update apply state", "apply_id", apply.ApplyIdentifier, "state", state.Apply.Running, "error", err)
	}

	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
		logMessage, oldApplyState, state.Apply.Running)

	stopHeartbeat := c.startApplyHeartbeat(context.Background(), apply)
	go func() {
		defer stopHeartbeat()
		c.pollForCompletionAtomic(context.Background(), apply, tasks, creds, nil)
	}()
	return nil
}

// ResumeApply resumes an in-progress apply whose heartbeat has expired.
// This can happen after a server restart or if the worker goroutine crashed.
// Spirit's checkpoint table allows resuming from where the schema change left off.
func (c *LocalClient) ResumeApply(ctx context.Context, apply *storage.Apply) error {
	// Get tasks for this apply
	tasks, err := c.storage.Tasks().GetByApplyID(ctx, apply.ID)
	if err != nil {
		return fmt.Errorf("get tasks for apply %s: %w", apply.ApplyIdentifier, err)
	}

	if len(tasks) == 0 {
		c.logger.Warn("no tasks found for apply, marking as failed",
			"apply_id", apply.ApplyIdentifier)
		apply.State = state.Apply.Failed
		apply.ErrorMessage = "no tasks found during recovery"
		if err := c.storage.Applies().Update(ctx, apply); err != nil {
			c.logger.Error("failed to update apply state", "apply_id", apply.ApplyIdentifier, "state", state.Apply.Failed, "error", err)
		}
		return nil
	}

	// Get the plan to retrieve original DDLs
	plan, err := c.storage.Plans().GetByID(ctx, apply.PlanID)
	if err != nil || plan == nil {
		c.logger.Warn("plan not found for apply, marking as failed",
			"apply_id", apply.ApplyIdentifier,
			"plan_id", apply.PlanID)
		apply.State = state.Apply.Failed
		apply.ErrorMessage = "plan not found during recovery"
		if err := c.storage.Applies().Update(ctx, apply); err != nil {
			c.logger.Error("failed to update apply state", "apply_id", apply.ApplyIdentifier, "state", state.Apply.Failed, "error", err)
		}
		return nil
	}

	c.logger.Info("resuming apply (heartbeat expired)",
		"apply_id", apply.ApplyIdentifier,
		"database", apply.Database,
		"state", apply.State,
		"task_count", len(tasks),
	)

	// Log recovery event
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
		fmt.Sprintf("Recovering apply (heartbeat expired, was in %s state)", apply.State), "", "")

	rp, err := c.replanAndFilterTasks(ctx, apply, tasks, plan)
	if err != nil {
		c.logger.Error("re-plan failed during recovery", "apply_id", apply.ApplyIdentifier, "error", err)
		return fmt.Errorf("re-plan failed during recovery: %w", err)
	}

	activeTasks := rp.ActiveTasks

	if len(activeTasks) == 0 {
		c.logger.Info("all tasks already completed, marking apply as completed",
			"apply_id", apply.ApplyIdentifier)
		now := time.Now()
		apply.State = state.Apply.Completed
		apply.CompletedAt = &now
		apply.UpdatedAt = now
		if err := c.storage.Applies().Update(ctx, apply); err != nil {
			c.logger.Error("failed to update apply state", "apply_id", apply.ApplyIdentifier, "state", state.Apply.Completed, "error", err)
		}
		return nil
	}

	options := buildApplyOptions(apply)

	if apply.GetOptions().DeferCutover {
		if err := c.launchAtomicResume(ctx, apply, activeTasks, plan, options, "Apply resumed from checkpoint (atomic)"); err != nil {
			c.logger.Error("engine apply failed during recovery",
				"apply_id", apply.ApplyIdentifier,
				"error", err)
			c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelError, storage.LogEventError, storage.LogSourceSchemaBot,
				fmt.Sprintf("Recovery failed: %v", err), apply.State, state.Apply.Failed)
			return err
		}
	} else {
		// Sequential mode: process each task one at a time
		now := time.Now()
		apply.State = state.Apply.Running
		apply.UpdatedAt = now
		if err := c.storage.Applies().Update(ctx, apply); err != nil {
			c.logger.Error("failed to update apply state", "apply_id", apply.ApplyIdentifier, "state", state.Apply.Running, "error", err)
		}

		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			"Apply resumed from checkpoint (sequential)", "", state.Apply.Running)

		go c.resumeApplySequential(context.Background(), apply, activeTasks, plan, options)
	}

	return nil
}
