package tern

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/metrics"
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

	// Deferred deploy that isn't ready yet — reject with a clear message.
	if apply.GetOptions().DeferDeploy && apply.State != state.Apply.WaitingForDeploy {
		return nil, fmt.Errorf("schema change is not ready for deploy (current state: %s)", apply.State)
	}

	// Deferred deploy: call engine Start to trigger the deploy request.
	// This is a separate path from the stopped-task resume flow below.
	if apply.State == state.Apply.WaitingForDeploy {
		if controlReq, err := pendingStopControlRequest(ctx, c.storage, apply); err != nil {
			return nil, fmt.Errorf("check pending stop before deferred deploy start for apply %s: %w", apply.ApplyIdentifier, err)
		} else if controlReq != nil {
			c.logger.Info("deferred deploy start blocked because stop request is pending",
				"apply_id", apply.ApplyIdentifier,
				"requested_by", controlRequestCaller(controlReq))
			return nil, fmt.Errorf("schema change has a pending stop request; start is blocked until stop is processed")
		}
		started, err := c.startDeferredDeploy(ctx, apply, "")
		if err != nil {
			return nil, err
		}
		return started.response, nil
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
		if err := c.launchAtomicResume(ctx, apply, resumeTasks, plan, options, logMsg, false, false); err != nil {
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

		resumeCtx, cancelResume := context.WithCancel(context.WithoutCancel(ctx))
		cancelGeneration := c.setApplyCancel(cancelResume)
		go func() {
			defer c.clearApplyCancel(cancelGeneration)
			defer cancelResume()
			c.resumeApplySequential(resumeCtx, apply, resumeTasks, plan, options)
		}()
	}

	return &ternv1.StartResponse{
		Accepted:     true,
		StartedCount: int64(len(resumeTasks)),
		SkippedCount: completedCount,
	}, nil
}

type deferredDeployStart struct {
	response    *ternv1.StartResponse
	tasks       []*storage.Task
	credentials *engine.Credentials
	resumeState *engine.ResumeState
}

func (c *LocalClient) startDeferredDeploy(ctx context.Context, apply *storage.Apply, caller string) (*deferredDeployStart, error) {
	applyTasks, taskErr := c.storage.Tasks().GetByApplyID(ctx, apply.ID)
	if taskErr != nil {
		return nil, fmt.Errorf("get tasks for deferred deploy apply %s: %w", apply.ApplyIdentifier, taskErr)
	}
	if len(applyTasks) == 0 {
		return nil, fmt.Errorf("no tasks found for apply %s", apply.ApplyIdentifier)
	}
	eng := c.getEngine()
	if eng == nil {
		return nil, fmt.Errorf("no engine configured for type: %s", c.config.Type)
	}
	creds, err := c.credentialsForTask(applyTasks[0])
	if err != nil {
		return nil, fmt.Errorf("resolve credentials for deferred deploy task %s: %w", applyTasks[0].TaskIdentifier, err)
	}
	controlReq, err := c.buildControlRequest(ctx, applyTasks[0], creds, eng, engine.ControlStart)
	if err != nil {
		return nil, fmt.Errorf("build deferred deploy request for task %s: %w", applyTasks[0].TaskIdentifier, err)
	}
	result, err := eng.Start(ctx, controlReq)
	if err != nil {
		return nil, fmt.Errorf("start deferred deploy: %w", err)
	}
	if !result.Accepted {
		return nil, fmt.Errorf("deferred deploy not accepted: %s", result.Message)
	}
	logMessage := "Deferred deploy start requested"
	if caller != "" {
		logMessage += callerApplyLogSuffix(caller)
	}
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStartRequested, storage.LogSourceSchemaBot,
		logMessage, state.Apply.WaitingForDeploy, state.Apply.Running)
	return &deferredDeployStart{
		response: &ternv1.StartResponse{
			Accepted:     true,
			StartedCount: 1,
		},
		tasks:       applyTasks,
		credentials: creds,
		resumeState: controlReq.ResumeState,
	}, nil
}

func (c *LocalClient) processPendingStartControlRequest(ctx context.Context, apply *storage.Apply) (bool, error) {
	controlReq, err := pendingStartControlRequest(ctx, c.storage, apply)
	if err != nil {
		return false, err
	}
	if controlReq == nil {
		return false, nil
	}
	if stopReq, err := pendingStopControlRequest(ctx, c.storage, apply); err != nil {
		return true, fmt.Errorf("check pending stop before pending start for apply %s: %w", apply.ApplyIdentifier, err)
	} else if stopReq != nil {
		c.logger.Info("pending start request is waiting for pending stop request to finish",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"requested_by", controlRequestCaller(controlReq),
			"stop_requested_by", controlRequestCaller(stopReq),
			"state", apply.State)
		return false, nil
	}
	if !state.IsState(apply.State, state.Apply.WaitingForDeploy) {
		return false, nil
	}
	started, err := c.startDeferredDeploy(ctx, apply, controlRequestCaller(controlReq))
	if err != nil {
		if failErr := failPendingStartControlRequests(ctx, c.storage, apply, err.Error()); failErr != nil {
			return true, fmt.Errorf("process pending start for apply %s: %w; fail pending start request: %w", apply.ApplyIdentifier, err, failErr)
		}
		return true, fmt.Errorf("process pending start for apply %s: %w", apply.ApplyIdentifier, err)
	}
	resp := started.response
	if resp == nil || !resp.Accepted {
		errorMessage := "not accepted"
		if resp != nil && resp.ErrorMessage != "" {
			errorMessage = resp.ErrorMessage
		}
		if err := failPendingStartControlRequests(ctx, c.storage, apply, errorMessage); err != nil {
			return true, err
		}
		return true, fmt.Errorf("process pending start for apply %s: %s", apply.ApplyIdentifier, errorMessage)
	}
	now := time.Now()
	apply.State = state.Apply.Running
	if apply.StartedAt == nil {
		apply.StartedAt = &now
	}
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		return true, fmt.Errorf("update started deferred deploy apply %s: %w", apply.ApplyIdentifier, err)
	}
	if err := completePendingStartControlRequests(ctx, c.storage, apply); err != nil {
		return true, err
	}
	c.logger.Info("pending start request accepted and completed",
		"apply_id", apply.ApplyIdentifier,
		"database", apply.Database,
		"environment", apply.Environment,
		"requested_by", controlRequestCaller(controlReq),
		"state", apply.State)
	c.pollForCompletionAtomic(ctx, apply, started.tasks, started.credentials, started.resumeState)
	return true, ctx.Err()
}

// resumeApplySequential processes resumed tasks one at a time in sequence.
// This preserves the sequential behavior of the original apply when --defer-cutover
// was NOT used. Each task gets its own eng.Apply + pollTaskToCompletion cycle.
func (c *LocalClient) resumeApplySequential(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, plan *storage.Plan, options map[string]string) {
	ctx, cancelApply := context.WithCancel(ctx)
	defer cancelApply()
	defer c.startApplyHeartbeat(ctx, apply, cancelApply)()
	creds := c.credentials()
	eng := c.getEngine()

	var failedTask *storage.Task
	var stoppedByUser bool

	for i, task := range tasks {
		if handled, err := c.processPendingStopControlRequest(ctx, apply); err != nil {
			c.logger.Warn("pending stop request processing failed; current apply owner will exit for operator retry",
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

		action = c.runEngineTask(ctx, apply, task, plan, options, creds)

		taskID := task.ID
		c.logApplyEvent(ctx, apply.ID, &taskID, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			fmt.Sprintf("Task %s resumed (sequential %d/%d)", task.TaskIdentifier, i+1, len(tasks)),
			state.Task.Stopped, state.Task.Running)

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
	c.finalizeSequentialApply(ctx, apply, tasks, failedTask, stoppedByUser)
	c.logger.Info("sequential resume finished", "apply_id", apply.ApplyIdentifier, "state", apply.State)
}

// tableStillNeedsChange does a quick re-plan to check if a specific table
// still needs schema changes. Returns false if the table already has the
// desired schema (e.g., Spirit's cutover completed during the stop sequence).
func (c *LocalClient) tableStillNeedsChange(ctx context.Context, apply *storage.Apply, plan *storage.Plan, tableName string) (bool, error) {
	result, err := c.planWithEngine(ctx, &ternv1.PlanRequest{}, apply.Database, plan.SchemaFiles)
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
	replanOut, err := c.planWithEngine(ctx, &ternv1.PlanRequest{}, apply.Database, plan.SchemaFiles)
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
	return apply.GetOptions().Map()
}

// prepareRetryableTasksForResume queues only the task work that previously
// stopped on a retryable engine failure. Completed tasks remain completed, and
// pending tasks remain queued behind the retried work.
func (c *LocalClient) prepareRetryableTasksForResume(ctx context.Context, apply *storage.Apply, tasks []*storage.Task) {
	if !state.IsState(apply.State, state.Apply.FailedRetryable) {
		return
	}
	apply.ErrorMessage = ""
	for _, task := range tasks {
		if !state.IsState(task.State, state.Task.FailedRetryable) {
			continue
		}
		task.Attempt++
		task.ErrorMessage = ""
		task.CompletedAt = nil
		c.transitionTaskState(ctx, task, apply.ID, state.Task.Pending,
			fmt.Sprintf("Task %s queued for retry", task.TaskIdentifier))
	}
}

// prepareStoppedTasksForResume turns an operator-claimed start request back into
// runnable task work. The start intent stays pending until stopped task rows are
// requeued and the apply is ready for execution, so a worker crash can still be
// recovered by another operator worker.
func (c *LocalClient) prepareStoppedTasksForResume(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, startRequested bool) {
	if !startRequested {
		return
	}
	for _, task := range tasks {
		if !state.IsState(task.State, state.Task.Stopped) {
			continue
		}
		task.CompletedAt = nil
		c.transitionTaskState(ctx, task, apply.ID, state.Task.Pending,
			fmt.Sprintf("Task %s queued for start", task.TaskIdentifier))
	}
}

func shouldInspectDeferredCutoverSignal(apply *storage.Apply) bool {
	return apply != nil &&
		apply.GetOptions().DeferCutover &&
		state.IsState(apply.State, state.Apply.WaitingForCutover, state.Apply.Recovering)
}

func (c *LocalClient) markApplyRecovering(ctx context.Context, apply *storage.Apply, tasks []*storage.Task) error {
	c.logger.Info("entering recovery state for deferred cutover checkpoint",
		"apply_id", apply.ApplyIdentifier,
		"database", apply.Database,
		"database_type", apply.DatabaseType,
		"task_count", len(tasks))
	oldApplyState := apply.State
	for _, task := range tasks {
		if state.IsTerminalTaskState(task.State) {
			c.logger.Debug("leaving terminal task unchanged during recovery",
				"apply_id", apply.ApplyIdentifier,
				"task_id", task.TaskIdentifier,
				"task_state", task.State)
			continue
		}
		c.transitionTaskState(ctx, task, apply.ID, state.Task.Recovering,
			fmt.Sprintf("Task %s is recovering after restart", task.TaskIdentifier))
	}
	apply.State = state.Apply.Recovering
	apply.CompletedAt = nil
	apply.UpdatedAt = time.Now()
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		return fmt.Errorf("mark apply %s recovering after restart: %w", apply.ApplyIdentifier, err)
	}
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
		"Recovering after restart before accepting cutover requests", oldApplyState, state.Apply.Recovering)
	return nil
}

// launchAtomicResume sends all DDLs to the engine in one call, marks tasks and
// apply as RUNNING, logs the provided message, and then polls for completion.
// Operator-owned calls block so the worker owns the apply until terminal or
// retry-waiting state; user start calls poll in the background and returns
// after the engine accepts the resume.
func (c *LocalClient) launchAtomicResume(ctx context.Context, apply *storage.Apply,
	tasks []*storage.Task, plan *storage.Plan, options map[string]string, logMessage string, block bool, startRequested bool) error {

	allTasks := tasks
	eng := c.getEngine()
	if eng == nil {
		return fmt.Errorf("no engine available for grouped resume apply %s", apply.ApplyIdentifier)
	}
	creds, err := c.credentialsForGroupedApply(plan)
	if err != nil {
		return fmt.Errorf("resolve credentials for grouped resume apply %s: %w", apply.ApplyIdentifier, err)
	}

	if drainer, ok := eng.(engine.Drainer); ok {
		drainer.Drain()
	}

	rp, err := c.replanAndFilterTasks(ctx, apply, tasks, plan)
	if err != nil {
		return fmt.Errorf("final schema check before grouped resume for apply %s: %w", apply.ApplyIdentifier, err)
	}
	tasks = rp.ActiveTasks
	if len(tasks) == 0 {
		c.logger.Info("final schema check found no remaining grouped resume work; completing apply",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"database_type", apply.DatabaseType,
			"task_count", len(allTasks))
		oldApplyState := apply.State
		now := time.Now()
		apply.State = state.Apply.Completed
		apply.CompletedAt = &now
		apply.UpdatedAt = now
		if err := c.storage.Applies().Update(ctx, apply); err != nil {
			c.logger.Error("failed to update apply state", "apply_id", apply.ApplyIdentifier, "state", state.Apply.Completed, "error", err)
			return fmt.Errorf("mark grouped resume apply %s completed after final schema check: %w", apply.ApplyIdentifier, err)
		}
		if startRequested {
			if err := completePendingStartControlRequests(ctx, c.storage, apply); err != nil {
				return err
			}
		}
		if !state.IsTerminalApplyState(oldApplyState) {
			metrics.AdjustActiveApplies(ctx, -1, apply.Database, apply.Deployment, apply.Environment)
		}
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			"All tasks already completed on resume (final schema check shows no remaining changes)", oldApplyState, state.Apply.Completed)
		c.notifyTerminalObserver(apply, allTasks)
		return nil
	}

	resumeState, err := c.groupedResumeState(ctx, apply, tasks)
	if err != nil {
		return err
	}

	// Resume the grouped apply with the engine's persisted state so it
	// reattaches to in-flight engine work instead of launching a duplicate
	// schema change. The changes are rebuilt from the stored tasks so the
	// engine keys per-table progress on the same namespace/table pairs the
	// tasks carry.
	result, err := eng.Apply(ctx, &engine.ApplyRequest{
		Database:    apply.Database,
		PlanID:      plan.PlanIdentifier,
		Changes:     groupedResumeChanges(tasks),
		SchemaFiles: plan.SchemaFiles,
		Options:     options,
		ResumeState: resumeState,
		Credentials: creds,
		OnStateChange: func(rs *engine.ResumeState) {
			if rs == nil {
				c.logger.Debug("OnStateChange: nil resume state", "apply_id", apply.ApplyIdentifier)
				return
			}
			if saveErr := c.saveEngineResumeState(ctx, tasks, rs); saveErr != nil {
				c.logger.Warn("OnStateChange: failed to persist opaque resume state", "apply_id", apply.ApplyIdentifier, "error", saveErr)
			}
		},
	})
	if err != nil {
		return fmt.Errorf("engine apply failed: %w", err)
	}
	if !result.Accepted {
		return fmt.Errorf("engine did not accept apply: %s", result.Message)
	}
	if result.ResumeState != nil {
		resumeState = result.ResumeState
		if c.config.Type == storage.DatabaseTypeVitess {
			// The engine has already accepted the resume and a deploy request is
			// running on the provider. A failure to persist the resume state must
			// not become terminal apply state — the owner exits and the operator
			// retries against the in-flight work rather than abandoning it.
			if saveErr := c.saveEngineResumeState(ctx, tasks, resumeState); saveErr != nil {
				return fmt.Errorf("%w: save engine resume state after grouped resume of apply %s (database %s): %w", errGroupedResumeStateUnavailable, apply.ApplyIdentifier, apply.Database, saveErr)
			}
		}
	}
	// Progress polling for Vitess applies is driven entirely by resume state
	// metadata; without it the poll loop can never observe the deploy request.
	// The engine has already accepted the resume, so an absent metadata invariant
	// leaves the owner unable to track in-flight work — exit non-terminally so the
	// operator can retry rather than failing an apply that is still running.
	if c.config.Type == storage.DatabaseTypeVitess && resumeState.Metadata == "" {
		return fmt.Errorf("%w: engine accepted grouped resume of Vitess apply %s (database %s) without resume state metadata", errGroupedResumeStateUnavailable, apply.ApplyIdentifier, apply.Database)
	}

	now := time.Now()
	oldApplyState := apply.State
	recovering := state.IsState(oldApplyState, state.Apply.Recovering)

	for _, task := range tasks {
		taskState := state.Task.Running
		if recovering {
			taskState = state.Task.Recovering
		}
		c.transitionTaskState(ctx, task, 0, taskState, "")
	}

	apply.State = state.Apply.Running
	if recovering {
		apply.State = state.Apply.Recovering
	}
	apply.UpdatedAt = now
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		c.logger.Error("failed to update apply state", "apply_id", apply.ApplyIdentifier, "state", apply.State, "error", err)
		return fmt.Errorf("mark grouped resume apply %s %s: %w", apply.ApplyIdentifier, apply.State, err)
	}
	if startRequested {
		if err := completePendingStartControlRequests(ctx, c.storage, apply); err != nil {
			return err
		}
	}

	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
		logMessage, oldApplyState, apply.State)

	if block {
		pollCtx, cancelPoll := context.WithCancel(ctx)
		defer cancelPoll()
		stopHeartbeat := c.startApplyHeartbeat(pollCtx, apply, cancelPoll)
		defer stopHeartbeat()
		c.pollForCompletionAtomic(pollCtx, apply, tasks, creds, resumeState)
		return nil
	}

	resumeCtx, cancelResume := context.WithCancel(context.WithoutCancel(ctx))
	stopHeartbeat := c.startApplyHeartbeat(resumeCtx, apply, cancelResume)
	go func() {
		defer cancelResume()
		defer stopHeartbeat()
		c.pollForCompletionAtomic(resumeCtx, apply, tasks, creds, resumeState)
	}()
	return nil
}

// errGroupedResumeStateUnavailable marks a resume attempt whose persisted engine
// resume state could not be loaded (or ruled out) before the engine apply, or
// could not be persisted (or confirmed present) after the engine accepted the
// reattach. The current apply owner must exit without writing terminal state so
// a later attempt can retry against intact storage — failing the apply here
// would abandon engine work that is still in flight on the provider.
var errGroupedResumeStateUnavailable = errors.New("grouped resume state unavailable")

// groupedResumeState returns the ResumeState handed to the engine when a
// grouped apply is resumed. Recovery must reattach to the engine's existing
// work via persisted resume state; launching fresh engine work would duplicate
// the in-flight schema change. Absent persisted state is expected for engines
// that reattach through durable database-side checkpoints keyed by the schema
// change context alone (Spirit); a storage read failure fails the resume
// attempt so a later attempt can retry with intact state.
func (c *LocalClient) groupedResumeState(ctx context.Context, apply *storage.Apply, tasks []*storage.Task) (*engine.ResumeState, error) {
	contextOnly := &engine.ResumeState{MigrationContext: apply.ApplyIdentifier}

	operationID, err := applyOperationIDForTasks(tasks)
	if err != nil {
		// Persisted engine resume state is scoped to an apply operation, so a
		// Vitess apply whose tasks cannot be resolved to one leaves SchemaBot
		// unable to prove there is no in-flight deploy request to reattach to.
		if c.config.Type == storage.DatabaseTypeVitess {
			return nil, fmt.Errorf("%w: resolve apply operation for grouped resume of apply %s (database %s): %w", errGroupedResumeStateUnavailable, apply.ApplyIdentifier, apply.Database, err)
		}
		c.logger.Info("tasks have no apply operation to hold persisted engine resume state; engine apply will start from the schema change context",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"database_type", apply.DatabaseType)
		return contextOnly, nil
	}

	stored, err := c.loadEngineResumeStateForOperation(ctx, operationID)
	if errors.Is(err, storage.ErrEngineResumeStateNotFound) {
		c.logger.Info("no persisted engine resume state for apply operation; engine apply will start from the schema change context",
			"apply_id", apply.ApplyIdentifier,
			"apply_operation_id", operationID,
			"database", apply.Database,
			"database_type", apply.DatabaseType)
		return contextOnly, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: load engine resume state for grouped resume of apply %s operation %d (database %s): %w", errGroupedResumeStateUnavailable, apply.ApplyIdentifier, operationID, apply.Database, err)
	}
	if stored.MigrationContext == "" {
		stored.MigrationContext = apply.ApplyIdentifier
	}
	return stored, nil
}

// groupedResumeChanges rebuilds the engine changes for a grouped resume from
// the stored tasks, grouped per namespace. Tasks carry the authoritative
// remaining work after re-planning, and engines key per-table progress on
// namespace and table, so the rebuilt changes must preserve both. VSchema tasks
// carry no DDL — they are excluded from TableChanges and instead flag their
// namespace with the vschema_changed metadata so the engine applies vschema.json
// from the schema files, mirroring how the fresh apply builds changes.
func groupedResumeChanges(tasks []*storage.Task) []engine.SchemaChange {
	indexByNamespace := make(map[string]int, len(tasks))
	var changes []engine.SchemaChange
	ensureNamespace := func(namespace string) int {
		idx, ok := indexByNamespace[namespace]
		if !ok {
			idx = len(changes)
			indexByNamespace[namespace] = idx
			changes = append(changes, engine.SchemaChange{Namespace: namespace})
		}
		return idx
	}
	for _, task := range tasks {
		idx := ensureNamespace(task.Namespace)
		if isVSchemaTask(task) {
			if changes[idx].Metadata == nil {
				changes[idx].Metadata = make(map[string]string)
			}
			changes[idx].Metadata["vschema_changed"] = "true"
			continue
		}
		changes[idx].TableChanges = append(changes[idx].TableChanges, engine.TableChange{
			Table:     task.TableName,
			DDL:       task.DDL,
			Operation: ddl.OpToStatementType(task.DDLAction),
		})
	}
	return changes
}

// isVSchemaTask reports whether a task represents a VSchema update rather than a
// table DDL change. VSchema tasks have no DDL; their work is applying the
// namespace's vschema.json.
func isVSchemaTask(task *storage.Task) bool {
	return task.DDLAction == "vschema_update"
}

func (c *LocalClient) notifyTerminalObserver(apply *storage.Apply, tasks []*storage.Task) {
	if obs := c.getObserver(apply.ID); obs != nil {
		obs.OnTerminal(apply, tasks)
		c.clearObserver(apply.ID)
	}
}

// ResumeApply starts or resumes an apply claimed by an operator worker.
// Pending applies are dispatched for the first time; stale applies use the
// engine's resume metadata to continue after a missed heartbeat.
func (c *LocalClient) ResumeApply(ctx context.Context, apply *storage.Apply) error {
	tasks, err := c.storage.Tasks().GetByApplyID(ctx, apply.ID)
	if err != nil {
		return fmt.Errorf("get tasks for apply %s: %w", apply.ApplyIdentifier, err)
	}
	return c.resumeApplyWithTasks(ctx, apply, tasks)
}

// ResumeApplyOperation starts or resumes a single apply_operation (one
// deployment of a multi-deployment apply), driving only that operation's tasks.
// The drive logic is identical to ResumeApply; the only difference is that tasks
// are loaded scoped to the operation rather than the whole apply, so a worker
// can advance one deployment independently of its siblings.
func (c *LocalClient) ResumeApplyOperation(ctx context.Context, apply *storage.Apply, applyOperationID int64) error {
	tasks, err := c.storage.Tasks().GetByApplyOperationID(ctx, applyOperationID)
	if err != nil {
		return fmt.Errorf("get tasks for apply_operation %d (apply %s): %w", applyOperationID, apply.ApplyIdentifier, err)
	}
	// An empty result is the fail-closed signal for an invalid or mismatched
	// applyOperationID. Unlike ResumeApply, we must not forward this to
	// resumeApplyWithTasks: that path marks the whole parent apply as failed,
	// which is incorrect when only one operation lookup came back empty.
	if len(tasks) == 0 {
		return fmt.Errorf("apply_operation %d (apply %s): %w", applyOperationID, apply.ApplyIdentifier, ErrNoTasksForApplyOperation)
	}
	return c.resumeApplyWithTasks(ctx, apply, tasks)
}

// resumeApplyWithTasks drives an apply (or one of its operations) from the set
// of tasks the caller has loaded. Callers choose whether tasks are scoped to the
// whole apply or to a single operation.
func (c *LocalClient) resumeApplyWithTasks(ctx context.Context, apply *storage.Apply, tasks []*storage.Task) error {
	if len(tasks) == 0 {
		c.logger.Warn("no tasks found for apply, marking as failed",
			"apply_id", apply.ApplyIdentifier)
		apply.State = state.Apply.Failed
		apply.ErrorMessage = "no tasks found during recovery"
		if err := c.storage.Applies().Update(ctx, apply); err != nil {
			c.logger.Error("failed to update apply state", "apply_id", apply.ApplyIdentifier, "state", state.Apply.Failed, "error", err)
		}
		c.notifyTerminalObserver(apply, tasks)
		return nil
	}

	if handled, err := c.processPendingStopControlRequest(ctx, apply); handled || err != nil {
		return err
	}
	if handled, err := c.processPendingStartControlRequest(ctx, apply); handled || err != nil {
		return err
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
		c.notifyTerminalObserver(apply, tasks)
		return nil
	}

	if state.IsState(apply.State, state.Apply.Pending) && apply.StartedAt == nil {
		c.dispatchQueuedApply(ctx, apply, tasks, plan)
		return ctx.Err()
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

	deferredCutoverSignalAbsent := false
	if shouldInspectDeferredCutoverSignal(apply) {
		signalExists, signalSupported, err := c.deferredCutoverSignalExists(ctx, apply)
		if err != nil {
			c.logger.Warn("deferred cutover recovery could not verify engine cutover signal; operator will retry",
				"apply_id", apply.ApplyIdentifier,
				"database", apply.Database,
				"database_type", apply.DatabaseType,
				"error", err)
			return fmt.Errorf("verify engine cutover signal before recovering deferred cutover apply %s: %w", apply.ApplyIdentifier, err)
		}
		if signalSupported {
			if signalExists {
				if err := c.markApplyRecovering(ctx, apply, tasks); err != nil {
					return err
				}
				options := buildApplyOptions(apply)
				resumeCtx, cancelResume := context.WithCancel(ctx)
				cancelGeneration := c.setApplyCancel(cancelResume)
				defer c.clearApplyCancel(cancelGeneration)
				defer cancelResume()
				if err := c.launchAtomicResume(resumeCtx, apply, tasks, plan, options, "Recovering from checkpoint", true, false); err != nil {
					if errors.Is(err, errGroupedResumeStateUnavailable) {
						c.logger.Warn("deferred cutover recovery could not load persisted engine resume state; current apply owner will exit for operator retry",
							"apply_id", apply.ApplyIdentifier,
							"database", apply.Database,
							"database_type", apply.DatabaseType,
							"error", err)
						return fmt.Errorf("recover deferred cutover apply %s from checkpoint: %w", apply.ApplyIdentifier, err)
					}
					return c.handleGroupedResumeFailure(ctx, apply, tasks, fmt.Errorf("recover deferred cutover apply %s from checkpoint: %w", apply.ApplyIdentifier, err), false)
				}
				return ctx.Err()
			}
			c.logger.Info("engine cutover signal is absent during deferred cutover recovery; re-plan will reconcile completed work",
				"apply_id", apply.ApplyIdentifier,
				"database", apply.Database,
				"database_type", apply.DatabaseType)
			deferredCutoverSignalAbsent = true
		} else {
			c.logger.Info("engine does not support deferred cutover signal lookup; re-plan will reconcile deferred cutover recovery",
				"apply_id", apply.ApplyIdentifier,
				"database", apply.Database,
				"database_type", apply.DatabaseType,
				"engine", c.getEngine().Name())
		}
	}

	rp, err := c.replanAndFilterTasks(ctx, apply, tasks, plan)
	if err != nil {
		c.logger.Error("re-plan failed during recovery", "apply_id", apply.ApplyIdentifier, "error", err)
		return fmt.Errorf("re-plan failed during recovery: %w", err)
	}

	activeTasks := rp.ActiveTasks
	if deferredCutoverSignalAbsent && len(activeTasks) > 0 {
		message := "deferred cutover signal is absent but live schema does not match desired schema; manual reconciliation required"
		c.logger.Error("deferred cutover recovery cannot reconcile absent cutover signal",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"database_type", apply.DatabaseType,
			"active_task_count", len(activeTasks))
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelError, storage.LogEventError, storage.LogSourceSchemaBot,
			message, apply.State, state.Apply.Failed)
		c.failApplyWithTasks(ctx, apply, activeTasks, message)
		c.notifyTerminalObserver(apply, tasks)
		return nil
	}
	startControlReq, err := pendingStartControlRequest(ctx, c.storage, apply)
	if err != nil {
		return err
	}
	startRequested := startControlReq != nil

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
		if startRequested {
			if err := completePendingStartControlRequests(ctx, c.storage, apply); err != nil {
				return err
			}
		}
		c.notifyTerminalObserver(apply, tasks)
		return nil
	}

	c.prepareRetryableTasksForResume(ctx, apply, activeTasks)
	c.prepareStoppedTasksForResume(ctx, apply, activeTasks, startRequested)

	options := buildApplyOptions(apply)

	if c.usesGroupedApply(apply) {
		resumeCtx, cancelResume := context.WithCancel(ctx)
		cancelGeneration := c.setApplyCancel(cancelResume)
		defer c.clearApplyCancel(cancelGeneration)
		defer cancelResume()
		if err := c.launchAtomicResume(resumeCtx, apply, activeTasks, plan, options, fmt.Sprintf("Apply resumed from checkpoint (%s)", groupedApplyModeDescription(apply)), true, startRequested); err != nil {
			if errors.Is(err, errGroupedResumeStateUnavailable) {
				c.logger.Warn("grouped resume could not load persisted engine resume state; current apply owner will exit for operator retry",
					"apply_id", apply.ApplyIdentifier,
					"database", apply.Database,
					"database_type", apply.DatabaseType,
					"error", err)
				return err
			}
			return c.handleGroupedResumeFailure(ctx, apply, activeTasks, err, startRequested)
		}
	} else {
		// Sequential mode: process each task one at a time
		now := time.Now()
		apply.State = state.Apply.Running
		apply.UpdatedAt = now
		if err := c.storage.Applies().Update(ctx, apply); err != nil {
			c.logger.Error("failed to update apply state", "apply_id", apply.ApplyIdentifier, "state", state.Apply.Running, "error", err)
			return fmt.Errorf("mark sequential resume apply %s running: %w", apply.ApplyIdentifier, err)
		}
		if startRequested {
			if err := completePendingStartControlRequests(ctx, c.storage, apply); err != nil {
				return err
			}
		}

		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			"Apply resumed from checkpoint (sequential)", "", state.Apply.Running)

		resumeCtx, cancelResume := context.WithCancel(ctx)
		cancelGeneration := c.setApplyCancel(cancelResume)
		defer c.clearApplyCancel(cancelGeneration)
		defer cancelResume()
		c.resumeApplySequential(resumeCtx, apply, activeTasks, plan, options)
	}

	return ctx.Err()
}

func (c *LocalClient) handleGroupedResumeFailure(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, err error, startRequested bool) error {
	if c.shouldRetryEngineError(err) {
		c.logger.Warn("engine apply failed during recovery, pausing apply for operator retry",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"database_type", apply.DatabaseType,
			"error", err)
		c.markApplyRetryableWithTasks(ctx, apply, tasks, err.Error())
		return nil
	}

	c.logger.Error("engine apply failed during recovery",
		"apply_id", apply.ApplyIdentifier,
		"database", apply.Database,
		"database_type", apply.DatabaseType,
		"error", err)
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelError, storage.LogEventError, storage.LogSourceSchemaBot,
		fmt.Sprintf("Recovery failed: %v", err), apply.State, state.Apply.Failed)
	c.failApplyWithTasks(ctx, apply, tasks, err.Error())
	if startRequested {
		if failErr := failPendingStartControlRequests(ctx, c.storage, apply, err.Error()); failErr != nil {
			return failErr
		}
	}
	c.notifyTerminalObserver(apply, tasks)
	return err
}

func (c *LocalClient) dispatchQueuedApply(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, plan *storage.Plan) {
	options := buildApplyOptions(apply)
	applyCtx, cancelApply := context.WithCancel(ctx)
	cancelGeneration := c.setApplyCancel(cancelApply)
	defer c.clearApplyCancel(cancelGeneration)
	defer cancelApply()

	c.logger.Info("dispatching queued apply",
		"apply_id", apply.ApplyIdentifier,
		"database", apply.Database,
		"state", apply.State,
		"task_count", len(tasks),
	)

	c.runApplyExecution(applyCtx, apply, tasks, plan, options)
}
