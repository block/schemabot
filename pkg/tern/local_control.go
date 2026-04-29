package tern

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// Cutover triggers the cutover phase when defer_cutover was used.
func (c *LocalClient) Cutover(ctx context.Context, req *ternv1.CutoverRequest) (*ternv1.CutoverResponse, error) {
	var task *storage.Task
	var err error

	if req.ApplyId != "" {
		apply, lookupErr := c.storage.Applies().GetByApplyIdentifier(ctx, req.ApplyId)
		if lookupErr != nil || apply == nil {
			return nil, fmt.Errorf("apply %s not found", req.ApplyId)
		}
		tasks, lookupErr := c.storage.Tasks().GetByApplyID(ctx, apply.ID)
		if lookupErr != nil {
			return nil, fmt.Errorf("get tasks failed: %w", lookupErr)
		}
		for _, t := range tasks {
			if !state.IsTerminalTaskState(t.State) {
				task = t
				break
			}
		}
	} else {
		task, err = c.getActiveTaskForDatabase(ctx, c.config.Database)
		if err != nil {
			return nil, err
		}
	}

	if task == nil {
		return nil, fmt.Errorf("no active schema change")
	}

	creds := c.credentials()
	eng := c.getEngine()
	if eng == nil {
		return nil, fmt.Errorf("no engine configured for type: %s", c.config.Type)
	}

	// Log cutover triggered
	c.logApplyEvent(ctx, task.ApplyID, nil, storage.LogLevelInfo, storage.LogEventCutoverTriggered, storage.LogSourceSchemaBot,
		"Cutover triggered", "", "")

	_, err = eng.Cutover(ctx, c.buildControlRequest(ctx, task, creds))
	if err != nil {
		c.logApplyEvent(ctx, task.ApplyID, nil, storage.LogLevelError, storage.LogEventError, storage.LogSourceSchemaBot,
			fmt.Sprintf("Cutover failed: %v", err), "", "")
		return nil, fmt.Errorf("cutover failed: %w", err)
	}

	return &ternv1.CutoverResponse{Accepted: true}, nil
}

// Stop pauses an in-progress schema change.
func (c *LocalClient) Stop(ctx context.Context, req *ternv1.StopRequest) (*ternv1.StopResponse, error) {
	c.logger.Info("Stop requested", "database", c.config.Database, "type", c.config.Type, "apply_id", req.ApplyId)
	tasks, err := c.storage.Tasks().GetByDatabase(ctx, c.config.Database)
	if err != nil {
		return nil, fmt.Errorf("get tasks failed: %w", err)
	}

	// If an apply_id was specified, resolve it and filter tasks to that apply only.
	var targetApplyID int64
	if req.ApplyId != "" {
		apply, err := c.storage.Applies().GetByApplyIdentifier(ctx, req.ApplyId)
		if err != nil || apply == nil {
			return nil, fmt.Errorf("apply %s not found", req.ApplyId)
		}
		targetApplyID = apply.ID
	}

	creds := c.credentials()
	eng := c.getEngine()

	// Stop the engine first, THEN snapshot progress.
	// eng.Stop() blocks until Spirit's goroutine exits, so by the time it
	// returns the progress data reflects the true final state of each table.
	if err := c.stopEngineForTasks(ctx, eng, creds, tasks, targetApplyID); err != nil {
		return nil, fmt.Errorf("engine stop failed: %w", err)
	}

	// Cancel the apply goroutine's context so it stops iterating over tasks.
	// Without this, executeApplySequential would continue to the next table
	// after Spirit's runner exits, racing with the resume goroutine.
	c.cancelMu.Lock()
	if c.cancelApply != nil {
		c.cancelApply()
		c.cancelApply = nil
	}
	c.cancelMu.Unlock()

	// Snapshot progress AFTER Spirit has fully stopped to preserve row copy progress.
	engineTableProgress := c.snapshotEngineProgress(ctx, eng, creds)

	// For Vitess/PlanetScale, stopping means cancelling the deploy request —
	// this is permanent (not resumable). Use "cancelled" instead of "stopped".
	terminalState := state.Task.Stopped
	if c.config.Type == storage.DatabaseTypeVitess {
		terminalState = state.Task.Cancelled
	}

	stoppedCount, skippedCount, applyID := c.markTasksWithState(ctx, tasks, targetApplyID, engineTableProgress, terminalState)

	if applyID > 0 && stoppedCount > 0 {
		eventMsg := fmt.Sprintf("Stop requested: %d tasks stopped, %d skipped", stoppedCount, skippedCount)
		if terminalState == state.Task.Cancelled {
			eventMsg = fmt.Sprintf("Cancel requested: %d tasks cancelled, %d skipped (deploy request cancelled)", stoppedCount, skippedCount)

			// For Vitess: set the apply state to cancelled now. The apply
			// goroutine will see a context cancellation error from the engine
			// and call failApplyWithTasks, but we set cancelled first so the
			// apply record reflects the true outcome. failApplyWithTasks skips
			// tasks already in terminal state, so the cancelled tasks are preserved.
			c.markApplyCancelled(ctx, applyID)
		}
		c.logApplyEvent(ctx, applyID, nil, storage.LogLevelInfo, storage.LogEventStopRequested, storage.LogSourceSchemaBot,
			eventMsg, "", "")
	}

	if stoppedCount == 0 && skippedCount == 0 {
		return nil, fmt.Errorf("no active schema change")
	}

	// Edge case: stop was requested but all tasks had already completed.
	// Mark the apply as completed so the TUI sees a clean terminal state.
	if stoppedCount == 0 && skippedCount > 0 && applyID > 0 {
		return c.handleStopAllCompleted(ctx, applyID, skippedCount)
	}

	return &ternv1.StopResponse{
		Accepted:     stoppedCount > 0,
		StoppedCount: stoppedCount,
		SkippedCount: skippedCount,
	}, nil
}

// stopEngineForTasks calls eng.Stop() if any targeted task is actively running.
// Returns an error if the engine stop fails (e.g., PlanetScale deploy request
// cancellation failed). For Spirit, stop errors are non-fatal since the runner
// may have already exited.
func (c *LocalClient) stopEngineForTasks(ctx context.Context, eng engine.Engine, creds *engine.Credentials, tasks []*storage.Task, targetApplyID int64) error {
	if eng == nil {
		c.logger.Error("stopEngineForTasks: engine is nil")
		return fmt.Errorf("no engine configured for type: %s", c.config.Type)
	}
	for _, task := range tasks {
		if targetApplyID > 0 && task.ApplyID != targetApplyID {
			continue
		}
		if state.IsTerminalTaskState(task.State) {
			c.logger.Info("skipping terminal task in stop", "task_id", task.TaskIdentifier, "state", task.State)
			continue
		}
		if task.State == state.Task.Running ||
			task.State == state.Task.WaitingForCutover ||
			task.State == state.Task.CuttingOver {
			req := c.buildControlRequest(ctx, task, creds)
			if _, err := eng.Stop(ctx, req); err != nil {
				c.logger.Warn("engine stop returned error (runner may have already exited)",
					"task_id", task.TaskIdentifier, "error", err)
			}
			return nil
		}
	}
	return nil
}

// snapshotEngineProgress captures per-table progress from the engine after stopping.
func (c *LocalClient) snapshotEngineProgress(ctx context.Context, eng engine.Engine, creds *engine.Credentials) map[string]*engine.TableProgress {
	if eng == nil {
		c.logger.Error("snapshotEngineProgress: engine is nil")
		return nil
	}
	progress, _ := eng.Progress(ctx, &engine.ProgressRequest{
		Database:    c.config.Database,
		Credentials: creds,
	})
	if progress != nil {
		return indexEngineTableProgress(progress.Tables)
	}
	return nil
}

// markTasksStopped sets all non-terminal targeted tasks to STOPPED, preserving engine progress.
// Returns (stopped count, skipped count, apply ID for logging).
func (c *LocalClient) markTasksWithState(ctx context.Context, tasks []*storage.Task, targetApplyID int64, engineProgress map[string]*engine.TableProgress, newState string) (int64, int64, int64) {
	var stoppedCount, skippedCount int64
	var applyID int64

	for _, task := range tasks {
		if targetApplyID > 0 && task.ApplyID != targetApplyID {
			continue
		}
		if applyID == 0 && task.ApplyID > 0 {
			applyID = task.ApplyID
		}
		if state.IsTerminalTaskState(task.State) {
			skippedCount++
			continue
		}

		// Mark as STOPPED — even if Spirit reports per-table IsComplete.
		// IsComplete means "row copy done", NOT "cutover done". The re-plan
		// during Start() will detect which tables truly completed.
		if et, ok := engineProgressForTask(engineProgress, task); ok {
			task.RowsCopied = et.RowsCopied
			task.RowsTotal = et.RowsTotal
			task.ProgressPercent = et.Progress
			task.ETASeconds = int(et.ETASeconds)
		}

		c.transitionTaskState(ctx, task, task.ApplyID, newState,
			fmt.Sprintf("Task %s %s", task.TaskIdentifier, newState))

		stoppedCount++
	}

	return stoppedCount, skippedCount, applyID
}

// handleStopAllCompleted handles the edge case where stop is requested but all tasks
// had already completed. Marks the apply as completed so the TUI sees a clean state.
func (c *LocalClient) handleStopAllCompleted(ctx context.Context, applyID int64, skippedCount int64) (*ternv1.StopResponse, error) {
	if apply, err := c.storage.Applies().Get(ctx, applyID); err == nil && apply != nil && !state.IsTerminalApplyState(apply.State) {
		now := time.Now()
		apply.State = state.Apply.Completed
		apply.CompletedAt = &now
		apply.UpdatedAt = now
		if err := c.storage.Applies().Update(ctx, apply); err != nil {
			c.logger.Error("failed to update apply state", "apply_id", apply.ApplyIdentifier, "state", state.Apply.Completed, "error", err)
		}

		c.logApplyEvent(ctx, applyID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			"All tasks completed before stop took effect", "", state.Apply.Completed)
	}

	return &ternv1.StopResponse{
		Accepted:     true,
		ErrorMessage: "Schema change already completed",
		StoppedCount: 0,
		SkippedCount: skippedCount,
	}, nil
}

// markApplyCancelled sets the apply to cancelled. Called by Stop() for Vitess
// databases where cancelling the deploy request is permanent. This runs before
// the apply goroutine sees the context cancellation, so failApplyWithTasks
// will find the apply already terminal and leave it alone.
func (c *LocalClient) markApplyCancelled(ctx context.Context, applyID int64) {
	apply, err := c.storage.Applies().Get(ctx, applyID)
	if err != nil || apply == nil {
		c.logger.Error("failed to load apply for cancellation", "apply_id", applyID, "error", err)
		return
	}
	now := time.Now()
	apply.State = state.Apply.Cancelled
	apply.CompletedAt = &now
	apply.UpdatedAt = now
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		c.logger.Error("failed to mark apply as cancelled", "apply_id", apply.ApplyIdentifier, "error", err)
	}
}

// controlSetup resolves the active task, credentials, and engine for a control operation.
// Returns an error if no active schema change exists or no engine is configured.
func (c *LocalClient) controlSetup(ctx context.Context) (*storage.Task, *engine.Credentials, engine.Engine, error) {
	task, err := c.getActiveTaskForDatabase(ctx, c.config.Database)
	if err != nil {
		return nil, nil, nil, err
	}
	if task == nil {
		return nil, nil, nil, fmt.Errorf("no active schema change")
	}
	eng := c.getEngine()
	if eng == nil {
		return nil, nil, nil, fmt.Errorf("no engine configured for type: %s", c.config.Type)
	}
	return task, c.credentials(), eng, nil
}

// buildControlRequest creates a ControlRequest with ResumeState from VitessApplyData.
// For PlanetScale, the engine needs the deploy request ID (in ResumeState.Metadata)
// to execute control operations (cutover, revert, skip-revert).
func (c *LocalClient) buildControlRequest(ctx context.Context, task *storage.Task, creds *engine.Credentials) *engine.ControlRequest {
	req := &engine.ControlRequest{
		Database:    c.config.Database,
		Credentials: creds,
	}
	if c.config.Type == storage.DatabaseTypeVitess {
		if vad, err := c.storage.VitessApplyData().GetByApplyID(ctx, task.ApplyID); err == nil {
			meta, _ := json.Marshal(map[string]any{
				"branch_name":        vad.BranchName,
				"deploy_request_id":  vad.DeployRequestID,
				"deploy_request_url": vad.DeployRequestURL,
				"is_instant":         vad.IsInstant,
				"deferred_deploy":    vad.DeferredDeploy,
			})
			req.ResumeState = &engine.ResumeState{
				MigrationContext: vad.MigrationContext,
				Metadata:         string(meta),
			}
		}
	}
	return req
}

// Volume modifies the schema change speed/concurrency in-flight.
func (c *LocalClient) Volume(ctx context.Context, req *ternv1.VolumeRequest) (*ternv1.VolumeResponse, error) {
	if req.Volume < 1 || req.Volume > 11 {
		return nil, fmt.Errorf("volume must be between 1 and 11")
	}

	_, creds, eng, err := c.controlSetup(ctx)
	if err != nil {
		return nil, err
	}

	result, err := eng.Volume(ctx, &engine.VolumeRequest{
		Database:    c.config.Database,
		Volume:      req.Volume,
		Credentials: creds,
	})
	if err != nil {
		return nil, fmt.Errorf("volume failed: %w", err)
	}

	return &ternv1.VolumeResponse{
		Accepted:       result.Accepted,
		PreviousVolume: result.PreviousVolume,
		NewVolume:      result.NewVolume,
	}, nil
}

// Revert reverts a completed schema change during the revert window.
func (c *LocalClient) Revert(ctx context.Context, req *ternv1.RevertRequest) (*ternv1.RevertResponse, error) {
	task, creds, eng, err := c.controlSetup(ctx)
	if err != nil {
		return nil, err
	}

	if _, err = eng.Revert(ctx, c.buildControlRequest(ctx, task, creds)); err != nil {
		return nil, fmt.Errorf("revert failed: %w", err)
	}
	return &ternv1.RevertResponse{Accepted: true}, nil
}

// SkipRevert skips the revert window and finalizes the schema change.
func (c *LocalClient) SkipRevert(ctx context.Context, req *ternv1.SkipRevertRequest) (*ternv1.SkipRevertResponse, error) {
	task, creds, eng, err := c.controlSetup(ctx)
	if err != nil {
		return nil, err
	}

	if _, err = eng.SkipRevert(ctx, c.buildControlRequest(ctx, task, creds)); err != nil {
		return nil, fmt.Errorf("skip revert failed: %w", err)
	}
	return &ternv1.SkipRevertResponse{Accepted: true}, nil
}

// RollbackPlan generates a plan to revert to the schema state before the most recent apply.
func (c *LocalClient) RollbackPlan(ctx context.Context, database string) (*ternv1.PlanResponse, error) {
	// Find the most recent completed task for this database
	tasks, err := c.storage.Tasks().GetByDatabase(ctx, c.config.Database)
	if err != nil {
		return nil, fmt.Errorf("get tasks failed: %w", err)
	}

	// Tasks are ordered by created_at DESC, so the first COMPLETED task is the most recent.
	// We prefer CompletedAt comparison when available, but fall back to creation order.
	var latestCompletedTask *storage.Task
	for _, t := range tasks {
		if t.State == state.Task.Completed {
			switch {
			case latestCompletedTask == nil:
				latestCompletedTask = t
			case t.CompletedAt != nil && latestCompletedTask.CompletedAt != nil:
				// Both have CompletedAt - use the later one
				if t.CompletedAt.After(*latestCompletedTask.CompletedAt) {
					latestCompletedTask = t
				}
			case t.CompletedAt != nil:
				// This task has CompletedAt, the other doesn't - prefer this one
				latestCompletedTask = t
			}
			// If neither has CompletedAt, keep the first one found (most recently created)
		}
	}

	if latestCompletedTask == nil {
		return nil, fmt.Errorf("no completed schema change found to rollback")
	}

	// Get the plan associated with this task
	plan, err := c.storage.Plans().GetByID(ctx, latestCompletedTask.PlanID)
	if err != nil {
		return nil, fmt.Errorf("get plan failed: %w", err)
	}
	if plan == nil {
		return nil, fmt.Errorf("plan not found for completed task")
	}

	originalSchema := plan.FlatOriginalSchema()
	if len(originalSchema) == 0 {
		return nil, fmt.Errorf("no original schema available for rollback (plan may predate rollback feature)")
	}

	// Convert OriginalSchema to SchemaFiles format for Plan request
	schemaFiles := make(map[string]*ternv1.SchemaFiles)
	for ns, nsData := range plan.Namespaces {
		if len(nsData.OriginalSchema) == 0 {
			continue
		}
		sqlFiles := make(map[string]string)
		for tableName, createSQL := range nsData.OriginalSchema {
			sqlFiles[tableName+".sql"] = createSQL
		}
		schemaFiles[ns] = &ternv1.SchemaFiles{Files: sqlFiles}
	}

	// Generate a new plan using the original schema as the target
	return c.Plan(ctx, &ternv1.PlanRequest{
		Database:    c.config.Database,
		Type:        c.config.Type,
		SchemaFiles: schemaFiles,
	})
}

// getActiveTaskForDatabase finds the first non-terminal task for a database.
func (c *LocalClient) getActiveTaskForDatabase(ctx context.Context, database string) (*storage.Task, error) {
	tasks, err := c.storage.Tasks().GetByDatabase(ctx, database)
	if err != nil {
		return nil, fmt.Errorf("get tasks failed: %w", err)
	}

	for _, t := range tasks {
		if !state.IsTerminalTaskState(t.State) {
			return t, nil
		}
	}
	return nil, nil
}
