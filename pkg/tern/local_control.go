package tern

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// Cutover triggers the cutover phase when defer_cutover was used.
func (c *LocalClient) Cutover(ctx context.Context, req *ternv1.CutoverRequest) (*ternv1.CutoverResponse, error) {
	return c.cutover(ctx, req, "")
}

func (c *LocalClient) cutover(ctx context.Context, req *ternv1.CutoverRequest, caller string) (*ternv1.CutoverResponse, error) {
	var task *storage.Task
	var apply *storage.Apply
	var err error

	if req.ApplyId != "" {
		var lookupErr error
		apply, lookupErr = c.storage.Applies().GetByApplyIdentifier(ctx, req.ApplyId)
		if lookupErr != nil {
			return nil, fmt.Errorf("load apply %s before cutover: %w", req.ApplyId, lookupErr)
		}
		if apply == nil {
			return nil, fmt.Errorf("load apply %s before cutover: %w", req.ApplyId, storage.ErrApplyNotFound)
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
		if task == nil && len(tasks) > 0 && state.IsState(apply.State, state.Apply.WaitingForCutover, state.Apply.CuttingOver) {
			c.logger.Info("cutover using completed task from cutover-ready apply",
				"apply_id", apply.ApplyIdentifier,
				"state", apply.State,
				"task_id", tasks[0].TaskIdentifier,
				"task_state", tasks[0].State)
			task = tasks[0]
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
	if apply == nil {
		apply, err = c.storage.Applies().Get(ctx, task.ApplyID)
		if err != nil {
			return nil, fmt.Errorf("load apply %d before cutover: %w", task.ApplyID, err)
		}
		if apply == nil {
			return nil, fmt.Errorf("load apply %d before cutover: %w", task.ApplyID, storage.ErrApplyNotFound)
		}
	}
	if state.IsState(apply.State, state.Apply.Recovering) {
		c.logger.Info("cutover blocked while apply is recovering state",
			"apply_id", apply.ApplyIdentifier,
			"task_id", task.TaskIdentifier,
			"task_state", task.State,
			"apply_state", apply.State)
		return &ternv1.CutoverResponse{
			Accepted:     false,
			ErrorMessage: "Schema change is recovering after restart; cutover will be available once recovery completes.",
		}, nil
	}
	if controlReq, err := pendingStopControlRequest(ctx, c.storage, apply); err != nil {
		return nil, fmt.Errorf("check pending stop request before cutover for apply %s: %w", apply.ApplyIdentifier, err)
	} else if controlReq != nil {
		c.logger.Info("cutover blocked because stop request is pending",
			"apply_id", apply.ApplyIdentifier,
			"requested_by", controlRequestCaller(controlReq))
		return nil, fmt.Errorf("schema change has a pending stop request; cutover is blocked until stop is processed")
	}

	creds, err := c.credentialsForTask(task)
	if err != nil {
		return nil, fmt.Errorf("resolve credentials for cutover task %s: %w", task.TaskIdentifier, err)
	}
	eng := c.getEngine()
	if eng == nil {
		return nil, fmt.Errorf("no engine configured for type: %s", c.config.Type)
	}

	controlReq, err := c.buildControlRequest(ctx, task, creds, eng, engine.ControlCutover)
	if err != nil {
		return nil, fmt.Errorf("build cutover request for apply %d: %w", task.ApplyID, err)
	}

	logMessage := "Cutover triggered"
	if caller != "" {
		logMessage += callerApplyLogSuffix(caller)
	}
	c.logApplyEvent(ctx, task.ApplyID, nil, storage.LogLevelInfo, storage.LogEventCutoverTriggered, storage.LogSourceSchemaBot,
		logMessage, "", "")

	result, err := eng.Cutover(ctx, controlReq)
	if err != nil {
		c.logApplyEvent(ctx, task.ApplyID, nil, storage.LogLevelError, storage.LogEventError, storage.LogSourceSchemaBot,
			fmt.Sprintf("Cutover failed: %v", err), "", "")
		return nil, fmt.Errorf("cutover failed: %w", err)
	}
	if result == nil {
		c.logApplyEvent(ctx, task.ApplyID, nil, storage.LogLevelError, storage.LogEventError, storage.LogSourceSchemaBot,
			"Cutover was not accepted: no response from engine", "", "")
		return &ternv1.CutoverResponse{Accepted: false, ErrorMessage: "not accepted"}, nil
	}
	if !result.Accepted {
		errorMessage := "not accepted"
		if result.Message != "" {
			errorMessage = result.Message
		}
		c.logApplyEvent(ctx, task.ApplyID, nil, storage.LogLevelError, storage.LogEventError, storage.LogSourceSchemaBot,
			fmt.Sprintf("Cutover was not accepted: %s", errorMessage), "", "")
		return &ternv1.CutoverResponse{Accepted: false, ErrorMessage: errorMessage}, nil
	}

	return &ternv1.CutoverResponse{Accepted: true}, nil
}

func (c *LocalClient) processPendingCutoverControlRequest(ctx context.Context, apply *storage.Apply) error {
	controlReq, err := pendingCutoverControlRequest(ctx, c.storage, apply)
	if err != nil {
		return err
	}
	if controlReq == nil {
		return nil
	}
	if cutoverRequestResolvedByApplyState(apply.State) {
		c.logger.Info("completing pending cutover request for resolved apply",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"requested_by", controlRequestCaller(controlReq),
			"state", apply.State)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventCutoverTriggered, storage.LogSourceSchemaBot,
			fmt.Sprintf("Pending cutover request completed for resolved apply%s", callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
		return completePendingCutoverControlRequests(ctx, c.storage, apply)
	}
	if cutoverRequestFailedByApplyState(apply.State) {
		message := fmt.Sprintf("cutover request was not applied because apply is %s", apply.State)
		if err := failPendingCutoverControlRequests(ctx, c.storage, apply, message); err != nil {
			return err
		}
		return fmt.Errorf("process pending cutover for apply %s: %s", apply.ApplyIdentifier, message)
	}
	if state.IsState(apply.State, state.Apply.Recovering) {
		c.logger.Info("pending cutover request is waiting for recovery to complete",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"requested_by", controlRequestCaller(controlReq),
			"state", apply.State)
		return nil
	}
	readyForCutover, err := applyReadyForCutoverRequest(ctx, c.storage, apply)
	if err != nil {
		return fmt.Errorf("check cutover readiness for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if !readyForCutover {
		c.logger.Info("pending cutover request is waiting for cutover-ready state",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"requested_by", controlRequestCaller(controlReq),
			"state", apply.State)
		return nil
	}
	if stopReq, err := pendingStopControlRequest(ctx, c.storage, apply); err != nil {
		return fmt.Errorf("check pending stop request before pending cutover for apply %s: %w", apply.ApplyIdentifier, err)
	} else if stopReq != nil {
		message := "schema change has a pending stop request; cutover is blocked until stop is processed"
		return fmt.Errorf("process pending cutover for apply %s: %s", apply.ApplyIdentifier, message)
	}
	if err := markApplyCuttingOverForControlRequest(ctx, c.storage, apply); err != nil {
		return err
	}
	resp, err := c.cutover(ctx, &ternv1.CutoverRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: apply.Environment,
	}, controlRequestCaller(controlReq))
	if err != nil {
		errorMessage := err.Error()
		if failErr := failPendingCutoverControlRequests(ctx, c.storage, apply, errorMessage); failErr != nil {
			return fmt.Errorf("process pending cutover for apply %s: %w; fail pending cutover request: %w", apply.ApplyIdentifier, err, failErr)
		}
		return fmt.Errorf("process pending cutover for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if resp == nil {
		errorMessage := "not accepted"
		if err := failPendingCutoverControlRequests(ctx, c.storage, apply, errorMessage); err != nil {
			return err
		}
		return fmt.Errorf("process pending cutover for apply %s: %s", apply.ApplyIdentifier, errorMessage)
	}
	if !resp.Accepted {
		errorMessage := "not accepted"
		if resp.ErrorMessage != "" {
			errorMessage = resp.ErrorMessage
		}
		if err := failPendingCutoverControlRequests(ctx, c.storage, apply, errorMessage); err != nil {
			return err
		}
		return fmt.Errorf("process pending cutover for apply %s: %s", apply.ApplyIdentifier, errorMessage)
	}
	if err := completePendingCutoverControlRequests(ctx, c.storage, apply); err != nil {
		return err
	}
	c.logger.Info("pending cutover request accepted and completed",
		"apply_id", apply.ApplyIdentifier,
		"database", apply.Database,
		"environment", apply.Environment,
		"requested_by", controlRequestCaller(controlReq),
		"state", apply.State)
	return nil
}

// Stop pauses an in-progress schema change.
func (c *LocalClient) Stop(ctx context.Context, req *ternv1.StopRequest) (*ternv1.StopResponse, error) {
	return c.stop(ctx, req, "")
}

func (c *LocalClient) stop(ctx context.Context, req *ternv1.StopRequest, caller string) (*ternv1.StopResponse, error) {
	c.logger.Info("Stop requested", "database", c.config.Database, "type", c.config.Type, "apply_id", req.ApplyId)
	tasks, err := c.storage.Tasks().GetByDatabase(ctx, c.config.Database)
	if err != nil {
		return nil, fmt.Errorf("get tasks failed: %w", err)
	}

	// If an apply_id was specified, resolve it and filter tasks to that apply only.
	var targetApplyID int64
	var targetApply *storage.Apply
	if req.ApplyId != "" {
		apply, err := c.storage.Applies().GetByApplyIdentifier(ctx, req.ApplyId)
		if err != nil || apply == nil {
			return nil, fmt.Errorf("apply %s not found", req.ApplyId)
		}
		targetApplyID = apply.ID
		targetApply = apply
	}

	// A task in the revert window has already cut over: the new schema is live.
	// Stop must not finalize it as cancelled — that would record a deployed
	// change as if nothing happened. Reject so the operator chooses explicitly
	// between revert (undo the deployed change) and skip-revert (finalize it).
	if revertTask := firstRevertWindowTask(tasks, targetApplyID); revertTask != nil {
		applyIdentifier := c.resolveRevertWindowApplyIdentifier(ctx, req, targetApply, revertTask)
		c.logger.Warn("stop rejected: schema change is in the revert window and has already cut over",
			"apply_id", applyIdentifier, "task_id", revertTask.TaskIdentifier, "state", revertTask.State)
		return nil, errors.New(revertWindowStopRejectionMessage(applyIdentifier))
	}

	eng := c.getEngine()
	applyCancel := c.currentApplyCancel()

	// Stop the engine first, THEN snapshot progress.
	// eng.Stop() blocks until Spirit's goroutine exits, so by the time it
	// returns the progress data reflects the true final state of each table.
	stopCreds, err := c.stopEngineForTasks(ctx, eng, tasks, targetApplyID)
	if err != nil {
		return nil, fmt.Errorf("engine stop failed: %w", err)
	}

	// Cancel the apply goroutine's context so it stops iterating over tasks.
	// Without this, executeApplySequential would continue to the next table
	// after Spirit's runner exits, racing with the resume goroutine.
	c.cancelApplyHandle(applyCancel)

	// For Vitess/PlanetScale, stopping means cancelling the deploy request —
	// this is permanent (not resumable). Use "cancelled" instead of "stopped".
	terminalState := state.Task.Stopped
	var engineTableProgress map[string]*engine.TableProgress
	if c.config.Type == storage.DatabaseTypeVitess {
		terminalState = state.Task.Cancelled
	} else {
		// Snapshot progress AFTER Spirit has fully stopped to preserve row copy progress.
		engineTableProgress = c.snapshotEngineProgress(ctx, eng, stopCreds)
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
			if err := c.markApplyCancelled(ctx, applyID); err != nil {
				return nil, err
			}
		} else if err := c.markApplyStopped(ctx, applyID); err != nil {
			return nil, err
		}
		if caller != "" {
			eventMsg += callerApplyLogSuffix(caller)
		}
		c.logApplyEvent(ctx, applyID, nil, storage.LogLevelInfo, storage.LogEventStopRequested, storage.LogSourceSchemaBot,
			eventMsg, "", "")
	}

	if stoppedCount == 0 && skippedCount == 0 {
		return nil, fmt.Errorf("no active schema change")
	}

	// Edge case: stop was requested but every targeted task is already
	// terminal. Finalize the apply from its task states so the TUI sees an
	// accurate terminal state.
	if stoppedCount == 0 && skippedCount > 0 && applyID > 0 {
		if targetApply != nil && state.IsTerminalApplyState(targetApply.State) && !state.IsState(targetApply.State, state.Apply.Completed) {
			c.logger.Info("all tasks are terminal and apply is already terminal; preserving apply state during stop",
				"apply_id", targetApply.ApplyIdentifier,
				"state", targetApply.State,
				"skipped_count", skippedCount)
			return &ternv1.StopResponse{
				Accepted:     true,
				StoppedCount: 0,
				SkippedCount: skippedCount,
			}, nil
		}
		return c.handleStopAllTasksTerminal(ctx, applyID, skippedCount)
	}

	return &ternv1.StopResponse{
		Accepted:     stoppedCount > 0,
		StoppedCount: stoppedCount,
		SkippedCount: skippedCount,
	}, nil
}

func (c *LocalClient) processPendingStopControlRequest(ctx context.Context, apply *storage.Apply) (bool, error) {
	controlReq, err := pendingStopControlRequest(ctx, c.storage, apply)
	if err != nil {
		return false, err
	}
	if controlReq == nil {
		return false, nil
	}
	if completed, err := completePendingStopIfStoredApplyResolved(ctx, c.storage, apply); err != nil {
		return true, err
	} else if completed {
		c.logger.Info("completing pending stop request for resolved apply",
			"apply_id", apply.ApplyIdentifier,
			"requested_by", controlRequestCaller(controlReq),
			"state", apply.State)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStopRequested, storage.LogSourceSchemaBot,
			fmt.Sprintf("Pending stop request completed for resolved apply%s", callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
		return true, nil
	}
	if state.IsTerminalApplyState(apply.State) {
		c.logger.Info("completing pending stop request for terminal apply",
			"apply_id", apply.ApplyIdentifier,
			"requested_by", controlRequestCaller(controlReq),
			"state", apply.State)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStopRequested, storage.LogSourceSchemaBot,
			fmt.Sprintf("Pending stop request completed for terminal apply%s", callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
		if err := completePendingStopControlRequests(ctx, c.storage, apply); err != nil {
			return true, err
		}
		return true, nil
	}

	// A revert-window apply has already cut over. Stop is a permanent rejection,
	// not a retryable error: failing the durable request resolves it terminally
	// so the operator-owned retry loop stops re-running stop. The operator must
	// revert (undo) or skip-revert (finalize) instead.
	if revertWindow, err := c.applyHasRevertWindowTask(ctx, apply); err != nil {
		return true, err
	} else if revertWindow {
		message := revertWindowStopRejectionMessage(apply.ApplyIdentifier)
		c.logger.Warn("rejecting pending stop request: schema change is in the revert window and has already cut over",
			"apply_id", apply.ApplyIdentifier,
			"requested_by", controlRequestCaller(controlReq),
			"state", apply.State)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventStopRequested, storage.LogSourceSchemaBot,
			fmt.Sprintf("Pending stop request rejected: %s%s", message, callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
		if err := failPendingStopControlRequests(ctx, c.storage, apply, message); err != nil {
			return true, err
		}
		return true, nil
	}

	stopCtx := context.WithoutCancel(ctx)
	resp, err := c.stop(stopCtx, &ternv1.StopRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: apply.Environment,
	}, controlRequestCaller(controlReq))
	if err != nil {
		return true, fmt.Errorf("process pending stop for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if resp == nil || !resp.Accepted {
		errorMessage := "not accepted"
		if resp != nil && resp.ErrorMessage != "" {
			errorMessage = resp.ErrorMessage
		}
		return true, fmt.Errorf("process pending stop for apply %s: %s", apply.ApplyIdentifier, errorMessage)
	}
	completed, err := completePendingStopIfStoredApplyResolved(stopCtx, c.storage, apply)
	if err != nil {
		return true, err
	}
	if !completed {
		return true, fmt.Errorf("process pending stop for apply %s: storage did not reach a resolved stop state", apply.ApplyIdentifier)
	}
	return true, nil
}

// firstRevertWindowTask returns the first targeted task that is in the revert
// window, or nil if none are. A revert-window task has already cut over, so the
// stop path must reject rather than treat it as a cancellable in-flight change.
//
// When targetApplyID is 0 (an untargeted stop with no apply id), any
// revert-window task on the database rejects the whole stop, even one belonging
// to a different apply. This is bounded by the one-active-apply-per-target
// invariant: storage permits at most one active apply per (database, database
// type, environment), and LocalClient is scoped to a single such target, so a
// revert-window task from a second, distinct apply cannot coexist with another
// active apply on the same target. Cross-apply coexistence is therefore not a
// case this scope has to disambiguate.
func firstRevertWindowTask(tasks []*storage.Task, targetApplyID int64) *storage.Task {
	for _, task := range tasks {
		if targetApplyID > 0 && task.ApplyID != targetApplyID {
			continue
		}
		if state.IsState(task.State, state.Task.RevertWindow) {
			return task
		}
	}
	return nil
}

// revertWindowStopRejectionMessage is the operator-facing reason a stop targeting
// a revert-window schema change is permanently rejected. The change has already
// cut over, so the operator must choose revert or skip-revert.
func revertWindowStopRejectionMessage(applyIdentifier string) string {
	return fmt.Sprintf("schema change %s is in the revert window and has already been applied: use revert to undo it or skip-revert to finalize it", applyIdentifier)
}

// resolveRevertWindowApplyIdentifier returns the apply-level identifier an
// operator supplied or recognizes, not the per-table task identifier. It prefers
// the requested apply id, then the resolved target apply, then a lookup of the
// revert task's apply, falling back to the task identifier only if the apply
// cannot be loaded.
func (c *LocalClient) resolveRevertWindowApplyIdentifier(ctx context.Context, req *ternv1.StopRequest, targetApply *storage.Apply, revertTask *storage.Task) string {
	if req.ApplyId != "" {
		return req.ApplyId
	}
	if targetApply != nil && targetApply.ApplyIdentifier != "" {
		return targetApply.ApplyIdentifier
	}
	apply, err := c.storage.Applies().Get(ctx, revertTask.ApplyID)
	if err != nil {
		c.logger.Warn("could not load apply to resolve revert-window stop identifier; using task identifier",
			"apply_db_id", revertTask.ApplyID, "task_id", revertTask.TaskIdentifier, "error", err)
		return revertTask.TaskIdentifier
	}
	if apply == nil {
		c.logger.Warn("apply not found while resolving revert-window stop identifier; using task identifier",
			"apply_db_id", revertTask.ApplyID, "task_id", revertTask.TaskIdentifier)
		return revertTask.TaskIdentifier
	}
	return apply.ApplyIdentifier
}

// applyHasRevertWindowTask reports whether any task for the apply is in the
// revert window. It reads the stored tasks directly so the durable stop path
// detects the same cut-over condition the synchronous stop path rejects on.
func (c *LocalClient) applyHasRevertWindowTask(ctx context.Context, apply *storage.Apply) (bool, error) {
	tasks, err := c.storage.Tasks().GetByApplyID(ctx, apply.ID)
	if err != nil {
		return false, fmt.Errorf("load tasks for apply %s to detect revert window before stop: %w", apply.ApplyIdentifier, err)
	}
	return firstRevertWindowTask(tasks, apply.ID) != nil, nil
}

// hasLiveEngineWork reports whether a task in this state has live engine or
// remote work that eng.Stop must terminate before storage records the stop.
// These are the non-terminal states where a Spirit runner is copying rows or a
// PlanetScale deploy request is created and can be cancelled:
//   - Running / CuttingOver: Spirit runner or PlanetScale deploy actively executing.
//   - WaitingForCutover: Spirit runner alive, holding connections until cutover.
//   - Recovering: Spirit's runner is restarted with a detached context during
//     recovery; only eng.Stop kills it, so without this the runner keeps copying
//     rows while storage reports stopped and a later resume blocks in Drain()
//     behind the abandoned runner.
//   - WaitingForDeploy: the PlanetScale deferred deploy request exists and stays
//     startable from the PlanetScale UI until eng.Stop cancels it.
//   - FailedRetryable: a transient failure (e.g. repeated progress-poll errors)
//     pauses the apply for operator retry, but the PlanetScale deploy request was
//     already created and its resume state persisted before the failure, so the
//     deploy request stays live and startable from the PlanetScale UI. Without
//     eng.Stop, recording the stop as cancelled would leave that deploy request
//     runnable from the provider side — the same storage-vs-engine divergence the
//     other live states avoid. eng.Stop (CancelDeployRequest) is keyed only on the
//     persisted deploy request id, so cancelling a retryable task is safe; stop is
//     a terminal operator action that ends the apply rather than retrying it.
func hasLiveEngineWork(taskState string) bool {
	return state.IsState(taskState,
		state.Task.Running,
		state.Task.WaitingForCutover,
		state.Task.CuttingOver,
		state.Task.Recovering,
		state.Task.WaitingForDeploy,
		state.Task.FailedRetryable)
}

// stopEngineForTasks calls eng.Stop() if any targeted task has live engine work.
// Returns an error if the engine stop fails (e.g., PlanetScale deploy request
// cancellation failed). For Spirit, stop errors are non-fatal since the runner
// may have already exited.
//
// It stops at the first task with live engine work and returns: an apply drives
// a single engine operation (one Spirit runner or one PlanetScale deploy
// request) whose stop terminates the whole operation, so one eng.Stop covers the
// targeted apply.
func (c *LocalClient) stopEngineForTasks(ctx context.Context, eng engine.Engine, tasks []*storage.Task, targetApplyID int64) (*engine.Credentials, error) {
	if eng == nil {
		c.logger.Error("stopEngineForTasks: engine is nil")
		return nil, fmt.Errorf("no engine configured for type: %s", c.config.Type)
	}
	for _, task := range tasks {
		if targetApplyID > 0 && task.ApplyID != targetApplyID {
			continue
		}
		if state.IsTerminalTaskState(task.State) {
			c.logger.Info("skipping terminal task in stop", "task_id", task.TaskIdentifier, "state", task.State)
			continue
		}
		if !hasLiveEngineWork(task.State) {
			c.logger.Debug("skipping engine stop for task with no live engine work",
				"task_id", task.TaskIdentifier, "state", task.State)
			continue
		}
		creds, err := c.credentialsForTask(task)
		if err != nil {
			return nil, fmt.Errorf("resolve credentials for stop task %s: %w", task.TaskIdentifier, err)
		}
		req, err := c.buildControlRequest(ctx, task, creds, eng, engine.ControlStop)
		if err != nil {
			return nil, fmt.Errorf("build stop request for task %s: %w", task.TaskIdentifier, err)
		}
		if _, err := eng.Stop(ctx, req); err != nil {
			if c.config.Type == storage.DatabaseTypeVitess {
				return nil, fmt.Errorf("cancel deploy request for task %s: %w", task.TaskIdentifier, err)
			}
			c.logger.Warn("engine stop returned error (runner may have already exited)",
				"task_id", task.TaskIdentifier, "error", err)
		}
		return creds, nil
	}
	c.logger.Debug("no targeted task has live engine work to stop", "database", c.config.Database, "type", c.config.Type, "target_apply_id", targetApplyID)
	return nil, nil
}

// snapshotEngineProgress captures per-table progress from the engine after stopping.
func (c *LocalClient) snapshotEngineProgress(ctx context.Context, eng engine.Engine, creds *engine.Credentials) map[string]*engine.TableProgress {
	if eng == nil {
		c.logger.Error("snapshotEngineProgress: engine is nil")
		return nil
	}
	if creds == nil {
		c.logger.Debug("skipping engine progress snapshot because no live engine work was stopped", "database", c.config.Database, "type", c.config.Type)
		return nil
	}
	progress, err := eng.Progress(ctx, &engine.ProgressRequest{
		Database:    c.config.Database,
		Credentials: creds,
	})
	if err != nil {
		c.logger.Warn("failed to snapshot engine progress after stop",
			"database", c.config.Database, "type", c.config.Type, "error", err)
		return nil
	}
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

// firstFailedTaskError returns an apply-level failure reason derived from task
// rows: the first failed task that recorded an error message, preferring
// hard-failed tasks over retryable ones. Returns "" when no failed task
// recorded a reason.
func firstFailedTaskError(tasks []*storage.Task) string {
	for _, failedState := range []string{state.Task.Failed, state.Task.FailedRetryable} {
		for _, task := range tasks {
			if state.IsState(task.State, failedState) && task.ErrorMessage != "" {
				return fmt.Sprintf("table %s failed: %s", task.TableName, task.ErrorMessage)
			}
		}
	}
	return ""
}

// handleStopAllTasksTerminal handles the edge case where stop is requested but
// every targeted task is already in a terminal state (completed, failed,
// cancelled, or reverted). The apply row may still be non-terminal — e.g., a
// worker exited after finalizing task rows but before the apply row — so the
// apply's final state is derived from its task states rather than assumed.
// A failed task must surface as a failed apply, never as a completed one, and
// its failure reason is propagated so operators can triage from the apply
// record. An ErrorMessage already on the apply is authoritative and kept.
func (c *LocalClient) handleStopAllTasksTerminal(ctx context.Context, applyID int64, skippedCount int64) (*ternv1.StopResponse, error) {
	apply, err := c.storage.Applies().Get(ctx, applyID)
	if err != nil {
		return nil, fmt.Errorf("load apply %d after stop found all tasks terminal: %w", applyID, err)
	}
	if apply == nil {
		return nil, fmt.Errorf("load apply %d after stop found all tasks terminal: %w", applyID, storage.ErrApplyNotFound)
	}

	if !state.IsTerminalApplyState(apply.State) {
		tasks, err := c.storage.Tasks().GetByApplyID(ctx, applyID)
		if err != nil {
			return nil, fmt.Errorf("load tasks for apply %s to derive final state: %w", apply.ApplyIdentifier, err)
		}
		derivedState := state.DeriveApplyState(taskStates(tasks))
		oldState := apply.State
		now := time.Now()
		apply.State = derivedState
		if state.IsState(derivedState, state.Apply.Failed) && apply.ErrorMessage == "" {
			apply.ErrorMessage = firstFailedTaskError(tasks)
		}
		if state.IsTerminalApplyState(derivedState) {
			apply.CompletedAt = &now
		}
		apply.UpdatedAt = now
		if err := c.storage.Applies().Update(ctx, apply); err != nil {
			return nil, fmt.Errorf("update apply %s to derived state %s: %w", apply.ApplyIdentifier, derivedState, err)
		}

		c.logger.Info("stop found all tasks terminal; apply state derived from tasks",
			"apply_id", apply.ApplyIdentifier,
			"old_state", oldState,
			"new_state", derivedState,
			"skipped_count", skippedCount)
		c.logApplyEvent(ctx, applyID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			fmt.Sprintf("All tasks terminal before stop took effect; apply state derived from tasks: %s", derivedState), oldState, derivedState)
	}

	return &ternv1.StopResponse{
		Accepted:     true,
		ErrorMessage: fmt.Sprintf("Schema change already %s", apply.State),
		StoppedCount: 0,
		SkippedCount: skippedCount,
	}, nil
}

func (c *LocalClient) markApplyStopped(ctx context.Context, applyID int64) error {
	apply, err := c.storage.Applies().Get(ctx, applyID)
	if err != nil {
		return fmt.Errorf("load apply %d for stopped state: %w", applyID, err)
	}
	if apply == nil {
		return fmt.Errorf("load apply %d for stopped state: %w", applyID, storage.ErrApplyNotFound)
	}
	if state.IsTerminalApplyState(apply.State) && !state.IsState(apply.State, state.Apply.Stopped) {
		c.logger.Info("apply already terminal after stop, not marking stopped",
			"apply_id", apply.ApplyIdentifier,
			"state", apply.State)
		return nil
	}

	apply.State = state.Apply.Stopped
	apply.CompletedAt = nil
	apply.UpdatedAt = time.Now()
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		return fmt.Errorf("mark apply %s stopped: %w", apply.ApplyIdentifier, err)
	}
	return nil
}

// markApplyCancelled sets the apply to cancelled. Called by Stop() for Vitess
// databases where cancelling the deploy request is permanent. This runs before
// the apply goroutine sees the context cancellation, so failApplyWithTasks
// will find the apply already terminal and leave it alone.
func (c *LocalClient) markApplyCancelled(ctx context.Context, applyID int64) error {
	apply, err := c.storage.Applies().Get(ctx, applyID)
	if err != nil {
		return fmt.Errorf("load apply %d for cancelled state: %w", applyID, err)
	}
	if apply == nil {
		return fmt.Errorf("load apply %d for cancelled state: %w", applyID, storage.ErrApplyNotFound)
	}
	now := time.Now()
	apply.State = state.Apply.Cancelled
	apply.CompletedAt = &now
	apply.UpdatedAt = now
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		return fmt.Errorf("mark apply %s cancelled: %w", apply.ApplyIdentifier, err)
	}
	return nil
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
	creds, err := c.credentialsForTask(task)
	if err != nil {
		return nil, nil, nil, err
	}
	return task, creds, eng, nil
}

// buildControlRequest creates a ControlRequest with persisted engine resume data.
// Engines own validation of opaque ResumeState.Metadata before control calls.
func (c *LocalClient) buildControlRequest(ctx context.Context, task *storage.Task, creds *engine.Credentials, eng engine.Engine, operation engine.ControlOperation) (*engine.ControlRequest, error) {
	req := &engine.ControlRequest{
		Database:    c.config.Database,
		Credentials: creds,
	}
	if c.config.Type == storage.DatabaseTypeVitess {
		resumeState, err := c.loadEngineResumeState(ctx, task)
		if err != nil {
			return nil, fmt.Errorf("load Vitess engine resume state for task %s: %w", task.TaskIdentifier, err)
		}
		req.ResumeState = resumeState
	}
	if validator, ok := eng.(engine.ControlResumeValidator); ok {
		if err := validator.ValidateControlResumeState(operation, req.ResumeState); err != nil {
			return nil, fmt.Errorf("validate %s resume state for task %s: %w", operation, task.TaskIdentifier, err)
		}
	}
	return req, nil
}

// Volume modifies the schema change speed/concurrency in-flight.
func (c *LocalClient) Volume(ctx context.Context, req *ternv1.VolumeRequest) (*ternv1.VolumeResponse, error) {
	if req.Volume < 1 || req.Volume > 11 {
		return nil, fmt.Errorf("volume must be between 1 and 11")
	}

	task, creds, eng, err := c.controlSetup(ctx)
	if err != nil {
		return nil, err
	}
	controlReq, err := c.buildControlRequest(ctx, task, creds, eng, engine.ControlVolume)
	if err != nil {
		return nil, fmt.Errorf("build volume request for task %s: %w", task.TaskIdentifier, err)
	}

	result, err := eng.Volume(ctx, &engine.VolumeRequest{
		Database:    c.config.Database,
		Volume:      req.Volume,
		ResumeState: controlReq.ResumeState,
		Credentials: controlReq.Credentials,
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

	controlReq, err := c.buildControlRequest(ctx, task, creds, eng, engine.ControlRevert)
	if err != nil {
		return nil, fmt.Errorf("build revert request for task %s: %w", task.TaskIdentifier, err)
	}
	if _, err = eng.Revert(ctx, controlReq); err != nil {
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

	controlReq, err := c.buildControlRequest(ctx, task, creds, eng, engine.ControlSkipRevert)
	if err != nil {
		return nil, fmt.Errorf("build skip-revert request for task %s: %w", task.TaskIdentifier, err)
	}
	if _, err = eng.SkipRevert(ctx, controlReq); err != nil {
		return nil, fmt.Errorf("skip revert failed: %w", err)
	}
	return &ternv1.SkipRevertResponse{Accepted: true}, nil
}

// RollbackPlan generates a plan to revert to the schema state before the most recent apply.
func (c *LocalClient) RollbackPlan(ctx context.Context, database, environment string) (*ternv1.PlanResponse, error) {
	if database != c.config.Database {
		return nil, fmt.Errorf("rollback plan requested database %s, but client is configured for database %s", database, c.config.Database)
	}

	// Find the most recent completed task for this database and environment.
	tasks, err := c.storage.Tasks().GetByDatabase(ctx, database)
	if err != nil {
		return nil, fmt.Errorf("get tasks failed: %w", err)
	}

	// Tasks are ordered by created_at DESC, so the first COMPLETED task is the most recent.
	// We prefer CompletedAt comparison when available, but fall back to creation order.
	var latestCompletedTask *storage.Task
	for _, t := range tasks {
		if t.Environment == environment && t.DatabaseType == c.config.Type && state.IsState(t.State, state.Task.Completed) {
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
		return nil, fmt.Errorf("no completed schema change found to rollback for database %s environment %s", database, environment)
	}

	// Get the plan associated with this task
	plan, err := c.storage.Plans().GetByID(ctx, latestCompletedTask.PlanID)
	if err != nil {
		return nil, fmt.Errorf("get plan failed: %w", err)
	}
	if plan == nil {
		return nil, fmt.Errorf("plan not found for completed task")
	}
	if plan.Environment != environment {
		return nil, fmt.Errorf("completed task for database %s environment %s points to plan for environment %s", database, environment, plan.Environment)
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
		Database:    database,
		Type:        c.config.Type,
		Environment: environment,
		Target:      plan.Target,
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
