package tern

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/metrics"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// Cutover triggers the cutover phase when defer_cutover was used.
// Cutover queues a durable cutover request. The instance that owns the schema
// change performs the cutover when it observes the pending request through
// shared storage, so the request is safe on any instance serving this route.
func (c *LocalClient) Cutover(ctx context.Context, req *ternv1.CutoverRequest) (*ternv1.CutoverResponse, error) {
	return c.requestCutover(ctx, req, req.Caller)
}

// requestCutover resolves the target apply and records a durable cutover request
// for its owner to process. It never invokes the local engine, mirroring how
// stop and start are routed to the apply owner.
func (c *LocalClient) requestCutover(ctx context.Context, req *ternv1.CutoverRequest, caller string) (*ternv1.CutoverResponse, error) {
	c.logger.Info("Cutover requested", "database", c.config.Database, "type", c.config.Type, "apply_id", req.ApplyId, "environment", req.Environment, "caller", caller)
	apply, err := c.resolveControlApply(ctx, req.ApplyId, "cutover")
	if err != nil {
		return nil, err
	}
	if apply == nil {
		return nil, fmt.Errorf("no active schema change")
	}
	return c.queueCutoverRequest(ctx, apply, caller)
}

// resolveControlApply finds the apply a control request targets, by apply
// identifier when provided or by the database's active task otherwise. The
// operation label names the control verb in errors and logs. Returns
// (nil, nil) when no apply id is given and the database has no active task.
func (c *LocalClient) resolveControlApply(ctx context.Context, applyID, operation string) (*storage.Apply, error) {
	if applyID != "" {
		apply, err := c.storage.Applies().GetByApplyIdentifier(ctx, applyID)
		if err != nil {
			return nil, fmt.Errorf("load apply %s before %s: %w", applyID, operation, err)
		}
		if apply == nil {
			return nil, fmt.Errorf("load apply %s before %s: %w", applyID, operation, storage.ErrApplyNotFound)
		}
		return apply, nil
	}

	task, err := c.getActiveTaskForDatabase(ctx, c.config.Database)
	if err != nil {
		return nil, err
	}
	if task == nil {
		c.logger.Info(operation+" request found no active task", "database", c.config.Database, "type", c.config.Type)
		return nil, nil
	}
	apply, err := c.storage.Applies().Get(ctx, task.ApplyID)
	if err != nil {
		return nil, fmt.Errorf("load apply %d before %s: %w", task.ApplyID, operation, err)
	}
	if apply == nil {
		return nil, fmt.Errorf("load apply %d before %s: %w", task.ApplyID, operation, storage.ErrApplyNotFound)
	}
	return apply, nil
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
	if controlReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationStop); err != nil {
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

	result, err := eng.Cutover(ctx, controlReq)
	if engine.IsNotReady(err) {
		// The engine's backend advertised the cutover gate before it was ready
		// to accept the cutover — a self-clearing condition, not a failure. No
		// timeline event is written for the rejected attempt: the caller
		// classifies this error and retries, so recording every attempt would
		// fill the user-visible timeline with triggers that had no effect.
		return nil, fmt.Errorf("cutover not accepted yet: %w", err)
	}

	// The trigger event is recorded once the backend has accepted the attempt
	// into flight or definitively failed it, so the timeline shows one trigger
	// per effective attempt rather than one per not-ready retry.
	logMessage := "Cutover triggered"
	if caller != "" {
		logMessage += callerApplyLogSuffix(caller)
	}
	c.logApplyEvent(ctx, task.ApplyID, nil, storage.LogLevelInfo, storage.LogEventCutoverTriggered, storage.LogSourceSchemaBot,
		logMessage, "", "")

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

// queueCutoverRequest records a durable cutover control request and returns
// once it is queued. The instance that owns the schema change observes the
// pending request through shared storage and performs the cutover, the same way
// stop and start are routed to the owner. A cutover RPC can land on any instance
// sharing the route's storage, so it must never act on a local engine that may
// not be running this schema change.
func (c *LocalClient) queueCutoverRequest(ctx context.Context, apply *storage.Apply, caller string) (*ternv1.CutoverResponse, error) {
	controlStore := c.storage.ControlRequests()
	if controlStore == nil {
		return nil, fmt.Errorf("control request store is not available")
	}
	requestedBy := controlRequestRequester(caller)
	_, alreadyPending, err := controlStore.RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCutover,
		Status:      storage.ControlRequestPending,
		RequestedBy: requestedBy,
	})
	if err != nil {
		return nil, fmt.Errorf("record cutover control request for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if alreadyPending {
		c.logger.Info("cutover request already pending for apply owner",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"requested_by", requestedBy)
	} else {
		c.logger.Info("cutover request queued for apply owner",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"requested_by", requestedBy)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventCutoverTriggered, storage.LogSourceSchemaBot,
			fmt.Sprintf("Cutover request queued for apply owner%s", callerApplyLogSuffix(requestedBy)), "", "")
	}
	c.wakeOperator(apply)
	return &ternv1.CutoverResponse{Accepted: true}, nil
}

func (c *LocalClient) processPendingCutoverControlRequest(ctx context.Context, apply *storage.Apply) error {
	controlReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationCutover)
	if err != nil {
		return err
	}
	if controlReq == nil {
		return nil
	}
	// Bind the apply's identity once so every consumption log line is
	// filterable by apply_id/repo/pr without hand-listing the attrs per call.
	logger := c.logger.With(apply.IdentityLogAttrs()...)
	if cutoverRequestResolvedByApplyState(apply.State) {
		logger.Info("completing pending cutover request for resolved apply",
			"requested_by", controlRequestCaller(controlReq),
			"state", apply.State)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventCutoverTriggered, storage.LogSourceSchemaBot,
			fmt.Sprintf("Pending cutover request completed for resolved apply%s", callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
		return completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCutover)
	}
	if cutoverRequestFailedByApplyState(apply.State) {
		message := fmt.Sprintf("cutover request was not applied because apply is %s", apply.State)
		if err := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCutover, message); err != nil {
			return err
		}
		return fmt.Errorf("process pending cutover for apply %s: %s", apply.ApplyIdentifier, message)
	}
	if state.IsState(apply.State, state.Apply.Recovering) {
		logger.Info("pending cutover request is waiting for recovery to complete",
			"requested_by", controlRequestCaller(controlReq),
			"state", apply.State)
		return nil
	}
	readyForCutover, err := applyReadyForCutoverRequest(ctx, c.storage, apply)
	if err != nil {
		return fmt.Errorf("check cutover readiness for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if !readyForCutover {
		logger.Info("pending cutover request is waiting for cutover-ready state",
			"requested_by", controlRequestCaller(controlReq),
			"state", apply.State)
		return nil
	}
	if stopReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationStop); err != nil {
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
	if engine.IsNotReady(err) {
		// The engine's backend has not finished staging the cutover; the
		// rejection clears on its own. Leave the request pending so the next
		// progress tick retries it — terminally failing it would strand a
		// deferred cutover until an operator notices and re-issues the command.
		logger.Info("pending cutover request not accepted yet by engine backend; retrying at the next progress tick",
			append(apply.MutableLogAttrs(), "requested_by", controlRequestCaller(controlReq), "error", err)...)
		return nil
	}
	if err != nil {
		errorMessage := err.Error()
		if failErr := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCutover, errorMessage); failErr != nil {
			return fmt.Errorf("process pending cutover for apply %s: %w; fail pending cutover request: %w", apply.ApplyIdentifier, err, failErr)
		}
		return fmt.Errorf("process pending cutover for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if resp == nil {
		errorMessage := "not accepted"
		if err := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCutover, errorMessage); err != nil {
			return err
		}
		return fmt.Errorf("process pending cutover for apply %s: %s", apply.ApplyIdentifier, errorMessage)
	}
	if !resp.Accepted {
		errorMessage := "not accepted"
		if resp.ErrorMessage != "" {
			errorMessage = resp.ErrorMessage
		}
		if err := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCutover, errorMessage); err != nil {
			return err
		}
		return fmt.Errorf("process pending cutover for apply %s: %s", apply.ApplyIdentifier, errorMessage)
	}
	if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCutover); err != nil {
		return err
	}
	logger.Info("pending cutover request accepted and completed",
		"requested_by", controlRequestCaller(controlReq),
		"state", apply.State)
	return nil
}

// Stop pauses an in-progress schema change.
func (c *LocalClient) Stop(ctx context.Context, req *ternv1.StopRequest) (*ternv1.StopResponse, error) {
	return c.requestStop(ctx, req, req.Caller)
}

// Cancel terminates an in-progress schema change permanently.
func (c *LocalClient) Cancel(ctx context.Context, req *ternv1.CancelRequest) (*ternv1.CancelResponse, error) {
	return c.requestCancel(ctx, req, req.Caller)
}

// requestCancel records a durable, owner-routed cancel control request rather than
// cancelling inline. A Cancel RPC can land on any pod, but only the lease owner
// driving the apply holds the in-process engine state — so the request is queued
// for the owning driver (processPendingCancelControlRequest) to claim and execute
// the engine cancel plus the terminal transitions on the pod that owns the apply.
// This mirrors requestStop's delivery; cancel keeps its own terminate semantics.
func (c *LocalClient) requestCancel(ctx context.Context, req *ternv1.CancelRequest, caller string) (*ternv1.CancelResponse, error) {
	c.logger.Info("Cancel requested", "database", c.config.Database, "type", c.config.Type, "apply_id", req.ApplyId)
	apply, err := c.resolveControlApply(ctx, req.ApplyId, "cancel")
	if err != nil {
		return nil, err
	}
	if apply == nil {
		return nil, fmt.Errorf("no active schema change")
	}

	// A terminal apply (other than stopped, which a driver can still cancel) has
	// no work to cancel; reject synchronously rather than queue a no-op request.
	if state.IsTerminalApplyState(apply.State) && !state.IsState(apply.State, state.Apply.Stopped) {
		c.logger.Warn("cancel rejected: schema change is already terminal",
			"apply_id", apply.ApplyIdentifier, "state", apply.State)
		return nil, fmt.Errorf("schema change %s is already terminal (state: %s)", apply.ApplyIdentifier, apply.State)
	}

	if revertPhase, err := c.applyRevertPhaseBlock(ctx, apply); err != nil {
		return nil, err
	} else if revertPhase != "" {
		c.logger.Warn("cancel rejected: schema change is in a revert phase and has already cut over",
			"apply_id", apply.ApplyIdentifier, "state", apply.State, "revert_phase", revertPhase)
		return nil, errors.New(revertPhaseControlRejectionMessage(apply.ApplyIdentifier, revertPhase))
	}

	controlStore := c.storage.ControlRequests()
	if controlStore == nil {
		return nil, fmt.Errorf("control request store is not available")
	}
	requestedBy := controlRequestRequester(caller)
	_, alreadyPending, err := controlStore.RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: requestedBy,
	})
	if err != nil {
		return nil, fmt.Errorf("record cancel control request for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if alreadyPending {
		c.logger.Info("cancel request already pending for apply owner",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"requested_by", requestedBy)
	} else {
		c.logger.Info("cancel request queued for apply owner",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"requested_by", requestedBy)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventCancelRequested, storage.LogSourceSchemaBot,
			fmt.Sprintf("Cancel request queued for apply owner%s", callerApplyLogSuffix(requestedBy)), "", "")
	}
	c.wakeOperator(apply)
	return &ternv1.CancelResponse{Accepted: true}, nil
}

func (c *LocalClient) requestStop(ctx context.Context, req *ternv1.StopRequest, caller string) (*ternv1.StopResponse, error) {
	c.logger.Info("Stop requested", "database", c.config.Database, "type", c.config.Type, "apply_id", req.ApplyId)
	if c.config.Type == storage.DatabaseTypeVitess {
		c.logger.Warn("stop rejected because this engine supports cancel instead", "database", c.config.Database, "type", c.config.Type, "apply_id", req.ApplyId)
		return nil, fmt.Errorf("stop not supported for this schema change; use cancel to permanently cancel it")
	}
	apply, err := c.resolveControlApply(ctx, req.ApplyId, "stop")
	if err != nil {
		return nil, err
	}
	if apply == nil {
		return nil, fmt.Errorf("no active schema change")
	}

	if revertPhase, err := c.applyRevertPhaseBlock(ctx, apply); err != nil {
		return nil, err
	} else if revertPhase != "" {
		c.logger.Warn("stop rejected: schema change is in a revert phase and has already cut over",
			"apply_id", apply.ApplyIdentifier,
			"state", apply.State,
			"revert_phase", revertPhase)
		return nil, errors.New(revertPhaseControlRejectionMessage(apply.ApplyIdentifier, revertPhase))
	}

	controlStore := c.storage.ControlRequests()
	if controlStore == nil {
		return nil, fmt.Errorf("control request store is not available")
	}
	requestedBy := controlRequestRequester(caller)
	_, alreadyPending, err := controlStore.RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: requestedBy,
	})
	if err != nil {
		return nil, fmt.Errorf("record stop control request for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if alreadyPending {
		c.logger.Info("stop request already pending for apply owner",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"requested_by", requestedBy)
	} else {
		c.logger.Info("stop request queued for apply owner",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"requested_by", requestedBy)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStopRequested, storage.LogSourceSchemaBot,
			fmt.Sprintf("Stop request queued for apply owner%s", callerApplyLogSuffix(requestedBy)), "", "")
	}
	c.wakeOperator(apply)
	return &ternv1.StopResponse{Accepted: true}, nil
}

func (c *LocalClient) stopOwnedApply(ctx context.Context, req *ternv1.StopRequest, caller string) (*ternv1.StopResponse, error) {
	c.logger.Info("Stop requested", "database", c.config.Database, "type", c.config.Type, "apply_id", req.ApplyId)
	if c.config.Type == storage.DatabaseTypeVitess {
		c.logger.Warn("stop rejected because this engine supports cancel instead", "database", c.config.Database, "type", c.config.Type, "apply_id", req.ApplyId)
		return nil, fmt.Errorf("stop not supported for this schema change; use cancel to permanently cancel it")
	}
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

	// A task in a revert phase has already cut over: the new schema is live (or
	// being unwound). Stop must not finalize it as cancelled — that would record
	// a deployed change as if nothing happened, or record an in-flight revert as
	// settled. Reject so the operator chooses revert or skip-revert, or waits
	// for the in-flight revert to finish.
	if revertTask := firstRevertPhaseTask(tasks, targetApplyID); revertTask != nil {
		applyIdentifier := c.resolveRevertPhaseApplyIdentifier(ctx, req, targetApply, revertTask)
		c.logger.Warn("stop rejected: schema change is in a revert phase and has already cut over",
			"apply_id", applyIdentifier, "task_id", revertTask.TaskIdentifier, "state", revertTask.State)
		return nil, errors.New(revertPhaseControlRejectionMessage(applyIdentifier, revertTask.State))
	}
	// Skip-revert finalization has no task marker (the tasks are already
	// terminal), so the resolved apply's own state is the only signal.
	if targetApply != nil && applyInRevertPhase(targetApply) {
		c.logger.Warn("stop rejected: schema change is in a revert phase and has already cut over",
			append(targetApply.LogAttrs(), "revert_phase", targetApply.State)...)
		return nil, errors.New(revertPhaseControlRejectionMessage(targetApply.ApplyIdentifier, targetApply.State))
	}

	eng := c.getEngine()
	applyCancel := c.currentApplyCancel()

	// Stop the engine first, THEN snapshot progress.
	// eng.Stop() blocks until Spirit's goroutine exits, so by the time it
	// returns the progress data reflects the true final state of each table.
	stopCreds, err := c.stopEngineForTasks(ctx, eng, tasks, targetApplyID)
	if err != nil {
		if engine.IsAlreadyCompleted(err) {
			skippedCount, settleErr := c.settleControlForCompletedEngineChange(ctx, "stop", tasks, targetApplyID, targetApply, caller, err)
			if settleErr != nil {
				return nil, settleErr
			}
			return &ternv1.StopResponse{
				Accepted:     true,
				SkippedCount: skippedCount,
				ErrorMessage: "Schema change already completed before the stop; apply recorded as completed",
			}, nil
		}
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
	if stopTerminatesChange(c.config.Type) {
		terminalState = state.Task.Cancelled
	} else {
		// Snapshot progress AFTER Spirit has fully stopped to preserve row copy progress.
		engineTableProgress = c.snapshotEngineProgress(ctx, eng, stopCreds)
	}

	stoppedCount, skippedCount, applyID := c.markTasksWithState(ctx, tasks, targetApplyID, engineTableProgress, terminalState)

	if applyID > 0 && stoppedCount > 0 {
		eventMsg := fmt.Sprintf("Stop requested: %d tasks stopped, %d skipped", stoppedCount, skippedCount)
		eventType := storage.LogEventStopRequested
		if terminalState == state.Task.Cancelled {
			eventMsg = fmt.Sprintf("Cancel requested: %d tasks cancelled, %d skipped (deploy request cancelled)", stoppedCount, skippedCount)
			eventType = storage.LogEventCancelRequested

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
		c.logApplyEvent(ctx, applyID, nil, storage.LogLevelInfo, eventType, storage.LogSourceSchemaBot,
			eventMsg, "", "")
	}

	if stoppedCount == 0 && skippedCount == 0 {
		// No apply was resolved and the database has no targetable tasks: there
		// is genuinely nothing to stop.
		if targetApply == nil {
			return nil, fmt.Errorf("no active schema change")
		}
		// A resolved apply with no targetable tasks is the task-less shape (a
		// queued apply stopped before its first drive created tasks). The apply
		// row itself is still active work, so settle it directly — erroring here
		// would leave the durable stop request pending and the apply re-claimed
		// on every operator poll forever.
		return c.settleStopForTasklessApply(ctx, targetApply, caller)
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

func (c *LocalClient) cancelOwnedApply(ctx context.Context, req *ternv1.CancelRequest, caller string) (*ternv1.CancelResponse, error) {
	c.logger.Info("Cancel requested", "database", c.config.Database, "type", c.config.Type, "apply_id", req.ApplyId)
	tasks, err := c.storage.Tasks().GetByDatabase(ctx, c.config.Database)
	if err != nil {
		return nil, fmt.Errorf("get tasks failed: %w", err)
	}

	var targetApplyID int64
	var targetApply *storage.Apply
	if req.ApplyId != "" {
		apply, err := c.storage.Applies().GetByApplyIdentifier(ctx, req.ApplyId)
		if err != nil {
			return nil, fmt.Errorf("load apply %s before cancel: %w", req.ApplyId, err)
		}
		if apply == nil {
			return nil, fmt.Errorf("load apply %s before cancel: %w", req.ApplyId, storage.ErrApplyNotFound)
		}
		targetApplyID = apply.ID
		targetApply = apply
	}

	if revertTask := firstRevertPhaseTask(tasks, targetApplyID); revertTask != nil {
		applyIdentifier := c.resolveRevertPhaseApplyIdentifier(ctx, &ternv1.StopRequest{ApplyId: req.ApplyId, Environment: req.Environment}, targetApply, revertTask)
		c.logger.Warn("cancel rejected: schema change is in a revert phase and has already cut over",
			append(revertTask.LogAttrs(), "apply_id", applyIdentifier)...)
		return nil, errors.New(revertPhaseControlRejectionMessage(applyIdentifier, revertTask.State))
	}
	// Skip-revert finalization has no task marker (the tasks are already
	// terminal), so the resolved apply's own state is the only signal.
	if targetApply != nil && applyInRevertPhase(targetApply) {
		c.logger.Warn("cancel rejected: schema change is in a revert phase and has already cut over",
			append(targetApply.LogAttrs(), "revert_phase", targetApply.State)...)
		return nil, errors.New(revertPhaseControlRejectionMessage(targetApply.ApplyIdentifier, targetApply.State))
	}

	eng := c.getEngine()
	applyCancel := c.currentApplyCancel()
	if err := c.cancelEngineForTasks(ctx, eng, tasks, targetApplyID); err != nil {
		if engine.IsAlreadyCompleted(err) {
			skippedCount, settleErr := c.settleControlForCompletedEngineChange(ctx, "cancel", tasks, targetApplyID, targetApply, caller, err)
			if settleErr != nil {
				return nil, settleErr
			}
			return &ternv1.CancelResponse{
				Accepted:     true,
				SkippedCount: skippedCount,
				ErrorMessage: "Schema change already completed before the cancel; apply recorded as completed",
			}, nil
		}
		return nil, fmt.Errorf("engine cancel failed: %w", err)
	}
	c.cancelApplyHandle(applyCancel)

	cancelledCount, skippedCount, applyID := c.markTasksWithState(ctx, tasks, targetApplyID, nil, state.Task.Cancelled)
	if applyID > 0 && cancelledCount > 0 {
		if err := c.markApplyCancelled(ctx, applyID); err != nil {
			return nil, err
		}
		eventMsg := fmt.Sprintf("Cancel requested: %d tasks cancelled, %d skipped", cancelledCount, skippedCount)
		if caller != "" {
			eventMsg += callerApplyLogSuffix(caller)
		}
		c.logApplyEvent(ctx, applyID, nil, storage.LogLevelInfo, storage.LogEventCancelRequested, storage.LogSourceSchemaBot,
			eventMsg, "", "")
	}
	if cancelledCount == 0 && skippedCount == 0 {
		// No apply was resolved and the database has no targetable tasks: there
		// is genuinely nothing to cancel.
		if targetApply == nil {
			return nil, fmt.Errorf("no active schema change")
		}
		// A resolved apply with no targetable tasks is the task-less shape (a
		// queued apply cancelled before its first drive created tasks, or a
		// VSchema-only apply that never has any). The apply row itself is still
		// active work, so settle it directly — erroring here would leave the
		// durable cancel request pending and the apply re-claimed on every
		// operator poll forever.
		return c.settleCancelForTasklessApply(ctx, targetApply, caller)
	}
	if cancelledCount == 0 && skippedCount > 0 && applyID > 0 {
		if targetApply != nil && state.IsState(targetApply.State, state.Apply.Cancelled) {
			return &ternv1.CancelResponse{Accepted: true, CancelledCount: 0, SkippedCount: skippedCount}, nil
		}
		if err := c.markApplyCancelled(ctx, applyID); err != nil {
			return nil, err
		}
	}
	return &ternv1.CancelResponse{
		Accepted:       true,
		CancelledCount: cancelledCount,
		SkippedCount:   skippedCount,
	}, nil
}

// resolveTasklessApplyForSettle reloads a control-targeted apply and confirms
// it truly owns no tasks, so a stop or cancel that found no task work can
// settle the apply row directly. A queued apply claimed before its first drive
// (and a VSchema-only apply, which never has tasks) is exactly this shape: the
// apply is active work with nothing for the engine or task paths to act on.
// Returns the freshly loaded apply. Fails closed — settling would abandon live
// work — when the apply cannot be reloaded or when it does own tasks that this
// client's database-scoped task read could not see.
func (c *LocalClient) resolveTasklessApplyForSettle(ctx context.Context, targetApply *storage.Apply, operation string) (*storage.Apply, error) {
	tasks, err := c.storage.Tasks().GetByApplyID(ctx, targetApply.ID)
	if err != nil {
		return nil, fmt.Errorf("load tasks for apply %s before task-less %s settle: %w", targetApply.ApplyIdentifier, operation, err)
	}
	if len(tasks) > 0 {
		c.logger.Warn(operation+" found no targetable tasks in this client's database scope, but the apply owns tasks; failing closed rather than settling over unseen task work",
			append(targetApply.LogAttrs(), "task_count", len(tasks), "client_database", c.config.Database)...)
		return nil, fmt.Errorf("%s cannot settle apply %s: its %d tasks are outside this client's database scope (%s)",
			operation, targetApply.ApplyIdentifier, len(tasks), c.config.Database)
	}
	apply, err := c.storage.Applies().Get(ctx, targetApply.ID)
	if err != nil {
		return nil, fmt.Errorf("reload apply %s before task-less %s settle: %w", targetApply.ApplyIdentifier, operation, err)
	}
	if apply == nil {
		return nil, fmt.Errorf("reload apply %s before task-less %s settle: %w", targetApply.ApplyIdentifier, operation, storage.ErrApplyNotFound)
	}
	return apply, nil
}

// settleCancelForTasklessApply terminalizes a cancel whose resolved apply owns
// no tasks. There is no task or engine work to cancel, but the apply row itself
// is still active work: it settles to cancelled through the existing
// markApplyCancelled path so the durable cancel request completes, the terminal
// observer is notified, and the operator never re-claims the apply — mirroring
// what the tasked cancel path does after markTasksWithState. Terminal states
// are never rewritten: an already-cancelled apply is accepted idempotently, and
// a completed/failed/reverted apply keeps its outcome (only a stopped apply,
// which cancel may still terminate, is moved to cancelled). The apply's
// dual-written apply_operations rows settle to cancelled alongside it, so
// operation-level claiming never sees a live operation under a cancelled apply.
func (c *LocalClient) settleCancelForTasklessApply(ctx context.Context, targetApply *storage.Apply, caller string) (*ternv1.CancelResponse, error) {
	apply, err := c.resolveTasklessApplyForSettle(ctx, targetApply, "cancel")
	if err != nil {
		return nil, err
	}
	if state.IsTerminalApplyState(apply.State) && !state.IsState(apply.State, state.Apply.Stopped) {
		c.logger.Info("cancel found task-less apply already terminal; accepting without a state change",
			append(apply.LogAttrs(), "requested_by", caller)...)
		return &ternv1.CancelResponse{Accepted: true}, nil
	}
	previousState := apply.State
	if err := c.markApplyCancelled(ctx, apply.ID); err != nil {
		if !errors.Is(err, storage.ErrApplyLeaseLost) {
			return nil, err
		}
		return c.deferTasklessCancelToProjection(ctx, apply, caller, err)
	}
	eventMsg := "Cancel requested: task-less apply cancelled before any task work started"
	if caller != "" {
		eventMsg += callerApplyLogSuffix(caller)
	}
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventCancelRequested, storage.LogSourceSchemaBot,
		eventMsg, previousState, state.Apply.Cancelled)
	now := time.Now()
	apply.State = state.Apply.Cancelled
	apply.CompletedAt = &now
	apply.UpdatedAt = now
	c.logger.Info("cancel settled task-less apply as cancelled",
		append(apply.LogAttrs(), "previous_state", previousState, "requested_by", caller)...)
	// The cancel is committed and authoritative, so it is still reported as
	// accepted. A row left behind belongs to a resolved apply, which no claim arm
	// reaches: it stays pending until an operator settles it, and nothing else
	// will.
	if err := c.settleOperationRowsForTasklessApply(ctx, apply, state.ApplyOperation.Cancelled); err != nil {
		c.logger.Error("cancel settled the apply but left operation rows unsettled; they stay pending under a cancelled apply until an operator settles them",
			append(apply.LogAttrs(), "target_operation_state", state.ApplyOperation.Cancelled, "error", err)...)
	}
	c.notifyTerminalObserver(apply, nil)
	return &ternv1.CancelResponse{Accepted: true}, nil
}

// deferTasklessCancelToProjection settles a task-less cancel whose apply-row
// write was refused because the apply lease was lost. Only the operation row is
// heartbeated through a drive, so a drive can outlive its parent apply lease
// while the operation lease stays live: the cancel is still honored by settling
// the drive's own leased operation row to cancelled and leaving the apply row
// to the operator's state projection, which derives cancelled from the terminal
// rows, completes the pending cancel request, and publishes the terminal
// summary. The terminal observer is not notified here — the apply is not yet
// terminal, and the projection owns the once-only summary.
func (c *LocalClient) deferTasklessCancelToProjection(ctx context.Context, apply *storage.Apply, caller string, applyWriteErr error) (*ternv1.CancelResponse, error) {
	if err := c.settleLeasedOperationRowForTasklessApply(ctx, apply, state.ApplyOperation.Cancelled, applyWriteErr); err != nil {
		return nil, err
	}
	eventMsg := "Cancel requested: task-less apply's operation row settled as cancelled; the apply record follows via the operator's state projection"
	if caller != "" {
		eventMsg += callerApplyLogSuffix(caller)
	}
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventCancelRequested, storage.LogSourceSchemaBot,
		eventMsg, "", "")
	c.logger.Warn("cancel settled the task-less apply's leased operation row but the apply lease was lost; the apply-state projection will settle the apply as cancelled and complete the pending cancel request",
		append(apply.LogAttrs(), "requested_by", caller, "error", applyWriteErr)...)
	metrics.RecordTasklessSettleDeferred(ctx, string(storage.ControlOperationCancel), apply.Database, apply.Deployment, apply.Environment)
	return &ternv1.CancelResponse{Accepted: true}, nil
}

// deferTasklessStopToProjection is the stop counterpart of
// deferTasklessCancelToProjection: the apply-row write was refused because the
// apply lease was lost, so the stop is honored by settling the drive's own
// leased operation row to stopped. A stopped row counts as settled for the
// operator's state projection, which derives the apply's stopped state from it,
// keeps completed_at nil (stopped is resumable), and completes the pending stop
// request; any pending sibling rows are swept by stop reconciliation.
func (c *LocalClient) deferTasklessStopToProjection(ctx context.Context, apply *storage.Apply, caller string, applyWriteErr error) (*ternv1.StopResponse, error) {
	if err := c.settleLeasedOperationRowForTasklessApply(ctx, apply, state.ApplyOperation.Stopped, applyWriteErr); err != nil {
		return nil, err
	}
	eventMsg := "Stop requested: task-less apply's operation row settled as stopped; the apply record follows via the operator's state projection"
	if caller != "" {
		eventMsg += callerApplyLogSuffix(caller)
	}
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStopRequested, storage.LogSourceSchemaBot,
		eventMsg, "", "")
	c.logger.Warn("stop settled the task-less apply's leased operation row but the apply lease was lost; the apply-state projection will settle the apply as stopped and complete the pending stop request",
		append(apply.LogAttrs(), "requested_by", caller, "error", applyWriteErr)...)
	metrics.RecordTasklessSettleDeferred(ctx, string(storage.ControlOperationStop), apply.Database, apply.Deployment, apply.Environment)
	return &ternv1.StopResponse{Accepted: true}, nil
}

// settleLeasedOperationRowForTasklessApply moves the drive's own leased
// operation row to the task-less settle's target state after the apply-row
// write lost the apply lease. The operation lease is the drive's surviving
// write capability, and it authorizes exactly one row — the leased one — so
// sibling rows are intentionally left alone: pending siblings are settled by
// their own drives (cancel) or by stop reconciliation (stop). Fails closed when
// no operation lease is held, when the leased row cannot be loaded, or when it
// belongs to a different apply, so a lost claim never settles someone else's
// work. An already-terminal row is left unchanged under the same discipline as
// settleOperationRowsForTasklessApply.
func (c *LocalClient) settleLeasedOperationRowForTasklessApply(ctx context.Context, apply *storage.Apply, operationState string, applyWriteErr error) error {
	opLease, ok := storage.OperationLeaseFromContext(ctx)
	if !ok {
		return fmt.Errorf("task-less settle of apply %s to %s: apply lease was lost and no operation lease is held: %w",
			apply.ApplyIdentifier, operationState, applyWriteErr)
	}
	op, err := c.storage.ApplyOperations().Get(ctx, opLease.OperationID)
	if err != nil {
		return fmt.Errorf("load leased apply_operation %d for task-less settle of apply %s to %s: %w",
			opLease.OperationID, apply.ApplyIdentifier, operationState, err)
	}
	if op == nil {
		return fmt.Errorf("load leased apply_operation %d for task-less settle of apply %s to %s: %w",
			opLease.OperationID, apply.ApplyIdentifier, operationState, storage.ErrApplyOperationNotFound)
	}
	if op.ApplyID != apply.ID {
		return fmt.Errorf("leased apply_operation %d belongs to a different apply than %s; refusing task-less settle to %s",
			op.ID, apply.ApplyIdentifier, operationState)
	}
	if !operationRowSettlesWithTasklessApply(op.State, operationState) {
		c.logger.Info("task-less settle leaving terminal leased operation row unchanged",
			append(apply.LogAttrs(),
				"apply_operation_id", op.ID,
				"operation_deployment", op.Deployment,
				"operation_state", op.State,
				"target_operation_state", operationState)...)
		return nil
	}
	var writeErr error
	if state.IsState(operationState, state.ApplyOperation.Stopped) {
		// stopped is resumable: mirror the state but keep completed_at nil,
		// matching the apply-level convention.
		writeErr = c.storage.ApplyOperations().UpdateState(ctx, op.ID, operationState)
	} else {
		writeErr = c.storage.ApplyOperations().MarkTerminal(ctx, op.ID, operationState)
	}
	if writeErr != nil {
		return fmt.Errorf("move leased apply_operation %d (%s) from %s to %s for task-less settle of apply %s: %w",
			op.ID, op.Deployment, op.State, operationState, apply.ApplyIdentifier, writeErr)
	}
	c.logger.Info("task-less settle moved the leased operation row; the apply-state projection settles the apply row",
		append(apply.LogAttrs(),
			"apply_operation_id", op.ID,
			"operation_deployment", op.Deployment,
			"previous_operation_state", op.State,
			"operation_state", operationState)...)
	return nil
}

// settleOperationRowsForTasklessApply moves the settled apply's dual-written
// apply_operations rows to the apply's settled state so the operation rows
// agree with the apply row the task-less settle just wrote. The settle runs
// inside a claimed drive, so the lease in ctx guards each write and a lost
// claim fails closed in storage.
//
// Every row is attempted before returning, so one failing write does not strand
// the rest, and the failures are returned joined. The caller decides what an
// unsettled row means: the apply-level settle already committed and is
// authoritative, but what happens to the row afterwards depends on which state
// the apply settled to, so the caller — not this function — owns that story.
func (c *LocalClient) settleOperationRowsForTasklessApply(ctx context.Context, apply *storage.Apply, operationState string) error {
	ops, err := c.storage.ApplyOperations().ListByApply(ctx, apply.ID)
	if err != nil {
		return fmt.Errorf("load operation rows for task-less settle of apply %s to %s: %w", apply.ApplyIdentifier, operationState, err)
	}
	var settleErrs []error
	for _, op := range ops {
		if !operationRowSettlesWithTasklessApply(op.State, operationState) {
			c.logger.Info("task-less settle leaving terminal operation row unchanged",
				append(apply.LogAttrs(),
					"apply_operation_id", op.ID,
					"operation_deployment", op.Deployment,
					"operation_state", op.State,
					"target_operation_state", operationState)...)
			continue
		}
		var writeErr error
		if state.IsState(operationState, state.ApplyOperation.Stopped) {
			// stopped is resumable: mirror the state but keep completed_at nil,
			// matching the apply-level convention.
			writeErr = c.storage.ApplyOperations().UpdateState(ctx, op.ID, operationState)
		} else {
			writeErr = c.storage.ApplyOperations().MarkTerminal(ctx, op.ID, operationState)
		}
		if writeErr != nil {
			settleErrs = append(settleErrs, fmt.Errorf("move apply_operation %d (%s) from %s to %s: %w",
				op.ID, op.Deployment, op.State, operationState, writeErr))
			continue
		}
		c.logger.Info("task-less settle moved operation row to the apply's settled state",
			append(apply.LogAttrs(),
				"apply_operation_id", op.ID,
				"operation_deployment", op.Deployment,
				"previous_operation_state", op.State,
				"operation_state", operationState)...)
	}
	return errors.Join(settleErrs...)
}

// operationRowSettlesWithTasklessApply reports whether a task-less settle may
// move an operation row from its current state to the apply's settled state.
// It mirrors the apply-level settle's terminal discipline: a terminal row keeps
// its outcome, except that a cancel settle still terminalizes a stopped row —
// stopped is the one terminal state cancel may move, exactly as the apply-level
// cancel settle moves a stopped apply to cancelled.
func operationRowSettlesWithTasklessApply(currentState, settledState string) bool {
	if !state.IsApplyOperationTerminal(currentState) {
		return true
	}
	return state.IsState(settledState, state.ApplyOperation.Cancelled) &&
		state.IsState(currentState, state.ApplyOperation.Stopped)
}

// settleStopForTasklessApply terminalizes a stop whose resolved apply owns no
// tasks. There is no task or engine work to stop, but the apply row itself is
// still active work: it settles to stopped through the existing
// markApplyStopped path so the durable stop request completes, the terminal
// observer is notified, and the operator never re-claims the apply — mirroring
// what the tasked stop path does after markTasksWithState. The apply settles to
// stopped (not cancelled): stop is the resumable operator verb, and a stopped
// task-less apply stays eligible for a later cancel. Terminal states are never
// rewritten: an already-terminal apply (stopped included) is accepted without a
// state change so the pending stop request resolves. The apply's dual-written
// apply_operations rows settle to stopped alongside it, keeping the resumable
// operation rows consistent with the stopped apply.
func (c *LocalClient) settleStopForTasklessApply(ctx context.Context, targetApply *storage.Apply, caller string) (*ternv1.StopResponse, error) {
	apply, err := c.resolveTasklessApplyForSettle(ctx, targetApply, "stop")
	if err != nil {
		return nil, err
	}
	if state.IsTerminalApplyState(apply.State) {
		c.logger.Info("stop found task-less apply already terminal; accepting without a state change",
			append(apply.LogAttrs(), "requested_by", caller)...)
		return &ternv1.StopResponse{
			Accepted:     true,
			ErrorMessage: fmt.Sprintf("Schema change already %s", apply.State),
		}, nil
	}
	previousState := apply.State
	if err := c.markApplyStopped(ctx, apply.ID); err != nil {
		if !errors.Is(err, storage.ErrApplyLeaseLost) {
			return nil, err
		}
		return c.deferTasklessStopToProjection(ctx, apply, caller, err)
	}
	eventMsg := "Stop requested: task-less apply stopped before any task work started"
	if caller != "" {
		eventMsg += callerApplyLogSuffix(caller)
	}
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStopRequested, storage.LogSourceSchemaBot,
		eventMsg, previousState, state.Apply.Stopped)
	apply.State = state.Apply.Stopped
	apply.CompletedAt = nil
	apply.UpdatedAt = time.Now()
	c.logger.Info("stop settled task-less apply as stopped",
		append(apply.LogAttrs(), "previous_state", previousState, "requested_by", caller)...)
	// The stop is committed and authoritative, so it is still reported as
	// accepted. A row left behind still resolves on its own: a stopped apply is
	// resumable, so its pending rows stay claimable and the next claim mirrors
	// the parent's stopped state onto them.
	if err := c.settleOperationRowsForTasklessApply(ctx, apply, state.ApplyOperation.Stopped); err != nil {
		c.logger.Error("stop settled the apply but left operation rows unsettled; the next operation claim mirrors the stopped parent onto them",
			append(apply.LogAttrs(), "target_operation_state", state.ApplyOperation.Stopped, "error", err)...)
	}
	c.notifyTerminalObserver(apply, nil)
	return &ternv1.StopResponse{Accepted: true}, nil
}

// stopHandledUnlessStartPending reports a completed stop as handled unless a
// start request is also pending, in which case it returns not-handled so the
// caller resumes the apply from the queued start in the same claim. Without
// this, a stop and a start that race into the same claim would consume only the
// stop, leaving the apply stopped with a pending start that the claim
// lease-freshness gate cannot re-claim until the lease goes stale.
func (c *LocalClient) stopHandledUnlessStartPending(ctx context.Context, logger *slog.Logger, apply *storage.Apply) (bool, error) {
	pendingStart, err := pendingStartControlRequest(ctx, c.storage, apply)
	if err != nil {
		return true, fmt.Errorf("check pending start request after stop for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if pendingStart != nil {
		logger.Info("pending stop completed but a start is queued; continuing to resume in the same claim",
			"state", apply.State)
		return false, nil
	}
	return true, nil
}

// failPendingRequestForUnsupportedOperation resolves a pending control request
// terminally when the engine declined the operation as unsupported for its
// database type. The decline is deterministic — no retry can ever succeed —
// so leaving the request pending would re-run the same rejection on every
// drive claim while the schema change keeps executing unwatched. The request
// is failed with the engine's reason, and the apply is left untouched: the
// running change settles itself through its own apply path. Returns
// handled=false when the error is not an unsupported-operation decline, so the
// caller applies its normal error handling.
func (c *LocalClient) failPendingRequestForUnsupportedOperation(ctx context.Context, logger *slog.Logger, apply *storage.Apply, operation storage.ControlOperation, eventType string, controlReq *storage.ApplyControlRequest, opErr error) (bool, error) {
	var unsupported *engine.UnsupportedOperationError
	if !errors.As(opErr, &unsupported) {
		return false, nil
	}
	message := unsupported.Error()
	caller := controlRequestCaller(controlReq)
	logger.Warn("rejecting pending control request: the engine does not support the operation; the schema change continues and settles on its own",
		"operation", string(operation),
		"requested_by", caller,
		"state", apply.State,
		"error", opErr)
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, eventType, storage.LogSourceSchemaBot,
		fmt.Sprintf("Pending %s request rejected: %s%s", operation, message, callerApplyLogSuffix(caller)), "", "")
	if err := failPendingControlRequests(ctx, c.storage, apply, operation, message); err != nil {
		return true, err
	}
	engineName := "unknown"
	if eng := c.getEngine(); eng != nil {
		engineName = eng.Name()
	}
	metrics.RecordControlRequestUnsupportedDecline(ctx, string(operation), engineName, apply.Database, apply.Deployment, apply.Environment)
	return true, nil
}

func (c *LocalClient) processPendingStopControlRequest(ctx context.Context, apply *storage.Apply) (bool, error) {
	controlReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationStop)
	if err != nil {
		return false, err
	}
	if controlReq == nil {
		return false, nil
	}
	// Bind the apply's identity once so every consumption log line is
	// filterable by apply_id/repo/pr without hand-listing the attrs per call.
	logger := c.logger.With(apply.IdentityLogAttrs()...)
	if completed, err := completePendingRequestIfStoredApplyResolved(ctx, c.storage, apply, storage.ControlOperationStop); err != nil {
		return true, err
	} else if completed {
		logger.Info("completing pending stop request for resolved apply",
			"requested_by", controlRequestCaller(controlReq),
			"state", apply.State)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStopRequested, storage.LogSourceSchemaBot,
			fmt.Sprintf("Pending stop request completed for resolved apply%s", callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
		return c.stopHandledUnlessStartPending(ctx, logger, apply)
	}
	if state.IsTerminalApplyState(apply.State) {
		logger.Info("completing pending stop request for terminal apply",
			"requested_by", controlRequestCaller(controlReq),
			"state", apply.State)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStopRequested, storage.LogSourceSchemaBot,
			fmt.Sprintf("Pending stop request completed for terminal apply%s", callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
		if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStop); err != nil {
			return true, err
		}
		return c.stopHandledUnlessStartPending(ctx, logger, apply)
	}

	// A revert-phase apply has already cut over. Stop is a permanent rejection,
	// not a retryable error: failing the durable request resolves it terminally
	// so the operator-owned retry loop stops re-running stop. The operator must
	// revert (undo) or skip-revert (finalize), or wait out an in-flight revert.
	if revertPhase, err := c.applyRevertPhaseBlock(ctx, apply); err != nil {
		return true, err
	} else if revertPhase != "" {
		message := revertPhaseControlRejectionMessage(apply.ApplyIdentifier, revertPhase)
		logger.Warn("rejecting pending stop request: schema change is in a revert phase and has already cut over",
			"requested_by", controlRequestCaller(controlReq),
			"state", apply.State,
			"revert_phase", revertPhase)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventStopRequested, storage.LogSourceSchemaBot,
			fmt.Sprintf("Pending stop request rejected: %s%s", message, callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
		if err := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStop, message); err != nil {
			return true, err
		}
		return true, nil
	}

	stopCtx := context.WithoutCancel(ctx)
	resp, err := c.stopOwnedApply(stopCtx, &ternv1.StopRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: apply.Environment,
	}, controlRequestCaller(controlReq))
	if err != nil {
		if handled, declineErr := c.failPendingRequestForUnsupportedOperation(stopCtx, logger, apply, storage.ControlOperationStop, storage.LogEventStopRequested, controlReq, err); handled {
			return true, declineErr
		}
		return true, fmt.Errorf("process pending stop for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if resp == nil || !resp.Accepted {
		errorMessage := "not accepted"
		if resp != nil && resp.ErrorMessage != "" {
			errorMessage = resp.ErrorMessage
		}
		return true, fmt.Errorf("process pending stop for apply %s: %s", apply.ApplyIdentifier, errorMessage)
	}
	completed, err := completePendingRequestIfStoredApplyResolved(stopCtx, c.storage, apply, storage.ControlOperationStop)
	if err != nil {
		return true, err
	}
	if !completed {
		// The stop was accepted but the apply row has not settled — the settle
		// lost the apply lease and moved only the drive's own operation row, so
		// the apply-state projection owns the rest. The request stays pending on
		// purpose: it keeps unstarted sibling rows gated off, and the projection
		// (or stop reconciliation, for pending siblings) completes it once the
		// stored apply resolves.
		logger.Warn("pending stop consumed but the stored apply has not settled; leaving the request pending for the apply-state projection to complete",
			"requested_by", controlRequestCaller(controlReq),
			"state", apply.State)
		return true, nil
	}
	return true, nil
}

func (c *LocalClient) processPendingCancelControlRequest(ctx context.Context, apply *storage.Apply) (bool, error) {
	controlReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationCancel)
	if err != nil {
		return false, err
	}
	if controlReq == nil {
		return false, nil
	}
	// Bind the apply's identity once so every consumption log line is
	// filterable by apply_id/repo/pr without hand-listing the attrs per call.
	logger := c.logger.With(apply.IdentityLogAttrs()...)
	if state.IsTerminalApplyState(apply.State) && !state.IsState(apply.State, state.Apply.Stopped) {
		logger.Info("completing pending cancel request for terminal apply",
			append(apply.MutableLogAttrs(), "requested_by", controlRequestCaller(controlReq))...)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventCancelRequested, storage.LogSourceSchemaBot,
			fmt.Sprintf("Pending cancel request completed for terminal apply%s", callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
		if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCancel); err != nil {
			return true, err
		}
		return true, nil
	}
	if revertPhase, err := c.applyRevertPhaseBlock(ctx, apply); err != nil {
		return true, err
	} else if revertPhase != "" {
		message := revertPhaseControlRejectionMessage(apply.ApplyIdentifier, revertPhase)
		logger.Warn("rejecting pending cancel request: schema change is in a revert phase and has already cut over",
			append(apply.MutableLogAttrs(), "requested_by", controlRequestCaller(controlReq), "revert_phase", revertPhase)...)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventCancelRequested, storage.LogSourceSchemaBot,
			fmt.Sprintf("Pending cancel request rejected: %s%s", message, callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
		if err := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCancel, message); err != nil {
			return true, err
		}
		return true, nil
	}

	cancelCtx := context.WithoutCancel(ctx)
	resp, err := c.cancelOwnedApply(cancelCtx, &ternv1.CancelRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: apply.Environment,
	}, controlRequestCaller(controlReq))
	if err != nil {
		if handled, declineErr := c.failPendingRequestForUnsupportedOperation(cancelCtx, logger, apply, storage.ControlOperationCancel, storage.LogEventCancelRequested, controlReq, err); handled {
			return true, declineErr
		}
		return true, fmt.Errorf("process pending cancel for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if resp == nil || !resp.Accepted {
		errorMessage := "not accepted"
		if resp != nil && resp.ErrorMessage != "" {
			errorMessage = resp.ErrorMessage
		}
		return true, fmt.Errorf("process pending cancel for apply %s: %s", apply.ApplyIdentifier, errorMessage)
	}
	completed, err := completePendingRequestIfStoredApplyResolved(cancelCtx, c.storage, apply, storage.ControlOperationCancel)
	if err != nil {
		return true, err
	}
	if !completed {
		// The cancel was accepted but the apply row has not settled — the settle
		// lost the apply lease and moved only the drive's own operation row, so
		// the apply-state projection owns the rest. The request stays pending on
		// purpose: sibling rows' own drives consume it for their share of the
		// apply, and the projection completes it once the stored apply resolves.
		logger.Warn("pending cancel consumed but the stored apply has not settled; leaving the request pending for the apply-state projection to complete",
			"requested_by", controlRequestCaller(controlReq),
			"state", apply.State)
		return true, nil
	}
	return true, nil
}

func (c *LocalClient) processPendingCancelOrStopControlRequest(ctx context.Context, apply *storage.Apply) (bool, error) {
	if handled, err := c.processPendingCancelControlRequest(ctx, apply); handled || err != nil {
		return handled, err
	}
	return c.processPendingStopControlRequest(ctx, apply)
}

// firstRevertPhaseTask returns the first targeted task that is in a revert
// phase (revert window held open, or a revert in flight), or nil if none are. A
// revert-phase task has already cut over, so the stop and cancel paths must
// reject rather than treat it as a cancellable in-flight change: a revert-window
// task needs the operator to choose revert or skip-revert, and a reverting task
// has the engine unwinding the change — cancelling the deploy request there
// would interrupt the revert at an arbitrary point.
//
// When targetApplyID is 0 (an untargeted stop with no apply id), any
// revert-phase task on the database rejects the whole stop, even one belonging
// to a different apply. This is bounded by the one-active-apply-per-target
// invariant: storage permits at most one active apply per (database, database
// type, environment), and LocalClient is scoped to a single such target, so a
// revert-phase task from a second, distinct apply cannot coexist with another
// active apply on the same target. Cross-apply coexistence is therefore not a
// case this scope has to disambiguate.
func firstRevertPhaseTask(tasks []*storage.Task, targetApplyID int64) *storage.Task {
	for _, task := range tasks {
		if targetApplyID > 0 && task.ApplyID != targetApplyID {
			continue
		}
		if state.IsState(task.State, state.Task.RevertWindow, state.Task.Reverting) {
			return task
		}
	}
	return nil
}

// revertPhaseControlRejectionMessage is the operator-facing reason a stop or
// cancel targeting a revert-phase schema change is permanently rejected. The
// change has already cut over, so the operator's options depend on the phase:
// in the revert window they must choose revert or skip-revert; once a revert or
// skip-revert is underway the engine finishes it on its own and no control
// operation can meaningfully interrupt it.
func revertPhaseControlRejectionMessage(applyIdentifier, phase string) string {
	switch {
	case state.IsState(phase, state.Apply.Reverting):
		return fmt.Sprintf("schema change %s is being reverted and has already been applied: the revert is in progress and will complete on its own", applyIdentifier)
	case state.IsState(phase, state.Apply.SkippingRevert):
		return fmt.Sprintf("schema change %s is finalizing skip-revert and has already been applied: skip-revert is in progress and will complete on its own", applyIdentifier)
	default:
		return fmt.Sprintf("schema change %s is in the revert window and has already been applied: use revert to undo it or skip-revert to finalize it", applyIdentifier)
	}
}

// resolveRevertPhaseApplyIdentifier returns the apply-level identifier an
// operator supplied or recognizes, not the per-table task identifier. It prefers
// the requested apply id, then the resolved target apply, then a lookup of the
// revert task's apply, falling back to the task identifier only if the apply
// cannot be loaded.
func (c *LocalClient) resolveRevertPhaseApplyIdentifier(ctx context.Context, req *ternv1.StopRequest, targetApply *storage.Apply, revertTask *storage.Task) string {
	if req.ApplyId != "" {
		return req.ApplyId
	}
	if targetApply != nil && targetApply.ApplyIdentifier != "" {
		return targetApply.ApplyIdentifier
	}
	apply, err := c.storage.Applies().Get(ctx, revertTask.ApplyID)
	if err != nil {
		c.logger.Warn("could not load apply to resolve revert-phase rejection identifier; using task identifier",
			"task_id", revertTask.TaskIdentifier, "error", err)
		return revertTask.TaskIdentifier
	}
	if apply == nil {
		c.logger.Warn("apply not found while resolving revert-phase rejection identifier; using task identifier",
			"task_id", revertTask.TaskIdentifier)
		return revertTask.TaskIdentifier
	}
	return apply.ApplyIdentifier
}

// applyRevertPhaseBlock returns the revert-phase state that blocks a stop or
// cancel of this apply, or "" when the apply is not in a revert phase. The
// apply state covers phases with no task marker (skip-revert finalization, and
// task-less applies such as VSchema-only changes); the stored tasks are read
// directly so the durable control path detects the same cut-over condition the
// synchronous path rejects on even when the apply row lags the task states.
func (c *LocalClient) applyRevertPhaseBlock(ctx context.Context, apply *storage.Apply) (string, error) {
	if applyInRevertPhase(apply) {
		return apply.State, nil
	}
	tasks, err := c.storage.Tasks().GetByApplyID(ctx, apply.ID)
	if err != nil {
		return "", fmt.Errorf("load tasks for apply %s to detect a revert phase before stop or cancel: %w", apply.ApplyIdentifier, err)
	}
	if revertTask := firstRevertPhaseTask(tasks, apply.ID); revertTask != nil {
		return revertTask.State, nil
	}
	return "", nil
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
//
// Reverting is deliberately absent even though the engine is actively unwinding
// the change: the control gates permanently reject stop/cancel for revert-phase
// tasks before any engine sweep runs, because cancelling a deploy request
// mid-revert would interrupt the unwind at an arbitrary point. The revert owns
// the terminal outcome.
func hasLiveEngineWork(taskState string) bool {
	return state.IsState(taskState,
		state.Task.Running,
		state.Task.CatchingUp,
		state.Task.Checksumming,
		state.Task.PostChecksum,
		state.Task.WaitingForCutover,
		state.Task.CuttingOver,
		state.Task.Recovering,
		state.Task.WaitingForDeploy,
		state.Task.FailedRetryable)
}

// stopEngineForTasks calls eng.Stop() if any targeted task has live engine work.
// Returns an error if the engine stop fails. Storage must not record stopped
// state until the apply owner has stopped the live engine work.
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
			return nil, fmt.Errorf("stop local engine for task %s: %w", task.TaskIdentifier, err)
		}
		return creds, nil
	}
	c.logger.Debug("no targeted task has live engine work to stop", "database", c.config.Database, "type", c.config.Type, "target_apply_id", targetApplyID)
	return nil, nil
}

func (c *LocalClient) cancelEngineForTasks(ctx context.Context, eng engine.Engine, tasks []*storage.Task, targetApplyID int64) error {
	if eng == nil {
		c.logger.Error("cancelEngineForTasks: engine is nil")
		return fmt.Errorf("no engine configured for type: %s", c.config.Type)
	}
	for _, task := range tasks {
		if targetApplyID > 0 && task.ApplyID != targetApplyID {
			continue
		}
		if state.IsTerminalTaskState(task.State) {
			c.logger.Info("skipping terminal task in cancel", task.LogAttrs()...)
			continue
		}
		if !hasLiveEngineWork(task.State) {
			c.logger.Debug("skipping engine cancel for task with no live engine work",
				task.LogAttrs()...)
			continue
		}
		creds, err := c.credentialsForTask(task)
		if err != nil {
			return fmt.Errorf("resolve credentials for cancel task %s: %w", task.TaskIdentifier, err)
		}
		req, err := c.buildControlRequest(ctx, task, creds, eng, engine.ControlCancel)
		if err != nil {
			return fmt.Errorf("build cancel request for task %s: %w", task.TaskIdentifier, err)
		}
		if _, err := eng.Cancel(ctx, req); err != nil {
			return fmt.Errorf("cancel engine for task %s: %w", task.TaskIdentifier, err)
		}
		return nil
	}
	c.logger.Debug("no targeted task has live engine work to cancel", "database", c.config.Database, "type", c.config.Type, "target_apply_id", targetApplyID)
	return nil
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
			task.ChecksumRowsChecked = et.ChecksumRowsChecked
			task.ChecksumRowsTotal = et.ChecksumRowsTotal
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

// ensureApplyFailureMessage derives the apply's failure reason from the failed
// task rows when the apply has resolved to a failure state but still carries no
// message. Under on_failure=continue the rollout projection can resolve the
// apply to failed/failed_retryable because of a sibling operation while the
// finishing operation's own engine result is non-failed, so the per-operation
// engine message is not always available. An ErrorMessage already on the apply
// is authoritative and left untouched.
func ensureApplyFailureMessage(apply *storage.Apply, tasks []*storage.Task) {
	if apply.ErrorMessage != "" {
		return
	}
	if !state.IsState(apply.State, state.Apply.Failed) && !state.IsState(apply.State, state.Apply.FailedRetryable) {
		return
	}
	if msg := firstFailedTaskError(tasks); msg != "" {
		apply.ErrorMessage = msg
	}
}

// handleStopAllTasksTerminal handles the edge case where stop is requested but
// every targeted task is already in a terminal state (completed, failed,
// cancelled, or reverted). The apply row may still be non-terminal — e.g., a
// driver exited after finalizing task rows but before the apply row — so the
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
	// A revert-phase apply has cut over and its outcome is owned by the engine's
	// revert or skip-revert; recording it cancelled would settle storage while
	// the provider keeps working. The control gates reject stop/cancel before
	// reaching here — this backstop keeps any other path fail-closed.
	if applyInRevertPhase(apply) {
		c.logger.Warn("refusing to mark revert-phase apply cancelled; the in-flight revert phase owns the outcome",
			apply.LogAttrs()...)
		return fmt.Errorf("mark apply %s cancelled: %s", apply.ApplyIdentifier, revertPhaseControlRejectionMessage(apply.ApplyIdentifier, apply.State))
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

// settleControlForCompletedEngineChange terminalizes a stop or cancel that the
// engine rejected because the schema change had already completed on its
// backend before the operation arrived. The backend's terminal outcome is
// authoritative: the change is live on the target, so the stored apply and its
// still-active tasks settle to completed — recording them stopped or cancelled
// would misrepresent a schema change that actually landed — and the accepted
// settle lets the durable control request resolve, so the operator's claim
// loop stops re-running an operation the engine will reject forever. Returns
// the number of targeted tasks that were already terminal. An apply already in
// a non-resumable terminal state keeps its outcome. operation names the
// rejected verb ("stop" or "cancel") for logs and the apply event.
func (c *LocalClient) settleControlForCompletedEngineChange(ctx context.Context, operation string, tasks []*storage.Task, targetApplyID int64, targetApply *storage.Apply, caller string, rejection error) (int64, error) {
	now := time.Now()
	var completedCount, skippedCount, applyID int64
	if targetApply != nil {
		applyID = targetApply.ID
	}
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
		task.ProgressPercent = 100
		task.CompletedAt = &now
		c.transitionTaskState(ctx, task, task.ApplyID, state.Task.Completed,
			fmt.Sprintf("Task %s completed on the engine before the %s took effect", task.TaskIdentifier, operation))
		completedCount++
	}
	if applyID == 0 {
		return 0, fmt.Errorf("%s found the schema change already completed on the engine, but resolved no apply to settle: %w", operation, rejection)
	}
	apply, err := c.storage.Applies().Get(ctx, applyID)
	if err != nil {
		return 0, fmt.Errorf("load apply %d to settle %s for a completed schema change: %w", applyID, operation, err)
	}
	if apply == nil {
		return 0, fmt.Errorf("load apply %d to settle %s for a completed schema change: %w", applyID, operation, storage.ErrApplyNotFound)
	}
	if state.IsTerminalApplyState(apply.State) && !state.IsState(apply.State, state.Apply.Stopped) {
		c.logger.Info(operation+" found the schema change already completed on the engine and the apply already terminal; accepting without a state change",
			append(apply.LogAttrs(), "requested_by", caller)...)
		return skippedCount, nil
	}
	previousState := apply.State
	apply.State = state.Apply.Completed
	apply.CompletedAt = &now
	apply.UpdatedAt = now
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		return 0, fmt.Errorf("mark apply %s completed after %s found the schema change already completed on the engine: %w", apply.ApplyIdentifier, operation, err)
	}
	c.logger.Warn("engine rejected "+operation+" because the schema change already completed; apply settled as completed",
		append(apply.LogAttrs(), "requested_by", caller, "previous_state", previousState, "error", rejection)...)
	eventMsg := fmt.Sprintf("%s arrived after the schema change completed on the engine; apply recorded as completed (%d tasks completed, %d already terminal)",
		capitalizeControlVerb(operation), completedCount, skippedCount)
	if caller != "" {
		eventMsg += callerApplyLogSuffix(caller)
	}
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
		eventMsg, previousState, state.Apply.Completed)
	c.notifyTerminalObserver(apply, tasks)
	return skippedCount, nil
}

// capitalizeControlVerb upper-cases the first letter of a control verb for use
// at the start of an operator-facing apply event message.
func capitalizeControlVerb(operation string) string {
	if operation == "" {
		return operation
	}
	return strings.ToUpper(operation[:1]) + operation[1:]
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

// Volume adjusts the speed/concurrency of an in-flight schema change by
// queueing a durable volume request. Only the instance driving the apply holds
// the engine state for the running schema change, and a volume RPC can land on
// any instance sharing this route's storage — so the request is recorded for
// the driver, which retunes the engine at its next progress tick. The response
// reports the queued target level; the previous level is only known to the
// driver, so PreviousVolume is left unset.
func (c *LocalClient) Volume(ctx context.Context, req *ternv1.VolumeRequest) (*ternv1.VolumeResponse, error) {
	if req.Volume < storage.MinVolume || req.Volume > storage.MaxVolume {
		return nil, fmt.Errorf("volume must be between %d and %d", storage.MinVolume, storage.MaxVolume)
	}
	c.logger.Info("Volume requested", "database", c.config.Database, "type", c.config.Type, "apply_id", req.ApplyId, "environment", req.Environment, "volume", req.Volume)
	apply, err := c.resolveControlApply(ctx, req.ApplyId, "volume")
	if err != nil {
		return nil, err
	}
	if apply == nil {
		return nil, fmt.Errorf("no active schema change")
	}

	// A terminal apply has no running copy to retune; reject synchronously
	// rather than queue a request no driver will ever service. This includes
	// stopped applies — resume the schema change first, then adjust volume.
	// Rejections travel as unaccepted responses (not errors) so callers across
	// the gRPC boundary can distinguish them from transport or storage failures.
	if state.IsTerminalApplyState(apply.State) {
		c.logger.Warn("volume rejected: schema change is not active",
			"apply_id", apply.ApplyIdentifier, "state", apply.State, "volume", req.Volume)
		return &ternv1.VolumeResponse{
			Accepted:     false,
			ErrorMessage: fmt.Sprintf("Schema change %s is %s; volume can only be adjusted while it is running", apply.ApplyIdentifier, apply.State),
		}, nil
	}

	// A volume request is apply-scoped, but a multi-deployment (fan-out) apply
	// runs one engine per operation: whichever operation driver ticked first
	// would retune only its own deployment's schema change and complete the
	// request, leaving sibling deployments copying at the old level while the
	// request row, progress, and the CLI all report the adjustment as applied.
	// Reject the shape outright until volume requests are scoped per operation.
	// Operation rows are created at dispatch and never grow while an apply
	// runs, so this queue-time check cannot be raced by fan-out.
	operations, err := c.storage.ApplyOperations().ListByApply(ctx, apply.ID)
	if err != nil {
		return nil, fmt.Errorf("load operations for apply %s to gate volume request: %w", apply.ApplyIdentifier, err)
	}
	if len(operations) > 1 {
		c.logger.Warn("volume rejected: apply fans out to multiple deployments",
			"apply_id", apply.ApplyIdentifier, "operation_count", len(operations), "volume", req.Volume)
		return &ternv1.VolumeResponse{
			Accepted:     false,
			ErrorMessage: fmt.Sprintf("Apply %s fans out to %d deployments; volume adjustment is not supported for multi-deployment applies", apply.ApplyIdentifier, len(operations)),
		}, nil
	}

	return c.queueVolumeRequest(ctx, apply, req.Volume, req.Caller)
}

// queueVolumeRequest records a durable volume control request carrying the
// desired level in its metadata and wakes the operator. A pending volume
// request is immutable until the driver resolves it: a second request for the
// same level is an idempotent accept, while a different level is rejected with
// retry guidance. This keeps the level the driver reads, applies, and completes
// unambiguous — there is no window where a superseding write could make the
// driver complete a level it never applied.
func (c *LocalClient) queueVolumeRequest(ctx context.Context, apply *storage.Apply, volume int32, caller string) (*ternv1.VolumeResponse, error) {
	controlStore := c.storage.ControlRequests()
	if controlStore == nil {
		return nil, fmt.Errorf("control request store is not available")
	}
	metadata, err := storage.EncodeVolumeControlRequestMetadata(volume)
	if err != nil {
		return nil, fmt.Errorf("encode volume request for apply %s: %w", apply.ApplyIdentifier, err)
	}
	requestedBy := controlRequestRequester(caller)
	controlReq, alreadyPending, err := controlStore.RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationVolume,
		Status:      storage.ControlRequestPending,
		RequestedBy: requestedBy,
		Metadata:    metadata,
	})
	if err != nil {
		return nil, fmt.Errorf("record volume control request for apply %s: %w", apply.ApplyIdentifier, err)
	}
	// The apply's state was checked before this insert, and the driver may have
	// settled the apply — and run its terminal pending-request sweep — in
	// between. A request recorded after that sweep would linger pending forever
	// with no driver left to service it, so re-check and sweep it here.
	storedApply, err := c.storage.Applies().Get(ctx, apply.ID)
	if err != nil {
		return nil, fmt.Errorf("reload apply %s after recording volume request: %w", apply.ApplyIdentifier, err)
	}
	if storedApply == nil {
		return nil, fmt.Errorf("reload apply %s after recording volume request: apply row not found", apply.ApplyIdentifier)
	}
	if state.IsTerminalApplyState(storedApply.State) {
		c.logger.Info("volume request arrived as the apply settled; sweeping it and rejecting",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"state", storedApply.State,
			"volume", volume)
		if sweepErr := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationVolume); sweepErr != nil {
			return nil, fmt.Errorf("sweep volume request for settled apply %s: %w", apply.ApplyIdentifier, sweepErr)
		}
		return &ternv1.VolumeResponse{
			Accepted:     false,
			ErrorMessage: fmt.Sprintf("Schema change %s is %s; volume can only be adjusted while it is running", apply.ApplyIdentifier, storedApply.State),
		}, nil
	}
	if alreadyPending {
		pendingVolume, decodeErr := storage.DecodeVolumeControlRequestMetadata(controlReq.Metadata)
		if decodeErr != nil {
			c.logger.Warn("volume rejected: pending volume request has undecodable metadata; the driver will fail it at its next progress tick",
				"apply_id", apply.ApplyIdentifier,
				"database", apply.Database,
				"environment", apply.Environment,
				"error", decodeErr)
			c.wakeOperator(apply)
			return &ternv1.VolumeResponse{
				Accepted:     false,
				ErrorMessage: fmt.Sprintf("A volume adjustment is already queued for apply %s; retry after the driver resolves it", apply.ApplyIdentifier),
			}, nil
		}
		if pendingVolume != volume {
			c.logger.Info("volume rejected: a different volume adjustment is already queued",
				"apply_id", apply.ApplyIdentifier,
				"database", apply.Database,
				"environment", apply.Environment,
				"pending_volume", pendingVolume,
				"requested_volume", volume)
			return &ternv1.VolumeResponse{
				Accepted:     false,
				ErrorMessage: fmt.Sprintf("A volume adjustment to %d is already queued for apply %s; the driver applies it at its next progress check — retry afterwards", pendingVolume, apply.ApplyIdentifier),
			}, nil
		}
		c.logger.Info("volume request already pending for apply driver",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"volume", volume,
			"requested_by", requestedBy)
	} else {
		c.logger.Info("volume request queued for apply driver",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"volume", volume,
			"requested_by", requestedBy)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventVolumeRequested, storage.LogSourceSchemaBot,
			fmt.Sprintf("Volume change to %d queued for apply driver%s", volume, callerApplyLogSuffix(requestedBy)), "", "")
	}
	c.wakeOperator(apply)
	return &ternv1.VolumeResponse{
		Accepted:  true,
		NewVolume: volume,
	}, nil
}

// processPendingVolumeControlRequest services a pending volume request from
// the driving instance's progress tick. The desired level travels in the
// request's metadata; the driver retunes the engine's running schema change
// and completes the request. Volume is a tuning knob, not a safety operation:
// an engine rejection fails the request terminally and the copy continues at
// its current volume, and a storage error is returned for the caller to log
// and retry at the next tick without aborting the drive.
func (c *LocalClient) processPendingVolumeControlRequest(ctx context.Context, apply *storage.Apply, eng engine.Engine, creds *engine.Credentials, resumeState *engine.ResumeState) error {
	controlReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationVolume)
	if err != nil {
		return err
	}
	if controlReq == nil {
		return nil
	}
	caller := controlRequestCaller(controlReq)
	// A fan-out apply runs one engine per operation, so an apply-scoped volume
	// request cannot be delivered to every deployment's schema change from one
	// driver's tick. Fail it closed rather than retune a single deployment and
	// report the adjustment as applied everywhere. Queue-time gating rejects
	// this shape before a request is recorded; this guard covers a request that
	// reached storage anyway.
	operations, err := c.storage.ApplyOperations().ListByApply(ctx, apply.ID)
	if err != nil {
		return fmt.Errorf("load operations for apply %s to service volume request: %w", apply.ApplyIdentifier, err)
	}
	if len(operations) > 1 {
		c.logger.Warn("pending volume request targets a multi-deployment apply; failing it without retuning any engine",
			append(apply.LogAttrs(), "operation_count", len(operations), "requested_by", caller)...)
		return failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationVolume,
			fmt.Sprintf("apply fans out to %d deployments; volume adjustment is not supported for multi-deployment applies", len(operations)))
	}
	volume, err := storage.DecodeVolumeControlRequestMetadata(controlReq.Metadata)
	if err != nil {
		c.logger.Warn("pending volume request has invalid metadata; failing the request and continuing at the current volume",
			append(apply.LogAttrs(), "requested_by", caller, "error", err)...)
		return failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationVolume, fmt.Sprintf("volume request metadata is invalid: %v", err))
	}
	result, err := eng.Volume(ctx, &engine.VolumeRequest{
		Database:    apply.Database,
		Volume:      volume,
		ResumeState: resumeState,
		Credentials: creds,
	})
	if err != nil {
		c.logger.Warn("engine rejected pending volume request; schema change continues at its current volume",
			append(apply.LogAttrs(), "volume", volume, "requested_by", caller, "error", err)...)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventError, storage.LogSourceSchemaBot,
			fmt.Sprintf("Volume change to %d failed: %v; schema change continues at its current volume", volume, err), "", "")
		return failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationVolume, err.Error())
	}
	if result == nil || !result.Accepted {
		message := "not accepted"
		if result != nil && result.Message != "" {
			message = result.Message
		}
		c.logger.Warn("engine did not accept pending volume request; schema change continues at its current volume",
			append(apply.LogAttrs(), "volume", volume, "requested_by", caller, "engine_message", message)...)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventError, storage.LogSourceSchemaBot,
			fmt.Sprintf("Volume change to %d was not accepted: %s; schema change continues at its current volume", volume, message), "", "")
		return failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationVolume, message)
	}
	if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationVolume); err != nil {
		return err
	}
	c.logger.Info("pending volume request applied",
		append(apply.LogAttrs(), "previous_volume", result.PreviousVolume, "volume", result.NewVolume, "requested_by", caller)...)
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
		fmt.Sprintf("Volume changed from %d to %d%s", result.PreviousVolume, result.NewVolume, callerApplyLogSuffix(caller)), "", "")
	if err := c.recordAppliedVolume(ctx, apply, result.NewVolume); err != nil {
		c.logger.Warn("failed to record applied volume on apply options; progress will keep reporting the previous volume",
			append(apply.LogAttrs(), "volume", result.NewVolume, "error", err)...)
	}
	return nil
}

// recordAppliedVolume persists the volume the engine is now running at onto
// the apply's stored options, which progress and the CLI read the volume from.
// The apply row is reloaded first so this targeted options write does not
// clobber state another write advanced past the drive's in-memory copy.
func (c *LocalClient) recordAppliedVolume(ctx context.Context, apply *storage.Apply, volume int32) error {
	if volume < storage.MinVolume || volume > storage.MaxVolume {
		return fmt.Errorf("engine reported applied volume %d out of range for apply %s", volume, apply.ApplyIdentifier)
	}
	stored, err := c.storage.Applies().Get(ctx, apply.ID)
	if err != nil {
		return fmt.Errorf("load apply %s to record applied volume: %w", apply.ApplyIdentifier, err)
	}
	if stored == nil {
		return fmt.Errorf("load apply %s to record applied volume: %w", apply.ApplyIdentifier, storage.ErrApplyNotFound)
	}
	opts := stored.GetOptions()
	opts.Volume = int(volume)
	stored.SetOptions(opts)
	if err := c.storage.Applies().Update(ctx, stored); err != nil {
		return fmt.Errorf("record applied volume %d on apply %s: %w", volume, apply.ApplyIdentifier, err)
	}
	apply.Options = stored.Options
	return nil
}

// convergeTaskVolumeToStoredLevel retunes a newly started task's schema change
// to the apply's stored volume level. Engine volume lives with each running
// schema change, so a task started after an operator adjusted the volume — a
// later table in a sequential drive, or a resumed apply — would otherwise run
// at the engine default while stored options, progress, and the CLI all report
// the adjusted level. Volume is a tuning knob, not a safety operation: a
// rejection is logged and the task continues at the engine default rather than
// failing the drive.
func (c *LocalClient) convergeTaskVolumeToStoredLevel(ctx context.Context, apply *storage.Apply, task *storage.Task, creds *engine.Credentials, resumeState *engine.ResumeState) {
	volume := int32(apply.GetOptions().Volume)
	if volume == 0 {
		c.logger.Debug("no stored volume level on apply options; task starts at the engine default",
			"apply_id", apply.ApplyIdentifier, "task_id", task.TaskIdentifier)
		return
	}
	if !taskHasTunableRowCopy(task) {
		c.logger.Debug("task has no row-copy work for volume to tune; volume convergence skipped",
			append(task.LogAttrs(), "ddl_action", task.DDLAction, "volume", volume)...)
		return
	}
	if volume < storage.MinVolume || volume > storage.MaxVolume {
		c.logger.Warn("stored volume level is out of range; task starts at the engine default",
			append(task.LogAttrs(), "volume", volume)...)
		return
	}
	result, err := c.getEngine().Volume(ctx, &engine.VolumeRequest{
		Database:    apply.Database,
		Volume:      volume,
		ResumeState: resumeState,
		Credentials: creds,
	})
	if err != nil {
		c.logger.Warn("engine rejected converging task to the stored volume level; task continues at the engine default",
			append(task.LogAttrs(), "volume", volume, "error", err)...)
		return
	}
	if result == nil || !result.Accepted {
		message := "not accepted"
		if result != nil && result.Message != "" {
			message = result.Message
		}
		c.logger.Warn("engine did not accept converging task to the stored volume level; task continues at the engine default",
			append(task.LogAttrs(), "volume", volume, "engine_message", message)...)
		return
	}
	c.logger.Info("task volume converged to the apply's stored level",
		append(task.LogAttrs(), "volume", volume)...)
}

// taskHasTunableRowCopy reports whether a task carries row-copy work that
// engine volume levels tune. Volume maps to copy throughput, which only exists
// for an ALTER's row copy; create and drop tasks run statement phases where an
// engine volume retune restarts the schema change for no benefit.
func taskHasTunableRowCopy(task *storage.Task) bool {
	return strings.EqualFold(task.DDLAction, "alter")
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
	c.logger.Info("sending revert request to engine", task.LogAttrs()...)
	if _, err = eng.Revert(ctx, controlReq); err != nil {
		return nil, fmt.Errorf("revert failed: %w", err)
	}
	c.logger.Info("engine accepted the revert request", task.LogAttrs()...)
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
	c.logger.Info("sending skip-revert request to engine", task.LogAttrs()...)
	if _, err = eng.SkipRevert(ctx, controlReq); err != nil {
		return nil, fmt.Errorf("skip revert failed: %w", err)
	}
	c.logger.Info("engine accepted the skip-revert request", task.LogAttrs()...)
	return &ternv1.SkipRevertResponse{Accepted: true}, nil
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
