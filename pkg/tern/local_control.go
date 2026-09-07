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
	if err := markApplyCuttingOverForControlRequest(ctx, c.storage, apply, logger); err != nil {
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
		errorMessage := "the cutover path returned neither a response nor an error"
		if err := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCutover, errorMessage); err != nil {
			return err
		}
		return fmt.Errorf("process pending cutover for apply %s: %s", apply.ApplyIdentifier, errorMessage)
	}
	if !resp.Accepted {
		errorMessage := controlRefusalMessage(storage.ControlOperationCutover, resp.ErrorMessage)
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

	// A revert-phase apply has already cut over, so this is a decision about the
	// schema change rather than a failure to act on it. It answers as a refusal
	// and not an error: a caller on the far side of a plane boundary sees every
	// error as one generic internal status, so it cannot tell this apart from a
	// transient failure and leaves its durable request pending to re-send the
	// same doomed cancel on every later claim.
	if revertPhase, err := c.applyRevertPhaseBlock(ctx, apply); err != nil {
		return nil, err
	} else if revertPhase != "" {
		c.logger.Warn("cancel refused: schema change is in a revert phase and has already cut over",
			"apply_id", apply.ApplyIdentifier, "state", apply.State, "revert_phase", revertPhase)
		return &ternv1.CancelResponse{
			Accepted:     false,
			ErrorMessage: revertPhaseControlRejectionMessage(apply.ApplyIdentifier, revertPhase),
		}, nil
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

	// A revert-phase apply has already cut over, so this is a decision about the
	// schema change rather than a failure to act on it, and it answers as a
	// refusal for the same reason the cancel path does.
	if revertPhase, err := c.applyRevertPhaseBlock(ctx, apply); err != nil {
		return nil, err
	} else if revertPhase != "" {
		c.logger.Warn("stop refused: schema change is in a revert phase and has already cut over",
			"apply_id", apply.ApplyIdentifier,
			"state", apply.State,
			"revert_phase", revertPhase)
		return &ternv1.StopResponse{
			Accepted:     false,
			ErrorMessage: revertPhaseControlRejectionMessage(apply.ApplyIdentifier, revertPhase),
		}, nil
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
	var engineTableProgress StatementIndex[engine.TableProgress]
	if stopTerminatesChange(c.config.Type) {
		terminalState = state.Task.Cancelled
	} else {
		// Snapshot progress AFTER Spirit has fully stopped to preserve row copy progress.
		engineTableProgress = c.snapshotEngineProgress(ctx, eng, stopCreds)
	}

	stoppedCount, skippedCount, applyID, err := c.markTasksWithState(ctx, tasks, targetApplyID, engineTableProgress, terminalState)
	if err != nil {
		// Fail the stop rather than settling the apply on top of task rows that
		// never moved. An operator re-issuing a stop is a recoverable outcome;
		// an apply recorded as stopped while its tasks still read as active work
		// holds the database with nothing left able to act on it.
		return nil, fmt.Errorf("stop schema change on database %s: %w", c.config.Database, err)
	}

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

	cancelledCount, skippedCount, applyID, err := c.markTasksWithState(ctx, tasks, targetApplyID, StatementIndex[engine.TableProgress]{}, state.Task.Cancelled)
	if err != nil {
		// Same reasoning as the stop: an apply recorded as cancelled over task
		// rows that never moved detaches the apply from its own tasks.
		return nil, fmt.Errorf("cancel schema change on database %s: %w", c.config.Database, err)
	}
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
// declined=false when the error is not an unsupported-operation decline, so
// the caller applies its normal error handling. When declined=true the
// operation did not take effect: a stop or cancel caller must report the
// request as not handled, or the drive loop would mark the still-running
// apply stopped.
func (c *LocalClient) failPendingRequestForUnsupportedOperation(ctx context.Context, logger *slog.Logger, apply *storage.Apply, operation storage.ControlOperation, eventType string, controlReq *storage.ApplyControlRequest, opErr error) (bool, error) {
	unsupported, ok := engine.AsUnsupportedOperation(opErr)
	if !ok {
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
		return true, fmt.Errorf("process pending %s for apply %s: %w; fail pending %s request: %w", operation, apply.ApplyIdentifier, opErr, operation, err)
	}
	engineName := "unknown"
	if eng := c.getEngine(); eng != nil {
		engineName = eng.Name()
	}
	metrics.RecordControlRequestUnsupportedDecline(ctx, string(operation), engineName, apply.Database, apply.Deployment, apply.Environment)
	return true, nil
}

// failRefusedControlRequest resolves a pending control request that the stop or
// cancel path answered with an explicit refusal. The refusal is a decision, not
// a delivery failure: the request is already recorded durably, so a later claim
// can only re-send it and collect the same refusal while the schema change keeps
// running unwatched. Failing it with the stated reason ends that loop and leaves
// the operator a request whose answer they can read.
//
// The refusal means the operation did not take effect, so callers report the
// request as not handled: the drive would otherwise record an operator stop or
// cancel over a change that is still running.
func (c *LocalClient) failRefusedControlRequest(ctx context.Context, logger *slog.Logger, apply *storage.Apply, operation storage.ControlOperation, eventType string, controlReq *storage.ApplyControlRequest, errorMessage string) error {
	message := controlRefusalMessage(operation, errorMessage)
	caller := controlRequestCaller(controlReq)
	logger.Warn("rejecting pending control request: the operation was refused; the schema change continues and settles on its own",
		"operation", string(operation),
		"requested_by", caller,
		"state", apply.State,
		"error_message", message)
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, eventType, storage.LogSourceSchemaBot,
		fmt.Sprintf("Pending %s request rejected: %s%s", operation, message, callerApplyLogSuffix(caller)), "", "")
	if err := failPendingControlRequests(ctx, c.storage, apply, operation, message); err != nil {
		return fmt.Errorf("process pending %s for apply %s: refused with %q; fail pending %s request: %w", operation, apply.ApplyIdentifier, message, operation, err)
	}
	return nil
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
		if declined, declineErr := c.failPendingRequestForUnsupportedOperation(stopCtx, logger, apply, storage.ControlOperationStop, storage.LogEventStopRequested, controlReq, err); declined {
			// The request is resolved terminally but no stop took effect: the
			// schema change keeps running, so the drive must not treat this as
			// an operator stop.
			return false, declineErr
		}
		return true, fmt.Errorf("process pending stop for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if resp == nil {
		return true, fmt.Errorf("process pending stop for apply %s: the stop path returned neither a response nor an error", apply.ApplyIdentifier)
	}
	if !resp.Accepted {
		// The refusal is the answer, so resolve the request on it. Returning an
		// error would leave the request pending and every later claim would
		// collect the same refusal while the schema change kept running.
		// Report the stop as not handled: none took effect.
		return false, c.failRefusedControlRequest(stopCtx, logger, apply, storage.ControlOperationStop, storage.LogEventStopRequested, controlReq, resp.ErrorMessage)
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
		if declined, declineErr := c.failPendingRequestForUnsupportedOperation(cancelCtx, logger, apply, storage.ControlOperationCancel, storage.LogEventCancelRequested, controlReq, err); declined {
			// The request is resolved terminally but no cancel took effect:
			// the schema change keeps running, so the drive must not treat
			// this as an operator cancel.
			return false, declineErr
		}
		return true, fmt.Errorf("process pending cancel for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if resp == nil {
		return true, fmt.Errorf("process pending cancel for apply %s: the cancel path returned neither a response nor an error", apply.ApplyIdentifier)
	}
	if !resp.Accepted {
		// See the stop counterpart: the refusal resolves the request, and the
		// cancel is reported as not handled because none took effect.
		return false, c.failRefusedControlRequest(cancelCtx, logger, apply, storage.ControlOperationCancel, storage.LogEventCancelRequested, controlReq, resp.ErrorMessage)
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
		if _, stopErr := eng.Stop(ctx, req); stopErr != nil {
			if err := c.resolveFailedEngineStop(ctx, eng, task, creds, req.ResumeState, stopErr); err != nil {
				return nil, err
			}
			// The engine tracks nothing for this task, so there is no live
			// progress to snapshot: returning no stop credentials keeps the
			// persisted checkpoint progress on the task rows.
			return nil, nil
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
		if _, cancelErr := eng.Cancel(ctx, req); cancelErr != nil {
			return c.resolveFailedEngineCancel(ctx, eng, task, creds, req.ResumeState, cancelErr)
		}
		return nil
	}
	c.logger.Debug("no targeted task has live engine work to cancel", "database", c.config.Database, "type", c.config.Type, "target_apply_id", targetApplyID)
	return nil
}

// resolveFailedEngineCancel decides whether a failed engine cancel may still
// complete durably. A cancel that cannot reach any running work has nothing
// left to stop — completing it is the fail-closed choice for cancel semantics,
// because the destructive direction is continuing the work, not stopping it.
// Surfacing such an error instead would abort the drive, leave the durable
// cancel request pending, and re-run the same failing cancel on every claim.
//
// The engine's own report decides: a progress probe scoped to the task shows
// whether live work remains. Nothing running resolves to nil so the caller
// proceeds to terminalize the tasks; live work — or any uncertainty about it
// (a failed probe) — surfaces the original cancel error unchanged. Typed
// rejections with their own resolution paths also surface unchanged: an
// already-completed rejection has the caller reconcile stored state to the
// completed outcome, and an unsupported-operation decline resolves the durable
// request without recording a cancel.
func (c *LocalClient) resolveFailedEngineCancel(ctx context.Context, eng engine.Engine, task *storage.Task, creds *engine.Credentials, resumeState *engine.ResumeState, cancelErr error) error {
	if engine.IsAlreadyCompleted(cancelErr) {
		return fmt.Errorf("cancel engine for task %s: %w", task.TaskIdentifier, cancelErr)
	}
	// An unsupported-operation decline means the engine refuses cancel for its
	// database type while the schema change keeps executing; it surfaces so the
	// pending-request decline path resolves the request terminally without
	// touching the running change.
	if engine.IsUnsupportedOperation(cancelErr) {
		return fmt.Errorf("cancel engine for task %s: %w", task.TaskIdentifier, cancelErr)
	}
	progress, probeErr := eng.Progress(ctx, &engine.ProgressRequest{
		Database:    c.config.Database,
		Credentials: creds,
		ResumeState: resumeState,
	})
	if probeErr != nil {
		c.logger.Warn("engine cancel failed and the live-work probe also failed; the cancel error surfaces and the drive will retry rather than complete a cancel over unknown engine state",
			append(task.LogAttrs(), "probe_error", probeErr, "error", cancelErr)...)
		return fmt.Errorf("cancel engine for task %s: %w", task.TaskIdentifier, cancelErr)
	}
	if engineProgressShowsLiveWork(eng, progress) {
		engineState := ""
		if progress != nil {
			engineState = string(progress.State)
		}
		c.logger.Warn("engine cancel failed while the engine still has live work; the cancel error surfaces so the work is never recorded cancelled while it keeps running",
			append(task.LogAttrs(), "engine_state", engineState, "error", cancelErr)...)
		return fmt.Errorf("cancel engine for task %s: %w", task.TaskIdentifier, cancelErr)
	}
	c.logger.Warn("engine cancel failed but the engine has no live work for the task; the durable cancel proceeds and terminalizes the task",
		append(task.LogAttrs(), "engine_state", string(progress.State), "error", cancelErr)...)
	return nil
}

// resolveFailedEngineStop decides whether a failed engine stop may still
// complete durably. A stop that cannot reach any running work has nothing left
// to pause — the persisted checkpoint is already the resume point a stop
// exists to preserve — so completing it records the tasks and apply as
// stopped, the resumable state a later start resumes from. Surfacing such an
// error instead would abort the drive, leave the durable stop request pending,
// and re-run the same failing stop on every claim.
//
// The engine's own report decides: a progress probe scoped to the task shows
// whether live work remains. Nothing running resolves to nil so the caller
// proceeds to record the stop; live work — or any uncertainty about it (a
// failed probe) — surfaces the original stop error unchanged, because a
// stopped record over an engine still executing would let the change keep
// running unwatched. Typed rejections with their own resolution paths also
// surface unchanged: an already-completed rejection has the caller reconcile
// stored state to the completed outcome, and an unsupported-operation decline
// resolves the durable request without recording a stop.
func (c *LocalClient) resolveFailedEngineStop(ctx context.Context, eng engine.Engine, task *storage.Task, creds *engine.Credentials, resumeState *engine.ResumeState, stopErr error) error {
	if engine.IsAlreadyCompleted(stopErr) {
		return c.wrapFailedEngineStop(task, stopErr)
	}
	// An unsupported-operation decline means the engine refuses stop for its
	// database type while the schema change keeps executing; it surfaces so the
	// pending-request decline path resolves the request terminally without
	// touching the running change.
	if engine.IsUnsupportedOperation(stopErr) {
		return c.wrapFailedEngineStop(task, stopErr)
	}
	progress, probeErr := eng.Progress(ctx, &engine.ProgressRequest{
		Database:    c.config.Database,
		Credentials: creds,
		ResumeState: resumeState,
	})
	if probeErr != nil {
		c.logger.Warn("engine stop failed and the live-work probe also failed; the stop error surfaces and the drive will retry rather than record a stop over unknown engine state",
			append(task.LogAttrs(), "probe_error", probeErr, "error", stopErr)...)
		return c.wrapFailedEngineStop(task, stopErr)
	}
	if engineProgressShowsLiveWork(eng, progress) {
		engineState := ""
		if progress != nil {
			engineState = string(progress.State)
		}
		c.logger.Warn("engine stop failed while the engine still has live work; the stop error surfaces so the work is never recorded stopped while it keeps running",
			append(task.LogAttrs(), "engine_state", engineState, "error", stopErr)...)
		return c.wrapFailedEngineStop(task, stopErr)
	}
	c.logger.Warn("engine stop failed but the engine has no live work for the task; the durable stop proceeds and records the tasks stopped for a later resume",
		append(task.LogAttrs(), "engine_state", string(progress.State), "error", stopErr)...)
	return nil
}

// wrapFailedEngineStop names, in the surfaced error, the engine operation a
// stop performs on this database type: on Vitess targets stop cancels the
// provider deploy request, everywhere else it stops the local engine's work.
func (c *LocalClient) wrapFailedEngineStop(task *storage.Task, stopErr error) error {
	if c.config.Type == storage.DatabaseTypeVitess {
		return fmt.Errorf("cancel deploy request for task %s: %w", task.TaskIdentifier, stopErr)
	}
	return fmt.Errorf("stop local engine for task %s: %w", task.TaskIdentifier, stopErr)
}

// engineProgressShowsLiveWork reports whether a progress probe shows engine
// work that a stop or cancel must still terminate before storage may record
// the schema change as stopped or cancelled. It reads the engine's own
// report, unlike hasLiveEngineWork, which reads the stored task state's
// expectation.
// Uncertainty counts as live work — only an affirmative nothing-left-to-settle
// answer clears the probe, and what qualifies depends on who owns the engine's
// truth:
//
//   - An engine whose progress is externally authoritative relays the
//     provider's record of the change, so only a provider answer that closed
//     the change clears the probe: cancelled, or failed with no retry path.
//     Pending from such an engine is an open change the provider can still
//     run, and a retryable failure remains runnable — both keep the control
//     error surfacing so the change is never recorded stopped or cancelled
//     while the provider can still land it.
//   - Any other engine executes in this process, so pending — the idle
//     sentinel — is the only answer proving nothing is left. A state the
//     engine still tracks, terminal or not, means engine-owned work (such as
//     target cleanup) remains that only a retried stop or cancel finishes.
//
// Completed deliberately counts as live for every engine: a change that
// landed must reconcile to its completed outcome, never settle as stopped or
// cancelled.
// The revert states keep the engine actively unwinding the change.
//
// A nil result is uncertainty of the same kind and counts as live, so a caller
// that reaches the cleared branch always holds a result it can report.
func engineProgressShowsLiveWork(eng engine.Engine, progress *engine.ProgressResult) bool {
	if progress == nil {
		return true
	}
	if engine.ProgressIsExternallyAuthoritative(eng) {
		switch progress.State {
		case engine.StateCancelled:
			return false
		case engine.StateFailed:
			return progress.Retryable
		default:
			return true
		}
	}
	return progress.State != engine.StatePending
}

// snapshotEngineProgress captures per-table progress from the engine after stopping.
func (c *LocalClient) snapshotEngineProgress(ctx context.Context, eng engine.Engine, creds *engine.Credentials) StatementIndex[engine.TableProgress] {
	var none StatementIndex[engine.TableProgress]
	if eng == nil {
		c.logger.Error("snapshotEngineProgress: engine is nil")
		return none
	}
	if creds == nil {
		c.logger.Debug("skipping engine progress snapshot because no live engine work was stopped", "database", c.config.Database, "type", c.config.Type)
		return none
	}
	progress, err := eng.Progress(ctx, &engine.ProgressRequest{
		Database:    c.config.Database,
		Credentials: creds,
	})
	if err != nil {
		c.logger.Warn("failed to snapshot engine progress after stop",
			"database", c.config.Database, "type", c.config.Type, "error", err)
		return none
	}
	if progress != nil {
		return indexEngineTableProgress(progress.Tables)
	}
	return none
}

// markTasksWithState settles all non-terminal targeted tasks into newState,
// preserving engine progress. Returns the marked count, the skipped count, an
// apply ID for logging, and an error joining every refused write.
//
// The counts are landed writes, not attempts: they become the operator-facing
// "N tasks stopped" event and response, and they gate the apply-level write
// that follows. A refused task write is returned as an error rather than
// counted, because the apply lease and the operation lease are separate — a
// stop can hold the first while a peer has taken the second, and marking the
// apply settled on top of task rows that never moved detaches the apply from
// its own tasks. Every task is still attempted, so an operator retrying the
// command has only the refused ones left to write.
func (c *LocalClient) markTasksWithState(ctx context.Context, tasks []*storage.Task, targetApplyID int64, engineProgress StatementIndex[engine.TableProgress], newState string) (int64, int64, int64, error) {
	var stoppedCount, skippedCount int64
	var applyID int64
	var refused []error

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
		if et, ok := engineProgress.ForTask(task); ok {
			task.RowsCopied = et.RowsCopied
			task.RowsTotal = et.RowsTotal
			task.ProgressPercent = et.Progress
			task.ETASeconds = int(et.ETASeconds)
			task.ChecksumRowsChecked = et.ChecksumRowsChecked
			task.ChecksumRowsTotal = et.ChecksumRowsTotal
		}

		if err := c.persistTaskStateTransition(ctx, task, task.ApplyID, newState,
			fmt.Sprintf("Task %s %s", task.TaskIdentifier, newState)); err != nil {
			c.logger.Error("failed to settle task during control operation; the task keeps its previous state",
				append(task.LogAttrs(), "target_state", newState, "error", err)...)
			refused = append(refused, err)
			continue
		}

		stoppedCount++
	}

	if len(refused) > 0 {
		return stoppedCount, skippedCount, applyID, fmt.Errorf("failed to settle %d of %d tasks to %s: %w",
			len(refused), int64(len(refused))+stoppedCount, newState, errors.Join(refused...))
	}
	return stoppedCount, skippedCount, applyID, nil
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
		// The completed state must durably land before the settle proceeds:
		// resolving the durable control request and terminalizing the apply
		// over a refused write — e.g. a lease-guarded update that lost to a
		// peer driver — would record the settle as done while the task row
		// durably stays non-terminal. Failing the settle keeps the request
		// pending so a later claim redoes it under a current lease.
		if err := c.persistTaskStateTransition(ctx, task, task.ApplyID, state.Task.Completed,
			fmt.Sprintf("Task %s completed on the engine before the %s took effect", task.TaskIdentifier, operation)); err != nil {
			return 0, fmt.Errorf("settle task %s completed after the engine rejected the %s as already completed: %w", task.TaskIdentifier, operation, err)
		}
		completedCount++
	}
	if applyID == 0 {
		return 0, fmt.Errorf("%s found the schema change already completed on the engine, but resolved no apply to settle: %w", operation, rejection)
	}
	// A multi-operation drive owns only its operation: the tasks settled above
	// carry the completed outcome, the operator derives the operation row from
	// them and projects the parent completed, so the parent write, apply event,
	// and terminal observer are the operator's to make — a direct write here
	// fails closed under the operation-only lease and would turn an accepted
	// settle into a drive error the claim loop re-runs forever.
	if suppressParentApplyWrites(ctx) {
		attrs := []any{"database", c.config.Database}
		if targetApply != nil {
			attrs = targetApply.LogAttrs()
		}
		c.logger.Info("engine rejected "+operation+" because the schema change already completed; operation drive settled its tasks and the operator projects the parent",
			append(attrs,
				"requested_by", caller,
				"completed_task_count", completedCount,
				"terminal_task_count", skippedCount,
				"error", rejection)...)
		return skippedCount, nil
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
// Stored resume state is loaded for every database type: an engine that
// addresses remote work (a deploy request) needs it to reach that work, and an
// engine that keys control on in-process or database-side state ignores it.
// Whether an operation may proceed without one is the engine's call, made
// through the ControlResumeValidator gate below — engines own validation of
// opaque ResumeState.Metadata before control calls.
func (c *LocalClient) buildControlRequest(ctx context.Context, task *storage.Task, creds *engine.Credentials, eng engine.Engine, operation engine.ControlOperation) (*engine.ControlRequest, error) {
	resumeState, err := c.loadStoredEngineResumeState(ctx, task, string(operation))
	if err != nil {
		return nil, fmt.Errorf("load engine resume state for %s of task %s: %w", operation, task.TaskIdentifier, err)
	}
	req := &engine.ControlRequest{
		Database:    c.config.Database,
		Credentials: creds,
		ResumeState: resumeState,
	}
	if validator, ok := eng.(engine.ControlResumeValidator); ok {
		if err := validator.ValidateControlResumeState(operation, req.ResumeState); err != nil {
			return nil, fmt.Errorf("validate %s resume state for task %s: %w", operation, task.TaskIdentifier, err)
		}
	}
	return req, nil
}

// loadStoredEngineResumeState returns the persisted engine resume state for
// the task's apply operation, or nil when none was ever persisted. Only a
// storage read failure is an error: a task without an apply operation and an
// operation without a stored row are both legitimate nothing-persisted shapes,
// because an engine that reattaches through in-process or database-side state
// never persists one. The purpose names the request being built, so a log line
// says which engine call went out without resume state.
func (c *LocalClient) loadStoredEngineResumeState(ctx context.Context, task *storage.Task, purpose string) (*engine.ResumeState, error) {
	operationID, err := applyOperationIDForTask(task)
	if err != nil {
		c.logger.Debug("task has no apply operation to hold persisted engine resume state; the engine request goes out without one",
			append(task.LogAttrs(), "purpose", purpose, "reason", err.Error())...)
		return nil, nil
	}
	stored, err := c.loadEngineResumeStateForOperation(ctx, operationID)
	if errors.Is(err, storage.ErrEngineResumeStateNotFound) {
		c.logger.Debug("no persisted engine resume state for the task's apply operation; the engine request goes out without one",
			append(task.LogAttrs(), "purpose", purpose, "apply_operation_id", operationID)...)
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load persisted engine resume state for apply operation %d: %w", operationID, err)
	}
	return stored, nil
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
		if unsupported, ok := engine.AsUnsupportedOperation(err); ok {
			c.logDeclinedControlOperation(task, "revert", unsupported)
			return &ternv1.RevertResponse{Accepted: false, ErrorMessage: unsupported.Error()}, nil
		}
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
		if unsupported, ok := engine.AsUnsupportedOperation(err); ok {
			c.logDeclinedControlOperation(task, "skip-revert", unsupported)
			return &ternv1.SkipRevertResponse{Accepted: false, ErrorMessage: unsupported.Error()}, nil
		}
		return nil, fmt.Errorf("skip revert failed: %w", err)
	}
	c.logger.Info("engine accepted the skip-revert request", task.LogAttrs()...)
	return &ternv1.SkipRevertResponse{Accepted: true}, nil
}

// logDeclinedControlOperation records an engine declining a control operation
// for its whole database type. The decline is returned to the caller as a
// refused response rather than an error, because an error is the one shape that
// cannot survive the RPC boundary: the gRPC server maps every error to a
// generic internal status, and the caller cannot tell a deterministic refusal
// from a transient failure, so it retries a request no retry can ever satisfy.
// A refusal carries its reason to both the immediate caller and the durable
// control request, which resolves on it.
func (c *LocalClient) logDeclinedControlOperation(task *storage.Task, operation string, err error) {
	c.logger.Warn("the engine does not support this control operation; refusing it so the request resolves instead of retrying",
		append(task.LogAttrs(), "operation", operation, "error", err)...)
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
