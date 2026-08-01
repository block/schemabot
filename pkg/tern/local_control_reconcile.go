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

// reconcileEngineTerminalTruthBeforeCommands runs at the top of a drive claim,
// before any pending stop/cancel control request is consumed. When the engine's
// backend is authoritative for the change (ExternallyAuthoritativeProgress) and
// already reports it terminal, the drive adopts that outcome — settling the
// stored tasks and apply to the engine's truth and mooting the pending
// commands — instead of running a command the backend will reject forever.
//
// This preserves the commands-first invariant's intent: a pending kill command
// must be honored before any non-terminal work is advanced. A progress read
// plus terminal adoption advances nothing — only resuming work would — so
// reading before consuming is safe, and it is the only engine-agnostic way to
// learn the change already settled. Without it, stored state only learns the
// backend's terminal truth through per-engine classification of a failed
// command's rejection, which livelocks on any rejection the engine has not
// enumerated. (Instance-local engines reconcile through the resume re-plan —
// replanAndFilterTasks diffs live schema against the reviewed target — so they
// are deliberately excluded here.)
//
// Returns handled=true when the apply settled to the engine's terminal truth
// and the pending commands were mooted. Every uncertain path — no engine, a
// failed progress read, a non-terminal or non-adoptable engine state — fails
// toward consuming the pending command exactly as before, never toward
// skipping it.
func (c *LocalClient) reconcileEngineTerminalTruthBeforeCommands(ctx context.Context, apply *storage.Apply, tasks []*storage.Task) (bool, error) {
	cancelReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationCancel)
	if err != nil {
		return false, err
	}
	stopReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationStop)
	if err != nil {
		return false, err
	}
	if cancelReq == nil && stopReq == nil {
		// No pending kill command to protect. The drive's own progress polling
		// and resume re-plan reconcile engine state as usual.
		return false, nil
	}
	if applyInRevertPhase(apply) {
		// A revert-phase apply has cut over: its outcome is owned by the revert
		// and skip-revert paths, and the command gates permanently reject
		// stop/cancel for it. Adopting a terminal state here would race the
		// in-flight revert phase.
		c.logger.Info("skipping engine terminal-truth reconcile for revert-phase apply; the command gates own the rejection",
			apply.LogAttrs()...)
		return false, nil
	}
	eng := c.getEngine()
	if eng == nil {
		// The command path fails closed on the missing engine with its own
		// error; there is no authoritative state to read here.
		c.logger.Warn("no engine to read terminal truth from before consuming pending commands; pending commands will be consumed",
			apply.LogAttrs()...)
		return false, nil
	}
	if !engine.ProgressIsExternallyAuthoritative(eng) {
		// An instance-local engine's progress reflects only this process's
		// in-memory state, which says nothing about work a previous process (or
		// another instance) drove. Those engines reconcile completed work
		// through the resume re-plan instead.
		c.logger.Debug("engine progress is instance-local; resume re-plan reconciles completed work, pending commands will be consumed",
			append(apply.LogAttrs(), "engine", eng.Name())...)
		return false, nil
	}
	target := firstTaskWithLiveEngineWork(tasks)
	if target == nil {
		// No task addresses live engine work, so there is no backend change
		// whose terminal truth could contradict the command. The command path
		// owns the terminal and task-less shapes.
		c.logger.Debug("no task with live engine work to reconcile; pending commands will be consumed",
			apply.LogAttrs()...)
		return false, nil
	}
	progress, err := c.readEngineProgressForTask(ctx, eng, target)
	if err != nil {
		// Fail toward consuming the command, never toward skipping it: an
		// unreadable backend must not delay a kill command that may still be
		// able to act.
		c.logger.Warn("could not read the engine's authoritative state before consuming pending commands; pending commands will be consumed",
			append(apply.LogAttrs(), "task_id", target.TaskIdentifier, "engine", eng.Name(), "error", err)...)
		metrics.RecordEngineTerminalTruthReconcile(ctx, apply.Database, apply.Deployment, apply.Environment, "progress_error")
		return false, nil
	}
	applyState, taskState, adoptable := adoptableEngineTerminalStates(progress.State, progress.Retryable)
	if !adoptable {
		c.logger.Info("engine does not report a settled terminal outcome; pending commands will be consumed",
			append(apply.LogAttrs(), "engine_state", string(progress.State))...)
		return false, nil
	}
	if err := c.adoptEngineTerminalTruth(ctx, apply, tasks, progress, applyState, taskState, reconcileRequestedBy(cancelReq, stopReq)); err != nil {
		return false, err
	}
	metrics.RecordEngineTerminalTruthReconcile(ctx, apply.Database, apply.Deployment, apply.Environment, "adopted_"+applyState)
	// The adopted terminal state moots the pending commands: the sweep
	// completes the pending stop, and the pending cancel too for every adopted
	// state (none of them is stopped, the one state that keeps a cancel
	// deliverable).
	if err := completePendingRequestsForTerminalApply(ctx, c.storage, apply); err != nil {
		return true, err
	}
	c.notifyTerminalObserver(apply, tasks)
	return true, nil
}

// adoptableEngineTerminalStates maps a terminal engine progress state to the
// stored apply and task states a drive adopts when it reconciles before
// consuming a pending kill command. ok is false for every state the drive must
// not adopt, so the pending command is consumed as usual:
//   - non-terminal states, including revert_window — the change is live engine
//     work (the revert window is owned by the revert/skip-revert paths, never
//     settled to completed here);
//   - stopped — a stopped change is resumable, so a pending cancel still has
//     live backend work to kill;
//   - failed while the engine reports it retryable — the change may be
//     retried, so a pending kill command must still be honored.
func adoptableEngineTerminalStates(s engine.State, retryable bool) (applyState, taskState string, ok bool) {
	switch s {
	case engine.StateCompleted:
		return state.Apply.Completed, state.Task.Completed, true
	case engine.StateFailed:
		if retryable {
			return "", "", false
		}
		return state.Apply.Failed, state.Task.Failed, true
	case engine.StateCancelled:
		return state.Apply.Cancelled, state.Task.Cancelled, true
	case engine.StateReverted:
		return state.Apply.Reverted, state.Task.Reverted, true
	default:
		return "", "", false
	}
}

// firstTaskWithLiveEngineWork returns the first task whose state says the
// engine's backend has (or may have) live work for this apply, or nil when no
// task does. Mirrors the stop/cancel engine sweeps: an apply drives a single
// engine operation, so one task is enough to address it.
func firstTaskWithLiveEngineWork(tasks []*storage.Task) *storage.Task {
	for _, task := range tasks {
		if hasLiveEngineWork(task.State) {
			return task
		}
	}
	return nil
}

// readEngineProgressForTask reads the engine's authoritative view of the change
// the task addresses. It mirrors buildControlRequest's addressing: Vitess
// targets are addressed through the persisted engine resume state (the deploy
// request identifier lives there), other targets by credentials alone.
func (c *LocalClient) readEngineProgressForTask(ctx context.Context, eng engine.Engine, task *storage.Task) (*engine.ProgressResult, error) {
	creds, err := c.credentialsForTask(task)
	if err != nil {
		return nil, fmt.Errorf("resolve credentials for task %s: %w", task.TaskIdentifier, err)
	}
	req := &engine.ProgressRequest{
		Database:    c.config.Database,
		Credentials: creds,
	}
	if c.config.Type == storage.DatabaseTypeVitess {
		resumeState, err := c.loadEngineResumeState(ctx, task)
		if err != nil {
			return nil, fmt.Errorf("load engine resume state for task %s: %w", task.TaskIdentifier, err)
		}
		req.ResumeState = resumeState
	}
	res, err := eng.Progress(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("read engine progress for task %s: %w", task.TaskIdentifier, err)
	}
	if res == nil {
		return nil, fmt.Errorf("engine returned no progress for task %s", task.TaskIdentifier)
	}
	return res, nil
}

// adoptEngineTerminalTruth settles a claimed apply and its still-active tasks
// to the terminal outcome the engine's backend reports. The backend's terminal
// state is authoritative — the change landed, failed, was cancelled, or was
// reverted there before this drive ran — so stored state adopts it rather than
// recording the outcome of a command the backend can no longer honor. It is
// the drive-owned counterpart to settleControlForCompletedEngineChange, which
// applies the same discipline when the engine rejects an already-issued
// command. An apply already in a non-resumable terminal state keeps its
// outcome.
func (c *LocalClient) adoptEngineTerminalTruth(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, progress *engine.ProgressResult, applyState, taskState, requestedBy string) error {
	now := time.Now()
	var settledCount, skippedCount int64
	for _, task := range tasks {
		if state.IsTerminalTaskState(task.State) {
			skippedCount++
			continue
		}
		if state.IsState(taskState, state.Task.Completed) {
			task.ProgressPercent = 100
		}
		if state.IsState(taskState, state.Task.Failed) && task.ErrorMessage == "" && progress.ErrorMessage != "" {
			task.ErrorMessage = progress.ErrorMessage
		}
		task.CompletedAt = &now
		c.transitionTaskState(ctx, task, task.ApplyID, taskState,
			fmt.Sprintf("Task %s adopted the engine's %s outcome (the engine reached it before the pending command was consumed)", task.TaskIdentifier, taskState))
		settledCount++
	}
	if state.IsTerminalApplyState(apply.State) && !state.IsState(apply.State, state.Apply.Stopped) {
		c.logger.Info("engine terminal-truth reconcile found the apply already terminal; keeping its outcome",
			append(apply.LogAttrs(), "engine_state", string(progress.State), "requested_by", requestedBy)...)
		return nil
	}
	previousState := apply.State
	apply.State = applyState
	apply.CompletedAt = &now
	apply.UpdatedAt = now
	if state.IsState(applyState, state.Apply.Failed) && apply.ErrorMessage == "" && progress.ErrorMessage != "" {
		apply.ErrorMessage = progress.ErrorMessage
	}
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		return fmt.Errorf("adopt engine terminal state %s for apply %s (database %s): %w", applyState, apply.ApplyIdentifier, apply.Database, err)
	}
	c.logger.Warn("engine reached a terminal state before a pending stop/cancel was consumed; apply settled to the engine's outcome",
		append(apply.LogAttrs(),
			"engine_state", string(progress.State),
			"previous_state", previousState,
			"requested_by", requestedBy,
			"settled_task_count", settledCount,
			"terminal_task_count", skippedCount)...)
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
		fmt.Sprintf("Schema change reached %s on the engine before the pending stop/cancel was consumed; apply recorded as %s (%d tasks settled, %d already terminal)%s",
			progress.State, applyState, settledCount, skippedCount, callerApplyLogSuffix(requestedBy)),
		previousState, applyState)
	return nil
}

// reconcileRequestedBy names the operator whose pending command triggered the
// terminal-truth reconcile, preferring the cancel request (the stronger verb)
// when both are pending.
func reconcileRequestedBy(cancelReq, stopReq *storage.ApplyControlRequest) string {
	if cancelReq != nil {
		return controlRequestCaller(cancelReq)
	}
	return controlRequestCaller(stopReq)
}
