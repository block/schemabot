package tern

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// executeApplySequential runs each DDL as a separate Spirit call (independent mode).
// Each table copies and cuts over independently.
func (c *LocalClient) executeApplySequential(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, plan *storage.Plan, options map[string]string) {
	ctx, cancelApply := context.WithCancel(ctx)
	defer cancelApply()
	defer c.startApplyHeartbeat(ctx, apply, cancelApply)()
	seqStart := time.Now()
	defer c.setupSpiritLogging(ctx, apply, tasks)()
	// Bind the apply's identity once so every line of this sequential drive is
	// filterable by apply_id/repo/pr without hand-listing the attrs per call.
	// Mutable attrs (task state, apply state) stay per-call so they are never
	// frozen stale into the bound logger.
	logger := c.logger.With(apply.IdentityLogAttrs()...)

	logger.Info("executeApplySequential starting",
		"task_count", len(tasks),
		"plan_ddl_count", len(plan.FlatDDLChanges()),
		"elapsed_ms", time.Since(seqStart).Milliseconds(),
	)

	now := time.Now()
	apply.State = state.Apply.Running
	apply.StartedAt = &now
	apply.UpdatedAt = now
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		logger.Error("failed to update apply state", append(apply.MutableLogAttrs(), "error", err)...)
	}

	var failedTask *storage.Task
	var stoppedByUser bool

	for i, task := range tasks {
		if handled, err := c.processPendingCancelOrStopControlRequest(ctx, apply); err != nil {
			logger.Warn("pending stop request processing failed; current apply owner will exit for operator retry",
				"error", err)
			return
		} else if handled {
			stoppedByUser = true
			break
		}

		action := c.checkTaskReady(ctx, logger, task)
		if action == taskHandover {
			return
		}
		if action == taskStopped {
			stoppedByUser = true
			break
		}
		if action == taskSkip {
			continue
		}

		logger.Info("executeApplySequential: starting task",
			"iteration", i+1, "total_tasks", len(tasks),
			"task_id", task.TaskIdentifier, "table", task.TableName,
			"elapsed_ms", time.Since(seqStart).Milliseconds(),
		)

		action = c.runEngineTask(ctx, apply, task, options)

		// Notify observer after each task completes
		if obs := c.getObserver(apply.ID); obs != nil {
			obs.OnProgress(apply, tasks)
		}

		if action == taskFailed {
			failedTask = task
			break
		}
		if action == taskAbort || action == taskHandover {
			return
		}
		if action == taskStopped {
			stoppedByUser = true
			break
		}
	}

	// Update apply state based on task outcomes
	logger.Info("executeApplySequential loop finished",
		"task_count", len(tasks),
		"failed_task", failedTask != nil,
		"stopped_by_user", stoppedByUser,
	)
	c.finalizeSequentialApply(ctx, apply, tasks, failedTask, stoppedByUser)
	logger.Info("sequential apply finished", "state", apply.State)
}

// taskAction indicates the outcome of a single task execution step.
type taskAction int

// taskStopped and taskHandover both end the loop early without further work, but
// they mean opposite things and must not be merged: taskStopped is an operator
// decision that the apply should durably rest in the stopped state until someone
// starts it again, while taskHandover is this process giving up the drive with
// the apply still active, to be reclaimed and resumed. Recording a handover as an
// operator stop would park every apply a shutdown interrupts.
const (
	taskContinue taskAction = iota // Task completed successfully, proceed to next
	taskFailed                     // Task failed, stop processing
	taskStopped                    // Task/apply was stopped by user, stop processing
	taskSkip                       // Task should be skipped (error fetching state)
	taskAbort                      // Current owner should exit without changing final state
	taskHandover                   // This drive's context was cancelled; the apply stays active for another driver to claim
)

// checkTaskReady verifies a task is ready to execute by checking context cancellation
// and re-fetching the task's current state from storage. The caller passes its
// identity-bound drive logger so these lines stay filterable by apply/PR.
func (c *LocalClient) checkTaskReady(ctx context.Context, logger *slog.Logger, task *storage.Task) taskAction {
	if ctx.Err() != nil {
		logger.Info("drive context cancelled before task start; handing the apply back for another driver to claim",
			"task_id", task.TaskIdentifier, "table", task.TableName)
		return taskHandover
	}
	freshTask, err := c.storage.Tasks().Get(ctx, task.TaskIdentifier)
	if err != nil {
		logger.Error("failed to fetch task state",
			"task_id", task.TaskIdentifier, "table", task.TableName, "state", task.State, "error", err)
		return taskSkip
	}
	if freshTask == nil {
		logger.Error("task not found",
			"task_id", task.TaskIdentifier, "table", task.TableName, "state", task.State)
		return taskSkip
	}
	if freshTask.State == state.Task.Stopped {
		logger.Info("task was stopped by user, skipping", "task_id", task.TaskIdentifier, "table", task.TableName)
		return taskStopped
	}
	if state.IsTerminalTaskState(freshTask.State) {
		logger.Info("task already in terminal state, skipping",
			"task_id", task.TaskIdentifier, "table", task.TableName, "state", freshTask.State)
		return taskSkip
	}
	return taskContinue
}

// sequentialEngineApplyRequest builds the engine request for one task in the
// sequential drive. A sharded task carries the shard it targets; propagate it so
// a sharded engine (Strata) receives exactly one target shard — without it the
// engine rejects the work with "expected exactly one target shard, got 0" even
// though the task is correctly shard-tagged. A non-sharded task has an empty
// shard and leaves both shard fields unset, unchanged. The grouped and resume
// drive paths already set TargetShards via taskTargetShards; this is the
// single-task path's equivalent. The identity-bound drive logger rides along
// so engine lines for this task inherit the apply's triage identity.
func sequentialEngineApplyRequest(task *storage.Task, options map[string]string, creds *engine.Credentials, logger *slog.Logger) *engine.ApplyRequest {
	change := engine.SchemaChange{
		Namespace:    task.Namespace,
		TableChanges: []engine.TableChange{{Table: task.TableName, DDL: task.DDL}},
	}
	if task.Shard != "" {
		change.Shard = engine.Shard{Name: task.Shard}
	}
	req := &engine.ApplyRequest{
		Database:    task.Database,
		Changes:     []engine.SchemaChange{change},
		Options:     options,
		ResumeState: &engine.ResumeState{MigrationContext: task.TaskIdentifier},
		Credentials: creds,
		Logger:      logger,
	}
	if task.Shard != "" {
		req.TargetShards = []string{task.Shard}
	}
	return req
}

// runEngineTask calls the engine for a single DDL, marks the task running, and polls to completion.
// Returns the outcome: taskContinue (completed), taskFailed, taskStopped, taskAbort, or taskHandover.
func (c *LocalClient) runEngineTask(ctx context.Context, apply *storage.Apply, task *storage.Task, options map[string]string) taskAction {
	logger := c.logger.With(apply.IdentityLogAttrs()...)
	if handled, err := c.processPendingCancelOrStopControlRequest(ctx, apply); err != nil {
		logger.Warn("pending stop request processing failed before sequential engine apply; current apply owner will exit for operator retry",
			"task_id", task.TaskIdentifier, "error", err)
		return taskAbort
	} else if handled {
		return taskStopped
	}
	taskCreds, err := c.credentialsForTask(task)
	if err != nil {
		c.markTaskFailed(ctx, task, err.Error())
		logger.Error("task failed to resolve namespace credentials",
			"task_id", task.TaskIdentifier, "table", task.TableName, "state", task.State, "namespace", task.Namespace, "error", err)
		return taskFailed
	}

	// Sequential mode: one DDL per engine call. The task identifier is used as the
	// engine resume key (ResumeState.MigrationContext) so each table's schema
	// change is tracked independently.
	result, err := c.getEngine().Apply(ctx, sequentialEngineApplyRequest(task, options, taskCreds, logger))

	if err != nil {
		if c.shouldRetryEngineError(err) {
			c.markTaskRetryable(ctx, task, err.Error())
		} else {
			c.markTaskFailed(ctx, task, err.Error())
		}
		logger.Error("task failed",
			"task_id", task.TaskIdentifier, "table", task.TableName, "state", task.State, "error", err)
		return taskFailed
	}
	if !result.Accepted {
		c.markTaskFailed(ctx, task, result.Message)
		logger.Error("task rejected",
			"task_id", task.TaskIdentifier, "table", task.TableName, "state", task.State, "engine_message", result.Message)
		return taskFailed
	}

	// Mark task running
	now := time.Now()
	task.StartedAt = &now
	c.transitionTaskState(ctx, task, 0, state.Task.Running, "")
	logger.Info("task running", "task_id", task.TaskIdentifier, "table", task.TableName)

	c.convergeTaskVolumeToStoredLevel(ctx, apply, task, taskCreds, result.ResumeState)

	// Poll to completion. Thread the engine's returned resume state into the
	// poll: a sharded engine (Strata) identifies the operation to report on from
	// ResumeState.Metadata and errors without it, so Progress must carry what
	// Apply returned. (Spirit reports per-database and returns no resume state, so
	// this stays nil and its behaviour is unchanged.)
	pollAction := c.pollTaskToCompletion(ctx, apply, task, taskCreds, result.ResumeState)
	if pollAction == taskAbort || pollAction == taskStopped || pollAction == taskHandover {
		return pollAction
	}

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

	// cutoverNotReadyEscalationAfter is how long consecutive not-ready cutover
	// rejections stay at Info before the drive escalates to Error logging and
	// records a timeline event. The window between a backend advertising the
	// cutover gate and accepting a cutover normally lasts seconds; a rejection
	// persisting this long means the backend is not staging the cutover and an
	// operator needs to investigate.
	cutoverNotReadyEscalationAfter = 2 * time.Minute

	// maxConsecutiveCutoverFailures is how many consecutive hard cutover
	// rejections the drive tolerates before settling the apply. The drive is
	// the sole cutover actor, so an unbounded retry would hold the database's
	// deploy queue and lock indefinitely; the bound mirrors the progress-poll
	// consecutive-error bound.
	maxConsecutiveCutoverFailures = 10
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

	// resumeEventLogged is set after this drive claim records the
	// engine-resumed-from-checkpoint timeline event, so the flag the engine
	// reports on every subsequent poll produces one event per claim.
	resumeEventLogged bool

	// cutoverTriggerLogged is set after the drive records the auto-cutover
	// trigger event, so retries of a not-yet-accepted cutover do not fill the
	// user-visible timeline with duplicate triggers.
	cutoverTriggerLogged bool

	// cutoverNotReadySince is when the engine backend first rejected an
	// auto-cutover as not ready; cleared when a cutover is accepted. Used to
	// escalate when the normally seconds-long staging window persists.
	cutoverNotReadySince time.Time

	// cutoverNotReadyEscalated is set once the not-ready window has outlived
	// cutoverNotReadyEscalationAfter and the timeline event was written, so
	// that event is recorded once while the Error log repeats every tick.
	cutoverNotReadyEscalated bool

	// consecutiveCutoverFailures tracks hard (not self-clearing) cutover
	// rejections so the drive settles the apply instead of retrying forever;
	// reset when a cutover is accepted or rejected as merely not ready.
	consecutiveCutoverFailures int

	// consecutiveErrors tracks progress poll failures to fail fast when the
	// engine is unreachable (e.g., branch deleted mid-apply).
	consecutiveErrors int

	// warnedPerShardUnavailable is set after the drive warns that a sharded
	// engine could not report per-shard/row-copy progress, so the warning is
	// emitted once per apply rather than on every poll.
	warnedPerShardUnavailable bool
}

// operationLeaseOnlyDrive reports the operation lease of a drive that holds an
// operation lease and no parent apply lease — the capability shape of an
// operation-scoped drive, which owns its operation row while the parent applies
// row belongs to the parent lease and the rollout projection. It reads the same
// signal ApplyStore.Heartbeat guards on, so a drive can never heartbeat a row
// storage would refuse to let it write.
func operationLeaseOnlyDrive(ctx context.Context) (storage.OperationLease, bool) {
	opLease, hasOpLease := storage.OperationLeaseFromContext(ctx)
	if !hasOpLease {
		return storage.OperationLease{}, false
	}
	if _, hasApplyLease := storage.ApplyLeaseFromContext(ctx); hasApplyLease {
		return storage.OperationLease{}, false
	}
	return opLease, true
}

// heartbeatDriveLease refreshes whichever lease keeps this drive claimed: an
// operation-scoped drive heartbeats its own operation row, every other drive
// heartbeats the parent applies row. Both renew the same staleness clock a peer
// driver reclaims work on, so the drive's liveness is reported wherever its
// claim actually lives.
func (c *LocalClient) heartbeatDriveLease(ctx context.Context, apply *storage.Apply) error {
	if opLease, operationScoped := operationLeaseOnlyDrive(ctx); operationScoped {
		return c.storage.ApplyOperations().Heartbeat(ctx, opLease.OperationID)
	}
	return c.storage.Applies().Heartbeat(ctx, apply.ID)
}

// startApplyHeartbeat starts a background goroutine that heartbeats the drive's
// claim on the client's heartbeat interval, preventing the operator from
// treating it as crashed. A definitively lost lease cancels the drive so the
// displaced driver stops executing. Transient heartbeat errors are retried on
// the next tick — until they have persisted for the full
// storage.ApplyLeaseStaleAfter window, at which point the lease is presumed
// lost (a peer operator can already have reclaimed the stale row) and the drive
// is cancelled the same way. Returns a cancel function that stops the
// heartbeat. Must be deferred by the caller.
func (c *LocalClient) startApplyHeartbeat(ctx context.Context, apply *storage.Apply, cancelApply ...context.CancelFunc) context.CancelFunc {
	hbCtx, cancel := context.WithCancel(ctx)
	stopDrive := func() {
		for _, cancel := range cancelApply {
			if cancel != nil {
				cancel()
			}
		}
		cancel()
	}
	_, operationScoped := operationLeaseOnlyDrive(ctx)
	go func() {
		ticker := time.NewTicker(c.heartbeatInterval)
		defer ticker.Stop()
		lastSuccess := time.Now()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				err := c.heartbeatDriveLease(hbCtx, apply)
				if err == nil {
					lastSuccess = time.Now()
					continue
				}
				if hbCtx.Err() != nil {
					// The drive is shutting down; the failed write is
					// cancellation fallout, not lease trouble.
					return
				}
				if c.applyHeartbeatFailureStopsDrive(hbCtx, apply, err, lastSuccess, operationScoped) {
					stopDrive()
					return
				}
			}
		}
	}()
	return cancel
}

// applyHeartbeatFailureStopsDrive classifies a failed drive heartbeat write and
// reports whether the driver must stop driving: either storage reported the
// lease definitively lost, or heartbeat failures have persisted since
// lastSuccess for the full lease staleness window, so a peer operator can
// already have reclaimed the stale row. A transient failure inside the window
// keeps the drive running and is retried on the next tick. operationScoped
// names which claim went stale in the logs — the two are reclaimed by different
// paths, so an operator triaging a displaced drive needs to know which.
func (c *LocalClient) applyHeartbeatFailureStopsDrive(ctx context.Context, apply *storage.Apply, hbErr error, lastSuccess time.Time, operationScoped bool) bool {
	logger := c.logger.With(apply.IdentityLogAttrs()...)
	logger = logger.With("heartbeat_scope", heartbeatScopeName(operationScoped))
	if errors.Is(hbErr, storage.ErrApplyLeaseLost) {
		logger.Warn("heartbeat failed because the drive's lease was lost; local driver will stop executing and writing apply state",
			append(apply.MutableLogAttrs(), "error", hbErr)...)
		metrics.RecordOperatorResumeFailure(ctx, apply.Database, apply.Deployment, apply.Environment, "lease_lost")
		return true
	}
	if time.Since(lastSuccess) >= storage.ApplyLeaseStaleAfter {
		logger.Warn("heartbeat has failed for the full lease staleness window; a peer operator can reclaim the work, so this local driver will stop executing and writing apply state",
			append(apply.MutableLogAttrs(), "last_successful_heartbeat", lastSuccess, "error", hbErr)...)
		metrics.RecordOperatorResumeFailure(ctx, apply.Database, apply.Deployment, apply.Environment, "lease_presumed_lost")
		return true
	}
	logger.Warn("heartbeat failed; will retry", append(apply.MutableLogAttrs(), "error", hbErr)...)
	return false
}

// heartbeatScopeName renders which row a drive heartbeats, for log triage.
func heartbeatScopeName(operationScoped bool) string {
	if operationScoped {
		return "apply_operation"
	}
	return "apply"
}

// pollForCompletionAtomic polls the engine for progress in atomic mode (all tasks share state).

// pollTaskToCompletion polls a single task to completion (sequential mode).
// Each poll tick persists the task row, even when no field moved: the operator
// reads tasks.updated_at as the drive's liveness signal (ApplyDriveStallAfter)
// and cancels a drive whose rows stop advancing, so the write must stay
// unconditional — including through parked states such as deferred cutovers
// and revert windows, where nothing changes tick to tick.
func (c *LocalClient) pollTaskToCompletion(ctx context.Context, apply *storage.Apply, task *storage.Task, creds *engine.Credentials, resumeState *engine.ResumeState) taskAction {
	eng := c.getEngine()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	logger := c.logger.With(apply.IdentityLogAttrs()...)

	var consecutiveErrors int
	var resumeEventLogged bool

	for {
		select {
		case <-ctx.Done():
			logger.Info("drive context cancelled while polling; handing the apply back for another driver to claim",
				"task_id", task.TaskIdentifier, "table", task.TableName)
			return taskHandover
		case <-ticker.C:
			if handled, err := c.processPendingCancelOrStopControlRequest(ctx, apply); err != nil {
				logger.Warn("pending stop request processing failed; current apply owner will exit for operator retry",
					"task_id", task.TaskIdentifier, "error", err)
				return taskAbort
			} else if handled {
				task.State = state.Task.Stopped
				return taskStopped
			}
			if err := c.processPendingCutoverControlRequest(ctx, apply); err != nil {
				logger.Warn("pending cutover request processing failed; current apply owner will exit for operator retry",
					"task_id", task.TaskIdentifier, "error", err)
				return taskAbort
			}
			// A volume failure never aborts the drive: the copy continues at
			// its current volume and a still-pending request is retried at the
			// next tick. A lost lease is the exception — this owner must stop
			// driving.
			if err := c.processPendingVolumeControlRequest(ctx, apply, eng, creds, resumeState); err != nil {
				if errors.Is(err, storage.ErrApplyLeaseLost) {
					logger.Warn("pending volume request processing lost the apply lease; current apply owner will exit for operator retry",
						"task_id", task.TaskIdentifier, "error", err)
					return taskAbort
				}
				logger.Warn("pending volume request processing failed; the drive continues and retries at the next progress tick",
					"task_id", task.TaskIdentifier, "error", err)
			}

			// Re-fetch task state from storage to detect external changes (e.g., Stop).
			// This also guards against a race where a new apply starts and the engine's
			// runningSchemaChange no longer corresponds to this task.
			freshTask, fetchErr := c.storage.Tasks().Get(ctx, task.TaskIdentifier)
			if fetchErr == nil && freshTask != nil && state.IsTerminalTaskState(freshTask.State) {
				// Task was already marked terminal externally — stop polling
				task.State = freshTask.State
				return taskContinue
			}

			result, err := eng.Progress(ctx, &engine.ProgressRequest{
				Database:    task.Database,
				Credentials: creds,
				ResumeState: resumeState,
			})
			if err != nil {
				// A permanent error can never succeed on retry, so fail immediately
				// rather than waiting out the consecutive-error budget — matching the
				// grouped poll.
				var permanent *engine.PermanentError
				if errors.As(err, &permanent) {
					logger.Error("progress check failed with permanent error",
						"task_id", task.TaskIdentifier, "table", task.TableName, "error", err)
					c.markTaskFailed(ctx, task, fmt.Sprintf("progress polling failed: %v", err))
					return taskFailed
				}
				// A transient poll that nonetheless never succeeds must not spin
				// forever: an apply that cannot reach a terminal state holds the
				// database's active-apply slot and blocks every later apply. Fail
				// after a bounded run of errors, matching the grouped poll.
				consecutiveErrors++
				logger.Warn("progress check failed",
					"task_id", task.TaskIdentifier, "table", task.TableName, "error", err, "consecutive_errors", consecutiveErrors)
				if consecutiveErrors >= 10 {
					if c.shouldRetryEngineError(err) {
						c.markTaskRetryable(ctx, task, fmt.Sprintf("progress polling failed after %d consecutive errors: %v", consecutiveErrors, err))
						return taskFailed
					}
					c.markTaskFailed(ctx, task, fmt.Sprintf("progress polling failed after %d consecutive errors: %v", consecutiveErrors, err))
					return taskFailed
				}
				continue
			}
			consecutiveErrors = 0
			c.logEngineResumeOnce(ctx, logger, apply, result.ResumedFromCheckpoint, &resumeEventLogged)

			now := time.Now()
			prevState := task.State
			engineTaskState := taskStateFromProgressResult(result)
			// A sequential task drives a single DDL, so the first table's
			// engine-reported post-copy phase (catching up, checksumming,
			// post-checksum, cutting over) refines a running task the same way
			// the grouped sync does. The no-backward guard keeps the displayed
			// endgame monotonic across engine phases that map back to Running.
			if len(result.Tables) > 0 && state.IsState(engineTaskState, state.Task.Running) {
				if phase, isPhase := tablePhaseTaskState(result.Tables[0].State); isPhase {
					engineTaskState = phase
				}
			}
			task.State = taskStateWithNoBackwardProgress(prevState, engineTaskState)
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
				task.ChecksumRowsChecked = tp.ChecksumRowsChecked
				task.ChecksumRowsTotal = tp.ChecksumRowsTotal
				task.Throttled = tp.Throttled
				task.ThrottleReason = tp.ThrottleReason
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
				logger.Info("task finished",
					"task_id", task.TaskIdentifier,
					"table", task.TableName,
					"engine_state", result.State,
					"engine_message", result.Message,
					"prev_storage_state", prevState,
					"rows_copied", task.RowsCopied,
					"rows_total", task.RowsTotal,
				)
				return taskContinue
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

// markTaskRetryable records a task failure that operator recovery may retry.
func (c *LocalClient) markTaskRetryable(ctx context.Context, task *storage.Task, errMsg string) {
	task.ErrorMessage = errMsg
	task.CompletedAt = nil
	c.transitionTaskState(ctx, task, 0, state.Task.FailedRetryable, "")
}

// shouldRetryEngineError decides whether an engine error pauses the apply as
// failed_retryable for operator recovery instead of failing it permanently.
func (c *LocalClient) shouldRetryEngineError(err error) bool {
	return recoveryResumesFromCheckpoint(c.config.Type) && engine.IsRetryable(err)
}

// recoveryResumesFromCheckpoint reports whether the database type's engine can
// safely retry a failed attempt: a re-claimed recovery attempt resumes the same
// in-flight work from the engine's durable checkpoint rather than issuing new
// external work. MySQL drives Spirit, which resumes from its checkpoint
// tables. Strata engines are embedder-registered, so the invariant is a
// contract on the registration: an engine registered for Strata must resume
// its in-flight work from a durable checkpoint on a recovery claim, never
// redispatch it — an embedder whose engine cannot honor that must not classify
// its errors retryable. Vitess stays out: its applies are deploy requests on
// an external provider, where an error-path retry could dispatch duplicate
// work — its recovery reconciles through resume state instead. Postgres stays
// out because its engine classifies transient versus permanent failures in its
// progress results, not through errors.
func recoveryResumesFromCheckpoint(databaseType string) bool {
	switch databaseType {
	case storage.DatabaseTypeMySQL, storage.DatabaseTypeStrata:
		return true
	case storage.DatabaseTypeVitess, storage.DatabaseTypePostgres:
		return false
	default:
		// LocalConfig.Type is open-world: embedder-registered engine types
		// (EngineFactories) and the zero-value type used by tests land here
		// and get the conservative disposition — no checkpoint resume, so a
		// failed attempt fails the apply instead of pausing it for a retry
		// that could dispatch duplicate work. Nothing pins the checkpoint
		// contract on those registrations.
		return false
	}
}

// failApplyWithTasks marks all tasks and the apply as failed with the given error.
// If the apply is already in a terminal state (e.g., cancelled by Stop()), the
// apply state is not overwritten.

// finalizeSequentialApply updates the apply state based on sequential task outcomes.
// Permanent failures cancel remaining pending tasks; retryable failures leave
// pending tasks queued for operator recovery.
func (c *LocalClient) finalizeSequentialApply(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, failedTask *storage.Task, stoppedByUser bool) {
	now := time.Now()
	logger := c.logger.With(apply.IdentityLogAttrs()...)
	// A multi-operation drive owns only its operation: the tasks it drove carry
	// the outcome, the operator derives the operation row from them and projects
	// the parent, so the parent terminal write, control-request completion,
	// apply-level metric, and terminal observer are all the operator's to make.
	// Pending tasks after a failed one are still this drive's to settle, and the
	// in-memory outcome is still adopted so the drive's own logs report what the
	// operation settled to rather than the projection's stale running state.
	if suppressParentApplyWrites(ctx) {
		if failedTask != nil && failedTask.State != state.Task.FailedRetryable {
			for _, task := range tasks {
				if task.State == state.Task.Pending {
					c.transitionTaskState(ctx, task, 0, state.Task.Cancelled, "")
				}
			}
		}
		adoptSequentialOutcome(apply, failedTask, stoppedByUser, now)
		logger.Info("sequential operation drive settled; operator derives the operation row and projects the parent",
			"stopped_by_user", stoppedByUser, "failed_task", failedTask != nil, "settled_state", apply.State)
		return
	}
	if freshApply, err := c.storage.Applies().Get(ctx, apply.ID); err != nil {
		logger.Error("failed to reload apply before sequential finalization",
			append(apply.MutableLogAttrs(), "error", err)...)
		return
	} else if freshApply != nil && state.IsTerminalApplyState(freshApply.State) {
		logger.Info("apply already terminal in storage, not overwriting during sequential finalization",
			"stored_state", freshApply.State)
		*apply = *freshApply
		if err := completePendingRequestsForTerminalApply(ctx, c.storage, apply); err != nil {
			logger.Warn("failed to complete pending control requests for terminal sequential apply",
				"error", err)
		}
		return
	}
	previousState := apply.State
	if failedTask != nil && failedTask.State != state.Task.FailedRetryable {
		for _, task := range tasks {
			if task.State == state.Task.Pending {
				c.transitionTaskState(ctx, task, 0, state.Task.Cancelled, "")
			}
		}
	}
	adoptSequentialOutcome(apply, failedTask, stoppedByUser, now)
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		logger.Error("failed to update apply state", append(apply.MutableLogAttrs(), "error", err)...)
	} else {
		// A sequential apply's failure reaches the operator through the same
		// apply log stream as every other path, so a failed table does not read
		// as an apply that went terminal for no stated reason.
		switch apply.State {
		case state.Apply.Failed:
			c.logApplyFailure(ctx, apply, previousState, apply.ErrorMessage)
		case state.Apply.FailedRetryable:
			c.logApplyPausedForRetry(ctx, apply, previousState, apply.ErrorMessage)
		}
	}
	if state.IsTerminalApplyState(apply.State) {
		if err := completePendingRequestsForTerminalApply(ctx, c.storage, apply); err != nil {
			logger.Warn("failed to complete pending control requests after sequential finalization",
				append(apply.MutableLogAttrs(), "error", err)...)
			return
		}
	}
	metrics.AdjustActiveApplies(ctx, -1, apply.Database, apply.Deployment, apply.Environment)

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

// adoptSequentialOutcome mutates the in-memory apply to the outcome its
// sequential task results settle to: failed_retryable for a retryable task
// failure, failed for a permanent one, stopped for an operator stop, completed
// otherwise. Callers own persisting (or not persisting) the mutated row.
func adoptSequentialOutcome(apply *storage.Apply, failedTask *storage.Task, stoppedByUser bool, now time.Time) {
	switch {
	case failedTask != nil && failedTask.State == state.Task.FailedRetryable:
		apply.State = state.Apply.FailedRetryable
		apply.ErrorMessage = fmt.Sprintf("table %s failed: %s", failedTask.TableName, failedTask.ErrorMessage)
		apply.CompletedAt = nil
	case failedTask != nil:
		apply.State = state.Apply.Failed
		apply.ErrorMessage = fmt.Sprintf("table %s failed: %s", failedTask.TableName, failedTask.ErrorMessage)
		apply.CompletedAt = &now
	case stoppedByUser:
		apply.State = state.Apply.Stopped
	default:
		apply.State = state.Apply.Completed
		apply.CompletedAt = &now
	}
	apply.UpdatedAt = now
}
