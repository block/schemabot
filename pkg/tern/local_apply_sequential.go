package tern

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// executeApplySequential runs each DDL as a separate Spirit call (independent mode).
// Each table copies and cuts over independently.
func (c *LocalClient) executeApplySequential(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, plan *storage.Plan, options map[string]string) error {
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

	if !c.recordDriveStarted(ctx, apply, logger) {
		return nil
	}

	var failedTask *storage.Task
	var stoppedByUser bool

	for i, task := range tasks {
		if standDown, err := c.processPendingCancelOrStopControlRequest(ctx, apply); err != nil {
			logger.Warn("pending stop request processing failed; current apply owner will exit for operator retry",
				"error", err)
			return nil
		} else if standDown {
			stoppedByUser = true
			break
		}

		action := c.checkTaskReady(ctx, logger, task)
		if action == taskHandover {
			return nil
		}
		if action == taskAbort {
			return nil
		}
		if action == taskMissing {
			// A fresh drive has no operator projection to refuse, so exiting
			// without a verdict is the whole fail-closed behaviour here; the
			// operator's resume of the still-active apply reports the cause.
			return nil
		}
		if action == taskStopped {
			stoppedByUser = true
			break
		}
		if action == taskAlreadyFailed {
			failedTask = task
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
			return nil
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
	if err := c.finalizeSequentialApply(ctx, apply, tasks, failedTask, stoppedByUser); err != nil {
		logger.Error("sequential drive exiting without finalizing the apply; it stays active for a later claim to finalize, so its pending control requests stay pending and no terminal summary is posted",
			"deployment", apply.Deployment, "error", err)
		return err
	}
	logger.Info("sequential apply finished", "state", apply.State)
	return nil
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
	taskContinue      taskAction = iota // Task completed successfully, proceed to next
	taskFailed                          // Task failed, stop processing
	taskStopped                         // Task/apply was stopped by user, stop processing
	taskSkip                            // Task is already terminal in storage; move on to the next
	taskAlreadyFailed                   // A previous drive failed the task permanently; it owns the apply's verdict, stop processing
	taskAbort                           // Current owner should exit without changing final state
	taskHandover                        // This drive's context was cancelled; the apply stays active for another driver to claim
	taskMissing                         // The task's row is gone; the drive exits without a verdict and reports the apply undriveable
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
	// The re-read decides whether this task's DDL runs, so a read that cannot
	// answer ends the drive attempt without finalizing: skipping the task would
	// let finalization record the apply completed with this task's DDL never run.
	// A storage error leaves the apply active for a later drive to re-read the
	// task and run it. A missing row is reported as undriveable, because the rows
	// that remain would otherwise derive a verdict this task never earned.
	freshTask, err := c.storage.Tasks().Get(ctx, task.TaskIdentifier)
	if err != nil {
		if ctx.Err() != nil {
			logger.Info("drive context cancelled before task start; handing the apply back for another driver to claim",
				"task_id", task.TaskIdentifier, "table", task.TableName)
			return taskHandover
		}
		logger.Error("re-reading task state before start failed; current apply owner will exit for operator retry",
			"task_id", task.TaskIdentifier, "table", task.TableName, "state", task.State, "error", err)
		return taskAbort
	}
	if freshTask == nil {
		logger.Error("task row not found when re-reading it before start; the apply is undriveable and stays claimable until the row is restored or an operator intervenes",
			"task_id", task.TaskIdentifier, "table", task.TableName, "state", task.State)
		return taskMissing
	}
	if freshTask.State == state.Task.Stopped {
		logger.Info("task was stopped by user, skipping", "task_id", task.TaskIdentifier, "table", task.TableName)
		return taskStopped
	}
	// A task a previous drive failed permanently still owns the apply's
	// verdict: skipping it would let finalization derive the apply's outcome
	// from the tasks that remain and record a failed schema change completed.
	// Finalization reads the verdict from the task it is handed, so the
	// in-memory row adopts the stored failure.
	if state.IsState(freshTask.State, state.Task.Failed) {
		logger.Info("task already failed permanently in a previous drive; the apply keeps that verdict",
			"task_id", task.TaskIdentifier, "table", task.TableName, "error", freshTask.ErrorMessage)
		task.State = freshTask.State
		task.ErrorMessage = freshTask.ErrorMessage
		return taskAlreadyFailed
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
	if standDown, err := c.processPendingCancelOrStopControlRequest(ctx, apply); err != nil {
		logger.Warn("pending stop request processing failed before sequential engine apply; current apply owner will exit for operator retry",
			"task_id", task.TaskIdentifier, "error", err)
		return taskAbort
	} else if standDown {
		return taskStopped
	}
	taskCreds, err := c.credentialsForTask(task)
	if err != nil {
		logger.Error("task failed to resolve namespace credentials",
			"task_id", task.TaskIdentifier, "table", task.TableName, "state", task.State, "namespace", task.Namespace, "error", err)
		return c.failureVerdictAction(apply.ApplyIdentifier, task, c.markTaskFailed(ctx, task, err.Error()))
	}

	// Sequential mode: one DDL per engine call. The task identifier is used as the
	// engine resume key (ResumeState.MigrationContext) so each table's schema
	// change is tracked independently.
	request := sequentialEngineApplyRequest(task, options, taskCreds, logger)
	needsFiles, err := c.taskNeedsRowSecurityFiles(task)
	if err != nil {
		return c.failureVerdictAction(apply.ApplyIdentifier, task, c.markTaskFailed(ctx, task, fmt.Sprintf("resolve row security parser: %v", err)))
	}
	if needsFiles {
		planID, err := planIDForTasks(apply, []*storage.Task{task})
		if err != nil {
			return c.failureVerdictAction(apply.ApplyIdentifier, task, c.markTaskFailed(ctx, task, fmt.Sprintf("resolve desired schema plan for row security apply: %v", err)))
		}
		plan, err := c.storage.Plans().GetByID(ctx, planID)
		if err != nil {
			return c.failureVerdictAction(apply.ApplyIdentifier, task, c.markTaskFailed(ctx, task, fmt.Sprintf("load desired schema for row security apply: %v", err)))
		}
		if plan == nil {
			return c.failureVerdictAction(apply.ApplyIdentifier, task, c.markTaskFailed(ctx, task, "row security apply has no stored desired schema plan"))
		}
		request.PlanID = plan.PlanIdentifier
		request.SchemaFiles = plan.SchemaFiles
	}
	result, err := c.applyWithEngine(ctx, c.getEngine(), request)

	if err != nil {
		var verdictErr error
		if c.shouldRetryEngineError(err) {
			verdictErr = c.markTaskRetryable(ctx, task, err.Error())
		} else {
			verdictErr = c.markTaskFailed(ctx, task, err.Error())
		}
		logger.Error("task failed",
			"task_id", task.TaskIdentifier, "table", task.TableName, "state", task.State, "error", err)
		return c.failureVerdictAction(apply.ApplyIdentifier, task, verdictErr)
	}
	if !result.Accepted {
		verdictErr := c.markTaskFailed(ctx, task, result.Message)
		logger.Error("task rejected",
			"task_id", task.TaskIdentifier, "table", task.TableName, "state", task.State, "engine_message", result.Message)
		return c.failureVerdictAction(apply.ApplyIdentifier, task, verdictErr)
	}

	// Mark task running
	now := time.Now()
	task.StartedAt = &now
	c.transitionTaskState(ctx, task, 0, state.Task.Running, "")
	logger.Info("task running", "task_id", task.TaskIdentifier, "table", task.TableName)

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

	// autoSkipRevertEscalationAfter is how long a failing automatic skip-revert
	// (--skip-revert or revert window expiry) stays at Warn before the drive
	// escalates to Error logging and records a timeline event. The engine
	// normally accepts a skip on the first attempt or after a transient busy
	// rejection clears within seconds; a failure persisting this long means
	// the revert window is not closing and an operator needs to investigate.
	autoSkipRevertEscalationAfter = 2 * time.Minute

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
	terminalErr     error

	// stateEnteredAt tracks when the current waiting state was entered,
	// used for timeout enforcement on deferred cutover and revert window.
	stateEnteredAt time.Time

	// revertSkipped is set once the engine accepts SkipRevert, so the drive
	// stops re-attempting it and surfaces skipping_revert while the engine
	// finalizes. A rejected attempt leaves it unset for the next tick to retry.
	revertSkipped bool

	// revertTriggered is set once this drive has seen that the engine accepted
	// an operator revert, either by carrying the revert out itself or by
	// reading the completed durable revert request. It is a shortcut over that
	// durable record, which stays the source of truth, and suppresses
	// skip-revert attempts while progress still reports the lagging
	// revert-window state.
	revertTriggered bool

	// autoSkipRevertLogged is set after the drive records the automatic
	// skip-revert trigger event (--skip-revert or revert window expiry), so
	// retries of a rejected skip do not fill the user-visible timeline with
	// duplicate triggers.
	autoSkipRevertLogged bool

	// autoSkipRevertFailingSince is when the engine first failed or rejected
	// the automatic skip-revert this drive keeps retrying. Used to escalate
	// once the failure has persisted past autoSkipRevertEscalationAfter.
	autoSkipRevertFailingSince time.Time

	// autoSkipRevertEscalated is set once the persisting skip-revert failure
	// has been recorded on the timeline, so the escalation lands there once.
	autoSkipRevertEscalated bool

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

	// lostWork measures how long the engine has been reporting no active
	// schema change while stored tasks say the work is in flight, so the drive
	// can stop trusting the engine and settle the tasks from the target
	// schema. One tracker covers the whole apply: grouped work is a single
	// engine operation, so the engine loses or keeps all of it together.
	lostWork lostEngineWorkTracker

	// warnedPerShardUnavailable is set after the drive warns that a sharded
	// engine could not report per-shard/row-copy progress, so the warning is
	// emitted once per apply rather than on every poll.
	warnedPerShardUnavailable bool

	lastProgressMetadata      map[string]string
	progressMetadataLeaseLost bool
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

const (
	// defaultTaskPollInterval is the cadence of a drive's engine progress
	// polls, shared by the sequential and grouped polls.
	defaultTaskPollInterval = 500 * time.Millisecond

	// maxConsecutiveProgressPollErrors bounds how many consecutive progress
	// poll failures a drive tolerates before settling its work, shared by the
	// sequential and grouped polls. An apply that cannot reach a terminal
	// state holds the database's active-apply slot and blocks every later
	// apply.
	maxConsecutiveProgressPollErrors = 10

	// defaultLostEngineWorkPendingBudget is how long an engine that provisions
	// resources after accepting work may keep reporting no active schema change
	// for a task whose stored state says the work is already in flight, before
	// the drive stops trusting the engine and verifies the target schema
	// directly. An engine declaring engine.SynchronousWorkRegistration needs no
	// budget at all, because a pending report is conclusive the first time it
	// arrives.
	//
	// The budget covers what a pending report cannot distinguish on its own: an
	// engine still cutting a branch or validating a deploy request is reporting
	// real, healthy work it has not begun executing, and an engine that just
	// restarted serves a stale snapshot until it catches up. Both resolve on
	// their own schedule, so it is a duration rather than a count of polls —
	// what it has to outlast is wall-clock engine behaviour, not a number of
	// round trips, and a poll-count budget silently shrinks to nothing whenever
	// the poll cadence is shortened. It is generous because distrusting a
	// provisioning engine too early bounces a healthy apply to retryable and
	// re-drives it, which on an engine that provisions per attempt is worse
	// than waiting, and it stays inside defaultTaskStallWarnInterval so the
	// drive resolves the ambiguity before an operator is told the task looks
	// stalled.
	defaultLostEngineWorkPendingBudget = 2 * time.Minute

	// defaultTaskStallWarnInterval is how long a polled task may sit in the
	// same stored state with unchanged progress fields before the drive warns
	// that the task looks stalled. The warning repeats once per interval, not
	// per poll, so a genuinely stuck task stays visible without flooding logs.
	defaultTaskStallWarnInterval = 5 * time.Minute
)

// taskPollInterval returns a drive's progress poll cadence, shared by the
// sequential and grouped polls.
func (c *LocalClient) taskPollInterval() time.Duration {
	if c.taskPollIntervalOverride > 0 {
		return c.taskPollIntervalOverride
	}
	return defaultTaskPollInterval
}

// taskStallWarnInterval returns how long a polled task may show no state or
// progress movement before the drive warns.
func (c *LocalClient) taskStallWarnInterval() time.Duration {
	if c.taskStallWarnIntervalOverride > 0 {
		return c.taskStallWarnIntervalOverride
	}
	return defaultTaskStallWarnInterval
}

// lostEngineWorkPendingBudget returns how long the drive keeps trusting eng
// when it reports no active schema change for an in-flight task. An engine that
// registers accepted work synchronously gets no budget: it has no
// pending-but-healthy phase to wait out, so the first such report is already
// conclusive. Every other engine gets the full budget, which is the safe
// default — an engine that has not declared itself is assumed to provision
// after accepting work.
func (c *LocalClient) lostEngineWorkPendingBudget(eng engine.Engine) time.Duration {
	if c.lostEngineWorkPendingBudgetOverride > 0 {
		return c.lostEngineWorkPendingBudgetOverride
	}
	if engine.RegistersWorkSynchronously(eng) {
		return 0
	}
	return defaultLostEngineWorkPendingBudget
}

// pollTaskToCompletion polls a single task to completion (sequential mode).
// Each poll tick persists the task row, even when no field moved: the operator
// reads tasks.updated_at as the drive's liveness signal (ApplyDriveStallAfter)
// and cancels a drive whose rows stop advancing, so the write must stay
// unconditional — including through parked states such as deferred cutovers
// and revert windows, where nothing changes tick to tick.
func (c *LocalClient) pollTaskToCompletion(ctx context.Context, apply *storage.Apply, task *storage.Task, creds *engine.Credentials, resumeState *engine.ResumeState) taskAction {
	eng := c.getEngine()
	ticker := time.NewTicker(c.taskPollInterval())
	defer ticker.Stop()
	logger := c.logger.With(apply.IdentityLogAttrs()...)

	var consecutiveErrors int
	// terminalWriteFailures counts consecutive failed writes of the task's
	// terminal state. It is separate from consecutiveErrors because every
	// successful poll resets that one, and the poll that finds the task
	// terminal is itself successful.
	var terminalWriteFailures int
	var resumeEventLogged bool
	var lastProgressMetadata map[string]string
	var progressMetadataLeaseLost bool
	lostWork := lostEngineWorkTracker{budget: c.lostEngineWorkPendingBudget(eng)}
	watchdog := taskStallWatchdog{interval: c.taskStallWarnInterval()}

	for {
		select {
		case <-ctx.Done():
			logger.Info("drive context cancelled while polling; handing the apply back for another driver to claim",
				"task_id", task.TaskIdentifier, "table", task.TableName)
			return taskHandover
		case <-ticker.C:
			if standDown, err := c.processPendingCancelOrStopControlRequest(ctx, apply); err != nil {
				logger.Warn("pending stop request processing failed; current apply owner will exit for operator retry",
					"task_id", task.TaskIdentifier, "error", err)
				return taskAbort
			} else if standDown {
				task.State = state.Task.Stopped
				return taskStopped
			}
			if err := c.processPendingCutoverControlRequest(ctx, apply, []*storage.Task{task}); err != nil {
				logger.Warn("pending cutover request processing failed; current apply owner will exit for operator retry",
					"task_id", task.TaskIdentifier, "error", err)
				return taskAbort
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

			result, err := c.progressWithEngine(ctx, eng, &engine.ProgressRequest{
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
					return c.failureVerdictAction(apply.ApplyIdentifier, task, c.markTaskFailed(ctx, task, fmt.Sprintf("progress polling failed: %v", err)))
				}
				// A transient poll that nonetheless never succeeds must not spin
				// forever: an apply that cannot reach a terminal state holds the
				// database's active-apply slot and blocks every later apply. Fail
				// after a bounded run of errors, matching the grouped poll.
				consecutiveErrors++
				logger.Warn("progress check failed",
					"task_id", task.TaskIdentifier, "table", task.TableName, "error", err, "consecutive_errors", consecutiveErrors)
				if consecutiveErrors >= maxConsecutiveProgressPollErrors {
					if c.shouldRetryEngineError(err) {
						return c.failureVerdictAction(apply.ApplyIdentifier, task, c.markTaskRetryable(ctx, task, fmt.Sprintf("progress polling failed after %d consecutive errors: %v", consecutiveErrors, err)))
					}
					return c.failureVerdictAction(apply.ApplyIdentifier, task, c.markTaskFailed(ctx, task, fmt.Sprintf("progress polling failed after %d consecutive errors: %v", consecutiveErrors, err)))
				}
				continue
			}
			prevState := task.State
			prevCompletedAt := task.CompletedAt
			// A sequential task drives a single DDL, so its progress is the
			// first table's: the same projection the grouped sync applies per
			// task, with the same refinement of a running task into the
			// engine-reported post-copy phase. The no-backward guard keeps the
			// displayed endgame monotonic across engine phases that map back to
			// Running.
			var tp *engine.TableProgress
			if len(result.Tables) > 0 {
				tp = &result.Tables[0]
				c.unrecognizedStatuses.observeTaskStatus(ctx, logger, task, tp.State)
			}
			engineTaskState := engineTaskStateClaim(taskStateFromProgressResult(result), tp)

			now := time.Now()
			if engineReportsLostWork(prevState, engineTaskState) {
				pendingFor, exhausted := lostWork.observePending(now)
				if exhausted {
					action, settleErr := c.settleLostEngineWork(ctx, apply, task, result.State)
					if settleErr == nil {
						return action
					}
					if errors.Is(settleErr, storage.ErrApplyLeaseLost) {
						// The target answered; only the settlement write was
						// refused, because a peer now holds the lease. That
						// peer settles the task, so this driver exits rather
						// than counting the refusal as a failed verification
						// and going on to rest and finalize an apply it no
						// longer owns.
						c.logger.Warn("settling lost engine work was refused because the drive's lease was lost; this driver exits and starts no further task",
							append(task.LogAttrs(), "apply_id", apply.ApplyIdentifier, "engine_state", result.State, "error", settleErr)...)
						return taskAbort
					}
					// Neither the engine nor the target has answered what
					// happened to the work, so count the failed verification
					// against the same bounded error budget as a failed poll —
					// this must never become an unbounded loop.
					consecutiveErrors++
					c.logger.Warn("engine reports no active schema change for an in-flight task and target verification failed; the drive re-verifies at the next poll",
						append(task.LogAttrs(), "apply_id", apply.ApplyIdentifier, "engine_state", result.State, "consecutive_errors", consecutiveErrors, "error", settleErr)...)
					if consecutiveErrors >= maxConsecutiveProgressPollErrors {
						return c.failureVerdictAction(apply.ApplyIdentifier, task, c.markTaskRetryable(ctx, task, fmt.Sprintf("engine reports no active schema change for an in-flight task and the target could not be verified; %d consecutive errors across progress polls and target verification; see server logs", consecutiveErrors)))
					}
					continue
				}
				// Inside the budget the engine is still trusted: it may be
				// serving a stale snapshot after a restart, or reporting
				// pending for real work it has not begun executing yet.
				c.logger.Debug("engine reports no active schema change for an in-flight task; still inside the trust budget",
					append(task.LogAttrs(), "apply_id", apply.ApplyIdentifier, "engine_state", result.State, "pending_for", pendingFor.Round(time.Second))...)
			} else {
				lostWork.reset()
			}

			consecutiveErrors = 0
			c.logEngineResumeOnce(ctx, logger, apply, result.ResumedFromCheckpoint, &resumeEventLogged)
			var saveErr error
			lastProgressMetadata, saveErr = c.persistProgressMetadataIfChanged(lastProgressMetadata, result.Metadata, &progressMetadataLeaseLost, func(metadata map[string]string) error {
				return c.saveProgressMetadata(ctx, task, metadata)
			})
			if errors.Is(saveErr, storage.ErrApplyLeaseLost) {
				logger.Debug("progress metadata persistence stopped because the operation lease was lost during failover",
					append(task.LogAttrs(), "error", saveErr)...)
			} else if saveErr != nil {
				logger.Warn("failed to persist engine progress metadata; the drive will retry on the next poll",
					append(task.LogAttrs(), "error", saveErr)...)
			}

			task.State = taskStateWithNoBackwardProgress(prevState, engineTaskState)
			task.UpdatedAt = now
			retryableFailure := state.IsState(task.State, state.Task.FailedRetryable)

			if tp != nil {
				applyEngineTableDisplayFields(task, tp)
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
				// The drive moves on to the next task only once this one's
				// outcome is durable: a later task's DDL must never run while
				// storage still records this one in flight, and the apply must
				// never finalize over a task row that never settled.
				terminalState := task.State
				if err := c.persistTaskStateTransition(ctx, task, task.ApplyID, terminalState, logMsg); err != nil {
					task.State = prevState
					task.CompletedAt = prevCompletedAt
					attrs := append(task.LogAttrs(), "apply_id", apply.ApplyIdentifier, "target_state", terminalState, "engine_state", result.State)
					if errors.Is(err, storage.ErrApplyLeaseLost) {
						c.logger.Warn("task finished on the engine but its terminal state was refused because the drive's lease was lost; this driver exits and starts no further task",
							append(attrs, "error", err)...)
						return taskAbort
					}
					terminalWriteFailures++
					if terminalWriteFailures >= maxConsecutiveProgressPollErrors {
						c.logger.Error("task finished on the engine but its terminal state could not be persisted; this driver exits without starting further tasks and leaves the apply for a later drive",
							append(attrs, "consecutive_write_failures", terminalWriteFailures, "error", err)...)
						return taskAbort
					}
					c.logger.Warn("task finished on the engine but persisting its terminal state failed; the drive retries the write at the next poll and starts no further task until it lands",
						append(attrs, "consecutive_write_failures", terminalWriteFailures, "error", err)...)
					continue
				}
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

			// The watchdog observes every poll so entering or leaving a state
			// always restarts its clock, but a task parked at a gate is
			// motionless by design and warning about it would fire on every
			// deferred cutover and deferred deploy for as long as the operator
			// takes to act.
			if stalledFor, warn := watchdog.observe(now, taskProgressSnapshotOf(task)); warn && !taskWaitsForOperatorAction(task.State) {
				// Throttle state answers the first triage question about a
				// motionless task — is the engine deliberately holding back —
				// without a trip to the task row.
				c.logger.Warn("task state and progress fields are unchanged past the stall-warning interval; the drive continues polling the engine",
					append(task.LogAttrs(), "apply_id", apply.ApplyIdentifier, "stalled_for", stalledFor.Round(time.Second), "engine_state", result.State, "throttled", task.Throttled, "throttle_reason", task.ThrottleReason)...)
			}

			polledState := task.State
			if err := c.persistTaskStateTransition(ctx, task, 0, polledState, ""); err != nil {
				task.State = prevState
				attrs := append(task.LogAttrs(), "apply_id", apply.ApplyIdentifier, "target_state", polledState, "engine_state", result.State)
				if errors.Is(err, storage.ErrApplyLeaseLost) {
					c.logger.Warn("task progress write was refused because the drive's lease was lost; this driver exits and starts no further task",
						append(attrs, "error", err)...)
					return taskAbort
				}
				c.logger.Warn("failed to persist task progress; the drive retries the write at the next poll",
					append(attrs, "error", err)...)
				continue
			}

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

// markTaskFailed sets a task to FAILED state with the given error message and
// persists it. A write that does not land is returned with the task left as it
// arrived, so no caller reads a failure verdict storage never recorded.
func (c *LocalClient) markTaskFailed(ctx context.Context, task *storage.Task, errMsg string) error {
	previous := *task
	now := time.Now()
	task.ErrorMessage = errMsg
	task.CompletedAt = &now
	if err := c.persistTaskStateTransition(ctx, task, 0, state.Task.Failed, ""); err != nil {
		*task = previous
		return fmt.Errorf("persist failed verdict for task %s: %w", task.TaskIdentifier, err)
	}
	return nil
}

// markTaskRetryable records a task failure that operator recovery may retry.
// A write that does not land is returned with the task left as it arrived.
func (c *LocalClient) markTaskRetryable(ctx context.Context, task *storage.Task, errMsg string) error {
	previous := *task
	task.ErrorMessage = errMsg
	task.CompletedAt = nil
	if err := c.persistTaskStateTransition(ctx, task, 0, state.Task.FailedRetryable, ""); err != nil {
		*task = previous
		return fmt.Errorf("persist retryable verdict for task %s: %w", task.TaskIdentifier, err)
	}
	return nil
}

// failureVerdictAction returns the drive's next step after it tried to record a
// task failure. Finalization derives the apply's outcome from the failed task,
// so the drive finalizes only over a verdict that landed: a refused write leaves
// the task row in its previous state, and finalizing over it would settle the
// apply from a verdict storage never recorded, reading a retryable failure as
// permanent and cancelling the tasks queued behind it. The drive exits instead,
// with the apply still active for a later claim to re-drive the task.
//
// The decision depends only on whether the write landed; the apply identifier
// is taken rather than the apply so the signature says so.
func (c *LocalClient) failureVerdictAction(applyIdentifier string, task *storage.Task, verdictErr error) taskAction {
	if verdictErr == nil {
		return taskFailed
	}
	attrs := append(task.LogAttrs(), "apply_id", applyIdentifier, "error", verdictErr)
	if errors.Is(verdictErr, storage.ErrApplyLeaseLost) {
		c.logger.Warn("task failure verdict was refused because the drive's lease was lost; this driver exits and starts no further task", attrs...)
		return taskAbort
	}
	c.logger.Error("task failure verdict could not be persisted; this driver exits without finalizing and leaves the apply active for a later drive", attrs...)
	return taskAbort
}

// engineReportsLostWork reports whether a successful progress poll came back
// with no active schema change (a state that maps to pending) for a task whose
// stored state says the work is already in flight.
//
// This shape means the engine's view and durable storage disagree, but it does
// not by itself mean the work is gone. Pending is an overloaded report: an
// engine serves it while a restarted instance reloads its snapshot, while
// remote work it has genuinely created is still being provisioned or validated,
// and also when it has truly lost (or never had) the work. Only the last case
// can never terminalize the task, and nothing in a single poll tells the three
// apart — so the caller treats this as the start of a timed trust budget rather
// than as evidence.
//
// Only genuinely in-flight stored states count: resting states such as stopped
// or failed_retryable have no active engine work by design, so a pending report
// for them is expected, not disagreement.
func engineReportsLostWork(storedTaskState, engineTaskState string) bool {
	if !state.IsState(engineTaskState, state.Task.Pending) {
		return false
	}
	return state.IsInFlightTaskState(storedTaskState)
}

// lostEngineWorkTracker measures how long an engine has been reporting no
// active schema change for a task storage says is in flight, and reports when
// that has outlasted the budget. It is a clock, not a poll counter, so a
// shorter poll cadence cannot shrink how long the engine is trusted for.
type lostEngineWorkTracker struct {
	budget time.Duration
	since  time.Time
}

// observePending records a poll that reported no active schema change and
// returns how long the run has lasted, plus whether the trust budget is spent.
// The first observation of a run starts the clock and, for an engine with a
// budget, is never exhausted — such an engine always gets at least its full
// budget before the drive stops trusting it. An engine with no budget has
// nothing to wait out, so its first report exhausts immediately.
func (t *lostEngineWorkTracker) observePending(now time.Time) (pendingFor time.Duration, exhausted bool) {
	if t.since.IsZero() {
		t.since = now
		return 0, t.budget <= 0
	}
	pendingFor = now.Sub(t.since)
	return pendingFor, pendingFor >= t.budget
}

// reset clears the run, so a single healthy report buys the engine a fresh
// budget.
func (t *lostEngineWorkTracker) reset() {
	t.since = time.Time{}
}

// settleLostEngineWork resolves a task the engine has stopped reporting on by
// verifying the target schema directly. The engine says no schema change is
// active while durable storage says this task's change is in flight, and that
// divergence outlasted the tolerated staleness window — so engine progress can
// never terminalize the task and the target itself is the only remaining
// authority. Revert-phase tasks rest retryable without a target read; every
// other task settles from the verification verdict (see
// settleLostRevertPhaseTask and settleLostVerifiedTask). A verification error
// is returned for the caller's consecutive-error budget to count.
func (c *LocalClient) settleLostEngineWork(ctx context.Context, apply *storage.Apply, task *storage.Task, engineState engine.State) (taskAction, error) {
	if taskInRevertPhase(task) {
		if err := c.settleLostRevertPhaseTask(ctx, apply, task, engineState); err != nil {
			return taskAbort, err
		}
		return taskFailed, nil
	}
	planID, err := planIDForTasks(apply, []*storage.Task{task})
	if err != nil {
		return taskContinue, fmt.Errorf("resolve plan to verify target schema for task %s: %w", task.TaskIdentifier, err)
	}
	plan, err := c.storage.Plans().GetByID(ctx, planID)
	if err != nil {
		return taskContinue, fmt.Errorf("load plan %d for apply %s to verify target schema for task %s: %w", planID, apply.ApplyIdentifier, task.TaskIdentifier, err)
	}
	if plan == nil {
		return taskContinue, fmt.Errorf("plan not found for apply %s while verifying target schema for task %s", apply.ApplyIdentifier, task.TaskIdentifier)
	}
	replanDDL, err := c.replanTargetSchema(ctx, apply, plan)
	if err != nil {
		return taskContinue, fmt.Errorf("verify target schema for task %s table %s: %w", task.TaskIdentifier, task.TableName, err)
	}
	verdict := replanVerdictForTask(replanDDL, task)
	if err := c.settleLostVerifiedTask(ctx, apply, task, verdict, engineState); err != nil {
		return taskAbort, err
	}
	if verdict == replanChangeLanded {
		return taskContinue, nil
	}
	return taskFailed, nil
}

// settleLostRevertPhaseTask rests a revert-phase task retryable after the
// engine stopped reporting on it. A revert-phase task can never be settled by
// reading the target: the forward change has already cut over, so the live
// schema matches the reviewed target by definition and a match says nothing
// about whether the revert this task was driving ever finished. Completing on
// it would report the apply as a successful schema change while the revert it
// was undoing is gone. Retryable is the only answer a schema read supports.
func (c *LocalClient) settleLostRevertPhaseTask(ctx context.Context, apply *storage.Apply, task *storage.Task, engineState engine.State) error {
	c.logger.Warn("engine reports no active schema change for a revert-phase task; marking it retryable because the target schema cannot settle a revert",
		append(task.LogAttrs(), "apply_id", apply.ApplyIdentifier, "engine_state", engineState)...)
	previous := *task
	task.ErrorMessage = fmt.Sprintf("engine reports no active schema change while table %s was in its revert phase; a fresh claim will re-drive it", task.TableName)
	task.CompletedAt = nil
	if err := c.persistTaskStateTransition(ctx, task, 0, state.Task.FailedRetryable, ""); err != nil {
		*task = previous
		return fmt.Errorf("persist lost revert-phase task %s settlement: %w", task.TaskIdentifier, err)
	}
	return nil
}

// settleLostVerifiedTask settles a task from its target-verification verdict
// after the engine stopped reporting on it. A target that already has the
// desired schema means the work finished and only its outcome was lost: the
// task completes. A target that still needs the change means the work is
// genuinely gone: the task rests retryable so a fresh claim re-drives it —
// never permanently failed, because nothing about the target is known to be
// broken. A re-plan that cannot speak for the task's scope settles nothing, so
// that task rests retryable too: completion is the one direction a schema read
// must never be guessed in, since it reports the change as made.
func (c *LocalClient) settleLostVerifiedTask(ctx context.Context, apply *storage.Apply, task *storage.Task, verdict replanVerdict, engineState engine.State) error {
	previous := *task
	var targetState, logMessage string
	switch verdict {
	case replanChangeLanded:
		now := time.Now()
		task.ProgressPercent = 100
		task.CompletedAt = &now
		c.logger.Info("engine reports no active schema change and the target already has the desired schema; completing the task",
			append(task.LogAttrs(), "apply_id", apply.ApplyIdentifier, "engine_state", engineState)...)
		targetState = state.Task.Completed
		logMessage = fmt.Sprintf("Task %s completed: engine no longer reports the schema change and the target has the desired schema", task.TaskIdentifier)
	case replanCannotAttribute:
		c.logger.Warn("engine reports no active schema change and the target re-plan does not cover this task's shard; marking the task retryable for a fresh claim to re-drive",
			append(task.LogAttrs(), "apply_id", apply.ApplyIdentifier, "engine_state", engineState)...)
		targetState = state.Task.FailedRetryable
		task.ErrorMessage = fmt.Sprintf("engine reports no active schema change for table %s and the target could not be verified for shard %s; a fresh claim will re-drive it", task.TableName, task.Shard)
		task.CompletedAt = nil
	case replanNeedsChange:
		c.logger.Warn("engine reports no active schema change but the target still needs it; marking the task retryable for a fresh claim to re-drive",
			append(task.LogAttrs(), "apply_id", apply.ApplyIdentifier, "engine_state", engineState)...)
		targetState = state.Task.FailedRetryable
		task.ErrorMessage = fmt.Sprintf("engine reports no active schema change for table %s but the target still needs the change; a fresh claim will re-drive it", task.TableName)
		task.CompletedAt = nil
	}
	if err := c.persistTaskStateTransition(ctx, task, task.ApplyID, targetState, logMessage); err != nil {
		*task = previous
		return fmt.Errorf("persist lost task %s settlement: %w", task.TaskIdentifier, err)
	}
	return nil
}

// taskWaitsForOperatorAction reports whether a task's state is one the drive is
// meant to sit in without moving, because the next step belongs to an operator
// rather than to the engine: a held cutover, a deferred deploy, or an open
// revert window. Progress fields do not advance in these states by design, so
// stall detection has nothing to say about them.
func taskWaitsForOperatorAction(taskState string) bool {
	return state.IsState(taskState, state.Task.WaitingForCutover, state.Task.WaitingForDeploy, state.Task.RevertWindow)
}

// taskProgressSnapshot captures the fields whose movement shows a polled task
// is advancing: the stored state plus the progress counters the engine
// refreshes while it works.
type taskProgressSnapshot struct {
	state               string
	rowsCopied          int64
	progressPercent     int
	checksumRowsChecked int64
}

func taskProgressSnapshotOf(task *storage.Task) taskProgressSnapshot {
	return taskProgressSnapshot{
		state:               task.State,
		rowsCopied:          task.RowsCopied,
		progressPercent:     task.ProgressPercent,
		checksumRowsChecked: task.ChecksumRowsChecked,
	}
}

// taskStallWatchdog tracks whether a polled task's state and progress fields
// are moving, so the drive can surface a task that is nominally being polled
// but has shown no movement for a full interval. It only observes and warns —
// it never changes task state.
type taskStallWatchdog struct {
	interval time.Duration
	last     taskProgressSnapshot
	since    time.Time
	lastWarn time.Time
}

// observe records this poll's snapshot and reports whether the task has now
// been motionless for a full interval since movement stopped, rate-limited to
// one warning per interval. The caller logs when warn is true; movement resets
// both the stall clock and the warning latch.
func (w *taskStallWatchdog) observe(now time.Time, snap taskProgressSnapshot) (stalledFor time.Duration, warn bool) {
	if w.since.IsZero() || snap != w.last {
		w.last = snap
		w.since = now
		w.lastWarn = time.Time{}
		return 0, false
	}
	stalledFor = now.Sub(w.since)
	if stalledFor < w.interval {
		return 0, false
	}
	if !w.lastWarn.IsZero() && now.Sub(w.lastWarn) < w.interval {
		return 0, false
	}
	w.lastWarn = now
	return stalledFor, true
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
//
// The apply-level side effects of the outcome — settling pending control
// requests, releasing the active-apply gauge, and notifying the observer that
// posts the terminal summary — run only once the outcome is durably stored.
// When the apply cannot be reloaded or the outcome write fails, it returns an
// error with the stored apply still active: the drive exits, and the claim
// that picks the apply up next finalizes it and settles its requests then.
// Once the outcome is stored it returns nil whatever happens to the side
// effects: a failed request settlement is logged and left for the operator's
// post-drive settlement, and the summary still posts.
// A drive whose context was cancelled hands the apply back with nil instead:
// its storage failures describe the cancellation, not the outcome, and the
// caller reads its own context to learn why the drive stopped.
func (c *LocalClient) finalizeSequentialApply(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, failedTask *storage.Task, stoppedByUser bool) error {
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
		return nil
	}
	if freshApply, err := c.storage.Applies().Get(ctx, apply.ID); err != nil {
		if c.driveCancelled(ctx, apply, "before reloading the apply for sequential finalization") {
			return nil
		}
		return fmt.Errorf("reload apply %s (database %s) before sequential finalization: %w", apply.ApplyIdentifier, apply.Database, err)
	} else if freshApply != nil && state.IsTerminalApplyState(freshApply.State) {
		logger.Info("apply already terminal in storage, not overwriting during sequential finalization",
			"stored_state", freshApply.State)
		*apply = *freshApply
		if err := settlePendingRequestsForTerminalApply(ctx, c.storage, c.logger, apply); err != nil {
			logger.Warn("failed to settle pending control requests for terminal sequential apply",
				"error", err)
		}
		return nil
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
		if c.driveCancelled(ctx, apply, "while persisting the sequential outcome") {
			return nil
		}
		// The stored apply is still active, so nothing that answers for its
		// outcome may run: a pending control request completed here would
		// resolve an operator's command against an apply storage still reports
		// running, and the observer would post a terminal summary for it.
		return fmt.Errorf("persist sequential outcome %s for apply %s (database %s) from stored state %s: %w",
			apply.State, apply.ApplyIdentifier, apply.Database, previousState, err)
	}
	// A sequential apply's failure reaches the operator through the same
	// apply log stream as every other path, so a failed table does not read
	// as an apply that went terminal for no stated reason.
	switch apply.State {
	case state.Apply.Failed:
		c.logApplyFailure(ctx, apply, previousState, apply.ErrorMessage)
	case state.Apply.FailedRetryable:
		c.logApplyPausedForRetry(ctx, apply, previousState, apply.ErrorMessage)
	}
	if state.IsTerminalApplyState(apply.State) {
		c.settleRequestsForStoredOutcome(ctx, logger, apply)
	}
	metrics.AdjustActiveApplies(ctx, -1, apply.Database, apply.Deployment, apply.Environment)

	if apply.State == state.Apply.FailedRetryable {
		if obs := c.getObserver(apply.ID); obs != nil {
			obs.OnProgress(apply, tasks)
		}
		return nil
	}

	// Notify observer of terminal state, then clean up
	if obs := c.getObserver(apply.ID); obs != nil {
		obs.OnTerminal(apply, tasks)
		c.clearObserver(apply.ID)
	}
	return nil
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

// The engine owns operation grammar. Detection here only decides whether to
// supply the stored desired schema; it never authorizes execution.
func (c *LocalClient) taskNeedsRowSecurityFiles(task *storage.Task) (bool, error) {
	// Other engines do not use this payload. Do not add a parser requirement
	// to their existing apply path (including custom engines).
	if schema.DialectForDatabaseType(c.config.Type) != schema.DialectPostgres {
		return false, nil
	}
	parser, err := c.statementParser()
	if err != nil {
		return false, err
	}
	rls, ok := parser.(interface{ CanonicalRowSecurity(string) (string, error) })
	if !ok {
		return false, nil
	}
	_, err = rls.CanonicalRowSecurity(task.DDL)
	return err == nil, nil
}
