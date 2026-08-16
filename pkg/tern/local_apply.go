package tern

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/spirit"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// checkActiveTaskConflict verifies there's no active schema change for this
// database that would conflict with a new apply targeting dispatchShard.
// Uses retry loop and engine verification to handle stale storage state.
//
// dispatchShard scopes the conflict to a single shard: a sharded apply is
// dispatched one shard at a time, and different shards of the same database are
// distinct physical primaries that run concurrently by design, so a task on
// another shard is not a conflict. dispatchShard is "" for a non-sharded apply,
// which conflicts with any active task on the database (today's behaviour).
func (c *LocalClient) checkActiveTaskConflict(ctx context.Context, plan *storage.Plan, dispatchShard string) error {
	for attempt := range 10 {
		existingTasks, err := c.storage.Tasks().GetByDatabase(ctx, plan.Database)
		if err != nil {
			return fmt.Errorf("check existing tasks: %w", err)
		}

		c.logger.Debug("conflict check: found tasks", "count", len(existingTasks), "database", plan.Database, "shard", dispatchShard, "attempt", attempt)

		blockingTaskID := c.findBlockingTask(ctx, existingTasks, plan, dispatchShard)
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

		return fmt.Errorf("schema change already in progress for database %q (plan %s): blocking task %s", plan.Database, plan.PlanIdentifier, blockingTaskID)
	}
	return nil
}

// findBlockingTask checks if any non-terminal task for this database is truly active.
// Returns the blocking task's identifier, or "" if no conflict exists.
// As a side effect, resolves stale tasks by checking engine state.
//
// dispatchShard scopes the conflict to a single shard (see checkActiveTaskConflict):
// when both the candidate apply and an existing task target a non-empty shard,
// a different shard does not conflict, so a sharded fan-out runs its shards
// concurrently instead of serializing on the first one.
func (c *LocalClient) findBlockingTask(ctx context.Context, tasks []*storage.Task, plan *storage.Plan, dispatchShard string) string {
	for _, t := range tasks {
		c.logger.Debug("conflict check: checking task", "task_id", t.TaskIdentifier, "state", t.State, "shard", t.Shard, "is_terminal", state.IsTerminalTaskState(t.State))
		if t.DatabaseType != plan.DatabaseType || state.IsTerminalTaskState(t.State) {
			continue
		}

		// A task on a different shard targets a different physical primary, so it
		// does not block this shard's apply. Only same-shard work, or work where
		// either side is non-sharded (database-wide), can conflict.
		if dispatchShard != "" && t.Shard != "" && t.Shard != dispatchShard {
			continue
		}

		// Both resolvers below decide based on the task's parent apply — its
		// state and its lease ownership. Any uncertainty (a storage failure or a
		// missing apply row) means the task cannot be proven resolvable, so it
		// keeps blocking (fail closed).
		apply, ok := c.applyForConflictCheck(ctx, t)
		if !ok {
			return t.TaskIdentifier
		}

		// A pending task of a terminal apply can never start; cancel it so it
		// stops blocking the database as phantom active work.
		if c.tryResolveOrphanedPendingTask(ctx, t, apply) {
			continue
		}

		// Storage says non-terminal — verify with engine before blocking.
		if c.tryResolveStaleTask(ctx, t, apply, plan.Database) {
			continue // Task was stale; engine confirmed it's done.
		}

		c.logger.Debug("conflict check: task is active", "task_id", t.TaskIdentifier)
		return t.TaskIdentifier
	}
	return ""
}

// applyForConflictCheck loads the parent apply the conflict-check resolvers
// decide on. Returns ok=false when the apply cannot be loaded or the row is
// missing — the caller must keep the task blocking, because without the apply
// neither the task's orphan status nor its lease ownership can be proven.
func (c *LocalClient) applyForConflictCheck(ctx context.Context, t *storage.Task) (*storage.Apply, bool) {
	apply, err := c.storage.Applies().Get(ctx, t.ApplyID)
	if err != nil {
		c.logger.Warn("conflict check: failed to load the task's apply; the task keeps blocking the database",
			append(t.LogAttrs(), "error", err)...)
		return nil, false
	}
	if apply == nil {
		c.logger.Warn("conflict check: task's apply row is missing; the task keeps blocking the database",
			t.LogAttrs()...)
		return nil, false
	}
	return apply, true
}

// tryResolveOrphanedPendingTask cancels a pending task whose parent apply has
// already reached a terminal state. Such a task can never start: a terminal
// apply is not claimable, so no drive will ever pick the task up, and pending
// means no engine work or checkpoint exists to preserve. Left alone the task
// would block every later apply targeting its database as phantom active work.
// A non-pending state leaves the task untouched so it stays with the
// engine-backed checks.
// Returns true if the task was cancelled and no longer blocks.
func (c *LocalClient) tryResolveOrphanedPendingTask(ctx context.Context, t *storage.Task, apply *storage.Apply) bool {
	if !state.IsState(t.State, state.Task.Pending) {
		// Only a pending task is provably unstarted; every other state may own
		// engine work or a checkpoint and stays with the engine-backed checks.
		return false
	}
	if !state.IsTerminalApplyState(apply.State) {
		c.logger.Debug("conflict check: pending task's apply is still active; the task blocks normally",
			append(t.LogAttrs(), "apply_id", apply.ApplyIdentifier, "apply_state", apply.State)...)
		return false
	}
	c.logger.Info("conflict check: cancelling orphaned pending task; its apply is terminal so the task can never start",
		append(t.LogAttrs(), "apply_id", apply.ApplyIdentifier, "apply_state", apply.State)...)
	previousState := t.State
	now := time.Now()
	t.State = state.Task.Cancelled
	t.ErrorMessage = "Task orphaned: its apply reached a terminal state before the task started"
	t.CompletedAt = &now
	t.UpdatedAt = now
	// The task only stops blocking once the cancellation is durably written:
	// reporting it resolved on a failed write would admit the new apply while
	// storage still records the orphan as active work.
	if err := c.storage.Tasks().Update(ctx, t); err != nil {
		t.State = previousState
		t.ErrorMessage = ""
		t.CompletedAt = nil
		c.logger.Error("conflict check: failed to persist orphaned task cancellation; the task keeps blocking the database",
			append(t.LogAttrs(), "apply_id", apply.ApplyIdentifier, "error", err)...)
		return false
	}
	taskID := t.ID
	c.logApplyEvent(ctx, apply.ID, &taskID, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
		"Cancelled orphaned pending task: its apply was already terminal, so the task could never start",
		previousState, state.Task.Cancelled)
	return true
}

// tryResolveStaleTask checks the engine to see if a non-terminal task is actually done.
// If the engine reports a terminal state, or reports no active work for a task that
// storage believes is in-flight, the task is updated in storage and no longer blocks.
// Resting tasks (Stopped, FailedRetryable) are left untouched.
//
// The engine probe is in-memory and database-scoped: it reports this process's
// last run on the database, not the task's actual cross-process state. The
// task's parent apply lease decides whether that memory is authoritative — a
// fresh lease means a live driver owns the work and the task keeps blocking,
// and a terminal report is only trusted when the last lease belongs to this
// process (the completing process's own report).
// Returns true if the task was resolved (no longer blocking).
func (c *LocalClient) tryResolveStaleTask(ctx context.Context, t *storage.Task, apply *storage.Apply, database string) bool {
	eng := c.getEngine()
	if eng == nil {
		c.logger.Error("tryResolveStaleTask: engine is nil", t.LogAttrs()...)
		return false
	}

	if apply.HasFreshLease(time.Now()) {
		c.logger.Info("conflict check: task's apply is actively driven; the task keeps blocking until its lease owner settles it",
			append(t.LogAttrs(), "apply_id", apply.ApplyIdentifier, "lease_owner", apply.LeaseOwner)...)
		metrics.RecordConflictCheckOwnershipBlock(ctx, t.Database, t.DatabaseType, "fresh_lease")
		return false
	}

	// The raw target credentials (no namespace mapping) are safe here only
	// because Spirit's Progress is purely in-memory and never queries by
	// request database or connection schema. An engine whose Progress inspects
	// the database must resolve credentials per task (credentialsForTask)
	// before this probe, or under schema overrides it would address the
	// canonical name instead of the physical schema.
	//
	// The task identifier rides along for engines that key progress by apply
	// identity (postgres): a probe about work the engine is still running
	// must see its live state and keep blocking, while an unrecognized
	// identity reads the idle sentinel and resolves as abandoned. Engines
	// that ignore identity (Spirit) or require resume metadata instead
	// (PlanetScale) behave exactly as before.
	result, err := eng.Progress(ctx, &engine.ProgressRequest{
		Database:    database,
		Credentials: c.credentials(),
		ResumeState: &engine.ResumeState{MigrationContext: t.TaskIdentifier},
	})
	if err != nil {
		// result may be nil when err is non-nil, so it must not be dereferenced here.
		c.logger.Warn("conflict check: engine progress failed", append(t.LogAttrs(), "err", err)...)
		return false
	}
	c.logger.Debug("conflict check: engine progress", "task_id", t.TaskIdentifier, "engine_state", result.State, "engine_message", result.Message)

	// Engine says terminal — update storage and unblock.
	// IMPORTANT: Only trust terminal states, NOT "No active schema change".
	// "No active schema change" just means Spirit has no runningSchemaChange,
	// which could mean completed, never started, or crashed.
	if result.State.IsTerminal() {
		// A terminal report for work last leased to another process is this
		// process's memory of an older run on the same database, not the
		// completing driver's own report — stamping it would mark someone
		// else's task done with state that says nothing about it. Leave the
		// task blocking until driver stale-claim recovery settles it.
		if apply.LeaseOwner != "" && !storage.LeaseOwnedByThisProcess(apply.LeaseOwner) {
			c.logger.Warn("conflict check: engine reports terminal state, but the apply's last lease belongs to another process; the task keeps blocking until driver recovery settles it",
				append(t.LogAttrs(), "apply_id", apply.ApplyIdentifier, "lease_owner", apply.LeaseOwner,
					"engine_state", result.State, "engine_message", result.Message)...)
			metrics.RecordConflictCheckOwnershipBlock(ctx, t.Database, t.DatabaseType, "foreign_terminal_report")
			return false
		}
		c.logger.Info("conflict check: engine reports terminal state",
			"task_id", t.TaskIdentifier, "engine_state", result.State,
			"engine_message", result.Message, "storage_state", t.State)
		now := time.Now()
		t.CompletedAt = &now
		c.transitionTaskState(ctx, t, 0, engineStateToStorage(result.State), "")
		return true
	}

	// The engine has no active work. For in-flight states this means the task was
	// abandoned (e.g. a server crash) and must be failed so it stops blocking.
	// Resting states (Stopped, FailedRetryable) also have no active engine work,
	// but that is expected — Spirit keeps the checkpoint until an operator resumes
	// or retries. Failing them here would destroy resumable work and void the
	// operator retry budget, so leave them untouched and let the conflict/lock
	// logic decide whether the new apply proceeds.
	if result.Message == "No active schema change" {
		if !state.IsInFlightTaskState(t.State) {
			c.logger.Debug("conflict check: leaving resting task untouched (no active engine work expected)",
				"task_id", t.TaskIdentifier, "storage_state", t.State)
			return false
		}
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
		c.logger.Warn("failed to log apply event", "error", err, "event", eventType, "event_message", message)
	}
}

// logEngineResumeOnce records a timeline event the first time a drive claim
// observes the engine reattached to a durable checkpoint instead of starting
// the copy fresh. One event per drive claim is intentional: every claim that
// resumes (pod restart, lease handover, operator start after a stop) is a
// real engine resume the operator should see on the timeline. eventLogged is
// the per-drive latch; the durable checkpoint itself lives in the engine.
func (c *LocalClient) logEngineResumeOnce(ctx context.Context, logger *slog.Logger, apply *storage.Apply, resumed bool, eventLogged *bool) {
	if !resumed || *eventLogged {
		return
	}
	*eventLogged = true
	const msg = "Engine resumed from checkpoint; row copy continues from durable progress"
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
		msg, "", "")
	logger.Info("engine resumed from checkpoint")
}

// setupSpiritLogging wires up Spirit's log callback to route engine logs to the apply_logs table.
// Returns a cleanup function that must be deferred.
func (c *LocalClient) setupSpiritLogging(ctx context.Context, apply *storage.Apply, tasks []*storage.Task) func() {
	spiritEng, ok := c.spiritEngine.(*spirit.Engine)
	if !ok {
		return func() {}
	}
	spiritEng.SetLogCallback(c.spiritApplyLogFunc(ctx, apply, tasks))
	return func() { spiritEng.SetLogCallback(nil) }
}

// spiritApplyLogFunc builds the callback that records one Spirit log line in
// the apply log stream. It attributes each line to its task by table name and
// embeds the table in the stored message so operators can tell interleaved
// lines apart during multi-table applies.
func (c *LocalClient) spiritApplyLogFunc(ctx context.Context, apply *storage.Apply, tasks []*storage.Task) func(level slog.Level, tableName, msg string) {
	taskByTable := make(map[string]*storage.Task)
	for _, task := range tasks {
		taskByTable[task.TableName] = task
	}

	return func(level slog.Level, tableName, msg string) {
		logLevel := storage.LogLevelInfo
		if level >= slog.LevelWarn {
			logLevel = storage.LogLevelWarn
		}
		if level >= slog.LevelError {
			logLevel = storage.LogLevelError
		}
		// Embed the table in the stored message so it survives every log
		// surface — CLI output, deployment log fetches, and PR comment log
		// folds render the message text only.
		if tableName != "" {
			msg = fmt.Sprintf("[%s] %s", tableName, msg)
		}
		// Run-level lines (or lines spanning several tables) carry no single
		// task's table name; attribute those to the apply, not to an
		// arbitrary task.
		var taskID *int64
		if task := taskByTable[tableName]; task != nil {
			id := task.ID
			taskID = &id
		}
		c.logApplyEvent(ctx, apply.ID, taskID, logLevel, storage.LogEventInfo, storage.LogSourceSpirit, msg, "", "")
	}
}

// transitionTaskState updates a task's state, persists it, and optionally logs a state transition.
// Fields like CompletedAt, StartedAt, ErrorMessage, or progress must be set on the task BEFORE calling this.
func (c *LocalClient) transitionTaskState(ctx context.Context, task *storage.Task, applyID int64, newState string, logMsg string) {
	oldState := task.State
	task.State = newState
	task.UpdatedAt = time.Now()
	// An ETA and throttle state are only meaningful while an engine is
	// actively driving the task and refreshing them. Clear them when the task
	// comes to rest (terminal, stopped, retryable, pending) so the stored row
	// never carries a frozen estimate or renders as paused with no copy in
	// flight; a resumed or retried task gets fresh figures from the first
	// engine poll.
	if !state.IsInFlightTaskState(newState) {
		task.ETASeconds = 0
		task.Throttled = false
		task.ThrottleReason = ""
	}
	if err := c.storage.Tasks().Update(ctx, task); err != nil {
		c.logger.Error("failed to update task state", append(task.LogAttrs(), "error", err)...)
	}
	if logMsg != "" && applyID > 0 {
		taskID := task.ID
		c.logApplyEvent(ctx, applyID, &taskID, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			logMsg, oldState, newState)
	}
}

// markTasksRunning sets DDL tasks to running state with a start timestamp.
func (c *LocalClient) markTasksRunning(ctx context.Context, tasks []*storage.Task) {
	now := time.Now()
	for _, task := range tasks {
		task.State = state.Task.Running
		task.StartedAt = &now
		task.UpdatedAt = now
		if err := c.storage.Tasks().Update(ctx, task); err != nil {
			c.logger.Error("failed to update task state", append(task.LogAttrs(), "error", err)...)
		}
	}
}

// runWithRecovery wraps an apply function with panic recovery so a single panic
// doesn't crash the entire process. On panic, all tasks and the apply are marked failed.
func (c *LocalClient) runWithRecovery(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			errMsg := fmt.Sprintf("panic in apply goroutine: %v", r)
			c.logger.Error(errMsg, apply.LogAttrs()...)
			c.failApplyWithTasks(ctx, apply, tasks, errMsg)
		}
	}()
	fn()
}

// groupedApplyMode classifies the grouped-apply strategy for a drive. It reads
// DeferCutover from the effective options map (which may carry an automatic
// barrier-park decision, see effectiveCopyDriveOptions) rather than from
// apply.GetOptions(), so an operation-scoped copy drive that must park at the
// cutover barrier takes the atomic-cutover path.
func groupedApplyMode(apply *storage.Apply, options map[string]string) string {
	opts := storage.ApplyOptionsFromMap(options)
	switch {
	case apply.DatabaseType == storage.DatabaseTypeMySQL && opts.DeferCutover:
		return "spirit_atomic_cutover"
	case apply.DatabaseType == storage.DatabaseTypeVitess:
		return "vitess_deploy_request"
	default:
		return "grouped_engine_apply"
	}
}

func groupedApplyModeDescription(apply *storage.Apply, options map[string]string) string {
	switch groupedApplyMode(apply, options) {
	case "spirit_atomic_cutover":
		return "Spirit atomic cutover"
	case "vitess_deploy_request":
		return "Vitess deploy request"
	default:
		return "grouped engine apply"
	}
}

func (c *LocalClient) usesGroupedApply(apply *storage.Apply, options map[string]string) bool {
	if apply.DatabaseType == storage.DatabaseTypeVitess {
		return true
	}
	return apply.DatabaseType == storage.DatabaseTypeMySQL && storage.ApplyOptionsFromMap(options).DeferCutover
}

func (c *LocalClient) setApplyCancel(cancel context.CancelFunc) uint64 {
	c.cancelMu.Lock()
	c.cancelApplyGeneration++
	generation := c.cancelApplyGeneration
	c.cancelApply = cancel
	c.cancelMu.Unlock()
	return generation
}

func (c *LocalClient) clearApplyCancel(generation uint64) {
	c.cancelMu.Lock()
	if c.cancelApplyGeneration == generation {
		c.cancelApply = nil
	}
	c.cancelMu.Unlock()
}

func (c *LocalClient) currentApplyCancel() applyCancelHandle {
	c.cancelMu.Lock()
	defer c.cancelMu.Unlock()
	return applyCancelHandle{generation: c.cancelApplyGeneration, cancel: c.cancelApply}
}

func (c *LocalClient) cancelApplyHandle(handle applyCancelHandle) {
	if handle.cancel != nil {
		handle.cancel()
	}
	c.cancelMu.Lock()
	if c.cancelApplyGeneration == handle.generation {
		c.cancelApply = nil
	}
	c.cancelMu.Unlock()
}

func (c *LocalClient) runApplyExecution(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, plan *storage.Plan, options map[string]string, releaseAtCutoverBarrier bool) {
	if c.usesGroupedApply(apply, options) {
		c.runWithRecovery(ctx, apply, tasks, func() {
			c.executeGroupedApply(ctx, apply, tasks, plan, options, releaseAtCutoverBarrier)
		})
		return
	}

	c.runWithRecovery(ctx, apply, tasks, func() {
		c.executeApplySequential(ctx, apply, tasks, plan, options)
	})
}

// executeGroupedApply runs all DDLs in one engine operation. For Spirit with
// defer_cutover, this is atomic cutover; for Vitess, this is one deploy request.

// deriveOverallState determines the overall state from a list of tasks.
// Priority order:
//  1. Active work: CUTTING_OVER, then the least-advanced active phase
//     (RUNNING, CATCHING_UP, CHECKSUMMING, POST_CHECKSUM), then
//     WAITING_FOR_CUTOVER once nothing is still working
//  2. FAILED - at least one task failed (CANCELLED tasks also indicate failure)
//  3. FAILED_RETRYABLE - operator recovery may retry failed task work
//  4. PENDING - more work queued
//  5. STOPPED - apply was stopped (even if some tasks completed)
//  6. COMPLETED - all tasks completed successfully
func deriveOverallState(tasks []*storage.Task) string {
	if len(tasks) == 0 {
		return state.Task.Pending
	}

	var hasRunning, hasCatchingUp, hasChecksumming, hasPostChecksum, hasWaitingForCutover, hasCuttingOver bool
	var hasPending, hasStopped, hasFailed, hasRetryableFailed, hasCancelled, hasCompleted, hasRevertWindow bool

	for _, t := range tasks {
		switch t.State {
		case state.Task.Running:
			hasRunning = true
		case state.Task.CatchingUp:
			hasCatchingUp = true
		case state.Task.Checksumming:
			hasChecksumming = true
		case state.Task.PostChecksum:
			hasPostChecksum = true
		case state.Task.WaitingForCutover:
			hasWaitingForCutover = true
		case state.Task.CuttingOver:
			hasCuttingOver = true
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

	// Active work, mirroring state.DeriveApplyState: once any table starts
	// its cutover the apply is transitioning; otherwise surface the
	// least-advanced active phase — while any table still copies rows the
	// apply is running, the post-copy phases surface only when every active
	// table is draining or verifying, and waiting_for_cutover only when
	// nothing is still working.
	switch {
	case hasCuttingOver:
		return state.Task.CuttingOver
	case hasRunning:
		return state.Task.Running
	case hasCatchingUp:
		return state.Task.CatchingUp
	case hasChecksumming:
		return state.Task.Checksumming
	case hasPostChecksum:
		return state.Task.PostChecksum
	case hasWaitingForCutover:
		return state.Task.WaitingForCutover
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
// The logger is expected to carry the apply's identity attributes already
// bound, so the failure line appends only the mutable snapshot.
func applyEventStateTransition(apply *storage.Apply, event engine.ApplyEvent, updateFn func(*storage.Apply) error, logger *slog.Logger) string {
	oldState := apply.State
	newState := deriveApplyPhase(event)
	if newState == "" || newState == oldState {
		return ""
	}
	apply.State = newState
	apply.UpdatedAt = time.Now()
	if err := updateFn(apply); err != nil {
		logger.Error("failed to update apply phase", append(apply.MutableLogAttrs(), "error", err)...)
		apply.State = oldState
		return ""
	}
	return newState
}
