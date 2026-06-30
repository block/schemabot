package tern

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/metrics"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// Start resumes a stopped schema change.
func (c *LocalClient) Start(ctx context.Context, req *ternv1.StartRequest) (*ternv1.StartResponse, error) {
	apply, startedCount, skippedCount, err := c.resolveStartRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	controlStore := c.storage.ControlRequests()
	if controlStore == nil {
		return nil, fmt.Errorf("control request store is not available")
	}
	metadata, err := json.Marshal(localStartControlRequestMetadata{
		StartedCount: startedCount,
		SkippedCount: skippedCount,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal start control request metadata for apply %s: %w", apply.ApplyIdentifier, err)
	}
	_, alreadyPending, err := controlStore.RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "tern-grpc",
		Metadata:    metadata,
	})
	if err != nil {
		return nil, fmt.Errorf("record start control request for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if alreadyPending {
		c.logger.Info("start request already pending for apply owner",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment)
	} else {
		c.logger.Info("start request queued for apply owner",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStartRequested, storage.LogSourceSchemaBot,
			"Start request queued for apply owner", "", "")
	}
	c.wakeOperatorForControlRequest(apply)
	return &ternv1.StartResponse{
		Accepted:     true,
		StartedCount: startedCount,
		SkippedCount: skippedCount,
	}, nil
}

type localStartControlRequestMetadata struct {
	StartedCount int64 `json:"started_count,omitempty"`
	SkippedCount int64 `json:"skipped_count,omitempty"`
}

func (c *LocalClient) resolveStartRequest(ctx context.Context, req *ternv1.StartRequest) (*storage.Apply, int64, int64, error) {
	tasks, err := c.storage.Tasks().GetByDatabase(ctx, c.config.Database)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("get tasks failed: %w", err)
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
			return nil, 0, 0, fmt.Errorf("apply %s not found", req.ApplyId)
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
		return nil, 0, 0, fmt.Errorf("no stopped schema change to resume")
	}

	// Deferred deploy that isn't ready yet — reject with a clear message.
	if apply.GetOptions().DeferDeploy && apply.State != state.Apply.WaitingForDeploy {
		return nil, 0, 0, fmt.Errorf("schema change is not ready for deploy (current state: %s)", apply.State)
	}
	if state.IsState(apply.State, state.Apply.WaitingForDeploy) {
		return apply, 1, 0, nil
	}
	if !state.IsState(apply.State, state.Apply.Stopped, state.Apply.Running) {
		return nil, 0, 0, fmt.Errorf("schema change is not stopped (current state: %s)", apply.State)
	}

	var startedCount int64
	var skippedCount int64
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
		switch {
		case state.IsState(task.State, state.Task.Stopped):
			startedCount++
		case state.IsTerminalTaskState(task.State):
			skippedCount++
		}
	}

	if startedCount == 0 {
		return nil, 0, 0, fmt.Errorf("no stopped schema change to resume (found %d tasks for database, apply has ID %d)", len(tasks), apply.ID)
	}
	return apply, startedCount, skippedCount, nil
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

func (c *LocalClient) processPendingStartControlRequest(ctx context.Context, apply *storage.Apply, options map[string]string, releaseAtCutoverBarrier bool) (bool, error) {
	controlReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationStart)
	if err != nil {
		return false, err
	}
	if controlReq == nil {
		return false, nil
	}
	if stopReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationStop); err != nil {
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
		if failErr := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart, err.Error()); failErr != nil {
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
		if err := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart, errorMessage); err != nil {
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
	if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart); err != nil {
		return true, err
	}
	c.logger.Info("pending start request accepted and completed",
		"apply_id", apply.ApplyIdentifier,
		"database", apply.Database,
		"environment", apply.Environment,
		"requested_by", controlRequestCaller(controlReq),
		"state", apply.State)
	c.pollForCompletionAtomic(ctx, apply, started.tasks, started.credentials, started.resumeState, options, releaseAtCutoverBarrier)
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
		if handled, err := c.processPendingCancelOrStopControlRequest(ctx, apply); err != nil {
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
		needsChange, err := c.tableStillNeedsChange(ctx, apply, plan, task)
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

		action = c.runEngineTask(ctx, apply, task, options, creds)

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

// shardTableKey identifies a table change within a specific (namespace, shard).
// A plan is keyed by (Namespace, Shard), so the same table name can appear in
// more than one namespace (multiple Vitess keyspaces) and on more than one shard
// within a namespace; both must be in the key to avoid conflating tasks. For a
// non-sharded engine the shard is empty, so keying degrades to (namespace,
// table) and matches the pre-sharding behavior.
type shardTableKey struct {
	namespace string
	shard     string
	table     string
}

// replanShardTableDDL indexes a re-plan's table changes by
// (namespace, shard, table) -> DDL so the resume/recovery path reconciles each
// task against its own namespace and shard. A sharded engine emits one
// SchemaChange per (namespace, shard) and the same table repeats across them, so
// keying by table name alone would conflate tasks: another shard's (or another
// keyspace's) remaining diff could keep this task active, or update it with the
// wrong DDL.
func replanShardTableDDL(result *engine.PlanResult) map[shardTableKey]string {
	out := make(map[shardTableKey]string)
	for _, sc := range result.Changes {
		for _, tc := range sc.TableChanges {
			out[shardTableKey{namespace: sc.Namespace, shard: sc.Shard.Name, table: tc.Table}] = tc.DDL
		}
	}
	return out
}

// tableStillNeedsChange does a quick re-plan to check if a task's table on its
// own shard still needs schema changes. Returns false if it already has the
// desired schema (e.g., Spirit's cutover completed during the stop sequence).
func (c *LocalClient) tableStillNeedsChange(ctx context.Context, apply *storage.Apply, plan *storage.Plan, task *storage.Task) (bool, error) {
	result, err := c.planWithEngine(ctx, &ternv1.PlanRequest{}, apply.Database, plan.SchemaFiles)
	if err != nil {
		return false, fmt.Errorf("re-plan check failed: %w", err)
	}
	_, stillNeeded := replanShardTableDDL(result)[shardTableKey{namespace: task.Namespace, shard: task.Shard, table: task.TableName}]
	return stillNeeded, nil
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

	// Index the re-plan's remaining changes by (namespace, shard, table) so each
	// task is reconciled against its own namespace and shard rather than conflated
	// with same-named tables in other keyspaces or on other shards.
	replanDDL := replanShardTableDDL(replanOut)

	// Partition tasks: already-done vs still-needed
	now := time.Now()
	var activeTasks []*storage.Task
	var completedCount int64
	for _, task := range tasks {
		if task.State == state.Task.Completed {
			continue
		}
		ddl, stillNeeded := replanDDL[shardTableKey{namespace: task.Namespace, shard: task.Shard, table: task.TableName}]
		if !stillNeeded {
			// This shard's table is no longer in the diff — it already completed.
			task.ProgressPercent = 100
			task.CompletedAt = &now
			c.transitionTaskState(ctx, task, apply.ID, state.Task.Completed,
				fmt.Sprintf("Task %s already completed (re-plan shows no remaining changes)", task.TaskIdentifier))
			completedCount++
		} else {
			// Fail closed if the re-plan would apply DDL this task was not
			// reviewed with: the re-plan recomputes the delta against live
			// schema, so on a drifted deployment it can produce unreviewed DDL
			// that overwriting task.DDL would silently apply.
			if err := verifyReplannedTaskDDL(task, ddl); err != nil {
				return nil, err
			}
			task.DDL = ddl
			activeTasks = append(activeTasks, task)
		}
	}

	return &replanResult{ActiveTasks: activeTasks, CompletedCount: completedCount}, nil
}

// verifyReplannedTaskDDL fails closed when the DDL a resume re-plan would now
// apply for a task differs from the DDL the task was reviewed with. The resume
// re-plan recomputes each deployment's own delta against its live schema; on a
// deployment whose schema has drifted, that recomputed delta is DDL no human
// reviewed, and overwriting task.DDL with it would apply it silently. Comparing
// canonical forms tolerates incidental formatting differences so only a real
// semantic divergence trips the guard. A task with no reviewed DDL carries no
// reference to compare against (only the legacy synthetic VSchema tasks, which
// the engine-change builder already skips), so it is left to existing handling.
func verifyReplannedTaskDDL(task *storage.Task, replannedDDL string) error {
	if task.DDL == "" {
		return nil
	}
	reviewedCanon, err := canonicalDDLForDrift(task.DDL)
	if err != nil {
		return fmt.Errorf("reviewed DDL for task %s: %w", task.TaskIdentifier, err)
	}
	replannedCanon, err := canonicalDDLForDrift(replannedDDL)
	if err != nil {
		return fmt.Errorf("re-planned DDL for task %s: %w", task.TaskIdentifier, err)
	}
	if reviewedCanon == replannedCanon {
		return nil
	}
	loc := formatDriftLocation(driftChangeKey{
		namespace: task.Namespace,
		shard:     task.Shard,
		table:     task.TableName,
		operation: task.DDLAction,
	})
	return fmt.Errorf("local schema has drifted from the reviewed plan; resume would apply unreviewed DDL for %s: reviewed %q, re-planned %q",
		loc, reviewedCanon, replannedCanon)
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
// requeued and the apply is ready for execution, so a driver crash can still be
// recovered by another operator driver.
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

// shouldInspectCutoverSignalForResume reports whether the resume path must load
// the engine's cutover checkpoint before driving. It is true for the normal
// deferred-cutover recovery (shouldInspectDeferredCutoverSignal) and, in
// addition, for an ordered-cutover drive (forceCutoverResume) of a parked
// operation. The barrier park deliberately does not persist DeferCutover onto
// the shared apply (it is an execution-time, per-operation decision), so the
// stored-options check alone would miss a parked operation; the force flag
// covers that case while still requiring the apply to actually be parked.
func shouldInspectCutoverSignalForResume(apply *storage.Apply, forceCutoverResume bool) bool {
	if shouldInspectDeferredCutoverSignal(apply) {
		return true
	}
	return forceCutoverResume && apply != nil &&
		state.IsState(apply.State, state.Apply.WaitingForCutover, state.Apply.Recovering)
}

// suppressParentApplyWrites reports whether the current drive runs under an
// operation lease only (no parent apply lease). That is the multi-operation
// fan-out case: the parent applies row is owned solely by the operator's
// rollout-projection CAS, so the drive must not write it directly — storage
// fails such writes closed with ErrApplyLeaseLost, and the cutover drive's
// recovering/running writes would otherwise abort the whole drive. A
// single-operation or whole-apply drive carries the parent apply lease and
// writes (and heartbeats) the parent directly, so this returns false for them.
// The operator advances the parent via updateApplyStateFromOperations after the
// drive returns.
func suppressParentApplyWrites(ctx context.Context) bool {
	if _, ok := storage.ApplyLeaseFromContext(ctx); ok {
		return false
	}
	opLease, ok := storage.OperationLeaseFromContext(ctx)
	return ok && opLease.Valid()
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
	// A multi-operation drive owns only its operation: the parent recovering
	// write is the operator's projection to make, and a direct write here fails
	// closed under the operation-only lease. Tasks are already recovering above.
	if suppressParentApplyWrites(ctx) {
		return nil
	}
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		return fmt.Errorf("mark apply %s recovering after restart: %w", apply.ApplyIdentifier, err)
	}
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
		"Recovering after restart before accepting cutover requests", oldApplyState, state.Apply.Recovering)
	return nil
}

// launchAtomicResume sends all DDLs to the engine in one call, marks tasks and
// apply as RUNNING, logs the provided message, and then polls for completion.
// Operator-owned calls block so the driver owns the apply until terminal or
// retry-waiting state; user start calls poll in the background and returns
// after the engine accepts the resume.
func (c *LocalClient) launchAtomicResume(ctx context.Context, apply *storage.Apply,
	tasks []*storage.Task, plan *storage.Plan, options map[string]string, logMessage string, block bool, startRequested bool, releaseAtCutoverBarrier bool) error {

	allTasks := tasks
	// A multi-operation drive (operation lease only) owns its operation, not the
	// parent applies row: it must not write the parent state, complete parent
	// stop/start requests, adjust the apply-level active metric, fire the parent
	// terminal observer, or heartbeat the parent. The operator advances the
	// parent via the projection CAS after the drive returns; the operation row is
	// heartbeated by the operator. Task and engine state are still persisted.
	suppressParent := suppressParentApplyWrites(ctx)
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
		// The drive's tasks are already terminal, so the operator derives this
		// operation completed and projects the parent; skip the parent writes and
		// side-effects a multi-operation drive does not own.
		if suppressParent {
			return nil
		}
		if err := c.storage.Applies().Update(ctx, apply); err != nil {
			c.logger.Error("failed to update apply state", append(apply.LogAttrs(), "error", err)...)
			return fmt.Errorf("mark grouped resume apply %s completed after final schema check: %w", apply.ApplyIdentifier, err)
		}
		if startRequested {
			if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart); err != nil {
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
		c.logger.Error("failed to resolve engine resume state for grouped resume; current apply owner will exit for operator retry",
			append(apply.LogAttrs(), "error", err)...)
		return err
	}

	// Resume the grouped apply with the engine's persisted state so it
	// reattaches to in-flight engine work instead of launching a duplicate
	// schema change. The changes are rebuilt from the stored tasks so the
	// engine keys per-table progress on the same namespace/table pairs the
	// tasks carry.
	result, err := eng.Apply(ctx, &engine.ApplyRequest{
		Database:     apply.Database,
		PlanID:       plan.PlanIdentifier,
		Changes:      groupedResumeChanges(tasks, plan),
		TargetShards: taskTargetShards(tasks),
		SchemaFiles:  plan.SchemaFiles,
		Options:      options,
		ResumeState:  resumeState,
		Credentials:  creds,
		OnStateChange: func(rs *engine.ResumeState) {
			if rs == nil {
				c.logger.Debug("OnStateChange: nil resume state", "apply_id", apply.ApplyIdentifier)
				return
			}
			if saveErr := c.saveEngineResumeState(ctx, apply, tasks, rs); saveErr != nil {
				c.logger.Warn("OnStateChange: failed to persist opaque resume state", append(apply.LogAttrs(), "error", saveErr)...)
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
			if saveErr := c.saveEngineResumeState(ctx, apply, tasks, resumeState); saveErr != nil {
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
	// A multi-operation drive does not write the parent running state or complete
	// parent start requests; the operator projected the parent running before the
	// drive. Tasks are already running/recovering above.
	if !suppressParent {
		if err := c.storage.Applies().Update(ctx, apply); err != nil {
			c.logger.Error("failed to update apply state", append(apply.LogAttrs(), "error", err)...)
			return fmt.Errorf("mark grouped resume apply %s %s: %w", apply.ApplyIdentifier, apply.State, err)
		}
		if startRequested {
			if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart); err != nil {
				return err
			}
		}
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			logMessage, oldApplyState, apply.State)
	}

	if block {
		pollCtx, cancelPoll := context.WithCancel(ctx)
		defer cancelPoll()
		// Heartbeat the parent apply only when this drive owns it; a
		// multi-operation drive's parent heartbeat fails closed under the
		// operation-only lease and would cancel the run, so the operator
		// heartbeats the operation row instead.
		stopHeartbeat := c.startParentApplyHeartbeat(pollCtx, apply, suppressParent, cancelPoll)
		defer stopHeartbeat()
		c.pollForCompletionAtomic(pollCtx, apply, tasks, creds, resumeState, options, releaseAtCutoverBarrier)
		return nil
	}

	resumeCtx, cancelResume := context.WithCancel(context.WithoutCancel(ctx))
	stopHeartbeat := c.startParentApplyHeartbeat(resumeCtx, apply, suppressParent, cancelResume)
	go func() {
		defer cancelResume()
		defer stopHeartbeat()
		c.pollForCompletionAtomic(resumeCtx, apply, tasks, creds, resumeState, options, releaseAtCutoverBarrier)
	}()
	return nil
}

// startParentApplyHeartbeat starts the parent apply heartbeat when this drive
// owns the parent apply lease. A multi-operation drive (operation lease only)
// does not own the parent row — its heartbeat fails closed and would cancel the
// run — so this returns a no-op and the operator heartbeats the operation row.
func (c *LocalClient) startParentApplyHeartbeat(ctx context.Context, apply *storage.Apply, suppressParent bool, cancelApply ...context.CancelFunc) context.CancelFunc {
	if suppressParent {
		return func() {}
	}
	return c.startApplyHeartbeat(ctx, apply, cancelApply...)
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

	operationID, err := c.applyOperationIDForApplyTasks(ctx, apply, tasks)
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

// groupedResumeChanges rebuilds the engine changes for a grouped apply from the
// stored tasks plus the plan. Tasks carry the table DDL (engines key per-table
// progress on namespace and table, so both are preserved). VSchema is not a
// task: each namespace whose plan carries a vschema.json artifact is flagged
// with vschema_changed so the engine applies its VSchema from the schema files
// alongside its DDL — or on its own for a VSchema-only namespace that has no DDL
// tasks. This mirrors how the fresh apply builds changes from the plan.
func groupedResumeChanges(tasks []*storage.Task, plan *storage.Plan) []engine.SchemaChange {
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
		// A task with no DDL carries no executable change. The only tasks that
		// ever had empty DDL were the removed synthetic VSchema tasks; an apply
		// created by an older binary mid-rolling-deploy can still carry one. Skip
		// it rather than emit a TableChange the engine cannot apply.
		if task.DDL == "" {
			continue
		}
		idx := ensureNamespace(task.Namespace)
		changes[idx].TableChanges = append(changes[idx].TableChanges, engine.TableChange{
			Table:     task.TableName,
			DDL:       task.DDL,
			Operation: ddl.OpToStatementType(task.DDLAction),
		})
	}
	if plan != nil {
		namespaces := make([]string, 0, len(plan.Namespaces))
		for ns := range plan.Namespaces {
			namespaces = append(namespaces, ns)
		}
		sort.Strings(namespaces)
		for _, ns := range namespaces {
			if !namespaceHasVSchemaArtifact(plan.Namespaces[ns]) {
				continue
			}
			idx := ensureNamespace(ns)
			if changes[idx].Metadata == nil {
				changes[idx].Metadata = make(map[string]string, 1)
			}
			changes[idx].Metadata["vschema_changed"] = "true"
		}
	}
	return changes
}

func (c *LocalClient) notifyTerminalObserver(apply *storage.Apply, tasks []*storage.Task) {
	if obs := c.getObserver(apply.ID); obs != nil {
		obs.OnTerminal(apply, tasks)
		c.clearObserver(apply.ID)
	}
}

// ResumeApply starts or resumes an apply claimed by an operator driver.
// Pending applies are dispatched for the first time; stale applies use the
// engine's resume metadata to continue after a missed heartbeat.
func (c *LocalClient) ResumeApply(ctx context.Context, apply *storage.Apply) error {
	tasks, err := c.storage.Tasks().GetByApplyID(ctx, apply.ID)
	if err != nil {
		return fmt.Errorf("get tasks for apply %s: %w", apply.ApplyIdentifier, err)
	}
	// Whole-apply scope has no single operation to order, so the stored apply
	// options govern the drive directly (no automatic barrier park).
	return c.resumeApplyWithTasks(ctx, apply, tasks, apply.GetOptions().Map(), false, false)
}

// ResumeApplyOperation starts or resumes a single apply_operation (one
// deployment of a multi-deployment apply), driving only that operation's tasks.
// The drive logic is identical to ResumeApply; the only difference is that tasks
// are loaded scoped to the operation rather than the whole apply, so a driver
// can advance one deployment independently of its siblings.
func (c *LocalClient) ResumeApplyOperation(ctx context.Context, apply *storage.Apply, applyOperationID int64) error {
	op, err := c.storage.ApplyOperations().Get(ctx, applyOperationID)
	if err != nil {
		return fmt.Errorf("get apply_operation %d (apply %s): %w", applyOperationID, apply.ApplyIdentifier, err)
	}
	if op == nil {
		return fmt.Errorf("apply_operation %d (apply %s): %w", applyOperationID, apply.ApplyIdentifier, ErrApplyOperationRowMissing)
	}
	// Trust-boundary guard: the operation must belong to the passed-in apply, or
	// an operation-scoped drive could advance another apply's deployment under
	// this apply's state. The routing and gRPC paths enforce the same invariant.
	if op.ApplyID != apply.ID {
		return fmt.Errorf("apply_operation %d belongs to apply %d, not %s (%d)", applyOperationID, op.ApplyID, apply.ApplyIdentifier, apply.ID)
	}
	tasks, err := c.storage.Tasks().GetByApplyOperationID(ctx, applyOperationID)
	if err != nil {
		return fmt.Errorf("get tasks for apply_operation %d (apply %s): %w", applyOperationID, apply.ApplyIdentifier, err)
	}
	if len(tasks) == 0 {
		// A group_finalizer carries no tasks: it applies the namespace's VSchema
		// from the plan once its sibling shard work has completed. A work operation
		// with no tasks is valid only when the plan itself carries VSchema work;
		// otherwise it is the fail-closed signal for an invalid or mismatched
		// applyOperationID.
		if op.OperationKind == storage.ApplyOperationKindGroupFinalizer {
			return c.driveGroupFinalizer(ctx, apply, op)
		}
		plan, planErr := c.storage.Plans().GetByID(ctx, apply.PlanID)
		if planErr != nil {
			return fmt.Errorf("get plan for task-less apply_operation %d (apply %s): %w", applyOperationID, apply.ApplyIdentifier, planErr)
		}
		if !isTasklessVSchemaOnlyPlan(tasks, plan) || op.OperationKind != storage.ApplyOperationKindWork || op.OperationKey != "" {
			return fmt.Errorf("apply_operation %d (apply %s): %w", applyOperationID, apply.ApplyIdentifier, ErrNoTasksForApplyOperation)
		}
		return c.resumeApplyWithTasks(ctx, apply, tasks, apply.GetOptions().Map(), false, false)
	}
	siblings, err := c.storage.ApplyOperations().ListByApply(ctx, apply.ID)
	if err != nil {
		return fmt.Errorf("list apply_operations for apply %s: %w", apply.ApplyIdentifier, err)
	}
	multiOperation := len(siblings) > 1
	// A multi-deployment barrier operation auto-defers its cutover and parks
	// (releases) the copy drive at the barrier; the deployment-ordered cutover
	// claim drives the swap later. Single-operation or rolling drives are
	// unchanged.
	releaseAtCutoverBarrier := shouldReleaseAtCutoverBarrier(apply, multiOperation, op)
	options := effectiveCopyDriveOptions(apply, multiOperation, op).Map()
	return c.resumeApplyWithTasks(ctx, apply, tasks, options, releaseAtCutoverBarrier, false)
}

// ResumeApplyOperationCutover drives a single apply_operation parked at the
// cutover barrier (waiting_for_cutover) through its cutover phase. It is the
// deployment-ordered counterpart to ResumeApplyOperation's copy drive: the
// operator claims the parked operation whose turn it is and calls this to force
// the high-risk swap, while siblings stay parked. Tasks are scoped to the
// operation, so an empty result fails closed the same way ResumeApplyOperation
// does rather than failing the whole parent apply.
func (c *LocalClient) ResumeApplyOperationCutover(ctx context.Context, apply *storage.Apply, applyOperationID int64) error {
	if apply == nil {
		return fmt.Errorf("stored apply is required to drive apply_operation %d cutover", applyOperationID)
	}
	op, err := c.storage.ApplyOperations().Get(ctx, applyOperationID)
	if err != nil {
		return fmt.Errorf("get apply_operation %d (apply %s): %w", applyOperationID, apply.ApplyIdentifier, err)
	}
	if op == nil {
		return fmt.Errorf("apply_operation %d (apply %s): %w", applyOperationID, apply.ApplyIdentifier, ErrApplyOperationRowMissing)
	}
	// Trust-boundary guard: the operation must belong to the passed-in apply, or
	// the cutover drive could force another apply's deployment through its swap
	// under this apply's state. The routing and gRPC paths enforce the same.
	if op.ApplyID != apply.ID {
		return fmt.Errorf("apply_operation %d belongs to apply %d, not %s (%d)", applyOperationID, op.ApplyID, apply.ApplyIdentifier, apply.ID)
	}
	tasks, err := c.storage.Tasks().GetByApplyOperationID(ctx, applyOperationID)
	if err != nil {
		return fmt.Errorf("get tasks for apply_operation %d (apply %s): %w", applyOperationID, apply.ApplyIdentifier, err)
	}
	if len(tasks) == 0 {
		// A task-less group_finalizer applies its namespace's VSchema (including
		// any cutover) from the plan; drive it directly. A work operation with no
		// tasks fails closed rather than failing the whole parent apply.
		if op.OperationKind == storage.ApplyOperationKindGroupFinalizer {
			return c.driveGroupFinalizer(ctx, apply, op)
		}
		return fmt.Errorf("apply_operation %d (apply %s): %w", applyOperationID, apply.ApplyIdentifier, ErrNoTasksForApplyOperation)
	}
	// Fail closed unless the operation is actually in a cutover phase. A
	// copy-phase or terminal operation must never be forced into a cutover drive.
	if !isCutoverDriveState(op.State) {
		return fmt.Errorf("apply_operation %d (apply %s) is in state %q, not parked or recovering for cutover", applyOperationID, apply.ApplyIdentifier, op.State)
	}
	// Force the cutover: clear DeferCutover so the drive auto-triggers the swap
	// when the engine reaches waiting_for_cutover, and never release at the
	// barrier (this drive *is* the ordered-cutover claim, not the copy park).
	// The barrier park deliberately does not persist DeferCutover onto the
	// shared apply, so forceCutoverResume tells the resume path to still load
	// the parked engine checkpoint before driving.
	opts := apply.GetOptions()
	opts.DeferCutover = false
	return c.resumeApplyWithTasks(ctx, apply, tasks, opts.Map(), false, true)
}

// finalizerOperationKeySuffix is the trailing segment of a group_finalizer
// operation key (namespace + "/" + segment), assigned at apply creation. The
// drive parses the namespace back out to reconstruct the VSchema change.
const finalizerOperationKeySuffix = "/group_finalizer"

// namespaceFromFinalizerKey recovers the namespace a group_finalizer operation
// targets from its operation key. Returns empty when the key is not a finalizer
// key, which the caller treats as a fail-closed condition.
func namespaceFromFinalizerKey(operationKey string) string {
	if !strings.HasSuffix(operationKey, finalizerOperationKeySuffix) {
		return ""
	}
	return strings.TrimSuffix(operationKey, finalizerOperationKeySuffix)
}

// driveGroupFinalizer drives a task-less group_finalizer operation: it applies
// the VSchema once its sibling shard work has completed. The change is
// reconstructed from the plan (the finalizer carries no task) and engine resume
// state is persisted on the operation row so a reclaim reattaches to in-flight
// work instead of starting over.
//
// The drive is engine-agnostic: it applies, then polls Progress to a terminal
// state. For an instance-local engine the VSchema apply is synchronous, so
// Progress reports terminal on the first poll; for an externally-authoritative
// engine (whose Apply starts an asynchronous deploy) Progress tracks the deploy
// to completion. Either way the operation is marked completed only once the
// engine reports the VSchema applied, and fails closed on any error (missing
// plan/engine, a rejected apply, a permanent progress error, or a failed
// terminal state), so the operator never advances the parent's aggregate as if
// the VSchema applied when it did not.
func (c *LocalClient) driveGroupFinalizer(ctx context.Context, apply *storage.Apply, op *storage.ApplyOperation) error {
	plan, err := c.storage.Plans().GetByID(ctx, apply.PlanID)
	if err != nil {
		return fmt.Errorf("load plan for group_finalizer apply_operation %d (apply %s): %w", op.ID, apply.ApplyIdentifier, err)
	}
	if plan == nil {
		return fmt.Errorf("plan %d not found for group_finalizer apply_operation %d (apply %s)", apply.PlanID, op.ID, apply.ApplyIdentifier)
	}
	namespace := namespaceFromFinalizerKey(op.OperationKey)
	changes, err := finalizerVSchemaChanges(plan, namespace)
	if err != nil {
		return fmt.Errorf("group_finalizer apply_operation %d (apply %s): %w", op.ID, apply.ApplyIdentifier, err)
	}
	eng := c.getEngine()
	if eng == nil {
		return fmt.Errorf("no engine available to drive group_finalizer apply_operation %d (apply %s)", op.ID, apply.ApplyIdentifier)
	}
	creds, err := c.credentialsForGroupedApply(plan)
	if err != nil {
		return fmt.Errorf("resolve credentials for group_finalizer apply_operation %d (apply %s): %w", op.ID, apply.ApplyIdentifier, err)
	}

	c.logger.Info("driving group_finalizer VSchema apply",
		"apply_id", apply.ApplyIdentifier,
		"apply_operation_id", op.ID,
		"deployment", op.Deployment,
		"namespace", namespace,
		"namespace_count", len(changes),
		"database", apply.Database,
	)
	if err := c.storage.ApplyOperations().MarkStarted(ctx, op.ID); err != nil {
		return fmt.Errorf("mark group_finalizer apply_operation %d started (apply %s): %w", op.ID, apply.ApplyIdentifier, err)
	}

	failClosed := func(cause error) error {
		if markErr := c.storage.ApplyOperations().MarkFailed(ctx, op.ID, cause.Error()); markErr != nil {
			c.logger.Error("group_finalizer: failed to mark operation failed",
				"apply_id", apply.ApplyIdentifier, "apply_operation_id", op.ID, "error", markErr)
		}
		return cause
	}
	persistResume := func(rs *engine.ResumeState) {
		if rs == nil {
			return
		}
		if saveErr := c.storage.ApplyOperations().SaveEngineResumeState(ctx, op.ID, &storage.EngineResumeState{
			ApplyOperationID: op.ID,
			MigrationContext: rs.MigrationContext,
			Metadata:         rs.Metadata,
		}); saveErr != nil {
			c.logger.Warn("group_finalizer: failed to persist engine resume state",
				"apply_id", apply.ApplyIdentifier, "apply_operation_id", op.ID, "error", saveErr)
		}
	}

	var resumeState *engine.ResumeState
	stored, getErr := c.storage.ApplyOperations().GetEngineResumeState(ctx, op.ID)
	switch {
	case errors.Is(getErr, storage.ErrEngineResumeStateNotFound):
		// No persisted resume state yet — this is the finalizer's first drive, so
		// start fresh.
	case getErr != nil:
		// A storage read failure must not be treated as "fresh": proceeding would
		// risk the engine restarting or duplicating in-flight VSchema work after a
		// transient DB error. Fail closed for the operator to retry.
		return failClosed(fmt.Errorf("load engine resume state for group_finalizer apply_operation %d (apply %s): %w", op.ID, apply.ApplyIdentifier, getErr))
	case stored != nil:
		resumeState = &engine.ResumeState{MigrationContext: stored.MigrationContext, Metadata: stored.Metadata}
	}

	result, err := eng.Apply(ctx, &engine.ApplyRequest{
		Database:      apply.Database,
		PlanID:        plan.PlanIdentifier,
		Changes:       changes,
		SchemaFiles:   plan.SchemaFiles,
		Options:       apply.GetOptions().Map(),
		ResumeState:   resumeState,
		Credentials:   creds,
		OnStateChange: persistResume,
	})
	if err != nil {
		return failClosed(fmt.Errorf("apply VSchema for group_finalizer (apply %s): %w", apply.ApplyIdentifier, err))
	}
	if result == nil || !result.Accepted {
		return failClosed(fmt.Errorf("group_finalizer VSchema apply for apply %s was not accepted", apply.ApplyIdentifier))
	}

	// A nil resume state means the engine has no in-flight work to track: the
	// VSchema apply finished synchronously, or the deploy was a no-op (no DDL
	// diff — a VSchema applied at the branch level produces a no-changes deploy).
	// The accepted result is terminal, so complete without polling. Only an
	// in-flight deploy (a returned resume state) is polled to completion.
	if result.ResumeState != nil {
		persistResume(result.ResumeState)
		finalState, err := c.driveFinalizerToTerminal(ctx, eng, apply, creds, result.ResumeState, persistResume)
		if err != nil {
			return failClosed(fmt.Errorf("await group_finalizer VSchema apply (apply %s): %w", apply.ApplyIdentifier, err))
		}
		if !finalizerVSchemaApplied(finalState) {
			return failClosed(fmt.Errorf("group_finalizer VSchema apply for apply %s ended in non-success state %q", apply.ApplyIdentifier, finalState))
		}
	}
	if err := c.storage.ApplyOperations().MarkCompleted(ctx, op.ID); err != nil {
		return fmt.Errorf("mark group_finalizer apply_operation %d completed (apply %s): %w", op.ID, apply.ApplyIdentifier, err)
	}
	c.logger.Info("group_finalizer VSchema apply completed",
		"apply_id", apply.ApplyIdentifier, "apply_operation_id", op.ID, "namespace", namespace)
	return nil
}

// finalizerVSchemaChanges reconstructs the VSchema change(s) a group_finalizer
// applies, from the plan. A namespace-scoped finalizer (operation key
// "<ns>/group_finalizer", from a sharded fan-out) applies that one namespace's
// VSchema. A finalizer with no namespace in its key (a non-sharded VSchema-only
// apply on an externally-authoritative engine) applies every VSchema-changed
// namespace in the plan, because that engine deploys the whole branch in one
// operation.
func finalizerVSchemaChanges(plan *storage.Plan, namespace string) ([]engine.SchemaChange, error) {
	vschemaChange := func(ns string) engine.SchemaChange {
		return engine.SchemaChange{Namespace: ns, Metadata: map[string]string{"vschema_changed": "true"}}
	}
	if namespace != "" {
		nsData := plan.Namespaces[namespace]
		if nsData == nil || !namespaceHasVSchemaArtifact(nsData) {
			return nil, fmt.Errorf("plan %d has no VSchema artifact for namespace %q", plan.ID, namespace)
		}
		return []engine.SchemaChange{vschemaChange(namespace)}, nil
	}
	namespaces := make([]string, 0, len(plan.Namespaces))
	for ns, nsData := range plan.Namespaces {
		if namespaceHasVSchemaArtifact(nsData) {
			namespaces = append(namespaces, ns)
		}
	}
	if len(namespaces) == 0 {
		return nil, fmt.Errorf("plan %d has no VSchema artifact for a deployment-scoped finalizer", plan.ID)
	}
	sort.Strings(namespaces)
	changes := make([]engine.SchemaChange, 0, len(namespaces))
	for _, ns := range namespaces {
		changes = append(changes, vschemaChange(ns))
	}
	return changes, nil
}

// finalizerVSchemaApplied reports whether an engine progress state means the
// VSchema is applied. Completed is the terminal success; revert_window means the
// deploy succeeded and is holding open its revert window (the VSchema is live),
// which is success for the finalizer — the apply-level revert window is managed
// separately.
func finalizerVSchemaApplied(s engine.State) bool {
	return s == engine.StateCompleted || s == engine.StateRevertWindow
}

// driveFinalizerToTerminal polls the engine until the finalizer's work reaches a
// terminal (or revert-window) state, persisting resume state on each poll so a
// reclaim reattaches. An instance-local engine reports terminal on the first
// poll; an externally-authoritative engine is tracked to deploy completion. A
// permanent progress error fails the drive; transient errors are retried while
// the operation lease is heartbeated by the operator.
func (c *LocalClient) driveFinalizerToTerminal(ctx context.Context, eng engine.Engine, apply *storage.Apply, creds *engine.Credentials, resumeState *engine.ResumeState, persistResume func(*engine.ResumeState)) (engine.State, error) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		res, err := eng.Progress(ctx, &engine.ProgressRequest{
			Database:    apply.Database,
			Credentials: creds,
			ResumeState: resumeState,
		})
		if err != nil {
			var permanent *engine.PermanentError
			if errors.As(err, &permanent) {
				return "", fmt.Errorf("group_finalizer progress poll failed permanently: %w", err)
			}
			c.logger.Warn("group_finalizer: transient progress error; will retry",
				"apply_id", apply.ApplyIdentifier, "error", err)
		} else {
			if res.ResumeState != nil {
				persistResume(res.ResumeState)
				resumeState = res.ResumeState
			}
			if res.State.IsTerminal() || res.State == engine.StateRevertWindow {
				return res.State, nil
			}
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}

// resumeApplyWithTasks drives an apply (or one of its operations) from the set
// of tasks the caller has loaded. Callers choose whether tasks are scoped to the
// whole apply or to a single operation.
func (c *LocalClient) resumeApplyWithTasks(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, options map[string]string, releaseAtCutoverBarrier bool, forceCutoverResume bool) error {
	if handled, err := c.processPendingCancelOrStopControlRequest(ctx, apply); handled || err != nil {
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
			c.logger.Error("failed to update apply state", append(apply.LogAttrs(), "error", err)...)
		}
		c.notifyTerminalObserver(apply, tasks)
		return nil
	}
	if len(tasks) == 0 && !isTasklessVSchemaOnlyPlan(tasks, plan) {
		// A task-less apply has no per-table work to drive — e.g. a sharded
		// dispatch for a shard whose schema already matches the desired state (a
		// no-op), or an apply whose tasks already completed. The initial drive
		// completes such an apply (finalizeSequentialApply with no failed task);
		// recovery must complete it too rather than failing it. (A VSchema-only
		// plan is exempted above and re-driven below so its VSchema is applied.)
		now := time.Now()
		if freshApply, err := c.storage.Applies().Get(ctx, apply.ID); err == nil && freshApply != nil && state.IsTerminalApplyState(freshApply.State) {
			// A concurrent drive already settled it — adopt its verdict and still
			// notify this recovery's observer so a registered waiter (e.g. the PR
			// check/comment) sees the terminal state instead of hanging.
			*apply = *freshApply
			c.notifyTerminalObserver(apply, tasks)
			return nil
		}
		c.logger.Info("no tasks found for apply during recovery; completing as a no-op",
			"apply_id", apply.ApplyIdentifier)
		apply.State = state.Apply.Completed
		apply.CompletedAt = &now
		apply.UpdatedAt = now
		if err := c.storage.Applies().Update(ctx, apply); err != nil {
			// Don't report a completion the operator can't see: surface the error so
			// recovery retries, and notify the observer only after a durable write.
			return fmt.Errorf("complete task-less apply %s during recovery: %w", apply.ApplyIdentifier, err)
		}
		c.notifyTerminalObserver(apply, tasks)
		return nil
	}
	if handled, err := c.processPendingStartControlRequest(ctx, apply, options, releaseAtCutoverBarrier); handled || err != nil {
		return err
	}

	if state.IsState(apply.State, state.Apply.Pending) && apply.StartedAt == nil {
		c.dispatchQueuedApply(ctx, apply, tasks, plan, options, releaseAtCutoverBarrier)
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
	if shouldInspectCutoverSignalForResume(apply, forceCutoverResume) {
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
				resumeCtx, cancelResume := context.WithCancel(ctx)
				cancelGeneration := c.setApplyCancel(cancelResume)
				defer c.clearApplyCancel(cancelGeneration)
				defer cancelResume()
				if err := c.launchAtomicResume(resumeCtx, apply, tasks, plan, options, "Recovering from checkpoint", true, false, releaseAtCutoverBarrier); err != nil {
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
		c.logger.Error("re-plan failed during recovery", append(apply.LogAttrs(), "error", err)...)
		return fmt.Errorf("re-plan failed during recovery for apply %s (database %s): %w", apply.ApplyIdentifier, apply.Database, err)
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
	startControlReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationStart)
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
			c.logger.Error("failed to update apply state", append(apply.LogAttrs(), "error", err)...)
		}
		if startRequested {
			if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart); err != nil {
				return err
			}
		}
		c.notifyTerminalObserver(apply, tasks)
		return nil
	}

	c.prepareRetryableTasksForResume(ctx, apply, activeTasks)
	c.prepareStoppedTasksForResume(ctx, apply, activeTasks, startRequested)

	if c.usesGroupedApply(apply, options) {
		resumeCtx, cancelResume := context.WithCancel(ctx)
		cancelGeneration := c.setApplyCancel(cancelResume)
		defer c.clearApplyCancel(cancelGeneration)
		defer cancelResume()
		if err := c.launchAtomicResume(resumeCtx, apply, activeTasks, plan, options, fmt.Sprintf("Apply resumed from checkpoint (%s)", groupedApplyModeDescription(apply, options)), true, startRequested, releaseAtCutoverBarrier); err != nil {
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
			c.logger.Error("failed to update apply state", append(apply.LogAttrs(), "error", err)...)
			return fmt.Errorf("mark sequential resume apply %s running: %w", apply.ApplyIdentifier, err)
		}
		if startRequested {
			if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart); err != nil {
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
		if failErr := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart, err.Error()); failErr != nil {
			return failErr
		}
	}
	c.notifyTerminalObserver(apply, tasks)
	return err
}

func (c *LocalClient) dispatchQueuedApply(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, plan *storage.Plan, options map[string]string, releaseAtCutoverBarrier bool) {
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

	c.runApplyExecution(applyCtx, apply, tasks, plan, options, releaseAtCutoverBarrier)
}
