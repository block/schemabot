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
		RequestedBy: controlRequestRequester(req.Caller),
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
	c.wakeOperator(apply)
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

// stoppedApplyDiscoveryWindow bounds how far back a start that names no apply
// looks when choosing which stopped change to resume. It is a discovery
// heuristic for picking among candidates, not a safety gate: a start that names
// its apply resumes it however long it has been resting. Refusing a change the
// operator explicitly named, because a timestamp on it is old, reads to them as
// the change not existing — and the re-plan and the engine both still get their
// say on whether its copy is still usable.
const stoppedApplyDiscoveryWindow = 7 * 24 * time.Hour

func (c *LocalClient) resolveStartRequest(ctx context.Context, req *ternv1.StartRequest) (*storage.Apply, int64, int64, error) {
	tasks, err := c.storage.Tasks().GetByDatabase(ctx, c.config.Database)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("get tasks failed: %w", err)
	}

	// Find the target apply: either from the request's apply_id or the most recent stopped apply.
	// We scope to a single apply to avoid cross-contamination: a poller race can
	// erroneously mark tasks from earlier applies as STOPPED (see pollTaskToCompletion).
	var apply *storage.Apply
	// The most recent stopped task discovery passed over for age alone. When
	// nothing lands in the window, its apply is the hint the refusal points at.
	var passedOver *storage.Task

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
			if !state.IsState(task.State, state.Task.Stopped) {
				continue
			}
			if time.Since(task.UpdatedAt) > stoppedApplyDiscoveryWindow {
				c.logger.Info("Start: stopped task is older than the discovery window, so an unqualified start passes over it; naming its apply still resumes it",
					append(task.LogAttrs(), "updated_at", task.UpdatedAt, "discovery_window", stoppedApplyDiscoveryWindow)...)
				if passedOver == nil || task.UpdatedAt.After(passedOver.UpdatedAt) {
					passedOver = task
				}
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
		return nil, 0, 0, c.noDiscoverableApplyError(ctx, len(tasks), passedOver)
	}

	// Deferred deploy that isn't ready yet — reject with a clear message.
	if apply.GetOptions().DeferDeploy && !state.IsState(apply.State, state.Apply.WaitingForDeploy) {
		return nil, 0, 0, fmt.Errorf("schema change is not ready for deploy (current state: %s)", apply.State)
	}
	if state.IsState(apply.State, state.Apply.WaitingForDeploy) {
		return apply, 1, 0, nil
	}
	if !state.IsState(apply.State, state.Apply.Stopped) && !state.IsRunningApplyState(apply.State) {
		return nil, 0, 0, fmt.Errorf("schema change is not stopped (current state: %s)", apply.State)
	}

	var startedCount, skippedCount int64
	var unresumable []string
	for _, task := range tasks {
		if task.ApplyID != apply.ID {
			continue
		}
		if state.IsTerminalTaskState(task.State) {
			// Settled by an earlier drive: the table needs no resuming, and the
			// count is what tells the operator how much of the change is done.
			skippedCount++
			continue
		}
		if !state.IsState(task.State, state.Task.Stopped) {
			// Not resting, so a start has nothing to hand it. Under a claimable
			// apply a driver reaches it on its own; under a terminal one the
			// conflict check settles it.
			unresumable = append(unresumable, task.State)
			c.logger.Warn("Start: task is not resting, so this start does not resume it",
				append(task.LogAttrs(), "apply_id", apply.ApplyIdentifier, "apply_state", apply.State)...)
			continue
		}
		startedCount++
	}

	if startedCount == 0 {
		return nil, 0, 0, c.noResumableWorkError(apply, len(tasks), skippedCount, unresumable)
	}
	c.logger.Info("Start: resolved resumable work",
		append(apply.LogAttrs(), "started_count", startedCount, "skipped_count", skippedCount)...)
	return apply, startedCount, skippedCount, nil
}

// noDiscoverableApplyError explains why an unqualified start resolved no apply
// at all. When discovery passed over a stopped task for age alone, the refusal
// names its apply and the command that resumes it anyway — "nothing to resume"
// would send the operator looking for a change that is sitting right there,
// and the hint is only useful on the surface they can see, not in a server log.
func (c *LocalClient) noDiscoverableApplyError(ctx context.Context, taskCount int, passedOver *storage.Task) error {
	if passedOver != nil && passedOver.ApplyID > 0 {
		a, err := c.storage.Applies().Get(ctx, passedOver.ApplyID)
		switch {
		case err != nil:
			c.logger.Warn("Start: failed to load the apply of the stopped task discovery passed over; the refusal cannot name it",
				append(passedOver.LogAttrs(), "error", err)...)
		case a == nil:
			c.logger.Warn("Start: the stopped task discovery passed over points at an apply that no longer exists; the refusal cannot name it",
				passedOver.LogAttrs()...)
		default:
			return fmt.Errorf("schema change %s has been resting longer than the %s an unqualified start searches; re-issue the start naming %s to resume it anyway",
				a.ApplyIdentifier, stoppedApplyDiscoveryWindow, a.ApplyIdentifier)
		}
	}
	return fmt.Errorf("no stopped schema change to resume (found %d tasks for database %s, none of them stopped within the last %s)",
		taskCount, c.config.Database, stoppedApplyDiscoveryWindow)
}

// noResumableWorkError explains why a start resolved an apply but found nothing
// on it to resume. Each cause asks a different thing of the operator — wait for
// the driver, reconcile the target, name the apply — so they are reported
// separately rather than collapsed into one "nothing to resume".
func (c *LocalClient) noResumableWorkError(apply *storage.Apply, taskCount int, skipped int64, unresumable []string) error {
	switch {
	case len(unresumable) > 0:
		return fmt.Errorf("schema change %s has no stopped work to resume: %d of its tasks are in a state start cannot act on (%s)",
			apply.ApplyIdentifier, len(unresumable), strings.Join(distinctSorted(unresumable), ", "))
	case skipped > 0:
		return fmt.Errorf("schema change %s has no stopped work to resume: all %d of its tasks already reached a terminal state",
			apply.ApplyIdentifier, skipped)
	default:
		return fmt.Errorf("no stopped schema change to resume (found %d tasks for database %s, none of them belonging to apply %s)",
			taskCount, apply.Database, apply.ApplyIdentifier)
	}
}

// distinctSorted reduces repeated task states to a stable, readable list for an
// operator-facing message.
func distinctSorted(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Strings(out)
	return out
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
	// The deferred deploy resolves credentials from applyTasks[0] and drives
	// every task with them, so all tasks must share one namespace: for MySQL it
	// selects the connection schema (per-target overrides can remap it to a
	// different physical schema), so with mixed namespaces every task would
	// silently run against tasks[0]'s schema.
	if usesPerNamespaceCredentials(c.config.Type) {
		if _, err := singleTaskNamespace(applyTasks); err != nil {
			return nil, fmt.Errorf("deferred deploy for apply %s: %w", apply.ApplyIdentifier, err)
		}
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
	// Bind the apply's identity once so every consumption log line is
	// filterable by apply_id/repo/pr without hand-listing the attrs per call.
	logger := c.logger.With(apply.IdentityLogAttrs()...)
	if stopReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationStop); err != nil {
		return true, fmt.Errorf("check pending stop before pending start for apply %s: %w", apply.ApplyIdentifier, err)
	} else if stopReq != nil {
		logger.Info("pending start request is waiting for pending stop request to finish",
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
	// A multi-operation drive owns only its operation: the parent running
	// write is the operator's projection to make — a direct write here fails
	// closed under the operation-only lease and would abort a deploy the
	// engine has already accepted. The start request also stays pending, so
	// sibling operations' deferred-deploy claims can still fire; the claim arm
	// closes once the projection moves the parent out of waiting_for_deploy.
	if suppressParentApplyWrites(ctx) {
		logger.Info("pending start request accepted under operation lease; parent running state is the operator's projection and the request stays pending for sibling operations",
			"requested_by", controlRequestCaller(controlReq))
	} else {
		if err := c.storage.Applies().Update(ctx, apply); err != nil {
			return true, fmt.Errorf("update started deferred deploy apply %s: %w", apply.ApplyIdentifier, err)
		}
		if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart); err != nil {
			return true, err
		}
		logger.Info("pending start request accepted and completed",
			"requested_by", controlRequestCaller(controlReq),
			"state", apply.State)
	}
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
	// A resumed drive runs the same engine work as the drive that started it, so
	// it needs the same log wiring: without it the engine's own lines stop
	// reaching the apply log stream the moment an apply changes hands, and the
	// stream goes quiet for exactly the drive an operator is trying to read.
	defer c.setupSpiritLogging(ctx, apply, tasks)()
	eng := c.getEngine()
	// Bind the apply's identity once so every line of this sequential resume is
	// filterable by apply_id/repo/pr without hand-listing the attrs per call.
	// Mutable attrs (task state, apply state) stay per-call so they are never
	// frozen stale into the bound logger.
	logger := c.logger.With(apply.IdentityLogAttrs()...)

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

		logger.Info("resumeApplySequential: starting task",
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
		replanned, needsChange, err := c.tableStillNeedsChange(ctx, apply, plan, task)
		if err != nil {
			logger.Warn("could not verify table schema state, proceeding with apply",
				"task_id", task.TaskIdentifier, "table", task.TableName, "error", err)
		} else if !needsChange {
			logger.Info("table already has desired schema, skipping",
				"task_id", task.TaskIdentifier, "table", task.TableName)
			now := time.Now()
			task.ProgressPercent = 100
			task.CompletedAt = &now
			// The completed state must durably land before the loop moves on:
			// finalization derives the apply's terminal state from these task
			// rows, so proceeding past a refused write — e.g. a lease-guarded
			// update that lost to a peer driver — could terminalize the apply
			// while the task row durably stays non-terminal. Aborting the
			// resume without finalizing leaves the apply claimable, so a later
			// drive redoes this settlement under a current lease.
			if err := c.persistTaskStateTransition(ctx, task, apply.ID, state.Task.Completed,
				fmt.Sprintf("Task %s already completed (cutover raced with re-plan)", task.TaskIdentifier)); err != nil {
				logger.Error("resume aborting: persisting a raced-cutover task settlement failed; the apply stays active for a later drive to redo the settlement",
					"task_id", task.TaskIdentifier, "table", task.TableName, "state", task.State, "error", err)
				return
			}
			continue
		} else if _, landed, err := c.verifyReplannedTaskDDL(task, replanned, tasks); err != nil {
			// The statements this shard now needs no longer include what this
			// task was reviewed with. Fail closed rather than apply unreviewed
			// DDL.
			logger.Error("resume aborting task: the re-plan no longer includes the task's reviewed DDL",
				"task_id", task.TaskIdentifier, "table", task.TableName, "state", task.State, "error", err)
			c.markTaskFailed(ctx, task, err.Error())
			failedTask = task
			break
		} else if landed {
			// The table still has pending statements, but every one of them
			// is the reviewed DDL of a sibling task that is not yet terminal:
			// this task's own statement executed before its outcome was
			// recorded. Settle it without re-running the statement; the same
			// durability rule as the raced-cutover branch above applies.
			logger.Info("task statement already landed on the table, skipping", task.LogAttrs()...)
			now := time.Now()
			task.ProgressPercent = 100
			task.CompletedAt = &now
			if err := c.persistTaskStateTransition(ctx, task, apply.ID, state.Task.Completed,
				fmt.Sprintf("Task %s already completed (its statement landed before its outcome was recorded)", task.TaskIdentifier)); err != nil {
				logger.Error("resume aborting: persisting a landed-statement task settlement failed; the apply stays active for a later drive to redo the settlement",
					append(task.LogAttrs(), "error", err)...)
				return
			}
			continue
		}

		action = c.runEngineTask(ctx, apply, task, options)

		taskID := task.ID
		c.logApplyEvent(ctx, apply.ID, &taskID, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			fmt.Sprintf("Task %s resumed (sequential %d/%d)", task.TaskIdentifier, i+1, len(tasks)),
			state.Task.Stopped, state.Task.Running)

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
	c.finalizeSequentialApply(ctx, apply, tasks, failedTask, stoppedByUser)
	logger.Info("sequential resume finished", "state", apply.State)
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
// (namespace, shard, table) -> the DDL statements still pending for that table,
// in plan order, so the resume/recovery path reconciles each task against its
// own namespace and shard. A sharded engine emits one SchemaChange per
// (namespace, shard) and the same table repeats across them, so keying by table
// name alone would conflate tasks: another shard's (or another keyspace's)
// remaining diff could keep this task active, or update it with the wrong DDL.
// Within one (namespace, shard) a table can carry several statements, each its
// own task, so every statement is kept for the task to be matched against.
func replanShardTableDDL(result *engine.PlanResult) map[shardTableKey][]string {
	out := make(map[shardTableKey][]string)
	for _, sc := range result.Changes {
		for _, tc := range sc.TableChanges {
			key := shardTableKey{namespace: sc.Namespace, shard: sc.ShardName(), table: tc.Table}
			out[key] = append(out[key], tc.DDL)
		}
	}
	return out
}

// replanTargetSchema re-plans the reviewed schema set against the live target
// and indexes the remaining changes by (namespace, shard, table), so callers
// can look up whether each task's table still needs its change.
//
// The plan it returns is all-or-nothing: an engine that cannot read one of the
// target's namespaces fails the whole plan rather than returning the rest, so
// a table's absence from the result means the target has the reviewed schema
// and never means the target could not be asked. Callers that read absence as
// "the change already landed" depend on that, and on the key space this plan
// speaks in — see replanVerdictForTask, which is how a task should be judged
// against it.
func (c *LocalClient) replanTargetSchema(ctx context.Context, apply *storage.Apply, plan *storage.Plan) (map[shardTableKey][]string, error) {
	result, err := c.planWithEngine(ctx, &ternv1.PlanRequest{}, apply.Database, plan.SchemaFiles)
	if err != nil {
		return nil, fmt.Errorf("re-plan check failed: %w", err)
	}
	return replanShardTableDDL(result), nil
}

// replanVerdict is what a re-plan of the reviewed schema set says about one
// task whose engine work has gone quiet.
type replanVerdict int

const (
	// replanNeedsChange: the target still needs this task's change, so the
	// work is genuinely gone.
	replanNeedsChange replanVerdict = iota
	// replanChangeLanded: the target already has the reviewed schema for this
	// task, so the work finished and only its outcome was lost.
	replanChangeLanded
	// replanCannotAttribute: the re-plan does not speak for this task's scope,
	// so its silence is not evidence either way.
	replanCannotAttribute
)

// replanVerdictForTask judges one task against a target re-plan.
//
// A table still in the re-plan needs its change. A table absent from it is
// where the caller has to be careful: absence only means "already applied"
// when the re-plan actually covers the scope the task ran in. Re-planning the
// reviewed schema set describes whole namespaces — no engine's Plan emits a
// per-shard change, because an engine that fans a change out to its shards
// behind one endpoint reports the namespace as a unit — while a shard-scoped
// dispatch tags its tasks with the shard they ran on. Such a task can never
// match a whole-namespace key, so reading its absence as success would
// complete a shard's change on evidence that never mentioned the shard. It is
// unattributable instead, and the caller rests it retryable.
//
// The check is on what the re-plan demonstrably covered rather than on the
// engine, so a plan that does key by shard settles its shard-tagged tasks
// normally.
func replanVerdictForTask(replanDDL map[shardTableKey][]string, task *storage.Task) replanVerdict {
	if _, needsChange := replanDDL[shardTableKey{namespace: task.Namespace, shard: task.Shard, table: task.TableName}]; needsChange {
		return replanNeedsChange
	}
	if task.Shard != "" && !replanCoversShards(replanDDL, task.Namespace) {
		return replanCannotAttribute
	}
	return replanChangeLanded
}

// replanCoversShards reports whether a re-plan described the namespace one
// shard at a time, which is what makes a shard-tagged table's absence from it
// meaningful.
func replanCoversShards(replanDDL map[shardTableKey][]string, namespace string) bool {
	for key := range replanDDL {
		if key.namespace == namespace && key.shard != "" {
			return true
		}
	}
	return false
}

// tableStillNeedsChange re-plans the full schema set and then looks up whether
// this task's table still needs a change on its (namespace, shard). Returns
// false if it already has the desired schema (e.g., Spirit's cutover completed
// during the stop sequence). When the table still needs changes, it also returns
// the statements the re-plan would now apply to it so the caller can confirm the
// task's own statement is still among them before applying it.
func (c *LocalClient) tableStillNeedsChange(ctx context.Context, apply *storage.Apply, plan *storage.Plan, task *storage.Task) ([]string, bool, error) {
	replanDDL, err := c.replanTargetSchema(ctx, apply, plan)
	if err != nil {
		return nil, false, err
	}
	statements, stillNeeded := replanDDL[shardTableKey{namespace: task.Namespace, shard: task.Shard, table: task.TableName}]
	return statements, stillNeeded, nil
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
		if state.IsState(task.State, state.Task.Completed) {
			continue
		}
		if state.IsState(task.State, state.Task.Reverted) {
			// Terminal: the revert already landed for this task. It carries no
			// remaining resume work, and re-activating it would flip a terminal
			// task back to running until the engine re-terminalizes it.
			c.logger.Debug("leaving reverted task terminal during resume re-plan", task.LogAttrs()...)
			continue
		}
		if taskInRevertPhase(task) {
			// Post-cutover, the live schema matches the reviewed target by
			// definition: the change is applied and the engine is holding the
			// revert window open or unwinding it. A schema match therefore says
			// nothing about this task settling — completing it here would
			// terminalize the apply as a success while the engine reverts the
			// schema change underneath it. The task stays active (reviewed DDL
			// untouched) so the resume reattaches to the engine and the task's
			// terminal state comes from engine progress, never from this
			// schema comparison.
			c.logger.Info("keeping revert-phase task active through resume re-plan; engine progress decides its terminal state",
				task.LogAttrs()...)
			activeTasks = append(activeTasks, task)
			continue
		}
		replanned, stillNeeded := replanDDL[shardTableKey{namespace: task.Namespace, shard: task.Shard, table: task.TableName}]
		if !stillNeeded {
			// The re-plan diffs the reviewed target (plan.SchemaFiles) against
			// this shard's live schema. A table dropping out of that diff means
			// live already matches the reviewed target, so there is no remaining
			// resume work for it — treat it as completed rather than drifted.
			task.ProgressPercent = 100
			task.CompletedAt = &now
			// The completed state must durably land before the task counts as
			// settled: the caller derives parent apply state from this
			// partition, so proceeding past a refused write — e.g. a
			// lease-guarded update that lost to a peer driver — would
			// terminalize the apply while the task row durably stays
			// non-terminal. Failing the re-plan releases the drive for a later
			// claim to redo this settlement under a current lease.
			if err := c.persistTaskStateTransition(ctx, task, apply.ID, state.Task.Completed,
				fmt.Sprintf("Task %s already completed (live schema matches the reviewed target)", task.TaskIdentifier)); err != nil {
				return nil, fmt.Errorf("persist completed state for task %s whose table left the resume re-plan diff: %w", task.TaskIdentifier, err)
			}
			completedCount++
		} else {
			// Fail closed if the re-plan would apply DDL this task was not
			// reviewed with: the re-plan recomputes the delta against live
			// schema, so on a drifted deployment it can produce unreviewed DDL
			// that overwriting task.DDL would silently apply.
			ddl, landed, err := c.verifyReplannedTaskDDL(task, replanned, tasks)
			if err != nil {
				return nil, err
			}
			if landed {
				// The table is still in the diff, but only for the reviewed
				// DDL of siblings that will still run it: this task's own
				// statement executed before its outcome was recorded. Settle
				// it under the same durability rule as the table-absent branch.
				c.logger.Info("task statement already landed; settling it without re-executing",
					task.LogAttrs()...)
				task.ProgressPercent = 100
				task.CompletedAt = &now
				if err := c.persistTaskStateTransition(ctx, task, apply.ID, state.Task.Completed,
					fmt.Sprintf("Task %s already completed (its statement landed before its outcome was recorded)", task.TaskIdentifier)); err != nil {
					return nil, fmt.Errorf("persist completed state for task %s whose statement landed before its outcome was recorded: %w", task.TaskIdentifier, err)
				}
				completedCount++
				continue
			}
			task.DDL = ddl
			activeTasks = append(activeTasks, task)
		}
	}

	return &replanResult{ActiveTasks: activeTasks, CompletedCount: completedCount}, nil
}

// taskInRevertPhase reports whether the task sits in an engine-monitored
// revert-phase state: the revert window is open or a revert is in flight.
// While a task is in one of these states, comparing live schema against the
// reviewed target proves nothing about the task's outcome — a match is the
// expected live state until a revert lands, and a mismatch just means the
// revert already landed.
func taskInRevertPhase(task *storage.Task) bool {
	return state.IsState(task.State, state.Task.RevertWindow, state.Task.Reverting)
}

// applyInRevertPhase reports whether the apply dwells in an engine-monitored
// revert-phase state: the revert window is open, a revert is in flight, or the
// window is being skipped. An apply in one of these states always has engine
// work in flight, and the persisted state is the durable signal that
// revert-phase handling — never fresh forward-apply handling — owns its
// outcome.
func applyInRevertPhase(apply *storage.Apply) bool {
	return state.IsState(apply.State, state.Apply.RevertWindow, state.Apply.Reverting, state.Apply.SkippingRevert)
}

// verifyReplannedTaskDDL returns, from the statements a resume re-plan would
// now apply to the task's table, the one this task will run, and fails closed
// when none of them is the DDL the task was reviewed with. The resume re-plan
// recomputes each deployment's own delta against its live schema; on a
// deployment whose schema has drifted, that recomputed delta is DDL no human
// reviewed, and overwriting task.DDL with it would apply it silently. Comparing
// canonical forms tolerates incidental formatting differences so only a real
// semantic divergence trips the guard.
//
// A table can carry several statements, each its own task, so the task's
// statement is matched among all of the table's re-planned statements. When
// none matches, the table's other tasks decide what the absence means. A
// remaining re-planned statement that is the reviewed DDL of a sibling task
// that will still run it is vouched for: it is reviewed plan DDL, not drift (a
// stopped sibling vouches too, since a start runs it forward). When every
// remaining statement is vouched for, this task's own statement is the only
// thing that could have left the diff, so it landed before its outcome was
// recorded and the task is reported landed with no DDL to run. That settlement
// rests on the schema evidence alone, not on the siblings. A remaining
// statement that is the reviewed DDL of a sibling that will not run it — one
// already terminal, or one in a revert phase whose statement the engine is
// unwinding — is not drift either, but nothing will run it forward, so the
// resume refuses and says so without calling it drift. A remaining statement
// no sibling was reviewed with is drift and is refused. The siblings are the other tasks of
// the same apply operation that share the task's (namespace, shard, table);
// the callers pass the task set they are iterating so that a sibling settled
// earlier in the same pass no longer vouches for a statement.
//
// A task with no reviewed DDL carries no reference to compare against (only the
// legacy synthetic VSchema tasks, which the engine-change builder already
// skips), so it is left to existing handling with the table's last statement.
func (c *LocalClient) verifyReplannedTaskDDL(task *storage.Task, replanned []string, tasks []*storage.Task) (ddl string, landed bool, err error) {
	if len(replanned) == 0 {
		return "", false, fmt.Errorf("task %s: re-plan emitted no statements for its table", task.TaskIdentifier)
	}
	if task.DDL == "" {
		return replanned[len(replanned)-1], false, nil
	}
	parser, err := c.statementParser()
	if err != nil {
		return "", false, fmt.Errorf("task %s: %w", task.TaskIdentifier, err)
	}
	reviewedCanon, err := canonicalDDLForDrift(parser, task.DDL)
	if err != nil {
		return "", false, fmt.Errorf("reviewed DDL for task %s: %w", task.TaskIdentifier, err)
	}
	replannedCanon := make([]string, 0, len(replanned))
	for _, stmt := range replanned {
		canon, err := canonicalDDLForDrift(parser, stmt)
		if err != nil {
			return "", false, fmt.Errorf("re-planned DDL for task %s: %w", task.TaskIdentifier, err)
		}
		if canon == reviewedCanon {
			return stmt, false, nil
		}
		replannedCanon = append(replannedCanon, canon)
	}

	siblings := siblingTasks(task, tasks)
	// Each pending sibling vouches for one occurrence of its statement, so a
	// statement the re-plan lists more often than pending siblings were
	// reviewed with it is still unreviewed.
	vouchers := make(map[string]int, len(siblings.pending))
	for _, sibling := range siblings.pending {
		canon, err := canonicalDDLForDrift(parser, sibling.DDL)
		if err != nil {
			return "", false, fmt.Errorf("reviewed DDL for sibling task %s of task %s: %w", sibling.TaskIdentifier, task.TaskIdentifier, err)
		}
		vouchers[canon]++
	}
	// Siblings that will not run their statement likewise explain one
	// occurrence of it each, so an occurrence beyond what they were reviewed
	// with is still unreviewed.
	ownerCanons := make([]string, len(siblings.willNotRun))
	owners := make(map[string]int, len(siblings.willNotRun))
	for i, sibling := range siblings.willNotRun {
		canon, err := canonicalDDLForDrift(parser, sibling.DDL)
		if err != nil {
			return "", false, fmt.Errorf("reviewed DDL for sibling task %s (%s) of task %s: %w", sibling.TaskIdentifier, sibling.State, task.TaskIdentifier, err)
		}
		ownerCanons[i] = canon
		owners[canon]++
	}
	// A remaining statement is vouched (a pending sibling's reviewed DDL),
	// orphaned (the reviewed DDL of a sibling that will not run it — settled,
	// or unwinding it in a revert phase), or unreviewed (drift).
	orphanedCanons := make(map[string]struct{})
	unreviewed := make([]string, 0, len(replannedCanon))
	for _, canon := range replannedCanon {
		if vouchers[canon] > 0 {
			vouchers[canon]--
			continue
		}
		if owners[canon] > 0 {
			owners[canon]--
			orphanedCanons[canon] = struct{}{}
			continue
		}
		unreviewed = append(unreviewed, canon)
	}
	if len(unreviewed) == 0 && len(orphanedCanons) == 0 {
		return "", true, nil
	}
	// The refusal names every non-running sibling reviewed with an orphaned
	// statement, not only the ones whose occurrence the accounting consumed:
	// when two such siblings share a statement the re-plan lists once, either
	// could be the one whose outcome the operator has to examine.
	var orphaned []*storage.Task
	for i, sibling := range siblings.willNotRun {
		if _, ok := orphanedCanons[ownerCanons[i]]; ok {
			orphaned = append(orphaned, sibling)
		}
	}

	loc := formatDriftLocation(driftChangeKey{
		namespace: task.Namespace,
		shard:     task.Shard,
		table:     task.TableName,
		operation: task.DDLAction,
	})
	if len(unreviewed) == 0 {
		return "", false, fmt.Errorf("local schema has not drifted from the reviewed plan, but resume cannot run task %s: its reviewed DDL %q is absent from the re-plan for %s while the re-plan still lists the reviewed DDL of %s, which will not run it; resume refuses to run another task's statement in this task's place",
			task.TaskIdentifier, reviewedCanon, loc, describeSiblingsThatWillNotRun(orphaned))
	}
	if len(siblings.pending) == 0 && len(orphaned) == 0 && len(unreviewed) == 1 {
		return "", false, fmt.Errorf("local schema has drifted from the reviewed plan; resume would apply unreviewed DDL for %s: reviewed %q, re-planned %q",
			loc, reviewedCanon, unreviewed[0])
	}
	return "", false, fmt.Errorf("local schema has drifted from the reviewed plan; the re-plan lists %s for %s, including %s that neither task %s nor %s was reviewed with, so resume refuses to apply unreviewed DDL: reviewed %q, unreviewed %q",
		countOf(len(replannedCanon), "pending statement"), loc, countOf(len(unreviewed), "statement"), task.TaskIdentifier, describePendingSiblings(siblings.pending), reviewedCanon, unreviewed)
}

// countOf renders a count with its regular-plural noun, e.g. "1 statement" or
// "2 statements".
func countOf(n int, noun string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s", noun)
	}
	return fmt.Sprintf("%d %ss", n, noun)
}

// describePendingSiblings names the pending sibling tasks for an error message
// in a form that reads correctly with none, one, or several.
func describePendingSiblings(siblings []*storage.Task) string {
	switch len(siblings) {
	case 0:
		return "any pending sibling task"
	case 1:
		return "its pending sibling task " + siblings[0].TaskIdentifier
	default:
		ids := make([]string, len(siblings))
		for i, sibling := range siblings {
			ids[i] = sibling.TaskIdentifier
		}
		return fmt.Sprintf("its %d pending sibling tasks %s", len(siblings), strings.Join(ids, ", "))
	}
}

// describeSiblingsThatWillNotRun names the sibling tasks whose reviewed DDL
// the re-plan still lists but which will not run it, with each one's state, so
// an operator reading the refusal can see which task's outcome to examine.
func describeSiblingsThatWillNotRun(siblings []*storage.Task) string {
	descriptions := make([]string, len(siblings))
	for i, sibling := range siblings {
		descriptions[i] = fmt.Sprintf("%s (%s)", sibling.TaskIdentifier, sibling.State)
	}
	if len(siblings) == 1 {
		return "sibling task " + descriptions[0]
	}
	return "sibling tasks " + strings.Join(descriptions, ", ")
}

// tableSiblings partitions a task's siblings by whether they will still run
// their reviewed DDL forward.
type tableSiblings struct {
	// pending siblings will still run their reviewed DDL, so it vouches for a
	// statement still in the re-plan diff.
	pending []*storage.Task
	// willNotRun siblings will never run their reviewed DDL forward: they have
	// already settled, or they sit in a revert phase and are unwinding it.
	// Their reviewed DDL explains a statement still in the re-plan diff
	// without vouching for it.
	willNotRun []*storage.Task
}

// siblingWillRunItsDDL reports whether a sibling's reviewed DDL is still going
// to be run forward by that sibling. A terminal sibling's statement will never
// be run, and neither will a revert-phase sibling's — the engine is removing
// it — so neither can vouch for a statement the re-plan still lists.
func siblingWillRunItsDDL(sibling *storage.Task) bool {
	return !state.IsTerminalTaskState(sibling.State) && !taskInRevertPhase(sibling)
}

// siblingTasks returns the other tasks of the same apply and apply operation
// that share task's namespace, shard and table, split by whether they will
// still run their reviewed DDL. A sibling with no reviewed DDL has nothing to
// compare against and is left out of both.
func siblingTasks(task *storage.Task, tasks []*storage.Task) tableSiblings {
	key := shardTableKey{namespace: task.Namespace, shard: task.Shard, table: task.TableName}
	var siblings tableSiblings
	for _, other := range tasks {
		if other == task || (task.ID != 0 && other.ID == task.ID) {
			continue
		}
		if other.ApplyID != task.ApplyID || !sameApplyOperation(other.ApplyOperationID, task.ApplyOperationID) {
			continue
		}
		if (shardTableKey{namespace: other.Namespace, shard: other.Shard, table: other.TableName}) != key {
			continue
		}
		if other.DDL == "" {
			continue
		}
		if !siblingWillRunItsDDL(other) {
			siblings.willNotRun = append(siblings.willNotRun, other)
			continue
		}
		siblings.pending = append(siblings.pending, other)
	}
	return siblings
}

// sameApplyOperation reports whether two tasks belong to the same apply
// operation; two legacy single-deployment tasks (no operation) count as the
// same operation.
func sameApplyOperation(a, b *int64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
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
		// Every task is already terminal (the re-plan keeps anything with
		// remaining work active), so the apply's terminal state is derived from
		// the task states: all completed → completed, any reverted → reverted.
		// An apply whose tasks all reverted must terminalize as reverted, never
		// as a success.
		taskStates := make([]string, 0, len(allTasks))
		for _, t := range allTasks {
			taskStates = append(taskStates, t.State)
		}
		terminalState := state.DeriveApplyState(taskStates)
		c.logger.Info("final schema check found no remaining grouped resume work; terminalizing apply",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"database_type", apply.DatabaseType,
			"task_count", len(allTasks),
			"terminal_state", terminalState)
		oldApplyState := apply.State
		now := time.Now()
		apply.State = terminalState
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
			return fmt.Errorf("mark grouped resume apply %s %s after final schema check: %w", apply.ApplyIdentifier, terminalState, err)
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
			"All tasks already terminal on resume (final schema check shows no remaining changes)", oldApplyState, terminalState)
		c.notifyTerminalObserver(apply, allTasks)
		return nil
	}

	resumeState, err := c.groupedResumeState(ctx, apply, tasks)
	if err != nil {
		c.logger.Error("failed to resolve engine resume state for grouped resume; current apply owner will exit for operator retry",
			append(apply.LogAttrs(), "error", err)...)
		return err
	}

	// Route the engine's own log lines into this apply's log stream, so a
	// resumed drive reads like the drive that started it. The wiring goes up
	// before the engine accepts the resume — it emits setup and lock lines from
	// inside Apply — and comes down when the drive that polls the work returns.
	// A detached resume polls in its own goroutine that outlives this call, so
	// that goroutine unwires it instead.
	stopEngineLogging := c.setupSpiritLogging(ctx, apply, tasks)
	pollDetached := false
	defer func() {
		if !pollDetached {
			stopEngineLogging()
		}
	}()

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
		Logger:       c.logger.With(apply.IdentityLogAttrs()...),
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

	if err := c.persistReattachedResumeStates(ctx, apply, tasks, suppressParent, startRequested, logMessage); err != nil {
		return err
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
	pollDetached = true
	// The detached poll deliberately outlives the caller's context, so its log
	// wiring has to as well: a callback holding the caller's context records
	// nothing once that context is cancelled, and the engine lines for the rest
	// of the schema change are exactly the ones worth keeping.
	stopEngineLogging = c.setupSpiritLogging(resumeCtx, apply, tasks)
	go func() {
		defer cancelResume()
		defer stopHeartbeat()
		defer stopEngineLogging()
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

// persistReattachedResumeStates persists task and apply states after the
// engine accepts a grouped resume: tasks and the apply move to running
// (recovering, during a deferred-cutover recovery). Revert-phase task and
// apply states are preserved instead — the persisted revert-phase state is the
// durable marker that revert-phase handling owns the outcome, and it must
// survive until engine progress moves it so a later reclaim never mistakes the
// apply for a forward-running one.
func (c *LocalClient) persistReattachedResumeStates(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, suppressParent, startRequested bool, logMessage string) error {
	now := time.Now()
	oldApplyState := apply.State
	recovering := state.IsState(oldApplyState, state.Apply.Recovering)

	for _, task := range tasks {
		if taskInRevertPhase(task) {
			c.logger.Info("preserving revert-phase task state through resume reattach", task.LogAttrs()...)
			continue
		}
		taskState := state.Task.Running
		if recovering {
			taskState = state.Task.Recovering
		}
		c.transitionTaskState(ctx, task, 0, taskState, "")
	}

	switch {
	case recovering:
		apply.State = state.Apply.Recovering
	case applyInRevertPhase(apply):
		// The apply keeps its revert-phase state, mirroring the tasks above.
	default:
		apply.State = state.Apply.Running
	}
	apply.UpdatedAt = now
	// A multi-operation drive does not write the parent running state or complete
	// parent start requests; the operator projected the parent running before the
	// drive. Tasks are already persisted above.
	if suppressParent {
		return nil
	}
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
	return nil
}

// errGroupedResumeStateUnavailable marks a resume attempt whose persisted engine
// resume state could not be loaded (or ruled out) before the engine apply, or
// could not be persisted (or confirmed present) after the engine accepted the
// reattach. The current apply owner must exit without writing terminal state so
// a later attempt can retry against intact storage — failing the apply here
// would abandon engine work that is still in flight on the provider.
var errGroupedResumeStateUnavailable = errors.New("grouped resume state unavailable")

// errRevertPhaseTaskInSequentialResume marks a sequential resume that found a
// task in an engine-monitored revert phase. Only the grouped drive reattaches
// to the engine that can settle such a task, so the sequential drive refuses
// before writing anything: the apply stays claimable, and every re-claim
// fails the same way until an operator examines it.
var errRevertPhaseTaskInSequentialResume = errors.New("revert-phase task cannot be settled by a sequential resume")

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
		// An apply dwelling in a revert phase has engine work in flight by
		// definition — a revert window held open, a revert or skip underway —
		// so a missing resume-state row can never legitimately mean "start
		// fresh". A fresh engine apply would re-deploy the reviewed schema
		// change on top of the revert. Fail the attempt so recovery retries
		// against intact storage.
		if c.config.Type == storage.DatabaseTypeVitess && applyInRevertPhase(apply) {
			return nil, fmt.Errorf("%w: no persisted engine resume state for revert-phase apply %s operation %d (database %s, state %s)",
				errGroupedResumeStateUnavailable, apply.ApplyIdentifier, operationID, apply.Database, apply.State)
		}
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
			if !plan.Namespaces[ns].ChangesVSchema() {
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
	// takeObserver (remove-then-notify, atomically) rather than get/notify/clear:
	// the drive's terminal path can race deliverTerminalIfSettled's re-check, and
	// OnTerminal must fire exactly once.
	if obs := c.takeObserver(apply.ID); obs != nil {
		obs.OnTerminal(apply, tasks)
	}
}

// guardDriveScope fails closed when a claimed apply does not belong to the
// database this client is bound to. Operators claim from whatever Tern storage
// they poll and resolve the drive client afterwards, and that resolution can
// fall back to a database-bound default client (RegisterGRPC wires the gRPC
// transport's client as the service-wide fallback) — so in a storage database
// shared by more than one tern, a claim of another tern's apply would
// otherwise run that apply's DDL against this client's target. Every
// legitimate drive reaches a client whose database matches the apply's
// deployment (Apply stamps Deployment from the creating client's database;
// routing resolves operation drives by their deployment) or the apply's
// database (the target router builds per-target clients keyed by it), so a
// claim matching neither is foreign work. The refused apply stays claimable
// by an operator whose client matches once its lease goes stale.
func (c *LocalClient) guardDriveScope(apply *storage.Apply) error {
	if apply == nil {
		return fmt.Errorf("stored apply is required")
	}
	if apply.Deployment == c.config.Database || apply.Database == c.config.Database {
		return nil
	}
	return fmt.Errorf("apply %s (database %q, deployment %q) is outside this client's database scope (%q); refusing to drive it against the wrong target",
		apply.ApplyIdentifier, apply.Database, apply.Deployment, c.config.Database)
}

// ResumeApply starts or resumes an apply claimed by an operator driver.
// Pending applies are dispatched for the first time; stale applies use the
// engine's resume metadata to continue after a missed heartbeat.
func (c *LocalClient) ResumeApply(ctx context.Context, apply *storage.Apply) error {
	if err := c.guardDriveScope(apply); err != nil {
		return err
	}
	tasks, err := c.storage.Tasks().GetByApplyID(ctx, apply.ID)
	if err != nil {
		return fmt.Errorf("get tasks for apply %s: %w", apply.ApplyIdentifier, err)
	}
	// Whole-apply scope has no single operation to order, so the stored apply
	// options govern the drive directly (no automatic barrier park). A task-less
	// apply is handled inside the shared resume path: VSchema-only plans are
	// re-driven so the VSchema is applied, and any other task-less shape (e.g. a
	// sharded dispatch whose shard already matches) completes as a no-op.
	return c.resumeApplyWithTasks(ctx, apply, tasks, apply.GetOptions().Map(), false, false)
}

// ResumeApplyOperation starts or resumes a single apply_operation (one
// deployment of a multi-deployment apply), driving only that operation's tasks.
// The drive logic is identical to ResumeApply; the only difference is that tasks
// are loaded scoped to the operation rather than the whole apply, so a driver
// can advance one deployment independently of its siblings.
func (c *LocalClient) ResumeApplyOperation(ctx context.Context, apply *storage.Apply, applyOperationID int64) error {
	if err := c.guardDriveScope(apply); err != nil {
		return err
	}
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
		// A missing plan row is its own cause, separate from a claim that resolved
		// to the wrong operation, so name it rather than reporting a stale claim.
		if plan == nil {
			return fmt.Errorf("plan %d for task-less apply_operation %d (apply %s): %w", apply.PlanID, applyOperationID, apply.ApplyIdentifier, ErrPlanMissingForApplyOperation)
		}
		if !op.IsTasklessVSchemaOnlyWork(plan) {
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
	if err := c.guardDriveScope(apply); err != nil {
		return err
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

// finalizerOperationKeySuffix is the trailing segment of a namespace-scoped
// group_finalizer operation key (namespace + "/" + segment), assigned at apply
// creation. The drive parses the namespace back out to reconstruct the VSchema
// change.
const finalizerOperationKeySuffix = "/group_finalizer"

// finalizerDeploymentScopedKey is the operation key of a deployment-scoped
// group_finalizer — the single operation a VSchema-only apply is shaped as. It
// carries no namespace: the drive applies every VSchema-changed namespace in
// the plan in one engine apply, because the engine treats the deployment (one
// branch, one deploy) as the unit of change.
const finalizerDeploymentScopedKey = "group_finalizer"

// namespaceFromFinalizerKey recovers the namespace a group_finalizer operation
// targets from its operation key. Returns empty for any key without a
// namespace prefix — callers must distinguish the deployment-scoped key
// (finalizerDeploymentScopedKey, where empty means "all VSchema namespaces")
// from a malformed key, which is a fail-closed condition.
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
		return fmt.Errorf("plan %d for group_finalizer apply_operation %d (apply %s): %w", apply.PlanID, op.ID, apply.ApplyIdentifier, ErrPlanMissingForApplyOperation)
	}
	namespace := namespaceFromFinalizerKey(op.OperationKey)
	if namespace == "" && op.OperationKey != finalizerDeploymentScopedKey {
		return fmt.Errorf("group_finalizer apply_operation %d (apply %s): malformed operation key %q", op.ID, apply.ApplyIdentifier, op.OperationKey)
	}
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
		Logger:        c.logger.With(apply.IdentityLogAttrs()...),
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
		if !plan.Namespaces[namespace].ChangesVSchema() {
			return nil, fmt.Errorf("plan %d has no VSchema artifact for namespace %q", plan.ID, namespace)
		}
		return []engine.SchemaChange{vschemaChange(namespace)}, nil
	}
	namespaces := plan.VSchemaNamespaces()
	if len(namespaces) == 0 {
		return nil, fmt.Errorf("plan %d has no VSchema artifact for a deployment-scoped finalizer", plan.ID)
	}
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
	// Bind the apply's identity once so every line of this resume is
	// filterable by apply_id/repo/pr without hand-listing the attrs per call.
	// Mutable attrs (state, deployment) stay per-call so the bound logger
	// never freezes stale values.
	logger := c.logger.With(apply.IdentityLogAttrs()...)
	// Before consuming a pending stop/cancel, learn whether the engine's
	// backend already drove the change to a terminal outcome. If it did, the
	// command can no longer act — the drive adopts the engine's truth and the
	// pending commands are mooted; otherwise (and on any uncertainty) the
	// commands are consumed exactly as before.
	if handled, err := c.reconcileEngineTerminalTruthBeforeCommands(ctx, apply, tasks); handled || err != nil {
		return err
	}
	if handled, err := c.processPendingCancelOrStopControlRequest(ctx, apply); handled || err != nil {
		return err
	}
	// A stopped apply is claimed only to deliver a pending control request. A
	// pending start means the operator asked to resume, and the drive below
	// consumes it (completing it once the engine accepts). No pending start
	// means the request that admitted the claim was a cancel that was declined
	// or could not finish this claim — exit without resuming, since a stopped
	// copy must never resume without an operator start. The durable request is
	// the authority here, not the in-memory state: a start claim transitions
	// the stored row to resuming but the drive still holds the claim-time
	// snapshot.
	if state.IsState(apply.State, state.Apply.Stopped) {
		startReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationStart)
		if err != nil {
			return fmt.Errorf("check pending start before resuming stopped apply %s: %w", apply.ApplyIdentifier, err)
		}
		if startReq == nil {
			logger.Info("stopped apply has no pending start request; drive exits without resuming",
				apply.MutableLogAttrs()...)
			return nil
		}
	}

	// Get the plan to retrieve original DDLs. A storage read failure says
	// nothing about whether the plan row exists, so it must not become terminal
	// apply state — the engine-side work (a checkpointed copy or a live deploy
	// request) is untouched. The recovery attempt exits with an error so the
	// claim is released and a later attempt retries against intact storage.
	plan, err := c.storage.Plans().GetByID(ctx, apply.PlanID)
	if err != nil {
		logger.Warn("failed to load plan during recovery; current apply owner will exit for operator retry",
			append(apply.MutableLogAttrs(), "error", err)...)
		return fmt.Errorf("load plan for recovery of apply %s (database %s): %w", apply.ApplyIdentifier, apply.Database, err)
	}
	// A confirmed-missing plan row is unrecoverable: the reviewed DDL cannot be
	// rebuilt, so the apply fails and its observer is notified.
	if plan == nil {
		logger.Warn("plan row does not exist for apply; recovery cannot rebuild the reviewed DDL, marking apply failed",
			apply.MutableLogAttrs()...)
		c.failApplyWithTasks(ctx, apply, tasks, "plan not found during recovery")
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
		// Completing with zero loaded tasks is only legitimate when the apply
		// truly owns no task rows. An apply that owns rows this drive did not
		// load is undriveable, not done: completing it would report success for
		// schema changes that never ran, and the unloaded rows would keep
		// blocking their database as active work. Count the raw rows — the same
		// "owns any task work" predicate the operator's claim gate uses — and
		// refuse completion on any mismatch. A failed count also refuses
		// completion, but surfaces as a storage error rather than
		// ErrApplyTasksNotLoaded so triage can tell a storage failure apart
		// from an ownership mismatch.
		totalTaskRows, err := c.storage.Tasks().CountByApplyID(ctx, apply.ID)
		if err != nil {
			return fmt.Errorf("count task rows for apply %s before task-less completion: %w", apply.ApplyIdentifier, err)
		}
		if totalTaskRows > 0 {
			logger.Error("refusing to complete apply as a task-less no-op: it owns task rows this drive did not load; the apply stays claimable and will not finish until its tasks load",
				append(apply.MutableLogAttrs(), "task_row_count", totalTaskRows)...)
			return fmt.Errorf("apply %s owns %d task rows: %w", apply.ApplyIdentifier, totalTaskRows, ErrApplyTasksNotLoaded)
		}
		logger.Info("no tasks found for apply during recovery; completing as a no-op")
		previousState := apply.State
		apply.State = state.Apply.Completed
		apply.CompletedAt = &now
		apply.UpdatedAt = now
		if err := c.storage.Applies().Update(ctx, apply); err != nil {
			// Don't report a completion the operator can't see: surface the error so
			// recovery retries, and notify the observer only after a durable write.
			return fmt.Errorf("complete task-less apply %s during recovery: %w", apply.ApplyIdentifier, err)
		}
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			"Apply owns no task work; completed without engine work", previousState, state.Apply.Completed)
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

	// This drive knows only that it holds the claim on an already-started apply;
	// the claim arm that selected it — a stale heartbeat, a pending control
	// request, a barrier-parked cutover — is decided by the operator's claim
	// query and is not carried here. Report what is known and let the operator's
	// claim logs name the cause, rather than asserting one this path never
	// checked.
	logger.Info("recovering apply: resuming an already-started apply from its stored state",
		"state", apply.State,
		"task_count", len(tasks),
	)

	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
		fmt.Sprintf("Recovering apply from its stored state (was in %s state)", apply.State), "", "")

	deferredCutoverSignalAbsent := false
	if shouldInspectCutoverSignalForResume(apply, forceCutoverResume) {
		signalExists, signalSupported, err := c.deferredCutoverSignalExists(ctx, apply, tasks)
		if err != nil {
			logger.Warn("deferred cutover recovery could not verify engine cutover signal; operator will retry",
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
						logger.Warn("deferred cutover recovery could not load persisted engine resume state; current apply owner will exit for operator retry",
							"error", err)
						return fmt.Errorf("recover deferred cutover apply %s from checkpoint: %w", apply.ApplyIdentifier, err)
					}
					return c.handleGroupedResumeFailure(ctx, apply, tasks, fmt.Errorf("recover deferred cutover apply %s from checkpoint: %w", apply.ApplyIdentifier, err), false)
				}
				return ctx.Err()
			}
			logger.Info("engine cutover signal is absent during deferred cutover recovery; re-plan will reconcile completed work")
			deferredCutoverSignalAbsent = true
		} else {
			logger.Info("engine does not support deferred cutover signal lookup; re-plan will reconcile deferred cutover recovery",
				"engine", c.getEngine().Name())
		}
	}

	rp, err := c.replanAndFilterTasks(ctx, apply, tasks, plan)
	if err != nil {
		logger.Error("re-plan failed during recovery", append(apply.MutableLogAttrs(), "error", err)...)
		return fmt.Errorf("re-plan failed during recovery for apply %s (database %s): %w", apply.ApplyIdentifier, apply.Database, err)
	}

	activeTasks := rp.ActiveTasks
	if deferredCutoverSignalAbsent && len(activeTasks) > 0 {
		message := "deferred cutover signal is absent but live schema does not match desired schema; manual reconciliation required"
		logger.Error("deferred cutover recovery cannot reconcile absent cutover signal",
			"active_task_count", len(activeTasks))
		c.failApplyWithTasks(ctx, apply, activeTasks, message)
		// A multi-operation drive owns only its operation; the operator's
		// projection settles the parent and posts the terminal summary.
		// failApplyWithTasks already logged the suppressed settle.
		if suppressParentApplyWrites(ctx) {
			return nil
		}
		c.notifyTerminalObserver(apply, tasks)
		return nil
	}
	startControlReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationStart)
	if err != nil {
		return err
	}
	startRequested := startControlReq != nil
	suppressParent := suppressParentApplyWrites(ctx)

	if len(activeTasks) == 0 {
		logger.Info("all tasks already completed, marking apply as completed")
		now := time.Now()
		apply.State = state.Apply.Completed
		apply.CompletedAt = &now
		apply.UpdatedAt = now
		// A multi-operation drive owns only its operation: its tasks are already
		// terminal, so the operator derives the operation row completed and
		// projects the parent — the parent write, start-request completion, and
		// terminal observer are the operator's to make.
		if suppressParent {
			logger.Info("operation drive found no remaining work; operator derives the operation row and projects the parent")
			return nil
		}
		if err := c.storage.Applies().Update(ctx, apply); err != nil {
			return fmt.Errorf("mark resumed apply %s completed after re-plan found no remaining work: %w", apply.ApplyIdentifier, err)
		}
		if startRequested {
			if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart); err != nil {
				return err
			}
		}
		c.notifyTerminalObserver(apply, tasks)
		return nil
	}

	grouped := c.usesGroupedApply(apply, options)
	// A revert-phase task is settled only by reattaching to the engine that
	// holds its revert window or is unwinding it, and only the grouped drive
	// reattaches. Revert-phase states come only from an engine whose database
	// type always drives grouped, so none should reach a sequential resume;
	// should one ever arrive, refuse before the apply is persisted running or
	// any task is handed to the engine. Past this point the sequential drive
	// persists the apply running and then compares schemas that prove nothing
	// post-cutover — the live schema matches the reviewed target by definition
	// until a revert lands — so settling the task there would report a
	// reverting change as applied. Resting the task retryable would be no
	// better: a later claim requeues it and drives it forward into that same
	// comparison. Neither the revert-phase task row nor the apply row is
	// written, so the apply stays claimable and visibly stuck — each re-claim
	// fails the resume and is counted as one — until an operator examines it.
	// The re-plan above never settles a task on the strength of a revert-phase
	// sibling: siblingTasks classes such a sibling as one that will not run
	// its statement, so it refuses instead of vouching.
	if !grouped {
		if task := firstRevertPhaseTask(activeTasks, apply.ID); task != nil {
			logger.Error("refusing sequential resume: a revert-phase task reached a drive whose schema comparison cannot settle it; the revert-phase task row and the apply row are left as found for an operator to examine",
				"task_id", task.TaskIdentifier, "table", task.TableName, "state", task.State)
			return fmt.Errorf("apply %s task %s is in %s: %w", apply.ApplyIdentifier, task.TaskIdentifier, task.State, errRevertPhaseTaskInSequentialResume)
		}
	}

	c.prepareRetryableTasksForResume(ctx, apply, activeTasks)
	c.prepareStoppedTasksForResume(ctx, apply, activeTasks, startRequested)

	if grouped {
		resumeCtx, cancelResume := context.WithCancel(ctx)
		cancelGeneration := c.setApplyCancel(cancelResume)
		defer c.clearApplyCancel(cancelGeneration)
		defer cancelResume()
		if err := c.launchAtomicResume(resumeCtx, apply, activeTasks, plan, options, fmt.Sprintf("Apply resumed from checkpoint (%s)", groupedApplyModeDescription(apply, options)), true, startRequested, releaseAtCutoverBarrier); err != nil {
			if errors.Is(err, errGroupedResumeStateUnavailable) {
				logger.Warn("grouped resume could not load persisted engine resume state; current apply owner will exit for operator retry",
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
		// A multi-operation drive does not write the parent running state or
		// complete parent start requests; the operator projected the parent
		// running before the drive. Task state is persisted per task below.
		if suppressParent {
			logger.Info("sequential resume under operation lease; parent running state is the operator's projection")
		} else {
			if err := c.storage.Applies().Update(ctx, apply); err != nil {
				logger.Error("failed to update apply state", append(apply.MutableLogAttrs(), "error", err)...)
				return fmt.Errorf("mark sequential resume apply %s running: %w", apply.ApplyIdentifier, err)
			}
			if startRequested {
				if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart); err != nil {
					return err
				}
			}

			c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
				"Apply resumed from checkpoint (sequential)", "", state.Apply.Running)
		}

		resumeCtx, cancelResume := context.WithCancel(ctx)
		cancelGeneration := c.setApplyCancel(cancelResume)
		defer c.clearApplyCancel(cancelGeneration)
		defer cancelResume()
		c.resumeApplySequential(resumeCtx, apply, activeTasks, plan, options)
	}

	return ctx.Err()
}

func (c *LocalClient) handleGroupedResumeFailure(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, err error, startRequested bool) error {
	logger := c.logger.With(apply.IdentityLogAttrs()...)
	// A cancelled drive is why the resume returned, so the error describes the
	// driver rather than the schema change it was reattaching to.
	if c.driveCancelled(ctx, apply, "while resuming the apply") {
		return nil
	}
	if c.shouldRetryEngineError(err) {
		logger.Warn("engine apply failed during recovery, pausing apply for operator retry",
			"error", err)
		c.markApplyRetryableWithTasks(ctx, apply, tasks, err.Error())
		return nil
	}

	logger.Error("engine apply failed during recovery",
		"error", err)
	c.failApplyWithTasks(ctx, apply, tasks, err.Error())
	// A multi-operation drive owns only its operation: its failed tasks carry
	// the outcome, and the operator's projection settles the parent, resolves
	// pending control requests, and posts the terminal summary. The drive
	// itself returns nil — the failure is already durably settled in the
	// tasks, and an error here would read as a transient drive failure that
	// leaves the operation claimable, re-leasing already-settled work instead
	// of letting the claim loop persist the operation row from its now-failed
	// tasks immediately. failApplyWithTasks already logged the suppressed settle.
	if suppressParentApplyWrites(ctx) {
		return nil
	}
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
