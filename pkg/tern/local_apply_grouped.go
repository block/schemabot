package tern

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// executeGroupedApply runs all DDLs in one engine operation. For Spirit with
// defer_cutover, this is atomic cutover; for Vitess, this is one deploy request.
func (c *LocalClient) executeGroupedApply(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, plan *storage.Plan, options map[string]string, releaseAtCutoverBarrier bool) {
	// Bind stable apply identity for every grouped-drive emission; mutable attrs remain per-call snapshots.
	logger := c.logger.With(apply.IdentityLogAttrs()...)
	ctx, cancelApply := context.WithCancel(ctx)
	defer cancelApply()
	defer c.startApplyHeartbeat(ctx, apply, cancelApply)()
	mode := groupedApplyMode(apply, options)
	modeDescription := groupedApplyModeDescription(apply, options)

	// Extract all DDLs and table names from tasks
	ddl := make([]string, len(tasks))
	tableNames := make([]string, len(tasks))
	for i, t := range tasks {
		ddl[i] = t.DDL
		tableNames[i] = t.TableName
	}

	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
		fmt.Sprintf("Starting %s with %d tables: %v", modeDescription, len(tasks), tableNames), "", "")

	eng := c.getEngine()
	defer c.setupSpiritLogging(ctx, apply, tasks)()

	// Call engine to apply all DDLs together
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
		"Calling engine.Apply for all tables", "", "")

	// Build per-namespace changes from the scoped tasks. Whole-apply drives pass
	// every task, while operation-scoped drives pass only one operation's tasks.
	logger.Info("building changes from scoped tasks", "task_count", len(tasks), "plan_id", plan.PlanIdentifier)
	if len(plan.Namespaces) == 0 {
		c.failApplyWithTasks(ctx, apply, tasks, "plan has no namespace data")
		return
	}
	if c.config.Type == storage.DatabaseTypeMySQL && len(plan.Namespaces) > 1 {
		var names []string
		for ns := range plan.Namespaces {
			names = append(names, ns)
		}
		c.failApplyWithTasks(ctx, apply, tasks,
			fmt.Sprintf("MySQL applies support one namespace per apply, but plan has %d: %v", len(plan.Namespaces), names))
		return
	}
	creds, err := c.credentialsForGroupedApply(plan)
	if err != nil {
		c.failApplyWithTasks(ctx, apply, tasks, err.Error())
		return
	}
	changes := groupedResumeChanges(tasks, plan)

	// Mark the apply as started before calling the engine. The engine may run
	// for a long time (branch creation, DDL application, deploy request) and
	// started_at should reflect when work actually began, not when it finished.
	now := time.Now()
	apply.State = state.Apply.Running
	apply.StartedAt = &now
	apply.UpdatedAt = now
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		logger.Error("failed to set started_at", append(apply.MutableLogAttrs(), "error", err)...)
	}
	if handled, err := c.processPendingCancelOrStopControlRequest(ctx, apply); err != nil {
		logger.Warn("pending stop request processing failed before grouped engine apply; current apply owner will exit for operator retry",
			append(apply.MutableLogAttrs(), "error", err)...)
		return
	} else if handled {
		return
	}

	// Grouped mode: all DDLs in one engine call. Use the apply identifier so all
	// table work shares one context for progress tracking.
	result, err := eng.Apply(ctx, &engine.ApplyRequest{
		Database:     apply.Database,
		PlanID:       plan.PlanIdentifier,
		Changes:      changes,
		TargetShards: taskTargetShards(tasks),
		SchemaFiles:  plan.SchemaFiles,
		Options:      options,
		ResumeState:  &engine.ResumeState{MigrationContext: apply.ApplyIdentifier},
		Credentials:  creds,
		Logger:       logger,
		OnEvent: func(event engine.ApplyEvent) {
			oldState := apply.State
			newState := deriveApplyPhase(event)
			c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
				event.Message, oldState, newState)
			applyEventStateTransition(apply, event, func(a *storage.Apply) error {
				return c.storage.Applies().Update(ctx, a)
			}, logger)
		},
		OnStateChange: func(rs *engine.ResumeState) {
			if rs == nil {
				logger.Debug("OnStateChange: nil resume state")
				return
			}
			if saveErr := c.saveEngineResumeState(ctx, apply, tasks, rs); saveErr != nil {
				logger.Warn("OnStateChange: failed to persist opaque resume state", append(apply.MutableLogAttrs(), "error", saveErr)...)
			}
		},
	})

	if err != nil {
		// A cancelled drive is why the engine call returned, so the error
		// describes the driver and not the change the engine already accepted.
		if c.driveCancelled(ctx, apply, "while the engine was applying") {
			return
		}
		newState := state.Apply.Failed
		if c.shouldRetryEngineError(err) {
			c.markApplyRetryableWithTasks(ctx, apply, tasks, err.Error())
			newState = state.Apply.FailedRetryable
		} else {
			c.failApplyWithTasks(ctx, apply, tasks, err.Error())
		}
		if newState == state.Apply.FailedRetryable {
			logger.Warn("apply paused for operator retry", append(apply.MutableLogAttrs(), "mode", mode, "error", err)...)
		} else {
			logger.Error("apply failed", append(apply.MutableLogAttrs(), "mode", mode, "error", err)...)
		}
		return
	}

	if !result.Accepted {
		c.failApplyWithTasks(ctx, apply, tasks, result.Message)
		return
	}

	if isTasklessVSchemaOnlyPlan(tasks, plan) {
		if completeErr := c.completeTasklessGroupedApply(ctx, apply, result.Message); completeErr != nil {
			logger.Error("failed to complete task-less grouped apply", append(apply.MutableLogAttrs(), "error", completeErr)...)
		}
		return
	}

	// Persist the engine resume state and set IsInstant on tasks before marking
	// running. The progress handler reads task.is_instant and the engine resume
	// state to render the instant label and deploy display fields, so both must
	// be committed before the first poll.
	var resumeState *engine.ResumeState
	if result.ResumeState != nil {
		resumeState = result.ResumeState
		if c.config.Type == storage.DatabaseTypeVitess {
			// The engine has already accepted the apply, so a deploy request is
			// live on the provider. A failure to persist its resume state is
			// storage uncertainty, not a failed schema change: pause for operator
			// retry so the resume path reattaches to the in-flight work instead
			// of abandoning it as terminal.
			if saveErr := c.saveEngineResumeState(ctx, apply, tasks, resumeState); saveErr != nil {
				if c.driveCancelled(ctx, apply, "while saving the engine resume state") {
					return
				}
				logger.Warn("failed to save engine resume state after accepted apply; pausing apply for operator retry",
					append(apply.MutableLogAttrs(), "error", saveErr)...)
				c.markApplyRetryableWithTasks(ctx, apply, tasks, fmt.Sprintf("failed to save engine resume state: %v", saveErr))
				return
			}
		}
	}
	if c.config.Type == storage.DatabaseTypeVitess && resumeState == nil {
		c.failApplyWithTasks(ctx, apply, tasks, "engine accepted Vitess apply without resume state")
		return
	}

	if result.ResumeState != nil {
		if meta, err := decodePSMetadataForStorage(result.ResumeState.Metadata); meta != nil && err == nil && meta.IsInstant {
			for _, task := range tasks {
				task.IsInstant = true
			}
		}
	}
	c.markTasksRunning(ctx, tasks)
	if c.config.Type == storage.DatabaseTypeVitess {
		apply.State = state.Apply.ValidatingDeployRequest
	} else {
		apply.State = state.Apply.Running
	}
	apply.UpdatedAt = time.Now()
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		logger.Error("failed to update apply state", append(apply.MutableLogAttrs(), "error", err)...)
	}
	logger.Info("apply started", "mode", mode, "task_count", len(tasks))
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
		fmt.Sprintf("All %d tables started copying in parallel", len(tasks)), state.Apply.Pending, apply.State)

	// Poll for completion - all tasks share the same state
	c.pollForCompletionAtomic(ctx, apply, tasks, creds, resumeState, options, releaseAtCutoverBarrier)
}

func (c *LocalClient) saveEngineResumeState(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, resumeState *engine.ResumeState) error {
	operationID, err := c.applyOperationIDForApplyTasks(ctx, apply, tasks)
	if err != nil {
		return err
	}
	return c.saveEngineResumeStateForOperation(ctx, operationID, resumeState)
}

func (c *LocalClient) applyOperationIDForApplyTasks(ctx context.Context, apply *storage.Apply, tasks []*storage.Task) (int64, error) {
	if len(tasks) > 0 {
		return applyOperationIDForTasks(tasks)
	}
	if apply == nil {
		return 0, fmt.Errorf("engine resume state has no tasks and no apply")
	}
	store := c.storage.ApplyOperations()
	if store == nil {
		return 0, fmt.Errorf("engine resume state has no tasks and no apply operation store")
	}
	ops, err := store.ListByApply(ctx, apply.ID)
	if err != nil {
		return 0, fmt.Errorf("list apply operations for task-less apply %s: %w", apply.ApplyIdentifier, err)
	}
	if len(ops) != 1 {
		return 0, fmt.Errorf("engine resume state has no tasks and apply %s has %d operations", apply.ApplyIdentifier, len(ops))
	}
	return ops[0].ID, nil
}

func isTasklessVSchemaOnlyPlan(tasks []*storage.Task, plan *storage.Plan) bool {
	return len(tasks) == 0 && plan.IsVSchemaOnly()
}

func (c *LocalClient) completeTasklessGroupedApply(ctx context.Context, apply *storage.Apply, message string) error {
	if suppressParentApplyWrites(ctx) {
		operationID, err := c.applyOperationIDForApplyTasks(ctx, apply, nil)
		if err != nil {
			return fmt.Errorf("resolve task-less apply operation for apply %s: %w", apply.ApplyIdentifier, err)
		}
		if err := c.storage.ApplyOperations().MarkCompleted(ctx, operationID); err != nil {
			return fmt.Errorf("mark task-less apply_operation %d completed (apply %s): %w", operationID, apply.ApplyIdentifier, err)
		}
		return nil
	}
	now := time.Now()
	apply.State = state.Apply.Completed
	apply.CompletedAt = &now
	apply.UpdatedAt = now
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		return fmt.Errorf("complete task-less grouped apply %s: %w", apply.ApplyIdentifier, err)
	}
	if message == "" {
		message = "Apply completed with state: completed"
	}
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
		message, state.Apply.Running, state.Apply.Completed)
	metrics.AdjustActiveApplies(ctx, -1, apply.Database, apply.Deployment, apply.Environment)
	c.notifyTerminalObserver(apply, nil)
	return nil
}

func (c *LocalClient) markTasklessOperationState(ctx context.Context, apply *storage.Apply, opState, errorMessage string) error {
	operationID, err := c.applyOperationIDForApplyTasks(ctx, apply, nil)
	if err != nil {
		return fmt.Errorf("resolve task-less apply operation for apply %s: %w", apply.ApplyIdentifier, err)
	}
	switch {
	case state.IsState(opState, state.Apply.Completed):
		return c.storage.ApplyOperations().MarkCompleted(ctx, operationID)
	case state.IsState(opState, state.Apply.Failed):
		return c.storage.ApplyOperations().MarkFailed(ctx, operationID, errorMessage)
	case state.IsState(opState, state.Apply.FailedRetryable, state.Apply.WaitingForCutover):
		return c.storage.ApplyOperations().UpdateState(ctx, operationID, opState)
	case state.IsTerminalApplyState(opState):
		return c.storage.ApplyOperations().MarkTerminal(ctx, operationID, opState)
	default:
		return nil
	}
}

func (c *LocalClient) saveEngineResumeStateForOperation(ctx context.Context, operationID int64, resumeState *engine.ResumeState) error {
	metadata := resumeState.Metadata
	if metadata == "" {
		metadata = "{}"
	}
	store := c.storage.ApplyOperations()
	if store == nil {
		return fmt.Errorf("apply operation store is not configured")
	}
	return store.SaveEngineResumeState(ctx, operationID, &storage.EngineResumeState{
		ApplyOperationID: operationID,
		MigrationContext: resumeState.MigrationContext,
		Metadata:         metadata,
	})
}

func (c *LocalClient) loadEngineResumeStateForOperation(ctx context.Context, operationID int64) (*engine.ResumeState, error) {
	store := c.storage.ApplyOperations()
	if store == nil {
		return nil, fmt.Errorf("apply operation store is not configured")
	}
	stored, err := store.GetEngineResumeState(ctx, operationID)
	if err != nil {
		return nil, err
	}
	return &engine.ResumeState{
		MigrationContext: stored.MigrationContext,
		Metadata:         stored.Metadata,
	}, nil
}

func applyOperationIDForTasks(tasks []*storage.Task) (int64, error) {
	var operationID int64
	for _, task := range tasks {
		if task == nil {
			return 0, fmt.Errorf("engine resume state task is nil")
		}
		id, err := applyOperationIDForTask(task)
		if err != nil {
			return 0, err
		}
		if operationID == 0 {
			operationID = id
			continue
		}
		if operationID != id {
			return 0, fmt.Errorf("engine resume state spans multiple apply operations: %d and %d", operationID, id)
		}
	}
	if operationID == 0 {
		return 0, fmt.Errorf("engine resume state has no apply operation")
	}
	return operationID, nil
}

func applyOperationIDForTask(task *storage.Task) (int64, error) {
	if task == nil {
		return 0, fmt.Errorf("engine resume state task is nil")
	}
	if task.ApplyOperationID == nil || *task.ApplyOperationID == 0 {
		return 0, fmt.Errorf("task %s has no apply_operation_id for engine resume state", task.TaskIdentifier)
	}
	return *task.ApplyOperationID, nil
}

// tasksForOperation returns the subset of tasks belonging to the given
// apply_operation. It is nil-safe: a nil task or one without an
// apply_operation_id is skipped. Callers use it to scope an apply-wide task set
// (which spans multiple operations once an apply fans out) down to a single
// operation before deriving that operation's state.
func tasksForOperation(tasks []*storage.Task, operationID int64) []*storage.Task {
	var scoped []*storage.Task
	for _, task := range tasks {
		if task == nil || task.ApplyOperationID == nil {
			continue
		}
		if *task.ApplyOperationID == operationID {
			scoped = append(scoped, task)
		}
	}
	return scoped
}

// classifyOperationTasks reports how a task set maps to the apply-operation
// model so deriveAggregateApplyState can distinguish three cases that must be
// handled differently:
//
//   - No operation model (usesModel=false, err=nil): every task carries no
//     apply_operation_id, or the set is empty. There are no siblings, so the
//     per-task derivation is authoritative and may terminalize. This preserves
//     single-writer/legacy behaviour for applies written before the apply-create
//     path populated apply_operation_id.
//   - Single operation (usesModel=true, err=nil): every task carries the same
//     apply_operation_id. The sibling-row projection applies.
//   - Ambiguous (err!=nil): the tasks span multiple operation ids, mix
//     operation-model and legacy rows, or include a nil task. The set cannot be
//     attributed to one operation, so a terminal aggregate derived from it would
//     be unsafe; the caller must fail closed.
//
// It is intentionally stricter than applyOperationIDForTasks's "no operation"
// fallback: a mixed set is an error here, not a legacy no-op-model case.
func classifyOperationTasks(tasks []*storage.Task) (operationID int64, usesModel bool, err error) {
	var sawNil, sawID bool
	for _, task := range tasks {
		if task == nil {
			return 0, false, fmt.Errorf("apply operation task is nil")
		}
		if task.ApplyOperationID == nil || *task.ApplyOperationID == 0 {
			sawNil = true
			continue
		}
		id := *task.ApplyOperationID
		if sawID && operationID != id {
			return 0, false, fmt.Errorf("tasks span multiple apply operations: %d and %d", operationID, id)
		}
		operationID = id
		sawID = true
	}
	switch {
	case sawID && sawNil:
		return 0, false, fmt.Errorf("tasks mix operation-model and legacy rows")
	case sawID:
		return operationID, true, nil
	default:
		return 0, false, nil
	}
}

// deriveAggregateApplyState computes applies.state as the rollout projection
// over every apply_operation row of the apply, accounting for each operation's
// on_failure policy via state.DeriveRolloutApplyState. The boolean is false when
// the projection could not be determined safely and the caller must leave the
// stored apply state unchanged.
//
// Under on_failure "continue" a terminal-failed sibling does not terminalize the
// apply while other siblings are still in flight: the apply is held running until
// the rollout settles, then takes the failed verdict. Every other policy fails
// closed on the verdict, and still holds the apply while a sibling that a driver
// already started still holds its target, since refusing new claims does not
// stop it.
//
// Invariant: applies.state is the rollout projection over all operations of the
// apply, not only the operation this drive is executing. The current
// deployment's freshly derived per-operation state is folded in over its own
// (possibly stale) operation row, then the aggregate is derived from the whole
// sibling set. Deriving from the current deployment's tasks alone would let one
// deployment move the apply to a terminal/aggregate state that ignores siblings;
// folding the current state into the sibling set keeps a still-pending or
// running sibling holding the apply non-terminal.
//
// With one operation per apply the sibling set is the current operation alone,
// so the projection collapses to the current deployment's derived state.
//
// Three outcomes are distinguished when the full sibling set is not available:
//
//   - The apply does not use the operation model — its tasks carry no
//     apply_operation_id, or the operation store is not configured. There are no
//     siblings, so the per-task derivation is authoritative and may terminalize.
//     This preserves single-writer/legacy behaviour for applies written before
//     the apply-create path populated apply_operation_id.
//
//   - The task set is not scoped to one operation — it spans multiple
//     apply_operation_ids or mixes operation-model and legacy rows. The set
//     cannot be attributed to a single operation, so its derived state is not a
//     meaningful per-operation state and must not terminalize the apply. The
//     projection fails closed (ok=false) so the caller keeps the stored value.
//
//   - The apply uses the operation model (its tasks carry an apply_operation_id)
//     but the sibling rows cannot be read consistently — the list call failed,
//     returned no rows, or omitted the current operation. Here the sibling
//     states are genuinely unknown, so a terminal current-deployment derivation
//     must not become the aggregate: a transient read failure on the
//     last-finishing deployment would otherwise mark the whole apply terminal
//     while siblings are still in flight. The projection is reported as
//     undetermined (ok=false) and the caller keeps the stored value for the next
//     poll to reconcile. A non-terminal derivation is still a safe fallback.
//
// The read-then-write is not atomic, so concurrent sibling drives last-write-
// wins from possibly stale reads; the aggregate converges on the next poll.
func (c *LocalClient) deriveAggregateApplyState(ctx context.Context, apply *storage.Apply, tasks []*storage.Task) (string, bool) {
	// Keep aggregate-state decisions filterable by the apply being projected.
	logger := c.logger.With(apply.IdentityLogAttrs()...)
	currentOpState := state.DeriveApplyState(taskStates(tasks))

	// failClosed reports the current deployment's derived state when the sibling
	// set is in use but unreadable, refusing to terminalize the apply on
	// incomplete information.
	failClosed := func() (string, bool) {
		if state.IsTerminalApplyState(currentOpState) {
			logger.Warn("cannot determine aggregate apply state and current deployment is terminal; leaving stored apply state unchanged",
				"current_deployment_state", currentOpState)
			return "", false
		}
		return currentOpState, true
	}

	operationID, usesModel, err := classifyOperationTasks(tasks)
	if err != nil {
		// The task set cannot be attributed to a single apply operation: it spans
		// multiple operation ids or mixes operation-model and legacy rows. The
		// sibling states are unknowable from such a mix, so fail closed rather
		// than task-deriving a possibly terminal aggregate from an ambiguous set.
		logger.Warn("cannot determine aggregate apply state: task set is not scoped to one apply operation",
			"error", err)
		return failClosed()
	}
	if !usesModel {
		// No operation model in use: tasks carry no apply_operation_id, so there
		// are no siblings and the per-task derivation is authoritative.
		logger.Debug("deriving apply state from tasks: apply has no operation model")
		return currentOpState, true
	}

	store := c.storage.ApplyOperations()
	if store == nil {
		// Operation store unavailable: no siblings can exist, so the per-task
		// derivation is authoritative.
		logger.Debug("deriving apply state from tasks: apply operation store is not configured")
		return currentOpState, true
	}

	ops, err := store.ListByApply(ctx, apply.ID)
	if err != nil {
		logger.Warn("cannot determine aggregate apply state: failed to list sibling apply operations",
			"apply_operation_id", operationID, "error", err)
		return failClosed()
	}
	if len(ops) == 0 {
		logger.Warn("cannot determine aggregate apply state: tasks reference an apply operation but no operation rows were found",
			"apply_operation_id", operationID)
		return failClosed()
	}

	// Load the release latch only when some operation uses on_failure=pause: it
	// is the only policy whose projection depends on whether an operator has
	// released the rollout, so an apply without a pause operation never pays the
	// read or fails closed on an unrelated latch read error. A released pause
	// behaves like continue; a failed release does not latch (fail-closed), per
	// ApplyControlRequest.ReleasesPausedRollout.
	released := false
	if slices.ContainsFunc(ops, func(op *storage.ApplyOperation) bool { return op.OnFailure == storage.OnFailurePause }) {
		if requests := c.storage.ControlRequests(); requests != nil {
			releaseReq, err := requests.GetByOperation(ctx, apply.ID, storage.ControlOperationRelease)
			if err != nil {
				logger.Warn("cannot determine aggregate apply state: failed to load release latch",
					"apply_operation_id", operationID, "error", err)
				return failClosed()
			}
			released = releaseReq.ReleasesPausedRollout()
		} else {
			// No control-request store: a release latch cannot exist, so an
			// unreleased pause stays held (fail-closed).
			logger.Debug("deriving apply state from tasks: control request store is not configured; treating rollout as unreleased")
		}
	}

	children := make([]state.RolloutChild, len(ops))
	foundCurrent := false
	for i, op := range ops {
		isContinue := op.OnFailure == storage.OnFailureContinue
		isPause := op.OnFailure == storage.OnFailurePause
		child := state.RolloutChild{
			State:             op.State,
			ContinueOnFailure: isContinue || (isPause && released),
			PauseOnFailure:    isPause && !released,
		}
		if op.ID == operationID {
			child.State = currentOpState
			foundCurrent = true
		}
		children[i] = child
	}
	if !foundCurrent {
		logger.Warn("cannot determine aggregate apply state: current operation row missing from sibling set",
			"apply_operation_id", operationID)
		return failClosed()
	}
	return state.DeriveRolloutApplyState(children), true
}

// executeApplySequential runs each DDL as a separate Spirit call (independent mode).
// Each table copies and cuts over independently.

// pollForCompletionAtomic polls the engine for progress in atomic mode (all tasks share state).
func (c *LocalClient) pollForCompletionAtomic(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, creds *engine.Credentials, resumeState *engine.ResumeState, options map[string]string, releaseAtCutoverBarrier bool) {
	eng := c.getEngine()
	ticker := time.NewTicker(c.taskPollInterval())
	defer ticker.Stop()

	// Seed revertSkipped from the durable signal so a driver that picks this apply
	// up after a restart treats skip-revert as already accepted: it won't re-attempt
	// SkipRevert, and it keeps surfacing skipping_revert while finalization is in
	// flight rather than falling back to revert_window.
	ps := &atomicPollState{
		lastProgressLog: time.Now(),
		revertSkipped:   apply.RevertSkippedAt != nil,
		lostWork:        lostEngineWorkTracker{budget: c.lostEngineWorkPendingBudget(eng)},
	}

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("drive context cancelled while polling; handing the apply back for another driver to claim",
				apply.IdentityLogAttrs()...)
			return
		case <-ticker.C:
			if done := c.handleAtomicProgressTick(ctx, eng, apply, tasks, creds, resumeState, ps, options, releaseAtCutoverBarrier); done {
				return
			}
		}
	}
}

// applyQuiesceDecision reports whether a drive should run the apply-level
// terminal/pause side-effects — stamping completed_at, dropping the active-
// applies metric, completing pending stop requests, notifying observers, and
// stopping polling — based on the rollout-projected apply state rather than one
// operation's engine result. Under on_failure=continue a failed operation holds
// the apply running while siblings are still in flight, so its terminal engine
// result must not quiesce the whole apply. retryablePause is reported separately
// because failed_retryable pauses for operator retry (completed_at stays nil,
// observers receive progress not terminal) rather than terminalizing the apply.
func applyQuiesceDecision(projectedApplyState string) (quiesce, retryablePause, stampCompletedAt bool) {
	retryablePause = state.IsState(projectedApplyState, state.Apply.FailedRetryable)
	quiesce = state.IsTerminalApplyState(projectedApplyState) || retryablePause
	// completed_at is stamped only when the apply is truly finished. Resumable
	// states keep it nil so an operator can resume: failed_retryable is a retry
	// pause, and stopped is terminal but explicitly resumable.
	resumable := retryablePause || state.IsState(projectedApplyState, state.Apply.Stopped)
	stampCompletedAt = quiesce && !resumable
	return quiesce, retryablePause, stampCompletedAt
}

// handleAtomicProgressTick processes a single progress poll tick in atomic mode.
// Returns true when this operation's drive should stop polling: the aggregate
// apply quiesced (terminal or paused for retry), this owner attempt must exit
// for operator retry, or — under on_failure=continue — this operation's own
// tasks settled while a sibling holds the apply running. The apply-level
// wind-down runs only when the aggregate quiesces, not when a single operation
// finishes ahead of its siblings.
func (c *LocalClient) handleAtomicProgressTick(ctx context.Context, eng engine.Engine, apply *storage.Apply, tasks []*storage.Task, creds *engine.Credentials, resumeState *engine.ResumeState, ps *atomicPollState, options map[string]string, releaseAtCutoverBarrier bool) bool {
	// Bind identity once for all decisions and state transitions in this progress tick.
	logger := c.logger.With(apply.IdentityLogAttrs()...)
	// The poll loop's select can pick a ready ticker over an equally ready
	// ctx.Done(), so a tick can begin after the drive has been cancelled. Every
	// engine call and storage write it made would fail on that context.
	if c.driveCancelled(ctx, apply, "before a progress tick") {
		return true
	}
	if handled, err := c.processPendingCancelOrStopControlRequest(ctx, apply); err != nil {
		logger.Warn("pending stop request processing failed; current apply owner will exit for operator retry",
			"error", err)
		return true
	} else if handled {
		return true
	}

	result, err := eng.Progress(ctx, &engine.ProgressRequest{
		Database:    apply.Database,
		Credentials: creds,
		ResumeState: resumeState,
	})
	if err != nil {
		// A cancelled drive is why the engine call failed, so the error says
		// nothing about the deployment and must not be classified as one.
		if c.driveCancelled(ctx, apply, "during a progress check") {
			return true
		}
		// Permanent errors (e.g., deploy request not found) fail immediately.
		var permanent *engine.PermanentError
		if errors.As(err, &permanent) {
			logger.Error("progress check failed with permanent error",
				append(apply.MutableLogAttrs(), "error", err)...)
			c.failApplyWithTasks(ctx, apply, tasks, fmt.Sprintf("progress polling failed: %v", err))
			return true
		}
		ps.consecutiveErrors++
		logger.Warn("progress check failed",
			append(apply.MutableLogAttrs(), "error", err, "consecutive_errors", ps.consecutiveErrors)...)
		if ps.consecutiveErrors >= maxConsecutiveProgressPollErrors {
			if c.shouldRetryEngineError(err) {
				logger.Warn("progress polling failed repeatedly, pausing apply for operator retry",
					"consecutive_errors", ps.consecutiveErrors)
				c.markApplyRetryableWithTasks(ctx, apply, tasks, fmt.Sprintf("progress polling failed after %d consecutive errors: %v", ps.consecutiveErrors, err))
				return true
			}
			logger.Error("progress polling failed repeatedly, failing apply",
				"consecutive_errors", ps.consecutiveErrors)
			c.failApplyWithTasks(ctx, apply, tasks, fmt.Sprintf("progress polling failed after %d consecutive errors: %v", ps.consecutiveErrors, err))
			return true
		}
		return false
	}
	now := time.Now()
	newState := taskStateFromProgressResult(result)

	settled := settledTaskSet{}
	if engineReportsLostApplyWork(newState, tasks) {
		pendingFor, exhausted := ps.lostWork.observePending(now)
		if exhausted {
			var settleErr error
			if settled, settleErr = c.settleLostEngineWorkForTasks(ctx, apply, tasks, result.State); settleErr != nil {
				// Neither the engine nor the target has answered what happened
				// to the work, so count the failed verification against the
				// same bounded error budget as a failed poll — this must never
				// become an unbounded loop. Return before the reset below so
				// the healthy poll that carried the pending report cannot
				// clear the count.
				ps.consecutiveErrors++
				logger.Warn("engine reports no active schema change for an in-flight apply and target verification failed; the drive re-verifies at the next poll",
					append(apply.MutableLogAttrs(), "engine_state", result.State, "consecutive_errors", ps.consecutiveErrors, "error", settleErr)...)
				if ps.consecutiveErrors >= maxConsecutiveProgressPollErrors {
					c.markApplyRetryableWithTasks(ctx, apply, tasks, fmt.Sprintf("engine reports no active schema change for an in-flight apply and the target could not be verified; %d consecutive errors across progress polls and target verification; see server logs", ps.consecutiveErrors))
					return true
				}
				return false
			}
			// The settled tasks are terminal or resting now, and the progress
			// sync below leaves them out, so the tick falls through and the
			// apply-state derivation quiesces the apply from the settled task
			// states.
		} else {
			// Inside the budget the engine is still trusted: it may be
			// serving a stale snapshot after a restart, or reporting
			// pending for real work it has not begun executing yet.
			logger.Debug("engine reports no active schema change for an in-flight apply; still inside the trust budget",
				append(apply.MutableLogAttrs(), "engine_state", result.State, "pending_for", pendingFor.Round(time.Second))...)
		}
	} else {
		ps.lostWork.reset()
	}

	ps.consecutiveErrors = 0
	c.logEngineResumeOnce(ctx, logger, apply, result.ResumedFromCheckpoint, &ps.resumeEventLogged)

	// Update resumeState if the engine returned a newer one (e.g., with
	// updated metadata like deploy request URL or migration context).
	if result.ResumeState != nil && resumeState != nil {
		*resumeState = *result.ResumeState
		// Persist the revert-window deadline so the PR comment and CLI can show
		// time remaining. revertWindowDeadline derives it from the engine's
		// deployed_at plus the configured window; merge it in key-preserving so
		// engine fields the storage struct does not model survive the rewrite.
		if result.State == engine.StateRevertWindow {
			if deadline := c.revertWindowDeadline(logger, result.ResumeState, ps.stateEnteredAt); !deadline.IsZero() {
				if merged, err := setRevertExpiresAtMetadata(resumeState.Metadata, deadline); err != nil {
					logger.Warn("failed to stamp revert_expires_at into resume metadata; comment will omit revert deadline",
						append(apply.MutableLogAttrs(), "error", err)...)
				} else {
					resumeState.Metadata = merged
				}
			}
		}
		if c.config.Type == storage.DatabaseTypeVitess {
			if saveErr := c.saveEngineResumeState(ctx, apply, tasks, resumeState); saveErr != nil {
				if c.driveCancelled(ctx, apply, "while saving the engine resume state") {
					return true
				}
				logger.Error("failed to save Vitess engine resume state from progress polling",
					append(apply.MutableLogAttrs(), "error", saveErr)...)
				c.markApplyRetryableWithTasks(ctx, apply, tasks, fmt.Sprintf("failed to save engine resume state from progress polling: %v", saveErr))
				return true
			}
		}
	}

	// Log state transitions and track when waiting states are entered (for timeouts)
	if newState != ps.lastTaskState {
		msg := fmt.Sprintf("State changed to %s", newState)
		if result.Message != "" {
			msg = fmt.Sprintf("State changed to %s (%s)", newState, result.Message)
		}
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			msg, ps.lastTaskState, newState)
		ps.lastTaskState = newState
		ps.stateEnteredAt = now
	}

	// Surface once, at Warn, when a sharded engine reached an active state but
	// could not report per-shard/row-copy progress for a reason that persists for
	// the whole apply (a missing vtgate DSN). Only the persistent reason warns:
	// the transient reasons (schema change context still being discovered, shard
	// rows not yet registered at copy start) can self-heal on a later poll, so a
	// one-shot warning for them would be a false alarm that latches forever; they
	// stay visible in the engine's per-poll Debug. Warn (not Debug) so the
	// degraded visibility is always in Datadog without enabling debug logging.
	if result.PerShardProgressUnavailable == engine.PerShardUnavailableNoVtgateDSN && !ps.warnedPerShardUnavailable &&
		state.IsState(newState, state.Task.Running, state.Task.WaitingForCutover, state.Task.RevertWindow) {
		logger.Warn("per-shard progress unavailable: per-shard and row-copy progress will not be reported for this apply",
			append(apply.MutableLogAttrs(), "reason", result.PerShardProgressUnavailable, "task_state", newState)...)
		ps.warnedPerShardUnavailable = true
	}

	// Log progress every 10 seconds
	c.logAtomicProgress(ctx, apply, result, ps, now)

	// Update all tasks with engine progress
	c.syncAtomicTaskProgress(ctx, logger, tasks, result, newState, now, settled)
	if handled, err := c.processPendingCancelOrStopControlRequest(ctx, apply); err != nil {
		logger.Warn("pending stop request processing failed after progress sync; current apply owner will exit for operator retry",
			"error", err)
		return true
	} else if handled {
		return true
	}
	if err := c.processPendingCutoverControlRequest(ctx, apply); err != nil {
		logger.Warn("pending cutover request processing failed after progress sync; current apply owner will exit for operator retry",
			"error", err)
		return true
	}
	opts := storage.ApplyOptionsFromMap(options)
	controlReq := &engine.ControlRequest{
		Database:    apply.Database,
		Credentials: creds,
		ResumeState: resumeState,
	}

	// Auto-trigger deploy if waiting and not in defer-deploy mode
	if result.State == engine.StateWaitingForDeploy && !opts.DeferDeploy {
		logger.Info("auto-triggering deploy (not in defer-deploy mode)")
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventDeployTriggered, storage.LogSourceSchemaBot,
			"Auto-triggering deploy (defer_deploy not set)", "", "")
		if _, err := eng.Start(ctx, controlReq); err != nil {
			logger.Error("auto-deploy failed", append(apply.MutableLogAttrs(), "error", err)...)
		}
	}

	// Auto-trigger cutover if waiting and not in defer mode
	if result.State == engine.StateWaitingForCutover && !opts.DeferCutover {
		if c.autoTriggerCutover(ctx, eng, apply, tasks, controlReq, ps, now) {
			return true
		}
	}

	// Timeout: cancel the apply if waiting for manual deploy too long.
	if result.State == engine.StateWaitingForDeploy && opts.DeferDeploy &&
		!ps.stateEnteredAt.IsZero() && time.Since(ps.stateEnteredAt) > waitingForManualActionTimeout {
		logger.Info("waiting-for-deploy timed out, cancelling apply",
			"timeout", waitingForManualActionTimeout)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			fmt.Sprintf("Waiting for deploy timed out after %s, cancelling", waitingForManualActionTimeout), "", "")
		if _, err := eng.Stop(ctx, controlReq); err != nil {
			logger.Error("timeout stop failed", append(apply.MutableLogAttrs(), "error", err)...)
		}
	}

	// Timeout: cancel the apply if waiting for manual cutover too long. An
	// operation parking at the barrier under an ordered-cutover policy is exempt:
	// it releases the copy drive below for the deployment-ordered cutover claim
	// to pick up later, so it must not be cancelled for "inaction".
	if result.State == engine.StateWaitingForCutover && opts.DeferCutover && !releaseAtCutoverBarrier &&
		!ps.stateEnteredAt.IsZero() && time.Since(ps.stateEnteredAt) > waitingForManualActionTimeout {
		logger.Info("waiting-for-cutover timed out, cancelling apply",
			"timeout", waitingForManualActionTimeout)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			fmt.Sprintf("Waiting for cutover timed out after %s, cancelling", waitingForManualActionTimeout), "", "")
		if _, err := eng.Stop(ctx, controlReq); err != nil {
			logger.Error("timeout stop failed", append(apply.MutableLogAttrs(), "error", err)...)
		}
	}

	// If --skip-revert was set, auto-skip the revert window immediately.
	if result.State == engine.StateRevertWindow && opts.SkipRevert && !ps.revertSkipped {
		logger.Info("auto-skipping revert window (--skip-revert)")
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			"Auto-skipping revert window (--skip-revert)", "", "")
		_, err := eng.SkipRevert(ctx, controlReq)
		if err != nil {
			logger.Error("auto-skip revert failed", append(apply.MutableLogAttrs(), "error", err)...)
		} else {
			logger.Info("skip-revert triggered", "reason", "--skip-revert")
			c.markRevertSkipped(ctx, apply)
		}
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventSkipRevertTriggered, storage.LogSourceSchemaBot,
			"Skip-revert triggered (--skip-revert)", state.Apply.RevertWindow, state.Apply.SkippingRevert)
		ps.revertSkipped = true
	}

	// A durable skip-revert control request (the interactive "skip now" command,
	// vs the upfront --skip-revert flag above) was queued; honor it. This is the
	// apply owner's retry path: the API's immediate skip attempt may have failed
	// or its process may have died, leaving the request pending for the drive.
	if result.State == engine.StateRevertWindow && !ps.revertSkipped {
		if pending, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationSkipRevert); err != nil {
			logger.Warn("could not load pending skip-revert control request", "error", err)
		} else if pending != nil {
			logger.Info("skip-revert requested by user; closing revert window", "requested_by", controlRequestCaller(pending))
			if _, err := eng.SkipRevert(ctx, controlReq); err != nil {
				c.resolveOrRetryRevertPhaseRequest(ctx, logger, apply, storage.ControlOperationSkipRevert, storage.LogEventSkipRevertTriggered, pending, err)
			} else {
				c.markRevertSkipped(ctx, apply)
				ps.revertSkipped = true
				if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationSkipRevert); err != nil {
					logger.Warn("failed to complete skip-revert control request", "error", err)
				}
				c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventSkipRevertTriggered, storage.LogSourceSchemaBot,
					fmt.Sprintf("Skip-revert triggered by user%s", callerApplyLogSuffix(controlRequestCaller(pending))), state.Apply.RevertWindow, state.Apply.SkippingRevert)
			}
		}
	}

	// A durable revert control request (the interactive "revert" command) was
	// queued; honor it. This is the apply owner's retry path: the API's immediate
	// revert attempt may have failed or its process may have died, leaving the
	// request pending for the drive.
	revertedByControlRequest := false
	if result.State == engine.StateRevertWindow && !ps.revertSkipped {
		if pending, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationRevert); err != nil {
			logger.Warn("could not load pending revert control request", "error", err)
		} else if pending != nil {
			logger.Info("revert requested by user; reverting schema change", "requested_by", controlRequestCaller(pending))
			if _, err := eng.Revert(ctx, controlReq); err != nil {
				c.resolveOrRetryRevertPhaseRequest(ctx, logger, apply, storage.ControlOperationRevert, storage.LogEventRevertTriggered, pending, err)
			} else {
				revertedByControlRequest = true
				if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationRevert); err != nil {
					logger.Warn("failed to complete revert control request", "error", err)
				}
				c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventRevertTriggered, storage.LogSourceSchemaBot,
					fmt.Sprintf("Revert triggered by user%s", callerApplyLogSuffix(controlRequestCaller(pending))), state.Apply.RevertWindow, state.Apply.Reverting)
			}
		}
	}

	// Revert window enabled (default): auto-skip based on deployed_at + configured duration.
	// Falls back to stateEnteredAt if deployed_at is unavailable. A user revert
	// this tick takes precedence — do not also auto-skip the window shut.
	if result.State == engine.StateRevertWindow && !opts.SkipRevert && !ps.revertSkipped && !revertedByControlRequest {
		revertDeadline := c.revertWindowDeadline(logger, result.ResumeState, ps.stateEnteredAt)
		if !revertDeadline.IsZero() && now.After(revertDeadline) {
			logger.Info("revert window expired, skipping", "deadline", revertDeadline)
			c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
				"Revert window expired, finalizing", "", "")
			if _, err := eng.SkipRevert(ctx, controlReq); err != nil {
				logger.Error("revert window timeout skip failed", append(apply.MutableLogAttrs(), "error", err)...)
			} else {
				c.markRevertSkipped(ctx, apply)
				c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventSkipRevertTriggered, storage.LogSourceSchemaBot,
					"Revert window expired, skip-revert triggered", state.Apply.RevertWindow, state.Apply.SkippingRevert)
			}
			ps.revertSkipped = true
		}
	}

	// A multi-operation drive (operation lease only) owns its operation, not the
	// parent applies row: it has synced its tasks and run the engine's auto
	// actions above, but the parent state, terminal side-effects, and the
	// terminal summary are the operator's projection to make after the drive
	// returns. Stop polling once this operation's own tasks settle (or park at
	// the cutover barrier); the operator persists the operation row and projects
	// the parent. Without this, a drive that won the terminal projection CAS here
	// would suppress the operator's once-only terminal summary.
	if suppressParentApplyWrites(ctx) {
		opState := state.DeriveApplyState(taskStates(tasks))
		if len(tasks) == 0 {
			opState = state.DeriveApplyState([]string{newState})
			if state.IsTerminalApplyState(opState) || state.IsState(opState, state.Apply.FailedRetryable, state.Apply.WaitingForCutover) {
				if err := c.markTasklessOperationState(ctx, apply, opState, result.ErrorMessage); err != nil {
					logger.Error("failed to mark task-less apply_operation from progress",
						"operation_state", opState, "error", err)
					return true
				}
			}
		}
		if releaseAtCutoverBarrier && state.IsState(opState, state.Apply.WaitingForCutover) {
			logger.Info("operation parked at cutover barrier; exiting operation drive",
				"mode", groupedApplyMode(apply, options), "operation_state", opState)
			return true
		}
		if state.IsTerminalApplyState(opState) || state.IsState(opState, state.Apply.FailedRetryable) {
			logger.Info("operation settled; exiting operation drive for operator projection",
				"mode", groupedApplyMode(apply, options), "operation_state", opState)
			return true
		}
		return false
	}

	// Update apply state from persisted task state so recovery guards can keep
	// storage ahead of stale engine progress until Spirit reaches the cutover wait again.
	if len(tasks) == 0 {
		apply.State = state.DeriveApplyState([]string{newState})
	} else if derived, ok := c.deriveAggregateApplyState(ctx, apply, tasks); ok {
		apply.State = derived
	}
	// Once skip-revert has been accepted, PlanetScale keeps reporting
	// complete_pending_revert (which derives to revert_window) until it finishes
	// discarding the staged revert. Surface that finalization as skipping_revert so
	// the comment and CLI stop offering a resumable revert window and show that the
	// change is being made permanent. It clears on its own: when the engine reports
	// complete the aggregate derives completed and this override no longer applies.
	if ps.revertSkipped && state.IsState(apply.State, state.Apply.RevertWindow) {
		apply.State = state.Apply.SkippingRevert
	}
	apply.UpdatedAt = now
	freshApply, err := c.storage.Applies().Get(ctx, apply.ID)
	if err != nil {
		logger.Error("failed to reload apply before progress state update", append(apply.MutableLogAttrs(), "error", err)...)
		return true
	}
	if freshApply == nil {
		logger.Warn("apply row missing before progress state update; yielding",
			apply.MutableLogAttrs()...)
		return true
	}
	if state.IsTerminalApplyState(freshApply.State) {
		logger.Info("apply already terminal in storage, not overwriting with stale progress state",
			"stored_state", freshApply.State,
			"progress_state", apply.State)
		*apply = *freshApply
		if err := completePendingRequestsForTerminalApply(ctx, c.storage, apply); err != nil {
			logger.Warn("failed to complete pending control requests for terminal apply; current apply owner will exit for operator retry",
				"error", err)
			return true
		}
		return true
	}
	// expectedState is the authoritative current value: the projection write
	// below compare-and-swaps on it so a stale projection cannot clobber a newer
	// state a sibling drive already wrote between this reload and the write.
	expectedState := freshApply.State

	// Gate apply-level terminal side-effects on the rollout-projected apply state
	// (apply.State, derived above), not the current operation's engine result.
	// Under on_failure=continue a failed operation holds the apply running while
	// siblings are still in flight, so one operation's terminal engine result
	// must not stamp completed_at, drop the active-applies metric, tear down
	// observers, or stop polling for the whole apply. With one operation per
	// apply the projection equals this operation's derived state, so this is a
	// no-op until the multi-deployment fan-out lands.
	quiesce, retryableFailure, stampCompletedAt := applyQuiesceDecision(apply.State)
	if quiesce {
		if stampCompletedAt {
			apply.CompletedAt = &now
		} else {
			apply.CompletedAt = nil
		}
		// Prefer this operation's engine failure message. Under on_failure=continue
		// the rollout projection can resolve the apply to a failure because of a
		// sibling operation while this engine result is non-failed, so fall back to
		// the failed task rows to avoid persisting a failed apply with no message.
		if result.State == engine.StateFailed {
			if msg := progressFailureMessage(result); msg != "" {
				apply.ErrorMessage = msg
			}
		}
		ensureApplyFailureMessage(apply, tasks)
		swapped, err := c.storage.Applies().UpdateDerivedState(ctx, apply.ID, expectedState, apply.State, apply.ErrorMessage, apply.StartedAt, apply.CompletedAt)
		if err != nil {
			logger.Error("failed to update apply state", append(apply.MutableLogAttrs(), "error", err)...)
		} else if !swapped {
			// Another drive advanced the apply between our reload and write; it
			// owns the terminal transition and its side-effects. Skip ours.
			logger.Info("apply terminal-state write lost a race; yielding to the owning drive",
				"expected_state", expectedState, "derived_state", apply.State)
			return true
		}
		if err := completePendingRequestsForTerminalApply(ctx, c.storage, apply); err != nil {
			logger.Warn("failed to complete pending control requests after terminal progress reconciliation; current apply owner will exit for operator retry",
				"error", err)
			return true
		}
		metrics.AdjustActiveApplies(ctx, -1, apply.Database, apply.Deployment, apply.Environment)
		switch {
		case retryableFailure:
			logger.Warn("apply paused for operator retry",
				"mode", groupedApplyMode(apply, options), "error", apply.ErrorMessage, "task_count", len(tasks))
		case state.IsState(apply.State, state.Apply.Failed):
			logger.Error("apply failed",
				"mode", groupedApplyMode(apply, options), "error", apply.ErrorMessage, "task_count", len(tasks))
		default:
			logger.Info("apply completed",
				"mode", groupedApplyMode(apply, options), "state", apply.State, "task_count", len(tasks))
		}
		eventMessage := fmt.Sprintf("Apply completed with state: %s", apply.State)
		if retryableFailure {
			eventMessage = "Apply paused for operator retry after retryable engine failure"
		}
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			eventMessage, ps.lastTaskState, apply.State)

		if retryableFailure {
			if obs := c.getObserver(apply.ID); obs != nil {
				obs.OnProgress(apply, tasks)
			}
			return true
		}

		// Notify observer of terminal state, then clean up
		if obs := c.getObserver(apply.ID); obs != nil {
			obs.OnTerminal(apply, tasks)
			c.clearObserver(apply.ID)
		}
		return true
	}

	swapped, err := c.storage.Applies().UpdateDerivedState(ctx, apply.ID, expectedState, apply.State, apply.ErrorMessage, apply.StartedAt, apply.CompletedAt)
	if err != nil {
		logger.Error("failed to update apply state", append(apply.MutableLogAttrs(), "error", err)...)
	} else if !swapped {
		// Another drive advanced the apply between our reload and write; our
		// progress projection is stale. Skip the observer update and let the next
		// poll reconcile.
		logger.Info("apply progress-state write lost a race; skipping",
			"expected_state", expectedState, "derived_state", apply.State)
		return false
	}

	// Notify observer of progress update
	if obs := c.getObserver(apply.ID); obs != nil {
		obs.OnProgress(apply, tasks)
	}

	// Exit this operation's drive once its own tasks have settled, even though
	// the aggregate apply has not quiesced. The apply-level gate above keys off
	// the rollout projection: under on_failure=continue a still-in-flight sibling
	// holds the apply running, so it was skipped. This operation's work is done,
	// so stop polling and let the operator persist its apply_operation row and
	// re-derive the parent; the apply-level wind-down (completed_at, metric drop,
	// observer teardown, stop-request completion) stays with the last sibling to
	// settle. With one operation per apply the projection equals this operation's
	// state, so the apply-level gate already fired when it finished and this is
	// never reached — single-operation behaviour is unchanged.
	opState := state.DeriveApplyState(taskStates(tasks))
	// Park-and-release at the cutover barrier. Under an ordered-cutover policy a
	// multi-deployment operation runs its copy phase and then stops at
	// waiting_for_cutover instead of holding the claim for a manual cutover: the
	// drive exits so the operator persists the operation row at
	// waiting_for_cutover (completed_at nil) and frees it for the
	// deployment-ordered cutover claim. releaseAtCutoverBarrier is set only for
	// multi-operation barrier operations, so single-operation drives (including
	// manual --defer-cutover) keep waiting for a manual cutover unchanged.
	if releaseAtCutoverBarrier && state.IsState(opState, state.Apply.WaitingForCutover) {
		logger.Info("operation parked at cutover barrier; exiting copy drive",
			"mode", groupedApplyMode(apply, options), "operation_state", opState, "apply_state", apply.State)
		return true
	}
	if state.IsTerminalApplyState(opState) || state.IsState(opState, state.Apply.FailedRetryable) {
		logger.Info("operation settled while apply continues; exiting operation drive",
			"mode", groupedApplyMode(apply, options), "operation_state", opState, "apply_state", apply.State)
		return true
	}
	return false
}

// engineReportsLostApplyWork reports whether a grouped progress poll came back
// with no active schema change (a state that maps to pending) while durable
// storage says at least one of the apply's tasks is in flight. A grouped apply
// is a single engine operation, so the engine-vs-storage divergence is
// detected at the apply level and one trust budget covers all of its tasks;
// the per-task stored states still decide which tasks a settlement touches.
// Like the sequential detection, this shape is ambiguous on its own — the
// caller treats it as the start of a timed trust budget, not as evidence (see
// engineReportsLostWork).
func engineReportsLostApplyWork(engineTaskState string, tasks []*storage.Task) bool {
	for _, task := range tasks {
		if engineReportsLostWork(task.State, engineTaskState) {
			return true
		}
	}
	return false
}

// settleLostEngineWorkForTasks resolves a grouped apply's in-flight tasks after
// the engine has stopped reporting on them, by verifying the target schema
// directly — the grouped counterpart of settleLostEngineWork. The engine says
// no schema change is active while durable storage says the apply's work is in
// flight, and that divergence outlasted the tolerated staleness window, so
// engine progress can never terminalize these tasks and the target itself is
// the only remaining authority.
//
// Revert-phase tasks rest retryable without a target read — a schema read
// cannot settle a revert — so they settle even when the re-plan below fails.
// Every other in-flight task settles from one shared re-plan of the reviewed
// schema set: completed when its (namespace, shard, table) no longer needs a
// change, retryable when it still does. Tasks already at rest or terminal are
// left untouched, so a settlement retried after a verification error never
// re-settles what an earlier pass already decided. A verification error is
// returned for the caller's consecutive-error budget to count.
//
// Returns the tasks it settled, so the caller can keep the same tick's engine
// report from claiming a state for work the target already answered for.
func (c *LocalClient) settleLostEngineWorkForTasks(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, engineState engine.State) (settledTaskSet, error) {
	settled := settledTaskSet{}
	var unverified []*storage.Task
	for _, task := range tasks {
		if !state.IsInFlightTaskState(task.State) {
			c.logger.Debug("leaving task out of lost-work settlement; its stored state has no active engine work",
				append(task.LogAttrs(), "apply_id", apply.ApplyIdentifier, "engine_state", engineState)...)
			continue
		}
		if taskInRevertPhase(task) {
			c.settleLostRevertPhaseTask(ctx, apply, task, engineState)
			settled.add(task)
			continue
		}
		unverified = append(unverified, task)
	}
	if len(unverified) == 0 {
		return settled, nil
	}
	plan, err := c.storage.Plans().GetByID(ctx, apply.PlanID)
	if err != nil {
		return settled, fmt.Errorf("load plan for apply %s to verify target schema: %w", apply.ApplyIdentifier, err)
	}
	if plan == nil {
		return settled, fmt.Errorf("plan not found for apply %s while verifying target schema", apply.ApplyIdentifier)
	}
	replanDDL, err := c.replanTargetSchema(ctx, apply, plan)
	if err != nil {
		return settled, fmt.Errorf("verify target schema for apply %s: %w", apply.ApplyIdentifier, err)
	}
	for _, task := range unverified {
		c.settleLostVerifiedTask(ctx, apply, task, replanVerdictForTask(replanDDL, task), engineState)
		settled.add(task)
	}
	return settled, nil
}

// settledTaskSet holds the tasks a drive resolved from a more authoritative
// source than the engine during a single tick — here, the live target schema.
// Their state is decided and persisted, so the same tick's engine report is no
// longer entitled to claim one for them.
type settledTaskSet map[*storage.Task]struct{}

func (s settledTaskSet) add(task *storage.Task) { s[task] = struct{}{} }

func (s settledTaskSet) contains(task *storage.Task) bool {
	_, ok := s[task]
	return ok
}

// autoTriggerCutover fires the engine cutover for a drive that is not
// deferring cutover, pacing the operator-visible signal around the backend's
// staging window: the trigger event is recorded once per drive so retries do
// not duplicate it, a not-ready rejection retries quietly on the next progress
// tick, and a rejection that outlives the staging window escalates to Error
// logging on every further tick plus a one-time timeline event, so a backend
// that never stages the cutover pages instead of idling.
//
// The drive is the sole cutover actor, so a hard rejection cannot be left to
// retry forever: each one counts toward a consecutive-failure bound (mirroring
// the progress-poll bound) that settles the apply — failed, or paused for
// operator retry — instead of holding the database's deploy queue and lock
// indefinitely. Returns true when the apply was settled and this owner must
// exit the drive.
func (c *LocalClient) autoTriggerCutover(ctx context.Context, eng engine.Engine, apply *storage.Apply, tasks []*storage.Task, controlReq *engine.ControlRequest, ps *atomicPollState, now time.Time) bool {
	// Carry stable apply identity through every automatic cutover attempt.
	logger := c.logger.With(apply.IdentityLogAttrs()...)
	if !ps.cutoverTriggerLogged {
		logger.Info("auto-triggering cutover (not in defer mode)")
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventCutoverTriggered, storage.LogSourceSchemaBot,
			"Auto-triggering cutover (defer_cutover not set)", "", "")
		ps.cutoverTriggerLogged = true
	}
	_, err := eng.Cutover(ctx, controlReq)
	// A cancelled drive is why the cutover call returned, so the error says
	// nothing about the backend's willingness to cut over. The driver that
	// reclaims the apply reattempts it from the stored state.
	if err != nil && c.driveCancelled(ctx, apply, "while triggering cutover") {
		return true
	}
	switch {
	case err == nil:
		ps.cutoverNotReadySince = time.Time{}
		ps.cutoverNotReadyEscalated = false
		ps.consecutiveCutoverFailures = 0
	case engine.IsNotReady(err):
		// The engine's backend advertised the cutover gate before it was ready
		// to accept the cutover; the drive reattempts on the next progress
		// tick, once it catches up. Self-clearing, so it does not count toward
		// the consecutive-failure bound — its own escalation below covers a
		// staging window that never closes.
		ps.consecutiveCutoverFailures = 0
		if ps.cutoverNotReadySince.IsZero() {
			ps.cutoverNotReadySince = now
		}
		notReadyFor := now.Sub(ps.cutoverNotReadySince)
		if notReadyFor < cutoverNotReadyEscalationAfter {
			logger.Info("auto-cutover not accepted yet; retrying at the next progress tick",
				append(apply.MutableLogAttrs(), "not_ready_for", notReadyFor, "error", err)...)
			return false
		}
		if !ps.cutoverNotReadyEscalated {
			c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelError, storage.LogEventError, storage.LogSourceSchemaBot,
				fmt.Sprintf("Cutover has not been accepted by the engine backend for %s; SchemaBot keeps retrying every progress tick", notReadyFor.Round(time.Second)), "", "")
			ps.cutoverNotReadyEscalated = true
		}
		logger.Error("auto-cutover has been rejected as not-ready beyond the staging window; the engine backend is not staging the cutover and the drive keeps retrying",
			append(apply.MutableLogAttrs(), "not_ready_for", notReadyFor, "error", err)...)
	default:
		metrics.RecordAutoCutoverFailure(ctx, apply.Database, apply.Deployment, apply.Environment)
		// Permanent rejections (e.g., the deploy request no longer exists) can
		// never succeed on retry; fail the apply immediately.
		var permanent *engine.PermanentError
		if errors.As(err, &permanent) {
			logger.Error("auto-cutover failed with permanent error",
				append(apply.MutableLogAttrs(), "error", err)...)
			c.failApplyWithTasks(ctx, apply, tasks, fmt.Sprintf("cutover failed: %v", err))
			return true
		}
		ps.consecutiveCutoverFailures++
		logger.Error("auto-cutover failed",
			append(apply.MutableLogAttrs(), "error", err, "consecutive_cutover_failures", ps.consecutiveCutoverFailures)...)
		if ps.consecutiveCutoverFailures < maxConsecutiveCutoverFailures {
			return false
		}
		if c.shouldRetryEngineError(err) {
			logger.Warn("auto-cutover failed repeatedly, pausing apply for operator retry",
				append(apply.MutableLogAttrs(), "consecutive_cutover_failures", ps.consecutiveCutoverFailures)...)
			c.markApplyRetryableWithTasks(ctx, apply, tasks, fmt.Sprintf("cutover failed after %d consecutive attempts: %v", ps.consecutiveCutoverFailures, err))
			return true
		}
		logger.Error("auto-cutover failed repeatedly, failing apply",
			append(apply.MutableLogAttrs(), "consecutive_cutover_failures", ps.consecutiveCutoverFailures)...)
		c.failApplyWithTasks(ctx, apply, tasks, fmt.Sprintf("cutover failed after %d consecutive attempts: %v", ps.consecutiveCutoverFailures, err))
		return true
	}
	return false
}

// markRevertSkipped records skip-revert on the apply so progress consumers know
// finalization is in progress.
func (c *LocalClient) markRevertSkipped(ctx context.Context, apply *storage.Apply) {
	// Keep skip-revert persistence warnings attributable to their apply.
	logger := c.logger.With(apply.IdentityLogAttrs()...)
	if err := c.storage.Applies().SetRevertSkipped(ctx, apply.ID, time.Now()); err != nil {
		logger.Warn("failed to record skip-revert on apply", append(apply.MutableLogAttrs(), "error", err)...)
	}
}

// resolveOrRetryRevertPhaseRequest disposes of a revert-phase control operation
// the engine refused. A refusal the engine issues for its whole database type is
// issued identically on every later claim, so the request is resolved with the
// engine's reason; any other failure leaves it pending for the next drive tick
// to retry. Neither outcome advances the revert window: a refusal declines the
// operator's command rather than carrying it out, so the window runs to its own
// deadline either way.
func (c *LocalClient) resolveOrRetryRevertPhaseRequest(ctx context.Context, logger *slog.Logger, apply *storage.Apply, operation storage.ControlOperation, eventType string, controlReq *storage.ApplyControlRequest, opErr error) {
	declined, declineErr := c.failPendingRequestForUnsupportedOperation(ctx, logger, apply, operation, eventType, controlReq, opErr)
	if declineErr != nil {
		logger.Error("could not resolve an engine-declined control request; it stays pending and the next drive claim collects the same decline",
			"operation", string(operation),
			"requested_by", controlRequestCaller(controlReq),
			"error", declineErr)
		return
	}
	if declined {
		// failPendingRequestForUnsupportedOperation logs the rejection and
		// records it on the apply's timeline for the operator who asked.
		return
	}
	logger.Error("control request failed; the next drive claim retries it",
		"operation", string(operation),
		"requested_by", controlRequestCaller(controlReq),
		"error", opErr)
}

// revertWindowDuration returns the configured revert window duration, falling
// back to the engine default when none is set. The server writes a canonical,
// already-validated duration into metadata, so a malformed value only reaches
// here when an embedder populates metadata directly. Rather than silently
// using the default — which would hide a misconfigured revert window — an
// unparseable or non-positive value is surfaced via a warning before falling
// back, so the whole class of bad input is observable.
func (c *LocalClient) revertWindowDuration(logger *slog.Logger) time.Duration {
	s := c.config.Metadata["revert_window_duration"]
	if s == "" {
		return defaultRevertWindowDuration
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		logger.Warn("invalid revert_window_duration metadata; using engine default",
			"value", s, "default", defaultRevertWindowDuration, "error", err)
		return defaultRevertWindowDuration
	}
	if d <= 0 {
		logger.Warn("non-positive revert_window_duration metadata; using engine default",
			"value", s, "default", defaultRevertWindowDuration)
		return defaultRevertWindowDuration
	}
	return d
}

// revertWindowDeadline computes when the revert window expires.
// Uses deployed_at from engine metadata (accurate to PlanetScale's clock) plus
// the configured revert period. Falls back to stateEnteredAt if metadata is unavailable.
func (c *LocalClient) revertWindowDeadline(logger *slog.Logger, resumeState *engine.ResumeState, stateEnteredAt time.Time) time.Time {
	duration := c.revertWindowDuration(logger)
	if resumeState != nil && resumeState.Metadata != "" {
		if meta, err := decodePSMetadataForStorage(resumeState.Metadata); err == nil && meta != nil && meta.DeployedAt != nil {
			return meta.DeployedAt.Add(duration)
		}
	}
	if !stateEnteredAt.IsZero() {
		return stateEnteredAt.Add(duration)
	}
	return time.Time{}
}

// logAtomicProgress logs per-table progress to apply_logs every 10 seconds.
func (c *LocalClient) logAtomicProgress(ctx context.Context, apply *storage.Apply, result *engine.ProgressResult, ps *atomicPollState, now time.Time) {
	if time.Since(ps.lastProgressLog) <= 10*time.Second || len(result.Tables) == 0 {
		return
	}
	var parts []string
	for _, t := range result.Tables {
		if t.RowsTotal > 0 {
			pct := float64(t.RowsCopied) / float64(t.RowsTotal) * 100
			parts = append(parts, fmt.Sprintf("%s: %.1f%%", t.Table, pct))
		} else {
			parts = append(parts, fmt.Sprintf("%s: %s", t.Table, t.State))
		}
	}
	if len(parts) > 0 && result.Message != ps.lastLoggedState {
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventProgress, storage.LogSourceSchemaBot,
			fmt.Sprintf("Progress: %s (%s)", strings.Join(parts, ", "), result.Message), "", "")
		ps.lastLoggedState = result.Message
	}
	ps.lastProgressLog = now
}

// enginePoll is one grouped progress poll, decoded once for the whole apply:
// the engine's report, the apply-wide task state the poll mapped to, the
// instant classification its resume metadata carried, and the tick's clock.
// Both halves of the projection read the same value, so a task can never be
// refreshed against one poll and advanced against another.
type enginePoll struct {
	result   *engine.ProgressResult
	newState string
	// instantFromMetadata is the engine's own instant classification, decoded
	// from the poll's resume metadata. It stands in for the per-table report an
	// instant DDL never produces, having copied no rows.
	instantFromMetadata bool
	now                 time.Time
}

// retryableFailure reports whether the state this poll claims is a failure the
// drive will retry, which leaves the task short of a finished outcome: no
// completion stamp, and no finished progress bar.
func (p enginePoll) retryableFailure() bool {
	return state.IsState(p.newState, state.Task.FailedRetryable)
}

// syncAtomicTaskProgress projects one grouped progress poll onto every task of
// the apply. A poll carries two separable things, and each task takes them
// through a separate helper: the fields the operator sees, which a poll may
// always refresh, and the state the poll claims for the task, which is a
// correctness decision.
//
// Every task the poll speaks for ends its tick with a persisted write, even
// when no field moved: the operator reads tasks.updated_at as the drive's
// liveness signal (ApplyDriveStallAfter) and cancels a drive whose rows stop
// advancing, so the write must stay unconditional — including through parked
// states such as deferred cutovers and revert windows, where nothing changes
// tick to tick. A task the poll does not speak for is one already settled from
// a more authoritative source earlier in the tick, and it took its persisted
// write from that settlement.
func (c *LocalClient) syncAtomicTaskProgress(ctx context.Context, logger *slog.Logger, tasks []*storage.Task, result *engine.ProgressResult, newState string, now time.Time, settled settledTaskSet) {
	tableProgress := indexEngineTableProgress(result.Tables)
	poll := enginePoll{result: result, newState: newState, now: now}
	if result.ResumeState != nil && result.ResumeState.Metadata != "" {
		if meta, err := decodePSMetadataForStorage(result.ResumeState.Metadata); err == nil && meta != nil {
			poll.instantFromMetadata = meta.IsInstant
		}
	}

	for _, task := range tasks {
		if poll.retryableFailure() && state.IsTerminalTaskState(task.State) {
			continue
		}
		if settled.contains(task) {
			// The target schema answered for this task earlier in the tick and
			// settlement persisted the result. The poll that sent the drive to
			// the target reports no active schema change, so it carries neither
			// progress to display nor a state this task may take.
			logger.Debug("leaving settled task out of the engine progress projection",
				append(task.LogAttrs(), "engine_state", result.State)...)
			continue
		}
		tp, _ := tableProgress.ForTask(task)
		if tp != nil {
			c.unrecognizedStatuses.observeTaskStatus(ctx, logger, task, tp.State)
		}
		c.refreshTaskDisplayFromEngine(ctx, logger, task, tp, poll)
		c.advanceTaskFromEngineProgress(ctx, task, tp, poll)
	}
}

// refreshTaskDisplayFromEngine projects a progress poll onto the fields the
// operator sees: row counts, percentage, ETA, checksum progress, throttle
// state, instant classification, and the per-shard breakdown. None of them
// decides what state a task is in, so this is always safe to run — a stale or
// absent engine snapshot degrades the display and nothing else. The per-shard
// breakdown does carry a state per shard, but it lands on rows of its own
// (shard != "") that only the per-shard renderer loads; the task state machine
// never reads them.
//
// Those shard rows are the only write made here: the task's own refreshed
// fields are mutated in memory and reach storage on the write
// advanceTaskFromEngineProgress makes, which is why the refresh runs first — a
// transition rolled back by a storage failure keeps the display fields the
// caller refreshed before it. A caller that refreshes the display and then
// declines the advance still owes the task a persisted write. The operator
// reads tasks.updated_at as the drive's liveness signal
// (ApplyDriveStallAfter), so a tick that ends without one reads as a drive
// that stopped making progress, however fresh the fields left in memory are.
func (c *LocalClient) refreshTaskDisplayFromEngine(ctx context.Context, logger *slog.Logger, task *storage.Task, tp *engine.TableProgress, poll enginePoll) {
	switch {
	case tp != nil:
		applyEngineTableDisplayFields(task, tp)
		// Persist the per-shard breakdown as per-shard tasks so the read path
		// serves per-shard state out of tasks storage rather than re-querying
		// the engine. Only a driver writes it, under the lease its claim holds.
		c.writeShardProgress(ctx, logger, task, tp, poll.now)
	case poll.instantFromMetadata:
		// An instant DDL copies no rows, so no per-table report arrives to
		// fill the bar: any terminal outcome it reaches other than a retryable
		// failure leaves the change made, and the bar full.
		task.IsInstant = true
		if poll.result.State.IsTerminal() && !poll.retryableFailure() {
			task.ProgressPercent = 100
		}
	}
	// A completed poll ends the bar at 100 whatever the table's last sample
	// read: row counts are estimates, and a finished copy can land a fraction
	// short of its own total.
	if poll.result.State == engine.StateCompleted {
		task.ProgressPercent = 100
	}
}

// advanceTaskFromEngineProgress applies the part of a progress poll that is a
// correctness decision: the state the poll claims for the task, and the
// start/completion/error stamps that belong to that outcome.
//
// It is separate from the display refresh so a drive that has already settled a
// task from a more authoritative source — a durable record, or the target
// schema itself — can keep showing live engine progress without letting the
// poll re-open what was settled. taskStateWithNoBackwardProgress remains the
// policy for whether the claim is allowed to move the stored state.
//
// This is also where the tick reaches storage, for the task's stamps and for
// the display fields the refresh left in memory.
func (c *LocalClient) advanceTaskFromEngineProgress(ctx context.Context, task *storage.Task, tp *engine.TableProgress, poll enginePoll) {
	retryableFailure := poll.retryableFailure()
	if tp != nil {
		if tp.StartedAt != nil && task.StartedAt == nil {
			task.StartedAt = tp.StartedAt
		}
		if tp.CompletedAt != nil && !retryableFailure && task.CompletedAt == nil {
			task.CompletedAt = tp.CompletedAt
		}
	}
	if task.StartedAt == nil && poll.newState != state.Task.Pending {
		task.StartedAt = &poll.now
	}
	if poll.result.State.IsTerminal() && !retryableFailure && task.CompletedAt == nil {
		task.CompletedAt = &poll.now
	}
	if poll.result.State == engine.StateFailed && task.ErrorMessage == "" {
		if msg := progressFailureMessage(poll.result); msg != "" {
			task.ErrorMessage = msg
		}
	}
	c.transitionTaskState(ctx, task, 0, taskStateWithNoBackwardProgress(task.State, engineTaskStateClaim(poll.newState, tp)), "")
}

// engineTaskStateClaim is the state a progress poll claims for one task: the
// apply-wide state the poll mapped to, refined into the table's own post-copy
// phase when the engine reported one. A table catching up on accumulated
// changes, checksumming, or cutting over is then stored — and rendered — as
// that phase rather than as a serene fully-copied bar.
//
// It is only a claim. Whether the stored state may move to it is decided by
// taskStateWithNoBackwardProgress.
func engineTaskStateClaim(newState string, tp *engine.TableProgress) string {
	if tp == nil {
		return newState
	}
	if phase, isPhase := tablePhaseTaskState(tp.State); isPhase && state.IsState(newState, state.Task.Running) {
		return phase
	}
	return newState
}

// applyEngineTableDisplayFields copies a poll's per-table progress onto the
// task's display fields. Shared by the grouped and sequential drives so both
// render from the same projection of an engine report.
func applyEngineTableDisplayFields(task *storage.Task, tp *engine.TableProgress) {
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

// tablePhaseTaskState maps a per-table engine state to the refined task state
// it represents, when that state is one of the post-copy phases surfaced per
// table: applying the accumulated changeset, verifying via checksum, or
// cutting over. Any other per-table state does not refine the task state — in
// particular "completed", which for engines like Spirit means only that the
// table's row copy finished, not that the table cut over.
func tablePhaseTaskState(tableState string) (string, bool) {
	switch normalized := state.NormalizeTaskStatus(tableState); normalized {
	case state.Task.CatchingUp, state.Task.Checksumming, state.Task.PostChecksum, state.Task.CuttingOver:
		return normalized, true
	default:
		return "", false
	}
}

// writeShardProgress persists a table's per-shard breakdown as per-shard tasks
// (shard != ""), so the renderer can show per-shard state from storage instead
// of a live re-query. It runs only inside the operator's lease-held drive: a
// multi-operation fan-out drive holds the operation lease, a single-operation
// (whole-apply) drive holds the apply lease, and UpsertShardProgress accepts
// either. Every drive runs under an operator claim, so a poll context without
// a lease is an invariant violation: the per-shard breakdown would silently
// stop persisting, which is warned loudly rather than skipped quietly. A
// failed shard write is logged, not fatal — the next reconcile re-applies it.
func (c *LocalClient) writeShardProgress(ctx context.Context, logger *slog.Logger, table *storage.Task, tp *engine.TableProgress, now time.Time) {
	if len(tp.Shards) == 0 {
		return
	}
	_, hasOpLease := storage.OperationLeaseFromContext(ctx)
	_, hasApplyLease := storage.ApplyLeaseFromContext(ctx)
	if !hasOpLease && !hasApplyLease {
		logger.Warn("per-shard progress will not be persisted: drive context carries no operation or apply lease",
			"task_id", table.TaskIdentifier, "table", table.TableName, "state", table.State,
			"namespace", table.Namespace, "shard_count", len(tp.Shards))
		return
	}
	var operationID int64
	if table.ApplyOperationID != nil {
		operationID = *table.ApplyOperationID
	}
	for _, sh := range tp.Shards {
		c.unrecognizedStatuses.observeShardStatus(ctx, logger, table, sh.Shard, sh.State)
		shardTask := &storage.Task{
			TaskIdentifier:   engine.NewTaskID(),
			ApplyID:          table.ApplyID,
			ApplyOperationID: table.ApplyOperationID,
			PlanID:           table.PlanID,
			Database:         table.Database,
			DatabaseType:     table.DatabaseType,
			Engine:           table.Engine,
			Repository:       table.Repository,
			PullRequest:      table.PullRequest,
			Environment:      table.Environment,
			Namespace:        table.Namespace,
			TableName:        table.TableName,
			Shard:            sh.Shard,
			DDL:              table.DDL,
			DDLAction:        table.DDLAction,
			State:            state.NormalizeShardStatus(sh.State),
			RowsCopied:       sh.RowsCopied,
			RowsTotal:        sh.RowsTotal,
			ProgressPercent:  sh.Progress,
			ETASeconds:       int(sh.ETASeconds),
			CutoverAttempts:  sh.CutoverAttempts,
			CreatedAt:        now,
			UpdatedAt:        now,
		}
		err := c.storage.Tasks().UpsertShardProgress(ctx, shardTask)
		if err == nil {
			continue
		}
		if errors.Is(err, storage.ErrApplyLeaseLost) {
			// A peer claimed the operation; this driver is displaced and the new
			// owner reconciles the remaining shards. Stop write-through — every
			// further shard would fail the same way. Expected during failover.
			logger.Debug("operator: stopping shard progress write-through, operation lease lost",
				"apply_operation_id", operationID,
				"namespace", table.Namespace, "table", table.TableName, "shard", sh.Shard)
			return
		}
		logger.Error("operator: failed to persist shard progress",
			"apply_operation_id", operationID,
			"namespace", table.Namespace, "table", table.TableName, "shard", sh.Shard,
			"error", err)
	}
}
