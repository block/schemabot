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
		c.failApplyWithTasks(ctx, apply, tasks, fmt.Sprintf("engine did not accept the apply: %s", result.Message))
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

func (c *LocalClient) loadEngineResumeState(ctx context.Context, task *storage.Task) (*engine.ResumeState, error) {
	operationID, err := applyOperationIDForTask(task)
	if err != nil {
		return nil, err
	}
	return c.loadEngineResumeStateForOperation(ctx, operationID)
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
// closed and a failed sibling terminalizes the apply immediately.
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
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	// Seed revertSkipped from the durable signal so a driver that picks this apply
	// up after a restart treats skip-revert as already accepted: it won't re-attempt
	// SkipRevert, and it keeps surfacing skipping_revert while finalization is in
	// flight rather than falling back to revert_window.
	ps := &atomicPollState{lastProgressLog: time.Now(), revertSkipped: apply.RevertSkippedAt != nil}

	for {
		select {
		case <-ctx.Done():
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
		if ps.consecutiveErrors >= 10 {
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
	ps.consecutiveErrors = 0

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
				logger.Error("failed to save Vitess engine resume state from progress polling",
					append(apply.MutableLogAttrs(), "error", saveErr)...)
				c.markApplyRetryableWithTasks(ctx, apply, tasks, fmt.Sprintf("failed to save engine resume state from progress polling: %v", saveErr))
				return true
			}
		}
	}

	now := time.Now()
	newState := taskStateFromProgressResult(result)

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
	// the transient reasons (schema-change context still being discovered, shard
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
	c.syncAtomicTaskProgress(ctx, logger, tasks, result, newState, now)
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
	// A volume failure never aborts the drive: the copy continues at its
	// current volume and a still-pending request is retried at the next tick.
	// A lost lease is the exception — this owner must stop driving.
	if err := c.processPendingVolumeControlRequest(ctx, apply, eng, creds, resumeState); err != nil {
		if errors.Is(err, storage.ErrApplyLeaseLost) {
			logger.Warn("pending volume request processing lost the apply lease; current apply owner will exit for operator retry",
				"error", err)
			return true
		}
		logger.Warn("pending volume request processing failed; the drive continues and retries at the next progress tick",
			"error", err)
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
				// Leave the request pending so the next drive tick retries.
				logger.Error("skip-revert (control request) failed; will retry", "error", err)
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
				// Leave the request pending so the next drive tick retries.
				logger.Error("revert (control request) failed; will retry", "error", err)
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
	switch _, err := eng.Cutover(ctx, controlReq); {
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

// syncAtomicTaskProgress updates all tasks with engine state and per-table progress.
func (c *LocalClient) syncAtomicTaskProgress(ctx context.Context, logger *slog.Logger, tasks []*storage.Task, result *engine.ProgressResult, newState string, now time.Time) {
	tableProgress := indexEngineTableProgress(result.Tables)
	retryableFailure := state.IsState(newState, state.Task.FailedRetryable)
	instantFromMetadata := false
	if result.ResumeState != nil && result.ResumeState.Metadata != "" {
		if meta, err := decodePSMetadataForStorage(result.ResumeState.Metadata); err == nil && meta != nil {
			instantFromMetadata = meta.IsInstant
		}
	}

	for _, task := range tasks {
		if retryableFailure && state.IsTerminalTaskState(task.State) {
			continue
		}
		engineTaskState := newState
		if tp, ok := engineProgressForTask(tableProgress, task); ok {
			task.RowsCopied = tp.RowsCopied
			task.RowsTotal = tp.RowsTotal
			task.ProgressPercent = tp.Progress
			task.ETASeconds = int(tp.ETASeconds)
			task.ChecksumRowsChecked = tp.ChecksumRowsChecked
			task.ChecksumRowsTotal = tp.ChecksumRowsTotal
			task.IsInstant = tp.IsInstant
			if tp.StartedAt != nil && task.StartedAt == nil {
				task.StartedAt = tp.StartedAt
			}
			if tp.CompletedAt != nil && !retryableFailure && task.CompletedAt == nil {
				task.CompletedAt = tp.CompletedAt
			}
			// A generic running apply refines into the table's post-copy phase
			// when the engine reports one, so a table catching up on accumulated
			// changes, checksumming, or cutting over is stored — and rendered —
			// as that phase rather than as a serene fully-copied bar.
			if phase, isPhase := tablePhaseTaskState(tp.State); isPhase && state.IsState(newState, state.Task.Running) {
				engineTaskState = phase
			}
			// Persist the per-shard breakdown as per-shard tasks so the renderer
			// can show per-shard state from storage. No-op outside the lease-held
			// operator drive (read-path callers carry no operation lease).
			c.writeShardProgress(ctx, logger, task, tp, now)
		} else if instantFromMetadata {
			task.IsInstant = true
			if result.State.IsTerminal() && !retryableFailure {
				task.ProgressPercent = 100
			}
		}
		if task.StartedAt == nil && newState != state.Task.Pending {
			task.StartedAt = &now
		}
		if result.State.IsTerminal() && !retryableFailure && task.CompletedAt == nil {
			task.CompletedAt = &now
		}
		if result.State == engine.StateFailed && task.ErrorMessage == "" {
			if msg := progressFailureMessage(result); msg != "" {
				task.ErrorMessage = msg
			}
		}
		if result.State == engine.StateCompleted {
			task.ProgressPercent = 100
		}
		c.transitionTaskState(ctx, task, 0, taskStateWithNoBackwardProgress(task.State, engineTaskState), "")
	}
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
		shardTask := &storage.Task{
			TaskIdentifier:   newTaskIdentifier(),
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
