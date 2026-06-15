package tern

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// executeGroupedApply runs all DDLs in one engine operation. For Spirit with
// defer_cutover, this is atomic cutover; for Vitess, this is one deploy request.
func (c *LocalClient) executeGroupedApply(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, plan *storage.Plan, options map[string]string) {
	ctx, cancelApply := context.WithCancel(ctx)
	defer cancelApply()
	defer c.startApplyHeartbeat(ctx, apply, cancelApply)()
	mode := groupedApplyMode(apply)
	modeDescription := groupedApplyModeDescription(apply)

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

	// Build per-namespace changes from the plan. For Vitess databases, each
	// namespace is a keyspace (e.g., "testapp", "testapp_sharded"). For MySQL,
	c.logger.Info("building changes from plan", "namespaces", len(plan.Namespaces), "plan_id", plan.PlanIdentifier)
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
	changes := planNamespacesToChanges(plan.Namespaces)

	// For Vitess: initialize the VitessApplyData row before the engine starts.
	// State transitions (preparing_branch, applying_branch_changes, etc.) are
	// handled by the engine via ApplyEvent.NewState in the OnEvent callback.
	if c.config.Type == storage.DatabaseTypeVitess {
		if err := c.storage.VitessApplyData().Save(ctx, &storage.VitessApplyData{
			ApplyID:          apply.ID,
			MigrationContext: apply.ApplyIdentifier,
		}); err != nil {
			c.logger.Error("failed to save vitess apply data", "apply_id", apply.ID, "error", err)
		}
	}

	// Mark the apply as started before calling the engine. The engine may run
	// for a long time (branch creation, DDL application, deploy request) and
	// started_at should reflect when work actually began, not when it finished.
	now := time.Now()
	apply.State = state.Apply.Running
	apply.StartedAt = &now
	apply.UpdatedAt = now
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		c.logger.Error("failed to set started_at", "apply_id", apply.ApplyIdentifier, "error", err)
	}
	if handled, err := c.processPendingStopControlRequest(ctx, apply); err != nil {
		c.logger.Warn("pending stop request processing failed before grouped engine apply; current apply owner will exit for operator retry",
			"apply_id", apply.ApplyIdentifier, "error", err)
		return
	} else if handled {
		return
	}

	// Grouped mode: all DDLs in one engine call. Use the apply identifier as
	// MigrationContext so all table work shares one context for progress tracking.
	result, err := eng.Apply(ctx, &engine.ApplyRequest{
		Database:    apply.Database,
		PlanID:      plan.PlanIdentifier,
		Changes:     changes,
		SchemaFiles: plan.SchemaFiles,
		Options:     options,
		ResumeState: &engine.ResumeState{MigrationContext: apply.ApplyIdentifier},
		Credentials: creds,
		OnEvent: func(event engine.ApplyEvent) {
			oldState := apply.State
			newState := deriveApplyPhase(event)
			c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
				event.Message, oldState, newState)
			applyEventStateTransition(apply, event, func(a *storage.Apply) error {
				return c.storage.Applies().Update(ctx, a)
			}, c.logger)
		},
		OnStateChange: func(rs *engine.ResumeState) {
			if rs == nil {
				c.logger.Debug("OnStateChange: nil resume state", "apply_id", apply.ApplyIdentifier)
				return
			}
			if saveErr := c.saveEngineResumeState(ctx, tasks, rs); saveErr != nil {
				c.logger.Warn("OnStateChange: failed to persist opaque resume state", "apply_id", apply.ApplyIdentifier, "error", saveErr)
				return
			}
			meta, err := decodePSMetadataForStorage(rs.Metadata)
			if err != nil {
				c.logger.Warn("OnStateChange: failed to decode metadata", "apply_id", apply.ApplyIdentifier, "error", err)
				return
			}
			if meta == nil {
				c.logger.Warn("OnStateChange: no PS metadata in resume state", "apply_id", apply.ApplyIdentifier)
				return
			}
			if saveErr := c.storage.VitessApplyData().Save(ctx, &storage.VitessApplyData{
				ApplyID:          apply.ID,
				BranchName:       meta.BranchName,
				DeployRequestID:  meta.DeployRequestID,
				MigrationContext: rs.MigrationContext,
				DeployRequestURL: meta.DeployRequestURL,
				IsInstant:        meta.IsInstant,
				DeferredDeploy:   meta.DeferredDeploy,
			}); saveErr != nil {
				c.logger.Warn("OnStateChange: failed to persist resume state", "apply_id", apply.ApplyIdentifier, "error", saveErr)
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
			c.logger.Warn("apply paused for operator retry", "mode", mode, "error", err, "apply_id", apply.ApplyIdentifier)
		} else {
			c.logger.Error("apply failed", "mode", mode, "error", err, "apply_id", apply.ApplyIdentifier)
		}
		logLevel := storage.LogLevelError
		if newState == state.Apply.FailedRetryable {
			logLevel = storage.LogLevelWarn
		}
		c.logApplyEvent(ctx, apply.ID, nil, logLevel, storage.LogEventError, storage.LogSourceSchemaBot,
			fmt.Sprintf("Engine apply failed: %v", err), state.Apply.Pending, newState)
		return
	}

	if !result.Accepted {
		c.failApplyWithTasks(ctx, apply, tasks, result.Message)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelError, storage.LogEventError, storage.LogSourceSchemaBot,
			fmt.Sprintf("Engine apply not accepted: %s", result.Message), state.Apply.Pending, state.Apply.Failed)
		return
	}

	// Save vitess_apply_data and set IsInstant on tasks BEFORE marking running.
	// The progress handler reads both vitess_apply_data.is_instant and task.is_instant
	// to determine the instant label — both must be committed before the first poll.
	var resumeState *engine.ResumeState
	if result.ResumeState != nil {
		resumeState = result.ResumeState
		if c.config.Type == storage.DatabaseTypeVitess {
			if saveErr := c.saveEngineResumeState(ctx, tasks, resumeState); saveErr != nil {
				c.logger.Error("failed to save opaque engine resume state", "apply_id", apply.ApplyIdentifier, "error", saveErr)
				c.failApplyWithTasks(ctx, apply, tasks, fmt.Sprintf("failed to save engine resume state: %v", saveErr))
				return
			}
		}
		if meta, err := decodePSMetadataForStorage(resumeState.Metadata); meta != nil && err == nil {
			c.logger.Info("saving VitessApplyData from apply result",
				"apply_id", apply.ApplyIdentifier,
				"is_instant", meta.IsInstant,
				"deploy_request_id", meta.DeployRequestID,
				"raw_metadata", resumeState.Metadata[:min(len(resumeState.Metadata), 200)],
			)
			if saveErr := c.storage.VitessApplyData().Save(ctx, &storage.VitessApplyData{
				ApplyID:          apply.ID,
				BranchName:       meta.BranchName,
				DeployRequestID:  meta.DeployRequestID,
				MigrationContext: resumeState.MigrationContext,
				DeployRequestURL: meta.DeployRequestURL,
				IsInstant:        meta.IsInstant,
				DeferredDeploy:   meta.DeferredDeploy,
			}); saveErr != nil {
				c.logger.Warn("failed to save vitess apply data", "apply_id", apply.ApplyIdentifier, "error", saveErr)
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
		c.logger.Error("failed to update apply state", "apply_id", apply.ApplyIdentifier, "state", state.Apply.Running, "error", err)
	}
	c.logger.Info("apply started", "mode", mode, "apply_id", apply.ApplyIdentifier, "task_count", len(tasks))
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
		fmt.Sprintf("All %d tables started copying in parallel", len(tasks)), state.Apply.Pending, apply.State)

	// Poll for completion - all tasks share the same state
	c.pollForCompletionAtomic(ctx, apply, tasks, creds, resumeState)
}

func (c *LocalClient) saveEngineResumeState(ctx context.Context, tasks []*storage.Task, resumeState *engine.ResumeState) error {
	operationID, err := applyOperationIDForTasks(tasks)
	if err != nil {
		return err
	}
	return c.saveEngineResumeStateForOperation(ctx, operationID, resumeState)
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
	currentOpState := state.DeriveApplyState(taskStates(tasks))

	// failClosed reports the current deployment's derived state when the sibling
	// set is in use but unreadable, refusing to terminalize the apply on
	// incomplete information.
	failClosed := func() (string, bool) {
		if state.IsTerminalApplyState(currentOpState) {
			c.logger.Warn("cannot determine aggregate apply state and current deployment is terminal; leaving stored apply state unchanged",
				"apply_id", apply.ApplyIdentifier, "current_deployment_state", currentOpState)
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
		c.logger.Warn("cannot determine aggregate apply state: task set is not scoped to one apply operation",
			"apply_id", apply.ApplyIdentifier, "error", err)
		return failClosed()
	}
	if !usesModel {
		// No operation model in use: tasks carry no apply_operation_id, so there
		// are no siblings and the per-task derivation is authoritative.
		c.logger.Debug("deriving apply state from tasks: apply has no operation model",
			"apply_id", apply.ApplyIdentifier)
		return currentOpState, true
	}

	store := c.storage.ApplyOperations()
	if store == nil {
		// Operation store unavailable: no siblings can exist, so the per-task
		// derivation is authoritative.
		c.logger.Debug("deriving apply state from tasks: apply operation store is not configured",
			"apply_id", apply.ApplyIdentifier)
		return currentOpState, true
	}

	ops, err := store.ListByApply(ctx, apply.ID)
	if err != nil {
		c.logger.Warn("cannot determine aggregate apply state: failed to list sibling apply operations",
			"apply_id", apply.ApplyIdentifier, "apply_operation_id", operationID, "error", err)
		return failClosed()
	}
	if len(ops) == 0 {
		c.logger.Warn("cannot determine aggregate apply state: tasks reference an apply operation but no operation rows were found",
			"apply_id", apply.ApplyIdentifier, "apply_operation_id", operationID)
		return failClosed()
	}

	children := make([]state.RolloutChild, len(ops))
	foundCurrent := false
	for i, op := range ops {
		child := state.RolloutChild{
			State:             op.State,
			ContinueOnFailure: op.OnFailure == storage.OnFailureContinue,
		}
		if op.ID == operationID {
			child.State = currentOpState
			foundCurrent = true
		}
		children[i] = child
	}
	if !foundCurrent {
		c.logger.Warn("cannot determine aggregate apply state: current operation row missing from sibling set",
			"apply_id", apply.ApplyIdentifier, "apply_operation_id", operationID)
		return failClosed()
	}
	return state.DeriveRolloutApplyState(children), true
}

// executeApplySequential runs each DDL as a separate Spirit call (independent mode).
// Each table copies and cuts over independently.

// pollForCompletionAtomic polls the engine for progress in atomic mode (all tasks share state).
func (c *LocalClient) pollForCompletionAtomic(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, creds *engine.Credentials, resumeState *engine.ResumeState) {
	eng := c.getEngine()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	ps := &atomicPollState{lastProgressLog: time.Now()}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if done := c.handleAtomicProgressTick(ctx, eng, apply, tasks, creds, resumeState, ps); done {
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
func (c *LocalClient) handleAtomicProgressTick(ctx context.Context, eng engine.Engine, apply *storage.Apply, tasks []*storage.Task, creds *engine.Credentials, resumeState *engine.ResumeState, ps *atomicPollState) bool {
	if handled, err := c.processPendingStopControlRequest(ctx, apply); err != nil {
		c.logger.Warn("pending stop request processing failed; current apply owner will exit for operator retry",
			"apply_id", apply.ApplyIdentifier, "error", err)
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
			c.logger.Error("progress check failed with permanent error",
				"error", err, "apply_id", apply.ApplyIdentifier)
			c.failApplyWithTasks(ctx, apply, tasks, fmt.Sprintf("progress polling failed: %v", err))
			return true
		}
		ps.consecutiveErrors++
		c.logger.Warn("progress check failed",
			"error", err, "apply_id", apply.ApplyIdentifier, "consecutive_errors", ps.consecutiveErrors)
		if ps.consecutiveErrors >= 10 {
			if c.shouldRetryEngineError(err) {
				c.logger.Warn("progress polling failed repeatedly, pausing apply for operator retry",
					"apply_id", apply.ApplyIdentifier, "consecutive_errors", ps.consecutiveErrors)
				c.markApplyRetryableWithTasks(ctx, apply, tasks, fmt.Sprintf("progress polling failed after %d consecutive errors: %v", ps.consecutiveErrors, err))
				return true
			}
			c.logger.Error("progress polling failed repeatedly, failing apply",
				"apply_id", apply.ApplyIdentifier, "consecutive_errors", ps.consecutiveErrors)
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
		if c.config.Type == storage.DatabaseTypeVitess {
			if saveErr := c.saveEngineResumeState(ctx, tasks, resumeState); saveErr != nil {
				c.logger.Error("failed to save Vitess engine resume state from progress polling",
					"apply_id", apply.ApplyIdentifier, "error", saveErr)
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

	// Log progress every 10 seconds
	c.logAtomicProgress(ctx, apply, result, ps, now)

	// Update all tasks with engine progress
	c.syncAtomicTaskProgress(ctx, tasks, result, newState, now)
	if handled, err := c.processPendingStopControlRequest(ctx, apply); err != nil {
		c.logger.Warn("pending stop request processing failed after progress sync; current apply owner will exit for operator retry",
			"apply_id", apply.ApplyIdentifier, "error", err)
		return true
	} else if handled {
		return true
	}
	if err := c.processPendingCutoverControlRequest(ctx, apply); err != nil {
		c.logger.Warn("pending cutover request processing failed after progress sync; current apply owner will exit for operator retry",
			"apply_id", apply.ApplyIdentifier, "error", err)
		return true
	}

	opts := apply.GetOptions()
	controlReq := &engine.ControlRequest{
		Database:    apply.Database,
		Credentials: creds,
		ResumeState: resumeState,
	}

	// Auto-trigger deploy if waiting and not in defer-deploy mode
	if result.State == engine.StateWaitingForDeploy && !opts.DeferDeploy {
		c.logger.Info("auto-triggering deploy (not in defer-deploy mode)", "apply_id", apply.ApplyIdentifier)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventDeployTriggered, storage.LogSourceSchemaBot,
			"Auto-triggering deploy (defer_deploy not set)", "", "")
		if _, err := eng.Start(ctx, controlReq); err != nil {
			c.logger.Error("auto-deploy failed", "error", err, "apply_id", apply.ApplyIdentifier)
		}
	}

	// Auto-trigger cutover if waiting and not in defer mode
	if result.State == engine.StateWaitingForCutover && !opts.DeferCutover {
		c.logger.Info("auto-triggering cutover (not in defer mode)", "apply_id", apply.ApplyIdentifier)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventCutoverTriggered, storage.LogSourceSchemaBot,
			"Auto-triggering cutover (defer_cutover not set)", "", "")
		if _, err := eng.Cutover(ctx, controlReq); err != nil {
			c.logger.Error("auto-cutover failed", "error", err, "apply_id", apply.ApplyIdentifier)
		}
	}

	// Timeout: cancel the apply if waiting for manual deploy too long.
	if result.State == engine.StateWaitingForDeploy && opts.DeferDeploy &&
		!ps.stateEnteredAt.IsZero() && time.Since(ps.stateEnteredAt) > waitingForManualActionTimeout {
		c.logger.Info("waiting-for-deploy timed out, cancelling apply",
			"apply_id", apply.ApplyIdentifier, "timeout", waitingForManualActionTimeout)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			fmt.Sprintf("Waiting for deploy timed out after %s, cancelling", waitingForManualActionTimeout), "", "")
		if _, err := eng.Stop(ctx, controlReq); err != nil {
			c.logger.Error("timeout stop failed", "error", err, "apply_id", apply.ApplyIdentifier)
		}
	}

	// Timeout: cancel the apply if waiting for manual cutover too long.
	if result.State == engine.StateWaitingForCutover && opts.DeferCutover &&
		!ps.stateEnteredAt.IsZero() && time.Since(ps.stateEnteredAt) > waitingForManualActionTimeout {
		c.logger.Info("waiting-for-cutover timed out, cancelling apply",
			"apply_id", apply.ApplyIdentifier, "timeout", waitingForManualActionTimeout)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			fmt.Sprintf("Waiting for cutover timed out after %s, cancelling", waitingForManualActionTimeout), "", "")
		if _, err := eng.Stop(ctx, controlReq); err != nil {
			c.logger.Error("timeout stop failed", "error", err, "apply_id", apply.ApplyIdentifier)
		}
	}

	// If --skip-revert was set, auto-skip the revert window immediately.
	if result.State == engine.StateRevertWindow && opts.SkipRevert && !ps.revertSkipped {
		c.logger.Info("auto-skipping revert window (--skip-revert)",
			"apply_id", apply.ApplyIdentifier,
		)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			"Auto-skipping revert window (--skip-revert)", "", "")
		_, err := eng.SkipRevert(ctx, controlReq)
		if err != nil {
			c.logger.Error("auto-skip revert failed", "error", err, "apply_id", apply.ApplyIdentifier)
		} else {
			c.logger.Info("skip-revert triggered", "apply_id", apply.ApplyIdentifier, "reason", "--skip-revert")
			c.markRevertSkipped(ctx, apply)
		}
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventSkipRevertTriggered, storage.LogSourceSchemaBot,
			"Skip-revert triggered (--skip-revert)", state.Apply.RevertWindow, "")
		ps.revertSkipped = true
	}

	// Revert window enabled (default): auto-skip based on deployed_at + configured duration.
	// Falls back to stateEnteredAt if deployed_at is unavailable.
	if result.State == engine.StateRevertWindow && !opts.SkipRevert && !ps.revertSkipped {
		revertDeadline := c.revertWindowDeadline(result.ResumeState, ps.stateEnteredAt)
		if !revertDeadline.IsZero() && now.After(revertDeadline) {
			c.logger.Info("revert window expired, skipping", "apply_id", apply.ApplyIdentifier, "deadline", revertDeadline)
			c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
				"Revert window expired, finalizing", "", "")
			if _, err := eng.SkipRevert(ctx, controlReq); err != nil {
				c.logger.Error("revert window timeout skip failed", "error", err, "apply_id", apply.ApplyIdentifier)
			} else {
				c.markRevertSkipped(ctx, apply)
				c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventSkipRevertTriggered, storage.LogSourceSchemaBot,
					"Revert window expired, skip-revert triggered", state.Apply.RevertWindow, "")
			}
			ps.revertSkipped = true
		}
	}

	// Update apply state from persisted task state so recovery guards can keep
	// storage ahead of stale engine progress until Spirit reaches the cutover wait again.
	if derived, ok := c.deriveAggregateApplyState(ctx, apply, tasks); ok {
		apply.State = derived
	}
	apply.UpdatedAt = now
	freshApply, err := c.storage.Applies().Get(ctx, apply.ID)
	if err != nil {
		c.logger.Error("failed to reload apply before progress state update", "apply_id", apply.ApplyIdentifier, "error", err)
		return true
	}
	if freshApply == nil {
		c.logger.Warn("apply row missing before progress state update; yielding",
			"apply_id", apply.ApplyIdentifier, "apply_db_id", apply.ID)
		return true
	}
	if state.IsTerminalApplyState(freshApply.State) {
		c.logger.Info("apply already terminal in storage, not overwriting with stale progress state",
			"apply_id", apply.ApplyIdentifier,
			"stored_state", freshApply.State,
			"progress_state", apply.State)
		*apply = *freshApply
		if err := completePendingStopControlRequests(ctx, c.storage, apply); err != nil {
			c.logger.Warn("failed to complete pending stop request for terminal apply; current apply owner will exit for operator retry",
				"apply_id", apply.ApplyIdentifier, "error", err)
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
		swapped, err := c.storage.Applies().UpdateDerivedState(ctx, apply.ID, expectedState, apply.State, apply.ErrorMessage, apply.CompletedAt)
		if err != nil {
			c.logger.Error("failed to update apply state", "apply_id", apply.ApplyIdentifier, "state", apply.State, "error", err)
		} else if !swapped {
			// Another drive advanced the apply between our reload and write; it
			// owns the terminal transition and its side-effects. Skip ours.
			c.logger.Info("apply terminal-state write lost a race; yielding to the owning drive",
				"apply_id", apply.ApplyIdentifier, "expected_state", expectedState, "derived_state", apply.State)
			return true
		}
		if err := completePendingStopControlRequests(ctx, c.storage, apply); err != nil {
			c.logger.Warn("failed to complete pending stop request after terminal progress reconciliation; current apply owner will exit for operator retry",
				"apply_id", apply.ApplyIdentifier, "error", err)
			return true
		}
		metrics.AdjustActiveApplies(ctx, -1, apply.Database, apply.Deployment, apply.Environment)
		switch {
		case retryableFailure:
			c.logger.Warn("apply paused for operator retry",
				"mode", groupedApplyMode(apply), "apply_id", apply.ApplyIdentifier, "error", apply.ErrorMessage, "task_count", len(tasks))
		case state.IsState(apply.State, state.Apply.Failed):
			c.logger.Error("apply failed",
				"mode", groupedApplyMode(apply), "apply_id", apply.ApplyIdentifier, "error", apply.ErrorMessage, "task_count", len(tasks))
		default:
			c.logger.Info("apply completed",
				"mode", groupedApplyMode(apply), "apply_id", apply.ApplyIdentifier, "state", apply.State, "task_count", len(tasks))
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

	swapped, err := c.storage.Applies().UpdateDerivedState(ctx, apply.ID, expectedState, apply.State, apply.ErrorMessage, apply.CompletedAt)
	if err != nil {
		c.logger.Error("failed to update apply state", "apply_id", apply.ApplyIdentifier, "state", apply.State, "error", err)
	} else if !swapped {
		// Another drive advanced the apply between our reload and write; our
		// progress projection is stale. Skip the observer update and let the next
		// poll reconcile.
		c.logger.Info("apply progress-state write lost a race; skipping",
			"apply_id", apply.ApplyIdentifier, "expected_state", expectedState, "derived_state", apply.State)
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
	if state.IsTerminalApplyState(opState) || state.IsState(opState, state.Apply.FailedRetryable) {
		c.logger.Info("operation settled while apply continues; exiting operation drive",
			"mode", groupedApplyMode(apply), "apply_id", apply.ApplyIdentifier, "operation_state", opState, "apply_state", apply.State)
		return true
	}
	return false
}

// markRevertSkipped sets RevertSkippedAt on the VitessApplyData record so
// progress consumers know finalization is in progress.
func (c *LocalClient) markRevertSkipped(ctx context.Context, apply *storage.Apply) {
	now := time.Now()
	if vad, err := c.storage.VitessApplyData().GetByApplyID(ctx, apply.ID); err == nil {
		vad.RevertSkippedAt = &now
		if saveErr := c.storage.VitessApplyData().Save(ctx, vad); saveErr != nil {
			c.logger.Warn("failed to save revert_skipped_at", "apply_id", apply.ApplyIdentifier, "error", saveErr)
		}
	}
}

// revertWindowDuration returns the configured revert window duration, falling
// back to the engine default when none is set. The server writes a canonical,
// already-validated duration into metadata, so a malformed value only reaches
// here when an embedder populates metadata directly. Rather than silently
// using the default — which would hide a misconfigured revert window — an
// unparseable or non-positive value is surfaced via a warning before falling
// back, so the whole class of bad input is observable.
func (c *LocalClient) revertWindowDuration() time.Duration {
	s := c.config.Metadata["revert_window_duration"]
	if s == "" {
		return defaultRevertWindowDuration
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		c.logger.Warn("invalid revert_window_duration metadata; using engine default",
			"database", c.config.Database, "value", s, "default", defaultRevertWindowDuration, "error", err)
		return defaultRevertWindowDuration
	}
	if d <= 0 {
		c.logger.Warn("non-positive revert_window_duration metadata; using engine default",
			"database", c.config.Database, "value", s, "default", defaultRevertWindowDuration)
		return defaultRevertWindowDuration
	}
	return d
}

// revertWindowDeadline computes when the revert window expires.
// Uses deployed_at from engine metadata (accurate to PlanetScale's clock) plus
// the configured revert period. Falls back to stateEnteredAt if metadata is unavailable.
func (c *LocalClient) revertWindowDeadline(resumeState *engine.ResumeState, stateEnteredAt time.Time) time.Time {
	duration := c.revertWindowDuration()
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
func (c *LocalClient) syncAtomicTaskProgress(ctx context.Context, tasks []*storage.Task, result *engine.ProgressResult, newState string, now time.Time) {
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
		// VSchema tasks follow deploy-request-level state, not per-migration state.
		// They have no SHOW VITESS_MIGRATIONS rows. Their state transitions are:
		// pending → running (during in_progress_vschema) → completed/failed.
		if isVSchemaTask(task) {
			vsState := c.deriveVSchemaTaskState(task, result, newState, now)
			if vsState != task.State {
				msg := fmt.Sprintf("VSchema %s → %s", task.State, vsState)
				c.transitionTaskState(ctx, task, task.ApplyID, vsState, msg)
			}
			continue
		}

		if tp, ok := engineProgressForTask(tableProgress, task); ok {
			task.RowsCopied = tp.RowsCopied
			task.RowsTotal = tp.RowsTotal
			task.ProgressPercent = tp.Progress
			task.ETASeconds = int(tp.ETASeconds)
			task.IsInstant = tp.IsInstant
			if tp.StartedAt != nil && task.StartedAt == nil {
				task.StartedAt = tp.StartedAt
			}
			if tp.CompletedAt != nil && !retryableFailure && task.CompletedAt == nil {
				task.CompletedAt = tp.CompletedAt
			}
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
		c.transitionTaskState(ctx, task, 0, taskStateWithNoBackwardProgress(task.State, newState), "")
	}
}

// deriveVSchemaTaskState determines the state for a VSchema task based on
// the engine progress result. VSchema tasks have no per-migration rows in
// SHOW VITESS_MIGRATIONS — their state tracks the deploy request's VSchema
// application phase (in_progress_vschema).
func (c *LocalClient) deriveVSchemaTaskState(task *storage.Task, result *engine.ProgressResult, taskState string, now time.Time) string {
	if state.IsTerminalTaskState(task.State) {
		return task.State
	}

	switch {
	case state.IsState(taskState, state.Task.FailedRetryable):
		if task.ErrorMessage == "" {
			task.ErrorMessage = progressFailureMessage(result)
		}
		return state.Task.FailedRetryable
	case result.Message == engine.MessageApplyingVSchema:
		if task.StartedAt == nil {
			task.StartedAt = &now
		}
		return state.Task.Running
	case result.State == engine.StateFailed:
		if task.CompletedAt == nil {
			task.CompletedAt = &now
		}
		return state.Task.Failed
	case state.IsState(taskState, state.Task.RevertWindow):
		if task.CompletedAt == nil {
			task.CompletedAt = &now
		}
		return state.Task.RevertWindow
	case result.State.IsTerminal(), state.IsState(taskState, state.Task.Completed):
		if task.CompletedAt == nil {
			task.CompletedAt = &now
		}
		return state.Task.Completed
	default:
		return task.State
	}
}
