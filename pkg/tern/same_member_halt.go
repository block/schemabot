package tern

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// sameMemberHaltCaller is the caller a drive names when it cancels its own
// work because another work row of the same rollout member failed. No
// operator asked for this cancel: the on_failure policy did.
const sameMemberHaltCaller = "schemabot (on_failure halt)"

// failedSiblingHaltingOperation returns the failed work sibling that halts the
// work row operationID, or nil when nothing halts it.
//
// Under halt, one failed shard ends its whole target's work: the claim gate
// keeps the target's unstarted shards from starting, and this is the half that
// reaches work a driver already started. The failure is stored state, so
// every drive of a sibling reads it again at each checkpoint, and a drive that
// takes over after a restart or a lost lease reaches the same answer. No
// in-memory signal carries it.
func failedSiblingHaltingOperation(ctx context.Context, store storage.Storage, apply *storage.Apply, operationID int64) (*storage.ApplyOperation, error) {
	operations := store.ApplyOperations()
	if operations == nil {
		return nil, fmt.Errorf("check apply %s operation %d for a failed sibling: apply operation store is not available", apply.ApplyIdentifier, operationID)
	}
	failed, err := operations.FailedSiblingHaltingOperation(ctx, apply.ID, operationID)
	if err != nil {
		return nil, fmt.Errorf("check apply %s operation %d for a failed sibling: %w", apply.ApplyIdentifier, operationID, err)
	}
	return failed, nil
}

// taskReachedCutover reports whether a task's change has begun to cut over,
// or already did. A halt stops copy work only: interrupting a swap part-way
// would leave the target in a state neither the old schema nor the new one
// describes, so a task this far along finishes and the rollout still settles
// failed on its sibling's failure.
func taskReachedCutover(taskState string) bool {
	return state.IsState(taskState, state.Task.CuttingOver, state.Task.RevertWindow, state.Task.Reverting, state.Task.Completed)
}

// firstTaskAtCutover returns the first task that reached cutover, or nil. It
// guards a drive whose tasks share one engine run: once any of them completed,
// the shared cutover ran for all of them, whatever their rows say yet.
func firstTaskAtCutover(tasks []*storage.Task) *storage.Task {
	for _, task := range tasks {
		if taskReachedCutover(task.State) {
			return task
		}
	}
	return nil
}

// firstTaskMidCutover returns the first task whose cutover is under way, or
// nil. It guards a drive that runs its tasks one at a time, each with its own
// cutover: a task that already completed keeps its change, and only the task
// being swapped right now holds the halt off.
func firstTaskMidCutover(tasks []*storage.Task) *storage.Task {
	for _, task := range tasks {
		if state.IsState(task.State, state.Task.CuttingOver, state.Task.RevertWindow, state.Task.Reverting) {
			return task
		}
	}
	return nil
}

// sameMemberHaltMessage is the apply log line a halted operation records, so
// the history says why work no operator stopped ended cancelled.
func sameMemberHaltMessage(halted, failed *storage.ApplyOperation) string {
	return fmt.Sprintf("Cancelled %s: %s failed, and on_failure halt stops the rest of the work on %s",
		operationLabel(halted), operationLabel(failed), failed.Target)
}

// operationLabel names an operation in an apply log line by its key, which
// carries the target and shard of a sharded rollout's work.
func operationLabel(op *storage.ApplyOperation) string {
	if op.OperationKey != "" {
		return op.OperationKey
	}
	return op.Target
}

// cancelForFailedSibling cancels the work an operation-scoped drive holds when
// another work row of the same rollout member has failed under halt, and
// reports whether the drive stands down. atCutover names the task that holds
// the halt off because its change is already being swapped, and
// loggedAtCutover keeps that case to one log line per drive.
//
// The drive cancels only its own operation's tasks, under its own lease, the
// way it consumes an operator's cancel. Each sibling's drive does the same at
// its own checkpoint. The apply row is the rollout projection's: failed
// outranks cancelled there, so the apply settles failed once every sibling
// has settled.
func (c *LocalClient) cancelForFailedSibling(ctx context.Context, logger *slog.Logger, eng engine.Engine, apply *storage.Apply, tasks []*storage.Task, atCutover func([]*storage.Task) *storage.Task, loggedAtCutover *bool) (standDown bool, err error) {
	opLease, operationScoped := operationLeaseOnlyDrive(ctx)
	if !operationScoped {
		logger.DebugContext(ctx, "drive holds the apply lease, so it drives the whole apply and has no sibling operation to halt it")
		return false, nil
	}
	failed, err := failedSiblingHaltingOperation(ctx, c.storage, apply, opLease.OperationID)
	if err != nil {
		return false, err
	}
	if failed == nil {
		logger.DebugContext(ctx, "no failed sibling of this operation's rollout member halts it", "apply_operation_id", opLease.OperationID)
		return false, nil
	}
	own := tasksForOperation(tasks, opLease.OperationID)
	attrs := []any{"apply_operation_id", opLease.OperationID, "failed_apply_operation_id", failed.ID, "failed_operation_key", failed.OperationKey}
	if task := atCutover(own); task != nil {
		if !*loggedAtCutover {
			logger.InfoContext(ctx, "a sibling of this operation failed under on_failure halt, but this operation's change already reached cutover; it finishes and the apply settles failed",
				append(attrs, "task_id", task.TaskIdentifier, "task_state", task.State)...)
			*loggedAtCutover = true
		}
		return false, nil
	}
	halted, err := c.storage.ApplyOperations().Get(ctx, opLease.OperationID)
	if err != nil {
		return false, fmt.Errorf("load apply %s operation %d to cancel it behind failed sibling %d: %w", apply.ApplyIdentifier, opLease.OperationID, failed.ID, err)
	}
	if halted == nil {
		return false, fmt.Errorf("load apply %s operation %d to cancel it behind failed sibling %d: %w", apply.ApplyIdentifier, opLease.OperationID, failed.ID, storage.ErrApplyOperationNotFound)
	}

	logger.WarnContext(ctx, "a sibling of this operation failed under on_failure halt; cancelling this operation's work", attrs...)
	// The cancel outlives the drive's context: once the engine has been told
	// to cancel, the tasks must be recorded cancelled even if the drive is
	// being shut down, or a later drive would read the cancelled run's end as
	// this operation's own outcome.
	cancelCtx := context.WithoutCancel(ctx)
	if err := c.cancelEngineForTasks(cancelCtx, eng, own, apply.ID); err != nil {
		if engine.IsAlreadyCompleted(err) {
			logger.InfoContext(ctx, "this operation's change completed before the halt reached it; the drive records the completion and the apply settles failed",
				append(attrs, "error", err)...)
			return false, nil
		}
		if engine.IsSettledOutcome(err) {
			// The engine's run already ended on its own, typically failed.
			// Recording it cancelled would hide that outcome (CO-3), so the
			// drive's next poll records what the engine reports instead.
			logger.InfoContext(ctx, "this operation's change already settled on the engine before the halt reached it; the drive records the engine's outcome and the apply settles failed",
				append(attrs, "error", err)...)
			return false, nil
		}
		return false, fmt.Errorf("cancel apply %s operation %d behind failed sibling %d: %w", apply.ApplyIdentifier, opLease.OperationID, failed.ID, err)
	}
	if err := c.releaseCancelledArtifacts(cancelCtx, eng, apply, own); err != nil {
		c.logSkippedArtifactRelease(cancelCtx, apply, cancelledRelease, err)
	}
	if err := c.reclaimCancelledResumeState(cancelCtx, eng, apply, own); err != nil {
		logger.WarnContext(ctx, "halted operation's stored engine state could not be fully reclaimed; the apply log names what was left in place",
			append(attrs, "error", err)...)
	}
	cancelled, skipped, _, err := c.markTasksWithState(cancelCtx, own, apply.ID, StatementIndex[engine.TableProgress]{}, state.Task.Cancelled)
	if err != nil {
		// The engine work is cancelled but some task rows did not record it.
		// The drive stands down without settling the operation, so the row
		// stays claimable and the next drive records the rest.
		return true, fmt.Errorf("%w: record apply %s operation %d cancelled behind failed sibling %d: %w", ErrTaskVerdictRefused, apply.ApplyIdentifier, opLease.OperationID, failed.ID, err)
	}
	c.logApplyEvent(cancelCtx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventInfo, storage.LogSourceSchemaBot,
		sameMemberHaltMessage(halted, failed), "", "")
	c.leaveApplyRowToProjection(apply, opLease, state.Task.Cancelled)
	logger.InfoContext(ctx, "cancelled this operation's work behind a failed sibling; the rollout projection settles the apply",
		append(attrs, "cancelled_tasks", cancelled, "skipped_tasks", skipped)...)
	return true, nil
}

// cancelRemoteForFailedSibling asks the data plane to cancel the remote apply
// an operation-scoped drive polls when another work row of the same rollout
// member has failed under halt. The drive keeps polling afterwards, so the
// remote's cancelled outcome reaches this operation's row the way any other
// remote outcome does, and the apply settles failed once every sibling has
// settled.
//
// The data plane records the cancel durably on first receipt. A send is
// repeated only on the bounded resend cadence, as insurance against a lost
// answer, so a drive that polls for a while does not flood the data plane with
// duplicates.
func (c *GRPCClient) cancelRemoteForFailedSibling(ctx context.Context, apply *storage.Apply, scope applyTaskScope, sentAt *time.Time) error {
	logger := c.applyLogger(apply)
	if !scope.isOperationScoped() {
		logger.DebugContext(ctx, "drive polls the whole apply, so it has no sibling operation to halt it")
		return nil
	}
	failed, err := failedSiblingHaltingOperation(ctx, c.storage, apply, scope.applyOperationID)
	if err != nil {
		return err
	}
	if failed == nil {
		logger.DebugContext(ctx, "no failed sibling of this operation's rollout member halts it", "apply_operation_id", scope.applyOperationID)
		return nil
	}
	now := time.Now()
	if !sentAt.IsZero() && now.Sub(*sentAt) < remoteControlResendInterval {
		logger.DebugContext(ctx, "halt cancel already sent to the data plane; waiting for the remote to settle before sending it again",
			"apply_operation_id", scope.applyOperationID, "sent_at", *sentAt)
		return nil
	}
	attrs := append(apply.MutableLogAttrs(), "apply_operation_id", scope.applyOperationID, "failed_apply_operation_id", failed.ID, "failed_operation_key", failed.OperationKey)
	remoteID := scope.remoteApplyID(apply)
	if remoteID == "" {
		// The claim gate refuses to start work behind a failed sibling, so
		// a drive with nothing dispatched has nothing running to cancel.
		logger.InfoContext(ctx, "a sibling of this operation failed under on_failure halt, but nothing of this operation was dispatched to the data plane to cancel", attrs...)
		return nil
	}
	if scope.sharesRemoteApplyWithSiblings() {
		// The data plane cancels a remote apply as a whole, and this one also
		// carries the deployment's other operations, other rollout members
		// among them. ST-10 lets a halt cancel nothing outside its own member,
		// so the drive sends no cancel: this operation's remote work runs on,
		// and the apply still settles failed on the sibling's failure.
		if sentAt.IsZero() {
			logger.WarnContext(ctx, "a sibling of this operation failed under on_failure halt, but its remote apply is shared with other operations of the deployment; no cancel is sent, the work runs on, and the apply settles failed",
				append(attrs, "remote_apply_id", remoteID, "deployment_operation_keys", scope.deploymentOperationKeys)...)
		}
		*sentAt = now
		return nil
	}
	tasks, err := c.loadApplyTasks(ctx, apply, scope)
	if err != nil {
		return fmt.Errorf("load tasks of apply %s operation %d to cancel it behind failed sibling %d: %w", apply.ApplyIdentifier, scope.applyOperationID, failed.ID, err)
	}
	if task := firstTaskAtCutover(tasks); task != nil {
		if sentAt.IsZero() {
			logger.InfoContext(ctx, "a sibling of this operation failed under on_failure halt, but this operation's change already reached cutover; it finishes and the apply settles failed",
				append(attrs, "task_id", task.TaskIdentifier, "task_state", task.State)...)
		}
		*sentAt = now
		return nil
	}
	resp, err := c.client.Cancel(ctx, &ternv1.CancelRequest{ApplyId: remoteID, Environment: apply.Environment, Caller: sameMemberHaltCaller})
	if err != nil {
		// The data plane answers with an error once the remote has settled,
		// and a transient failure looks the same from here. Either way the
		// poll keeps reading the remote's own state, so a settled remote
		// reaches this operation's row, and a running one gets the cancel
		// again on the resend cadence.
		logger.WarnContext(ctx, "halt cancel request to the data plane failed; the drive keeps polling and sends it again on the resend cadence",
			append(attrs, "remote_apply_id", remoteID, "error", err)...)
		*sentAt = now
		return nil
	}
	if resp == nil {
		return fmt.Errorf("request remote cancel of apply %s operation %d remote %s behind failed sibling %d: the data plane returned neither a response nor an error", apply.ApplyIdentifier, scope.applyOperationID, remoteID, failed.ID)
	}
	if !resp.Accepted {
		// The data plane refuses a cancel once the remote has cut over or
		// settled. Its own outcome then reaches this operation through the
		// poll, so the refusal is recorded and the drive keeps polling.
		logger.WarnContext(ctx, "data plane refused the halt cancel; the drive keeps polling for the remote's own outcome",
			append(attrs, "remote_apply_id", remoteID, "error_message", resp.ErrorMessage)...)
		*sentAt = now
		return nil
	}
	if sentAt.IsZero() {
		halted := scope.operation
		if halted == nil {
			halted = &storage.ApplyOperation{ID: scope.applyOperationID, Target: failed.Target}
		}
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventInfo, sameMemberHaltMessage(halted, failed), "", "")
	}
	logger.WarnContext(ctx, "a sibling of this operation failed under on_failure halt; the data plane accepted the cancel of this operation's remote apply",
		append(attrs, "remote_apply_id", remoteID)...)
	*sentAt = now
	return nil
}
