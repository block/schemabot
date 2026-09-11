package tern

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// pendingControlRequest loads the pending control request of the given operation
// for an apply, returning nil when none is pending.
func pendingControlRequest(ctx context.Context, store storage.Storage, apply *storage.Apply, operation storage.ControlOperation) (*storage.ApplyControlRequest, error) {
	if store == nil {
		return nil, fmt.Errorf("storage is not available")
	}
	controlStore := store.ControlRequests()
	if controlStore == nil {
		return nil, fmt.Errorf("control request store is not available")
	}
	controlReq, err := controlStore.GetPending(ctx, apply.ID, operation)
	if err != nil {
		return nil, fmt.Errorf("load pending %s control request for apply %s: %w", operation, apply.ApplyIdentifier, err)
	}
	return controlReq, nil
}

// completePendingControlRequests marks the pending control request of the given
// operation completed, after verifying the apply lease still holds.
func completePendingControlRequests(ctx context.Context, store storage.Storage, apply *storage.Apply, operation storage.ControlOperation) error {
	if store == nil {
		return fmt.Errorf("storage is not available")
	}
	if err := ensureApplyLeaseForControlRequest(ctx, store, apply, operation); err != nil {
		return err
	}
	controlStore := store.ControlRequests()
	if controlStore == nil {
		return fmt.Errorf("control request store is not available")
	}
	if err := controlStore.CompletePending(ctx, apply.ID, operation); err != nil {
		return fmt.Errorf("complete pending %s control request for apply %s: %w", operation, apply.ApplyIdentifier, err)
	}
	return nil
}

// settlePendingRequestsForTerminalApply settles the pending control requests
// that a terminal apply moots: a pending stop is settled (the apply can no
// longer be stopped), and a pending revert or skip-revert can no longer act
// because the revert window is gone. Sweeping the mooted requests keeps a
// request issued moments before the apply settled — or one that lost to a
// contradictory command — from lingering pending forever.
//
// A pending cancel is mooted by every terminal state except stopped, since a
// stopped apply remains cancellable and its pending cancel stays deliverable
// for the next drive. The cancel is settled rather than completed outright: the
// terminal state decides whether the operator's command took effect.
func settlePendingRequestsForTerminalApply(ctx context.Context, store storage.Storage, logger *slog.Logger, apply *storage.Apply) error {
	ops := []storage.ControlOperation{
		storage.ControlOperationStop,
		storage.ControlOperationRevert,
		storage.ControlOperationSkipRevert,
	}
	// A pending request for a retired operation can only be a row written by a
	// previous release; no driver services it anymore, so this sweep is its
	// only settlement path.
	ops = append(ops, storage.RetiredControlOperations()...)
	for _, op := range ops {
		if err := completePendingControlRequests(ctx, store, apply, op); err != nil {
			return err
		}
	}
	return SettlePendingCancelForResolvedApply(ctx, store, logger, apply)
}

// SettlePendingCancelForResolvedApply settles the pending cancel of an apply
// that has reached a terminal state, whichever path drove it there. An apply
// resolves under more than one drive shape, and every one of them owes the
// operator the same answer about the command they issued, so they share this
// entry point rather than each deciding for itself what a settled cancel means.
//
// The caller must have established that the apply is terminal.
func SettlePendingCancelForResolvedApply(ctx context.Context, store storage.Storage, logger *slog.Logger, apply *storage.Apply) error {
	if state.IsState(apply.State, state.Apply.Stopped) {
		// A stopped apply remains cancellable, so the request stays deliverable
		// for the drive that resumes it.
		logger.DebugContext(ctx, "leaving the pending cancel request deliverable for a stopped apply", apply.LogAttrs()...)
		return nil
	}
	return settlePendingCancelForTerminalApply(ctx, store, logger, apply)
}

// settlePendingCancelForTerminalApply settles an apply's pending cancel once the
// apply has reached a terminal state, and discloses which outcome the operator
// got. A cancel the apply's own terminal state answered completes. A cancel the
// schema change outran fails with that reason, so the surfaces an operator reads
// — the PR comment's command-not-applied notice, the apply history, and the
// answer a re-issued cancel gets back — say the command did not take effect
// instead of reporting it applied.
//
// The caller must have established that the apply is terminal.
//
// The pending request is read here rather than taken from the caller, because
// the settle is a conditional update on a still-pending row and a zero-row
// write reports success: a caller acting on a copy it read before another path
// settled the same request would otherwise append a second disclosure
// contradicting the outcome already recorded.
func settlePendingCancelForTerminalApply(ctx context.Context, store storage.Storage, logger *slog.Logger, apply *storage.Apply) error {
	controlReq, err := pendingControlRequest(ctx, store, apply, storage.ControlOperationCancel)
	if err != nil {
		return err
	}
	if controlReq == nil {
		logger.DebugContext(ctx, "no pending cancel request to settle; leaving the recorded outcome as found", apply.LogAttrs()...)
		return nil
	}
	reason := cancelOutrunReason(apply.State)
	if reason == "" {
		if err := completePendingControlRequests(ctx, store, apply, storage.ControlOperationCancel); err != nil {
			return err
		}
	} else if err := failPendingControlRequests(ctx, store, apply, storage.ControlOperationCancel, reason); err != nil {
		return err
	}
	discloseSettledTerminalCancel(ctx, store, logger, apply, controlReq, reason)
	return nil
}

// cancelOutrunReason names what overtook an accepted cancel on an apply that
// reached applyState, and returns "" when nothing did. It is read by an operator
// looking for the effect of a command they issued, so it says what happened to
// the schema change rather than reporting a rejection. The state is normalized
// before it reaches the operator, so a caller holding a proto-form state does
// not put SchemaBot's internal spelling of it in a PR comment.
func cancelOutrunReason(applyState string) string {
	switch {
	case state.IsState(applyState, state.Apply.Cancelled):
		// The cancel is what settled the apply.
		return ""
	case state.IsState(applyState, state.Apply.Stopped):
		// A stopped apply remains cancellable, so nothing has outrun the command:
		// either it took effect on work that was already stopped, or a later drive
		// still has one to deliver.
		return ""
	case state.IsState(applyState, state.Apply.Completed):
		return "the schema change completed before the cancel could take effect; the change is live on the target"
	default:
		return fmt.Sprintf("the schema change reached %s before the cancel could take effect", state.NormalizeState(applyState))
	}
}

// discloseSettledTerminalCancel records how a terminal apply settled its pending
// cancel, in the server log and in the apply history. An empty reason is the
// cancel having taken effect; otherwise the entries disclose that it did not, so
// the history does not show an accepted cancel followed by a different terminal
// state with no explanation.
//
// Best-effort: the request is already settled, so a failure to append the apply
// event is logged rather than returned. Retrying the settlement would find no
// pending cancel and would never write the event.
func discloseSettledTerminalCancel(ctx context.Context, store storage.Storage, logger *slog.Logger, apply *storage.Apply, controlReq *storage.ApplyControlRequest, reason string) {
	caller := controlRequestCaller(controlReq)
	var level, message string
	if reason == "" {
		level = storage.LogLevelInfo
		message = fmt.Sprintf("Pending cancel request completed for %s apply%s", state.NormalizeState(apply.State), callerApplyLogSuffix(caller))
		logger.InfoContext(ctx, "completing pending cancel request for terminal apply",
			append(apply.LogAttrs(), "requested_by", caller)...)
	} else {
		level = storage.LogLevelWarn
		message = fmt.Sprintf("Cancel did not take effect: %s%s", reason, callerApplyLogSuffix(caller))
		logger.WarnContext(ctx, "failing pending cancel request: the schema change settled before the cancel could take effect",
			append(apply.LogAttrs(), "requested_by", caller, "reason", reason)...)
	}
	logStore := store.ApplyLogs()
	if logStore == nil {
		logger.ErrorContext(ctx, "apply log store is not available; the settled cancel will not appear in the apply history",
			append(apply.LogAttrs(), "event_message", message)...)
		return
	}
	entry := &storage.ApplyLog{
		ApplyID:   apply.ID,
		Level:     level,
		EventType: storage.LogEventCancelRequested,
		Source:    storage.LogSourceSchemaBot,
		Message:   message,
		CreatedAt: time.Now(),
	}
	if err := logStore.Append(ctx, entry); err != nil {
		logger.WarnContext(ctx, "failed to append the settled-cancel apply event; the rejection remains in the control request and the server log",
			append(apply.LogAttrs(), "error", err, "event_message", message)...)
	}
}

// failPendingControlRequests marks the pending control request of the given
// operation terminally failed. A failed request is no longer pending, so the
// operator-owned retry loop stops re-running the operation instead of spinning
// on a permanent rejection.
//
// The stored message is rewritten for the operator before it is persisted, so a
// rejection reads the same whether it arrived on the API's immediate attempt or
// on this retry — the durable record must not spell the schema change
// differently depending on which path reached the data plane. remoteIDs are the
// remote identifiers the caller addressed; a remote caller passes the id it sent
// the RPC to, and a local caller passes none because its identifiers are already
// operator-facing.
func failPendingControlRequests(ctx context.Context, store storage.Storage, apply *storage.Apply, operation storage.ControlOperation, errorMessage string, remoteIDs ...string) error {
	if store == nil {
		return fmt.Errorf("storage is not available")
	}
	errorMessage = apply.OperatorFacingMessage(errorMessage, remoteIDs...)
	if err := ensureApplyLeaseForControlRequest(ctx, store, apply, operation); err != nil {
		return err
	}
	controlStore := store.ControlRequests()
	if controlStore == nil {
		return fmt.Errorf("control request store is not available")
	}
	if err := controlStore.FailPending(ctx, apply.ID, operation, errorMessage); err != nil {
		return fmt.Errorf("fail pending %s control request for apply %s: %w", operation, apply.ApplyIdentifier, err)
	}
	return nil
}

// controlRefusalMessage renders the reason the executing side gave for refusing
// a control request. A refusal that arrives without a reason still has to read
// as an answer to the operator's command rather than a bare "not accepted", so
// the fallback names the operation that was refused.
func controlRefusalMessage(operation storage.ControlOperation, errorMessage string) string {
	if errorMessage == "" {
		return fmt.Sprintf("%s was refused with no reason given", operation)
	}
	return errorMessage
}

func markApplyCuttingOverForControlRequest(ctx context.Context, store storage.Storage, apply *storage.Apply, logger *slog.Logger) error {
	if !state.IsState(apply.State, state.Apply.WaitingForCutover) && !state.IsRunningApplyState(apply.State) {
		return nil
	}
	if store == nil {
		return fmt.Errorf("storage is not available")
	}
	applyStore := store.Applies()
	if applyStore == nil {
		return fmt.Errorf("apply store is not available")
	}
	previous := *apply
	now := time.Now()
	apply.State = state.Apply.CuttingOver
	apply.UpdatedAt = now
	// A multi-operation drive owns only its operation: the parent cutting_over
	// write is the operator's projection to make — a direct write here fails
	// closed under the operation-only lease and would block a cutover the
	// engine is ready to accept. The in-memory transition still stands so this
	// drive proceeds to dispatch the cutover.
	if suppressParentApplyWrites(ctx) {
		logger.Info("pending cutover request accepted under operation lease; parent cutting_over state is the operator's projection",
			"state", apply.State)
		return nil
	}
	if err := applyStore.Update(ctx, apply); err != nil {
		*apply = previous
		return fmt.Errorf("mark apply %s cutting over for pending cutover request: %w", apply.ApplyIdentifier, err)
	}
	return nil
}

func applyReadyForCutoverRequest(ctx context.Context, store storage.Storage, apply *storage.Apply) (bool, error) {
	if state.IsState(apply.State, state.Apply.WaitingForCutover, state.Apply.CuttingOver) {
		return true, nil
	}
	if !state.IsRunningApplyState(apply.State) {
		return false, nil
	}
	if store == nil {
		return false, fmt.Errorf("storage is not available")
	}
	taskStore := store.Tasks()
	if taskStore == nil {
		return false, fmt.Errorf("task store is not available")
	}
	tasks, err := taskStore.GetByApplyID(ctx, apply.ID)
	if err != nil {
		return false, fmt.Errorf("load tasks for apply %s before cutover request: %w", apply.ApplyIdentifier, err)
	}
	for _, task := range tasks {
		if state.IsState(task.State, state.Task.WaitingForCutover, state.Task.CuttingOver) {
			return true, nil
		}
	}
	return false, nil
}

func cutoverRequestResolvedByApplyState(applyState string) bool {
	return state.IsState(applyState, state.Apply.RevertWindow, state.Apply.Completed, state.Apply.Reverted)
}

func cutoverRequestFailedByApplyState(applyState string) bool {
	return state.IsTerminalApplyState(applyState) && !cutoverRequestResolvedByApplyState(applyState)
}

func ensureApplyLeaseForControlRequest(ctx context.Context, store storage.Storage, apply *storage.Apply, operation storage.ControlOperation) error {
	lease, ok := storage.ApplyLeaseFromContext(ctx)
	if !ok {
		return nil
	}
	if apply == nil {
		return fmt.Errorf("cannot complete %s control request without apply: %w", operation, storage.ErrApplyLeaseLost)
	}
	if !lease.Valid() {
		return fmt.Errorf("invalid apply lease before completing %s control request for apply %s (%d): %w", operation, apply.ApplyIdentifier, apply.ID, storage.ErrApplyLeaseLost)
	}
	if lease.ApplyID != apply.ID {
		return fmt.Errorf("apply lease for apply %d cannot complete %s control request for apply %s (%d): %w", lease.ApplyID, operation, apply.ApplyIdentifier, apply.ID, storage.ErrApplyLeaseLost)
	}
	if err := store.Applies().CheckLease(ctx, lease); err != nil {
		return fmt.Errorf("check apply lease before completing %s control request for apply %s: %w", operation, apply.ApplyIdentifier, err)
	}
	return nil
}

// settlePendingCancelIfStoredApplyResolved is the cancel counterpart of
// completePendingRequestIfStoredApplyResolved: it reloads the apply and, once
// the stored row is terminal, settles the pending cancel against the state the
// apply actually reached. A cancel the engine rejected as already completed
// settles the apply completed, and this is where that outcome resolves the
// operator's command — as one that did not take effect, not as one applied.
func settlePendingCancelIfStoredApplyResolved(ctx context.Context, store storage.Storage, logger *slog.Logger, apply *storage.Apply) (bool, error) {
	if store == nil {
		return false, fmt.Errorf("storage is not available")
	}
	storedApply, err := store.Applies().Get(ctx, apply.ID)
	if err != nil {
		return false, fmt.Errorf("load apply %s before settling pending cancel: %w", apply.ApplyIdentifier, err)
	}
	if storedApply == nil {
		return false, fmt.Errorf("load apply %s before settling pending cancel: %w", apply.ApplyIdentifier, storage.ErrApplyNotFound)
	}
	if !state.IsTerminalApplyState(storedApply.State) {
		return false, nil
	}
	if err := settlePendingCancelForTerminalApply(ctx, store, logger, storedApply); err != nil {
		return false, err
	}
	*apply = *storedApply
	return true, nil
}

// completePendingRequestIfStoredApplyResolved reloads the apply and, when its
// stored state is terminal, completes the operation's pending control requests
// and refreshes the caller's copy of the apply. It returns false without error
// when the stored apply has not resolved yet — the caller decides whether that
// means the request stays pending (a settle deferred to the apply-state
// projection) or the consumption failed.
func completePendingRequestIfStoredApplyResolved(ctx context.Context, store storage.Storage, apply *storage.Apply, operation storage.ControlOperation) (bool, error) {
	if store == nil {
		return false, fmt.Errorf("storage is not available")
	}
	storedApply, err := store.Applies().Get(ctx, apply.ID)
	if err != nil {
		return false, fmt.Errorf("load apply %s before completing pending %s: %w", apply.ApplyIdentifier, operation, err)
	}
	if storedApply == nil {
		return false, fmt.Errorf("load apply %s before completing pending %s: %w", apply.ApplyIdentifier, operation, storage.ErrApplyNotFound)
	}
	if !state.IsTerminalApplyState(storedApply.State) {
		return false, nil
	}
	if err := completePendingControlRequests(ctx, store, storedApply, operation); err != nil {
		return false, err
	}
	*apply = *storedApply
	return true, nil
}

func controlRequestCaller(req *storage.ApplyControlRequest) string {
	if req == nil || req.RequestedBy == "" {
		return "unknown"
	}
	return req.RequestedBy
}

// controlRequestRequester names the requester to record on a durable control
// request. An empty caller means the command reached this plane without an
// operator identity — an internal resume, or a plane that predates the caller
// field — so the row names the path it arrived on rather than inventing a
// person.
func controlRequestRequester(caller string) string {
	if caller == "" {
		return storage.ForwardingControlRequestCaller
	}
	return caller
}

func callerApplyLogSuffix(caller string) string {
	return fmt.Sprintf(" (caller: %s)", storage.ApplyLogCaller(caller))
}
