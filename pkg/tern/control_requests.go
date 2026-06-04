package tern

import (
	"context"
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func pendingStartControlRequest(ctx context.Context, store storage.Storage, apply *storage.Apply) (*storage.ApplyControlRequest, error) {
	if store == nil {
		return nil, fmt.Errorf("storage is not available")
	}
	controlStore := store.ControlRequests()
	if controlStore == nil {
		return nil, fmt.Errorf("control request store is not available")
	}
	controlReq, err := controlStore.GetPending(ctx, apply.ID, storage.ControlOperationStart)
	if err != nil {
		return nil, fmt.Errorf("load pending start control request for apply %s: %w", apply.ApplyIdentifier, err)
	}
	return controlReq, nil
}

func completePendingStartControlRequests(ctx context.Context, store storage.Storage, apply *storage.Apply) error {
	if store == nil {
		return fmt.Errorf("storage is not available")
	}
	if err := ensureApplyLeaseForControlRequest(ctx, store, apply, storage.ControlOperationStart); err != nil {
		return err
	}
	controlStore := store.ControlRequests()
	if controlStore == nil {
		return fmt.Errorf("control request store is not available")
	}
	if err := controlStore.CompletePending(ctx, apply.ID, storage.ControlOperationStart); err != nil {
		return fmt.Errorf("complete pending start control request for apply %s: %w", apply.ApplyIdentifier, err)
	}
	return nil
}

func failPendingStartControlRequests(ctx context.Context, store storage.Storage, apply *storage.Apply, errorMessage string) error {
	if store == nil {
		return fmt.Errorf("storage is not available")
	}
	if err := ensureApplyLeaseForControlRequest(ctx, store, apply, storage.ControlOperationStart); err != nil {
		return err
	}
	controlStore := store.ControlRequests()
	if controlStore == nil {
		return fmt.Errorf("control request store is not available")
	}
	if err := controlStore.FailPending(ctx, apply.ID, storage.ControlOperationStart, errorMessage); err != nil {
		return fmt.Errorf("fail pending start control request for apply %s: %w", apply.ApplyIdentifier, err)
	}
	return nil
}

func pendingCutoverControlRequest(ctx context.Context, store storage.Storage, apply *storage.Apply) (*storage.ApplyControlRequest, error) {
	if store == nil {
		return nil, fmt.Errorf("storage is not available")
	}
	controlStore := store.ControlRequests()
	if controlStore == nil {
		return nil, fmt.Errorf("control request store is not available")
	}
	controlReq, err := controlStore.GetPending(ctx, apply.ID, storage.ControlOperationCutover)
	if err != nil {
		return nil, fmt.Errorf("load pending cutover control request for apply %s: %w", apply.ApplyIdentifier, err)
	}
	return controlReq, nil
}

func completePendingCutoverControlRequests(ctx context.Context, store storage.Storage, apply *storage.Apply) error {
	if store == nil {
		return fmt.Errorf("storage is not available")
	}
	if err := ensureApplyLeaseForControlRequest(ctx, store, apply, storage.ControlOperationCutover); err != nil {
		return err
	}
	controlStore := store.ControlRequests()
	if controlStore == nil {
		return fmt.Errorf("control request store is not available")
	}
	if err := controlStore.CompletePending(ctx, apply.ID, storage.ControlOperationCutover); err != nil {
		return fmt.Errorf("complete pending cutover control request for apply %s: %w", apply.ApplyIdentifier, err)
	}
	return nil
}

func failPendingCutoverControlRequests(ctx context.Context, store storage.Storage, apply *storage.Apply, errorMessage string) error {
	if store == nil {
		return fmt.Errorf("storage is not available")
	}
	if err := ensureApplyLeaseForControlRequest(ctx, store, apply, storage.ControlOperationCutover); err != nil {
		return err
	}
	controlStore := store.ControlRequests()
	if controlStore == nil {
		return fmt.Errorf("control request store is not available")
	}
	if err := controlStore.FailPending(ctx, apply.ID, storage.ControlOperationCutover, errorMessage); err != nil {
		return fmt.Errorf("fail pending cutover control request for apply %s: %w", apply.ApplyIdentifier, err)
	}
	return nil
}

func markApplyCuttingOverForControlRequest(ctx context.Context, store storage.Storage, apply *storage.Apply) error {
	if !state.IsState(apply.State, state.Apply.WaitingForCutover, state.Apply.Running) {
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
	if !state.IsState(apply.State, state.Apply.Running) {
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

func pendingStopControlRequest(ctx context.Context, store storage.Storage, apply *storage.Apply) (*storage.ApplyControlRequest, error) {
	if store == nil {
		return nil, fmt.Errorf("storage is not available")
	}
	controlStore := store.ControlRequests()
	if controlStore == nil {
		return nil, fmt.Errorf("control request store is not available")
	}
	controlReq, err := controlStore.GetPending(ctx, apply.ID, storage.ControlOperationStop)
	if err != nil {
		return nil, fmt.Errorf("load pending stop control request for apply %s: %w", apply.ApplyIdentifier, err)
	}
	return controlReq, nil
}

func completePendingStopControlRequests(ctx context.Context, store storage.Storage, apply *storage.Apply) error {
	if store == nil {
		return fmt.Errorf("storage is not available")
	}
	if err := ensureApplyLeaseForControlRequest(ctx, store, apply, storage.ControlOperationStop); err != nil {
		return err
	}
	controlStore := store.ControlRequests()
	if controlStore == nil {
		return fmt.Errorf("control request store is not available")
	}
	if err := controlStore.CompletePending(ctx, apply.ID, storage.ControlOperationStop); err != nil {
		return fmt.Errorf("complete pending stop control request for apply %s: %w", apply.ApplyIdentifier, err)
	}
	return nil
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

func completePendingStopIfStoredApplyResolved(ctx context.Context, store storage.Storage, apply *storage.Apply) (bool, error) {
	if store == nil {
		return false, fmt.Errorf("storage is not available")
	}
	storedApply, err := store.Applies().Get(ctx, apply.ID)
	if err != nil {
		return false, fmt.Errorf("load apply %s before completing pending stop: %w", apply.ApplyIdentifier, err)
	}
	if storedApply == nil {
		return false, fmt.Errorf("load apply %s before completing pending stop: %w", apply.ApplyIdentifier, storage.ErrApplyNotFound)
	}
	if !state.IsTerminalApplyState(storedApply.State) {
		return false, nil
	}
	if err := completePendingStopControlRequests(ctx, store, storedApply); err != nil {
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

func callerApplyLogSuffix(caller string) string {
	if caller == "" {
		caller = "unknown"
	}
	return fmt.Sprintf(" (caller: %s)", caller)
}
