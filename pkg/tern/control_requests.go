package tern

import (
	"context"
	"fmt"

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
	controlStore := store.ControlRequests()
	if controlStore == nil {
		return fmt.Errorf("control request store is not available")
	}
	if err := controlStore.FailPending(ctx, apply.ID, storage.ControlOperationStart, errorMessage); err != nil {
		return fmt.Errorf("fail pending start control request for apply %s: %w", apply.ApplyIdentifier, err)
	}
	return nil
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
	controlStore := store.ControlRequests()
	if controlStore == nil {
		return fmt.Errorf("control request store is not available")
	}
	if err := controlStore.CompletePending(ctx, apply.ID, storage.ControlOperationStop); err != nil {
		return fmt.Errorf("complete pending stop control request for apply %s: %w", apply.ApplyIdentifier, err)
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
