package tern

import (
	"context"
	"fmt"

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
