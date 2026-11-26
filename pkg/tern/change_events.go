package tern

import (
	"context"

	"github.com/block/schemabot/pkg/storage"
)

type changeDispatchKey struct{}

func withChangeDispatchApply(ctx context.Context, apply *storage.Apply) context.Context {
	return context.WithValue(ctx, changeDispatchKey{}, apply)
}

func (c *LocalClient) observeOperationDispatch(ctx context.Context) {
	recorder, ok := c.storage.(storage.OperationChangeRecorder)
	if !ok {
		return
	}
	if apply, ok := ctx.Value(changeDispatchKey{}).(*storage.Apply); ok && apply != nil {
		recorder.ObserveOperationDispatch(ctx, apply)
	}
}

const changeEventAppOption = "change_event_app"

func (c *GRPCClient) setChangeEventApp(options map[string]string, database string) {
	if c.changeEventApp == nil {
		return
	}
	if app := c.changeEventApp(database); app != "" {
		options[changeEventAppOption] = app
	}
}
