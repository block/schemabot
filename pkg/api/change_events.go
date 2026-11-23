package api

import (
	"context"

	"github.com/block/schemabot/pkg/storage"
)

// SetChangeEvents enables operation lifecycle events. Call it before starting
// the service's operator.
func (s *Service) SetChangeEvents(publisher *storage.ChangeEvents) {
	s.changeEvents = publisher
}

// offerReapedApplyFinished runs only after a reaper commits its transition.
func (s *Service) offerReapedApplyFinished(ctx context.Context, apply *storage.Apply) {
	if s.changeEvents == nil {
		return
	}
	ops, err := s.storage.ApplyOperations().ListByApply(ctx, apply.ID)
	if err != nil {
		s.logger.Warn("could not load operation outcomes for change event", append(apply.LogAttrs(), "error", err)...)
		return
	}
	ctx = storage.WithApplyChangeEventSnapshot(ctx, apply, ops)
	s.changeEvents.OfferApplyFinished(ctx, apply, apply.State)
}

// ChangeEventApp returns the application registered for a managed database.
func (c *ServerConfig) ChangeEventApp(database string) string {
	if db := c.Database(database); db != nil {
		return db.App
	}
	return ""
}

func (s *Service) withOperationChangeEvents(ctx context.Context, apply *storage.Apply, op *storage.ApplyOperation) context.Context {
	if s.changeEvents == nil {
		return ctx
	}
	client, err := s.TernClient(op.Deployment, apply.Environment)
	if err != nil || client.IsRemote() {
		return ctx
	}
	return s.changeEvents.WithOperationChangeEvents(ctx, apply, op)
}
