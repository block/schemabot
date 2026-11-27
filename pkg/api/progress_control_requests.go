package api

import (
	"context"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
)

// progressControlRequests lists the latest request of each control operation
// made against apply, pending ones included, so a caller that issued a
// command can tell whether it took effect: a cancel the engine declined
// settles as failed with the engine's reason while the change runs on. A
// failed read is logged and leaves the list empty; the rest of the progress
// answer does not depend on it.
func (s *Service) progressControlRequests(ctx context.Context, apply *storage.Apply) []apitypes.ControlRequestOutcome {
	control := s.storage.ControlRequests()
	if control == nil || apply == nil {
		return nil
	}
	pending, err := control.ListPending(ctx, apply.ID)
	if err != nil {
		s.logger.Warn("progress omits control request outcomes: failed to list pending control requests",
			append(apply.LogAttrs(), "error", err)...)
		return nil
	}
	settled, err := control.ListSettled(ctx, apply.ID)
	if err != nil {
		s.logger.Warn("progress omits control request outcomes: failed to list settled control requests",
			append(apply.LogAttrs(), "error", err)...)
		return nil
	}
	out := make([]apitypes.ControlRequestOutcome, 0, len(pending)+len(settled))
	for _, req := range append(pending, settled...) {
		outcome := apitypes.ControlRequestOutcome{
			Operation:   string(req.Operation),
			Status:      string(req.Status),
			Revision:    req.Revision,
			RequestedBy: req.RequestedBy,
		}
		if req.Status == storage.ControlRequestFailed {
			outcome.ErrorMessage = apply.OperatorFacingMessage(req.ErrorMessage)
		}
		if req.CompletedAt != nil && req.Status != storage.ControlRequestPending {
			outcome.SettledAt = req.CompletedAt.UTC().Format(time.RFC3339)
		}
		out = append(out, outcome)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
