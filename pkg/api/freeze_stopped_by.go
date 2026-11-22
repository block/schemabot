package api

import (
	"context"
	"errors"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// StoppedByFreeze names the freeze that stopped apply: set only while apply is
// stopped and the stop recorded on it is the one a freeze with running "stop"
// issued (storage.FreezeStopCaller), at the freeze's stop sweep or at the
// claim checkpoint (AZ-11). It returns nil for an apply an operator stopped,
// and for one it cannot tell, which then reads as an ordinary stop.
//
// The stop names the freeze by its identifier. The reason is read from the
// freeze itself, and left empty when the freeze cannot be read, so status
// still says which freeze stopped the apply. Ended says the freeze has since
// ended or been lifted, so status tells the reader a plain start resumes it.
func (s *Service) StoppedByFreeze(ctx context.Context, apply *storage.Apply) *apitypes.FreezeStopRef {
	return StoppedByFreeze(ctx, s.storage, s.logger, apply)
}

// StoppedByFreeze is Service.StoppedByFreeze for a caller that holds the
// storage but not the service, such as the PR comment observer.
func StoppedByFreeze(ctx context.Context, stor storage.Storage, logger interface {
	Warn(msg string, args ...any)
}, apply *storage.Apply) *apitypes.FreezeStopRef {
	if apply == nil || !state.IsState(apply.State, state.Apply.Stopped) {
		return nil
	}
	control := stor.ControlRequests()
	if control == nil {
		return nil
	}
	stop, err := control.GetByOperation(ctx, apply.ID, storage.ControlOperationStop)
	if err != nil {
		logger.Warn("could not read the stop of a stopped apply; status does not say whether a freeze stopped it",
			append(apply.LogAttrs(), "error", err)...)
		return nil
	}
	if stop == nil {
		// A stopped apply always has the stop that stopped it.
		logger.Warn("a stopped apply has no stop recorded; status reads it as an ordinary stop", apply.LogAttrs()...)
		return nil
	}
	if stop.Status == storage.ControlRequestFailed {
		// The last stop failed, so it is not what stopped the apply.
		return nil
	}
	freezeID, ok := storage.FreezeFromStopCaller(stop.RequestedBy)
	if !ok {
		return nil
	}
	ref := &apitypes.FreezeStopRef{ID: freezeID}
	freezes := stor.Freezes()
	if freezes == nil {
		return ref
	}
	freeze, err := freezes.Get(ctx, freezeID)
	switch {
	case errors.Is(err, storage.ErrFreezeNotFound):
		logger.Warn("the freeze that stopped this apply no longer exists; status names it without its reason",
			append(apply.LogAttrs(), "freeze_id", freezeID)...)
	case err != nil:
		logger.Warn("could not read the freeze that stopped this apply; status names it without its reason",
			append(apply.LogAttrs(), "freeze_id", freezeID, "error", err)...)
	default:
		ref.Reason = freeze.Reason
		now, err := freezes.Now(ctx)
		if err != nil {
			logger.Warn("could not read the database clock; status reads the freeze that stopped this apply as still in effect",
				append(apply.LogAttrs(), "freeze_id", freezeID, "error", err)...)
			break
		}
		ref.Ended = !freeze.InEffectAt(now)
	}
	return ref
}
