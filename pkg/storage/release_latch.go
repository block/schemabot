package storage

import (
	"context"
	"slices"
)

// AnyPauseOnFailure reports whether any operation uses the on_failure=pause
// policy — the only policy whose projection depends on whether an operator has
// released the rollout. It gates the release-latch read so an apply without a
// pause operation never pays the read or risks an unrelated latch read error.
func AnyPauseOnFailure(ops []*ApplyOperation) bool {
	return slices.ContainsFunc(ops, func(op *ApplyOperation) bool {
		return op.OnFailure == OnFailurePause
	})
}

// ReleaseLatched reports whether a paused rollout has been released open,
// resolving the boundary that the rollout projection and every presentation
// surface gate on so they agree with what the operator will actually claim
// next. It reads the release control request only when some operation uses
// on_failure=pause — the only policy whose projection depends on release — so an
// apply without a pause operation never touches the control-request store. It
// fails closed: no pause operation, a nil store, or a non-latching release
// request (no pending or completed release row, or a failed one) all leave it
// false. Only a store read error is surfaced; callers fail closed on it. A
// released pause behaves like continue (see
// ApplyControlRequest.ReleasesPausedRollout).
func ReleaseLatched(ctx context.Context, stor Storage, applyID int64, ops []*ApplyOperation) (bool, error) {
	if !AnyPauseOnFailure(ops) {
		return false, nil
	}
	if stor == nil {
		return false, nil
	}
	requests := stor.ControlRequests()
	if requests == nil {
		return false, nil
	}
	req, err := requests.GetByOperation(ctx, applyID, ControlOperationRelease)
	if err != nil {
		return false, err
	}
	return req.ReleasesPausedRollout(), nil
}
